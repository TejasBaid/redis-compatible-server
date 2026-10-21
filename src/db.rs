//! The keyspace: 16 logical databases partitioned into lock-striped shards.
//!
//! A key's 64-bit hash picks its shard (top bits) and its bucket inside the
//! shard's [`Dict`] (low bits), so the hash is computed once per key. Each
//! shard owns, for every logical database, a main dict and an expires dict
//! (like Redis), plus the per-shard registries that must change atomically
//! with the data: blocked clients, WATCHed keys, and the pending snapshot
//! marker used for fork-less point-in-time snapshots.
//!
//! **Locking discipline** (deadlock freedom): shard locks are only ever taken
//! in ascending index order, and no shard lock is acquired while holding any
//! other lock (propagation, pub/sub, client buffers). Commands declare their
//! keys up front, so the dispatcher can take every shard lock a command needs
//! in one ordered pass before running it.

use crate::blocking::BlockedClient;
use crate::dict::Dict;
use crate::persistence::snapshot::SnapshotJob;
use crate::sds::Sds;
use crate::types::Value;
use parking_lot::{Mutex, MutexGuard};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

/// log2 of the number of shards.
pub const SHARD_BITS: u32 = 8;
/// Number of keyspace shards.
pub const NUM_SHARDS: usize = 1 << SHARD_BITS;

/// The shard owning a key with hash `h`.
#[inline]
pub fn shard_of(h: u64) -> usize {
    (h >> (64 - SHARD_BITS)) as usize
}

/// Cache-line padding so neighbouring shard locks don't false-share.
#[repr(align(128))]
pub struct Padded<T>(pub T);

/// High bit of [`Obj::meta`]: the key has an entry in the expires dict.
const FLAG_EXPIRE: u32 = 1 << 31;
/// String modified in place (APPEND/SETRANGE): `OBJECT ENCODING` says `raw`.
const FLAG_RAW: u32 = 1 << 30;
const CLOCK_MASK: u32 = (1 << 24) - 1;

/// A stored value plus Redis' 24-bit LRU clock / LFU counter.
pub struct Obj {
    /// The value.
    pub value: Value,
    /// Bit 31: has TTL. Bit 30: string modified in place (`raw`).
    /// Bits 0..24: LRU timestamp (seconds) or LFU `(minutes << 8) | counter`.
    pub meta: u32,
}

impl Obj {
    /// Wraps a value with the given access clock.
    #[inline]
    pub fn new(value: Value, clock: u32) -> Self {
        Obj {
            value,
            meta: clock & CLOCK_MASK,
        }
    }
    /// True if the key has a TTL.
    #[inline]
    pub fn has_expire(&self) -> bool {
        self.meta & FLAG_EXPIRE != 0
    }
    #[inline]
    fn set_expire_flag(&mut self, on: bool) {
        if on {
            self.meta |= FLAG_EXPIRE
        } else {
            self.meta &= !FLAG_EXPIRE
        }
    }
    /// True if the string was modified in place.
    #[inline]
    pub fn is_raw(&self) -> bool {
        self.meta & FLAG_RAW != 0
    }
    /// Marks the string as modified in place.
    #[inline]
    pub fn set_raw(&mut self) {
        self.meta |= FLAG_RAW;
    }
    /// `OBJECT ENCODING` name, honoring the raw flag.
    pub fn encoding_name(&self) -> &'static str {
        match &self.value {
            Value::Str(crate::types::RStr::Raw(_)) if self.is_raw() => "raw",
            v => v.encoding_name(),
        }
    }
    /// The 24-bit LRU/LFU field.
    #[inline]
    pub fn clock(&self) -> u32 {
        self.meta & CLOCK_MASK
    }
    /// Replaces the 24-bit LRU/LFU field.
    #[inline]
    pub fn set_clock(&mut self, c: u32) {
        self.meta = (self.meta & !CLOCK_MASK) | (c & CLOCK_MASK);
    }
}

/// Global LRU clock in seconds (24 bits), refreshed by the cron.
pub static LRU_CLOCK: AtomicU32 = AtomicU32::new(0);

/// Current LRU clock value.
#[inline]
pub fn lru_clock() -> u32 {
    LRU_CLOCK.load(Ordering::Relaxed)
}

/// Recomputes the LRU clock from the wall clock.
pub fn update_lru_clock() {
    LRU_CLOCK.store(
        ((crate::util::now_ms() / 1000) as u32) & CLOCK_MASK,
        Ordering::Relaxed,
    );
}

/// Idle time in milliseconds of an object accessed at LRU clock `obj_clock`.
pub fn lru_idle_ms(obj_clock: u32) -> u64 {
    let now = lru_clock();
    let secs = if now >= obj_clock {
        now - obj_clock
    } else {
        now + (CLOCK_MASK - obj_clock)
    };
    secs as u64 * 1000
}

/// LFU initial counter (`LFU_INIT_VAL`).
pub const LFU_INIT_VAL: u32 = 5;

fn lfu_minutes() -> u32 {
    ((crate::util::now_ms() / 60_000) as u32) & 0xFFFF
}

/// Initial LFU field for a new key.
pub fn lfu_initial() -> u32 {
    (lfu_minutes() << 8) | LFU_INIT_VAL
}

/// Counter after applying time decay (`LFUDecrAndReturn`).
pub fn lfu_decayed(field: u32, decay_time: u32) -> u32 {
    let ldt = field >> 8;
    let counter = field & 0xFF;
    let now = lfu_minutes();
    let elapsed = if now >= ldt {
        now - ldt
    } else {
        65535 - ldt + now
    };
    let periods = elapsed.checked_div(decay_time).unwrap_or(0);
    counter.saturating_sub(periods)
}

/// New LFU field after an access (`updateLFU`).
pub fn lfu_touch(field: u32, log_factor: u32, decay_time: u32) -> u32 {
    let mut counter = lfu_decayed(field, decay_time);
    if counter < 255 {
        let r = (crate::util::rand_u64() >> 11) as f64 / (1u64 << 53) as f64;
        let base = counter.saturating_sub(LFU_INIT_VAL) as f64;
        let p = 1.0 / (base * log_factor as f64 + 1.0);
        if r < p {
            counter += 1;
        }
    }
    (lfu_minutes() << 8) | counter
}

/// One logical database's slice of a shard.
#[derive(Default)]
pub struct Db {
    /// key → value.
    pub main: Dict<Obj>,
    /// key → absolute expiry (ms since epoch).
    pub expires: Dict<u64>,
}

impl Db {
    /// Number of keys.
    #[inline]
    pub fn len(&self) -> usize {
        self.main.len()
    }

    /// True if empty.
    pub fn is_empty(&self) -> bool {
        self.main.is_empty()
    }

    /// Expiry deadline of a key known to be present.
    #[inline]
    pub fn expire_of(&self, h: u64, key: &[u8], obj: &Obj) -> Option<u64> {
        if obj.has_expire() {
            self.expires.get_h(h, key).copied()
        } else {
            None
        }
    }

    /// Deadline lookup without an object reference.
    pub fn expire_of_key(&self, h: u64, key: &[u8]) -> Option<u64> {
        if self.expires.is_empty() {
            return None;
        }
        self.expires.get_h(h, key).copied()
    }

    /// True if the key exists but its deadline has passed.
    #[inline]
    pub fn is_expired(&self, h: u64, key: &[u8], now: u64) -> bool {
        match self.main.get_h(h, key) {
            Some(o) if o.has_expire() => self.expires.get_h(h, key).is_some_and(|&t| t <= now),
            _ => false,
        }
    }

    /// Inserts or overwrites. The TTL is cleared unless `keep_ttl`.
    pub fn set(&mut self, h: u64, key: &[u8], value: Value, clock: u32, keep_ttl: bool) {
        match self.main.get_mut_h(h, key) {
            Some(obj) => {
                obj.value = value;
                obj.meta &= !FLAG_RAW;
                if !keep_ttl && obj.has_expire() {
                    obj.set_expire_flag(false);
                    self.expires.remove_h(h, key);
                }
            }
            None => self
                .main
                .insert_new_h(h, Sds::from_slice(key), Obj::new(value, clock)),
        }
    }

    /// Inserts a key the caller knows is absent.
    #[inline]
    pub fn insert_new(&mut self, h: u64, key: Sds, obj: Obj) {
        self.main.insert_new_h(h, key, obj);
    }

    /// Deletes a key and its TTL.
    pub fn remove(&mut self, h: u64, key: &[u8]) -> Option<Obj> {
        let (_, obj) = self.main.remove_h(h, key)?;
        if obj.has_expire() {
            self.expires.remove_h(h, key);
        }
        Some(obj)
    }

    /// Sets the deadline of an existing key; returns false if absent.
    pub fn set_expire(&mut self, h: u64, key: &[u8], when: u64) -> bool {
        match self.main.get_mut_h(h, key) {
            Some(obj) => {
                obj.set_expire_flag(true);
                self.expires.insert_h(h, Sds::from_slice(key), when);
                true
            }
            None => false,
        }
    }

    /// Removes a TTL; returns true if one was removed.
    pub fn persist(&mut self, h: u64, key: &[u8]) -> bool {
        match self.main.get_mut_h(h, key) {
            Some(obj) if obj.has_expire() => {
                obj.set_expire_flag(false);
                self.expires.remove_h(h, key);
                true
            }
            _ => false,
        }
    }

    /// Drops everything.
    pub fn clear(&mut self) {
        self.main.clear();
        self.expires.clear();
    }
}

/// Dirty flag shared between a client's `WATCH` registrations.
#[derive(Default, Debug)]
pub struct WatchFlag {
    /// Set when any watched key is modified.
    pub dirty: AtomicBool,
}

/// One lock-protected partition of the keyspace.
pub struct Shard {
    /// This shard's index.
    pub idx: usize,
    /// Per logical database data.
    pub dbs: Vec<Db>,
    /// Clients blocked on keys of this shard, FIFO per key.
    pub blocked: HashMap<(usize, Sds), VecDeque<Arc<BlockedClient>>, foldhash::fast::RandomState>,
    /// WATCH registrations for keys of this shard.
    pub watched: HashMap<(usize, Sds), Vec<Arc<WatchFlag>>, foldhash::fast::RandomState>,
    /// A snapshot started but this shard is not serialized yet: serialize
    /// before the first mutation (copy-before-write).
    pub snap: Option<Arc<SnapshotJob>>,
    /// Active-expire scan cursor per database.
    pub expire_cursor: Vec<u64>,
}

impl Shard {
    fn new(idx: usize, databases: usize) -> Self {
        Shard {
            idx,
            dbs: (0..databases).map(|_| Db::default()).collect(),
            blocked: HashMap::default(),
            watched: HashMap::default(),
            snap: None,
            expire_cursor: vec![0; databases],
        }
    }

    /// Must be called before mutating a shard: if a snapshot is waiting for
    /// this shard, serialize its current (point-in-time) contents first.
    #[inline]
    pub fn before_write(&mut self) {
        if let Some(job) = self.snap.take() {
            job.capture(self);
        }
    }

    /// Marks WATCHers of `(db, key)` dirty.
    #[inline]
    pub fn touch_watched(&self, db: usize, key: &[u8]) {
        if self.watched.is_empty() {
            return;
        }
        if let Some(list) = self.watched.get(&(db, Sds::from_slice(key))) {
            for w in list {
                w.dirty.store(true, Ordering::Release);
            }
        }
    }

    /// Marks every WATCHer of `db` dirty whose key currently exists
    /// (FLUSHDB/FLUSHALL semantics); `db = None` means every database.
    pub fn touch_all_watched(&self, db: Option<usize>) {
        for ((d, key), list) in &self.watched {
            if (db.is_none() || db == Some(*d)) && self.dbs[*d].main.contains_key(key) {
                for w in list {
                    w.dirty.store(true, Ordering::Release);
                }
            }
        }
    }

    /// True if any client is blocked on `(db, key)`.
    #[inline]
    pub fn has_blocked(&self, db: usize, key: &[u8]) -> bool {
        !self.blocked.is_empty() && self.blocked.contains_key(&(db, Sds::from_slice(key)))
    }

    /// Total keys across databases.
    pub fn key_count(&self) -> usize {
        self.dbs.iter().map(|d| d.len()).sum()
    }
}

/// All shards.
pub struct Keyspace {
    shards: Box<[Padded<Mutex<Shard>>]>,
    /// Number of logical databases.
    pub databases: usize,
}

impl Keyspace {
    /// Creates an empty keyspace.
    pub fn new(databases: usize) -> Self {
        Keyspace {
            shards: (0..NUM_SHARDS)
                .map(|i| Padded(Mutex::new(Shard::new(i, databases))))
                .collect(),
            databases,
        }
    }

    /// Locks shard `i`.
    #[inline]
    pub fn lock(&self, i: usize) -> MutexGuard<'_, Shard> {
        self.shards[i].0.lock()
    }

    /// Locks every shard in order.
    pub fn lock_all(&self) -> Vec<MutexGuard<'_, Shard>> {
        self.shards.iter().map(|s| s.0.lock()).collect()
    }

    /// Locks the shards in `idx` (must be sorted and unique).
    pub fn lock_set(&self, idx: &[usize]) -> Vec<(usize, MutexGuard<'_, Shard>)> {
        debug_assert!(idx.windows(2).all(|w| w[0] < w[1]));
        idx.iter().map(|&i| (i, self.shards[i].0.lock())).collect()
    }

    /// Keys per database, summed across shards (locks one shard at a time).
    pub fn db_sizes(&self) -> Vec<(usize, usize)> {
        let mut keys = vec![0usize; self.databases];
        let mut expires = vec![0usize; self.databases];
        for i in 0..NUM_SHARDS {
            let s = self.lock(i);
            for (d, db) in s.dbs.iter().enumerate() {
                keys[d] += db.main.len();
                expires[d] += db.expires.len();
            }
        }
        keys.into_iter().zip(expires).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn obj_meta_bits() {
        let mut o = Obj::new(Value::string(b"x"), 12345);
        assert!(!o.has_expire());
        o.set_expire_flag(true);
        assert!(o.has_expire());
        assert_eq!(o.clock(), 12345);
        o.set_clock(99);
        assert!(o.has_expire());
        assert_eq!(o.clock(), 99);
    }

    #[test]
    fn db_expire_bookkeeping() {
        let mut db = Db::default();
        let h = crate::dict::hash_key(b"k");
        db.set(h, b"k", Value::string(b"v"), 0, false);
        assert!(db.set_expire(h, b"k", 100));
        assert!(db.is_expired(h, b"k", 100));
        assert!(!db.is_expired(h, b"k", 99));
        db.set(h, b"k", Value::string(b"w"), 0, true);
        assert_eq!(db.expire_of_key(h, b"k"), Some(100));
        db.set(h, b"k", Value::string(b"w"), 0, false);
        assert_eq!(db.expire_of_key(h, b"k"), None);
        assert!(db.set_expire(h, b"k", 5));
        assert!(db.persist(h, b"k"));
        assert!(db.expires.is_empty());
        assert!(db.remove(h, b"k").is_some());
        assert!(db.is_empty());
    }

    #[test]
    fn lfu_counter_grows_logarithmically() {
        let mut f = lfu_initial();
        for _ in 0..1000 {
            f = lfu_touch(f, 10, 1);
        }
        let c = f & 0xFF;
        assert!(c > LFU_INIT_VAL && c < 40, "counter {c}");
    }
}
