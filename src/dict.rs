//! `Dict`: a chained hash table with incremental rehashing and Redis' reverse
//! binary cursor scan.
//!
//! This is the structure behind every keyspace, large hash, large set and the
//! member→score map of large sorted sets. It follows Redis' `dict.c`:
//!
//! * Power-of-two bucket arrays with separate chaining.
//! * **Incremental rehashing.** Growing or shrinking allocates a second table
//!   and moves one bucket per mutating operation (plus a time-bounded amount
//!   from the server cron), so no single command ever pays for rehashing a
//!   million keys.
//! * **`scan` with a reverse-binary cursor.** The cursor increments its
//!   *reversed* bits, so every element present for the whole scan is returned
//!   at least once even if the table grows or shrinks between calls. A
//!   `HashMap` cannot offer this guarantee, which is why `SCAN`/`HSCAN`/`SSCAN`
//!   need a hand-rolled table.
//! * Random sampling primitives for eviction and `RANDOMKEY`.

use crate::sds::Sds;
use std::hash::BuildHasher;
use std::sync::OnceLock;

static SEED: OnceLock<u64> = OnceLock::new();

/// Hashes a key with the process-wide seeded hasher. The same hash is used to
/// pick a keyspace shard and a bucket, so it is computed once per key.
#[inline]
pub fn hash_key(key: &[u8]) -> u64 {
    let seed = *SEED.get_or_init(crate::util::rand_u64);
    foldhash::fast::FixedState::with_seed(seed).hash_one(key)
}

const INITIAL_SIZE: usize = 4;
/// Shrink when fewer than 1/8 of the buckets are used (Redis 7.2 policy).
const MIN_FILL_RATIO: usize = 8;
/// Visit at most this many empty buckets per rehash step.
const EMPTY_VISITS_PER_STEP: usize = 10;

struct Node<V> {
    next: Option<Box<Node<V>>>,
    key: Sds,
    value: V,
}

struct Table<V> {
    buckets: Vec<Option<Box<Node<V>>>>,
    used: usize,
}

impl<V> Table<V> {
    fn empty() -> Self {
        Table {
            buckets: Vec::new(),
            used: 0,
        }
    }

    fn with_size(n: usize) -> Self {
        let mut buckets = Vec::with_capacity(n);
        buckets.resize_with(n, || None);
        Table { buckets, used: 0 }
    }

    #[inline]
    fn mask(&self) -> usize {
        self.buckets.len().wrapping_sub(1)
    }

    fn find(&self, hash: u64, key: &[u8]) -> Option<&Node<V>> {
        if self.buckets.is_empty() {
            return None;
        }
        let mut cur = self.buckets[hash as usize & self.mask()].as_deref();
        while let Some(n) = cur {
            if n.key.as_bytes() == key {
                return Some(n);
            }
            cur = n.next.as_deref();
        }
        None
    }

    fn find_mut(&mut self, hash: u64, key: &[u8]) -> Option<&mut Node<V>> {
        if self.buckets.is_empty() {
            return None;
        }
        let idx = hash as usize & self.mask();
        let mut cur = self.buckets[idx].as_deref_mut();
        while let Some(n) = cur {
            if n.key.as_bytes() == key {
                return Some(n);
            }
            cur = n.next.as_deref_mut();
        }
        None
    }

    fn remove(&mut self, hash: u64, key: &[u8]) -> Option<Box<Node<V>>> {
        if self.buckets.is_empty() {
            return None;
        }
        let idx = hash as usize & self.mask();
        let mut link = &mut self.buckets[idx];
        loop {
            let found = link.as_ref()?.key.as_bytes() == key;
            if found {
                let mut node = link.take().expect("checked above");
                *link = node.next.take();
                self.used -= 1;
                return Some(node);
            }
            link = &mut link.as_mut().expect("checked above").next;
        }
    }

    fn push_front(&mut self, hash: u64, mut node: Box<Node<V>>) {
        let idx = hash as usize & self.mask();
        node.next = self.buckets[idx].take();
        self.buckets[idx] = Some(node);
        self.used += 1;
    }
}

impl<V> Drop for Table<V> {
    fn drop(&mut self) {
        // Unlink chains iteratively so a pathological chain cannot overflow the
        // stack through recursive `Box` drops.
        for b in self.buckets.iter_mut() {
            let mut cur = b.take();
            while let Some(mut n) = cur {
                cur = n.next.take();
            }
        }
    }
}

/// A hash table keyed by [`Sds`]. See the module docs.
pub struct Dict<V> {
    t: [Table<V>; 2],
    /// Next bucket of `t[0]` to migrate, or `None` when not rehashing.
    rehash_idx: Option<usize>,
}

impl<V> Default for Dict<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V> Dict<V> {
    /// An empty dictionary; no memory is allocated until the first insert.
    pub fn new() -> Self {
        Dict {
            t: [Table::empty(), Table::empty()],
            rehash_idx: None,
        }
    }

    /// Number of entries.
    #[inline]
    pub fn len(&self) -> usize {
        self.t[0].used + self.t[1].used
    }

    /// True when the dictionary has no entries.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// True while an incremental rehash is in progress.
    pub fn is_rehashing(&self) -> bool {
        self.rehash_idx.is_some()
    }

    /// Total number of buckets across both tables (for INFO/MEMORY).
    pub fn bucket_count(&self) -> usize {
        self.t[0].buckets.len() + self.t[1].buckets.len()
    }

    /// Approximate heap bytes used by buckets and nodes (excluding values' own
    /// heap allocations and keys stored out of line).
    pub fn overhead_bytes(&self) -> usize {
        self.bucket_count() * std::mem::size_of::<usize>()
            + self.len() * std::mem::size_of::<Node<V>>()
    }

    /// Removes every entry and frees the tables.
    pub fn clear(&mut self) {
        self.t = [Table::empty(), Table::empty()];
        self.rehash_idx = None;
    }

    /// Looks up `key`.
    #[inline]
    pub fn get(&self, key: &[u8]) -> Option<&V> {
        self.get_h(hash_key(key), key)
    }

    /// Looks up `key` using a precomputed [`hash_key`].
    #[inline]
    pub fn get_h(&self, hash: u64, key: &[u8]) -> Option<&V> {
        if let Some(n) = self.t[0].find(hash, key) {
            return Some(&n.value);
        }
        if self.rehash_idx.is_some() {
            return self.t[1].find(hash, key).map(|n| &n.value);
        }
        None
    }

    /// Looks up `key` and returns the stored key too.
    pub fn get_key_value(&self, key: &[u8]) -> Option<(&Sds, &V)> {
        let hash = hash_key(key);
        if let Some(n) = self.t[0].find(hash, key) {
            return Some((&n.key, &n.value));
        }
        if self.rehash_idx.is_some() {
            return self.t[1].find(hash, key).map(|n| (&n.key, &n.value));
        }
        None
    }

    /// True if `key` is present.
    #[inline]
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.get(key).is_some()
    }

    /// Mutable lookup. Performs one incremental rehash step.
    #[inline]
    pub fn get_mut(&mut self, key: &[u8]) -> Option<&mut V> {
        self.get_mut_h(hash_key(key), key)
    }

    /// Mutable lookup with a precomputed hash.
    pub fn get_mut_h(&mut self, hash: u64, key: &[u8]) -> Option<&mut V> {
        self.rehash_step();
        let rehashing = self.rehash_idx.is_some();
        let [t0, t1] = &mut self.t;
        if let Some(n) = t0.find_mut(hash, key) {
            return Some(&mut n.value);
        }
        if rehashing {
            return t1.find_mut(hash, key).map(|n| &mut n.value);
        }
        None
    }

    /// Inserts or replaces. Returns the previous value if the key existed.
    #[inline]
    pub fn insert(&mut self, key: Sds, value: V) -> Option<V> {
        let h = hash_key(&key);
        self.insert_h(h, key, value)
    }

    /// Inserts or replaces using a precomputed hash.
    pub fn insert_h(&mut self, hash: u64, key: Sds, value: V) -> Option<V> {
        if let Some(slot) = self.get_mut_h(hash, &key) {
            return Some(std::mem::replace(slot, value));
        }
        self.insert_new_h(hash, key, value);
        None
    }

    /// Inserts a key that the caller knows is absent.
    pub fn insert_new_h(&mut self, hash: u64, key: Sds, value: V) {
        // Like dictAdd: every insert advances an in-progress rehash, otherwise
        // an insert-only workload would never finish migrating and the new
        // table's chains would grow without bound.
        self.rehash_step();
        self.expand_if_needed();
        let node = Box::new(Node {
            next: None,
            key,
            value,
        });
        if self.rehash_idx.is_some() {
            self.t[1].push_front(hash, node);
        } else {
            self.t[0].push_front(hash, node);
        }
    }

    /// Returns the value for `key`, inserting `make()` first if absent. The
    /// boolean is true when an insert happened.
    pub fn get_or_insert_with(&mut self, key: &[u8], make: impl FnOnce() -> V) -> (&mut V, bool) {
        let hash = hash_key(key);
        // Two-phase to keep the borrow checker happy without unsafe.
        let exists = {
            self.rehash_step();
            self.t[0].find(hash, key).is_some()
                || (self.rehash_idx.is_some() && self.t[1].find(hash, key).is_some())
        };
        if !exists {
            self.insert_new_h(hash, Sds::from_slice(key), make());
        }
        let v = self.get_mut_h(hash, key).expect("present after insert");
        (v, !exists)
    }

    /// Removes `key`, returning the stored key and value.
    #[inline]
    pub fn remove(&mut self, key: &[u8]) -> Option<(Sds, V)> {
        self.remove_h(hash_key(key), key)
    }

    /// Removes `key` using a precomputed hash.
    pub fn remove_h(&mut self, hash: u64, key: &[u8]) -> Option<(Sds, V)> {
        self.rehash_step();
        let node = match self.t[0].remove(hash, key) {
            Some(n) => Some(n),
            None if self.rehash_idx.is_some() => self.t[1].remove(hash, key),
            None => None,
        }?;
        let Node { key, value, .. } = *node;
        self.shrink_if_needed();
        Some((key, value))
    }

    fn expand_if_needed(&mut self) {
        if self.rehash_idx.is_some() {
            return;
        }
        let size = self.t[0].buckets.len();
        if size == 0 {
            self.t[0] = Table::with_size(INITIAL_SIZE);
            return;
        }
        if self.t[0].used >= size {
            self.start_resize((self.t[0].used + 1).next_power_of_two());
        }
    }

    /// Starts shrinking when the table is mostly empty. Called after removals
    /// and from the server cron.
    pub fn shrink_if_needed(&mut self) {
        if self.rehash_idx.is_some() {
            return;
        }
        let size = self.t[0].buckets.len();
        let used = self.t[0].used;
        if size > INITIAL_SIZE && used * MIN_FILL_RATIO < size {
            let target = used.max(INITIAL_SIZE).next_power_of_two();
            if target < size {
                self.start_resize(target);
            }
        }
        if used == 0 && size > 0 {
            self.clear();
        }
    }

    fn start_resize(&mut self, new_size: usize) {
        debug_assert!(self.rehash_idx.is_none());
        self.t[1] = Table::with_size(new_size);
        self.rehash_idx = Some(0);
    }

    /// Migrates one bucket (visiting at most a few empty ones). Returns true if
    /// more rehashing work remains.
    pub fn rehash_step(&mut self) -> bool {
        let Some(mut idx) = self.rehash_idx else {
            return false;
        };
        let mut empty_visits = EMPTY_VISITS_PER_STEP;
        while idx < self.t[0].buckets.len() && self.t[0].buckets[idx].is_none() {
            idx += 1;
            empty_visits -= 1;
            if empty_visits == 0 {
                self.rehash_idx = Some(idx);
                return true;
            }
        }
        if idx < self.t[0].buckets.len() {
            let mut chain = self.t[0].buckets[idx].take();
            while let Some(mut node) = chain {
                chain = node.next.take();
                self.t[0].used -= 1;
                let h = hash_key(&node.key);
                self.t[1].push_front(h, node);
            }
            idx += 1;
        }
        if idx >= self.t[0].buckets.len() {
            debug_assert_eq!(self.t[0].used, 0);
            self.t[0] = std::mem::replace(&mut self.t[1], Table::empty());
            self.rehash_idx = None;
            false
        } else {
            self.rehash_idx = Some(idx);
            true
        }
    }

    /// Rehashes for up to `steps` steps (used by the cron for idle tables).
    pub fn rehash_steps(&mut self, steps: usize) {
        for _ in 0..steps {
            if !self.rehash_step() {
                break;
            }
        }
    }

    /// Iterates every entry (order is unspecified).
    pub fn iter(&self) -> Iter<'_, V> {
        Iter {
            dict: self,
            table: 0,
            bucket: 0,
            node: None,
        }
    }

    /// Consumes the dictionary, yielding owned entries.
    pub fn into_entries(mut self) -> Vec<(Sds, V)> {
        let mut out = Vec::with_capacity(self.len());
        for t in self.t.iter_mut() {
            for b in t.buckets.iter_mut() {
                let mut cur = b.take();
                while let Some(mut n) = cur {
                    cur = n.next.take();
                    let Node { key, value, .. } = *n;
                    out.push((key, value));
                }
            }
            t.used = 0;
        }
        out
    }

    /// Visits every value mutably.
    pub fn for_each_mut(&mut self, mut f: impl FnMut(&Sds, &mut V)) {
        for t in self.t.iter_mut() {
            for b in t.buckets.iter_mut() {
                let mut cur = b.as_deref_mut();
                while let Some(n) = cur {
                    f(&n.key, &mut n.value);
                    cur = n.next.as_deref_mut();
                }
            }
        }
    }

    fn emit_bucket(t: &Table<V>, idx: usize, f: &mut impl FnMut(&Sds, &V)) {
        let mut cur = t.buckets[idx].as_deref();
        while let Some(n) = cur {
            f(&n.key, &n.value);
            cur = n.next.as_deref();
        }
    }

    /// One step of a `SCAN`-style iteration. Calls `f` for the entries at
    /// `cursor` and returns the next cursor (0 when the scan is complete).
    ///
    /// Guarantee (same as Redis): every entry present for the whole duration
    /// of a full scan is reported at least once, even across resizes; entries
    /// may be reported more than once.
    pub fn scan(&self, cursor: u64, mut f: impl FnMut(&Sds, &V)) -> u64 {
        if self.is_empty() {
            return 0;
        }
        let mut v = cursor;
        if self.rehash_idx.is_none() {
            let t0 = &self.t[0];
            let m0 = t0.mask() as u64;
            Self::emit_bucket(t0, (v & m0) as usize, &mut f);
            v |= !m0;
            v = v.reverse_bits().wrapping_add(1).reverse_bits();
        } else {
            let (small, big) = if self.t[0].buckets.len() <= self.t[1].buckets.len() {
                (&self.t[0], &self.t[1])
            } else {
                (&self.t[1], &self.t[0])
            };
            let m0 = small.mask() as u64;
            let m1 = big.mask() as u64;
            Self::emit_bucket(small, (v & m0) as usize, &mut f);
            loop {
                Self::emit_bucket(big, (v & m1) as usize, &mut f);
                v |= !m1;
                v = v.reverse_bits().wrapping_add(1).reverse_bits();
                if v & (m0 ^ m1) == 0 {
                    break;
                }
            }
        }
        v
    }

    /// A uniformly chosen bucket's random entry (`dictGetRandomKey`).
    pub fn random_entry(&self) -> Option<(&Sds, &V)> {
        if self.is_empty() {
            return None;
        }
        // Pick a random non-empty bucket across both tables. Buckets of t0
        // before `rehash_idx` are known to be empty during a rehash.
        let start0 = self.rehash_idx.unwrap_or(0);
        let n0 = self.t[0].buckets.len() - start0.min(self.t[0].buckets.len());
        let n1 = if self.rehash_idx.is_some() {
            self.t[1].buckets.len()
        } else {
            0
        };
        let total = (n0 + n1) as u64;
        loop {
            let r = crate::util::rand_below(total) as usize;
            let head = if r < n0 {
                self.t[0].buckets[start0 + r].as_deref()
            } else {
                self.t[1].buckets[r - n0].as_deref()
            };
            if let Some(first) = head {
                let mut len = 0;
                let mut cur = Some(first);
                while let Some(n) = cur {
                    len += 1;
                    cur = n.next.as_deref();
                }
                let mut pick = crate::util::rand_below(len) as usize;
                let mut cur = first;
                while pick > 0 {
                    cur = cur.next.as_deref().expect("within chain length");
                    pick -= 1;
                }
                return Some((&cur.key, &cur.value));
            }
        }
    }

    /// Collects up to `count` entries starting at a random bucket
    /// (`dictGetSomeKeys`): cheap, not uniformly random, good enough for
    /// eviction sampling.
    pub fn sample<'a>(&'a self, count: usize, out: &mut Vec<(&'a Sds, &'a V)>) {
        if self.is_empty() || count == 0 {
            return;
        }
        let count = count.min(self.len());
        let tables: &[&Table<V>] = if self.rehash_idx.is_some() {
            &[&self.t[0], &self.t[1]]
        } else {
            &[&self.t[0]]
        };
        let max_size = tables.iter().map(|t| t.buckets.len()).max().unwrap_or(0);
        let mut i = crate::util::rand_below(max_size as u64) as usize;
        let mut steps = count * 10;
        let start_len = out.len();
        while out.len() - start_len < count && steps > 0 {
            for t in tables {
                if i < t.buckets.len() {
                    let mut cur = t.buckets[i].as_deref();
                    while let Some(n) = cur {
                        out.push((&n.key, &n.value));
                        if out.len() - start_len == count {
                            return;
                        }
                        cur = n.next.as_deref();
                    }
                }
            }
            i = (i + 1) & (max_size - 1);
            steps -= 1;
        }
    }
}

/// Iterator over a [`Dict`].
pub struct Iter<'a, V> {
    dict: &'a Dict<V>,
    table: usize,
    bucket: usize,
    node: Option<&'a Node<V>>,
}

impl<'a, V> Iterator for Iter<'a, V> {
    type Item = (&'a Sds, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(n) = self.node {
                self.node = n.next.as_deref();
                return Some((&n.key, &n.value));
            }
            let t = self.dict.t.get(self.table)?;
            if self.bucket >= t.buckets.len() {
                self.table += 1;
                self.bucket = 0;
                continue;
            }
            self.node = t.buckets[self.bucket].as_deref();
            self.bucket += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::{HashMap, HashSet};

    fn k(i: usize) -> Sds {
        Sds::from(format!("key:{i}").as_bytes())
    }

    #[test]
    fn insert_get_remove() {
        let mut d = Dict::new();
        for i in 0..1000 {
            assert!(d.insert(k(i), i).is_none());
        }
        assert_eq!(d.len(), 1000);
        for i in 0..1000 {
            assert_eq!(d.get(&k(i)), Some(&i));
        }
        assert_eq!(d.insert(k(5), 55), Some(5));
        for i in (0..1000).step_by(2) {
            assert_eq!(d.remove(&k(i)).map(|(_, v)| v), Some(i));
        }
        assert_eq!(d.len(), 500);
        assert!(d.get(&k(0)).is_none());
        assert_eq!(d.get(&k(1)), Some(&1));
    }

    #[test]
    fn insert_only_workload_keeps_chains_short() {
        let mut d = Dict::new();
        for i in 0..100_000 {
            let k = k(i);
            d.insert_new_h(hash_key(&k), k, ());
        }
        // Growth must have kept up: at most one rehash in flight, and the
        // load factor stays bounded.
        assert!(
            d.bucket_count() >= d.len() / 2,
            "buckets {} for {} keys",
            d.bucket_count(),
            d.len()
        );
        let longest =
            d.t.iter()
                .flat_map(|t| t.buckets.iter())
                .map(|b| {
                    let mut n = 0;
                    let mut cur = b.as_deref();
                    while let Some(node) = cur {
                        n += 1;
                        cur = node.next.as_deref();
                    }
                    n
                })
                .max()
                .unwrap();
        assert!(longest < 20, "longest chain {longest}");
    }

    #[test]
    fn shrinks_after_mass_delete() {
        let mut d = Dict::new();
        for i in 0..10_000 {
            d.insert(k(i), ());
        }
        for i in 0..9_990 {
            d.remove(&k(i));
        }
        while d.rehash_step() {}
        d.shrink_if_needed();
        while d.rehash_step() {}
        assert!(d.bucket_count() <= 64, "buckets = {}", d.bucket_count());
        assert_eq!(d.len(), 10);
        assert_eq!(d.iter().count(), 10);
    }

    #[test]
    fn scan_survives_growth() {
        let mut d = Dict::new();
        for i in 0..100 {
            d.insert(k(i), i);
        }
        let mut seen = HashSet::new();
        let mut cursor = 0;
        let mut extra = 100;
        loop {
            cursor = d.scan(cursor, |key, _| {
                seen.insert(key.clone());
            });
            // Grow the table mid-scan (bounded, so the scan can finish).
            if extra < 3000 {
                for _ in 0..50 {
                    d.insert(k(extra), extra);
                    extra += 1;
                }
            }
            if cursor == 0 {
                break;
            }
        }
        for i in 0..100 {
            assert!(seen.contains(&k(i)), "missed key {i}");
        }
    }

    #[test]
    fn scan_survives_shrink() {
        let mut d = Dict::new();
        for i in 0..5000 {
            d.insert(k(i), i);
        }
        let mut seen = HashSet::new();
        let mut cursor = 0;
        let mut next_del = 100;
        loop {
            cursor = d.scan(cursor, |key, _| {
                seen.insert(key.clone());
            });
            for _ in 0..200 {
                if next_del < 5000 {
                    d.remove(&k(next_del));
                    next_del += 1;
                }
            }
            if cursor == 0 {
                break;
            }
        }
        for i in 0..100 {
            assert!(seen.contains(&k(i)), "missed key {i}");
        }
    }

    #[test]
    fn random_and_sample() {
        let mut d = Dict::new();
        assert!(d.random_entry().is_none());
        for i in 0..64 {
            d.insert(k(i), i);
        }
        let mut hits = HashSet::new();
        for _ in 0..2000 {
            hits.insert(*d.random_entry().unwrap().1);
        }
        assert!(
            hits.len() > 50,
            "random_entry is badly skewed: {}",
            hits.len()
        );
        let mut out = Vec::new();
        d.sample(5, &mut out);
        assert_eq!(out.len(), 5);
    }

    #[derive(Debug, Clone)]
    enum Op {
        Insert(u16, u32),
        Remove(u16),
        Get(u16),
        Steps(u8),
    }

    fn op() -> impl Strategy<Value = Op> {
        prop_oneof![
            (0u16..300, any::<u32>()).prop_map(|(k, v)| Op::Insert(k, v)),
            (0u16..300).prop_map(Op::Remove),
            (0u16..300).prop_map(Op::Get),
            any::<u8>().prop_map(Op::Steps),
        ]
    }

    proptest! {
        /// The dict behaves exactly like a `HashMap` under any sequence of
        /// operations, including mid-rehash states.
        #[test]
        fn matches_hashmap_model(ops in proptest::collection::vec(op(), 1..600)) {
            let mut d = Dict::new();
            let mut m: HashMap<Vec<u8>, u32> = HashMap::new();
            for op in ops {
                match op {
                    Op::Insert(key, v) => {
                        let key = format!("{key}").into_bytes();
                        prop_assert_eq!(d.insert(Sds::from_slice(&key), v), m.insert(key, v));
                    }
                    Op::Remove(key) => {
                        let key = format!("{key}").into_bytes();
                        prop_assert_eq!(d.remove(&key).map(|(_, v)| v), m.remove(&key));
                    }
                    Op::Get(key) => {
                        let key = format!("{key}").into_bytes();
                        prop_assert_eq!(d.get(&key), m.get(&key));
                    }
                    Op::Steps(n) => d.rehash_steps(n as usize % 4),
                }
                prop_assert_eq!(d.len(), m.len());
            }
            let mut all: Vec<_> = d.iter().map(|(k, v)| (k.to_vec(), *v)).collect();
            all.sort();
            let mut want: Vec<_> = m.into_iter().collect();
            want.sort();
            prop_assert_eq!(all, want);
        }

        /// A full scan returns every element (possibly with duplicates).
        #[test]
        fn full_scan_is_complete(n in 0usize..400, steps in 0usize..20) {
            let mut d = Dict::new();
            for i in 0..n { d.insert(k(i), ()); }
            d.rehash_steps(steps);
            let mut seen = HashSet::new();
            let mut c = 0;
            loop {
                c = d.scan(c, |key, _| { seen.insert(key.clone()); });
                if c == 0 { break; }
            }
            prop_assert_eq!(seen.len(), n);
        }
    }
}
