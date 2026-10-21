//! Command table, execution context and the dispatcher.
//!
//! Every command declares its arity, flags and key positions. The dispatcher
//! uses the key positions to lock exactly the shards a command touches (in
//! ascending order, so concurrent commands can never deadlock), runs the
//! handler against a [`DbView`] of those shards, then propagates the effect to
//! the AOF/replicas while the locks are still held. Transactions (`EXEC`) lock
//! the union of all queued commands' shards once, which makes them atomic
//! with respect to every other client.

pub mod conn;
pub mod hash;
pub mod keys;
pub mod list;
pub mod multi;
pub mod pubsub;
pub mod repl;
pub mod server;
pub mod set;
pub mod stream;
pub mod string;
pub mod zset;

use crate::blocking::{BlockKind, BlockedClient};
use crate::db::{Obj, Shard, lru_clock, shard_of};
use crate::dict::hash_key;
use crate::resp::{Reply, encode_command};
use crate::sds::Sds;
use crate::server::client::ClientState;
use crate::server::{Server, WorkerStats};
use crate::types::{Limits, Value};
use parking_lot::MutexGuard;
use std::collections::HashMap;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, OnceLock};

/// `WRONGTYPE` error text.
pub const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";
/// Integer parse error text.
pub const NOT_INT: &str = "ERR value is not an integer or out of range";
/// Float parse error text.
pub const NOT_FLOAT: &str = "ERR value is not a valid float";
/// Generic syntax error text.
pub const SYNTAX: &str = "ERR syntax error";
/// Missing key error text.
pub const NO_KEY: &str = "ERR no such key";
/// Overflow error text.
pub const OVERFLOW: &str = "ERR increment or decrement would overflow";

/// Command flags.
pub mod flags {
    /// Modifies data.
    pub const WRITE: u32 = 1;
    /// Only reads data.
    pub const READONLY: u32 = 1 << 1;
    /// May increase memory usage (rejected/evicts under `maxmemory`).
    pub const DENYOOM: u32 = 1 << 2;
    /// Administrative.
    pub const ADMIN: u32 = 1 << 3;
    /// Pub/sub related.
    pub const PUBSUB: u32 = 1 << 4;
    /// May block.
    pub const BLOCKING: u32 = 1 << 5;
    /// Allowed while loading.
    pub const LOADING: u32 = 1 << 6;
    /// O(1)-ish.
    pub const FAST: u32 = 1 << 7;
    /// Not allowed inside MULTI.
    pub const NO_MULTI: u32 = 1 << 8;
    /// Needs every shard locked (FLUSHALL, KEYS, DBSIZE, ...).
    pub const ALL_SHARDS: u32 = 1 << 9;
    /// The handler locks shards itself, one at a time (SCAN, INFO, ...).
    pub const SELF_LOCK: u32 = 1 << 10;
    /// Allowed before AUTH.
    pub const NO_AUTH: u32 = 1 << 11;
    /// Allowed in RESP2 subscriber mode.
    pub const SUB_OK: u32 = 1 << 12;
    /// Allowed on a stale replica / for internal use.
    pub const STALE: u32 = 1 << 13;
    /// Never propagated even when it reports changes.
    pub const NO_PROPAGATE: u32 = 1 << 14;
}
use flags::*;

/// A command handler.
pub type Handler = fn(&mut Ctx<'_, '_>, &[&[u8]]);
/// Custom key-position extractor (returns argument indices).
pub type KeysFn = fn(&[&[u8]]) -> Vec<usize>;

/// Where a command's keys are.
#[derive(Clone, Copy)]
pub enum KeySpec {
    /// No keys.
    None,
    /// `(first, last, step)`; negative `last` counts from the end.
    Range(i32, i32, i32),
    /// Computed from the arguments.
    Custom(KeysFn),
}

/// A command table entry.
pub struct Command {
    /// Lowercase name.
    pub name: &'static str,
    /// Redis arity: positive = exact, negative = minimum.
    pub arity: i32,
    /// [`flags`] bits.
    pub flags: u32,
    /// Key positions.
    pub keys: KeySpec,
    /// Implementation.
    pub handler: Handler,
    /// Group for `COMMAND DOCS`.
    pub group: &'static str,
    /// One-line summary for `COMMAND DOCS`.
    pub summary: &'static str,
}

/// Shorthand constructor used by the per-group tables.
pub const fn c(
    name: &'static str,
    arity: i32,
    flags: u32,
    keys: KeySpec,
    handler: Handler,
    group: &'static str,
    summary: &'static str,
) -> Command {
    Command {
        name,
        arity,
        flags,
        keys,
        handler,
        group,
        summary,
    }
}

/// Keys at `(first, last, step)`.
pub const fn k(first: i32, last: i32, step: i32) -> KeySpec {
    KeySpec::Range(first, last, step)
}

/// No keys.
pub const NOKEYS: KeySpec = KeySpec::None;

struct Table {
    cmds: Vec<Command>,
    by_name: HashMap<&'static str, usize>,
}

fn table() -> &'static Table {
    static T: OnceLock<Table> = OnceLock::new();
    T.get_or_init(|| {
        let mut cmds = Vec::new();
        cmds.extend(string::commands());
        cmds.extend(keys::commands());
        cmds.extend(list::commands());
        cmds.extend(hash::commands());
        cmds.extend(set::commands());
        cmds.extend(zset::commands());
        cmds.extend(stream::commands());
        cmds.extend(conn::commands());
        cmds.extend(server::commands());
        cmds.extend(pubsub::commands());
        cmds.extend(multi::commands());
        cmds.extend(repl::commands());
        let by_name = cmds.iter().enumerate().map(|(i, c)| (c.name, i)).collect();
        Table { cmds, by_name }
    })
}

/// All commands.
pub fn all() -> &'static [Command] {
    &table().cmds
}

/// Looks up a command by (case-insensitive) name.
pub fn lookup(name: &[u8]) -> Option<(usize, &'static Command)> {
    let mut buf = [0u8; 32];
    if name.len() > buf.len() {
        return None;
    }
    for (i, c) in name.iter().enumerate() {
        buf[i] = c.to_ascii_lowercase();
    }
    let s = std::str::from_utf8(&buf[..name.len()]).ok()?;
    let t = table();
    t.by_name.get(s).map(|&i| (i, &t.cmds[i]))
}

/// Command by table index.
pub fn by_index(i: usize) -> Option<&'static Command> {
    table().cmds.get(i)
}

/// Argument indices of a command's keys.
pub fn key_positions(cmd: &Command, args: &[&[u8]]) -> Vec<usize> {
    match cmd.keys {
        KeySpec::None => Vec::new(),
        KeySpec::Range(first, last, step) => {
            let n = args.len() as i32;
            let last = if last < 0 { n + last } else { last };
            let mut v = Vec::new();
            let mut i = first;
            while i <= last && i < n && step > 0 {
                v.push(i as usize);
                i += step;
            }
            v
        }
        KeySpec::Custom(f) => f(args).into_iter().filter(|&i| i < args.len()).collect(),
    }
}

/// Keys from a `numkeys` argument at `at` followed by the keys
/// (`ZUNION`, `SINTERCARD`, `LMPOP`, ...).
pub fn numkeys_at(args: &[&[u8]], at: usize) -> Vec<usize> {
    let n = args
        .get(at)
        .and_then(|a| crate::util::parse_i64(a))
        .unwrap_or(0);
    if n <= 0 {
        return Vec::new();
    }
    (at + 1..(at + 1 + n as usize).min(args.len())).collect()
}

enum Locks<'s> {
    None,
    One(usize, MutexGuard<'s, Shard>),
    Many(Vec<(usize, MutexGuard<'s, Shard>)>),
    All(Vec<MutexGuard<'s, Shard>>),
}

impl<'s> Locks<'s> {
    fn acquire(server: &'s Server, mut idx: Vec<usize>, all: bool) -> Self {
        if all {
            return Locks::All(server.keyspace.lock_all());
        }
        idx.sort_unstable();
        idx.dedup();
        match idx.len() {
            0 => Locks::None,
            1 => Locks::One(idx[0], server.keyspace.lock(idx[0])),
            _ => Locks::Many(server.keyspace.lock_set(&idx)),
        }
    }

    #[inline]
    fn get(&mut self, i: usize) -> Option<&mut Shard> {
        match self {
            Locks::None => None,
            Locks::One(j, g) => (*j == i).then_some(&mut **g),
            Locks::Many(v) => v.iter_mut().find(|(j, _)| *j == i).map(|(_, g)| &mut **g),
            Locks::All(v) => v.get_mut(i).map(|g| &mut **g),
        }
    }

    fn get_ref(&self, i: usize) -> Option<&Shard> {
        match self {
            Locks::None => None,
            Locks::One(j, g) => (*j == i).then_some(&**g),
            Locks::Many(v) => v.iter().find(|(j, _)| *j == i).map(|(_, g)| &**g),
            Locks::All(v) => v.get(i).map(|g| &**g),
        }
    }

    fn for_each(&mut self, mut f: impl FnMut(&mut Shard)) {
        match self {
            Locks::None => {}
            Locks::One(_, g) => f(g),
            Locks::Many(v) => v.iter_mut().for_each(|(_, g)| f(g)),
            Locks::All(v) => v.iter_mut().for_each(|g| f(g)),
        }
    }

    fn is_all(&self) -> bool {
        matches!(self, Locks::All(_))
    }
}

/// Which per-key access clock to maintain.
#[derive(Clone, Copy)]
enum ClockMode {
    Lru,
    Lfu { log_factor: u32, decay: u32 },
}

/// The locked shards a command may touch, plus everything that must be
/// recorded about its effects (dirtiness, propagation, ready keys).
pub struct DbView<'s> {
    server: &'s Server,
    locks: Locks<'s>,
    /// Selected database.
    pub sel: usize,
    /// Command time (ms); constant for the whole command / transaction.
    pub now: u64,
    /// Encoding thresholds.
    pub limits: Limits,
    /// Number of changes made (Redis' `server.dirty` delta).
    pub dirty: u64,
    /// Keys deleted by lazy expiry: `(db, key)`, propagated as `DEL`.
    pub expired: Vec<(usize, Sds)>,
    /// Keys that gained data while clients were blocked on them.
    pub ready: Vec<(usize, Sds)>,
    replica: bool,
    /// Commands from our master (or replayed from the AOF while loading)
    /// never treat keys as expired: the master is authoritative.
    authoritative: bool,
    clock: ClockMode,
    hits: u64,
    misses: u64,
}

impl<'s> DbView<'s> {
    fn new(server: &'s Server, locks: Locks<'s>, sel: usize, now: u64) -> Self {
        let policy = server.config.policy();
        let clock = if policy.is_lfu() {
            ClockMode::Lfu {
                log_factor: server.config.lfu_log_factor.load(Relaxed),
                decay: server.config.lfu_decay_time.load(Relaxed),
            }
        } else {
            ClockMode::Lru
        };
        DbView {
            server,
            locks,
            sel,
            now,
            limits: server.config.limits(),
            dirty: 0,
            expired: Vec::new(),
            ready: Vec::new(),
            replica: server.is_replica(),
            authoritative: server.loading.load(Relaxed),
            clock,
            hits: 0,
            misses: 0,
        }
    }

    /// The server.
    pub fn server(&self) -> &'s Server {
        self.server
    }

    #[inline]
    fn shard(&mut self, h: u64) -> &mut Shard {
        let i = shard_of(h);
        match self.locks.get(i) {
            Some(s) => s,
            None => panic!("BUG: shard {i} accessed without its lock (undeclared key)"),
        }
    }

    fn new_clock(&self) -> u32 {
        match self.clock {
            ClockMode::Lru => lru_clock(),
            ClockMode::Lfu { .. } => crate::db::lfu_initial(),
        }
    }

    /// Deletes `key` if its TTL passed (on a master). Returns true when the
    /// key is logically expired.
    fn expire_if_needed(&mut self, h: u64, key: &[u8]) -> bool {
        if self.authoritative {
            return false;
        }
        let (now, sel, replica) = (self.now, self.sel, self.replica);
        let shard = self.shard(h);
        let db = &shard.dbs[sel];
        match db.main.get_h(h, key) {
            Some(o) if o.has_expire() => {}
            _ => return false,
        }
        if !db.expires.get_h(h, key).is_some_and(|&t| t <= now) {
            return false;
        }
        if replica {
            return true;
        }
        shard.before_write();
        shard.dbs[sel].remove(h, key);
        self.expired.push((sel, Sds::from_slice(key)));
        true
    }

    #[inline]
    fn touch(clock: ClockMode, obj: &mut Obj) {
        match clock {
            ClockMode::Lru => obj.set_clock(lru_clock()),
            ClockMode::Lfu { log_factor, decay } => {
                obj.set_clock(crate::db::lfu_touch(obj.clock(), log_factor, decay))
            }
        }
    }

    /// Read lookup (`lookupKeyRead`): honors expiry, updates the access
    /// clock and the keyspace hit/miss counters.
    pub fn get(&mut self, key: &[u8]) -> Option<&Value> {
        let h = hash_key(key);
        if self.expire_if_needed(h, key) {
            self.misses += 1;
            return None;
        }
        let (sel, clock) = (self.sel, self.clock);
        let found = self.shard(h).dbs[sel].main.get_mut_h(h, key).map(|o| {
            Self::touch(clock, o);
            &o.value
        });
        if found.is_some() {
            self.hits += 1;
        } else {
            self.misses += 1;
        }
        // Re-borrow to satisfy the borrow checker without unsafe.
        let sel = self.sel;
        self.shard(h).dbs[sel].main.get_h(h, key).map(|o| &o.value)
    }

    /// Write lookup (`lookupKeyWrite`).
    pub fn get_mut(&mut self, key: &[u8]) -> Option<&mut Value> {
        let h = hash_key(key);
        if self.expire_if_needed(h, key) {
            return None;
        }
        let (sel, clock) = (self.sel, self.clock);
        self.shard(h).dbs[sel].main.get_mut_h(h, key).map(|o| {
            Self::touch(clock, o);
            &mut o.value
        })
    }

    /// Lookup without side effects (expiry still honored logically). Use
    /// after [`DbView::prepare`] when several values must be borrowed at once.
    pub fn peek(&self, key: &[u8]) -> Option<&Value> {
        let h = hash_key(key);
        let shard = self
            .locks
            .get_ref(shard_of(h))
            .unwrap_or_else(|| panic!("BUG: shard accessed without its lock"));
        let db = &shard.dbs[self.sel];
        let o = db.main.get_h(h, key)?;
        if !self.authoritative
            && o.has_expire()
            && db.expires.get_h(h, key).is_some_and(|&t| t <= self.now)
        {
            return None;
        }
        Some(&o.value)
    }

    /// Runs lazy expiry and access-clock updates for `keys` so they can then
    /// be read together with [`DbView::peek`].
    pub fn prepare(&mut self, keys: &[&[u8]]) {
        for k in keys {
            let _ = self.get(k);
        }
    }

    /// True if `key` exists (does not touch the access clock).
    pub fn exists(&mut self, key: &[u8]) -> bool {
        let h = hash_key(key);
        if self.expire_if_needed(h, key) {
            return false;
        }
        let sel = self.sel;
        self.shard(h).dbs[sel].main.contains_key(key)
    }

    /// `setKey`: insert or overwrite; clears the TTL unless `keep_ttl`.
    pub fn set(&mut self, key: &[u8], value: Value, keep_ttl: bool) {
        let h = hash_key(key);
        // Overwriting clears the TTL anyway, so an expired key only needs
        // the (more expensive) expiry check when its TTL would be kept.
        if keep_ttl {
            self.expire_if_needed(h, key);
        }
        let (sel, clock) = (self.sel, self.new_clock());
        self.shard(h).dbs[sel].set(h, key, value, clock, keep_ttl);
    }

    /// Adds a key known to be absent (after a failed lookup).
    pub fn insert_new(&mut self, key: &[u8], value: Value) {
        let h = hash_key(key);
        let (sel, clock) = (self.sel, self.new_clock());
        self.shard(h).dbs[sel].insert_new(h, Sds::from_slice(key), Obj::new(value, clock));
    }

    /// Removes a key (and its TTL).
    pub fn remove(&mut self, key: &[u8]) -> Option<Value> {
        let h = hash_key(key);
        if self.expire_if_needed(h, key) {
            return None;
        }
        let sel = self.sel;
        self.shard(h).dbs[sel].remove(h, key).map(|o| o.value)
    }

    /// Deletes `key` if it holds an empty collection; returns true if so.
    pub fn delete_if_empty(&mut self, key: &[u8]) -> bool {
        let h = hash_key(key);
        let sel = self.sel;
        let db = &mut self.shard(h).dbs[sel];
        if db
            .main
            .get_h(h, key)
            .is_some_and(|o| o.value.is_empty_collection())
        {
            db.remove(h, key);
            true
        } else {
            false
        }
    }

    /// TTL deadline (ms) of an existing key.
    pub fn expire_of(&mut self, key: &[u8]) -> Option<u64> {
        let h = hash_key(key);
        let sel = self.sel;
        self.shard(h).dbs[sel].expire_of_key(h, key)
    }

    /// Sets the deadline of an existing key.
    pub fn set_expire(&mut self, key: &[u8], when: u64) -> bool {
        let h = hash_key(key);
        let sel = self.sel;
        self.shard(h).dbs[sel].set_expire(h, key, when)
    }

    /// Removes the TTL of a key.
    pub fn persist(&mut self, key: &[u8]) -> bool {
        let h = hash_key(key);
        let sel = self.sel;
        self.shard(h).dbs[sel].persist(h, key)
    }

    /// Flags a string as modified in place (`OBJECT ENCODING` → `raw`).
    pub fn mark_raw(&mut self, key: &[u8]) {
        let h = hash_key(key);
        let sel = self.sel;
        if let Some(o) = self.shard(h).dbs[sel].main.get_mut_h(h, key) {
            o.set_raw();
        }
    }

    /// Raw object access (for OBJECT/MEMORY/DEBUG).
    pub fn obj(&mut self, key: &[u8]) -> Option<&mut Obj> {
        let h = hash_key(key);
        if self.expire_if_needed(h, key) {
            return None;
        }
        let sel = self.sel;
        self.shard(h).dbs[sel].main.get_mut_h(h, key)
    }

    /// `signalModifiedKey` + one unit of dirtiness.
    pub fn modified(&mut self, key: &[u8]) {
        self.dirty += 1;
        let h = hash_key(key);
        let sel = self.sel;
        self.shard(h).touch_watched(sel, key);
    }

    /// Records that `key` gained data, waking blocked clients after the
    /// command completes.
    pub fn signal_ready(&mut self, key: &[u8]) {
        let h = hash_key(key);
        let sel = self.sel;
        if self.shard(h).has_blocked(sel, key) {
            self.ready.push((sel, Sds::from_slice(key)));
        }
    }

    /// Shard by index, when held (ALL_SHARDS commands).
    pub fn shard_at(&mut self, i: usize) -> Option<&mut Shard> {
        self.locks.get(i)
    }

    /// Runs `f` on shard `i`: uses the held lock if any, otherwise locks it
    /// just for the call (SELF_LOCK commands such as SCAN).
    pub fn with_shard<R>(&mut self, i: usize, f: impl FnOnce(&mut Shard) -> R) -> R {
        if let Some(s) = self.locks.get(i) {
            return f(s);
        }
        let mut g = self.server.keyspace.lock(i);
        f(&mut g)
    }

    /// Calls `f` on every held shard (ALL_SHARDS commands).
    pub fn for_each_shard(&mut self, f: impl FnMut(&mut Shard)) {
        assert!(
            self.locks.is_all(),
            "BUG: for_each_shard requires ALL_SHARDS"
        );
        self.locks.for_each(f)
    }

    fn register_blocked(&mut self, b: &Arc<BlockedClient>) {
        for key in &b.keys {
            let h = hash_key(key);
            self.shard(h)
                .blocked
                .entry((b.db, key.clone()))
                .or_default()
                .push_back(b.clone());
        }
    }
}

/// A request to block, returned by blocking commands.
pub struct BlockRequest {
    /// Keys to wait on.
    pub keys: Vec<Sds>,
    /// Absolute deadline (ms) or `None` to wait forever.
    pub deadline: Option<u64>,
    /// Action on wake-up.
    pub kind: BlockKind,
}

/// Everything a command handler can touch.
pub struct Ctx<'c, 's> {
    /// The server.
    pub server: &'s Server,
    /// The calling client's private state.
    pub client: &'c mut ClientState,
    /// Reply encoder.
    pub out: Reply<'c>,
    /// Keyspace access.
    pub db: &'c mut DbView<'s>,
    /// Set by blocking commands that found nothing.
    pub block: Option<BlockRequest>,
    /// True while executing inside `EXEC` (blocking commands don't block).
    pub in_exec: bool,
    /// Replacement for the verbatim command in the propagated stream.
    pub prop_override: Option<Vec<Vec<Vec<u8>>>>,
    /// Statistics of the executing thread.
    pub wstats: &'c WorkerStats,
}

impl Ctx<'_, '_> {
    /// Parses an integer argument or replies with the standard error.
    pub fn int(&mut self, a: &[u8]) -> Option<i64> {
        let v = crate::util::parse_i64(a);
        if v.is_none() {
            self.out.error(NOT_INT);
        }
        v
    }

    /// Parses a float argument or replies with the standard error.
    pub fn float(&mut self, a: &[u8]) -> Option<f64> {
        let v = crate::util::parse_f64(a);
        if v.is_none() {
            self.out.error(NOT_FLOAT);
        }
        v
    }

    /// Replaces what gets propagated for this command.
    pub fn propagate_as(&mut self, cmds: Vec<Vec<Vec<u8>>>) {
        self.prop_override = Some(cmds);
    }

    /// Error for a wrong subcommand / arity of a container command.
    pub fn subcommand_error(&mut self, cmd: &str, sub: &[u8]) {
        self.out.error(&format!(
            "ERR unknown subcommand '{}'. Try {} HELP.",
            String::from_utf8_lossy(sub),
            cmd.to_uppercase()
        ));
    }

    /// `ERR wrong number of arguments for '<name>' command`.
    pub fn arity_error(&mut self, name: &str) {
        self.out.error(&format!(
            "ERR wrong number of arguments for '{name}' command"
        ));
    }
}

/// What the connection should do after a command.
pub enum Outcome {
    /// Carry on.
    Normal,
    /// The client is now blocked.
    Blocked(Arc<BlockedClient>, Option<u64>),
    /// Close after flushing the reply.
    Close,
}

fn unknown_command_error(out: &mut Reply<'_>, args: &[&[u8]]) {
    let mut msg = format!(
        "ERR unknown command '{}'",
        String::from_utf8_lossy(&args[0][..args[0].len().min(128)])
    );
    if args.len() > 1 {
        msg.push_str(", with args beginning with: ");
        for a in args.iter().skip(1) {
            let a = &a[..a.len().min(128)];
            msg.push_str(&format!("'{}' ", String::from_utf8_lossy(a)));
            if msg.len() > 512 {
                break;
            }
        }
    }
    out.error(&msg);
}

/// Subcommand tables of container commands (name, arity), mirroring Redis 7,
/// where subcommands are first-class commands validated before execution
/// (and before queueing inside MULTI).
fn subcommands(name: &str) -> Option<&'static [(&'static str, i32)]> {
    Some(match name {
        "object" => &[
            ("encoding", 3),
            ("freq", 3),
            ("idletime", 3),
            ("refcount", 3),
            ("help", 2),
        ],
        "config" => &[
            ("get", -3),
            ("set", -4),
            ("resetstat", 2),
            ("rewrite", 2),
            ("help", 2),
        ],
        "client" => &[
            ("caching", 3),
            ("getname", 2),
            ("getredir", 2),
            ("id", 2),
            ("info", 2),
            ("kill", -3),
            ("list", -2),
            ("no-evict", 3),
            ("no-touch", 3),
            ("pause", -3),
            ("reply", 3),
            ("setinfo", 4),
            ("setname", 3),
            ("tracking", -3),
            ("trackinginfo", 2),
            ("unblock", -3),
            ("unpause", 2),
            ("help", 2),
        ],
        "command" => &[
            ("count", 2),
            ("docs", -2),
            ("getkeys", -3),
            ("getkeysandflags", -3),
            ("info", -2),
            ("list", -2),
            ("help", 2),
        ],
        "pubsub" => &[
            ("channels", -2),
            ("numpat", 2),
            ("numsub", -2),
            ("shardchannels", -2),
            ("shardnumsub", -2),
            ("help", 2),
        ],
        "slowlog" => &[("get", -2), ("len", 2), ("reset", 2), ("help", 2)],
        "memory" => &[
            ("doctor", 2),
            ("malloc-stats", 2),
            ("purge", 2),
            ("stats", 2),
            ("usage", -3),
            ("help", 2),
        ],
        "latency" => &[
            ("doctor", 2),
            ("graph", 3),
            ("histogram", -2),
            ("history", 3),
            ("latest", 2),
            ("reset", -2),
            ("help", 2),
        ],
        _ => return None,
    })
}

/// Validates the subcommand of a container command. Returns the error text.
fn check_subcommand(cmd: &Command, args: &[&[u8]]) -> Option<String> {
    let table = subcommands(cmd.name)?;
    if args.len() < 2 {
        return None;
    }
    let sub = String::from_utf8_lossy(args[1]).to_ascii_lowercase();
    match table.iter().find(|(n, _)| *n == sub) {
        None => Some(format!(
            "ERR unknown subcommand '{}'. Try {} HELP.",
            String::from_utf8_lossy(args[1]),
            cmd.name.to_uppercase()
        )),
        Some(&(n, arity)) => {
            let len = args.len() as i32;
            let ok = if arity >= 0 {
                len == arity
            } else {
                len >= -arity
            };
            (!ok).then(|| {
                format!(
                    "ERR wrong number of arguments for '{}|{}' command",
                    cmd.name, n
                )
            })
        }
    }
}

fn arity_ok(cmd: &Command, n: usize) -> bool {
    let n = n as i32;
    if cmd.arity >= 0 {
        n == cmd.arity
    } else {
        n >= -cmd.arity
    }
}

/// Commands allowed in RESP2 subscriber context.
fn allowed_in_pubsub(cmd: &Command) -> bool {
    cmd.flags & SUB_OK != 0
}

/// Executes one command for a client. `out` receives the reply.
pub fn execute(
    server: &Server,
    client: &mut ClientState,
    wstats: &WorkerStats,
    out: &mut Vec<u8>,
    args: &[&[u8]],
    now: u64,
) -> Outcome {
    if args.is_empty() {
        return Outcome::Normal;
    }
    let resp3 = client.resp3;
    let Some((idx, cmd)) = lookup(args[0]) else {
        if client.multi.is_some() {
            client.multi_error = true;
        }
        unknown_command_error(&mut Reply::new(out, resp3), args);
        wstats.errors.fetch_add(1, Relaxed);
        return Outcome::Normal;
    };
    if !client.authenticated && cmd.flags & NO_AUTH == 0 {
        Reply::new(out, resp3).error("NOAUTH Authentication required.");
        return Outcome::Normal;
    }
    if !arity_ok(cmd, args.len()) {
        if client.multi.is_some() {
            client.multi_error = true;
        }
        Reply::new(out, resp3).error(&format!(
            "ERR wrong number of arguments for '{}' command",
            cmd.name
        ));
        wstats.errors.fetch_add(1, Relaxed);
        return Outcome::Normal;
    }
    if let Some(err) = check_subcommand(cmd, args) {
        if client.multi.is_some() {
            client.multi_error = true;
        }
        Reply::new(out, resp3).error(&err);
        wstats.errors.fetch_add(1, Relaxed);
        return Outcome::Normal;
    }
    if client.in_resp2_pubsub() && !allowed_in_pubsub(cmd) {
        Reply::new(out, resp3).error(&format!(
            "ERR Can't execute '{}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
            cmd.name
        ));
        return Outcome::Normal;
    }
    if client.multi.is_some()
        && !matches!(
            cmd.name,
            "exec" | "discard" | "multi" | "watch" | "quit" | "reset"
        )
    {
        if cmd.flags & NO_MULTI != 0 {
            client.multi_error = true;
            Reply::new(out, resp3).error("ERR Command not allowed inside a transaction");
            return Outcome::Normal;
        }
        if let Some(err) = precheck(server, client, cmd) {
            client.multi_error = true;
            Reply::new(out, resp3).error(err);
            return Outcome::Normal;
        }
        if let Some(q) = client.multi.as_mut() {
            q.push(args.iter().map(|a| a.to_vec()).collect());
            client.handle.multi.store(q.len() as i64, Relaxed);
        }
        Reply::new(out, resp3).simple("QUEUED");
        return Outcome::Normal;
    }
    if let Some(err) = precheck(server, client, cmd) {
        Reply::new(out, resp3).error(err);
        wstats.rejected.fetch_add(1, Relaxed);
        return Outcome::Normal;
    }
    client.handle.last_cmd.store(idx, Relaxed);
    let start = crate::util::ticks();
    let out_start = out.len();
    if cmd.name == "exec" {
        let denyoom = client.multi.as_ref().is_some_and(|q| {
            q.iter()
                .any(|a| lookup(&a[0]).is_some_and(|(_, c)| c.flags & DENYOOM != 0))
        });
        if denyoom && !client.is_master && !crate::server::evict::ensure_memory(server) {
            client.multi = None;
            client.handle.multi.store(-1, Relaxed);
            multi::unwatch_all(server, client);
            Reply::new(out, resp3).error(
                "EXECABORT Transaction discarded because of: OOM command not allowed when used memory > 'maxmemory'.",
            );
            return Outcome::Normal;
        }
        exec_transaction(server, client, wstats, out, now);
        wstats.record(
            idx,
            crate::util::ticks_to_us(crate::util::ticks() - start),
            out.get(out_start) == Some(&b'-'),
        );
        return if client.close_after_reply {
            Outcome::Close
        } else {
            Outcome::Normal
        };
    }
    let outcome = run(server, client, wstats, out, idx, cmd, args, now);
    let us = crate::util::ticks_to_us(crate::util::ticks() - start);
    wstats.record(idx, us, out.get(out_start) == Some(&b'-'));
    let threshold = server.config.slowlog_slower_than.load(Relaxed);
    if threshold >= 0 && us >= threshold as u64 && cmd.name != "exec" {
        server.slowlog_push(args, us, client);
    }
    outcome
}

/// Checks that don't depend on data: read-only replica, OOM, loading.
fn precheck(server: &Server, client: &ClientState, cmd: &Command) -> Option<&'static str> {
    if server.loading.load(Relaxed) && cmd.flags & LOADING == 0 {
        return Some("LOADING Redis is loading the dataset in memory");
    }
    if cmd.flags & WRITE != 0
        && !client.is_master
        && server.is_replica()
        && server.config.replica_read_only.load(Relaxed)
    {
        return Some("READONLY You can't write against a read only replica.");
    }
    if cmd.flags & DENYOOM != 0 && !client.is_master && !crate::server::evict::ensure_memory(server)
    {
        return Some("OOM command not allowed when used memory > 'maxmemory'.");
    }
    None
}

/// Locks the shards `cmd` needs. Single-key commands (the vast majority)
/// take one lock without allocating.
fn lock_for<'s>(server: &'s Server, cmd: &Command, args: &[&[u8]]) -> Locks<'s> {
    let all = cmd.flags & ALL_SHARDS != 0;
    if all || cmd.flags & SELF_LOCK != 0 {
        return Locks::acquire(server, Vec::new(), all);
    }
    if let KeySpec::Range(first, last, _) = cmd.keys {
        let n = args.len() as i32;
        let last = if last < 0 { n + last } else { last };
        if first == last && first > 0 && first < n {
            let i = shard_of(hash_key(args[first as usize]));
            return Locks::One(i, server.keyspace.lock(i));
        }
    }
    let shards = key_positions(cmd, args)
        .into_iter()
        .map(|i| shard_of(hash_key(args[i])))
        .collect();
    Locks::acquire(server, shards, false)
}

#[allow(clippy::too_many_arguments)]
fn run(
    server: &Server,
    client: &mut ClientState,
    wstats: &WorkerStats,
    out: &mut Vec<u8>,
    _idx: usize,
    cmd: &'static Command,
    args: &[&[u8]],
    now: u64,
) -> Outcome {
    let locks = lock_for(server, cmd, args);
    let mut view = DbView::new(server, locks, client.db, now);
    view.authoritative |= client.is_master && view.replica;
    if cmd.flags & WRITE != 0 {
        view.locks.for_each(|s| s.before_write());
    }
    let resp3 = client.resp3;
    let (block, prop_override) = {
        let mut ctx = Ctx {
            server,
            client: &mut *client,
            out: Reply::new(out, resp3),
            db: &mut view,
            block: None,
            in_exec: false,
            prop_override: None,
            wstats,
        };
        (cmd.handler)(&mut ctx, args);
        (ctx.block.take(), ctx.prop_override.take())
    };
    finish_propagation(server, &mut view, cmd, args, prop_override, None);
    let mut outcome = Outcome::Normal;
    if let Some(req) = block {
        let b = Arc::new(BlockedClient::new(
            client.handle.clone(),
            view.sel,
            req.keys,
            req.kind,
            client.resp3,
        ));
        view.register_blocked(&b);
        client.handle.blocked.store(true, Relaxed);
        outcome = Outcome::Blocked(b, req.deadline);
    }
    wstats.hits.fetch_add(view.hits, Relaxed);
    wstats.misses.fetch_add(view.misses, Relaxed);
    wstats.dirty.fetch_add(view.dirty, Relaxed);
    wstats.expired.fetch_add(view.expired.len() as u64, Relaxed);
    let ready = std::mem::take(&mut view.ready);
    drop(view);
    if !ready.is_empty() {
        crate::blocking::serve_ready(server, ready);
    }
    if client.close_after_reply {
        outcome = Outcome::Close;
    }
    outcome
}

/// Encodes and propagates the effects of a command (or a transaction when
/// `exec` holds the collected commands). Must run while locks are held.
fn finish_propagation(
    server: &Server,
    view: &mut DbView<'_>,
    cmd: &Command,
    args: &[&[u8]],
    prop_override: Option<Vec<Vec<Vec<u8>>>>,
    exec: Option<&mut Vec<(usize, Vec<u8>)>>,
) {
    if !server.prop.is_active() {
        return;
    }
    let mut chunks: Vec<(usize, Vec<u8>)> = Vec::new();
    for (db, key) in view.expired.iter() {
        let mut b = Vec::new();
        encode_command(&mut b, &[b"DEL", key]);
        chunks.push((*db, b));
    }
    if view.dirty > 0 && cmd.flags & NO_PROPAGATE == 0 {
        let mut b = Vec::new();
        match prop_override {
            Some(cmds) => {
                for c in cmds {
                    let refs: Vec<&[u8]> = c.iter().map(|a| &a[..]).collect();
                    encode_command(&mut b, &refs);
                }
            }
            None => encode_command(&mut b, args),
        }
        chunks.push((view.sel, b));
    }
    match exec {
        Some(collected) => collected.extend(chunks),
        None => {
            if chunks.is_empty() {
                return;
            }
            let mut g = server.prop.lock();
            for (db, b) in chunks {
                crate::propagate::Propagator::propagate_locked(&mut g, db, &b);
            }
        }
    }
}

/// `EXEC`: runs the queued commands atomically.
pub fn exec_transaction(
    server: &Server,
    client: &mut ClientState,
    wstats: &WorkerStats,
    out: &mut Vec<u8>,
    now: u64,
) {
    let resp3 = client.resp3;
    let Some(queued) = client.multi.take() else {
        Reply::new(out, resp3).error("ERR EXEC without MULTI");
        return;
    };
    client.handle.multi.store(-1, Relaxed);
    if std::mem::take(&mut client.multi_error) {
        multi::unwatch_all(server, client);
        Reply::new(out, resp3).error("EXECABORT Transaction discarded because of previous errors.");
        return;
    }
    let resolved: Vec<(&'static Command, &Vec<Vec<u8>>)> = queued
        .iter()
        .filter_map(|a| lookup(&a[0]).map(|(_, c)| (c, a)))
        .collect();
    // Lock the union of every queued command's shards (watched keys too, so
    // the dirty check below is atomic with execution).
    let mut all = false;
    let mut shards = Vec::new();
    for (cmd, a) in &resolved {
        if cmd.flags & (ALL_SHARDS | SELF_LOCK) != 0 {
            all = true;
        }
        let refs: Vec<&[u8]> = a.iter().map(|x| &x[..]).collect();
        for i in key_positions(cmd, &refs) {
            shards.push(shard_of(hash_key(&a[i])));
        }
    }
    for (_, key, _) in &client.watched {
        shards.push(shard_of(hash_key(key)));
    }
    let locks = Locks::acquire(server, shards, all);
    let mut view = DbView::new(server, locks, client.db, now);
    view.authoritative |= client.is_master && view.replica;
    // WATCH check: modified, or expired since WATCH.
    let mut aborted = client
        .watch_flag
        .as_ref()
        .is_some_and(|f| f.dirty.load(std::sync::atomic::Ordering::Acquire));
    if !aborted {
        let sel = view.sel;
        for (db, key, was_expired) in client.watched.clone() {
            view.sel = db;
            let h = hash_key(&key);
            let expired_now = view
                .locks
                .get_ref(shard_of(h))
                .is_some_and(|s| s.dbs[db].is_expired(h, &key, now));
            if !was_expired && expired_now {
                aborted = true;
            }
        }
        view.sel = sel;
    }
    if aborted {
        drop(view);
        multi::unwatch_all(server, client);
        Reply::new(out, resp3).null_array();
        return;
    }
    if resolved.iter().any(|(c, _)| c.flags & WRITE != 0) {
        view.locks.for_each(|s| s.before_write());
    }
    Reply::new(out, resp3).array(resolved.len());
    let mut collected: Vec<(usize, Vec<u8>)> = Vec::new();
    let mut total_dirty = 0;
    for (cmd, a) in &resolved {
        let refs: Vec<&[u8]> = a.iter().map(|x| &x[..]).collect();
        view.dirty = 0;
        view.expired.clear();
        let start = crate::util::ticks();
        let out_start = out.len();
        let prop_override = {
            let mut ctx = Ctx {
                server,
                client: &mut *client,
                out: Reply::new(out, resp3),
                db: &mut view,
                block: None,
                in_exec: true,
                prop_override: None,
                wstats,
            };
            (cmd.handler)(&mut ctx, &refs);
            ctx.prop_override.take()
        };
        if let Some((idx, _)) = lookup(&a[0]) {
            wstats.record(
                idx,
                crate::util::ticks_to_us(crate::util::ticks() - start),
                out.get(out_start) == Some(&b'-'),
            );
        }
        total_dirty += view.dirty;
        finish_propagation(
            server,
            &mut view,
            cmd,
            &refs,
            prop_override,
            Some(&mut collected),
        );
        // SELECT inside MULTI changes the db for subsequent commands.
        view.sel = client.db;
    }
    if !collected.is_empty() {
        let mut g = server.prop.lock();
        let wrap = collected.len() > 1;
        if wrap {
            let mut b = Vec::new();
            encode_command(&mut b, &[b"MULTI"]);
            crate::propagate::Propagator::propagate_locked(&mut g, collected[0].0, &b);
        }
        let last_db = collected.last().map(|c| c.0).unwrap_or(0);
        for (db, b) in &collected {
            crate::propagate::Propagator::propagate_locked(&mut g, *db, b);
        }
        if wrap {
            let mut b = Vec::new();
            encode_command(&mut b, &[b"EXEC"]);
            crate::propagate::Propagator::propagate_locked(&mut g, last_db, &b);
        }
    }
    wstats.hits.fetch_add(view.hits, Relaxed);
    wstats.misses.fetch_add(view.misses, Relaxed);
    wstats.dirty.fetch_add(total_dirty, Relaxed);
    let ready = std::mem::take(&mut view.ready);
    drop(view);
    multi::unwatch_all(server, client);
    if !ready.is_empty() {
        crate::blocking::serve_ready(server, ready);
    }
}

/// Executes a command coming from the replication stream or the AOF: no
/// reply, no ACL/OOM checks, writes allowed on replicas.
pub fn execute_internal(
    server: &Server,
    client: &mut ClientState,
    wstats: &WorkerStats,
    args: &[&[u8]],
    now: u64,
) -> Vec<u8> {
    let mut out = Vec::new();
    let Some((idx, cmd)) = lookup(args[0]) else {
        return out;
    };
    if !arity_ok(cmd, args.len()) {
        return out;
    }
    match cmd.name {
        "multi" => {
            client.multi = Some(Vec::new());
            return out;
        }
        "exec" => {
            exec_transaction(server, client, wstats, &mut out, now);
            return out;
        }
        "discard" => {
            client.multi = None;
            return out;
        }
        _ => {}
    }
    if let Some(q) = client.multi.as_mut() {
        q.push(args.iter().map(|a| a.to_vec()).collect());
        return out;
    }
    let _ = run(server, client, wstats, &mut out, idx, cmd, args, now);
    out
}

/// Replies `WRONGTYPE` and returns `None` unless `v` matches `pat`.
#[macro_export]
macro_rules! expect_type {
    ($ctx:expr, $v:expr, $pat:path) => {
        match $v {
            Some($pat(x)) => Some(x),
            Some(_) => {
                $ctx.out.error($crate::cmd::WRONGTYPE);
                return;
            }
            None => None,
        }
    };
}

#[cfg(test)]
mod tests {
    //! In-process tests of the dispatcher: no sockets, replies inspected as
    //! raw RESP bytes.
    use super::*;
    use crate::config::Config;
    use crate::server::client::ClientHandle;

    fn setup() -> (Arc<Server>, ClientState) {
        let server = Server::new(Config::default());
        let h = Arc::new(ClientHandle::new(
            server.next_client_id(),
            None,
            "test".into(),
            "test".into(),
        ));
        (server, ClientState::new(h, true))
    }

    fn exec(server: &Server, c: &mut ClientState, args: &[&str]) -> (String, Outcome) {
        let mut out = Vec::new();
        let argv: Vec<&[u8]> = args.iter().map(|a| a.as_bytes()).collect();
        let o = execute(
            server,
            c,
            server.background_stats(),
            &mut out,
            &argv,
            crate::util::now_ms(),
        );
        (String::from_utf8_lossy(&out).into_owned(), o)
    }

    fn reply(server: &Server, c: &mut ClientState, args: &[&str]) -> String {
        exec(server, c, args).0
    }

    #[test]
    fn basic_commands_and_errors() {
        let (s, mut c) = setup();
        assert_eq!(reply(&s, &mut c, &["SET", "k", "v"]), "+OK\r\n");
        assert_eq!(reply(&s, &mut c, &["get", "k"]), "$1\r\nv\r\n");
        assert_eq!(
            reply(&s, &mut c, &["GET"]),
            "-ERR wrong number of arguments for 'get' command\r\n"
        );
        assert_eq!(
            reply(&s, &mut c, &["NOPE"]),
            "-ERR unknown command 'NOPE'\r\n"
        );
        assert_eq!(
            reply(&s, &mut c, &["OBJECT", "x"]),
            "-ERR unknown subcommand 'x'. Try OBJECT HELP.\r\n"
        );
        assert_eq!(
            reply(&s, &mut c, &["LPUSH", "k", "x"]),
            format!("-{WRONGTYPE}\r\n")
        );
        assert_eq!(reply(&s, &mut c, &["INCR", "n"]), ":1\r\n");
    }

    #[test]
    fn transactions_queue_and_watch() {
        let (s, mut c) = setup();
        let (_, mut other) = setup();
        assert_eq!(reply(&s, &mut c, &["MULTI"]), "+OK\r\n");
        assert_eq!(reply(&s, &mut c, &["INCR", "x"]), "+QUEUED\r\n");
        assert_eq!(reply(&s, &mut c, &["INCR", "x"]), "+QUEUED\r\n");
        assert_eq!(reply(&s, &mut c, &["EXEC"]), "*2\r\n:1\r\n:2\r\n");
        reply(&s, &mut c, &["WATCH", "x"]);
        reply(&s, &mut other, &["SET", "x", "9"]);
        reply(&s, &mut c, &["MULTI"]);
        reply(&s, &mut c, &["INCR", "x"]);
        assert_eq!(reply(&s, &mut c, &["EXEC"]), "*-1\r\n");
        assert_eq!(reply(&s, &mut c, &["GET", "x"]), "$1\r\n9\r\n");
    }

    #[test]
    fn resp3_switch_changes_encodings() {
        let (s, mut c) = setup();
        reply(&s, &mut c, &["HSET", "h", "f", "v"]);
        assert!(reply(&s, &mut c, &["HELLO", "3"]).starts_with('%'));
        assert_eq!(
            reply(&s, &mut c, &["HGETALL", "h"]),
            "%1\r\n$1\r\nf\r\n$1\r\nv\r\n"
        );
        assert_eq!(reply(&s, &mut c, &["GET", "none"]), "_\r\n");
    }

    #[test]
    fn blocking_registers_and_is_served_by_push() {
        let (s, mut waiter) = setup();
        let (_, mut pusher) = setup();
        let (_, outcome) = exec(&s, &mut waiter, &["BLPOP", "q", "0"]);
        let Outcome::Blocked(b, deadline) = outcome else {
            panic!("expected to block")
        };
        assert!(deadline.is_none());
        assert!(b.is_waiting());
        assert_eq!(reply(&s, &mut pusher, &["RPUSH", "q", "job"]), ":1\r\n");
        assert_eq!(b.state(), crate::blocking::SERVED);
        let (data, unblocked) = waiter.handle.take_pending();
        assert!(unblocked);
        assert_eq!(
            String::from_utf8_lossy(&data),
            "*2\r\n$1\r\nq\r\n$3\r\njob\r\n"
        );
        // The element went to the waiter, not to the list.
        assert_eq!(reply(&s, &mut pusher, &["LLEN", "q"]), ":0\r\n");
    }

    #[test]
    fn key_positions_follow_specs() {
        let (_, cmd) = lookup(b"mset").unwrap();
        let args: Vec<&[u8]> = vec![b"MSET", b"a", b"1", b"b", b"2"];
        assert_eq!(key_positions(cmd, &args), vec![1, 3]);
        let (_, cmd) = lookup(b"zunionstore").unwrap();
        let args: Vec<&[u8]> = vec![
            b"ZUNIONSTORE",
            b"dst",
            b"2",
            b"x",
            b"y",
            b"WEIGHTS",
            b"1",
            b"2",
        ];
        assert_eq!(key_positions(cmd, &args), vec![1, 3, 4]);
        let (_, cmd) = lookup(b"xread").unwrap();
        let args: Vec<&[u8]> = vec![
            b"XREAD", b"COUNT", b"1", b"STREAMS", b"s1", b"s2", b"0", b"0",
        ];
        assert_eq!(key_positions(cmd, &args), vec![4, 5]);
    }

    #[test]
    fn propagation_rewrites_relative_expiry() {
        let (s, mut c) = setup();
        {
            let mut p = s.prop.lock();
            p.aof = Some(crate::propagate::AofBuf::default());
            s.prop.refresh_active(&p);
        }
        reply(&s, &mut c, &["SET", "k", "v", "EX", "100"]);
        reply(&s, &mut c, &["GET", "k"]);
        reply(&s, &mut c, &["EXPIRE", "k", "50"]);
        let buf = std::mem::take(&mut s.prop.lock().aof.as_mut().unwrap().buf);
        let text = String::from_utf8_lossy(&buf);
        assert!(text.contains("PXAT"), "{text}");
        assert!(text.contains("PEXPIREAT"), "{text}");
        assert!(!text.contains("GET"), "reads are not propagated: {text}");
    }
}
