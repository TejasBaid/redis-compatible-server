//! Server configuration: startup parsing (`redis.conf`-style files and
//! `--name value` flags) and runtime `CONFIG GET`/`CONFIG SET`.
//!
//! Settings read on hot paths are atomics, so commands never take a lock to
//! read configuration.

use crate::types::Limits;
use parking_lot::RwLock;
use std::path::PathBuf;
use std::sync::atomic::{
    AtomicBool, AtomicI64, AtomicU8, AtomicU32, AtomicU64, AtomicUsize, Ordering::Relaxed,
};

/// `maxmemory-policy` values.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum EvictionPolicy {
    /// Reject writes when over the limit.
    NoEviction = 0,
    /// Approximated LRU over all keys.
    AllKeysLru,
    /// Approximated LRU over keys with a TTL.
    VolatileLru,
    /// Approximated LFU over all keys.
    AllKeysLfu,
    /// Approximated LFU over keys with a TTL.
    VolatileLfu,
    /// Random key.
    AllKeysRandom,
    /// Random key with a TTL.
    VolatileRandom,
    /// Key with the nearest expiry.
    VolatileTtl,
}

const POLICIES: [(&str, EvictionPolicy); 8] = [
    ("noeviction", EvictionPolicy::NoEviction),
    ("allkeys-lru", EvictionPolicy::AllKeysLru),
    ("volatile-lru", EvictionPolicy::VolatileLru),
    ("allkeys-lfu", EvictionPolicy::AllKeysLfu),
    ("volatile-lfu", EvictionPolicy::VolatileLfu),
    ("allkeys-random", EvictionPolicy::AllKeysRandom),
    ("volatile-random", EvictionPolicy::VolatileRandom),
    ("volatile-ttl", EvictionPolicy::VolatileTtl),
];

impl EvictionPolicy {
    fn from_u8(v: u8) -> Self {
        POLICIES[v as usize % POLICIES.len()].1
    }
    /// Config name.
    pub fn name(self) -> &'static str {
        POLICIES[self as usize].0
    }
    /// True for LFU policies (which change the meaning of the per-key clock).
    pub fn is_lfu(self) -> bool {
        matches!(
            self,
            EvictionPolicy::AllKeysLfu | EvictionPolicy::VolatileLfu
        )
    }
    /// True for policies that only consider keys with a TTL.
    pub fn is_volatile(self) -> bool {
        matches!(
            self,
            EvictionPolicy::VolatileLru
                | EvictionPolicy::VolatileLfu
                | EvictionPolicy::VolatileRandom
                | EvictionPolicy::VolatileTtl
        )
    }
}

/// `appendfsync` values.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum FsyncPolicy {
    /// fsync before acknowledging each write batch.
    Always = 0,
    /// fsync once per second in the background.
    EverySec = 1,
    /// Leave flushing to the OS.
    No = 2,
}

impl FsyncPolicy {
    /// Config name.
    pub fn name(self) -> &'static str {
        match self {
            FsyncPolicy::Always => "always",
            FsyncPolicy::EverySec => "everysec",
            FsyncPolicy::No => "no",
        }
    }
    fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "always" => Some(FsyncPolicy::Always),
            "everysec" => Some(FsyncPolicy::EverySec),
            "no" => Some(FsyncPolicy::No),
            _ => None,
        }
    }
}

/// All server settings.
pub struct Config {
    /// Bind address (startup only).
    pub bind: RwLock<String>,
    /// TCP port (startup only; `0` picks a free port).
    pub port: AtomicU32,
    /// Number of I/O + execution worker threads (startup only).
    pub threads: AtomicUsize,
    /// Number of logical databases (startup only).
    pub databases: usize,
    /// `maxmemory` in bytes (0 = unlimited).
    pub maxmemory: AtomicU64,
    policy: AtomicU8,
    /// `maxmemory-samples`.
    pub maxmemory_samples: AtomicUsize,
    /// `lfu-log-factor`.
    pub lfu_log_factor: AtomicU32,
    /// `lfu-decay-time` (minutes).
    pub lfu_decay_time: AtomicU32,
    /// Idle client timeout in seconds (0 = never).
    pub timeout: AtomicU64,
    /// `slowlog-log-slower-than` (µs; negative disables).
    pub slowlog_slower_than: AtomicI64,
    /// `slowlog-max-len`.
    pub slowlog_max_len: AtomicUsize,
    /// `hz`: background cron frequency.
    pub hz: AtomicU32,
    list_max_listpack_size: AtomicI64,
    hash_max_listpack_entries: AtomicUsize,
    hash_max_listpack_value: AtomicUsize,
    set_max_intset_entries: AtomicUsize,
    set_max_listpack_entries: AtomicUsize,
    set_max_listpack_value: AtomicUsize,
    zset_max_listpack_entries: AtomicUsize,
    zset_max_listpack_value: AtomicUsize,
    /// `appendonly`.
    pub appendonly: AtomicBool,
    fsync: AtomicU8,
    /// `aof-load-truncated`.
    pub aof_load_truncated: AtomicBool,
    /// `repl-backlog-size`.
    pub repl_backlog_size: AtomicUsize,
    /// `replica-read-only`.
    pub replica_read_only: AtomicBool,
    /// `repl-ping-replica-period` (seconds).
    pub repl_ping_period: AtomicU64,
    /// `repl-timeout` (seconds).
    pub repl_timeout: AtomicU64,
    /// Active expire cycle enabled (`DEBUG SET-ACTIVE-EXPIRE`).
    pub active_expire: AtomicBool,
    /// `activerehashing`.
    pub active_rehashing: AtomicBool,
    /// `requirepass` (empty = no auth).
    pub requirepass: RwLock<String>,
    /// Working directory for persistence files.
    pub dir: RwLock<PathBuf>,
    /// Snapshot file name.
    pub dbfilename: RwLock<String>,
    /// AOF directory name (inside `dir`).
    pub appenddirname: RwLock<String>,
    /// AOF base file name.
    pub appendfilename: RwLock<String>,
    /// `save` points: `(seconds, changes)`.
    pub save_params: RwLock<Vec<(u64, u64)>>,
    /// `loglevel`.
    pub loglevel: RwLock<String>,
    /// `logfile` (empty = stdout).
    pub logfile: RwLock<String>,
    /// `replicaof` given at startup.
    pub replicaof: RwLock<Option<(String, u16)>>,
    /// `daemonize` is accepted for compatibility but ignored.
    pub daemonize: AtomicBool,
}

impl Default for Config {
    fn default() -> Self {
        let lim = Limits::default();
        let threads = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
            .min(8);
        Config {
            bind: RwLock::new("127.0.0.1".into()),
            port: AtomicU32::new(6379),
            threads: AtomicUsize::new(threads),
            databases: 16,
            maxmemory: AtomicU64::new(0),
            policy: AtomicU8::new(EvictionPolicy::NoEviction as u8),
            maxmemory_samples: AtomicUsize::new(5),
            lfu_log_factor: AtomicU32::new(10),
            lfu_decay_time: AtomicU32::new(1),
            timeout: AtomicU64::new(0),
            slowlog_slower_than: AtomicI64::new(10_000),
            slowlog_max_len: AtomicUsize::new(128),
            hz: AtomicU32::new(10),
            list_max_listpack_size: AtomicI64::new(lim.list_max_listpack_size),
            hash_max_listpack_entries: AtomicUsize::new(lim.hash_max_listpack_entries),
            hash_max_listpack_value: AtomicUsize::new(lim.hash_max_listpack_value),
            set_max_intset_entries: AtomicUsize::new(lim.set_max_intset_entries),
            set_max_listpack_entries: AtomicUsize::new(lim.set_max_listpack_entries),
            set_max_listpack_value: AtomicUsize::new(lim.set_max_listpack_value),
            zset_max_listpack_entries: AtomicUsize::new(lim.zset_max_listpack_entries),
            zset_max_listpack_value: AtomicUsize::new(lim.zset_max_listpack_value),
            appendonly: AtomicBool::new(false),
            fsync: AtomicU8::new(FsyncPolicy::EverySec as u8),
            aof_load_truncated: AtomicBool::new(true),
            repl_backlog_size: AtomicUsize::new(1024 * 1024),
            replica_read_only: AtomicBool::new(true),
            repl_ping_period: AtomicU64::new(10),
            repl_timeout: AtomicU64::new(60),
            active_expire: AtomicBool::new(true),
            active_rehashing: AtomicBool::new(true),
            requirepass: RwLock::new(String::new()),
            dir: RwLock::new(PathBuf::from(".")),
            dbfilename: RwLock::new("dump.rcsnap".into()),
            appenddirname: RwLock::new("appendonlydir".into()),
            appendfilename: RwLock::new("appendonly.aof".into()),
            save_params: RwLock::new(vec![(3600, 1), (300, 100), (60, 10000)]),
            loglevel: RwLock::new("notice".into()),
            logfile: RwLock::new(String::new()),
            replicaof: RwLock::new(None),
            daemonize: AtomicBool::new(false),
        }
    }
}

/// Parses Redis memory notation: `100`, `1k` (1000), `1kb` (1024), `5mb`, `2gb`.
pub fn parse_memory(s: &str) -> Option<u64> {
    let s = s.trim().to_ascii_lowercase();
    let units: [(&str, u64); 7] = [
        ("gb", 1 << 30),
        ("mb", 1 << 20),
        ("kb", 1 << 10),
        ("g", 1_000_000_000),
        ("m", 1_000_000),
        ("k", 1000),
        ("b", 1),
    ];
    for (u, mul) in units {
        if let Some(n) = s.strip_suffix(u) {
            return n.parse::<u64>().ok()?.checked_mul(mul);
        }
    }
    s.parse().ok()
}

fn parse_bool(s: &str) -> Option<bool> {
    match s.to_ascii_lowercase().as_str() {
        "yes" => Some(true),
        "no" => Some(false),
        _ => None,
    }
}

fn yes_no(b: bool) -> String {
    if b { "yes".into() } else { "no".into() }
}

/// Names reported by `CONFIG GET *`, in a stable order.
pub const PARAMS: &[&str] = &[
    "bind",
    "port",
    "io-threads",
    "databases",
    "maxmemory",
    "maxmemory-policy",
    "maxmemory-samples",
    "lfu-log-factor",
    "lfu-decay-time",
    "timeout",
    "slowlog-log-slower-than",
    "slowlog-max-len",
    "hz",
    "list-max-listpack-size",
    "list-max-ziplist-size",
    "hash-max-listpack-entries",
    "hash-max-ziplist-entries",
    "hash-max-listpack-value",
    "hash-max-ziplist-value",
    "set-max-intset-entries",
    "set-max-listpack-entries",
    "set-max-listpack-value",
    "zset-max-listpack-entries",
    "zset-max-ziplist-entries",
    "zset-max-listpack-value",
    "zset-max-ziplist-value",
    "appendonly",
    "appendfsync",
    "aof-load-truncated",
    "repl-backlog-size",
    "replica-read-only",
    "slave-read-only",
    "repl-ping-replica-period",
    "repl-timeout",
    "activerehashing",
    "requirepass",
    "dir",
    "dbfilename",
    "appenddirname",
    "appendfilename",
    "save",
    "loglevel",
    "logfile",
    "daemonize",
    "replicaof",
];

impl Config {
    /// Current eviction policy.
    #[inline]
    pub fn policy(&self) -> EvictionPolicy {
        EvictionPolicy::from_u8(self.policy.load(Relaxed))
    }

    /// Current fsync policy.
    #[inline]
    pub fn fsync(&self) -> FsyncPolicy {
        match self.fsync.load(Relaxed) {
            0 => FsyncPolicy::Always,
            1 => FsyncPolicy::EverySec,
            _ => FsyncPolicy::No,
        }
    }

    /// Encoding thresholds, read from atomics (no locking).
    #[inline]
    pub fn limits(&self) -> Limits {
        Limits {
            list_max_listpack_size: self.list_max_listpack_size.load(Relaxed),
            hash_max_listpack_entries: self.hash_max_listpack_entries.load(Relaxed),
            hash_max_listpack_value: self.hash_max_listpack_value.load(Relaxed),
            set_max_intset_entries: self.set_max_intset_entries.load(Relaxed),
            set_max_listpack_entries: self.set_max_listpack_entries.load(Relaxed),
            set_max_listpack_value: self.set_max_listpack_value.load(Relaxed),
            zset_max_listpack_entries: self.zset_max_listpack_entries.load(Relaxed),
            zset_max_listpack_value: self.zset_max_listpack_value.load(Relaxed),
        }
    }

    /// Value of a parameter as `CONFIG GET` reports it.
    pub fn get(&self, name: &str) -> Option<String> {
        Some(match name {
            "bind" => self.bind.read().clone(),
            "port" => self.port.load(Relaxed).to_string(),
            "io-threads" => self.threads.load(Relaxed).to_string(),
            "databases" => self.databases.to_string(),
            "maxmemory" => self.maxmemory.load(Relaxed).to_string(),
            "maxmemory-policy" => self.policy().name().into(),
            "maxmemory-samples" => self.maxmemory_samples.load(Relaxed).to_string(),
            "lfu-log-factor" => self.lfu_log_factor.load(Relaxed).to_string(),
            "lfu-decay-time" => self.lfu_decay_time.load(Relaxed).to_string(),
            "timeout" => self.timeout.load(Relaxed).to_string(),
            "slowlog-log-slower-than" => self.slowlog_slower_than.load(Relaxed).to_string(),
            "slowlog-max-len" => self.slowlog_max_len.load(Relaxed).to_string(),
            "hz" => self.hz.load(Relaxed).to_string(),
            "list-max-listpack-size" | "list-max-ziplist-size" => {
                self.list_max_listpack_size.load(Relaxed).to_string()
            }
            "hash-max-listpack-entries" | "hash-max-ziplist-entries" => {
                self.hash_max_listpack_entries.load(Relaxed).to_string()
            }
            "hash-max-listpack-value" | "hash-max-ziplist-value" => {
                self.hash_max_listpack_value.load(Relaxed).to_string()
            }
            "set-max-intset-entries" => self.set_max_intset_entries.load(Relaxed).to_string(),
            "set-max-listpack-entries" => self.set_max_listpack_entries.load(Relaxed).to_string(),
            "set-max-listpack-value" => self.set_max_listpack_value.load(Relaxed).to_string(),
            "zset-max-listpack-entries" | "zset-max-ziplist-entries" => {
                self.zset_max_listpack_entries.load(Relaxed).to_string()
            }
            "zset-max-listpack-value" | "zset-max-ziplist-value" => {
                self.zset_max_listpack_value.load(Relaxed).to_string()
            }
            "appendonly" => yes_no(self.appendonly.load(Relaxed)),
            "appendfsync" => self.fsync().name().into(),
            "aof-load-truncated" => yes_no(self.aof_load_truncated.load(Relaxed)),
            "repl-backlog-size" => self.repl_backlog_size.load(Relaxed).to_string(),
            "replica-read-only" | "slave-read-only" => yes_no(self.replica_read_only.load(Relaxed)),
            "repl-ping-replica-period" => self.repl_ping_period.load(Relaxed).to_string(),
            "repl-timeout" => self.repl_timeout.load(Relaxed).to_string(),
            "activerehashing" => yes_no(self.active_rehashing.load(Relaxed)),
            "requirepass" => self.requirepass.read().clone(),
            "dir" => {
                let d = self.dir.read().clone();
                std::fs::canonicalize(&d).unwrap_or(d).display().to_string()
            }
            "dbfilename" => self.dbfilename.read().clone(),
            "appenddirname" => self.appenddirname.read().clone(),
            "appendfilename" => self.appendfilename.read().clone(),
            "save" => self
                .save_params
                .read()
                .iter()
                .map(|(s, c)| format!("{s} {c}"))
                .collect::<Vec<_>>()
                .join(" "),
            "loglevel" => self.loglevel.read().clone(),
            "logfile" => self.logfile.read().clone(),
            "daemonize" => yes_no(self.daemonize.load(Relaxed)),
            "replicaof" => match &*self.replicaof.read() {
                Some((h, p)) => format!("{h} {p}"),
                None => String::new(),
            },
            _ => return None,
        })
    }

    /// Applies a parameter. `startup` allows immutable parameters. Returns a
    /// Redis-style reason on failure.
    pub fn set(&self, name: &str, value: &str, startup: bool) -> Result<(), String> {
        let int = |v: &str| {
            v.trim()
                .parse::<i64>()
                .map_err(|_| "argument couldn't be parsed into an integer".to_string())
        };
        let uint = |v: &str| {
            v.trim()
                .parse::<u64>()
                .map_err(|_| "argument couldn't be parsed into an integer".to_string())
        };
        let boolean =
            |v: &str| parse_bool(v).ok_or_else(|| "argument must be 'yes' or 'no'".to_string());
        let immutable = || Err("can't set immutable config".to_string());
        match name {
            "bind" if startup => {
                *self.bind.write() = value
                    .split_whitespace()
                    .next()
                    .unwrap_or("127.0.0.1")
                    .into()
            }
            "port" if startup => self.port.store(uint(value)? as u32, Relaxed),
            "io-threads" | "threads" if startup => {
                self.threads.store(uint(value)?.max(1) as usize, Relaxed)
            }
            "bind" | "port" | "io-threads" | "databases" => return immutable(),
            "maxmemory" => {
                let v = parse_memory(value).ok_or("argument must be a memory value")?;
                self.maxmemory.store(v, Relaxed)
            }
            "maxmemory-policy" => {
                let p = POLICIES
                    .iter()
                    .find(|(n, _)| n.eq_ignore_ascii_case(value))
                    .ok_or("argument(s) must be one of the following: volatile-lru, allkeys-lru, volatile-lfu, allkeys-lfu, volatile-random, allkeys-random, volatile-ttl, noeviction")?;
                self.policy.store(p.1 as u8, Relaxed)
            }
            "maxmemory-samples" => self
                .maxmemory_samples
                .store(uint(value)?.clamp(1, 64) as usize, Relaxed),
            "lfu-log-factor" => self.lfu_log_factor.store(uint(value)? as u32, Relaxed),
            "lfu-decay-time" => self.lfu_decay_time.store(uint(value)? as u32, Relaxed),
            "timeout" => self.timeout.store(uint(value)?, Relaxed),
            "slowlog-log-slower-than" => self.slowlog_slower_than.store(int(value)?, Relaxed),
            "slowlog-max-len" => self.slowlog_max_len.store(uint(value)? as usize, Relaxed),
            "hz" => self.hz.store(uint(value)?.clamp(1, 500) as u32, Relaxed),
            "list-max-listpack-size" | "list-max-ziplist-size" => {
                self.list_max_listpack_size.store(int(value)?, Relaxed)
            }
            "hash-max-listpack-entries" | "hash-max-ziplist-entries" => self
                .hash_max_listpack_entries
                .store(uint(value)? as usize, Relaxed),
            "hash-max-listpack-value" | "hash-max-ziplist-value" => self
                .hash_max_listpack_value
                .store(uint(value)? as usize, Relaxed),
            "set-max-intset-entries" => self
                .set_max_intset_entries
                .store(uint(value)? as usize, Relaxed),
            "set-max-listpack-entries" => self
                .set_max_listpack_entries
                .store(uint(value)? as usize, Relaxed),
            "set-max-listpack-value" => self
                .set_max_listpack_value
                .store(uint(value)? as usize, Relaxed),
            "zset-max-listpack-entries" | "zset-max-ziplist-entries" => self
                .zset_max_listpack_entries
                .store(uint(value)? as usize, Relaxed),
            "zset-max-listpack-value" | "zset-max-ziplist-value" => self
                .zset_max_listpack_value
                .store(uint(value)? as usize, Relaxed),
            "appendonly" => self.appendonly.store(boolean(value)?, Relaxed),
            "appendfsync" => {
                let p = FsyncPolicy::parse(value)
                    .ok_or("argument(s) must be one of the following: always, everysec, no")?;
                self.fsync.store(p as u8, Relaxed)
            }
            "aof-load-truncated" => self.aof_load_truncated.store(boolean(value)?, Relaxed),
            "repl-backlog-size" => {
                let v = parse_memory(value).ok_or("argument must be a memory value")?;
                self.repl_backlog_size
                    .store(v.max(16 * 1024) as usize, Relaxed)
            }
            "replica-read-only" | "slave-read-only" => {
                self.replica_read_only.store(boolean(value)?, Relaxed)
            }
            "repl-ping-replica-period" => self.repl_ping_period.store(uint(value)?.max(1), Relaxed),
            "repl-timeout" => self.repl_timeout.store(uint(value)?.max(1), Relaxed),
            "activerehashing" => self.active_rehashing.store(boolean(value)?, Relaxed),
            "requirepass" => *self.requirepass.write() = value.to_string(),
            "dir" => {
                let p = PathBuf::from(value);
                if !startup && !p.is_dir() {
                    return Err("No such file or directory".into());
                }
                if startup {
                    std::fs::create_dir_all(&p).map_err(|e| e.to_string())?;
                }
                *self.dir.write() = p;
            }
            "dbfilename" => {
                if value.contains('/') {
                    return Err("dbfilename can't be a path, just a filename".into());
                }
                *self.dbfilename.write() = value.into()
            }
            "appenddirname" if startup => *self.appenddirname.write() = value.into(),
            "appendfilename" if startup => *self.appendfilename.write() = value.into(),
            "appenddirname" | "appendfilename" => return immutable(),
            "save" => {
                let parts: Vec<&str> = value.split_whitespace().collect();
                if !parts.len().is_multiple_of(2) {
                    return Err("Invalid save parameters".into());
                }
                let mut v = Vec::new();
                for pair in parts.chunks(2) {
                    let s = pair[0]
                        .parse::<u64>()
                        .map_err(|_| "Invalid save parameters")?;
                    let c = pair[1]
                        .parse::<u64>()
                        .map_err(|_| "Invalid save parameters")?;
                    v.push((s, c));
                }
                *self.save_params.write() = v;
            }
            "loglevel" => {
                let l = value.to_ascii_lowercase();
                if !["debug", "verbose", "notice", "warning", "nothing"].contains(&l.as_str()) {
                    return Err("argument(s) must be one of the following: debug, verbose, notice, warning, nothing".into());
                }
                *self.loglevel.write() = l;
            }
            "logfile" if startup => *self.logfile.write() = value.into(),
            "daemonize" => self.daemonize.store(boolean(value)?, Relaxed),
            "replicaof" | "slaveof" if startup => {
                let parts: Vec<&str> = value.split_whitespace().collect();
                if parts.len() == 2 {
                    let port = parts[1].parse::<u16>().map_err(|_| "Invalid master port")?;
                    *self.replicaof.write() = Some((parts[0].to_string(), port));
                } else {
                    return Err("replicaof requires host and port".into());
                }
            }
            // Accepted for compatibility with stock redis.conf files.
            "protected-mode"
            | "tcp-backlog"
            | "tcp-keepalive"
            | "supervised"
            | "pidfile"
            | "always-show-logo"
            | "stop-writes-on-bgsave-error"
            | "rdbcompression"
            | "rdbchecksum"
            | "lazyfree-lazy-eviction"
            | "appendfsync-on-rewrite"
            | "no-appendfsync-on-rewrite"
            | "auto-aof-rewrite-percentage"
            | "auto-aof-rewrite-min-size"
            | "lua-time-limit"
            | "latency-monitor-threshold"
            | "notify-keyspace-events"
            | "enable-debug-command"
            | "list-compress-depth" => {}
            _ => return Err("Unknown option".into()),
        }
        Ok(())
    }

    /// Applies `name value...` pairs from command-line flags (`--port 7000`)
    /// and config-file lines.
    pub fn apply_args(&self, args: &[String]) -> Result<(), String> {
        let mut i = 0;
        let mut first_save = true;
        while i < args.len() {
            let a = &args[i];
            let name = a
                .strip_prefix("--")
                .ok_or_else(|| format!("unexpected argument '{a}'"))?
                .to_ascii_lowercase();
            let mut vals = Vec::new();
            i += 1;
            while i < args.len() && !args[i].starts_with("--") {
                vals.push(args[i].clone());
                i += 1;
            }
            let value = vals.join(" ");
            if name == "save" {
                if first_save {
                    self.set("save", &value, true)?;
                    first_save = false;
                } else {
                    let cur = self.get("save").unwrap_or_default();
                    self.set("save", &format!("{cur} {value}"), true)?;
                }
                continue;
            }
            self.set(&name, &value, true)
                .map_err(|e| format!("'{name}': {e}"))?;
        }
        Ok(())
    }

    /// Loads a `redis.conf`-style file (one `name value...` per line).
    pub fn load_file(&self, path: &str) -> Result<(), String> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("can't open config file '{path}': {e}"))?;
        let mut args = Vec::new();
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let parts = crate::resp::split_args(line.as_bytes())
                .ok_or_else(|| format!("bad config line: {line}"))?;
            let mut parts = parts
                .into_iter()
                .map(|p| String::from_utf8_lossy(&p).into_owned());
            if let Some(name) = parts.next() {
                args.push(format!("--{name}"));
                let rest: Vec<String> = parts.collect();
                if rest.is_empty() {
                    args.push(String::new());
                } else {
                    args.extend(rest);
                }
            }
        }
        self.apply_args(&args)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_units() {
        assert_eq!(parse_memory("100"), Some(100));
        assert_eq!(parse_memory("1kb"), Some(1024));
        assert_eq!(parse_memory("1k"), Some(1000));
        assert_eq!(parse_memory("2MB"), Some(2 << 20));
        assert_eq!(parse_memory("x"), None);
    }

    #[test]
    fn get_set_roundtrip() {
        let c = Config::default();
        c.set("maxmemory", "10mb", false).unwrap();
        assert_eq!(c.get("maxmemory").unwrap(), (10u64 << 20).to_string());
        c.set("maxmemory-policy", "allkeys-lru", false).unwrap();
        assert_eq!(c.policy(), EvictionPolicy::AllKeysLru);
        assert!(c.set("maxmemory-policy", "bogus", false).is_err());
        assert!(c.set("port", "1", false).is_err());
        c.set("save", "", false).unwrap();
        assert_eq!(c.get("save").unwrap(), "");
        c.apply_args(&[
            "--save".into(),
            "900".into(),
            "1".into(),
            "--save".into(),
            "300".into(),
            "10".into(),
        ])
        .unwrap();
        assert_eq!(c.get("save").unwrap(), "900 1 300 10");
        c.set("hash-max-ziplist-entries", "7", false).unwrap();
        assert_eq!(c.limits().hash_max_listpack_entries, 7);
    }
}
