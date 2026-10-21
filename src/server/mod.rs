//! Server runtime: shared state, startup, the acceptor, the cron thread and
//! graceful shutdown.
//!
//! ```text
//!              acceptor ──round robin──▶ worker 0..N (mio event loop each)
//!                                          │ parse → lock shards → execute
//!                                          ▼
//!   cron (hz) ── active expiry, rehash,  Keyspace: 256 lock-striped shards
//!               save points, fsync       │
//!                                        ▼ (under shard locks)
//!                                     Propagator ──▶ AOF buffer / repl backlog
//! ```

pub mod client;
pub mod evict;
pub mod expire;
pub mod worker;

use crate::config::Config;
use crate::db::Keyspace;
use crate::propagate::Propagator;
use crate::pubsub::PubSub;
use client::{ClientHandle, ClientState, Mailbox, Msg};
use parking_lot::{Mutex, RwLock};
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

/// Version string reported in INFO and HELLO.
pub const VERSION: &str = "7.4.0";

/// Counters owned by one worker thread (no cross-thread contention on the
/// hot path; INFO sums them).
pub struct WorkerStats {
    /// Commands processed.
    pub commands: AtomicU64,
    /// Per-command calls (indexed like the command table).
    pub calls: Vec<AtomicU64>,
    /// Per-command total microseconds.
    pub usec: Vec<AtomicU64>,
    /// Per-command calls that replied with an error.
    pub failed: Vec<AtomicU64>,
    /// Keyspace hits.
    pub hits: AtomicU64,
    /// Keyspace misses.
    pub misses: AtomicU64,
    /// Keys expired lazily.
    pub expired: AtomicU64,
    /// Changes (dirty) made.
    pub dirty: AtomicU64,
    /// Bytes read from clients.
    pub net_in: AtomicU64,
    /// Bytes written to clients.
    pub net_out: AtomicU64,
    /// Error replies (unknown command, arity...).
    pub errors: AtomicU64,
    /// Commands rejected before execution (OOM, READONLY...).
    pub rejected: AtomicU64,
}

impl WorkerStats {
    /// Zeroed counters for `ncmds` commands.
    pub fn new(ncmds: usize) -> Self {
        let v = || (0..ncmds).map(|_| AtomicU64::new(0)).collect();
        WorkerStats {
            commands: AtomicU64::new(0),
            calls: v(),
            usec: v(),
            failed: v(),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            expired: AtomicU64::new(0),
            dirty: AtomicU64::new(0),
            net_in: AtomicU64::new(0),
            net_out: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
        }
    }

    /// Records one execution of command `idx`.
    #[inline]
    pub fn record(&self, idx: usize, us: u64, failed: bool) {
        self.commands.fetch_add(1, Relaxed);
        self.calls[idx].fetch_add(1, Relaxed);
        self.usec[idx].fetch_add(us, Relaxed);
        if failed {
            self.failed[idx].fetch_add(1, Relaxed);
        }
    }
}

/// Server-wide counters for rare events.
#[derive(Default)]
pub struct Stats {
    /// Connections accepted.
    pub connections: AtomicU64,
    /// Keys removed by the active expire cycle.
    pub expired_active: AtomicU64,
    /// Keys evicted by maxmemory.
    pub evicted: AtomicU64,
    /// Changes made outside workers (blocking serves, expiry, eviction).
    pub dirty_background: AtomicU64,
    /// Full resyncs served.
    pub sync_full: AtomicU64,
    /// Partial resyncs accepted.
    pub sync_partial_ok: AtomicU64,
    /// Partial resyncs refused.
    pub sync_partial_err: AtomicU64,
    /// Ops/sec samples (instantaneous_ops_per_sec).
    pub ops_samples: Mutex<VecDeque<(Instant, u64)>>,
}

/// One SLOWLOG entry.
pub struct SlowLogEntry {
    /// Unique id.
    pub id: u64,
    /// Unix time (s).
    pub time: u64,
    /// Duration (µs).
    pub micros: u64,
    /// Truncated arguments.
    pub args: Vec<Vec<u8>>,
    /// Client address.
    pub addr: String,
    /// Client name.
    pub name: String,
}

/// The slow log ring.
#[derive(Default)]
pub struct SlowLog {
    /// Newest first.
    pub entries: VecDeque<SlowLogEntry>,
    /// Next id.
    pub next_id: u64,
}

/// Shared state of a running server.
pub struct Server {
    self_ref: std::sync::Weak<Server>,
    /// Configuration.
    pub config: Config,
    /// The data.
    pub keyspace: Keyspace,
    /// Pub/sub registry.
    pub pubsub: RwLock<PubSub>,
    /// AOF + replication stream.
    pub prop: Propagator,
    /// Connected clients by id.
    pub clients: Mutex<HashMap<u64, Arc<ClientHandle>>>,
    next_client_id: AtomicU64,
    /// Worker mailboxes.
    pub mailboxes: OnceLock<Vec<Arc<Mailbox>>>,
    /// Global counters.
    pub stats: Stats,
    /// Per-worker counters; the last entry belongs to background threads.
    pub worker_stats: Vec<Arc<WorkerStats>>,
    /// SLOWLOG.
    pub slowlog: Mutex<SlowLog>,
    /// Persistence state.
    pub persist: crate::persistence::PersistState,
    /// Replication state.
    pub repl: crate::replication::ReplState,
    /// Eviction candidate pool.
    pub evict_pool: Mutex<evict::EvictionPool>,
    /// Set when shutting down.
    pub shutdown: AtomicBool,
    /// Set while loading data at startup.
    pub loading: AtomicBool,
    /// Process start.
    pub started: Instant,
    /// Unique run id.
    pub run_id: String,
    /// Bound TCP port.
    pub port: AtomicU16,
    /// Connected clients count.
    pub connected: AtomicUsize,
}

impl Server {
    /// Creates server state from a configuration.
    pub fn new(config: Config) -> Arc<Server> {
        let ncmds = crate::cmd::all().len();
        let threads = config.threads.load(Relaxed);
        let databases = config.databases;
        Arc::new_cyclic(|w| Server {
            self_ref: w.clone(),
            config,
            keyspace: Keyspace::new(databases),
            pubsub: RwLock::new(PubSub::default()),
            prop: Propagator::new(),
            clients: Mutex::new(HashMap::new()),
            next_client_id: AtomicU64::new(1),
            mailboxes: OnceLock::new(),
            stats: Stats::default(),
            worker_stats: (0..=threads)
                .map(|_| Arc::new(WorkerStats::new(ncmds)))
                .collect(),
            slowlog: Mutex::new(SlowLog::default()),
            persist: crate::persistence::PersistState::default(),
            repl: crate::replication::ReplState::default(),
            evict_pool: Mutex::new(evict::EvictionPool::default()),
            shutdown: AtomicBool::new(false),
            loading: AtomicBool::new(false),
            started: Instant::now(),
            run_id: crate::propagate::new_replid(),
            port: AtomicU16::new(0),
            connected: AtomicUsize::new(0),
        })
    }

    /// A strong reference to this server (for spawning background threads).
    pub fn arc(&self) -> Arc<Server> {
        self.self_ref
            .upgrade()
            .expect("server is alive while in use")
    }

    /// Allocates a client id.
    pub fn next_client_id(&self) -> u64 {
        self.next_client_id.fetch_add(1, Relaxed)
    }

    /// True when this server replicates from a master.
    #[inline]
    pub fn is_replica(&self) -> bool {
        self.repl.is_replica.load(Relaxed)
    }

    /// Stats for threads that are not workers (cron, replication, loading).
    pub fn background_stats(&self) -> &WorkerStats {
        self.worker_stats.last().expect("at least one stats slot")
    }

    /// A detached client state for internal execution (AOF loading, the
    /// replication link).
    pub fn internal_client(&self) -> ClientState {
        let h = Arc::new(ClientHandle::new(
            self.next_client_id(),
            None,
            String::new(),
            String::new(),
        ));
        let mut c = ClientState::new(h, true);
        c.is_master = true;
        c
    }

    /// Total dirty counter (changes since startup).
    pub fn dirty_total(&self) -> u64 {
        self.worker_stats
            .iter()
            .map(|w| w.dirty.load(Relaxed))
            .sum::<u64>()
            + self.stats.dirty_background.load(Relaxed)
            + self.stats.expired_active.load(Relaxed)
            + self.stats.evicted.load(Relaxed)
    }

    /// Records a SLOWLOG entry.
    pub fn slowlog_push(&self, args: &[&[u8]], micros: u64, client: &ClientState) {
        let max = self.config.slowlog_max_len.load(Relaxed);
        if max == 0 {
            return;
        }
        let mut stored: Vec<Vec<u8>> = Vec::new();
        let argc = args.len().min(32);
        for (i, a) in args.iter().take(argc).enumerate() {
            if i == 31 && args.len() > 32 {
                stored.push(format!("... ({} more arguments)", args.len() - 31).into_bytes());
                break;
            }
            if a.len() > 128 {
                let mut v = a[..128].to_vec();
                v.extend_from_slice(format!("... ({} more bytes)", a.len() - 128).as_bytes());
                stored.push(v);
            } else {
                stored.push(a.to_vec());
            }
        }
        let mut sl = self.slowlog.lock();
        let id = sl.next_id;
        sl.next_id += 1;
        sl.entries.push_front(SlowLogEntry {
            id,
            time: crate::util::now_ms() / 1000,
            micros,
            args: stored,
            addr: client.handle.addr.clone(),
            name: client.handle.name.lock().clone(),
        });
        sl.entries.truncate(max);
    }

    /// Logs a line in Redis' format.
    pub fn log(&self, level: char, msg: &str) {
        log(level, msg);
    }

    /// Initiates shutdown: optional final save, AOF fsync, stop threads.
    /// Returns an error message if the final save failed (shutdown aborted).
    pub fn shutdown(&self, save: Option<bool>) -> Result<(), String> {
        let should_save = save.unwrap_or_else(|| !self.config.save_params.read().is_empty());
        if should_save && !self.is_replica() {
            self.log('#', "User requested shutdown...");
            if let Err(e) = crate::persistence::save_sync(self) {
                self.log(
                    '#',
                    &format!("Error trying to save the DB, can't exit: {e}"),
                );
                return Err(format!("Errors trying to SHUTDOWN. Check logs. ({e})"));
            }
        }
        if self.config.appendonly.load(Relaxed) {
            crate::persistence::aof::flush(self, true);
        }
        self.shutdown.store(true, Relaxed);
        if let Some(mbs) = self.mailboxes.get() {
            for m in mbs {
                m.send(Msg::Shutdown);
            }
        }
        self.log('#', "Redis is now ready to exit, bye bye...");
        Ok(())
    }
}

/// Prints a log line: `pid:M dd Mon yyyy hh:mm:ss.mmm <level> msg`.
pub fn log(level: char, msg: &str) {
    let ms = crate::util::now_ms();
    let secs = ms / 1000;
    let (h, m, s) = ((secs / 3600) % 24, (secs / 60) % 60, secs % 60);
    println!(
        "{}:M {:02}:{:02}:{:02}.{:03} {} {}",
        std::process::id(),
        h,
        m,
        s,
        ms % 1000,
        level,
        msg
    );
}

/// A running server.
pub struct ServerHandle {
    /// Shared state.
    pub server: Arc<Server>,
    /// Bound address.
    pub addr: SocketAddr,
    threads: Vec<std::thread::JoinHandle<()>>,
}

impl ServerHandle {
    /// Blocks until all server threads exit.
    pub fn join(self) {
        for t in self.threads {
            let _ = t.join();
        }
    }
}

/// Loads persisted data, binds the listener and spawns all threads.
pub fn start(config: Config) -> std::io::Result<ServerHandle> {
    let server = Server::new(config);
    crate::db::update_lru_clock();
    server.loading.store(true, Relaxed);
    if let Err(e) = crate::persistence::load_on_startup(&server) {
        log('#', &format!("Fatal error loading the DB: {e}. Exiting."));
        return Err(std::io::Error::other(e));
    }
    server.loading.store(false, Relaxed);

    let bind = server.config.bind.read().clone();
    let port = server.config.port.load(Relaxed) as u16;
    let addr: SocketAddr = format!("{bind}:{port}").parse().map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("bad bind address: {e}"),
        )
    })?;
    let std_listener = std::net::TcpListener::bind(addr)?;
    std_listener.set_nonblocking(true)?;
    let local = std_listener.local_addr()?;
    server.port.store(local.port(), Relaxed);
    server.config.port.store(local.port() as u32, Relaxed);

    let nworkers = server.config.threads.load(Relaxed).max(1);
    let mut threads = Vec::new();
    let mut mailboxes = Vec::new();
    let mut polls = Vec::new();
    for i in 0..nworkers {
        let poll = mio::Poll::new()?;
        let waker = mio::Waker::new(poll.registry(), worker::WAKER)?;
        mailboxes.push(Arc::new(Mailbox::new(waker, i)));
        polls.push(poll);
    }
    let _ = server.mailboxes.set(mailboxes.clone());
    for (i, poll) in polls.into_iter().enumerate() {
        let s = server.clone();
        let mb = mailboxes[i].clone();
        threads.push(
            std::thread::Builder::new()
                .name(format!("worker-{i}"))
                .spawn(move || worker::Worker::new(i, s, poll, mb).run())?,
        );
    }
    {
        let s = server.clone();
        let listener = mio::net::TcpListener::from_std(std_listener);
        threads.push(
            std::thread::Builder::new()
                .name("acceptor".into())
                .spawn(move || accept_loop(s, listener))?,
        );
    }
    {
        let s = server.clone();
        threads.push(
            std::thread::Builder::new()
                .name("cron".into())
                .spawn(move || cron_loop(s))?,
        );
    }
    if let Some((h, p)) = server.config.replicaof.read().clone() {
        crate::replication::replica::start(&server, h, p);
    }
    log(
        '*',
        &format!("Ready to accept connections tcp port={}", local.port()),
    );
    Ok(ServerHandle {
        server,
        addr: local,
        threads,
    })
}

fn accept_loop(server: Arc<Server>, mut listener: mio::net::TcpListener) {
    const LISTENER: mio::Token = mio::Token(0);
    let mut poll = match mio::Poll::new() {
        Ok(p) => p,
        Err(e) => {
            log('#', &format!("acceptor: {e}"));
            return;
        }
    };
    if poll
        .registry()
        .register(&mut listener, LISTENER, mio::Interest::READABLE)
        .is_err()
    {
        return;
    }
    let mut events = mio::Events::with_capacity(64);
    let mut next = 0usize;
    let mailboxes = server.mailboxes.get().expect("mailboxes set").clone();
    while !server.shutdown.load(Relaxed) {
        if poll
            .poll(&mut events, Some(Duration::from_millis(100)))
            .is_err()
        {
            continue;
        }
        loop {
            match listener.accept() {
                Ok((stream, peer)) => {
                    let _ = stream.set_nodelay(true);
                    server.stats.connections.fetch_add(1, Relaxed);
                    mailboxes[next % mailboxes.len()].send(Msg::NewConn(stream, peer));
                    next = next.wrapping_add(1);
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => {
                    log('#', &format!("accept: {e}"));
                    std::thread::sleep(Duration::from_millis(10));
                    break;
                }
            }
        }
    }
}

fn cron_loop(server: Arc<Server>) {
    let mut last_fsync = Instant::now();
    let mut last_ping = Instant::now();
    let mut last_sample = Instant::now();
    let mut expire_state = expire::ExpireState::default();
    while !server.shutdown.load(Relaxed) {
        let hz = server.config.hz.load(Relaxed).max(1);
        let period = Duration::from_millis(1000 / hz as u64);
        std::thread::sleep(period);
        crate::db::update_lru_clock();
        if !server.is_replica() && server.config.active_expire.load(Relaxed) {
            // ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC = 25% of each cron period.
            expire::active_expire_cycle(&server, &mut expire_state, period / 4);
        }
        if server.config.active_rehashing.load(Relaxed) {
            expire::incremental_rehash(&server, Duration::from_millis(1));
        }
        if last_sample.elapsed() >= Duration::from_millis(100) {
            last_sample = Instant::now();
            let total: u64 = server
                .worker_stats
                .iter()
                .map(|w| w.commands.load(Relaxed))
                .sum();
            let mut s = server.stats.ops_samples.lock();
            s.push_back((Instant::now(), total));
            while s.len() > 16 {
                s.pop_front();
            }
        }
        if server.config.appendonly.load(Relaxed)
            && server.config.fsync() == crate::config::FsyncPolicy::EverySec
            && last_fsync.elapsed() >= Duration::from_secs(1)
        {
            last_fsync = Instant::now();
            crate::persistence::aof::flush(&server, true);
        }
        crate::persistence::check_save_points(&server);
        let ping_period = Duration::from_secs(server.config.repl_ping_period.load(Relaxed));
        if last_ping.elapsed() >= ping_period {
            last_ping = Instant::now();
            crate::replication::master::ping_replicas(&server);
        }
    }
}
