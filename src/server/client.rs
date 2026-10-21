//! Per-client state.
//!
//! [`ClientState`] is owned by the worker thread serving the connection and is
//! never shared. [`ClientHandle`] is the small shared part other threads may
//! touch: a cross-thread output queue (pub/sub messages, replies to unblocked
//! `BLPOP`s, the replication stream), plus atomics describing the client for
//! `CLIENT LIST`.

use crate::db::WatchFlag;
use crate::sds::Sds;
use parking_lot::Mutex;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering::Relaxed};

/// Messages delivered to a worker's mailbox.
pub enum Msg {
    /// A freshly accepted connection to adopt.
    NewConn(mio::net::TcpStream, SocketAddr),
    /// Cross-thread output is waiting for client `id`.
    Pending(u64),
    /// Close client `id` (`CLIENT KILL`, shutdown).
    Kill(u64),
    /// Stop the worker.
    Shutdown,
}

/// A worker's inbox plus the mio waker that interrupts its poll.
pub struct Mailbox {
    queue: Mutex<Vec<Msg>>,
    waker: mio::Waker,
    /// Worker index.
    pub worker: usize,
}

impl Mailbox {
    /// Creates a mailbox bound to a worker's waker.
    pub fn new(waker: mio::Waker, worker: usize) -> Self {
        Mailbox {
            queue: Mutex::new(Vec::new()),
            waker,
            worker,
        }
    }

    /// Enqueues a message and wakes the worker.
    pub fn send(&self, m: Msg) {
        let was_empty = {
            let mut q = self.queue.lock();
            let e = q.is_empty();
            q.push(m);
            e
        };
        if was_empty {
            let _ = self.waker.wake();
        }
    }

    /// Takes all queued messages.
    pub fn drain(&self, into: &mut Vec<Msg>) {
        std::mem::swap(&mut *self.queue.lock(), into);
    }
}

#[derive(Default)]
struct Pending {
    data: Vec<u8>,
    notified: bool,
    unblock: bool,
}

/// Maximum cross-thread output buffered for a non-replica client before it is
/// disconnected.
pub const PENDING_LIMIT: usize = 32 * 1024 * 1024;

/// Client kinds shown in `CLIENT LIST` flags.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClientKind {
    /// A normal client.
    Normal,
    /// A replica connected to us.
    Replica,
    /// Our master (on a replica).
    Master,
}

/// Shared, thread-safe view of a client.
pub struct ClientHandle {
    /// Unique client id.
    pub id: u64,
    mailbox: Option<Arc<Mailbox>>,
    pending: Mutex<Pending>,
    /// Client speaks RESP3.
    pub resp3: AtomicBool,
    /// Peer address.
    pub addr: String,
    /// Local address.
    pub laddr: String,
    /// Creation time (ms).
    pub created_ms: u64,
    /// Last command time (ms).
    pub last_active_ms: AtomicU64,
    /// Selected database.
    pub db: AtomicUsize,
    /// Index of the last command in the command table.
    pub last_cmd: AtomicUsize,
    /// `CLIENT SETNAME`.
    pub name: Mutex<String>,
    /// `CLIENT SETINFO LIB-NAME`.
    pub lib_name: Mutex<String>,
    /// `CLIENT SETINFO LIB-VER`.
    pub lib_ver: Mutex<String>,
    /// Channel subscriptions.
    pub sub: AtomicUsize,
    /// Pattern subscriptions.
    pub psub: AtomicUsize,
    /// Queued MULTI commands (-1 when not in MULTI).
    pub multi: AtomicI64,
    /// Number of WATCHed keys.
    pub watch: AtomicUsize,
    /// Bytes of pending input.
    pub qbuf: AtomicUsize,
    /// Bytes of pending output.
    pub omem: AtomicUsize,
    /// Blocked in a blocking command.
    pub blocked: AtomicBool,
    /// Kind, as a `ClientKind` discriminant.
    kind: AtomicUsize,
    /// Set once the client is closing.
    pub closing: AtomicBool,
}

impl ClientHandle {
    /// Creates a handle. `mailbox` is `None` for internal pseudo-clients.
    pub fn new(id: u64, mailbox: Option<Arc<Mailbox>>, addr: String, laddr: String) -> Self {
        let now = crate::util::now_ms();
        ClientHandle {
            id,
            mailbox,
            pending: Mutex::new(Pending::default()),
            resp3: AtomicBool::new(false),
            addr,
            laddr,
            created_ms: now,
            last_active_ms: AtomicU64::new(now),
            db: AtomicUsize::new(0),
            last_cmd: AtomicUsize::new(usize::MAX),
            name: Mutex::new(String::new()),
            lib_name: Mutex::new(String::new()),
            lib_ver: Mutex::new(String::new()),
            sub: AtomicUsize::new(0),
            psub: AtomicUsize::new(0),
            multi: AtomicI64::new(-1),
            watch: AtomicUsize::new(0),
            qbuf: AtomicUsize::new(0),
            omem: AtomicUsize::new(0),
            blocked: AtomicBool::new(false),
            kind: AtomicUsize::new(0),
            closing: AtomicBool::new(false),
        }
    }

    /// Worker index serving this client.
    pub fn worker(&self) -> Option<usize> {
        self.mailbox.as_ref().map(|m| m.worker)
    }

    /// Kind of client.
    pub fn kind(&self) -> ClientKind {
        match self.kind.load(Relaxed) {
            1 => ClientKind::Replica,
            2 => ClientKind::Master,
            _ => ClientKind::Normal,
        }
    }

    /// Sets the kind.
    pub fn set_kind(&self, k: ClientKind) {
        self.kind.store(
            match k {
                ClientKind::Normal => 0,
                ClientKind::Replica => 1,
                ClientKind::Master => 2,
            },
            Relaxed,
        );
    }

    fn push_inner(&self, data: &[u8], unblock: bool) {
        let notify = {
            let mut p = self.pending.lock();
            // Hard output-buffer limit for slow consumers (Redis' default
            // `client-output-buffer-limit pubsub` hard limit is 32mb). The
            // replication stream is exempt: a snapshot payload may be large.
            if self.kind() != ClientKind::Replica && p.data.len() + data.len() > PENDING_LIMIT {
                drop(p);
                self.kill();
                return;
            }
            p.data.extend_from_slice(data);
            p.unblock |= unblock;
            !std::mem::replace(&mut p.notified, true)
        };
        if notify && let Some(m) = &self.mailbox {
            m.send(Msg::Pending(self.id));
        }
    }

    /// Queues output from another thread (pub/sub, replication stream).
    pub fn push(&self, data: &[u8]) {
        self.push_inner(data, false);
    }

    /// Queues the reply that unblocks a blocked client.
    pub fn push_unblock(&self, data: &[u8]) {
        self.push_inner(data, true);
    }

    /// Takes queued output: `(bytes, unblocked)`.
    pub fn take_pending(&self) -> (Vec<u8>, bool) {
        let mut p = self.pending.lock();
        p.notified = false;
        (std::mem::take(&mut p.data), std::mem::take(&mut p.unblock))
    }

    /// Bytes queued but not yet taken by the owning worker.
    pub fn pending_len(&self) -> usize {
        self.pending.lock().data.len()
    }

    /// Asks the owning worker to close this client.
    pub fn kill(&self) {
        self.closing.store(true, Relaxed);
        if let Some(m) = &self.mailbox {
            m.send(Msg::Kill(self.id));
        }
    }
}

/// State owned by the connection's worker thread.
pub struct ClientState {
    /// Client id.
    pub id: u64,
    /// Selected database.
    pub db: usize,
    /// RESP3 negotiated with `HELLO 3`.
    pub resp3: bool,
    /// Passed `AUTH` (or no password configured).
    pub authenticated: bool,
    /// Queued commands while in `MULTI`.
    pub multi: Option<Vec<Vec<Vec<u8>>>>,
    /// A command failed to queue: `EXEC` must abort.
    pub multi_error: bool,
    /// Shared dirty flag for `WATCH`.
    pub watch_flag: Option<Arc<WatchFlag>>,
    /// Watched `(db, key, was_expired_at_watch_time)`.
    pub watched: Vec<(usize, Sds, bool)>,
    /// Subscribed channels.
    pub channels: HashSet<Sds>,
    /// Subscribed patterns.
    pub patterns: HashSet<Sds>,
    /// Shared handle.
    pub handle: Arc<ClientHandle>,
    /// This connection is our replication master (replies suppressed).
    pub is_master: bool,
    /// This connection is a replica fed by us.
    pub is_replica: bool,
    /// Close the connection once pending output is flushed.
    pub close_after_reply: bool,
    /// `CLIENT REPLY OFF/SKIP` state: 0 = on, 1 = off, 2 = skip next.
    pub reply_mode: u8,
    /// `REPLCONF listening-port` announced by a replica.
    pub repl_port: u16,
}

impl ClientState {
    /// Fresh state for a new connection.
    pub fn new(handle: Arc<ClientHandle>, authenticated: bool) -> Self {
        ClientState {
            id: handle.id,
            db: 0,
            resp3: false,
            authenticated,
            multi: None,
            multi_error: false,
            watch_flag: None,
            watched: Vec::new(),
            channels: HashSet::new(),
            patterns: HashSet::new(),
            handle,
            is_master: false,
            is_replica: false,
            close_after_reply: false,
            reply_mode: 0,
            repl_port: 0,
        }
    }

    /// Number of active subscriptions.
    pub fn sub_count(&self) -> usize {
        self.channels.len() + self.patterns.len()
    }

    /// True in RESP2 subscriber mode, where only pub/sub commands are allowed.
    pub fn in_resp2_pubsub(&self) -> bool {
        !self.resp3 && self.sub_count() > 0
    }
}
