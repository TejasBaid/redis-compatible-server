//! Master side of replication: PSYNC handling, the per-replica feed, ACKs,
//! keepalive PINGs and WAIT.

use crate::persistence::snapshot;
use crate::propagate::{Backlog, PropInner, Propagator};
use crate::resp::encode_command;
use crate::server::client::{ClientHandle, ClientKind};
use crate::server::{Server, log};
use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, AtomicU64, Ordering::Relaxed};

/// Output buffered for a replica waiting for its snapshot is capped (like
/// `client-output-buffer-limit replica 256mb`); beyond it the replica is
/// dropped and will resync.
const MAX_PENDING: usize = 256 * 1024 * 1024;

enum LinkState {
    /// Snapshot in progress; stream bytes are buffered.
    WaitSnapshot(Vec<u8>),
    /// Streaming.
    Online,
}

/// An attached replica.
pub struct ReplicaLink {
    /// The replica connection's handle (its worker writes our stream).
    pub client: Arc<ClientHandle>,
    state: Mutex<LinkState>,
    /// Last offset the replica acknowledged.
    pub ack_offset: AtomicU64,
    /// When the last ACK arrived (ms).
    pub last_ack_ms: AtomicU64,
    port: AtomicU16,
}

impl ReplicaLink {
    fn new(client: Arc<ClientHandle>, port: u16, online: bool) -> Self {
        ReplicaLink {
            client,
            state: Mutex::new(if online {
                LinkState::Online
            } else {
                LinkState::WaitSnapshot(Vec::new())
            }),
            ack_offset: AtomicU64::new(0),
            last_ack_ms: AtomicU64::new(crate::util::now_ms()),
            port: AtomicU16::new(port),
        }
    }

    /// Announced listening port.
    pub fn port(&self) -> u16 {
        self.port.load(Relaxed)
    }

    /// True once streaming.
    pub fn is_online(&self) -> bool {
        matches!(*self.state.lock(), LinkState::Online)
    }

    /// Appends stream bytes (called under the propagation lock).
    pub fn feed(&self, data: &[u8]) {
        let mut st = self.state.lock();
        match &mut *st {
            LinkState::WaitSnapshot(buf) => {
                if buf.len() + data.len() > MAX_PENDING {
                    log(
                        '#',
                        "Replica output buffer limit reached while waiting for snapshot; dropping replica",
                    );
                    self.client.kill();
                    return;
                }
                buf.extend_from_slice(data);
            }
            LinkState::Online => self.client.push(data),
        }
    }
}

fn ensure_backlog(server: &Server, p: &mut PropInner) {
    if p.repl.backlog.is_none() {
        p.repl.backlog = Some(Backlog::new(server.config.repl_backlog_size.load(Relaxed)));
        // The next command must re-announce its database.
        p.repl.selected_db = None;
    }
}

/// Handles `PSYNC <replid> <offset>` from a replica connection. Replies
/// `+CONTINUE` (partial) or `+FULLRESYNC` (full, asynchronously).
pub fn psync(
    server: &Server,
    client: &Arc<ClientHandle>,
    port: u16,
    replid: &[u8],
    offset: &[u8],
    out: &mut Vec<u8>,
) {
    client.set_kind(ClientKind::Replica);
    let req_off: i64 = std::str::from_utf8(offset)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(-1);
    let req_id = String::from_utf8_lossy(replid).into_owned();
    {
        let mut p = server.prop.lock();
        let id_ok = req_id == p.repl.replid
            || (req_id == p.repl.replid2 && req_off <= p.repl.second_offset);
        if id_ok
            && req_off >= 0
            && let Some(b) = &p.repl.backlog
        {
            // `req_off` is the first byte the replica wants (1-based, Redis
            // style): our backlog holds bytes (offset - histlen, offset].
            let have_from = p.repl.offset as i64 - b.histlen() as i64 + 1;
            let wanted = req_off;
            if wanted >= have_from && wanted <= p.repl.offset as i64 + 1 {
                let missing = (p.repl.offset as i64 + 1 - wanted) as usize;
                let data = b.tail(missing);
                out.extend_from_slice(format!("+CONTINUE {}\r\n", p.repl.replid).as_bytes());
                client.push(&data);
                let link = Arc::new(ReplicaLink::new(client.clone(), port, true));
                link.ack_offset.store((wanted - 1).max(0) as u64, Relaxed);
                p.repl.replicas.push(link);
                server.stats.sync_partial_ok.fetch_add(1, Relaxed);
                log(
                    '*',
                    &format!(
                        "Partial resynchronization request from {} accepted. Sending {missing} bytes of backlog.",
                        client.addr
                    ),
                );
                return;
            }
        }
        if req_id != "?" {
            server.stats.sync_partial_err.fetch_add(1, Relaxed);
        }
    }
    server.stats.sync_full.fetch_add(1, Relaxed);
    log(
        '*',
        &format!(
            "Starting full resynchronization with replica {}",
            client.addr
        ),
    );
    let server = server.arc();
    let client = client.clone();
    let _ = std::thread::Builder::new()
        .name("full-sync".into())
        .spawn(move || full_sync(&server, client, port));
}

fn full_sync(server: &Server, client: Arc<ClientHandle>, port: u16) {
    let _one = server.persist.snapshot_lock.lock();
    if client.closing.load(Relaxed) {
        return;
    }
    let link = Arc::new(ReplicaLink::new(client.clone(), port, false));
    let (job, replid, offset) = snapshot::begin(server, |p| {
        ensure_backlog(server, p);
        // Force a SELECT before the first streamed command.
        p.repl.selected_db = None;
        client.push(format!("+FULLRESYNC {} {}\r\n", p.repl.replid, p.repl.offset).as_bytes());
        p.repl.replicas.push(link.clone());
    });
    let bytes = snapshot::finish(server, &job, &replid, offset);
    let p = server.prop.lock();
    let mut st = link.state.lock();
    if let LinkState::WaitSnapshot(buffered) = std::mem::replace(&mut *st, LinkState::Online) {
        let mut payload = format!("${}\r\n", bytes.len()).into_bytes();
        payload.extend_from_slice(&bytes);
        payload.extend_from_slice(&buffered);
        client.push(&payload);
    }
    link.ack_offset.store(offset, Relaxed);
    drop(st);
    drop(p);
    log(
        '*',
        &format!(
            "Synchronization with replica {} succeeded ({} bytes)",
            client.addr,
            bytes.len()
        ),
    );
}

/// Removes a replica when its connection closes.
pub fn detach_replica(server: &Server, client_id: u64) {
    let mut p = server.prop.lock();
    p.repl.replicas.retain(|l| l.client.id != client_id);
}

/// Records `REPLCONF ACK <offset>`.
pub fn ack(server: &Server, client_id: u64, offset: u64) {
    let p = server.prop.lock();
    if let Some(l) = p.repl.replicas.iter().find(|l| l.client.id == client_id) {
        l.ack_offset.store(offset, Relaxed);
        l.last_ack_ms.store(crate::util::now_ms(), Relaxed);
    }
}

/// Keepalive PING into the replication stream (cron).
pub fn ping_replicas(server: &Server) {
    if server.is_replica() {
        return;
    }
    let mut p = server.prop.lock();
    if p.repl.replicas.is_empty() {
        return;
    }
    let mut b = Vec::new();
    encode_command(&mut b, &[b"PING"]);
    Propagator::feed_locked(&mut p, &b);
}

/// Applies a new `repl-backlog-size`.
pub fn resize_backlog(server: &Server) {
    let mut p = server.prop.lock();
    let size = server.config.repl_backlog_size.load(Relaxed);
    if let Some(b) = p.repl.backlog.as_ref() {
        if b.size() == size {
            return;
        }
        let keep = b.histlen().min(size);
        let tail = b.tail(keep);
        let mut nb = Backlog::new(size);
        nb.append(&tail);
        p.repl.backlog = Some(nb);
    }
}

/// `WAIT numreplicas timeout`: blocks the calling worker until enough
/// replicas acknowledged the current offset or the timeout elapses.
pub fn wait(server: &Server, numreplicas: usize, timeout_ms: u64) -> usize {
    let target = {
        let mut p = server.prop.lock();
        let target = p.repl.offset;
        if p.repl.replicas.is_empty() {
            return 0;
        }
        let mut b = Vec::new();
        encode_command(&mut b, &[b"REPLCONF", b"GETACK", b"*"]);
        Propagator::feed_locked(&mut p, &b);
        target
    };
    let deadline = if timeout_ms == 0 {
        u64::MAX
    } else {
        crate::util::now_ms() + timeout_ms
    };
    loop {
        let acked = {
            let p = server.prop.lock();
            p.repl
                .replicas
                .iter()
                .filter(|l| l.ack_offset.load(Relaxed) >= target)
                .count()
        };
        if acked >= numreplicas
            || crate::util::now_ms() >= deadline
            || server.shutdown.load(Relaxed)
        {
            return acked;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}

/// Drops every attached replica (when this server becomes a replica).
pub fn drop_all_replicas(server: &Server) {
    let links = std::mem::take(&mut server.prop.lock().repl.replicas);
    for l in links {
        l.client.kill();
    }
}
