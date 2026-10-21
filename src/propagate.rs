//! Write propagation: the single ordered stream of effects that feeds the AOF
//! and the replication backlog/replicas.
//!
//! Commands are appended while their shard locks are still held, so any two
//! conflicting commands (which always share a shard lock) appear in the
//! stream in the order they executed. Non-conflicting commands may interleave
//! arbitrarily, which is harmless because they commute. This is what lets a
//! multi-threaded executor produce a replayable, Redis-compatible stream.

use crate::replication::master::ReplicaLink;
use crate::resp::encode_command;
use parking_lot::{Mutex, MutexGuard};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// A fixed-size ring buffer holding the tail of the replication stream, used
/// to serve partial resynchronizations (`PSYNC replid offset`).
pub struct Backlog {
    buf: Vec<u8>,
    /// Next write index.
    idx: usize,
    /// Valid bytes (≤ capacity).
    histlen: usize,
}

impl Backlog {
    /// A backlog of `size` bytes.
    pub fn new(size: usize) -> Self {
        Backlog {
            buf: vec![0; size.max(1)],
            idx: 0,
            histlen: 0,
        }
    }

    /// Capacity in bytes.
    pub fn size(&self) -> usize {
        self.buf.len()
    }

    /// Bytes of history held.
    pub fn histlen(&self) -> usize {
        self.histlen
    }

    /// Appends stream bytes, overwriting the oldest history.
    pub fn append(&mut self, mut data: &[u8]) {
        let size = self.buf.len();
        if data.len() >= size {
            data = &data[data.len() - size..];
        }
        let first = (size - self.idx).min(data.len());
        self.buf[self.idx..self.idx + first].copy_from_slice(&data[..first]);
        let rest = data.len() - first;
        self.buf[..rest].copy_from_slice(&data[first..]);
        self.idx = (self.idx + data.len()) % size;
        self.histlen = (self.histlen + data.len()).min(size);
    }

    /// The last `n` bytes of history (`n ≤ histlen`).
    pub fn tail(&self, n: usize) -> Vec<u8> {
        assert!(n <= self.histlen);
        let size = self.buf.len();
        let start = (self.idx + size - n) % size;
        let mut out = Vec::with_capacity(n);
        if start + n <= size {
            out.extend_from_slice(&self.buf[start..start + n]);
        } else {
            out.extend_from_slice(&self.buf[start..]);
            out.extend_from_slice(&self.buf[..n - (size - start)]);
        }
        out
    }
}

/// Pending AOF bytes, written to disk by `persistence::aof::flush`.
#[derive(Default)]
pub struct AofBuf {
    /// Encoded commands not yet written.
    pub buf: Vec<u8>,
    /// Database the AOF stream currently has selected.
    pub selected_db: Option<usize>,
}

/// Replication stream state (master side).
pub struct ReplFeed {
    /// Replication ID of this history.
    pub replid: String,
    /// Previous replication ID (after a promotion), for PSYNC continuity.
    pub replid2: String,
    /// Offset up to which `replid2` is valid.
    pub second_offset: i64,
    /// Total bytes ever fed (`master_repl_offset`).
    pub offset: u64,
    /// The backlog; created when the first replica attaches.
    pub backlog: Option<Backlog>,
    /// Database the replication stream currently has selected.
    pub selected_db: Option<usize>,
    /// Attached replicas.
    pub replicas: Vec<Arc<ReplicaLink>>,
}

/// Everything behind the propagation lock.
pub struct PropInner {
    /// AOF buffer (present while `appendonly` is on).
    pub aof: Option<AofBuf>,
    /// Replication feed.
    pub repl: ReplFeed,
}

/// The propagation hub. See the module docs.
pub struct Propagator {
    active: AtomicBool,
    inner: Mutex<PropInner>,
}

/// Generates a 40-hex-character replication id.
pub fn new_replid() -> String {
    (0..5)
        .map(|_| format!("{:08x}", crate::util::rand_u64() as u32))
        .collect()
}

fn select_cmd(db: usize) -> Vec<u8> {
    let mut v = Vec::new();
    let d = db.to_string();
    encode_command(&mut v, &[b"SELECT", d.as_bytes()]);
    v
}

impl Default for Propagator {
    fn default() -> Self {
        Self::new()
    }
}

impl Propagator {
    /// A propagator with no sinks.
    pub fn new() -> Self {
        Propagator {
            active: AtomicBool::new(false),
            inner: Mutex::new(PropInner {
                aof: None,
                repl: ReplFeed {
                    replid: new_replid(),
                    replid2: "0".repeat(40),
                    second_offset: -1,
                    offset: 0,
                    backlog: None,
                    selected_db: None,
                    replicas: Vec::new(),
                },
            }),
        }
    }

    /// Fast check: is anything listening? Commands skip encoding when not.
    #[inline]
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed)
    }

    /// Locks the propagation state.
    pub fn lock(&self) -> MutexGuard<'_, PropInner> {
        self.inner.lock()
    }

    /// Recomputes the fast-path flag after sinks change.
    pub fn refresh_active(&self, inner: &PropInner) {
        self.active.store(
            inner.aof.is_some() || inner.repl.backlog.is_some(),
            Ordering::Relaxed,
        );
    }

    /// Appends commands executed against `db`. `cmds` holds one or more
    /// RESP-encoded commands.
    pub fn propagate(&self, db: usize, cmds: &[u8]) {
        if cmds.is_empty() {
            return;
        }
        let mut g = self.inner.lock();
        Self::propagate_locked(&mut g, db, cmds);
    }

    /// Like [`Propagator::propagate`] with the lock already held.
    pub fn propagate_locked(g: &mut PropInner, db: usize, cmds: &[u8]) {
        if let Some(aof) = g.aof.as_mut() {
            if aof.selected_db != Some(db) {
                aof.buf.extend_from_slice(&select_cmd(db));
                aof.selected_db = Some(db);
            }
            aof.buf.extend_from_slice(cmds);
        }
        if g.repl.backlog.is_some() {
            if g.repl.selected_db != Some(db) {
                let sel = select_cmd(db);
                Self::feed_locked(g, &sel);
                g.repl.selected_db = Some(db);
            }
            Self::feed_locked(g, cmds);
        }
    }

    /// Feeds raw bytes to the replication stream only (e.g. keepalive PINGs).
    pub fn feed_locked(g: &mut PropInner, data: &[u8]) {
        let Some(backlog) = g.repl.backlog.as_mut() else {
            return;
        };
        backlog.append(data);
        g.repl.offset += data.len() as u64;
        for r in &g.repl.replicas {
            r.feed(data);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backlog_ring() {
        let mut b = Backlog::new(8);
        b.append(b"abc");
        assert_eq!(b.tail(3), b"abc");
        b.append(b"defgh");
        assert_eq!(b.histlen(), 8);
        assert_eq!(b.tail(8), b"abcdefgh");
        b.append(b"XY");
        assert_eq!(b.tail(8), b"cdefghXY");
        assert_eq!(b.tail(2), b"XY");
        b.append(b"0123456789");
        assert_eq!(b.tail(8), b"23456789");
    }

    #[test]
    fn aof_gets_select() {
        let p = Propagator::new();
        {
            let mut g = p.lock();
            g.aof = Some(AofBuf::default());
            p.refresh_active(&g);
        }
        assert!(p.is_active());
        let mut cmd = Vec::new();
        encode_command(&mut cmd, &[b"SET", b"a", b"1"]);
        p.propagate(3, &cmd);
        p.propagate(3, &cmd);
        let g = p.lock();
        let buf = &g.aof.as_ref().unwrap().buf;
        let text = String::from_utf8_lossy(buf);
        assert_eq!(text.matches("SELECT").count(), 1);
        assert_eq!(text.matches("SET\r\n").count(), 2);
    }
}
