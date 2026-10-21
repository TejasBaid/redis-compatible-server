//! A worker thread: one mio event loop serving a subset of the connections.
//!
//! Each worker reads requests, executes them directly (locking only the
//! shards a command touches) and writes the replies, so request parsing,
//! execution and socket I/O all scale across cores. Pipelined requests that
//! arrive in one read are executed back to back and answered with one write.

use super::client::{ClientHandle, ClientState, Mailbox, Msg};
use super::{Server, WorkerStats, log};
use crate::blocking::{BlockedClient, CANCELLED, TIMED_OUT};
use crate::cmd::{self, Outcome};
use crate::resp::{Args, Parser, Reply};
use mio::net::TcpStream;
use mio::{Events, Interest, Poll, Token};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::io::{ErrorKind, Read, Write};
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;

/// Token reserved for the mailbox waker.
pub const WAKER: Token = Token(usize::MAX);

const READ_CHUNK: usize = 16 * 1024;

struct Conn {
    stream: TcpStream,
    /// Input buffer; always fully initialized, `ilen` bytes are valid.
    inbuf: Vec<u8>,
    ilen: usize,
    rpos: usize,
    parser: Parser,
    out: Vec<u8>,
    opos: usize,
    state: ClientState,
    blocked: Option<(Arc<BlockedClient>, Option<u64>)>,
    writable: bool,
    closing: bool,
    eof: bool,
    last_active: u64,
}

/// A worker. See the module docs.
pub struct Worker {
    idx: usize,
    /// The server outlives every worker; a `'static` reference (from one
    /// leaked `Arc` clone per worker) avoids touching the shared reference
    /// count on the hot path.
    server: &'static Server,
    poll: Poll,
    mailbox: Arc<Mailbox>,
    conns: Vec<Option<Conn>>,
    free: Vec<usize>,
    by_id: HashMap<u64, usize>,
    timers: BinaryHeap<Reverse<(u64, u64)>>,
    stats: &'static WorkerStats,
    now: u64,
}

impl Worker {
    /// Creates worker `idx`.
    pub fn new(idx: usize, server: Arc<Server>, poll: Poll, mailbox: Arc<Mailbox>) -> Self {
        let server: &'static Server = Box::leak(Box::new(server));
        let stats: &'static WorkerStats = &server.worker_stats[idx];
        Worker {
            idx,
            server,
            poll,
            mailbox,
            conns: Vec::new(),
            free: Vec::new(),
            by_id: HashMap::new(),
            timers: BinaryHeap::new(),
            stats,
            now: crate::util::now_ms(),
        }
    }

    /// Runs the event loop until shutdown.
    pub fn run(mut self) {
        let mut events = Events::with_capacity(1024);
        let mut msgs = Vec::new();
        let mut last_timeout_check = self.now;
        loop {
            let timeout = self
                .timers
                .peek()
                .map(|Reverse((d, _))| {
                    Duration::from_millis(d.saturating_sub(crate::util::now_ms()))
                })
                .unwrap_or(Duration::from_millis(100))
                .min(Duration::from_millis(100));
            if let Err(e) = self.poll.poll(&mut events, Some(timeout))
                && e.kind() != ErrorKind::Interrupted
            {
                log('#', &format!("worker {}: poll error: {e}", self.idx));
            }
            self.now = crate::util::now_ms();
            for ev in events.iter() {
                if ev.token() == WAKER {
                    continue;
                }
                let t = ev.token().0;
                if ev.is_readable() || ev.is_read_closed() || ev.is_error() {
                    self.on_readable(t);
                }
                if ev.is_writable() {
                    self.flush(t);
                }
            }
            self.mailbox.drain(&mut msgs);
            for m in msgs.drain(..) {
                match m {
                    Msg::NewConn(stream, addr) => self.adopt(stream, addr),
                    Msg::Pending(id) => self.on_pending(id),
                    Msg::Kill(id) => {
                        if let Some(&t) = self.by_id.get(&id) {
                            self.close(t);
                        }
                    }
                    Msg::Shutdown => {
                        let tokens: Vec<usize> = self.by_id.values().copied().collect();
                        for t in tokens {
                            self.flush(t);
                            self.close(t);
                        }
                        return;
                    }
                }
            }
            self.fire_timers();
            if self.now.saturating_sub(last_timeout_check) >= 1000 {
                last_timeout_check = self.now;
                self.check_idle_timeouts();
            }
        }
    }

    fn adopt(&mut self, mut stream: TcpStream, addr: std::net::SocketAddr) {
        let t = self.free.pop().unwrap_or_else(|| {
            self.conns.push(None);
            self.conns.len() - 1
        });
        if self
            .poll
            .registry()
            .register(&mut stream, Token(t), Interest::READABLE)
            .is_err()
        {
            self.free.push(t);
            return;
        }
        let id = self.server.next_client_id();
        let laddr = stream
            .local_addr()
            .map(|a| a.to_string())
            .unwrap_or_default();
        let handle = Arc::new(ClientHandle::new(
            id,
            Some(self.mailbox.clone()),
            addr.to_string(),
            laddr,
        ));
        self.server.clients.lock().insert(id, handle.clone());
        self.server.connected.fetch_add(1, Relaxed);
        let authed = self.server.config.requirepass.read().is_empty();
        self.conns[t] = Some(Conn {
            stream,
            inbuf: vec![0; READ_CHUNK],
            ilen: 0,
            rpos: 0,
            parser: Parser::new(),
            out: Vec::new(),
            opos: 0,
            state: ClientState::new(handle, authed),
            blocked: None,
            writable: false,
            closing: false,
            eof: false,
            last_active: self.now,
        });
        self.by_id.insert(id, t);
    }

    fn on_readable(&mut self, t: usize) {
        let Some(c) = self.conns.get_mut(t).and_then(|c| c.as_mut()) else {
            return;
        };
        let mut read_total = 0usize;
        loop {
            if c.inbuf.len() - c.ilen < READ_CHUNK / 2 {
                let grow = (c.inbuf.len() * 2).max(READ_CHUNK);
                c.inbuf.resize(grow, 0);
            }
            let space = c.inbuf.len() - c.ilen;
            match c.stream.read(&mut c.inbuf[c.ilen..]) {
                Ok(0) => {
                    c.eof = true;
                    break;
                }
                Ok(n) => {
                    c.ilen += n;
                    read_total += n;
                    // A short read drained the socket buffer; with edge-triggered
                    // readiness any later data raises a new event. Skipping the
                    // extra read that would return EAGAIN saves a syscall per
                    // request (Redis likewise reads once per event).
                    if n < space {
                        break;
                    }
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(_) => {
                    c.eof = true;
                    break;
                }
            }
        }
        if read_total > 0 {
            self.stats.net_in.fetch_add(read_total as u64, Relaxed);
            c.last_active = self.now;
            c.state.handle.last_active_ms.store(self.now, Relaxed);
        }
        self.process(t);
        let eof = self
            .conns
            .get(t)
            .and_then(|c| c.as_ref())
            .is_some_and(|c| c.eof);
        if eof {
            // Peer closed: answer what we already have, then close.
            self.flush(t);
            self.close(t);
        }
    }

    /// Executes every complete request in the input buffer.
    fn process(&mut self, t: usize) {
        let server = self.server;
        let stats = self.stats;
        let now = self.now;
        let Some(c) = self.conns.get_mut(t).and_then(|c| c.as_mut()) else {
            return;
        };
        let dirty_before = stats.dirty.load(Relaxed);
        let mut new_block = None;
        while c.blocked.is_none() && !c.closing {
            let mut pos = c.rpos;
            let parsed = c.parser.parse(&c.inbuf[..c.ilen], &mut pos);
            c.rpos = pos;
            let args = match parsed {
                Ok(Some(a)) => a,
                Ok(None) => break,
                Err(e) => {
                    Reply::new(&mut c.out, c.state.resp3)
                        .error(&format!("ERR Protocol error: {}", e.0));
                    c.closing = true;
                    break;
                }
            };
            let mut stack: [&[u8]; 8] = [&[]; 8];
            let heap: Vec<&[u8]>;
            let argv: &[&[u8]] = match &args {
                Args::Ranges(r) if r.len() <= 8 => {
                    for (i, &(s, l)) in r.iter().enumerate() {
                        stack[i] = &c.inbuf[s..s + l];
                    }
                    &stack[..r.len()]
                }
                Args::Ranges(r) => {
                    heap = r.iter().map(|&(s, l)| &c.inbuf[s..s + l]).collect();
                    &heap
                }
                Args::Owned(o) => {
                    heap = o.iter().map(|v| &v[..]).collect();
                    &heap
                }
            };
            if argv.is_empty() {
                continue;
            }
            let skip_reply = c.state.reply_mode != 0;
            let out_len = c.out.len();
            let outcome = if c.state.is_master {
                // Commands from our master: execute, never reply (except
                // REPLCONF GETACK, handled by the replication thread).
                let _ = cmd::execute_internal(server, &mut c.state, stats, argv, now);
                Outcome::Normal
            } else {
                cmd::execute(server, &mut c.state, stats, &mut c.out, argv, now)
            };
            if skip_reply {
                c.out.truncate(out_len);
                if c.state.reply_mode == 2 {
                    c.state.reply_mode = 0;
                }
            }
            match outcome {
                Outcome::Normal => {}
                Outcome::Close => c.closing = true,
                Outcome::Blocked(b, deadline) => {
                    if let Some(d) = deadline {
                        new_block = Some((d, c.state.id));
                    }
                    c.blocked = Some((b, deadline));
                }
            }
        }
        if !c.parser.in_progress() && c.rpos > 0 {
            if c.rpos >= c.ilen {
                c.ilen = 0;
                if c.inbuf.len() > 1 << 20 {
                    c.inbuf.truncate(READ_CHUNK);
                    c.inbuf.shrink_to_fit();
                }
            } else {
                c.inbuf.copy_within(c.rpos..c.ilen, 0);
                c.ilen -= c.rpos;
            }
            c.rpos = 0;
        }
        c.state.handle.qbuf.store(c.ilen - c.rpos, Relaxed);
        if let Some(timer) = new_block {
            self.timers.push(Reverse(timer));
        }
        // Make writes durable (per appendfsync) before acknowledging them.
        if stats.dirty.load(Relaxed) != dirty_before && server.config.appendonly.load(Relaxed) {
            let always = server.config.fsync() == crate::config::FsyncPolicy::Always;
            crate::persistence::aof::flush(server, always);
        }
        self.flush(t);
        let closing = self
            .conns
            .get(t)
            .and_then(|c| c.as_ref())
            .is_some_and(|c| c.closing && c.out.len() == c.opos);
        if closing {
            self.close(t);
        }
    }

    /// Writes as much pending output as the socket accepts.
    fn flush(&mut self, t: usize) {
        let Some(c) = self.conns.get_mut(t).and_then(|c| c.as_mut()) else {
            return;
        };
        let mut written = 0;
        while c.opos < c.out.len() {
            match c.stream.write(&c.out[c.opos..]) {
                Ok(0) => break,
                Ok(n) => {
                    c.opos += n;
                    written += n;
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(_) => {
                    c.closing = true;
                    c.eof = true;
                    c.out.clear();
                    c.opos = 0;
                    break;
                }
            }
        }
        self.stats.net_out.fetch_add(written as u64, Relaxed);
        if c.opos == c.out.len() {
            c.out.clear();
            c.opos = 0;
            if c.out.capacity() > 4 << 20 {
                c.out.shrink_to(64 * 1024);
            }
            if c.writable {
                c.writable = false;
                let _ =
                    self.poll
                        .registry()
                        .reregister(&mut c.stream, Token(t), Interest::READABLE);
            }
            if c.closing {
                // Closed by the caller once it sees the drained buffer.
            }
        } else if !c.writable {
            c.writable = true;
            let _ = self.poll.registry().reregister(
                &mut c.stream,
                Token(t),
                Interest::READABLE | Interest::WRITABLE,
            );
        }
        c.state.handle.omem.store(c.out.len() - c.opos, Relaxed);
        if c.closing && c.opos == 0 && c.out.is_empty() && c.eof {
            self.close(t);
        }
    }

    /// Cross-thread output arrived (pub/sub, unblock reply, replication).
    fn on_pending(&mut self, id: u64) {
        let Some(&t) = self.by_id.get(&id) else {
            return;
        };
        let server = self.server;
        let Some(c) = self.conns.get_mut(t).and_then(|c| c.as_mut()) else {
            return;
        };
        let (data, unblock) = c.state.handle.take_pending();
        c.out.extend_from_slice(&data);
        if !c.state.is_replica && c.out.len() - c.opos > super::client::PENDING_LIMIT {
            log(
                '#',
                &format!(
                    "Client id={} closed for overcoming output buffer limits.",
                    c.state.id
                ),
            );
            self.close(t);
            return;
        }
        if unblock && let Some((b, _)) = c.blocked.take() {
            c.state.handle.blocked.store(false, Relaxed);
            crate::blocking::unregister(server, &b);
            self.flush(t);
            // Resume pipelined requests queued behind the blocking command,
            // and drain the socket (edge-triggered: no new event will come).
            self.on_readable(t);
            return;
        }
        self.flush(t);
    }

    fn fire_timers(&mut self) {
        let now = crate::util::now_ms();
        while let Some(&Reverse((deadline, id))) = self.timers.peek() {
            if deadline > now {
                break;
            }
            self.timers.pop();
            let Some(&t) = self.by_id.get(&id) else {
                continue;
            };
            let server = self.server;
            let Some(c) = self.conns.get_mut(t).and_then(|c| c.as_mut()) else {
                continue;
            };
            let Some((b, Some(d))) = &c.blocked else {
                continue;
            };
            if *d > now {
                continue;
            }
            if b.claim(TIMED_OUT) {
                let b = b.clone();
                c.out.extend_from_slice(&b.kind.timeout_reply(b.resp3));
                c.blocked = None;
                c.state.handle.blocked.store(false, Relaxed);
                crate::blocking::unregister(server, &b);
                self.flush(t);
                self.on_readable(t);
            }
            // Otherwise a producer already served it; its reply is in flight.
        }
    }

    fn check_idle_timeouts(&mut self) {
        let timeout = self.server.config.timeout.load(Relaxed);
        if timeout == 0 {
            return;
        }
        let now = self.now;
        let idle: Vec<usize> = self
            .by_id
            .values()
            .copied()
            .filter(|&t| {
                self.conns[t].as_ref().is_some_and(|c| {
                    c.blocked.is_none()
                        && !c.state.is_replica
                        && !c.state.is_master
                        && c.state.sub_count() == 0
                        && now.saturating_sub(c.last_active) > timeout * 1000
                })
            })
            .collect();
        for t in idle {
            self.close(t);
        }
    }

    fn close(&mut self, t: usize) {
        let Some(mut c) = self.conns.get_mut(t).and_then(|c| c.take()) else {
            return;
        };
        let _ = self.poll.registry().deregister(&mut c.stream);
        self.by_id.remove(&c.state.id);
        self.free.push(t);
        let server = self.server;
        if let Some((b, _)) = c.blocked.take()
            && b.claim(CANCELLED)
        {
            crate::blocking::unregister(server, &b);
        }
        cmd::pubsub::unsubscribe_all(server, &mut c.state);
        cmd::multi::unwatch_all(server, &mut c.state);
        if c.state.is_replica {
            crate::replication::master::detach_replica(server, c.state.id);
        }
        c.state.handle.closing.store(true, Relaxed);
        server.clients.lock().remove(&c.state.id);
        server.connected.fetch_sub(1, Relaxed);
    }
}
