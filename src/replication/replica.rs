//! Replica side: connects to the master, performs the PSYNC handshake, loads
//! the snapshot on a full sync and applies the command stream.

use crate::persistence::snapshot;
use crate::resp::{Args, Parser, encode_command};
use crate::server::{Server, log};
use std::io::{ErrorKind, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::sync::atomic::Ordering::{Relaxed, SeqCst};
use std::time::{Duration, Instant};

/// Starts replicating from `host:port` (`REPLICAOF host port`).
pub fn start(server: &Server, host: String, port: u16) {
    let generation = server.repl.generation.fetch_add(1, SeqCst) + 1;
    crate::replication::master::drop_all_replicas(server);
    server.repl.is_replica.store(true, Relaxed);
    *server.repl.master.lock() = Some((host.clone(), port));
    server.repl.link_up.store(false, Relaxed);
    let s = server.arc();
    let _ = std::thread::Builder::new()
        .name("replication".into())
        .spawn(move || run(s, generation, host, port));
}

/// Stops replicating and promotes this server to a master
/// (`REPLICAOF NO ONE`). The dataset is kept.
pub fn stop(server: &Server) {
    server.repl.generation.fetch_add(1, SeqCst);
    if !server.repl.is_replica.swap(false, Relaxed) {
        return;
    }
    *server.repl.master.lock() = None;
    server.repl.link_up.store(false, Relaxed);
    let off = server.repl.offset.load(Relaxed);
    let old_id = server.repl.master_replid.lock().clone();
    let mut p = server.prop.lock();
    if !old_id.is_empty() {
        p.repl.replid2 = old_id;
        p.repl.second_offset = off as i64 + 1;
    }
    p.repl.replid = crate::propagate::new_replid();
    p.repl.offset = off;
    p.repl.backlog = None;
    p.repl.selected_db = None;
    server.prop.refresh_active(&p);
    log(
        '*',
        "MASTER MODE enabled (user request from replica promotion)",
    );
}

fn alive(server: &Server, generation: u64) -> bool {
    server.repl.generation.load(SeqCst) == generation && !server.shutdown.load(Relaxed)
}

fn run(server: Arc<Server>, generation: u64, host: String, port: u16) {
    while alive(&server, generation) {
        log('*', &format!("Connecting to MASTER {host}:{port}"));
        if let Err(e) = session(&server, generation, &host, port) {
            log('#', &format!("MASTER <-> REPLICA sync: {e}"));
        }
        server.repl.link_up.store(false, Relaxed);
        server.repl.sync_in_progress.store(false, Relaxed);
        for _ in 0..10 {
            if !alive(&server, generation) {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}

struct Conn {
    stream: TcpStream,
    buf: Vec<u8>,
}

impl Conn {
    fn send(&mut self, args: &[&[u8]]) -> Result<(), String> {
        let mut b = Vec::new();
        encode_command(&mut b, args);
        self.stream
            .write_all(&b)
            .map_err(|e| format!("write to master failed: {e}"))
    }

    /// Reads more bytes; returns false on timeout.
    fn fill(&mut self) -> Result<bool, String> {
        let mut tmp = [0u8; 64 * 1024];
        match self.stream.read(&mut tmp) {
            Ok(0) => Err("connection with master lost".into()),
            Ok(n) => {
                self.buf.extend_from_slice(&tmp[..n]);
                Ok(true)
            }
            Err(e)
                if matches!(
                    e.kind(),
                    ErrorKind::WouldBlock | ErrorKind::TimedOut | ErrorKind::Interrupted
                ) =>
            {
                Ok(false)
            }
            Err(e) => Err(format!("read from master failed: {e}")),
        }
    }

    /// Reads one `\r\n`-terminated line (skipping bare `\n` keepalives).
    fn line(&mut self, timeout: Duration) -> Result<String, String> {
        let deadline = Instant::now() + timeout;
        loop {
            while self.buf.first() == Some(&b'\n') {
                self.buf.remove(0);
            }
            if let Some(i) = self.buf.windows(2).position(|w| w == b"\r\n") {
                let l = String::from_utf8_lossy(&self.buf[..i]).into_owned();
                self.buf.drain(..i + 2);
                return Ok(l);
            }
            if Instant::now() >= deadline {
                return Err("timeout waiting for master reply".into());
            }
            self.fill()?;
        }
    }

    fn exact(&mut self, n: usize, timeout: Duration) -> Result<Vec<u8>, String> {
        let deadline = Instant::now() + timeout;
        while self.buf.len() < n {
            if Instant::now() >= deadline {
                return Err("timeout receiving snapshot from master".into());
            }
            self.fill()?;
        }
        Ok(self.buf.drain(..n).collect())
    }
}

fn session(server: &Server, generation: u64, host: &str, port: u16) -> Result<(), String> {
    let addr = (host, port)
        .to_socket_addrs()
        .map_err(|e| format!("can't resolve {host}: {e}"))?
        .next()
        .ok_or("no address for master")?;
    let stream = TcpStream::connect_timeout(&addr, Duration::from_secs(5))
        .map_err(|e| format!("connect failed: {e}"))?;
    let _ = stream.set_nodelay(true);
    stream
        .set_read_timeout(Some(Duration::from_millis(100)))
        .map_err(|e| e.to_string())?;
    let mut c = Conn {
        stream,
        buf: Vec::new(),
    };
    let t = Duration::from_secs(server.config.repl_timeout.load(Relaxed).max(1));
    c.send(&[b"PING"])?;
    let pong = c.line(t)?;
    if pong.starts_with('-') && !pong.starts_with("-NOAUTH") {
        return Err(format!("master replied to PING: {pong}"));
    }
    let my_port = server.port.load(Relaxed).to_string();
    c.send(&[b"REPLCONF", b"listening-port", my_port.as_bytes()])?;
    let r = c.line(t)?;
    if r.starts_with('-') {
        return Err(format!("REPLCONF listening-port failed: {r}"));
    }
    c.send(&[b"REPLCONF", b"capa", b"eof", b"capa", b"psync2"])?;
    let _ = c.line(t)?;
    let known = server.repl.master_replid.lock().clone();
    let off = server.repl.offset.load(Relaxed);
    if known.is_empty() {
        c.send(&[b"PSYNC", b"?", b"-1"])?;
    } else {
        let next = (off + 1).to_string();
        c.send(&[b"PSYNC", known.as_bytes(), next.as_bytes()])?;
    }
    let reply = c.line(t)?;
    if let Some(rest) = reply.strip_prefix("+FULLRESYNC ") {
        let mut parts = rest.split_whitespace();
        let id = parts.next().ok_or("bad FULLRESYNC reply")?.to_string();
        let moff: u64 = parts
            .next()
            .and_then(|s| s.parse().ok())
            .ok_or("bad FULLRESYNC offset")?;
        server.repl.sync_in_progress.store(true, Relaxed);
        // The snapshot may take a while to be produced: wait generously.
        let hdr = c.line(Duration::from_secs(3600))?;
        let len: usize = hdr
            .strip_prefix('$')
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| format!("bad snapshot header from master: {hdr}"))?;
        let payload = c.exact(len, Duration::from_secs(3600))?;
        {
            let mut guards = server.keyspace.lock_all();
            for g in guards.iter_mut() {
                g.before_write();
                g.touch_all_watched(None);
                for db in g.dbs.iter_mut() {
                    db.clear();
                }
            }
        }
        let (n, _) = snapshot::load_into(server, &payload, false)?;
        *server.repl.master_replid.lock() = id;
        server.repl.offset.store(moff, Relaxed);
        server.repl.master_db.store(0, Relaxed);
        server.repl.full_syncs.fetch_add(1, Relaxed);
        server.repl.sync_in_progress.store(false, Relaxed);
        log(
            '*',
            &format!("MASTER <-> REPLICA sync: Finished with success ({n} keys, {len} bytes)"),
        );
        if server.config.appendonly.load(Relaxed) {
            let _ = crate::persistence::aof::bgrewrite(server);
        }
    } else if let Some(rest) = reply.strip_prefix("+CONTINUE") {
        let new_id = rest.trim();
        if !new_id.is_empty() {
            *server.repl.master_replid.lock() = new_id.to_string();
        }
        server.repl.partial_syncs.fetch_add(1, Relaxed);
        log(
            '*',
            "MASTER <-> REPLICA sync: Master accepted a Partial Resynchronization.",
        );
    } else {
        return Err(format!("unexpected reply to PSYNC: {reply}"));
    }
    server.repl.link_up.store(true, Relaxed);
    server.repl.last_io_ms.store(crate::util::now_ms(), Relaxed);
    stream_loop(server, generation, &mut c)
}

fn stream_loop(server: &Server, generation: u64, c: &mut Conn) -> Result<(), String> {
    let mut client = server.internal_client();
    client.db = server.repl.master_db.load(Relaxed);
    let stats = server.background_stats();
    let mut parser = Parser::new();
    let (mut ppos, mut cmd_start) = (0usize, 0usize);
    let mut last_ack = Instant::now() - Duration::from_secs(2);
    let timeout_ms = server.config.repl_timeout.load(Relaxed) * 1000;
    loop {
        if !alive(server, generation) {
            return Ok(());
        }
        loop {
            let parsed = parser
                .parse(&c.buf, &mut ppos)
                .map_err(|e| format!("protocol error in replication stream: {}", e.0))?;
            let Some(args) = parsed else { break };
            let consumed = (ppos - cmd_start) as u64;
            let owned: Vec<Vec<u8>> = match args {
                Args::Ranges(r) => r.iter().map(|&(s, l)| c.buf[s..s + l].to_vec()).collect(),
                Args::Owned(o) => o,
            };
            cmd_start = ppos;
            if owned.is_empty() {
                server.repl.offset.fetch_add(consumed, Relaxed);
                continue;
            }
            let refs: Vec<&[u8]> = owned.iter().map(|a| &a[..]).collect();
            if refs.len() >= 2
                && refs[0].eq_ignore_ascii_case(b"replconf")
                && refs[1].eq_ignore_ascii_case(b"getack")
            {
                let off = server.repl.offset.load(Relaxed).to_string();
                c.send(&[b"REPLCONF", b"ACK", off.as_bytes()])?;
                server.repl.offset.fetch_add(consumed, Relaxed);
                continue;
            }
            crate::cmd::execute_internal(server, &mut client, stats, &refs, crate::util::now_ms());
            server.repl.master_db.store(client.db, Relaxed);
            server.repl.offset.fetch_add(consumed, Relaxed);
        }
        if !parser.in_progress() && cmd_start > 0 {
            c.buf.drain(..cmd_start);
            ppos -= cmd_start;
            cmd_start = 0;
        }
        if last_ack.elapsed() >= Duration::from_secs(1) {
            last_ack = Instant::now();
            let off = server.repl.offset.load(Relaxed).to_string();
            c.send(&[b"REPLCONF", b"ACK", off.as_bytes()])?;
        }
        if c.fill()? {
            server.repl.last_io_ms.store(crate::util::now_ms(), Relaxed);
        } else if crate::util::now_ms().saturating_sub(server.repl.last_io_ms.load(Relaxed))
            > timeout_ms
        {
            return Err("timeout: no data from master".into());
        }
    }
}
