//! Shared helpers for integration tests: launch the server binary (or a real
//! `redis-server`) on a free port, and a minimal RESP2/RESP3 client.
#![allow(dead_code)]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// A decoded reply.
#[derive(Clone, Debug, PartialEq)]
pub enum Reply {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Vec<u8>),
    Nil,
    NilArray,
    Array(Vec<Reply>),
    Map(Vec<(Reply, Reply)>),
    Set(Vec<Reply>),
    Double(String),
    Bool(bool),
    Verbatim(String),
    Push(Vec<Reply>),
    BigNum(String),
}

impl Reply {
    /// Bulk/simple/verbatim as a string.
    pub fn text(&self) -> String {
        match self {
            Reply::Simple(s)
            | Reply::Error(s)
            | Reply::Double(s)
            | Reply::Verbatim(s)
            | Reply::BigNum(s) => s.clone(),
            Reply::Bulk(b) => String::from_utf8_lossy(b).into_owned(),
            Reply::Int(i) => i.to_string(),
            other => format!("{other:?}"),
        }
    }
    pub fn int(&self) -> i64 {
        match self {
            Reply::Int(i) => *i,
            other => panic!("expected integer, got {other:?}"),
        }
    }
    pub fn array(&self) -> Vec<Reply> {
        match self {
            Reply::Array(v) | Reply::Set(v) | Reply::Push(v) => v.clone(),
            Reply::NilArray => Vec::new(),
            other => panic!("expected array, got {other:?}"),
        }
    }
    pub fn strings(&self) -> Vec<String> {
        self.array().iter().map(|r| r.text()).collect()
    }
    /// Human-readable rendering with bulk strings shown as text.
    pub fn flat(&self) -> String {
        match self {
            Reply::Array(v) | Reply::Set(v) | Reply::Push(v) => {
                format!(
                    "[{}]",
                    v.iter().map(|r| r.flat()).collect::<Vec<_>>().join(", ")
                )
            }
            Reply::Map(v) => format!(
                "{{{}}}",
                v.iter()
                    .map(|(k, x)| format!("{}: {}", k.flat(), x.flat()))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Reply::Nil | Reply::NilArray => "nil".into(),
            other => other.text(),
        }
    }
    pub fn is_error(&self) -> bool {
        matches!(self, Reply::Error(_))
    }
}

/// A blocking RESP client.
pub struct Client {
    stream: TcpStream,
    buf: Vec<u8>,
    pos: usize,
}

fn encode(args: &[&[u8]]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        out.extend_from_slice(a);
        out.extend_from_slice(b"\r\n");
    }
    out
}

impl Client {
    pub fn connect(port: u16) -> Client {
        let stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        stream.set_nodelay(true).ok();
        stream.set_read_timeout(Some(Duration::from_secs(30))).ok();
        Client {
            stream,
            buf: Vec::new(),
            pos: 0,
        }
    }

    pub fn set_timeout(&mut self, d: Duration) {
        self.stream.set_read_timeout(Some(d)).ok();
    }

    pub fn cmd(&mut self, args: &[&str]) -> Reply {
        let b: Vec<&[u8]> = args.iter().map(|a| a.as_bytes()).collect();
        self.cmd_bytes(&b)
    }

    pub fn cmd_bytes(&mut self, args: &[&[u8]]) -> Reply {
        self.send_bytes(args);
        self.read().expect("reply")
    }

    pub fn send(&mut self, args: &[&str]) {
        let b: Vec<&[u8]> = args.iter().map(|a| a.as_bytes()).collect();
        self.send_bytes(&b);
    }

    pub fn send_bytes(&mut self, args: &[&[u8]]) {
        self.stream.write_all(&encode(args)).expect("write");
    }

    pub fn send_raw(&mut self, raw: &[u8]) {
        self.stream.write_all(raw).expect("write");
    }

    fn fill(&mut self) -> std::io::Result<()> {
        let mut tmp = [0u8; 65536];
        let n = self.stream.read(&mut tmp)?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "closed",
            ));
        }
        if self.pos > 0 && self.pos == self.buf.len() {
            self.buf.clear();
            self.pos = 0;
        }
        self.buf.extend_from_slice(&tmp[..n]);
        Ok(())
    }

    fn line(&mut self) -> std::io::Result<String> {
        loop {
            if let Some(i) = self.buf[self.pos..].windows(2).position(|w| w == b"\r\n") {
                let s = String::from_utf8_lossy(&self.buf[self.pos..self.pos + i]).into_owned();
                self.pos += i + 2;
                return Ok(s);
            }
            self.fill()?;
        }
    }

    fn bytes(&mut self, n: usize) -> std::io::Result<Vec<u8>> {
        while self.buf.len() - self.pos < n + 2 {
            self.fill()?;
        }
        let v = self.buf[self.pos..self.pos + n].to_vec();
        self.pos += n + 2;
        Ok(v)
    }

    /// Reads one reply.
    pub fn read(&mut self) -> std::io::Result<Reply> {
        let l = self.line()?;
        let (t, rest) = l.split_at(1);
        Ok(match t {
            "+" => Reply::Simple(rest.into()),
            "-" => Reply::Error(rest.into()),
            ":" => Reply::Int(rest.parse().unwrap()),
            "$" => {
                let n: i64 = rest.parse().unwrap();
                if n < 0 {
                    Reply::Nil
                } else {
                    Reply::Bulk(self.bytes(n as usize)?)
                }
            }
            "*" | "~" | ">" => {
                let n: i64 = rest.parse().unwrap();
                if n < 0 {
                    Reply::NilArray
                } else {
                    let mut v = Vec::with_capacity(n as usize);
                    for _ in 0..n {
                        v.push(self.read()?);
                    }
                    match t {
                        "*" => Reply::Array(v),
                        "~" => Reply::Set(v),
                        _ => Reply::Push(v),
                    }
                }
            }
            "%" => {
                let n: usize = rest.parse().unwrap();
                let mut v = Vec::with_capacity(n);
                for _ in 0..n {
                    let k = self.read()?;
                    let val = self.read()?;
                    v.push((k, val));
                }
                Reply::Map(v)
            }
            "_" => Reply::Nil,
            "," => Reply::Double(rest.into()),
            "#" => Reply::Bool(rest == "t"),
            "(" => Reply::BigNum(rest.into()),
            "=" => {
                let n: usize = rest.parse().unwrap();
                let b = self.bytes(n)?;
                Reply::Verbatim(String::from_utf8_lossy(&b[4..]).into_owned())
            }
            other => panic!("unknown reply type {other:?} in line {l:?}"),
        })
    }
}

/// Path of the server binary under test.
pub fn binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_redis-clone"))
}

/// A child server process.
pub struct Server {
    child: Option<Child>,
    pub port: u16,
    pub dir: PathBuf,
    _tmp: Option<tempfile::TempDir>,
    pub log: Arc<Mutex<String>>,
}

impl Server {
    /// Starts redis-clone in a fresh temp dir with persistence off.
    pub fn start(extra: &[&str]) -> Server {
        let tmp = tempfile::Builder::new()
            .prefix("rclone-test")
            .tempdir_in(scratch_dir())
            .unwrap();
        let dir = tmp.path().to_path_buf();
        let mut s = Server::start_in(&dir, extra);
        s._tmp = Some(tmp);
        s
    }

    /// Starts redis-clone with `dir` as its working directory.
    pub fn start_in(dir: &Path, extra: &[&str]) -> Server {
        let mut args: Vec<String> = vec![
            "--port".into(),
            "0".into(),
            "--dir".into(),
            dir.display().to_string(),
            "--threads".into(),
            "4".into(),
        ];
        if !extra.contains(&"--save") {
            args.push("--save".into());
            args.push("".into());
        }
        args.extend(extra.iter().map(|s| s.to_string()));
        let mut child = Command::new(binary())
            .args(&args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn server");
        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take().unwrap();
        let log = Arc::new(Mutex::new(String::new()));
        let (tx, rx) = std::sync::mpsc::channel();
        {
            let log = log.clone();
            std::thread::spawn(move || {
                let mut tx = Some(tx);
                for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                    if let Some(p) = line.split("Ready to accept connections tcp port=").nth(1)
                        && let Some(t) = tx.take()
                    {
                        let _ = t.send(p.trim().parse::<u16>().ok());
                    }
                    let mut l = log.lock().unwrap();
                    l.push_str(&line);
                    l.push('\n');
                }
                if let Some(t) = tx.take() {
                    let _ = t.send(None);
                }
            });
        }
        {
            let log = log.clone();
            std::thread::spawn(move || {
                for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                    let mut l = log.lock().unwrap();
                    l.push_str(&line);
                    l.push('\n');
                }
            });
        }
        let port = rx
            .recv_timeout(Duration::from_secs(30))
            .ok()
            .flatten()
            .unwrap_or_else(|| panic!("server failed to start; log:\n{}", log.lock().unwrap()));
        Server {
            child: Some(child),
            port,
            dir: dir.to_path_buf(),
            _tmp: None,
            log,
        }
    }

    /// Tries to start; returns the log on failure instead of panicking.
    pub fn try_start_in(dir: &Path, extra: &[&str]) -> Result<Server, String> {
        let dir = dir.to_path_buf();
        let extra: Vec<String> = extra.iter().map(|s| s.to_string()).collect();
        std::panic::catch_unwind(move || {
            let e: Vec<&str> = extra.iter().map(|s| s.as_str()).collect();
            Server::start_in(&dir, &e)
        })
        .map_err(|e| {
            e.downcast_ref::<String>()
                .cloned()
                .or_else(|| e.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_default()
        })
    }

    pub fn client(&self) -> Client {
        Client::connect(self.port)
    }

    /// SIGKILL: no shutdown hooks run.
    pub fn kill9(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }

    /// SIGTERM and wait for exit.
    pub fn terminate(&mut self) {
        if let Some(mut c) = self.child.take() {
            // SAFETY-free: use the `kill` utility to send SIGTERM.
            let _ = Command::new("kill")
                .arg("-TERM")
                .arg(c.id().to_string())
                .status();
            let deadline = Instant::now() + Duration::from_secs(20);
            while Instant::now() < deadline {
                if let Ok(Some(_)) = c.try_wait() {
                    return;
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            let _ = c.kill();
            let _ = c.wait();
        }
    }

    /// Waits for the process to exit on its own (e.g. after SHUTDOWN).
    pub fn wait_exit(&mut self, timeout: Duration) -> bool {
        if let Some(c) = self.child.as_mut() {
            let deadline = Instant::now() + timeout;
            while Instant::now() < deadline {
                if let Ok(Some(_)) = c.try_wait() {
                    self.child = None;
                    return true;
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            return false;
        }
        true
    }

    pub fn log_text(&self) -> String {
        self.log.lock().unwrap().clone()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.kill9();
    }
}

/// Scratch directory for test data (inside target/, so it's gitignored).
pub fn scratch_dir() -> PathBuf {
    let d = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("test-scratch");
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// A free TCP port (best effort).
pub fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Finds an executable on PATH (also checks Homebrew's default prefix).
pub fn which(name: &str) -> Option<PathBuf> {
    let path = std::env::var("PATH").unwrap_or_default();
    let mut dirs: Vec<PathBuf> = std::env::split_paths(&path).collect();
    dirs.push("/opt/homebrew/bin".into());
    dirs.push("/usr/local/bin".into());
    dirs.push("/usr/bin".into());
    dirs.into_iter().map(|d| d.join(name)).find(|p| p.is_file())
}

/// A real redis-server, for differential testing.
pub struct RealRedis {
    child: Child,
    pub port: u16,
    _tmp: tempfile::TempDir,
}

impl RealRedis {
    pub fn start() -> Option<RealRedis> {
        let bin = which("redis-server")?;
        let tmp = tempfile::Builder::new()
            .prefix("real-redis")
            .tempdir_in(scratch_dir())
            .ok()?;
        for _ in 0..5 {
            let port = free_port();
            let child = Command::new(&bin)
                .args([
                    "--port",
                    &port.to_string(),
                    "--save",
                    "",
                    "--appendonly",
                    "no",
                    "--dir",
                    &tmp.path().display().to_string(),
                    "--enable-debug-command",
                    "yes",
                ])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .ok()?;
            let deadline = Instant::now() + Duration::from_secs(10);
            while Instant::now() < deadline {
                if let Ok(mut s) = TcpStream::connect(("127.0.0.1", port)) {
                    let _ = s.write_all(b"PING\r\n");
                    let mut b = [0u8; 16];
                    s.set_read_timeout(Some(Duration::from_secs(1))).ok();
                    if let Ok(n) = s.read(&mut b)
                        && b[..n].starts_with(b"+PONG")
                    {
                        return Some(RealRedis {
                            child,
                            port,
                            _tmp: tmp,
                        });
                    }
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            let mut c = child;
            let _ = c.kill();
            let _ = c.wait();
        }
        None
    }

    pub fn client(&self) -> Client {
        Client::connect(self.port)
    }
}

impl Drop for RealRedis {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Polls `f` until it returns true or the timeout expires.
pub fn wait_until(timeout: Duration, mut f: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if f() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    f()
}

/// Parses `field:value` out of an INFO reply.
pub fn info_field(info: &str, field: &str) -> Option<String> {
    info.lines().find_map(|l| {
        l.strip_prefix(&format!("{field}:"))
            .map(|v| v.trim().to_string())
    })
}
