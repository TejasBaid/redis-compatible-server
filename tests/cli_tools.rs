//! The unmodified `redis-cli` and `redis-benchmark` work against the server.
//! Skipped when the tools are not installed.

mod common;

use common::{Server, which};
use std::io::Write;
use std::process::{Command, Stdio};

fn cli(port: u16, args: &[&str]) -> Option<String> {
    let bin = which("redis-cli")?;
    let out = Command::new(bin)
        .arg("-p")
        .arg(port.to_string())
        .args(args)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "redis-cli {args:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    Some(String::from_utf8_lossy(&out.stdout).into_owned())
}

#[test]
fn redis_cli_basic_commands() {
    let s = Server::start(&[]);
    let Some(out) = cli(s.port, &["SET", "greeting", "hello"]) else {
        eprintln!("SKIPPED: redis-cli not installed");
        return;
    };
    assert_eq!(out.trim(), "OK");
    assert_eq!(cli(s.port, &["GET", "greeting"]).unwrap().trim(), "hello");
    cli(s.port, &["RPUSH", "l", "a", "b"]).unwrap();
    assert_eq!(cli(s.port, &["LRANGE", "l", "0", "-1"]).unwrap(), "a\nb\n");
    cli(s.port, &["HSET", "h", "f", "v"]).unwrap();
    // RESP3 mode.
    let r3 = cli(s.port, &["-3", "HGETALL", "h"]).unwrap();
    assert!(r3.contains("f") && r3.contains("v"), "{r3}");
    assert!(
        cli(s.port, &["INFO", "server"])
            .unwrap()
            .contains("redis_version")
    );
    assert_eq!(cli(s.port, &["DBSIZE"]).unwrap().trim(), "3");
    assert!(
        cli(s.port, &["NOSUCHCMD"])
            .unwrap()
            .contains("ERR unknown command")
    );
}

#[test]
fn redis_cli_pipe_scan_and_bigkeys() {
    let s = Server::start(&[]);
    let Some(bin) = which("redis-cli") else {
        eprintln!("SKIPPED: redis-cli not installed");
        return;
    };
    // --pipe mass insertion (raw RESP on stdin).
    let mut proto = Vec::new();
    for i in 0..2000 {
        let k = format!("key:{i}");
        proto.extend_from_slice(
            format!("*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n$1\r\nv\r\n", k.len(), k).as_bytes(),
        );
    }
    let mut child = Command::new(&bin)
        .args(["-p", &s.port.to_string(), "--pipe"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(&proto).unwrap();
    let out = child.wait_with_output().unwrap();
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.contains("errors: 0, replies: 2000"), "{text}");
    // Commands from stdin (non-interactive mode).
    let mut child = Command::new(&bin)
        .args(["-p", &s.port.to_string()])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(b"INCR c\nINCR c\nGET c\n")
        .unwrap();
    let out = String::from_utf8_lossy(&child.wait_with_output().unwrap().stdout).into_owned();
    assert_eq!(out, "1\n2\n2\n");
    // --scan uses SCAN under the hood.
    let scanned = cli(s.port, &["--scan", "--pattern", "key:1??"]).unwrap();
    assert_eq!(scanned.lines().count(), 100);
    // --bigkeys walks the keyspace with SCAN + TYPE + size commands.
    cli(s.port, &["ZADD", "z", "1", "a", "2", "b"]).unwrap();
    let big = cli(s.port, &["--bigkeys"]).unwrap();
    assert!(big.contains("Biggest") || big.contains("biggest"), "{big}");
}

#[test]
fn redis_cli_command_docs_and_repeat_mode() {
    let s = Server::start(&[]);
    // Interactive redis-cli fetches COMMAND DOCS at start-up for its hints;
    // the reply must be a well-formed map with a summary per command.
    let Some(docs) = cli(s.port, &["COMMAND", "DOCS", "get", "set", "zadd"]) else {
        eprintln!("SKIPPED: redis-cli not installed");
        return;
    };
    assert_eq!(docs.matches("summary").count(), 3, "{docs}");
    let count: usize = cli(s.port, &["COMMAND", "COUNT"])
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    let all = cli(s.port, &["COMMAND", "DOCS"]).unwrap();
    assert_eq!(all.matches("summary").count(), count);
    // Repeat mode (-r) and RESP3 sessions.
    assert_eq!(
        cli(s.port, &["-r", "3", "PING"]).unwrap(),
        "PONG\nPONG\nPONG\n"
    );
    assert_eq!(cli(s.port, &["-3", "PING"]).unwrap().trim(), "PONG");
}

#[test]
fn redis_benchmark_runs_all_tests() {
    let s = Server::start(&[]);
    let Some(bin) = which("redis-benchmark") else {
        eprintln!("SKIPPED: redis-benchmark not installed");
        return;
    };
    let tests =
        "ping,set,get,incr,lpush,rpush,lpop,rpop,sadd,hset,spop,zadd,zpopmin,lrange_100,mset";
    let out = Command::new(bin)
        .args([
            "-p",
            &s.port.to_string(),
            "-n",
            "2000",
            "-c",
            "10",
            "-q",
            "-t",
            tests,
            "-P",
            "4",
        ])
        .output()
        .unwrap();
    let text = String::from_utf8_lossy(&out.stdout).replace('\r', "\n");
    assert!(out.status.success(), "{text}");
    for t in [
        "PING_INLINE",
        "SET",
        "GET",
        "INCR",
        "LPUSH",
        "RPUSH",
        "LPOP",
        "RPOP",
        "SADD",
        "HSET",
        "SPOP",
        "ZADD",
        "ZPOPMIN",
        "LRANGE_100",
        "MSET",
    ] {
        assert!(
            text.lines()
                .any(|l| l.starts_with(t) && l.contains("requests per second")),
            "missing result for {t}:\n{text}"
        );
    }
}
