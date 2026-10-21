//! Replication: full sync, streaming, read-only replicas, partial resync from
//! the backlog after a disconnect, full resync when the backlog is exceeded,
//! WAIT, INFO replication and promotion.

mod common;

use common::{Client, Reply, Server, info_field, wait_until};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

fn dump(c: &mut Client) -> Vec<String> {
    let mut out = Vec::new();
    for db in 0..3 {
        c.cmd(&["SELECT", &db.to_string()]);
        let mut keys = c.cmd(&["KEYS", "*"]).strings();
        keys.sort();
        for k in keys {
            let t = c.cmd(&["TYPE", &k]).text();
            let v = match t.as_str() {
                "string" => c.cmd(&["GET", &k]).flat(),
                "list" => c.cmd(&["LRANGE", &k, "0", "-1"]).flat(),
                "set" => {
                    let mut m = c.cmd(&["SMEMBERS", &k]).strings();
                    m.sort();
                    format!("{m:?}")
                }
                "zset" => c.cmd(&["ZRANGE", &k, "0", "-1", "WITHSCORES"]).flat(),
                "hash" => {
                    let v = c.cmd(&["HGETALL", &k]).strings();
                    let mut p: Vec<_> = v.chunks(2).map(|x| format!("{}={}", x[0], x[1])).collect();
                    p.sort();
                    format!("{p:?}")
                }
                "stream" => c.cmd(&["XRANGE", &k, "-", "+"]).flat(),
                _ => String::new(),
            };
            out.push(format!("db{db} {k} {t} {v}"));
        }
    }
    c.cmd(&["SELECT", "0"]);
    out
}

fn info(c: &mut Client, field: &str) -> String {
    let text = c.cmd(&["INFO", "all"]).text();
    info_field(&text, field).unwrap_or_default()
}

fn wait_link_up(replica: &mut Client) {
    assert!(
        wait_until(Duration::from_secs(20), || info(
            replica,
            "master_link_status"
        ) == "up"),
        "replica never connected"
    );
}

fn wait_converged(master: &mut Client, replica: &mut Client) {
    let mut last = (Vec::new(), Vec::new());
    let ok = wait_until(Duration::from_secs(20), || {
        last = (dump(master), dump(replica));
        last.0 == last.1
    });
    assert!(
        ok,
        "replica did not converge:\nmaster: {:?}\nreplica: {:?}",
        last.0.len(),
        last.1.len()
    );
}

fn write_mix(c: &mut Client, tag: &str) {
    c.cmd(&["SET", &format!("{tag}:str"), "v"]);
    c.cmd(&["INCRBY", &format!("{tag}:ctr"), "5"]);
    c.cmd(&["RPUSH", &format!("{tag}:list"), "a", "b", "c"]);
    c.cmd(&["HSET", &format!("{tag}:hash"), "f", "v", "g", "w"]);
    c.cmd(&["SADD", &format!("{tag}:set"), "x", "y", "1"]);
    c.cmd(&["ZADD", &format!("{tag}:zset"), "1", "a", "2.5", "b"]);
    c.cmd(&["XADD", &format!("{tag}:stream"), "*", "f", "v"]);
    c.cmd(&["SET", &format!("{tag}:ttl"), "v", "EX", "1000"]);
    c.cmd(&["INCRBYFLOAT", &format!("{tag}:float"), "1.25"]);
    c.cmd(&["SPOP", &format!("{tag}:set")]);
    c.cmd(&["SELECT", "1"]);
    c.cmd(&["SET", &format!("{tag}:db1"), "one"]);
    c.cmd(&["SELECT", "2"]);
    c.cmd(&["LPUSH", &format!("{tag}:db2"), "two"]);
    c.cmd(&["SELECT", "0"]);
    c.cmd(&["MULTI"]);
    c.cmd(&["INCR", &format!("{tag}:tx")]);
    c.cmd(&["INCR", &format!("{tag}:tx")]);
    c.cmd(&["EXEC"]);
    c.cmd(&["DEL", &format!("{tag}:str")]);
}

#[test]
fn full_sync_then_streaming() {
    let master = Server::start(&[]);
    let mut m = master.client();
    write_mix(&mut m, "pre");
    for i in 0..5000 {
        m.send(&["SET", &format!("bulk:{i}"), &i.to_string()]);
    }
    for _ in 0..5000 {
        m.read().unwrap();
    }
    let replica = Server::start(&["--replicaof", "127.0.0.1", &master.port.to_string()]);
    let mut r = replica.client();
    wait_link_up(&mut r);
    wait_converged(&mut m, &mut r);
    assert_eq!(info(&mut r, "role"), "slave");
    // Streaming after the sync, including blocking pops served on the master.
    let mut blocked = master.client();
    blocked.send(&["BLPOP", "jobs", "5"]);
    std::thread::sleep(Duration::from_millis(50));
    write_mix(&mut m, "post");
    m.cmd(&["RPUSH", "jobs", "j1", "j2"]);
    assert_eq!(blocked.read().unwrap().strings(), vec!["jobs", "j1"]);
    wait_converged(&mut m, &mut r);
    assert_eq!(r.cmd(&["LRANGE", "jobs", "0", "-1"]).strings(), vec!["j2"]);
    // Offsets match once the replica has caught up.
    assert!(wait_until(Duration::from_secs(10), || {
        info(&mut m, "master_repl_offset") == info(&mut r, "slave_repl_offset")
    }));
    let slave0 = info(&mut m, "slave0");
    assert!(slave0.contains("state=online"), "{slave0}");
    assert_eq!(info(&mut m, "connected_slaves"), "1");
}

#[test]
fn replica_rejects_writes_and_serves_reads() {
    let master = Server::start(&[]);
    let mut m = master.client();
    m.cmd(&["SET", "k", "v"]);
    let replica = Server::start(&[]);
    let mut r = replica.client();
    assert_eq!(
        r.cmd(&["REPLICAOF", "127.0.0.1", &master.port.to_string()]),
        Reply::Simple("OK".into())
    );
    wait_link_up(&mut r);
    wait_converged(&mut m, &mut r);
    let err = r.cmd(&["SET", "k", "other"]);
    assert_eq!(
        err,
        Reply::Error("READONLY You can't write against a read only replica.".into())
    );
    assert_eq!(r.cmd(&["GET", "k"]).text(), "v");
    assert_eq!(r.cmd(&["ROLE"]).array()[0].text(), "slave");
    // Promotion keeps the data and accepts writes.
    assert_eq!(
        r.cmd(&["REPLICAOF", "NO", "ONE"]),
        Reply::Simple("OK".into())
    );
    assert_eq!(r.cmd(&["SET", "k", "promoted"]), Reply::Simple("OK".into()));
    assert_eq!(info(&mut r, "role"), "master");
    assert_eq!(m.cmd(&["GET", "k"]).text(), "v");
}

#[test]
fn partial_resync_after_short_disconnect() {
    let master = Server::start(&["--repl-backlog-size", "10mb"]);
    let mut m = master.client();
    write_mix(&mut m, "a");
    let replica = Server::start(&["--replicaof", "127.0.0.1", &master.port.to_string()]);
    let mut r = replica.client();
    wait_link_up(&mut r);
    wait_converged(&mut m, &mut r);
    assert_eq!(info(&mut m, "sync_full"), "1");
    // Drop the replication link from the master side and write meanwhile.
    assert_eq!(m.cmd(&["CLIENT", "KILL", "TYPE", "replica"]), Reply::Int(1));
    for i in 0..2000 {
        m.send(&["SET", &format!("during:{i}"), "v"]);
    }
    for _ in 0..2000 {
        m.read().unwrap();
    }
    write_mix(&mut m, "b");
    wait_converged(&mut m, &mut r);
    assert!(wait_until(Duration::from_secs(10), || info(
        &mut m,
        "sync_partial_ok"
    ) == "1"));
    assert_eq!(
        info(&mut m, "sync_full"),
        "1",
        "reconnect must not trigger a full resync"
    );
}

#[test]
fn full_resync_when_backlog_is_exceeded() {
    let master = Server::start(&["--repl-backlog-size", "16kb"]);
    let mut m = master.client();
    m.cmd(&["SET", "seed", "1"]);
    let replica = Server::start(&["--replicaof", "127.0.0.1", &master.port.to_string()]);
    let mut r = replica.client();
    wait_link_up(&mut r);
    wait_converged(&mut m, &mut r);
    m.cmd(&["CLIENT", "KILL", "TYPE", "replica"]);
    let big = "z".repeat(1000);
    for i in 0..200 {
        m.cmd(&["SET", &format!("big:{i}"), &big]); // ~200 KB ≫ 16 KB backlog
    }
    wait_converged(&mut m, &mut r);
    assert!(wait_until(Duration::from_secs(10), || info(
        &mut m,
        "sync_full"
    ) == "2"));
}

#[test]
fn sync_while_master_is_under_write_load() {
    let master = Server::start(&[]);
    let port = master.port;
    let stop = Arc::new(AtomicBool::new(false));
    let writers: Vec<_> = (0..4)
        .map(|t| {
            let stop = stop.clone();
            std::thread::spawn(move || {
                let mut c = Client::connect(port);
                let mut i = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    c.cmd(&["INCR", &format!("ctr:{t}")]);
                    c.cmd(&["RPUSH", &format!("log:{t}"), &i.to_string()]);
                    c.cmd(&["SET", &format!("k:{t}:{}", i % 500), &i.to_string()]);
                    i += 1;
                }
            })
        })
        .collect();
    std::thread::sleep(Duration::from_millis(200));
    let replica = Server::start(&["--replicaof", "127.0.0.1", &port.to_string()]);
    let mut r = replica.client();
    wait_link_up(&mut r);
    std::thread::sleep(Duration::from_millis(300));
    stop.store(true, Ordering::Relaxed);
    for w in writers {
        w.join().unwrap();
    }
    let mut m = master.client();
    wait_converged(&mut m, &mut r);
    // WAIT confirms the replica acknowledged everything.
    m.cmd(&["SET", "final", "1"]);
    assert_eq!(m.cmd(&["WAIT", "1", "5000"]), Reply::Int(1));
    assert_eq!(r.cmd(&["GET", "final"]).text(), "1");
}

#[test]
fn expiry_is_driven_by_the_master() {
    let master = Server::start(&["--hz", "50"]);
    let mut m = master.client();
    let replica = Server::start(&["--replicaof", "127.0.0.1", &master.port.to_string()]);
    let mut r = replica.client();
    wait_link_up(&mut r);
    m.cmd(&["SET", "temp", "v", "PX", "300"]);
    m.cmd(&["SET", "perm", "v"]);
    assert!(wait_until(Duration::from_secs(5), || r
        .cmd(&["GET", "temp"])
        .text()
        == "v"));
    std::thread::sleep(Duration::from_millis(400));
    // Logically expired on the replica even before the master's DEL arrives.
    assert_eq!(r.cmd(&["GET", "temp"]), Reply::Nil);
    // The master's active expiry deletes it and replicates a DEL.
    assert!(wait_until(Duration::from_secs(5), || r.cmd(&["DBSIZE"])
        == Reply::Int(1)));
}
