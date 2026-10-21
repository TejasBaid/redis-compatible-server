//! End-to-end tests over TCP against the server binary on a random port.

mod common;

use common::{Client, Reply, Server, info_field, wait_until};
use std::collections::HashSet;
use std::time::{Duration, Instant};

fn ok() -> Reply {
    Reply::Simple("OK".into())
}

fn bulk(s: &str) -> Reply {
    Reply::Bulk(s.as_bytes().to_vec())
}

#[test]
fn pipelining_many_commands_in_one_write() {
    let s = Server::start(&[]);
    let mut c = s.client();
    let mut raw = Vec::new();
    for i in 0..5000 {
        raw.extend_from_slice(
            format!("*3\r\n$3\r\nSET\r\n$5\r\nk{i:04}\r\n$1\r\nv\r\n").as_bytes(),
        );
    }
    raw.extend_from_slice(b"*1\r\n$6\r\nDBSIZE\r\n");
    c.send_raw(&raw);
    for _ in 0..5000 {
        assert_eq!(c.read().unwrap(), ok());
    }
    assert_eq!(c.read().unwrap(), Reply::Int(5000));
}

#[test]
fn inline_commands_and_quoting() {
    let s = Server::start(&[]);
    let mut c = s.client();
    c.send_raw(
        b"SET greeting \"hello world\"\r\nGET greeting\r\nPING\nECHO 'single \\' quote'\r\n",
    );
    assert_eq!(c.read().unwrap(), ok());
    assert_eq!(c.read().unwrap(), bulk("hello world"));
    assert_eq!(c.read().unwrap(), Reply::Simple("PONG".into()));
    assert_eq!(c.read().unwrap(), bulk("single ' quote"));
}

#[test]
fn request_split_across_many_writes() {
    let s = Server::start(&[]);
    let mut c = s.client();
    let payload = vec![b'x'; 300_000];
    let mut raw = format!("*3\r\n$3\r\nSET\r\n$3\r\nbig\r\n${}\r\n", payload.len()).into_bytes();
    raw.extend_from_slice(&payload);
    raw.extend_from_slice(b"\r\n");
    for chunk in raw.chunks(7919) {
        c.send_raw(chunk);
        std::thread::sleep(Duration::from_micros(200));
    }
    assert_eq!(c.read().unwrap(), ok());
    assert_eq!(c.cmd(&["STRLEN", "big"]), Reply::Int(300_000));
}

#[test]
fn protocol_error_closes_connection() {
    let s = Server::start(&[]);
    let mut c = s.client();
    c.send_raw(b"*1\r\n+bad\r\n");
    match c.read().unwrap() {
        Reply::Error(e) => assert!(e.starts_with("ERR Protocol error"), "{e}"),
        other => panic!("{other:?}"),
    }
    assert!(c.read().is_err(), "connection should be closed");
}

#[test]
fn resp3_hello_and_types() {
    let s = Server::start(&[]);
    let mut c = s.client();
    let hello = c.cmd(&["HELLO", "3"]);
    let Reply::Map(fields) = hello else {
        panic!("{hello:?}")
    };
    assert!(fields.contains(&(bulk("proto"), Reply::Int(3))));
    c.cmd(&["HSET", "h", "a", "1"]);
    assert!(matches!(c.cmd(&["HGETALL", "h"]), Reply::Map(_)));
    c.cmd(&["SADD", "s", "x"]);
    assert!(matches!(c.cmd(&["SMEMBERS", "s"]), Reply::Set(_)));
    c.cmd(&["ZADD", "z", "1.5", "m"]);
    assert_eq!(c.cmd(&["ZSCORE", "z", "m"]), Reply::Double("1.5".into()));
    assert_eq!(c.cmd(&["GET", "missing"]), Reply::Nil);
    assert!(matches!(c.cmd(&["INFO", "server"]), Reply::Verbatim(_)));
    assert_eq!(
        c.cmd(&["HELLO", "4"]),
        Reply::Error("NOPROTO unsupported protocol version".into())
    );
    let back = c.cmd(&["HELLO", "2"]);
    assert!(matches!(back, Reply::Array(_)));
    assert_eq!(c.cmd(&["GET", "missing"]), Reply::Nil);
}

#[test]
fn sixteen_databases_and_select() {
    let s = Server::start(&[]);
    let mut c = s.client();
    for db in 0..16 {
        assert_eq!(c.cmd(&["SELECT", &db.to_string()]), ok());
        c.cmd(&["SET", "k", &format!("db{db}")]);
    }
    for db in 0..16 {
        c.cmd(&["SELECT", &db.to_string()]);
        assert_eq!(c.cmd(&["GET", "k"]), bulk(&format!("db{db}")));
        assert_eq!(c.cmd(&["DBSIZE"]), Reply::Int(1));
    }
    assert!(c.cmd(&["SELECT", "16"]).is_error());
    c.cmd(&["SELECT", "3"]);
    assert_eq!(c.cmd(&["MOVE", "k", "4"]), Reply::Int(0)); // exists in db4
    c.cmd(&["SET", "only3", "v"]);
    assert_eq!(c.cmd(&["MOVE", "only3", "4"]), Reply::Int(1));
    c.cmd(&["SELECT", "4"]);
    assert_eq!(c.cmd(&["GET", "only3"]), bulk("v"));
    assert_eq!(c.cmd(&["SWAPDB", "4", "5"]), ok());
    assert_eq!(c.cmd(&["GET", "only3"]), Reply::Nil);
    c.cmd(&["SELECT", "5"]);
    assert_eq!(c.cmd(&["GET", "only3"]), bulk("v"));
    c.cmd(&["FLUSHDB"]);
    assert_eq!(c.cmd(&["DBSIZE"]), Reply::Int(0));
    c.cmd(&["SELECT", "0"]);
    assert_eq!(c.cmd(&["DBSIZE"]), Reply::Int(1));
    c.cmd(&["FLUSHALL"]);
    for db in 0..16 {
        c.cmd(&["SELECT", &db.to_string()]);
        assert_eq!(c.cmd(&["DBSIZE"]), Reply::Int(0));
    }
}

#[test]
fn scan_returns_every_key_with_match_count_and_type() {
    let s = Server::start(&[]);
    let mut c = s.client();
    for i in 0..3000 {
        c.send(&["SET", &format!("str:{i}"), "v"]);
    }
    for i in 0..500 {
        c.send(&["RPUSH", &format!("list:{i}"), "v"]);
    }
    for _ in 0..3500 {
        c.read().unwrap();
    }
    let scan_all = |c: &mut Client, extra: &[&str]| {
        let mut seen = HashSet::new();
        let mut cursor = "0".to_string();
        let mut calls = 0;
        loop {
            let mut args = vec!["SCAN", cursor.as_str()];
            args.extend_from_slice(extra);
            let r = c.cmd(&args).array();
            cursor = r[0].text();
            for k in r[1].strings() {
                seen.insert(k);
            }
            calls += 1;
            if cursor == "0" {
                break;
            }
        }
        (seen, calls)
    };
    let (all, calls) = scan_all(&mut c, &[]);
    assert_eq!(all.len(), 3500);
    assert!(calls > 10, "SCAN should paginate (calls = {calls})");
    let (lists, _) = scan_all(&mut c, &["TYPE", "list", "COUNT", "100"]);
    assert_eq!(lists.len(), 500);
    assert!(lists.iter().all(|k| k.starts_with("list:")));
    let (matched, _) = scan_all(&mut c, &["MATCH", "str:1??", "COUNT", "1000"]);
    assert_eq!(matched.len(), 100);
    assert_eq!(c.cmd(&["KEYS", "list:4??"]).array().len(), 100);
    assert!(c.cmd(&["SCAN", "abc"]).is_error());
}

#[test]
fn scan_is_complete_while_keys_are_added_and_deleted() {
    let s = Server::start(&[]);
    let mut c = s.client();
    let mut writer = s.client();
    for i in 0..2000 {
        c.send(&["SET", &format!("stable:{i}"), "v"]);
    }
    for _ in 0..2000 {
        c.read().unwrap();
    }
    let mut seen = HashSet::new();
    let mut cursor = "0".to_string();
    let mut n = 0;
    loop {
        let r = c.cmd(&["SCAN", &cursor, "COUNT", "50"]).array();
        cursor = r[0].text();
        for k in r[1].strings() {
            seen.insert(k);
        }
        // Churn: grow and shrink the keyspace between calls (forces rehash).
        for j in 0..40 {
            writer.cmd(&["SET", &format!("churn:{n}:{j}"), "v"]);
        }
        if n % 3 == 0 {
            writer.cmd(&["DEL", &format!("churn:{}:0", n.max(1) - 1)]);
        }
        n += 1;
        if cursor == "0" {
            break;
        }
    }
    for i in 0..2000 {
        assert!(
            seen.contains(&format!("stable:{i}")),
            "SCAN missed stable:{i}"
        );
    }
}

#[test]
fn lazy_and_active_expiry() {
    let s = Server::start(&["--hz", "50"]);
    let mut c = s.client();
    c.cmd(&["SET", "lazy", "v", "PX", "100"]);
    assert_eq!(c.cmd(&["GET", "lazy"]), bulk("v"));
    std::thread::sleep(Duration::from_millis(150));
    assert_eq!(c.cmd(&["GET", "lazy"]), Reply::Nil);
    assert_eq!(c.cmd(&["TTL", "lazy"]), Reply::Int(-2));
    // Active expiry removes keys nobody touches again.
    for i in 0..2000 {
        c.send(&["SET", &format!("tmp:{i}"), "v", "PX", "50"]);
    }
    for i in 0..100 {
        c.send(&["SET", &format!("keep:{i}"), "v"]);
    }
    for _ in 0..2100 {
        c.read().unwrap();
    }
    let ok = wait_until(Duration::from_secs(10), || {
        c.cmd(&["DBSIZE"]) == Reply::Int(100)
    });
    assert!(ok, "active expiry left {:?} keys", c.cmd(&["DBSIZE"]));
    let info = c.cmd(&["INFO", "stats"]).text();
    let active: u64 = info_field(&info, "expired_keys_active")
        .unwrap()
        .parse()
        .unwrap();
    assert!(
        active > 1500,
        "most keys should be reclaimed by the active cycle, got {active}"
    );
}

#[test]
fn ttl_family() {
    let s = Server::start(&[]);
    let mut c = s.client();
    c.cmd(&["SET", "k", "v"]);
    assert_eq!(c.cmd(&["TTL", "k"]), Reply::Int(-1));
    assert_eq!(c.cmd(&["EXPIRE", "k", "100"]), Reply::Int(1));
    assert_eq!(c.cmd(&["TTL", "k"]), Reply::Int(100));
    assert_eq!(c.cmd(&["EXPIRE", "k", "50", "GT"]), Reply::Int(0));
    assert_eq!(c.cmd(&["EXPIRE", "k", "50", "LT"]), Reply::Int(1));
    assert!(c.cmd(&["PTTL", "k"]).int() <= 50_000);
    assert_eq!(c.cmd(&["PERSIST", "k"]), Reply::Int(1));
    assert_eq!(c.cmd(&["TTL", "k"]), Reply::Int(-1));
    assert_eq!(c.cmd(&["SET", "k", "v2", "EX", "10", "GET"]), bulk("v"));
    assert_eq!(c.cmd(&["SET", "k", "v3", "KEEPTTL"]), ok());
    assert!(c.cmd(&["TTL", "k"]).int() > 0);
    assert_eq!(c.cmd(&["SET", "k", "v4"]), ok());
    assert_eq!(c.cmd(&["TTL", "k"]), Reply::Int(-1));
    let at = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 1000)
        .to_string();
    c.cmd(&["EXPIREAT", "k", &at]);
    assert_eq!(c.cmd(&["EXPIRETIME", "k"]).int().to_string(), at);
    c.cmd(&["EXPIRE", "k", "-1"]);
    assert_eq!(c.cmd(&["EXISTS", "k"]), Reply::Int(0));
}

#[test]
fn blocking_pop_is_woken_by_push_from_another_client() {
    let s = Server::start(&[]);
    let mut waiter = s.client();
    let mut pusher = s.client();
    waiter.send(&["BLPOP", "q1", "q2", "5"]);
    std::thread::sleep(Duration::from_millis(100));
    let start = Instant::now();
    assert_eq!(pusher.cmd(&["RPUSH", "q2", "job"]), Reply::Int(1));
    assert_eq!(
        waiter.read().unwrap(),
        Reply::Array(vec![bulk("q2"), bulk("job")])
    );
    assert!(start.elapsed() < Duration::from_secs(1));
    // The element was consumed by the waiter.
    assert_eq!(pusher.cmd(&["LLEN", "q2"]), Reply::Int(0));
    // The waiter's connection keeps working (pipelined commands after BLPOP).
    waiter.send(&["BLPOP", "q3", "5"]);
    waiter.send(&["PING"]);
    std::thread::sleep(Duration::from_millis(50));
    pusher.cmd(&["LPUSH", "q3", "x"]);
    assert_eq!(
        waiter.read().unwrap(),
        Reply::Array(vec![bulk("q3"), bulk("x")])
    );
    assert_eq!(waiter.read().unwrap(), Reply::Simple("PONG".into()));
}

#[test]
fn blocking_pop_timeout_and_fifo_order() {
    let s = Server::start(&[]);
    let mut c = s.client();
    let start = Instant::now();
    assert_eq!(c.cmd(&["BLPOP", "nothing", "0.2"]), Reply::NilArray);
    let el = start.elapsed();
    assert!(
        el >= Duration::from_millis(190) && el < Duration::from_secs(2),
        "{el:?}"
    );
    assert_eq!(
        c.cmd(&["BLMOVE", "nothing", "dst", "LEFT", "RIGHT", "0.1"]),
        Reply::Nil
    );
    // Three waiters are served in the order they blocked.
    let mut waiters: Vec<Client> = (0..3).map(|_| s.client()).collect();
    for w in waiters.iter_mut() {
        w.send(&["BRPOP", "fifo", "5"]);
        std::thread::sleep(Duration::from_millis(30));
    }
    c.cmd(&["RPUSH", "fifo", "a", "b", "c"]);
    // RPUSH a b c then BRPOP pops from the tail: first waiter gets "c".
    let got: Vec<Reply> = waiters.iter_mut().map(|w| w.read().unwrap()).collect();
    assert_eq!(got[0], Reply::Array(vec![bulk("fifo"), bulk("c")]));
    assert_eq!(got[1], Reply::Array(vec![bulk("fifo"), bulk("b")]));
    assert_eq!(got[2], Reply::Array(vec![bulk("fifo"), bulk("a")]));
}

#[test]
fn blmove_chain_and_bzpop_and_xread_block() {
    let s = Server::start(&[]);
    let mut a = s.client();
    let mut b = s.client();
    let mut p = s.client();
    // b waits on "mid", a moves from "src" into "mid" when src gets data.
    b.send(&["BLPOP", "mid", "5"]);
    a.send(&["BLMOVE", "src", "mid", "LEFT", "RIGHT", "5"]);
    std::thread::sleep(Duration::from_millis(100));
    p.cmd(&["RPUSH", "src", "token"]);
    assert_eq!(a.read().unwrap(), bulk("token"));
    assert_eq!(
        b.read().unwrap(),
        Reply::Array(vec![bulk("mid"), bulk("token")])
    );
    // BZPOPMIN
    a.send(&["BZPOPMIN", "zq", "5"]);
    std::thread::sleep(Duration::from_millis(50));
    p.cmd(&["ZADD", "zq", "3", "x", "1", "y"]);
    assert_eq!(
        a.read().unwrap(),
        Reply::Array(vec![bulk("zq"), bulk("y"), bulk("1")])
    );
    // XREAD BLOCK with $
    p.cmd(&["XADD", "stream", "1-1", "f", "old"]);
    a.send(&["XREAD", "BLOCK", "5000", "STREAMS", "stream", "$"]);
    std::thread::sleep(Duration::from_millis(50));
    p.cmd(&["XADD", "stream", "2-1", "f", "new"]);
    let txt = a.read().unwrap().flat();
    assert_eq!(txt, "[[stream, [[2-1, [f, new]]]]]");
    // Blocking command inside MULTI does not block.
    a.cmd(&["MULTI"]);
    a.cmd(&["BLPOP", "empty", "0"]);
    assert_eq!(a.cmd(&["EXEC"]), Reply::Array(vec![Reply::NilArray]));
}

#[test]
fn transactions_and_watch() {
    let s = Server::start(&[]);
    let mut c = s.client();
    let mut other = s.client();
    assert_eq!(c.cmd(&["MULTI"]), ok());
    assert_eq!(c.cmd(&["SET", "a", "1"]), Reply::Simple("QUEUED".into()));
    assert_eq!(c.cmd(&["INCR", "a"]), Reply::Simple("QUEUED".into()));
    assert_eq!(c.cmd(&["EXEC"]), Reply::Array(vec![ok(), Reply::Int(2)]));
    // WATCH + concurrent modification aborts.
    c.cmd(&["WATCH", "a"]);
    other.cmd(&["SET", "a", "changed"]);
    c.cmd(&["MULTI"]);
    c.cmd(&["SET", "a", "mine"]);
    assert_eq!(c.cmd(&["EXEC"]), Reply::NilArray);
    assert_eq!(c.cmd(&["GET", "a"]), bulk("changed"));
    // WATCH without interference succeeds; UNWATCH clears.
    c.cmd(&["WATCH", "a"]);
    c.cmd(&["MULTI"]);
    c.cmd(&["SET", "a", "mine"]);
    assert_eq!(c.cmd(&["EXEC"]), Reply::Array(vec![ok()]));
    c.cmd(&["WATCH", "a"]);
    c.cmd(&["UNWATCH"]);
    other.cmd(&["SET", "a", "x"]);
    c.cmd(&["MULTI"]);
    c.cmd(&["GET", "a"]);
    assert_eq!(c.cmd(&["EXEC"]), Reply::Array(vec![bulk("x")]));
    // FLUSHALL touches watched keys that existed.
    c.cmd(&["WATCH", "a"]);
    other.cmd(&["FLUSHALL"]);
    c.cmd(&["MULTI"]);
    c.cmd(&["PING"]);
    assert_eq!(c.cmd(&["EXEC"]), Reply::NilArray);
    // Queue-time errors abort with EXECABORT.
    c.cmd(&["MULTI"]);
    assert!(c.cmd(&["NOSUCHCMD"]).is_error());
    assert!(c.cmd(&["EXEC"]).text().starts_with("EXECABORT"));
    // A watched key that expires aborts EXEC.
    c.cmd(&["SET", "e", "v", "PX", "50"]);
    c.cmd(&["WATCH", "e"]);
    std::thread::sleep(Duration::from_millis(100));
    c.cmd(&["MULTI"]);
    c.cmd(&["PING"]);
    assert_eq!(c.cmd(&["EXEC"]), Reply::NilArray);
}

#[test]
fn transactions_are_atomic_under_concurrency() {
    let s = Server::start(&[]);
    let port = s.port;
    // Each transaction moves one unit between two keys in different shards;
    // concurrent readers (also using MULTI) must always see the sum intact.
    let mut setup = s.client();
    setup.cmd(&["MSET", "acct:a", "1000", "acct:b", "1000"]);
    let writers: Vec<_> = (0..4u32)
        .map(|i| {
            std::thread::spawn(move || {
                let mut c = Client::connect(port);
                for _ in 0..500 {
                    c.cmd(&["MULTI"]);
                    let (from, to) = if i.is_multiple_of(2) {
                        ("acct:a", "acct:b")
                    } else {
                        ("acct:b", "acct:a")
                    };
                    c.cmd(&["DECR", from]);
                    c.cmd(&["INCR", to]);
                    assert_eq!(c.cmd(&["EXEC"]).array().len(), 2);
                }
            })
        })
        .collect();
    let reader = std::thread::spawn(move || {
        let mut c = Client::connect(port);
        for _ in 0..500 {
            let v = c.cmd(&["MGET", "acct:a", "acct:b"]).strings();
            let sum: i64 = v.iter().map(|x| x.parse::<i64>().unwrap()).sum();
            assert_eq!(sum, 2000, "MGET observed a torn transaction: {v:?}");
        }
    });
    for w in writers {
        w.join().unwrap();
    }
    reader.join().unwrap();
    let v = setup.cmd(&["MGET", "acct:a", "acct:b"]).strings();
    assert_eq!(v, vec!["1000", "1000"]);
}

#[test]
fn concurrent_increments_are_exact() {
    let s = Server::start(&["--threads", "8"]);
    let port = s.port;
    let threads: Vec<_> = (0..16)
        .map(|_| {
            std::thread::spawn(move || {
                let mut c = Client::connect(port);
                // Pipeline 100 commands at a time for throughput.
                for _ in 0..10 {
                    for _ in 0..100 {
                        c.send(&["INCR", "counter"]);
                        c.send(&["HINCRBY", "h", "f", "2"]);
                        c.send(&["RPUSH", "list", "x"]);
                        c.send(&["SADD", "set", "member"]);
                    }
                    for _ in 0..400 {
                        assert!(!c.read().unwrap().is_error());
                    }
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    let mut c = s.client();
    assert_eq!(c.cmd(&["GET", "counter"]), bulk("16000"));
    assert_eq!(c.cmd(&["HGET", "h", "f"]), bulk("32000"));
    assert_eq!(c.cmd(&["LLEN", "list"]), Reply::Int(16000));
    assert_eq!(c.cmd(&["SCARD", "set"]), Reply::Int(1));
}

#[test]
fn pubsub_channels_patterns_and_resp3_push() {
    let s = Server::start(&[]);
    let mut sub = s.client();
    let mut psub = s.client();
    let mut publ = s.client();
    assert_eq!(
        sub.cmd(&["SUBSCRIBE", "news", "sport"]),
        Reply::Array(vec![bulk("subscribe"), bulk("news"), Reply::Int(1)])
    );
    assert_eq!(
        sub.read().unwrap(),
        Reply::Array(vec![bulk("subscribe"), bulk("sport"), Reply::Int(2)])
    );
    psub.cmd(&["HELLO", "3"]);
    assert_eq!(
        psub.cmd(&["PSUBSCRIBE", "n*"]),
        Reply::Push(vec![bulk("psubscribe"), bulk("n*"), Reply::Int(1)])
    );
    assert_eq!(publ.cmd(&["PUBLISH", "news", "hello"]), Reply::Int(2));
    assert_eq!(
        sub.read().unwrap(),
        Reply::Array(vec![bulk("message"), bulk("news"), bulk("hello")])
    );
    assert_eq!(
        psub.read().unwrap(),
        Reply::Push(vec![
            bulk("pmessage"),
            bulk("n*"),
            bulk("news"),
            bulk("hello")
        ])
    );
    // RESP2 subscribers can only run pub/sub commands.
    assert!(
        sub.cmd(&["GET", "x"])
            .text()
            .contains("only (P|S)SUBSCRIBE")
    );
    assert_eq!(
        sub.cmd(&["PING"]),
        Reply::Array(vec![bulk("pong"), bulk("")])
    );
    // RESP3 subscribers can run normal commands.
    assert_eq!(psub.cmd(&["SET", "x", "1"]), ok());
    assert_eq!(
        publ.cmd(&["PUBSUB", "NUMSUB", "news", "none"]).array(),
        vec![bulk("news"), Reply::Int(1), bulk("none"), Reply::Int(0)]
    );
    assert_eq!(publ.cmd(&["PUBSUB", "NUMPAT"]), Reply::Int(1));
    assert_eq!(
        sub.cmd(&["UNSUBSCRIBE", "news"]),
        Reply::Array(vec![bulk("unsubscribe"), bulk("news"), Reply::Int(1)])
    );
    assert_eq!(publ.cmd(&["PUBLISH", "news", "again"]), Reply::Int(1));
    sub.cmd(&["UNSUBSCRIBE"]);
    assert_eq!(sub.cmd(&["GET", "x"]), bulk("1"));
}

#[test]
fn maxmemory_noeviction_rejects_writes() {
    let s = Server::start(&["--maxmemory", "20mb", "--maxmemory-policy", "noeviction"]);
    let mut c = s.client();
    let val = "x".repeat(10_000);
    let mut oom = false;
    for i in 0..5000 {
        if let Reply::Error(e) = c.cmd(&["SET", &format!("k{i}"), &val]) {
            assert!(e.starts_with("OOM"), "{e}");
            oom = true;
            break;
        }
    }
    assert!(oom, "writes must eventually be rejected");
    // Reads and deletes still work.
    assert_eq!(c.cmd(&["GET", "k0"]).text().len(), 10_000);
    assert_eq!(c.cmd(&["DEL", "k0"]), Reply::Int(1));
}

#[test]
fn maxmemory_lru_evicts_cold_keys() {
    let s = Server::start(&["--maxmemory", "30mb", "--maxmemory-policy", "allkeys-lru"]);
    let mut c = s.client();
    let val = "x".repeat(1000);
    for i in 0..1000 {
        c.send(&["SET", &format!("hot:{i}"), &val]);
    }
    for _ in 0..1000 {
        c.read().unwrap();
    }
    // The LRU clock has 1 s resolution (like Redis), so cold keys are written
    // in phases at least a second apart while the hot set is re-read each time.
    for phase in 0..6 {
        std::thread::sleep(Duration::from_millis(1100));
        for i in 0..1000 {
            c.send(&["GET", &format!("hot:{i}")]);
        }
        for i in 0..8000 {
            c.send(&["SET", &format!("cold:{phase}:{i}"), &val]);
        }
        for _ in 0..9000 {
            assert!(!c.read().unwrap().is_error());
        }
    }
    let info = c.cmd(&["INFO", "all"]).text();
    let evicted: u64 = info_field(&info, "evicted_keys").unwrap().parse().unwrap();
    let used: u64 = info_field(&info, "used_memory").unwrap().parse().unwrap();
    assert!(evicted > 10_000, "evicted {evicted}");
    assert!(
        used < 34 * 1024 * 1024,
        "used_memory {used} must stay near the limit"
    );
    let mut hot_alive = 0;
    for i in 0..1000 {
        if c.cmd(&["EXISTS", &format!("hot:{i}")]) == Reply::Int(1) {
            hot_alive += 1;
        }
    }
    assert!(
        hot_alive >= 950,
        "approximated LRU should keep the hot set, kept {hot_alive}/1000"
    );
}

#[test]
fn maxmemory_lfu_and_volatile_policies() {
    let s = Server::start(&["--maxmemory", "20mb", "--maxmemory-policy", "volatile-ttl"]);
    let mut c = s.client();
    let val = "y".repeat(1000);
    for i in 0..500 {
        c.cmd(&["SET", &format!("persistent:{i}"), &val]);
    }
    for i in 0..40_000 {
        c.send(&["SET", &format!("v:{i}"), &val, "EX", "10000"]);
        if i % 1000 == 999 {
            for _ in 0..1000 {
                c.read().unwrap();
            }
        }
    }
    for i in 0..500 {
        assert_eq!(
            c.cmd(&["EXISTS", &format!("persistent:{i}")]),
            Reply::Int(1),
            "volatile policy evicted a key without TTL"
        );
    }
    assert_eq!(
        c.cmd(&["CONFIG", "SET", "maxmemory-policy", "allkeys-lfu"]),
        ok()
    );
    c.cmd(&["SET", "freq", "v"]);
    for _ in 0..100 {
        c.cmd(&["GET", "freq"]);
    }
    assert!(c.cmd(&["OBJECT", "FREQ", "freq"]).int() > 5);
    assert!(c.cmd(&["OBJECT", "IDLETIME", "freq"]).is_error());
}

#[test]
fn admin_commands() {
    let s = Server::start(&["--slowlog-log-slower-than", "50000"]);
    let mut c = s.client();
    let info = c.cmd(&["INFO"]).text();
    for section in [
        "# Server",
        "# Clients",
        "# Memory",
        "# Persistence",
        "# Stats",
        "# Replication",
        "# Keyspace",
    ] {
        assert!(info.contains(section), "missing {section}");
    }
    assert_eq!(info_field(&info, "role").unwrap(), "master");
    let cfg = c.cmd(&["CONFIG", "GET", "maxmemory*"]).strings();
    assert!(cfg.contains(&"maxmemory-policy".to_string()));
    assert_eq!(c.cmd(&["CONFIG", "SET", "maxmemory", "100mb"]), ok());
    assert_eq!(
        c.cmd(&["CONFIG", "GET", "maxmemory"]).strings(),
        vec!["maxmemory", "104857600"]
    );
    assert!(c.cmd(&["CONFIG", "SET", "nosuch", "1"]).is_error());
    assert!(c.cmd(&["CONFIG", "SET", "port", "1"]).is_error());
    // CLIENT
    let id = c.cmd(&["CLIENT", "ID"]).int();
    assert_eq!(c.cmd(&["CLIENT", "SETNAME", "tester"]), ok());
    assert_eq!(c.cmd(&["CLIENT", "GETNAME"]), bulk("tester"));
    let list = c.cmd(&["CLIENT", "LIST"]).text();
    assert!(
        list.contains(&format!("id={id} ")) && list.contains("name=tester"),
        "{list}"
    );
    let mut victim = s.client();
    let vid = victim.cmd(&["CLIENT", "ID"]).int();
    assert_eq!(
        c.cmd(&["CLIENT", "KILL", "ID", &vid.to_string()]),
        Reply::Int(1)
    );
    assert!(victim.read().is_err() || victim.cmd(&["PING"]) != Reply::Simple("PONG".into()));
    // COMMAND
    assert!(c.cmd(&["COMMAND", "COUNT"]).int() > 150);
    let docs = c.cmd(&["COMMAND", "DOCS", "get"]).flat();
    assert!(
        docs.starts_with("[get, [summary, Returns the string value of a key."),
        "{docs}"
    );
    assert_eq!(
        c.cmd(&["COMMAND", "GETKEYS", "MSET", "a", "1", "b", "2"])
            .strings(),
        vec!["a", "b"]
    );
    // SLOWLOG + DEBUG SLEEP
    c.cmd(&["SLOWLOG", "RESET"]);
    assert_eq!(c.cmd(&["DEBUG", "SLEEP", "0.1"]), ok());
    let log = c.cmd(&["SLOWLOG", "GET"]).array();
    assert_eq!(log.len(), 1);
    let entry = log[0].array();
    assert!(entry[2].int() >= 100_000);
    assert_eq!(entry[3].strings(), vec!["DEBUG", "SLEEP", "0.1"]);
    assert_eq!(c.cmd(&["SLOWLOG", "LEN"]), Reply::Int(1));
    // TIME, DBSIZE, OBJECT ENCODING
    assert_eq!(c.cmd(&["TIME"]).array().len(), 2);
    c.cmd(&["SET", "n", "123"]);
    assert_eq!(c.cmd(&["OBJECT", "ENCODING", "n"]), bulk("int"));
    c.cmd(&["RPUSH", "l", "a"]);
    assert_eq!(c.cmd(&["OBJECT", "ENCODING", "l"]), bulk("listpack"));
    assert!(c.cmd(&["MEMORY", "USAGE", "l"]).int() > 0);
    let cmdstats = c.cmd(&["INFO", "commandstats"]).text();
    assert!(cmdstats.contains("cmdstat_set:calls="), "{cmdstats}");
}

#[test]
fn auth_with_requirepass() {
    let s = Server::start(&["--requirepass", "s3cret"]);
    let mut c = s.client();
    assert!(c.cmd(&["GET", "x"]).text().starts_with("NOAUTH"));
    assert!(c.cmd(&["AUTH", "wrong"]).text().starts_with("WRONGPASS"));
    assert_eq!(c.cmd(&["AUTH", "s3cret"]), ok());
    assert_eq!(c.cmd(&["SET", "x", "1"]), ok());
    let mut c2 = s.client();
    let r = c2.cmd(&["HELLO", "3", "AUTH", "default", "s3cret"]);
    assert!(matches!(r, Reply::Map(_)));
    assert_eq!(c2.cmd(&["GET", "x"]), bulk("1"));
}

#[test]
fn encodings_convert_like_redis() {
    let s = Server::start(&[]);
    let mut c = s.client();
    c.cmd(&["SADD", "s", "1", "2", "3"]);
    assert_eq!(c.cmd(&["OBJECT", "ENCODING", "s"]), bulk("intset"));
    c.cmd(&["SADD", "s", "x"]);
    assert_eq!(c.cmd(&["OBJECT", "ENCODING", "s"]), bulk("listpack"));
    for i in 0..200 {
        c.send(&["SADD", "s", &format!("m{i}")]);
    }
    for _ in 0..200 {
        c.read().unwrap();
    }
    assert_eq!(c.cmd(&["OBJECT", "ENCODING", "s"]), bulk("hashtable"));
    c.cmd(&["HSET", "h", "f", "v"]);
    assert_eq!(c.cmd(&["OBJECT", "ENCODING", "h"]), bulk("listpack"));
    c.cmd(&["HSET", "h", "big", &"v".repeat(100)]);
    assert_eq!(c.cmd(&["OBJECT", "ENCODING", "h"]), bulk("hashtable"));
    c.cmd(&["ZADD", "z", "1", "a"]);
    assert_eq!(c.cmd(&["OBJECT", "ENCODING", "z"]), bulk("listpack"));
    for i in 0..200 {
        c.send(&["ZADD", "z", &i.to_string(), &format!("m{i}")]);
    }
    for _ in 0..200 {
        c.read().unwrap();
    }
    assert_eq!(c.cmd(&["OBJECT", "ENCODING", "z"]), bulk("skiplist"));
    assert_eq!(c.cmd(&["ZRANK", "z", "m150"]), Reply::Int(151));
    c.cmd(&["CONFIG", "SET", "zset-max-listpack-entries", "1000"]);
    c.cmd(&["ZADD", "z2", "1", "a"]);
    for i in 0..500 {
        c.send(&["ZADD", "z2", &i.to_string(), &format!("m{i}")]);
    }
    for _ in 0..500 {
        c.read().unwrap();
    }
    assert_eq!(c.cmd(&["OBJECT", "ENCODING", "z2"]), bulk("listpack"));
}

#[test]
fn quit_and_reset() {
    let s = Server::start(&[]);
    let mut c = s.client();
    c.cmd(&["SELECT", "2"]);
    c.cmd(&["MULTI"]);
    assert_eq!(c.cmd(&["RESET"]), Reply::Simple("RESET".into()));
    assert_eq!(c.cmd(&["SET", "a", "1"]), ok()); // not queued
    c.cmd(&["SELECT", "0"]);
    assert_eq!(c.cmd(&["GET", "a"]), bulk("1")); // RESET selected db 0
    assert_eq!(c.cmd(&["QUIT"]), ok());
    assert!(c.read().is_err());
}

#[test]
fn shutdown_command_exits() {
    let mut s = Server::start(&[]);
    let mut c = s.client();
    c.send(&["SHUTDOWN", "NOSAVE"]);
    assert!(
        s.wait_exit(Duration::from_secs(10)),
        "server did not exit after SHUTDOWN"
    );
}

#[test]
fn large_values_and_binary_safety() {
    let s = Server::start(&[]);
    let mut c = s.client();
    let blob: Vec<u8> = (0..2_000_000u32).map(|i| (i % 251) as u8).collect();
    assert_eq!(c.cmd_bytes(&[b"SET", b"blob\r\n\0key", &blob]), ok());
    assert_eq!(c.cmd_bytes(&[b"GET", b"blob\r\n\0key"]), Reply::Bulk(blob));
    let many: Vec<String> = (0..100_000).map(|i| i.to_string()).collect();
    let mut args = vec!["RPUSH", "biglist"];
    args.extend(many.iter().map(|s| s.as_str()));
    assert_eq!(c.cmd(&args), Reply::Int(100_000));
    assert_eq!(
        c.cmd(&["LRANGE", "biglist", "0", "-1"]).array().len(),
        100_000
    );
    assert_eq!(c.cmd(&["LINDEX", "biglist", "-1"]), bulk("99999"));
}

#[test]
fn many_concurrent_connections() {
    let s = Server::start(&[]);
    let mut clients: Vec<Client> = (0..1000).map(|_| s.client()).collect();
    for (i, c) in clients.iter_mut().enumerate() {
        c.send(&["SET", &format!("conn:{i}"), &i.to_string()]);
    }
    for c in clients.iter_mut() {
        assert_eq!(c.read().unwrap(), ok());
    }
    let mut admin = s.client();
    assert_eq!(admin.cmd(&["DBSIZE"]), Reply::Int(1000));
    let info = admin.cmd(&["INFO", "clients"]).text();
    assert_eq!(info_field(&info, "connected_clients").unwrap(), "1001");
    drop(clients);
    assert!(wait_until(Duration::from_secs(10), || {
        info_field(&admin.cmd(&["INFO", "clients"]).text(), "connected_clients").as_deref()
            == Some("1")
    }));
}

#[test]
fn slow_subscriber_is_disconnected_at_output_limit() {
    let s = Server::start(&[]);
    let mut sub = s.client();
    sub.cmd(&["SUBSCRIBE", "firehose"]);
    // Never read from `sub`; publish ~64 MB.
    let mut publ = s.client();
    let payload = "p".repeat(64 * 1024);
    let mut disconnected = false;
    for _ in 0..1000 {
        if publ.cmd(&["PUBLISH", "firehose", &payload]) == Reply::Int(0) {
            disconnected = true;
            break;
        }
    }
    assert!(
        disconnected,
        "a subscriber that never reads must be dropped at the output buffer limit"
    );
}
