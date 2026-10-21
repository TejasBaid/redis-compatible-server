//! Compatibility with a mainstream client library: the `redis` crate
//! (redis-rs), over RESP2 and RESP3, including pipelines, transactions,
//! pub/sub and typed replies.

mod common;

use common::Server;
use redis::Commands;
use std::collections::{HashMap, HashSet};

fn client(port: u16, resp3: bool) -> redis::Connection {
    let url = if resp3 {
        format!("redis://127.0.0.1:{port}/?protocol=resp3")
    } else {
        format!("redis://127.0.0.1:{port}/")
    };
    redis::Client::open(url).unwrap().get_connection().unwrap()
}

fn exercise(con: &mut redis::Connection) {
    let _: () = redis::cmd("FLUSHALL").query(con).unwrap();
    let _: () = con.set("name", "redis-clone").unwrap();
    let v: String = con.get("name").unwrap();
    assert_eq!(v, "redis-clone");
    let n: i64 = con.incr("counter", 5).unwrap();
    assert_eq!(n, 5);
    let missing: Option<String> = con.get("nope").unwrap();
    assert_eq!(missing, None);
    let _: () = con.rpush("list", &["a", "b", "c"]).unwrap();
    let items: Vec<String> = con.lrange("list", 0, -1).unwrap();
    assert_eq!(items, vec!["a", "b", "c"]);
    let _: () = con
        .hset_multiple("hash", &[("f1", "v1"), ("f2", "v2")])
        .unwrap();
    let h: HashMap<String, String> = con.hgetall("hash").unwrap();
    assert_eq!(h.len(), 2);
    assert_eq!(h["f2"], "v2");
    let _: () = con.sadd("set", &["x", "y", "z"]).unwrap();
    let s: HashSet<String> = con.smembers("set").unwrap();
    assert_eq!(s.len(), 3);
    let _: () = con
        .zadd_multiple("zset", &[(1.5, "a"), (0.5, "b"), (3.0, "c")])
        .unwrap();
    let z: Vec<(String, f64)> = con.zrange_withscores("zset", 0, -1).unwrap();
    assert_eq!(
        z,
        vec![("b".into(), 0.5), ("a".into(), 1.5), ("c".into(), 3.0)]
    );
    let score: Option<f64> = con.zscore("zset", "a").unwrap();
    assert_eq!(score, Some(1.5));
    let _: () = con.set_ex("ttl", "v", 100).unwrap();
    let ttl: i64 = con.ttl("ttl").unwrap();
    assert!(ttl > 90 && ttl <= 100);

    // Pipelines and MULTI/EXEC through the client's own abstractions.
    let (a, b): (i64, i64) = redis::pipe().incr("p", 1).incr("p", 10).query(con).unwrap();
    assert_eq!((a, b), (1, 11));
    let (x, y): (String, i64) = redis::pipe()
        .atomic()
        .set("tx", "value")
        .ignore()
        .get("tx")
        .incr("tx-counter", 3)
        .query(con)
        .unwrap();
    assert_eq!((x.as_str(), y), ("value", 3));

    // Errors surface as client errors.
    let err: redis::RedisResult<i64> = con.incr("name", 1);
    assert!(err.is_err());
    let err = redis::cmd("LPUSH")
        .arg("name")
        .arg("x")
        .query::<i64>(con)
        .unwrap_err();
    assert_eq!(err.code(), Some("WRONGTYPE"));
    let keys: Vec<String> = con.keys("*").unwrap();
    assert!(keys.len() >= 8);
    let iter: Vec<String> = con
        .scan_match::<_, String>("l*")
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(iter, vec!["list"]);
}

#[test]
fn redis_rs_resp2() {
    let s = Server::start(&[]);
    exercise(&mut client(s.port, false));
}

#[test]
fn redis_rs_resp3() {
    let s = Server::start(&[]);
    exercise(&mut client(s.port, true));
}

#[test]
fn redis_rs_pubsub() {
    let s = Server::start(&[]);
    let mut sub_con = client(s.port, false);
    let mut pub_con = client(s.port, false);
    let mut pubsub = sub_con.as_pubsub();
    pubsub.subscribe("events").unwrap();
    pubsub.psubscribe("ev*").unwrap();
    let receivers: i64 = pub_con.publish("events", "hello").unwrap();
    assert_eq!(receivers, 2);
    let mut got = Vec::new();
    for _ in 0..2 {
        let msg = pubsub.get_message().unwrap();
        got.push((
            msg.get_channel_name().to_string(),
            msg.get_payload::<String>().unwrap(),
        ));
    }
    assert!(got.iter().all(|(c, p)| c == "events" && p == "hello"));
}
