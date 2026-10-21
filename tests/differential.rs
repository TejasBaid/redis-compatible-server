//! Differential testing against a real `redis-server`.
//!
//! The same seeded, randomized command sequences are sent to redis-clone and
//! to the real Redis, over RESP2 and RESP3, and every reply is compared
//! (after normalizing replies whose order Redis leaves unspecified). After each
//! sequence the entire keyspace of both servers is dumped and compared too.
//!
//! The generator deliberately mixes types on shared keys (to hit WRONGTYPE
//! paths), crosses encoding thresholds (long members, many elements), uses
//! negative/out-of-range indices, exclusive and infinite score bounds,
//! conflicting options and malformed arguments.
//!
//! Skipped (with a message) when `redis-server` is not installed.
//! Tune with `DIFF_SEEDS` and `DIFF_OPS`.

mod common;

use common::{Client, RealRedis, Reply, Server};

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
    fn chance(&mut self, pct: usize) -> bool {
        self.below(100) < pct
    }
    fn pick<'a>(&mut self, xs: &[&'a str]) -> &'a str {
        xs[self.below(xs.len())]
    }
}

const SHARED: &[&str] = &["a", "b", "c", "d"];

fn long_member(rng: &mut Rng) -> String {
    format!("long-{}-{}", rng.below(4), "x".repeat(70))
}

fn member(rng: &mut Rng) -> String {
    if rng.chance(4) {
        return long_member(rng);
    }
    rng.pick(&[
        "a", "b", "c", "d", "e", "f", "1", "2", "3", "10", "-1", "0", "foo bar", "", "zz", "apple",
        "banana",
    ])
    .to_string()
}

fn int_member(rng: &mut Rng) -> String {
    if rng.chance(10) {
        return member(rng);
    }
    (rng.below(40) as i64 - 10).to_string()
}

fn int_arg(rng: &mut Rng) -> String {
    match rng.below(20) {
        0 => "9223372036854775807".into(),
        1 => "-9223372036854775808".into(),
        2 => "abc".into(),
        3 => "1.5".into(),
        4 => "".into(),
        5 => "99999999999999999999".into(),
        _ => (rng.below(21) as i64 - 10).to_string(),
    }
}

fn index(rng: &mut Rng) -> String {
    match rng.below(12) {
        0 => "100".into(),
        1 => "-100".into(),
        2 => "x".into(),
        _ => (rng.below(13) as i64 - 6).to_string(),
    }
}

fn score(rng: &mut Rng) -> String {
    rng.pick(&[
        "0", "1", "2", "3", "-1", "1.5", "-2.5", "10", "100", "0.25", "+inf", "-inf", "inf", "3.0",
        "1e3", "5", "7", "nan", "abc",
    ])
    .to_string()
}

fn score_bound(rng: &mut Rng) -> String {
    let s = rng.pick(&[
        "0", "1", "2", "3", "-1", "1.5", "10", "+inf", "-inf", "5", "x",
    ]);
    if rng.chance(30) && s != "x" {
        format!("({s}")
    } else {
        s.to_string()
    }
}

fn lex_bound(rng: &mut Rng) -> String {
    match rng.below(8) {
        0 => "-".into(),
        1 => "+".into(),
        2 => "bad".into(),
        3 | 4 => format!("({}", rng.pick(&["a", "b", "c", "d", "e", "1", "10"])),
        _ => format!("[{}", rng.pick(&["a", "b", "c", "d", "e", "1", "10", ""])),
    }
}

fn key(rng: &mut Rng, pool: &[&str]) -> String {
    if rng.chance(20) {
        rng.pick(SHARED).to_string()
    } else {
        rng.pick(pool).to_string()
    }
}

const LISTS: &[&str] = &["l1", "l2", "l3"];
const HASHES: &[&str] = &["h1", "h2"];
const SETS: &[&str] = &["s1", "s2", "s3"];
const ZSETS: &[&str] = &["z1", "z2", "z3"];
const STRS: &[&str] = &["x1", "x2", "x3", "n1"];
const STREAMS: &[&str] = &["st1", "st2"];
/// Sorted sets whose members all share score 0: lexicographic range
/// commands are only well-defined there (with mixed scores, even Redis'
/// answer depends on its random skiplist levels).
const LEXZ: &[&str] = &["zl1", "zl2"];

struct Gen {
    rng: Rng,
    stream_ms: u64,
}

impl Gen {
    fn string_cmd(&mut self) -> Vec<String> {
        let r = &mut self.rng;
        let k = key(r, STRS);
        match r.below(22) {
            0..=3 => {
                let mut c = vec!["SET".into(), k, member(r)];
                for opt in ["NX", "XX", "GET", "KEEPTTL"] {
                    if r.chance(12) {
                        c.push(opt.into());
                    }
                }
                if r.chance(15) {
                    let opt = r.pick(&["EX", "PX", "EXAT", "PXAT"]);
                    c.push(opt.into());
                    c.push(
                        match opt {
                            "EX" => r.pick(&["1000", "100000", "0", "-5", "abc", "99999999999"]),
                            "PX" => r.pick(&["10000000", "0", "-5", "abc"]),
                            "EXAT" => r.pick(&["1000", "4102444800", "0", "abc"]),
                            _ => r.pick(&["1000", "4102444800000", "0"]),
                        }
                        .into(),
                    );
                }
                c
            }
            4 | 5 => vec!["GET".into(), k],
            6 => vec!["INCR".into(), k],
            7 => vec!["DECR".into(), k],
            8 => vec!["INCRBY".into(), k, int_arg(r)],
            9 => vec!["DECRBY".into(), k, int_arg(r)],
            10 => vec!["APPEND".into(), k, member(r)],
            11 => vec!["STRLEN".into(), k],
            12 => vec!["GETRANGE".into(), k, index(r), index(r)],
            13 => vec![
                "SETRANGE".into(),
                k,
                r.pick(&["0", "1", "5", "-1", "x"]).into(),
                member(r),
            ],
            14 => {
                let mut c = vec!["MSET".into()];
                for _ in 0..1 + r.below(3) {
                    c.push(key(r, STRS));
                    c.push(member(r));
                }
                if r.chance(5) {
                    c.pop();
                }
                c
            }
            15 => {
                let mut c = vec!["MGET".into()];
                for _ in 0..1 + r.below(4) {
                    c.push(key(r, STRS));
                }
                c
            }
            16 => vec![
                "MSETNX".into(),
                key(r, STRS),
                member(r),
                key(r, STRS),
                member(r),
            ],
            17 => vec!["SETNX".into(), k, member(r)],
            18 => vec!["GETSET".into(), k, member(r)],
            19 => vec!["GETDEL".into(), k],
            20 => vec![
                "GETEX".into(),
                k,
                r.pick(&["PERSIST", "EX", "BAD"]).into(),
                "500".into(),
            ],
            _ => vec!["SET".into(), k, int_member(r)],
        }
    }

    fn key_cmd(&mut self) -> Vec<String> {
        let r = &mut self.rng;
        let all: Vec<&str> = [SHARED, LISTS, HASHES, SETS, ZSETS, STRS].concat();
        let k = r.pick(&all).to_string();
        match r.below(16) {
            0 | 1 => vec!["DEL".into(), k, r.pick(&all).into()],
            2 => vec!["EXISTS".into(), k, r.pick(&all).into(), r.pick(&all).into()],
            3 | 4 => vec!["TYPE".into(), k],
            5 => vec!["RENAME".into(), k, r.pick(&all).into()],
            6 => vec!["RENAMENX".into(), k, r.pick(&all).into()],
            7 => {
                // NX/XX/GT/LT compare deadlines; relative times would make the
                // outcome depend on which millisecond each server ran in, so
                // option variants use absolute EXPIREAT timestamps.
                let mut c = vec![
                    "EXPIRE".into(),
                    k,
                    r.pick(&["1000", "5000", "-1", "x", "0"]).into(),
                ];
                if r.chance(40) {
                    c[0] = "EXPIREAT".into();
                    c[2] = r
                        .pick(&["4102444800", "4102448400", "4200000000", "x"])
                        .into();
                    c.push(r.pick(&["NX", "XX", "GT", "LT", "BAD"]).into());
                    if r.chance(25) {
                        c.push(r.pick(&["NX", "XX", "GT", "LT"]).into());
                    }
                }
                c
            }
            8 => vec!["PEXPIRE".into(), k, r.pick(&["1000000", "5000000"]).into()],
            9 => vec!["PERSIST".into(), k],
            10 | 11 => vec!["TTL".into(), k],
            12 => vec!["DBSIZE".into()],
            13 => {
                let mut c = vec!["COPY".into(), k, r.pick(&all).into()];
                if r.chance(50) {
                    c.push("REPLACE".into());
                }
                c
            }
            14 => vec!["EXPIRETIME".into(), k],
            _ => {
                if r.chance(50) {
                    let mut c = vec!["SCAN".into(), "0".into(), "COUNT".into(), "10000".into()];
                    if r.chance(40) {
                        c.extend(["MATCH".into(), r.pick(&["*", "l*", "?1", "[sz]*"]).into()]);
                    }
                    if r.chance(40) {
                        c.extend([
                            "TYPE".into(),
                            r.pick(&["string", "list", "zset", "hash", "set", "none"])
                                .into(),
                        ]);
                    }
                    c
                } else {
                    vec![
                        "KEYS".into(),
                        r.pick(&["*", "l*", "?1", "[sz]*", "x[1-2]"]).into(),
                    ]
                }
            }
        }
    }

    fn list_cmd(&mut self) -> Vec<String> {
        let r = &mut self.rng;
        let k = key(r, LISTS);
        match r.below(24) {
            0..=3 => {
                let mut c = vec![r.pick(&["LPUSH", "RPUSH"]).into(), k];
                for _ in 0..1 + r.below(4) {
                    c.push(member(r));
                }
                c
            }
            4 => vec![r.pick(&["LPUSHX", "RPUSHX"]).into(), k, member(r)],
            5 | 6 => {
                let mut c = vec![r.pick(&["LPOP", "RPOP"]).into(), k];
                if r.chance(40) {
                    c.push(r.pick(&["0", "1", "2", "10", "-1", "x"]).into());
                }
                c
            }
            7..=9 => vec!["LRANGE".into(), k, index(r), index(r)],
            10 => vec!["LLEN".into(), k],
            11 => vec!["LINDEX".into(), k, index(r)],
            12 => vec!["LSET".into(), k, index(r), member(r)],
            13 => vec![
                "LREM".into(),
                k,
                r.pick(&["0", "1", "-1", "2", "-2", "x"]).into(),
                member(r),
            ],
            14 => vec!["LTRIM".into(), k, index(r), index(r)],
            15 => vec![
                "LINSERT".into(),
                k,
                r.pick(&["BEFORE", "AFTER", "bad"]).into(),
                member(r),
                member(r),
            ],
            16 => vec![
                "LMOVE".into(),
                k,
                key(r, LISTS),
                r.pick(&["LEFT", "RIGHT"]).into(),
                r.pick(&["LEFT", "RIGHT", "UP"]).into(),
            ],
            17 => vec!["RPOPLPUSH".into(), k, key(r, LISTS)],
            18 => {
                let mut c = vec!["LPOS".into(), k, member(r)];
                if r.chance(50) {
                    c.push("RANK".into());
                    c.push(r.pick(&["1", "-1", "2", "0", "-2"]).into());
                }
                if r.chance(50) {
                    c.push("COUNT".into());
                    c.push(r.pick(&["0", "1", "2", "-1"]).into());
                }
                if r.chance(20) {
                    c.push("MAXLEN".into());
                    c.push(r.pick(&["0", "2", "-1"]).into());
                }
                c
            }
            19 => {
                let mut c = vec![
                    "LMPOP".into(),
                    r.pick(&["1", "2", "0", "x"]).into(),
                    k,
                    key(r, LISTS),
                ];
                c.push(r.pick(&["LEFT", "RIGHT", "MIDDLE"]).into());
                if r.chance(50) {
                    c.push("COUNT".into());
                    c.push(r.pick(&["1", "3", "0", "-1"]).into());
                }
                c
            }
            20 => vec!["BLPOP".into(), k, key(r, LISTS), "0.01".into()],
            _ => {
                let mut c = vec!["RPUSH".into(), k];
                for _ in 0..r.below(30) {
                    c.push(int_member(r));
                }
                if c.len() == 2 {
                    c.push("x".into());
                }
                c
            }
        }
    }

    fn hash_cmd(&mut self) -> Vec<String> {
        let r = &mut self.rng;
        let k = key(r, HASHES);
        let field = |r: &mut Rng| r.pick(&["f1", "f2", "f3", "f4", "n", "x y"]).to_string();
        match r.below(16) {
            0..=3 => {
                let mut c = vec!["HSET".into(), k];
                for _ in 0..1 + r.below(3) {
                    c.push(field(r));
                    c.push(member(r));
                }
                if r.chance(5) {
                    c.pop();
                }
                c
            }
            4 => vec!["HSETNX".into(), k, field(r), member(r)],
            5 | 6 => vec!["HGET".into(), k, field(r)],
            7 => vec!["HMGET".into(), k, field(r), field(r), field(r)],
            8 => vec!["HDEL".into(), k, field(r), field(r)],
            9 => vec!["HGETALL".into(), k],
            10 => vec![r.pick(&["HKEYS", "HVALS", "HLEN"]).into(), k],
            11 => vec!["HEXISTS".into(), k, field(r)],
            12 => vec!["HINCRBY".into(), k, field(r), int_arg(r)],
            13 => vec!["HSTRLEN".into(), k, field(r)],
            14 => match r.below(3) {
                0 => vec!["HMSET".into(), k, field(r), member(r)],
                1 => vec!["HSCAN".into(), k, "0".into(), "COUNT".into(), "1000".into()],
                _ => vec!["OBJECT".into(), "ENCODING".into(), k],
            },
            _ => {
                let mut c = vec!["HSET".into(), k];
                for i in 0..r.below(70) {
                    c.push(format!("bulk{i}"));
                    c.push(int_member(r));
                }
                if c.len() == 2 {
                    c.push("f".into());
                    c.push("v".into());
                }
                c
            }
        }
    }

    fn set_cmd(&mut self) -> Vec<String> {
        let r = &mut self.rng;
        let k = key(r, SETS);
        match r.below(18) {
            0..=4 => {
                let mut c = vec!["SADD".into(), k];
                let ints = r.chance(50);
                for _ in 0..1 + r.below(5) {
                    c.push(if ints { int_member(r) } else { member(r) });
                }
                c
            }
            5 => vec!["SREM".into(), k, member(r), int_member(r)],
            6 | 7 => vec!["SMEMBERS".into(), k],
            8 => vec!["SISMEMBER".into(), k, member(r)],
            9 => vec!["SMISMEMBER".into(), k, member(r), int_member(r)],
            10 => vec!["SCARD".into(), k],
            11 => vec![
                r.pick(&["SINTER", "SUNION", "SDIFF"]).into(),
                k,
                key(r, SETS),
                key(r, SETS),
            ],
            12 => vec![
                r.pick(&["SINTERSTORE", "SUNIONSTORE", "SDIFFSTORE"]).into(),
                key(r, SETS),
                k,
                key(r, SETS),
            ],
            13 => vec!["SMOVE".into(), k, key(r, SETS), int_member(r)],
            14 => {
                let mut c = vec![
                    "SINTERCARD".into(),
                    r.pick(&["2", "1", "0", "5"]).into(),
                    k,
                    key(r, SETS),
                ];
                if r.chance(40) {
                    c.push("LIMIT".into());
                    c.push(r.pick(&["0", "1", "-1"]).into());
                }
                c
            }
            15 => match r.below(4) {
                0 => vec!["SRANDMEMBER".into(), k, r.pick(&["0", "-3", "100"]).into()],
                1 => vec!["SPOP".into(), k, "1000".into()],
                2 => {
                    let mut c = vec!["SSCAN".into(), k, "0".into(), "COUNT".into(), "1000".into()];
                    if r.chance(40) {
                        c.extend(["MATCH".into(), r.pick(&["*1*", "a*", "?"]).into()]);
                    }
                    c
                }
                _ => vec!["OBJECT".into(), "ENCODING".into(), k],
            },
            _ => {
                let mut c = vec!["SADD".into(), k];
                for _ in 0..r.below(140) {
                    c.push(int_member(r));
                }
                if c.len() == 2 {
                    c.push("1".into());
                }
                c
            }
        }
    }

    fn zset_cmd(&mut self) -> Vec<String> {
        let r = &mut self.rng;
        let k = key(r, ZSETS);
        match r.below(34) {
            0..=4 => {
                let mut c = vec!["ZADD".into(), k];
                for opt in ["NX", "XX", "GT", "LT", "CH", "INCR"] {
                    if r.chance(8) {
                        c.push(opt.into());
                    }
                }
                for _ in 0..1 + r.below(4) {
                    c.push(score(r));
                    c.push(member(r));
                }
                c
            }
            5 => vec!["ZINCRBY".into(), k, score(r), member(r)],
            6 => vec!["ZREM".into(), k, member(r), member(r)],
            7 => vec!["ZSCORE".into(), k, member(r)],
            8 => vec!["ZMSCORE".into(), k, member(r), member(r)],
            9 => vec!["ZCARD".into(), k],
            10 => vec!["ZCOUNT".into(), k, score_bound(r), score_bound(r)],
            11 => vec![
                "ZLEXCOUNT".into(),
                r.pick(LEXZ).into(),
                lex_bound(r),
                lex_bound(r),
            ],
            12 => {
                let mut c = vec![r.pick(&["ZRANK", "ZREVRANK"]).into(), k, member(r)];
                if r.chance(30) {
                    c.push("WITHSCORE".into());
                }
                c
            }
            13..=15 => {
                let mut c = vec!["ZRANGE".into(), k];
                match r.below(3) {
                    0 => {
                        c.push(index(r));
                        c.push(index(r));
                    }
                    1 => {
                        c.push(score_bound(r));
                        c.push(score_bound(r));
                        c.push("BYSCORE".into());
                    }
                    _ => {
                        c[1] = r.pick(LEXZ).into();
                        c.push(lex_bound(r));
                        c.push(lex_bound(r));
                        c.push("BYLEX".into());
                    }
                }
                if r.chance(30) {
                    c.push("REV".into());
                }
                if r.chance(25) {
                    c.push("LIMIT".into());
                    c.push(r.pick(&["0", "1", "-1", "2"]).into());
                    c.push(r.pick(&["1", "2", "-1", "0", "5"]).into());
                }
                if r.chance(30) {
                    c.push("WITHSCORES".into());
                }
                c
            }
            16 => {
                let mut c = vec![
                    r.pick(&["ZRANGEBYSCORE", "ZREVRANGEBYSCORE"]).into(),
                    k,
                    score_bound(r),
                    score_bound(r),
                ];
                if r.chance(40) {
                    c.push("WITHSCORES".into());
                }
                if r.chance(30) {
                    c.push("LIMIT".into());
                    c.push(r.pick(&["0", "1", "-1"]).into());
                    c.push(r.pick(&["1", "2", "-1"]).into());
                }
                c
            }
            17 => {
                let mut c = vec![
                    r.pick(&["ZRANGEBYLEX", "ZREVRANGEBYLEX"]).into(),
                    r.pick(LEXZ).into(),
                    lex_bound(r),
                    lex_bound(r),
                ];
                if r.chance(30) {
                    c.push("LIMIT".into());
                    c.push(r.pick(&["0", "1"]).into());
                    c.push(r.pick(&["1", "2", "-1"]).into());
                }
                c
            }
            18 => {
                let mut c = vec!["ZREVRANGE".into(), k, index(r), index(r)];
                if r.chance(40) {
                    c.push("WITHSCORES".into());
                }
                c
            }
            19 => vec!["ZREMRANGEBYRANK".into(), k, index(r), index(r)],
            20 => vec!["ZREMRANGEBYSCORE".into(), k, score_bound(r), score_bound(r)],
            21 => vec![
                "ZREMRANGEBYLEX".into(),
                r.pick(LEXZ).into(),
                lex_bound(r),
                lex_bound(r),
            ],
            22 | 23 => {
                let mut c = vec![r.pick(&["ZPOPMIN", "ZPOPMAX"]).into(), k];
                if r.chance(40) {
                    c.push(r.pick(&["0", "1", "2", "10", "-1"]).into());
                }
                c
            }
            24 | 25 => {
                let op = r.pick(&["ZUNIONSTORE", "ZINTERSTORE", "ZDIFFSTORE"]);
                let mut c = vec![
                    op.into(),
                    key(r, ZSETS),
                    "2".into(),
                    k,
                    r.pick(&[ZSETS, SETS].concat()).into(),
                ];
                if op != "ZDIFFSTORE" {
                    if r.chance(40) {
                        c.push("WEIGHTS".into());
                        c.push(r.pick(&["1", "2", "-1", "0.5", "0"]).into());
                        c.push(r.pick(&["1", "3", "inf"]).into());
                    }
                    if r.chance(40) {
                        c.push("AGGREGATE".into());
                        c.push(r.pick(&["SUM", "MIN", "MAX"]).into());
                    }
                }
                c
            }
            26 => {
                let op = r.pick(&["ZUNION", "ZINTER", "ZDIFF"]);
                let mut c = vec![op.into(), "2".into(), k, key(r, ZSETS)];
                if r.chance(50) {
                    c.push("WITHSCORES".into());
                }
                c
            }
            27 => {
                let mut c = vec!["ZRANGESTORE".into(), key(r, ZSETS), k, index(r), index(r)];
                if r.chance(30) {
                    c.push("REV".into());
                }
                c
            }
            28 => {
                let mut c = vec![
                    "ZMPOP".into(),
                    "2".into(),
                    k,
                    key(r, ZSETS),
                    r.pick(&["MIN", "MAX"]).into(),
                ];
                if r.chance(50) {
                    c.push("COUNT".into());
                    c.push(r.pick(&["1", "2", "5"]).into());
                }
                c
            }
            29 => match r.below(3) {
                0 => vec!["ZINTERCARD".into(), "2".into(), k, key(r, ZSETS)],
                1 => vec!["ZSCAN".into(), k, "0".into(), "COUNT".into(), "1000".into()],
                _ => vec!["OBJECT".into(), "ENCODING".into(), k],
            },
            30 => vec!["BZPOPMIN".into(), k, "0.01".into()],
            31 | 32 => {
                let mut c = vec!["ZADD".into(), r.pick(LEXZ).into()];
                for _ in 0..1 + r.below(6) {
                    c.push("0".into());
                    c.push(member(r));
                }
                c
            }
            _ => {
                let mut c = vec!["ZADD".into(), k];
                for i in 0..r.below(140) {
                    c.push((r.below(50) as i64 - 10).to_string());
                    c.push(format!("m{i}"));
                }
                if c.len() == 2 {
                    c.push("1".into());
                    c.push("one".into());
                }
                c
            }
        }
    }

    fn stream_cmd(&mut self) -> Vec<String> {
        let k = key(&mut self.rng, STREAMS);
        match self.rng.below(8) {
            0..=2 => {
                self.stream_ms += 1 + self.rng.below(3) as u64;
                let id = if self.rng.chance(10) {
                    self.rng.pick(&["0-0", "1-1", "bad", "5-*"]).to_string()
                } else if self.rng.chance(20) {
                    format!("{}-*", self.stream_ms)
                } else {
                    format!("{}-{}", self.stream_ms, self.rng.below(3))
                };
                let mut c = vec!["XADD".into(), k];
                if self.rng.chance(15) {
                    c.extend(["MAXLEN".into(), "=".into(), "3".into()]);
                }
                c.push(id);
                c.push("f".into());
                c.push(member(&mut self.rng));
                c
            }
            3 => vec!["XLEN".into(), k],
            4 => {
                let mut c = vec![
                    "XRANGE".into(),
                    k,
                    self.rng.pick(&["-", "0", "2", "(3-0", "bad"]).into(),
                    self.rng.pick(&["+", "10", "5-1", "(9"]).into(),
                ];
                if self.rng.chance(30) {
                    c.push("COUNT".into());
                    c.push(self.rng.pick(&["1", "2", "0", "-1"]).into());
                }
                c
            }
            5 => vec![
                "XREVRANGE".into(),
                k,
                "+".into(),
                "-".into(),
                "COUNT".into(),
                "3".into(),
            ],
            6 => vec![
                "XDEL".into(),
                k,
                format!("{}-0", self.rng.below(self.stream_ms as usize + 1)),
            ],
            _ => vec![
                "XREAD".into(),
                "COUNT".into(),
                "2".into(),
                "STREAMS".into(),
                k,
                self.rng.pick(&["0", "0-0", "3", "$"]).into(),
            ],
        }
    }

    fn misc_cmd(&mut self) -> Vec<String> {
        let r = &mut self.rng;
        match r.below(8) {
            0 => vec!["PING".into()],
            1 => vec!["ECHO".into(), member(r)],
            2 => vec!["SELECT".into(), r.pick(&["0", "1", "0", "16", "x"]).into()],
            3 => vec!["NOSUCHCOMMAND".into(), member(r)],
            4 => vec!["GET".into()],
            5 => vec!["EXISTS".into()],
            6 => vec!["OBJECT".into(), "HELP_ME".into(), "x".into()],
            _ => vec!["ZADD".into(), "z1".into()],
        }
    }

    fn command(&mut self) -> Vec<String> {
        match self.rng.below(100) {
            0..=13 => self.string_cmd(),
            14..=23 => self.key_cmd(),
            24..=40 => self.list_cmd(),
            41..=52 => self.hash_cmd(),
            53..=65 => self.set_cmd(),
            66..=88 => self.zset_cmd(),
            89..=95 => self.stream_cmd(),
            _ => self.misc_cmd(),
        }
    }
}

fn upper(c: &[String]) -> String {
    c[0].to_ascii_uppercase()
}

/// Sorts replies whose element order Redis doesn't define.
fn normalize(cmd: &[String], r: Reply, resp3: bool) -> Reply {
    let name = upper(cmd);
    let sort = |mut v: Vec<Reply>| {
        v.sort_by_key(|x| format!("{x:?}"));
        v
    };
    let sort_pairs = |v: Vec<Reply>| {
        let mut pairs: Vec<(Reply, Reply)> =
            v.chunks(2).map(|p| (p[0].clone(), p[1].clone())).collect();
        pairs.sort_by_key(|p| format!("{:?}", p.0));
        pairs
            .into_iter()
            .flat_map(|(a, b)| [a, b])
            .collect::<Vec<_>>()
    };
    match (name.as_str(), r) {
        (
            "SMEMBERS" | "SINTER" | "SUNION" | "SDIFF" | "HKEYS" | "HVALS" | "KEYS",
            Reply::Array(v),
        ) => Reply::Array(sort(v)),
        ("SMEMBERS" | "SINTER" | "SUNION" | "SDIFF", Reply::Set(v)) => Reply::Set(sort(v)),
        ("HGETALL", Reply::Array(v)) => Reply::Array(sort_pairs(v)),
        ("HGETALL", Reply::Map(mut v)) => {
            v.sort_by_key(|p| format!("{:?}", p.0));
            Reply::Map(v)
        }
        ("SPOP", Reply::Array(v)) => Reply::Array(sort(v)),
        ("SPOP", Reply::Set(v)) => Reply::Set(sort(v)),
        ("SCAN" | "SSCAN" | "HSCAN" | "ZSCAN", Reply::Array(v)) if v.len() == 2 => {
            let items = match &v[1] {
                Reply::Array(items) if name == "HSCAN" || name == "ZSCAN" => {
                    Reply::Array(sort_pairs(items.clone()))
                }
                Reply::Array(items) => Reply::Array(sort(items.clone())),
                other => other.clone(),
            };
            Reply::Array(vec![v[0].clone(), items])
        }
        ("SRANDMEMBER", Reply::Array(v)) => Reply::Int(v.len() as i64),
        ("SRANDMEMBER", Reply::Bulk(_)) => Reply::Simple("random".into()),
        // Absolute/relative times differ by the few ms between the two
        // servers executing the same command; positivity is what matters.
        ("TTL" | "PTTL" | "EXPIRETIME" | "PEXPIRETIME", Reply::Int(i)) if i > 0 => {
            Reply::Simple("positive-time".into())
        }
        (_, r) => {
            let _ = resp3;
            r
        }
    }
}

fn equal(cmd: &[String], a: &Reply, b: &Reply) -> bool {
    if a == b {
        return true;
    }
    // Redis < 8 always appends ", with args beginning with: " to this error.
    if let (Reply::Error(x), Reply::Error(y)) = (a, b)
        && x.starts_with("ERR unknown command")
        && x.split(", with args").next() == y.split(", with args").next()
    {
        return true;
    }
    // TTL rounding at a second boundary.
    if let (Reply::Int(x), Reply::Int(y)) = (a, b)
        && upper(cmd) == "TTL"
        && (x - y).abs() <= 1
    {
        return true;
    }
    false
}

fn dump(c: &mut Client) -> Vec<(String, String)> {
    let mut out = Vec::new();
    c.cmd(&["HELLO", "2"]);
    for db in ["0", "1"] {
        c.cmd(&["SELECT", db]);
        let mut keys = c.cmd(&["KEYS", "*"]).strings();
        keys.sort();
        for k in keys {
            let t = c.cmd(&["TYPE", &k]).text();
            let content = match t.as_str() {
                "string" => c.cmd(&["GET", &k]).text(),
                "list" => format!("{:?}", c.cmd(&["LRANGE", &k, "0", "-1"]).strings()),
                "set" => {
                    let mut v = c.cmd(&["SMEMBERS", &k]).strings();
                    v.sort();
                    format!("{v:?}")
                }
                "zset" => format!(
                    "{:?}",
                    c.cmd(&["ZRANGE", &k, "0", "-1", "WITHSCORES"]).strings()
                ),
                "hash" => {
                    let v = c.cmd(&["HGETALL", &k]).strings();
                    let mut pairs: Vec<(String, String)> =
                        v.chunks(2).map(|p| (p[0].clone(), p[1].clone())).collect();
                    pairs.sort();
                    format!("{pairs:?}")
                }
                "stream" => format!("{:?}", c.cmd(&["XRANGE", &k, "-", "+"])),
                other => other.to_string(),
            };
            let ttl = c.cmd(&["TTL", &k]).int();
            out.push((
                format!("{db}:{k}:{t}"),
                format!("{content} ttl={}", ttl > 0),
            ));
        }
    }
    c.cmd(&["SELECT", "0"]);
    out
}

fn run_seed(ours: &mut Client, real: &mut Client, seed: u64, ops: usize, resp3: bool) {
    let proto = if resp3 { "3" } else { "2" };
    for c in [&mut *ours, &mut *real] {
        assert_eq!(c.cmd(&["FLUSHALL"]), Reply::Simple("OK".into()));
        c.cmd(&["HELLO", proto]);
        c.cmd(&["SELECT", "0"]);
    }
    let mut g = Gen {
        rng: Rng(seed.wrapping_mul(0x9E3779B97F4A7C15) | 1),
        stream_ms: 0,
    };
    let mut history: Vec<String> = Vec::new();
    let mut in_multi = false;
    let mut queued: Vec<Vec<String>> = Vec::new();
    for i in 0..ops {
        let mut cmd = g.command();
        // Occasionally wrap a few commands in a transaction.
        if !in_multi && g.rng.chance(3) {
            cmd = vec!["MULTI".into()];
        } else if in_multi && g.rng.chance(20) {
            cmd = vec![if g.rng.chance(85) { "EXEC" } else { "DISCARD" }.into()];
        }
        match upper(&cmd).as_str() {
            "MULTI" => {
                in_multi = true;
                queued.clear();
            }
            "EXEC" | "DISCARD" => in_multi = false,
            _ => {}
        }
        if in_multi && upper(&cmd).starts_with('B') {
            continue; // blocking commands inside MULTI are covered elsewhere
        }
        let refs: Vec<&str> = cmd.iter().map(|s| s.as_str()).collect();
        let a = ours.cmd(&refs);
        let b = real.cmd(&refs);
        history.push(format!("{refs:?}"));
        if let Ok(k) = std::env::var("DIFF_TRACE_KEY")
            && refs.iter().any(|a| *a == k)
        {
            eprintln!("trace {refs:?} -> ours {a:?} / redis {b:?}");
        }
        if in_multi && a == Reply::Simple("QUEUED".into()) {
            queued.push(cmd.clone());
        }
        let (na, nb) = if upper(&cmd) == "EXEC" {
            let q = std::mem::take(&mut queued);
            let norm = |r: &Reply| match r {
                Reply::Array(items) if items.len() == q.len() => Reply::Array(
                    items
                        .iter()
                        .zip(&q)
                        .map(|(it, c)| normalize(c, it.clone(), resp3))
                        .collect(),
                ),
                other => other.clone(),
            };
            (norm(&a), norm(&b))
        } else {
            (
                normalize(&cmd, a.clone(), resp3),
                normalize(&cmd, b.clone(), resp3),
            )
        };
        if !equal(&cmd, &na, &nb) {
            let mut diag = String::new();
            for k in &cmd[1..] {
                for (name, c) in [("ours", &mut *ours), ("real", &mut *real)] {
                    if c.cmd(&["EXISTS", k]) == Reply::Int(1) {
                        let enc = c.cmd(&["OBJECT", "ENCODING", k]);
                        let content = match c.cmd(&["TYPE", k]).text().as_str() {
                            "zset" => format!(
                                "{:?}",
                                c.cmd(&["ZRANGE", k, "0", "-1", "WITHSCORES"]).strings()
                            ),
                            "list" => format!("{:?}", c.cmd(&["LRANGE", k, "0", "-1"]).strings()),
                            _ => String::new(),
                        };
                        diag.push_str(&format!("\n  {name} {k}: {enc:?} {content}"));
                    }
                }
            }
            let start = if std::env::var("DIFF_FULL").is_ok() {
                0
            } else {
                history.len().saturating_sub(25)
            };
            panic!(
                "MISMATCH (seed {seed}, op {i}, resp{proto})\n  command: {refs:?}\n  redis-clone: {a:?}\n  redis:       {b:?}{diag}\n  recent history:\n    {}",
                history[start..].join("\n    ")
            );
        }
    }
    if in_multi {
        ours.cmd(&["DISCARD"]);
        real.cmd(&["DISCARD"]);
    }
    let (da, db) = (dump(ours), dump(real));
    if da != db {
        let diff: Vec<String> = da
            .iter()
            .zip(db.iter())
            .filter(|(x, y)| x != y)
            .take(5)
            .map(|(x, y)| format!("ours {x:?}\n    real {y:?}"))
            .collect();
        panic!(
            "STATE MISMATCH after seed {seed} (resp{proto}): {} vs {} keys\n    {}",
            da.len(),
            db.len(),
            diff.join("\n    ")
        );
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[test]
fn randomized_commands_match_real_redis() {
    let Some(real) = RealRedis::start() else {
        eprintln!("SKIPPED: redis-server not found; install Redis to run the differential test");
        return;
    };
    let server = Server::start(&[]);
    let mut ours = server.client();
    let mut theirs = real.client();
    let seeds = env_usize("DIFF_SEEDS", 300) as u64;
    let ops = env_usize("DIFF_OPS", 400);
    let start = env_usize("DIFF_START", 0) as u64;
    for seed in start..start + seeds {
        run_seed(&mut ours, &mut theirs, seed, ops, false);
    }
    let start3 = env_usize("DIFF_START3", 0) as u64;
    for seed in start3..seeds / 3 {
        run_seed(&mut ours, &mut theirs, 1_000_000 + seed, ops, true);
    }
}

/// Fixed edge cases that are easy to get subtly wrong.
#[test]
fn curated_edge_cases_match_real_redis() {
    let Some(real) = RealRedis::start() else {
        eprintln!("SKIPPED: redis-server not found");
        return;
    };
    let server = Server::start(&[]);
    let mut ours = server.client();
    let mut theirs = real.client();
    let script: &[&[&str]] = &[
        &["SET", "n", "10"],
        &["INCRBYFLOAT", "n", "abc"],
        &["INCRBY", "n", "9223372036854775807"],
        &["SET", "big", "9223372036854775807"],
        &["INCR", "big"],
        &["DECRBY", "n", "-9223372036854775808"],
        &["SETRANGE", "sr", "3", "ab"],
        &["GET", "sr"],
        &["SETRANGE", "missing", "0", ""],
        &["GETRANGE", "sr", "-1", "-5"],
        &["GETRANGE", "sr", "0", "-100"],
        &["SET", "k", "v", "EX", "10", "PX", "100"],
        &["SET", "k", "v", "NX", "XX"],
        &["SET", "k", "v", "KEEPTTL", "EX", "10"],
        &["SET", "k", "v", "EX", "9223372036854775807"],
        &["RPUSH", "l", "a", "b", "c", "d", "e"],
        &["LRANGE", "l", "-100", "100"],
        &["LRANGE", "l", "3", "1"],
        &["LPOS", "l", "c", "RANK", "-1", "COUNT", "0"],
        &["LPOS", "l", "c", "RANK", "0"],
        &["LINSERT", "l", "BEFORE", "zz", "x"],
        &["LSET", "nolist", "0", "x"],
        &["LSET", "l", "10", "x"],
        &["LPOP", "l", "0"],
        &["LPOP", "nolist", "0"],
        &["LPOP", "l", "-1"],
        &["LMOVE", "l", "l", "LEFT", "RIGHT"],
        &["LRANGE", "l", "0", "-1"],
        &["ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d"],
        &["ZADD", "z", "NX", "XX", "1", "a"],
        &["ZADD", "z", "GT", "LT", "1", "a"],
        &["ZADD", "z", "INCR", "1", "a", "2", "b"],
        &["ZADD", "z", "XX", "CH", "10", "a", "20", "new"],
        &["ZADD", "z", "GT", "CH", "5", "b", "1", "c"],
        &["ZADD", "z", "INCR", "+inf", "a"],
        &["ZINCRBY", "z", "-inf", "a"],
        &["ZRANGE", "z", "0", "-1", "WITHSCORES"],
        &["ZRANGE", "z", "(1", "+inf", "BYSCORE", "LIMIT", "1", "1"],
        &["ZRANGE", "z", "+inf", "(1", "BYSCORE", "REV", "WITHSCORES"],
        &["ZRANGE", "z", "0", "1", "LIMIT", "0", "1"],
        &["ZRANGE", "z", "-", "+", "BYLEX", "WITHSCORES"],
        &["ZRANGEBYSCORE", "z", "(", "5"],
        &["ZRANGEBYSCORE", "z", "", "5"],
        &["ZCOUNT", "z", "(3", "(3"],
        &["ZRANGEBYLEX", "z", "a", "c"],
        &["ZPOPMIN", "z", "-1"],
        &["ZPOPMIN", "z", "x"],
        &["ZUNIONSTORE", "out", "0", "z"],
        &["ZUNIONSTORE", "out", "3", "z"],
        &["ZINTERSTORE", "out", "1", "z", "WEIGHTS", "x"],
        &["ZADD", "inf1", "+inf", "m"],
        &["ZADD", "inf2", "-inf", "m"],
        &["ZUNION", "2", "inf1", "inf2", "WITHSCORES"],
        &[
            "ZINTER",
            "2",
            "inf1",
            "inf2",
            "WEIGHTS",
            "0",
            "1",
            "WITHSCORES",
        ],
        &[
            "ZADD",
            "fl",
            "0.1",
            "a",
            "1e-7",
            "b",
            "123456789.123",
            "c",
            "1e20",
            "d",
            "-0.0",
            "e",
        ],
        &["ZRANGE", "fl", "0", "-1", "WITHSCORES"],
        &["ZSCORE", "fl", "d"],
        &["HSET", "h", "f"],
        &["HINCRBY", "h", "f", "1"],
        &["HSET", "h", "s", "str"],
        &["HINCRBY", "h", "s", "1"],
        &["HINCRBYFLOAT", "h", "s", "1"],
        &["SADD", "s", "1", "2", "3"],
        &["SINTERCARD", "0", "s"],
        &["SINTERCARD", "5", "s"],
        &["SPOP", "s", "-1"],
        &["SRANDMEMBER", "s", "x"],
        &["SMOVE", "s", "s", "1"],
        &["OBJECT", "ENCODING", "s"],
        &["EXPIRE", "s", "100", "NX", "XX"],
        &["EXPIRE", "s", "100", "GT", "LT"],
        &["EXPIRE", "s", "100", "FOO"],
        &["EXPIRE", "s", "9223372036854775807"],
        &["XADD", "st", "0-0", "f", "v"],
        &["XADD", "st", "5-1", "f", "v"],
        &["XADD", "st", "5-1", "f", "v"],
        &["XADD", "st", "5-*", "f", "v"],
        &["XADD", "st", "5", "f"],
        &["XRANGE", "st", "(5-2", "+"],
        &["XRANGE", "st", "-", "+", "COUNT", "0"],
        &["XRANGE", "st", "bad", "+"],
        &["XREAD", "STREAMS", "st"],
        &["XTRIM", "st", "MAXLEN", "=", "1"],
        &["XLEN", "st"],
        &["MULTI"],
        &["SET", "t", "1"],
        &["INCR", "t"],
        &["LPUSH", "t", "x"],
        &["EXEC"],
        &["MULTI"],
        &["SET", "t", "1"],
        &["NOSUCH"],
        &["EXEC"],
        &["EXEC"],
        &["DISCARD"],
        &["MULTI"],
        &["MULTI"],
        &["DISCARD"],
        &["SELECT", "100"],
        &["RENAME", "nokey", "x"],
        &["RENAME", "t", "t"],
        &["COPY", "t", "t"],
        &["OBJECT", "FREQ", "t"],
        &["TYPE", "nokey"],
        &["PING", "a", "b"],
    ];
    for resp in ["2", "3"] {
        ours.cmd(&["FLUSHALL"]);
        theirs.cmd(&["FLUSHALL"]);
        ours.cmd(&["HELLO", resp]);
        theirs.cmd(&["HELLO", resp]);
        for cmd in script {
            let a = ours.cmd(cmd);
            let b = theirs.cmd(cmd);
            assert_eq!(a, b, "resp{resp} command {cmd:?}");
        }
    }
}
