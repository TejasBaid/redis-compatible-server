//! Persistence: snapshots, AOF (all fsync policies), rewrite, crash safety
//! (SIGKILL mid-write, truncated tails, corrupt files).

mod common;

use common::{Client, Reply, Server, info_field, scratch_dir, wait_until};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

fn bulk(s: &str) -> Reply {
    Reply::Bulk(s.as_bytes().to_vec())
}

fn tempdir() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("rclone-persist")
        .tempdir_in(scratch_dir())
        .unwrap()
}

/// A canonical dump of the dataset for equality checks.
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
            let ttl = c.cmd(&["TTL", &k]).int();
            out.push(format!("db{db} {k} {t} {v} ttl:{}", ttl > 0));
        }
    }
    c.cmd(&["SELECT", "0"]);
    out
}

fn populate(c: &mut Client) {
    c.cmd(&["SET", "str", "hello"]);
    c.cmd(&["SET", "int", "12345"]);
    c.cmd(&["SET", "ttl", "v", "EX", "10000"]);
    c.cmd(&["RPUSH", "small-list", "a", "b", "c"]);
    let big: Vec<String> = (0..2000).map(|i| format!("item-{i}")).collect();
    let mut args = vec!["RPUSH", "big-list"];
    args.extend(big.iter().map(|s| s.as_str()));
    c.cmd(&args);
    c.cmd(&["HSET", "small-hash", "f1", "v1", "f2", "v2"]);
    for i in 0..300 {
        c.send(&["HSET", "big-hash", &format!("f{i}"), &format!("v{i}")]);
        c.send(&["SADD", "big-set", &format!("m{i}")]);
        c.send(&["ZADD", "big-zset", &format!("{}.5", i), &format!("m{i}")]);
    }
    for _ in 0..900 {
        c.read().unwrap();
    }
    c.cmd(&["SADD", "intset", "1", "2", "3"]);
    c.cmd(&["ZADD", "small-zset", "1", "a", "-inf", "b", "2.5", "c"]);
    c.cmd(&["XADD", "stream", "1-1", "f", "v"]);
    c.cmd(&["XADD", "stream", "2-5", "a", "b", "c", "d"]);
    c.cmd(&["SELECT", "1"]);
    c.cmd(&["SET", "in-db1", "x"]);
    c.cmd(&["SELECT", "2"]);
    c.cmd(&["LPUSH", "in-db2", "y"]);
    c.cmd(&["SELECT", "0"]);
}

#[test]
fn snapshot_save_restart_roundtrip() {
    let dir = tempdir();
    let before;
    {
        let s = Server::start_in(dir.path(), &[]);
        let mut c = s.client();
        populate(&mut c);
        before = dump(&mut c);
        assert_eq!(c.cmd(&["SAVE"]), Reply::Simple("OK".into()));
    } // SIGKILL on drop
    let s = Server::start_in(dir.path(), &[]);
    let mut c = s.client();
    assert_eq!(dump(&mut c), before);
    assert!(c.cmd(&["TTL", "ttl"]).int() > 9000);
    assert_eq!(c.cmd(&["OBJECT", "ENCODING", "big-zset"]), bulk("skiplist"));
    assert_eq!(c.cmd(&["OBJECT", "ENCODING", "intset"]), bulk("intset"));
    // DEBUG RELOAD (save + flush + load) is also lossless.
    assert_eq!(c.cmd(&["DEBUG", "RELOAD"]), Reply::Simple("OK".into()));
    assert_eq!(dump(&mut c), before);
}

#[test]
fn bgsave_is_point_in_time_across_shards() {
    let dir = tempdir();
    const ACCOUNTS: usize = 2000;
    const TOTAL: i64 = ACCOUNTS as i64 * 100;
    let s = Server::start_in(dir.path(), &["--threads", "4"]);
    let mut c = s.client();
    for i in 0..ACCOUNTS {
        c.send(&["SET", &format!("acct:{i}"), "100"]);
    }
    for _ in 0..ACCOUNTS {
        c.read().unwrap();
    }
    // Filler so the snapshot takes a while to serialize.
    let filler = "f".repeat(200);
    for i in 0..100_000 {
        c.send(&["SET", &format!("filler:{i}"), &filler]);
        if i % 5000 == 4999 {
            for _ in 0..5000 {
                c.read().unwrap();
            }
        }
    }
    // Concurrent transfers between accounts (different shards) while BGSAVE runs.
    let stop = Arc::new(AtomicBool::new(false));
    let port = s.port;
    let movers: Vec<_> = (0..4)
        .map(|t| {
            let stop = stop.clone();
            std::thread::spawn(move || {
                let mut c = Client::connect(port);
                let mut n = t as usize;
                while !stop.load(Ordering::Relaxed) {
                    let (a, b) = (n % ACCOUNTS, (n * 7919 + 13) % ACCOUNTS);
                    n += 4;
                    c.cmd(&["MULTI"]);
                    c.cmd(&["DECRBY", &format!("acct:{a}"), "3"]);
                    c.cmd(&["INCRBY", &format!("acct:{b}"), "3"]);
                    c.cmd(&["EXEC"]);
                }
            })
        })
        .collect();
    std::thread::sleep(Duration::from_millis(100));
    let before = c.cmd(&["LASTSAVE"]).int();
    assert!(matches!(c.cmd(&["BGSAVE"]), Reply::Simple(_)));
    assert!(wait_until(Duration::from_secs(60), || {
        let info = c.cmd(&["INFO", "persistence"]).text();
        info_field(&info, "rdb_bgsave_in_progress").as_deref() == Some("0")
    }));
    stop.store(true, Ordering::Relaxed);
    for m in movers {
        m.join().unwrap();
    }
    assert!(c.cmd(&["LASTSAVE"]).int() >= before);
    drop(c);
    drop(s);
    let s = Server::start_in(dir.path(), &[]);
    let mut c = s.client();
    let mut sum = 0i64;
    for chunk in (0..ACCOUNTS).collect::<Vec<_>>().chunks(500) {
        let keys: Vec<String> = chunk.iter().map(|i| format!("acct:{i}")).collect();
        let mut args = vec!["MGET"];
        args.extend(keys.iter().map(|k| k.as_str()));
        for v in c.cmd(&args).strings() {
            sum += v.parse::<i64>().unwrap();
        }
    }
    assert_eq!(
        sum, TOTAL,
        "snapshot is not a consistent point-in-time view"
    );
    assert_eq!(c.cmd(&["DBSIZE"]), Reply::Int(ACCOUNTS as i64 + 100_000));
}

fn aof_incr_files(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = std::fs::read_dir(dir.join("appendonlydir"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "aof"))
        .collect();
    v.sort();
    v
}

/// Writes in a loop until SIGKILL; every acknowledged INCR must survive.
fn sigkill_mid_write(policy: &str) {
    let dir = tempdir();
    let acked = Arc::new(AtomicU64::new(0));
    {
        let mut s = Server::start_in(
            dir.path(),
            &["--appendonly", "yes", "--appendfsync", policy],
        );
        let port = s.port;
        let a = acked.clone();
        let writer = std::thread::spawn(move || {
            let mut c = Client::connect(port);
            c.set_timeout(Duration::from_secs(2));
            loop {
                c.send(&["INCR", "counter"]);
                c.send(&["RPUSH", "log", "entry"]);
                match (c.read(), c.read()) {
                    (Ok(Reply::Int(n)), Ok(Reply::Int(_))) => a.store(n as u64, Ordering::SeqCst),
                    _ => break,
                }
            }
        });
        std::thread::sleep(Duration::from_millis(1000));
        s.kill9();
        writer.join().unwrap();
    }
    let acked = acked.load(Ordering::SeqCst);
    // With appendfsync always every batch waits for a real fsync
    // (F_FULLFSYNC on macOS, a few ms), so expect tens of writes, not thousands.
    assert!(acked > 20, "too few writes before the kill ({acked})");
    let s = Server::start_in(dir.path(), &["--appendonly", "yes"]);
    let mut c = s.client();
    let counter: u64 = c.cmd(&["GET", "counter"]).text().parse().unwrap();
    let log = c.cmd(&["LLEN", "log"]).int() as u64;
    assert!(
        counter >= acked && counter <= acked + 1,
        "{policy}: acked {acked}, recovered {counter}"
    );
    // INCR and RPUSH went out in one batch: they're both durable or both not.
    assert!(
        log == counter || log + 1 == counter || log == counter + 1,
        "counter {counter} log {log}"
    );
}

#[test]
fn aof_always_survives_sigkill() {
    sigkill_mid_write("always");
}

#[test]
fn aof_everysec_survives_process_crash() {
    // A process crash (not a power loss) loses nothing acknowledged: data is
    // written to the OS before replies are sent; fsync only matters for power loss.
    sigkill_mid_write("everysec");
}

#[test]
fn aof_no_fsync_survives_process_crash() {
    sigkill_mid_write("no");
}

#[test]
fn truncated_aof_tail_is_repaired_on_load() {
    let dir = tempdir();
    {
        let mut s = Server::start_in(
            dir.path(),
            &["--appendonly", "yes", "--appendfsync", "always"],
        );
        let mut c = s.client();
        for i in 0..100 {
            c.cmd(&["SET", &format!("k{i}"), "v"]);
        }
        s.terminate();
    }
    let incr = aof_incr_files(dir.path()).pop().unwrap();
    let good_len = std::fs::metadata(&incr).unwrap().len();
    // Simulate a torn write: half a command at the end of the file.
    std::fs::OpenOptions::new()
        .append(true)
        .open(&incr)
        .unwrap()
        .write_all(b"*3\r\n$3\r\nSET\r\n$4\r\nlost\r\n$5\r\nval")
        .unwrap();
    let s = Server::start_in(dir.path(), &["--appendonly", "yes"]);
    let mut c = s.client();
    assert_eq!(c.cmd(&["DBSIZE"]), Reply::Int(100));
    assert_eq!(c.cmd(&["GET", "lost"]), Reply::Nil);
    assert!(
        s.log_text().contains("short read while loading the AOF"),
        "{}",
        s.log_text()
    );
    assert_eq!(
        std::fs::metadata(&incr).unwrap().len(),
        good_len,
        "tail must be truncated"
    );
    // The repaired AOF keeps working.
    c.cmd(&["SET", "after", "repair"]);
    drop(c);
    drop(s);
    let s = Server::start_in(dir.path(), &["--appendonly", "yes"]);
    assert_eq!(s.client().cmd(&["GET", "after"]), bulk("repair"));
}

#[test]
fn truncated_aof_refused_when_aof_load_truncated_is_off() {
    let dir = tempdir();
    {
        let mut s = Server::start_in(dir.path(), &["--appendonly", "yes"]);
        s.client().cmd(&["SET", "a", "1"]);
        s.terminate();
    }
    let incr = aof_incr_files(dir.path()).pop().unwrap();
    std::fs::OpenOptions::new()
        .append(true)
        .open(&incr)
        .unwrap()
        .write_all(b"*2\r\n$3\r\nDEL")
        .unwrap();
    let r = Server::try_start_in(
        dir.path(),
        &["--appendonly", "yes", "--aof-load-truncated", "no"],
    );
    let err = r.err().expect("server must refuse to start");
    assert!(err.contains("Unexpected end of file"), "{err}");
}

#[test]
fn aof_incomplete_transaction_is_discarded() {
    let dir = tempdir();
    {
        let mut s = Server::start_in(dir.path(), &["--appendonly", "yes"]);
        let mut c = s.client();
        c.cmd(&["SET", "before", "1"]);
        s.terminate();
    }
    let incr = aof_incr_files(dir.path()).pop().unwrap();
    std::fs::OpenOptions::new()
        .append(true)
        .open(&incr)
        .unwrap()
        .write_all(b"*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$7\r\npartial\r\n$1\r\n1\r\n")
        .unwrap();
    let s = Server::start_in(dir.path(), &["--appendonly", "yes"]);
    let mut c = s.client();
    assert_eq!(c.cmd(&["GET", "before"]), bulk("1"));
    assert_eq!(
        c.cmd(&["GET", "partial"]),
        Reply::Nil,
        "uncommitted MULTI must not be applied"
    );
}

#[test]
fn corrupt_aof_in_the_middle_is_fatal() {
    let dir = tempdir();
    {
        let mut s = Server::start_in(dir.path(), &["--appendonly", "yes"]);
        let mut c = s.client();
        for i in 0..20 {
            c.cmd(&["SET", &format!("k{i}"), "v"]);
        }
        s.terminate();
    }
    let incr = aof_incr_files(dir.path()).pop().unwrap();
    let mut bytes = std::fs::read(&incr).unwrap();
    let mid = bytes.len() / 2;
    let star = mid + bytes[mid..].iter().position(|&b| b == b'*').unwrap();
    bytes[star] = b'#';
    std::fs::write(&incr, bytes).unwrap();
    let r = Server::try_start_in(dir.path(), &["--appendonly", "yes"]);
    assert!(
        r.is_err(),
        "corruption in the middle of the AOF must be fatal"
    );
}

#[test]
fn corrupt_snapshot_is_detected_by_crc() {
    let dir = tempdir();
    {
        let s = Server::start_in(dir.path(), &[]);
        let mut c = s.client();
        populate(&mut c);
        c.cmd(&["SAVE"]);
    }
    let path = dir.path().join("dump.rcsnap");
    let mut bytes = std::fs::read(&path).unwrap();
    let n = bytes.len();
    bytes[n / 2] ^= 0x40;
    std::fs::write(&path, bytes).unwrap();
    let err = Server::try_start_in(dir.path(), &[])
        .err()
        .expect("must refuse a corrupt snapshot");
    assert!(err.contains("checksum"), "{err}");
}

#[test]
fn bgrewriteaof_compacts_and_survives_sigkill() {
    let dir = tempdir();
    let expected;
    {
        let mut s = Server::start_in(dir.path(), &["--appendonly", "yes"]);
        let mut c = s.client();
        for _ in 0..20 {
            for i in 0..1000 {
                c.send(&["INCR", &format!("ctr:{i}")]);
            }
            for _ in 0..1000 {
                c.read().unwrap();
            }
        }
        let before: u64 = aof_incr_files(dir.path())
            .iter()
            .map(|p| std::fs::metadata(p).unwrap().len())
            .sum();
        // Keep writing while the rewrite runs.
        let port = s.port;
        let writer = std::thread::spawn(move || {
            let mut c = Client::connect(port);
            for i in 0..5000 {
                c.cmd(&["INCR", &format!("during:{}", i % 100)]);
            }
        });
        assert!(matches!(c.cmd(&["BGREWRITEAOF"]), Reply::Simple(_)));
        writer.join().unwrap();
        assert!(wait_until(Duration::from_secs(30), || {
            let info = c.cmd(&["INFO", "persistence"]).text();
            info_field(&info, "aof_rewrite_in_progress").as_deref() == Some("0")
        }));
        let manifest =
            std::fs::read_to_string(dir.path().join("appendonlydir/appendonly.aof.manifest"))
                .unwrap();
        assert!(manifest.contains("type b"), "{manifest}");
        assert_eq!(manifest.lines().count(), 2, "{manifest}");
        let after: u64 = aof_incr_files(dir.path())
            .iter()
            .map(|p| std::fs::metadata(p).unwrap().len())
            .sum();
        // Only commands after the snapshot instant (≤ the 5000 concurrent INCRs)
        // remain in the incr file; the 20k earlier ones moved into the base.
        assert!(
            after < before / 2,
            "incr AOF should shrink after rewrite: {before} -> {after}"
        );
        c.cmd(&["SET", "post-rewrite", "yes"]);
        expected = dump(&mut c);
        s.kill9();
    }
    let s = Server::start_in(dir.path(), &["--appendonly", "yes"]);
    let mut c = s.client();
    assert_eq!(dump(&mut c), expected);
    assert_eq!(c.cmd(&["GET", "ctr:7"]), bulk("20"));
}

#[test]
fn shutdown_and_sigterm_save_when_save_points_are_configured() {
    let dir = tempdir();
    {
        let mut s = Server::start_in(dir.path(), &["--save", "3600", "1"]);
        let mut c = s.client();
        c.cmd(&["SET", "via", "shutdown"]);
        c.send(&["SHUTDOWN"]);
        assert!(s.wait_exit(Duration::from_secs(10)));
    }
    {
        let mut s = Server::start_in(dir.path(), &["--save", "3600", "1"]);
        let mut c = s.client();
        assert_eq!(c.cmd(&["GET", "via"]), bulk("shutdown"));
        c.cmd(&["SET", "via", "sigterm"]);
        s.terminate();
    }
    let s = Server::start_in(dir.path(), &[]);
    assert_eq!(s.client().cmd(&["GET", "via"]), bulk("sigterm"));
}

#[test]
fn enabling_aof_at_runtime_captures_existing_data() {
    let dir = tempdir();
    {
        let s = Server::start_in(dir.path(), &[]);
        let mut c = s.client();
        populate(&mut c);
        assert_eq!(
            c.cmd(&["CONFIG", "SET", "appendonly", "yes"]),
            Reply::Simple("OK".into())
        );
        c.cmd(&["SET", "after-enable", "1"]);
        c.cmd(&["INCR", "after-enable"]);
        std::thread::sleep(Duration::from_millis(100));
    } // SIGKILL
    let s = Server::start_in(dir.path(), &["--appendonly", "yes"]);
    let mut c = s.client();
    assert_eq!(c.cmd(&["GET", "after-enable"]), bulk("2"));
    assert_eq!(c.cmd(&["LLEN", "big-list"]), Reply::Int(2000));
    assert_eq!(c.cmd(&["SELECT", "2"]), Reply::Simple("OK".into()));
    assert_eq!(c.cmd(&["LRANGE", "in-db2", "0", "-1"]).strings(), vec!["y"]);
}

#[test]
fn expired_keys_are_not_resurrected_by_restart() {
    let dir = tempdir();
    {
        let mut s = Server::start_in(dir.path(), &["--appendonly", "yes"]);
        let mut c = s.client();
        c.cmd(&["SET", "short", "v", "PX", "200"]);
        c.cmd(&["SET", "long", "v", "EX", "1000"]);
        c.cmd(&["SAVE"]);
        std::thread::sleep(Duration::from_millis(300));
        s.terminate();
    }
    let s = Server::start_in(dir.path(), &["--appendonly", "yes"]);
    let mut c = s.client();
    assert_eq!(c.cmd(&["EXISTS", "short"]), Reply::Int(0));
    assert_eq!(c.cmd(&["EXISTS", "long"]), Reply::Int(1));
}
