//! Point-in-time snapshots without `fork()`.
//!
//! Redis snapshots by forking and letting the kernel's copy-on-write keep the
//! child's view frozen. A multi-threaded process cannot fork safely, so this
//! server does **copy-on-write at shard granularity** instead:
//!
//! 1. *Begin*: take every shard lock (in order) plus the propagation lock,
//!    record the replication offset / switch AOF files, and mark every shard
//!    "pending" for this job. The instant all locks are held is the snapshot's
//!    logical time.
//! 2. *Capture*: a background thread serializes shards one by one. A writer
//!    that wants to mutate a still-pending shard serializes it first
//!    ([`Shard::before_write`]), so every shard is captured exactly as it was
//!    at step 1. Readers never wait; a writer pays at most one shard's
//!    serialization (1/256th of the dataset).
//!
//! ## File format
//!
//! ```text
//! "RCLSNAP" version:u8
//! { 0xFA aux-key aux-value }*                       metadata
//! { 0xFE db:varint                                  select database
//!   { [0xFC expire-ms:u64le] type:u8 key value }* }*
//! 0xFF crc32:u32le                                  CRC32 of all prior bytes
//! ```
//!
//! Strings are `len:varint bytes`. Types: 0 string, 1 list, 2 set, 3 zset
//! (member + f64le score), 4 hash, 5 stream.

use crate::db::{NUM_SHARDS, Obj, Shard};
use crate::dict::hash_key;
use crate::propagate::PropInner;
use crate::sds::Sds;
use crate::server::Server;
use crate::types::stream::{Stream, StreamId};
use crate::types::{Hash, Limits, List, Set, Value, ZSet};
use parking_lot::Mutex;
use std::sync::Arc;

/// File magic.
pub const MAGIC: &[u8] = b"RCLSNAP";
/// Format version.
pub const VERSION: u8 = 1;

const OP_AUX: u8 = 0xFA;
const OP_EXPIRE: u8 = 0xFC;
const OP_SELECTDB: u8 = 0xFE;
const OP_EOF: u8 = 0xFF;

const T_STRING: u8 = 0;
const T_LIST: u8 = 1;
const T_SET: u8 = 2;
const T_ZSET: u8 = 3;
const T_HASH: u8 = 4;
const T_STREAM: u8 = 5;

/// An in-progress snapshot: one slot per shard, filled by whichever thread
/// serializes the shard first.
pub struct SnapshotJob {
    slots: Vec<Mutex<Option<Vec<u8>>>>,
    /// Logical time of the snapshot (keys already expired are skipped).
    pub now: u64,
}

impl SnapshotJob {
    fn new(now: u64) -> Self {
        SnapshotJob {
            slots: (0..NUM_SHARDS).map(|_| Mutex::new(None)).collect(),
            now,
        }
    }

    /// Serializes `shard` into its slot (called under the shard lock).
    pub fn capture(&self, shard: &Shard) {
        let bytes = serialize_shard(shard, self.now);
        *self.slots[shard.idx].lock() = Some(bytes);
    }

    fn take(&self, i: usize) -> Option<Vec<u8>> {
        self.slots[i].lock().take()
    }
}

fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8 & 0x7f) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn put_str(out: &mut Vec<u8>, s: &[u8]) {
    put_varint(out, s.len() as u64);
    out.extend_from_slice(s);
}

fn value_type(v: &Value) -> u8 {
    match v {
        Value::Str(_) => T_STRING,
        Value::List(_) => T_LIST,
        Value::Set(_) => T_SET,
        Value::ZSet(_) => T_ZSET,
        Value::Hash(_) => T_HASH,
        Value::Stream(_) => T_STREAM,
    }
}

fn put_body(out: &mut Vec<u8>, v: &Value) {
    match v {
        Value::Str(s) => {
            s.with_bytes(|b| put_str(out, b));
        }
        Value::List(l) => {
            put_varint(out, l.len() as u64);
            for it in l.iter() {
                put_str(out, it);
            }
        }
        Value::Set(s) => {
            put_varint(out, s.len() as u64);
            s.for_each(|m| put_str(out, m));
        }
        Value::ZSet(z) => {
            put_varint(out, z.len() as u64);
            z.for_each(|m, s| {
                put_str(out, m);
                out.extend_from_slice(&s.to_le_bytes());
            });
        }
        Value::Hash(h) => {
            put_varint(out, h.len() as u64);
            h.for_each(|f, v| {
                put_str(out, f);
                put_str(out, v);
            });
        }
        Value::Stream(s) => {
            put_varint(out, s.len() as u64);
            for (id, fields) in s.iter() {
                put_varint(out, id.ms);
                put_varint(out, id.seq);
                put_varint(out, fields.len() as u64);
                for f in fields {
                    put_str(out, f);
                }
            }
            put_varint(out, s.last_id.ms);
            put_varint(out, s.last_id.seq);
            put_varint(out, s.entries_added);
            put_varint(out, s.max_deleted_id.ms);
            put_varint(out, s.max_deleted_id.seq);
        }
    }
}

/// Serializes every database of one shard.
pub fn serialize_shard(shard: &Shard, now: u64) -> Vec<u8> {
    let mut out = Vec::new();
    for (d, db) in shard.dbs.iter().enumerate() {
        if db.is_empty() {
            continue;
        }
        out.push(OP_SELECTDB);
        put_varint(&mut out, d as u64);
        for (key, obj) in db.main.iter() {
            let exp = if obj.has_expire() {
                db.expires.get(key).copied()
            } else {
                None
            };
            if let Some(t) = exp {
                if t <= now {
                    continue;
                }
                out.push(OP_EXPIRE);
                out.extend_from_slice(&t.to_le_bytes());
            }
            out.push(value_type(&obj.value));
            put_str(&mut out, key);
            put_body(&mut out, &obj.value);
        }
    }
    out
}

/// Starts a snapshot: marks every shard pending and runs `attach` under the
/// propagation lock at the snapshot instant. Returns the job and the
/// `(replid, offset)` of the replication stream at that instant.
pub fn begin(
    server: &Server,
    attach: impl FnOnce(&mut PropInner),
) -> (Arc<SnapshotJob>, String, u64) {
    let job = Arc::new(SnapshotJob::new(crate::util::now_ms()));
    let mut guards = server.keyspace.lock_all();
    let (replid, offset) = {
        let mut p = server.prop.lock();
        attach(&mut p);
        server.prop.refresh_active(&p);
        (p.repl.replid.clone(), p.repl.offset)
    };
    for g in guards.iter_mut() {
        g.snap = Some(job.clone());
    }
    drop(guards);
    (job, replid, offset)
}

/// Serializes all shards of a job begun with [`begin`] and returns the file.
pub fn finish(server: &Server, job: &SnapshotJob, replid: &str, offset: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 << 16);
    out.extend_from_slice(MAGIC);
    out.push(VERSION);
    let aux = [
        ("redis-ver", crate::server::VERSION.to_string()),
        ("rclone-ver", env!("CARGO_PKG_VERSION").to_string()),
        ("ctime", (job.now / 1000).to_string()),
        ("repl-id", replid.to_string()),
        ("repl-offset", offset.to_string()),
    ];
    for (k, v) in aux {
        out.push(OP_AUX);
        put_str(&mut out, k.as_bytes());
        put_str(&mut out, v.as_bytes());
    }
    for i in 0..NUM_SHARDS {
        server.keyspace.lock(i).before_write();
        if let Some(b) = job.take(i) {
            out.extend_from_slice(&b);
        }
    }
    out.push(OP_EOF);
    let crc = crc32fast::hash(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    out
}

/// Takes a complete snapshot of the current dataset.
pub fn create(server: &Server, attach: impl FnOnce(&mut PropInner)) -> (Vec<u8>, String, u64) {
    let (job, replid, offset) = begin(server, attach);
    let bytes = finish(server, &job, &replid, offset);
    (bytes, replid, offset)
}

struct Reader<'a> {
    b: &'a [u8],
    p: usize,
}

impl Reader<'_> {
    fn u8(&mut self) -> Result<u8, String> {
        let v = *self.b.get(self.p).ok_or("unexpected end of snapshot")?;
        self.p += 1;
        Ok(v)
    }
    fn varint(&mut self) -> Result<u64, String> {
        let mut v = 0u64;
        let mut shift = 0;
        loop {
            let b = self.u8()?;
            if shift > 63 {
                return Err("bad varint".into());
            }
            v |= ((b & 0x7f) as u64) << shift;
            if b & 0x80 == 0 {
                return Ok(v);
            }
            shift += 7;
        }
    }
    fn bytes(&mut self, n: usize) -> Result<&[u8], String> {
        if self.p + n > self.b.len() {
            return Err("unexpected end of snapshot".into());
        }
        let s = &self.b[self.p..self.p + n];
        self.p += n;
        Ok(s)
    }
    fn str(&mut self) -> Result<&[u8], String> {
        let n = self.varint()? as usize;
        self.bytes(n)
    }
    fn u64le(&mut self) -> Result<u64, String> {
        Ok(u64::from_le_bytes(
            self.bytes(8)?.try_into().expect("8 bytes"),
        ))
    }
}

fn read_value(r: &mut Reader, t: u8, lim: &Limits) -> Result<Value, String> {
    Ok(match t {
        T_STRING => Value::string(r.str()?),
        T_LIST => {
            let n = r.varint()?;
            let mut items = Vec::with_capacity(n.min(1 << 20) as usize);
            for _ in 0..n {
                items.push(r.str()?.to_vec());
            }
            let refs: Vec<&[u8]> = items.iter().map(|i| &i[..]).collect();
            let mut l = List::new();
            l.push(&refs, false, lim);
            Value::List(Box::new(l))
        }
        T_SET => {
            let n = r.varint()? as usize;
            let mut members = Vec::with_capacity(n.min(1 << 20));
            for _ in 0..n {
                members.push(r.str()?.to_vec());
            }
            let mut s = match members.first() {
                Some(f) => Set::create(f, n, lim),
                None => Set::new(),
            };
            for m in &members {
                s.add(m, lim);
            }
            Value::Set(Box::new(s))
        }
        T_ZSET => {
            let n = r.varint()? as usize;
            let mut items = Vec::with_capacity(n.min(1 << 20));
            for _ in 0..n {
                let m = r.str()?.to_vec();
                let s = f64::from_le_bytes(r.bytes(8)?.try_into().expect("8 bytes"));
                items.push((m, s));
            }
            let max_len = items.iter().map(|(m, _)| m.len()).max().unwrap_or(0);
            let mut z = ZSet::create(n, max_len, lim);
            for (m, s) in &items {
                z.insert(m, *s, lim);
            }
            Value::ZSet(Box::new(z))
        }
        T_HASH => {
            let n = r.varint()? as usize;
            let mut args = Vec::with_capacity(n.min(1 << 20) * 2);
            for _ in 0..n {
                args.push(r.str()?.to_vec());
                args.push(r.str()?.to_vec());
            }
            let refs: Vec<&[u8]> = args.iter().map(|a| &a[..]).collect();
            let mut h = Hash::new();
            h.try_conversion(&refs, lim);
            for kv in refs.chunks(2) {
                h.set(kv[0], kv[1], lim);
            }
            Value::Hash(Box::new(h))
        }
        T_STREAM => {
            let n = r.varint()?;
            let mut s = Stream::new();
            for _ in 0..n {
                let id = StreamId {
                    ms: r.varint()?,
                    seq: r.varint()?,
                };
                let nf = r.varint()?;
                let mut fields = Vec::with_capacity(nf.min(1 << 16) as usize);
                for _ in 0..nf {
                    fields.push(Sds::from_slice(r.str()?));
                }
                s.append(id, fields);
            }
            s.last_id = StreamId {
                ms: r.varint()?,
                seq: r.varint()?,
            };
            s.entries_added = r.varint()?;
            s.max_deleted_id = StreamId {
                ms: r.varint()?,
                seq: r.varint()?,
            };
            Value::Stream(Box::new(s))
        }
        other => return Err(format!("unknown value type {other}")),
    })
}

/// Metadata stored in a snapshot.
pub type Aux = Vec<(String, String)>;

/// Parses a snapshot, verifying magic, version and CRC, calling `sink` for
/// every key.
pub fn parse(
    bytes: &[u8],
    lim: &Limits,
    mut sink: impl FnMut(usize, &[u8], Value, Option<u64>),
) -> Result<Aux, String> {
    if bytes.len() < MAGIC.len() + 1 + 1 + 4 || &bytes[..MAGIC.len()] != MAGIC {
        return Err("not a redis-clone snapshot (bad magic)".into());
    }
    let body = &bytes[..bytes.len() - 4];
    let want = u32::from_le_bytes(bytes[bytes.len() - 4..].try_into().expect("4 bytes"));
    let got = crc32fast::hash(body);
    if got != want {
        return Err(format!(
            "snapshot checksum mismatch (expected {want:08x}, got {got:08x})"
        ));
    }
    let mut r = Reader {
        b: body,
        p: MAGIC.len(),
    };
    let ver = r.u8()?;
    if ver != VERSION {
        return Err(format!("unsupported snapshot version {ver}"));
    }
    let mut aux = Vec::new();
    let mut db = 0usize;
    loop {
        let op = r.u8()?;
        match op {
            OP_EOF => break,
            OP_AUX => {
                let k = String::from_utf8_lossy(r.str()?).into_owned();
                let v = String::from_utf8_lossy(r.str()?).into_owned();
                aux.push((k, v));
            }
            OP_SELECTDB => db = r.varint()? as usize,
            OP_EXPIRE => {
                let t = r.u64le()?;
                let ty = r.u8()?;
                let key = r.str()?.to_vec();
                let v = read_value(&mut r, ty, lim)?;
                sink(db, &key, v, Some(t));
            }
            ty => {
                let key = r.str()?.to_vec();
                let v = read_value(&mut r, ty, lim)?;
                sink(db, &key, v, None);
            }
        }
    }
    if r.p != body.len() {
        return Err("trailing bytes after snapshot EOF".into());
    }
    Ok(aux)
}

/// Loads a snapshot into the keyspace. Keys already expired are dropped when
/// `skip_expired` (masters do; replicas keep them for the master to delete).
pub fn load_into(
    server: &Server,
    bytes: &[u8],
    skip_expired: bool,
) -> Result<(usize, Aux), String> {
    let lim = server.config.limits();
    let now = crate::util::now_ms();
    let clock = crate::db::lru_clock();
    let mut n = 0;
    let databases = server.keyspace.databases;
    let aux = parse(bytes, &lim, |db, key, v, exp| {
        if db >= databases || (skip_expired && exp.is_some_and(|t| t <= now)) {
            return;
        }
        let h = hash_key(key);
        let mut sh = server.keyspace.lock(crate::db::shard_of(h));
        let d = &mut sh.dbs[db];
        d.remove(h, key);
        d.insert_new(h, Sds::from_slice(key), Obj::new(v, clock));
        if let Some(t) = exp {
            d.set_expire(h, key, t);
        }
        n += 1;
    })?;
    Ok((n, aux))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Keyspace;

    #[test]
    fn shard_roundtrip_and_crc() {
        let ks = Keyspace::new(16);
        let lim = Limits::default();
        let mut entries: Vec<(usize, Vec<u8>)> = Vec::new();
        for i in 0..200u32 {
            let key = format!("k{i}").into_bytes();
            let h = hash_key(&key);
            let mut sh = ks.lock(crate::db::shard_of(h));
            let db = (i % 3) as usize;
            let v = match i % 6 {
                0 => Value::string(format!("v{i}").as_bytes()),
                1 => {
                    let mut l = List::new();
                    l.push(&[b"a", b"b"], false, &lim);
                    Value::List(Box::new(l))
                }
                2 => {
                    let mut s = Set::new();
                    s.add(b"1", &lim);
                    s.add(b"x", &lim);
                    Value::Set(Box::new(s))
                }
                3 => {
                    let mut z = ZSet::default();
                    z.insert(b"m", 1.5, &lim);
                    Value::ZSet(Box::new(z))
                }
                4 => {
                    let mut hh = Hash::new();
                    hh.set(b"f", b"v", &lim);
                    Value::Hash(Box::new(hh))
                }
                _ => {
                    let mut s = Stream::new();
                    s.append(
                        StreamId { ms: 1, seq: 1 },
                        vec![Sds::from("f"), Sds::from("v")],
                    );
                    Value::Stream(Box::new(s))
                }
            };
            sh.dbs[db].insert_new(h, Sds::from_slice(&key), Obj::new(v, 0));
            if i % 5 == 0 {
                sh.dbs[db].set_expire(h, &key, u64::MAX / 2);
            }
            entries.push((db, key));
        }
        let mut out = MAGIC.to_vec();
        out.push(VERSION);
        for i in 0..NUM_SHARDS {
            out.extend(serialize_shard(&ks.lock(i), 0));
        }
        out.push(OP_EOF);
        let crc = crc32fast::hash(&out);
        out.extend_from_slice(&crc.to_le_bytes());
        let mut seen = Vec::new();
        parse(&out, &lim, |db, key, v, exp| {
            seen.push((db, key.to_vec(), v.type_name(), exp.is_some()));
        })
        .unwrap();
        assert_eq!(seen.len(), 200);
        assert_eq!(seen.iter().filter(|e| e.3).count(), 40);
        // Flip one bit: the CRC must catch it.
        let mut bad = out.clone();
        bad[20] ^= 1;
        assert!(
            parse(&bad, &lim, |_, _, _, _| {})
                .unwrap_err()
                .contains("checksum")
        );
        // Truncation is detected too.
        assert!(parse(&out[..out.len() - 10], &lim, |_, _, _, _| {}).is_err());
    }
}
