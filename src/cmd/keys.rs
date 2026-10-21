//! Generic keyspace commands (DEL, EXPIRE family, RENAME, SCAN, ...).

use super::flags::*;
use super::{Command, Ctx, NO_KEY, NOKEYS, NOT_INT, SYNTAX, c, k};
use crate::db::{NUM_SHARDS, lfu_decayed, lru_idle_ms, shard_of};
use crate::dict::hash_key;
use crate::sds::Sds;
use crate::types::{Value, clone_value};
use crate::util::{eq_ic, glob_match, parse_i64, parse_u64, rand_below};
use std::sync::atomic::Ordering::Relaxed;

pub(crate) fn commands() -> Vec<Command> {
    let g = "generic";
    vec![
        c(
            "del",
            -2,
            WRITE,
            k(1, -1, 1),
            del,
            g,
            "Deletes one or more keys.",
        ),
        c(
            "unlink",
            -2,
            WRITE | FAST,
            k(1, -1, 1),
            del,
            g,
            "Asynchronously deletes one or more keys.",
        ),
        c(
            "exists",
            -2,
            READONLY | FAST,
            k(1, -1, 1),
            exists,
            g,
            "Determines whether one or more keys exist.",
        ),
        c(
            "touch",
            -2,
            READONLY | FAST,
            k(1, -1, 1),
            touch,
            g,
            "Returns the number of existing keys out of those specified after updating the time they were last accessed.",
        ),
        c(
            "expire",
            -3,
            WRITE | FAST,
            k(1, 1, 1),
            expire,
            g,
            "Sets the expiration time of a key in seconds.",
        ),
        c(
            "pexpire",
            -3,
            WRITE | FAST,
            k(1, 1, 1),
            pexpire,
            g,
            "Sets the expiration time of a key in milliseconds.",
        ),
        c(
            "expireat",
            -3,
            WRITE | FAST,
            k(1, 1, 1),
            expireat,
            g,
            "Sets the expiration time of a key to a Unix timestamp.",
        ),
        c(
            "pexpireat",
            -3,
            WRITE | FAST,
            k(1, 1, 1),
            pexpireat,
            g,
            "Sets the expiration time of a key to a Unix milliseconds timestamp.",
        ),
        c(
            "ttl",
            2,
            READONLY | FAST,
            k(1, 1, 1),
            ttl,
            g,
            "Returns the expiration time in seconds of a key.",
        ),
        c(
            "pttl",
            2,
            READONLY | FAST,
            k(1, 1, 1),
            pttl,
            g,
            "Returns the expiration time in milliseconds of a key.",
        ),
        c(
            "expiretime",
            2,
            READONLY | FAST,
            k(1, 1, 1),
            expiretime,
            g,
            "Returns the expiration time of a key as a Unix timestamp.",
        ),
        c(
            "pexpiretime",
            2,
            READONLY | FAST,
            k(1, 1, 1),
            pexpiretime,
            g,
            "Returns the expiration time of a key as a Unix milliseconds timestamp.",
        ),
        c(
            "persist",
            2,
            WRITE | FAST,
            k(1, 1, 1),
            persist,
            g,
            "Removes the expiration time of a key.",
        ),
        c(
            "type",
            2,
            READONLY | FAST,
            k(1, 1, 1),
            type_cmd,
            g,
            "Determines the type of value stored at a key.",
        ),
        c(
            "rename",
            3,
            WRITE,
            k(1, 2, 1),
            rename,
            g,
            "Renames a key and overwrites the destination.",
        ),
        c(
            "renamenx",
            3,
            WRITE | FAST,
            k(1, 2, 1),
            renamenx,
            g,
            "Renames a key only when the target key name doesn't exist.",
        ),
        c(
            "move",
            3,
            WRITE | FAST,
            k(1, 1, 1),
            move_cmd,
            g,
            "Moves a key to another database.",
        ),
        c(
            "copy",
            -3,
            WRITE | DENYOOM,
            k(1, 2, 1),
            copy,
            g,
            "Copies the value of a key to a new key.",
        ),
        c(
            "keys",
            2,
            READONLY | SELF_LOCK,
            NOKEYS,
            keys,
            g,
            "Returns all key names that match a pattern.",
        ),
        c(
            "scan",
            -2,
            READONLY | SELF_LOCK,
            NOKEYS,
            scan,
            g,
            "Iterates over the key names in the database.",
        ),
        c(
            "randomkey",
            1,
            READONLY | SELF_LOCK,
            NOKEYS,
            randomkey,
            g,
            "Returns a random key name from the database.",
        ),
        c(
            "dbsize",
            1,
            READONLY | FAST | SELF_LOCK,
            NOKEYS,
            dbsize,
            "server",
            "Returns the number of keys in the database.",
        ),
        c(
            "flushdb",
            -1,
            WRITE | ALL_SHARDS,
            NOKEYS,
            flushdb,
            "server",
            "Remove all keys from the current database.",
        ),
        c(
            "flushall",
            -1,
            WRITE | ALL_SHARDS,
            NOKEYS,
            flushall,
            "server",
            "Removes all keys from all databases.",
        ),
        c(
            "swapdb",
            3,
            WRITE | FAST | ALL_SHARDS,
            NOKEYS,
            swapdb,
            "server",
            "Swaps two Redis databases.",
        ),
        c(
            "object",
            -2,
            READONLY,
            k(2, 2, 1),
            object,
            g,
            "A container for object introspection commands.",
        ),
    ]
}

fn del(ctx: &mut Ctx, args: &[&[u8]]) {
    let mut n = 0;
    for key in &args[1..] {
        if ctx.db.remove(key).is_some() {
            ctx.db.modified(key);
            n += 1;
        }
    }
    ctx.out.int(n);
}

fn exists(ctx: &mut Ctx, args: &[&[u8]]) {
    let n = args[1..].iter().filter(|k| ctx.db.exists(k)).count();
    ctx.out.int(n as i64);
}

fn touch(ctx: &mut Ctx, args: &[&[u8]]) {
    let n = args[1..].iter().filter(|k| ctx.db.get(k).is_some()).count();
    ctx.out.int(n as i64);
}

fn expire_generic(ctx: &mut Ctx, args: &[&[u8]], unit: i64, absolute: bool, name: &str) {
    let key = args[1];
    let (mut nx, mut xx, mut gt, mut lt) = (false, false, false, false);
    for a in &args[3..] {
        if eq_ic(a, "nx") {
            nx = true;
        } else if eq_ic(a, "xx") {
            xx = true;
        } else if eq_ic(a, "gt") {
            gt = true;
        } else if eq_ic(a, "lt") {
            lt = true;
        } else {
            return ctx.out.error(&format!(
                "ERR Unsupported option {}",
                String::from_utf8_lossy(a)
            ));
        }
    }
    if nx && (xx || gt || lt) {
        return ctx
            .out
            .error("ERR NX and XX, GT or LT options at the same time are not compatible");
    }
    if gt && lt {
        return ctx
            .out
            .error("ERR GT and LT options at the same time are not compatible");
    }
    let Some(v) = parse_i64(args[2]) else {
        return ctx.out.error(NOT_INT);
    };
    let invalid = || format!("ERR invalid expire time in '{name}' command");
    let Some(ms) = v.checked_mul(unit) else {
        return ctx.out.error(&invalid());
    };
    let when = if absolute {
        ms
    } else {
        match (ctx.db.now as i64).checked_add(ms) {
            Some(w) => w,
            None => return ctx.out.error(&invalid()),
        }
    };
    if !ctx.db.exists(key) {
        return ctx.out.int(0);
    }
    let cur = ctx.db.expire_of(key);
    let when_i = when;
    if nx && cur.is_some() || xx && cur.is_none() {
        return ctx.out.int(0);
    }
    if gt && cur.is_none_or(|c| when_i <= c as i64) {
        return ctx.out.int(0);
    }
    if lt && cur.is_some_and(|c| when_i >= c as i64) {
        return ctx.out.int(0);
    }
    if when <= ctx.db.now as i64 && !ctx.client.is_master && !ctx.server.loading.load(Relaxed) {
        ctx.db.remove(key);
        ctx.db.modified(key);
        ctx.propagate_as(vec![vec![b"DEL".to_vec(), key.to_vec()]]);
        return ctx.out.int(1);
    }
    ctx.db.set_expire(key, when.max(0) as u64);
    ctx.db.modified(key);
    let mut p = vec![
        b"PEXPIREAT".to_vec(),
        key.to_vec(),
        when.to_string().into_bytes(),
    ];
    p.extend(args[3..].iter().map(|a| a.to_vec()));
    ctx.propagate_as(vec![p]);
    ctx.out.int(1);
}

fn expire(ctx: &mut Ctx, args: &[&[u8]]) {
    expire_generic(ctx, args, 1000, false, "expire");
}
fn pexpire(ctx: &mut Ctx, args: &[&[u8]]) {
    expire_generic(ctx, args, 1, false, "pexpire");
}
fn expireat(ctx: &mut Ctx, args: &[&[u8]]) {
    expire_generic(ctx, args, 1000, true, "expireat");
}
fn pexpireat(ctx: &mut Ctx, args: &[&[u8]]) {
    expire_generic(ctx, args, 1, true, "pexpireat");
}

enum TtlKind {
    Ttl,
    Pttl,
    ExpireTime,
    PexpireTime,
}

fn ttl_generic(ctx: &mut Ctx, key: &[u8], kind: TtlKind) {
    if ctx.db.get(key).is_none() {
        return ctx.out.int(-2);
    }
    let Some(when) = ctx.db.expire_of(key) else {
        return ctx.out.int(-1);
    };
    let left = when.saturating_sub(ctx.db.now) as i64;
    ctx.out.int(match kind {
        TtlKind::Ttl => (left + 500) / 1000,
        TtlKind::Pttl => left,
        TtlKind::ExpireTime => (when / 1000) as i64,
        TtlKind::PexpireTime => when as i64,
    });
}

fn ttl(ctx: &mut Ctx, args: &[&[u8]]) {
    ttl_generic(ctx, args[1], TtlKind::Ttl);
}
fn pttl(ctx: &mut Ctx, args: &[&[u8]]) {
    ttl_generic(ctx, args[1], TtlKind::Pttl);
}
fn expiretime(ctx: &mut Ctx, args: &[&[u8]]) {
    ttl_generic(ctx, args[1], TtlKind::ExpireTime);
}
fn pexpiretime(ctx: &mut Ctx, args: &[&[u8]]) {
    ttl_generic(ctx, args[1], TtlKind::PexpireTime);
}

fn persist(ctx: &mut Ctx, args: &[&[u8]]) {
    if ctx.db.get_mut(args[1]).is_none() {
        return ctx.out.int(0);
    }
    if ctx.db.persist(args[1]) {
        ctx.db.modified(args[1]);
        ctx.out.int(1);
    } else {
        ctx.out.int(0);
    }
}

fn type_cmd(ctx: &mut Ctx, args: &[&[u8]]) {
    let t = match ctx.db.obj(args[1]) {
        Some(o) => o.value.type_name(),
        None => "none",
    };
    ctx.out.simple(t);
}

fn rename_generic(ctx: &mut Ctx, args: &[&[u8]], nx: bool) {
    let (src, dst) = (args[1], args[2]);
    if !ctx.db.exists(src) {
        return ctx.out.error(NO_KEY);
    }
    if src == dst {
        return if nx { ctx.out.int(0) } else { ctx.out.ok() };
    }
    if nx && ctx.db.exists(dst) {
        return ctx.out.int(0);
    }
    let ttl = ctx.db.expire_of(src);
    let raw = ctx.db.obj(src).is_some_and(|o| o.is_raw());
    let v = ctx.db.remove(src).expect("exists");
    let is_list_like = matches!(v, Value::List(_) | Value::ZSet(_) | Value::Stream(_));
    if ctx.db.remove(dst).is_some() {
        // Overwritten destination counts as a modification of dst.
    }
    ctx.db.set(dst, v, false);
    if raw {
        ctx.db.mark_raw(dst);
    }
    if let Some(t) = ttl {
        ctx.db.set_expire(dst, t);
    }
    ctx.db.modified(src);
    ctx.db.modified(dst);
    if is_list_like {
        ctx.db.signal_ready(dst);
    }
    if nx { ctx.out.int(1) } else { ctx.out.ok() }
}

fn rename(ctx: &mut Ctx, args: &[&[u8]]) {
    rename_generic(ctx, args, false);
}
fn renamenx(ctx: &mut Ctx, args: &[&[u8]]) {
    rename_generic(ctx, args, true);
}

fn parse_db(ctx: &mut Ctx, a: &[u8]) -> Option<usize> {
    let Some(v) = parse_i64(a) else {
        ctx.out.error(NOT_INT);
        return None;
    };
    if v < 0 || v as usize >= ctx.server.keyspace.databases {
        ctx.out.error("ERR DB index is out of range");
        return None;
    }
    Some(v as usize)
}

fn move_cmd(ctx: &mut Ctx, args: &[&[u8]]) {
    let key = args[1];
    let Some(dst_db) = parse_db(ctx, args[2]) else {
        return;
    };
    let src_db = ctx.db.sel;
    if dst_db == src_db {
        return ctx
            .out
            .error("ERR source and destination objects are the same");
    }
    if !ctx.db.exists(key) {
        return ctx.out.int(0);
    }
    ctx.db.sel = dst_db;
    let dst_exists = ctx.db.exists(key);
    ctx.db.sel = src_db;
    if dst_exists {
        return ctx.out.int(0);
    }
    let ttl = ctx.db.expire_of(key);
    let raw = ctx.db.obj(key).is_some_and(|o| o.is_raw());
    let v = ctx.db.remove(key).expect("exists");
    ctx.db.modified(key);
    ctx.db.sel = dst_db;
    ctx.db.set(key, v, false);
    if raw {
        ctx.db.mark_raw(key);
    }
    if let Some(t) = ttl {
        ctx.db.set_expire(key, t);
    }
    ctx.db.modified(key);
    ctx.db.dirty -= 1;
    ctx.db.sel = src_db;
    ctx.out.int(1);
}

fn copy(ctx: &mut Ctx, args: &[&[u8]]) {
    let (src, dst) = (args[1], args[2]);
    let mut dst_db = ctx.db.sel;
    let mut replace = false;
    let mut i = 3;
    while i < args.len() {
        if eq_ic(args[i], "replace") {
            replace = true;
        } else if eq_ic(args[i], "db") && i + 1 < args.len() {
            i += 1;
            let Some(d) = parse_db(ctx, args[i]) else {
                return;
            };
            dst_db = d;
        } else {
            return ctx.out.error(SYNTAX);
        }
        i += 1;
    }
    let src_db = ctx.db.sel;
    if src == dst && src_db == dst_db {
        return ctx
            .out
            .error("ERR source and destination objects are the same");
    }
    let lim = ctx.db.limits;
    let Some(v) = ctx.db.get(src).map(|v| clone_value(v, &lim)) else {
        return ctx.out.int(0);
    };
    let ttl = ctx.db.expire_of(src);
    let raw = ctx.db.obj(src).is_some_and(|o| o.is_raw());
    ctx.db.sel = dst_db;
    if ctx.db.exists(dst) && !replace {
        ctx.db.sel = src_db;
        return ctx.out.int(0);
    }
    ctx.db.set(dst, v, false);
    if raw {
        ctx.db.mark_raw(dst);
    }
    if let Some(t) = ttl {
        ctx.db.set_expire(dst, t);
    }
    ctx.db.modified(dst);
    ctx.db.signal_ready(dst);
    ctx.db.sel = src_db;
    ctx.out.int(1);
}

fn keys(ctx: &mut Ctx, args: &[&[u8]]) {
    let pat = args[1];
    let all = pat == b"*";
    let (sel, now) = (ctx.db.sel, ctx.db.now);
    let mut found: Vec<Sds> = Vec::new();
    for i in 0..NUM_SHARDS {
        ctx.db.with_shard(i, |sh| {
            let db = &sh.dbs[sel];
            for (key, obj) in db.main.iter() {
                if (all || glob_match(pat, key, false))
                    && !(obj.has_expire() && db.expires.get(key).is_some_and(|&t| t <= now))
                {
                    found.push(key.clone());
                }
            }
        });
    }
    ctx.out.array(found.len());
    for k in found {
        ctx.out.bulk(&k);
    }
}

/// Parsed `[MATCH p] [COUNT n] [TYPE t]` options shared by the SCAN family.
pub(crate) struct ScanOpts {
    pub pattern: Option<Vec<u8>>,
    pub count: usize,
    pub type_filter: Option<Vec<u8>>,
}

/// Parses SCAN-family options starting at `args[from]`.
pub(crate) fn parse_scan_opts(
    ctx: &mut Ctx,
    args: &[&[u8]],
    from: usize,
    allow_type: bool,
) -> Option<ScanOpts> {
    let mut o = ScanOpts {
        pattern: None,
        count: 10,
        type_filter: None,
    };
    let mut i = from;
    while i < args.len() {
        if i + 1 >= args.len() {
            ctx.out.error(SYNTAX);
            return None;
        }
        let a = args[i];
        if eq_ic(a, "count") {
            let Some(n) = parse_i64(args[i + 1]) else {
                ctx.out.error(NOT_INT);
                return None;
            };
            if n < 1 {
                ctx.out.error(SYNTAX);
                return None;
            }
            o.count = n as usize;
        } else if eq_ic(a, "match") {
            o.pattern = Some(args[i + 1].to_vec());
        } else if allow_type && eq_ic(a, "type") {
            o.type_filter = Some(args[i + 1].to_ascii_lowercase());
        } else {
            ctx.out.error(SYNTAX);
            return None;
        }
        i += 2;
    }
    Some(o)
}

/// Parses a SCAN cursor (unsigned 64-bit).
pub(crate) fn parse_cursor(ctx: &mut Ctx, a: &[u8]) -> Option<u64> {
    let v = std::str::from_utf8(a)
        .ok()
        .and_then(|s| s.parse::<u64>().ok());
    if v.is_none() {
        ctx.out.error("ERR invalid cursor");
    }
    v
}

fn scan(ctx: &mut Ctx, args: &[&[u8]]) {
    let Some(cursor) = parse_cursor(ctx, args[1]) else {
        return;
    };
    let Some(opts) = parse_scan_opts(ctx, args, 2, true) else {
        return;
    };
    let (sel, now) = (ctx.db.sel, ctx.db.now);
    let mut shard = (cursor % NUM_SHARDS as u64) as usize;
    let mut dc = cursor / NUM_SHARDS as u64;
    let mut out: Vec<Sds> = Vec::new();
    let mut visited = 0usize;
    let budget = opts.count;
    let next_cursor;
    loop {
        let mut keys: Vec<(Sds, &'static str, bool)> = Vec::new();
        dc = ctx.db.with_shard(shard, |sh| {
            let db = &sh.dbs[sel];
            db.main.scan(dc, |key, obj| {
                let expired = obj.has_expire() && db.expires.get(key).is_some_and(|&t| t <= now);
                keys.push((key.clone(), obj.value.type_name(), expired));
            })
        });
        for (key, t, expired) in keys {
            visited += 1;
            if expired {
                continue;
            }
            if let Some(p) = &opts.pattern
                && !glob_match(p, &key, false)
            {
                continue;
            }
            if let Some(tf) = &opts.type_filter
                && t.as_bytes() != tf.as_slice()
            {
                continue;
            }
            out.push(key);
        }
        if dc == 0 {
            shard += 1;
            if shard >= NUM_SHARDS {
                next_cursor = 0;
                break;
            }
        }
        if visited >= budget {
            next_cursor = dc * NUM_SHARDS as u64 + shard as u64;
            break;
        }
    }
    ctx.out.array(2);
    ctx.out.bulk(next_cursor.to_string().as_bytes());
    ctx.out.array(out.len());
    for k in out {
        ctx.out.bulk(&k);
    }
}

fn randomkey(ctx: &mut Ctx, _args: &[&[u8]]) {
    let (sel, now) = (ctx.db.sel, ctx.db.now);
    let start = rand_below(NUM_SHARDS as u64) as usize;
    for off in 0..NUM_SHARDS {
        let i = (start + off) % NUM_SHARDS;
        let found = ctx.db.with_shard(i, |sh| {
            let db = &sh.dbs[sel];
            for _ in 0..16 {
                match db.main.random_entry() {
                    None => return None,
                    Some((k, o)) => {
                        let expired =
                            o.has_expire() && db.expires.get(k).is_some_and(|&t| t <= now);
                        if !expired {
                            return Some(k.clone());
                        }
                    }
                }
            }
            None
        });
        if let Some(k) = found {
            return ctx.out.bulk(&k);
        }
    }
    ctx.out.null();
}

fn dbsize(ctx: &mut Ctx, _args: &[&[u8]]) {
    let sel = ctx.db.sel;
    let mut n = 0;
    for i in 0..NUM_SHARDS {
        n += ctx.db.with_shard(i, |sh| sh.dbs[sel].len());
    }
    ctx.out.int(n as i64);
}

fn flush_opts_ok(ctx: &mut Ctx, args: &[&[u8]]) -> bool {
    match args.len() {
        1 => true,
        2 if eq_ic(args[1], "async") || eq_ic(args[1], "sync") => true,
        _ => {
            ctx.out.error(SYNTAX);
            false
        }
    }
}

fn flushdb(ctx: &mut Ctx, args: &[&[u8]]) {
    if !flush_opts_ok(ctx, args) {
        return;
    }
    let sel = ctx.db.sel;
    let mut removed = 0;
    ctx.db.for_each_shard(|sh| {
        sh.touch_all_watched(Some(sel));
        removed += sh.dbs[sel].len();
        sh.dbs[sel].clear();
    });
    ctx.db.dirty += removed as u64 + 1;
    ctx.out.ok();
}

fn flushall(ctx: &mut Ctx, args: &[&[u8]]) {
    if !flush_opts_ok(ctx, args) {
        return;
    }
    let mut removed = 0;
    ctx.db.for_each_shard(|sh| {
        sh.touch_all_watched(None);
        for db in sh.dbs.iter_mut() {
            removed += db.len();
            db.clear();
        }
    });
    ctx.db.dirty += removed as u64 + 1;
    ctx.out.ok();
}

fn swapdb(ctx: &mut Ctx, args: &[&[u8]]) {
    let n = ctx.server.keyspace.databases as i64;
    let (Some(a), Some(b)) = (parse_i64(args[1]), parse_i64(args[2])) else {
        return ctx.out.error("ERR invalid first DB index");
    };
    if a < 0 || a >= n {
        return ctx.out.error("ERR DB index is out of range");
    }
    if b < 0 || b >= n {
        return ctx.out.error("ERR DB index is out of range");
    }
    let (a, b) = (a as usize, b as usize);
    if a != b {
        ctx.db.for_each_shard(|sh| {
            sh.touch_all_watched(Some(a));
            sh.touch_all_watched(Some(b));
            sh.dbs.swap(a, b);
            sh.touch_all_watched(Some(a));
            sh.touch_all_watched(Some(b));
        });
    }
    ctx.db.dirty += 1;
    ctx.out.ok();
}

fn object(ctx: &mut Ctx, args: &[&[u8]]) {
    let sub = args[1].to_ascii_lowercase();
    if sub == b"help" {
        let lines = [
            "OBJECT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            "ENCODING <key>",
            "    Return the kind of internal representation used in order to store the value",
            "    associated with a <key>.",
            "FREQ <key>",
            "    Return the access frequency index of the <key>. The returned integer is",
            "    proportional to the logarithm of the recent access frequency of the key.",
            "IDLETIME <key>",
            "    Return the idle time of the <key>, that is the approximated number of",
            "    seconds elapsed since the last access to the key.",
            "REFCOUNT <key>",
            "    Return the number of references of the value associated with the specified",
            "    <key>.",
            "HELP",
            "    Print this help.",
        ];
        ctx.out.array(lines.len());
        for l in lines {
            ctx.out.simple(l);
        }
        return;
    }
    if !matches!(
        sub.as_slice(),
        b"encoding" | b"refcount" | b"idletime" | b"freq"
    ) {
        return ctx.subcommand_error("object", args[1]);
    }
    if args.len() != 3 {
        return ctx.out.error(&format!(
            "ERR wrong number of arguments for 'object|{}' command",
            String::from_utf8_lossy(&sub)
        ));
    }
    let policy = ctx.server.config.policy();
    let decay = ctx.server.config.lfu_decay_time.load(Relaxed);
    let Some(obj) = ctx.db.obj(args[2]) else {
        return ctx.out.null();
    };
    match sub.as_slice() {
        b"encoding" => {
            let e = obj.encoding_name();
            ctx.out.bulk(e.as_bytes())
        }
        b"refcount" => ctx.out.int(1),
        b"idletime" => {
            if policy.is_lfu() {
                return ctx.out.error("ERR An LFU maxmemory policy is selected, idle time not tracked. Please note that when switching between policies at runtime LRU and LFU data will take some time to adjust.");
            }
            let idle = lru_idle_ms(obj.clock()) / 1000;
            ctx.out.int(idle as i64)
        }
        b"freq" => {
            if !policy.is_lfu() {
                return ctx.out.error("ERR An LFU maxmemory policy is not selected, access frequency not tracked. Please note that when switching between policies at runtime LRU and LFU data will take some time to adjust.");
            }
            let f = lfu_decayed(obj.clock(), decay);
            ctx.out.int(f as i64)
        }
        _ => ctx.subcommand_error("object", args[1]),
    }
}

/// Shard index of a key (used by commands that lock shards themselves).
pub fn shard_for(key: &[u8]) -> usize {
    shard_of(hash_key(key))
}

/// Parses an unsigned count argument with the standard error.
pub fn parse_count(ctx: &mut Ctx, a: &[u8]) -> Option<u64> {
    let v = parse_u64(a);
    if v.is_none() {
        ctx.out.error("ERR value is out of range, must be positive");
    }
    v
}
