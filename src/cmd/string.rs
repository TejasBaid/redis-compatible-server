//! String commands.

use super::flags::*;
use super::{Command, Ctx, NOT_FLOAT, NOT_INT, OVERFLOW, SYNTAX, WRONGTYPE, c, k};
use crate::types::{RStr, Value};
use crate::util::{eq_ic, fmt_f64_human, parse_f64, parse_i64};

/// Maximum string size (`proto-max-bulk-len`).
pub const MAX_STRING: usize = 512 * 1024 * 1024;

pub(crate) fn commands() -> Vec<Command> {
    let g = "string";
    vec![
        c(
            "get",
            2,
            READONLY | FAST,
            k(1, 1, 1),
            get,
            g,
            "Returns the string value of a key.",
        ),
        c(
            "set",
            -3,
            WRITE | DENYOOM,
            k(1, 1, 1),
            set,
            g,
            "Sets the string value of a key, ignoring its type. The key is created if it doesn't exist.",
        ),
        c(
            "setnx",
            3,
            WRITE | DENYOOM | FAST,
            k(1, 1, 1),
            setnx,
            g,
            "Set the string value of a key only when the key doesn't exist.",
        ),
        c(
            "setex",
            4,
            WRITE | DENYOOM,
            k(1, 1, 1),
            setex,
            g,
            "Sets the string value and expiration time of a key. Creates the key if it doesn't exist.",
        ),
        c(
            "psetex",
            4,
            WRITE | DENYOOM,
            k(1, 1, 1),
            psetex,
            g,
            "Sets both string value and expiration time in milliseconds of a key. The key is created if it doesn't exist.",
        ),
        c(
            "getset",
            3,
            WRITE | DENYOOM | FAST,
            k(1, 1, 1),
            getset,
            g,
            "Returns the previous string value of a key after setting it to a new value.",
        ),
        c(
            "getdel",
            2,
            WRITE | FAST,
            k(1, 1, 1),
            getdel,
            g,
            "Returns the string value of a key after deleting the key.",
        ),
        c(
            "getex",
            -2,
            WRITE | FAST,
            k(1, 1, 1),
            getex,
            g,
            "Returns the string value of a key after setting its expiration time.",
        ),
        c(
            "mget",
            -2,
            READONLY | FAST,
            k(1, -1, 1),
            mget,
            g,
            "Atomically returns the string values of one or more keys.",
        ),
        c(
            "mset",
            -3,
            WRITE | DENYOOM,
            k(1, -1, 2),
            mset,
            g,
            "Atomically creates or modifies the string values of one or more keys.",
        ),
        c(
            "msetnx",
            -3,
            WRITE | DENYOOM,
            k(1, -1, 2),
            msetnx,
            g,
            "Atomically modifies the string values of one or more keys only when all keys don't exist.",
        ),
        c(
            "incr",
            2,
            WRITE | DENYOOM | FAST,
            k(1, 1, 1),
            incr,
            g,
            "Increments the integer value of a key by one. Uses 0 as initial value if the key doesn't exist.",
        ),
        c(
            "decr",
            2,
            WRITE | DENYOOM | FAST,
            k(1, 1, 1),
            decr,
            g,
            "Decrements the integer value of a key by one. Uses 0 as initial value if the key doesn't exist.",
        ),
        c(
            "incrby",
            3,
            WRITE | DENYOOM | FAST,
            k(1, 1, 1),
            incrby,
            g,
            "Increments the integer value of a key by a number. Uses 0 as initial value if the key doesn't exist.",
        ),
        c(
            "decrby",
            3,
            WRITE | DENYOOM | FAST,
            k(1, 1, 1),
            decrby,
            g,
            "Decrements a number from the integer value of a key. Uses 0 as initial value if the key doesn't exist.",
        ),
        c(
            "incrbyfloat",
            3,
            WRITE | DENYOOM | FAST,
            k(1, 1, 1),
            incrbyfloat,
            g,
            "Increment the floating point value of a key by a number. Uses 0 as initial value if the key doesn't exist.",
        ),
        c(
            "append",
            3,
            WRITE | DENYOOM | FAST,
            k(1, 1, 1),
            append,
            g,
            "Appends a string to the value of a key. Creates the key if it doesn't exist.",
        ),
        c(
            "strlen",
            2,
            READONLY | FAST,
            k(1, 1, 1),
            strlen,
            g,
            "Returns the length of a string value.",
        ),
        c(
            "getrange",
            4,
            READONLY,
            k(1, 1, 1),
            getrange,
            g,
            "Returns a substring of the string stored at a key.",
        ),
        c(
            "substr",
            4,
            READONLY,
            k(1, 1, 1),
            getrange,
            g,
            "Returns a substring from a string value.",
        ),
        c(
            "setrange",
            4,
            WRITE | DENYOOM,
            k(1, 1, 1),
            setrange,
            g,
            "Overwrites a part of a string value with another by an offset. Creates the key if it doesn't exist.",
        ),
    ]
}

fn get(ctx: &mut Ctx, args: &[&[u8]]) {
    match ctx.db.get(args[1]) {
        None => ctx.out.null(),
        Some(Value::Str(s)) => s.with_bytes(|b| ctx.out.bulk(b)),
        Some(_) => ctx.out.error(WRONGTYPE),
    }
}

/// Expiration options shared by SET and GETEX.
#[derive(Default)]
pub(crate) struct ExpireOpt {
    /// Absolute deadline in ms.
    pub at: Option<u64>,
    /// KEEPTTL.
    pub keep_ttl: bool,
    /// PERSIST (GETEX only).
    pub persist: bool,
}

/// Converts a relative/absolute expire argument into an absolute ms deadline.
/// Returns `Err(())` for non-positive or overflowing values.
pub(crate) fn deadline(now: u64, v: i64, unit_ms: i64, absolute: bool) -> Result<u64, ()> {
    if v <= 0 {
        return Err(());
    }
    let ms = v.checked_mul(unit_ms).ok_or(())?;
    if absolute {
        Ok(ms as u64)
    } else {
        (now as i64).checked_add(ms).map(|t| t as u64).ok_or(())
    }
}

fn set(ctx: &mut Ctx, args: &[&[u8]]) {
    let (key, val) = (args[1], args[2]);
    let (mut nx, mut xx, mut want_get) = (false, false, false);
    let mut exp = ExpireOpt::default();
    let mut unit: Option<(i64, bool)> = None;
    let mut i = 3;
    while i < args.len() {
        let a = args[i];
        let has_next = i + 1 < args.len();
        if eq_ic(a, "nx") && !xx {
            nx = true;
        } else if eq_ic(a, "xx") && !nx {
            xx = true;
        } else if eq_ic(a, "get") {
            want_get = true;
        } else if eq_ic(a, "keepttl") && unit.is_none() {
            exp.keep_ttl = true;
        } else if (eq_ic(a, "ex") || eq_ic(a, "px") || eq_ic(a, "exat") || eq_ic(a, "pxat"))
            && !exp.keep_ttl
            && unit.is_none()
            && has_next
        {
            let (u, abs) = match a.to_ascii_lowercase().as_slice() {
                b"ex" => (1000, false),
                b"px" => (1, false),
                b"exat" => (1000, true),
                _ => (1, true),
            };
            unit = Some((u, abs));
            i += 1;
            let Some(v) = ctx.int(args[i]) else { return };
            match deadline(ctx.db.now, v, u, abs) {
                Ok(t) => exp.at = Some(t),
                Err(()) => return ctx.out.error("ERR invalid expire time in 'set' command"),
            }
        } else {
            return ctx.out.error(SYNTAX);
        }
        i += 1;
    }
    // Plain SET (the hot path) never needs the old value.
    let old = if nx || xx || want_get {
        match ctx.db.get_mut(key) {
            Some(Value::Str(s)) => Some(s.to_vec()),
            Some(_) if want_get => return ctx.out.error(WRONGTYPE),
            Some(_) => Some(Vec::new()),
            None => None,
        }
    } else {
        None
    };
    let exists = old.is_some();
    if (nx && exists) || (xx && !exists) {
        if want_get {
            match old {
                Some(v) => ctx.out.bulk(&v),
                None => ctx.out.null(),
            }
        } else {
            ctx.out.null();
        }
        return;
    }
    if let Some(t) = exp.at
        && t <= ctx.db.now
        && !ctx.client.is_master
        && !ctx
            .server
            .loading
            .load(std::sync::atomic::Ordering::Relaxed)
    {
        // Already expired: the key ends up deleted (as in Redis).
        ctx.db.remove(key);
        ctx.db.modified(key);
        ctx.propagate_as(vec![vec![b"DEL".to_vec(), key.to_vec()]]);
        return match (want_get, old) {
            (true, Some(v)) => ctx.out.bulk(&v),
            (true, None) => ctx.out.null(),
            _ => ctx.out.ok(),
        };
    }
    ctx.db.set(key, Value::string(val), exp.keep_ttl);
    if let Some(t) = exp.at {
        ctx.db.set_expire(key, t);
        ctx.propagate_as(vec![vec![
            b"SET".to_vec(),
            key.to_vec(),
            val.to_vec(),
            b"PXAT".to_vec(),
            t.to_string().into_bytes(),
        ]]);
    } else if want_get || nx || xx {
        let mut cmd = vec![b"SET".to_vec(), key.to_vec(), val.to_vec()];
        if exp.keep_ttl {
            cmd.push(b"KEEPTTL".to_vec());
        }
        ctx.propagate_as(vec![cmd]);
    }
    ctx.db.modified(key);
    if want_get {
        match old {
            Some(v) => ctx.out.bulk(&v),
            None => ctx.out.null(),
        }
    } else {
        ctx.out.ok();
    }
}

fn setnx(ctx: &mut Ctx, args: &[&[u8]]) {
    if ctx.db.exists(args[1]) {
        return ctx.out.int(0);
    }
    ctx.db.set(args[1], Value::string(args[2]), false);
    ctx.db.modified(args[1]);
    ctx.out.int(1);
}

fn setex_generic(ctx: &mut Ctx, args: &[&[u8]], unit: i64, name: &str) {
    let Some(v) = ctx.int(args[2]) else { return };
    let Ok(t) = deadline(ctx.db.now, v, unit, false) else {
        return ctx
            .out
            .error(&format!("ERR invalid expire time in '{name}' command"));
    };
    ctx.db.set(args[1], Value::string(args[3]), false);
    ctx.db.set_expire(args[1], t);
    ctx.db.modified(args[1]);
    ctx.propagate_as(vec![vec![
        b"SET".to_vec(),
        args[1].to_vec(),
        args[3].to_vec(),
        b"PXAT".to_vec(),
        t.to_string().into_bytes(),
    ]]);
    ctx.out.ok();
}

fn setex(ctx: &mut Ctx, args: &[&[u8]]) {
    setex_generic(ctx, args, 1000, "setex");
}

fn psetex(ctx: &mut Ctx, args: &[&[u8]]) {
    setex_generic(ctx, args, 1, "psetex");
}

fn getset(ctx: &mut Ctx, args: &[&[u8]]) {
    let old = match ctx.db.get_mut(args[1]) {
        Some(Value::Str(s)) => Some(s.to_vec()),
        Some(_) => return ctx.out.error(WRONGTYPE),
        None => None,
    };
    ctx.db.set(args[1], Value::string(args[2]), false);
    ctx.db.modified(args[1]);
    match old {
        Some(v) => ctx.out.bulk(&v),
        None => ctx.out.null(),
    }
}

fn getdel(ctx: &mut Ctx, args: &[&[u8]]) {
    let v = match ctx.db.get_mut(args[1]) {
        Some(Value::Str(s)) => s.to_vec(),
        Some(_) => return ctx.out.error(WRONGTYPE),
        None => return ctx.out.null(),
    };
    ctx.db.remove(args[1]);
    ctx.db.modified(args[1]);
    ctx.propagate_as(vec![vec![b"DEL".to_vec(), args[1].to_vec()]]);
    ctx.out.bulk(&v);
}

fn getex(ctx: &mut Ctx, args: &[&[u8]]) {
    let key = args[1];
    let mut exp = ExpireOpt::default();
    let mut i = 2;
    let mut seen = false;
    while i < args.len() {
        let a = args[i];
        if eq_ic(a, "persist") && !seen {
            exp.persist = true;
            seen = true;
        } else if (eq_ic(a, "ex") || eq_ic(a, "px") || eq_ic(a, "exat") || eq_ic(a, "pxat"))
            && !seen
            && i + 1 < args.len()
        {
            let (u, abs) = match a.to_ascii_lowercase().as_slice() {
                b"ex" => (1000, false),
                b"px" => (1, false),
                b"exat" => (1000, true),
                _ => (1, true),
            };
            i += 1;
            let Some(v) = ctx.int(args[i]) else { return };
            match deadline(ctx.db.now, v, u, abs) {
                Ok(t) => exp.at = Some(t),
                Err(()) => return ctx.out.error("ERR invalid expire time in 'getex' command"),
            }
            seen = true;
        } else {
            return ctx.out.error(SYNTAX);
        }
        i += 1;
    }
    let v = match ctx.db.get(key) {
        Some(Value::Str(s)) => s.to_vec(),
        Some(_) => return ctx.out.error(WRONGTYPE),
        None => return ctx.out.null(),
    };
    if let Some(t) = exp.at {
        if t <= ctx.db.now {
            ctx.db.remove(key);
            ctx.propagate_as(vec![vec![b"DEL".to_vec(), key.to_vec()]]);
        } else {
            ctx.db.set_expire(key, t);
            ctx.propagate_as(vec![vec![
                b"PEXPIREAT".to_vec(),
                key.to_vec(),
                t.to_string().into_bytes(),
            ]]);
        }
        ctx.db.modified(key);
    } else if exp.persist && ctx.db.persist(key) {
        ctx.propagate_as(vec![vec![b"PERSIST".to_vec(), key.to_vec()]]);
        ctx.db.modified(key);
    }
    ctx.out.bulk(&v);
}

fn mget(ctx: &mut Ctx, args: &[&[u8]]) {
    ctx.out.array(args.len() - 1);
    for key in &args[1..] {
        match ctx.db.get(key) {
            Some(Value::Str(s)) => s.with_bytes(|b| ctx.out.bulk(b)),
            _ => ctx.out.null(),
        }
    }
}

fn mset(ctx: &mut Ctx, args: &[&[u8]]) {
    if args.len().is_multiple_of(2) {
        return ctx.arity_error("mset");
    }
    for kv in args[1..].chunks(2) {
        ctx.db.set(kv[0], Value::string(kv[1]), false);
        ctx.db.modified(kv[0]);
    }
    ctx.out.ok();
}

fn msetnx(ctx: &mut Ctx, args: &[&[u8]]) {
    if args.len().is_multiple_of(2) {
        return ctx.arity_error("msetnx");
    }
    if args[1..].chunks(2).any(|kv| ctx.db.exists(kv[0])) {
        return ctx.out.int(0);
    }
    for kv in args[1..].chunks(2) {
        ctx.db.set(kv[0], Value::string(kv[1]), false);
        ctx.db.modified(kv[0]);
    }
    ctx.out.int(1);
}

fn incr_by(ctx: &mut Ctx, key: &[u8], by: i64) {
    let cur = match ctx.db.get_mut(key) {
        Some(Value::Str(RStr::Int(v))) => {
            let Some(n) = v.checked_add(by) else {
                return ctx.out.error(OVERFLOW);
            };
            *v = n;
            ctx.db.modified(key);
            return ctx.out.int(n);
        }
        Some(Value::Str(s)) => match s.as_i64() {
            Some(v) => v,
            None => return ctx.out.error(NOT_INT),
        },
        Some(_) => return ctx.out.error(WRONGTYPE),
        None => 0,
    };
    let Some(n) = cur.checked_add(by) else {
        return ctx.out.error(OVERFLOW);
    };
    ctx.db.set(key, Value::Str(RStr::Int(n)), true);
    ctx.db.modified(key);
    ctx.out.int(n);
}

fn incr(ctx: &mut Ctx, args: &[&[u8]]) {
    incr_by(ctx, args[1], 1);
}

fn decr(ctx: &mut Ctx, args: &[&[u8]]) {
    incr_by(ctx, args[1], -1);
}

fn incrby(ctx: &mut Ctx, args: &[&[u8]]) {
    let Some(by) = ctx.int(args[2]) else { return };
    incr_by(ctx, args[1], by);
}

fn decrby(ctx: &mut Ctx, args: &[&[u8]]) {
    let Some(by) = ctx.int(args[2]) else { return };
    let Some(neg) = by.checked_neg() else {
        return ctx.out.error("ERR decrement would overflow");
    };
    incr_by(ctx, args[1], neg);
}

fn incrbyfloat(ctx: &mut Ctx, args: &[&[u8]]) {
    let key = args[1];
    let cur = match ctx.db.get_mut(key) {
        Some(Value::Str(s)) => match s.with_bytes(parse_f64) {
            Some(v) => v,
            None => return ctx.out.error(NOT_FLOAT),
        },
        Some(_) => return ctx.out.error(WRONGTYPE),
        None => 0.0,
    };
    let Some(by) = parse_f64(args[2]) else {
        return ctx.out.error(NOT_FLOAT);
    };
    let n = cur + by;
    if !n.is_finite() {
        return ctx.out.error("ERR increment would produce NaN or Infinity");
    }
    let s = fmt_f64_human(n);
    ctx.db.set(key, Value::string(s.as_bytes()), true);
    ctx.db.modified(key);
    ctx.propagate_as(vec![vec![
        b"SET".to_vec(),
        key.to_vec(),
        s.clone().into_bytes(),
        b"KEEPTTL".to_vec(),
    ]]);
    ctx.out.bulk(s.as_bytes());
}

fn append(ctx: &mut Ctx, args: &[&[u8]]) {
    let key = args[1];
    let mut v = match ctx.db.get_mut(key) {
        Some(Value::Str(s)) => s.to_vec(),
        Some(_) => return ctx.out.error(WRONGTYPE),
        None => {
            ctx.db.insert_new(key, Value::string(args[2]));
            ctx.db.modified(key);
            return ctx.out.int(args[2].len() as i64);
        }
    };
    if v.len() + args[2].len() > MAX_STRING {
        return ctx
            .out
            .error("ERR string exceeds maximum allowed size (proto-max-bulk-len)");
    }
    v.extend_from_slice(args[2]);
    let len = v.len();
    if let Some(Value::Str(s)) = ctx.db.get_mut(key) {
        *s = RStr::Raw(crate::sds::Sds::from_vec(v));
    }
    ctx.db.mark_raw(key);
    ctx.db.modified(key);
    ctx.out.int(len as i64);
}

fn strlen(ctx: &mut Ctx, args: &[&[u8]]) {
    match ctx.db.get(args[1]) {
        Some(Value::Str(s)) => ctx.out.int(s.len() as i64),
        Some(_) => ctx.out.error(WRONGTYPE),
        None => ctx.out.int(0),
    }
}

fn getrange(ctx: &mut Ctx, args: &[&[u8]]) {
    let (Some(mut start), Some(mut end)) = (parse_i64(args[2]), parse_i64(args[3])) else {
        return ctx.out.error(NOT_INT);
    };
    let v = match ctx.db.get(args[1]) {
        Some(Value::Str(s)) => s.to_vec(),
        Some(_) => return ctx.out.error(WRONGTYPE),
        None => return ctx.out.bulk(b""),
    };
    let len = v.len() as i64;
    if start < 0 && end < 0 && start > end {
        return ctx.out.bulk(b"");
    }
    if start < 0 {
        start += len;
    }
    if end < 0 {
        end += len;
    }
    start = start.max(0);
    end = end.max(0);
    if end >= len {
        end = len - 1;
    }
    if start > end || len == 0 {
        return ctx.out.bulk(b"");
    }
    ctx.out.bulk(&v[start as usize..=end as usize]);
}

fn setrange(ctx: &mut Ctx, args: &[&[u8]]) {
    let key = args[1];
    let Some(off) = parse_i64(args[2]) else {
        return ctx.out.error(NOT_INT);
    };
    if off < 0 {
        return ctx.out.error("ERR offset is out of range");
    }
    let val = args[3];
    let cur = match ctx.db.get_mut(key) {
        Some(Value::Str(s)) => Some(s.to_vec()),
        Some(_) => return ctx.out.error(WRONGTYPE),
        None => None,
    };
    if val.is_empty() {
        return ctx.out.int(cur.map_or(0, |v| v.len() as i64));
    }
    if off as usize + val.len() > MAX_STRING {
        return ctx
            .out
            .error("ERR string exceeds maximum allowed size (proto-max-bulk-len)");
    }
    let mut v = cur.unwrap_or_default();
    let off = off as usize;
    if v.len() < off + val.len() {
        v.resize(off + val.len(), 0);
    }
    v[off..off + val.len()].copy_from_slice(val);
    let len = v.len();
    ctx.db.set(
        key,
        Value::Str(RStr::Raw(crate::sds::Sds::from_vec(v))),
        true,
    );
    ctx.db.mark_raw(key);
    ctx.db.modified(key);
    ctx.out.int(len as i64);
}
