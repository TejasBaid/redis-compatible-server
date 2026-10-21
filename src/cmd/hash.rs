//! Hash commands.

use super::flags::*;
use super::keys::{parse_cursor, parse_scan_opts};
use super::{Command, Ctx, NOT_FLOAT, NOT_INT, OVERFLOW, SYNTAX, WRONGTYPE, c, k};
use crate::types::{Hash, Value};
use crate::util::{eq_ic, fmt_f64_human, glob_match, parse_f64, parse_i64};

pub(crate) fn commands() -> Vec<Command> {
    let g = "hash";
    vec![
        c(
            "hset",
            -4,
            WRITE | DENYOOM | FAST,
            k(1, 1, 1),
            hset,
            g,
            "Creates or modifies the value of a field in a hash.",
        ),
        c(
            "hmset",
            -4,
            WRITE | DENYOOM | FAST,
            k(1, 1, 1),
            hmset,
            g,
            "Sets the values of multiple fields.",
        ),
        c(
            "hsetnx",
            4,
            WRITE | DENYOOM | FAST,
            k(1, 1, 1),
            hsetnx,
            g,
            "Sets the value of a field in a hash only when the field doesn't exist.",
        ),
        c(
            "hget",
            3,
            READONLY | FAST,
            k(1, 1, 1),
            hget,
            g,
            "Returns the value of a field in a hash.",
        ),
        c(
            "hmget",
            -3,
            READONLY | FAST,
            k(1, 1, 1),
            hmget,
            g,
            "Returns the values of all fields in a hash.",
        ),
        c(
            "hdel",
            -3,
            WRITE | FAST,
            k(1, 1, 1),
            hdel,
            g,
            "Deletes one or more fields and their values from a hash. Deletes the hash if no fields remain.",
        ),
        c(
            "hgetall",
            2,
            READONLY,
            k(1, 1, 1),
            hgetall,
            g,
            "Returns all fields and values in a hash.",
        ),
        c(
            "hkeys",
            2,
            READONLY,
            k(1, 1, 1),
            hkeys,
            g,
            "Returns all fields in a hash.",
        ),
        c(
            "hvals",
            2,
            READONLY,
            k(1, 1, 1),
            hvals,
            g,
            "Returns all values in a hash.",
        ),
        c(
            "hlen",
            2,
            READONLY | FAST,
            k(1, 1, 1),
            hlen,
            g,
            "Returns the number of fields in a hash.",
        ),
        c(
            "hexists",
            3,
            READONLY | FAST,
            k(1, 1, 1),
            hexists,
            g,
            "Determines whether a field exists in a hash.",
        ),
        c(
            "hstrlen",
            3,
            READONLY | FAST,
            k(1, 1, 1),
            hstrlen,
            g,
            "Returns the length of the value of a field.",
        ),
        c(
            "hincrby",
            4,
            WRITE | DENYOOM | FAST,
            k(1, 1, 1),
            hincrby,
            g,
            "Increments the integer value of a field in a hash by a number. Uses 0 as initial value if the field doesn't exist.",
        ),
        c(
            "hincrbyfloat",
            4,
            WRITE | DENYOOM | FAST,
            k(1, 1, 1),
            hincrbyfloat,
            g,
            "Increments the floating point value of a field by a number. Uses 0 as initial value if the field doesn't exist.",
        ),
        c(
            "hscan",
            -3,
            READONLY,
            k(1, 1, 1),
            hscan,
            g,
            "Iterates over fields and values of a hash.",
        ),
        c(
            "hrandfield",
            -2,
            READONLY,
            k(1, 1, 1),
            hrandfield,
            g,
            "Returns one or more random fields from a hash.",
        ),
    ]
}

/// Returns the hash at `key`, creating it if needed. `Err` = wrong type.
fn hash_for_write<'a>(ctx: &'a mut Ctx, key: &[u8]) -> Result<&'a mut Hash, ()> {
    if ctx.db.get_mut(key).is_none() {
        ctx.db.insert_new(key, Value::Hash(Box::default()));
    }
    match ctx.db.get_mut(key) {
        Some(Value::Hash(h)) => Ok(h),
        _ => Err(()),
    }
}

fn hset_generic(ctx: &mut Ctx, args: &[&[u8]], name: &str) -> Option<usize> {
    if !args.len().is_multiple_of(2) {
        ctx.arity_error(name);
        return None;
    }
    match ctx.db.get(args[1]) {
        Some(Value::Hash(_)) | None => {}
        Some(_) => {
            ctx.out.error(WRONGTYPE);
            return None;
        }
    }
    let lim = ctx.db.limits;
    let h = hash_for_write(ctx, args[1]).ok()?;
    h.try_conversion(&args[2..], &lim);
    let mut added = 0;
    for kv in args[2..].chunks(2) {
        if h.set(kv[0], kv[1], &lim) {
            added += 1;
        }
    }
    ctx.db.modified(args[1]);
    Some(added)
}

fn hset(ctx: &mut Ctx, args: &[&[u8]]) {
    if let Some(n) = hset_generic(ctx, args, "hset") {
        ctx.out.int(n as i64);
    }
}

fn hmset(ctx: &mut Ctx, args: &[&[u8]]) {
    if hset_generic(ctx, args, "hmset").is_some() {
        ctx.out.ok();
    }
}

fn hsetnx(ctx: &mut Ctx, args: &[&[u8]]) {
    match ctx.db.get(args[1]) {
        Some(Value::Hash(h)) if h.contains(args[2]) => return ctx.out.int(0),
        Some(Value::Hash(_)) | None => {}
        Some(_) => return ctx.out.error(WRONGTYPE),
    }
    let lim = ctx.db.limits;
    let Ok(h) = hash_for_write(ctx, args[1]) else {
        return;
    };
    h.try_conversion(&args[2..4], &lim);
    h.set(args[2], args[3], &lim);
    ctx.db.modified(args[1]);
    ctx.out.int(1);
}

fn hget(ctx: &mut Ctx, args: &[&[u8]]) {
    match ctx.db.get(args[1]) {
        Some(Value::Hash(h)) => match h.get(args[2]) {
            Some(v) => ctx.out.bulk(v),
            None => ctx.out.null(),
        },
        Some(_) => ctx.out.error(WRONGTYPE),
        None => ctx.out.null(),
    }
}

fn hmget(ctx: &mut Ctx, args: &[&[u8]]) {
    let h = match ctx.db.get(args[1]) {
        Some(Value::Hash(h)) => Some(h),
        Some(_) => return ctx.out.error(WRONGTYPE),
        None => None,
    };
    ctx.out.array(args.len() - 2);
    for f in &args[2..] {
        match h.and_then(|h| h.get(f)) {
            Some(v) => ctx.out.bulk(v),
            None => ctx.out.null(),
        }
    }
}

fn hdel(ctx: &mut Ctx, args: &[&[u8]]) {
    let n = match ctx.db.get_mut(args[1]) {
        Some(Value::Hash(h)) => args[2..].iter().filter(|f| h.remove(f)).count(),
        Some(_) => return ctx.out.error(WRONGTYPE),
        None => return ctx.out.int(0),
    };
    if n > 0 {
        ctx.db.delete_if_empty(args[1]);
        ctx.db.modified(args[1]);
    }
    ctx.out.int(n as i64);
}

fn hgetall_generic(ctx: &mut Ctx, args: &[&[u8]], fields: bool, values: bool) {
    let h = match ctx.db.get(args[1]) {
        Some(Value::Hash(h)) => h,
        Some(_) => return ctx.out.error(WRONGTYPE),
        None => {
            return if fields && values {
                ctx.out.map(0)
            } else {
                ctx.out.array(0)
            };
        }
    };
    if fields && values {
        ctx.out.map(h.len());
    } else {
        ctx.out.array(h.len());
    }
    let out = &mut ctx.out;
    h.for_each(|f, v| {
        if fields {
            out.bulk(f);
        }
        if values {
            out.bulk(v);
        }
    });
}

fn hgetall(ctx: &mut Ctx, args: &[&[u8]]) {
    hgetall_generic(ctx, args, true, true);
}
fn hkeys(ctx: &mut Ctx, args: &[&[u8]]) {
    hgetall_generic(ctx, args, true, false);
}
fn hvals(ctx: &mut Ctx, args: &[&[u8]]) {
    hgetall_generic(ctx, args, false, true);
}

fn hlen(ctx: &mut Ctx, args: &[&[u8]]) {
    match ctx.db.get(args[1]) {
        Some(Value::Hash(h)) => ctx.out.int(h.len() as i64),
        Some(_) => ctx.out.error(WRONGTYPE),
        None => ctx.out.int(0),
    }
}

fn hexists(ctx: &mut Ctx, args: &[&[u8]]) {
    match ctx.db.get(args[1]) {
        Some(Value::Hash(h)) => ctx.out.int(h.contains(args[2]) as i64),
        Some(_) => ctx.out.error(WRONGTYPE),
        None => ctx.out.int(0),
    }
}

fn hstrlen(ctx: &mut Ctx, args: &[&[u8]]) {
    match ctx.db.get(args[1]) {
        Some(Value::Hash(h)) => ctx.out.int(h.get(args[2]).map_or(0, |v| v.len()) as i64),
        Some(_) => ctx.out.error(WRONGTYPE),
        None => ctx.out.int(0),
    }
}

fn hincrby(ctx: &mut Ctx, args: &[&[u8]]) {
    let Some(by) = parse_i64(args[3]) else {
        return ctx.out.error(NOT_INT);
    };
    let cur = match ctx.db.get(args[1]) {
        Some(Value::Hash(h)) => match h.get(args[2]) {
            Some(v) => match parse_i64(v) {
                Some(n) => n,
                None => return ctx.out.error("ERR hash value is not an integer"),
            },
            None => 0,
        },
        Some(_) => return ctx.out.error(WRONGTYPE),
        None => 0,
    };
    let Some(n) = cur.checked_add(by) else {
        return ctx.out.error(OVERFLOW);
    };
    let lim = ctx.db.limits;
    let Ok(h) = hash_for_write(ctx, args[1]) else {
        return;
    };
    let mut b = itoa::Buffer::new();
    h.set(args[2], b.format(n).as_bytes(), &lim);
    ctx.db.modified(args[1]);
    ctx.out.int(n);
}

fn hincrbyfloat(ctx: &mut Ctx, args: &[&[u8]]) {
    let Some(by) = parse_f64(args[3]) else {
        return ctx.out.error(NOT_FLOAT);
    };
    let cur = match ctx.db.get(args[1]) {
        Some(Value::Hash(h)) => match h.get(args[2]) {
            Some(v) => match parse_f64(v) {
                Some(n) => n,
                None => return ctx.out.error("ERR hash value is not a float"),
            },
            None => 0.0,
        },
        Some(_) => return ctx.out.error(WRONGTYPE),
        None => 0.0,
    };
    let n = cur + by;
    if !n.is_finite() {
        return ctx.out.error("ERR increment would produce NaN or Infinity");
    }
    let s = fmt_f64_human(n);
    let lim = ctx.db.limits;
    let Ok(h) = hash_for_write(ctx, args[1]) else {
        return;
    };
    h.set(args[2], s.as_bytes(), &lim);
    ctx.db.modified(args[1]);
    ctx.propagate_as(vec![vec![
        b"HSET".to_vec(),
        args[1].to_vec(),
        args[2].to_vec(),
        s.clone().into_bytes(),
    ]]);
    ctx.out.bulk(s.as_bytes());
}

fn hscan(ctx: &mut Ctx, args: &[&[u8]]) {
    let Some(cursor) = parse_cursor(ctx, args[2]) else {
        return;
    };
    let mut novalues = false;
    let mut rest: Vec<&[u8]> = args[..3].to_vec();
    for a in &args[3..] {
        if eq_ic(a, "novalues") {
            novalues = true;
        } else {
            rest.push(a);
        }
    }
    let Some(opts) = parse_scan_opts(ctx, &rest, 3, false) else {
        return;
    };
    let h = match ctx.db.get(args[1]) {
        Some(Value::Hash(h)) => h,
        Some(_) => return ctx.out.error(WRONGTYPE),
        None => {
            ctx.out.array(2);
            ctx.out.bulk(b"0");
            return ctx.out.array(0);
        }
    };
    let mut items: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let next = h.scan(cursor, opts.count, |f, v| {
        if opts
            .pattern
            .as_ref()
            .is_none_or(|p| glob_match(p, f, false))
        {
            items.push((f.to_vec(), v.to_vec()));
        }
    });
    ctx.out.array(2);
    ctx.out.bulk(next.to_string().as_bytes());
    ctx.out.array(items.len() * if novalues { 1 } else { 2 });
    for (f, v) in items {
        ctx.out.bulk(&f);
        if !novalues {
            ctx.out.bulk(&v);
        }
    }
}

fn hrandfield(ctx: &mut Ctx, args: &[&[u8]]) {
    let (count, withvalues) = match args.len() {
        2 => (None, false),
        3 => match parse_i64(args[2]) {
            Some(c) => (Some(c), false),
            None => return ctx.out.error(NOT_INT),
        },
        4 if eq_ic(args[3], "withvalues") => match parse_i64(args[2]) {
            Some(c) => (Some(c), true),
            None => return ctx.out.error(NOT_INT),
        },
        _ => return ctx.out.error(SYNTAX),
    };
    if let Some(c) = count
        && !(-(i64::MAX / 2)..=i64::MAX / 2).contains(&c)
    {
        return ctx.out.error("ERR value is out of range");
    }
    let h = match ctx.db.get(args[1]) {
        Some(Value::Hash(h)) => h,
        Some(_) => return ctx.out.error(WRONGTYPE),
        None => {
            return if count.is_some() {
                ctx.out.array(0)
            } else {
                ctx.out.null()
            };
        }
    };
    let Some(c) = count else {
        let (f, _) = h.random_pair().expect("non-empty");
        return ctx.out.bulk(&f);
    };
    let pairs: Vec<(Vec<u8>, Vec<u8>)> = if c >= 0 {
        let mut all = h.pairs();
        let n = (c as usize).min(all.len());
        // Partial Fisher-Yates for a uniform sample without repetition.
        for i in 0..n {
            let j = i + crate::util::rand_below((all.len() - i) as u64) as usize;
            all.swap(i, j);
        }
        all.truncate(n);
        all
    } else {
        (0..c.unsigned_abs())
            .filter_map(|_| h.random_pair())
            .collect()
    };
    let resp3 = ctx.out.resp3;
    if withvalues && resp3 {
        ctx.out.array(pairs.len());
        for (f, v) in pairs {
            ctx.out.array(2);
            ctx.out.bulk(&f);
            ctx.out.bulk(&v);
        }
    } else {
        ctx.out.array(pairs.len() * if withvalues { 2 } else { 1 });
        for (f, v) in pairs {
            ctx.out.bulk(&f);
            if withvalues {
                ctx.out.bulk(&v);
            }
        }
    }
}
