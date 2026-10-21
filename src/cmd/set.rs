//! Set commands.

use super::flags::*;
use super::keys::{parse_count, parse_cursor, parse_scan_opts};
use super::{Command, Ctx, KeySpec, NOT_INT, SYNTAX, WRONGTYPE, c, k, numkeys_at};
use crate::types::{Set, Value};
use crate::util::{eq_ic, glob_match, parse_i64};
use std::collections::HashSet;

pub(crate) fn commands() -> Vec<Command> {
    let g = "set";
    vec![
        c(
            "sadd",
            -3,
            WRITE | DENYOOM | FAST,
            k(1, 1, 1),
            sadd,
            g,
            "Adds one or more members to a set. Creates the key if it doesn't exist.",
        ),
        c(
            "srem",
            -3,
            WRITE | FAST,
            k(1, 1, 1),
            srem,
            g,
            "Removes one or more members from a set. Deletes the set if the last member was removed.",
        ),
        c(
            "smembers",
            2,
            READONLY,
            k(1, 1, 1),
            smembers,
            g,
            "Returns all members of a set.",
        ),
        c(
            "sismember",
            3,
            READONLY | FAST,
            k(1, 1, 1),
            sismember,
            g,
            "Determines whether a member belongs to a set.",
        ),
        c(
            "smismember",
            -3,
            READONLY | FAST,
            k(1, 1, 1),
            smismember,
            g,
            "Determines whether multiple members belong to a set.",
        ),
        c(
            "scard",
            2,
            READONLY | FAST,
            k(1, 1, 1),
            scard,
            g,
            "Returns the number of members in a set.",
        ),
        c(
            "sinter",
            -2,
            READONLY,
            k(1, -1, 1),
            sinter,
            g,
            "Returns the intersect of multiple sets.",
        ),
        c(
            "sinterstore",
            -3,
            WRITE | DENYOOM,
            k(1, -1, 1),
            sinterstore,
            g,
            "Stores the intersect of multiple sets in a key.",
        ),
        c(
            "sintercard",
            -3,
            READONLY,
            KeySpec::Custom(|a| numkeys_at(a, 1)),
            sintercard,
            g,
            "Returns the number of members of the intersect of multiple sets.",
        ),
        c(
            "sunion",
            -2,
            READONLY,
            k(1, -1, 1),
            sunion,
            g,
            "Returns the union of multiple sets.",
        ),
        c(
            "sunionstore",
            -3,
            WRITE | DENYOOM,
            k(1, -1, 1),
            sunionstore,
            g,
            "Stores the union of multiple sets in a key.",
        ),
        c(
            "sdiff",
            -2,
            READONLY,
            k(1, -1, 1),
            sdiff,
            g,
            "Returns the difference of multiple sets.",
        ),
        c(
            "sdiffstore",
            -3,
            WRITE | DENYOOM,
            k(1, -1, 1),
            sdiffstore,
            g,
            "Stores the difference of multiple sets in a key.",
        ),
        c(
            "spop",
            -2,
            WRITE | FAST,
            k(1, 1, 1),
            spop,
            g,
            "Returns one or more random members from a set after removing them. Deletes the set if the last member was popped.",
        ),
        c(
            "srandmember",
            -2,
            READONLY,
            k(1, 1, 1),
            srandmember,
            g,
            "Get one or multiple random members from a set",
        ),
        c(
            "smove",
            4,
            WRITE | FAST,
            k(1, 2, 1),
            smove,
            g,
            "Moves a member from one set to another.",
        ),
        c(
            "sscan",
            -3,
            READONLY,
            k(1, 1, 1),
            sscan,
            g,
            "Iterates over members of a set.",
        ),
    ]
}

fn sadd(ctx: &mut Ctx, args: &[&[u8]]) {
    let key = args[1];
    let lim = ctx.db.limits;
    let hint = args.len() - 2;
    match ctx.db.get_mut(key) {
        Some(Value::Set(s)) => s.maybe_convert(hint, &lim),
        Some(_) => return ctx.out.error(WRONGTYPE),
        None => ctx
            .db
            .insert_new(key, Value::Set(Box::new(Set::create(args[2], hint, &lim)))),
    }
    let Some(Value::Set(s)) = ctx.db.get_mut(key) else {
        unreachable!()
    };
    let added = args[2..].iter().filter(|m| s.add(m, &lim)).count();
    if added > 0 {
        ctx.db.modified(key);
    }
    ctx.out.int(added as i64);
}

fn srem(ctx: &mut Ctx, args: &[&[u8]]) {
    let n = match ctx.db.get_mut(args[1]) {
        Some(Value::Set(s)) => args[2..].iter().filter(|m| s.remove(m)).count(),
        Some(_) => return ctx.out.error(WRONGTYPE),
        None => return ctx.out.int(0),
    };
    if n > 0 {
        ctx.db.delete_if_empty(args[1]);
        ctx.db.modified(args[1]);
    }
    ctx.out.int(n as i64);
}

fn smembers(ctx: &mut Ctx, args: &[&[u8]]) {
    let s = match ctx.db.get(args[1]) {
        Some(Value::Set(s)) => s,
        Some(_) => return ctx.out.error(WRONGTYPE),
        None => return ctx.out.set(0),
    };
    ctx.out.set(s.len());
    let out = &mut ctx.out;
    s.for_each(|m| out.bulk(m));
}

fn sismember(ctx: &mut Ctx, args: &[&[u8]]) {
    match ctx.db.get(args[1]) {
        Some(Value::Set(s)) => ctx.out.int(s.contains(args[2]) as i64),
        Some(_) => ctx.out.error(WRONGTYPE),
        None => ctx.out.int(0),
    }
}

fn smismember(ctx: &mut Ctx, args: &[&[u8]]) {
    let s = match ctx.db.get(args[1]) {
        Some(Value::Set(s)) => Some(s),
        Some(_) => return ctx.out.error(WRONGTYPE),
        None => None,
    };
    ctx.out.array(args.len() - 2);
    for m in &args[2..] {
        let v = s.is_some_and(|s| s.contains(m));
        ctx.out.int(v as i64);
    }
}

fn scard(ctx: &mut Ctx, args: &[&[u8]]) {
    match ctx.db.get(args[1]) {
        Some(Value::Set(s)) => ctx.out.int(s.len() as i64),
        Some(_) => ctx.out.error(WRONGTYPE),
        None => ctx.out.int(0),
    }
}

enum Op {
    Inter,
    Union,
    Diff,
}

/// Computes a set operation over `keys`. `Err` means a WRONGTYPE reply was
/// already sent.
fn compute(ctx: &mut Ctx, keys: &[&[u8]], op: Op, limit: usize) -> Result<Vec<Vec<u8>>, ()> {
    ctx.db.prepare(keys);
    let mut sets: Vec<Option<&Set>> = Vec::with_capacity(keys.len());
    for key in keys {
        match ctx.db.peek(key) {
            Some(Value::Set(s)) => sets.push(Some(s)),
            Some(_) => {
                ctx.out.error(WRONGTYPE);
                return Err(());
            }
            None => sets.push(None),
        }
    }
    let mut out = Vec::new();
    match op {
        Op::Inter => {
            if sets.iter().any(|s| s.is_none()) {
                return Ok(out);
            }
            let mut sets: Vec<&Set> = sets.into_iter().flatten().collect();
            sets.sort_by_key(|s| s.len());
            let (first, rest) = sets.split_first().expect("at least one key");
            first.for_each(|m| {
                if out.len() < limit && rest.iter().all(|s| s.contains(m)) {
                    out.push(m.to_vec());
                }
            });
        }
        Op::Union => {
            let mut seen = HashSet::new();
            for s in sets.into_iter().flatten() {
                s.for_each(|m| {
                    if seen.insert(m.to_vec()) {
                        out.push(m.to_vec());
                    }
                });
            }
        }
        Op::Diff => {
            let Some(Some(first)) = sets.first() else {
                return Ok(out);
            };
            let rest: Vec<&Set> = sets[1..].iter().flatten().copied().collect();
            first.for_each(|m| {
                if !rest.iter().any(|s| s.contains(m)) {
                    out.push(m.to_vec());
                }
            });
        }
    }
    Ok(out)
}

fn reply_members(ctx: &mut Ctx, members: Vec<Vec<u8>>) {
    ctx.out.set(members.len());
    for m in members {
        ctx.out.bulk(&m);
    }
}

fn store(ctx: &mut Ctx, dst: &[u8], members: Vec<Vec<u8>>) {
    let n = members.len();
    let existed = ctx.db.remove(dst).is_some();
    if n > 0 {
        let lim = ctx.db.limits;
        let mut s = Set::new();
        for m in &members {
            s.add(m, &lim);
        }
        ctx.db.set(dst, Value::Set(Box::new(s)), false);
        ctx.db.modified(dst);
    } else if existed {
        ctx.db.modified(dst);
    }
    ctx.out.int(n as i64);
}

fn sinter(ctx: &mut Ctx, args: &[&[u8]]) {
    if let Ok(m) = compute(ctx, &args[1..], Op::Inter, usize::MAX) {
        reply_members(ctx, m);
    }
}

fn sinterstore(ctx: &mut Ctx, args: &[&[u8]]) {
    if let Ok(m) = compute(ctx, &args[2..], Op::Inter, usize::MAX) {
        store(ctx, args[1], m);
    }
}

fn sintercard(ctx: &mut Ctx, args: &[&[u8]]) {
    let n = parse_i64(args[1]).unwrap_or(0);
    if n <= 0 {
        return ctx.out.error("ERR numkeys should be greater than 0");
    }
    let n = n as usize;
    if n > args.len() - 2 {
        return ctx
            .out
            .error("ERR Number of keys can't be greater than number of args");
    }
    let mut limit = usize::MAX;
    let mut i = 2 + n;
    while i < args.len() {
        if eq_ic(args[i], "limit") && i + 1 < args.len() {
            match parse_i64(args[i + 1]) {
                Some(l) if l >= 0 => limit = if l == 0 { usize::MAX } else { l as usize },
                _ => return ctx.out.error("ERR LIMIT can't be negative"),
            }
            i += 2;
        } else {
            return ctx.out.error(SYNTAX);
        }
    }
    if let Ok(m) = compute(ctx, &args[2..2 + n], Op::Inter, limit) {
        ctx.out.int(m.len() as i64);
    }
}

fn sunion(ctx: &mut Ctx, args: &[&[u8]]) {
    if let Ok(m) = compute(ctx, &args[1..], Op::Union, usize::MAX) {
        reply_members(ctx, m);
    }
}

fn sunionstore(ctx: &mut Ctx, args: &[&[u8]]) {
    if let Ok(m) = compute(ctx, &args[2..], Op::Union, usize::MAX) {
        store(ctx, args[1], m);
    }
}

fn sdiff(ctx: &mut Ctx, args: &[&[u8]]) {
    if let Ok(m) = compute(ctx, &args[1..], Op::Diff, usize::MAX) {
        reply_members(ctx, m);
    }
}

fn sdiffstore(ctx: &mut Ctx, args: &[&[u8]]) {
    if let Ok(m) = compute(ctx, &args[2..], Op::Diff, usize::MAX) {
        store(ctx, args[1], m);
    }
}

fn spop(ctx: &mut Ctx, args: &[&[u8]]) {
    let key = args[1];
    let count = match args.len() {
        2 => None,
        3 => match parse_count(ctx, args[2]) {
            Some(c) => Some(c as usize),
            None => return,
        },
        _ => return ctx.out.error(SYNTAX),
    };
    let s = match ctx.db.get_mut(key) {
        Some(Value::Set(s)) => s,
        Some(_) => return ctx.out.error(WRONGTYPE),
        None => {
            return if count.is_some() {
                ctx.out.set(0)
            } else {
                ctx.out.null()
            };
        }
    };
    let popped: Vec<Vec<u8>> = match count {
        None => vec![s.pop_random().expect("non-empty")],
        Some(n) => (0..n.min(s.len())).filter_map(|_| s.pop_random()).collect(),
    };
    if count.is_none() {
        ctx.out.bulk(&popped[0]);
    } else {
        ctx.out.set(popped.len());
        for m in &popped {
            ctx.out.bulk(m);
        }
    }
    if popped.is_empty() {
        return;
    }
    ctx.db.delete_if_empty(key);
    ctx.db.modified(key);
    let mut srem = vec![b"SREM".to_vec(), key.to_vec()];
    srem.extend(popped);
    ctx.propagate_as(vec![srem]);
}

fn srandmember(ctx: &mut Ctx, args: &[&[u8]]) {
    let count = match args.len() {
        2 => None,
        3 => match parse_i64(args[2]) {
            Some(c) if c > -(i64::MAX / 2) && c < i64::MAX / 2 => Some(c),
            Some(_) => return ctx.out.error("ERR value is out of range"),
            None => return ctx.out.error(NOT_INT),
        },
        _ => return ctx.out.error(SYNTAX),
    };
    let s = match ctx.db.get(args[1]) {
        Some(Value::Set(s)) => s,
        Some(_) => return ctx.out.error(WRONGTYPE),
        None => {
            return if count.is_some() {
                ctx.out.array(0)
            } else {
                ctx.out.null()
            };
        }
    };
    match count {
        None => {
            let m = s.random_member().expect("non-empty");
            ctx.out.bulk(&m);
        }
        Some(c) if c >= 0 => {
            let mut all = s.members();
            let n = (c as usize).min(all.len());
            for i in 0..n {
                let j = i + crate::util::rand_below((all.len() - i) as u64) as usize;
                all.swap(i, j);
            }
            ctx.out.array(n);
            for m in &all[..n] {
                ctx.out.bulk(m);
            }
        }
        Some(c) => {
            let n = c.unsigned_abs() as usize;
            ctx.out.array(n);
            for _ in 0..n {
                let m = s.random_member().expect("non-empty");
                ctx.out.bulk(&m);
            }
        }
    }
}

fn smove(ctx: &mut Ctx, args: &[&[u8]]) {
    let (src, dst, m) = (args[1], args[2], args[3]);
    let src_has = match ctx.db.get_mut(src) {
        Some(Value::Set(s)) => s.contains(m),
        Some(_) => return ctx.out.error(WRONGTYPE),
        None => return ctx.out.int(0),
    };
    match ctx.db.get_mut(dst) {
        Some(Value::Set(_)) | None => {}
        Some(_) => return ctx.out.error(WRONGTYPE),
    }
    if !src_has {
        return ctx.out.int(0);
    }
    if src == dst {
        return ctx.out.int(1);
    }
    if let Some(Value::Set(s)) = ctx.db.get_mut(src) {
        s.remove(m);
    }
    ctx.db.delete_if_empty(src);
    ctx.db.modified(src);
    let lim = ctx.db.limits;
    match ctx.db.get_mut(dst) {
        Some(Value::Set(s)) => {
            s.add(m, &lim);
        }
        _ => {
            let mut s = Set::create(m, 1, &lim);
            s.add(m, &lim);
            ctx.db.insert_new(dst, Value::Set(Box::new(s)));
        }
    }
    ctx.db.modified(dst);
    ctx.out.int(1);
}

fn sscan(ctx: &mut Ctx, args: &[&[u8]]) {
    let Some(cursor) = parse_cursor(ctx, args[2]) else {
        return;
    };
    let Some(opts) = parse_scan_opts(ctx, args, 3, false) else {
        return;
    };
    let s = match ctx.db.get(args[1]) {
        Some(Value::Set(s)) => s,
        Some(_) => return ctx.out.error(WRONGTYPE),
        None => {
            ctx.out.array(2);
            ctx.out.bulk(b"0");
            return ctx.out.array(0);
        }
    };
    let mut items = Vec::new();
    let next = s.scan(cursor, opts.count, |m| {
        if opts
            .pattern
            .as_ref()
            .is_none_or(|p| glob_match(p, m, false))
        {
            items.push(m.to_vec());
        }
    });
    ctx.out.array(2);
    ctx.out.bulk(next.to_string().as_bytes());
    ctx.out.array(items.len());
    for m in items {
        ctx.out.bulk(&m);
    }
}
