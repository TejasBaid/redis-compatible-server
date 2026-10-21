//! List commands, including the blocking variants.

use super::flags::*;
use super::keys::parse_count;
use super::{
    BlockRequest, Command, Ctx, KeySpec, NO_KEY, NOT_INT, SYNTAX, WRONGTYPE, c, k, numkeys_at,
};
use crate::blocking::BlockKind;
use crate::sds::Sds;
use crate::types::{List, Value};
use crate::util::{eq_ic, normalize_range, parse_f64, parse_i64};

pub(crate) fn commands() -> Vec<Command> {
    let g = "list";
    vec![
        c(
            "lpush",
            -3,
            WRITE | DENYOOM | FAST,
            k(1, 1, 1),
            lpush,
            g,
            "Prepends one or more elements to a list. Creates the key if it doesn't exist.",
        ),
        c(
            "rpush",
            -3,
            WRITE | DENYOOM | FAST,
            k(1, 1, 1),
            rpush,
            g,
            "Appends one or more elements to a list. Creates the key if it doesn't exist.",
        ),
        c(
            "lpushx",
            -3,
            WRITE | DENYOOM | FAST,
            k(1, 1, 1),
            lpushx,
            g,
            "Prepends one or more elements to a list only when the list exists.",
        ),
        c(
            "rpushx",
            -3,
            WRITE | DENYOOM | FAST,
            k(1, 1, 1),
            rpushx,
            g,
            "Appends an element to a list only when the list exists.",
        ),
        c(
            "lpop",
            -2,
            WRITE | FAST,
            k(1, 1, 1),
            lpop,
            g,
            "Returns the first elements in a list after removing it. Deletes the list if the last element was popped.",
        ),
        c(
            "rpop",
            -2,
            WRITE | FAST,
            k(1, 1, 1),
            rpop,
            g,
            "Returns and removes the last elements of a list. Deletes the list if the last element was popped.",
        ),
        c(
            "lrange",
            4,
            READONLY,
            k(1, 1, 1),
            lrange,
            g,
            "Returns a range of elements from a list.",
        ),
        c(
            "llen",
            2,
            READONLY | FAST,
            k(1, 1, 1),
            llen,
            g,
            "Returns the length of a list.",
        ),
        c(
            "lindex",
            3,
            READONLY,
            k(1, 1, 1),
            lindex,
            g,
            "Returns an element from a list by its index.",
        ),
        c(
            "lset",
            4,
            WRITE | DENYOOM,
            k(1, 1, 1),
            lset,
            g,
            "Sets the value of an element in a list by its index.",
        ),
        c(
            "lrem",
            4,
            WRITE,
            k(1, 1, 1),
            lrem,
            g,
            "Removes elements from a list. Deletes the list if the last element was removed.",
        ),
        c(
            "ltrim",
            4,
            WRITE,
            k(1, 1, 1),
            ltrim,
            g,
            "Removes elements from both ends a list. Deletes the list if all elements were trimmed.",
        ),
        c(
            "linsert",
            5,
            WRITE | DENYOOM,
            k(1, 1, 1),
            linsert,
            g,
            "Inserts an element before or after another element in a list.",
        ),
        c(
            "lmove",
            5,
            WRITE | DENYOOM,
            k(1, 2, 1),
            lmove,
            g,
            "Returns an element after popping it from one list and pushing it to another. Deletes the list if the last element was moved.",
        ),
        c(
            "rpoplpush",
            3,
            WRITE | DENYOOM,
            k(1, 2, 1),
            rpoplpush,
            g,
            "Returns the last element of a list after removing and pushing it to another list. Deletes the list if the last element was popped.",
        ),
        c(
            "lpos",
            -3,
            READONLY,
            k(1, 1, 1),
            lpos,
            g,
            "Returns the index of matching elements in a list.",
        ),
        c(
            "lmpop",
            -4,
            WRITE,
            KeySpec::Custom(|a| numkeys_at(a, 1)),
            lmpop,
            g,
            "Returns multiple elements from a list after removing them. Deletes the list if the last element was popped.",
        ),
        c(
            "blpop",
            -3,
            WRITE | BLOCKING,
            k(1, -2, 1),
            blpop,
            g,
            "Removes and returns the first element in a list. Blocks until an element is available otherwise. Deletes the list if the last element was popped.",
        ),
        c(
            "brpop",
            -3,
            WRITE | BLOCKING,
            k(1, -2, 1),
            brpop,
            g,
            "Removes and returns the last element in a list. Blocks until an element is available otherwise. Deletes the list if the last element was popped.",
        ),
        c(
            "blmove",
            6,
            WRITE | DENYOOM | BLOCKING,
            k(1, 2, 1),
            blmove,
            g,
            "Pops an element from a list, pushes it to another list and returns it. Blocks until an element is available otherwise. Deletes the list if the last element was moved.",
        ),
        c(
            "brpoplpush",
            4,
            WRITE | DENYOOM | BLOCKING,
            k(1, 2, 1),
            brpoplpush,
            g,
            "Pops an element from a list, pushes it to another list and returns it. Block until an element is available otherwise. Deletes the list if the last element was popped.",
        ),
        c(
            "blmpop",
            -5,
            WRITE | BLOCKING,
            KeySpec::Custom(|a| numkeys_at(a, 2)),
            blmpop,
            g,
            "Pops the first element from one of multiple lists. Blocks until an element is available otherwise. Deletes the list if the last element was popped.",
        ),
    ]
}

fn push_generic(ctx: &mut Ctx, args: &[&[u8]], front: bool, only_existing: bool) {
    let key = args[1];
    let lim = ctx.db.limits;
    let len = match ctx.db.get_mut(key) {
        Some(Value::List(l)) => {
            l.push(&args[2..], front, &lim);
            l.len()
        }
        Some(_) => return ctx.out.error(WRONGTYPE),
        None if only_existing => return ctx.out.int(0),
        None => {
            let mut l = List::new();
            l.push(&args[2..], front, &lim);
            let n = l.len();
            ctx.db.insert_new(key, Value::List(Box::new(l)));
            n
        }
    };
    ctx.db.modified(key);
    ctx.db.signal_ready(key);
    ctx.out.int(len as i64);
}

fn lpush(ctx: &mut Ctx, args: &[&[u8]]) {
    push_generic(ctx, args, true, false);
}
fn rpush(ctx: &mut Ctx, args: &[&[u8]]) {
    push_generic(ctx, args, false, false);
}
fn lpushx(ctx: &mut Ctx, args: &[&[u8]]) {
    push_generic(ctx, args, true, true);
}
fn rpushx(ctx: &mut Ctx, args: &[&[u8]]) {
    push_generic(ctx, args, false, true);
}

fn pop_generic(ctx: &mut Ctx, args: &[&[u8]], front: bool) {
    let key = args[1];
    let count = if args.len() == 3 {
        match parse_count(ctx, args[2]) {
            Some(c) => Some(c as usize),
            None => return,
        }
    } else if args.len() > 3 {
        return ctx.arity_error(if front { "lpop" } else { "rpop" });
    } else {
        None
    };
    let lim = ctx.db.limits;
    let l = match ctx.db.get_mut(key) {
        Some(Value::List(l)) => l,
        Some(_) => return ctx.out.error(WRONGTYPE),
        None => {
            return if count.is_some() {
                ctx.out.null_array()
            } else {
                ctx.out.null()
            };
        }
    };
    match count {
        None => {
            let v = l.pop(front).expect("lists are never empty");
            l.shrink_check(&lim);
            ctx.out.bulk(&v);
        }
        Some(0) => return ctx.out.array(0),
        Some(n) => {
            let n = n.min(l.len());
            ctx.out.array(n);
            for _ in 0..n {
                let v = l.pop(front).expect("within length");
                ctx.out.bulk(&v);
            }
            l.shrink_check(&lim);
        }
    }
    ctx.db.delete_if_empty(key);
    ctx.db.modified(key);
}

fn lpop(ctx: &mut Ctx, args: &[&[u8]]) {
    pop_generic(ctx, args, true);
}
fn rpop(ctx: &mut Ctx, args: &[&[u8]]) {
    pop_generic(ctx, args, false);
}

fn lrange(ctx: &mut Ctx, args: &[&[u8]]) {
    let (Some(s), Some(e)) = (parse_i64(args[2]), parse_i64(args[3])) else {
        return ctx.out.error(NOT_INT);
    };
    let l = match ctx.db.get(args[1]) {
        Some(Value::List(l)) => l,
        Some(_) => return ctx.out.error(WRONGTYPE),
        None => return ctx.out.array(0),
    };
    match normalize_range(s, e, l.len()) {
        None => ctx.out.array(0),
        Some((a, b)) => {
            ctx.out.array(b - a);
            for v in l.iter().skip(a).take(b - a) {
                ctx.out.bulk(v);
            }
        }
    }
}

fn llen(ctx: &mut Ctx, args: &[&[u8]]) {
    match ctx.db.get(args[1]) {
        Some(Value::List(l)) => ctx.out.int(l.len() as i64),
        Some(_) => ctx.out.error(WRONGTYPE),
        None => ctx.out.int(0),
    }
}

fn lindex(ctx: &mut Ctx, args: &[&[u8]]) {
    let l = match ctx.db.get(args[1]) {
        Some(Value::List(l)) => l,
        Some(_) => return ctx.out.error(WRONGTYPE),
        None => return ctx.out.null(),
    };
    let Some(i) = parse_i64(args[2]) else {
        return ctx.out.error(NOT_INT);
    };
    match l.get(i) {
        Some(v) => ctx.out.bulk(v),
        None => ctx.out.null(),
    }
}

fn lset(ctx: &mut Ctx, args: &[&[u8]]) {
    let lim = ctx.db.limits;
    match ctx.db.get_mut(args[1]) {
        Some(Value::List(l)) => {
            let Some(i) = parse_i64(args[2]) else {
                return ctx.out.error(NOT_INT);
            };
            if !l.set(i, args[3], &lim) {
                return ctx.out.error("ERR index out of range");
            }
        }
        Some(_) => return ctx.out.error(WRONGTYPE),
        None => return ctx.out.error(NO_KEY),
    }
    ctx.db.modified(args[1]);
    ctx.out.ok();
}

fn lrem(ctx: &mut Ctx, args: &[&[u8]]) {
    let Some(count) = parse_i64(args[2]) else {
        return ctx.out.error(NOT_INT);
    };
    let lim = ctx.db.limits;
    let n = match ctx.db.get_mut(args[1]) {
        Some(Value::List(l)) => l.remove(count, args[3], &lim),
        Some(_) => return ctx.out.error(WRONGTYPE),
        None => return ctx.out.int(0),
    };
    if n > 0 {
        ctx.db.delete_if_empty(args[1]);
        ctx.db.modified(args[1]);
    }
    ctx.out.int(n as i64);
}

fn ltrim(ctx: &mut Ctx, args: &[&[u8]]) {
    let (Some(s), Some(e)) = (parse_i64(args[2]), parse_i64(args[3])) else {
        return ctx.out.error(NOT_INT);
    };
    let lim = ctx.db.limits;
    match ctx.db.get_mut(args[1]) {
        Some(Value::List(l)) => {
            let r = normalize_range(s, e, l.len());
            l.trim(r, &lim);
        }
        Some(_) => return ctx.out.error(WRONGTYPE),
        None => return ctx.out.ok(),
    }
    ctx.db.delete_if_empty(args[1]);
    ctx.db.modified(args[1]);
    ctx.out.ok();
}

fn linsert(ctx: &mut Ctx, args: &[&[u8]]) {
    let after = if eq_ic(args[2], "after") {
        true
    } else if eq_ic(args[2], "before") {
        false
    } else {
        return ctx.out.error(SYNTAX);
    };
    let lim = ctx.db.limits;
    let r = match ctx.db.get_mut(args[1]) {
        Some(Value::List(l)) => l.insert(after, args[3], args[4], &lim),
        Some(_) => return ctx.out.error(WRONGTYPE),
        None => return ctx.out.int(0),
    };
    match r {
        Some(n) => {
            ctx.db.modified(args[1]);
            ctx.out.int(n as i64)
        }
        None => ctx.out.int(-1),
    }
}

fn parse_side(a: &[u8]) -> Option<bool> {
    if eq_ic(a, "left") {
        Some(true)
    } else if eq_ic(a, "right") {
        Some(false)
    } else {
        None
    }
}

/// Pops from `src` and pushes to `dst`. Returns `Ok(None)` if `src` is
/// missing. Replies are left to the caller.
fn do_move(
    ctx: &mut Ctx,
    src: &[u8],
    dst: &[u8],
    from_front: bool,
    to_front: bool,
) -> Result<Option<Sds>, &'static str> {
    match ctx.db.get_mut(src) {
        Some(Value::List(_)) => {}
        Some(_) => return Err(WRONGTYPE),
        None => return Ok(None),
    }
    match ctx.db.get_mut(dst) {
        Some(Value::List(_)) | None => {}
        Some(_) => return Err(WRONGTYPE),
    }
    let lim = ctx.db.limits;
    let v = match ctx.db.get_mut(src) {
        Some(Value::List(l)) => {
            let v = l.pop(from_front).expect("non-empty");
            l.shrink_check(&lim);
            v
        }
        _ => unreachable!(),
    };
    ctx.db.modified(src);
    match ctx.db.get_mut(dst) {
        Some(Value::List(l)) => l.push(&[&v], to_front, &lim),
        _ => {
            let mut l = List::new();
            l.push(&[&v], to_front, &lim);
            ctx.db.insert_new(dst, Value::List(Box::new(l)));
        }
    }
    // Only after the push: rotating a one-element list onto itself must keep
    // the key (and its TTL), as in Redis.
    ctx.db.delete_if_empty(src);
    ctx.db.modified(dst);
    ctx.db.signal_ready(dst);
    Ok(Some(v))
}

fn lmove_reply(ctx: &mut Ctx, args: &[&[u8]], from_front: bool, to_front: bool) {
    match do_move(ctx, args[1], args[2], from_front, to_front) {
        Ok(Some(v)) => ctx.out.bulk(&v),
        Ok(None) => ctx.out.null(),
        Err(e) => ctx.out.error(e),
    }
}

fn lmove(ctx: &mut Ctx, args: &[&[u8]]) {
    let (Some(f), Some(t)) = (parse_side(args[3]), parse_side(args[4])) else {
        return ctx.out.error(SYNTAX);
    };
    lmove_reply(ctx, args, f, t);
}

fn rpoplpush(ctx: &mut Ctx, args: &[&[u8]]) {
    lmove_reply(ctx, args, false, true);
}

fn lpos(ctx: &mut Ctx, args: &[&[u8]]) {
    let (mut rank, mut count, mut maxlen) = (1i64, None::<i64>, 0i64);
    let mut i = 3;
    while i < args.len() {
        if i + 1 >= args.len() {
            return ctx.out.error(SYNTAX);
        }
        let a = args[i];
        let Some(v) = parse_i64(args[i + 1]) else {
            return ctx.out.error(NOT_INT);
        };
        if eq_ic(a, "rank") {
            if v == 0 {
                return ctx.out.error("ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list");
            }
            if v == i64::MIN {
                return ctx.out.error("ERR value is out of range, value must between -9223372036854775807 and 9223372036854775807");
            }
            rank = v;
        } else if eq_ic(a, "count") {
            if v < 0 {
                return ctx.out.error("ERR COUNT can't be negative");
            }
            count = Some(v);
        } else if eq_ic(a, "maxlen") {
            if v < 0 {
                return ctx.out.error("ERR MAXLEN can't be negative");
            }
            maxlen = v;
        } else {
            return ctx.out.error(SYNTAX);
        }
        i += 2;
    }
    let l = match ctx.db.get(args[1]) {
        Some(Value::List(l)) => l,
        Some(_) => return ctx.out.error(WRONGTYPE),
        None => {
            return if count.is_some() {
                ctx.out.array(0)
            } else {
                ctx.out.null()
            };
        }
    };
    let len = l.len();
    let want = match count {
        Some(0) => usize::MAX,
        Some(n) => n as usize,
        None => 1,
    };
    let max_scan = if maxlen == 0 {
        usize::MAX
    } else {
        maxlen as usize
    };
    let mut skip = rank.unsigned_abs() as usize - 1;
    let mut found = Vec::new();
    let elem = args[2];
    let it: Box<dyn Iterator<Item = (usize, &[u8])>> = if rank > 0 {
        Box::new(l.iter().enumerate())
    } else {
        Box::new(
            l.iter()
                .rev()
                .enumerate()
                .map(move |(i, v)| (len - 1 - i, v)),
        )
    };
    for (scanned, (idx, v)) in it.enumerate() {
        if scanned >= max_scan || found.len() >= want {
            break;
        }
        if v == elem {
            if skip > 0 {
                skip -= 1;
            } else {
                found.push(idx);
            }
        }
    }
    if count.is_some() {
        ctx.out.array(found.len());
        for i in found {
            ctx.out.int(i as i64);
        }
    } else {
        match found.first() {
            Some(i) => ctx.out.int(*i as i64),
            None => ctx.out.null(),
        }
    }
}

/// Parses `numkeys key... LEFT|RIGHT [COUNT n]` starting at `args[at]`.
fn parse_mpop(
    ctx: &mut Ctx,
    args: &[&[u8]],
    at: usize,
    sides: (&str, &str),
) -> Option<(Vec<Vec<u8>>, bool, usize)> {
    let n = parse_i64(args[at]).unwrap_or(0);
    if n <= 0 || n as usize > args.len() {
        ctx.out.error("ERR numkeys should be greater than 0");
        return None;
    }
    let n = n as usize;
    let keys_end = at + 1 + n;
    if keys_end >= args.len() {
        ctx.out.error(SYNTAX);
        return None;
    }
    let keys = args[at + 1..keys_end].iter().map(|k| k.to_vec()).collect();
    let first = if eq_ic(args[keys_end], sides.0) {
        true
    } else if eq_ic(args[keys_end], sides.1) {
        false
    } else {
        ctx.out.error(SYNTAX);
        return None;
    };
    let mut count = 1usize;
    let mut i = keys_end + 1;
    let mut seen_count = false;
    while i < args.len() {
        if eq_ic(args[i], "count") && i + 1 < args.len() && !seen_count {
            match parse_i64(args[i + 1]) {
                Some(c) if c > 0 => count = c as usize,
                _ => {
                    ctx.out.error("ERR count should be greater than 0");
                    return None;
                }
            }
            seen_count = true;
            i += 2;
        } else {
            ctx.out.error(SYNTAX);
            return None;
        }
    }
    Some((keys, first, count))
}

/// Pops up to `count` from the first non-empty list among `keys`.
fn mpop_once(ctx: &mut Ctx, keys: &[Vec<u8>], front: bool, count: usize) -> Result<bool, ()> {
    let lim = ctx.db.limits;
    for key in keys {
        match ctx.db.get_mut(key) {
            Some(Value::List(l)) => {
                let n = count.min(l.len());
                ctx.out.array(2);
                ctx.out.bulk(key);
                ctx.out.array(n);
                for _ in 0..n {
                    let v = l.pop(front).expect("within length");
                    ctx.out.bulk(&v);
                }
                l.shrink_check(&lim);
                ctx.db.delete_if_empty(key);
                ctx.db.modified(key);
                let c = n.to_string().into_bytes();
                ctx.propagate_as(vec![vec![
                    if front {
                        b"LPOP".to_vec()
                    } else {
                        b"RPOP".to_vec()
                    },
                    key.clone(),
                    c,
                ]]);
                return Ok(true);
            }
            Some(_) => {
                ctx.out.error(WRONGTYPE);
                return Err(());
            }
            None => {}
        }
    }
    Ok(false)
}

fn lmpop(ctx: &mut Ctx, args: &[&[u8]]) {
    let Some((keys, front, count)) = parse_mpop(ctx, args, 1, ("left", "right")) else {
        return;
    };
    match mpop_once(ctx, &keys, front, count) {
        Ok(true) | Err(()) => {}
        Ok(false) => ctx.out.null_array(),
    }
}

/// Parses a blocking timeout (seconds, may be fractional) into an absolute
/// deadline; `None` inside the result means "block forever".
pub(crate) fn parse_timeout(ctx: &mut Ctx, a: &[u8]) -> Option<Option<u64>> {
    let Some(secs) = parse_f64(a) else {
        ctx.out.error("ERR timeout is not a float or out of range");
        return None;
    };
    if secs < 0.0 {
        ctx.out.error("ERR timeout is negative");
        return None;
    }
    if !secs.is_finite() || secs > 1e12 {
        ctx.out.error("ERR timeout is out of range");
        return None;
    }
    let ms = (secs * 1000.0).ceil() as u64;
    Some(if ms == 0 { None } else { Some(ctx.db.now + ms) })
}

fn bpop(ctx: &mut Ctx, args: &[&[u8]], front: bool) {
    let keys = &args[1..args.len() - 1];
    let Some(deadline) = parse_timeout(ctx, args[args.len() - 1]) else {
        return;
    };
    let lim = ctx.db.limits;
    for key in keys {
        match ctx.db.get_mut(key) {
            Some(Value::List(l)) => {
                let v = l.pop(front).expect("non-empty");
                l.shrink_check(&lim);
                ctx.db.delete_if_empty(key);
                ctx.db.modified(key);
                ctx.propagate_as(vec![vec![
                    if front {
                        b"LPOP".to_vec()
                    } else {
                        b"RPOP".to_vec()
                    },
                    key.to_vec(),
                ]]);
                ctx.out.array(2);
                ctx.out.bulk(key);
                ctx.out.bulk(&v);
                return;
            }
            Some(_) => return ctx.out.error(WRONGTYPE),
            None => {}
        }
    }
    if ctx.in_exec {
        return ctx.out.null_array();
    }
    ctx.block = Some(BlockRequest {
        keys: keys.iter().map(|k| Sds::from_slice(k)).collect(),
        deadline,
        kind: BlockKind::ListPop { front },
    });
}

fn blpop(ctx: &mut Ctx, args: &[&[u8]]) {
    bpop(ctx, args, true);
}
fn brpop(ctx: &mut Ctx, args: &[&[u8]]) {
    bpop(ctx, args, false);
}

fn bmove(ctx: &mut Ctx, src: &[u8], dst: &[u8], from_front: bool, to_front: bool, timeout: &[u8]) {
    let Some(deadline) = parse_timeout(ctx, timeout) else {
        return;
    };
    match do_move(ctx, src, dst, from_front, to_front) {
        Ok(Some(v)) => {
            let side = |f: bool| {
                if f {
                    b"LEFT".to_vec()
                } else {
                    b"RIGHT".to_vec()
                }
            };
            ctx.propagate_as(vec![vec![
                b"LMOVE".to_vec(),
                src.to_vec(),
                dst.to_vec(),
                side(from_front),
                side(to_front),
            ]]);
            ctx.out.bulk(&v)
        }
        Err(e) => ctx.out.error(e),
        Ok(None) => {
            if ctx.in_exec {
                return ctx.out.null();
            }
            ctx.block = Some(BlockRequest {
                keys: vec![Sds::from_slice(src)],
                deadline,
                kind: BlockKind::ListMove {
                    dst: Sds::from_slice(dst),
                    from_front,
                    to_front,
                },
            });
        }
    }
}

fn blmove(ctx: &mut Ctx, args: &[&[u8]]) {
    let (Some(f), Some(t)) = (parse_side(args[3]), parse_side(args[4])) else {
        return ctx.out.error(SYNTAX);
    };
    bmove(ctx, args[1], args[2], f, t, args[5]);
}

fn brpoplpush(ctx: &mut Ctx, args: &[&[u8]]) {
    bmove(ctx, args[1], args[2], false, true, args[3]);
}

fn blmpop(ctx: &mut Ctx, args: &[&[u8]]) {
    let Some(deadline) = parse_timeout(ctx, args[1]) else {
        return;
    };
    let Some((keys, front, count)) = parse_mpop(ctx, args, 2, ("left", "right")) else {
        return;
    };
    match mpop_once(ctx, &keys, front, count) {
        Ok(true) | Err(()) => {}
        Ok(false) => {
            if ctx.in_exec {
                return ctx.out.null_array();
            }
            ctx.block = Some(BlockRequest {
                keys: keys.iter().map(|k| Sds::from_slice(k)).collect(),
                deadline,
                kind: BlockKind::ListMPop { front, count },
            });
        }
    }
}
