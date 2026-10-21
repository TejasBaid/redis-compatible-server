//! Sorted set commands.

use super::flags::*;
use super::keys::{parse_count, parse_cursor, parse_scan_opts};
use super::list::parse_timeout;
use super::{
    BlockRequest, Command, Ctx, KeySpec, NOT_FLOAT, NOT_INT, SYNTAX, WRONGTYPE, c, k, numkeys_at,
};
use crate::blocking::BlockKind;
use crate::sds::Sds;
use crate::skiplist::{LexBound, LexRange, ScoreRange, cmp_entry};
use crate::types::{Set, Value, ZSet};
use crate::util::{eq_ic, glob_match, normalize_range, parse_f64, parse_f64_range, parse_i64};
use std::collections::HashMap;

pub(crate) fn commands() -> Vec<Command> {
    let g = "sorted-set";
    let store_keys: KeySpec = KeySpec::Custom(|a| {
        let mut v = vec![1];
        v.extend(numkeys_at(a, 2));
        v
    });
    vec![
        c(
            "zadd",
            -4,
            WRITE | DENYOOM | FAST,
            k(1, 1, 1),
            zadd,
            g,
            "Adds one or more members to a sorted set, or updates their scores. Creates the key if it doesn't exist.",
        ),
        c(
            "zincrby",
            4,
            WRITE | DENYOOM | FAST,
            k(1, 1, 1),
            zincrby,
            g,
            "Increments the score of a member in a sorted set.",
        ),
        c(
            "zrem",
            -3,
            WRITE | FAST,
            k(1, 1, 1),
            zrem,
            g,
            "Removes one or more members from a sorted set. Deletes the sorted set if all members were removed.",
        ),
        c(
            "zscore",
            3,
            READONLY | FAST,
            k(1, 1, 1),
            zscore,
            g,
            "Returns the score of a member in a sorted set.",
        ),
        c(
            "zmscore",
            -3,
            READONLY | FAST,
            k(1, 1, 1),
            zmscore,
            g,
            "Returns the score of one or more members in a sorted set.",
        ),
        c(
            "zcard",
            2,
            READONLY | FAST,
            k(1, 1, 1),
            zcard,
            g,
            "Returns the number of members in a sorted set.",
        ),
        c(
            "zcount",
            4,
            READONLY | FAST,
            k(1, 1, 1),
            zcount,
            g,
            "Returns the count of members in a sorted set that have scores within a range.",
        ),
        c(
            "zlexcount",
            4,
            READONLY | FAST,
            k(1, 1, 1),
            zlexcount,
            g,
            "Returns the number of members in a sorted set within a lexicographical range.",
        ),
        c(
            "zrank",
            -3,
            READONLY | FAST,
            k(1, 1, 1),
            zrank,
            g,
            "Returns the index of a member in a sorted set ordered by ascending scores.",
        ),
        c(
            "zrevrank",
            -3,
            READONLY | FAST,
            k(1, 1, 1),
            zrevrank,
            g,
            "Returns the index of a member in a sorted set ordered by descending scores.",
        ),
        c(
            "zrange",
            -4,
            READONLY,
            k(1, 1, 1),
            zrange,
            g,
            "Returns members in a sorted set within a range of indexes.",
        ),
        c(
            "zrangestore",
            -5,
            WRITE | DENYOOM,
            k(1, 2, 1),
            zrangestore,
            g,
            "Stores a range of members from sorted set in a key.",
        ),
        c(
            "zrevrange",
            -4,
            READONLY,
            k(1, 1, 1),
            zrevrange,
            g,
            "Returns members in a sorted set within a range of indexes in reverse order.",
        ),
        c(
            "zrangebyscore",
            -4,
            READONLY,
            k(1, 1, 1),
            zrangebyscore,
            g,
            "Returns members in a sorted set within a range of scores.",
        ),
        c(
            "zrevrangebyscore",
            -4,
            READONLY,
            k(1, 1, 1),
            zrevrangebyscore,
            g,
            "Returns members in a sorted set within a range of scores in reverse order.",
        ),
        c(
            "zrangebylex",
            -4,
            READONLY,
            k(1, 1, 1),
            zrangebylex,
            g,
            "Returns members in a sorted set within a lexicographical range.",
        ),
        c(
            "zrevrangebylex",
            -4,
            READONLY,
            k(1, 1, 1),
            zrevrangebylex,
            g,
            "Returns members in a sorted set within a lexicographical range in reverse order.",
        ),
        c(
            "zremrangebyrank",
            4,
            WRITE,
            k(1, 1, 1),
            zremrangebyrank,
            g,
            "Removes members in a sorted set within a range of indexes. Deletes the sorted set if all members were removed.",
        ),
        c(
            "zremrangebyscore",
            4,
            WRITE,
            k(1, 1, 1),
            zremrangebyscore,
            g,
            "Removes members in a sorted set within a range of scores. Deletes the sorted set if all members were removed.",
        ),
        c(
            "zremrangebylex",
            4,
            WRITE,
            k(1, 1, 1),
            zremrangebylex,
            g,
            "Removes members in a sorted set within a lexicographical range. Deletes the sorted set if all members were removed.",
        ),
        c(
            "zpopmin",
            -2,
            WRITE | FAST,
            k(1, 1, 1),
            zpopmin,
            g,
            "Returns the lowest-scoring members from a sorted set after removing them. Deletes the sorted set if the last member was popped.",
        ),
        c(
            "zpopmax",
            -2,
            WRITE | FAST,
            k(1, 1, 1),
            zpopmax,
            g,
            "Returns the highest-scoring members from a sorted set after removing them. Deletes the sorted set if the last member was popped.",
        ),
        c(
            "bzpopmin",
            -3,
            WRITE | FAST | BLOCKING,
            k(1, -2, 1),
            bzpopmin,
            g,
            "Removes and returns the member with the lowest score from one or more sorted sets. Blocks until a member is available otherwise. Deletes the sorted set if the last element was popped.",
        ),
        c(
            "bzpopmax",
            -3,
            WRITE | FAST | BLOCKING,
            k(1, -2, 1),
            bzpopmax,
            g,
            "Removes and returns the member with the highest score from one or more sorted sets. Blocks until a member available otherwise.  Deletes the sorted set if the last element was popped.",
        ),
        c(
            "zmpop",
            -4,
            WRITE,
            KeySpec::Custom(|a| numkeys_at(a, 1)),
            zmpop,
            g,
            "Returns the highest- or lowest-scoring members from one or more sorted sets after removing them. Deletes the sorted set if the last member was popped.",
        ),
        c(
            "bzmpop",
            -5,
            WRITE | BLOCKING,
            KeySpec::Custom(|a| numkeys_at(a, 2)),
            bzmpop,
            g,
            "Removes and returns a member by score from one or more sorted sets. Blocks until a member is available otherwise. Deletes the sorted set if the last element was popped.",
        ),
        c(
            "zrandmember",
            -2,
            READONLY,
            k(1, 1, 1),
            zrandmember,
            g,
            "Returns one or more random members from a sorted set.",
        ),
        c(
            "zscan",
            -3,
            READONLY,
            k(1, 1, 1),
            zscan,
            g,
            "Iterates over members and scores of a sorted set.",
        ),
        c(
            "zunionstore",
            -4,
            WRITE | DENYOOM,
            store_keys,
            zunionstore,
            g,
            "Stores the union of multiple sorted sets in a key.",
        ),
        c(
            "zinterstore",
            -4,
            WRITE | DENYOOM,
            store_keys,
            zinterstore,
            g,
            "Stores the intersect of multiple sorted sets in a key.",
        ),
        c(
            "zdiffstore",
            -4,
            WRITE | DENYOOM,
            store_keys,
            zdiffstore,
            g,
            "Stores the difference of multiple sorted sets in a key.",
        ),
        c(
            "zunion",
            -3,
            READONLY,
            KeySpec::Custom(|a| numkeys_at(a, 1)),
            zunion,
            g,
            "Returns the union of multiple sorted sets.",
        ),
        c(
            "zinter",
            -3,
            READONLY,
            KeySpec::Custom(|a| numkeys_at(a, 1)),
            zinter,
            g,
            "Returns the intersect of multiple sorted sets.",
        ),
        c(
            "zdiff",
            -3,
            READONLY,
            KeySpec::Custom(|a| numkeys_at(a, 1)),
            zdiff,
            g,
            "Returns the difference between multiple sorted sets.",
        ),
        c(
            "zintercard",
            -3,
            READONLY,
            KeySpec::Custom(|a| numkeys_at(a, 1)),
            zintercard,
            g,
            "Returns the number of members of the intersect of multiple sorted sets.",
        ),
    ]
}

fn zadd_generic(ctx: &mut Ctx, args: &[&[u8]], force_incr: bool) {
    let key = args[1];
    let (mut nx, mut xx, mut gt, mut lt, mut ch, mut incr) =
        (false, false, false, false, false, force_incr);
    let mut i = 2;
    if !force_incr {
        while i < args.len() {
            let a = args[i];
            if eq_ic(a, "nx") {
                nx = true;
            } else if eq_ic(a, "xx") {
                xx = true;
            } else if eq_ic(a, "gt") {
                gt = true;
            } else if eq_ic(a, "lt") {
                lt = true;
            } else if eq_ic(a, "ch") {
                ch = true;
            } else if eq_ic(a, "incr") {
                incr = true;
            } else {
                break;
            }
            i += 1;
        }
    }
    let rest = &args[i..];
    if rest.is_empty() || !rest.len().is_multiple_of(2) {
        return ctx.out.error(SYNTAX);
    }
    if nx && xx {
        return ctx
            .out
            .error("ERR XX and NX options at the same time are not compatible");
    }
    if (gt && nx) || (lt && nx) || (gt && lt) {
        return ctx
            .out
            .error("ERR GT, LT, and/or NX options at the same time are not compatible");
    }
    if incr && rest.len() > 2 {
        return ctx
            .out
            .error("ERR INCR option supports a single increment-element pair");
    }
    let mut pairs = Vec::with_capacity(rest.len() / 2);
    for p in rest.chunks(2) {
        let Some(s) = parse_f64(p[0]) else {
            return ctx.out.error(NOT_FLOAT);
        };
        pairs.push((s, p[1]));
    }
    let lim = ctx.db.limits;
    let max_len = pairs.iter().map(|(_, m)| m.len()).max().unwrap_or(0);
    match ctx.db.get_mut(key) {
        // With XX nothing new can be added, so Redis doesn't pre-convert.
        Some(Value::ZSet(z)) if !xx => z.maybe_convert(pairs.len(), max_len, &lim),
        Some(Value::ZSet(_)) => {}
        Some(_) => return ctx.out.error(WRONGTYPE),
        None if xx => {
            return if incr { ctx.out.null() } else { ctx.out.int(0) };
        }
        None => ctx.db.insert_new(
            key,
            Value::ZSet(Box::new(ZSet::create(pairs.len(), max_len, &lim))),
        ),
    }
    let Some(Value::ZSet(z)) = ctx.db.get_mut(key) else {
        unreachable!()
    };
    let (mut added, mut updated, mut processed) = (0, 0, 0);
    let mut last_score = 0.0;
    for (mut score, member) in pairs {
        match z.score(member) {
            Some(cur) => {
                if nx {
                    continue;
                }
                if incr {
                    score += cur;
                    if score.is_nan() {
                        ctx.db.delete_if_empty(key);
                        return ctx.out.error("ERR resulting score is not a number (NaN)");
                    }
                }
                if (lt && score >= cur) || (gt && score <= cur) {
                    continue;
                }
                last_score = score;
                processed += 1;
                if score != cur {
                    z.update(member, cur, score, &lim);
                    updated += 1;
                }
            }
            None if !xx => {
                z.insert(member, score, &lim);
                added += 1;
                processed += 1;
                last_score = score;
            }
            None => {}
        }
    }
    ctx.db.delete_if_empty(key);
    if added + updated > 0 {
        ctx.db.modified(key);
        ctx.db.dirty += (added + updated - 1) as u64;
        ctx.db.signal_ready(key);
    }
    if incr {
        if processed > 0 {
            ctx.out.double(last_score)
        } else {
            ctx.out.null()
        }
    } else {
        ctx.out.int(if ch { added + updated } else { added })
    }
}

fn zadd(ctx: &mut Ctx, args: &[&[u8]]) {
    zadd_generic(ctx, args, false);
}

fn zincrby(ctx: &mut Ctx, args: &[&[u8]]) {
    zadd_generic(ctx, args, true);
}

fn zrem(ctx: &mut Ctx, args: &[&[u8]]) {
    let n = match ctx.db.get_mut(args[1]) {
        Some(Value::ZSet(z)) => args[2..].iter().filter(|m| z.remove(m)).count(),
        Some(_) => return ctx.out.error(WRONGTYPE),
        None => return ctx.out.int(0),
    };
    if n > 0 {
        ctx.db.delete_if_empty(args[1]);
        ctx.db.modified(args[1]);
    }
    ctx.out.int(n as i64);
}

fn zscore(ctx: &mut Ctx, args: &[&[u8]]) {
    match ctx.db.get(args[1]) {
        Some(Value::ZSet(z)) => match z.score(args[2]) {
            Some(s) => ctx.out.double(s),
            None => ctx.out.null(),
        },
        Some(_) => ctx.out.error(WRONGTYPE),
        None => ctx.out.null(),
    }
}

fn zmscore(ctx: &mut Ctx, args: &[&[u8]]) {
    let z = match ctx.db.get(args[1]) {
        Some(Value::ZSet(z)) => Some(z),
        Some(_) => return ctx.out.error(WRONGTYPE),
        None => None,
    };
    ctx.out.array(args.len() - 2);
    for m in &args[2..] {
        match z.and_then(|z| z.score(m)) {
            Some(s) => ctx.out.double(s),
            None => ctx.out.null(),
        }
    }
}

fn zcard(ctx: &mut Ctx, args: &[&[u8]]) {
    match ctx.db.get(args[1]) {
        Some(Value::ZSet(z)) => ctx.out.int(z.len() as i64),
        Some(_) => ctx.out.error(WRONGTYPE),
        None => ctx.out.int(0),
    }
}

fn parse_score_bound(a: &[u8]) -> Option<(f64, bool)> {
    let (body, ex) = match a.first() {
        Some(b'(') => (&a[1..], true),
        _ => (a, false),
    };
    // zslParseRange uses raw strtod: an empty string parses as 0.
    let v = if body.is_empty() {
        Some(0.0)
    } else {
        parse_f64_range(body)
    }?;
    Some((v, ex))
}

/// Parses `min max` score bounds (Redis error text on failure).
fn parse_score_range(ctx: &mut Ctx, min: &[u8], max: &[u8]) -> Option<ScoreRange> {
    match (parse_score_bound(min), parse_score_bound(max)) {
        (Some((min, minex)), Some((max, maxex))) => Some(ScoreRange {
            min,
            max,
            minex,
            maxex,
        }),
        _ => {
            ctx.out.error("ERR min or max is not a float");
            None
        }
    }
}

fn parse_lex_range(ctx: &mut Ctx, min: &[u8], max: &[u8]) -> Option<LexRange> {
    match (LexBound::parse(min), LexBound::parse(max)) {
        (Some(min), Some(max)) => Some(LexRange { min, max }),
        _ => {
            ctx.out.error("ERR min or max not valid string range item");
            None
        }
    }
}

fn zcount(ctx: &mut Ctx, args: &[&[u8]]) {
    let Some(r) = parse_score_range(ctx, args[2], args[3]) else {
        return;
    };
    match ctx.db.get(args[1]) {
        Some(Value::ZSet(z)) => ctx.out.int(if r.is_empty() {
            0
        } else {
            z.count_score(&r) as i64
        }),
        Some(_) => ctx.out.error(WRONGTYPE),
        None => ctx.out.int(0),
    }
}

fn zlexcount(ctx: &mut Ctx, args: &[&[u8]]) {
    let Some(r) = parse_lex_range(ctx, args[2], args[3]) else {
        return;
    };
    match ctx.db.get(args[1]) {
        Some(Value::ZSet(z)) => ctx.out.int(z.count_lex(&r)),
        Some(_) => ctx.out.error(WRONGTYPE),
        None => ctx.out.int(0),
    }
}

fn zrank_generic(ctx: &mut Ctx, args: &[&[u8]], rev: bool) {
    let withscore = match args.len() {
        3 => false,
        4 if eq_ic(args[3], "withscore") => true,
        _ => return ctx.out.error(SYNTAX),
    };
    let r = match ctx.db.get(args[1]) {
        Some(Value::ZSet(z)) => z.rank(args[2], rev),
        Some(_) => return ctx.out.error(WRONGTYPE),
        None => None,
    };
    match (r, withscore) {
        (Some((rank, _)), false) => ctx.out.int(rank as i64),
        (Some((rank, s)), true) => {
            ctx.out.array(2);
            ctx.out.int(rank as i64);
            ctx.out.double(s);
        }
        (None, false) => ctx.out.null(),
        (None, true) => ctx.out.null_array(),
    }
}

fn zrank(ctx: &mut Ctx, args: &[&[u8]]) {
    zrank_generic(ctx, args, false);
}
fn zrevrank(ctx: &mut Ctx, args: &[&[u8]]) {
    zrank_generic(ctx, args, true);
}

#[derive(Clone, Copy, PartialEq)]
enum RangeType {
    Auto,
    Rank,
    Score,
    Lex,
}

enum Range {
    Rank(i64, i64),
    Score(ScoreRange),
    Lex(LexRange),
}

struct RangeReq {
    range: Range,
    rev: bool,
    offset: i64,
    limit: i64,
    withscores: bool,
}

/// Parses the shared ZRANGE grammar. `start` indexes the `<key>` argument.
fn parse_range(
    ctx: &mut Ctx,
    args: &[&[u8]],
    start: usize,
    store: bool,
    mut rtype: RangeType,
    mut rev: Option<bool>,
) -> Option<RangeReq> {
    let (mut withscores, mut offset, mut limit) = (false, 0i64, -1i64);
    let mut j = start + 3;
    while j < args.len() {
        let a = args[j];
        let left = args.len() - j - 1;
        if !store && eq_ic(a, "withscores") {
            withscores = true;
        } else if eq_ic(a, "limit") && left >= 2 {
            let (Some(o), Some(l)) = (parse_i64(args[j + 1]), parse_i64(args[j + 2])) else {
                ctx.out.error(NOT_INT);
                return None;
            };
            offset = o;
            limit = l;
            j += 2;
        } else if rev.is_none() && eq_ic(a, "rev") {
            rev = Some(true);
        } else if rtype == RangeType::Auto && eq_ic(a, "bylex") {
            rtype = RangeType::Lex;
        } else if rtype == RangeType::Auto && eq_ic(a, "byscore") {
            rtype = RangeType::Score;
        } else {
            ctx.out.error(SYNTAX);
            return None;
        }
        j += 1;
    }
    let rev = rev.unwrap_or(false);
    if rtype == RangeType::Auto {
        rtype = RangeType::Rank;
    }
    if limit != -1 && rtype == RangeType::Rank {
        ctx.out.error(
            "ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX",
        );
        return None;
    }
    if withscores && rtype == RangeType::Lex {
        ctx.out
            .error("ERR syntax error, WITHSCORES not supported in combination with BYLEX");
        return None;
    }
    let (mut lo, mut hi) = (args[start + 1], args[start + 2]);
    if rev && rtype != RangeType::Rank {
        std::mem::swap(&mut lo, &mut hi);
    }
    let range = match rtype {
        RangeType::Rank => {
            let (Some(a), Some(b)) = (parse_i64(lo), parse_i64(hi)) else {
                ctx.out.error(NOT_INT);
                return None;
            };
            Range::Rank(a, b)
        }
        RangeType::Score => Range::Score(parse_score_range(ctx, lo, hi)?),
        _ => Range::Lex(parse_lex_range(ctx, lo, hi)?),
    };
    Some(RangeReq {
        range,
        rev,
        offset,
        limit,
        withscores,
    })
}

fn collect_range(z: &ZSet, req: &RangeReq) -> Vec<(Vec<u8>, f64)> {
    let mut out = Vec::new();
    let mut push = |m: &[u8], s: f64| out.push((m.to_vec(), s));
    match &req.range {
        Range::Rank(a, b) => {
            if let Some((s, e)) = normalize_range(*a, *b, z.len()) {
                z.range_by_rank(s, e, req.rev, &mut push);
            }
        }
        Range::Score(r) => {
            if req.offset >= 0
                && !r.is_empty()
                && (req.offset == 0 || (req.offset as usize) < z.len())
            {
                z.range_by_score(r, req.rev, req.offset as usize, req.limit, &mut push);
            }
        }
        Range::Lex(r) => {
            if req.offset >= 0 && (req.offset == 0 || (req.offset as usize) < z.len()) {
                z.range_by_lex(r, req.rev, req.offset as usize, req.limit, &mut push);
            }
        }
    }
    out
}

fn reply_elements(ctx: &mut Ctx, items: &[(Vec<u8>, f64)], withscores: bool) {
    if withscores && ctx.out.resp3 {
        ctx.out.array(items.len());
        for (m, s) in items {
            ctx.out.array(2);
            ctx.out.bulk(m);
            ctx.out.double(*s);
        }
    } else {
        ctx.out.array(items.len() * if withscores { 2 } else { 1 });
        for (m, s) in items {
            ctx.out.bulk(m);
            if withscores {
                ctx.out.double(*s);
            }
        }
    }
}

fn zrange_generic(ctx: &mut Ctx, args: &[&[u8]], rtype: RangeType, rev: Option<bool>) {
    let Some(req) = parse_range(ctx, args, 1, false, rtype, rev) else {
        return;
    };
    let items = match ctx.db.get(args[1]) {
        Some(Value::ZSet(z)) => collect_range(z, &req),
        Some(_) => return ctx.out.error(WRONGTYPE),
        None => return ctx.out.array(0),
    };
    reply_elements(ctx, &items, req.withscores);
}

fn zrange(ctx: &mut Ctx, args: &[&[u8]]) {
    zrange_generic(ctx, args, RangeType::Auto, None);
}
fn zrevrange(ctx: &mut Ctx, args: &[&[u8]]) {
    zrange_generic(ctx, args, RangeType::Rank, Some(true));
}
fn zrangebyscore(ctx: &mut Ctx, args: &[&[u8]]) {
    zrange_generic(ctx, args, RangeType::Score, Some(false));
}
fn zrevrangebyscore(ctx: &mut Ctx, args: &[&[u8]]) {
    zrange_generic(ctx, args, RangeType::Score, Some(true));
}
fn zrangebylex(ctx: &mut Ctx, args: &[&[u8]]) {
    zrange_generic(ctx, args, RangeType::Lex, Some(false));
}
fn zrevrangebylex(ctx: &mut Ctx, args: &[&[u8]]) {
    zrange_generic(ctx, args, RangeType::Lex, Some(true));
}

/// Replaces `dst` with a zset holding `items` (deletes it when empty) and
/// replies with the cardinality.
/// `listpack_first` mirrors ZRANGESTORE, which starts from a listpack and
/// converts while inserting (so scores written before a conversion went
/// through the listpack, normalizing `-0` to `0`); the algebra commands build a
/// skiplist and shrink it afterwards.
fn store_zset(ctx: &mut Ctx, dst: &[u8], items: Vec<(Vec<u8>, f64)>, listpack_first: bool) {
    let existed = ctx.db.remove(dst).is_some();
    let n = items.len();
    if n > 0 {
        let lim = ctx.db.limits;
        let max_len = if listpack_first {
            0
        } else {
            items.iter().map(|(m, _)| m.len()).max().unwrap_or(0)
        };
        let mut z = ZSet::create(n, max_len, &lim);
        for (m, s) in &items {
            z.insert(m, *s, &lim);
        }
        ctx.db.set(dst, Value::ZSet(Box::new(z)), false);
        ctx.db.modified(dst);
        ctx.db.signal_ready(dst);
    } else if existed {
        ctx.db.modified(dst);
    }
    ctx.out.int(n as i64);
}

fn zrangestore(ctx: &mut Ctx, args: &[&[u8]]) {
    let Some(req) = parse_range(ctx, args, 2, true, RangeType::Auto, None) else {
        return;
    };
    let items = match ctx.db.get(args[2]) {
        Some(Value::ZSet(z)) => collect_range(z, &req),
        Some(_) => return ctx.out.error(WRONGTYPE),
        None => Vec::new(),
    };
    store_zset(ctx, args[1], items, true);
}

fn zremrange_generic(ctx: &mut Ctx, args: &[&[u8]], which: RangeType) {
    let range = match which {
        RangeType::Rank => {
            let (Some(a), Some(b)) = (parse_i64(args[2]), parse_i64(args[3])) else {
                return ctx.out.error(NOT_INT);
            };
            Range::Rank(a, b)
        }
        RangeType::Score => match parse_score_range(ctx, args[2], args[3]) {
            Some(r) => Range::Score(r),
            None => return,
        },
        _ => match parse_lex_range(ctx, args[2], args[3]) {
            Some(r) => Range::Lex(r),
            None => return,
        },
    };
    let n = match ctx.db.get_mut(args[1]) {
        Some(Value::ZSet(z)) => match &range {
            Range::Rank(a, b) => match normalize_range(*a, *b, z.len()) {
                Some((s, e)) => z.remove_rank_range(s, e),
                None => 0,
            },
            Range::Score(r) => {
                if r.is_empty() {
                    0
                } else {
                    z.remove_score_range(r)
                }
            }
            Range::Lex(r) => z.remove_lex_range(r),
        },
        Some(_) => return ctx.out.error(WRONGTYPE),
        None => return ctx.out.int(0),
    };
    if n > 0 {
        ctx.db.delete_if_empty(args[1]);
        ctx.db.modified(args[1]);
        ctx.db.dirty += n as u64 - 1;
    }
    ctx.out.int(n as i64);
}

fn zremrangebyrank(ctx: &mut Ctx, args: &[&[u8]]) {
    zremrange_generic(ctx, args, RangeType::Rank);
}
fn zremrangebyscore(ctx: &mut Ctx, args: &[&[u8]]) {
    zremrange_generic(ctx, args, RangeType::Score);
}
fn zremrangebylex(ctx: &mut Ctx, args: &[&[u8]]) {
    zremrange_generic(ctx, args, RangeType::Lex);
}

fn zpop_generic(ctx: &mut Ctx, args: &[&[u8]], max: bool) {
    let count = match args.len() {
        2 => None,
        3 => match parse_count(ctx, args[2]) {
            Some(c) => Some(c as usize),
            None => return,
        },
        _ => return ctx.arity_error(if max { "zpopmax" } else { "zpopmin" }),
    };
    let nested = ctx.out.resp3 && count.is_some();
    let z = match ctx.db.get_mut(args[1]) {
        Some(Value::ZSet(z)) => z,
        Some(_) => return ctx.out.error(WRONGTYPE),
        None => return ctx.out.array(0),
    };
    let n = count.unwrap_or(1).min(z.len());
    let popped: Vec<(Vec<u8>, f64)> = (0..n).filter_map(|_| z.pop(max)).collect();
    if nested {
        ctx.out.array(popped.len());
    } else {
        ctx.out.array(popped.len() * 2);
    }
    for (m, s) in &popped {
        if nested {
            ctx.out.array(2);
        }
        ctx.out.bulk(m);
        ctx.out.double(*s);
    }
    if !popped.is_empty() {
        ctx.db.delete_if_empty(args[1]);
        ctx.db.modified(args[1]);
    }
}

fn zpopmin(ctx: &mut Ctx, args: &[&[u8]]) {
    zpop_generic(ctx, args, false);
}
fn zpopmax(ctx: &mut Ctx, args: &[&[u8]]) {
    zpop_generic(ctx, args, true);
}

fn bzpop(ctx: &mut Ctx, args: &[&[u8]], max: bool) {
    let keys = &args[1..args.len() - 1];
    let Some(deadline) = parse_timeout(ctx, args[args.len() - 1]) else {
        return;
    };
    for key in keys {
        match ctx.db.get_mut(key) {
            Some(Value::ZSet(z)) => {
                let (m, s) = z.pop(max).expect("non-empty");
                ctx.db.delete_if_empty(key);
                ctx.db.modified(key);
                ctx.propagate_as(vec![vec![
                    if max {
                        b"ZPOPMAX".to_vec()
                    } else {
                        b"ZPOPMIN".to_vec()
                    },
                    key.to_vec(),
                ]]);
                ctx.out.array(3);
                ctx.out.bulk(key);
                ctx.out.bulk(&m);
                ctx.out.double(s);
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
        kind: BlockKind::ZPop { max },
    });
}

fn bzpopmin(ctx: &mut Ctx, args: &[&[u8]]) {
    bzpop(ctx, args, false);
}
fn bzpopmax(ctx: &mut Ctx, args: &[&[u8]]) {
    bzpop(ctx, args, true);
}

fn parse_zmpop(ctx: &mut Ctx, args: &[&[u8]], at: usize) -> Option<(Vec<Vec<u8>>, bool, usize)> {
    let n = parse_i64(args[at]).unwrap_or(0);
    if n <= 0 || n as usize > args.len() {
        ctx.out.error("ERR numkeys should be greater than 0");
        return None;
    }
    let keys_end = at + 1 + n as usize;
    if keys_end >= args.len() {
        ctx.out.error(SYNTAX);
        return None;
    }
    let keys = args[at + 1..keys_end].iter().map(|k| k.to_vec()).collect();
    let max = if eq_ic(args[keys_end], "max") {
        true
    } else if eq_ic(args[keys_end], "min") {
        false
    } else {
        ctx.out.error(SYNTAX);
        return None;
    };
    let mut count = 1;
    let mut i = keys_end + 1;
    let mut seen = false;
    while i < args.len() {
        if eq_ic(args[i], "count") && i + 1 < args.len() && !seen {
            match parse_i64(args[i + 1]) {
                Some(c) if c > 0 => count = c as usize,
                _ => {
                    ctx.out.error("ERR count should be greater than 0");
                    return None;
                }
            }
            seen = true;
            i += 2;
        } else {
            ctx.out.error(SYNTAX);
            return None;
        }
    }
    Some((keys, max, count))
}

fn zmpop_once(ctx: &mut Ctx, keys: &[Vec<u8>], max: bool, count: usize) -> Result<bool, ()> {
    for key in keys {
        match ctx.db.get_mut(key) {
            Some(Value::ZSet(z)) => {
                let n = count.min(z.len());
                let popped: Vec<(Vec<u8>, f64)> = (0..n).filter_map(|_| z.pop(max)).collect();
                ctx.out.array(2);
                ctx.out.bulk(key);
                ctx.out.array(popped.len());
                for (m, s) in &popped {
                    ctx.out.array(2);
                    ctx.out.bulk(m);
                    ctx.out.double(*s);
                }
                ctx.db.delete_if_empty(key);
                ctx.db.modified(key);
                ctx.propagate_as(vec![vec![
                    if max {
                        b"ZPOPMAX".to_vec()
                    } else {
                        b"ZPOPMIN".to_vec()
                    },
                    key.clone(),
                    n.to_string().into_bytes(),
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

fn zmpop(ctx: &mut Ctx, args: &[&[u8]]) {
    let Some((keys, max, count)) = parse_zmpop(ctx, args, 1) else {
        return;
    };
    if let Ok(false) = zmpop_once(ctx, &keys, max, count) {
        ctx.out.null_array();
    }
}

fn bzmpop(ctx: &mut Ctx, args: &[&[u8]]) {
    let Some(deadline) = parse_timeout(ctx, args[1]) else {
        return;
    };
    let Some((keys, max, count)) = parse_zmpop(ctx, args, 2) else {
        return;
    };
    if let Ok(false) = zmpop_once(ctx, &keys, max, count) {
        if ctx.in_exec {
            return ctx.out.null_array();
        }
        ctx.block = Some(BlockRequest {
            keys: keys.iter().map(|k| Sds::from_slice(k)).collect(),
            deadline,
            kind: BlockKind::ZMPop { max, count },
        });
    }
}

fn zrandmember(ctx: &mut Ctx, args: &[&[u8]]) {
    let (count, withscores) = match args.len() {
        2 => (None, false),
        3 => match parse_i64(args[2]) {
            Some(c) => (Some(c), false),
            None => return ctx.out.error(NOT_INT),
        },
        4 if eq_ic(args[3], "withscores") => match parse_i64(args[2]) {
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
    let z = match ctx.db.get(args[1]) {
        Some(Value::ZSet(z)) => z,
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
        let (m, _) = z.random_element().expect("non-empty");
        return ctx.out.bulk(&m);
    };
    let items: Vec<(Vec<u8>, f64)> = if c >= 0 {
        let mut all = Vec::with_capacity(z.len());
        z.for_each(|m, s| all.push((m.to_vec(), s)));
        let n = (c as usize).min(all.len());
        for i in 0..n {
            let j = i + crate::util::rand_below((all.len() - i) as u64) as usize;
            all.swap(i, j);
        }
        all.truncate(n);
        all
    } else {
        (0..c.unsigned_abs())
            .filter_map(|_| z.random_element())
            .collect()
    };
    reply_elements(ctx, &items, withscores);
}

fn zscan(ctx: &mut Ctx, args: &[&[u8]]) {
    let Some(cursor) = parse_cursor(ctx, args[2]) else {
        return;
    };
    let mut noscores = false;
    let mut rest: Vec<&[u8]> = args[..3].to_vec();
    for a in &args[3..] {
        if eq_ic(a, "noscores") {
            noscores = true;
        } else {
            rest.push(a);
        }
    }
    let Some(opts) = parse_scan_opts(ctx, &rest, 3, false) else {
        return;
    };
    let z = match ctx.db.get(args[1]) {
        Some(Value::ZSet(z)) => z,
        Some(_) => return ctx.out.error(WRONGTYPE),
        None => {
            ctx.out.array(2);
            ctx.out.bulk(b"0");
            return ctx.out.array(0);
        }
    };
    let mut items = Vec::new();
    let next = z.scan(cursor, opts.count, |m, s| {
        if opts
            .pattern
            .as_ref()
            .is_none_or(|p| glob_match(p, m, false))
        {
            items.push((m.to_vec(), s));
        }
    });
    ctx.out.array(2);
    ctx.out.bulk(next.to_string().as_bytes());
    ctx.out.array(items.len() * if noscores { 1 } else { 2 });
    for (m, s) in items {
        ctx.out.bulk(&m);
        if !noscores {
            ctx.out.bulk(crate::util::fmt_f64(s).as_bytes());
        }
    }
}

#[derive(Clone, Copy)]
enum Agg {
    Sum,
    Min,
    Max,
}

#[derive(Clone, Copy, PartialEq)]
enum SetOp {
    Union,
    Inter,
    Diff,
}

struct AlgebraReq {
    keys: Vec<Vec<u8>>,
    weights: Vec<f64>,
    agg: Agg,
    withscores: bool,
    limit: usize,
}

/// Parses `numkeys key... [WEIGHTS ...] [AGGREGATE ...] [WITHSCORES]`.
fn parse_algebra(
    ctx: &mut Ctx,
    args: &[&[u8]],
    at: usize,
    op: SetOp,
    store: bool,
    name: &str,
) -> Option<AlgebraReq> {
    let Some(n) = parse_i64(args[at]) else {
        ctx.out.error(NOT_INT);
        return None;
    };
    if n < 1 {
        ctx.out.error(&format!(
            "ERR at least 1 input key is needed for '{name}' command"
        ));
        return None;
    }
    let n = n as usize;
    if at + 1 + n > args.len() {
        ctx.out.error(SYNTAX);
        return None;
    }
    let keys: Vec<Vec<u8>> = args[at + 1..at + 1 + n]
        .iter()
        .map(|k| k.to_vec())
        .collect();
    let mut req = AlgebraReq {
        keys,
        weights: vec![1.0; n],
        agg: Agg::Sum,
        withscores: false,
        limit: usize::MAX,
    };
    let mut j = at + 1 + n;
    while j < args.len() {
        let left = args.len() - j - 1;
        let a = args[j];
        if op != SetOp::Diff && eq_ic(a, "weights") && left >= n {
            for w in 0..n {
                match parse_f64(args[j + 1 + w]) {
                    Some(v) => req.weights[w] = v,
                    None => {
                        ctx.out.error("ERR weight value is not a float");
                        return None;
                    }
                }
            }
            j += n;
        } else if op != SetOp::Diff && eq_ic(a, "aggregate") && left >= 1 {
            let v = args[j + 1];
            req.agg = if eq_ic(v, "sum") {
                Agg::Sum
            } else if eq_ic(v, "min") {
                Agg::Min
            } else if eq_ic(v, "max") {
                Agg::Max
            } else {
                ctx.out.error(SYNTAX);
                return None;
            };
            j += 1;
        } else if !store && eq_ic(a, "withscores") {
            req.withscores = true;
        } else {
            ctx.out.error(SYNTAX);
            return None;
        }
        j += 1;
    }
    Some(req)
}

/// Snapshot of one input (zset or plain set, whose members score 1).
type Elements = Vec<(Vec<u8>, f64)>;

fn input_elements(v: Option<&Value>) -> Result<Option<Elements>, ()> {
    match v {
        None => Ok(None),
        Some(Value::ZSet(z)) => {
            let mut e = Vec::with_capacity(z.len());
            z.for_each(|m, s| e.push((m.to_vec(), s)));
            Ok(Some(e))
        }
        Some(Value::Set(s)) => {
            let s: &Set = s;
            Ok(Some(s.members().into_iter().map(|m| (m, 1.0)).collect()))
        }
        Some(_) => Err(()),
    }
}

fn weighted(v: f64, w: f64) -> f64 {
    let r = v * w;
    if r.is_nan() { 0.0 } else { r }
}

fn aggregate(agg: Agg, acc: f64, v: f64) -> f64 {
    match agg {
        Agg::Sum => {
            let r = acc + v;
            if r.is_nan() { 0.0 } else { r }
        }
        // Plain comparisons, exactly like Redis: a NaN `v` never wins.
        Agg::Min => {
            if v < acc {
                v
            } else {
                acc
            }
        }
        Agg::Max => {
            if v > acc {
                v
            } else {
                acc
            }
        }
    }
}

fn compute_algebra(ctx: &mut Ctx, req: &AlgebraReq, op: SetOp) -> Result<Vec<(Vec<u8>, f64)>, ()> {
    let keys: Vec<&[u8]> = req.keys.iter().map(|k| &k[..]).collect();
    ctx.db.prepare(&keys);
    let mut inputs = Vec::with_capacity(keys.len());
    for k in &keys {
        match input_elements(ctx.db.peek(k)) {
            Ok(e) => inputs.push(e),
            Err(()) => {
                ctx.out.error(WRONGTYPE);
                return Err(());
            }
        }
    }
    let mut result: Vec<(Vec<u8>, f64)> = match op {
        SetOp::Union => {
            // Redis visits inputs from the smallest to the largest (stable
            // order for ties); weighted NaNs become 0 before aggregating.
            let mut order: Vec<usize> = (0..inputs.len()).collect();
            order.sort_by_key(|&i| inputs[i].as_ref().map_or(0, |v| v.len()));
            let mut acc: HashMap<Vec<u8>, f64> = HashMap::new();
            for &i in &order {
                for (m, s) in inputs[i].iter().flatten() {
                    let v = weighted(*s, req.weights[i]);
                    acc.entry(m.clone())
                        .and_modify(|a| *a = aggregate(req.agg, *a, v))
                        .or_insert(v);
                }
            }
            acc.into_iter().collect()
        }
        SetOp::Inter => {
            if inputs.iter().any(|i| i.is_none()) {
                Vec::new()
            } else {
                let maps: Vec<HashMap<&[u8], f64>> = inputs
                    .iter()
                    .map(|i| {
                        i.as_ref()
                            .expect("checked")
                            .iter()
                            .map(|(m, s)| (&m[..], *s))
                            .collect()
                    })
                    .collect();
                // Same order as Redis: smallest input drives the iteration and
                // only its weighted score is NaN-sanitized; later inputs are
                // folded in with `aggregate` (which zeroes a NaN sum).
                let mut order: Vec<usize> = (0..maps.len()).collect();
                order.sort_by_key(|&i| maps[i].len());
                let first = order[0];
                let mut out = Vec::new();
                for (m, s) in inputs[first].as_ref().expect("checked") {
                    if out.len() >= req.limit {
                        break;
                    }
                    let mut acc = weighted(*s, req.weights[first]);
                    let mut present = true;
                    for &j in &order[1..] {
                        match maps[j].get(&m[..]) {
                            Some(v) => acc = aggregate(req.agg, acc, v * req.weights[j]),
                            None => {
                                present = false;
                                break;
                            }
                        }
                    }
                    if present {
                        out.push((m.clone(), acc));
                    }
                }
                out
            }
        }
        SetOp::Diff => match &inputs[0] {
            None => Vec::new(),
            Some(first) => {
                let others: Vec<HashMap<&[u8], f64>> = inputs[1..]
                    .iter()
                    .flatten()
                    .map(|i| i.iter().map(|(m, s)| (&m[..], *s)).collect())
                    .collect();
                first
                    .iter()
                    .filter(|(m, _)| !others.iter().any(|o| o.contains_key(&m[..])))
                    .cloned()
                    .collect()
            }
        },
    };
    result.sort_by(|a, b| cmp_entry(a.1, &a.0, b.1, &b.0));
    Ok(result)
}

fn algebra_store(ctx: &mut Ctx, args: &[&[u8]], op: SetOp, name: &str) {
    let Some(req) = parse_algebra(ctx, args, 2, op, true, name) else {
        return;
    };
    let Ok(items) = compute_algebra(ctx, &req, op) else {
        return;
    };
    store_zset(ctx, args[1], items, false);
}

fn algebra_reply(ctx: &mut Ctx, args: &[&[u8]], op: SetOp, name: &str) {
    let Some(req) = parse_algebra(ctx, args, 1, op, false, name) else {
        return;
    };
    let Ok(items) = compute_algebra(ctx, &req, op) else {
        return;
    };
    reply_elements(ctx, &items, req.withscores);
}

fn zunionstore(ctx: &mut Ctx, args: &[&[u8]]) {
    algebra_store(ctx, args, SetOp::Union, "zunionstore");
}
fn zinterstore(ctx: &mut Ctx, args: &[&[u8]]) {
    algebra_store(ctx, args, SetOp::Inter, "zinterstore");
}
fn zdiffstore(ctx: &mut Ctx, args: &[&[u8]]) {
    algebra_store(ctx, args, SetOp::Diff, "zdiffstore");
}
fn zunion(ctx: &mut Ctx, args: &[&[u8]]) {
    algebra_reply(ctx, args, SetOp::Union, "zunion");
}
fn zinter(ctx: &mut Ctx, args: &[&[u8]]) {
    algebra_reply(ctx, args, SetOp::Inter, "zinter");
}
fn zdiff(ctx: &mut Ctx, args: &[&[u8]]) {
    algebra_reply(ctx, args, SetOp::Diff, "zdiff");
}

fn zintercard(ctx: &mut Ctx, args: &[&[u8]]) {
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
    let req = AlgebraReq {
        keys: args[2..2 + n].iter().map(|k| k.to_vec()).collect(),
        weights: vec![1.0; n],
        agg: Agg::Sum,
        withscores: false,
        limit,
    };
    if let Ok(items) = compute_algebra(ctx, &req, SetOp::Inter) {
        ctx.out.int(items.len().min(limit) as i64);
    }
}
