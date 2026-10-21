//! Stream commands: XADD, XRANGE/XREVRANGE, XLEN, XDEL, XTRIM, XREAD (with
//! BLOCK). Consumer groups are not implemented.

use super::flags::*;
use super::{BlockRequest, Command, Ctx, KeySpec, NOT_INT, SYNTAX, WRONGTYPE, c, k};
use crate::blocking::BlockKind;
use crate::resp::Reply;
use crate::sds::Sds;
use crate::types::Value;
use crate::types::stream::{Stream, StreamId};
use crate::util::{eq_ic, parse_i64};

const INVALID_ID: &str = "ERR Invalid stream ID specified as stream command argument";
/// Entries per Redis radix-tree node; `~` trimming works in node units.
const NODE_ENTRIES: usize = 100;

pub(crate) fn commands() -> Vec<Command> {
    let g = "stream";
    vec![
        c(
            "xadd",
            -5,
            WRITE | DENYOOM | FAST,
            k(1, 1, 1),
            xadd,
            g,
            "Appends a new message to a stream. Creates the key if it doesn't exist.",
        ),
        c(
            "xrange",
            -4,
            READONLY,
            k(1, 1, 1),
            xrange,
            g,
            "Returns the messages from a stream within a range of IDs.",
        ),
        c(
            "xrevrange",
            -4,
            READONLY,
            k(1, 1, 1),
            xrevrange,
            g,
            "Returns the messages from a stream within a range of IDs in reverse order.",
        ),
        c(
            "xlen",
            2,
            READONLY | FAST,
            k(1, 1, 1),
            xlen,
            g,
            "Return the number of messages in a stream.",
        ),
        c(
            "xdel",
            -3,
            WRITE | FAST,
            k(1, 1, 1),
            xdel,
            g,
            "Returns the number of messages after removing them from a stream.",
        ),
        c(
            "xtrim",
            -4,
            WRITE,
            k(1, 1, 1),
            xtrim,
            g,
            "Deletes messages from the beginning of a stream.",
        ),
        c(
            "xread",
            -4,
            READONLY | BLOCKING,
            KeySpec::Custom(xread_keys),
            xread,
            g,
            "Returns messages from multiple streams with IDs greater than the ones requested. Blocks until a message is available otherwise.",
        ),
    ]
}

/// Writes `[[id, [field, value, ...]], ...]`.
pub fn write_entries(r: &mut Reply, entries: &[(StreamId, &[Sds])]) {
    r.array(entries.len());
    for (id, fields) in entries {
        r.array(2);
        r.bulk(&id.to_bytes());
        r.array(fields.len());
        for f in fields.iter() {
            r.bulk(f);
        }
    }
}

enum Trim {
    MaxLen(usize),
    MinId(StreamId),
}

struct TrimSpec {
    trim: Trim,
    approx: bool,
    limit: Option<usize>,
}

/// Parses `MAXLEN|MINID [=|~] threshold [LIMIT n]` at `args[*i]`.
fn parse_trim(ctx: &mut Ctx, args: &[&[u8]], i: &mut usize) -> Option<Option<TrimSpec>> {
    let a = args[*i];
    let is_max = eq_ic(a, "maxlen");
    if !is_max && !eq_ic(a, "minid") {
        return Some(None);
    }
    *i += 1;
    let mut approx = false;
    if *i < args.len() && (args[*i] == b"~" || args[*i] == b"=") {
        approx = args[*i] == b"~";
        *i += 1;
    }
    if *i >= args.len() {
        ctx.out.error(SYNTAX);
        return None;
    }
    let trim = if is_max {
        let Some(n) = parse_i64(args[*i]) else {
            ctx.out.error(NOT_INT);
            return None;
        };
        if n < 0 {
            ctx.out.error("ERR The MAXLEN argument must be >= 0.");
            return None;
        }
        Trim::MaxLen(n as usize)
    } else {
        let Some(id) = StreamId::parse(args[*i], 0) else {
            ctx.out.error(INVALID_ID);
            return None;
        };
        Trim::MinId(id)
    };
    *i += 1;
    let mut limit = None;
    if *i + 1 < args.len() && eq_ic(args[*i], "limit") {
        let Some(n) = parse_i64(args[*i + 1]) else {
            ctx.out.error(NOT_INT);
            return None;
        };
        if n < 0 {
            ctx.out.error("ERR The LIMIT argument must be >= 0.");
            return None;
        }
        if !approx {
            ctx.out
                .error("ERR syntax error, LIMIT cannot be used without the special ~ option");
            return None;
        }
        limit = Some(n as usize);
        *i += 2;
    }
    Some(Some(TrimSpec {
        trim,
        approx,
        limit,
    }))
}

fn apply_trim(s: &mut Stream, t: &TrimSpec) -> usize {
    let limit = match (t.approx, t.limit) {
        (true, Some(0)) => usize::MAX,
        (true, Some(l)) => l,
        (true, None) => NODE_ENTRIES * 100,
        _ => usize::MAX,
    };
    match t.trim {
        Trim::MaxLen(n) => {
            let excess = s.len().saturating_sub(n);
            let remove = if t.approx {
                excess / NODE_ENTRIES * NODE_ENTRIES
            } else {
                excess
            };
            s.trim_maxlen(s.len() - remove, limit)
        }
        Trim::MinId(id) => {
            if t.approx {
                let below = s.iter().take_while(|(eid, _)| *eid < id).count();
                let remove = below / NODE_ENTRIES * NODE_ENTRIES;
                let keep = s.len() - remove;
                s.trim_maxlen(keep, limit)
            } else {
                s.trim_minid(id, limit)
            }
        }
    }
}

fn xadd(ctx: &mut Ctx, args: &[&[u8]]) {
    let key = args[1];
    let mut i = 2;
    let mut nomkstream = false;
    let mut trim = None;
    loop {
        if i >= args.len() {
            return ctx.out.error(SYNTAX);
        }
        if eq_ic(args[i], "nomkstream") {
            nomkstream = true;
            i += 1;
            continue;
        }
        match parse_trim(ctx, args, &mut i) {
            None => return,
            Some(Some(t)) => {
                trim = Some(t);
                continue;
            }
            Some(None) => break,
        }
    }
    let id_arg = args[i];
    let fields = &args[i + 1..];
    if fields.is_empty() || !fields.len().is_multiple_of(2) {
        return ctx.arity_error("xadd");
    }
    // Parse the requested ID: "*", "<ms>-*", "<ms>-<seq>" or "<ms>".
    enum Req {
        Auto,
        AutoSeq(u64),
        Explicit(StreamId),
    }
    let req = if id_arg == b"*" {
        Req::Auto
    } else if let Some(ms) = id_arg.strip_suffix(b"-*") {
        match std::str::from_utf8(ms)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
        {
            Some(ms) => Req::AutoSeq(ms),
            None => return ctx.out.error(INVALID_ID),
        }
    } else {
        match StreamId::parse(id_arg, 0) {
            Some(id) => Req::Explicit(id),
            None => return ctx.out.error(INVALID_ID),
        }
    };
    if let Req::Explicit(id) = &req
        && *id == StreamId::MIN
    {
        return ctx
            .out
            .error("ERR The ID specified in XADD must be greater than 0-0");
    }
    let now = ctx.db.now;
    match ctx.db.get_mut(key) {
        Some(Value::Stream(_)) => {}
        Some(_) => return ctx.out.error(WRONGTYPE),
        None if nomkstream => return ctx.out.null(),
        None => ctx.db.insert_new(key, Value::Stream(Box::default())),
    }
    let Some(Value::Stream(s)) = ctx.db.get_mut(key) else {
        unreachable!()
    };
    let last = s.last_id;
    let fresh = s.entries_added == 0 && s.is_empty();
    let id = match req {
        Req::Auto => {
            if now > last.ms {
                StreamId { ms: now, seq: 0 }
            } else {
                match last.incr() {
                    Some(id) => id,
                    None => return ctx.out.error("ERR The stream has exhausted the last possible ID, unable to add more items"),
                }
            }
        }
        Req::AutoSeq(ms) => {
            if ms > last.ms || (fresh && ms == 0 && last.ms == 0) {
                StreamId {
                    ms,
                    seq: if ms == 0 { 1 } else { 0 },
                }
            } else if ms == last.ms {
                match last.seq.checked_add(1) {
                    Some(seq) => StreamId { ms, seq },
                    None => return xadd_too_small(ctx, key),
                }
            } else {
                return xadd_too_small(ctx, key);
            }
        }
        Req::Explicit(id) => {
            if id <= last && !(fresh && last == StreamId::MIN) {
                return xadd_too_small(ctx, key);
            }
            id
        }
    };
    s.append(id, fields.iter().map(|f| Sds::from_slice(f)).collect());
    let trimmed = trim.as_ref().map_or(0, |t| apply_trim(s, t));
    let first_left = s.first_id();
    let id_bytes = id.to_bytes();
    let mut prop = vec![b"XADD".to_vec(), key.to_vec()];
    if nomkstream {
        prop.push(b"NOMKSTREAM".to_vec());
    }
    prop.push(id_bytes.clone());
    prop.extend(fields.iter().map(|f| f.to_vec()));
    let mut cmds = vec![prop];
    if trimmed > 0
        && let Some(first) = first_left
    {
        cmds.push(vec![
            b"XTRIM".to_vec(),
            key.to_vec(),
            b"MINID".to_vec(),
            b"=".to_vec(),
            first.to_bytes(),
        ]);
    }
    ctx.propagate_as(cmds);
    ctx.db.modified(key);
    ctx.db.signal_ready(key);
    ctx.out.bulk(&id_bytes);
}

fn xadd_too_small(ctx: &mut Ctx, key: &[u8]) {
    ctx.db.delete_if_empty(key);
    if let Some(Value::Stream(s)) = ctx.db.get(key)
        && s.is_empty()
        && s.entries_added == 0
    {
        ctx.db.remove(key);
    }
    ctx.out
        .error("ERR The ID specified in XADD is equal or smaller than the target stream top item");
}

/// Parses a range bound: `-`, `+`, `(id` (exclusive) or an ID where a missing
/// sequence defaults to 0 (start) or max (end).
fn parse_range_id(a: &[u8], is_start: bool) -> Result<Option<StreamId>, &'static str> {
    if a == b"-" {
        return Ok(Some(StreamId::MIN));
    }
    if a == b"+" {
        return Ok(Some(StreamId::MAX));
    }
    let (body, excl) = match a.first() {
        Some(b'(') => (&a[1..], true),
        _ => (a, false),
    };
    let id = StreamId::parse(body, if is_start { 0 } else { u64::MAX }).ok_or(INVALID_ID)?;
    if !excl {
        return Ok(Some(id));
    }
    let adj = if is_start { id.incr() } else { id.decr() };
    match adj {
        Some(id) => Ok(Some(id)),
        None => Err(if is_start {
            "ERR invalid start ID for the interval"
        } else {
            "ERR invalid end ID for the interval"
        }),
    }
}

fn xrange_generic(ctx: &mut Ctx, args: &[&[u8]], rev: bool) {
    let (sa, ea) = if rev {
        (args[3], args[2])
    } else {
        (args[2], args[3])
    };
    let start = match parse_range_id(sa, true) {
        Ok(Some(id)) => id,
        Ok(None) => return,
        Err(e) => return ctx.out.error(e),
    };
    let end = match parse_range_id(ea, false) {
        Ok(Some(id)) => id,
        Ok(None) => return,
        Err(e) => return ctx.out.error(e),
    };
    let mut count = usize::MAX;
    let mut j = 4;
    while j < args.len() {
        if args.len() - j >= 2 && eq_ic(args[j], "count") {
            let Some(n) = parse_i64(args[j + 1]) else {
                return ctx.out.error(NOT_INT);
            };
            count = if n < 0 { 0 } else { n as usize };
            j += 2;
        } else {
            return ctx.out.error(SYNTAX);
        }
    }
    let s = match ctx.db.get(args[1]) {
        Some(Value::Stream(s)) => s,
        Some(_) => return ctx.out.error(WRONGTYPE),
        None => return ctx.out.array(0),
    };
    if count == 0 {
        return ctx.out.null_array();
    }
    let entries = s.range(start, end, rev, count);
    write_entries(&mut ctx.out, &entries);
}

fn xrange(ctx: &mut Ctx, args: &[&[u8]]) {
    xrange_generic(ctx, args, false);
}
fn xrevrange(ctx: &mut Ctx, args: &[&[u8]]) {
    xrange_generic(ctx, args, true);
}

fn xlen(ctx: &mut Ctx, args: &[&[u8]]) {
    match ctx.db.get(args[1]) {
        Some(Value::Stream(s)) => ctx.out.int(s.len() as i64),
        Some(_) => ctx.out.error(WRONGTYPE),
        None => ctx.out.int(0),
    }
}

fn xdel(ctx: &mut Ctx, args: &[&[u8]]) {
    let mut ids = Vec::new();
    for a in &args[2..] {
        match StreamId::parse(a, 0) {
            Some(id) => ids.push(id),
            None => return ctx.out.error(INVALID_ID),
        }
    }
    let n = match ctx.db.get_mut(args[1]) {
        Some(Value::Stream(s)) => ids.iter().filter(|id| s.delete(**id)).count(),
        Some(_) => return ctx.out.error(WRONGTYPE),
        None => return ctx.out.int(0),
    };
    if n > 0 {
        ctx.db.modified(args[1]);
    }
    ctx.out.int(n as i64);
}

fn xtrim(ctx: &mut Ctx, args: &[&[u8]]) {
    let mut i = 2;
    let spec = match parse_trim(ctx, args, &mut i) {
        None => return,
        Some(None) => return ctx.out.error(SYNTAX),
        Some(Some(t)) => t,
    };
    if i != args.len() {
        return ctx.out.error(SYNTAX);
    }
    let n = match ctx.db.get_mut(args[1]) {
        Some(Value::Stream(s)) => {
            let n = apply_trim(s, &spec);
            if n > 0 && spec.approx {
                let first = s.first_id();
                let mut cmd = vec![
                    b"XTRIM".to_vec(),
                    args[1].to_vec(),
                    b"MINID".to_vec(),
                    b"=".to_vec(),
                ];
                cmd.push(first.map_or(b"0-0".to_vec(), |f| f.to_bytes()));
                if first.is_none() {
                    cmd = vec![
                        b"XTRIM".to_vec(),
                        args[1].to_vec(),
                        b"MAXLEN".to_vec(),
                        b"0".to_vec(),
                    ];
                }
                ctx.propagate_as(vec![cmd]);
            }
            n
        }
        Some(_) => return ctx.out.error(WRONGTYPE),
        None => return ctx.out.int(0),
    };
    if n > 0 {
        ctx.db.modified(args[1]);
    }
    ctx.out.int(n as i64);
}

fn xread_keys(args: &[&[u8]]) -> Vec<usize> {
    let Some(pos) = args.iter().position(|a| eq_ic(a, "streams")) else {
        return Vec::new();
    };
    let rest = args.len() - pos - 1;
    if rest == 0 || !rest.is_multiple_of(2) {
        return Vec::new();
    }
    (pos + 1..pos + 1 + rest / 2).collect()
}

fn xread(ctx: &mut Ctx, args: &[&[u8]]) {
    let mut count = usize::MAX;
    let mut block: Option<Option<u64>> = None;
    let mut i = 1;
    let mut streams_at = None;
    while i < args.len() {
        let a = args[i];
        if eq_ic(a, "streams") {
            streams_at = Some(i + 1);
            break;
        } else if eq_ic(a, "count") && i + 1 < args.len() {
            let Some(n) = parse_i64(args[i + 1]) else {
                return ctx.out.error(NOT_INT);
            };
            count = if n <= 0 { usize::MAX } else { n as usize };
            i += 2;
        } else if eq_ic(a, "block") && i + 1 < args.len() {
            let Some(ms) = parse_i64(args[i + 1]) else {
                return ctx
                    .out
                    .error("ERR timeout is not an integer or out of range");
            };
            if ms < 0 {
                return ctx.out.error("ERR timeout is negative");
            }
            block = Some(if ms == 0 {
                None
            } else {
                Some(ctx.db.now + ms as u64)
            });
            i += 2;
        } else {
            return ctx.out.error(SYNTAX);
        }
    }
    let Some(at) = streams_at else {
        return ctx.out.error(SYNTAX);
    };
    let rest = args.len() - at;
    if rest == 0 || !rest.is_multiple_of(2) {
        return ctx.out.error("ERR Unbalanced 'xread' list of streams: for each stream key an ID or '$' must be specified.");
    }
    let n = rest / 2;
    let keys = &args[at..at + n];
    let id_args = &args[at + n..];
    // Resolve IDs ("$" = current last ID) and type-check every key first.
    let mut ids = Vec::with_capacity(n);
    for (key, ida) in keys.iter().zip(id_args) {
        let last = match ctx.db.get(key) {
            Some(Value::Stream(s)) => Some(s.last_id),
            Some(_) => return ctx.out.error(WRONGTYPE),
            None => None,
        };
        let id = if *ida == b"$" {
            last.unwrap_or(StreamId::MIN)
        } else {
            match StreamId::parse(ida, 0) {
                Some(id) => id,
                None => return ctx.out.error(INVALID_ID),
            }
        };
        ids.push(id);
    }
    type Entries = Vec<(StreamId, Vec<Sds>)>;
    let mut results: Vec<(Vec<u8>, Entries)> = Vec::new();
    for (key, id) in keys.iter().zip(&ids) {
        if let Some(Value::Stream(s)) = ctx.db.get(key) {
            let e = s.after(*id, count);
            if !e.is_empty() {
                results.push((
                    key.to_vec(),
                    e.into_iter().map(|(i, f)| (i, f.to_vec())).collect(),
                ));
            }
        }
    }
    if !results.is_empty() {
        if ctx.out.resp3 {
            ctx.out.map(results.len());
        } else {
            ctx.out.array(results.len());
        }
        for (key, entries) in &results {
            if !ctx.out.resp3 {
                ctx.out.array(2);
            }
            ctx.out.bulk(key);
            let refs: Vec<(StreamId, &[Sds])> = entries.iter().map(|(i, f)| (*i, &f[..])).collect();
            write_entries(&mut ctx.out, &refs);
        }
        return;
    }
    match block {
        Some(deadline) if !ctx.in_exec => {
            ctx.block = Some(BlockRequest {
                keys: keys.iter().map(|k| Sds::from_slice(k)).collect(),
                deadline,
                kind: BlockKind::XRead { ids, count },
            });
        }
        _ => ctx.out.null_array(),
    }
}
