//! Connection commands: PING, ECHO, SELECT, HELLO, AUTH, QUIT, RESET, CLIENT.

use super::flags::*;
use super::{Command, Ctx, NOKEYS, NOT_INT, SYNTAX, c};
use crate::server::client::{ClientHandle, ClientKind};
use crate::util::{eq_ic, parse_i64};
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;

pub(crate) fn commands() -> Vec<Command> {
    let g = "connection";
    vec![
        c(
            "ping",
            -1,
            FAST | SUB_OK | LOADING | STALE,
            NOKEYS,
            ping,
            g,
            "Returns the server's liveliness response.",
        ),
        c(
            "echo",
            2,
            FAST | LOADING | STALE,
            NOKEYS,
            echo,
            g,
            "Returns the given string.",
        ),
        c(
            "select",
            2,
            FAST | LOADING | STALE,
            NOKEYS,
            select,
            g,
            "Changes the selected database.",
        ),
        c(
            "hello",
            -1,
            FAST | NO_AUTH | LOADING | STALE,
            NOKEYS,
            hello,
            g,
            "Handshakes with the Redis server.",
        ),
        c(
            "auth",
            -2,
            FAST | NO_AUTH | LOADING | STALE,
            NOKEYS,
            auth,
            g,
            "Authenticates the connection.",
        ),
        c(
            "quit",
            -1,
            FAST | NO_AUTH | SUB_OK | LOADING | STALE,
            NOKEYS,
            quit,
            g,
            "Closes the connection.",
        ),
        c(
            "reset",
            1,
            FAST | NO_AUTH | SUB_OK | LOADING | STALE,
            NOKEYS,
            reset,
            g,
            "Resets the connection.",
        ),
        c(
            "client",
            -2,
            ADMIN | LOADING | STALE,
            NOKEYS,
            client,
            g,
            "A container for client connection commands.",
        ),
    ]
}

fn ping(ctx: &mut Ctx, args: &[&[u8]]) {
    if args.len() > 2 {
        return ctx.arity_error("ping");
    }
    if ctx.client.in_resp2_pubsub() {
        ctx.out.array(2);
        ctx.out.bulk(b"pong");
        ctx.out.bulk(args.get(1).copied().unwrap_or(b""));
        return;
    }
    match args.get(1) {
        Some(m) => ctx.out.bulk(m),
        None => ctx.out.simple("PONG"),
    }
}

fn echo(ctx: &mut Ctx, args: &[&[u8]]) {
    ctx.out.bulk(args[1]);
}

fn select(ctx: &mut Ctx, args: &[&[u8]]) {
    let Some(v) = parse_i64(args[1]) else {
        return ctx.out.error(NOT_INT);
    };
    if v < 0 || v as usize >= ctx.server.keyspace.databases {
        return ctx.out.error("ERR DB index is out of range");
    }
    ctx.client.db = v as usize;
    ctx.db.sel = v as usize;
    ctx.client.handle.db.store(v as usize, Relaxed);
    ctx.out.ok();
}

fn check_password(ctx: &Ctx, user: Option<&[u8]>, pass: &[u8]) -> bool {
    let required = ctx.server.config.requirepass.read().clone();
    let user_ok = user.is_none_or(|u| u == b"default");
    user_ok && !required.is_empty() && pass == required.as_bytes()
}

const WRONGPASS: &str = "WRONGPASS invalid username-password pair or user is disabled.";

fn hello(ctx: &mut Ctx, args: &[&[u8]]) {
    let mut proto = None;
    let mut i = 1;
    if args.len() > 1 {
        match parse_i64(args[1]) {
            Some(v) => proto = Some(v),
            None => {
                return ctx
                    .out
                    .error("ERR Protocol version is not an integer or out of range");
            }
        }
        i = 2;
        if !(2..=3).contains(&proto.unwrap_or(0)) {
            return ctx.out.error("NOPROTO unsupported protocol version");
        }
    }
    let mut setname = None;
    while i < args.len() {
        let left = args.len() - i - 1;
        if eq_ic(args[i], "auth") && left >= 2 {
            if !check_password(ctx, Some(args[i + 1]), args[i + 2]) {
                return ctx.out.error(WRONGPASS);
            }
            ctx.client.authenticated = true;
            i += 3;
        } else if eq_ic(args[i], "setname") && left >= 1 {
            setname = Some(args[i + 1].to_vec());
            i += 2;
        } else {
            return ctx.out.error(&format!(
                "ERR Syntax error in HELLO option '{}'",
                String::from_utf8_lossy(args[i])
            ));
        }
    }
    if !ctx.client.authenticated {
        return ctx
            .out
            .error("NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time");
    }
    if let Some(n) = setname {
        if !valid_name(&n) {
            return ctx
                .out
                .error("ERR Client names cannot contain spaces, newlines or special characters.");
        }
        *ctx.client.handle.name.lock() = String::from_utf8_lossy(&n).into_owned();
    }
    if let Some(p) = proto {
        let r3 = p == 3;
        ctx.client.resp3 = r3;
        ctx.out.resp3 = r3;
        ctx.client.handle.resp3.store(r3, Relaxed);
    }
    let role = if ctx.server.is_replica() {
        "replica"
    } else {
        "master"
    };
    ctx.out.map(7);
    ctx.out.bulk(b"server");
    ctx.out.bulk(b"redis");
    ctx.out.bulk(b"version");
    ctx.out.bulk(crate::server::VERSION.as_bytes());
    ctx.out.bulk(b"proto");
    ctx.out.int(if ctx.client.resp3 { 3 } else { 2 });
    ctx.out.bulk(b"id");
    ctx.out.int(ctx.client.id as i64);
    ctx.out.bulk(b"mode");
    ctx.out.bulk(b"standalone");
    ctx.out.bulk(b"role");
    ctx.out.bulk(role.as_bytes());
    ctx.out.bulk(b"modules");
    ctx.out.array(0);
}

fn auth(ctx: &mut Ctx, args: &[&[u8]]) {
    if args.len() > 3 {
        return ctx.out.error(SYNTAX);
    }
    if ctx.server.config.requirepass.read().is_empty() && args.len() == 2 {
        return ctx.out.error("ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?");
    }
    let (user, pass) = if args.len() == 3 {
        (Some(args[1]), args[2])
    } else {
        (None, args[1])
    };
    if check_password(ctx, user, pass) {
        ctx.client.authenticated = true;
        ctx.out.ok();
    } else {
        ctx.out.error(WRONGPASS);
    }
}

fn quit(ctx: &mut Ctx, _args: &[&[u8]]) {
    ctx.client.close_after_reply = true;
    ctx.out.ok();
}

fn reset(ctx: &mut Ctx, _args: &[&[u8]]) {
    ctx.client.multi = None;
    ctx.client.multi_error = false;
    ctx.client.handle.multi.store(-1, Relaxed);
    super::multi::unwatch_ctx(ctx);
    super::pubsub::unsubscribe_all(ctx.server, ctx.client);
    ctx.client.db = 0;
    ctx.db.sel = 0;
    ctx.client.handle.db.store(0, Relaxed);
    ctx.client.resp3 = false;
    ctx.client.handle.resp3.store(false, Relaxed);
    ctx.client.reply_mode = 0;
    ctx.client.authenticated = ctx.server.config.requirepass.read().is_empty();
    ctx.out.resp3 = false;
    *ctx.client.handle.name.lock() = String::new();
    ctx.out.simple("RESET");
}

fn valid_name(n: &[u8]) -> bool {
    n.iter().all(|&c| (b'!'..=b'~').contains(&c))
}

/// One `CLIENT LIST` line.
pub fn client_line(h: &ClientHandle, now_ms: u64) -> String {
    let mut flags = String::new();
    match h.kind() {
        ClientKind::Replica => flags.push('S'),
        ClientKind::Master => flags.push('M'),
        ClientKind::Normal => {}
    }
    if h.sub.load(Relaxed) + h.psub.load(Relaxed) > 0 {
        flags.push('P');
    }
    if h.multi.load(Relaxed) >= 0 {
        flags.push('x');
    }
    if h.blocked.load(Relaxed) {
        flags.push('b');
    }
    if flags.is_empty() {
        flags.push('N');
    }
    let cmd = super::by_index(h.last_cmd.load(Relaxed)).map_or("NULL", |c| c.name);
    let qbuf = h.qbuf.load(Relaxed);
    let omem = h.omem.load(Relaxed);
    format!(
        "id={} addr={} laddr={} fd=-1 name={} age={} idle={} flags={} db={} sub={} psub={} ssub=0 multi={} watch={} qbuf={} qbuf-free=0 argv-mem=0 multi-mem=0 rbs=16384 rbp=0 obl=0 oll=0 omem={} tot-mem={} events=r cmd={} user=default redir=-1 resp={} lib-name={} lib-ver={} io-thread={}",
        h.id,
        h.addr,
        h.laddr,
        h.name.lock(),
        now_ms.saturating_sub(h.created_ms) / 1000,
        now_ms.saturating_sub(h.last_active_ms.load(Relaxed)) / 1000,
        flags,
        h.db.load(Relaxed),
        h.sub.load(Relaxed),
        h.psub.load(Relaxed),
        h.multi.load(Relaxed),
        h.watch.load(Relaxed),
        qbuf,
        omem,
        qbuf + omem + 16384,
        cmd,
        if h.resp3.load(Relaxed) { 3 } else { 2 },
        h.lib_name.lock(),
        h.lib_ver.lock(),
        h.worker().unwrap_or(0),
    )
}

fn type_matches(h: &ClientHandle, t: &[u8]) -> bool {
    match t.to_ascii_lowercase().as_slice() {
        b"normal" => {
            h.kind() == ClientKind::Normal && h.sub.load(Relaxed) + h.psub.load(Relaxed) == 0
        }
        b"replica" | b"slave" => h.kind() == ClientKind::Replica,
        b"master" => h.kind() == ClientKind::Master,
        b"pubsub" => h.sub.load(Relaxed) + h.psub.load(Relaxed) > 0,
        _ => false,
    }
}

fn client(ctx: &mut Ctx, args: &[&[u8]]) {
    let sub = args[1].to_ascii_lowercase();
    let now = ctx.db.now;
    match sub.as_slice() {
        b"id" if args.len() == 2 => ctx.out.int(ctx.client.id as i64),
        b"setname" if args.len() == 3 => {
            if !valid_name(args[2]) {
                return ctx.out.error(
                    "ERR Client names cannot contain spaces, newlines or special characters.",
                );
            }
            *ctx.client.handle.name.lock() = String::from_utf8_lossy(args[2]).into_owned();
            ctx.out.ok();
        }
        b"getname" if args.len() == 2 => {
            let n = ctx.client.handle.name.lock().clone();
            if n.is_empty() {
                ctx.out.null()
            } else {
                ctx.out.bulk(n.as_bytes())
            }
        }
        b"info" if args.len() == 2 => {
            let line = client_line(&ctx.client.handle, now) + "\n";
            ctx.out.verbatim(line.as_bytes());
        }
        b"list" => {
            let mut filter_type: Option<Vec<u8>> = None;
            let mut ids: Option<Vec<u64>> = None;
            let mut i = 2;
            while i < args.len() {
                if eq_ic(args[i], "type") && i + 1 < args.len() {
                    let t = args[i + 1].to_ascii_lowercase();
                    if !matches!(
                        t.as_slice(),
                        b"normal" | b"replica" | b"slave" | b"master" | b"pubsub"
                    ) {
                        return ctx.out.error(&format!(
                            "ERR Unknown client type '{}'",
                            String::from_utf8_lossy(args[i + 1])
                        ));
                    }
                    filter_type = Some(t);
                    i += 2;
                } else if eq_ic(args[i], "id") && i + 1 < args.len() {
                    let mut v = Vec::new();
                    for a in &args[i + 1..] {
                        match parse_i64(a) {
                            Some(id) if id > 0 => v.push(id as u64),
                            _ => return ctx.out.error("ERR Invalid client ID"),
                        }
                    }
                    ids = Some(v);
                    i = args.len();
                } else {
                    return ctx.out.error(SYNTAX);
                }
            }
            let mut clients: Vec<Arc<ClientHandle>> =
                ctx.server.clients.lock().values().cloned().collect();
            clients.sort_by_key(|h| h.id);
            let mut text = String::new();
            for h in clients {
                if filter_type.as_ref().is_some_and(|t| !type_matches(&h, t)) {
                    continue;
                }
                if ids.as_ref().is_some_and(|v| !v.contains(&h.id)) {
                    continue;
                }
                text.push_str(&client_line(&h, now));
                text.push('\n');
            }
            ctx.out.verbatim(text.as_bytes());
        }
        b"kill" => client_kill(ctx, args),
        b"setinfo" if args.len() == 4 => {
            let v = String::from_utf8_lossy(args[3]).into_owned();
            if v.contains(' ') || v.contains('\n') {
                return ctx.out.error(&format!(
                    "ERR {} cannot contain spaces, newlines or special characters.",
                    String::from_utf8_lossy(args[2]).to_ascii_lowercase()
                ));
            }
            if eq_ic(args[2], "lib-name") {
                *ctx.client.handle.lib_name.lock() = v;
            } else if eq_ic(args[2], "lib-ver") {
                *ctx.client.handle.lib_ver.lock() = v;
            } else {
                return ctx.out.error(&format!(
                    "ERR Unrecognized option '{}'",
                    String::from_utf8_lossy(args[2])
                ));
            }
            ctx.out.ok();
        }
        b"reply" if args.len() == 3 => {
            if eq_ic(args[2], "on") {
                ctx.client.reply_mode = 0;
                ctx.out.ok();
            } else if eq_ic(args[2], "off") {
                ctx.client.reply_mode = 1;
            } else if eq_ic(args[2], "skip") {
                ctx.client.reply_mode = 2;
            } else {
                ctx.out.error(SYNTAX);
            }
        }
        b"no-evict" | b"no-touch" if args.len() == 3 => {
            if eq_ic(args[2], "on") || eq_ic(args[2], "off") {
                ctx.out.ok()
            } else {
                ctx.out.error(SYNTAX)
            }
        }
        b"help" => {
            let lines = [
                "CLIENT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                "GETNAME",
                "ID",
                "INFO",
                "KILL <ip:port> | KILL <option> <value> [<option> <value> ...]",
                "LIST [TYPE (NORMAL|MASTER|REPLICA|PUBSUB)] [ID <id> ...]",
                "REPLY (ON|OFF|SKIP)",
                "SETINFO <option> <value>",
                "SETNAME <name>",
                "HELP",
            ];
            ctx.out.array(lines.len());
            for l in lines {
                ctx.out.simple(l);
            }
        }
        _ => {
            if matches!(
                sub.as_slice(),
                b"id"
                    | b"setname"
                    | b"getname"
                    | b"info"
                    | b"setinfo"
                    | b"reply"
                    | b"no-evict"
                    | b"no-touch"
            ) {
                ctx.out.error(&format!(
                    "ERR wrong number of arguments for 'client|{}' command",
                    String::from_utf8_lossy(&sub)
                ));
            } else {
                ctx.subcommand_error("client", args[1]);
            }
        }
    }
}

fn client_kill(ctx: &mut Ctx, args: &[&[u8]]) {
    let clients: Vec<Arc<ClientHandle>> = ctx.server.clients.lock().values().cloned().collect();
    if args.len() == 3 {
        let addr = String::from_utf8_lossy(args[2]).into_owned();
        return match clients.iter().find(|h| h.addr == addr) {
            Some(h) => {
                if h.id == ctx.client.id {
                    ctx.client.close_after_reply = true;
                } else {
                    h.kill();
                }
                ctx.out.ok()
            }
            None => ctx.out.error("ERR No such client"),
        };
    }
    if !args.len().is_multiple_of(2) {
        return ctx.out.error(SYNTAX);
    }
    let mut id = None;
    let mut addr = None;
    let mut ktype = None;
    let mut skipme = true;
    for kv in args[2..].chunks(2) {
        let (k, v) = (kv[0], kv[1]);
        if eq_ic(k, "id") {
            match parse_i64(v) {
                Some(n) if n > 0 => id = Some(n as u64),
                _ => return ctx.out.error("ERR client-id should be greater than 0"),
            }
        } else if eq_ic(k, "addr") {
            addr = Some(String::from_utf8_lossy(v).into_owned());
        } else if eq_ic(k, "type") {
            let t = v.to_ascii_lowercase();
            if !matches!(
                t.as_slice(),
                b"normal" | b"replica" | b"slave" | b"master" | b"pubsub"
            ) {
                return ctx.out.error(&format!(
                    "ERR Unknown client type '{}'",
                    String::from_utf8_lossy(v)
                ));
            }
            ktype = Some(t);
        } else if eq_ic(k, "skipme") {
            skipme = if eq_ic(v, "yes") {
                true
            } else if eq_ic(v, "no") {
                false
            } else {
                return ctx.out.error(SYNTAX);
            };
        } else if eq_ic(k, "user") || eq_ic(k, "laddr") || eq_ic(k, "maxage") {
            // Accepted; filters we don't track never match.
            if eq_ic(k, "user") && v != b"default" {
                return ctx.out.int(0);
            }
        } else {
            return ctx.out.error(SYNTAX);
        }
    }
    let mut killed = 0;
    for h in clients {
        if id.is_some_and(|i| i != h.id)
            || addr.as_ref().is_some_and(|a| *a != h.addr)
            || ktype.as_ref().is_some_and(|t| !type_matches(&h, t))
        {
            continue;
        }
        if h.id == ctx.client.id {
            if skipme {
                continue;
            }
            ctx.client.close_after_reply = true;
        } else {
            h.kill();
        }
        killed += 1;
    }
    ctx.out.int(killed);
}
