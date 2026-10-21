//! Replication commands: REPLICAOF/SLAVEOF, PSYNC/SYNC, REPLCONF, WAIT, ROLE.

use super::flags::*;
use super::{Command, Ctx, NOKEYS, NOT_INT, c};
use crate::replication::{master, replica};
use crate::util::{eq_ic, parse_i64};
use std::sync::atomic::Ordering::Relaxed;

pub(crate) fn commands() -> Vec<Command> {
    let g = "server";
    vec![
        c(
            "replicaof",
            3,
            ADMIN | NO_MULTI | STALE,
            NOKEYS,
            replicaof,
            g,
            "Configures a server as replica of another, or promotes it to a master.",
        ),
        c(
            "slaveof",
            3,
            ADMIN | NO_MULTI | STALE,
            NOKEYS,
            replicaof,
            g,
            "Sets a Redis server as a replica of another, or promotes it to being a master.",
        ),
        c(
            "psync",
            -3,
            ADMIN | NO_MULTI,
            NOKEYS,
            psync,
            g,
            "An internal command used in replication.",
        ),
        c(
            "sync",
            1,
            ADMIN | NO_MULTI,
            NOKEYS,
            sync,
            g,
            "An internal command used in replication.",
        ),
        c(
            "replconf",
            -1,
            ADMIN | LOADING | STALE,
            NOKEYS,
            replconf,
            g,
            "An internal command for configuring the replication stream.",
        ),
        c(
            "wait",
            3,
            FAST,
            NOKEYS,
            wait,
            "generic",
            "Blocks until the asynchronous replication of all preceding write commands sent by the connection is completed.",
        ),
        c(
            "role",
            1,
            FAST | LOADING | STALE,
            NOKEYS,
            role,
            g,
            "Returns the replication role.",
        ),
    ]
}

fn replicaof(ctx: &mut Ctx, args: &[&[u8]]) {
    if eq_ic(args[1], "no") && eq_ic(args[2], "one") {
        replica::stop(ctx.server);
        return ctx.out.ok();
    }
    let Some(port) = parse_i64(args[2]).filter(|p| (0..=65535).contains(p)) else {
        return ctx.out.error("ERR Invalid master port");
    };
    let host = String::from_utf8_lossy(args[1]).into_owned();
    if ctx.server.is_replica()
        && *ctx.server.repl.master.lock() == Some((host.clone(), port as u16))
    {
        return ctx.out.simple("OK Already connected to specified master");
    }
    // A new master means a new history.
    *ctx.server.repl.master_replid.lock() = String::new();
    ctx.server.repl.offset.store(0, Relaxed);
    replica::start(ctx.server, host, port as u16);
    ctx.out.ok();
}

fn psync(ctx: &mut Ctx, args: &[&[u8]]) {
    if ctx.server.is_replica() {
        return ctx
            .out
            .error("ERR Chained replication is not supported by this server");
    }
    ctx.client.is_replica = true;
    let handle = ctx.client.handle.clone();
    master::psync(
        ctx.server,
        &handle,
        ctx.client.repl_port,
        args[1],
        args[2],
        ctx.out.buf,
    );
}

fn sync(ctx: &mut Ctx, _args: &[&[u8]]) {
    psync(ctx, &[b"psync", b"?", b"-1"]);
}

fn replconf(ctx: &mut Ctx, args: &[&[u8]]) {
    if args.len().is_multiple_of(2) {
        return ctx.out.error("ERR syntax error");
    }
    let mut i = 1;
    while i + 1 < args.len() {
        let (k, v) = (args[i], args[i + 1]);
        if eq_ic(k, "ack") {
            if let Some(off) = parse_i64(v) {
                master::ack(ctx.server, ctx.client.id, off.max(0) as u64);
            }
            return; // ACKs are never answered
        } else if eq_ic(k, "getack") {
            return; // only meaningful on replicas (handled by the link)
        } else if eq_ic(k, "listening-port") {
            match parse_i64(v) {
                Some(p) if (0..=65535).contains(&p) => ctx.client.repl_port = p as u16,
                _ => return ctx.out.error(NOT_INT),
            }
        } else if eq_ic(k, "capa")
            || eq_ic(k, "ip-address")
            || eq_ic(k, "rdb-only")
            || eq_ic(k, "rdb-filter-only")
        {
        } else {
            return ctx.out.error(&format!(
                "ERR Unrecognized REPLCONF option: {}",
                String::from_utf8_lossy(k)
            ));
        }
        i += 2;
    }
    ctx.out.ok();
}

fn wait(ctx: &mut Ctx, args: &[&[u8]]) {
    if ctx.server.is_replica() {
        return ctx.out.error("ERR WAIT cannot be used with replica instances. Please also note that since Redis 4.0 if a replica is configured to be writable (which is not the default) writes to replicas are just local and are not propagated.");
    }
    let (Some(n), Some(t)) = (parse_i64(args[1]), parse_i64(args[2])) else {
        return ctx.out.error(NOT_INT);
    };
    if t < 0 {
        return ctx.out.error("ERR timeout is negative");
    }
    let acked = master::wait(ctx.server, n.max(0) as usize, t as u64);
    ctx.out.int(acked as i64);
}

fn role(ctx: &mut Ctx, _args: &[&[u8]]) {
    if ctx.server.is_replica() {
        let (host, port) = ctx.server.repl.master.lock().clone().unwrap_or_default();
        let state = if ctx.server.repl.link_up.load(Relaxed) {
            "connected"
        } else if ctx.server.repl.sync_in_progress.load(Relaxed) {
            "sync"
        } else {
            "connecting"
        };
        ctx.out.array(5);
        ctx.out.bulk(b"slave");
        ctx.out.bulk(host.as_bytes());
        ctx.out.int(port as i64);
        ctx.out.bulk(state.as_bytes());
        ctx.out.int(ctx.server.repl.offset.load(Relaxed) as i64);
        return;
    }
    let p = ctx.server.prop.lock();
    ctx.out.array(3);
    ctx.out.bulk(b"master");
    ctx.out.int(p.repl.offset as i64);
    ctx.out.array(p.repl.replicas.len());
    for l in &p.repl.replicas {
        let ip = l
            .client
            .addr
            .rsplit_once(':')
            .map(|(h, _)| h.to_string())
            .unwrap_or_default();
        ctx.out.array(3);
        ctx.out.bulk(ip.as_bytes());
        ctx.out.bulk(l.port().to_string().as_bytes());
        ctx.out
            .bulk(l.ack_offset.load(Relaxed).to_string().as_bytes());
    }
}
