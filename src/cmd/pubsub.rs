//! Pub/sub commands.

use super::flags::*;
use super::{Command, Ctx, NOKEYS, c};
use crate::pubsub::confirm;
use crate::sds::Sds;
use crate::server::Server;
use crate::server::client::ClientState;
use std::sync::atomic::Ordering::Relaxed;

pub(crate) fn commands() -> Vec<Command> {
    let g = "pubsub";
    vec![
        c(
            "subscribe",
            -2,
            PUBSUB | SUB_OK | LOADING | STALE,
            NOKEYS,
            subscribe,
            g,
            "Listens for messages published to channels.",
        ),
        c(
            "unsubscribe",
            -1,
            PUBSUB | SUB_OK | LOADING | STALE,
            NOKEYS,
            unsubscribe,
            g,
            "Stops listening to messages posted to channels.",
        ),
        c(
            "psubscribe",
            -2,
            PUBSUB | SUB_OK | LOADING | STALE,
            NOKEYS,
            psubscribe,
            g,
            "Listens for messages published to channels that match one or more patterns.",
        ),
        c(
            "punsubscribe",
            -1,
            PUBSUB | SUB_OK | LOADING | STALE,
            NOKEYS,
            punsubscribe,
            g,
            "Stops listening to messages published to channels that match one or more patterns.",
        ),
        c(
            "publish",
            3,
            PUBSUB | FAST | LOADING | STALE,
            NOKEYS,
            publish,
            g,
            "Posts a message to a channel.",
        ),
        c(
            "pubsub",
            -2,
            PUBSUB | LOADING | STALE,
            NOKEYS,
            pubsub,
            g,
            "A container for Pub/Sub commands.",
        ),
    ]
}

fn sync_counts(client: &ClientState) {
    client.handle.sub.store(client.channels.len(), Relaxed);
    client.handle.psub.store(client.patterns.len(), Relaxed);
}

fn subscribe(ctx: &mut Ctx, args: &[&[u8]]) {
    let resp3 = ctx.client.resp3;
    let mut ps = ctx.server.pubsub.write();
    for ch in &args[1..] {
        if ctx.client.channels.insert(Sds::from_slice(ch)) {
            ps.subscribe(ch, &ctx.client.handle);
        }
        confirm(
            ctx.out.buf,
            resp3,
            b"subscribe",
            Some(ch),
            ctx.client.sub_count(),
        );
    }
    sync_counts(ctx.client);
}

fn psubscribe(ctx: &mut Ctx, args: &[&[u8]]) {
    let resp3 = ctx.client.resp3;
    let mut ps = ctx.server.pubsub.write();
    for p in &args[1..] {
        if ctx.client.patterns.insert(Sds::from_slice(p)) {
            ps.psubscribe(p, &ctx.client.handle);
        }
        confirm(
            ctx.out.buf,
            resp3,
            b"psubscribe",
            Some(p),
            ctx.client.sub_count(),
        );
    }
    sync_counts(ctx.client);
}

fn unsubscribe(ctx: &mut Ctx, args: &[&[u8]]) {
    let resp3 = ctx.client.resp3;
    let targets: Vec<Sds> = if args.len() > 1 {
        args[1..].iter().map(|a| Sds::from_slice(a)).collect()
    } else {
        ctx.client.channels.iter().cloned().collect()
    };
    if targets.is_empty() {
        confirm(
            ctx.out.buf,
            resp3,
            b"unsubscribe",
            None,
            ctx.client.sub_count(),
        );
        return;
    }
    let mut ps = ctx.server.pubsub.write();
    for ch in targets {
        if ctx.client.channels.remove(&ch) {
            ps.unsubscribe(&ch, ctx.client.id);
        }
        confirm(
            ctx.out.buf,
            resp3,
            b"unsubscribe",
            Some(&ch),
            ctx.client.sub_count(),
        );
    }
    sync_counts(ctx.client);
}

fn punsubscribe(ctx: &mut Ctx, args: &[&[u8]]) {
    let resp3 = ctx.client.resp3;
    let targets: Vec<Sds> = if args.len() > 1 {
        args[1..].iter().map(|a| Sds::from_slice(a)).collect()
    } else {
        ctx.client.patterns.iter().cloned().collect()
    };
    if targets.is_empty() {
        confirm(
            ctx.out.buf,
            resp3,
            b"punsubscribe",
            None,
            ctx.client.sub_count(),
        );
        return;
    }
    let mut ps = ctx.server.pubsub.write();
    for p in targets {
        if ctx.client.patterns.remove(&p) {
            ps.punsubscribe(&p, ctx.client.id);
        }
        confirm(
            ctx.out.buf,
            resp3,
            b"punsubscribe",
            Some(&p),
            ctx.client.sub_count(),
        );
    }
    sync_counts(ctx.client);
}

fn publish(ctx: &mut Ctx, args: &[&[u8]]) {
    let n = ctx
        .server
        .pubsub
        .read()
        .publish(args[1], args[2], ctx.client.id, ctx.out.buf);
    ctx.out.int(n as i64);
}

fn pubsub(ctx: &mut Ctx, args: &[&[u8]]) {
    let sub = args[1].to_ascii_lowercase();
    match sub.as_slice() {
        b"channels" if args.len() <= 3 => {
            let chans = ctx.server.pubsub.read().channels(args.get(2).copied());
            ctx.out.array(chans.len());
            for ch in chans {
                ctx.out.bulk(&ch);
            }
        }
        b"numsub" => {
            let ps = ctx.server.pubsub.read();
            ctx.out.map(args.len() - 2);
            for ch in &args[2..] {
                ctx.out.bulk(ch);
                ctx.out.int(ps.numsub(ch) as i64);
            }
        }
        b"numpat" if args.len() == 2 => {
            let n = ctx.server.pubsub.read().numpat();
            ctx.out.int(n as i64);
        }
        b"shardchannels" => ctx.out.array(0),
        b"shardnumsub" => {
            ctx.out.map(args.len() - 2);
            for ch in &args[2..] {
                ctx.out.bulk(ch);
                ctx.out.int(0);
            }
        }
        b"help" => {
            let lines = [
                "PUBSUB <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                "CHANNELS [<pattern>]",
                "NUMPAT",
                "NUMSUB [<channel> ...]",
                "HELP",
            ];
            ctx.out.array(lines.len());
            for l in lines {
                ctx.out.simple(l);
            }
        }
        _ => ctx.subcommand_error("pubsub", args[1]),
    }
}

/// Drops every subscription of a client (disconnect, RESET).
pub fn unsubscribe_all(server: &Server, client: &mut ClientState) {
    if client.channels.is_empty() && client.patterns.is_empty() {
        return;
    }
    let mut ps = server.pubsub.write();
    for ch in client.channels.drain() {
        ps.unsubscribe(&ch, client.id);
    }
    for p in client.patterns.drain() {
        ps.punsubscribe(&p, client.id);
    }
    sync_counts(client);
}
