//! Transactions: MULTI, EXEC, DISCARD, WATCH, UNWATCH.
//!
//! `EXEC` itself is implemented in [`super::exec_transaction`] because it
//! needs to lock the union of the queued commands' shards.

use super::flags::*;
use super::{Command, Ctx, NOKEYS, c, k};
use crate::db::{WatchFlag, shard_of};
use crate::dict::hash_key;
use crate::sds::Sds;
use crate::server::Server;
use crate::server::client::ClientState;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;

pub(crate) fn commands() -> Vec<Command> {
    let g = "transactions";
    vec![
        c(
            "multi",
            1,
            FAST | LOADING | STALE,
            NOKEYS,
            multi,
            g,
            "Starts a transaction.",
        ),
        c(
            "exec",
            1,
            LOADING | STALE,
            NOKEYS,
            exec_placeholder,
            g,
            "Executes all commands in a transaction.",
        ),
        c(
            "discard",
            1,
            FAST | LOADING | STALE,
            NOKEYS,
            discard,
            g,
            "Discards a transaction.",
        ),
        c(
            "watch",
            -2,
            FAST | LOADING | STALE,
            k(1, -1, 1),
            watch,
            g,
            "Monitors changes to keys to determine the execution of a transaction.",
        ),
        c(
            "unwatch",
            1,
            FAST | LOADING | STALE,
            NOKEYS,
            unwatch,
            g,
            "Forgets about watched keys of a transaction.",
        ),
    ]
}

fn multi(ctx: &mut Ctx, _args: &[&[u8]]) {
    if ctx.client.multi.is_some() {
        return ctx.out.error("ERR MULTI calls can not be nested");
    }
    ctx.client.multi = Some(Vec::new());
    ctx.client.multi_error = false;
    ctx.client.handle.multi.store(0, Relaxed);
    ctx.out.ok();
}

fn exec_placeholder(ctx: &mut Ctx, _args: &[&[u8]]) {
    // Dispatched specially by `cmd::execute`; reaching here means EXEC was
    // called without MULTI from an internal context.
    ctx.out.error("ERR EXEC without MULTI");
}

fn discard(ctx: &mut Ctx, _args: &[&[u8]]) {
    if ctx.client.multi.take().is_none() {
        return ctx.out.error("ERR DISCARD without MULTI");
    }
    ctx.client.multi_error = false;
    ctx.client.handle.multi.store(-1, Relaxed);
    unwatch_ctx(ctx);
    ctx.out.ok();
}

fn watch(ctx: &mut Ctx, args: &[&[u8]]) {
    if ctx.client.multi.is_some() {
        return ctx.out.error("ERR WATCH inside MULTI is not allowed");
    }
    let flag = ctx
        .client
        .watch_flag
        .get_or_insert_with(|| Arc::new(WatchFlag::default()))
        .clone();
    let (db, now) = (ctx.db.sel, ctx.db.now);
    for key in &args[1..] {
        if ctx
            .client
            .watched
            .iter()
            .any(|(d, k, _)| *d == db && &k[..] == *key)
        {
            continue;
        }
        let h = hash_key(key);
        let expired = ctx.db.with_shard(shard_of(h), |sh| {
            sh.watched
                .entry((db, Sds::from_slice(key)))
                .or_default()
                .push(flag.clone());
            sh.dbs[db].is_expired(h, key, now)
        });
        ctx.client.watched.push((db, Sds::from_slice(key), expired));
    }
    ctx.client
        .handle
        .watch
        .store(ctx.client.watched.len(), Relaxed);
    ctx.out.ok();
}

fn unwatch(ctx: &mut Ctx, _args: &[&[u8]]) {
    unwatch_ctx(ctx);
    ctx.out.ok();
}

fn remove_watch(sh: &mut crate::db::Shard, db: usize, key: &Sds, flag: &Arc<WatchFlag>) {
    let wk = (db, key.clone());
    if let Some(list) = sh.watched.get_mut(&wk) {
        list.retain(|f| !Arc::ptr_eq(f, flag));
        if list.is_empty() {
            sh.watched.remove(&wk);
        }
    }
}

/// Unwatches using the shard locks the current command holds (safe inside
/// EXEC, which may hold every shard).
pub fn unwatch_ctx(ctx: &mut Ctx) {
    let Some(flag) = ctx.client.watch_flag.take() else {
        ctx.client.watched.clear();
        return;
    };
    for (db, key, _) in std::mem::take(&mut ctx.client.watched) {
        ctx.db.with_shard(shard_of(hash_key(&key)), |sh| {
            remove_watch(sh, db, &key, &flag)
        });
    }
    ctx.client.handle.watch.store(0, Relaxed);
}

/// Unwatches everything; the caller must hold no shard locks.
pub fn unwatch_all(server: &Server, client: &mut ClientState) {
    let Some(flag) = client.watch_flag.take() else {
        client.watched.clear();
        return;
    };
    for (db, key, _) in std::mem::take(&mut client.watched) {
        let mut sh = server.keyspace.lock(shard_of(hash_key(&key)));
        remove_watch(&mut sh, db, &key, &flag);
    }
    client.handle.watch.store(0, Relaxed);
}
