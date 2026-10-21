//! Blocking commands (`BLPOP`, `BLMOVE`, `BZPOPMIN`, `XREAD BLOCK`, ...).
//!
//! A client that must wait registers a [`BlockedClient`] in the FIFO queue of
//! every key it waits on. Registration happens while the dispatcher still
//! holds the keys' shard locks, so a concurrent push cannot slip in between
//! "the list is empty" and "I'm registered" (no lost wakeups).
//!
//! A command that adds data to a key with waiters records the key as
//! *ready*. After that command finishes and drops its locks, the same thread
//! calls [`serve_ready`], which pops on behalf of waiters in FIFO order and
//! delivers each reply through the waiter's cross-thread output queue. This
//! mirrors Redis' `handleClientsBlockedOnKeys`, which also runs right after
//! the command that made the key ready.
//!
//! Every waiter carries an atomic state; serving, timing out and
//! disconnecting all race through a single compare-and-swap, so a client is
//! answered exactly once.

use crate::db::{Shard, shard_of};
use crate::dict::hash_key;
use crate::resp::{Reply, encode_command};
use crate::sds::Sds;
use crate::server::Server;
use crate::server::client::ClientHandle;
use crate::types::Value;
use crate::types::stream::StreamId;
use parking_lot::MutexGuard;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

/// Waiting for data.
pub const WAITING: u8 = 0;
/// Served by a producer.
pub const SERVED: u8 = 1;
/// Timed out.
pub const TIMED_OUT: u8 = 2;
/// Client went away or was unblocked administratively.
pub const CANCELLED: u8 = 3;

/// What a blocked client will do once data arrives.
#[derive(Debug, Clone)]
pub enum BlockKind {
    /// `BLPOP` / `BRPOP`.
    ListPop {
        /// Pop from the head.
        front: bool,
    },
    /// `BLMPOP`.
    ListMPop {
        /// Pop from the head.
        front: bool,
        /// Max elements.
        count: usize,
    },
    /// `BLMOVE` / `BRPOPLPUSH`.
    ListMove {
        /// Destination key.
        dst: Sds,
        /// Pop from the source head.
        from_front: bool,
        /// Push to the destination head.
        to_front: bool,
    },
    /// `BZPOPMIN` / `BZPOPMAX`.
    ZPop {
        /// Pop the highest score.
        max: bool,
    },
    /// `BZMPOP`.
    ZMPop {
        /// Pop the highest scores.
        max: bool,
        /// Max elements.
        count: usize,
    },
    /// `XREAD BLOCK`: one last-seen ID per key (same order as `keys`).
    XRead {
        /// Per-key IDs.
        ids: Vec<StreamId>,
        /// `COUNT` (usize::MAX when absent).
        count: usize,
    },
}

impl BlockKind {
    fn wants(&self, v: &Value) -> bool {
        match self {
            BlockKind::ListPop { .. } | BlockKind::ListMPop { .. } | BlockKind::ListMove { .. } => {
                matches!(v, Value::List(l) if !l.is_empty())
            }
            BlockKind::ZPop { .. } | BlockKind::ZMPop { .. } => {
                matches!(v, Value::ZSet(z) if !z.is_empty())
            }
            BlockKind::XRead { .. } => matches!(v, Value::Stream(_)),
        }
    }

    /// Reply sent when the timeout expires.
    pub fn timeout_reply(&self, resp3: bool) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut r = Reply::new(&mut buf, resp3);
        match self {
            BlockKind::ListMove { .. } => r.null(),
            _ => r.null_array(),
        }
        buf
    }
}

/// A registered waiter.
pub struct BlockedClient {
    state: AtomicU8,
    /// The waiting client's shared handle (where the reply goes).
    pub client: Arc<ClientHandle>,
    /// Database of the keys.
    pub db: usize,
    /// Keys waited on.
    pub keys: Vec<Sds>,
    /// What to do when served.
    pub kind: BlockKind,
    /// Reply protocol.
    pub resp3: bool,
}

impl BlockedClient {
    /// Creates a waiter in the `WAITING` state.
    pub fn new(
        client: Arc<ClientHandle>,
        db: usize,
        keys: Vec<Sds>,
        kind: BlockKind,
        resp3: bool,
    ) -> Self {
        BlockedClient {
            state: AtomicU8::new(WAITING),
            client,
            db,
            keys,
            kind,
            resp3,
        }
    }

    /// Current state.
    pub fn state(&self) -> u8 {
        self.state.load(Ordering::Acquire)
    }

    /// True while waiting.
    pub fn is_waiting(&self) -> bool {
        self.state() == WAITING
    }

    /// Atomically moves `WAITING → to`. Exactly one party wins.
    pub fn claim(&self, to: u8) -> bool {
        self.state
            .compare_exchange(WAITING, to, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }
}

/// Removes `b`'s registrations from all of its keys (locks one shard at a time;
/// the caller must hold no shard locks).
pub fn unregister(server: &Server, b: &Arc<BlockedClient>) {
    for key in &b.keys {
        let mut sh = server.keyspace.lock(shard_of(hash_key(key)));
        let qk = (b.db, key.clone());
        if let Some(q) = sh.blocked.get_mut(&qk) {
            q.retain(|x| !Arc::ptr_eq(x, b) && x.is_waiting());
            if q.is_empty() {
                sh.blocked.remove(&qk);
            }
        }
    }
}

fn find_guard<'a, 'b>(
    guards: &'a mut [(usize, MutexGuard<'b, Shard>)],
    idx: usize,
) -> &'a mut Shard {
    &mut guards
        .iter_mut()
        .find(|(i, _)| *i == idx)
        .expect("shard locked")
        .1
}

fn remove_from_queue(shard: &mut Shard, db: usize, key: &Sds, b: &Arc<BlockedClient>) {
    let qk = (db, key.clone());
    if let Some(q) = shard.blocked.get_mut(&qk) {
        q.retain(|x| !Arc::ptr_eq(x, b));
        if q.is_empty() {
            shard.blocked.remove(&qk);
        }
    }
}

enum Step {
    /// Served one waiter; maybe another key became ready (BLMOVE target).
    Served(Option<(usize, Sds)>),
    /// The waiter vanished meanwhile; look again.
    Retry,
    /// Nothing more to do for this key.
    Stop,
}

/// Serves clients blocked on keys made ready by the last command. Must be
/// called with no shard locks held.
pub fn serve_ready(server: &Server, mut ready: Vec<(usize, Sds)>) {
    let mut rounds = 0usize;
    while let Some((db, key)) = ready.pop() {
        loop {
            rounds += 1;
            if rounds > 1_000_000 {
                return; // defensive bound; never reached in practice
            }
            let h = hash_key(&key);
            let si = shard_of(h);
            // Phase 1: find the first waiter that wants what the key holds.
            let waiter = {
                let mut sh = server.keyspace.lock(si);
                let qk = (db, key.clone());
                let Some(q) = sh.blocked.get_mut(&qk) else {
                    break;
                };
                q.retain(|b| b.is_waiting());
                if q.is_empty() {
                    sh.blocked.remove(&qk);
                    break;
                }
                let sh = &*sh;
                let Some(obj) = sh.dbs[db].main.get_h(h, &key) else {
                    break;
                };
                let q = &sh.blocked[&qk];
                match q.iter().find(|b| b.kind.wants(&obj.value)) {
                    Some(b) => b.clone(),
                    None => break,
                }
            };
            if let BlockKind::XRead { .. } = waiter.kind {
                serve_xread(server, db, &key, h);
                break;
            }
            // Phase 2: lock everything the waiter needs, in order.
            let mut idx = vec![si];
            if let BlockKind::ListMove { dst, .. } = &waiter.kind {
                idx.push(shard_of(hash_key(dst)));
            }
            idx.sort_unstable();
            idx.dedup();
            let mut guards = server.keyspace.lock_set(&idx);
            match serve_one(server, &mut guards, db, &key, h, &waiter) {
                Step::Served(next) => {
                    if let Some(n) = next {
                        ready.push(n);
                    }
                }
                Step::Retry => {}
                Step::Stop => break,
            }
        }
    }
}

fn serve_one(
    server: &Server,
    guards: &mut [(usize, MutexGuard<'_, Shard>)],
    db: usize,
    key: &Sds,
    h: u64,
    w: &Arc<BlockedClient>,
) -> Step {
    let si = shard_of(h);
    {
        let shard = find_guard(guards, si);
        let still_queued = shard
            .blocked
            .get(&(db, key.clone()))
            .is_some_and(|q| q.iter().any(|x| Arc::ptr_eq(x, w)));
        if !still_queued || !w.is_waiting() {
            return Step::Retry;
        }
        match shard.dbs[db].main.get_h(h, key) {
            Some(obj) if w.kind.wants(&obj.value) => {}
            _ => return Step::Stop,
        }
    }
    let lim = server.config.limits();
    let mut reply = Vec::new();
    let mut prop = Vec::new();
    let mut next_ready = None;
    // A BLMOVE whose destination holds the wrong type gets an error and
    // nothing is popped (Redis behaves the same way).
    if let BlockKind::ListMove { dst, .. } = &w.kind {
        let dh = hash_key(dst);
        let dshard = find_guard(guards, shard_of(dh));
        if let Some(o) = dshard.dbs[db].main.get_h(dh, dst)
            && !matches!(o.value, Value::List(_))
        {
            if !w.claim(SERVED) {
                return Step::Retry;
            }
            remove_from_queue(find_guard(guards, si), db, key, w);
            let mut r = Reply::new(&mut reply, w.resp3);
            r.error(crate::cmd::WRONGTYPE);
            w.client.push_unblock(&reply);
            return Step::Served(None);
        }
    }
    if !w.claim(SERVED) {
        return Step::Retry;
    }
    let shard = find_guard(guards, si);
    shard.before_write();
    remove_from_queue(shard, db, key, w);
    let obj = shard.dbs[db]
        .main
        .get_mut_h(h, key)
        .expect("validated above");
    let mut r = Reply::new(&mut reply, w.resp3);
    let mut moved: Option<(Sds, Sds, bool)> = None;
    match (&w.kind, &mut obj.value) {
        (BlockKind::ListPop { front }, Value::List(l)) => {
            let v = l.pop(*front).expect("non-empty");
            l.shrink_check(&lim);
            r.array(2);
            r.bulk(key);
            r.bulk(&v);
            encode_command(&mut prop, &[if *front { b"LPOP" } else { b"RPOP" }, key]);
        }
        (BlockKind::ListMPop { front, count }, Value::List(l)) => {
            let n = (*count).min(l.len());
            r.array(2);
            r.bulk(key);
            r.array(n);
            for _ in 0..n {
                let v = l.pop(*front).expect("non-empty");
                r.bulk(&v);
            }
            l.shrink_check(&lim);
            let c = n.to_string();
            encode_command(
                &mut prop,
                &[if *front { b"LPOP" } else { b"RPOP" }, key, c.as_bytes()],
            );
        }
        (
            BlockKind::ListMove {
                dst,
                from_front,
                to_front,
            },
            Value::List(l),
        ) => {
            let v = l.pop(*from_front).expect("non-empty");
            l.shrink_check(&lim);
            r.bulk(&v);
            let side = |f: bool| if f { &b"LEFT"[..] } else { &b"RIGHT"[..] };
            encode_command(
                &mut prop,
                &[b"LMOVE", key, dst, side(*from_front), side(*to_front)],
            );
            moved = Some((dst.clone(), v, *to_front));
        }
        (BlockKind::ZPop { max }, Value::ZSet(z)) => {
            let (m, s) = z.pop(*max).expect("non-empty");
            r.array(3);
            r.bulk(key);
            r.bulk(&m);
            r.double(s);
            encode_command(
                &mut prop,
                &[if *max { b"ZPOPMAX" } else { b"ZPOPMIN" }, key],
            );
        }
        (BlockKind::ZMPop { max, count }, Value::ZSet(z)) => {
            let n = (*count).min(z.len());
            r.array(2);
            r.bulk(key);
            r.array(n);
            for _ in 0..n {
                let (m, s) = z.pop(*max).expect("non-empty");
                r.array(2);
                r.bulk(&m);
                r.double(s);
            }
            let c = n.to_string();
            encode_command(
                &mut prop,
                &[
                    if *max { b"ZPOPMAX" } else { b"ZPOPMIN" },
                    key,
                    c.as_bytes(),
                ],
            );
        }
        _ => unreachable!("kind matched the value type in phase 1"),
    }
    shard.touch_watched(db, key);
    if let Some((dst, v, to_front)) = moved {
        let dh = hash_key(&dst);
        let dshard = find_guard(guards, shard_of(dh));
        dshard.before_write();
        let clock = crate::db::lru_clock();
        let ddb = &mut dshard.dbs[db];
        match ddb.main.get_mut_h(dh, &dst) {
            Some(o) => {
                if let Value::List(l) = &mut o.value {
                    l.push(&[&v], to_front, &lim);
                }
            }
            None => {
                let mut l = crate::types::List::new();
                l.push(&[&v], to_front, &lim);
                ddb.insert_new(
                    dh,
                    dst.clone(),
                    crate::db::Obj::new(Value::List(Box::new(l)), clock),
                );
            }
        }
        dshard.touch_watched(db, &dst);
        if dshard.has_blocked(db, &dst) {
            next_ready = Some((db, dst));
        }
    }
    // Empty source lists are deleted only after the push, so `BLMOVE k k`
    // rotating the last element keeps the key and its TTL.
    let src = find_guard(guards, si);
    if src.dbs[db]
        .main
        .get_h(h, key)
        .is_some_and(|o| o.value.is_empty_collection())
    {
        src.dbs[db].remove(h, key);
    }
    server
        .stats
        .dirty_background
        .fetch_add(1, Ordering::Relaxed);
    if server.prop.is_active() {
        server.prop.propagate(db, &prop);
    }
    w.client.push_unblock(&reply);
    Step::Served(next_ready)
}

/// `XREAD BLOCK` waiters do not consume data: every waiter whose last-seen
/// ID is behind the stream's tail is answered.
fn serve_xread(server: &Server, db: usize, key: &Sds, h: u64) {
    let mut sh = server.keyspace.lock(shard_of(h));
    let qk = (db, key.clone());
    let waiters: Vec<Arc<BlockedClient>> = match sh.blocked.get(&qk) {
        Some(q) => q
            .iter()
            .filter(|b| matches!(b.kind, BlockKind::XRead { .. }))
            .cloned()
            .collect(),
        None => return,
    };
    {
        let Some(obj) = sh.dbs[db].main.get_h(h, key) else {
            return;
        };
        let Value::Stream(s) = &obj.value else {
            return;
        };
        for w in waiters {
            let BlockKind::XRead { ids, count } = &w.kind else {
                continue;
            };
            let pos = w.keys.iter().position(|k| k == key).unwrap_or(0);
            let after = ids.get(pos).copied().unwrap_or(StreamId::MAX);
            let entries = s.after(after, *count);
            if entries.is_empty() || !w.claim(SERVED) {
                continue;
            }
            let mut reply = Vec::new();
            let mut r = Reply::new(&mut reply, w.resp3);
            if w.resp3 {
                r.map(1);
            } else {
                r.array(1);
                r.array(2);
            }
            r.bulk(key);
            crate::cmd::stream::write_entries(&mut r, &entries);
            w.client.push_unblock(&reply);
        }
    }
    if let Some(q) = sh.blocked.get_mut(&qk) {
        q.retain(|b| b.is_waiting());
        if q.is_empty() {
            sh.blocked.remove(&qk);
        }
    }
}
