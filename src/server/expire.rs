//! Background maintenance: Redis' active expire cycle and incremental
//! rehashing of idle tables.
//!
//! Lazy expiry alone would leave keys that are never read again in memory
//! forever. The active cycle walks shards round-robin; for each database it
//! scans ~20 keys of the expires dict, deletes those past their deadline and
//! repeats while more than 10% of the sample was expired (meaning many stale
//! keys remain), all within a CPU-time budget per cron tick (25% of the
//! period, `ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC`).

use super::Server;
use crate::db::NUM_SHARDS;
use crate::dict::hash_key;
use crate::resp::encode_command;
use crate::sds::Sds;
use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, Instant};

/// Keys sampled per database per iteration (`ACTIVE_EXPIRE_CYCLE_KEYS_PER_LOOP`).
const KEYS_PER_LOOP: usize = 20;
/// Keep going while more than this percentage of samples was expired.
const ACCEPTABLE_STALE_PERCENT: usize = 10;

/// Where the cycle resumes next tick.
#[derive(Default)]
pub struct ExpireState {
    shard: usize,
}

/// Runs one time-bounded active expire cycle.
pub fn active_expire_cycle(server: &Server, st: &mut ExpireState, budget: Duration) {
    let start = Instant::now();
    let now = crate::util::now_ms();
    for _ in 0..NUM_SHARDS {
        if start.elapsed() >= budget {
            return;
        }
        let si = st.shard;
        st.shard = (st.shard + 1) % NUM_SHARDS;
        let mut sh = server.keyspace.lock(si);
        if sh.dbs.iter().all(|d| d.expires.is_empty()) {
            continue;
        }
        for db in 0..sh.dbs.len() {
            loop {
                if sh.dbs[db].expires.is_empty() {
                    break;
                }
                let mut sampled = 0usize;
                let mut expired: Vec<Sds> = Vec::new();
                let mut cursor = sh.expire_cursor[db];
                let mut iterations = 0;
                while sampled < KEYS_PER_LOOP && iterations < KEYS_PER_LOOP * 4 {
                    iterations += 1;
                    cursor = sh.dbs[db].expires.scan(cursor, |k, &t| {
                        sampled += 1;
                        if t <= now {
                            expired.push(k.clone());
                        }
                    });
                    if cursor == 0 {
                        break;
                    }
                }
                sh.expire_cursor[db] = cursor;
                if !expired.is_empty() {
                    sh.before_write();
                    let mut prop = Vec::new();
                    for k in &expired {
                        let h = hash_key(k);
                        if sh.dbs[db].remove(h, k).is_some() {
                            sh.touch_watched(db, k);
                            if server.prop.is_active() {
                                encode_command(&mut prop, &[b"DEL", k]);
                            }
                        }
                    }
                    if !prop.is_empty() {
                        server.prop.propagate(db, &prop);
                    }
                    server
                        .stats
                        .expired_active
                        .fetch_add(expired.len() as u64, Relaxed);
                }
                if expired.len() * 100 <= sampled * ACCEPTABLE_STALE_PERCENT
                    || start.elapsed() >= budget
                    || cursor == 0
                {
                    break;
                }
            }
        }
    }
}

/// Spends up to `budget` advancing rehashes of the main/expires tables and
/// shrinking tables that became sparse.
pub fn incremental_rehash(server: &Server, budget: Duration) {
    let start = Instant::now();
    for si in 0..NUM_SHARDS {
        if start.elapsed() >= budget {
            return;
        }
        let mut sh = server.keyspace.lock(si);
        for db in sh.dbs.iter_mut() {
            db.main.shrink_if_needed();
            db.expires.shrink_if_needed();
            if db.main.is_rehashing() {
                db.main.rehash_steps(100);
            }
            if db.expires.is_rehashing() {
                db.expires.rehash_steps(100);
            }
        }
    }
}
