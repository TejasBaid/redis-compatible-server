//! `maxmemory` enforcement with Redis' approximated LRU/LFU eviction.
//!
//! Like Redis, we never track a global LRU order. Before a command that may
//! grow memory runs, if `used_memory > maxmemory`, we sample a few keys
//! (`maxmemory-samples`) from random shards, score them (idle time for LRU,
//! inverse access frequency for LFU, nearest deadline for volatile-ttl) and
//! keep the best candidates in a small pool shared across calls. The best
//! candidate is evicted, repeating until memory is back under the limit.

use super::Server;
use crate::alloc;
use crate::config::EvictionPolicy;
use crate::db::{NUM_SHARDS, lfu_decayed, lru_idle_ms, shard_of};
use crate::dict::hash_key;
use crate::resp::encode_command;
use crate::sds::Sds;
use crate::util::rand_below;
use std::sync::atomic::Ordering::Relaxed;

/// Pool size (`EVPOOL_SIZE`).
const POOL_SIZE: usize = 16;

struct Candidate {
    score: u64,
    db: usize,
    key: Sds,
}

/// The eviction pool: best candidates seen so far, ascending by score.
#[derive(Default)]
pub struct EvictionPool {
    entries: Vec<Candidate>,
}

impl EvictionPool {
    fn insert(&mut self, c: Candidate) {
        if self.entries.iter().any(|e| e.key == c.key && e.db == c.db) {
            return;
        }
        if self.entries.len() == POOL_SIZE && self.entries[0].score >= c.score {
            return;
        }
        let pos = self.entries.partition_point(|e| e.score < c.score);
        self.entries.insert(pos, c);
        if self.entries.len() > POOL_SIZE {
            self.entries.remove(0);
        }
    }
}

/// Returns false when the command must be rejected with `-OOM`.
pub fn ensure_memory(server: &Server) -> bool {
    let max = server.config.maxmemory.load(Relaxed);
    if max == 0 || !alloc::is_installed() || alloc::used_memory() <= max {
        return true;
    }
    perform_evictions(server, max)
}

fn perform_evictions(server: &Server, max: u64) -> bool {
    let policy = server.config.policy();
    if policy == EvictionPolicy::NoEviction {
        return false;
    }
    let mut evicted = 0usize;
    while alloc::used_memory() > max {
        if evicted >= 10_000 {
            return true; // bound the latency of a single command
        }
        let victim = match policy {
            EvictionPolicy::AllKeysRandom | EvictionPolicy::VolatileRandom => {
                random_victim(server, policy.is_volatile())
            }
            _ => pool_victim(server, policy),
        };
        let Some((db, key)) = victim else {
            return evicted > 0;
        };
        let h = hash_key(&key);
        let mut sh = server.keyspace.lock(shard_of(h));
        if policy.is_volatile() && sh.dbs[db].expire_of_key(h, &key).is_none() {
            continue;
        }
        sh.before_write();
        if sh.dbs[db].remove(h, &key).is_some() {
            sh.touch_watched(db, &key);
            if server.prop.is_active() {
                let mut b = Vec::new();
                encode_command(&mut b, &[b"DEL", &key]);
                server.prop.propagate(db, &b);
            }
            server.stats.evicted.fetch_add(1, Relaxed);
            evicted += 1;
        }
        drop(sh);
        alloc::flush_local();
    }
    true
}

fn random_victim(server: &Server, volatile: bool) -> Option<(usize, Sds)> {
    let start = rand_below(NUM_SHARDS as u64) as usize;
    for off in 0..NUM_SHARDS {
        let si = (start + off) % NUM_SHARDS;
        let sh = server.keyspace.lock(si);
        let dbs: Vec<usize> = (0..sh.dbs.len())
            .filter(|&d| {
                if volatile {
                    !sh.dbs[d].expires.is_empty()
                } else {
                    !sh.dbs[d].main.is_empty()
                }
            })
            .collect();
        if dbs.is_empty() {
            continue;
        }
        let d = dbs[rand_below(dbs.len() as u64) as usize];
        let key = if volatile {
            sh.dbs[d].expires.random_entry().map(|(k, _)| k.clone())
        } else {
            sh.dbs[d].main.random_entry().map(|(k, _)| k.clone())
        };
        if let Some(k) = key {
            return Some((d, k));
        }
    }
    None
}

fn pool_victim(server: &Server, policy: EvictionPolicy) -> Option<(usize, Sds)> {
    let samples = server.config.maxmemory_samples.load(Relaxed).max(1);
    let decay = server.config.lfu_decay_time.load(Relaxed);
    let volatile = policy.is_volatile();
    let mut pool = server.evict_pool.lock();
    // Populate from a few random shards; fall back to a full sweep if the
    // keyspace is sparse.
    let mut probes = 0;
    let start = rand_below(NUM_SHARDS as u64) as usize;
    let mut off = 0;
    while off < NUM_SHARDS && (probes < 4 || pool.entries.is_empty()) {
        let si = (start + off * 37) % NUM_SHARDS;
        off += 1;
        let sh = server.keyspace.lock(si);
        let mut found = false;
        for (d, db) in sh.dbs.iter().enumerate() {
            let mut picked: Vec<(&Sds, u64)> = Vec::new();
            if volatile {
                let mut s = Vec::new();
                db.expires.sample(samples, &mut s);
                for (k, &deadline) in s {
                    let Some(o) = db.main.get(k) else { continue };
                    let score = match policy {
                        EvictionPolicy::VolatileTtl => u64::MAX - deadline,
                        EvictionPolicy::VolatileLfu => 255 - lfu_decayed(o.clock(), decay) as u64,
                        _ => lru_idle_ms(o.clock()),
                    };
                    picked.push((k, score));
                }
            } else {
                let mut s = Vec::new();
                db.main.sample(samples, &mut s);
                for (k, o) in s {
                    let score = if policy.is_lfu() {
                        255 - lfu_decayed(o.clock(), decay) as u64
                    } else {
                        lru_idle_ms(o.clock())
                    };
                    picked.push((k, score));
                }
            }
            for (k, score) in picked {
                found = true;
                pool.insert(Candidate {
                    score,
                    db: d,
                    key: k.clone(),
                });
            }
        }
        if found {
            probes += 1;
        }
    }
    pool.entries.pop().map(|c| (c.db, c.key))
}
