# redis-clone

**redis-clone** is a Redis-compatible in-memory data store written from scratch in Rust.
It speaks RESP2 and RESP3, so the unmodified `redis-cli`, `redis-benchmark` and client
libraries such as `redis-rs` work against it. It implements ~185 commands across strings,
lists, hashes, sets, sorted sets (a real skiplist with rank spans) and streams, plus
transactions, pub/sub, blocking operations, expiry, `maxmemory` eviction, AOF and snapshot
persistence, and master/replica replication with partial resync. Unlike Redis, it executes
commands on **all cores**: the keyspace is split into 256 lock-striped shards, and every
worker thread parses, executes and replies on its own. Snapshots are point-in-time without
`fork()`. The main correctness check is a **differential test** that sends the same
randomized command sequences (160,000 commands per run) to this server and to a real
`redis-server` 8.x and requires byte-identical replies and identical final datasets.

---

## Features

**Protocol** — [`src/resp.rs`](src/resp.rs)
- RESP2 and RESP3 (`HELLO 3`): maps, sets, doubles, nulls, booleans, verbatim strings and
  push messages are encoded per client protocol
- Pipelining (one `write()` per batch of replies), inline commands with Redis' quoting rules,
  a resumable zero-copy parser, Redis-exact protocol errors
- `AUTH`/`requirepass`, `HELLO ... AUTH ... SETNAME ...`, `CLIENT REPLY ON|OFF|SKIP`

**Data types and encodings** — [`src/types/`](src/types), [`src/dict.rs`](src/dict.rs), [`src/skiplist.rs`](src/skiplist.rs)
- Strings stored as integers (`INCR` never parses) or inline small strings (≤ 22 bytes, no heap allocation)
- Small lists, hashes, sets and sorted sets use a **listpack** (one contiguous allocation,
  bidirectional), integer sets use an **intset** (16/32/64-bit widths); they convert to
  hash tables / a skiplist at Redis' thresholds (`*-max-listpack-*`, `set-max-intset-entries`),
  and `OBJECT ENCODING` reports the same encoding Redis does (checked by the differential test)
- Sorted sets: **skiplist with rank spans** + member→score dict, O(log n) `ZRANK`/`ZRANGE`
- Streams: `XADD` (auto/explicit/`ms-*` IDs, `MAXLEN`/`MINID` trimming), `XRANGE`,
  `XREVRANGE`, `XREAD` (with `BLOCK`), `XLEN`, `XDEL`, `XTRIM`

**Server features**
- `MULTI`/`EXEC`/`DISCARD`/`WATCH`/`UNWATCH`, atomic across shards
- Pub/sub: `SUBSCRIBE`, `PSUBSCRIBE`, `PUBLISH`, `UNSUBSCRIBE`, `PUBSUB`, RESP3 push messages
- Blocking commands: `BLPOP`, `BRPOP`, `BLMOVE`, `BRPOPLPUSH`, `BLMPOP`, `BZPOPMIN`,
  `BZPOPMAX`, `BZMPOP`, `XREAD BLOCK`, with FIFO fairness and timeouts
- Expiry: lazy on access + Redis' active, probabilistic expire cycle (sample 20, repeat while
  more than 10% were stale, 25% CPU budget per tick)
- `maxmemory` with all 8 policies: approximated **LRU** and **LFU** (Redis' log counter with
  decay) using sampling and an eviction pool, `volatile-ttl`, random, `noeviction`

**Persistence** — [`src/persistence/`](src/persistence)
- Snapshots: `SAVE`, `BGSAVE`, `save <secs> <changes>` points, CRC32-checked custom format,
  loaded on startup, **point-in-time without fork** (see design decision 3)
- AOF with `appendfsync always|everysec|no`, Redis 7-style multi-part layout
  (base snapshot + incremental files + manifest), `BGREWRITEAOF` while serving writes
- Crash safety: acknowledged writes survive `kill -9`; a truncated AOF tail (or a `MULTI`
  without `EXEC`) is detected and repaired (`aof-load-truncated yes`) or refused; mid-file
  corruption and snapshot checksum mismatches are fatal

**Replication** — [`src/replication/`](src/replication)
- `REPLICAOF host port` / `REPLICAOF NO ONE`, full sync (snapshot transfer, then the command
  stream), **partial resync** from a backlog (`repl-backlog-size`) after a disconnect
- `REPLCONF ACK`/`GETACK`, `WAIT`, `ROLE`, `INFO replication`, read-only replicas,
  master-driven expiry (replicas never expire keys themselves, but hide expired ones)

**Admin and observability**
- `INFO` (server, clients, memory, persistence, stats, replication, cpu, commandstats, keyspace)
- `CONFIG GET/SET/RESETSTAT` (glob patterns, ~45 parameters), `CLIENT LIST/INFO/ID/SETNAME/GETNAME/KILL/SETINFO`
- `COMMAND`, `COMMAND COUNT/DOCS/INFO/LIST/GETKEYS` (enough for `redis-cli` hints)
- `SLOWLOG GET/LEN/RESET`, `DEBUG SLEEP/RELOAD/SET-ACTIVE-EXPIRE`, `MEMORY USAGE/STATS`,
  `SHUTDOWN [NOSAVE|SAVE]`, graceful shutdown on `SIGTERM`/`SIGINT`

### Supported commands

| Group | Commands |
|---|---|
| String | `GET SET SETNX SETEX PSETEX GETSET GETDEL GETEX MGET MSET MSETNX INCR DECR INCRBY DECRBY INCRBYFLOAT APPEND STRLEN GETRANGE SUBSTR SETRANGE` |
| Keys | `DEL UNLINK EXISTS TOUCH EXPIRE PEXPIRE EXPIREAT PEXPIREAT TTL PTTL EXPIRETIME PEXPIRETIME PERSIST TYPE RENAME RENAMENX MOVE COPY KEYS SCAN RANDOMKEY OBJECT DBSIZE FLUSHDB FLUSHALL SWAPDB` |
| List | `LPUSH RPUSH LPUSHX RPUSHX LPOP RPOP LRANGE LLEN LINDEX LSET LREM LTRIM LINSERT LMOVE RPOPLPUSH LPOS LMPOP BLPOP BRPOP BLMOVE BRPOPLPUSH BLMPOP` |
| Hash | `HSET HMSET HSETNX HGET HMGET HDEL HGETALL HKEYS HVALS HLEN HEXISTS HSTRLEN HINCRBY HINCRBYFLOAT HSCAN HRANDFIELD` |
| Set | `SADD SREM SMEMBERS SISMEMBER SMISMEMBER SCARD SINTER SINTERSTORE SINTERCARD SUNION SUNIONSTORE SDIFF SDIFFSTORE SPOP SRANDMEMBER SMOVE SSCAN` |
| Sorted set | `ZADD ZINCRBY ZREM ZSCORE ZMSCORE ZCARD ZCOUNT ZLEXCOUNT ZRANK ZREVRANK ZRANGE ZRANGESTORE ZREVRANGE ZRANGEBYSCORE ZREVRANGEBYSCORE ZRANGEBYLEX ZREVRANGEBYLEX ZREMRANGEBYRANK ZREMRANGEBYSCORE ZREMRANGEBYLEX ZPOPMIN ZPOPMAX BZPOPMIN BZPOPMAX ZMPOP BZMPOP ZRANDMEMBER ZSCAN ZUNIONSTORE ZINTERSTORE ZDIFFSTORE ZUNION ZINTER ZDIFF ZINTERCARD` |
| Stream | `XADD XRANGE XREVRANGE XLEN XDEL XTRIM XREAD` |
| Transactions | `MULTI EXEC DISCARD WATCH UNWATCH` |
| Pub/sub | `SUBSCRIBE UNSUBSCRIBE PSUBSCRIBE PUNSUBSCRIBE PUBLISH PUBSUB` |
| Connection | `PING ECHO SELECT HELLO AUTH QUIT RESET CLIENT` |
| Server | `INFO CONFIG COMMAND SAVE BGSAVE BGREWRITEAOF LASTSAVE SHUTDOWN DEBUG SLOWLOG MEMORY TIME LATENCY LOLWUT READONLY READWRITE` |
| Replication | `REPLICAOF SLAVEOF PSYNC SYNC REPLCONF WAIT ROLE` |

---

## Architecture

```
            clients: redis-cli · redis-benchmark · redis-rs · ...
                                   │ TCP
                           ┌───────▼───────┐
                           │   acceptor    │  round-robin
                           └──┬────┬────┬──┘
              ┌───────────────┘    │    └───────────────┐
        ┌─────▼─────┐        ┌─────▼─────┐        ┌─────▼─────┐   worker = one mio event loop:
        │ worker 0  │        │ worker 1  │  ...   │ worker N  │   read → parse RESP → lock the
        └─────┬─────┘        └─────┬─────┘        └─────┬─────┘   command's shards → execute
              │   lock only the shards the command's keys hash to,  → reply (one write per batch)
              │   always in ascending order (no deadlocks)
        ┌─────▼──────────────────────────────────────────────────────┐
        │ Keyspace: 256 × Mutex<Shard>                                │
        │   Shard { dbs[16] { main: Dict<Obj>, expires: Dict<u64> },  │
        │           blocked clients · WATCH registry · snapshot mark } │
        └─────┬──────────────────────────────────────────────┬────────┘
              │ effects appended while shard locks are held   │ copy-before-write
        ┌─────▼──────────────┐                        ┌──────▼─────────────┐
        │ Propagator         │──► AOF buffer ─► disk  │ snapshot builder   │
        │ (one ordered write │──► replication backlog │ (BGSAVE, AOF base, │
        │  stream)           │──► replica sockets     │  replica full sync)│
        └────────────────────┘                        └────────────────────┘
   cron thread: LRU clock · active expiry · incremental rehash · save points · AOF fsync · replica PINGs
   replica thread (on replicas): PSYNC handshake → load snapshot → apply stream → ACK every second
```

A request's path: the worker owning the connection reads all available bytes, parses every
complete request, and for each one looks up the command, takes the locks of the shards its
keys hash to (one lock for single-key commands, no allocation), runs the handler against a
`DbView` of those shards, appends the effect to the AOF/replication stream while the locks
are still held, releases the locks and, if the command made a key "ready", serves clients
blocked on that key. Replies accumulate in the connection's output buffer and are written
once per batch.

### Module map

| Module | Responsibility |
|---|---|
| [`resp`](src/resp.rs) | RESP2/RESP3 request parser (multibulk + inline) and reply encoder |
| [`sds`](src/sds.rs), [`dict`](src/dict.rs), [`listpack`](src/listpack.rs), [`intset`](src/intset.rs), [`skiplist`](src/skiplist.rs) | Data structures |
| [`types`](src/types) | Value types and their small/large encodings |
| [`db`](src/db.rs) | Sharded keyspace, per-shard blocked/WATCH registries, LRU/LFU clocks |
| [`cmd`](src/cmd) | Command table, dispatcher, `DbView`, MULTI/EXEC, all command handlers |
| [`blocking`](src/blocking.rs), [`pubsub`](src/pubsub.rs) | Blocking operations, pub/sub registry |
| [`server`](src/server) | Worker event loops, acceptor, cron, eviction, active expiry, client state |
| [`propagate`](src/propagate.rs) | Ordered write stream, replication backlog |
| [`persistence`](src/persistence) | Snapshots, multi-part AOF, rewrite, crash recovery |
| [`replication`](src/replication) | PSYNC master and replica sides |
| [`alloc`](src/alloc.rs) | Counting allocator behind `used_memory`/`maxmemory` |

---

## Key design decisions and tradeoffs

**1. A shared, lock-striped keyspace executed on the I/O threads.**
There were three realistic options. A Redis-style single-threaded loop keeps every
semantic trivial but leaves 9 of 10 cores idle. A shared-nothing, shard-per-core design
(Dragonfly, Seastar) scales best, but every multi-key command, `MULTI`, blocking move and
`RENAME` across shards turns into a distributed protocol between threads. Redis 6-style
threaded I/O still funnels execution through one thread. I chose the middle path: N worker
threads each run a full event loop and execute commands themselves, and the data is split
into 256 shards, each behind its own mutex. A command declares its keys up front (Redis'
key specs), so the dispatcher can take exactly the shard locks it needs, in ascending
order, before running it. That rules out deadlock by construction while keeping Redis'
single-threaded semantics: a command, or a whole `EXEC`, is atomic with respect to every
other client. The cost is that a single hot key serializes on one mutex, so workloads like
redis-benchmark's single-key `LPUSH` only scale their I/O (see the benchmarks).

**2. One ordered effect stream, built under the shard locks.**
AOF and replication need a replayable order. Each command appends its effect to the
propagation buffer *before* releasing its shard locks. Two commands that conflict always
share a shard lock, so they reach the stream in the order they executed; commands that
don't conflict commute, so their relative order doesn't matter. Non-deterministic commands
are rewritten first, as Redis does: `SET .. EX` becomes `SET .. PXAT`, `EXPIRE` becomes
`PEXPIREAT`, `SPOP` becomes `SREM`, a served `BLPOP` becomes `LPOP`, `INCRBYFLOAT`
becomes `SET .. KEEPTTL`, lazily expired keys produce `DEL`, and transactions are wrapped
in `MULTI`/`EXEC`.

**3. Point-in-time snapshots without `fork()`.**
Redis forks and lets the kernel's copy-on-write freeze the child's view. A multi-threaded
process cannot fork safely. Instead, a snapshot takes every shard lock once (a few µs),
records the replication offset or switches AOF files at that instant, and marks every shard
"pending". A background thread then serializes shards one by one. Any writer that wants to
mutate a still-pending shard serializes that shard first (copy-before-write at shard
granularity). Every shard is therefore captured exactly as it was at the snapshot instant.
Readers never wait, and a writer pays at most one shard's serialization (1/256 of the
data). A test runs concurrent cross-shard `MULTI` transfers during `BGSAVE` and checks that
the restored total is conserved.

**4. Multi-part AOF, no rewrite buffer.**
`BGREWRITEAOF` opens a new incremental file *at the snapshot instant* (under the same
global lock acquisition), so every later command lands in the new file. It then writes the
new base snapshot in the background and swaps the manifest atomically (temp file + rename
+ directory fsync). This is Redis 7's design. It needs no buffer of commands received
during the rewrite, and a crash at any point leaves a valid manifest. Before a worker sends
the replies of a batch that wrote data, it `write()`s the AOF buffer (one syscall shared by
every thread's pending commands) and, with `appendfsync always`, fsyncs it, so
acknowledged writes survive `kill -9` under every policy (power loss needs `always`).

**5. A hand-written hash table, for `SCAN`.**
`SCAN` must return every element present for the whole iteration, even if the table resizes
between calls. A `HashMap` cannot promise that. `Dict` is a port of Redis' `dict.c`:
power-of-two chained tables, **incremental rehashing** (one bucket per write plus a
time-bounded cron step, so no command pays for rehashing a million keys), and the
**reverse-binary cursor**, whose increment order stays valid across growth and shrinkage.
Property tests check it against `HashMap`, and integration tests churn the keyspace during a
`SCAN`.

**6. A skiplist in an arena.**
The skiplist (rank spans, backward links, `update[]`/`rank[]` insertion: a faithful port of
`t_zset.c`) stores nodes in a `Vec` addressed by `u32` indices instead of pointers, so it is
100% safe Rust. Level 0 lives inline in the node; only the ~25% of nodes that get promoted
carry a heap slice for upper levels. A property test compares every operation, including
rank and range deletes, against a sorted-`Vec` model and checks all span invariants.

**7. Compact representations, measured in bytes.**
A keyspace entry is `next pointer (8) + key (24) + value (24) + LRU/flags (4, padded)`, so a
dict node is **exactly 64 bytes**. Getting there took a trick: the inline length of `Sds` is
an enum with 23 values, which leaves 233 unused bit patterns that the compiler uses to
store the discriminants of `RStr` and `Value` at no cost (`size_of::<Value>() == 24` is a
test). Short keys and values live inside the node itself. Small collections use listpacks
and intsets, as in Redis.

**8. Memory accounting that doesn't hurt scaling.**
`used_memory` comes from a counting global allocator. A shared atomic counter updated on
every allocation would be a contended cache line on all cores. Instead each thread keeps a
local delta and publishes it once it drifts past 64 KB. Reads are one atomic load, accurate
to `threads × 64 KB`, which is plenty for `maxmemory` enforcement.

**9. Blocking operations without lost wakeups.**
A client that must block registers in the per-key FIFO queue *while the dispatcher still
holds the key's shard lock*. A push therefore can never fall between "the list is empty"
and "I'm registered". A push to a key with waiters marks it ready. After that command
releases its locks, the same thread pops on behalf of waiters and delivers the reply through
the waiter's cross-thread output queue. Serving, timing out and disconnecting race through
a single compare-and-swap, so each blocked client is answered exactly once.

**10. Redis is the specification.**
Error messages, option-parsing order, edge cases (`ZADD` flag conflicts, `LPOS` ranks,
`-0` scores, NaN handling in `ZUNIONSTORE` weights, lex ranges over mixed scores, encoding
conversion points) were made to match `redis-server` byte for byte. Most of these
differences were found by the differential test rather than by reading the documentation.

---

## Quick start

```bash
cargo build --release
./target/release/redis-clone --port 6380                  # foreground, logs to stdout
./target/release/redis-clone redis.conf --threads 8       # a redis.conf-style file also works
```

```text
$ redis-cli -p 6380 SET user:1 alice EX 3600
OK
$ redis-cli -p 6380 ZADD board 120 ana 95 bo 140 cy
3
$ redis-cli -p 6380 ZRANGE board 0 -1 REV WITHSCORES
cy
140
ana
120
bo
95
$ redis-cli -p 6380 -3 HELLO 3
server redis
version 7.4.0
proto 3
id 4
mode standalone
role master
modules
$ redis-cli -p 6380 OBJECT ENCODING board
listpack
$ redis-cli -p 6380 INFO keyspace
# Keyspace
db0:keys=2,expires=1,avg_ttl=0
```
(Output as printed by `redis-cli` when stdout is not a terminal; interactively it adds
its usual `(integer)` / `1)` decorations.)

Every option can be passed as `--name value` or in a config file: `port`, `bind`, `threads`
(worker threads, default `min(cores, 8)`), `maxmemory`, `maxmemory-policy`, `appendonly`,
`appendfsync`, `save`, `dir`, `dbfilename`, `requirepass`, `replicaof`, `hz`, the
`*-max-listpack-*` thresholds and more (`CONFIG GET *` lists them).

## Persistence and replication

```bash
# Snapshots: written to <dir>/dump.rcsnap and loaded on startup.
redis-clone --dir ./data --save "900 1 300 100"
redis-cli -p 6379 BGSAVE

# Append-only file in <dir>/appendonlydir/ (base snapshot + incremental RESP + manifest).
redis-clone --dir ./data --appendonly yes --appendfsync everysec
redis-cli -p 6379 BGREWRITEAOF
redis-cli -p 6379 CONFIG SET appendonly yes      # can be enabled at runtime, too

# Replication: start a replica (or run REPLICAOF on a live server).
redis-clone --port 6380 --replicaof 127.0.0.1 6379
redis-cli -p 6380 INFO replication               # role:slave, master_link_status:up, offsets
redis-cli -p 6379 WAIT 1 1000                    # block until 1 replica has acked everything
redis-cli -p 6380 REPLICAOF NO ONE               # promote
```

If the AOF ends with a partial command (a crash in the middle of `write()`), the server logs
a warning, truncates the file to the last complete command and starts
(`aof-load-truncated yes`, the default). With `aof-load-truncated no` it refuses to start.

---

## Testing

```bash
cargo test                                   # everything (skips Redis-dependent tests if redis-server is absent)
cargo test --test differential               # compare against a real redis-server
DIFF_SEEDS=2000 DIFF_OPS=1000 cargo test --release --test differential   # a longer differential run
cargo clippy --all-targets -- -D warnings && cargo fmt --check
```

**120 tests**, all passing on the final run (`cargo test`, with `redis-server` 8.10.2 installed):

| Suite | Tests | What it covers |
|---|---:|---|
| unit + property (`src/`) | 63 | dict, skiplist, listpack, intset, sds, every value type, RESP parser/encoder, config, snapshot format, AOF manifest, dispatcher (in-process), propagation |
| [`differential`](tests/differential.rs) | 2 | 160,000 randomized commands against real Redis + a curated edge-case script, RESP2 and RESP3 |
| [`integration`](tests/integration.rs) | 28 | protocol, keyspace, expiry, blocking, transactions, pub/sub, eviction, admin, concurrency |
| [`persistence`](tests/persistence.rs) | 14 | snapshots, AOF under all fsync policies, `kill -9`, truncated/corrupt files, rewrite |
| [`replication`](tests/replication.rs) | 6 | full sync, streaming, partial resync, backlog overflow, `WAIT`, expiry |
| [`client_lib`](tests/client_lib.rs) | 3 | redis-rs over RESP2 and RESP3, pipelines, transactions, pub/sub |
| [`cli_tools`](tests/cli_tools.rs) | 4 | `redis-cli` and `redis-benchmark` |

The full suite was run 3 times on the final code (120/120 each time), after 10 earlier
clean runs. Two longer differential runs
(`DIFF_SEEDS=1500` from two different seed ranges, ~1.4M commands) also passed. One
earlier flaky failure turned out to be a timing-dependent case in the generator (`EXPIRE ...
GT` with equal *relative* deadlines depends on which millisecond each server ran in), which
now uses absolute `EXPIREAT` timestamps.

**Differential testing against real Redis** ([`tests/differential.rs`](tests/differential.rs)).
A seeded generator produces random command sequences over a small key space and sends each
command to redis-clone and to a real `redis-server` (started on a free port). Every reply
must match byte for byte, after sorting the replies whose order Redis leaves unspecified
(`SMEMBERS`, `HGETALL`, `KEYS`, `SCAN`, ...). After each sequence both datasets are dumped
and compared. One run covers 300 seeds × 400 commands over RESP2 plus 100 seeds over RESP3.
The generator is built to find disagreements:
- keys are shared between types, so WRONGTYPE paths get exercised;
- member lengths and counts cross the listpack/intset/hashtable/skiplist thresholds, and
  `OBJECT ENCODING` is compared;
- indices are negative or out of range; score bounds are exclusive, infinite or malformed;
- options conflict (`ZADD NX XX`, `SET KEEPTTL EX`, `EXPIRE GT LT`), and numbers overflow
  or aren't numbers;
- some commands run inside `MULTI`/`EXEC`/`DISCARD`; streams use explicit, partial
  (`ms-*`) and invalid IDs.

A further test replays a curated script of tricky cases on both servers. This harness found
about twenty real incompatibilities during development, for example: validation order (`LSET` checks the
key before the index; `EXPIRE` parses options before the integer), negative zero (Redis
prints `-0` for skiplist scores but stores listpack scores as integers), `ZUNIONSTORE`
NaN handling (only the first input's weighted score is sanitized, inputs are visited
smallest first), `ZADD XX` not pre-converting the encoding, Redis 7 rejecting unknown
subcommands at `MULTI` queue time, `LMOVE k k` on a one-element list keeping the key's
TTL, `COPY`/`RENAME` preserving the `raw` string encoding, `ZRANGESTORE` filling a listpack
before converting, and lexicographic ranges over mixed scores (where even
Redis' answer depends on its random skiplist levels, so the generator keeps lex queries on
equal-score sets).

**Property tests** (proptest) compare the skiplist (contents, ranks, rank lookups, score
ranges, range deletions, span invariants) to a sorted vector; the dict (including
mid-rehash states and scan completeness) to `HashMap`; listpack, intset, list, hash, set
and both zset encodings to simple models.

**Integration tests** start the real binary on a random port and drive it over TCP:
pipelining 5,000 commands per write, requests split across thousands of tiny writes, inline
commands, protocol errors, RESP3, 16 databases, `SCAN` completeness while the keyspace
grows and shrinks, lazy and active expiry, blocking-command wakeups/timeouts/FIFO
order/`BLMOVE` chains, `WATCH` (including expiry), transaction atomicity under concurrent
readers, exact counters under 16 concurrent pipelining clients, pub/sub, `maxmemory` for
LRU/LFU/volatile/noeviction, 1,000 concurrent connections, slow-subscriber disconnection,
and the admin commands.

**Crash and persistence tests** `kill -9` the server while a client is writing (under all
three fsync policies) and check that every acknowledged write survives. They also corrupt
AOF tails, leave a `MULTI` without its `EXEC`, flip a bit in a snapshot, rewrite the AOF
under load and then `kill -9`, and check that `BGSAVE` taken during concurrent cross-shard
transfers restores a consistent total.

**Replication tests** cover full sync followed by streaming (all types, multiple
databases, `MULTI`, served blocking pops), read-only replicas and promotion, partial resync
after the master drops the link (`sync_partial_ok` goes up, `sync_full` doesn't), full
resync when the backlog is exceeded, sync while 4 clients are writing (then `WAIT`), and
master-driven expiry.

**Compatibility tests** run the unmodified `redis-cli` (commands, RESP3, `--pipe` mass
insertion, `--scan`, `--bigkeys`, `COMMAND DOCS`), `redis-benchmark` (15 tests) and the
`redis` crate over RESP2 and RESP3 (pipelines, `MULTI` via `pipe().atomic()`, pub/sub,
typed replies).

---

## Benchmarks

All numbers below are real measurements from
[`bench/results/20261015-203827/`](bench/results/20261015-203827) (raw `redis-benchmark`
CSV output for every run, `throughput.json`, `scaling.json`, `memory.json`, and
`machine.json` with versions and the load average at the start of every step). Rerun with
`python3 bench/run_bench.py`.

**Setup.** Apple M4 (10 cores: 4 performance + 6 efficiency), 16 GB, macOS 26.4. Redis 8.10.2 (Homebrew build,
`malloc=libc`), redis-benchmark 8.10.2, rustc 1.99.0, redis-clone release build (`lto`,
`codegen-units=1`) with `--threads 4`. Both servers ran with persistence off (`--save ""`,
no AOF), on the same machine as the benchmark client, over loopback. Every
`redis-benchmark` invocation used `-r 1000000` (random keys/members) and the default 3-byte
values, for example `redis-benchmark -p PORT -c 50 -n 2000000 -P 16 -r 1000000 -t
set,get,incr,lpush,lpop,sadd,hset,zadd,lrange_100,mset --csv`. Each configuration was run
**3 times** and the tables show the **median**. Other agents benchmark on the same machine, so
every repetition held a shared lock and started only once the 1-minute load average was
below ~4. **Background desktop load:** the user's own apps (Chrome, a vite dev server, a VM,
WindowServer) kept a baseline load of roughly 2–6 throughout, so expect ±10–20% noise
between repetitions. The raw files show the spread.

### One benchmark client (the standard `redis-benchmark` setup)

| test | c=50 P=1<br>Redis | c=50 P=1<br>clone | c=50 P=16<br>Redis | c=50 P=16<br>clone | P=16<br>clone/Redis | c=500 P=1<br>Redis | c=500 P=1<br>clone | c=500 P=16<br>Redis | c=500 P=16<br>clone | P=16<br>clone/Redis |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| SET | 243k | 241k | 1.65M | 3.24M | **1.96×** | 247k | 222k | 1.63M | 3.05M | **1.88×** |
| GET | 243k | 242k | 2.17M | 3.00M | **1.38×** | 243k | 215k | 2.19M | 2.90M | **1.32×** |
| INCR | 248k | 238k | 1.81M | 3.36M | **1.85×** | 249k | 234k | 1.82M | 2.99M | **1.64×** |
| LPUSH | 164k | 187k | 1.99M | 3.34M | **1.68×** | 247k | 213k | 1.98M | 3.08M | **1.56×** |
| LPOP | 202k | 165k | 1.90M | 3.31M | **1.74×** | 245k | 208k | 1.91M | 3.15M | **1.65×** |
| SADD | 205k | 155k | 1.56M | 1.82M | **1.17×** | 248k | 234k | 1.50M | 1.78M | **1.18×** |
| HSET | 142k | 214k | 1.39M | 1.73M | **1.24×** | 248k | 229k | 1.17M | 1.68M | **1.43×** |
| ZADD | 158k | 222k | 491k | 288k | **0.59×** | 245k | 228k | 495k | 279k | **0.56×** |
| LRANGE_100 | 123k | 125k | 243k | 249k | **1.02×** | 123k | 120k | 242k | 247k | **1.02×** |
| MSET (10 keys) | 163k | 229k | 285k | 516k | **1.81×** | 159k | 224k | 299k | 506k | **1.69×** |

(requests/second; P = pipeline depth, c = connections)

### Four benchmark clients at once (4 × 50 connections, P=16), summed

A single `redis-benchmark` process is single-threaded, so with pipelining it saturates
around 2–3M req/s itself. To see what the servers can do, four processes run the same test
concurrently. This table also includes Redis with its own multi-threaded I/O
(`--io-threads 4`), which is the fairer multi-core baseline.

| test | Redis | Redis `io-threads 4` | redis-clone (4 workers) | clone / Redis | clone / Redis io4 |
|---|---:|---:|---:|---:|---:|
| SET | 1.17M | 2.63M | 3.48M | **2.99×** | 1.32× |
| GET | 1.99M | 3.22M | 3.51M | **1.76×** | 1.09× |
| INCR | 1.84M | 3.17M | 3.52M | **1.91×** | 1.11× |
| LPUSH | 1.89M | 3.30M | 3.22M | **1.70×** | 0.97× |
| LPOP | 1.85M | 3.26M | 3.16M | **1.71×** | 0.97× |
| SADD | 1.53M | 2.88M | 1.90M | **1.24×** | 0.66× |
| HSET | 1.37M | 2.73M | 1.72M | **1.25×** | 0.63× |
| ZADD | 459k | 559k | 210k | **0.46×** | 0.38× |
| LRANGE_100 | 380k | 437k | 586k | **1.54×** | 1.34× |
| MSET (10 keys) | 300k | 398k | 487k | **1.62×** | 1.22× |

The same experiment without pipelining (4 × 50 connections, P=1) gives **~215–235k req/s
for every server on every test** (see `scaling.json`). The loopback TCP stack and the
client processes are the bottleneck there, not the servers.

### Memory: 1M keys (`SET key:%012d xxx`, loaded with `redis-cli --pipe`)

| server | `used_memory` / key | RSS / key |
|---|---:|---:|
| Redis 8.10.2 (libc malloc) | 62.2 B | 66.7 B |
| redis-clone | 72.5 B | 85.4 B |

### Reading the results honestly

- **Pipelined, random-key workloads (SET/GET/INCR/MSET/LPUSH/LPOP)**: redis-clone does
  1.3–2× single-threaded Redis with one client and 1.6–3× with four, because parsing,
  execution and socket I/O all run in parallel on 4 workers and random keys rarely share a
  shard. Against Redis with `io-threads 4` it is roughly even (0.97–1.3×). Redis' threaded
  I/O offloads exactly the parsing/writing work that dominates these tiny commands, and
  its single execution thread isn't saturated yet at this client count.
- **Without pipelining** everything is bound by round trips through the kernel and the
  benchmark client (~240k req/s per client process here). redis-clone matches Redis within
  noise at 50 connections and is ~5–10% lower at 500 connections (more worker wake-ups per
  request).
- **Single-key collection writes (SADD, HSET, ZADD on one key)**: redis-benchmark sends
  every SADD/HSET/ZADD to the same key (`myset`, `myhash`, `myzset`). All of them
  serialize on that key's shard mutex, so these benchmarks gain nothing from more workers,
  and with four clients the lock hand-offs between cores cost more than Redis' lock-free
  single executor (0.63–0.66× of Redis io4). This is the cost of design decision 1.
- **ZADD is where Redis clearly wins (0.46–0.59×).** Each ZADD walks a skiplist that grows
  to ~600k members, and the walk is memory-latency bound (a microbenchmark of the bare
  skiplist measures ~1.6 µs per insert at 1M elements). I already moved to 64-byte,
  cache-line-aligned nodes with the first four levels inline and the members stored
  separately (~20% faster). Redis still has two advantages: it does one dict lookup per
  update where this code does two (score check, then update), and it has no lock hand-off
  between threads on the single hot key.
- **LRANGE_100** is dominated by encoding 100 bulk replies. It's even with one client and
  1.3–1.5× faster with four, because reply encoding runs on all workers (the list is only
  read under the lock).
- **Memory: Redis uses ~14% less (`used_memory`) and ~22% less RSS per key.** A redis-clone
  key costs a 64-byte dict node (key and value inline) plus an 8-byte bucket slot. Redis 8
  embeds the key and value in a single `kvobj` allocation in its new open-addressing
  hashtable. `used_memory` here counts requested bytes, not malloc's rounded size classes,
  which is part of why the RSS gap is larger than the `used_memory` gap.

---

## Limitations and differences from Redis

- **Not implemented**: Lua scripting and functions, cluster mode, ACL users (only the
  default user with `requirepass`), keyspace notifications, `MONITOR`, client-side caching
  (`CLIENT TRACKING`), stream consumer groups (`XGROUP`/`XREADGROUP`/`XACK`), `SORT`,
  bitmaps, HyperLogLog, geo commands, `DUMP`/`RESTORE`, `OBJECT` serialization.
- **Persistence formats are our own**: snapshots are not RDB files and cannot be read by
  Redis (or read Redis' `dump.rdb`). The AOF is plain RESP and the layout matches Redis 7's
  multi-part AOF, but its base file is our snapshot format.
- **Replication works only between redis-clone instances** (the snapshot payload is our
  format). There is no chained replication (a replica refuses `PSYNC`) and no PSYNC2
  history after a failover.
- **Hot keys don't scale**: all commands on one key serialize on its shard mutex. Redis is
  single-threaded anyway, so this is never slower than one core, but single-key benchmarks
  gain only from parallel I/O.
- `WAIT` and `DEBUG SLEEP` block the calling worker thread (and the other clients that
  worker serves), not the whole server; `SAVE` blocks only the calling client.
- Large lists use a ring buffer of compact strings instead of a quicklist: O(1) `LINDEX`
  and pushes at both ends, but ~24 bytes per element instead of a few. `OBJECT ENCODING`
  still reports `quicklist`, and a large list returns to `listpack` when it shrinks to half
  the size limit. Redis only converts back once the list is down to a single quicklist
  node, so the two can briefly disagree on the reported encoding.
- `INCRBYFLOAT`/`HINCRBYFLOAT` print the shortest round-trip decimal (`10.6`). Redis prints
  `%.17Lf`, which gives `10.6` on x86-64 but `10.59999999999999964` on ARM, where `long double` is 64-bit.
- `XADD/XTRIM ... ~` (approximate trimming) removes whole 100-entry blocks, as Redis does
  with its default `stream-node-max-entries`, but Redis' nodes can also close early because
  of `stream-node-max-bytes`, so approximate trims may differ slightly.
- `SCAN`/`KEYS`/`SMEMBERS` order differs from Redis (different hash function); the
  guarantees are the same.
- Commands in `MULTI` are validated at queue time like Redis 7 (unknown command, arity,
  unknown subcommand), but runtime errors inside `EXEC` don't roll back, exactly as in Redis.

## License

MIT, see [LICENSE](LICENSE).
