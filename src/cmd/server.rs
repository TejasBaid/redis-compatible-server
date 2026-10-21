//! Server/admin commands: INFO, CONFIG, COMMAND, SAVE/BGSAVE/BGREWRITEAOF,
//! SHUTDOWN, DEBUG, SLOWLOG, MEMORY, TIME, LASTSAVE.

use super::flags::*;
use super::{Command, Ctx, KeySpec, NOKEYS, NOT_INT, SYNTAX, c};
use crate::db::NUM_SHARDS;
use crate::util::{eq_ic, glob_match, parse_f64, parse_i64};
use std::fmt::Write as _;
use std::sync::atomic::Ordering::Relaxed;

pub(crate) fn commands() -> Vec<Command> {
    let g = "server";
    vec![
        c(
            "info",
            -1,
            LOADING | STALE | SELF_LOCK,
            NOKEYS,
            info,
            g,
            "Returns information and statistics about the server.",
        ),
        c(
            "config",
            -2,
            ADMIN | LOADING | STALE,
            NOKEYS,
            config,
            g,
            "A container for server configuration commands.",
        ),
        c(
            "command",
            -1,
            LOADING | STALE,
            NOKEYS,
            command,
            g,
            "Returns detailed information about all commands.",
        ),
        c(
            "save",
            1,
            ADMIN | NO_MULTI,
            NOKEYS,
            save,
            g,
            "Synchronously saves the database(s) to disk.",
        ),
        c(
            "bgsave",
            -1,
            ADMIN,
            NOKEYS,
            bgsave,
            g,
            "Asynchronously saves the database(s) to disk.",
        ),
        c(
            "bgrewriteaof",
            1,
            ADMIN,
            NOKEYS,
            bgrewriteaof,
            g,
            "Asynchronously rewrites the append-only file to disk.",
        ),
        c(
            "lastsave",
            1,
            LOADING | STALE | FAST,
            NOKEYS,
            lastsave,
            g,
            "Returns the Unix timestamp of the last successful save to disk.",
        ),
        c(
            "shutdown",
            -1,
            ADMIN | LOADING | STALE | NO_MULTI,
            NOKEYS,
            shutdown,
            g,
            "Synchronously saves the database(s) to disk and shuts down the Redis server.",
        ),
        c(
            "debug",
            -2,
            ADMIN | LOADING | STALE,
            NOKEYS,
            debug,
            g,
            "A container for debugging commands.",
        ),
        c(
            "slowlog",
            -2,
            ADMIN | LOADING | STALE,
            NOKEYS,
            slowlog,
            g,
            "A container for slow log commands.",
        ),
        c(
            "memory",
            -2,
            READONLY,
            KeySpec::Custom(memory_keys),
            memory,
            g,
            "A container for memory diagnostics commands.",
        ),
        c(
            "time",
            1,
            LOADING | STALE | FAST,
            NOKEYS,
            time,
            g,
            "Returns the server time.",
        ),
        c(
            "latency",
            -2,
            ADMIN | LOADING | STALE,
            NOKEYS,
            latency,
            g,
            "A container for latency diagnostics commands.",
        ),
        c(
            "lolwut",
            -1,
            READONLY | FAST,
            NOKEYS,
            lolwut,
            g,
            "Displays computer art and the Redis version",
        ),
        c(
            "monitor",
            1,
            ADMIN,
            NOKEYS,
            monitor,
            g,
            "Listens for all requests received by the server in real-time.",
        ),
        c(
            "readonly",
            1,
            FAST | LOADING | STALE,
            NOKEYS,
            okcmd,
            "cluster",
            "Enables read-only queries for a connection to a Redis Cluster replica node.",
        ),
        c(
            "readwrite",
            1,
            FAST | LOADING | STALE,
            NOKEYS,
            okcmd,
            "cluster",
            "Enables read-write queries for a connection to a Redis Cluster replica node.",
        ),
    ]
}

fn okcmd(ctx: &mut Ctx, _args: &[&[u8]]) {
    ctx.out.ok();
}

fn human(bytes: u64) -> String {
    let b = bytes as f64;
    if b < 1024.0 {
        format!("{bytes}B")
    } else if b < 1024.0 * 1024.0 {
        format!("{:.2}K", b / 1024.0)
    } else if b < 1024.0 * 1024.0 * 1024.0 {
        format!("{:.2}M", b / 1024.0 / 1024.0)
    } else {
        format!("{:.2}G", b / 1024.0 / 1024.0 / 1024.0)
    }
}

fn cpu_times() -> (f64, f64) {
    // SAFETY: getrusage fills a zeroed rusage struct; no other preconditions.
    unsafe {
        let mut ru: libc::rusage = std::mem::zeroed();
        if libc::getrusage(libc::RUSAGE_SELF, &mut ru) == 0 {
            let t = |tv: libc::timeval| tv.tv_sec as f64 + tv.tv_usec as f64 / 1e6;
            return (t(ru.ru_stime), t(ru.ru_utime));
        }
    }
    (0.0, 0.0)
}

/// Builds the INFO text for the requested sections.
pub fn info_text(ctx: &mut Ctx, sections: &[String]) -> String {
    let server = ctx.server;
    let all = sections.iter().any(|s| s == "all" || s == "everything");
    let default = sections.is_empty() || sections.iter().any(|s| s == "default");
    let want = |name: &str| {
        all || sections.iter().any(|s| s == name) || (default && name != "commandstats")
    };
    let mut out = String::new();
    let now_ms = crate::util::now_ms();
    if want("server") {
        let uptime = server.started.elapsed().as_secs();
        let _ = write!(
            out,
            "# Server\r\nredis_version:{}\r\nredis_git_sha1:00000000\r\nredis_git_dirty:0\r\nredis_build_id:0\r\nredis_mode:standalone\r\nimplementation:redis-clone {}\r\nos:{} {}\r\narch_bits:64\r\nmultiplexing_api:{}\r\nprocess_id:{}\r\nrun_id:{}\r\ntcp_port:{}\r\nserver_time_usec:{}\r\nuptime_in_seconds:{}\r\nuptime_in_days:{}\r\nhz:{}\r\nconfigured_hz:{}\r\nlru_clock:{}\r\nexecutable:{}\r\nconfig_file:\r\nio_threads_active:1\r\nworker_threads:{}\r\nkeyspace_shards:{}\r\n\r\n",
            crate::server::VERSION,
            env!("CARGO_PKG_VERSION"),
            std::env::consts::OS,
            std::env::consts::ARCH,
            if cfg!(target_os = "linux") {
                "epoll"
            } else {
                "kqueue"
            },
            std::process::id(),
            server.run_id,
            server.port.load(Relaxed),
            crate::util::now_us(),
            uptime,
            uptime / 86400,
            server.config.hz.load(Relaxed),
            server.config.hz.load(Relaxed),
            crate::db::lru_clock(),
            std::env::current_exe()
                .map(|p| p.display().to_string())
                .unwrap_or_default(),
            server.config.threads.load(Relaxed),
            NUM_SHARDS,
        );
    }
    if want("clients") {
        let clients: Vec<_> = server.clients.lock().values().cloned().collect();
        let blocked = clients.iter().filter(|h| h.blocked.load(Relaxed)).count();
        let pubsub = clients
            .iter()
            .filter(|h| h.sub.load(Relaxed) + h.psub.load(Relaxed) > 0)
            .count();
        let watching = clients.iter().filter(|h| h.watch.load(Relaxed) > 0).count();
        let watched: usize = clients.iter().map(|h| h.watch.load(Relaxed)).sum();
        let _ = write!(
            out,
            "# Clients\r\nconnected_clients:{}\r\ncluster_connections:0\r\nmaxclients:10000\r\nclient_recent_max_input_buffer:0\r\nclient_recent_max_output_buffer:0\r\nblocked_clients:{}\r\ntracking_clients:0\r\npubsub_clients:{}\r\nwatching_clients:{}\r\nclients_in_timeout_table:{}\r\ntotal_watched_keys:{}\r\ntotal_blocking_keys:0\r\ntotal_blocking_keys_on_nokey:0\r\n\r\n",
            clients.len(),
            blocked,
            pubsub,
            watching,
            blocked,
            watched
        );
    }
    if want("memory") {
        let used = crate::alloc::used_memory();
        let rss = crate::alloc::rss_bytes();
        let peak = server
            .persist
            .peak_memory
            .fetch_max(used, Relaxed)
            .max(used);
        let maxmem = server.config.maxmemory.load(Relaxed);
        let frag = if used > 0 {
            rss as f64 / used as f64
        } else {
            0.0
        };
        let _ = write!(
            out,
            "# Memory\r\nused_memory:{used}\r\nused_memory_human:{}\r\nused_memory_rss:{rss}\r\nused_memory_rss_human:{}\r\nused_memory_peak:{peak}\r\nused_memory_peak_human:{}\r\nmaxmemory:{maxmem}\r\nmaxmemory_human:{}\r\nmaxmemory_policy:{}\r\nmem_fragmentation_ratio:{:.2}\r\nmem_allocator:libc\r\nlazyfree_pending_objects:0\r\n\r\n",
            human(used),
            human(rss),
            human(peak),
            human(maxmem),
            server.config.policy().name(),
            frag
        );
    }
    if want("persistence") {
        out.push_str(&crate::persistence::info_section(server));
    }
    if want("stats") {
        let sum = |f: fn(&crate::server::WorkerStats) -> u64| {
            server.worker_stats.iter().map(|w| f(w)).sum::<u64>()
        };
        let ops = {
            let s = server.stats.ops_samples.lock();
            match (s.front(), s.back()) {
                (Some(a), Some(b)) if b.0 > a.0 => {
                    ((b.1 - a.1) as f64 / (b.0 - a.0).as_secs_f64()).round() as u64
                }
                _ => 0,
            }
        };
        let ps = server.pubsub.read();
        let _ = write!(
            out,
            "# Stats\r\ntotal_connections_received:{}\r\ntotal_commands_processed:{}\r\ninstantaneous_ops_per_sec:{}\r\ntotal_net_input_bytes:{}\r\ntotal_net_output_bytes:{}\r\nrejected_connections:0\r\nsync_full:{}\r\nsync_partial_ok:{}\r\nsync_partial_err:{}\r\nexpired_keys:{}\r\nexpired_keys_active:{}\r\nevicted_keys:{}\r\nkeyspace_hits:{}\r\nkeyspace_misses:{}\r\npubsub_channels:{}\r\npubsub_patterns:{}\r\ntotal_error_replies:{}\r\ntotal_rejected_calls:{}\r\n\r\n",
            server.stats.connections.load(Relaxed),
            sum(|w| w.commands.load(Relaxed)),
            ops,
            sum(|w| w.net_in.load(Relaxed)),
            sum(|w| w.net_out.load(Relaxed)),
            server.stats.sync_full.load(Relaxed),
            server.stats.sync_partial_ok.load(Relaxed),
            server.stats.sync_partial_err.load(Relaxed),
            sum(|w| w.expired.load(Relaxed)) + server.stats.expired_active.load(Relaxed),
            server.stats.expired_active.load(Relaxed),
            server.stats.evicted.load(Relaxed),
            sum(|w| w.hits.load(Relaxed)),
            sum(|w| w.misses.load(Relaxed)),
            ps.channel_count(),
            ps.numpat(),
            sum(|w| w.errors.load(Relaxed))
                + sum(|w| w.failed.iter().map(|f| f.load(Relaxed)).sum()),
            sum(|w| w.rejected.load(Relaxed)),
        );
    }
    if want("replication") {
        out.push_str(&crate::replication::info_section(server));
    }
    if want("cpu") {
        let (sys, user) = cpu_times();
        let _ = write!(
            out,
            "# CPU\r\nused_cpu_sys:{sys:.6}\r\nused_cpu_user:{user:.6}\r\n\r\n"
        );
    }
    if want("commandstats") {
        out.push_str("# Commandstats\r\n");
        for (i, cmd) in super::all().iter().enumerate() {
            let calls: u64 = server
                .worker_stats
                .iter()
                .map(|w| w.calls[i].load(Relaxed))
                .sum();
            if calls == 0 {
                continue;
            }
            let usec: u64 = server
                .worker_stats
                .iter()
                .map(|w| w.usec[i].load(Relaxed))
                .sum();
            let failed: u64 = server
                .worker_stats
                .iter()
                .map(|w| w.failed[i].load(Relaxed))
                .sum();
            let _ = write!(
                out,
                "cmdstat_{}:calls={},usec={},usec_per_call={:.2},rejected_calls=0,failed_calls={}\r\n",
                cmd.name,
                calls,
                usec,
                usec as f64 / calls as f64,
                failed
            );
        }
        out.push_str("\r\n");
    }
    if want("keyspace") {
        out.push_str("# Keyspace\r\n");
        let mut keys = vec![0usize; server.keyspace.databases];
        let mut expires = vec![0usize; server.keyspace.databases];
        for i in 0..NUM_SHARDS {
            ctx.db.with_shard(i, |sh| {
                for (d, db) in sh.dbs.iter().enumerate() {
                    keys[d] += db.len();
                    expires[d] += db.expires.len();
                }
            });
        }
        for (d, (k, e)) in keys.iter().zip(&expires).enumerate() {
            if *k > 0 {
                let _ = write!(out, "db{d}:keys={k},expires={e},avg_ttl=0\r\n");
            }
        }
        out.push_str("\r\n");
    }
    let _ = now_ms;
    while out.ends_with("\r\n\r\n") {
        out.truncate(out.len() - 2);
    }
    out
}

fn info(ctx: &mut Ctx, args: &[&[u8]]) {
    let sections: Vec<String> = args[1..]
        .iter()
        .map(|a| String::from_utf8_lossy(a).to_ascii_lowercase())
        .collect();
    let text = info_text(ctx, &sections);
    ctx.out.verbatim(text.as_bytes());
}

fn config(ctx: &mut Ctx, args: &[&[u8]]) {
    let sub = args[1].to_ascii_lowercase();
    match sub.as_slice() {
        b"get" if args.len() >= 3 => {
            let mut pairs: Vec<(String, String)> = Vec::new();
            for pat in &args[2..] {
                let pat = pat.to_ascii_lowercase();
                for name in crate::config::PARAMS {
                    if glob_match(&pat, name.as_bytes(), true)
                        && !pairs.iter().any(|(n, _)| n == name)
                        && let Some(v) = ctx.server.config.get(name)
                    {
                        pairs.push((name.to_string(), v));
                    }
                }
            }
            ctx.out.map(pairs.len());
            for (n, v) in pairs {
                ctx.out.bulk(n.as_bytes());
                ctx.out.bulk(v.as_bytes());
            }
        }
        b"set" if args.len() >= 4 && args.len().is_multiple_of(2) => {
            for kv in args[2..].chunks(2) {
                let name = String::from_utf8_lossy(kv[0]).to_ascii_lowercase();
                let value = String::from_utf8_lossy(kv[1]).into_owned();
                if !crate::config::PARAMS.contains(&name.as_str()) {
                    return ctx.out.error(&format!(
                        "ERR Unknown option or number of arguments for CONFIG SET - '{name}'"
                    ));
                }
                let was_aof = ctx.server.config.appendonly.load(Relaxed);
                if let Err(e) = ctx.server.config.set(&name, &value, false) {
                    return ctx.out.error(&format!(
                        "ERR CONFIG SET failed (possibly related to argument '{name}') - {e}"
                    ));
                }
                if name == "appendonly" {
                    let now_aof = ctx.server.config.appendonly.load(Relaxed);
                    if now_aof && !was_aof {
                        if let Err(e) = crate::persistence::aof::enable(ctx.server) {
                            ctx.server.config.appendonly.store(false, Relaxed);
                            return ctx.out.error(&format!("ERR CONFIG SET failed (possibly related to argument 'appendonly') - {e}"));
                        }
                    } else if !now_aof && was_aof {
                        crate::persistence::aof::disable(ctx.server);
                    }
                }
                if name == "repl-backlog-size" {
                    crate::replication::master::resize_backlog(ctx.server);
                }
            }
            ctx.out.ok();
        }
        b"resetstat" if args.len() == 2 => {
            for w in &ctx.server.worker_stats {
                for v in w.calls.iter().chain(w.usec.iter()).chain(w.failed.iter()) {
                    v.store(0, Relaxed);
                }
                for v in [
                    &w.commands,
                    &w.hits,
                    &w.misses,
                    &w.expired,
                    &w.net_in,
                    &w.net_out,
                    &w.errors,
                    &w.rejected,
                ] {
                    v.store(0, Relaxed);
                }
            }
            for v in [
                &ctx.server.stats.connections,
                &ctx.server.stats.expired_active,
                &ctx.server.stats.evicted,
                &ctx.server.stats.sync_full,
                &ctx.server.stats.sync_partial_ok,
                &ctx.server.stats.sync_partial_err,
            ] {
                v.store(0, Relaxed);
            }
            ctx.out.ok();
        }
        b"rewrite" if args.len() == 2 => ctx
            .out
            .error("ERR The server is running without a config file"),
        b"help" => {
            let lines = [
                "CONFIG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                "GET <pattern>",
                "    Return parameters matching the glob-like <pattern> and their values.",
                "SET <directive> <value>",
                "    Set the configuration <directive> to <value>.",
                "RESETSTAT",
                "    Reset statistics reported by the INFO command.",
                "REWRITE",
                "    Rewrite the configuration file.",
                "HELP",
                "    Print this help.",
            ];
            ctx.out.array(lines.len());
            for l in lines {
                ctx.out.simple(l);
            }
        }
        b"get" | b"set" | b"resetstat" | b"rewrite" => ctx.out.error(&format!(
            "ERR wrong number of arguments for 'config|{}' command",
            String::from_utf8_lossy(&sub)
        )),
        _ => ctx.subcommand_error("config", args[1]),
    }
}

fn flag_names(f: u32) -> Vec<&'static str> {
    let mut v = Vec::new();
    for (bit, name) in [
        (WRITE, "write"),
        (READONLY, "readonly"),
        (DENYOOM, "denyoom"),
        (ADMIN, "admin"),
        (PUBSUB, "pubsub"),
        (BLOCKING, "blocking"),
        (LOADING, "loading"),
        (STALE, "stale"),
        (FAST, "fast"),
        (NO_AUTH, "no_auth"),
    ] {
        if f & bit != 0 {
            v.push(name);
        }
    }
    v
}

fn key_triple(cmd: &Command) -> (i64, i64, i64) {
    match cmd.keys {
        KeySpec::Range(a, b, s) => (a as i64, b as i64, s as i64),
        KeySpec::Custom(_) => (1, 1, 1),
        KeySpec::None => (0, 0, 0),
    }
}

fn command_info(ctx: &mut Ctx, cmd: &Command) {
    let (f, l, s) = key_triple(cmd);
    let flags = flag_names(cmd.flags);
    ctx.out.array(10);
    ctx.out.bulk(cmd.name.as_bytes());
    ctx.out.int(cmd.arity as i64);
    ctx.out.set(flags.len());
    for fl in flags {
        ctx.out.simple(fl);
    }
    ctx.out.int(f);
    ctx.out.int(l);
    ctx.out.int(s);
    ctx.out.set(0);
    ctx.out.set(0);
    ctx.out.array(0);
    ctx.out.array(0);
}

fn command_docs(ctx: &mut Ctx, cmd: &Command) {
    ctx.out.bulk(cmd.name.as_bytes());
    ctx.out.map(4);
    ctx.out.bulk(b"summary");
    ctx.out.bulk(cmd.summary.as_bytes());
    ctx.out.bulk(b"since");
    ctx.out.bulk(b"1.0.0");
    ctx.out.bulk(b"group");
    ctx.out.bulk(cmd.group.as_bytes());
    ctx.out.bulk(b"complexity");
    ctx.out.bulk(b"O(1)");
}

fn command(ctx: &mut Ctx, args: &[&[u8]]) {
    let all = super::all();
    if args.len() == 1 {
        ctx.out.array(all.len());
        for cmd in all {
            command_info(ctx, cmd);
        }
        return;
    }
    let sub = args[1].to_ascii_lowercase();
    match sub.as_slice() {
        b"count" => ctx.out.int(all.len() as i64),
        b"info" => {
            if args.len() == 2 {
                ctx.out.array(all.len());
                for cmd in all {
                    command_info(ctx, cmd);
                }
            } else {
                ctx.out.array(args.len() - 2);
                for name in &args[2..] {
                    match super::lookup(name) {
                        Some((_, cmd)) => command_info(ctx, cmd),
                        None => ctx.out.null_array(),
                    }
                }
            }
        }
        b"docs" => {
            let picked: Vec<&Command> = if args.len() == 2 {
                all.iter().collect()
            } else {
                args[2..]
                    .iter()
                    .filter_map(|n| super::lookup(n).map(|(_, c)| c))
                    .collect()
            };
            ctx.out.map(picked.len());
            for cmd in picked {
                command_docs(ctx, cmd);
            }
        }
        b"list" => {
            ctx.out.array(all.len());
            for cmd in all {
                ctx.out.bulk(cmd.name.as_bytes());
            }
        }
        b"getkeys" if args.len() >= 3 => {
            let Some((_, cmd)) = super::lookup(args[2]) else {
                return ctx.out.error("ERR Invalid command specified");
            };
            let sub_args = &args[2..];
            let pos = super::key_positions(cmd, sub_args);
            if pos.is_empty() {
                return ctx.out.error("ERR The command has no key arguments");
            }
            ctx.out.array(pos.len());
            for p in pos {
                ctx.out.bulk(sub_args[p]);
            }
        }
        b"help" => {
            ctx.out.array(1);
            ctx.out.simple("COMMAND [COUNT|DOCS|INFO|LIST|GETKEYS]");
        }
        _ => ctx.subcommand_error("command", args[1]),
    }
}

fn save(ctx: &mut Ctx, _args: &[&[u8]]) {
    match crate::persistence::save_sync(ctx.server) {
        Ok(()) => ctx.out.ok(),
        Err(e) => ctx.out.error(&format!("ERR {e}")),
    }
}

fn bgsave(ctx: &mut Ctx, args: &[&[u8]]) {
    if args.len() > 2 || (args.len() == 2 && !eq_ic(args[1], "schedule")) {
        return ctx.out.error(SYNTAX);
    }
    match crate::persistence::bgsave(ctx.server) {
        Ok(()) => ctx.out.simple("Background saving started"),
        Err(e) => ctx.out.error(&format!("ERR {e}")),
    }
}

fn bgrewriteaof(ctx: &mut Ctx, _args: &[&[u8]]) {
    match crate::persistence::aof::bgrewrite(ctx.server) {
        Ok(()) => ctx
            .out
            .simple("Background append only file rewriting started"),
        Err(e) => ctx.out.error(&format!("ERR {e}")),
    }
}

fn lastsave(ctx: &mut Ctx, _args: &[&[u8]]) {
    ctx.out
        .int(ctx.server.persist.last_save.load(Relaxed) as i64);
}

fn shutdown(ctx: &mut Ctx, args: &[&[u8]]) {
    let mut save = None;
    for a in &args[1..] {
        if eq_ic(a, "nosave") {
            save = Some(false);
        } else if eq_ic(a, "save") {
            save = Some(true);
        } else if eq_ic(a, "now") || eq_ic(a, "force") {
        } else if eq_ic(a, "abort") {
            return ctx.out.error("ERR No shutdown in progress.");
        } else {
            return ctx.out.error(SYNTAX);
        }
    }
    match ctx.server.shutdown(save) {
        Ok(()) => {
            // The connection closes without a reply, like Redis.
            ctx.client.close_after_reply = true;
        }
        Err(e) => ctx.out.error(&format!("ERR {e}")),
    }
}

fn debug(ctx: &mut Ctx, args: &[&[u8]]) {
    let sub = args[1].to_ascii_lowercase();
    match sub.as_slice() {
        b"sleep" if args.len() == 3 => {
            let Some(secs) = parse_f64(args[2]) else {
                return ctx.out.error(crate::cmd::NOT_FLOAT);
            };
            std::thread::sleep(std::time::Duration::from_secs_f64(secs.max(0.0)));
            ctx.out.ok();
        }
        b"set-active-expire" if args.len() == 3 => {
            let Some(v) = parse_i64(args[2]) else {
                return ctx.out.error(NOT_INT);
            };
            ctx.server.config.active_expire.store(v != 0, Relaxed);
            ctx.out.ok();
        }
        b"reload" => match crate::persistence::debug_reload(ctx.server) {
            Ok(()) => ctx.out.ok(),
            Err(e) => ctx
                .out
                .error(&format!("ERR Error trying to load the snapshot: {e}")),
        },
        b"change-repl-id" => {
            let mut p = ctx.server.prop.lock();
            p.repl.replid2 = std::mem::replace(&mut p.repl.replid, crate::propagate::new_replid());
            p.repl.second_offset = p.repl.offset as i64 + 1;
            ctx.out.ok();
        }
        b"jmap" | b"quicklist-packed-threshold" | b"stringmatch-len" => ctx.out.ok(),
        b"help" => {
            let lines = [
                "DEBUG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                "SLEEP <seconds>",
                "SET-ACTIVE-EXPIRE <0|1>",
                "RELOAD",
                "CHANGE-REPL-ID",
                "HELP",
            ];
            ctx.out.array(lines.len());
            for l in lines {
                ctx.out.simple(l);
            }
        }
        _ => ctx.subcommand_error("debug", args[1]),
    }
}

fn slowlog(ctx: &mut Ctx, args: &[&[u8]]) {
    let sub = args[1].to_ascii_lowercase();
    match sub.as_slice() {
        b"get" if args.len() <= 3 => {
            let count = match args.get(2) {
                Some(a) => match parse_i64(a) {
                    Some(n) if n >= -1 => {
                        if n == -1 {
                            usize::MAX
                        } else {
                            n as usize
                        }
                    }
                    _ => {
                        return ctx
                            .out
                            .error("ERR count should be greater than or equal to -1");
                    }
                },
                None => 10,
            };
            let sl = ctx.server.slowlog.lock();
            let n = count.min(sl.entries.len());
            ctx.out.array(n);
            for e in sl.entries.iter().take(n) {
                ctx.out.array(6);
                ctx.out.int(e.id as i64);
                ctx.out.int(e.time as i64);
                ctx.out.int(e.micros as i64);
                ctx.out.array(e.args.len());
                for a in &e.args {
                    ctx.out.bulk(a);
                }
                ctx.out.bulk(e.addr.as_bytes());
                ctx.out.bulk(e.name.as_bytes());
            }
        }
        b"len" if args.len() == 2 => {
            let n = ctx.server.slowlog.lock().entries.len();
            ctx.out.int(n as i64);
        }
        b"reset" if args.len() == 2 => {
            ctx.server.slowlog.lock().entries.clear();
            ctx.out.ok();
        }
        b"help" => {
            let lines = [
                "SLOWLOG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                "GET [<count>]",
                "LEN",
                "RESET",
                "HELP",
            ];
            ctx.out.array(lines.len());
            for l in lines {
                ctx.out.simple(l);
            }
        }
        _ => ctx.subcommand_error("slowlog", args[1]),
    }
}

fn memory_keys(args: &[&[u8]]) -> Vec<usize> {
    if args.len() >= 3 && eq_ic(args[1], "usage") {
        vec![2]
    } else {
        Vec::new()
    }
}

fn memory(ctx: &mut Ctx, args: &[&[u8]]) {
    let sub = args[1].to_ascii_lowercase();
    match sub.as_slice() {
        b"usage" if args.len() >= 3 => {
            let key = args[2];
            let Some(obj) = ctx.db.obj(key) else {
                return ctx.out.null();
            };
            let node = 8 + std::mem::size_of::<crate::sds::Sds>() + std::mem::size_of::<crate::db::Obj>();
            let key_heap = if key.len() > crate::sds::INLINE_CAP { key.len() } else { 0 };
            let total = node + key_heap + obj.value.heap_bytes() + if obj.has_expire() { 48 } else { 0 };
            ctx.out.int(total as i64);
        }
        b"stats" => {
            let used = crate::alloc::used_memory();
            ctx.out.map(3);
            ctx.out.bulk(b"peak.allocated");
            ctx.out.int(ctx.server.persist.peak_memory.load(Relaxed).max(used) as i64);
            ctx.out.bulk(b"total.allocated");
            ctx.out.int(used as i64);
            ctx.out.bulk(b"keys.count");
            let sizes = ctx.server.keyspace.db_sizes();
            ctx.out.int(sizes.iter().map(|(k, _)| *k as i64).sum());
        }
        b"doctor" => ctx.out.verbatim(b"Hi Sam, I can't find any memory issue in your instance. I can only account for what occurs on this base."),
        b"malloc-stats" => ctx.out.verbatim(b"Stats not supported for the current allocator"),
        b"purge" => ctx.out.ok(),
        b"help" => {
            let lines = ["MEMORY <subcommand> [<arg> [value] [opt] ...]. Subcommands are:", "USAGE <key>", "STATS", "DOCTOR", "HELP"];
            ctx.out.array(lines.len());
            for l in lines {
                ctx.out.simple(l);
            }
        }
        _ => ctx.subcommand_error("memory", args[1]),
    }
}

fn time(ctx: &mut Ctx, _args: &[&[u8]]) {
    let us = crate::util::now_us();
    ctx.out.array(2);
    ctx.out.bulk((us / 1_000_000).to_string().as_bytes());
    ctx.out.bulk((us % 1_000_000).to_string().as_bytes());
}

fn latency(ctx: &mut Ctx, args: &[&[u8]]) {
    let sub = args[1].to_ascii_lowercase();
    match sub.as_slice() {
        b"latest" | b"history" | b"histogram" => ctx.out.array(0),
        b"reset" => ctx.out.int(0),
        b"doctor" => ctx.out.verbatim(b"Latency monitoring is not implemented."),
        _ => ctx.subcommand_error("latency", args[1]),
    }
}

fn lolwut(ctx: &mut Ctx, _args: &[&[u8]]) {
    let s = format!("redis-clone ver. {}\n", env!("CARGO_PKG_VERSION"));
    ctx.out.verbatim(s.as_bytes());
}

fn monitor(ctx: &mut Ctx, _args: &[&[u8]]) {
    ctx.out.error("ERR MONITOR is not supported by this server");
}
