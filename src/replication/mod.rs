//! Master/replica replication.
//!
//! * **Full sync**: a replica sends `PSYNC ? -1` (or an unknown history); the
//!   master answers `+FULLRESYNC <replid> <offset>`, takes a fork-less
//!   point-in-time snapshot, ships it as one bulk payload and then streams
//!   every write executed after the snapshot instant.
//! * **Partial resync**: after a short disconnect the replica sends
//!   `PSYNC <replid> <offset+1>`; if the master's backlog (a ring buffer of the
//!   most recent stream bytes) still covers that offset, it answers
//!   `+CONTINUE` and resends only the missing tail.
//! * Replicas acknowledge their processed offset with `REPLCONF ACK` every
//!   second and on `REPLCONF GETACK`, which powers `WAIT` and the `lag`/
//!   `offset` fields of `INFO replication`.

pub mod master;
pub mod replica;

use crate::server::Server;
use parking_lot::Mutex;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::Relaxed};

/// Replica-side replication state.
#[derive(Default)]
pub struct ReplState {
    /// This server is a replica.
    pub is_replica: AtomicBool,
    /// Master address.
    pub master: Mutex<Option<(String, u16)>>,
    /// Link to the master is established and streaming.
    pub link_up: AtomicBool,
    /// A full sync is being received.
    pub sync_in_progress: AtomicBool,
    /// Last time we heard from the master (ms).
    pub last_io_ms: AtomicU64,
    /// Bytes of the master's stream processed.
    pub offset: AtomicU64,
    /// The master's replication id (empty if never synced).
    pub master_replid: Mutex<String>,
    /// Database selected in the master stream (kept across reconnects).
    pub master_db: AtomicUsize,
    /// Bumped to stop the current replication thread.
    pub generation: AtomicU64,
    /// Full syncs performed as a replica.
    pub full_syncs: AtomicU64,
    /// Partial resyncs performed as a replica.
    pub partial_syncs: AtomicU64,
}

/// The `# Replication` INFO section.
pub fn info_section(server: &Server) -> String {
    let mut s = String::from("# Replication\r\n");
    let p = server.prop.lock();
    let r = &server.repl;
    if server.is_replica() {
        let (host, port) = r.master.lock().clone().unwrap_or_default();
        let link = r.link_up.load(Relaxed);
        let off = r.offset.load(Relaxed);
        let _ = write!(
            s,
            "role:slave\r\nmaster_host:{host}\r\nmaster_port:{port}\r\nmaster_link_status:{}\r\nmaster_last_io_seconds_ago:{}\r\nmaster_sync_in_progress:{}\r\nslave_read_repl_offset:{off}\r\nslave_repl_offset:{off}\r\nslave_priority:100\r\nslave_read_only:{}\r\nreplica_announced:1\r\nconnected_slaves:0\r\nmaster_failover_state:no-failover\r\nmaster_replid:{}\r\nmaster_replid2:{}\r\nmaster_repl_offset:{off}\r\nsecond_repl_offset:-1\r\n",
            if link { "up" } else { "down" },
            if link {
                crate::util::now_ms().saturating_sub(r.last_io_ms.load(Relaxed)) / 1000
            } else {
                0
            },
            r.sync_in_progress.load(Relaxed) as u8,
            server.config.replica_read_only.load(Relaxed) as u8,
            {
                let id = r.master_replid.lock().clone();
                if id.is_empty() {
                    p.repl.replid.clone()
                } else {
                    id
                }
            },
            p.repl.replid2,
        );
    } else {
        let _ = write!(
            s,
            "role:master\r\nconnected_slaves:{}\r\n",
            p.repl.replicas.len()
        );
        let now = crate::util::now_ms();
        for (i, l) in p.repl.replicas.iter().enumerate() {
            let ip = l
                .client
                .addr
                .rsplit_once(':')
                .map(|(h, _)| h.to_string())
                .unwrap_or_default();
            let _ = write!(
                s,
                "slave{i}:ip={ip},port={},state={},offset={},lag={}\r\n",
                l.port(),
                if l.is_online() {
                    "online"
                } else {
                    "wait_bgsave"
                },
                l.ack_offset.load(Relaxed),
                now.saturating_sub(l.last_ack_ms.load(Relaxed)) / 1000
            );
        }
        let _ = write!(
            s,
            "master_failover_state:no-failover\r\nmaster_replid:{}\r\nmaster_replid2:{}\r\nmaster_repl_offset:{}\r\nsecond_repl_offset:{}\r\n",
            p.repl.replid, p.repl.replid2, p.repl.offset, p.repl.second_offset
        );
    }
    match &p.repl.backlog {
        Some(b) => {
            let _ = write!(
                s,
                "repl_backlog_active:1\r\nrepl_backlog_size:{}\r\nrepl_backlog_first_byte_offset:{}\r\nrepl_backlog_histlen:{}\r\n",
                b.size(),
                p.repl.offset - b.histlen() as u64 + 1,
                b.histlen()
            );
        }
        None => s.push_str("repl_backlog_active:0\r\nrepl_backlog_size:1048576\r\nrepl_backlog_first_byte_offset:0\r\nrepl_backlog_histlen:0\r\n"),
    }
    s.push_str("\r\n");
    s
}
