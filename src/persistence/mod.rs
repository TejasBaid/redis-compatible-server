//! Persistence: snapshots (SAVE/BGSAVE, save points, load on startup) and the
//! append-only file ([`aof`]).

pub mod aof;
pub mod snapshot;

use crate::server::{Server, log};
use parking_lot::Mutex;
use std::fmt::Write as _;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering::Relaxed};

/// Persistence bookkeeping shared by the server.
pub struct PersistState {
    /// Serializes snapshot jobs (one at a time).
    pub snapshot_lock: Mutex<()>,
    /// A BGSAVE thread is running.
    pub bgsave_in_progress: AtomicBool,
    /// Unix time of the last successful save.
    pub last_save: AtomicU64,
    /// Last BGSAVE succeeded.
    pub last_bgsave_ok: AtomicBool,
    /// Duration of the last BGSAVE (s), -1 if none.
    pub last_bgsave_secs: AtomicI64,
    /// `dirty_total()` when the last successful save began.
    pub dirty_at_save: AtomicU64,
    /// Peak `used_memory` observed.
    pub peak_memory: AtomicU64,
    /// Open AOF files (the "file lock"; always taken before shard locks).
    pub aof: Mutex<Option<aof::AofFiles>>,
    /// An AOF rewrite is running.
    pub aof_rewrite_in_progress: AtomicBool,
    /// Last AOF rewrite succeeded.
    pub aof_last_rewrite_ok: AtomicBool,
    /// Last AOF write succeeded.
    pub aof_last_write_ok: AtomicBool,
}

impl Default for PersistState {
    fn default() -> Self {
        PersistState {
            snapshot_lock: Mutex::new(()),
            bgsave_in_progress: AtomicBool::new(false),
            last_save: AtomicU64::new(crate::util::now_ms() / 1000),
            last_bgsave_ok: AtomicBool::new(true),
            last_bgsave_secs: AtomicI64::new(-1),
            dirty_at_save: AtomicU64::new(0),
            peak_memory: AtomicU64::new(0),
            aof: Mutex::new(None),
            aof_rewrite_in_progress: AtomicBool::new(false),
            aof_last_rewrite_ok: AtomicBool::new(true),
            aof_last_write_ok: AtomicBool::new(true),
        }
    }
}

/// Path of the snapshot file.
pub fn snapshot_path(server: &Server) -> PathBuf {
    server
        .config
        .dir
        .read()
        .join(&*server.config.dbfilename.read())
}

/// Writes `bytes` to `path` atomically: temp file, fsync, rename, fsync dir.
pub fn write_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let tmp = path.with_extension(format!("tmp-{}", std::process::id()));
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    if let Some(dir) = path.parent()
        && let Ok(d) = std::fs::File::open(if dir.as_os_str().is_empty() {
            Path::new(".")
        } else {
            dir
        })
    {
        let _ = d.sync_all();
    }
    Ok(())
}

/// SAVE: takes a snapshot on the calling thread and writes it to disk. The
/// snapshot is point-in-time; other clients keep running meanwhile.
pub fn save_sync(server: &Server) -> Result<(), String> {
    let _job = server.persist.snapshot_lock.lock();
    let start = std::time::Instant::now();
    let dirty = server.dirty_total();
    let (bytes, _, _) = snapshot::create(server, |_| {});
    write_atomic(&snapshot_path(server), &bytes)
        .map_err(|e| format!("Failed opening the temp file needed for saving: {e}"))?;
    server
        .persist
        .last_save
        .store(crate::util::now_ms() / 1000, Relaxed);
    server.persist.dirty_at_save.store(dirty, Relaxed);
    log(
        '*',
        &format!(
            "DB saved on disk ({} bytes, {} ms)",
            bytes.len(),
            start.elapsed().as_millis()
        ),
    );
    Ok(())
}

/// BGSAVE: like SAVE on a background thread.
pub fn bgsave(server: &Server) -> Result<(), String> {
    if server.persist.bgsave_in_progress.swap(true, Relaxed) {
        return Err("Background save already in progress".into());
    }
    let server = server.arc();
    std::thread::Builder::new()
        .name("bgsave".into())
        .spawn(move || {
            let start = std::time::Instant::now();
            log('*', "Background saving started");
            let r = save_sync(&server);
            server.persist.last_bgsave_ok.store(r.is_ok(), Relaxed);
            server
                .persist
                .last_bgsave_secs
                .store(start.elapsed().as_secs() as i64, Relaxed);
            match r {
                Ok(()) => log('*', "Background saving terminated with success"),
                Err(e) => log('#', &format!("Background saving error: {e}")),
            }
            server.persist.bgsave_in_progress.store(false, Relaxed);
        })
        .map_err(|e| e.to_string())?;
    Ok(())
}

/// Triggers a BGSAVE when a `save <seconds> <changes>` point is reached.
pub fn check_save_points(server: &Server) {
    if server.persist.bgsave_in_progress.load(Relaxed) {
        return;
    }
    let changes = server
        .dirty_total()
        .saturating_sub(server.persist.dirty_at_save.load(Relaxed));
    if changes == 0 {
        return;
    }
    let elapsed =
        (crate::util::now_ms() / 1000).saturating_sub(server.persist.last_save.load(Relaxed));
    let points = server.config.save_params.read().clone();
    for (secs, min_changes) in points {
        if changes >= min_changes && elapsed >= secs {
            log(
                '*',
                &format!("{min_changes} changes in {secs} seconds. Saving..."),
            );
            let _ = bgsave(server);
            return;
        }
    }
}

/// Loads persisted data at startup: the AOF when `appendonly yes` (creating
/// it from the snapshot if it doesn't exist yet), otherwise the snapshot.
pub fn load_on_startup(server: &Server) -> Result<(), String> {
    if server.config.appendonly.load(Relaxed) {
        if aof::load(server)? {
            return Ok(());
        }
        load_snapshot_file(server)?;
        aof::enable(server)?;
        return Ok(());
    }
    load_snapshot_file(server)
}

fn load_snapshot_file(server: &Server) -> Result<(), String> {
    let path = snapshot_path(server);
    let bytes = match std::fs::read(&path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(format!("can't read {}: {e}", path.display())),
    };
    let start = std::time::Instant::now();
    let (n, _) = snapshot::load_into(server, &bytes, true)
        .map_err(|e| format!("{}: {e}", path.display()))?;
    log(
        '*',
        &format!(
            "DB loaded from disk: {n} keys in {:.3} seconds",
            start.elapsed().as_secs_f64()
        ),
    );
    Ok(())
}

/// DEBUG RELOAD: snapshot, flush, load the snapshot back.
pub fn debug_reload(server: &Server) -> Result<(), String> {
    save_sync(server)?;
    let bytes = std::fs::read(snapshot_path(server)).map_err(|e| e.to_string())?;
    {
        let mut guards = server.keyspace.lock_all();
        for g in guards.iter_mut() {
            g.before_write();
            for db in g.dbs.iter_mut() {
                db.clear();
            }
        }
    }
    snapshot::load_into(server, &bytes, true).map(|_| ())
}

/// The `# Persistence` INFO section.
pub fn info_section(server: &Server) -> String {
    let p = &server.persist;
    let mut s = String::new();
    let changes = server
        .dirty_total()
        .saturating_sub(p.dirty_at_save.load(Relaxed));
    let _ = write!(
        s,
        "# Persistence\r\nloading:{}\r\nasync_loading:0\r\nrdb_changes_since_last_save:{}\r\nrdb_bgsave_in_progress:{}\r\nrdb_last_save_time:{}\r\nrdb_last_bgsave_status:{}\r\nrdb_last_bgsave_time_sec:{}\r\naof_enabled:{}\r\naof_rewrite_in_progress:{}\r\naof_rewrite_scheduled:0\r\naof_last_bgrewrite_status:{}\r\naof_last_write_status:{}\r\n",
        server.loading.load(Relaxed) as u8,
        changes,
        p.bgsave_in_progress.load(Relaxed) as u8,
        p.last_save.load(Relaxed),
        if p.last_bgsave_ok.load(Relaxed) {
            "ok"
        } else {
            "err"
        },
        p.last_bgsave_secs.load(Relaxed),
        server.config.appendonly.load(Relaxed) as u8,
        p.aof_rewrite_in_progress.load(Relaxed) as u8,
        if p.aof_last_rewrite_ok.load(Relaxed) {
            "ok"
        } else {
            "err"
        },
        if p.aof_last_write_ok.load(Relaxed) {
            "ok"
        } else {
            "err"
        },
    );
    if let Some(f) = p.aof.lock().as_ref() {
        let _ = write!(
            s,
            "aof_current_size:{}\r\naof_base_size:{}\r\n",
            f.current_size(),
            f.base_size()
        );
    }
    s.push_str("\r\n");
    s
}
