//! Append-only file, in Redis 7's multi-part layout.
//!
//! ```text
//! <dir>/appendonlydir/
//!     appendonly.aof.manifest          which files make up the AOF, in order
//!     appendonly.aof.3.base.rcsnap     snapshot of the dataset at rewrite time
//!     appendonly.aof.3.incr.aof        RESP commands written since then
//! ```
//!
//! **Write path.** Commands are appended to an in-memory buffer while their
//! shard locks are held (see [`crate::propagate`]). Before a worker sends the
//! replies of a batch that modified data, it calls [`flush`], which writes the
//! buffer with one `write()` (group commit across threads) and, with
//! `appendfsync always`, `fsync`s before any reply leaves. With `everysec`,
//! the cron thread fsyncs once per second; with `no`, the OS decides.
//!
//! **Rewrite.** `BGREWRITEAOF` opens a new incr file *at the snapshot
//! instant* (under all shard locks), so every later command lands in the new
//! file; it then writes the new base snapshot in the background and finally
//! swaps the manifest atomically (temp file + rename) and deletes the old
//! files. No rewrite buffer is needed and a crash at any point leaves a valid
//! manifest.
//!
//! **Crash safety.** On load, a truncated tail (a command cut short by a
//! crash or power loss, or a `MULTI` without its `EXEC`) is detected; with
//! `aof-load-truncated yes` (the default) the file is truncated back to the
//! last complete command and loading continues, otherwise startup fails.
//! A malformed command anywhere else is always a fatal error.

use super::{snapshot, write_atomic};
use crate::propagate::AofBuf;
use crate::resp::{Args, Parser};
use crate::server::{Server, log};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::Ordering::Relaxed;

#[derive(Clone, Debug, Default)]
struct Manifest {
    base: Option<(String, u64)>,
    incrs: Vec<(String, u64)>,
}

impl Manifest {
    fn parse(text: &str) -> Result<Manifest, String> {
        let mut m = Manifest::default();
        for line in text
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
        {
            let parts: Vec<&str> = line.split_whitespace().collect();
            let get = |k: &str| {
                parts
                    .iter()
                    .position(|p| *p == k)
                    .and_then(|i| parts.get(i + 1))
                    .copied()
            };
            let (Some(name), Some(seq), Some(t)) = (get("file"), get("seq"), get("type")) else {
                return Err(format!("invalid AOF manifest line: {line}"));
            };
            let seq: u64 = seq
                .parse()
                .map_err(|_| format!("invalid seq in manifest: {line}"))?;
            match t {
                "b" => m.base = Some((name.to_string(), seq)),
                "i" => m.incrs.push((name.to_string(), seq)),
                _ => return Err(format!("invalid file type in manifest: {line}")),
            }
        }
        Ok(m)
    }

    fn render(&self) -> String {
        let mut s = String::new();
        if let Some((n, seq)) = &self.base {
            s.push_str(&format!("file {n} seq {seq} type b\n"));
        }
        for (n, seq) in &self.incrs {
            s.push_str(&format!("file {n} seq {seq} type i\n"));
        }
        s
    }

    fn next_seq(&self) -> u64 {
        let b = self.base.as_ref().map_or(0, |(_, s)| *s);
        let i = self.incrs.iter().map(|(_, s)| *s).max().unwrap_or(0);
        b.max(i) + 1
    }
}

/// Open AOF state (guarded by `PersistState::aof`).
pub struct AofFiles {
    dir: PathBuf,
    name: String,
    manifest: Manifest,
    file: File,
    incr_size: u64,
    base_size: u64,
}

impl AofFiles {
    /// Bytes in the current incr file.
    pub fn current_size(&self) -> u64 {
        self.incr_size + self.base_size
    }
    /// Bytes in the base file.
    pub fn base_size(&self) -> u64 {
        self.base_size
    }
    fn manifest_path(&self) -> PathBuf {
        self.dir.join(format!("{}.manifest", self.name))
    }
    fn write_manifest(&self) -> std::io::Result<()> {
        write_atomic(&self.manifest_path(), self.manifest.render().as_bytes())
    }
}

fn aof_dir(server: &Server) -> PathBuf {
    server
        .config
        .dir
        .read()
        .join(&*server.config.appenddirname.read())
}

fn base_name(name: &str, seq: u64) -> String {
    format!("{name}.{seq}.base.rcsnap")
}

fn incr_name(name: &str, seq: u64) -> String {
    format!("{name}.{seq}.incr.aof")
}

fn open_append(path: &PathBuf) -> std::io::Result<File> {
    OpenOptions::new().create(true).append(true).open(path)
}

/// Writes buffered AOF data; with `sync`, also fsyncs. Safe to call from any
/// thread that holds no shard locks.
pub fn flush(server: &Server, sync: bool) {
    let mut files = server.persist.aof.lock();
    let Some(f) = files.as_mut() else { return };
    let data = {
        let mut p = server.prop.lock();
        match p.aof.as_mut() {
            Some(a) => std::mem::take(&mut a.buf),
            None => return,
        }
    };
    if !data.is_empty() {
        match f.file.write_all(&data) {
            Ok(()) => {
                f.incr_size += data.len() as u64;
                server.persist.aof_last_write_ok.store(true, Relaxed);
            }
            Err(e) => {
                server.persist.aof_last_write_ok.store(false, Relaxed);
                log('#', &format!("Error writing to the AOF file: {e}"));
            }
        }
    }
    if sync && let Err(e) = f.file.sync_data() {
        log('#', &format!("Error fsyncing the AOF file: {e}"));
    }
}

/// Creates (or re-creates) the AOF from the current dataset and starts
/// logging to it. Used at startup when no AOF exists and by
/// `CONFIG SET appendonly yes`.
pub fn enable(server: &Server) -> Result<(), String> {
    rewrite(server, true)
}

/// Stops AOF logging (`CONFIG SET appendonly no`).
pub fn disable(server: &Server) {
    flush(server, true);
    let mut files = server.persist.aof.lock();
    let mut p = server.prop.lock();
    p.aof = None;
    server.prop.refresh_active(&p);
    *files = None;
}

/// BGREWRITEAOF.
pub fn bgrewrite(server: &Server) -> Result<(), String> {
    if !server.config.appendonly.load(Relaxed) {
        return Err("Background AOF rewriting requires appendonly yes".into());
    }
    if server.persist.aof_rewrite_in_progress.swap(true, Relaxed) {
        return Err("Background append only file rewriting already in progress".into());
    }
    let server = server.arc();
    std::thread::Builder::new()
        .name("aof-rewrite".into())
        .spawn(move || {
            let r = rewrite(&server, false);
            server.persist.aof_last_rewrite_ok.store(r.is_ok(), Relaxed);
            if let Err(e) = r {
                log('#', &format!("Background AOF rewrite failed: {e}"));
            }
            server.persist.aof_rewrite_in_progress.store(false, Relaxed);
        })
        .map_err(|e| e.to_string())?;
    Ok(())
}

/// Rewrites the AOF: new base snapshot + fresh incr file. With `create`,
/// starts AOF logging if it was off.
pub fn rewrite(server: &Server, create: bool) -> Result<(), String> {
    let _one_snapshot = server.persist.snapshot_lock.lock();
    let dir = aof_dir(server);
    std::fs::create_dir_all(&dir).map_err(|e| format!("can't create {}: {e}", dir.display()))?;
    let name = server.config.appendfilename.read().clone();

    // Step 1 (under the file lock): pick sequence numbers, open the new incr
    // file and switch logging to it at the snapshot instant.
    let mut files = server.persist.aof.lock();
    let mut manifest = match files.as_ref() {
        Some(f) => f.manifest.clone(),
        None => {
            let p = dir.join(format!("{name}.manifest"));
            match std::fs::read_to_string(&p) {
                Ok(t) => Manifest::parse(&t)?,
                Err(_) => Manifest::default(),
            }
        }
    };
    let seq = manifest.next_seq();
    let new_incr = incr_name(&name, seq);
    let new_file =
        open_append(&dir.join(&new_incr)).map_err(|e| format!("can't open AOF incr file: {e}"))?;
    let mut old_file = files
        .as_mut()
        .map(|f| f.file.try_clone())
        .transpose()
        .map_err(|e| e.to_string())?;
    let mut switch_err = None;
    let (job, replid, offset) = snapshot::begin(server, |p| {
        match p.aof.as_mut() {
            Some(a) => {
                // Everything before the snapshot instant goes to the old file.
                let pending = std::mem::take(&mut a.buf);
                if let Some(f) = old_file.as_mut()
                    && let Err(e) = f.write_all(&pending)
                {
                    switch_err = Some(e.to_string());
                }
                a.selected_db = None;
            }
            None if create => p.aof = Some(AofBuf::default()),
            None => {}
        }
    });
    if let Some(e) = switch_err {
        log('#', &format!("AOF write failed during rewrite switch: {e}"));
    }
    manifest.incrs.push((new_incr.clone(), seq));
    let state = AofFiles {
        dir: dir.clone(),
        name: name.clone(),
        manifest: manifest.clone(),
        file: new_file,
        incr_size: 0,
        base_size: files.as_ref().map_or(0, |f| f.base_size),
    };
    // Manifest now lists old base + old incrs + the new incr: still valid if
    // we crash before the new base is written.
    state
        .write_manifest()
        .map_err(|e| format!("can't write AOF manifest: {e}"))?;
    *files = Some(state);
    drop(files);

    // Step 2 (no locks): serialize the base snapshot.
    let bytes = snapshot::finish(server, &job, &replid, offset);
    let new_base = base_name(&name, seq);
    write_atomic(&dir.join(&new_base), &bytes).map_err(|e| format!("can't write AOF base: {e}"))?;

    // Step 3: swap the manifest to new base + new incr, drop old files.
    let mut files = server.persist.aof.lock();
    let Some(f) = files.as_mut() else {
        return Err("AOF disabled during rewrite".into());
    };
    let old = f.manifest.clone();
    f.manifest = Manifest {
        base: Some((new_base.clone(), seq)),
        incrs: vec![(new_incr.clone(), seq)],
    };
    f.base_size = bytes.len() as u64;
    f.write_manifest()
        .map_err(|e| format!("can't write AOF manifest: {e}"))?;
    drop(files);
    if let Some((b, _)) = old.base
        && b != new_base
    {
        let _ = std::fs::remove_file(dir.join(b));
    }
    for (i, _) in old.incrs {
        if i != new_incr {
            let _ = std::fs::remove_file(dir.join(i));
        }
    }
    log(
        '*',
        &format!(
            "Background AOF rewrite finished successfully (base {} bytes)",
            bytes.len()
        ),
    );
    Ok(())
}

/// Loads the AOF if a manifest exists. Returns `Ok(false)` when there is no
/// AOF yet.
pub fn load(server: &Server) -> Result<bool, String> {
    let dir = aof_dir(server);
    let name = server.config.appendfilename.read().clone();
    let mpath = dir.join(format!("{name}.manifest"));
    let text = match std::fs::read_to_string(&mpath) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(format!("can't read {}: {e}", mpath.display())),
    };
    let manifest = Manifest::parse(&text)?;
    let start = std::time::Instant::now();
    let mut base_size = 0;
    if let Some((b, _)) = &manifest.base {
        let bytes =
            std::fs::read(dir.join(b)).map_err(|e| format!("can't read AOF base {b}: {e}"))?;
        base_size = bytes.len() as u64;
        let (n, _) =
            snapshot::load_into(server, &bytes, true).map_err(|e| format!("AOF base {b}: {e}"))?;
        log('*', &format!("Loaded AOF base {b}: {n} keys"));
    }
    let truncated_ok = server.config.aof_load_truncated.load(Relaxed);
    let mut client = server.internal_client();
    let mut commands = 0usize;
    let count = manifest.incrs.len();
    for (i, (inc, _)) in manifest.incrs.iter().enumerate() {
        let path = dir.join(inc);
        let bytes = match std::fs::read(&path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound && i + 1 == count => Vec::new(),
            Err(e) => return Err(format!("can't read AOF file {inc}: {e}")),
        };
        let last = i + 1 == count;
        let good = replay(server, &mut client, &bytes, &mut commands)
            .map_err(|e| format!("{inc}: {e}"))?;
        if good < bytes.len() {
            if !last || !truncated_ok {
                return Err(format!(
                    "Unexpected end of file reading the append only file {inc} at offset {good}. You can: 1) Make a backup of your AOF file, then use ./redis-check-aof --fix <filename.manifest>. 2) Alternatively you can set the 'aof-load-truncated' configuration option to yes and restart the server."
                ));
            }
            log(
                '#',
                &format!(
                    "!!! Warning: short read while loading the AOF file {inc}!!! AOF loaded anyway because aof-load-truncated is enabled; truncating {} bytes of trailing garbage at offset {good}",
                    bytes.len() - good
                ),
            );
            let f = OpenOptions::new()
                .write(true)
                .open(&path)
                .map_err(|e| e.to_string())?;
            f.set_len(good as u64)
                .map_err(|e| format!("can't truncate AOF: {e}"))?;
            f.sync_all().map_err(|e| e.to_string())?;
        }
    }
    let (cur_name, _) = manifest
        .incrs
        .last()
        .cloned()
        .unwrap_or_else(|| (incr_name(&name, manifest.next_seq()), manifest.next_seq()));
    let path = dir.join(&cur_name);
    let file = open_append(&path).map_err(|e| format!("can't open {}: {e}", path.display()))?;
    let incr_size = file.metadata().map(|m| m.len()).unwrap_or(0);
    let mut manifest = manifest;
    if manifest.incrs.is_empty() {
        manifest.incrs.push((cur_name.clone(), manifest.next_seq()));
    }
    let state = AofFiles {
        dir,
        name,
        manifest,
        file,
        incr_size,
        base_size,
    };
    state.write_manifest().map_err(|e| e.to_string())?;
    *server.persist.aof.lock() = Some(state);
    {
        let mut p = server.prop.lock();
        p.aof = Some(AofBuf::default());
        server.prop.refresh_active(&p);
    }
    log(
        '*',
        &format!(
            "DB loaded from append only file: {commands} commands in {:.3} seconds",
            start.elapsed().as_secs_f64()
        ),
    );
    Ok(true)
}

/// Replays RESP commands; returns the offset just past the last complete
/// command (and past any complete MULTI/EXEC block).
fn replay(
    server: &Server,
    client: &mut crate::server::client::ClientState,
    bytes: &[u8],
    commands: &mut usize,
) -> Result<usize, String> {
    let mut parser = Parser::new();
    let mut pos = 0;
    let mut good = 0;
    let mut in_multi_since: Option<usize> = None;
    let stats = server.background_stats();
    let now = crate::util::now_ms();
    loop {
        let start = pos;
        let parsed = parser.parse(bytes, &mut pos).map_err(|e| {
            format!(
                "Bad file format reading the append only file (offset {start}): {}",
                e.0
            )
        })?;
        let Some(args) = parsed else { break };
        let owned: Vec<Vec<u8>> = match args {
            Args::Ranges(r) => r.iter().map(|&(s, l)| bytes[s..s + l].to_vec()).collect(),
            Args::Owned(o) => o,
        };
        if owned.is_empty() {
            good = pos;
            continue;
        }
        if bytes.get(start) != Some(&b'*') {
            return Err(format!(
                "Bad file format reading the append only file (offset {start})"
            ));
        }
        let refs: Vec<&[u8]> = owned.iter().map(|a| &a[..]).collect();
        if refs[0].eq_ignore_ascii_case(b"multi") {
            in_multi_since = Some(start);
        }
        crate::cmd::execute_internal(server, client, stats, &refs, now);
        *commands += 1;
        if refs[0].eq_ignore_ascii_case(b"exec") {
            in_multi_since = None;
        }
        if in_multi_since.is_none() {
            good = pos;
        }
    }
    if let Some(m) = in_multi_since {
        // A transaction without its EXEC: discard it (it never committed).
        client.multi = None;
        return Ok(m);
    }
    Ok(good)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_roundtrip() {
        let m = Manifest {
            base: Some(("a.1.base.rcsnap".into(), 1)),
            incrs: vec![("a.1.incr.aof".into(), 1), ("a.2.incr.aof".into(), 2)],
        };
        let p = Manifest::parse(&m.render()).unwrap();
        assert_eq!(p.base, m.base);
        assert_eq!(p.incrs, m.incrs);
        assert_eq!(p.next_seq(), 3);
        assert!(Manifest::parse("garbage line").is_err());
    }
}
