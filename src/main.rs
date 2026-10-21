//! `redis-clone` server binary.
//!
//! ```text
//! redis-clone [/path/to/redis.conf] [--port 6379] [--appendonly yes] [--save ""] ...
//! ```

use redis_clone::config::Config;
use redis_clone::server;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

#[global_allocator]
static GLOBAL: redis_clone::alloc::CountingAlloc = redis_clone::alloc::CountingAlloc;

fn main() {
    redis_clone::alloc::mark_installed();
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    if args.first().is_some_and(|a| a == "-v" || a == "--version") {
        println!(
            "redis-clone v={} (Redis protocol compatible: {})",
            env!("CARGO_PKG_VERSION"),
            server::VERSION
        );
        return;
    }
    if args.first().is_some_and(|a| a == "-h" || a == "--help") {
        println!("Usage: redis-clone [/path/to/redis.conf] [--option value ...]");
        println!("Examples: redis-clone --port 7000 --appendonly yes --save \"\" --threads 4");
        return;
    }
    let config = Config::default();
    if args.first().is_some_and(|a| !a.starts_with("--")) {
        let path = args.remove(0);
        if let Err(e) = config.load_file(&path) {
            eprintln!("FATAL CONFIG FILE ERROR: {e}");
            std::process::exit(1);
        }
    }
    if let Err(e) = config.apply_args(&args) {
        eprintln!("FATAL CONFIG ERROR: {e}");
        std::process::exit(1);
    }
    let handle = match server::start(config) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("Failed to start server: {e}");
            std::process::exit(1);
        }
    };
    // SIGTERM/SIGINT: graceful shutdown (final save if configured).
    let term = Arc::new(AtomicBool::new(false));
    for sig in [signal_hook::consts::SIGTERM, signal_hook::consts::SIGINT] {
        let _ = signal_hook::flag::register(sig, term.clone());
    }
    let srv = handle.server.clone();
    loop {
        std::thread::sleep(std::time::Duration::from_millis(50));
        if srv.shutdown.load(Ordering::Relaxed) {
            break;
        }
        if term.load(Ordering::Relaxed) {
            server::log('#', "Received SIGTERM scheduling shutdown...");
            if srv.shutdown(None).is_err() {
                // Save failed: still exit on a signal, like Redis does with
                // SHUTDOWN NOSAVE semantics after a failed save on SIGTERM.
                let _ = srv.shutdown(Some(false));
            }
            break;
        }
    }
    // Give workers a moment to flush final replies, then exit.
    std::thread::sleep(std::time::Duration::from_millis(50));
    std::process::exit(0);
}
