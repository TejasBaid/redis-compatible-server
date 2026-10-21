//! A counting global allocator that powers `used_memory` and `maxmemory`.
//!
//! Every allocation adjusts a thread-local counter; counters are folded into a
//! global atomic once they drift by more than [`FLUSH_THRESHOLD`] bytes. Reads
//! of [`used_memory`] are a single atomic load, and the hot allocation path
//! never touches a shared cache line, so accounting does not hurt multi-core
//! scaling. The global value is exact up to `threads × FLUSH_THRESHOLD`.
//!
//! The binary installs it with `#[global_allocator]`; library users (and unit
//! tests) that don't will simply see `used_memory() == 0`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicIsize, Ordering};

/// Per-thread drift tolerated before publishing to the global counter.
pub const FLUSH_THRESHOLD: isize = 64 * 1024;

static USED: AtomicIsize = AtomicIsize::new(0);
static INSTALLED: AtomicBool = AtomicBool::new(false);

thread_local! {
    static LOCAL: Cell<isize> = const { Cell::new(0) };
}

#[inline]
fn account(delta: isize) {
    let flushed = LOCAL.try_with(|c| {
        let v = c.get() + delta;
        if !(-FLUSH_THRESHOLD..=FLUSH_THRESHOLD).contains(&v) {
            USED.fetch_add(v, Ordering::Relaxed);
            c.set(0);
        } else {
            c.set(v);
        }
    });
    if flushed.is_err() {
        USED.fetch_add(delta, Ordering::Relaxed);
    }
}

/// The counting allocator. Wraps the system allocator.
pub struct CountingAlloc;

// SAFETY: every method forwards to `System` with the caller's arguments and
// only adds bookkeeping that never touches the returned memory.
unsafe impl GlobalAlloc for CountingAlloc {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded verbatim; the caller upholds `alloc`'s contract.
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            account(layout.size() as isize);
        }
        p
    }

    #[inline]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded verbatim.
        let p = unsafe { System.alloc_zeroed(layout) };
        if !p.is_null() {
            account(layout.size() as isize);
        }
        p
    }

    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `ptr` was allocated by `System` with `layout` (we only ever
        // hand out `System` pointers).
        unsafe { System.dealloc(ptr, layout) };
        account(-(layout.size() as isize));
    }

    #[inline]
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: forwarded verbatim; `ptr`/`layout` came from this allocator.
        let p = unsafe { System.realloc(ptr, layout, new_size) };
        if !p.is_null() {
            account(new_size as isize - layout.size() as isize);
        }
        p
    }
}

/// Marks the counting allocator as installed (called by the binary).
pub fn mark_installed() {
    INSTALLED.store(true, Ordering::Relaxed);
}

/// True when the binary installed [`CountingAlloc`].
pub fn is_installed() -> bool {
    INSTALLED.load(Ordering::Relaxed)
}

/// Bytes currently allocated through the counting allocator.
pub fn used_memory() -> u64 {
    USED.load(Ordering::Relaxed).max(0) as u64
}

/// Resident set size of the process, in bytes (best effort).
pub fn rss_bytes() -> u64 {
    #[cfg(target_os = "macos")]
    {
        // SAFETY: `task_info` writes at most `count` integers into `info`,
        // which is a properly sized, zero-initialized struct.
        unsafe {
            let mut info: libc::mach_task_basic_info = std::mem::zeroed();
            let mut count = (std::mem::size_of::<libc::mach_task_basic_info>()
                / std::mem::size_of::<libc::natural_t>())
                as libc::mach_msg_type_number_t;
            #[allow(deprecated)]
            let task = libc::mach_task_self();
            let kr = libc::task_info(
                task,
                libc::MACH_TASK_BASIC_INFO,
                &mut info as *mut _ as libc::task_info_t,
                &mut count,
            );
            if kr == libc::KERN_SUCCESS {
                return info.resident_size;
            }
        }
        0
    }
    #[cfg(target_os = "linux")]
    {
        let statm = std::fs::read_to_string("/proc/self/statm").unwrap_or_default();
        let pages: u64 = statm
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        // SAFETY: sysconf has no preconditions.
        let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as u64;
        pages * page
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        0
    }
}

/// Publishes this thread's pending accounting delta immediately (used by the
/// eviction loop so it sees the memory it just freed).
pub fn flush_local() {
    let _ = LOCAL.try_with(|c| {
        let v = c.replace(0);
        if v != 0 {
            USED.fetch_add(v, Ordering::Relaxed);
        }
    });
}
