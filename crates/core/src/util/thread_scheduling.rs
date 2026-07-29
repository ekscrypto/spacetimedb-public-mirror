use core_affinity::CoreId;

/// Niceness for public-mirror CPU-heavy background threads (DB apply, seed decode).
///
/// Tokio async workers stay at the process default (typically 0) so WebSocket
/// I/O wins when cores are saturated.
pub const MIRROR_BACKGROUND_NICENESS: i32 = 5;

/// Apply the current platform's preferred scheduler hint for compute-heavy worker threads.
///
/// On Linux and other non-macOS platforms, this uses CPU affinity when a core is provided.
/// On macOS, scheduler hints are intentionally disabled.
pub(crate) fn apply_compute_thread_hint(core_id: Option<CoreId>) {
    #[cfg(target_os = "macos")]
    {
        let _ = core_id;
    }

    #[cfg(not(target_os = "macos"))]
    if let Some(core_id) = core_id {
        core_affinity::set_for_current(core_id);
    }
}

/// Lower the scheduling priority of the calling thread (once per thread, Linux only).
///
/// Used for public-mirror database worker threads and offloaded seed decode so
/// I/O-bound Tokio tasks keep responsive under CPU contention.
pub fn deprioritize_mirror_background_thread() {
    #[cfg(target_os = "linux")]
    {
        use std::cell::Cell;
        thread_local! {
            static DEPRIORITIZED: Cell<bool> = const { Cell::new(false) };
        }
        DEPRIORITIZED.with(|d| {
            if !d.replace(true) {
                // SAFETY: setpriority with PRIO_PROCESS and who=0 adjusts the
                // calling thread's niceness on Linux; no memory is touched.
                // (`as _` bridges the glibc/musl disagreement on `which`'s type.)
                let rc = unsafe {
                    nix::libc::setpriority(
                        nix::libc::PRIO_PROCESS as _,
                        0,
                        MIRROR_BACKGROUND_NICENESS,
                    )
                };
                if rc != 0 {
                    log::debug!(
                        "public-mirror: setpriority({MIRROR_BACKGROUND_NICENESS}) failed: {}",
                        std::io::Error::last_os_error()
                    );
                }
            }
        });
    }
}
