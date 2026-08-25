//! RAII buffer guard, dimension/time sources, and debug-env detection.

use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use super::ProgressDebugSink;

// ---- RAII buffer guard -----------------------------------------------

/// RAII guard that temporarily disables buffering and restores it on drop.
///
/// On creation, stores `false` (buffer OFF — next draw goes to terminal).
/// On drop, stores `true` (buffer ON — subsequent writes suppressed).
/// When `flag` is `None` (test mode with user-provided `MultiProgress`),
/// both operations are no-ops.
#[derive(Debug)]
pub(crate) struct BufferGuard {
    flag: Option<Arc<AtomicBool>>,
}

impl BufferGuard {
    pub(crate) fn new(flag: Option<&Arc<AtomicBool>>) -> Self {
        if let Some(flag) = flag {
            flag.store(false, Ordering::Release);
        }
        Self { flag: flag.cloned() }
    }
}

impl Drop for BufferGuard {
    fn drop(&mut self) {
        if let Some(ref flag) = self.flag {
            flag.store(true, Ordering::Release);
        }
    }
}

// ---- dimension source (injectable for tests) -------------------------

/// Source of terminal dimensions for responsive progress rendering.
pub trait DimensionSource: Send + Sync {
    /// Returns `(rows, columns)` — the current terminal dimensions.
    fn dimensions(&self) -> (u16, u16);
}

/// Real terminal dimensions via [`console::Term::stderr`].
pub struct RealTerminalSource;

impl DimensionSource for RealTerminalSource {
    fn dimensions(&self) -> (u16, u16) {
        console::Term::stderr().size()
    }
}

/// Injectable dimensions for testing.
///
/// Use [`set`](TestDimensionSource::set) to change dimensions mid-test
/// so resize reactivity can be exercised without a real terminal.
#[allow(dead_code)]
pub struct TestDimensionSource {
    dims: Mutex<(u16, u16)>,
}

#[allow(dead_code)]
impl TestDimensionSource {
    /// Create a source with the given initial dimensions.
    #[must_use]
    pub fn new(dims: (u16, u16)) -> Self {
        Self { dims: Mutex::new(dims) }
    }

    /// Override the dimensions returned by [`DimensionSource::dimensions`].
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    pub fn set(&self, dims: (u16, u16)) {
        *self.dims.lock().unwrap() = dims;
    }
}

impl DimensionSource for TestDimensionSource {
    /// Returns the current dimensions.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    fn dimensions(&self) -> (u16, u16) {
        *self.dims.lock().unwrap()
    }
}

// ---- time source (injectable for tests) ------------------------------

/// Injectable time source for testing.
pub trait TimeSource: Send + Sync {
    /// Returns the current instant.
    fn now(&self) -> Instant;
}

/// Real time via [`Instant::now`].
pub struct RealTimeSource;

impl TimeSource for RealTimeSource {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// Injectable time for testing.
///
/// Use [`advance`](TestTimeSource::advance) to move time forward
/// synthetically without real wall-clock delay.
#[allow(dead_code)]
pub struct TestTimeSource {
    now: Mutex<Instant>,
}

impl Default for TestTimeSource {
    fn default() -> Self {
        Self { now: Mutex::new(Instant::now()) }
    }
}

#[allow(dead_code)]
impl TestTimeSource {
    /// Create a source initialized to [`Instant::now`].
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Advance the synthetic clock by `dur`.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    pub fn advance(&self, dur: Duration) {
        *self.now.lock().unwrap() += dur;
    }

    /// Override the instant returned by [`TimeSource::now`].
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    #[allow(dead_code)]
    pub fn set(&self, instant: Instant) {
        *self.now.lock().unwrap() = instant;
    }
}

impl TimeSource for TestTimeSource {
    /// Returns the current synthetic instant.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    fn now(&self) -> Instant {
        *self.now.lock().unwrap()
    }
}

pub(crate) fn detect_progress_debug_env() -> Option<ProgressDebugSink> {
    let val = std::env::var("MEDIAPM_PROGRESS_DEBUG").ok()?;
    let writer: Box<dyn Write + Send> = if val == "auto" || val.is_empty() {
        let path = std::path::PathBuf::from(format!("progress-debug-{}.jsonl", std::process::id()));
        Box::new(std::fs::File::create(&path).expect("failed to create progress debug file"))
    } else {
        let path = std::path::PathBuf::from(&val);
        Box::new(std::fs::File::create(&path).expect("failed to create progress debug file"))
    };
    Some(ProgressDebugSink::new(writer))
}
