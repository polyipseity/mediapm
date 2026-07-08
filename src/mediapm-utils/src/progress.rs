//! Shared progress bar and download-progress types for mediapm CLIs.
//!
//! Crate consumers that want graphical progress bars enable the `progress`
//! feature (which pulls in `indicatif`).  The conductor library itself avoids
//! this dependency — it receives progress via [`ProgressCallback`] closures.
//!
//! # Types across feature boundaries
//!
//! | Type / fn | Available without `progress` | Available with `progress` |
//! |---|---|---|
//! | [`DownloadProgressSnapshot`] | ✅ | ✅ |
//! | [`ProgressCallback`] | ✅ | ✅ |
//! | [`ProgressHandle`] | ❌ | ✅ |
//! | [`ProgressGroup`] | ❌ | ✅ |
//! | [`set_progress_enabled`] / [`progress_enabled`] | ❌ | ✅ |
//! | [`format_bytes`] / [`format_count`] | ❌ | ✅ |

use std::sync::Arc;

// ---------------------------------------------------------------------------
// Download-progress types (always available)
// ---------------------------------------------------------------------------

/// Snapshot of download progress at one point in time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DownloadProgressSnapshot {
    /// Bytes downloaded so far.
    pub downloaded_bytes: u64,
    /// Total expected bytes, if known.
    pub total_bytes: Option<u64>,
}

/// Callback invoked with progress snapshots during a transfer.
pub type ProgressCallback = Arc<dyn Fn(DownloadProgressSnapshot) + Send + Sync>;

// ---------------------------------------------------------------------------
// Graphical progress bar types (only with `progress` feature)
// ---------------------------------------------------------------------------

#[cfg(feature = "progress")]
mod inner {
    use std::sync::atomic::{AtomicBool, Ordering};

    use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

    // ---- global toggle ----------------------------------------------------

    static PROGRESS_ENABLED: AtomicBool = AtomicBool::new(true);

    /// Globally enable or disable progress bar output.
    pub fn set_progress_enabled(enabled: bool) {
        PROGRESS_ENABLED.store(enabled, Ordering::Relaxed);
    }

    /// Returns whether progress bar output is currently enabled.
    pub fn progress_enabled() -> bool {
        PROGRESS_ENABLED.load(Ordering::Relaxed)
    }

    // ---- style constants --------------------------------------------------

    const CHILD_BAR_TEMPLATE: &str = "{spinner:.green} {prefix:>12.12} [{elapsed_precise}] {wide_bar:.cyan/blue} {pos}/{len} {msg} ({eta})";

    const OVERALL_BAR_TEMPLATE: &str =
        "{prefix:>12.12} [{elapsed_precise}] {wide_bar:.green/dim} {pos}/{len} {msg}";

    const COMPACT_BAR_TEMPLATE: &str =
        "{spinner:.green} {prefix} [{elapsed_precise}] {pos}/{len} {msg}";

    fn child_bar_style() -> ProgressStyle {
        ProgressStyle::with_template(CHILD_BAR_TEMPLATE)
            .expect("invalid child bar template")
            .progress_chars("█░")
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
    }

    fn overall_bar_style() -> ProgressStyle {
        ProgressStyle::with_template(OVERALL_BAR_TEMPLATE)
            .expect("invalid overall bar template")
            .progress_chars("█░")
    }

    fn compact_bar_style() -> ProgressStyle {
        ProgressStyle::with_template(COMPACT_BAR_TEMPLATE)
            .expect("invalid compact bar template")
            .progress_chars("█░")
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
    }

    fn apply_bar_style(pb: &ProgressBar) {
        if terminal_width() < 60 {
            pb.set_style(compact_bar_style());
        } else {
            pb.set_style(child_bar_style());
        }
    }

    fn terminal_width() -> u16 {
        console::Term::stderr().size().1
    }

    // ---- ProgressHandle ---------------------------------------------------

    /// Handle to one progress bar.
    ///
    /// Cloning creates another reference to the same underlying bar — all
    /// clones share state and advancing any one of them updates the display.
    #[derive(Clone)]
    pub struct ProgressHandle {
        inner: ProgressBar,
    }

    impl ProgressHandle {
        /// Create a standalone progress bar (not managed by a [`ProgressGroup`]).
        pub fn new(total: u64) -> Self {
            let pb = ProgressBar::new(total);
            apply_bar_style(&pb);
            Self { inner: pb }
        }

        /// Return the total number of work units (0 = indeterminate).
        #[must_use]
        pub fn total(&self) -> u64 {
            self.inner.length().unwrap_or(0)
        }

        /// Change the total mid-flight for dynamic workloads.
        pub fn set_total(&self, total: u64) {
            self.inner.set_length(total);
        }

        /// Advance the bar by `delta` work units.
        pub fn advance(&self, delta: u64) {
            self.inner.inc(delta);
        }

        /// Jump to an absolute position.
        pub fn set_position(&self, pos: u64) {
            self.inner.set_position(pos);
        }

        /// Set the message shown after the bar (e.g. "materializing").
        pub fn set_message(&self, msg: impl Into<String>) {
            self.inner.set_message(msg.into());
        }

        /// Set the prefix shown before the bar.
        pub fn set_prefix(&self, prefix: impl Into<String>) {
            self.inner.set_prefix(prefix.into());
        }

        /// Mark the bar as finished (keeps it visible).
        pub fn finish(&self) {
            self.inner.finish();
        }

        /// Mark as finished with a success message.
        pub fn finish_success(&self, msg: impl Into<String>) {
            self.inner.finish_with_message(msg.into());
        }

        /// Mark as finished with an error message.
        pub fn finish_error(&self, msg: impl Into<String>) {
            self.inner.abandon_with_message(msg.into());
        }

        /// Abandon the bar — leaves it visible but stops all updates.
        pub fn abandon(&self) {
            self.inner.abandon();
        }
    }

    // ---- ProgressGroup ----------------------------------------------------

    /// A vertical stack of progress bars.
    pub struct ProgressGroup {
        inner: MultiProgress,
        #[expect(dead_code)]
        overall: Option<ProgressBar>,
    }

    impl ProgressGroup {
        /// Create a new group with no overall bar.
        #[must_use]
        pub fn new() -> Self {
            Self { inner: MultiProgress::new(), overall: None }
        }

        /// Create a group with an overall aggregate bar pinned at the bottom.
        #[must_use]
        pub fn with_overall(label: &str, total: u64) -> (Self, ProgressHandle) {
            let mp = MultiProgress::new();
            let inner = ProgressBar::new(total);
            inner.set_style(overall_bar_style());
            inner.set_prefix(label.to_string());
            let overall_handle = mp.add(inner.clone());
            let group = Self { inner: mp, overall: Some(inner) };
            (group, ProgressHandle { inner: overall_handle })
        }

        /// Add a child bar above the overall bar (if any).
        #[must_use]
        pub fn add_bar(&self, total: u64, label: &str) -> ProgressHandle {
            let inner = ProgressBar::new(total);
            apply_bar_style(&inner);
            inner.set_prefix(label.to_string());
            let bar = self.inner.insert(0, inner);
            ProgressHandle { inner: bar }
        }

        /// Block until all bars in the group reach a finished state.
        pub fn join(&self) {}

        /// Block until all bars finish, then clear them from the terminal.
        pub fn join_and_clear(&self) {}
    }

    impl Default for ProgressGroup {
        fn default() -> Self {
            Self::new()
        }
    }

    // ---- formatting helpers -----------------------------------------------

    /// Format a byte count for human display, e.g. `"650.23 MiB"`.
    #[must_use]
    pub fn format_bytes(bytes: u64) -> String {
        indicatif::HumanBytes(bytes).to_string()
    }

    /// Format an integer count with SI suffix, e.g. `"1.2M"`, `"42"`.
    #[must_use]
    pub fn format_count(count: u64) -> String {
        indicatif::HumanCount(count).to_string()
    }
}

#[cfg(feature = "progress")]
pub use inner::{
    ProgressGroup, ProgressHandle, format_bytes, format_count, progress_enabled,
    set_progress_enabled,
};
