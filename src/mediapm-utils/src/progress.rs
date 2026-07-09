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
//! | [`recording::RecordingProgressGroup`] | ❌ | ✅ |
//! | [`recording::RecordingProgressHandle`] | ❌ | ✅ |
//! | [`recording::ProgressOp`] | ❌ | ✅ |

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

    use std::time::Duration;

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

    const COMPACT_OVERALL_BAR_TEMPLATE: &str = "{prefix} [{elapsed_precise}] {pos}/{len} {msg}";

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

    fn compact_overall_bar_style() -> ProgressStyle {
        ProgressStyle::with_template(COMPACT_OVERALL_BAR_TEMPLATE)
            .expect("invalid compact overall bar template")
    }

    fn apply_overall_bar_style(pb: &ProgressBar) {
        if terminal_width() < 60 {
            pb.set_style(compact_overall_bar_style());
        } else {
            pb.set_style(overall_bar_style());
        }
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
    ///
    /// When progress is globally disabled (see [`set_progress_enabled`]), the
    /// handle is a no-op — all methods are zero-cost and do nothing.
    #[derive(Clone)]
    pub struct ProgressHandle {
        inner: Option<ProgressBar>,
    }

    impl ProgressHandle {
        /// Create a no-op handle (all methods are zero-cost).
        #[must_use]
        pub fn disabled() -> Self {
            Self { inner: None }
        }

        /// Create a standalone progress bar (not managed by a [`ProgressGroup`]).
        pub fn new(total: u64) -> Self {
            if !progress_enabled() {
                return Self::disabled();
            }
            let pb = ProgressBar::new(total);
            apply_bar_style(&pb);
            pb.enable_steady_tick(Duration::from_millis(100));
            Self { inner: Some(pb) }
        }

        /// Return the total number of work units (0 = indeterminate).
        #[must_use]
        pub fn total(&self) -> u64 {
            self.inner.as_ref().and_then(|pb| pb.length()).unwrap_or(0)
        }

        /// Change the total mid-flight for dynamic workloads.
        pub fn set_total(&self, total: u64) {
            if let Some(ref inner) = self.inner {
                inner.set_length(total);
            }
        }

        /// Advance the bar by `delta` work units.
        pub fn advance(&self, delta: u64) {
            if let Some(ref inner) = self.inner {
                inner.inc(delta);
            }
        }

        /// Jump to an absolute position.
        pub fn set_position(&self, pos: u64) {
            if let Some(ref inner) = self.inner {
                inner.set_position(pos);
            }
        }

        /// Set the message shown after the bar (e.g. "materializing").
        pub fn set_message(&self, msg: impl Into<String>) {
            if let Some(ref inner) = self.inner {
                inner.set_message(msg.into());
            }
        }

        /// Set the prefix shown before the bar.
        pub fn set_prefix(&self, prefix: impl Into<String>) {
            if let Some(ref inner) = self.inner {
                inner.set_prefix(prefix.into());
            }
        }

        /// Mark the bar as finished (keeps it visible).
        pub fn finish(&self) {
            if let Some(ref inner) = self.inner {
                inner.finish();
            }
        }

        /// Mark as finished with a success message (keeps it visible).
        pub fn finish_success(&self, msg: impl Into<String>) {
            if let Some(ref inner) = self.inner {
                inner.finish_with_message(msg.into());
            }
        }

        /// Mark as finished with an error message (keeps it visible).
        pub fn finish_error(&self, msg: impl Into<String>) {
            if let Some(ref inner) = self.inner {
                inner.abandon_with_message(msg.into());
            }
        }

        /// Finish and clear the bar from the display.
        ///
        /// Stops the ticker and marks the bar as hidden. Call this instead of
        /// [`finish`](Self::finish) when the bar should disappear immediately.
        pub fn finish_and_clear(&self) {
            if let Some(ref inner) = self.inner {
                inner.finish_and_clear();
            }
        }

        /// Abandon the bar — leaves it visible but stops all updates.
        pub fn abandon(&self) {
            if let Some(ref inner) = self.inner {
                inner.abandon();
            }
        }
    }

    // ---- ProgressGroup ----------------------------------------------------

    /// A vertical stack of progress bars.
    ///
    /// When progress is globally disabled (see [`set_progress_enabled`]), the
    /// group and all bars added to it are no-ops.
    pub struct ProgressGroup {
        inner: MultiProgress,
        overall: Option<ProgressHandle>,
        enabled: bool,
    }

    impl ProgressGroup {
        /// Create a new group with no overall bar.
        #[must_use]
        pub fn new() -> Self {
            Self { inner: MultiProgress::new(), overall: None, enabled: progress_enabled() }
        }

        /// Create a group with an overall aggregate bar pinned at the bottom.
        ///
        /// The overall bar has no [`{spinner}`] in its template, so
        /// [`enable_steady_tick`] is intentionally **not** called — the bar
        /// only redraws when its position or message changes.
        ///
        /// When progress has been globally disabled, returns a no-op group and
        /// a no-op handle.
        #[must_use]
        pub fn with_overall(label: &str, total: u64) -> (Self, ProgressHandle) {
            if !progress_enabled() {
                return (
                    Self { inner: MultiProgress::new(), overall: None, enabled: false },
                    ProgressHandle::disabled(),
                );
            }
            let mp = MultiProgress::new();
            let inner = ProgressBar::new(total);
            apply_overall_bar_style(&inner);
            inner.set_prefix(label.to_string());
            let overall_handle = mp.add(inner);
            let handle = ProgressHandle { inner: Some(overall_handle) };
            let group = Self { inner: mp, overall: Some(handle.clone()), enabled: true };
            (group, handle)
        }

        /// Add a child bar.
        ///
        /// When the group has an overall bar, the child is inserted above it.
        /// Otherwise children stack in insertion order.
        ///
        /// When the group is disabled, returns a no-op handle.
        #[must_use]
        pub fn add_bar(&self, total: u64, label: &str) -> ProgressHandle {
            if !self.enabled {
                return ProgressHandle::disabled();
            }
            let inner = ProgressBar::new(total);
            apply_bar_style(&inner);
            inner.set_prefix(label.to_string());
            inner.enable_steady_tick(Duration::from_millis(100));
            let bar = if let Some(ref overall) = self.overall {
                let overall_inner = overall
                    .inner
                    .as_ref()
                    .expect("overall handle must be enabled when group is enabled");
                self.inner.insert_before(overall_inner, inner)
            } else {
                self.inner.add(inner)
            };
            ProgressHandle { inner: Some(bar) }
        }

        /// Block until all bars in the group reach a finished state.
        ///
        /// In indicatif 0.17 `MultiProgress` has no blocking join, so this is
        /// effectively a no-op. The draw thread terminates when the group is
        /// dropped and all bars are finished.
        pub fn join(&self) {}

        /// Clear the terminal display after all bars are done.
        ///
        /// Call this after all bars have been finished (via [`finish`](ProgressHandle::finish)
        /// / [`finish_success`](ProgressHandle::finish_success) / [`finish_and_clear`](ProgressHandle::finish_and_clear)).
        /// Unfinished bars will have their tickers stopped when their handles
        /// are dropped — [`clear`](MultiProgress::clear) only wipes the
        /// terminal display so stale bar lines don't pollute scrollback.
        ///
        /// No-op when the group is disabled.
        pub fn join_and_clear(&self) {
            if !self.enabled {
                return;
            }
            self.inner.clear().ok();
        }
    }

    impl Default for ProgressGroup {
        fn default() -> Self {
            Self::new()
        }
    }

    impl Drop for ProgressGroup {
        fn drop(&mut self) {
            if !self.enabled {
                return;
            }
            // Safety net: clear terminal so stale bars don't pollute scrollback
            // if the caller forgot `join_and_clear()` or took an early-exit
            // error path.
            self.inner.clear().ok();
        }
    }
}

#[cfg(feature = "progress")]
pub use inner::{ProgressGroup, ProgressHandle, progress_enabled, set_progress_enabled};

// ---- Shared API traits for dependency injection (feature-gated) -------

/// Minimal progress-bar handle API for dependency injection.
///
/// Both [`ProgressHandle`] and
/// [`RecordingProgressHandle`](recording::RecordingProgressHandle) implement
/// this trait, allowing consumer functions to accept either a real display
/// bar or a recording bar for testing.
#[cfg(feature = "progress")]
pub trait ProgressBarApi: Send + Sync {
    /// Advance the bar by `delta` work units.
    fn advance(&self, delta: u64);
    /// Mark as finished with a success message.
    fn finish_success(&self, msg: &str);
    /// Mark as finished with an error message.
    fn finish_error(&self, msg: &str);
}

#[cfg(feature = "progress")]
impl ProgressBarApi for ProgressHandle {
    fn advance(&self, delta: u64) {
        ProgressHandle::advance(self, delta);
    }
    fn finish_success(&self, msg: &str) {
        ProgressHandle::finish_success(self, msg);
    }
    fn finish_error(&self, msg: &str) {
        ProgressHandle::finish_error(self, msg);
    }
}

/// Minimal progress-group API for dependency injection.
///
/// Both [`ProgressGroup`] and
/// [`RecordingProgressGroup`](recording::RecordingProgressGroup) implement
/// this trait, allowing consumer functions to accept either a real display
/// group or a recording group for testing.
#[cfg(feature = "progress")]
pub trait ProgressGroupApi {
    /// Add a child bar and return an [`Arc`]-wrapped handle.
    fn add_bar(&self, total: u64, label: &str) -> Arc<dyn ProgressBarApi>;
}

#[cfg(feature = "progress")]
impl ProgressGroupApi for ProgressGroup {
    fn add_bar(&self, total: u64, label: &str) -> Arc<dyn ProgressBarApi> {
        Arc::new(ProgressGroup::add_bar(self, total, label))
    }
}

// ---- Recording types for test assertions (feature-gated) ---------------

/// Recording progress operations for test assertions.
///
/// This module provides [`RecordingProgressGroup`] and
/// [`RecordingProgressHandle`] that record all operations into a shared
/// operation log without any visual output. Use [`RecordingProgressGroup::ops`]
/// to retrieve the recorded sequence for verification.
///
/// Only available when the `progress` feature is enabled.
#[cfg(feature = "progress")]
pub mod recording {
    use std::sync::{Arc, Mutex};

    /// Recorded progress operation for test assertions.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum ProgressOp {
        /// A bar was added to a group.
        AddBar {
            /// Total work units for the bar.
            total: u64,
            /// Display label for the bar.
            label: String,
        },
        /// `advance(delta)` was called.
        Advance {
            /// Number of work units advanced.
            delta: u64,
        },
        /// `set_total(total)` was called.
        SetTotal {
            /// New total work units.
            total: u64,
        },
        /// `set_position(pos)` was called.
        SetPosition {
            /// Absolute position to jump to.
            pos: u64,
        },
        /// `set_message(msg)` was called.
        SetMessage {
            /// Message text.
            msg: String,
        },
        /// `set_prefix(prefix)` was called.
        SetPrefix {
            /// Prefix text.
            prefix: String,
        },
        /// `finish()` was called.
        Finish,
        /// `finish_success(msg)` was called.
        FinishSuccess {
            /// Success message.
            msg: String,
        },
        /// `finish_error(msg)` was called.
        FinishError {
            /// Error message.
            msg: String,
        },
        /// `finish_and_clear()` was called.
        FinishAndClear,
        /// `abandon()` was called.
        Abandon,
    }

    /// A recording progress group that records operations into a shared
    /// [`Vec<ProgressOp>`] for test assertions.
    ///
    /// Does not display anything. All bars added via
    /// [`add_bar`](RecordingProgressGroup::add_bar) share the same operation
    /// log.
    #[derive(Clone)]
    pub struct RecordingProgressGroup {
        ops: Arc<Mutex<Vec<ProgressOp>>>,
    }

    impl RecordingProgressGroup {
        /// Create a new empty recording group.
        #[must_use]
        pub fn new() -> Self {
            Self { ops: Arc::new(Mutex::new(Vec::new())) }
        }

        /// Record adding a bar with the given `total` and `label`.
        ///
        /// Returns a [`RecordingProgressHandle`] that shares this group's
        /// operation log.
        #[must_use]
        pub fn add_bar(&self, total: u64, label: &str) -> RecordingProgressHandle {
            self.ops
                .lock()
                .expect("recording lock")
                .push(ProgressOp::AddBar { total, label: label.to_string() });
            RecordingProgressHandle { ops: self.ops.clone(), total: Some(total) }
        }

        /// Return a snapshot of all recorded operations.
        #[must_use]
        pub fn ops(&self) -> Vec<ProgressOp> {
            self.ops.lock().expect("recording lock").clone()
        }

        /// Clear all recorded operations.
        pub fn clear(&self) {
            self.ops.lock().expect("recording lock").clear();
        }
    }

    impl Default for RecordingProgressGroup {
        fn default() -> Self {
            Self::new()
        }
    }

    /// A recording progress handle that records operations into the shared
    /// log of its parent [`RecordingProgressGroup`].
    #[derive(Clone)]
    pub struct RecordingProgressHandle {
        ops: Arc<Mutex<Vec<ProgressOp>>>,
        total: Option<u64>,
    }

    impl RecordingProgressHandle {
        /// Create a standalone recording handle (not managed by a group).
        ///
        /// The handle has its own private operation log.
        #[must_use]
        pub fn new(total: u64) -> Self {
            Self { ops: Arc::new(Mutex::new(Vec::new())), total: Some(total) }
        }

        /// Create a disabled (no-op) recording handle.
        ///
        /// All methods are no-ops; the handle logs nothing and reports
        /// [`total`](RecordingProgressHandle::total) as 0.
        #[must_use]
        pub fn disabled() -> Self {
            Self { ops: Arc::new(Mutex::new(Vec::new())), total: None }
        }

        /// Return the total number of work units (0 = indeterminate/disabled).
        #[must_use]
        pub fn total(&self) -> u64 {
            self.total.unwrap_or(0)
        }

        /// Change the total mid-flight (recorded but not reflected in
        /// [`total()`](RecordingProgressHandle::total) — use
        /// [`ops()`](RecordingProgressHandle::ops) to verify).
        pub fn set_total(&self, total: u64) {
            self.ops.lock().expect("recording lock").push(ProgressOp::SetTotal { total });
        }

        /// Advance the handle by `delta` work units.
        pub fn advance(&self, delta: u64) {
            self.ops.lock().expect("recording lock").push(ProgressOp::Advance { delta });
        }

        /// Jump to an absolute position.
        pub fn set_position(&self, pos: u64) {
            self.ops.lock().expect("recording lock").push(ProgressOp::SetPosition { pos });
        }

        /// Set the message.
        pub fn set_message(&self, msg: impl Into<String>) {
            self.ops
                .lock()
                .expect("recording lock")
                .push(ProgressOp::SetMessage { msg: msg.into() });
        }

        /// Set the prefix.
        pub fn set_prefix(&self, prefix: impl Into<String>) {
            self.ops
                .lock()
                .expect("recording lock")
                .push(ProgressOp::SetPrefix { prefix: prefix.into() });
        }

        /// Mark the handle as finished.
        pub fn finish(&self) {
            self.ops.lock().expect("recording lock").push(ProgressOp::Finish);
        }

        /// Mark as finished with a success message.
        pub fn finish_success(&self, msg: impl Into<String>) {
            self.ops
                .lock()
                .expect("recording lock")
                .push(ProgressOp::FinishSuccess { msg: msg.into() });
        }

        /// Mark as finished with an error message.
        pub fn finish_error(&self, msg: impl Into<String>) {
            self.ops
                .lock()
                .expect("recording lock")
                .push(ProgressOp::FinishError { msg: msg.into() });
        }

        /// Finish and clear from display.
        pub fn finish_and_clear(&self) {
            self.ops.lock().expect("recording lock").push(ProgressOp::FinishAndClear);
        }

        /// Abandon — leaves visible but stops updates.
        pub fn abandon(&self) {
            self.ops.lock().expect("recording lock").push(ProgressOp::Abandon);
        }

        /// Return a snapshot of recorded operations for this handle.
        ///
        /// When created via [`RecordingProgressGroup::add_bar`], this returns
        /// the same shared log as all handles from that group.
        #[must_use]
        pub fn ops(&self) -> Vec<ProgressOp> {
            self.ops.lock().expect("recording lock").clone()
        }
    }
}

// ---- Trait impls for recording types -------------------------------------

#[cfg(feature = "progress")]
impl ProgressBarApi for recording::RecordingProgressHandle {
    fn advance(&self, delta: u64) {
        recording::RecordingProgressHandle::advance(self, delta);
    }
    fn finish_success(&self, msg: &str) {
        recording::RecordingProgressHandle::finish_success(self, msg);
    }
    fn finish_error(&self, msg: &str) {
        recording::RecordingProgressHandle::finish_error(self, msg);
    }
}

#[cfg(feature = "progress")]
impl ProgressGroupApi for recording::RecordingProgressGroup {
    fn add_bar(&self, total: u64, label: &str) -> Arc<dyn ProgressBarApi> {
        Arc::new(recording::RecordingProgressGroup::add_bar(self, total, label))
    }
}

// ---- Tests ---------------------------------------------------------------

#[cfg(test)]
#[cfg(feature = "progress")]
mod tests {
    use super::recording::{ProgressOp, RecordingProgressGroup, RecordingProgressHandle};
    use super::{ProgressGroup, ProgressHandle, progress_enabled, set_progress_enabled};

    // ---- PROGRESS_ENABLED wiring -----------------------------------------
    //
    // All toggle-dependent assertions live in one test to avoid races on the
    // global `PROGRESS_ENABLED` `AtomicBool` across parallel test threads.

    #[test]
    fn progress_enabled_toggle_affects_construction() {
        let prev = progress_enabled();

        // --- disabled ---
        set_progress_enabled(false);
        let h = ProgressHandle::new(100);
        assert_eq!(h.total(), 0, "disabled handle reports 0 total");
        let g = ProgressGroup::new();
        let ch = g.add_bar(50, "child");
        assert_eq!(ch.total(), 0);
        let (_og, oh) = ProgressGroup::with_overall("all", 300);
        assert_eq!(oh.total(), 0);
        g.join_and_clear();

        // All mutation methods are no-ops on a disabled handle
        h.advance(10);
        h.set_total(50);
        h.set_position(5);
        h.set_message("hi");
        h.set_prefix("pfx");
        h.finish();
        h.finish_success("ok");
        h.finish_error("err");
        h.finish_and_clear();
        h.abandon();

        // --- enabled ---
        set_progress_enabled(true);
        let h = ProgressHandle::new(100);
        assert_eq!(h.total(), 100, "enabled handle reports initial total");
        let g = ProgressGroup::new();
        let ch = g.add_bar(50, "child");
        assert_eq!(ch.total(), 50);
        let (_og, oh) = ProgressGroup::with_overall("all", 300);
        assert_eq!(oh.total(), 300);
        g.join_and_clear();

        h.set_total(200);
        assert_eq!(h.total(), 200);
        h.advance(10);
        h.set_position(20);
        h.set_message("msg");
        h.set_prefix("pfx");
        h.finish();

        set_progress_enabled(prev);
    }

    #[test]
    fn handle_disabled_is_noop() {
        let h = ProgressHandle::disabled();
        assert_eq!(h.total(), 0);
        h.advance(10);
        h.set_total(50);
        h.set_position(5);
        h.set_message("hi");
        h.set_prefix("pfx");
        h.finish();
        h.finish_success("ok");
        h.finish_error("err");
        h.finish_and_clear();
        h.abandon();
    }

    // ---- RecordingProgressGroup ------------------------------------------

    #[test]
    fn recording_group_add_bar_creates_op() {
        let rg = RecordingProgressGroup::new();
        let _h = rg.add_bar(100, "test-bar");
        assert_eq!(rg.ops(), vec![ProgressOp::AddBar { total: 100, label: "test-bar".into() }]);
    }

    #[test]
    fn recording_group_clear_resets_ops() {
        let rg = RecordingProgressGroup::new();
        let _ = rg.add_bar(10, "a");
        let _ = rg.add_bar(20, "b");
        assert_eq!(rg.ops().len(), 2);
        rg.clear();
        assert!(rg.ops().is_empty());
    }

    #[test]
    fn recording_handle_records_all_ops() {
        let h = RecordingProgressHandle::new(100);
        assert_eq!(h.total(), 100);

        h.set_total(200);
        h.advance(5);
        h.set_position(10);
        h.set_message("hello");
        h.set_prefix("pfx");
        h.finish();

        assert_eq!(
            h.ops(),
            vec![
                ProgressOp::SetTotal { total: 200 },
                ProgressOp::Advance { delta: 5 },
                ProgressOp::SetPosition { pos: 10 },
                ProgressOp::SetMessage { msg: "hello".into() },
                ProgressOp::SetPrefix { prefix: "pfx".into() },
                ProgressOp::Finish,
            ]
        );
    }

    #[test]
    fn recording_handle_shared_log() {
        let rg = RecordingProgressGroup::new();
        let h1 = rg.add_bar(50, "first");
        let h2 = rg.add_bar(100, "second");

        h1.advance(1);
        h2.advance(2);
        h1.finish();

        assert_eq!(
            rg.ops(),
            vec![
                ProgressOp::AddBar { total: 50, label: "first".into() },
                ProgressOp::AddBar { total: 100, label: "second".into() },
                ProgressOp::Advance { delta: 1 },
                ProgressOp::Advance { delta: 2 },
                ProgressOp::Finish,
            ]
        );
    }

    #[test]
    fn recording_handle_disabled_has_zero_total() {
        let h = RecordingProgressHandle::disabled();
        assert_eq!(h.total(), 0);
        // Even a disabled handle records ops (it uses a fresh log).
        assert!(h.ops().is_empty());
        h.advance(1);
        assert_eq!(h.ops(), vec![ProgressOp::Advance { delta: 1 }]);
    }

    #[test]
    fn recording_handle_finish_success_and_error() {
        let h = RecordingProgressHandle::new(10);
        h.finish_success("ok!");
        h.finish_error("oh no");
        assert_eq!(
            h.ops(),
            vec![
                ProgressOp::FinishSuccess { msg: "ok!".into() },
                ProgressOp::FinishError { msg: "oh no".into() },
            ]
        );
    }

    #[test]
    fn recording_handle_finish_and_clear_abandon() {
        let h = RecordingProgressHandle::new(1);
        h.finish_and_clear();
        h.abandon();
        assert_eq!(h.ops(), vec![ProgressOp::FinishAndClear, ProgressOp::Abandon]);
    }
}
