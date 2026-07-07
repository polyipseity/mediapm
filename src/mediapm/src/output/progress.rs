//! Progress bar rendering for long-running operations.
//!
//! Provides [`ProgressGroup`] (multi-bar stack with optional overall bar)
//! and [`ProgressHandle`] (single bar handle) built on top of `indicatif`.
//!
//! Output is automatically suppressed when stderr is not a TTY or when
//! [`set_progress_enabled`] is set to `false` (`--quiet` mode).

use std::sync::atomic::{AtomicBool, Ordering};

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

// ---------------------------------------------------------------------------
// Global toggle
// ---------------------------------------------------------------------------

static PROGRESS_ENABLED: AtomicBool = AtomicBool::new(true);

/// Globally enable or disable progress bar output.
///
/// Set to `false` when the user passes `--quiet` or `MEDIAPM_QUIET` is set.
pub fn set_progress_enabled(enabled: bool) {
    PROGRESS_ENABLED.store(enabled, Ordering::Relaxed);
}

/// Returns whether progress bar output is currently enabled.
///
/// When disabled, all progress-bar rendering is suppressed (but internal
/// state tracking continues).
pub fn progress_enabled() -> bool {
    PROGRESS_ENABLED.load(Ordering::Relaxed)
}

// ---------------------------------------------------------------------------
// Style constants
// ---------------------------------------------------------------------------

/// Template for individual child bars.
const CHILD_BAR_TEMPLATE: &str = "{spinner:.green} {prefix:>12.12} [{elapsed_precise}] {wide_bar:.cyan/blue} {pos}/{len} {msg} ({eta})";

/// Template for the overall aggregate bar (pinned at bottom).
const OVERALL_BAR_TEMPLATE: &str =
    "{prefix:>12.12} [{elapsed_precise}] {wide_bar:.green/dim} {pos}/{len} {msg}";

/// Compact template for narrow terminals (< 60 columns).
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
    let term_width = terminal_width();
    if term_width < 60 {
        pb.set_style(compact_bar_style());
    } else {
        pb.set_style(child_bar_style());
    }
}

/// Detect terminal width via console crate.
fn terminal_width() -> u16 {
    console::Term::stderr().size().1
}

// ---------------------------------------------------------------------------
// ProgressHandle
// ---------------------------------------------------------------------------

/// Handle to one progress bar.
///
/// Cloning creates another reference to the same underlying bar — all clones
/// share state and advancing any one of them updates the display.
#[derive(Clone)]
pub struct ProgressHandle {
    inner: ProgressBar,
}

impl ProgressHandle {
    /// Create a standalone progress bar (not managed by a [`ProgressGroup`]).
    ///
    /// When `total` is 0 the bar operates in indeterminate (spinner) mode.
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
    ///
    /// Prefix is right-aligned to 12 characters in the template.
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

    /// Mark as finished with an error message (shown in red/abandoned style).
    pub fn finish_error(&self, msg: impl Into<String>) {
        self.inner.abandon_with_message(msg.into());
    }

    /// Abandon the bar — leaves it visible but stops all updates.
    pub fn abandon(&self) {
        self.inner.abandon();
    }
}

// ---------------------------------------------------------------------------
// ProgressGroup
// ---------------------------------------------------------------------------

/// A vertical stack of progress bars.
///
/// Use [`ProgressGroup::with_overall`] when you need an aggregate bar pinned
/// at the bottom. Child bars are added via [`add_bar`](Self::add_bar).
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
    ///
    /// Returns the group and a handle to the overall bar. The overall bar
    /// updates independently — the caller is responsible for advancing it
    /// as phases complete.
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
    ///
    /// `label` is shown as the bar's prefix text.
    #[must_use]
    pub fn add_bar(&self, total: u64, label: &str) -> ProgressHandle {
        let inner = ProgressBar::new(total);
        apply_bar_style(&inner);
        inner.set_prefix(label.to_string());
        let bar = self.inner.insert(0, inner);
        ProgressHandle { inner: bar }
    }

    /// Block until all bars in the group reach a finished state (clears on drop if not called).
    pub fn join(&self) {
        // MultiProgress auto-joins on drop; this is a no-op placeholder.
    }

    /// Block until all bars finish, then clear them from the terminal.
    pub fn join_and_clear(&self) {
        // MultiProgress auto-joins on drop; this is a no-op placeholder.
    }
}

impl Default for ProgressGroup {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------

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
