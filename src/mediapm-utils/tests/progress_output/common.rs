//! Shared constants, helper functions, and re-exports for progress output tests.

use std::sync::Arc;

use indicatif::{InMemoryTerm, MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use mediapm_utils::progress::{ProgressGroup, TestTimeSource};

/// Default terminal dimensions for standard tests.
pub const H: u16 = 24;
pub const W: u16 = 40;

/// Template with wide bar so the visual progress bar is visible in output.
pub const T: &str = "{prefix:>8.8} [{elapsed_precise}] {wide_bar} {pos}/{len} {msg}";

/// Template with spinner + wide bar for spinner tests.
pub const TS: &str = "{spinner} {prefix:>8.8} [{elapsed_precise}] {wide_bar} {pos}/{len} {msg}";

/// Template for narrow terminals where the full template overflows W.
pub const TN: &str = "{prefix:>8.8} {wide_bar} {pos}/{len}";

/// Create a standard [`ProgressStyle`] from template [`T`].
pub fn style() -> ProgressStyle {
    ProgressStyle::with_template(T).unwrap().progress_chars("█░")
}

/// Create a spinner [`ProgressStyle`] from template [`TS`].
pub fn spinner_style() -> ProgressStyle {
    ProgressStyle::with_template(TS).unwrap().progress_chars("█░").tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
}

/// Create a [`ProgressStyle`] from a custom template string.
pub fn style_from(template: &str) -> ProgressStyle {
    ProgressStyle::with_template(template).unwrap().progress_chars("█░")
}

/// Create a [`MultiProgress`] + [`InMemoryTerm`] pair for one test
/// at the default terminal size (H=24, W=40).
pub fn mk() -> (MultiProgress, InMemoryTerm) {
    mk_with_size(H, W)
}

/// Read terminal contents and assert the expected number of lines appear.
pub fn poll_lines(group: &ProgressGroup, term: &InMemoryTerm, expected: usize) -> Vec<String> {
    group.tick();
    let contents = term.contents();
    let lines: Vec<String> = contents.lines().map(String::from).collect();
    assert_eq!(
        lines.len(),
        expected,
        "poll_lines: expected {expected} lines, got {}\ncontents:\n{contents}",
        lines.len(),
    );
    lines
}

/// Create a [`MultiProgress`] + [`InMemoryTerm`] pair at a custom size.
pub fn mk_with_size(h: u16, w: u16) -> (MultiProgress, InMemoryTerm) {
    let term = InMemoryTerm::new(h, w);
    let target = ProgressDrawTarget::term_like(Box::new(term.clone()));
    (MultiProgress::with_draw_target(target), term)
}

/// Create a [`MultiProgress`] + [`InMemoryTerm`] + [`TestTimeSource`] triple
/// for tests that need deterministic elapsed timing.
pub fn mk_with_size_and_ts(h: u16, w: u16) -> (MultiProgress, InMemoryTerm, Arc<TestTimeSource>) {
    let term = InMemoryTerm::new(h, w);
    let target = ProgressDrawTarget::term_like(Box::new(term.clone()));
    let ts = Arc::new(TestTimeSource::new());
    (MultiProgress::with_draw_target(target), term, ts)
}

/// Shorthand: create a bar, set style+prefix, add to mp, return it.
pub fn add_bar(mp: &MultiProgress, total: u64, prefix: &str) -> ProgressBar {
    let pb = ProgressBar::new(total);
    pb.set_style(style());
    pb.set_prefix(prefix.to_string());
    mp.add(pb)
}

/// Like `add_bar` but inserted before `before` (appears above it on screen).
pub fn ins_bar(mp: &MultiProgress, before: &ProgressBar, total: u64, prefix: &str) -> ProgressBar {
    let pb = ProgressBar::new(total);
    pb.set_style(style());
    pb.set_prefix(prefix.to_string());
    mp.insert_before(before, pb)
}
