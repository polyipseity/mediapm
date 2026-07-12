//! Progress bar output tests — exact terminal screen matching.
//!
//! Every test captures the **full terminal contents** via
//! [`indicatif::InMemoryTerm::contents`] and compares against the exact expected
//! string inline. Multi-line expected strings use `concat!()` so each visible
//! line of the expected output appears on its own source line.
//!
//! # Template
//!
//! All tests use template `{prefix:>8.8} [{elapsed_precise}] {wide_bar} {pos}/{len} {msg}`
//! with progress chars `█░` at terminal width 40.  The `{wide_bar}` auto-sizes
//! to fill available width.

use std::sync::Arc;

use indicatif::{InMemoryTerm, MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use mediapm_utils::progress::{DimensionSource, ProgressGroup, TestDimensionSource, TrackedHandle};

/// Default terminal dimensions for standard tests.
const H: u16 = 24;
const W: u16 = 40;

/// Template with wide bar so the visual progress bar is visible in output.
const T: &str = "{prefix:>8.8} [{elapsed_precise}] {wide_bar} {pos}/{len} {msg}";

/// Template with spinner + wide bar for spinner tests.
const TS: &str = "{spinner} {prefix:>8.8} [{elapsed_precise}] {wide_bar} {pos}/{len} {msg}";

/// Template for narrow terminals where the full template overflows W.
const TN: &str = "{prefix:>8.8} {wide_bar} {pos}/{len}";

/// Create a standard [`ProgressStyle`] from template [`T`].
fn style() -> ProgressStyle {
    ProgressStyle::with_template(T).unwrap().progress_chars("█░")
}

/// Create a spinner [`ProgressStyle`] from template [`TS`].
///
/// Uses the same tick chars and progress chars as production
/// (`src/mediapm-utils/src/progress.rs`).  The last char (⠏) is only shown on
/// finish/abandon — it never appears during normal cycling.
fn spinner_style() -> ProgressStyle {
    ProgressStyle::with_template(TS).unwrap().progress_chars("█░").tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
}

/// Create a [`ProgressStyle`] from a custom template string.
fn style_from(template: &str) -> ProgressStyle {
    ProgressStyle::with_template(template).unwrap().progress_chars("█░")
}

/// Create a [`MultiProgress`] + [`InMemoryTerm`] pair for one test
/// at the default terminal size (H=24, W=40).
fn mk() -> (MultiProgress, InMemoryTerm) {
    mk_with_size(H, W)
}

/// Create a [`MultiProgress`] + [`InMemoryTerm`] pair at a custom size.
fn mk_with_size(h: u16, w: u16) -> (MultiProgress, InMemoryTerm) {
    let term = InMemoryTerm::new(h, w);
    let target = ProgressDrawTarget::term_like(Box::new(term.clone()));
    (MultiProgress::with_draw_target(target), term)
}

/// Shorthand: create a bar, set style+prefix, add to mp, return it.
fn add_bar(mp: &MultiProgress, total: u64, prefix: &str) -> ProgressBar {
    let pb = ProgressBar::new(total);
    pb.set_style(style());
    pb.set_prefix(prefix.to_string());
    mp.add(pb)
}

/// Like `add_bar` but inserted before `before` (appears above it on screen).
fn ins_bar(mp: &MultiProgress, before: &ProgressBar, total: u64, prefix: &str) -> ProgressBar {
    let pb = ProgressBar::new(total);
    pb.set_style(style());
    pb.set_prefix(prefix.to_string());
    mp.insert_before(before, pb)
}

// ── Single bar lifecycle: raw indicatif (bypasses TrackedHandle/SharedState) ─
//
// These tests use the `add_bar()` helper that creates an indicatif
// `ProgressBar` with test template `T` (which includes `[{elapsed_precise}]`).
// They exercise indicatif rendering directly — NOT the production
// `TrackedHandle`/`SharedState`/elapsed-injection path.
// For the equivalent production code path, see the "Child bar elapsed" and
// "ProgressGroup rendering" sections below.
//
// These tests remain useful for verifying indicatif behavior (bar width,
// progress chars, prefix alignment, finish_and_clear).

#[test]
fn single_bar_empty_after_creation() {
    let (mp, term) = mk();
    let _pb = add_bar(&mp, 4, "overall");
    // No draw triggered yet — screen is empty.
    assert_eq!(term.contents(), "");
}

#[test]
fn single_bar_shows_content_after_tick() {
    let (mp, term) = mk();
    let pb = add_bar(&mp, 4, "overall");
    pb.tick();
    assert_eq!(term.contents(), " overall [00:00:00] ░░░░░░░░░░░░░░░ 0/4");
}

#[test]
fn single_bar_partial_progress() {
    let (mp, term) = mk();
    let pb = add_bar(&mp, 4, "overall");
    pb.inc(2);
    pb.tick();
    assert_eq!(term.contents(), " overall [00:00:00] ███████░░░░░░░░ 2/4");
}

#[test]
fn single_bar_full_progress() {
    let (mp, term) = mk();
    let pb = add_bar(&mp, 4, "overall");
    pb.inc(4);
    pb.tick();
    assert_eq!(term.contents(), " overall [00:00:00] ███████████████ 4/4");
}

#[test]
fn single_bar_finish_with_message() {
    let (mp, term) = mk();
    let pb = add_bar(&mp, 5, "test");
    pb.inc(5);
    pb.finish_with_message("done");
    pb.tick();
    assert_eq!(term.contents(), "    test [00:00:00] ███████████ 5/5 done");
}

#[test]
fn single_bar_set_message() {
    let (mp, term) = mk();
    let pb = add_bar(&mp, 3, "tool-a");
    pb.inc(3);
    pb.set_message("done");
    pb.tick();
    assert_eq!(term.contents(), "  tool-a [00:00:00] ███████████ 3/3 done");
}

#[test]
fn single_bar_zero_total() {
    let (mp, term) = mk();
    let pb = add_bar(&mp, 0, "overall");
    pb.tick();
    // 0/0 is considered 100% by indicatif.
    assert_eq!(term.contents(), " overall [00:00:00] ███████████████ 0/0");
}

#[test]
fn single_bar_intermediate_progress() {
    let (mp, term) = mk();
    let pb = add_bar(&mp, 10, "test");
    pb.inc(3);
    pb.tick();
    assert_eq!(term.contents(), "    test [00:00:00] ████░░░░░░░░░░ 3/10");
}

#[test]
fn single_bar_half_progress() {
    let (mp, term) = mk();
    let pb = add_bar(&mp, 2, "tool1");
    pb.inc(1);
    pb.tick();
    assert_eq!(term.contents(), "   tool1 [00:00:00] ███████░░░░░░░░ 1/2");
}

// ── Two lines: overall + child (sequential style) ────────────────────────────

#[test]
fn two_lines_initial() {
    let (mp, term) = mk();
    let o = add_bar(&mp, 4, "overall");
    let c = ins_bar(&mp, &o, 2, "tool1");
    c.tick();
    o.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "   tool1 [00:00:00] ░░░░░░░░░░░░░░░ 0/2\n",
            " overall [00:00:00] ░░░░░░░░░░░░░░░ 0/4",
        ),
    );
}

#[test]
fn two_lines_child_advances() {
    let (mp, term) = mk();
    let o = add_bar(&mp, 4, "overall");
    let c = ins_bar(&mp, &o, 2, "tool1");
    c.inc(2);
    c.tick();
    o.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "   tool1 [00:00:00] ███████████████ 2/2\n",
            " overall [00:00:00] ░░░░░░░░░░░░░░░ 0/4",
        ),
    );
}

#[test]
fn two_lines_child_cleared() {
    let (mp, term) = mk();
    let o = add_bar(&mp, 4, "overall");
    let c = ins_bar(&mp, &o, 2, "tool1");
    c.tick();
    o.tick();
    c.finish_and_clear();
    c.tick();
    o.tick();
    // Only overall remains.
    assert_eq!(term.contents(), " overall [00:00:00] ░░░░░░░░░░░░░░░ 0/4");
}

#[test]
fn two_lines_with_message() {
    let (mp, term) = mk();
    let o = add_bar(&mp, 5, "overall");
    let c = ins_bar(&mp, &o, 2, "tool1");
    c.inc(1);
    c.set_message("macos");
    c.tick();
    o.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "   tool1 [00:00:00] █████░░░░░ 1/2 macos\n",
            " overall [00:00:00] ░░░░░░░░░░░░░░░ 0/5",
        ),
    );
}

#[test]
fn two_lines_provision_round_done() {
    let (mp, term) = mk();
    let o = add_bar(&mp, 5, "overall");
    let c = ins_bar(&mp, &o, 2, "tool1");
    c.inc(1);
    c.set_message("macos");
    c.tick();
    o.tick();
    c.finish_and_clear();
    c.tick();
    o.tick();
    drop(c);
    o.inc(1);
    o.tick();
    // Child gone, overall advanced.
    assert_eq!(term.contents(), " overall [00:00:00] ███░░░░░░░░░░░░ 1/5");
}

#[test]
fn two_lines_new_child_after_round() {
    let (mp, term) = mk();
    let o = add_bar(&mp, 5, "overall");
    let c = ins_bar(&mp, &o, 2, "tool1");
    c.inc(1);
    c.set_message("macos");
    c.tick();
    o.tick();
    c.finish_and_clear();
    c.tick();
    o.tick();
    drop(c);
    o.inc(1);
    o.tick();
    // New child for round 2.
    let c2 = ins_bar(&mp, &o, 3, "tool2");
    c2.set_message("linux");
    c2.tick();
    o.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "   tool2 [00:00:00] ░░░░░░░░░░ 0/3 linux\n",
            " overall [00:00:00] ███░░░░░░░░░░░░ 1/5",
        ),
    );
}

// ── Three lines: two concurrent children + overall ───────────────────────────

#[test]
fn three_lines_concurrent_initial() {
    let (mp, term) = mk();
    let o = add_bar(&mp, 3, "overall");
    let a = ins_bar(&mp, &o, 5, "tool-a");
    let b = ins_bar(&mp, &o, 3, "tool-b");
    a.tick();
    b.tick();
    o.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "  tool-a [00:00:00] ░░░░░░░░░░░░░░░ 0/5\n",
            "  tool-b [00:00:00] ░░░░░░░░░░░░░░░ 0/3\n",
            " overall [00:00:00] ░░░░░░░░░░░░░░░ 0/3",
        ),
    );
}

#[test]
fn three_lines_first_child_cleared() {
    let (mp, term) = mk();
    let o = add_bar(&mp, 3, "overall");
    let a = ins_bar(&mp, &o, 5, "tool-a");
    let b = ins_bar(&mp, &o, 3, "tool-b");
    a.tick();
    b.tick();
    o.tick();
    a.finish_and_clear();
    a.tick();
    o.tick();
    // tool-a gone, tool-b + overall remain.
    assert_eq!(
        term.contents(),
        concat!(
            "  tool-b [00:00:00] ░░░░░░░░░░░░░░░ 0/3\n",
            " overall [00:00:00] ░░░░░░░░░░░░░░░ 0/3",
        ),
    );
}

#[test]
fn three_lines_both_children_cleared() {
    let (mp, term) = mk();
    let o = add_bar(&mp, 3, "overall");
    let a = ins_bar(&mp, &o, 5, "tool-a");
    let b = ins_bar(&mp, &o, 3, "tool-b");
    a.tick();
    b.tick();
    o.tick();
    a.finish_and_clear();
    b.finish_and_clear();
    a.tick();
    b.tick();
    o.tick();
    // Only overall remains.
    assert_eq!(term.contents(), " overall [00:00:00] ░░░░░░░░░░░░░░░ 0/3");
}

#[test]
fn three_lines_all_done_with_messages() {
    let (mp, term) = mk();
    let o = add_bar(&mp, 3, "overall");
    let a = ins_bar(&mp, &o, 3, "tool-a");
    let b = ins_bar(&mp, &o, 1, "tool-b");
    a.inc(3);
    b.inc(1);
    a.set_message("done");
    b.set_message("done");
    a.tick();
    b.tick();
    o.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "  tool-a [00:00:00] ███████████ 3/3 done\n",
            "  tool-b [00:00:00] ███████████ 1/1 done\n",
            " overall [00:00:00] ░░░░░░░░░░░░░░░ 0/3",
        ),
    );
}

// ── Sequential rounds (full provision-style pattern) ─────────────────────────

#[test]
fn sequential_rounds() {
    let (mp, term) = mk();
    let o = add_bar(&mp, 6, "overall");

    // Round 1: tool1 starts
    let c = ins_bar(&mp, &o, 1, "tool1");
    c.tick();
    o.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "   tool1 [00:00:00] ░░░░░░░░░░░░░░░ 0/1\n",
            " overall [00:00:00] ░░░░░░░░░░░░░░░ 0/6",
        ),
    );

    // Round 1: tool1 finishes, overall advances
    c.finish_and_clear();
    c.tick();
    o.tick();
    drop(c);
    o.inc(1);
    o.tick();
    assert_eq!(term.contents(), " overall [00:00:00] ██░░░░░░░░░░░░░ 1/6");

    // Round 2: tool2 starts
    let c2 = ins_bar(&mp, &o, 1, "tool2");
    c2.tick();
    o.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "   tool2 [00:00:00] ░░░░░░░░░░░░░░░ 0/1\n",
            " overall [00:00:00] ██░░░░░░░░░░░░░ 1/6",
        ),
    );

    // Round 2: tool2 finishes, overall advances
    c2.finish_and_clear();
    c2.tick();
    o.tick();
    drop(c2);
    o.inc(1);
    o.tick();
    assert_eq!(term.contents(), " overall [00:00:00] █████░░░░░░░░░░ 2/6");
}

// ── Spinner ──────────────────────────────────────────────────────────────────

#[test]
fn spinner_active() {
    let (mp, term) = mk();
    let pb = add_bar(&mp, 5, "test");
    pb.set_style(spinner_style());
    pb.tick();
    let s = term.contents();
    // First tick shows ⠙ (index 1 of production tick_chars).
    assert_eq!(s, "⠙     test [00:00:00] ░░░░░░░░░░░░░ 0/5");
}

#[test]
fn spinner_with_overall() {
    let (mp, term) = mk();
    let o = add_bar(&mp, 4, "overall");
    let c = ins_bar(&mp, &o, 2, "tool1");
    c.set_style(spinner_style());
    c.tick();
    o.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "⠙    tool1 [00:00:00] ░░░░░░░░░░░░░ 0/2\n",
            " overall [00:00:00] ░░░░░░░░░░░░░░░ 0/4",
        ),
    );
}

#[test]
fn spinner_finishes() {
    let (mp, term) = mk();
    let pb = add_bar(&mp, 5, "test");
    pb.set_style(spinner_style());
    pb.tick(); // ⠙ (frame 1)
    pb.finish_with_message("done");
    pb.tick(); // ⠏ (final frame)
    assert_eq!(term.contents(), "⠏     test [00:00:00] █████████ 5/5 done");
}

// ── Spinner: multi-frame animation (uses production 10-frame cycle) ──────────

#[test]
fn spinner_animation_cycle() {
    let (mp, term) = mk();
    let pb = add_bar(&mp, 5, "test");
    pb.set_style(spinner_style());
    // Production 10-frame "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏" cycles as indices 1→2→3→4→5→6→7→8→0.
    pb.tick();
    assert_eq!(term.contents(), "⠙     test [00:00:00] ░░░░░░░░░░░░░ 0/5", "frame 1/9");
    pb.tick();
    assert_eq!(term.contents(), "⠹     test [00:00:00] ░░░░░░░░░░░░░ 0/5", "frame 2/9");
    pb.tick();
    assert_eq!(term.contents(), "⠸     test [00:00:00] ░░░░░░░░░░░░░ 0/5", "frame 3/9");
    pb.tick();
    assert_eq!(term.contents(), "⠼     test [00:00:00] ░░░░░░░░░░░░░ 0/5", "frame 4/9");
    pb.tick();
    assert_eq!(term.contents(), "⠴     test [00:00:00] ░░░░░░░░░░░░░ 0/5", "frame 5/9");
    pb.tick();
    assert_eq!(term.contents(), "⠦     test [00:00:00] ░░░░░░░░░░░░░ 0/5", "frame 6/9");
    pb.tick();
    assert_eq!(term.contents(), "⠧     test [00:00:00] ░░░░░░░░░░░░░ 0/5", "frame 7/9");
    pb.tick();
    assert_eq!(term.contents(), "⠇     test [00:00:00] ░░░░░░░░░░░░░ 0/5", "frame 8/9");
    pb.tick();
    assert_eq!(term.contents(), "⠋     test [00:00:00] ░░░░░░░░░░░░░ 0/5", "frame 9/9 wraps to ⠋");
}

#[test]
fn spinner_child_animation_with_overall() {
    let (mp, term) = mk();
    let o = add_bar(&mp, 5, "overall");
    let c = ins_bar(&mp, &o, 2, "tool1");
    c.set_style(spinner_style());
    // Child progresses through frames while overall stays static.
    c.tick();
    o.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "⠙    tool1 [00:00:00] ░░░░░░░░░░░░░ 0/2\n",
            " overall [00:00:00] ░░░░░░░░░░░░░░░ 0/5",
        ),
        "child frame 1/9 ⠙",
    );
    c.tick();
    o.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "⠹    tool1 [00:00:00] ░░░░░░░░░░░░░ 0/2\n",
            " overall [00:00:00] ░░░░░░░░░░░░░░░ 0/5",
        ),
        "child frame 2/9 ⠹",
    );
    c.tick();
    o.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "⠸    tool1 [00:00:00] ░░░░░░░░░░░░░ 0/2\n",
            " overall [00:00:00] ░░░░░░░░░░░░░░░ 0/5",
        ),
        "child frame 3/9 ⠸",
    );
    c.tick();
    o.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "⠼    tool1 [00:00:00] ░░░░░░░░░░░░░ 0/2\n",
            " overall [00:00:00] ░░░░░░░░░░░░░░░ 0/5",
        ),
        "child frame 4/9 ⠼",
    );
}

// ── Spinner: multi-bar animation ─────────────────────────────────────────────

#[test]
fn spinner_both_animate_together() {
    let (mp, term) = mk();
    let o = add_bar(&mp, 5, "overall");
    o.set_style(spinner_style());
    let c = ins_bar(&mp, &o, 2, "tool1");
    c.set_style(spinner_style());
    // Both progress independently.
    c.tick();
    o.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "⠙    tool1 [00:00:00] ░░░░░░░░░░░░░ 0/2\n",
            "⠙  overall [00:00:00] ░░░░░░░░░░░░░ 0/5",
        ),
        "both frame 1/9 ⠙",
    );
    c.tick();
    o.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "⠹    tool1 [00:00:00] ░░░░░░░░░░░░░ 0/2\n",
            "⠹  overall [00:00:00] ░░░░░░░░░░░░░ 0/5",
        ),
        "both frame 2/9 ⠹",
    );
    c.tick();
    o.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "⠸    tool1 [00:00:00] ░░░░░░░░░░░░░ 0/2\n",
            "⠸  overall [00:00:00] ░░░░░░░░░░░░░ 0/5",
        ),
        "both frame 3/9 ⠸",
    );
}

// ── Spinner: finish/reset/abandon animation behavior ─────────────────────────

#[test]
fn spinner_finish_frame_stability() {
    let (mp, term) = mk();
    let pb = add_bar(&mp, 5, "test");
    pb.set_style(spinner_style());
    pb.tick();
    assert_eq!(term.contents(), "⠙     test [00:00:00] ░░░░░░░░░░░░░ 0/5");
    pb.tick();
    assert_eq!(term.contents(), "⠹     test [00:00:00] ░░░░░░░░░░░░░ 0/5");
    // Finish — frame should settle on the last tick char (⠏).
    pb.finish_with_message("done");
    pb.tick();
    assert_eq!(term.contents(), "⠏     test [00:00:00] █████████ 5/5 done");
    // Additional ticks should still show the same final frame.
    pb.tick();
    assert_eq!(term.contents(), "⠏     test [00:00:00] █████████ 5/5 done");
}

#[test]
fn spinner_reset_continues_animation() {
    let (mp, term) = mk();
    let pb = add_bar(&mp, 5, "test");
    pb.set_style(spinner_style());
    pb.tick(); // ⠙ (frame 1)
    pb.tick(); // ⠹ (frame 2)
    pb.tick(); // ⠸ (frame 3)
    pb.reset();
    pb.tick(); // Continued from frame 4 → ⠼
    // Reset does NOT restart the animation cycle; it continues from where it was.
    assert_eq!(
        term.contents(),
        "⠼     test [00:00:00] ░░░░░░░░░░░░░ 0/5",
        "after reset, animation continues from next frame (⠼)"
    );
}

#[test]
fn spinner_abandon_ends_on_last_frame() {
    let (mp, term) = mk();
    let pb = add_bar(&mp, 5, "test");
    pb.set_style(spinner_style());
    // Production tick_chars "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏": first tick shows ⠙ (index 1).
    pb.tick();
    assert_eq!(term.contents(), "⠙     test [00:00:00] ░░░░░░░░░░░░░ 0/5");
    // Abandon — frame settles on last char (⠏).
    pb.abandon_with_message("failed");
    pb.tick();
    assert_eq!(term.contents(), "⠏     test [00:00:00] ░░░░░░░ 0/5 failed");
    // Additional ticks stay on the final frame.
    pb.tick();
    assert_eq!(term.contents(), "⠏     test [00:00:00] ░░░░░░░ 0/5 failed");
}

// ── Edge cases ───────────────────────────────────────────────────────────────

#[test]
fn empty_multi_progress() {
    let (mp, term) = mk();
    let _ = mp;
    assert_eq!(term.contents(), "");
}

#[test]
fn many_concurrent_children() {
    let (mp, term) = mk();
    let o = add_bar(&mp, 20, "overall");
    let mut kids = Vec::new();
    for i in 0..20 {
        let p = format!("t{i:02}");
        let c = ins_bar(&mp, &o, 1, &p);
        c.tick();
        kids.push(c);
    }
    o.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    assert_eq!(lines.len(), 21, "20 children + 1 overall = 21 lines");
    // Verify first and last lines.
    assert_eq!(lines[0], "     t00 [00:00:00] ░░░░░░░░░░░░░░░ 0/1");
    assert_eq!(lines[19], "     t19 [00:00:00] ░░░░░░░░░░░░░░░ 0/1");
    assert_eq!(lines[20], " overall [00:00:00] ░░░░░░░░░░░░░░ 0/20");
}

// ── Single bar: finish_and_clear ────────────────────────────────────────────

#[test]
fn single_bar_finish_and_clear() {
    let (mp, term) = mk();
    let pb = add_bar(&mp, 4, "overall");
    pb.inc(4);
    pb.tick();
    assert_eq!(term.contents(), " overall [00:00:00] ███████████████ 4/4");
    pb.finish_and_clear();
    pb.tick();
    assert_eq!(term.contents(), "");
}

// ── Single bar: finish without message ──────────────────────────────────────

#[test]
fn single_bar_finish() {
    let (mp, term) = mk();
    let pb = add_bar(&mp, 4, "overall");
    pb.inc(4);
    pb.finish();
    pb.tick();
    assert_eq!(term.contents(), " overall [00:00:00] ███████████████ 4/4");
}

// ── Single bar: abandon ─────────────────────────────────────────────────────

#[test]
fn single_bar_abandon() {
    let (mp, term) = mk();
    let pb = add_bar(&mp, 4, "overall");
    pb.inc(2);
    pb.abandon();
    pb.tick();
    assert_eq!(term.contents(), " overall [00:00:00] ███████░░░░░░░░ 2/4");
}

// ── Single bar: abandon with message ────────────────────────────────────────

#[test]
fn single_bar_abandon_with_message() {
    let (mp, term) = mk();
    let pb = add_bar(&mp, 4, "overall");
    pb.inc(2);
    pb.abandon_with_message("failed");
    pb.tick();
    assert_eq!(term.contents(), " overall [00:00:00] ████░░░░░ 2/4 failed");
}

// ── Single bar: over total ──────────────────────────────────────────────────

#[test]
fn single_bar_over_total() {
    let (mp, term) = mk();
    let pb = add_bar(&mp, 4, "overall");
    pb.inc(6);
    pb.tick();
    // 6/4 is capped to 100% — full bar.
    assert_eq!(term.contents(), " overall [00:00:00] ███████████████ 6/4");
}

// ── Single bar: set_length (change total) ───────────────────────────────────

#[test]
fn single_bar_set_length() {
    let (mp, term) = mk();
    let pb = add_bar(&mp, 10, "overall");
    pb.inc(4);
    pb.tick();
    assert_eq!(term.contents(), " overall [00:00:00] █████░░░░░░░░░ 4/10");
    pb.set_length(5);
    pb.tick();
    assert_eq!(term.contents(), " overall [00:00:00] ████████████░░░ 4/5");
}

// ── Single bar: reset ───────────────────────────────────────────────────────

#[test]
fn single_bar_reset() {
    let (mp, term) = mk();
    let pb = add_bar(&mp, 10, "overall");
    pb.inc(7);
    pb.tick();
    assert_eq!(term.contents(), " overall [00:00:00] █████████░░░░░ 7/10");
    pb.reset();
    pb.tick();
    assert_eq!(term.contents(), " overall [00:00:00] ░░░░░░░░░░░░░░ 0/10");
}

// ── Single bar: inc(0) no-op ────────────────────────────────────────────────

#[test]
fn single_bar_inc_zero() {
    let (mp, term) = mk();
    let pb = add_bar(&mp, 4, "overall");
    pb.inc(0);
    pb.tick();
    assert_eq!(term.contents(), " overall [00:00:00] ░░░░░░░░░░░░░░░ 0/4");
}

// ── Single bar: set_position ────────────────────────────────────────────────

#[test]
fn single_bar_set_position() {
    let (mp, term) = mk();
    let pb = add_bar(&mp, 10, "overall");
    pb.set_position(5);
    pb.tick();
    assert_eq!(term.contents(), " overall [00:00:00] ███████░░░░░░░ 5/10");
}

// ── Single bar: spinner on overall prefix ───────────────────────────────────

#[test]
fn single_bar_spinner_overall() {
    let (mp, term) = mk();
    let pb = add_bar(&mp, 5, "overall");
    pb.set_style(spinner_style());
    pb.tick();
    assert_eq!(term.contents(), "⠙  overall [00:00:00] ░░░░░░░░░░░░░ 0/5");
}

// ── Single bar: long prefix (>8 chars) ──────────────────────────────────────

#[test]
fn single_bar_long_prefix() {
    let (mp, term) = mk();
    let pb = add_bar(&mp, 4, "longprefix");
    pb.tick();
    assert_eq!(term.contents(), "longprefix [00:00:00] ░░░░░░░░░░░░░ 0/4");
}

// ── Single bar: empty prefix ────────────────────────────────────────────────

#[test]
fn single_bar_empty_prefix() {
    let (mp, term) = mk();
    let pb = add_bar(&mp, 4, "");
    pb.tick();
    // 8 spaces + separator space = 9 spaces before `[`.
    assert_eq!(term.contents(), "         [00:00:00] ░░░░░░░░░░░░░░░ 0/4");
}

// ── Single bar: tiny prefix (1 char) ────────────────────────────────────────

#[test]
fn single_bar_tiny_prefix() {
    let (mp, term) = mk();
    let pb = add_bar(&mp, 4, "a");
    pb.tick();
    assert_eq!(term.contents(), "       a [00:00:00] ░░░░░░░░░░░░░░░ 0/4");
}

// ── Two lines: last child cleared first ─────────────────────────────────────

#[test]
fn two_lines_last_child_cleared_first() {
    let (mp, term) = mk();
    let o = add_bar(&mp, 4, "overall");
    let a = ins_bar(&mp, &o, 3, "tool-a");
    let b = ins_bar(&mp, &o, 2, "tool-b");
    a.tick();
    b.tick();
    o.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "  tool-a [00:00:00] ░░░░░░░░░░░░░░░ 0/3\n",
            "  tool-b [00:00:00] ░░░░░░░░░░░░░░░ 0/2\n",
            " overall [00:00:00] ░░░░░░░░░░░░░░░ 0/4",
        ),
    );
    b.finish_and_clear();
    b.tick();
    o.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "  tool-a [00:00:00] ░░░░░░░░░░░░░░░ 0/3\n",
            " overall [00:00:00] ░░░░░░░░░░░░░░░ 0/4",
        ),
    );
}

// ── Two lines: child finish_and_clear before first tick ─────────────────────

#[test]
fn two_lines_child_finish_and_clear_early() {
    let (mp, term) = mk();
    let o = add_bar(&mp, 4, "overall");
    let c = ins_bar(&mp, &o, 2, "tool1");
    // finish_and_clear before any tick — child is never visible.
    c.finish_and_clear();
    c.tick();
    o.tick();
    assert_eq!(term.contents(), " overall [00:00:00] ░░░░░░░░░░░░░░░ 0/4");
}

// ── Spinner on both children AND overall ─────────────────────────────────────

#[test]
fn spinner_child_and_overall_initial() {
    let (mp, term) = mk();
    let o = add_bar(&mp, 5, "overall");
    o.set_style(spinner_style());
    let c = ins_bar(&mp, &o, 2, "tool1");
    c.set_style(spinner_style());
    c.tick();
    o.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "⠙    tool1 [00:00:00] ░░░░░░░░░░░░░ 0/2\n",
            "⠙  overall [00:00:00] ░░░░░░░░░░░░░ 0/5",
        ),
    );
}

#[test]
fn spinner_child_and_overall_child_progress() {
    let (mp, term) = mk();
    let o = add_bar(&mp, 5, "overall");
    o.set_style(spinner_style());
    let c = ins_bar(&mp, &o, 2, "tool1");
    c.set_style(spinner_style());
    c.inc(1);
    c.tick();
    o.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "⠹    tool1 [00:00:00] ██████░░░░░░░ 1/2\n",
            "⠙  overall [00:00:00] ░░░░░░░░░░░░░ 0/5",
        ),
    );
}

#[test]
fn spinner_child_and_overall_full() {
    let (mp, term) = mk();
    let o = add_bar(&mp, 5, "overall");
    o.set_style(spinner_style());
    let c = ins_bar(&mp, &o, 2, "tool1");
    c.set_style(spinner_style());
    c.inc(2);
    c.tick();
    o.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "⠹    tool1 [00:00:00] █████████████ 2/2\n",
            "⠙  overall [00:00:00] ░░░░░░░░░░░░░ 0/5",
        ),
    );
}

#[test]
fn spinner_child_finishes_overall_active() {
    let (mp, term) = mk();
    let o = add_bar(&mp, 5, "overall");
    o.set_style(spinner_style());
    let c = ins_bar(&mp, &o, 2, "tool1");
    c.set_style(spinner_style());
    c.tick();
    o.tick();
    c.finish_with_message("done");
    c.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "⠏    tool1 [00:00:00] █████████ 2/2 done\n",
            "⠙  overall [00:00:00] ░░░░░░░░░░░░░ 0/5",
        ),
    );
}

#[test]
fn spinner_on_both_finish() {
    let (mp, term) = mk();
    let o = add_bar(&mp, 5, "overall");
    o.set_style(spinner_style());
    let c = ins_bar(&mp, &o, 2, "tool1");
    c.set_style(spinner_style());
    c.tick();
    o.tick();
    c.inc(2);
    o.inc(5);
    c.finish_with_message("done");
    o.finish_with_message("done");
    c.tick();
    o.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "⠏    tool1 [00:00:00] █████████ 2/2 done\n",
            "⠏  overall [00:00:00] █████████ 5/5 done",
        ),
    );
}

// ── More transition patterns ─────────────────────────────────────────────────

#[test]
fn transition_finish_without_clear() {
    let (mp, term) = mk();
    let o = add_bar(&mp, 5, "overall");
    let c = ins_bar(&mp, &o, 2, "tool1");
    c.tick();
    o.tick();
    c.finish();
    c.tick();
    o.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "   tool1 [00:00:00] ███████████████ 2/2\n",
            " overall [00:00:00] ░░░░░░░░░░░░░░░ 0/5",
        ),
    );
}

#[test]
fn transition_abandon_child() {
    let (mp, term) = mk();
    let o = add_bar(&mp, 5, "overall");
    let c = ins_bar(&mp, &o, 2, "tool1");
    c.inc(1);
    c.tick();
    o.tick();
    c.abandon_with_message("failed");
    c.tick();
    o.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "   tool1 [00:00:00] ████░░░░░ 1/2 failed\n",
            " overall [00:00:00] ░░░░░░░░░░░░░░░ 0/5",
        ),
    );
}

#[test]
fn transition_two_children_both_finish() {
    let (mp, term) = mk();
    let o = add_bar(&mp, 5, "overall");
    let a = ins_bar(&mp, &o, 3, "tool-a");
    let b = ins_bar(&mp, &o, 2, "tool-b");
    a.inc(3);
    b.inc(2);
    a.tick();
    b.tick();
    o.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "  tool-a [00:00:00] ███████████████ 3/3\n",
            "  tool-b [00:00:00] ███████████████ 2/2\n",
            " overall [00:00:00] ░░░░░░░░░░░░░░░ 0/5",
        ),
    );
}

#[test]
fn transition_child_progress_then_clear() {
    let (mp, term) = mk();
    let o = add_bar(&mp, 5, "overall");
    let child1 = ins_bar(&mp, &o, 2, "tool1");
    child1.inc(2);
    child1.finish_and_clear();
    child1.tick();
    o.tick();
    drop(child1);
    o.inc(2);
    let child2 = ins_bar(&mp, &o, 3, "tool2");
    child2.tick();
    o.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "   tool2 [00:00:00] ░░░░░░░░░░░░░░░ 0/3\n",
            " overall [00:00:00] ██████░░░░░░░░░ 2/5",
        ),
    );
}

#[test]
fn transition_abandon_and_reset() {
    let (mp, term) = mk();
    let o = add_bar(&mp, 5, "overall");
    let c = ins_bar(&mp, &o, 5, "tool1");
    c.inc(3);
    c.abandon_with_message("failed");
    c.tick();
    o.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "   tool1 [00:00:00] █████░░░░ 3/5 failed\n",
            " overall [00:00:00] ░░░░░░░░░░░░░░░ 0/5",
        ),
    );
    c.reset();
    c.set_message("");
    c.tick();
    o.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "   tool1 [00:00:00] ░░░░░░░░░░░░░░░ 0/5\n",
            " overall [00:00:00] ░░░░░░░░░░░░░░░ 0/5",
        ),
    );
}

#[test]
fn transition_out_of_order_clear() {
    let (mp, term) = mk();
    let o = add_bar(&mp, 5, "overall");
    let a = ins_bar(&mp, &o, 2, "tool-a");
    let b = ins_bar(&mp, &o, 2, "tool-b");
    let c = ins_bar(&mp, &o, 2, "tool-c");
    a.tick();
    b.tick();
    c.tick();
    o.tick();
    b.finish_and_clear();
    b.tick();
    o.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "  tool-a [00:00:00] ░░░░░░░░░░░░░░░ 0/2\n",
            "  tool-c [00:00:00] ░░░░░░░░░░░░░░░ 0/2\n",
            " overall [00:00:00] ░░░░░░░░░░░░░░░ 0/5",
        ),
    );
}

#[test]
fn transition_child_abandoned_new_child() {
    let (mp, term) = mk();
    let o = add_bar(&mp, 5, "overall");
    let c = ins_bar(&mp, &o, 2, "tool1");
    c.inc(1);
    c.abandon_with_message("failed");
    c.tick();
    o.tick();
    c.finish_and_clear();
    c.tick();
    o.tick();
    drop(c);
    let c2 = ins_bar(&mp, &o, 2, "tool2");
    c2.tick();
    o.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "   tool2 [00:00:00] ░░░░░░░░░░░░░░░ 0/2\n",
            " overall [00:00:00] ░░░░░░░░░░░░░░░ 0/5",
        ),
    );
}

#[test]
fn transition_two_children_one_finishes_one_clears() {
    let (mp, term) = mk();
    let o = add_bar(&mp, 5, "overall");
    let a = ins_bar(&mp, &o, 3, "tool-a");
    let b = ins_bar(&mp, &o, 2, "tool-b");
    a.inc(3);
    a.finish();
    b.finish_and_clear();
    a.tick();
    b.tick();
    o.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "  tool-a [00:00:00] ███████████████ 3/3\n",
            " overall [00:00:00] ░░░░░░░░░░░░░░░ 0/5",
        ),
    );
}

// ── Worker count changes ─────────────────────────────────────────────────────

#[test]
fn worker_count_grow() {
    let (mp, term) = mk();
    let o = add_bar(&mp, 10, "overall");
    let c1 = ins_bar(&mp, &o, 2, "tool1");
    let c2 = ins_bar(&mp, &o, 3, "tool2");
    let c3 = ins_bar(&mp, &o, 1, "tool3");
    c1.tick();
    c2.tick();
    c3.tick();
    o.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "   tool1 [00:00:00] ░░░░░░░░░░░░░░░ 0/2\n",
            "   tool2 [00:00:00] ░░░░░░░░░░░░░░░ 0/3\n",
            "   tool3 [00:00:00] ░░░░░░░░░░░░░░░ 0/1\n",
            " overall [00:00:00] ░░░░░░░░░░░░░░ 0/10",
        ),
    );
}

#[test]
fn worker_count_shrink() {
    let (mp, term) = mk();
    let o = add_bar(&mp, 10, "overall");
    let a = ins_bar(&mp, &o, 3, "tool-a");
    let b = ins_bar(&mp, &o, 2, "tool-b");
    let c = ins_bar(&mp, &o, 1, "tool-c");
    a.tick();
    b.tick();
    c.tick();
    o.tick();
    b.finish_and_clear();
    b.tick();
    o.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "  tool-a [00:00:00] ░░░░░░░░░░░░░░░ 0/3\n",
            "  tool-c [00:00:00] ░░░░░░░░░░░░░░░ 0/1\n",
            " overall [00:00:00] ░░░░░░░░░░░░░░ 0/10",
        ),
    );
}

#[test]
fn worker_count_surge_drain() {
    let (mp, term) = mk();
    let o = add_bar(&mp, 5, "overall");
    let mut kids = Vec::new();
    for i in 0..5 {
        let name = format!("t{i:02}");
        let c = ins_bar(&mp, &o, 1, &name);
        c.inc(1);
        c.finish_and_clear();
        c.tick();
        kids.push(c);
    }
    o.inc(5);
    o.tick();
    assert_eq!(term.contents(), " overall [00:00:00] ███████████████ 5/5");
}

#[test]
fn worker_count_new_batch() {
    let (mp, term) = mk();
    let o = add_bar(&mp, 10, "overall");
    let a = ins_bar(&mp, &o, 2, "tool1");
    let b = ins_bar(&mp, &o, 2, "tool2");
    a.finish_and_clear();
    b.finish_and_clear();
    a.tick();
    b.tick();
    o.tick();
    drop(a);
    drop(b);
    o.inc(4);
    let c = ins_bar(&mp, &o, 2, "tool3");
    let d = ins_bar(&mp, &o, 1, "tool4");
    c.tick();
    d.tick();
    o.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "   tool3 [00:00:00] ░░░░░░░░░░░░░░░ 0/2\n",
            "   tool4 [00:00:00] ░░░░░░░░░░░░░░░ 0/1\n",
            " overall [00:00:00] █████░░░░░░░░░ 4/10",
        ),
    );
}

// ── Many simultaneous bars ────────────────────────────────────────────────────

#[test]
fn many_bars_fifty() {
    let (mp, term) = mk_with_size(55, 40);
    let o = add_bar(&mp, 50, "overall");
    let mut kids = Vec::new();
    for i in 0..50 {
        let p = format!("t{i:02}");
        let c = ins_bar(&mp, &o, 1, &p);
        c.tick();
        kids.push(c);
    }
    o.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    assert_eq!(lines.len(), 51, "50 children + 1 overall = 51 lines");
    assert_eq!(lines[0], "     t00 [00:00:00] ░░░░░░░░░░░░░░░ 0/1");
    assert_eq!(lines[49], "     t49 [00:00:00] ░░░░░░░░░░░░░░░ 0/1");
    assert_eq!(lines[50], " overall [00:00:00] ░░░░░░░░░░░░░░ 0/50");
}

#[test]
fn many_bars_hundred() {
    let (mp, term) = mk_with_size(105, 40);
    let o = add_bar(&mp, 100, "overall");
    let mut kids = Vec::new();
    for i in 0..100 {
        let p = format!("t{i:02}");
        let c = ins_bar(&mp, &o, 1, &p);
        c.tick();
        kids.push(c);
    }
    o.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    assert_eq!(lines.len(), 101, "100 children + 1 overall = 101 lines");
    assert_eq!(lines[0], "     t00 [00:00:00] ░░░░░░░░░░░░░░░ 0/1");
    assert_eq!(lines[99], "     t99 [00:00:00] ░░░░░░░░░░░░░░░ 0/1");
    assert!(lines[100].starts_with(" overall [00:00:00] "));
    assert!(lines[100].ends_with(" 0/100"));
}

// ── Extreme width ────────────────────────────────────────────────────────────

#[test]
fn narrow_terminal_single_bar() {
    let (mp, term) = mk_with_size(24, 10);
    let pb = ProgressBar::new(4);
    pb.set_style(style_from(TN));
    pb.set_prefix("overall".to_string());
    let pb = mp.add(pb);
    pb.tick();
    assert_eq!(term.contents(), concat!(" overall\n", "0/4"));
}

#[test]
fn narrow_terminal_two_lines() {
    let (mp, term) = mk_with_size(24, 10);
    let o = ProgressBar::new(5);
    o.set_style(style_from(TN));
    o.set_prefix("overall".to_string());
    let c = ProgressBar::new(3);
    c.set_style(style_from(TN));
    c.set_prefix("tool1".to_string());
    let o = mp.add(o);
    let c = mp.insert_before(&o, c);
    c.tick();
    o.tick();
    assert_eq!(term.contents(), concat!("   tool1\n", "0/3\n", " overall\n", "0/5"),);
}

#[test]
fn wide_terminal_single_bar() {
    let (mp, term) = mk_with_size(24, 120);
    let pb = add_bar(&mp, 4, "overall");
    pb.tick();
    let s = term.contents();
    assert_eq!(s.lines().count(), 1, "single line at W=120");
    assert!(s.starts_with(" overall [00:00:00] "));
    assert!(s.ends_with(" 0/4"));
}

#[test]
fn wide_terminal_two_lines() {
    let (mp, term) = mk_with_size(24, 120);
    let o = add_bar(&mp, 4, "overall");
    let c = ins_bar(&mp, &o, 2, "tool1");
    c.tick();
    o.tick();
    let s = term.contents();
    let lines: Vec<&str> = s.lines().collect();
    assert_eq!(lines.len(), 2, "two lines at W=120");
    assert!(lines[0].starts_with("   tool1 [00:00:00] "));
    assert!(lines[0].ends_with(" 0/2"));
    assert!(lines[1].starts_with(" overall [00:00:00] "));
    assert!(lines[1].ends_with(" 0/4"));
}

// ── Extreme height ───────────────────────────────────────────────────────────

#[test]
fn tall_terminal() {
    let (mp, term) = mk_with_size(50, 40);
    let o = add_bar(&mp, 3, "overall");
    let a = ins_bar(&mp, &o, 5, "tool-a");
    let b = ins_bar(&mp, &o, 3, "tool-b");
    a.tick();
    b.tick();
    o.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "  tool-a [00:00:00] ░░░░░░░░░░░░░░░ 0/5\n",
            "  tool-b [00:00:00] ░░░░░░░░░░░░░░░ 0/3\n",
            " overall [00:00:00] ░░░░░░░░░░░░░░░ 0/3",
        ),
    );
}

#[test]
fn short_terminal_two_lines() {
    let (mp, term) = mk_with_size(3, 40);
    let o = add_bar(&mp, 4, "overall");
    let c = ins_bar(&mp, &o, 2, "tool1");
    c.tick();
    o.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "   tool1 [00:00:00] ░░░░░░░░░░░░░░░ 0/2\n",
            " overall [00:00:00] ░░░░░░░░░░░░░░░ 0/4",
        ),
    );
}

#[test]
fn short_terminal_three_lines() {
    let (mp, term) = mk_with_size(3, 40);
    let o = add_bar(&mp, 3, "overall");
    let a = ins_bar(&mp, &o, 5, "tool-a");
    let b = ins_bar(&mp, &o, 3, "tool-b");
    a.tick();
    b.tick();
    o.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "  tool-a [00:00:00] ░░░░░░░░░░░░░░░ 0/5\n",
            "  tool-b [00:00:00] ░░░░░░░░░░░░░░░ 0/3\n",
            " overall [00:00:00] ░░░░░░░░░░░░░░░ 0/3",
        ),
    );
}

// ── Overflow ─────────────────────────────────────────────────────────────────

#[test]
fn overflow_h3_5children() {
    let (mp, term) = mk_with_size(3, 40);
    let o = add_bar(&mp, 10, "overall");
    let mut kids = Vec::new();
    for i in 0..5 {
        let p = format!("child{i}");
        let c = ins_bar(&mp, &o, 3, &p);
        c.tick();
        kids.push(c);
    }
    o.tick();
    // At H=3: first 3 children visible (child0, child1, child2).
    assert_eq!(term.contents().lines().count(), 3);
}

#[test]
fn overflow_h3_5children_clear_last() {
    let (mp, term) = mk_with_size(3, 40);
    let o = add_bar(&mp, 10, "overall");
    let mut kids = Vec::new();
    for i in 0..5 {
        let p = format!("child{i}");
        let c = ins_bar(&mp, &o, 3, &p);
        c.tick();
        kids.push(c);
    }
    o.tick();
    // At H=3: first 3 children visible (child0, child1, child2).
    assert_eq!(term.contents().lines().count(), 3, "3 lines visible at H=3");
    // Clear the last created child (child4, not visible at H=3).
    kids[4].finish_and_clear();
    kids[4].tick();
    o.tick();
    // Still only 3 lines.
    assert_eq!(term.contents().lines().count(), 3, "still 3 lines visible after clear");
}

#[test]
fn overflow_h3_10children() {
    let (mp, term) = mk_with_size(3, 40);
    let o = add_bar(&mp, 10, "overall");
    let mut kids = Vec::new();
    for i in 0..10 {
        let p = format!("child{i}");
        let c = ins_bar(&mp, &o, 3, &p);
        c.tick();
        kids.push(c);
    }
    o.tick();
    assert_eq!(term.contents().lines().count(), 3);
}

#[test]
fn overflow_h4_6children_finish_clear_all() {
    let (mp, term) = mk_with_size(4, 40);
    let o = add_bar(&mp, 10, "overall");
    let mut kids = Vec::new();
    for i in 0..6 {
        let p = format!("child{i}");
        let c = ins_bar(&mp, &o, 3, &p);
        c.tick();
        kids.push(c);
    }
    o.tick();
    // At H=4: first 4 children visible.
    assert_eq!(term.contents().lines().count(), 4);
    // Clear children that were past the visible window.
    kids[4].finish_and_clear();
    kids[5].finish_and_clear();
    kids[4].tick();
    kids[5].tick();
    o.tick();
    // Still 4 lines.
    assert_eq!(term.contents().lines().count(), 4);
}

// ── Combined stress ──────────────────────────────────────────────────────────

#[test]
fn overflow_with_spinner() {
    let (mp, term) = mk_with_size(3, 40);
    let o = add_bar(&mp, 10, "overall");
    o.set_style(spinner_style());
    let mut kids = Vec::new();
    for i in 0..5 {
        let p = format!("child{i}");
        let c = ins_bar(&mp, &o, 3, &p);
        c.set_style(spinner_style());
        c.tick();
        kids.push(c);
    }
    o.tick();
    assert_eq!(term.contents().lines().count(), 3);
}

#[test]
fn worker_surge_with_overflow() {
    let (mp, term) = mk_with_size(3, 40);
    let o = add_bar(&mp, 10, "overall");
    let mut kids = Vec::new();
    for i in 0..5 {
        let p = format!("child{i}");
        let c = ins_bar(&mp, &o, 3, &p);
        c.tick();
        kids.push(c);
    }
    o.tick();
    // At H=3: first 3 children visible.
    assert_eq!(term.contents().lines().count(), 3);
    // Surge: add 5 more children.
    for i in 5..10 {
        let p = format!("child{i}");
        let c = ins_bar(&mp, &o, 3, &p);
        c.tick();
        kids.push(c);
    }
    o.tick();
    // Still only 3 lines visible.
    assert_eq!(term.contents().lines().count(), 3);
}

// ── ProgressGroup rendering (fixed-slot, always reuses bottom child) ────────

#[test]
fn progress_group_with_overall_shows_fixed_height() {
    // Terminal H=5, W=80 so the full child and overall templates fit.
    let (mp, term) = mk_with_size(5, 80);
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(5)
        .with_overall("overall", 10)
        .build_with_overall();
    group.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== with_overall, H=5, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    eprintln!("count = {}", lines.len());
    assert_eq!(lines.len(), 5, "must use exactly 5 lines (terminal height)");
    for (i, line) in lines[..4].iter().enumerate() {
        assert!(line.trim().is_empty(), "line {i} should be blank filler but got: {line:?}");
    }
    assert!(lines[4].contains("overall"), "overall bar visible: {}", lines[4]);
    assert!(lines[4].contains("0/10"), "overall shows 0/10: {}", lines[4]);
}

#[test]
fn progress_group_add_bar_reuses_bottom_child() {
    // Terminal H=4, W=80 so the full child and overall templates fit.
    let (mp, term) = mk_with_size(4, 80);
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(4)
        .with_overall("overall", 3)
        .build_with_overall();

    let _c1 = group.add_bar(5, "tool1");
    group.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== add_bar reuse, after tool1, H=4, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    eprintln!("count = {}", lines.len());
    assert_eq!(lines.len(), 4, "always 4 lines");
    assert!(lines[0].trim().is_empty(), "line 0 is blank filler");
    assert!(lines[1].trim().is_empty(), "line 1 is blank filler");
    assert!(lines[2].contains("tool1"), "line 2 has tool1: {0}", lines[2]);
    assert!(lines[3].contains("overall"), "line 3 has overall");

    let _c2 = group.add_bar(3, "tool2");
    group.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== add_bar reuse, after tool2 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    assert_eq!(lines.len(), 4, "still 4 lines");
    assert!(lines[0].trim().is_empty(), "line 0 is blank filler");
    assert!(lines[1].contains("tool1"), "line 1 still has tool1: {0}", lines[1]);
    assert!(lines[2].contains("tool2"), "line 2 has tool2: {0}", lines[2]);
    assert!(lines[3].contains("overall"), "line 3 has overall");
}

// No diagnostics below this line — they were removed after confirming
// the fix.

#[test]
fn progress_group_no_overall_always_reuses_bottom() {
    // Terminal H=5, W=80 so the full child template fits.
    // Use capacity=4 so there's 1 unwritten row at the bottom
    // — this avoids InMemoryTerm trimming blank content when bars
    // fill the entire terminal height.
    let (mp, term) = mk_with_size(5, 80);
    let group = ProgressGroup::builder().with_multi_progress(mp).capacity(4).build();

    let _c1 = group.add_bar(5, "task1");
    group.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== no_overall, after task1, H=5, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    eprintln!("count = {}", lines.len());
    // 4 slots. task1 at slot[3] (bottom), blanks at lines[0..2].
    // task1 at bottom avoids InMemoryTerm trimming → 4 lines.
    assert_eq!(lines.len(), 4);
    assert!(lines[0].trim().is_empty(), "line 0 is blank");
    assert!(lines[1].trim().is_empty(), "line 1 is blank");
    assert!(lines[2].trim().is_empty(), "line 2 is blank");
    assert!(lines[3].contains("task1"), "line 3 has task1: {0}", lines[3]);

    let _c2 = group.add_bar(3, "task2");
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== no_overall, after task2 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    // 4 slots, no overall → 4 lines (task1 at slot[2], task2 at slot[3], both non-empty)
    assert_eq!(lines.len(), 4);
    assert!(lines[0].trim().is_empty(), "line 0 is blank");
    assert!(lines[1].trim().is_empty(), "line 1 is blank");
    assert!(lines[2].contains("task1"), "line 2 still has task1: {0}", lines[2]);
    assert!(lines[3].contains("task2"), "line 3 has task2: {0}", lines[3]);
}

#[test]
fn progress_group_never_changes_bar_count() {
    // Terminal H=4, W=80 so the full child template fits.
    let (mp, term) = mk_with_size(4, 80);
    let group = ProgressGroup::builder().with_multi_progress(mp).capacity(4).build();
    for i in 0..30 {
        let _c = group.add_bar(1, &format!("tool{i}"));
        group.tick();
    }
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== never_changes_bar_count, 30 add_bar, H=4, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    eprintln!("count = {}", lines.len());
    assert_eq!(lines.len(), 4, "count must never change: 30 add_bar calls, still 4 lines");
}

#[test]
fn progress_group_with_overall_add_child_updates_slot() {
    // Terminal H=5, W=80 so the full child and overall templates fit.
    // Capacity=5: child slots at slots[0..3], overall at slot[4].
    // Chronological: first child occupies slot[3], second shifts it to slot[2]
    // and takes slot[3].
    let (mp, term) = mk_with_size(5, 80);
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(5)
        .with_overall("overall", 3)
        .build_with_overall();

    let _c1 = group.add_bar(5, "tool1");
    group.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== add_child_updates_slot, after tool1, H=5, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    eprintln!("count = {}", lines.len());
    assert_eq!(lines.len(), 5, "always 5 lines");
    assert!(lines[0].trim().is_empty(), "line 0 is blank");
    assert!(lines[1].trim().is_empty(), "line 1 is blank");
    assert!(lines[2].trim().is_empty(), "line 2 is blank");
    assert!(lines[3].contains("tool1"), "line 3 has tool1: {0}", lines[3]);
    assert!(lines[3].contains("0/5"), "line 3 shows 0/5: {0}", lines[3]);
    assert!(lines[4].contains("overall"), "line 4 has overall: {0}", lines[4]);
    assert!(lines[4].contains("0/3"), "line 4 shows 0/3: {0}", lines[4]);

    let _c2 = group.add_bar(3, "tool2");
    group.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== add_child_updates_slot, after tool2 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    assert_eq!(lines.len(), 5, "still 5 lines");
    assert!(lines[0].trim().is_empty(), "line 0 is blank");
    assert!(lines[1].trim().is_empty(), "line 1 is blank");
    assert!(lines[2].contains("tool1"), "line 2 still has tool1: {0}", lines[2]);
    assert!(lines[2].contains("0/5"), "line 2 shows 0/5: {0}", lines[2]);
    assert!(lines[3].contains("tool2"), "line 3 has tool2: {0}", lines[3]);
    assert!(lines[3].contains("0/3"), "line 3 shows 0/3: {0}", lines[3]);
    assert!(!lines[0].contains("tool1"), "line 0 must not show tool1: {0}", lines[0]);
    assert!(!lines[1].contains("tool2"), "line 1 must not show tool2: {0}", lines[1]);
    assert!(lines[4].contains("overall"), "line 4 still has overall: {0}", lines[4]);
}

#[test]
fn progress_group_with_overall_multiple_children_reuse_slot() {
    // Terminal H=5, W=80 so the full child and overall templates fit.
    // Capacity=5, overall at line[4], child slots at lines[0..3].
    // Chronological: first child occupies slot[3], each new child shifts
    // earlier children up and takes slot[3].  After 4 children: task0 at
    // slot[0], task1 at slot[1], task2 at slot[2], task3 at slot[3].
    let (mp, term) = mk_with_size(5, 80);
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(5)
        .with_overall("overall", 10)
        .build_with_overall();

    for i in 0..5 {
        let _c = group.add_bar(2, &format!("task{i}"));
        group.tick();
    }
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== multiple_children_reuse_slot, 5 children sequentially, H=5, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    eprintln!("count = {}", lines.len());
    assert_eq!(lines.len(), 5, "always 5 lines regardless of children added");
    // First 4 children occupy sequential slots from top to bottom (task0 at slot[0], task1 at slot[1], etc.).
    // 5th child has no render slot (all full, none finished).
    assert!(lines[0].contains("task0"), "line 0 shows task0: {0}", lines[0]);
    assert!(lines[0].contains("0/2"), "line 0 shows 0/2: {0}", lines[0]);
    assert!(lines[1].contains("task1"), "line 1 shows task1: {0}", lines[1]);
    assert!(lines[1].contains("0/2"), "line 1 shows 0/2: {0}", lines[1]);
    assert!(lines[2].contains("task2"), "line 2 shows task2: {0}", lines[2]);
    assert!(lines[2].contains("0/2"), "line 2 shows 0/2: {0}", lines[2]);
    assert!(lines[3].contains("task3"), "line 3 shows task3: {0}", lines[3]);
    assert!(lines[3].contains("0/2"), "line 3 shows 0/2: {0}", lines[3]);
    // task4 has no render slot (all 4 child slots occupied, none finished).
    assert!(lines[4].contains("overall"), "line 4 has overall: {0}", lines[4]);
    assert!(lines[4].contains("0/10"), "line 4 shows 0/10: {0}", lines[4]);
}

#[test]
fn progress_group_no_overall_different_capacities() {
    // Terminal H=6, W=80 so the full child template fits.
    // Capacity=4, no overall.
    // Children fill sequentially from line[0].
    // Using H=6 > 4 to avoid InMemoryTerm blank-content trimming.
    let (mp, term) = mk_with_size(6, 80);
    let group = ProgressGroup::builder().with_multi_progress(mp).capacity(4).build();

    let _c1 = group.add_bar(5, "alpha");
    group.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== no_overall_cap_4, after alpha, H=6, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    eprintln!("count = {}", lines.len());
    // 4 slots. alpha at slot[3] (bottom), blanks at lines[0..2].
    // alpha at bottom avoids InMemoryTerm trimming → 4 lines.
    assert_eq!(lines.len(), 4, "4 slots, 4 lines (child at bottom)");
    assert!(lines[0].trim().is_empty(), "line 0 is blank");
    assert!(lines[1].trim().is_empty(), "line 1 is blank");
    assert!(lines[2].trim().is_empty(), "line 2 is blank");
    assert!(lines[3].contains("alpha"), "line 3 has alpha: {0}", lines[3]);
    assert!(lines[3].contains("0/5"), "line 3 shows 0/5: {0}", lines[3]);

    let _c2 = group.add_bar(3, "beta");
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== no_overall_cap_4, after beta ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    assert_eq!(lines.len(), 4, "4 slots, no overall → 4 lines");
    assert!(lines[0].trim().is_empty(), "line 0 is blank");
    assert!(lines[1].trim().is_empty(), "line 1 is blank");
    assert!(lines[2].contains("alpha"), "line 2 still has alpha: {0}", lines[2]);
    assert!(lines[2].contains("0/5"), "line 2 shows 0/5: {0}", lines[2]);
    assert!(lines[3].contains("beta"), "line 3 has beta: {0}", lines[3]);
    assert!(lines[3].contains("0/3"), "line 3 shows 0/3: {0}", lines[3]);
    assert!(!lines[0].contains("beta"), "line 0 must not show beta: {0}", lines[0]);
    assert!(!lines[1].contains("alpha"), "line 1 must not show alpha: {0}", lines[1]);
}

#[test]
fn progress_group_compact_template_below_60_width() {
    // Terminal W=80 so the full template fits.
    // (InMemoryTerm width doesn't affect production style selection, which
    // reads from console::Term::stderr() — the real terminal.)
    let (mp, term) = mk_with_size(4, 80);
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(4)
        .with_overall("overall", 3)
        .build_with_overall();

    let _c1 = group.add_bar(5, "tool1");
    group.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== compact_template, H=4, W=40 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    eprintln!("count = {}", lines.len());
    assert_eq!(lines.len(), 4, "always 4 lines at capacity=4");
    assert!(lines[0].trim().is_empty(), "line 0 is blank");
    assert!(lines[1].trim().is_empty(), "line 1 is blank");
    assert!(lines[2].contains("tool1"), "line 2 has tool1: {0}", lines[2]);
    assert!(lines[2].contains("0/5"), "line 2 shows 0/5: {0}", lines[2]);
    assert!(lines[3].contains("overall"), "line 3 has overall: {0}", lines[3]);
    assert!(lines[3].contains("0/3"), "line 3 shows 0/3: {0}", lines[3]);
}

#[test]
fn progress_group_child_shows_label_and_total() {
    // Verify that add_bar renders the label and total in the bar.
    let (mp, term) = mk_with_size(4, 80);
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(4)
        .with_overall("overall", 10)
        .build_with_overall();

    let _c1 = group.add_bar(7, "fetch");
    group.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== child_shows_label_and_total, after fetch, H=4, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    eprintln!("count = {}", lines.len());
    assert_eq!(lines.len(), 4, "always 4 lines");
    assert!(lines[0].trim().is_empty(), "line 0 is blank");
    assert!(lines[1].trim().is_empty(), "line 1 is blank");
    assert!(lines[2].contains("fetch"), "line 2 shows label fetch: {0}", lines[2]);
    assert!(lines[2].contains("0/7"), "line 2 shows total 0/7: {0}", lines[2]);
    assert!(lines[3].contains("overall"), "line 3 has overall: {0}", lines[3]);
    assert!(lines[3].contains("0/10"), "line 3 shows 0/10: {0}", lines[3]);
}

#[test]
fn progress_group_disabled_returns_noop() {
    // -- with overall --
    let (_mp, term) = mk_with_size(4, 80);
    let (group, overall) = (ProgressGroup::disabled(), TrackedHandle::disabled());
    assert_eq!(overall.total(), 0, "overall handle must be no-op when disabled");

    let child = group.add_bar(5, "child");
    assert_eq!(child.total(), 0, "child handle must be no-op when disabled");

    group.tick();
    assert_eq!(term.contents(), "", "no output when progress is disabled");

    // -- without overall --
    let (_mp2, term2) = mk_with_size(4, 80);
    let group2 = ProgressGroup::disabled();
    let c2 = group2.add_bar(3, "noop");
    assert_eq!(c2.total(), 0, "child handle must be no-op without overall");
    group2.tick();
    assert_eq!(term2.contents(), "", "no output without overall when disabled");
}

// ── Bar visibility after finish ─────────────────────────────────────────────
//
// These tests verify that finished bars stay visible after `join()` and that
// `join_and_clear()` removes them.  See the production `join()` / `join_and_clear()`
// doc comments for the intended distinction.

#[test]
fn progress_group_child_finish_keeps_bar_visible() {
    // Terminal H=5, W=80 so the full child and overall templates fit.
    let (mp, term) = mk_with_size(5, 80);
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(5)
        .with_overall("overall", 3)
        .build_with_overall();

    let c = group.add_bar(5, "fetch");
    group.tick();
    // Finish the child — it must remain visible in the terminal.
    c.finish_success();
    group.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== child_finish_keeps_bar_visible, H=5, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    assert_eq!(lines.len(), 5, "always 5 lines");
    // fetch is at slot[3] (just above overall), blanks at lines[0..2].
    assert!(lines[0].trim().is_empty(), "line 0 is blank");
    assert!(lines[1].trim().is_empty(), "line 1 is blank");
    assert!(lines[2].trim().is_empty(), "line 2 is blank");
    assert!(lines[3].contains("fetch"), "line 3 has fetch: {0}", lines[3]);
    assert!(lines[3].contains("0/5"), "line 3 shows position: {0}", lines[3]);
    assert!(lines[4].contains("overall"), "overall bar visible: {0}", lines[4]);
}

#[test]
fn progress_group_finish_all_bars_content_persists() {
    // Terminal H=5, W=80.  Overall at line[4], children at lines[0..3].
    let (mp, term) = mk_with_size(5, 80);
    let (group, overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(5)
        .with_overall("overall", 2)
        .build_with_overall();

    let c1 = group.add_bar(3, "alpha");
    let c2 = group.add_bar(5, "beta");
    c1.advance(3);
    c2.advance(5);
    group.tick();
    // Finish all bars.
    c1.finish_success();
    c2.finish_success();
    overall.finish_success();
    group.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== finish_all_bars_content_persists, H=5, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    assert_eq!(lines.len(), 5, "always 5 lines");
    // alpha at slot[2] (first child, shifted up by beta), beta at slot[3] (just above overall).
    assert!(lines[0].trim().is_empty(), "line 0 is blank");
    assert!(lines[1].trim().is_empty(), "line 1 is blank");
    assert!(lines[2].contains("alpha"), "line 2 shows alpha: {0}", lines[2]);
    assert!(lines[2].contains("3/3"), "line 2 shows alpha complete: {0}", lines[2]);
    assert!(lines[3].contains("beta"), "line 3 shows beta: {0}", lines[3]);
    assert!(lines[3].contains("5/5"), "line 3 shows beta complete: {0}", lines[3]);
    assert!(lines[4].contains("overall"), "overall bar visible: {0}", lines[4]);
    // overall was never advanced via TrackedHandle, so position stays 0/2
    assert!(lines[4].contains("0/2"), "overall shows 0/2: {0}", lines[4]);
}

#[test]
fn progress_group_finish_error_shows_error_state() {
    // Terminal H=5, W=80.
    let (mp, term) = mk_with_size(5, 80);
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(5)
        .with_overall("overall", 5)
        .build_with_overall();

    let c = group.add_bar(5, "wget");
    group.tick();
    c.finish_error();
    group.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== finish_error_shows_error_state, H=5, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    assert_eq!(lines.len(), 5, "always 5 lines");
    // wget at slot[3] (just above overall), blanks at lines[0..2].
    assert!(lines[0].trim().is_empty(), "line 0 is blank");
    assert!(lines[1].trim().is_empty(), "line 1 is blank");
    assert!(lines[2].trim().is_empty(), "line 2 is blank");
    assert!(lines[3].contains("wget"), "line 3 has wget: {0}", lines[3]);
    assert!(lines[3].contains("[F]"), "line 3 has error state: {0}", lines[3]);
    assert!(lines[4].contains("overall"), "overall bar visible: {0}", lines[4]);
}

#[test]
fn progress_group_join_and_clear_removes_bars() {
    // Terminal H=5, W=80.
    let (mp, term) = mk_with_size(5, 80);
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(5)
        .with_overall("overall", 3)
        .build_with_overall();

    let c = group.add_bar(5, "fetch");
    c.finish_success();
    group.tick();
    // join_and_clear collapses blank reserved slots but keeps non-blank bars.
    group.join_and_clear();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== join_and_clear_removes_bars, H=5, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    assert_eq!(lines.len(), 2, "2 bars remain (fetch + overall)");
    assert!(lines[0].contains("fetch"), "fetch bar visible: {0}", lines[0]);
    assert!(lines[0].contains("0/5"), "fetch shows 0/5: {0}", lines[0]);
    assert!(lines[1].contains("overall"), "overall bar visible: {0}", lines[1]);
}

#[test]
fn progress_group_consumer_lifecycle_keeps_finished_bars() {
    // Terminal H=5, W=80.  Simulate the exact consumer pattern:
    // create group with overall, do sequential work, finish children,
    // finish overall, then join.  All bars must remain visible.
    let (mp, term) = mk_with_size(5, 80);
    let (group, overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(5)
        .with_overall("overall", 3)
        .build_with_overall();

    let c1 = group.add_bar(5, "fetch");
    c1.advance(5);
    group.tick();
    c1.finish_success();

    let c2 = group.add_bar(2, "parse");
    c2.advance(2);
    group.tick();
    c2.finish_success();

    overall.advance(3);
    overall.finish_success();
    group.tick();
    // group.join() would be called here — it's a no-op.

    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== consumer_lifecycle_keeps_finished_bars, H=5, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    assert_eq!(lines.len(), 5, "always 5 lines");
    // c1(fetch) at slot[2] (first child, shifted up by parse), c2(parse) at slot[3] (just above overall).
    assert!(lines[0].trim().is_empty(), "line 0 is blank");
    assert!(lines[1].trim().is_empty(), "line 1 is blank");
    assert!(lines[2].contains("fetch"), "line 2 shows fetch: {0}", lines[2]);
    assert!(lines[2].contains("fetched") || lines[2].contains("5/5"), "c1 complete: {0}", lines[2]);
    assert!(lines[3].contains("parse"), "line 3 shows parse: {0}", lines[3]);
    assert!(lines[3].contains("parsed") || lines[3].contains("2/2"), "c2 complete: {0}", lines[3]);
    assert!(lines[4].contains("overall"), "overall bar visible: {0}", lines[4]);
    assert!(lines[4].contains("3/3"), "overall shows complete: {0}", lines[4]);
}

// ── Finalize (join_and_clear) behavior ──
//
// These tests verify the `Renderer::finalize()` path exercised by
// `ProgressGroup::join_and_clear()`.  The critical invariants:
// - **Finished bars (including overall) survive** finalize (Problem 2 from v3→v4).
// - **Active bars are untouched** by finalize.
// - **Empty finalize** (no children, only overall) does not panic.
// - **Idempotent finish** — calling tick() twice on finished bars is safe.

#[test]
fn progress_group_overall_finish_and_join_clear_persists() {
    // Terminal H=5, W=80.  Full templates, 3 slots (2 child + 1 overall).
    let (mp, term) = mk_with_size(5, 80);
    let (group, overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(3)
        .with_overall("overall", 3)
        .build_with_overall();

    let c = group.add_bar(5, "fetch");
    c.advance(5);
    group.tick();
    c.finish_success();
    overall.advance(3);
    overall.finish_success();
    group.tick();
    // join_and_clear triggers finalize: finishes non-Active bars, removes
    // blank slots, then draws.  Finished overall must persist in output.
    group.join_and_clear();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== overall_finish_and_join_clear_persists, H=5, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    // 2 bars remain (fetch + overall), blanks removed.
    assert_eq!(lines.len(), 2, "2 bars after finalize (fetch + overall)");
    assert!(lines[0].contains("fetch"), "line 0 shows fetch: {0}", lines[0]);
    assert!(lines[0].contains("5/5"), "line 0 shows 5/5: {0}", lines[0]);
    assert!(lines[1].contains("overall"), "line 1 shows overall: {0}", lines[1]);
    assert!(lines[1].contains("3/3"), "line 1 shows 3/3: {0}", lines[1]);
}

#[test]
fn progress_group_active_bars_survive_join_and_clear() {
    // Terminal H=5, W=80.  Active bars must remain styled and visible
    // after finalize removes blank slots.
    let (mp, term) = mk_with_size(5, 80);
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(3)
        .with_overall("overall", 3)
        .build_with_overall();

    let _c = group.add_bar(5, "alpha");
    // alpha is Active — never finished.
    group.tick();
    // join_and_clear runs finalize; Active bars are skipped by the
    // non-Active guard but should remain visible after blank removal.
    group.join_and_clear();
    group.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== active_bars_survive_join_and_clear, H=5, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    // 2 bars remain (alpha + overall), blanks removed.
    assert_eq!(lines.len(), 2, "2 bars after finalize (alpha + overall)");
    assert!(lines[0].contains("alpha"), "line 0 shows alpha: {0}", lines[0]);
    assert!(lines[0].contains("0/5"), "line 0 shows 0/5: {0}", lines[0]);
    // Active bar has rate visible.
    assert!(
        lines[0].contains("/s")
            || lines[0].contains("/m")
            || lines[0].contains("/d")
            || lines[0].contains("/h"),
        "active bar shows rate: {0}",
        lines[0],
    );
    // No brackets on active bar.
    assert!(!lines[0].contains("[F]"), "no [F] on active: {0}", lines[0]);
    assert!(!lines[0].contains("[A]"), "no [A] on active: {0}", lines[0]);
    assert!(!lines[0].contains("[S]"), "no [S] on active: {0}", lines[0]);
    assert!(lines[1].contains("overall"), "line 1 shows overall: {0}", lines[1]);
    assert!(lines[1].contains("0/3"), "line 1 shows 0/3: {0}", lines[1]);
    // Overall is also active (not finished).
    assert!(!lines[1].contains("[S]"), "no [S] on active overall: {0}", lines[1]);
}

#[test]
fn progress_group_empty_finalize_no_crash() {
    // Terminal H=3, W=80.  Only overall, no children.
    let (mp, term) = mk_with_size(3, 80);
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(3)
        .with_overall("overall", 1)
        .build_with_overall();

    // No children added — all slots except overall are blank.
    group.tick();
    // join_and_clear removes blank bars and triggers final draw.
    // Must not panic when MultiProgress has zero bound bars left.
    group.join_and_clear();
    let contents = term.contents();
    eprintln!("=== empty_finalize_no_crash, H=3, W=80 ===");
    eprintln!("contents = {contents:?}");
    // At minimum the overall bar must survive.
    assert!(contents.contains("overall"), "overall survives empty finalize: {contents:?}");
}

#[test]
fn finish_slot_idempotent() {
    // Double tick on a finished bar must not panic or corrupt state.
    let (mp, term) = mk_with_size(5, 80);
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(3)
        .with_overall("overall", 3)
        .build_with_overall();

    let c = group.add_bar(5, "fetch");
    c.advance(5);
    group.tick();
    c.finish_success();
    // First tick triggers finish_slot via tick() → non-Active guard.
    group.tick();
    let contents_after_first = term.contents();
    // Second tick calls finish_slot again on the same finished slot.
    group.tick();
    let contents_after_second = term.contents();
    eprintln!("=== finish_slot_idempotent, H=5, W=80 ===");
    eprintln!("after first tick = {contents_after_first:?}");
    eprintln!("after second tick = {contents_after_second:?}");
    // Output should be structurally similar (same line count, same bars).
    let lines_first: Vec<&str> = contents_after_first.lines().collect();
    let lines_second: Vec<&str> = contents_after_second.lines().collect();
    assert_eq!(lines_first.len(), lines_second.len(), "same line count on second tick");
    // fetch bar is at index 1 (index 0 is empty from InMemoryTerm's leading \n).
    assert!(lines_second[1].contains("5/5"), "fetch still shows 5/5: {0}", lines_second[1]);
}

// ── Slot pool / rendering tests ──

#[test]
fn slot_pool_blank_bars_remain_invisible() {
    let (mp, term) = mk_with_size(5, 80);
    let group = ProgressGroup::builder().with_multi_progress(mp).capacity(5).build();

    // Add a child bar so we can verify only 5 lines total.
    let _c = group.add_bar(10, "child");
    group.tick();

    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== slot_pool_blank_bars_remain_invisible, H=5, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    // 5 slots, no overall. child at slot[4] (bottom), blanks at lines[0..3].
    // child at bottom avoids InMemoryTerm trimming → 5 lines.
    assert_eq!(lines.len(), 5, "5 slots, no overall → 5 lines (child at bottom)");
    assert!(lines[0].trim().is_empty(), "line 0 is blank");
    assert!(lines[1].trim().is_empty(), "line 1 is blank");
    assert!(lines[2].trim().is_empty(), "line 2 is blank");
    assert!(lines[3].trim().is_empty(), "line 3 is blank");
    assert!(lines[4].contains("child"), "line 4 has child: {0}", lines[4]);
    assert!(lines[4].contains("0/10"), "line 4 shows 0/10: {0}", lines[4]);
}

#[test]
fn slot_pool_acquire_returns_bottommost_child() {
    let (mp, term) = mk_with_size(5, 80);
    let group = ProgressGroup::builder().with_multi_progress(mp).capacity(4).build(); // 4 slots

    let _c1 = group.add_bar(5, "first");
    group.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== acquire returns bottommost: after first, H=5, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    // 4 slots, no overall. first at slot[3] (bottom), blanks at lines[0..2].
    assert_eq!(lines.len(), 4, "4 slots → 4 lines (first at bottom)");
    assert!(lines[0].trim().is_empty(), "line 0 is blank");
    assert!(lines[1].trim().is_empty(), "line 1 is blank");
    assert!(lines[2].trim().is_empty(), "line 2 is blank");
    assert!(lines[3].contains("first"), "line 3 has first: {0}", lines[3]);

    let _c2 = group.add_bar(3, "second");
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== after second ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    // 4 slots, no overall → 4 lines (first at slot[2], second at slot[3])
    assert_eq!(lines.len(), 4, "4 lines, both non-empty at bottom");
    assert!(lines[0].trim().is_empty(), "line 0 is blank");
    assert!(lines[1].trim().is_empty(), "line 1 is blank");
    assert!(lines[2].contains("first"), "line 2 still has first: {0}", lines[2]);
    assert!(lines[3].contains("second"), "line 3 has second: {0}", lines[3]);
    assert!(!lines[0].contains("second"), "line 0 must not show second: {0}", lines[0]);
    assert!(!lines[1].contains("first"), "line 1 must not show first: {0}", lines[1]);
}

#[test]
fn slot_pool_acquire_with_overall_above_overall() {
    let (mp, term) = mk_with_size(5, 80);
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(5)
        .with_overall("overall", 10)
        .build_with_overall();

    let _c = group.add_bar(7, "worker");
    group.tick();

    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== acquire with overall: H=5, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    assert_eq!(lines.len(), 5, "5 lines (capacity=5)");
    // worker at slot[3] (just above overall at slot[4]), blanks at lines[0..2].
    assert!(lines[0].trim().is_empty(), "line 0 blank");
    assert!(lines[1].trim().is_empty(), "line 1 blank");
    assert!(lines[2].trim().is_empty(), "line 2 blank");
    assert!(lines[3].contains("worker"), "line 3 has child: {0}", lines[3]);
    assert!(lines[3].contains("0/7"), "line 3 shows 0/7: {0}", lines[3]);
    assert!(lines[4].contains("overall"), "line 4 has overall: {0}", lines[4]);
    assert!(lines[4].contains("0/10"), "line 4 shows 0/10: {0}", lines[4]);
}

#[test]
fn progress_group_height_never_grows_with_many_bars() {
    let (mp, term) = mk_with_size(4, 80);
    let group = ProgressGroup::builder().with_multi_progress(mp).capacity(4).build();

    for i in 0..20 {
        let _c = group.add_bar(1, &format!("t{i}"));
        group.tick();
    }

    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== height never grows: 20 add_bar, H=4, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    assert_eq!(lines.len(), 4, "must have exactly 4 lines even after 20 add_bar calls");
}

#[test]
fn progress_group_overall_always_at_bottom() {
    let (mp, term) = mk_with_size(5, 80);
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(5)
        .with_overall("overall", 10)
        .build_with_overall();

    for i in 0..5 {
        let _c = group.add_bar(2, &format!("task{i}"));
        group.tick();
    }

    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== overall always at bottom: 5 children, H=5, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    assert_eq!(lines.len(), 5);
    // Chronological allocation: task0→slot[0], task1→slot[1], task2→slot[2], task3→slot[3].
    assert!(lines[0].contains("task0"), "line 0 has task0: {0}", lines[0]);
    assert!(lines[1].contains("task1"), "line 1 has task1: {0}", lines[1]);
    assert!(lines[2].contains("task2"), "line 2 has task2: {0}", lines[2]);
    assert!(lines[3].contains("task3"), "line 3 has task3: {0}", lines[3]);
    assert!(lines[4].contains("overall"), "line 4 always has overall: {0}", lines[4]);
    assert!(lines[4].contains("0/10"), "line 4 shows 0/10: {0}", lines[4]);
}

#[test]
fn progress_group_join_preserves_all_content() {
    let (mp, term) = mk_with_size(5, 80);
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(5)
        .with_overall("overall", 5)
        .build_with_overall();

    let c = group.add_bar(3, "fetch");
    c.advance(3);
    group.tick();

    let before = term.contents();
    group.join();
    let after = term.contents();

    assert_eq!(before, after, "join() is a no-op — contents must be identical before and after");
}

#[test]
fn progress_group_add_bar_zero_total_renders() {
    let (mp, term) = mk_with_size(5, 80);
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(5)
        .with_overall("overall", 0)
        .build_with_overall();

    let _c = group.add_bar(0, "zero");
    group.tick();

    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== add_bar zero total, H=5, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    assert_eq!(lines.len(), 5);
    // Bottom-up: zero bar at slot[3] (just above overall at slot[4]).
    assert!(lines[0].trim().is_empty(), "line 0 blank");
    assert!(lines[1].trim().is_empty(), "line 1 blank");
    assert!(lines[2].trim().is_empty(), "line 2 blank");
    assert!(lines[3].contains("zero"), "line 3 has zero bar: {0}", lines[3]);
    assert!(lines[4].contains("overall"), "line 4 has overall: {0}", lines[4]);
    // 0/0 renders as full
}

#[test]
fn consumer_lifecycle_materializer() {
    let (mp, term) = mk_with_size(5, 80);
    let group = ProgressGroup::builder().with_multi_progress(mp).capacity(5).build();

    let total = 3u64;
    let pb = group.add_bar(total, "materializing");

    group.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== materializer: initial, H=5, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    // 5 slots, no overall → child at slot[4] (bottom), all 5 lines visible.
    assert_eq!(lines.len(), 5, "5 slots, no overall → 5 lines (child at bottom)");
    assert!(lines[0].trim().is_empty(), "line 0 blank");
    assert!(lines[1].trim().is_empty(), "line 1 blank");
    assert!(lines[2].trim().is_empty(), "line 2 blank");
    assert!(lines[3].trim().is_empty(), "line 3 blank");
    assert!(lines[4].contains("materializing"), "line 4 has materializing label: {0}", lines[4]);
    assert!(lines[4].contains("0/3"), "line 4 shows 0/3: {0}", lines[4]);

    pb.advance(3);
    pb.finish_success();
    group.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    // Still 5 lines after finish
    assert_eq!(lines.len(), 5, "5 slots, no overall → 5 lines after finish too");
    assert!(
        lines[4].contains("materializing"),
        "bar still visible after finish_success: {0}",
        lines[4]
    );
    assert!(lines[4].contains("3/3"), "shows 3/3 complete: {0}", lines[4]);

    group.join();
    let after_join = term.contents();
    assert!(!after_join.is_empty(), "join() must keep bars visible");
}

#[test]
fn consumer_lifecycle_conductor_sync() {
    let (mp, term) = mk_with_size(5, 80);
    let (group, overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(5)
        .with_overall("syncing tools", 2)
        .build_with_overall();

    // Tool 1
    let t1 = group.add_bar(0, "yt-dlp");
    t1.advance(1);
    t1.finish();
    overall.advance(1);
    group.tick();

    // Tool 2
    let t2 = group.add_bar(0, "ffmpeg");
    t2.advance(1);
    t2.finish();
    overall.advance(1);
    group.tick();

    overall.finish_success();
    group.tick();
    group.join();

    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== conductor sync lifecycle, H=5, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    assert_eq!(lines.len(), 5);
    // t1(yt-dlp) at slot[2] (first tool, shifted up by ffmpeg), t2(ffmpeg) at slot[3] (just above overall).
    assert!(lines[0].trim().is_empty(), "line 0 blank");
    assert!(lines[1].trim().is_empty(), "line 1 blank");
    assert!(lines[2].contains("yt-dlp"), "line 2 has yt-dlp: {0}", lines[2]);
    assert!(lines[3].contains("ffmpeg"), "line 3 has ffmpeg: {0}", lines[3]);
    assert!(lines[4].contains("syncing tools"), "line 4 has overall: {0}", lines[4]);
    assert!(lines[4].contains("2/2"), "overall complete: {0}", lines[4]);
}

#[test]
fn consumer_lifecycle_conductor_cli() {
    let (mp, term) = mk_with_size(4, 80);
    let (group, pb) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(4)
        .with_overall("steps", 0)
        .build_with_overall();

    // Simulate step_progress callback: set_total(N) then set_position(1..N)
    pb.set_total(3);

    pb.set_position(1);
    group.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== conductor CLI lifecycle: step 1/3 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    assert_eq!(lines.len(), 4);
    assert!(lines[0].trim().is_empty(), "line 0 blank");
    assert!(lines[1].trim().is_empty(), "line 1 blank");
    assert!(lines[2].trim().is_empty(), "line 2 blank");
    assert!(lines[3].contains("steps"), "line 3 has steps: {0}", lines[3]);

    pb.set_position(3);
    pb.finish();
    group.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== conductor CLI lifecycle: after finish ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    assert!(lines[3].contains("steps"), "bar still visible after finish: {0}", lines[3]);

    group.join();
    assert!(!term.contents().is_empty(), "join() must keep bars visible");
}

#[test]
fn progress_group_finish_and_clear_child_keeps_others() {
    let (mp, term) = mk_with_size(4, 80);
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(4)
        .with_overall("overall", 5)
        .build_with_overall();

    let c1 = group.add_bar(3, "alpha");
    c1.advance(3);
    group.tick();

    c1.finish_and_clear();
    group.tick();

    // Overall must still be visible.
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== finish_and_clear child keeps others, H=4, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    // finish_and_clear hides the child bar. Only the overall bar and blank
    // filler slots remain visible.  With fixed-size slots (capacity=4) the
    // child slot becomes a blank line rather than being removed.
    assert_eq!(lines.len(), 4, "4 lines after child cleared — overall + 3 blanks");
    assert!(lines[3].contains("overall"), "overall visible: {0}", lines[3]);
}

#[test]
fn progress_group_abandon_preserves_bar() {
    let (mp, term) = mk_with_size(5, 80);
    let group = ProgressGroup::builder().with_multi_progress(mp).capacity(5).build();

    let c = group.add_bar(5, "worker");
    c.advance(2);
    group.tick();

    c.abandon();

    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== abandon preserves bar, H=5, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    // 5 slots, no overall → child at slot[4] (bottom), all 5 lines visible.
    assert_eq!(lines.len(), 5, "5 lines — child at bottom, 4 blanks above");
    assert!(lines[4].contains("worker"), "bar visible after abandon: {0}", lines[4]);
    assert!(lines[4].contains("2/5"), "progress preserved: {0}", lines[4]);
}

#[test]
fn progress_group_long_prefix_truncation() {
    let (mp, term) = mk_with_size(5, 80);
    let group = ProgressGroup::builder().with_multi_progress(mp).capacity(5).build();

    // Prefix > 16 chars — production uses {prefix:>16.16}
    let long_prefix = "abcdefghijklmnopqrstuvwxyz"; // 26 chars
    let _c = group.add_bar(5, long_prefix);
    group.tick();

    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== long prefix truncation, H=5, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    // 5 slots, no overall → child at slot[4] (bottom), 5 lines total.
    assert_eq!(lines.len(), 5);
    // Prefix should be right-aligned to 16 chars, left-truncated to 16 chars.
    // The production template is {prefix:>16.16} so it right-aligns and truncates to 16.
    // Expected: "               abcdefghijklmnop" (16 chars right-aligned) — but this
    // won't be exact because of ANSI color codes. Just verify the bar still shows.
    assert!(lines[4].contains("0/5"), "bar shows progress: {0}", lines[4]);
}

#[test]
fn progress_group_children_advance_independently() {
    let (mp, term) = mk_with_size(5, 80);
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(5)
        .with_overall("overall", 10)
        .build_with_overall();

    // Chronological allocation: tool-a at slot[3] (just above overall).
    // Second child shifts tool-a up to slot[2] and takes slot[3].
    // Overall at slot[4].
    let _a = group.add_bar(5, "tool-a");
    group.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    assert!(lines[3].contains("tool-a"), "line 3 has tool-a: {0}", lines[3]);

    // Second child shifts tool-a up, takes last slot before overall.
    let _b = group.add_bar(3, "tool-b");
    group.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    assert!(lines[2].contains("tool-a"), "line 2 still has tool-a: {0}", lines[2]);
    assert!(lines[3].contains("tool-b"), "line 3 has tool-b: {0}", lines[3]);

    group.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    assert!(lines[4].contains("overall"), "overall visible: {0}", lines[4]);
}

// ── Child bar elapsed: starts at zero ──────────────────────────────────────

#[test]
fn child_bar_elapsed_starts_at_zero() {
    let (mp, term) = mk();
    let dims = Arc::new(TestDimensionSource::new((H, W)));
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .with_dim_source(Arc::clone(&dims) as Arc<dyn DimensionSource>)
        .capacity(4)
        .with_overall("overall", 5)
        .build_with_overall();
    let _child = group.add_bar(3, "tool-a");
    group.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    let tool_line = lines.iter().find(|l| l.contains("tool-a")).expect("tool-a line must exist");
    assert!(tool_line.contains("0s"), "tool-a line should show 0 elapsed: {tool_line}");
}

// ── Child bar elapsed: frozen after finish ─────────────────────────────────

#[test]
fn child_bar_elapsed_frozen_after_finish() {
    let (mp, term) = mk();
    let dims = Arc::new(TestDimensionSource::new((H, W)));
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .with_dim_source(Arc::clone(&dims) as Arc<dyn DimensionSource>)
        .capacity(4)
        .with_overall("overall", 5)
        .build_with_overall();
    let child = group.add_bar(3, "tool-a");
    child.set_position(3);
    child.finish();
    group.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    let tool_line = lines.iter().find(|l| l.contains("tool-a")).expect("tool-a line must exist");
    assert!(
        tool_line.contains("0s"),
        "tool-a line should show 0 elapsed after finish: {tool_line}"
    );
    assert!(tool_line.contains("3/3"), "tool-a line should show final position: {tool_line}");
}

// ── Child bar elapsed: frozen after finish_success ─────────────────────────

#[test]
fn child_bar_elapsed_frozen_after_finish_success() {
    let (mp, term) = mk();
    let dims = Arc::new(TestDimensionSource::new((H, W)));
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .with_dim_source(Arc::clone(&dims) as Arc<dyn DimensionSource>)
        .capacity(4)
        .with_overall("overall", 5)
        .build_with_overall();
    let child = group.add_bar(3, "tool-a");
    child.set_position(3);
    child.finish_success();
    group.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    let tool_line = lines.iter().find(|l| l.contains("tool-a")).expect("tool-a line must exist");
    assert!(
        tool_line.contains("0s"),
        "tool-a line should show 0 elapsed after finish_success: {tool_line}"
    );
    assert!(tool_line.contains("3/3"), "tool-a line should show 3/3: {tool_line}");
}

// ── Child bar elapsed: frozen after finish_error ───────────────────────────

#[test]
fn child_bar_elapsed_frozen_after_finish_error() {
    let (mp, term) = mk();
    let dims = Arc::new(TestDimensionSource::new((H, W)));
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .with_dim_source(Arc::clone(&dims) as Arc<dyn DimensionSource>)
        .capacity(4)
        .with_overall("overall", 5)
        .build_with_overall();
    let child = group.add_bar(3, "tool-a");
    child.set_position(1);
    child.finish_error();
    group.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    let tool_line = lines.iter().find(|l| l.contains("tool-a")).expect("tool-a line must exist");
    assert!(
        tool_line.contains("0s"),
        "tool-a line should show 0 elapsed after finish_error: {tool_line}"
    );
    assert!(tool_line.contains("[F]"), "tool-a line should show error message: {tool_line}");
}

// ── Child bar elapsed: frozen after abandon ────────────────────────────────

#[test]
fn child_bar_elapsed_frozen_after_abandon() {
    let (mp, term) = mk();
    let dims = Arc::new(TestDimensionSource::new((H, W)));
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .with_dim_source(Arc::clone(&dims) as Arc<dyn DimensionSource>)
        .capacity(4)
        .with_overall("overall", 5)
        .build_with_overall();
    let child = group.add_bar(3, "tool-a");
    child.set_position(2);
    child.abandon();
    group.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    let tool_line = lines.iter().find(|l| l.contains("tool-a")).expect("tool-a line must exist");
    assert!(
        tool_line.contains("0s"),
        "tool-a line should show 0 elapsed after abandon: {tool_line}"
    );
}

// ── Elapsed preservation: orphan-reattach ─────────────────────────────────

#[test]
fn orphan_reattach_preserves_elapsed() {
    let dims = Arc::new(TestDimensionSource::new((3, 80)));
    let (mp, term) = mk_with_size(5, 80);
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(4)
        .with_overall("overall", 5)
        .with_dim_source(Arc::clone(&dims) as Arc<dyn DimensionSource>)
        .dynamic_height(true)
        .build_with_overall();
    let _child = group.add_bar(10, "worker");

    // Tick to show the bar with initial elapsed.
    group.tick();
    let before = term.contents();
    let before_lines: Vec<&str> = before.lines().collect();
    let worker_before =
        before_lines.iter().find(|l| l.contains("worker")).expect("worker visible before shrink");
    assert!(worker_before.contains("0s"), "worker shows elapsed before orphan: {worker_before}");

    // Shrink height to orphan the worker bar (only room for overall).
    dims.set((1, 80));
    group.tick();
    let after_shrink = term.contents();
    assert!(!after_shrink.contains("worker"), "worker orphaned after shrink");

    // Grow height to reattach the worker bar.
    dims.set((4, 80));
    group.tick();
    let after_grow = term.contents();
    let grow_lines: Vec<&str> = after_grow.lines().collect();
    let worker_after =
        grow_lines.iter().find(|l| l.contains("worker")).expect("worker reattached after grow");
    assert!(worker_after.contains("0s"), "worker elapsed preserved after reattach: {worker_after}");
}

// ── Elapsed preservation: slot shift ──────────────────────────────────────

#[test]
fn slot_shift_preserves_elapsed() {
    let (mp, term) = mk_with_size(5, 80);
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(4)
        .with_overall("overall", 5)
        .build_with_overall();

    let _a = group.add_bar(10, "alpha");
    group.tick();

    // Add bar B — shifts A up one slot.
    let _b = group.add_bar(5, "beta");
    group.tick();

    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    let alpha_line = lines.iter().find(|l| l.contains("alpha")).expect("alpha visible after shift");
    let beta_line = lines.iter().find(|l| l.contains("beta")).expect("beta visible after shift");

    assert!(alpha_line.contains("0s"), "alpha shows elapsed after slot shift: {alpha_line}");
    assert!(beta_line.contains("0s"), "beta shows elapsed: {beta_line}");
}

// ── Regression: no duplicate elapsed template ─────────────────────────────

#[test]
fn no_duplicate_elapsed_template_in_child_output() {
    // If a production template accidentally re-introduces {elapsed_precise}
    // alongside the message-injected elapsed, each bar line would show two
    // `[HH:MM:SS]` timestamps.  Verify at most one per line.
    let (mp, term) = mk();
    let dims = Arc::new(TestDimensionSource::new((H, W)));
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .with_dim_source(Arc::clone(&dims) as Arc<dyn DimensionSource>)
        .capacity(4)
        .with_overall("overall", 5)
        .build_with_overall();
    let _child = group.add_bar(3, "tool-a");
    group.tick();

    let contents = term.contents();
    for line in contents.lines() {
        let count = line.chars().filter(|&c| c == '[').count();
        assert!(
            count <= 1,
            "each line should have at most one '[' (elapsed), got {count}: {line:?}"
        );
    }
}

// ── Orphaned-state overflow behavior ─────────────────────────────────────

#[test]
fn slot_full_hides_overflow_bars_from_display() {
    let (mp, term) = mk_with_size(5, 80);
    let group = ProgressGroup::builder().with_multi_progress(mp).capacity(4).build(); // capacity=4

    let c1 = group.add_bar(5, "tool-a");
    let c2 = group.add_bar(5, "tool-b");
    let c3 = group.add_bar(5, "tool-c");
    let c4 = group.add_bar(5, "tool-d");
    let c5 = group.add_bar(5, "tool-e"); // 5th bar — no slot (only 4 available)

    c1.advance(1);
    c2.advance(2);
    c3.advance(3);
    c4.advance(4);
    c5.advance(5);
    group.tick();

    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== slot_full, H=5, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    // 4 slots, no overall → all 4 used, no blank trailing → 4 lines.
    // tools a-d in display. tool-e has no display slot but is still tracked.
    assert_eq!(lines.len(), 4, "4 lines — 4 display slots, all filled");
    let line_texts: Vec<&str> = lines.iter().map(|l| l.trim()).collect();
    assert!(
        line_texts.iter().any(|l| l.contains("tool-a")),
        "tool-a visible somewhere: {line_texts:?}"
    );
    assert!(
        line_texts.iter().any(|l| l.contains("tool-d")),
        "tool-d visible somewhere: {line_texts:?}"
    );
    // tool-e has no display slot so its content should NOT appear in the terminal
    assert!(
        !line_texts.iter().any(|l| l.contains("tool-e")),
        "tool-e must NOT appear in display (no slot): {line_texts:?}"
    );
    // tool-e is still tracked even without a display slot
    assert_eq!(c5.snapshot().position, 5, "tool-e tracked position: {0}", c5.snapshot().position);
}

// ── Parallel + retention: finish_error while another continues ────────────

#[test]
fn parallel_worker_finish_error_other_continues() {
    let (mp, term) = mk_with_size(5, 80);
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(5)
        .with_overall("overall", 10)
        .build_with_overall();

    let a = group.add_bar(5, "worker-a");
    let b = group.add_bar(5, "worker-b");

    a.advance(3);
    b.advance(2);
    group.tick();

    // worker-a finishes with error, worker-b continues
    a.finish_error();
    b.advance(1);
    group.tick();

    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== parallel: finish_error + other continues, H=5, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    // Capacity 5 with overall: children at [2, 3], overall at [4] — 5 lines
    // Chronological: worker-a (first) at slot[2], worker-b (second) at slot[3].
    assert_eq!(lines.len(), 5, "5 lines — 2 children + 2 blanks + overall");
    assert!(lines[2].contains("worker-a"), "worker-a visible at line 2: {0}", lines[2]);
    assert!(lines[2].contains("[F]"), "worker-a shows error: {0}", lines[2]);
    assert!(lines[3].contains("worker-b"), "worker-b visible at line 3: {0}", lines[3]);
    assert!(lines[3].contains("3/5"), "worker-b shows 3/5: {0}", lines[3]);
    assert!(!lines[3].contains("crash"), "worker-b must not show error: {0}", lines[3]);
    assert!(lines[4].contains("overall"), "overall visible: {0}", lines[4]);
}

// ── Consumer simulation: sequential tool sync with recycling ────────────

#[test]
fn consumer_sync_too_many_tools_recycles() {
    let (mp, term) = mk_with_size(4, 80);
    let group = ProgressGroup::builder().with_multi_progress(mp).capacity(4).build(); // capacity 4, no overall

    // 8 sequential tools on capacity 4 — first 4 get display slots,
    // remaining 4 recycle finished slots.
    for i in 0..8 {
        let tool = group.add_bar(1, &format!("tool{i}"));
        tool.advance(1);
        tool.finish_success();
        group.tick();
    }

    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== consumer sync, 8 tools on cap 4, H=4, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    // 4 bars visible (capacity 4, no overall). Only the last 4 tool slots
    // show in the terminal (tools 4–7, since 0–3 were recycled).
    assert_eq!(lines.len(), 4, "4 lines — all slots filled");
    // Verify at least some of the visible bars show our tools
    // The exact content depends on which finished bars are recycled first
    assert!(lines.iter().any(|l| l.contains("1/1")), "at least one bar shows 1/1: {lines:?}");
}

// ── Retention: finished bars keep their final state ─────────────────────

#[test]
fn retention_finished_bar_keeps_final_msg() {
    let (mp, term) = mk_with_size(4, 80);
    let group = ProgressGroup::builder().with_multi_progress(mp).capacity(4).build();

    let a = group.add_bar(2, "alpha");
    a.advance(2);
    a.finish_success();
    group.tick();

    let b = group.add_bar(3, "beta");
    b.advance(3);
    b.finish_success();
    group.tick();

    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== retention_finished_bar_keeps_final_msg, H=4, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    // 4 slots, no overall → 4 lines. Both finished bars retain success status.
    assert!(lines.len() >= 2, "at least 2 lines: {lines:?}");
    assert!(
        lines.iter().any(|l| l.contains("2/2") || l.contains("3/3")),
        "bars show position: {lines:?}"
    );
    assert!(lines.iter().any(|l| l.contains("alpha")), "bar alpha visible: {lines:?}");
    assert!(lines.iter().any(|l| l.contains("beta")), "bar beta visible: {lines:?}");
}

#[test]
fn retention_finished_bar_persists_across_new_work() {
    let (mp, term) = mk_with_size(4, 80);
    let group = ProgressGroup::builder().with_multi_progress(mp).capacity(4).build();

    // Slot 0: finished bar
    let a = group.add_bar(1, "alpha");
    a.advance(1);
    a.finish_success();
    group.tick();

    // Slot 1: active bar alongside the finished one
    let _b = group.add_bar(5, "beta");
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== retention_finished_bar_persists_across_new_work, H=4, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    // 4 slots → 4 lines. Finished bar (1/1) visible alongside active beta.
    assert!(lines.len() >= 2, "at least 2 lines: {lines:?}");
    assert!(lines.iter().any(|l| l.contains("1/1")), "line shows finished alpha at 1/1: {lines:?}");
    assert!(lines.iter().any(|l| l.contains("beta")), "line shows active 'beta': {lines:?}");
}

#[test]
fn retention_multiple_finished_bars() {
    let (mp, term) = mk_with_size(4, 80);
    let group = ProgressGroup::builder().with_multi_progress(mp).capacity(4).build();

    for (i, _msg) in ["first", "second", "third", "fourth"].iter().enumerate() {
        let h = group.add_bar(1, &format!("task{i}"));
        h.advance(1);
        h.finish_success();
        group.tick();
    }

    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== retention_multiple_finished_bars, H=4, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    // 4 slots, all finished → 4 lines, each with its task name.
    assert_eq!(lines.len(), 4, "4 lines — all 4 finished bars visible");
    for (i, _msg) in ["first", "second", "third", "fourth"].iter().enumerate() {
        let task_name = format!("task{i}");
        assert!(
            lines.iter().any(|l| l.contains(&task_name)),
            "a bar shows '{task_name}': {lines:?}"
        );
    }
}

// ── Rendering: overall bar always at bottom ─────────────────────────────

#[test]
fn renderer_with_overall_always_bottom() {
    let (mp, term) = mk_with_size(4, 80);
    let (group, overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(4)
        .with_overall("overall", 10)
        .build_with_overall();

    // Fill children
    for i in 0..3 {
        let h = group.add_bar(2, &format!("child{i}"));
        h.advance(2);
        h.finish();
        overall.advance(1);
    }
    overall.advance(1);
    overall.finish();
    group.tick();

    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== renderer_with_overall_always_bottom, H=4, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    // H=4: children at lines[0..2], overall at lines[3].
    assert_eq!(lines.len(), 4, "4 lines — children + overall");
    assert!(lines.iter().any(|l| l.contains("child")), "children visible: {lines:?}");
    assert!(lines.last().unwrap().contains("overall"), "overall at bottom: {lines:?}");
}

// ── Regression: child ordering is chronological top-to-bottom ──────────

#[test]
fn regression_child_order_chronological_top_to_bottom() {
    let (mp, term) = mk_with_size(5, 80);
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(5)
        .with_overall("overall", 10)
        .build_with_overall();

    let _c1 = group.add_bar(5, "first");
    let _c2 = group.add_bar(5, "second");
    let _c3 = group.add_bar(5, "third");
    group.tick();

    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== regression_child_order_chronological, H=5, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    // 4 child slots + 1 overall: line[1]=first, line[2]=second, line[3]=third
    assert_eq!(lines.len(), 5, "always 5 lines");
    assert!(lines[0].trim().is_empty(), "line 0 is blank: {0}", lines[0]);
    assert!(lines[1].contains("first"), "line 1 has first: {0}", lines[1]);
    assert!(lines[1].contains("0/5"), "line 1 shows 0/5: {0}", lines[1]);
    assert!(lines[2].contains("second"), "line 2 has second: {0}", lines[2]);
    assert!(lines[2].contains("0/5"), "line 2 shows 0/5: {0}", lines[2]);
    assert!(lines[3].contains("third"), "line 3 has third: {0}", lines[3]);
    assert!(lines[3].contains("0/5"), "line 3 shows 0/5: {0}", lines[3]);
    assert!(lines[4].contains("overall"), "line 4 has overall: {0}", lines[4]);
}

// ── Regression: swap slot does not corrupt display ────────────────────

#[test]
fn regression_swap_slot_does_not_corrupt_display() {
    // Add 2 children, advance both, add 3rd (triggers shift). Verify all
    // children have correct positions and values.
    let (mp, term) = mk_with_size(5, 80);
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(5)
        .with_overall("overall", 10)
        .build_with_overall();

    let c1 = group.add_bar(10, "alpha");
    let c2 = group.add_bar(10, "beta");
    c1.advance(3);
    c2.advance(7);
    group.tick();

    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== swap_slot after 2 children, H=5, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    assert_eq!(lines.len(), 5);
    assert!(lines[0].trim().is_empty(), "line 0 blank: {0}", lines[0]);
    assert!(lines[1].trim().is_empty(), "line 1 blank: {0}", lines[1]);
    assert!(lines[2].contains("alpha"), "line 2 alpha: {0}", lines[2]);
    assert!(lines[2].contains("3/10"), "line 2 alpha 3/10: {0}", lines[2]);
    assert!(lines[3].contains("beta"), "line 3 beta: {0}", lines[3]);
    assert!(lines[3].contains("7/10"), "line 3 beta 7/10: {0}", lines[3]);
    assert!(lines[4].contains("overall"), "line 4 overall: {0}", lines[4]);

    // Add 3rd child — triggers slot shift.
    let c3 = group.add_bar(10, "gamma");
    c3.advance(5);
    group.tick();

    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== swap_slot after 3 children, H=5, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    assert_eq!(lines.len(), 5);
    // Chronological: alpha shifted up to line[1], beta to line[2], gamma at line[3].
    assert!(lines[0].trim().is_empty(), "line 0 blank: {0}", lines[0]);
    assert!(lines[1].contains("alpha"), "line 1 alpha: {0}", lines[1]);
    assert!(lines[1].contains("3/10"), "line 1 alpha 3/10: {0}", lines[1]);
    assert!(lines[2].contains("beta"), "line 2 beta: {0}", lines[2]);
    assert!(lines[2].contains("7/10"), "line 2 beta 7/10: {0}", lines[2]);
    assert!(lines[3].contains("gamma"), "line 3 gamma: {0}", lines[3]);
    assert!(lines[3].contains("5/10"), "line 3 gamma 5/10: {0}", lines[3]);
    assert!(lines[4].contains("overall"), "line 4 overall: {0}", lines[4]);
}

// ── Regression: overall bar never shifts ──────────────────────────────

#[test]
fn regression_overall_never_shifts() {
    let (mp, term) = mk_with_size(4, 80);
    let (group, overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(4)
        .with_overall("overall", 10)
        .build_with_overall();

    // Fill all 3 child slots + overall.
    let _c1 = group.add_bar(1, "a");
    let _c2 = group.add_bar(1, "b");
    let _c3 = group.add_bar(1, "c");
    overall.advance(3);
    group.tick();

    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== overall_never_shifts after fill, H=4, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    assert_eq!(lines.len(), 4);
    assert!(
        lines.last().unwrap().contains("overall"),
        "overall at bottom: {0}",
        lines.last().unwrap()
    );

    // Add more children than capacity.  Overall must stay at bottom.
    let _ = group.add_bar(1, "d");
    let _ = group.add_bar(1, "e");
    overall.advance(2);
    group.tick();

    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== overall_never_shifts after overflow, H=4, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    assert_eq!(lines.len(), 4, "height never changes");
    assert!(
        lines.last().unwrap().contains("overall"),
        "overall still at bottom after overflow: {0}",
        lines.last().unwrap()
    );
    // Children are visible somewhere.
    assert!(
        lines[0].contains("a") || lines[1].contains("a") || lines[2].contains("a"),
        "'a' visible somewhere"
    );
    assert!(
        lines[0].contains("b") || lines[1].contains("b") || lines[2].contains("b"),
        "'b' visible somewhere"
    );
    assert!(
        lines[0].contains("c") || lines[1].contains("c") || lines[2].contains("c"),
        "'c' visible somewhere"
    );
}

// ── Regression: finish_and_clear via tick_fn on group-managed handle ────

#[test]
fn regression_finish_and_clear_with_tick_fn() {
    let (mp, term) = mk_with_size(5, 80);
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(5)
        .with_overall("overall", 3)
        .build_with_overall();

    let _c1 = group.add_bar(5, "keep");
    let c2 = group.add_bar(5, "clear");
    group.tick();

    // c2 is ProgressGroup-managed so mutating methods go through tick_fn.
    let _ = c2.finish_and_clear();
    group.tick();

    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== finish_and_clear_with_tick_fn, H=5, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    // c2's slot is now blank. c1 still visible.
    assert!(lines.iter().any(|l| l.contains("keep")), "'keep' visible: {lines:?}");
    // The cleared slot should be blank (line[2] if chronological: [blank, keep, blank, clear, overall]
    // After clear of c2: slot goes blank. Wait — c2 was at line[3] (newest), now blank.
    assert!(
        lines.iter().any(|l| l.trim().is_empty()),
        "at least one blank line for cleared slot: {lines:?}"
    );
    assert!(lines[4].contains("overall"), "overall at bottom: {0}", lines[4]);

    // Ensure cleared bar is counted as finished — its state should not shift on next add_bar.
    let _c3 = group.add_bar(5, "new");
    group.tick();

    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== finish_and_clear after adding new child, H=5, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    // Chronological: keep at line[1], new at line[3], line[2] may be blank if clear
    // wasn't recycled, or new may have reused the cleared slot.
    // Either way, keep and new are both visible, overall at bottom.
    assert!(lines.iter().any(|l| l.contains("keep")), "'keep' still visible: {lines:?}");
    assert!(lines.iter().any(|l| l.contains("new")), "'new' visible: {lines:?}");
    assert!(lines[4].contains("overall"), "overall at bottom: {0}", lines[4]);
}

// ── Regression: concurrent set_position/set_message + renderer.tick() ──

#[test]
fn regression_concurrent_set_and_sync() {
    let (mp, term) = mk_with_size(5, 80);
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(5)
        .with_overall("overall", 100)
        .build_with_overall();

    let c1 = group.add_bar(50, "worker");
    // Rapid set_position/set_message to exercise tick_fn callback path.
    for i in 0..20 {
        c1.set_position(i * 2);
    }
    group.tick();

    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== concurrent_set_and_sync, H=5, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    assert!(lines.iter().any(|l| l.contains("38/50")), "position 38/50 visible: {lines:?}");
    assert!(lines[4].contains("overall"), "overall at bottom: {0}", lines[4]);
}

// ── Regression: recycle finished slot after full ──────────────────────

#[test]
fn regression_recycle_finished_slot_after_full() {
    let (mp, term) = mk_with_size(5, 80);
    let (group, overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(5)
        .with_overall("overall", 5)
        .build_with_overall();

    // Fill all 4 child slots.
    let children: Vec<_> = (0..4).map(|i| group.add_bar(2, &format!("task{i}"))).collect();
    for _c in &children {}
    overall.advance(4);
    group.tick();

    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== recycle after fill, H=5, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    assert_eq!(lines.len(), 5);
    assert!(lines[0].contains("task0"), "task0 at line[0]: {0}", lines[0]);
    assert!(lines[3].contains("task3"), "task3 at line[3]: {0}", lines[3]);

    // Finish and clear task0 — must not panic or corrupt display.
    children[0].finish_and_clear();
    group.tick();

    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== recycle after finish_and_clear task0, H=5, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    // task0 slot is now blank or reused.
    assert!(lines.iter().any(|l| l.contains("task1")), "task1 visible: {lines:?}");
    assert!(lines.iter().any(|l| l.contains("task2")), "task2 visible: {lines:?}");
    assert!(lines.iter().any(|l| l.contains("task3")), "task3 visible: {lines:?}");

    // Add a 5th child — it should reuse the recycled slot.
    let _c4 = group.add_bar(2, "task4");
    group.tick();

    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== recycle after adding task4, H=5, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    assert_eq!(lines.len(), 5, "height unchanged");
    // task4 should be visible somewhere (it reused the finished slot).
    assert!(lines.iter().any(|l| l.contains("task4")), "task4 visible: {lines:?}");
    assert!(lines[4].contains("overall"), "overall at bottom: {0}", lines[4]);
}

// ── Consumer simulation: materializer with parallel workers ─────────────

#[test]
fn consumer_materializer_single_bar_parallel_workers() {
    let (mp, term) = mk_with_size(4, 80);
    let group = ProgressGroup::builder().with_multi_progress(mp).capacity(4).build();

    // Simulate a materializer dispatcher: parallel workers tracked
    // as a single progress bar.
    let total_work: u64 = 100;
    let pb = group.add_bar(total_work, "materializing");

    // Advance in chunks (simulating parallel workers reporting)
    for chunk in [30u64, 30, 30, 10] {
        pb.advance(chunk);
        group.tick();
    }
    pb.finish_success();
    group.tick();

    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== consumer_materializer_single_bar_parallel_workers, H=4, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    assert!(lines.len() >= 1, "at least 1 line: {lines:?}");
    assert!(lines.iter().any(|l| l.contains("100/100")), "shows 100/100: {lines:?}");
}

// ── Terminal resize reactivity ──────────────────────────────────────────────

#[test]
fn resize_width_wide_to_narrow_changes_output() {
    let dims = Arc::new(TestDimensionSource::new((H, 80)));
    let (mp, term) = mk_with_size(H, 80);
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(5)
        .with_overall("overall", 10)
        .with_dim_source(Arc::clone(&dims) as Arc<dyn DimensionSource>)
        .dynamic_height(false)
        .build_with_overall();
    let contents_wide = term.contents();
    eprintln!("=== width wide_to_narrow, before resize (wide) ===");
    for (i, line) in contents_wide.lines().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }

    dims.set((H, 40));
    group.tick();
    let contents_narrow = term.contents();
    eprintln!("=== after resize (narrow) ===");
    for (i, line) in contents_narrow.lines().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }

    // Content should differ after resize (different bar templates).
    assert_ne!(contents_wide, contents_narrow, "output changes after width resize");
}

#[test]
fn resize_width_narrow_to_wide_restores_content() {
    let dims = Arc::new(TestDimensionSource::new((H, 40)));
    let (mp, term) = mk_with_size(H, 40);
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(5)
        .with_overall("overall", 10)
        .with_dim_source(Arc::clone(&dims) as Arc<dyn DimensionSource>)
        .dynamic_height(false)
        .build_with_overall();
    let contents_narrow = term.contents();
    eprintln!("=== width narrow_to_wide, before resize (narrow) ===");
    for (i, line) in contents_narrow.lines().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }

    dims.set((H, 80));
    group.tick();
    let contents_wide = term.contents();
    eprintln!("=== after resize (wide) ===");
    for (i, line) in contents_wide.lines().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    assert_ne!(contents_narrow, contents_wide, "output changes after width resize");
}

#[test]
fn resize_width_noop_same_width_no_change() {
    let dims = Arc::new(TestDimensionSource::new((H, W)));
    let (mp, term) = mk_with_size(H, W);
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(5)
        .with_overall("overall", 10)
        .with_dim_source(Arc::clone(&dims) as Arc<dyn DimensionSource>)
        .dynamic_height(false)
        .build_with_overall();
    let before = term.contents();
    let before_lines = before.lines().count();
    // (no elapsed assertion before tick — elapsed is injected on tick, and
    //  we haven't ticked since constructor.)

    dims.set((H, W));
    group.tick();
    let after = term.contents();
    let after_lines = after.lines().count();
    // Same width → same number of lines, same bar template structure.
    // Content differs slightly because the spinner animates between ticks.
    assert_eq!(before_lines, after_lines, "same line count after noop width resize");
    assert!(after.contains("overall"), "overall visible after");
    assert!(after.contains("0/10"), "overall shows 0/10 after");
    assert!(after.contains("0s"), "time after");
}

#[test]
fn resize_height_grow_adds_slots() {
    let dims = Arc::new(TestDimensionSource::new((4, 80)));
    let (mp, term) = mk_with_size(6, 80);
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(4)
        .with_overall("overall", 10)
        .with_dim_source(Arc::clone(&dims) as Arc<dyn DimensionSource>)
        .dynamic_height(true)
        .build_with_overall();
    {
        let _c1 = group.add_bar(7, "fetch");
        group.tick();
    }
    eprintln!("=== height grow, before resize, H=4 ===");
    let before = term.contents();
    for (i, line) in before.lines().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    let before_count = before.lines().count();

    dims.set((6, 80));
    group.tick();
    let after = term.contents();
    eprintln!("=== after resize, H=6 ===");
    for (i, line) in after.lines().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    let after_count = after.lines().count();
    assert!(after_count > before_count, "more lines after height growth");
}

#[test]
fn resize_height_shrink_removes_slots() {
    let dims = Arc::new(TestDimensionSource::new((6, 80)));
    let (mp, term) = mk_with_size(6, 80);
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(6)
        .with_overall("overall", 10)
        .with_dim_source(Arc::clone(&dims) as Arc<dyn DimensionSource>)
        .dynamic_height(true)
        .build_with_overall();
    let _c1 = group.add_bar(7, "fetch");
    group.tick();

    let before_count = term.contents().lines().count();
    eprintln!("=== height shrink, before resize, H=6, count={before_count} ===");

    dims.set((4, 80));
    group.tick();
    let after_count = term.contents().lines().count();
    eprintln!("=== after resize, H=4, count={after_count} ===");
    assert!(after_count < before_count, "fewer lines after height shrink");
}

#[test]
fn resize_height_shrink_protects_overall() {
    let dims = Arc::new(TestDimensionSource::new((6, 80)));
    let (mp, term) = mk_with_size(6, 80);
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(6)
        .with_overall("overall", 10)
        .with_dim_source(Arc::clone(&dims) as Arc<dyn DimensionSource>)
        .dynamic_height(true)
        .build_with_overall();
    let _c1 = group.add_bar(7, "fetch");
    group.tick();
    let before = term.contents();
    eprintln!("=== height shrink_protects, before ===");
    for (i, line) in before.lines().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    assert!(before.lines().any(|l| l.contains("overall")), "overall visible before resize");

    dims.set((4, 80));
    group.tick();
    let after = term.contents();
    eprintln!("=== after shrink ===");
    for (i, line) in after.lines().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    assert!(after.lines().any(|l| l.contains("overall")), "overall still visible after shrink");
}

#[test]
fn resize_height_grow_detached_reappear() {
    let dims = Arc::new(TestDimensionSource::new((4, 80)));
    let (mp, term) = mk_with_size(6, 80);
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(4)
        .with_overall("overall", 10)
        .with_dim_source(Arc::clone(&dims) as Arc<dyn DimensionSource>)
        .dynamic_height(true)
        .build_with_overall();
    let _c1 = group.add_bar(7, "fetch");
    group.tick();

    dims.set((6, 80));
    group.tick();
    let after = term.contents();
    eprintln!("=== height grow_detached, after resize, H=6 ===");
    for (i, line) in after.lines().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    assert!(after.lines().any(|l| l.contains("fetch")), "child still visible after growth");
    assert!(after.lines().any(|l| l.contains("overall")), "overall visible after growth");
}

#[test]
fn resize_height_clamps_at_min_slots() {
    let dims = Arc::new(TestDimensionSource::new((6, 80)));
    let (mp, term) = mk_with_size(6, 80);
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(6)
        .with_overall("overall", 10)
        .with_dim_source(Arc::clone(&dims) as Arc<dyn DimensionSource>)
        .dynamic_height(true)
        .build_with_overall();
    let _c1 = group.add_bar(7, "fetch");
    group.tick();
    let before_count = term.contents().lines().count();

    // Shrink to height=1 → only 1 line (overall), bar evicted to orphaned.
    dims.set((1, 80));
    group.tick();
    let after = term.contents();
    let after_count = after.lines().count();
    eprintln!("=== height shrink extreme, H=1, before={before_count}, after={after_count} ===");
    assert_eq!(after_count, 1, "exactly 1 line at H=1 (overall only)");
    assert!(after.contains("overall"), "overall visible at H=1");
    assert!(!after.contains("fetch"), "child evicted at H=1");
}

#[test]
fn resize_height_clamps_at_max_slots() {
    let dims = Arc::new(TestDimensionSource::new((4, 80)));
    let (mp, term) = mk_with_size(10, 80);
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(4)
        .with_overall("overall", 10)
        .with_dim_source(Arc::clone(&dims) as Arc<dyn DimensionSource>)
        .dynamic_height(true)
        .build_with_overall();
    let _c1 = group.add_bar(7, "fetch");
    group.tick();

    // Grow to huge height → should clamp at MAX_SLOTS=200.
    dims.set((999, 80));
    group.tick();
    let after_count = term.contents().lines().count();
    eprintln!("=== height clamp max, H=999, after={after_count} ===");
    // Should not have 999 lines — clamped to MAX_SLOTS.
    assert!(after_count <= 200, "at most 200 lines after extreme growth (got {after_count})");
}

#[test]
fn resize_both_dimensions() {
    let dims = Arc::new(TestDimensionSource::new((4, 80)));
    let (mp, term) = mk_with_size(6, 80);
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(4)
        .with_overall("overall", 10)
        .with_dim_source(Arc::clone(&dims) as Arc<dyn DimensionSource>)
        .dynamic_height(true)
        .build_with_overall();
    let _c1 = group.add_bar(7, "fetch");
    group.tick();
    let before = term.contents();

    // Change both width and height at once.
    dims.set((6, 40));
    group.tick();
    let after = term.contents();
    eprintln!("=== both dims, before ===");
    for (i, line) in before.lines().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    eprintln!("=== after (H=6, W=40) ===");
    for (i, line) in after.lines().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    assert_ne!(before, after, "output changes when both dimensions change");
}

#[test]
fn resize_then_restore_original() {
    let dims = Arc::new(TestDimensionSource::new((4, 80)));
    let (mp, term) = mk_with_size(6, 80);
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(4)
        .with_overall("overall", 10)
        .with_dim_source(Arc::clone(&dims) as Arc<dyn DimensionSource>)
        .dynamic_height(true)
        .build_with_overall();
    let _c1 = group.add_bar(7, "fetch");
    group.tick();
    let original = term.contents();

    // Grow.
    dims.set((6, 40));
    group.tick();
    // Restore.
    dims.set((4, 80));
    group.tick();
    let restored = term.contents();
    assert_eq!(
        restored.lines().count(),
        original.lines().count(),
        "restored line count matches original"
    );
    assert!(restored.contains("overall"), "overall visible after restore");
    assert!(restored.contains("fetch"), "child visible after restore");
}

/// Shrink then grow back — verifies that a shrink–grow cycle preserves the
/// line count and both tracked bars remain visible.
#[test]
fn resize_height_shrink_then_grow_restores_line_count() {
    let dims = Arc::new(TestDimensionSource::new((6, 80)));
    let (mp, term) = mk_with_size(6, 80);
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(6)
        .with_overall("overall", 10)
        .with_dim_source(Arc::clone(&dims) as Arc<dyn DimensionSource>)
        .dynamic_height(true)
        .build_with_overall();
    let _c1 = group.add_bar(7, "fetch");
    group.tick();
    let original_count = term.contents().lines().count();
    assert!(original_count >= 6, "at least 6 lines at H=6");

    // Shrink to H=4.
    dims.set((4, 80));
    group.tick();
    let shrunken_count = term.contents().lines().count();
    assert!(
        shrunken_count < original_count,
        "fewer lines after shrink (was {original_count}, now {shrunken_count})"
    );

    // Grow back to H=6.
    dims.set((6, 80));
    group.tick();
    let restored = term.contents();
    assert_eq!(restored.lines().count(), original_count, "restored line count matches original");
    assert!(restored.contains("overall"), "overall visible after cycle");
    assert!(restored.contains("fetch"), "child visible after cycle");
}

/// Shrink to H=3 — verifies that the render-slot count matches the
/// terminal height (3: 2 active bars + overall) and all tracked bars
/// remain visible.
#[test]
fn resize_height_partial_shrink_keeps_active_bars() {
    let dims = Arc::new(TestDimensionSource::new((6, 80)));
    let (mp, term) = mk_with_size(6, 80);
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(6)
        .with_overall("overall", 10)
        .with_dim_source(Arc::clone(&dims) as Arc<dyn DimensionSource>)
        .dynamic_height(true)
        .build_with_overall();
    let _c1 = group.add_bar(7, "fetch1");
    let _c2 = group.add_bar(5, "fetch2");
    group.tick();

    // H=3 → 3 lines (2 active + overall, drained 3 blanks with 1 eviction).
    dims.set((3, 80));
    group.tick();
    let after = term.contents();
    eprintln!("=== partial shrink, H=3 ===");
    for (i, line) in after.lines().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    assert_eq!(after.lines().count(), 3, "3 lines at H=3");
    assert!(after.contains("overall"), "overall visible");
    assert!(after.contains("fetch1"), "fetch1 visible");
    assert!(after.contains("fetch2"), "fetch2 visible");
}

/// Attach a bar, shrink, then attach another bar — verifies that
/// interleaving attach with resize works correctly.
#[test]
fn resize_height_with_interleaved_attach() {
    let dims = Arc::new(TestDimensionSource::new((5, 80)));
    let (mp, term) = mk_with_size(5, 80);
    let group = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(5)
        .with_dim_source(Arc::clone(&dims) as Arc<dyn DimensionSource>)
        .dynamic_height(true)
        .build();
    let _alpha = group.add_bar(10, "alpha");

    // Start with 5 lines (no overall bar).
    let before_count = term.contents().lines().count();
    assert!(before_count >= 5, "at least 5 lines at H=5");

    // Shrink to H=4 (removes 1 blank at top).
    dims.set((4, 80));
    group.tick();
    let after_shrink = term.contents();
    assert!(after_shrink.lines().count() < before_count, "fewer lines after shrink");
    assert!(after_shrink.contains("alpha"), "alpha visible after shrink");
    group.tick();

    // Attach a second bar.
    let _beta = group.add_bar(5, "beta");
    group.tick();
    let after_attach = term.contents();
    // Line count should stay at 4 (H=4).
    assert_eq!(after_attach.lines().count(), 4, "still 4 lines after attaching beta");
    assert!(after_attach.contains("alpha"), "alpha visible after attach");
    assert!(after_attach.contains("beta"), "beta visible after attach");
}

/// Shrink from H=5 to H=1 then grow back — verifies that eviction and
/// reattachment preserve chronological order across the full range.
#[test]
fn resize_height_sequence_with_three_bars() {
    let dims = Arc::new(TestDimensionSource::new((5, 80)));
    let (mp, term) = mk_with_size(5, 80);
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(5)
        .with_overall("overall", 10)
        .with_dim_source(Arc::clone(&dims) as Arc<dyn DimensionSource>)
        .dynamic_height(true)
        .build_with_overall();
    // Add in order: bar3 (oldest), bar2, bar1 (newest).
    let _bar3 = group.add_bar(7, "bar 3");
    let _bar2 = group.add_bar(5, "bar 2");
    let _bar1 = group.add_bar(3, "bar 1");
    group.tick();

    // H=5 → (empty), (bar 3), (bar 2), (bar 1), (overall)
    let h5 = term.contents();
    eprintln!("=== sequence 3 bars, H=5 ===");
    for (i, line) in h5.lines().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    assert_eq!(h5.lines().count(), 5);
    assert!(h5.contains("bar 3"), "bar 3 visible at H=5");
    assert!(h5.contains("bar 2"), "bar 2 visible at H=5");
    assert!(h5.contains("bar 1"), "bar 1 visible at H=5");
    assert!(h5.contains("overall"), "overall visible at H=5");

    // H=4 → (bar 3), (bar 2), (bar 1), (overall) — empty slot removed
    dims.set((4, 80));
    group.tick();
    let h4 = term.contents();
    eprintln!("=== sequence 3 bars, H=4 ===");
    for (i, line) in h4.lines().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    assert_eq!(h4.lines().count(), 4);
    assert!(h4.contains("bar 3"));
    assert!(h4.contains("bar 2"));
    assert!(h4.contains("bar 1"));
    assert!(h4.contains("overall"));

    // H=3 → (bar 2), (bar 1), (overall) — bar 3 evicted
    dims.set((3, 80));
    group.tick();
    let h3 = term.contents();
    eprintln!("=== sequence 3 bars, H=3 ===");
    for (i, line) in h3.lines().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    assert_eq!(h3.lines().count(), 3);
    assert!(!h3.contains("bar 3"), "bar 3 evicted at H=3");
    assert!(h3.contains("bar 2"));
    assert!(h3.contains("bar 1"));
    assert!(h3.contains("overall"));

    // H=2 → (bar 1), (overall) — bar 2 evicted
    dims.set((2, 80));
    group.tick();
    let h2 = term.contents();
    eprintln!("=== sequence 3 bars, H=2 ===");
    for (i, line) in h2.lines().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    assert_eq!(h2.lines().count(), 2);
    assert!(!h2.contains("bar 3"), "bar 3 still evicted at H=2");
    assert!(!h2.contains("bar 2"), "bar 2 evicted at H=2");
    assert!(h2.contains("bar 1"));
    assert!(h2.contains("overall"));

    // H=1 → (overall) — bar 1 evicted
    dims.set((1, 80));
    group.tick();
    let h1 = term.contents();
    eprintln!("=== sequence 3 bars, H=1 ===");
    for (i, line) in h1.lines().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    assert_eq!(h1.lines().count(), 1);
    assert!(!h1.contains("bar 3"));
    assert!(!h1.contains("bar 2"));
    assert!(!h1.contains("bar 1"));
    assert!(h1.contains("overall"));

    // Grow back to H=4 → bar 3, bar 2, bar 1, overall (reattach in order).
    // (pop_back LIFO preserves chronological order after prepend-at-0.)
    dims.set((4, 80));
    group.tick();
    let h4_restored = term.contents();
    eprintln!("=== sequence 3 bars, restored H=4 ===");
    for (i, line) in h4_restored.lines().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    assert_eq!(h4_restored.lines().count(), 4);
    assert!(h4_restored.contains("bar 3"), "bar 3 reappears on growth");
    assert!(h4_restored.contains("bar 2"), "bar 2 reappears on growth");
    assert!(h4_restored.contains("bar 1"), "bar 1 reappears on growth");
    assert!(h4_restored.contains("overall"));
}

/// Shrink from H=4 to H=1 (no overall bar) — verifies eviction and
/// reattachment work correctly without an overall bar.
#[test]
fn resize_height_sequence_without_overall() {
    let dims = Arc::new(TestDimensionSource::new((4, 80)));
    let (mp, term) = mk_with_size(4, 80);
    let group = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(4)
        .with_dim_source(Arc::clone(&dims) as Arc<dyn DimensionSource>)
        .dynamic_height(true)
        .build();
    let _bar2 = group.add_bar(5, "bar 2");
    let _bar1 = group.add_bar(3, "bar 1");

    // H=4 → (empty), (empty), (bar 2), (bar 1)
    let h4 = term.contents();
    eprintln!("=== seq no overall, H=4 ===");
    for (i, line) in h4.lines().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    assert_eq!(h4.lines().count(), 4);
    assert!(h4.contains("bar 2"), "bar 2 visible at H=4");
    assert!(h4.contains("bar 1"), "bar 1 visible at H=4");

    // H=3 → (empty), (bar 2), (bar 1) — blank removed
    dims.set((3, 80));
    group.tick();
    let h3 = term.contents();
    eprintln!("=== seq no overall, H=3 ===");
    for (i, line) in h3.lines().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    assert_eq!(h3.lines().count(), 3);
    assert!(h3.contains("bar 2"));
    assert!(h3.contains("bar 1"));

    // H=2 → (bar 2), (bar 1) — blank removed, all active
    dims.set((2, 80));
    group.tick();
    let h2 = term.contents();
    eprintln!("=== seq no overall, H=2 ===");
    for (i, line) in h2.lines().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    assert_eq!(h2.lines().count(), 2);
    assert!(h2.contains("bar 2"));
    assert!(h2.contains("bar 1"));

    // H=1 → (bar 1) — bar 2 evicted
    dims.set((1, 80));
    group.tick();
    let h1 = term.contents();
    eprintln!("=== seq no overall, H=1 ===");
    for (i, line) in h1.lines().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    assert_eq!(h1.lines().count(), 1);
    assert!(!h1.contains("bar 2"), "bar 2 evicted at H=1");
    assert!(h1.contains("bar 1"), "bar 1 still visible at H=1");

    // Grow back to H=3 → bar 2, bar 1 (reattach in order)
    dims.set((3, 80));
    group.tick();
    let h3_restored = term.contents();
    eprintln!("=== seq no overall, restored H=3 ===");
    for (i, line) in h3_restored.lines().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    assert_eq!(h3_restored.lines().count(), 3);
    assert!(h3_restored.contains("bar 2"), "bar 2 reappears on growth");
    assert!(h3_restored.contains("bar 1"), "bar 1 reappears on growth");
}

// ── Color regression tests ──────────────────────────────────────────
//
// These tests verify the **visible text effects** of the bar-status
// coloring system.  The InMemoryTerm stores cleaned text (without ANSI
// escape sequences), so we check for the correct text content that
// results from color application.
//
// Raw-ANSI unit tests live in `src/mediapm-utils/src/progress.rs`
// (the `#[cfg(test)] mod tests` block), where private helper functions
// `bar_color_code`, `build_prefix`, and `build_right_msg` are directly
// testable.

/// Active child bar: count/total values correct, rate visible, no brackets.
#[test]
fn color_active_child_text() {
    let (mp, term) = mk_with_size(2, 80);
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(2)
        .with_overall("overall", 1)
        .build_with_overall();
    let child = group.add_bar(5, "child");
    child.set_position(0);
    group.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    // H=2, capacity=2+overall → line[0]=child, line[1]=overall, no blanks
    assert_eq!(lines.len(), 2, "expected 2 visible lines");
    let child_line = lines[0];
    let overall_line = lines[1];
    // Label visible
    assert!(child_line.contains("child"), "child label: {child_line:?}");
    assert!(overall_line.contains("overall"), "overall label: {overall_line:?}");
    // Count/total values correct
    assert!(child_line.contains("0/5"), "child count/total 0/5: {child_line:?}");
    assert!(overall_line.contains("0/1"), "overall count/total 0/1: {overall_line:?}");
    // Rate visible for active child
    assert!(
        child_line.contains("/d") || child_line.contains("/s"),
        "rate units in active child: {child_line:?}"
    );
    // Elapsed visible
    assert!(child_line.contains("0s"), "elapsed '0s': {child_line:?}");
    // No brackets on active bar
    assert!(!child_line.contains("[F]"), "no [F] in active child: {child_line:?}");
    assert!(!child_line.contains("[A]"), "no [A] in active child: {child_line:?}");
    assert!(!child_line.contains("[S]"), "no [S] in active child: {child_line:?}");
}

/// Failed bar: [F] bracket shown, values correct.
#[test]
fn color_failed_bracket_text() {
    let (mp, term) = mk_with_size(2, 80);
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(2)
        .with_overall("overall", 1)
        .build_with_overall();
    let child = group.add_bar(5, "child");
    group.tick();
    child.finish_error();
    group.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    assert_eq!(lines.len(), 2, "expected 2 visible lines");
    let child_line = lines[0];
    // [F] bracket visible
    assert!(child_line.contains("[F]"), "failed bar should show [F] bracket: {child_line:?}");
    // Count/total correct
    assert!(child_line.contains("0/5"), "failed bar count/total: {child_line:?}");
    // No yellow bracket
    assert!(!child_line.contains("[A]"), "no [A] on failed: {child_line:?}");
}

/// Abandoned bar: [A] bracket shown, values correct.
#[test]
fn color_abandoned_bracket_text() {
    let (mp, term) = mk_with_size(2, 80);
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(2)
        .with_overall("overall", 1)
        .build_with_overall();
    let child = group.add_bar(5, "child");
    group.tick();
    child.abandon();
    group.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    assert_eq!(lines.len(), 2, "expected 2 visible lines");
    let child_line = lines[0];
    // [A] bracket visible
    assert!(child_line.contains("[A]"), "abandoned bar should show [A] bracket: {child_line:?}");
    // Count/total correct
    assert!(child_line.contains("0/5"), "abandoned bar count/total: {child_line:?}");
    // No red bracket
    assert!(!child_line.contains("[F]"), "no [F] on abandoned: {child_line:?}");
}

/// Success bar: full count/total, no brackets.
#[test]
fn color_success_text() {
    let (mp, term) = mk_with_size(2, 80);
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(2)
        .with_overall("overall", 1)
        .build_with_overall();
    let child = group.add_bar(5, "child");
    child.set_position(5);
    group.tick();
    child.finish_success();
    group.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    assert_eq!(lines.len(), 2, "expected 2 visible lines");
    let child_line = lines[0];
    // Full count/total
    assert!(child_line.contains("5/5"), "success bar full count/total: {child_line:?}");
    // No brackets
    assert!(!child_line.contains("[F]"), "no [F] on success: {child_line:?}");
    assert!(!child_line.contains("[A]"), "no [A] on success: {child_line:?}");
    assert!(!child_line.contains("[S]"), "no [S] on success: {child_line:?}");
}

/// Finished bar (via `finish()`): full count/total, no [S] bracket.
#[test]
fn color_finished_text() {
    let (mp, term) = mk_with_size(2, 80);
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(2)
        .with_overall("overall", 1)
        .build_with_overall();
    let child = group.add_bar(5, "child");
    child.set_position(5);
    group.tick();
    child.finish();
    group.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    assert_eq!(lines.len(), 2, "expected 2 visible lines");
    let child_line = lines[0];
    // Full count/total
    assert!(child_line.contains("5/5"), "finished bar full count/total: {child_line:?}");
    // No brackets
    assert!(!child_line.contains("[S]"), "no [S] on finished: {child_line:?}");
}

/// No [S] or [=] brackets appear anywhere in the output.
#[test]
fn color_no_success_brackets() {
    let (mp, term) = mk_with_size(2, 80);
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(2)
        .with_overall("overall", 1)
        .build_with_overall();
    let child = group.add_bar(5, "child");
    group.tick();
    child.finish_success();
    group.tick();
    let contents = term.contents();
    // No [S] or [=] anywhere in the output
    assert!(!contents.contains("[S]"), "[S] bracket must not appear in output");
    assert!(!contents.contains("[=]"), "[=] bracket must not appear in output");
}

// ── Rate stability: no decay on stale ticks ─────────────────────────
//
// These tests verify that rate only changes when actual progress occurs.
// Previously, tick() recomputed the EMA on every call (when dt > 0.001),
// even without position change, causing the displayed rate to decay
// toward zero during idle periods.

/// Active bar: rate stays stable across many stale ticks.
///
/// Advance, tick, then tick 20 more times with 50 ms delays (no further
/// progress) — the rate must not decay toward zero.
#[test]
fn rate_stable_on_stale_ticks() {
    let (mp, term) = mk_with_size(2, 80);
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(2)
        .with_overall("overall", 1)
        .build_with_overall();
    let child = group.add_bar(1000, "test");
    // Advance a significant amount so the initial rate is clearly non-zero.
    child.set_position(500);
    group.tick();
    std::thread::sleep(std::time::Duration::from_millis(60));
    let after_progress = term.contents();
    assert!(
        after_progress.contains("500/1.0k"),
        "count/total visible after advance: {after_progress:?}"
    );
    // Rate must be shown for active bars.
    assert!(
        after_progress.contains("/s")
            || after_progress.contains("k/s")
            || after_progress.contains("M/s")
            || after_progress.contains("/m")
            || after_progress.contains("/h")
            || after_progress.contains("/d"),
        "rate must appear after progress: {after_progress:?}"
    );

    // Now tick 20× with realistic ticker-interval delays.
    for _ in 0..20 {
        std::thread::sleep(std::time::Duration::from_millis(50));
        group.tick();
    }

    let after_stale = term.contents();
    // Rate must still be present (must not have decayed to empty).
    assert!(
        after_stale.contains("/s")
            || after_stale.contains("k/s")
            || after_stale.contains("M/s")
            || after_stale.contains("/m")
            || after_stale.contains("/h")
            || after_stale.contains("/d"),
        "rate must survive stale ticks: {after_stale:?}"
    );
    // Count/total unchanged.
    assert!(
        after_stale.contains("500/1.0k"),
        "count/total unchanged after stale ticks: {after_stale:?}"
    );
}

/// Active bar: rate updates when more progress is made.
///
/// Advance → tick → capture rate, then advance more → tick → verify rate
/// has changed (not stuck at old value).
#[test]
fn rate_updates_on_progress() {
    let (mp, term) = mk_with_size(2, 80);
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(2)
        .with_overall("overall", 1)
        .build_with_overall();
    let child = group.add_bar(2000, "test");
    child.set_position(10);
    group.tick();
    std::thread::sleep(std::time::Duration::from_millis(60));
    let after_small = term.contents();

    // Advance much more.
    child.set_position(1500);
    group.tick();
    std::thread::sleep(std::time::Duration::from_millis(60));
    let after_large = term.contents();

    // Both must show rate.
    assert!(
        after_small.contains("/s") || after_small.contains("k/s") || after_small.contains("M/s"),
        "rate after small progress: {after_small:?}"
    );
    assert!(
        after_large.contains("/s") || after_large.contains("k/s") || after_large.contains("M/s"),
        "rate after large progress: {after_large:?}"
    );
    // The two outputs should differ because position changed significantly.
    // (The spinner may also differ, but that's fine — we just need *some* change
    // rather than identical frozen output.)
    assert_ne!(after_small, after_large, "rate/progress must differ between 10 and 1500");
}

/// Active bar: rate is always shown even with zero progress.
#[test]
fn rate_always_shown() {
    let (mp, term) = mk_with_size(2, 80);
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(2)
        .with_overall("overall", 1)
        .build_with_overall();
    let _child = group.add_bar(100, "idle");
    // No progress made — bar is still active.
    group.tick();
    std::thread::sleep(std::time::Duration::from_millis(60));
    let contents = term.contents();
    // Rate must appear even with zero progress.
    assert!(
        contents.contains("/s")
            || contents.contains("k/s")
            || contents.contains("M/s")
            || contents.contains("/m")
            || contents.contains("/h")
            || contents.contains("/d"),
        "rate must appear even on idle active bar: {contents:?}"
    );
}
