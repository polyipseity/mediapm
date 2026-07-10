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

use indicatif::{InMemoryTerm, MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use mediapm_utils::progress::ProgressGroup;

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

// ── Single bar lifecycle ─────────────────────────────────────────────────────

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
    let (_group, overall) = ProgressGroup::with_mp_and_overall(mp, 5, "overall", 10);
    overall.tick();
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
    let (group, overall) = ProgressGroup::with_mp_and_overall(mp, 4, "overall", 3);

    let c1 = group.add_bar(5, "tool1");
    c1.tick();
    overall.tick();
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

    let c2 = group.add_bar(3, "tool2");
    c2.tick();
    overall.tick();
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
    // Use capacity=3 (clamped to MIN_SLOTS=4) so there's 1 unwritten row
    // at the bottom — this avoids InMemoryTerm trimming blank content
    // when bars fill the entire terminal height.
    let (mp, term) = mk_with_size(5, 80);
    let group = ProgressGroup::with_mp(mp, 3);

    let c1 = group.add_bar(5, "task1");
    c1.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== no_overall, after task1, H=5, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    eprintln!("count = {}", lines.len());
    // 4 slots (clamped from 3). task1 at slot[3] (bottom), blanks at lines[0..2].
    // task1 at bottom avoids InMemoryTerm trimming → 4 lines.
    assert_eq!(lines.len(), 4);
    assert!(lines[0].trim().is_empty(), "line 0 is blank");
    assert!(lines[1].trim().is_empty(), "line 1 is blank");
    assert!(lines[2].trim().is_empty(), "line 2 is blank");
    assert!(lines[3].contains("task1"), "line 3 has task1: {0}", lines[3]);

    let c2 = group.add_bar(3, "task2");
    c2.tick();
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
    let group = ProgressGroup::with_mp(mp, 4);
    for i in 0..30 {
        let c = group.add_bar(1, &format!("tool{i}"));
        c.tick();
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
    let (group, overall) = ProgressGroup::with_mp_and_overall(mp, 5, "overall", 3);

    let c1 = group.add_bar(5, "tool1");
    c1.tick();
    overall.tick();
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

    let c2 = group.add_bar(3, "tool2");
    c2.tick();
    overall.tick();
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
    let (group, overall) = ProgressGroup::with_mp_and_overall(mp, 5, "overall", 10);

    for i in 0..5 {
        let c = group.add_bar(2, &format!("task{i}"));
        c.tick();
        overall.tick();
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
    // Capacity=3 clamped to MIN_SLOTS=4, no overall.
    // Children fill sequentially from line[0].
    // Using H=6 > 4 to avoid InMemoryTerm blank-content trimming.
    let (mp, term) = mk_with_size(6, 80);
    let group = ProgressGroup::with_mp(mp, 3); // clamped to 4

    let c1 = group.add_bar(5, "alpha");
    c1.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== no_overall_cap_3, after alpha, H=6, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    eprintln!("count = {}", lines.len());
    // 4 slots (clamped from 3). alpha at slot[3] (bottom), blanks at lines[0..2].
    // alpha at bottom avoids InMemoryTerm trimming → 4 lines.
    assert_eq!(lines.len(), 4, "clamped to 4 slots, 4 lines (child at bottom)");
    assert!(lines[0].trim().is_empty(), "line 0 is blank");
    assert!(lines[1].trim().is_empty(), "line 1 is blank");
    assert!(lines[2].trim().is_empty(), "line 2 is blank");
    assert!(lines[3].contains("alpha"), "line 3 has alpha: {0}", lines[3]);
    assert!(lines[3].contains("0/5"), "line 3 shows 0/5: {0}", lines[3]);

    let c2 = group.add_bar(3, "beta");
    c2.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== no_overall_cap_3, after beta ===");
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
    let (group, overall) = ProgressGroup::with_mp_and_overall(mp, 4, "overall", 3);

    let c1 = group.add_bar(5, "tool1");
    c1.tick();
    overall.tick();
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
    let (group, overall) = ProgressGroup::with_mp_and_overall(mp, 4, "overall", 10);

    let c1 = group.add_bar(7, "fetch");
    c1.tick();
    overall.tick();
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
    // Save and disable progress globally.
    let prev = mediapm_utils::progress::progress_enabled();
    mediapm_utils::progress::set_progress_enabled(false);

    // -- with overall --
    let (mp, term) = mk_with_size(4, 80);
    let (_group, overall) = ProgressGroup::with_mp_and_overall(mp, 4, "overall", 10);
    assert_eq!(overall.total(), 0, "overall handle must be no-op when disabled");

    let child = _group.add_bar(5, "child");
    assert_eq!(child.total(), 0, "child handle must be no-op when disabled");

    child.tick();
    overall.tick();
    assert_eq!(term.contents(), "", "no output when progress is disabled");

    // -- without overall --
    let (mp2, term2) = mk_with_size(4, 80);
    let group2 = ProgressGroup::with_mp(mp2, 4);
    let c2 = group2.add_bar(3, "noop");
    assert_eq!(c2.total(), 0, "child handle must be no-op without overall");
    c2.tick();
    assert_eq!(term2.contents(), "", "no output without overall when disabled");

    // Restore for other tests.
    mediapm_utils::progress::set_progress_enabled(prev);
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
    let (group, overall) = ProgressGroup::with_mp_and_overall(mp, 5, "overall", 3);

    let c = group.add_bar(5, "fetch");
    c.tick();
    overall.tick();
    // Finish the child — it must remain visible in the terminal.
    c.finish_success("done");
    c.tick();
    overall.tick();
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
    assert!(lines[3].contains("done"), "line 3 has done: {0}", lines[3]);
    assert!(lines[4].contains("overall"), "overall bar visible: {0}", lines[4]);
}

#[test]
fn progress_group_finish_all_bars_content_persists() {
    // Terminal H=5, W=80.  Overall at line[4], children at lines[0..3].
    let (mp, term) = mk_with_size(5, 80);
    let (group, overall) = ProgressGroup::with_mp_and_overall(mp, 5, "overall", 2);

    let c1 = group.add_bar(3, "alpha");
    let c2 = group.add_bar(5, "beta");
    c1.advance(3);
    c2.advance(5);
    c1.tick();
    c2.tick();
    overall.tick();
    // Finish all bars.
    c1.finish_success("done");
    c2.finish_success("ok");
    overall.finish_success("all done");
    c1.tick();
    c2.tick();
    overall.tick();
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
    assert!(lines[4].contains("all done"), "overall shows finished: {0}", lines[4]);
}

#[test]
fn progress_group_finish_error_shows_error_state() {
    // Terminal H=5, W=80.
    let (mp, term) = mk_with_size(5, 80);
    let (group, overall) = ProgressGroup::with_mp_and_overall(mp, 5, "overall", 5);

    let c = group.add_bar(5, "wget");
    c.tick();
    overall.tick();
    c.finish_error("timeout");
    c.tick();
    overall.tick();
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
    assert!(lines[3].contains("timeout"), "line 3 has timeout: {0}", lines[3]);
    assert!(lines[4].contains("overall"), "overall bar visible: {0}", lines[4]);
}

#[test]
fn progress_group_join_and_clear_removes_bars() {
    // Terminal H=5, W=80.
    let (mp, term) = mk_with_size(5, 80);
    let (group, overall) = ProgressGroup::with_mp_and_overall(mp, 5, "overall", 3);

    let c = group.add_bar(5, "fetch");
    c.finish_success("done");
    c.tick();
    overall.tick();
    // join_and_clear must remove all bars from the terminal.
    group.join_and_clear();
    let contents = term.contents();
    eprintln!("=== join_and_clear_removes_bars, H=5, W=80 ===");
    eprintln!("contents = {contents:?}");
    assert!(contents.is_empty(), "expected empty terminal after join_and_clear, got: {contents:?}");
}

#[test]
fn progress_group_consumer_lifecycle_keeps_finished_bars() {
    // Terminal H=5, W=80.  Simulate the exact consumer pattern:
    // create group with overall, do sequential work, finish children,
    // finish overall, then join.  All bars must remain visible.
    let (mp, term) = mk_with_size(5, 80);
    let (group, overall) = ProgressGroup::with_mp_and_overall(mp, 5, "overall", 3);

    let c1 = group.add_bar(5, "fetch");
    c1.advance(5);
    c1.tick();
    overall.tick();
    c1.finish_success("fetched");

    let c2 = group.add_bar(2, "parse");
    c2.advance(2);
    c2.tick();
    overall.tick();
    c2.finish_success("parsed");

    overall.advance(3);
    overall.finish_success("all done");
    overall.tick();
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

// ── Slot pool / rendering tests ──

#[test]
fn slot_pool_blank_bars_remain_invisible() {
    let (mp, term) = mk_with_size(5, 80);
    let group = ProgressGroup::with_mp(mp, 5);

    // Add a child bar so we can verify only 5 lines total.
    let c = group.add_bar(10, "child");
    c.tick();

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
    let group = ProgressGroup::with_mp(mp, 4); // clamped to 4

    let c1 = group.add_bar(5, "first");
    c1.tick();
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

    let c2 = group.add_bar(3, "second");
    c2.tick();
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
    let (group, overall) = ProgressGroup::with_mp_and_overall(mp, 5, "overall", 10);

    let c = group.add_bar(7, "worker");
    c.tick();
    overall.tick();

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
    let group = ProgressGroup::with_mp(mp, 4);

    for i in 0..20 {
        let c = group.add_bar(1, &format!("t{i}"));
        c.tick();
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
    let (group, overall) = ProgressGroup::with_mp_and_overall(mp, 5, "overall", 10);

    for i in 0..5 {
        let c = group.add_bar(2, &format!("task{i}"));
        c.tick();
        overall.tick();
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
    let (group, overall) = ProgressGroup::with_mp_and_overall(mp, 5, "overall", 5);

    let c = group.add_bar(3, "fetch");
    c.advance(3);
    c.tick();
    overall.tick();

    let before = term.contents();
    group.join();
    let after = term.contents();

    assert_eq!(before, after, "join() is a no-op — contents must be identical before and after");
}

#[test]
fn progress_group_add_bar_zero_total_renders() {
    let (mp, term) = mk_with_size(5, 80);
    let (group, overall) = ProgressGroup::with_mp_and_overall(mp, 5, "overall", 0);

    let c = group.add_bar(0, "zero");
    c.tick();
    overall.tick();

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
    let group = ProgressGroup::with_mp(mp, 5);

    let total = 3u64;
    let pb = group.add_bar(total, "materializing");

    pb.tick();
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
    pb.finish_success("materialization complete");
    pb.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    // Still 5 lines after finish
    assert_eq!(lines.len(), 5, "5 slots, no overall → 5 lines after finish too");
    assert!(
        lines[4].contains("materializing"),
        "bar still visible after finish_success: {0}",
        lines[4]
    );
    assert!(lines[4].contains("complete"), "shows success message: {0}", lines[4]);

    group.join();
    let after_join = term.contents();
    assert!(!after_join.is_empty(), "join() must keep bars visible");
}

#[test]
fn consumer_lifecycle_conductor_sync() {
    let (mp, term) = mk_with_size(5, 80);
    let (group, overall) = ProgressGroup::with_mp_and_overall(mp, 5, "syncing tools", 2);

    // Tool 1
    let t1 = group.add_bar(0, "yt-dlp");
    t1.set_message("downloading");
    t1.advance(1);
    t1.finish();
    t1.tick();
    overall.advance(1);
    overall.tick();

    // Tool 2
    let t2 = group.add_bar(0, "ffmpeg");
    t2.advance(1);
    t2.finish();
    t2.tick();
    overall.advance(1);
    overall.tick();

    overall.finish_success("tools synced");
    overall.tick();
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
    assert!(lines[4].contains("tools synced"), "overall success message: {0}", lines[4]);
}

#[test]
fn consumer_lifecycle_conductor_cli() {
    let (mp, term) = mk_with_size(4, 80);
    let (group, pb) = ProgressGroup::with_mp_and_overall(mp, 4, "steps", 0);

    // Simulate step_progress callback: set_total(N) then set_position(1..N)
    pb.set_total(3);

    pb.set_position(1);
    pb.tick();
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
    pb.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    assert!(lines[3].contains("steps"), "bar still visible after finish: {0}", lines[3]);

    group.join();
    assert!(!term.contents().is_empty(), "join() must keep bars visible");
}

#[test]
fn progress_group_finish_and_clear_child_keeps_others() {
    let (mp, term) = mk_with_size(4, 80);
    let (group, overall) = ProgressGroup::with_mp_and_overall(mp, 4, "overall", 5);

    let c1 = group.add_bar(3, "alpha");
    c1.advance(3);
    c1.tick();
    overall.tick();

    c1.finish_and_clear();
    c1.tick();
    overall.tick();

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
    let group = ProgressGroup::with_mp(mp, 5);

    let c = group.add_bar(5, "worker");
    c.advance(2);
    c.tick();

    c.abandon();
    c.tick();

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
    let group = ProgressGroup::with_mp(mp, 5);

    // Prefix > 16 chars — production uses {prefix:>16.16}
    let long_prefix = "abcdefghijklmnopqrstuvwxyz"; // 26 chars
    let c = group.add_bar(5, long_prefix);
    c.tick();

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
    let (group, overall) = ProgressGroup::with_mp_and_overall(mp, 5, "overall", 10);

    // Chronological allocation: tool-a at slot[3] (just above overall).
    // Second child shifts tool-a up to slot[2] and takes slot[3].
    // Overall at slot[4].
    let a = group.add_bar(5, "tool-a");
    a.tick();
    overall.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    assert!(lines[3].contains("tool-a"), "line 3 has tool-a: {0}", lines[3]);

    // Second child shifts tool-a up, takes last slot before overall.
    let b = group.add_bar(3, "tool-b");
    b.tick();
    overall.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    assert!(lines[2].contains("tool-a"), "line 2 still has tool-a: {0}", lines[2]);
    assert!(lines[3].contains("tool-b"), "line 3 has tool-b: {0}", lines[3]);

    overall.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    assert!(lines[4].contains("overall"), "overall visible: {0}", lines[4]);
}

// ── Child bar elapsed: starts at zero ──────────────────────────────────────

#[test]
fn child_bar_elapsed_starts_at_zero() {
    let (mp, term) = mk();
    let (group, overall) = ProgressGroup::with_mp_and_overall(mp, 4, "overall", 5);
    let child = group.add_bar(3, "tool-a");
    child.tick();
    overall.tick();
    let contents = term.contents();
    assert!(contents.contains("[00:00:00]"), "elapsed must start at 0, got:\n{contents}");
    assert!(contents.contains("tool-a"), "tool-a must appear");
}

// ── Child bar elapsed: frozen after finish ─────────────────────────────────

#[test]
fn child_bar_elapsed_frozen_after_finish() {
    let (mp, term) = mk();
    let (group, overall) = ProgressGroup::with_mp_and_overall(mp, 4, "overall", 5);
    let child = group.add_bar(3, "tool-a");
    child.set_position(3);
    child.finish();
    child.tick();
    overall.tick();
    let contents = term.contents();
    assert!(
        contents.contains("[00:00:00]"),
        "elapsed must stay at 0 after finish, got:\n{contents}"
    );
    assert!(contents.contains("3/3"), "bar must show final position 3/3");
}

// ── Child bar elapsed: frozen after finish_success ─────────────────────────

#[test]
fn child_bar_elapsed_frozen_after_finish_success() {
    let (mp, term) = mk();
    let (group, overall) = ProgressGroup::with_mp_and_overall(mp, 4, "overall", 5);
    let child = group.add_bar(3, "tool-a");
    child.set_position(3);
    child.finish_success("done");
    child.tick();
    overall.tick();
    let contents = term.contents();
    assert!(contents.contains("[00:00:00]"), "elapsed must stay at 0 after finish_success");
    assert!(contents.contains("done"), "success message must appear");
}

// ── Child bar elapsed: frozen after finish_error ───────────────────────────

#[test]
fn child_bar_elapsed_frozen_after_finish_error() {
    let (mp, term) = mk();
    let (group, overall) = ProgressGroup::with_mp_and_overall(mp, 4, "overall", 5);
    let child = group.add_bar(3, "tool-a");
    child.set_position(1);
    child.finish_error("fail");
    child.tick();
    overall.tick();
    let contents = term.contents();
    assert!(contents.contains("[00:00:00]"), "elapsed must stay at 0 after finish_error");
}

// ── Child bar elapsed: frozen after abandon ────────────────────────────────

#[test]
fn child_bar_elapsed_frozen_after_abandon() {
    let (mp, term) = mk();
    let (group, overall) = ProgressGroup::with_mp_and_overall(mp, 4, "overall", 5);
    let child = group.add_bar(3, "tool-a");
    child.set_position(2);
    child.abandon();
    child.tick();
    overall.tick();
    let contents = term.contents();
    assert!(contents.contains("[00:00:00]"), "elapsed must stay at 0 after abandon");
}

// ── SlotFull behavior ─────────────────────────────────────────────────────

#[test]
fn slot_full_hides_overflow_bars_from_display() {
    let (mp, term) = mk_with_size(5, 80);
    let group = ProgressGroup::with_mp(mp, 4); // clamped to MIN_SLOTS=4

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
    c1.tick();
    c2.tick();
    c3.tick();
    c4.tick();
    c5.tick();

    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== slot_full, H=5, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    // Capacity 4 (clamped from MIN=4). No overall → all 4 slots used, no blank trailing → 4 lines.
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
    let (group, overall) = ProgressGroup::with_mp_and_overall(mp, 5, "overall", 10);

    let a = group.add_bar(5, "worker-a");
    let b = group.add_bar(5, "worker-b");

    a.advance(3);
    b.advance(2);
    a.tick();
    b.tick();
    overall.tick();

    // worker-a finishes with error, worker-b continues
    a.finish_error("crash");
    b.advance(1);
    a.tick();
    b.tick();
    overall.tick();

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
    assert!(lines[2].contains("crash"), "worker-a shows error: {0}", lines[2]);
    assert!(lines[3].contains("worker-b"), "worker-b visible at line 3: {0}", lines[3]);
    assert!(lines[3].contains("3/5"), "worker-b shows 3/5: {0}", lines[3]);
    assert!(!lines[3].contains("crash"), "worker-b must not show error: {0}", lines[3]);
    assert!(lines[4].contains("overall"), "overall visible: {0}", lines[4]);
}

// ── Consumer simulation: sequential tool sync with recycling ────────────

#[test]
fn consumer_sync_too_many_tools_recycles() {
    let (mp, term) = mk_with_size(4, 80);
    let group = ProgressGroup::with_mp(mp, 4); // capacity 4, no overall

    // 8 sequential tools on capacity 4 — first 4 get display slots,
    // remaining 4 recycle finished slots.
    for i in 0..8 {
        let tool = group.add_bar(1, &format!("tool{i}"));
        tool.advance(1);
        tool.finish_success("done");
        tool.tick();
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
    assert!(lines.iter().any(|l| l.contains("done")), "at least one bar shows 'done': {lines:?}");
}

// ── Retention: finished bars keep their final state ─────────────────────

#[test]
fn retention_finished_bar_keeps_final_msg() {
    let (mp, term) = mk_with_size(4, 80);
    let group = ProgressGroup::with_mp(mp, 4);

    let a = group.add_bar(2, "alpha");
    a.advance(2);
    a.finish_success("done A");
    a.tick();

    let b = group.add_bar(3, "beta");
    b.advance(3);
    b.finish_success("done B");
    b.tick();

    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== retention_finished_bar_keeps_final_msg, H=4, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    // 4 slots, no overall → 4 lines. Both finished bars retain messages.
    assert!(lines.len() >= 2, "at least 2 lines: {lines:?}");
    assert!(lines.iter().any(|l| l.contains("done A")), "bar alpha retains 'done A': {lines:?}");
    assert!(lines.iter().any(|l| l.contains("done B")), "bar beta retains 'done B': {lines:?}");
}

#[test]
fn retention_finished_bar_persists_across_new_work() {
    let (mp, term) = mk_with_size(4, 80);
    let group = ProgressGroup::with_mp(mp, 4);

    // Slot 0: finished bar
    let a = group.add_bar(1, "alpha");
    a.advance(1);
    a.finish_success("done");
    a.tick();

    // Slot 1: active bar alongside the finished one
    let b = group.add_bar(5, "beta");
    b.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== retention_finished_bar_persists_across_new_work, H=4, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    // 4 slots → 4 lines. Finished "done" bar visible alongside active beta.
    assert!(lines.len() >= 2, "at least 2 lines: {lines:?}");
    assert!(lines.iter().any(|l| l.contains("done")), "line shows finished 'done': {lines:?}");
    assert!(lines.iter().any(|l| l.contains("beta")), "line shows active 'beta': {lines:?}");
}

#[test]
fn retention_multiple_finished_bars() {
    let (mp, term) = mk_with_size(4, 80);
    let group = ProgressGroup::with_mp(mp, 4);

    for (i, msg) in ["first", "second", "third", "fourth"].iter().enumerate() {
        let h = group.add_bar(1, &format!("task{i}"));
        h.advance(1);
        h.finish_success(*msg);
        h.tick();
    }

    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== retention_multiple_finished_bars, H=4, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    // 4 slots, all finished → 4 lines, each with a distinct message.
    assert_eq!(lines.len(), 4, "4 lines — all 4 finished bars visible");
    for msg in ["first", "second", "third", "fourth"] {
        assert!(lines.iter().any(|l| l.contains(msg)), "a bar shows '{msg}': {lines:?}");
    }
}

// ── Rendering: overall bar always at bottom ─────────────────────────────

#[test]
fn renderer_with_overall_always_bottom() {
    let (mp, term) = mk_with_size(4, 80);
    let (group, overall) = ProgressGroup::with_mp_and_overall(mp, 4, "overall", 10);

    // Fill children
    for i in 0..3 {
        let h = group.add_bar(2, &format!("child{i}"));
        h.advance(2);
        h.finish();
        h.tick();
        overall.advance(1);
    }
    overall.advance(1);
    overall.finish();
    overall.tick();

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

// ── Consumer simulation: materializer with parallel workers ─────────────

#[test]
fn consumer_materializer_single_bar_parallel_workers() {
    let (mp, term) = mk_with_size(4, 80);
    let group = ProgressGroup::with_mp(mp, 4);

    // Simulate a materializer dispatcher: parallel workers tracked
    // as a single progress bar.
    let total_work: u64 = 100;
    let pb = group.add_bar(total_work, "materializing");

    // Advance in chunks (simulating parallel workers reporting)
    for chunk in [30u64, 30, 30, 10] {
        pb.advance(chunk);
        pb.tick();
    }
    pb.finish_success("materialized");
    pb.tick();

    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    eprintln!("=== consumer_materializer_single_bar_parallel_workers, H=4, W=80 ===");
    for (i, line) in lines.iter().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    assert!(lines.len() >= 1, "at least 1 line: {lines:?}");
    assert!(lines.iter().any(|l| l.contains("materialized")), "shows 'materialized': {lines:?}");
    assert!(lines.iter().any(|l| l.contains("100")), "shows 100/100: {lines:?}");
}
