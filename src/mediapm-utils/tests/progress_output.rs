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
fn spinner_style() -> ProgressStyle {
    ProgressStyle::with_template(TS).unwrap().progress_chars("█░").tick_chars("◐◓")
}

/// Create a spinner [`ProgressStyle`] with a 6-frame cycle for animation tests.
///
/// Empirical probe: with N tick chars, the visible cycling goes through indices
/// 1, 2, …, N-2, 0 (skipping the last index N-1).  A 6-frame string yields
/// 5 distinct cycling frames: ◓ → ◑ → ◒ → ● → ◐ → …  The 6th frame (○) is
/// only shown when the bar finishes or is abandoned.
fn spinner_long_style() -> ProgressStyle {
    ProgressStyle::with_template(TS).unwrap().progress_chars("█░").tick_chars("◐◓◑◒●○")
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
    // First tick shows ◐.
    assert_eq!(s, "◐     test [00:00:00] ░░░░░░░░░░░░░ 0/5");
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
            "◐    tool1 [00:00:00] ░░░░░░░░░░░░░ 0/2\n",
            " overall [00:00:00] ░░░░░░░░░░░░░░░ 0/4",
        ),
    );
}

#[test]
fn spinner_finishes() {
    let (mp, term) = mk();
    let pb = add_bar(&mp, 5, "test");
    pb.set_style(spinner_style());
    pb.tick(); // ◐
    pb.finish_with_message("done");
    pb.tick(); // ◓ (second tick char after finish ticks)
    assert_eq!(term.contents(), "◓     test [00:00:00] █████████ 5/5 done");
}

// ── Spinner: multi-frame animation (uses 5-frame cycle) ──────────────────────

#[test]
fn spinner_animation_cycle() {
    let (mp, term) = mk();
    let pb = add_bar(&mp, 5, "test");
    pb.set_style(spinner_long_style());
    // 6-frame "◐◓◑◒●○" cycles as: ◓ → ◑ → ◒ → ● → ◐ → ◓ → …
    pb.tick();
    assert_eq!(term.contents(), "◓     test [00:00:00] ░░░░░░░░░░░░░ 0/5", "frame 1/5");
    pb.tick();
    assert_eq!(term.contents(), "◑     test [00:00:00] ░░░░░░░░░░░░░ 0/5", "frame 2/5");
    pb.tick();
    assert_eq!(term.contents(), "◒     test [00:00:00] ░░░░░░░░░░░░░ 0/5", "frame 3/5");
    pb.tick();
    assert_eq!(term.contents(), "●     test [00:00:00] ░░░░░░░░░░░░░ 0/5", "frame 4/5");
    pb.tick();
    assert_eq!(term.contents(), "◐     test [00:00:00] ░░░░░░░░░░░░░ 0/5", "frame 5/5 wraps to ◐");
}

#[test]
fn spinner_child_animation_with_overall() {
    let (mp, term) = mk();
    let o = add_bar(&mp, 5, "overall");
    let c = ins_bar(&mp, &o, 2, "tool1");
    c.set_style(spinner_long_style());
    // Child progresses through frames while overall stays static.
    c.tick();
    o.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "◓    tool1 [00:00:00] ░░░░░░░░░░░░░ 0/2\n",
            " overall [00:00:00] ░░░░░░░░░░░░░░░ 0/5",
        ),
        "child frame 1/5 ◓",
    );
    c.tick();
    o.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "◑    tool1 [00:00:00] ░░░░░░░░░░░░░ 0/2\n",
            " overall [00:00:00] ░░░░░░░░░░░░░░░ 0/5",
        ),
        "child frame 2/5 ◑",
    );
    c.tick();
    o.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "◒    tool1 [00:00:00] ░░░░░░░░░░░░░ 0/2\n",
            " overall [00:00:00] ░░░░░░░░░░░░░░░ 0/5",
        ),
        "child frame 3/5 ◒",
    );
    c.tick();
    o.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "●    tool1 [00:00:00] ░░░░░░░░░░░░░ 0/2\n",
            " overall [00:00:00] ░░░░░░░░░░░░░░░ 0/5",
        ),
        "child frame 4/5 ●",
    );
}

// ── Spinner: multi-bar animation ─────────────────────────────────────────────

#[test]
fn spinner_both_animate_together() {
    let (mp, term) = mk();
    let o = add_bar(&mp, 5, "overall");
    o.set_style(spinner_long_style());
    let c = ins_bar(&mp, &o, 2, "tool1");
    c.set_style(spinner_long_style());
    // Both progress independently.
    c.tick();
    o.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "◓    tool1 [00:00:00] ░░░░░░░░░░░░░ 0/2\n",
            "◓  overall [00:00:00] ░░░░░░░░░░░░░ 0/5",
        ),
        "both frame 1/5 ◓",
    );
    c.tick();
    o.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "◑    tool1 [00:00:00] ░░░░░░░░░░░░░ 0/2\n",
            "◑  overall [00:00:00] ░░░░░░░░░░░░░ 0/5",
        ),
        "both frame 2/5 ◑",
    );
    c.tick();
    o.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "◒    tool1 [00:00:00] ░░░░░░░░░░░░░ 0/2\n",
            "◒  overall [00:00:00] ░░░░░░░░░░░░░ 0/5",
        ),
        "both frame 3/5 ◒",
    );
}

// ── Spinner: finish/reset/abandon animation behavior ─────────────────────────

#[test]
fn spinner_finish_frame_stability() {
    let (mp, term) = mk();
    let pb = add_bar(&mp, 5, "test");
    pb.set_style(spinner_long_style());
    pb.tick();
    assert_eq!(term.contents(), "◓     test [00:00:00] ░░░░░░░░░░░░░ 0/5");
    pb.tick();
    assert_eq!(term.contents(), "◑     test [00:00:00] ░░░░░░░░░░░░░ 0/5");
    // Finish — frame should settle on the last tick char (○).
    pb.finish_with_message("done");
    pb.tick();
    assert_eq!(term.contents(), "○     test [00:00:00] █████████ 5/5 done");
    // Additional ticks should still show the same final frame.
    pb.tick();
    assert_eq!(term.contents(), "○     test [00:00:00] █████████ 5/5 done");
}

#[test]
fn spinner_reset_continues_animation() {
    let (mp, term) = mk();
    let pb = add_bar(&mp, 5, "test");
    pb.set_style(spinner_long_style());
    pb.tick(); // ◓ (frame 1)
    pb.tick(); // ◑ (frame 2)
    pb.tick(); // ◒ (frame 3)
    pb.reset();
    pb.tick(); // Continued from frame 4 → ●
    // Reset does NOT restart the animation cycle; it continues from where it was.
    assert_eq!(
        term.contents(),
        "●     test [00:00:00] ░░░░░░░░░░░░░ 0/5",
        "after reset, animation continues from next frame (●)"
    );
}

#[test]
fn spinner_abandon_ends_on_last_frame() {
    let (mp, term) = mk();
    let pb = add_bar(&mp, 5, "test");
    pb.set_style(spinner_style());
    // 2-frame "◐◓": first tick shows ◐.
    pb.tick();
    assert_eq!(term.contents(), "◐     test [00:00:00] ░░░░░░░░░░░░░ 0/5");
    // Abandon — frame settles on the last tick char (◓).
    pb.abandon_with_message("failed");
    pb.tick();
    assert_eq!(term.contents(), "◓     test [00:00:00] ░░░░░░░ 0/5 failed");
    // Additional ticks stay on the final frame.
    pb.tick();
    assert_eq!(term.contents(), "◓     test [00:00:00] ░░░░░░░ 0/5 failed");
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
    assert_eq!(term.contents(), "◐  overall [00:00:00] ░░░░░░░░░░░░░ 0/5");
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
            "◐    tool1 [00:00:00] ░░░░░░░░░░░░░ 0/2\n",
            "◐  overall [00:00:00] ░░░░░░░░░░░░░ 0/5",
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
            "◐    tool1 [00:00:00] ██████░░░░░░░ 1/2\n",
            "◐  overall [00:00:00] ░░░░░░░░░░░░░ 0/5",
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
            "◐    tool1 [00:00:00] █████████████ 2/2\n",
            "◐  overall [00:00:00] ░░░░░░░░░░░░░ 0/5",
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
            "◓    tool1 [00:00:00] █████████ 2/2 done\n",
            "◐  overall [00:00:00] ░░░░░░░░░░░░░ 0/5",
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
            "◓    tool1 [00:00:00] █████████ 2/2 done\n",
            "◓  overall [00:00:00] █████████ 5/5 done",
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
