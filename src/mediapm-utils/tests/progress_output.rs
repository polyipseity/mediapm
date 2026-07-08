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

const H: u16 = 24;
const W: u16 = 40;

/// Template with wide bar so the visual progress bar is visible in output.
const T: &str = "{prefix:>8.8} [{elapsed_precise}] {wide_bar} {pos}/{len} {msg}";

/// Template with spinner + wide bar for spinner tests.
const TS: &str = "{spinner} {prefix:>8.8} [{elapsed_precise}] {wide_bar} {pos}/{len} {msg}";

fn style() -> ProgressStyle {
    ProgressStyle::with_template(T).unwrap().progress_chars("█░")
}

fn spinner_style() -> ProgressStyle {
    ProgressStyle::with_template(TS).unwrap().progress_chars("█░").tick_chars("◐◓")
}

/// Create a [`MultiProgress`] + [`InMemoryTerm`] pair for one test.
fn mk() -> (MultiProgress, InMemoryTerm) {
    let term = InMemoryTerm::new(H, W);
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
