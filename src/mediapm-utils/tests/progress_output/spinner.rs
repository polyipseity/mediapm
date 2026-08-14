use indicatif::{InMemoryTerm, MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use mediapm_utils::progress::{
    DimensionSource, ProgressGroup, TestDimensionSource, TestTimeSource, TimeSource, TrackedHandle,
};
use std::sync::Arc;

use super::common::*;

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

// ── ProgressGroup spinner tests: dirty-independent redraw ────────────────────
//
// These tests verify the spec: every tick() advances the spinner character on
// active bars regardless of dirty state, and finished bars' spinners are frozen.

#[test]
fn spinner_advances_without_dirty() {
    let (mp, term, ts) = mk_with_size_and_ts(4, 80);
    let group = group_with_ts(mp, 4, &ts);
    let _child = group.add_bar(10, "test");

    // First tick establishes initial spinner frame.
    group.tick();
    let t1 = term.contents();

    // Subsequent ticks should advance the spinner even without dirty state.
    group.tick();
    let t2 = term.contents();
    group.tick();
    let t3 = term.contents();

    // All must show 0/10 (no progress made).
    assert_eq!(
        t1,
        concat!(
            "\n",
            "\n",
            "\n",
            "⠸                           test ░░░░░░░░░░░░░░░░░░░░░  0/10 0s 0/d",
        ),
        "tick 1 shows 0/10",
    );
    assert_eq!(
        t2,
        concat!(
            "\n",
            "\n",
            "\n",
            "⠼                           test ░░░░░░░░░░░░░░░░░░░░░  0/10 0s 0/d",
        ),
        "tick 2 shows 0/10",
    );
    assert_eq!(
        t3,
        concat!(
            "\n",
            "\n",
            "\n",
            "⠴                           test ░░░░░░░░░░░░░░░░░░░░░  0/10 0s 0/d",
        ),
        "tick 3 shows 0/10",
    );

    // Spinner must advance between each tick (time frozen → only spinner differs).
    assert_ne!(t1, t2, "spinner must advance on tick 1→2 (no dirty)");
    assert_ne!(t2, t3, "spinner must advance on tick 2→3 (no dirty)");
}

#[test]
fn spinner_does_not_advance_on_finished_bar() {
    let (mp, term, ts) = mk_with_size_and_ts(4, 80);
    let group = group_with_ts(mp, 4, &ts);
    let finished = group.add_bar(3, "done");
    finished.finish_success();
    let active = group.add_bar(10, "working");
    group.tick();

    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    let finished_line = lines[2].to_string();
    drop(contents);

    for i in 0..5 {
        active.advance(1);
        group.tick();
        let contents = term.contents();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(
            lines[2], finished_line,
            "finished bar must stay frozen across ticks (iteration {i})"
        );
    }
}

#[test]
fn spinner_active_among_finished() {
    let (mp, term, ts) = mk_with_size_and_ts(4, 80);
    let group = group_with_ts(mp, 4, &ts);
    let finished = group.add_bar(3, "done");
    finished.finish_success();
    let active = group.add_bar(10, "working");
    group.tick();

    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    let finished_line = lines[2].to_string();
    let first_active_line = lines[3].to_string();
    drop(contents);

    // Advance the active bar.
    active.advance(2);
    group.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();

    // Finished bar must stay frozen.
    assert_eq!(lines[2], finished_line, "finished bar must stay frozen");
    // Active bar shows progress.
    assert_eq!(
        lines[3], "⠦                        working ████░░░░░░░░░░░░░░░░░  2/10 0s 0/d",
        "active bar shows 2/10: {}",
        lines[3],
    );
    // Active bar content changed from previous tick (spinner + position).
    assert_ne!(lines[3], first_active_line, "active bar line changed");
}

#[test]
fn regression_spinner_dirty_independence() {
    let (mp, term, ts) = mk_with_size_and_ts(4, 80);
    let group = group_with_ts(mp, 4, &ts);
    let child = group.add_bar(10, "test");
    child.set_position(5);
    group.tick(); // Initial draw: 5/10, spinner at some frame
    let t1 = term.contents();

    // Three more ticks with NO changes.
    group.tick();
    let t2 = term.contents();
    group.tick();
    let t3 = term.contents();
    group.tick();
    let t4 = term.contents();

    // All ticks show 5/10 (stable position).
    assert_eq!(
        t1,
        concat!(
            "\n",
            "\n",
            "\n",
            "⠼                           test ██████████░░░░░░░░░░░  5/10 0s 0/d",
        ),
        "tick 1: 5/10",
    );
    assert_eq!(
        t2,
        concat!(
            "\n",
            "\n",
            "\n",
            "⠴                           test ██████████░░░░░░░░░░░  5/10 0s 0/d",
        ),
        "tick 2: 5/10",
    );
    assert_eq!(
        t3,
        concat!(
            "\n",
            "\n",
            "\n",
            "⠦                           test ██████████░░░░░░░░░░░  5/10 0s 0/d",
        ),
        "tick 3: 5/10",
    );
    assert_eq!(
        t4,
        concat!(
            "\n",
            "\n",
            "\n",
            "⠧                           test ██████████░░░░░░░░░░░  5/10 0s 0/d",
        ),
        "tick 4: 5/10",
    );

    // Spinner advances on each tick (content differs).
    assert_ne!(t1, t2, "spinner must advance tick 1→2 (no dirty)");
    assert_ne!(t2, t3, "spinner must advance tick 2→3 (no dirty)");
    assert_ne!(t3, t4, "spinner must advance tick 3→4 (no dirty)");
}

#[test]
fn spinner_stops_on_abandoned_bar() {
    let (mp, term, ts) = mk_with_size_and_ts(4, 80);
    let group = group_with_ts(mp, 4, &ts);
    let abandoned = group.add_bar(5, "abandoned");
    abandoned.set_position(2);
    abandoned.abandon();
    let active = group.add_bar(3, "active");
    group.tick();

    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    let abandoned_line = lines[2].to_string();
    drop(contents);

    for i in 0..5 {
        active.advance(1);
        group.tick();
        let contents = term.contents();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines[2], abandoned_line, "abandoned bar must stay frozen (iteration {i})");
    }
}

#[test]
fn spinner_stops_on_failed_bar() {
    let (mp, term, ts) = mk_with_size_and_ts(4, 80);
    let group = group_with_ts(mp, 4, &ts);
    let failed = group.add_bar(5, "failed");
    failed.set_position(2);
    failed.finish_error();
    let active = group.add_bar(3, "active");
    group.tick();

    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    let failed_line = lines[2].to_string();
    drop(contents);

    for i in 0..5 {
        active.advance(1);
        group.tick();
        let contents = term.contents();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines[2], failed_line, "failed bar must stay frozen (iteration {i})");
    }
}
