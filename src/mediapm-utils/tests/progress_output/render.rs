//! Exact-output tests for rendered bar shapes.
//!
//! One canonical test per terminal status (active, success, `[F]` failure, an
//! overall bar that failed while its child succeeded), plus the geometry and
//! rate/ETA fields those lines carry. Every assertion is an exact equality
//! against the full captured frame: rendered output is not a stability
//! contract in general, but these strings are what pins the layout algorithm,
//! the prefix/suffix field order, and the status markers until the shapes are
//! deliberately changed.

use std::sync::Arc;
use std::time::Duration;

use mediapm_utils::progress::{SuffixComponents, TestTimeSource};

use super::common::{H, W, mk_with_capacity, mk_with_capacity_and_ts};

/// An active child above a pinned overall bar renders both lines.
#[test]
fn active_child_with_overall_renders_exactly() {
    let (terminal, term) = mk_with_capacity(3, 80, 2);
    let (screen, _overall) = terminal.screen().with_overall("overall", 1).build();
    let child = screen.add_bar(5, "child");
    child.set_position(0);
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "\n",
            "⠸       child ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/5 0s 0/d\n",
            "⠹     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/1 0s 0/d"
        ),
        "active_child_with_overall_renders_exactly"
    );
}

/// A failed child carries the `[F]` marker and loses its rate field.
#[test]
fn failed_child_with_overall_shows_marker() {
    let (terminal, term) = mk_with_capacity(3, 80, 2);
    let (screen, _overall) = terminal.screen().with_overall("overall", 1).build();
    let child = screen.add_bar(5, "child");
    screen.tick();
    child.finish_error();
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "\n",
            "⠏              [F] child ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/5 0s\n",
            "⠸                overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/1 0s 0/d"
        ),
        "failed_child_with_overall_shows_marker"
    );
}

/// A successful child renders a full bar with no status bracket at all.
#[test]
fn success_child_with_overall_renders_full_bar() {
    let (terminal, term) = mk_with_capacity(3, 80, 2);
    let (screen, _overall) = terminal.screen().with_overall("overall", 1).build();
    let child = screen.add_bar(5, "child");
    child.set_position(5);
    screen.tick();
    child.finish_success();
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "\n",
            "⠏       child ███████████████████████████████████████████████████  5/5 0s\n",
            "⠸     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/1 0s 0/d"
        ),
        "success_child_with_overall_renders_full_bar"
    );
}

/// A successful child and a successful overall bar both render full bars and
/// full counts.
///
/// Restored from the pre-rebuild suite (`two_lines.rs:78`, inventory
/// `two_lines_exact_both_finished`), which could not be carried over while the
/// overall handle was disconnected from the renderer.
#[test]
fn both_child_and_overall_finish_render_exactly() {
    let ts = Arc::new(TestTimeSource::new());
    let (terminal, term) = mk_with_capacity_and_ts(3, 80, 2, &ts);
    let (screen, overall) = terminal.screen().with_overall("overall", 3).build();
    let child = screen.add_bar(5, "child");
    child.set_position(5);
    ts.advance(Duration::from_secs(1));
    screen.tick();
    child.finish_success();
    overall.advance(3);
    overall.finish_success();
    ts.advance(Duration::from_secs(1));
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "\n",
            "⠏       child ██████████████████████████████████████████████████████████  5/5 1s\n",
            "⠏     overall ██████████████████████████████████████████████████████████  3/3 1s"
        ),
        "both_child_and_overall_finish_render_exactly"
    );
}

/// A failed overall bar carries the `[F]` marker while its child still renders
/// as a success.
///
/// Restored from the pre-rebuild suite (`two_lines.rs:102`, inventory
/// `two_lines_exact_overall_abandoned`, an error variant byte-identical to
/// `two_lines.rs:125`). The overall's failure state only reaches the frame
/// through the handle the builder returns.
#[test]
fn overall_failed_while_child_succeeded_renders_marker() {
    let ts = Arc::new(TestTimeSource::new());
    let (terminal, term) = mk_with_capacity_and_ts(3, 80, 2, &ts);
    let (screen, overall) = terminal.screen().with_overall("overall", 3).build();
    let child = screen.add_bar(5, "child");
    child.set_position(5);
    ts.advance(Duration::from_secs(1));
    screen.tick();
    child.finish_success();
    overall.finish_error();
    ts.advance(Duration::from_secs(1));
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "\n",
            "⠏                    child █████████████████████████████████████████████  5/5 1s\n",
            "⠏              [F] overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/3 1s"
        ),
        "overall_failed_while_child_succeeded_renders_marker"
    );
}

/// A custom suffix survives onto a finished, failed bar.
#[test]
fn failed_child_custom_suffix_renders() {
    let ts = Arc::new(TestTimeSource::new());
    let (terminal, term) = mk_with_capacity_and_ts(3, 80, 2, &ts);
    let (screen, _overall) = terminal.screen().with_overall("overall", 5).build();
    let child = screen.add_bar(5, "child");
    child.set_position(3);
    ts.advance(Duration::from_secs(1));
    screen.tick();
    child
        .set_suffix_components(SuffixComponents { custom: "aborted".into(), ..Default::default() });
    child.finish_error();
    ts.advance(Duration::from_secs(1));
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "\n",
            "⠏              [F] child ███████████████████████░░░░░░░░░░░░░░░░  3/5 1s aborted\n",
            "⠸                overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/5 2s 0/d"
        ),
        "failed_child_custom_suffix_renders"
    );
}

/// A known progress/time pair renders a known rate and a derived ETA.
#[test]
fn rate_and_eta_at_known_progress() {
    let ts = Arc::new(TestTimeSource::new());
    let (terminal, term) = mk_with_capacity_and_ts(3, 80, 2, &ts);
    let (screen, _overall) = terminal.screen().with_overall("overall", 1).build();
    let child = screen.add_bar(1000, "test");
    child.set_position(500);
    ts.advance(Duration::from_secs(1));
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "\n",
            "⠼        test █████████████████████░░░░░░░░░░░░░░░░░░░░░  500/1.0k 1s 50/s 10s\n",
            "⠹     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/1 1s 0/d"
        ),
        "rate_and_eta_at_known_progress"
    );
}

/// An idle bar still renders a rate field (`0/d`) rather than omitting it.
#[test]
fn idle_bar_renders_zero_rate() {
    let ts = Arc::new(TestTimeSource::new());
    let (terminal, term) = mk_with_capacity_and_ts(3, 80, 2, &ts);
    let (screen, _overall) = terminal.screen().with_overall("overall", 1).build();
    let _child = screen.add_bar(100, "idle");
    ts.advance(Duration::from_secs(2));
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "\n",
            "⠸        idle ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/100 2s 0/d\n",
            "⠹     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/1 2s 0/d"
        ),
        "idle_bar_renders_zero_rate"
    );
}

/// The rate must not drift while nothing progresses: the elapsed clock keeps
/// moving, so a stale tick recomputes elapsed without touching the rate.
#[test]
fn rate_stable_on_stale_ticks() {
    let ts = Arc::new(TestTimeSource::new());
    let (terminal, term) = mk_with_capacity_and_ts(3, 80, 2, &ts);
    let (screen, _overall) = terminal.screen().with_overall("overall", 1).build();
    let child = screen.add_bar(1000, "test");
    child.set_position(500);
    screen.tick();
    ts.advance(Duration::from_millis(60));
    assert_eq!(
        &term.contents(),
        concat!(
            "\n",
            "⠼        test █████████████████████░░░░░░░░░░░░░░░░░░░░░░  500/1.0k 0s 0/d\n",
            "⠹     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/1 0s 0/d"
        ),
        "rate_stable_on_stale_ticks/progress"
    );

    for _ in 0..20 {
        ts.advance(Duration::from_millis(50));
        screen.tick();
    }
    assert_eq!(
        &term.contents(),
        concat!(
            "\n",
            "⠦        test ██████████████████████░░░░░░░░░░░░░░░░░░░░░░  500/1.0k 1s 455/s\n",
            "⠼     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/1 1s 0/d"
        ),
        "rate_stable_on_stale_ticks/stale"
    );
}

/// More progress recomputes the rate instead of leaving the earlier value.
#[test]
fn rate_recomputed_after_more_progress() {
    let ts = Arc::new(TestTimeSource::new());
    let (terminal, term) = mk_with_capacity_and_ts(3, 80, 2, &ts);
    let (screen, _overall) = terminal.screen().with_overall("overall", 1).build();
    let child = screen.add_bar(2000, "test");
    child.set_position(10);
    ts.advance(Duration::from_millis(2));
    screen.tick();
    ts.advance(Duration::from_millis(60));
    let small = term.contents();

    child.set_position(1500);
    screen.tick();
    ts.advance(Duration::from_millis(60));
    let large = term.contents();

    assert_ne!(small, large, "the rate/progress fields must differ between 10 and 1500");
    assert_eq!(
        &small,
        concat!(
            "\n",
            "⠼        test ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  10/2.0k 0s 500/s 3s\n",
            "⠹     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/1 0s 0/d"
        ),
        "rate_recomputed_after_more_progress/small"
    );
    assert_eq!(
        &large,
        concat!(
            "\n",
            "⠦        test ███████████████████████████████░░░░░░░░░░░  1.5k/2.0k 0s 2.9k/s 0s\n",
            "⠸     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/1 0s 0/d"
        ),
        "rate_recomputed_after_more_progress/large"
    );
}

/// A label inside the prefix budget is drawn in full: nothing is trimmed, and the
/// count/total field sits in the suffix exactly as composed.
///
/// The label is 26 characters against the 40-column prefix budget, so no
/// truncation happens here; this pins that a label which fits is not truncated.
/// The trim itself, and the order it removes fields in, is pinned by the inline
/// `semantic_truncate_prefix_*` unit tests.
#[test]
fn label_within_the_prefix_budget_is_not_truncated() {
    let (terminal, term) = mk_with_capacity(6, 80, 5);
    let screen = terminal.screen().build();
    let _child = screen.add_bar(5, "abcdefghijklmnopqrstuvwxyz");
    screen.tick();
    let contents = term.contents();
    let bar_line = contents.lines().next_back().expect("a bar line must be drawn");
    assert!(bar_line.contains("0/5"), "count/total must be present in full: {bar_line:?}");
    assert_eq!(
        contents,
        concat!(
            "\n",
            "\n",
            "\n",
            "\n",
            "⠸     abcdefghijklmnopqrstuvwxyz ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/5 0s 0/d"
        ),
        "label_within_the_prefix_budget_is_not_truncated"
    );
}

/// A bar whose total is zero still renders as a tracked bar (the `0/0` guard).
#[test]
fn zero_total_bar_renders() {
    let (terminal, term) = mk_with_capacity(6, 80, 5);
    let (screen, _overall) = terminal.screen().with_overall("overall", 0).build();
    let _child = screen.add_bar(0, "zero");
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "\n",
            "\n",
            "\n",
            "\n",
            "⠸        zero ██████████████████████████████████████████████████████  0/0 0s 0/d\n",
            "⠹     overall ██████████████████████████████████████████████████████  0/0 0s 0/d"
        ),
        "zero_total_bar_renders"
    );
}

/// No rendered line may carry an elapsed *bracket*: the elapsed field is part
/// of the composed suffix, so a template that re-introduced `{elapsed_precise}`
/// would print a second timestamp (and a `[` that does not belong).
#[test]
fn no_line_carries_an_elapsed_bracket() {
    let (terminal, term) = mk_with_capacity(H, W, 4);
    let (screen, _overall) = terminal.screen().with_overall("overall", 5).build();
    let _child = screen.add_bar(3, "tool-a");
    screen.tick();
    let contents = term.contents();
    for line in contents.lines() {
        assert!(
            !line.contains('['),
            "no status marker brackets are expected on these bars, so a `[` means a \
             duplicate elapsed template: {line:?}"
        );
    }
    assert_eq!(
        contents,
        concat!(
            "\n",
            "\n",
            "\n",
            "⠸      tool-a ░░░░░░░░░░░  0/3 0s 0/d\n",
            "⠹     overall ░░░░░░░░░░░  0/5 0s 0/d"
        ),
        "no_line_carries_an_elapsed_bracket"
    );
}
