use indicatif::{InMemoryTerm, MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use mediapm_utils::progress::{
    DimensionSource, ProgressGroup, TestDimensionSource, TestTimeSource, TimeSource, TrackedHandle,
};
use std::sync::Arc;

use super::common::*;

#[test]
fn color_active_child_text() {
    let (mp, term) = mk_with_size(2, 80);
    let (group, _overall) = group_with_overall(mp, 2, "overall", 1);
    let child = group.add_bar(5, "child");
    child.set_position(0);
    group.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "⠹                          child ░░░░░░░░░░░░░░░░░░░░░  0/5 0s 0/d\n",
            "⠹                        overall ░░░░░░░░░░░░░░░░░░░░░  0/1 0s 0/d",
        )
    );
}

/// Failed bar: [F] bracket shown, values correct.
#[test]
fn color_failed_bracket_text() {
    let (mp, term) = mk_with_size(2, 80);
    let (group, _overall) = group_with_overall(mp, 2, "overall", 1);
    let child = group.add_bar(5, "child");
    group.tick();
    child.finish_error();
    group.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "⠏                      [F] child ░░░░░░░░░░░░░░░░░░░░░  0/5 0s\n",
            "⠸                        overall ░░░░░░░░░░░░░░░░░░░░░  0/1 0s 0/d",
        )
    );
}

/// Abandoned bar: [A] bracket shown, values correct.
#[test]
fn color_abandoned_bracket_text() {
    let (mp, term) = mk_with_size(2, 80);
    let (group, _overall) = group_with_overall(mp, 2, "overall", 1);
    let child = group.add_bar(5, "child");
    group.tick();
    child.abandon();
    group.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "⠏                      [A] child ░░░░░░░░░░░░░░░░░░░░░  0/5 0s\n",
            "⠸                        overall ░░░░░░░░░░░░░░░░░░░░░  0/1 0s 0/d",
        )
    );
}

/// Success bar: full count/total, no brackets.
#[test]
fn color_success_text() {
    let (mp, term) = mk_with_size(2, 80);
    let (group, _overall) = group_with_overall(mp, 2, "overall", 1);
    let child = group.add_bar(5, "child");
    child.set_position(5);
    group.tick();
    child.finish_success();
    group.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "⠏                          child █████████████████████  5/5 0s\n",
            "⠸                        overall ░░░░░░░░░░░░░░░░░░░░░  0/1 0s 0/d",
        )
    );
}

/// Finished bar (via `finish()`): full count/total, no [S] bracket.
#[test]
fn color_finished_text() {
    let (mp, term) = mk_with_size(2, 80);
    let (group, _overall) = group_with_overall(mp, 2, "overall", 1);
    let child = group.add_bar(5, "child");
    child.set_position(5);
    group.tick();
    child.finish();
    group.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "⠏                          child █████████████████████  5/5 0s\n",
            "⠸                        overall ░░░░░░░░░░░░░░░░░░░░░  0/1 0s 0/d",
        )
    );
}

/// No [S] or [=] brackets appear anywhere in the output.
#[test]
fn color_no_success_brackets() {
    let (mp, term) = mk_with_size(2, 80);
    let (group, _overall) = group_with_overall(mp, 2, "overall", 1);
    let child = group.add_bar(5, "child");
    group.tick();
    child.finish_success();
    group.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "⠏                          child █████████████████████  0/5 0s\n",
            "⠸                        overall ░░░░░░░░░░░░░░░░░░░░░  0/1 0s 0/d",
        )
    );
}

// ═════════════════════════════════════════════════════════════════════════════
// Phase 2: Exact-output regression tests
// ═════════════════════════════════════════════════════════════════════════════

/// Exact output match for active child with overall bar.
#[test]
fn exact_color_active_child_with_overall() {
    let (mp, term) = mk_with_size(2, 80);
    let (group, _overall) = group_with_overall(mp, 2, "overall", 1);
    let child = group.add_bar(5, "child");
    child.set_position(0);
    group.tick();

    assert_eq!(
        term.contents(),
        concat!(
            "⠹                          child ░░░░░░░░░░░░░░░░░░░░░  0/5 0s 0/d\n",
            "⠹                        overall ░░░░░░░░░░░░░░░░░░░░░  0/1 0s 0/d",
        )
    );
}

/// Exact output match for failed child with overall bar.
#[test]
fn exact_color_failed_child_with_overall() {
    let (mp, term) = mk_with_size(2, 80);
    let (group, _overall) = group_with_overall(mp, 2, "overall", 1);
    let child = group.add_bar(5, "child");
    group.tick();
    child.finish_error();
    group.tick();

    assert_eq!(
        term.contents(),
        concat!(
            "⠏                      [F] child ░░░░░░░░░░░░░░░░░░░░░  0/5 0s\n",
            "⠸                        overall ░░░░░░░░░░░░░░░░░░░░░  0/1 0s 0/d",
        )
    );
}

/// Exact output match for abandoned child with overall bar.
#[test]
fn exact_color_abandoned_child_with_overall() {
    let (mp, term) = mk_with_size(2, 80);
    let (group, _overall) = group_with_overall(mp, 2, "overall", 1);
    let child = group.add_bar(5, "child");
    group.tick();
    child.abandon();
    group.tick();

    assert_eq!(
        term.contents(),
        concat!(
            "⠏                      [A] child ░░░░░░░░░░░░░░░░░░░░░  0/5 0s\n",
            "⠸                        overall ░░░░░░░░░░░░░░░░░░░░░  0/1 0s 0/d",
        )
    );
}

/// Exact output match for successful child with overall bar.
#[test]
fn exact_color_success_child_with_overall() {
    let (mp, term) = mk_with_size(2, 80);
    let (group, _overall) = group_with_overall(mp, 2, "overall", 1);
    let child = group.add_bar(5, "child");
    child.set_position(5);
    group.tick();
    child.finish_success();
    group.tick();

    assert_eq!(
        term.contents(),
        concat!(
            "⠏                          child █████████████████████  5/5 0s\n",
            "⠸                        overall ░░░░░░░░░░░░░░░░░░░░░  0/1 0s 0/d",
        )
    );
}
