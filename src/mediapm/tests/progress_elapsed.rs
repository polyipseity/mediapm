//! Consumer integration tests for progress bar elapsed/duration behavior.
//!
//! These tests verify that child bars acquired from a [`ProgressGroup`] have
//! their elapsed timer start at zero (rather than inheriting the pool-creation
//! timestamp) and freeze after finish/abandon.

use indicatif::{InMemoryTerm, MultiProgress, ProgressDrawTarget};
use mediapm::output::ProgressGroup;

fn mk() -> (MultiProgress, InMemoryTerm) {
    let term = InMemoryTerm::new(24, 80);
    let target = ProgressDrawTarget::term_like(Box::new(term.clone()));
    (MultiProgress::with_draw_target(target), term)
}

#[test]
fn consumer_child_bar_elapsed_starts_at_zero() {
    let (mp, term) = mk();
    let group = ProgressGroup::with_mp(mp, 4);
    let _ = group.add_bar(5, "tool-a");
    let contents = term.contents();
    assert!(contents.contains("[00:00:00]"), "elapsed must start at 0, got:\n{contents}");
}

#[test]
fn consumer_child_bar_elapsed_frozen_after_finish() {
    let (mp, term) = mk();
    let group = ProgressGroup::with_mp(mp, 4);
    let child = group.add_bar(5, "tool-a");
    child.set_position(5);
    child.finish();
    group.tick();
    let contents = term.contents();
    assert!(
        contents.contains("[00:00:00]"),
        "elapsed must stay at 0 after finish, got:\n{contents}"
    );
    assert!(contents.contains("5/5"), "bar must show final position 5/5");
}

#[test]
fn consumer_child_bar_elapsed_frozen_after_finish_success() {
    let (mp, term) = mk();
    let group = ProgressGroup::with_mp(mp, 4);
    let child = group.add_bar(5, "tool-a");
    child.set_position(5);
    child.finish_success("ok");
    group.tick();
    let contents = term.contents();
    assert!(
        contents.contains("[00:00:00]"),
        "elapsed must stay at 0 after finish_success, got:\n{contents}"
    );
    assert!(contents.contains("ok"), "success message must appear");
}

#[test]
fn consumer_child_bar_elapsed_frozen_after_finish_error() {
    let (mp, term) = mk();
    let group = ProgressGroup::with_mp(mp, 4);
    let child = group.add_bar(5, "tool-a");
    child.set_position(2);
    child.finish_error("fail");
    let contents = term.contents();
    assert!(
        contents.contains("[00:00:00]"),
        "elapsed must stay at 0 after finish_error, got:\n{contents}"
    );
}

#[test]
fn consumer_child_bar_elapsed_frozen_after_abandon() {
    let (mp, term) = mk();
    let group = ProgressGroup::with_mp(mp, 4);
    let child = group.add_bar(5, "tool-a");
    child.set_position(3);
    child.abandon();
    let contents = term.contents();
    assert!(
        contents.contains("[00:00:00]"),
        "elapsed must stay at 0 after abandon, got:\n{contents}"
    );
}
