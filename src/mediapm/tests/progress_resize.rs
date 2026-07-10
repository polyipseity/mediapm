//! Consumer integration tests for progress bar terminal-resize reactivity.
//!
//! These tests validate that the [`ProgressGroup`] patterns used by the
//! service layer (`sync_hierarchy`, `reconcile_desired_tools`) handle
//! terminal width and height changes correctly when rendering to an
//! [`InMemoryTerm`](indicatif::InMemoryTerm).

use std::sync::Arc;

use indicatif::{InMemoryTerm, MultiProgress, ProgressDrawTarget};
use mediapm::output::{DimensionSource, ProgressGroup, TestDimensionSource};

fn mk_with_size(h: u16, w: u16) -> (MultiProgress, InMemoryTerm) {
    let term = InMemoryTerm::new(h, w);
    let target = ProgressDrawTarget::term_like(Box::new(term.clone()));
    (MultiProgress::with_draw_target(target), term)
}

/// Emulates the `sync_hierarchy` pattern (`ProgressGroup::new()` with no
/// overall bar) — verifies that height growth widens the terminal output.
#[test]
fn sync_hierarchy_height_change() {
    let dims = Arc::new(TestDimensionSource::new((4, 80)));
    let (mp, term) = mk_with_size(6, 80);
    let group =
        ProgressGroup::with_mp_and_dim(mp, 4, Arc::clone(&dims) as Arc<dyn DimensionSource>, true);
    let child = group.add_bar(10, "materialize");
    child.tick();

    let initial = term.contents();
    eprintln!("=== sync_hierarchy, H=4 ===");
    for (i, line) in initial.lines().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    assert!(initial.contains("materialize"), "child visible before resize");
    let initial_count = initial.lines().count();

    dims.set((6, 80));
    child.tick();
    let after = term.contents();
    eprintln!("=== after resize, H=6 ===");
    for (i, line) in after.lines().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    let after_count = after.lines().count();
    assert!(after.contains("materialize"), "child still visible after height resize");
    // Height grew from 4 to 6 → more render lines.
    assert!(
        after_count > initial_count,
        "more lines after height growth (was {initial_count}, now {after_count})"
    );
}

/// Emulates the `reconcile_desired_tools` pattern (`ProgressGroup::with_overall()`
/// with an overall bar) — verifies that width narrowing changes the bar template.
#[test]
fn reconcile_desired_tools_width_change() {
    let dims = Arc::new(TestDimensionSource::new((4, 80)));
    let (mp, term) = mk_with_size(4, 80);
    let (_group, overall) = ProgressGroup::with_mp_and_overall_and_dim(
        mp,
        4,
        "syncing tools",
        5,
        Arc::clone(&dims) as Arc<dyn DimensionSource>,
        false,
    );
    let contents_wide = term.contents();
    eprintln!("=== reconcile_tools, W=80 ===");
    for (i, line) in contents_wide.lines().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    assert!(contents_wide.contains("syncing tools"), "overall visible");

    dims.set((4, 40));
    overall.tick();
    let contents_narrow = term.contents();
    eprintln!("=== after resize, W=40 ===");
    for (i, line) in contents_narrow.lines().enumerate() {
        eprintln!("line[{i}] = {line:?}");
    }
    assert!(contents_narrow.contains("syncing tools"), "overall still visible after width resize");
    assert_ne!(
        contents_wide, contents_narrow,
        "output changes when width narrows (different bar templates)"
    );
}

/// Emulates a combined resize scenario: start, grow, then restore original
/// dimensions.  Uses the `sync_hierarchy` no-overall pattern.
#[test]
fn sync_hierarchy_complex_resize_scenario() {
    let dims = Arc::new(TestDimensionSource::new((4, 80)));
    let (mp, term) = mk_with_size(6, 80);
    let group =
        ProgressGroup::with_mp_and_dim(mp, 4, Arc::clone(&dims) as Arc<dyn DimensionSource>, true);
    let child = group.add_bar(10, "materialize");
    child.tick();
    let original = term.contents();
    assert!(original.contains("materialize"), "child visible at start");

    // Grow height.
    dims.set((6, 80));
    child.tick();
    let grown = term.contents();
    assert!(grown.contains("materialize"), "child visible after growth");
    assert!(grown.lines().count() > original.lines().count(), "more lines after growth");

    // Restore original dimensions.
    dims.set((4, 80));
    child.tick();
    let restored = term.contents();
    assert!(restored.contains("materialize"), "child visible after restore");
    // Width is unchanged so the content should match original modulo spinner.
    assert_eq!(
        restored.lines().count(),
        original.lines().count(),
        "restored line count matches original"
    );
}
