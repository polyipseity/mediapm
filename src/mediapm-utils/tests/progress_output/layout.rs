//! Slot-layout tests: fixed grid height, slot reuse, ordering, and labels.
//!
//! The renderer owns a fixed grid of reserved slots — one line per slot, never
//! more — and decides which bars are bound to which slot. These tests pin that
//! allocation: children fill upward from the bottom of the child region, the
//! overall bar stays in the last slot, older children keep the earlier lines,
//! and a bar that cannot be bound stays tracked but invisible.

use std::sync::Arc;
use std::time::Duration;

use mediapm_utils::progress::{
    BarLabelTruncation, Segment, SuffixComponents, TestDimensionSource, TestTimeSource,
    fit_segments,
};

use super::common::{mk_with_capacity, mk_with_capacity_and_ts, mk_with_dims};

/// A worker row's label, laid out the way
/// `mediapm_conductor::orchestration::progress_labels::WorkerBarLabel` lays it
/// out: the activity marker leads, the workflow and step identifiers follow,
/// and the parenthesised tool name is the one elastic segment.
///
/// Duplicated here because `mediapm-utils` cannot depend on the conductor.
/// What these tests are about is the budget the renderer hands the label, and
/// that is the same whichever client fills the slot.
struct WorkerRowLabel {
    /// Workflow the slot is running, e.g. `"default"`.
    workflow_id: String,
    /// Step the slot is running, e.g. `"s3"`.
    step_id: String,
    /// Conductor tool name, e.g. `"yt-dlp"`.
    tool: String,
    /// Worker state marker, e.g. `"active"`.
    activity: String,
}

impl BarLabelTruncation for WorkerRowLabel {
    fn truncate_prefix(&self, max_width: usize) -> String {
        let segments = vec![
            Segment::keep(format!("[{}]", self.activity)),
            Segment::keep(self.workflow_id.clone()),
            Segment::keep(self.step_id.clone()),
            Segment::elastic(format!("({})", self.tool)),
        ];
        fit_segments(&segments, max_width)
    }

    fn truncate_suffix(&self, max_width: usize, suffix: &SuffixComponents) -> String {
        let mut segments = Vec::new();
        if !suffix.elapsed.is_empty() {
            segments.push(Segment::keep(suffix.elapsed.clone()));
        }
        if let Some(ref rate) = suffix.rate {
            segments.push(Segment::keep(rate.clone()));
        }
        if let Some(ref eta) = suffix.eta {
            segments.push(Segment::keep(eta.clone()));
        }
        fit_segments(&segments, max_width)
    }
}

/// A screen with only an overall bar still reserves every slot: the grid height
/// is fixed, so the overall bar sits on the last line and the rest stay blank.
#[test]
fn fixed_height_grid_with_overall() {
    let (terminal, term) = mk_with_capacity(6, 80, 5);
    let (screen, _overall) = terminal.screen().with_overall("overall", 10).build();
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "\n",
            "\n",
            "\n",
            "\n",
            "\n",
            "⠹     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/10 0s 0/d"
        ),
        "fixed_height_grid_with_overall"
    );
}

/// A new child takes the bottom child slot; the previous child shifts up one
/// line to make room, and the overall bar does not move.
#[test]
fn add_bar_reuses_bottom_child_slot() {
    let (terminal, term) = mk_with_capacity(5, 80, 4);
    let (screen, _overall) = terminal.screen().with_overall("overall", 3).build();

    let _c1 = screen.add_bar(5, "tool1");
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "\n",
            "\n",
            "\n",
            "⠸       tool1 ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/5 0s 0/d\n",
            "⠹     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/3 0s 0/d"
        ),
        "add_bar_reuses_bottom_child_slot/first"
    );

    let _c2 = screen.add_bar(3, "tool2");
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "\n",
            "\n",
            "⠹       tool1 ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/5 0s 0/d\n",
            "⠴       tool2 ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/3 0s 0/d\n",
            "⠸     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/3 0s 0/d"
        ),
        "add_bar_reuses_bottom_child_slot/second"
    );
}

/// Without an overall bar the child region is the whole grid and children still
/// fill upward from the bottom slot.
#[test]
fn no_overall_reuses_bottom_child_slot() {
    let (terminal, term) = mk_with_capacity(5, 80, 4);
    let screen = terminal.screen().build();

    let _c1 = screen.add_bar(5, "task1");
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "\n",
            "\n",
            "\n",
            "⠸     task1 ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/5 0s 0/d"
        ),
        "no_overall_reuses_bottom_child_slot/first"
    );

    let _c2 = screen.add_bar(3, "task2");
    assert_eq!(
        &term.contents(),
        concat!(
            "\n",
            "\n",
            "⠙     task1 ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/5 0s 0/d\n",
            "⠼     task2 ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/3 0s 0/d"
        ),
        "no_overall_reuses_bottom_child_slot/second"
    );
}

/// Adding far more children than there are slots never grows the grid: the four
/// lines keep the four children that took a slot first, oldest at the top, while
/// every later child stays tracked but unbound.
#[test]
fn bar_count_never_grows() {
    let (terminal, term) = mk_with_capacity(5, 80, 4);
    let screen = terminal.screen().build();
    for i in 0..30 {
        let _c = screen.add_bar(1, &format!("tool{i}"));
        screen.tick();
    }
    assert_eq!(
        &term.contents(),
        concat!(
            "⠙     tool0 ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/1 0s 0/d\n",
            "⠹     tool1 ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/1 0s 0/d\n",
            "⠸     tool2 ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/1 0s 0/d\n",
            "⠇     tool3 ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/1 0s 0/d"
        ),
        "bar_count_never_grows"
    );
}

/// With an overall bar the child region is the grid minus the overall's own
/// line; five children fill the five child slots, oldest at the top, and the
/// overall bar takes the sixth line.
#[test]
fn children_fill_slots_chronologically() {
    let (terminal, term) = mk_with_capacity(6, 80, 5);
    let (screen, _overall) = terminal.screen().with_overall("overall", 10).build();

    for i in 0..5 {
        let _c = screen.add_bar(2, &format!("task{i}"));
        screen.tick();
    }
    assert_eq!(
        &term.contents(),
        concat!(
            "⠹       task0 ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/2 0s 0/d\n",
            "⠸       task1 ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/2 0s 0/d\n",
            "⠼       task2 ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/2 0s 0/d\n",
            "⠴       task3 ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/2 0s 0/d\n",
            "⠹       task4 ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/2 0s 0/d\n",
            "⠦     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/10 0s 0/d"
        ),
        "children_fill_slots_chronologically"
    );
}

/// Allocation order is age order: the first child takes the topmost child line
/// and each later child takes the line below it.
#[test]
fn children_are_ordered_by_age() {
    let (terminal, term) = mk_with_capacity(6, 80, 5);
    let (screen, _overall) = terminal.screen().with_overall("overall", 10).build();

    let _c1 = screen.add_bar(5, "first");
    let _c2 = screen.add_bar(5, "second");
    let _c3 = screen.add_bar(5, "third");
    screen.tick();
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "\n",
            "\n",
            "⠸       first ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/5 0s 0/d\n",
            "⠸      second ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/5 0s 0/d\n",
            "⠦       third ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/5 0s 0/d\n",
            "⠸     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/10 0s 0/d"
        ),
        "children_are_ordered_by_age"
    );
}

/// Binding a third child shifts the earlier two up one line without corrupting
/// any rendered value: positions, rates, and ETAs all follow their own bar.
#[test]
fn slot_shift_does_not_corrupt_display() {
    let ts = Arc::new(TestTimeSource::new());
    let (terminal, term) = mk_with_capacity_and_ts(6, 80, 5, &ts);
    let (screen, _overall) = terminal.screen().with_overall("overall", 10).build();

    let c1 = screen.add_bar(10, "alpha");
    let c2 = screen.add_bar(10, "beta");
    c1.advance(3);
    c2.advance(7);
    ts.advance(Duration::from_secs(1));
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "\n",
            "\n",
            "\n",
            "⠸       alpha ██████████████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  3/10 1s 18/m\n",
            "⠴        beta ██████████████████████████████████░░░░░░░░░░░░░░░  7/10 1s 42/m 4s\n",
            "⠹     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/10 1s 0/d"
        ),
        "slot_shift_does_not_corrupt_display/before"
    );

    let c3 = screen.add_bar(10, "gamma");
    c3.advance(5);
    ts.advance(Duration::from_secs(1));
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "\n",
            "\n",
            "⠹       alpha ██████████████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  3/10 2s 18/m 23s\n",
            "⠴        beta █████████████████████████████████░░░░░░░░░░░░░░░  7/10 2s 42/m 4s\n",
            "⠇       gamma ████████████████████████░░░░░░░░░░░░░░░░░░░░░░░░  5/10 1s 30/m 10s\n",
            "⠸     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/10 2s 0/d"
        ),
        "slot_shift_does_not_corrupt_display/after"
    );
}

/// The overall bar keeps its own slot when more children arrive than can be
/// bound: children take the remaining slots in the order they were added, so the
/// display keeps the oldest children and the newest arrivals are the ones left
/// undrawn.
#[test]
fn overall_never_shifts_when_children_overflow() {
    let ts = Arc::new(TestTimeSource::new());
    let (terminal, term) = mk_with_capacity_and_ts(5, 80, 4, &ts);
    let (screen, overall) = terminal.screen().with_overall("overall", 10).build();

    let _c1 = screen.add_bar(1, "a");
    let _c2 = screen.add_bar(1, "b");
    let _c3 = screen.add_bar(1, "c");
    overall.advance(3);
    ts.advance(Duration::from_secs(1));
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "\n",
            "⠹           a ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/1 1s 0/d\n",
            "⠹           b ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/1 1s 0/d\n",
            "⠴           c ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/1 1s 0/d\n",
            "⠸     overall ██████████████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  3/10 1s 18/m"
        ),
        "overall_never_shifts_when_children_overflow/full"
    );

    let _ = screen.add_bar(1, "d");
    let _ = screen.add_bar(1, "e");
    overall.advance(2);
    ts.advance(Duration::from_secs(1));
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "⠹           a ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/1 2s 0/d\n",
            "⠸           b ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/1 2s 0/d\n",
            "⠸           c ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/1 2s 0/d\n",
            "⠧           d ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/1 1s 0/d\n",
            "⠴     overall ████████████████████████░░░░░░░░░░░░░░░░░░░░░░░░  5/10 2s 28/m 10s"
        ),
        "overall_never_shifts_when_children_overflow/overflow"
    );
}

/// Reserved-but-unbound slots render as empty lines rather than ghost bars.
#[test]
fn blank_slots_render_nothing() {
    let (terminal, term) = mk_with_capacity(6, 80, 5);
    let screen = terminal.screen().build();

    let _c = screen.add_bar(10, "child");
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "\n",
            "\n",
            "\n",
            "\n",
            "⠸     child ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/10 0s 0/d"
        ),
        "blank_slots_render_nothing"
    );
}

/// The child slot is the line directly above the overall bar, never below it.
#[test]
fn child_slot_sits_above_the_pinned_overall() {
    let (terminal, term) = mk_with_capacity(6, 80, 5);
    let (screen, _overall) = terminal.screen().with_overall("overall", 10).build();

    let _c = screen.add_bar(7, "worker");
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "\n",
            "\n",
            "\n",
            "\n",
            "⠸      worker ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/7 0s 0/d\n",
            "⠹     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/10 0s 0/d"
        ),
        "child_slot_sits_above_the_pinned_overall"
    );
}

/// The overall bar keeps the bottom slot even after five children are added.
#[test]
fn overall_stays_in_the_bottom_slot() {
    let (terminal, term) = mk_with_capacity(6, 80, 5);
    let (screen, _overall) = terminal.screen().with_overall("overall", 10).build();

    for i in 0..5 {
        let _c = screen.add_bar(2, &format!("task{i}"));
        screen.tick();
    }
    assert_eq!(
        &term.contents(),
        concat!(
            "⠹       task0 ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/2 0s 0/d\n",
            "⠸       task1 ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/2 0s 0/d\n",
            "⠼       task2 ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/2 0s 0/d\n",
            "⠴       task3 ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/2 0s 0/d\n",
            "⠹       task4 ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/2 0s 0/d\n",
            "⠦     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/10 0s 0/d"
        ),
        "overall_stays_in_the_bottom_slot"
    );
}

/// A bar with no free slot is still tracked even though it is not drawn: the
/// fifth child reports its position through its handle while the grid shows
/// only the four bound children.
#[test]
fn overflow_bars_are_tracked_but_not_drawn() {
    let (terminal, term) = mk_with_capacity(5, 80, 4);
    let screen = terminal.screen().build();

    let c1 = screen.add_bar(5, "tool-a");
    let c2 = screen.add_bar(5, "tool-b");
    let c3 = screen.add_bar(5, "tool-c");
    let c4 = screen.add_bar(5, "tool-d");
    let c5 = screen.add_bar(5, "tool-e");

    c1.advance(1);
    c2.advance(2);
    c3.advance(3);
    c4.advance(4);
    c5.advance(5);
    screen.tick();

    let contents = term.contents();
    assert!(!contents.contains("tool-e"), "unbound bar must not be drawn: {contents:?}");
    assert_eq!(c5.snapshot().position, 5, "unbound bar must still track its position");
    assert_eq!(
        &contents,
        concat!(
            "⠸     tool-a ██████████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  1/5 0s 0/d\n",
            "⠸     tool-b ████████████████████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  2/5 0s 0/d\n",
            "⠸     tool-c ███████████████████████████████░░░░░░░░░░░░░░░░░░░░░  3/5 0s 0/d\n",
            "⠧     tool-d █████████████████████████████████████████░░░░░░░░░░░  4/5 0s 0/d"
        ),
        "overflow_bars_are_tracked_but_not_drawn"
    );
}

/// A resolve label that fits the prefix budget keeps its `[phase]` tag: the label
/// is parsed into components at construction, so the tool name and the phase tag
/// are separate fields before anything is drawn.
///
/// The label is 33 characters and the 80-column terminal pays for the prefix,
/// the suffix and the fill, so nothing is truncated here. The drop order that
/// would apply if it were, where the version shrinks before the phase is
/// removed, is pinned by the inline `truncate_parsed_resolve_label_preserves_phase`
/// unit test.
#[test]
fn resolve_label_within_budget_keeps_phase_tag() {
    let dims = Arc::new(TestDimensionSource::new((4, 80)));
    let ts = Arc::new(TestTimeSource::new());
    let (terminal, term) = mk_with_dims(5, 120, 4, &dims, Some(&ts), false);
    let (screen, _overall) = terminal.screen().with_overall("overall", 1).build();

    let child = screen.add_bar(100, "ffmpeg autobuild-2026-07-31 [res]");
    child.set_position(0);
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "\n",
            "\n",
            "\n",
            "⠸     ffmpeg autobuild-2026-07-31 [res] ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/100 0s 0/d\n",
            "⠹                               overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/1 0s 0/d"
        ),
        "resolve_label_within_budget_keeps_phase_tag"
    );
}

/// A multi-word label with no bracket is kept whole as the tool name rather
/// than being reduced to its first token.
#[test]
fn resolve_label_multiword_no_bracket_keeps_whole_label() {
    let dims = Arc::new(TestDimensionSource::new((4, 80)));
    let ts = Arc::new(TestTimeSource::new());
    let (terminal, term) = mk_with_dims(5, 120, 4, &dims, Some(&ts), false);
    let (screen, _overall) = terminal.screen().with_overall("overall", 1).build();

    let child = screen.add_bar(100, "syncing tools");
    child.set_position(0);
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "\n",
            "\n",
            "\n",
            "⠸     syncing tools ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/100 0s 0/d\n",
            "⠹           overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/1 0s 0/d"
        ),
        "resolve_label_multiword_no_bracket_keeps_whole_label"
    );
}

/// A worker slot seeded with the coordinator's `idle [wf]` still draws its tool
/// name at 80 columns.
///
/// The seed is nine columns and a client label can only be shortened by
/// [`fit_segments`], never lengthened, so a prefix slot sized from the seed
/// leaves the label nothing to grow into. The 80-column line has room for
/// `default s3 (yt-dlp)` after the marker and the suffix, and a run that drops
/// the tool name here loses the only thing that says which tool the slot is
/// running.
#[test]
fn prefix_slot_is_not_pinned_to_the_seed_label() {
    let (terminal, term) = mk_with_capacity(4, 80, 3);
    let screen = terminal.screen().build();

    let worker = screen.add_bar(1, "idle [wf]");
    worker.set_truncation(Arc::new(WorkerRowLabel {
        workflow_id: "default".to_string(),
        step_id: "s3".to_string(),
        tool: "yt-dlp".to_string(),
        activity: "active".to_string(),
    }));
    screen.tick();

    let contents = term.contents();
    assert!(contents.contains("(yt-dlp)"), "worker row lost its tool name: {contents:?}");
}

/// A frame whose fill would be at its floor draws no bar, and the columns the
/// bar held go to the label and the count.
///
/// `MIN_BAR_FILL` is four cells, and below the point where a screen's labels
/// stop overflowing the line, every width from 8 up draws exactly four at every
/// width: the budget pays the label and the suffix first, so the fill cannot
/// grow. Four cells say nothing the count beside them does not, and they are
/// paid for out of a label clipped down to its tail. Measured across the three
/// example screens, that point is width 59 for tool sync and materialization and
/// width 66 for the workflow screen, whose labels are seven columns wider.
///
/// The wide half is the rest of the contract: at 80 the fill has room to grow,
/// so the row keeps its bar.
#[test]
fn fill_at_its_floor_gives_its_columns_to_the_label_and_count() {
    let (terminal, term) = mk_with_capacity(2, 20, 1);
    let screen = terminal.screen().build();
    let bar = screen.add_bar(3, "default s3 (ffmpeg)");
    bar.advance(1);
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!("⠼     default", "   1/3"),
        "narrow row must keep the label and the count and drop the fill"
    );

    let (terminal, term) = mk_with_capacity(2, 80, 1);
    let screen = terminal.screen().build();
    let bar = screen.add_bar(3, "default s3 (ffmpeg)");
    bar.advance(1);
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "⠼     default s3 (ffmpeg) ",
            "█████████████░░░░░░░░░░░░░░░░░░░░░░░░░░",
            "  1/3 0s 0/d"
        ),
        "a wide row keeps its fill and its timing"
    );
}
