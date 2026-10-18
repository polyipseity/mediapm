//! Client-defined bar-label truncation contract tests.
//!
//! Validates that the conductor-owned label structs implement
//! [`BarLabelTruncation`] with the allocation policy: a worker bar renders
//! a tally only from its own `completed`/`total` fields (and drops the
//! renderer-supplied pair), while a step bar carries that tally in the
//! suffix only, so the prefix holds names and the suffix
//! holds counts. The status marker is reserved first and always drawn whole;
//! the tool is sized next; workflow and step split what remains evenly; and
//! the worker's activity tag is drawn only from columns left after those,
//! so it yields first. The inline `allocation_policy` module in
//! `progress_labels.rs` pins the split mechanics; the tests here pin the
//! field-order contracts and the widths at which each field changes shape.

use mediapm_conductor::orchestration::progress_labels::{StepBarLabel, WorkerBarLabel};
use mediapm_utils::progress::{BarLabelTruncation, SuffixComponents};

#[test]
fn worker_label_ignores_a_populated_tally_in_the_suffix() {
    let label = WorkerBarLabel {
        status_marker: String::new(),
        workflow_id: "wf".into(),
        step_id: "s1".into(),
        tool: "echo@v1".into(),
        activity: "active".into(),
        completed: String::new(),
        total: String::new(),
    };
    // The tally a worker renders comes only from the label's own fields,
    // which are empty here. The renderer separately offers a count/total
    // pair in the suffix components, so the label has to drop that pair
    // deliberately rather than echo it: this fails if `suffix_segments`
    // ever starts emitting the renderer's count/total.
    let suffix = SuffixComponents {
        count: "3".into(),
        total: "10".into(),
        elapsed: "2m 05s".into(),
        ..Default::default()
    };
    let out = label.truncate_suffix(80, &suffix);
    assert!(!out.contains("3/10"), "worker suffix must not carry a tally: {out:?}");
    assert!(out.contains("2m 05s"), "worker suffix lost elapsed: {out:?}");

    // The prefix names the workflow, the step, and the tool, then closes with
    // the activity marker.
    let wide = label.truncate_prefix(80);
    assert_eq!(wide, "wf s1 (echo@v1) [active]");
}

#[test]
fn worker_label_tight_prefix_keeps_the_names_and_sheds_the_tag() {
    let label = WorkerBarLabel {
        status_marker: String::new(),
        workflow_id: "wf".into(),
        step_id: "s1".into(),
        tool: "mediapm-conductor-builtin-archive".into(),
        activity: "active".into(),
        completed: String::new(),
        total: String::new(),
    };
    // Narrow enough that the names need the room: each is held at its
    // 4-column floor and the tool — which outranks both — gives columns
    // back, clipping from the front and dropping its brackets with the
    // clip. The activity tag yields first, so at this width it is gone.
    let tight = label.truncate_prefix(14);
    assert_eq!(tight, "wf s1 -archive");
    assert!(!tight.contains("[active]"), "activity yields before the names: {tight:?}");
}

#[test]
fn worker_label_idle_renders_its_activity_marker() {
    let label = WorkerBarLabel {
        status_marker: String::new(),
        workflow_id: String::new(),
        step_id: String::new(),
        tool: "archive".into(),
        activity: "idle".into(),
        completed: String::new(),
        total: String::new(),
    };
    // The tool name is deliberately not "idle", so it cannot stand in for the
    // marker; the assertion is on the bracketed form the label renders.
    let prefix = label.truncate_prefix(40);
    assert_eq!(prefix, "(archive) [idle]");
}

#[test]
fn step_label_keeps_the_version_while_the_tool_name_is_whole() {
    // The version trails the tool name, so it is the first segment to yield
    // and the tool name the last to be touched. At 18 the version has given
    // back three of its five columns and reads `1.2`, while the tool name
    // still renders in full. That is the order tool-sync already uses, where
    // its own `[res]` tag goes before its tool name is clipped.
    //
    // The name's parentheses wrap the name and the version together, so the
    // version shortening takes the pair down with it: the row shows the whole
    // name rather than half a pair around it.
    let label = StepBarLabel {
        status_marker: String::new(),
        workflow_id: "wf".into(),
        step_id: "s1".into(),
        tool: "echo@v1".into(),
        version: "1.2.3".into(),
        completed: "1".into(),
        total: "4".into(),
    };
    let wide = label.truncate_prefix(80);
    assert_eq!(wide, "wf s1 (echo@v1 1.2.3)");

    // The width-80 case fits whole, so on its own it would still pass if
    // truncation were removed.
    let tight = label.truncate_prefix(18);
    assert_eq!(tight, "wf s1 echo@v1 1.2");
    assert!(tight.contains("echo@v1"), "tool name should be untouched at 18: {tight:?}");
    assert!(!tight.contains('('), "half a bracket group outlived its version: {tight:?}");
}

#[test]
fn step_label_brackets_the_version_inside_the_tool_name_pair() {
    // One pair of round brackets around the tool name and the version, so a
    // reader sees `(ffmpeg v7.1)` as one thing rather than a name and a
    // figure that happen to sit side by side.
    let label = StepBarLabel {
        status_marker: String::new(),
        workflow_id: "default".into(),
        step_id: "s3".into(),
        tool: "ffmpeg".into(),
        version: "v7.1".into(),
        completed: String::new(),
        total: String::new(),
    };
    let wide = label.truncate_prefix(40);
    assert_eq!(wide, "default s3 (ffmpeg v7.1)");
    assert_eq!(wide.matches('(').count(), 1, "the group needs one opening bracket: {wide:?}");
    assert_eq!(wide.matches(')').count(), 1, "the group needs one closing bracket: {wide:?}");

    // At 22 the names split the remainder and the tool+version pair comes
    // through whole: the workflow is cut from the front (its tail survives)
    // and the bracketed pair needs no columns given back.
    let narrow = label.truncate_prefix(22);
    assert_eq!(narrow, "fault s3 (ffmpeg v7.1)");

    // Tighter still, the workflow is held at its 4-column floor: the tool
    // group gives columns back, the version shortens from its head, and the
    // shortened version takes the pair's brackets down with it — half a
    // group reads as a bracket nothing opened or nothing closed, which is
    // the fault the fitter-level sweep in `mediapm-utils` watches at every
    // width.
    let tighter = label.truncate_prefix(18);
    assert_eq!(tighter, "ault s3 ffmpeg v7");
    assert!(!tighter.contains('('), "opening bracket outlived the version: {tighter:?}");
    assert!(!tighter.contains(')'), "closing bracket outlived the version: {tighter:?}");
}

#[test]
fn step_label_brackets_a_tool_name_that_has_no_version() {
    // With no version to wrap, the tool name is the whole group, which is the
    // one-member case every other bracketed segment in the tree draws.
    let label = StepBarLabel {
        status_marker: String::new(),
        workflow_id: "default".into(),
        step_id: "s3".into(),
        tool: "media-tagger".into(),
        version: String::new(),
        completed: String::new(),
        total: String::new(),
    };
    assert_eq!(label.truncate_prefix(40), "default s3 (media-tagger)");
}

/// Visible width of the leading `[F] wf s1` head a step bar shares with a
/// worker bar: the narrowest width at which both names survive whole under
/// the even split (one column wider than the tail-drop ladder needed,
/// because the names now share width with the tool instead of holding their
/// columns ahead of it).
const STEP_HEAD_WIDTH: usize = 10;
/// Visible width of the leading `[F] wf s1` head on a worker bar. See
/// [`STEP_HEAD_WIDTH`]; the two labels share the allocation.
const WORKER_HEAD_WIDTH: usize = 10;

#[test]
fn step_label_head_survives_across_narrow_widths() {
    let label = StepBarLabel {
        status_marker: "F".into(),
        workflow_id: "wf".into(),
        step_id: "s1".into(),
        tool: "echo@v1".into(),
        version: "9.9.9".into(),
        completed: "1".into(),
        total: "4".into(),
    };
    for width in STEP_HEAD_WIDTH..=20 {
        let out = label.truncate_prefix(width);
        assert!(out.contains("[F]"), "status lost at width {width}: {out:?}");
        assert!(out.contains("wf"), "workflow lost at width {width}: {out:?}");
        assert!(out.contains("s1"), "step lost at width {width}: {out:?}");
        assert!(out.chars().count() <= width, "overflowed at width {width}: {out:?}");
    }
}

#[test]
fn step_label_drops_the_version_before_touching_the_tool_name() {
    // The version runs out before the tool name does. It is the tail segment,
    // so once it has nothing left to give it is dropped rather than rendered
    // as a fragment, and the row falls back to the identifiers and the tool
    // name. Only past that width is the tool name itself shortened. The pair
    // goes with the version, since a bracket group that lost a member draws
    // no brackets at all.
    let label = StepBarLabel {
        status_marker: String::new(),
        workflow_id: "wf".into(),
        step_id: "s1".into(),
        tool: "echo@v1".into(),
        version: "1.2.3".into(),
        completed: "1".into(),
        total: "4".into(),
    };
    assert_eq!(label.truncate_prefix(15), "wf s1 echo@v1");
    assert_eq!(label.truncate_prefix(13), "wf s1 echo@v1");
    // One column narrower than the bare name, the name clips from its front.
    assert_eq!(label.truncate_prefix(12), "wf s1 cho@v1");
}

#[test]
fn worker_label_head_survives_across_narrow_widths() {
    let label = WorkerBarLabel {
        status_marker: "F".into(),
        workflow_id: "wf".into(),
        step_id: "s1".into(),
        tool: "echo@v1".into(),
        activity: "active".into(),
        completed: String::new(),
        total: String::new(),
    };
    for width in WORKER_HEAD_WIDTH..=20 {
        let out = label.truncate_prefix(width);
        assert!(out.contains("[F]"), "status lost at width {width}: {out:?}");
        assert!(out.contains("wf"), "workflow lost at width {width}: {out:?}");
        assert!(out.contains("s1"), "step lost at width {width}: {out:?}");
        assert!(out.chars().count() <= width, "overflowed at width {width}: {out:?}");
    }
}

#[test]
fn worker_label_sheds_the_activity_tag_after_the_names_and_tool() {
    // The activity tag is drawn only from columns left after the marker,
    // tool and names, so at the widths where identifiers and tool fill the
    // row the tag is gone; it returns as soon as one column more frees up.
    let label = WorkerBarLabel {
        status_marker: "F".into(),
        workflow_id: "wf".into(),
        step_id: "s1".into(),
        tool: "echo@v1".into(),
        activity: "active".into(),
        completed: String::new(),
        total: String::new(),
    };
    // 18: names at floor, tool clipped bare to what the floor left, no
    // room for the 8-column tag.
    assert_eq!(label.truncate_prefix(18), "[F] wf s1 echo@v1");
    // 27: tool back in brackets, still no room for the tag.
    assert_eq!(label.truncate_prefix(27), "[F] wf s1 (echo@v1)");
    // 28: exactly enough left after the row for the tag.
    assert_eq!(label.truncate_prefix(28), "[F] wf s1 (echo@v1) [active]");
}

#[test]
fn step_label_renders_the_tally_once_and_only_in_the_suffix() {
    // Names live in the prefix and counts live in the suffix, so the tally
    // leaves the prefix entirely. The renderer offers its own count and total
    // in the suffix components, and the label renders its own tally from its
    // own fields, so a label that echoed the renderer's count as well would
    // put the same figure on the row twice.
    let label = StepBarLabel {
        status_marker: String::new(),
        workflow_id: "wf".into(),
        step_id: "s1".into(),
        tool: "echo@v1".into(),
        version: "7.1".into(),
        completed: "1".into(),
        total: "3".into(),
    };
    let suffix = SuffixComponents {
        count: "1".into(),
        total: "3".into(),
        elapsed: "23s".into(),
        ..Default::default()
    };
    let prefix = label.truncate_prefix(80);
    assert!(!prefix.contains("1/3"), "tally must not render in the prefix: {prefix:?}");
    let out = label.truncate_suffix(80, &suffix);
    assert_eq!(out.matches("1/3").count(), 1, "tally rendered more than once: {out:?}");
    assert!(out.ends_with("1/3 23s"), "tally should lead the suffix: {out:?}");
}

#[test]
fn step_label_clips_a_long_tool_name_after_the_version_is_gone() {
    // The version is already gone at both widths below, so what changes
    // between them is the tool name and nothing else. A name long enough to
    // need the room is clipped from its front rather than dropped whole,
    // which keeps the tail that says which tool it was.
    //
    // The name's parentheses wrap the name and the version together, so the
    // dropped version takes them too. What is left is the name on its own,
    // which is the same shape a name has when it clips.
    let label = StepBarLabel {
        status_marker: String::new(),
        workflow_id: "wf".into(),
        step_id: "s1".into(),
        tool: "a-very-long-tool-name@v1".into(),
        version: "9.9.9".into(),
        completed: "1".into(),
        total: "4".into(),
    };
    assert_eq!(label.truncate_prefix(32), "wf s1 a-very-long-tool-name@v1");

    // One column narrower than the bare name, so the name clips from its
    // front. A name is never shown cut with a bracket left over: the
    // clipper is handed the name, not `(name)`.
    let tight = label.truncate_prefix(29);
    assert_eq!(tight, "wf s1 -very-long-tool-name@v1");
    assert!(!tight.contains(')'), "a closing parenthesis outlived its name: {tight:?}");
    assert!(!tight.contains("9.9.9"), "version should be gone at 29: {tight:?}");
}

#[test]
fn worker_label_clips_a_very_long_tool_name_from_the_front() {
    // A tool name longer than the budget is clipped to its tail, so the row
    // keeps the informative end of the name. The tool fills the whole row at
    // its clip, so the activity tag — drawn only from columns left after the
    // tool — yields before the name loses a column.
    let label = WorkerBarLabel {
        status_marker: String::new(),
        workflow_id: String::new(),
        step_id: String::new(),
        tool: "a-very-long-tool-name-that-exceeds-the-maximum-width-limit".into(),
        activity: "active".into(),
        completed: String::new(),
        total: String::new(),
    };
    let tight = label.truncate_prefix(20);
    assert_eq!(tight, "-maximum-width-limit");
    assert_eq!(tight.chars().count(), 20, "the clip must fill the budget: {tight:?}");
    assert!(!tight.contains("[active]"), "the tag yields before the name: {tight:?}");
}

#[test]
fn worker_suffix_keeps_auto_derived_timing_fields() {
    // A worker suffix may carry a tally from the label's own fields, but
    // the renderer separately supplies auto-derived elapsed, rate, and ETA
    // in the suffix argument it passes to `truncate_suffix`. Those fields
    // must survive:
    // dropping them would silently remove timing information from every
    // worker bar, which no other test in this file would catch.
    let label = WorkerBarLabel {
        status_marker: String::new(),
        workflow_id: String::new(),
        step_id: String::new(),
        tool: String::new(),
        activity: "active".into(),
        completed: String::new(),
        total: String::new(),
    };
    let suffix = SuffixComponents {
        elapsed: "1m 30s".into(),
        rate: Some("2.1 MiB/s".into()),
        eta: Some("00:45".into()),
        custom: String::new(),
        ..Default::default()
    };
    let out = label.truncate_suffix(80, &suffix);
    assert!(out.contains("1m 30s"), "elapsed lost: {out:?}");
    assert!(out.contains("2.1 MiB/s"), "rate lost: {out:?}");
    assert!(out.contains("00:45"), "eta lost: {out:?}");
}

#[test]
fn step_label_clips_a_very_long_tool_name_from_the_front() {
    // With every other field empty the tool name is the only segment, so the
    // clip has nothing to fall back on and the result is the bare tail of the
    // name at exactly the budget.
    let label = StepBarLabel {
        status_marker: String::new(),
        workflow_id: String::new(),
        step_id: String::new(),
        tool: "extremely-long-tool-name-that-exceeds-the-maximum-width-limit".into(),
        version: String::new(),
        completed: String::new(),
        total: String::new(),
    };
    let tight = label.truncate_prefix(20);
    assert_eq!(tight, "-maximum-width-limit");
    assert_eq!(tight.chars().count(), 20);
}

#[test]
fn step_suffix_keeps_the_leading_tally_and_timing_fields() {
    // The tally is the step bar's only count and renders in the suffix, and the
    // renderer separately supplies auto-derived elapsed, rate and ETA in the
    // suffix argument. Both must reach the rendered suffix: the timing fields
    // reach it only through that argument, so a `suffix_segments` that stopped
    // reading them would silently strip timing from every step bar, and no
    // other test in this file would catch it.
    let label = StepBarLabel {
        status_marker: String::new(),
        workflow_id: String::new(),
        step_id: String::new(),
        tool: String::new(),
        version: String::new(),
        completed: "3".into(),
        total: "10".into(),
    };
    let suffix = SuffixComponents {
        elapsed: "2m 05s".into(),
        rate: Some("1.4 MiB/s".into()),
        eta: Some("01:12".into()),
        ..Default::default()
    };

    // Wide enough for every field: all four must be present.
    let wide = label.truncate_suffix(80, &suffix);
    assert!(wide.contains("3/10"), "tally lost: {wide:?}");
    assert!(wide.contains("2m 05s"), "elapsed lost: {wide:?}");
    assert!(wide.contains("1.4 MiB/s"), "rate lost: {wide:?}");
    assert!(wide.contains("01:12"), "eta lost: {wide:?}");

    // Too narrow for all four. Segments yield from the tail, so the tally
    // leads the order and is the last one standing; the timing fields are
    // surrendered whole rather than clipped, because none of them is elastic.
    let tight = label.truncate_suffix(10, &suffix);
    assert_eq!(tight, "3/10", "tight case must keep the leading tally whole: {tight:?}");
}

/// The step row's identifying fields under the allocation policy: the
/// workflow and step split width evenly, and the status marker is reserved
/// so it stands even when both names are gone.
///
/// The widths below each straddle one of those boundaries. At 16 everything
/// fits; below it the names share what the tool leaves and each is cut from
/// the front, so both survive as tails before either disappears; and once
/// the names are gone the marker still draws — a `[F]` a row could not
/// carry would read as a succeeded one, which is the one thing a row must
/// not do.
#[test]
fn step_label_splits_the_names_evenly_and_keeps_the_marker() {
    let label = StepBarLabel {
        status_marker: "F".into(),
        workflow_id: "wf".into(),
        step_id: "s1".into(),
        tool: "echo".into(),
        version: String::new(),
        completed: String::new(),
        total: String::new(),
    };
    // At 16 the whole row fits, brackets included.
    assert_eq!(label.truncate_prefix(16), "[F] wf s1 (echo)");
    // One or two narrower the tool drops its brackets, then clips, while
    // both names stay whole at their floor.
    assert_eq!(label.truncate_prefix(15), "[F] wf s1 echo");
    assert_eq!(label.truncate_prefix(14), "[F] wf s1 echo");
    // Ten is the narrowest width at which both names survive whole.
    assert_eq!(label.truncate_prefix(10), "[F] wf s1");
    // Below that each name shrinks toward its natural width; a name the
    // tool could not lift to its floor is dropped rather than drawn as a
    // one-column fragment that says nothing.
    assert_eq!(label.truncate_prefix(9), "[F] wf");
    // At 8 neither name can reach even its own natural floor: both drop
    // and the marker carries the row.
    assert_eq!(label.truncate_prefix(8), "[F]");
    // The names gone, the marker stands alone until it cannot fit at all.
    assert_eq!(label.truncate_prefix(6), "[F]");
    assert_eq!(label.truncate_prefix(5), "[F]");
    assert_eq!(label.truncate_prefix(3), "[F]");
    assert_eq!(label.truncate_prefix(2), "");
}

/// A worker row's suffix gives up its trailing timing columns first, so a
/// narrow row still names how long it has been running.
///
/// The widths below straddle each boundary, so a suffix that listed the fields
/// in another order would keep the wrong one at the width where only one
/// survives. None of the four is elastic, so width can only drop them whole.
#[test]
fn worker_suffix_yields_eta_then_rate_before_the_elapsed() {
    let label = WorkerBarLabel {
        status_marker: String::new(),
        workflow_id: String::new(),
        step_id: String::new(),
        tool: String::new(),
        activity: "active".into(),
        completed: String::new(),
        total: String::new(),
    };
    let suffix = SuffixComponents {
        elapsed: "1m 30s".into(),
        rate: Some("2.1 MiB/s".into()),
        eta: Some("00:45".into()),
        custom: "queued".into(),
        ..Default::default()
    };
    // `custom` is the one elastic field, so it is clipped rather than dropped
    // at the widths between the timing boundaries. Those are the widths at which
    // one fewer timing column survives, and which one it is says the order.
    assert_eq!(label.truncate_suffix(30, &suffix), "1m 30s 2.1 MiB/s 00:45 queued");
    assert_eq!(label.truncate_suffix(29, &suffix), "1m 30s 2.1 MiB/s 00:45 queued");
    assert_eq!(label.truncate_suffix(22, &suffix), "1m 30s 2.1 MiB/s 00:45");
    assert_eq!(label.truncate_suffix(21, &suffix), "1m 30s 2.1 MiB/s");
    assert_eq!(label.truncate_suffix(16, &suffix), "1m 30s 2.1 MiB/s");
    assert_eq!(label.truncate_suffix(15, &suffix), "1m 30s");
    assert_eq!(label.truncate_suffix(6, &suffix), "1m 30s");
    assert_eq!(label.truncate_suffix(5, &suffix), "");
}
