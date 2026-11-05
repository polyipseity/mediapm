//! The pinned overall bar of the tool phase and the screen the caller hands in.

use mediapm_utils::progress::recording::{
    BarId, ProgressOp, RecordingProgressTracker, RecordingTrackedHandle,
};
use std::collections::BTreeMap;

use super::*;

#[tokio::test]
async fn reconcile_desired_tools_records_progress_ops() {
    let tmp = mediapm_utils::temp::artifact_dir().unwrap();
    let cache_root = mediapm_utils::temp::cache_dir().unwrap();
    let paths = MediaPmPaths::from_root(tmp.path());
    let tracker = RecordingProgressTracker::new();
    let state = MediaPmState::default();
    let workspace_cas = super::open_workspace_cas_store(&paths).await.expect("open workspace cas");
    let result = reconcile_desired_tools(
        workspace_cas,
        &paths,
        &BTreeMap::new(),
        &BTreeMap::new(),
        PayloadStaging::InPlace,
        RecheckPolicy::default(),
        &state,
        Some(cache_root.path()),
        &tracker,
        None,
    )
    .await
    .map(ToolSyncOutcome::into_report);

    assert!(result.is_ok(), "reconcile_desired_tools failed: {:?}", result.err());

    let ops = tracker.ops();

    // Both bars the sync creates are registered through the tracker: the
    // overall "syncing tools" bar, and the `[prn]` prune bar — which this
    // fixture must NOT create, because nothing in the empty generated doc
    // is a prune candidate (the documented zero-bar guard). Each bar is
    // asserted by label, not by count: a count would also pass if the
    // overall bar vanished and two prune bars appeared in its place.
    let add_bars: Vec<(usize, &ProgressOp)> =
        ops.iter().enumerate().filter(|(_, op)| matches!(op, ProgressOp::AddBar { .. })).collect();
    let label_of = |op: &ProgressOp| match op {
        ProgressOp::AddBar { label, .. } => Some(label.clone()),
        _ => None,
    };
    let overall_index = add_bars
        .iter()
        .position(|(_, op)| label_of(op).as_deref() == Some("syncing tools"))
        .expect("the overall bar must be registered through the tracker");
    let ProgressOp::AddBar { total: overall_total, .. } = add_bars[overall_index].1 else {
        unreachable!("filtered to AddBar ops")
    };
    assert_eq!(*overall_total, 0, "overall bar total should be 0 (indeterminate)");
    assert_eq!(add_bars.len(), 1, "nothing to prune means no `[prn]` bar: {add_bars:?}");
    assert!(
        add_bars.iter().all(|(_, op)| label_of(op).as_deref() == Some("syncing tools")),
        "the only registered bar is the overall bar: {add_bars:?}"
    );

    // Every bar created finishes successfully.
    let finish_successes: usize =
        ops.iter().filter(|op| matches!(op, ProgressOp::FinishSuccess)).count();
    assert_eq!(
        finish_successes,
        add_bars.len(),
        "every registered bar must finish successfully, got {finish_successes} finishes for {} bars",
        add_bars.len(),
    );
}

// Provisioning failures are now fatal (fail fast): an unresolvable tool aborts
// the whole sync with the phase-tagged cause instead of recording a warning to
// retry on the next sync. The bar for the failed run finishes red
// (`finish_error`, `[F]`), matching the fatal outcome, and nothing is applied
// or written. This reverses the old Phase 2 warn-continue contract.
#[tokio::test]
async fn reconcile_desired_tools_resolve_failure_fails_fast() {
    let tmp = mediapm_utils::temp::artifact_dir().unwrap();
    let cache_root = mediapm_utils::temp::cache_dir().unwrap();
    let paths = MediaPmPaths::from_root(tmp.path());
    let tracker = RecordingProgressTracker::new();
    let state = MediaPmState::default();
    let workspace_cas = super::open_workspace_cas_store(&paths).await.expect("open workspace cas");

    // An unknown tool name has no provider registered for resolution, so
    // `resolve_tool_fetch` returns Err and the whole sync aborts (fail fast).
    let mut desired = BTreeMap::new();
    desired.insert("nonexistent-tool".to_string(), serde_json::json!({ "version_spec": "latest" }));

    let result = reconcile_desired_tools(
        workspace_cas,
        &paths,
        &desired,
        &BTreeMap::new(),
        PayloadStaging::InPlace,
        RecheckPolicy::default(),
        &state,
        Some(cache_root.path()),
        &tracker,
        None,
    )
    .await
    .map(ToolSyncOutcome::into_report);

    // The sync now fails fast naming the failing tool and its phase.
    let err = result.expect_err("an unresolvable tool must abort the whole sync");
    let message = err.to_string();
    assert!(
        message.contains("tool sync failed: nonexistent-tool (resolve)"),
        "the error must name the failing tool and resolve phase: {message}"
    );

    let ops = tracker.ops();
    assert!(
        ops.iter().any(|op| matches!(op, ProgressOp::FinishError)),
        "a fatal provisioning failure must finish the bar red (finish_error); got {ops:?}"
    );
    assert!(
        !ops.iter().any(|op| matches!(op, ProgressOp::FinishWarning)),
        "a fatal provisioning failure is no longer a warning; got {ops:?}"
    );
}

/// The caller's pinned overall bar is driven by this phase, not left idle.
///
/// The tool phase keeps its pinned `"syncing tools"` overall bar instead of
/// taking a child bar on the caller's screen, so it must adopt the handle
/// the caller built `with_overall` and set its total to the entry count.
/// A phase that ignored the handle would commit a permanently idle overall
/// bar — a bar the renderer draws from state nobody ever updates.
#[tokio::test]
async fn reconcile_desired_tools_drives_the_callers_overall_bar() {
    let tmp = mediapm_utils::temp::artifact_dir().unwrap();
    let cache_root = mediapm_utils::temp::cache_dir().unwrap();
    let paths = MediaPmPaths::from_root(tmp.path());
    let state = MediaPmState::default();
    let workspace_cas = super::open_workspace_cas_store(&paths).await.expect("open workspace cas");
    // The caller's own screen and overall handle, as the service supplies
    // them: the overall bar starts at a placeholder total of 1 and must be
    // re-totalled by the phase.
    let (screen, overall) = RecordingProgressTracker::with_overall("syncing tools", 1);

    // One desired tool, unresolvable, so the entry count is known without
    // any network access (the resolve failure aborts the sync fast, but only
    // after the overall bar is totalled).
    let mut desired = BTreeMap::new();
    desired.insert("nonexistent-tool".to_string(), serde_json::json!({ "version_spec": "latest" }));

    let result = reconcile_desired_tools(
        workspace_cas,
        &paths,
        &desired,
        &BTreeMap::new(),
        PayloadStaging::InPlace,
        RecheckPolicy::default(),
        &state,
        Some(cache_root.path()),
        &screen,
        Some(Arc::new(overall)),
    )
    .await
    .map(ToolSyncOutcome::into_report);
    // The unknown tool aborts the sync (fail fast), but the caller's overall
    // bar is totalled to the entry count BEFORE provisioning runs, so the
    // SetTotal is still observable on the failure path.
    assert!(result.is_err(), "the unknown tool must fail the sync fast: {:?}", result.ok());

    let ops = screen.ops();
    assert!(
        ops.iter().any(|op| matches!(op, ProgressOp::SetTotal { total: 1 })),
        "the caller's `syncing tools` overall bar must be totalled to the tool count; \
         got {ops:?}"
    );
    // End-to-end proof of the pool's advancement wiring (spec AC3): the
    // single entry failed at resolve, yet its future still settled inside
    // the concurrent phase and was counted exactly once through
    // `OverallProgress` — a wiring that fired nowhere would leave the bar
    // frozen at 0, one that fired in both pool and apply would double-count.
    let advances: Vec<u64> = ops
        .iter()
        .filter_map(|op| match op {
            ProgressOp::Advance { delta } => Some(*delta),
            _ => None,
        })
        .collect();
    assert_eq!(
        advances,
        vec![1],
        "the single failing entry must be counted exactly once when its provision future \
         settles; got {ops:?}"
    );
}

/// Exactly one advance per entry, no matter how many call sites ask.
///
/// The defect this pins: the overall bar used to advance only in the
/// post-pool skip arms, so a fetched-only sync showed `0/7` with every child
/// finished, and a second call site would have double-counted a skipped
/// entry. The counter makes duplicate asks idempotent, so the pool can
/// advance on completion and the apply pass can re-assert without lying.
#[test]
fn overall_progress_advances_once_per_entry() {
    let screen = RecordingProgressTracker::new();
    let bar: Arc<dyn ProgressBarApi> = Arc::new(screen.add_bar(7, "syncing tools"));
    let progress = OverallProgress::new(&bar);

    progress.advance_once(0);
    progress.advance_once(0);
    progress.advance_once(1);
    progress.advance_once(1);
    progress.advance_once(1);

    let ops = screen.ops();
    let advances: Vec<u64> = ops
        .iter()
        .filter_map(|op| match op {
            ProgressOp::Advance { delta } => Some(*delta),
            _ => None,
        })
        .collect();
    assert_eq!(advances, vec![1, 1], "one advance per entry index; got {ops:?}");
}

/// A run that carried a notice ends its overall bar yellow, not green.
///
/// The ending is a status, not a decoration: `[W]` tells a reader the phase
/// finished with something they should look at. It is the run's signal too, since
/// the library sync copies this list into the summary's `warnings`: a tool-phase
/// warning such as the prune sweep's surviving directory turns this bar yellow,
/// the summary icon yellow, and the exit status into the warning one. Swapping the
/// two branches of `finish_overall_bar` fails this test.
#[test]
fn finish_overall_bar_ends_warning_when_warnings_present() {
    let bar = RecordingTrackedHandle::new(3);

    finish_overall_bar(&bar, &["a payload directory survived its own removal".to_string()]);

    assert_eq!(
        bar.ops(),
        vec![ProgressOp::FinishWarning],
        "a warning-carrying sync must end the overall bar with `finish_warning` ([W])"
    );
}

/// A run with nothing to report ends its overall bar green.
///
/// The other half of the same rule: an empty warning list is a clean run, so
/// the bar ends `[S]` and not `[W]`. Swapping the two branches of
/// `finish_overall_bar` fails this test.
#[test]
fn finish_overall_bar_ends_success_without_warnings() {
    let bar = RecordingTrackedHandle::new(3);

    finish_overall_bar(&bar, &[]);

    assert_eq!(
        bar.ops(),
        vec![ProgressOp::FinishSuccess],
        "a warning-free sync must end the overall bar with `finish_success` ([S])"
    );
}

/// A run whose tail failed ends its overall bar red, not animating forever.
///
/// The steps after provisioning — materialize, the env-file write, the document
/// save, the prune sweep — share one error route, and that route finishes the
/// bar red exactly as a resolve or fetch failure does. A pass that unwound with
/// the caller's row still animating would hide its own failure from the screen
/// the caller is about to commit (`service.rs` returns before joining it).
///
/// This fixture registers no `[prn]` prune bar: it desires and generates no
/// tools, so the rewrite has no prune candidate and the zero-candidate guard
/// skips the bar entirely. The case where the bar does exist is
/// `reconcile_desired_tools_finish_prune_bar_red_when_tail_fails`.
#[tokio::test]
async fn reconcile_desired_tools_finish_overall_bar_red_when_tail_fails() {
    let tmp = mediapm_utils::temp::artifact_dir().unwrap();
    let cache_root = mediapm_utils::temp::cache_dir().unwrap();
    let paths = MediaPmPaths::from_root(tmp.path());
    let tracker = RecordingProgressTracker::new();
    let state = MediaPmState::default();
    let workspace_cas = super::open_workspace_cas_store(&paths).await.expect("open workspace cas");

    // A regular file where the tools directory belongs fails the tail's first
    // filesystem step with a real IO error, so the route is exercised end to end
    // without a mocked upstream.
    std::fs::create_dir_all(&paths.runtime_root).expect("create runtime root");
    std::fs::write(&paths.tools_dir, b"not a directory").expect("plant the conflicting file");

    let result = reconcile_desired_tools(
        workspace_cas,
        &paths,
        &BTreeMap::new(),
        &BTreeMap::new(),
        PayloadStaging::InPlace,
        RecheckPolicy::default(),
        &state,
        Some(cache_root.path()),
        &tracker,
        None,
    )
    .await
    .map(ToolSyncOutcome::into_report);

    assert!(
        result.is_err(),
        "a file where the tools directory belongs must fail the sync: {:?}",
        result.ok()
    );
    let ops = tracker.ops();
    assert_eq!(
        ops.iter().filter(|op| matches!(op, ProgressOp::FinishError)).count(),
        1,
        "a failing tail must end the overall bar red exactly once; got {ops:?}"
    );
    assert!(
        !ops.iter().any(|op| matches!(op, ProgressOp::FinishSuccess | ProgressOp::FinishWarning)),
        "a failed pass must not end green or yellow; got {ops:?}"
    );
}

/// A run whose tail failed ends the `[prn]` prune bar red as well.
///
/// The prune bar is registered before the tail block, and its only other
/// terminal op (`finish_success`) sits inside that block, so a tail that fails
/// after the registration used to leave the bar animating under an error the
/// caller propagates without finishing it. Both bars the pass owns end red, and
/// each ending is asserted per bar, by recording identity: a shared count of
/// terminal ops would also pass if one bar finished twice and the other never.
///
/// The fixture seeds one prune candidate, because the bar exists only when the
/// rewrite found something to prune — the candidate-less case is
/// `reconcile_desired_tools_finish_overall_bar_red_when_tail_fails`. Removing
/// the prune bar's `finish_error` from the tail's error arm fails this test.
#[tokio::test]
async fn reconcile_desired_tools_finish_prune_bar_red_when_tail_fails() {
    let tmp = mediapm_utils::temp::artifact_dir().unwrap();
    let cache_root = mediapm_utils::temp::cache_dir().unwrap();
    let paths = MediaPmPaths::from_root(tmp.path());
    let tracker = RecordingProgressTracker::new();
    let state = MediaPmState::default();
    let workspace_cas = super::open_workspace_cas_store(&paths).await.expect("open workspace cas");

    // A manual tool in the generated document is a prune candidate: nothing in
    // the desired set provisions it, so the rewrite removes it and the sync
    // registers the bar that tracks that removal.
    let mut content_map = BTreeMap::new();
    content_map.insert("linux/user_script".to_string(), "blake3:manual".to_string());
    let tool_spec = ToolSpec {
        name: "user_script".to_string(),
        runtime: ToolRuntime {
            content_map,
            // `in_place` staging requires a payload root the seeded spec does
            // not have; the candidate is pruned either way.
            payload_staging: ConfigPayloadStaging::Copy,
            ..Default::default()
        },
        ..Default::default()
    };
    let doc = NickelDocument {
        tools: BTreeMap::from([("user_script@somehash".to_string(), tool_spec)]),
        ..Default::default()
    };
    save_conductor_generated_document(&paths, &doc).expect("pre-save generated doc");

    // A regular file where the tools directory belongs fails the tail's first
    // filesystem step, after both bars are registered.
    std::fs::write(&paths.tools_dir, b"not a directory").expect("plant the conflicting file");

    let result = reconcile_desired_tools(
        workspace_cas,
        &paths,
        &BTreeMap::new(),
        &BTreeMap::new(),
        PayloadStaging::InPlace,
        RecheckPolicy::default(),
        &state,
        Some(cache_root.path()),
        &tracker,
        None,
    )
    .await
    .map(ToolSyncOutcome::into_report);
    assert!(
        result.is_err(),
        "a file where the tools directory belongs must fail the sync: {:?}",
        result.ok()
    );

    let recorded = tracker.recorded();
    let bar_of = |label: &str| {
        recorded.iter().find_map(|entry| match &entry.op {
            ProgressOp::AddBar { label: added, .. } if added == label => Some(entry.bar),
            _ => None,
        })
    };
    let overall = bar_of("syncing tools").expect("the phase registers its overall bar");
    let prune =
        bar_of("pruning [prn]").expect("a seeded prune candidate must register the `[prn]` bar");

    // The terminal ops of one bar, in order: a bar ends red once, and neither
    // finishing green nor yellow may appear on either bar of a failed pass.
    let endings = |bar: BarId| -> Vec<ProgressOp> {
        recorded
            .iter()
            .filter(|entry| entry.bar == bar)
            .map(|entry| entry.op.clone())
            .filter(|op| {
                matches!(
                    op,
                    ProgressOp::FinishSuccess
                        | ProgressOp::FinishError
                        | ProgressOp::FinishWarning
                        | ProgressOp::FinishAndClear
                )
            })
            .collect()
    };
    assert_eq!(
        endings(overall),
        vec![ProgressOp::FinishError],
        "a failing tail must end the overall bar red exactly once; got {recorded:?}"
    );
    assert_eq!(
        endings(prune),
        vec![ProgressOp::FinishError],
        "a failing tail must end the `[prn]` prune bar red too, not leave it animating; \
         got {recorded:?}"
    );
}

/// The `Skipped` arm may re-assert an entry the pool already counted without
/// counting it twice.
///
/// The pool advances index 0 when its future settles; the apply pass then
/// asks for the same index through the one arm that advances at all (the
/// `Fetched` arms never touch the counter, so a re-assert there would pin
/// nothing). Exactly one `Advance { delta: 1 }` is the invariant: an entry
/// counts once no matter how many routes ask.
#[test]
fn apply_pass_reasserting_a_counted_entry_does_not_double_advance() {
    let screen = RecordingProgressTracker::new();
    let bar: Arc<dyn ProgressBarApi> = Arc::new(screen.add_bar(7, "syncing tools"));
    let progress = OverallProgress::new(&bar);

    progress.advance_once(0); // the pool counted this entry at completion

    // Re-assert entry 0 through the `Skipped` arm — the arm whose
    // `advance_once` call is the second route to the same index.
    let entry = ProvisionEntry {
        tool_id: "ffmpeg".to_string(),
        tool_requirement: ToolRequirement::default(),
        kind: EntryKind::Explicit,
    };
    let mut generated_doc = NickelDocument::default();
    let mut tool_runtimes = BTreeMap::new();
    let mut report = ToolSyncReport::default();
    let mut live_state = std::collections::HashMap::new();
    let mut pruned_tools = 0usize;

    apply_entry_outcome(
        &entry,
        EntryOutcome::Skipped { key: "ffmpeg".to_string(), spec_runtime: ToolRuntime::default() },
        &mut generated_doc,
        &mut tool_runtimes,
        &mut report,
        &mut live_state,
        &mut pruned_tools,
        0,
        &progress,
    );

    let ops = screen.ops();
    let advances: usize = ops.iter().filter(|op| matches!(op, ProgressOp::Advance { .. })).count();
    assert_eq!(advances, 1, "re-asserting a counted entry must be a no-op; got {ops:?}");
    // Non-vacuity: the re-assert only means something if the arm actually ran.
    assert_eq!(
        report.tools_skipped, 1,
        "the `Skipped` arm must have run for the re-assert to mean anything; got {report:?}"
    );
}
