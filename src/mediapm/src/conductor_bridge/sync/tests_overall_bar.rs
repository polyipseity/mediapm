//! The pinned overall bar of the tool phase and the screen the caller hands in.

use mediapm_utils::progress::recording::{ProgressOp, RecordingProgressTracker};
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
        RecheckPolicy::default(),
        &state,
        Some(cache_root.path()),
        &tracker,
        None,
    )
    .await;

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
        RecheckPolicy::default(),
        &state,
        Some(cache_root.path()),
        &tracker,
        None,
    )
    .await;

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
        RecheckPolicy::default(),
        &state,
        Some(cache_root.path()),
        &screen,
        Some(Arc::new(overall)),
    )
    .await;
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
    let inherited_env_vars = BTreeMap::new();

    apply_entry_outcome(
        &entry,
        EntryOutcome::Skipped { key: "ffmpeg".to_string(), spec_runtime: ToolRuntime::default() },
        &mut generated_doc,
        &mut tool_runtimes,
        &mut report,
        &mut live_state,
        &mut pruned_tools,
        &inherited_env_vars,
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
