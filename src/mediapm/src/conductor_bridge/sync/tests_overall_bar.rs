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

// Non-fatal tool-sync failures (resolve/provision errors) are recorded as
// `report.warnings` and retried on the next sync. The child bars for those
// failed tools must therefore finish with a warning (yellow `[W]`), never
// with an error (red `[F]`). This guards the Phase 2 fix: a resolve failure
// used to call `finish_error()` on the child bar, producing a misleading
// `[F]` while the overall bar correctly showed `[W]`.
#[tokio::test]
async fn reconcile_desired_tools_resolve_failure_shows_warning_not_error() {
    let tmp = mediapm_utils::temp::artifact_dir().unwrap();
    let cache_root = mediapm_utils::temp::cache_dir().unwrap();
    let paths = MediaPmPaths::from_root(tmp.path());
    let tracker = RecordingProgressTracker::new();
    let state = MediaPmState::default();
    let workspace_cas = super::open_workspace_cas_store(&paths).await.expect("open workspace cas");

    // An unknown tool name has no provider registered for resolution, so
    // `resolve_tool_fetch` returns Err and the tool is skipped with a
    // warning rather than aborting the whole sync.
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

    // The sync still succeeds overall (the failure is non-fatal).
    assert!(result.is_ok(), "reconcile_desired_tools failed: {:?}", result.err());
    let report = result.unwrap();
    assert_eq!(
        report.warnings.len(),
        1,
        "expected exactly one warning for the unresolvable tool, got {:?}",
        report.warnings
    );

    let ops = tracker.ops();
    let finish_errors: Vec<&ProgressOp> =
        ops.iter().filter(|op| matches!(op, ProgressOp::FinishError)).collect();
    assert!(
        finish_errors.is_empty(),
        "non-fatal tool-sync failure must not emit FinishError (red [F]); got {finish_errors:?}",
    );

    let finish_warnings: Vec<&ProgressOp> =
        ops.iter().filter(|op| matches!(op, ProgressOp::FinishWarning)).collect();
    assert!(
        !finish_warnings.is_empty(),
        "expected at least one FinishWarning (yellow [W]) for the failed tool, got {ops:?}",
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
    // any network access (the resolve failure is a warning, not an error).
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
    assert!(result.is_ok(), "reconcile_desired_tools failed: {:?}", result.err());

    let ops = screen.ops();
    assert!(
        ops.iter().any(|op| matches!(op, ProgressOp::SetTotal { total: 1 })),
        "the caller's `syncing tools` overall bar must be totalled to the tool count; \
         got {ops:?}"
    );
}
