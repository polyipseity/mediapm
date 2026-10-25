//! What `mediapm tool sync` counts as added, updated and skipped.
//!
//! The counters are the script-facing answer to "did anything change", so a
//! tool that reconciles to the artifact already deployed must not be reported
//! as an update: only a changed canonical version or a changed content-map hash
//! is one. A builtin carries no content map, so its identity is the mediapm
//! build it resolves to.

use mediapm::{MediaRuntimeStorage, ToolRequirement};

use crate::common::{service_with_cache, test_sync_progress_overrides};

/// A runtime storage declaring the no-payload `import` builtin.
///
/// A builtin needs no download, so these tests run hermetically with no mock
/// upstream: the pass resolves its launcher locally and fetches nothing.
fn runtime_with_import() -> MediaRuntimeStorage {
    let mut runtime = MediaRuntimeStorage::default();
    runtime.tools.insert("import".to_string(), ToolRequirement::default());
    runtime
}

/// A second sync over an unchanged workspace reports no update.
///
/// The `import` builtin re-registers on every pass, because its skip path is
/// closed to a tool with no content map: it has no hash to match a stored
/// record against. Re-registering the same build is not a change to the tool,
/// so the pass counts it as skipped and leaves `updated` at zero — which is
/// what a script keys off.
#[tokio::test]
async fn regression_a_builtin_that_did_not_change_is_not_updated()
-> Result<(), mediapm::MediaPmError> {
    let (mut service, _root, _cache_root) = service_with_cache(runtime_with_import()).await?;

    let first = service
        .sync_tools_with_progress_overrides(false, false, test_sync_progress_overrides())
        .await?;
    assert_eq!(first.added_tools, 1, "the first pass deploys the builtin: {first:?}");
    assert_eq!(first.updated_tools, 0, "an absent record is an addition, not an update: {first:?}");

    let second = service
        .sync_tools_with_progress_overrides(false, false, test_sync_progress_overrides())
        .await?;
    assert_eq!(second.added_tools, 0, "the builtin is already recorded: {second:?}");
    assert_eq!(
        second.updated_tools, 0,
        "an unchanged build must not be reported as an update: {second:?}"
    );
    assert_eq!(
        second.skipped_tools, 1,
        "the re-registration is counted as a skip instead: {second:?}"
    );
    Ok(())
}

/// A changed build identity is an update.
///
/// The recorded canonical version stands for the build a builtin shipped in.
/// A record naming a different one is a workspace whose tool does not match
/// what this build provides, so the pass reproduces it and says so.
#[tokio::test]
async fn regression_a_builtin_build_change_is_an_update() -> Result<(), mediapm::MediaPmError> {
    let (mut service, _root, _cache_root) = service_with_cache(runtime_with_import()).await?;
    let first = service
        .sync_tools_with_progress_overrides(false, false, test_sync_progress_overrides())
        .await?;
    assert_eq!(first.added_tools, 1, "the first pass deploys the builtin: {first:?}");

    // Rewrite the recorded identity in place: the workspace now holds a
    // deployment of a build this pass does not reproduce.
    let state_path = service.paths().mediapm_state_json.clone();
    let bytes = std::fs::read(&state_path).expect("state.json exists after a sync");
    let mut state: serde_json::Value = serde_json::from_slice(&bytes).expect("state.json parses");
    let records = state
        .get_mut("managed_tools")
        .and_then(serde_json::Value::as_array_mut)
        .expect("managed_tools is an array");
    let record = records
        .iter_mut()
        .find(|record| record.get("tool_id").and_then(serde_json::Value::as_str) == Some("import"))
        .expect("the first pass recorded the import builtin");
    record["canonical_version"] = serde_json::Value::String("a-different-build".to_string());
    std::fs::write(&state_path, serde_json::to_vec_pretty(&state).expect("state encodes"))
        .expect("state.json is writable");

    let second = service
        .sync_tools_with_progress_overrides(false, false, test_sync_progress_overrides())
        .await?;
    assert_eq!(second.added_tools, 0, "the tool is still recorded: {second:?}");
    assert_eq!(
        second.updated_tools, 1,
        "a record naming a different build is an update: {second:?}"
    );
    assert_eq!(second.skipped_tools, 0, "and it is not also counted as a skip: {second:?}");
    Ok(())
}
