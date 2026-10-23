use mediapm::{MediaRuntimeStorage, ToolRequirement};

use crate::common::stub_upstream::mock_all_managed_tools;
use crate::common::{read_generated_doc, service_with_cache, test_sync_progress_overrides};

// ---------------------------------------------------------------------------
// Dependency validation integration tests
// ---------------------------------------------------------------------------

/// Sync rejects a tool with an unknown dependency key (e.g. `ffmpeg_version`
/// instead of `ffmpeg`) and suggests the correct key via "did you mean".
///
/// Uses `runtime_storage_overrides.tools` to inject the bad dependency
/// (bypasses NCL evaluation, which expects Nickel syntax).
#[tokio::test]
async fn sync_rejects_bad_dependency_key() {
    use std::collections::BTreeMap;

    let bad_deps: BTreeMap<String, mediapm::ConfigVersionSpec> =
        [("ffmpeg_version".to_string(), mediapm::ConfigVersionSpec::Latest)].into();

    let mut runtime = MediaRuntimeStorage::default();
    runtime.tools.insert(
        "yt-dlp".to_string(),
        ToolRequirement { dependencies: bad_deps, ..ToolRequirement::default() },
    );

    let (mut service, _root, _cache_root) =
        service_with_cache(runtime).await.expect("service creation");

    // Sync should fail with MPM-E001 and a suggestion.
    let result = service
        .sync_tools_with_progress_overrides(false, false, test_sync_progress_overrides())
        .await;
    let Err(err) = result else {
        panic!("sync should fail with bad dep key, but succeeded");
    };
    let msg = err.to_string();
    assert!(msg.contains("MPM-E001"), "error should contain MPM-E001 code, got: {msg}");
    assert!(msg.contains("ffmpeg_version"), "error should mention the bad key: {msg}");
    assert!(
        msg.contains("did you mean") || msg.contains("suggestion"),
        "error should suggest alternatives: {msg}"
    );
    assert!(msg.contains("ffmpeg"), "suggestion should mention 'ffmpeg': {msg}");
}

/// Sync rejects a dependency key that is not in the tool's registered
/// `dependency_types()`, even when that key is present in `desired_tools`
/// as a configured tool.
///
/// e.g. `sd` is registered as a dependency for `rsgain`, not for `yt-dlp`.
/// So yt-dlp with `dependencies.sd = "latest"` must be rejected.
#[tokio::test]
async fn sync_rejects_dep_key_not_in_known_types() {
    use std::collections::BTreeMap;

    let bad_deps: BTreeMap<String, mediapm::ConfigVersionSpec> =
        [("sd".to_string(), mediapm::ConfigVersionSpec::Latest)].into();

    let mut runtime = MediaRuntimeStorage::default();
    // Configure yt-dlp with `sd` as a dependency — `sd` is in rsgain's
    // dependency_types but NOT in yt-dlp's.
    runtime.tools.insert(
        "yt-dlp".to_string(),
        ToolRequirement { dependencies: bad_deps, ..ToolRequirement::default() },
    );
    // Also configure `sd` as a tool in desired_tools — should NOT make it
    // a valid dependency key for yt-dlp.
    runtime.tools.insert("sd".to_string(), ToolRequirement::default());

    let (mut service, _root, _cache_root) =
        service_with_cache(runtime).await.expect("service creation");

    let result = service
        .sync_tools_with_progress_overrides(false, false, test_sync_progress_overrides())
        .await;
    let Err(err) = result else {
        panic!("sync should fail with dep key not in known types, but succeeded");
    };
    let msg = err.to_string();
    assert!(msg.contains("MPM-E001"), "error should contain MPM-E001 code, got: {msg}");
    assert!(msg.contains("sd"), "error should mention the bad key: {msg}");
    assert!(
        msg.contains("did you mean") || msg.contains("suggestion"),
        "error should suggest alternatives: {msg}"
    );
    assert!(msg.contains("ffmpeg"), "suggestion should mention 'ffmpeg': {msg}");
    assert!(msg.contains("deno"), "suggestion should mention 'deno': {msg}");
}

/// Sync rejects a non-ffmpeg tool that sets `max_input_slots` (a field that
/// only applies to ffmpeg) with `MPM-E001`.
#[tokio::test]
async fn sync_rejects_slot_field_on_non_ffmpeg() {
    let mut runtime = MediaRuntimeStorage::default();
    runtime.tools.insert(
        "yt-dlp".to_string(),
        ToolRequirement { max_input_slots: 8, ..ToolRequirement::default() },
    );

    let (mut service, _root, _cache_root) =
        service_with_cache(runtime).await.expect("service creation");

    let result = service
        .sync_tools_with_progress_overrides(false, false, test_sync_progress_overrides())
        .await;
    let Err(err) = result else {
        panic!("sync should fail with slot field on non-ffmpeg tool, but succeeded");
    };
    let msg = err.to_string();
    assert!(msg.contains("MPM-E001"), "error should contain MPM-E001 code, got: {msg}");
    assert!(msg.contains("max_input_slots"), "error should mention the field: {msg}");
    assert!(msg.contains("yt-dlp"), "error should mention the tool: {msg}");
}

// The former `sync_accepts_slot_field_on_ffmpeg` sync-level test was moved to
// the crate-internal boundary unit test `slot_fields_are_accepted_on_ffmpeg`
// (`tools/dependency.rs`) because no network-free tool accepts slot fields
// (only ffmpeg does), so a sync-level acceptance run could not be hermetic and
// under fail-fast provisioning a real ffmpeg fetch aborted the sync on an
// unreachable upstream. That premise no longer holds: the provider seams let a
// test answer ffmpeg's three upstream hosts locally, so the test is back at the
// sync level below, where it exercises the real provisioning path. The reject
// direction stays in `sync_rejects_slot_field_on_non_ffmpeg` (it fires before
// any network).

/// Sync accepts `max_input_slots` and `max_output_slots` on ffmpeg, the only
/// tool those fields apply to.
///
/// ffmpeg is the one tool that accepts slot fields, and its payload is a real
/// download across three upstream hosts, so an acceptance run at the sync level
/// needs a mocked upstream: under fail-fast provisioning an unmocked ffmpeg
/// fetch aborts the sync on an unreachable host.
///
/// Asserting the sync returned `Ok` alone would be weak, because it would also
/// pass if validation short-circuited before provisioning. The payload
/// assertion is what makes the acceptance real: it can only hold if the run got
/// past validation and completed a full ffmpeg provision.
#[tokio::test]
async fn sync_accepts_slot_field_on_ffmpeg() -> Result<(), mediapm::MediaPmError> {
    let mut runtime = MediaRuntimeStorage::default();
    runtime.tools.insert(
        "ffmpeg".to_string(),
        ToolRequirement { max_input_slots: 8, max_output_slots: 4, ..ToolRequirement::default() },
    );

    let (_mock, _seams) = mock_all_managed_tools().await;
    let (mut service, _root, _cache_root) = service_with_cache(runtime).await?;

    service
        .sync_tools_with_progress_overrides(false, false, test_sync_progress_overrides())
        .await?;

    let doc = read_generated_doc(&service);
    let content_map = &doc
        .tools
        .values()
        .find(|spec| spec.name == "ffmpeg" && !spec.runtime.content_map.is_empty())
        .expect("ffmpeg must carry an active spec after an accepted sync")
        .runtime
        .content_map;
    for os in ["linux", "macos", "windows"] {
        assert!(
            content_map.keys().any(|key| key.starts_with(&format!("{os}/"))),
            "ffmpeg must have provisioned the {os} slot; keys are {:?}",
            content_map.keys().collect::<Vec<_>>(),
        );
    }
    Ok(())
}

/// Sync accepts `recheck_seconds` on any tool — it is a global optional field,
/// not a tool-specific slot field.
///
/// Hermetic by design: `media-tagger` resolves to its build-time identity with
/// no network (launcher generation, no upstream release), so the sync reaches
/// success on a real provision path. The originally used network-provisioned
/// tools (yt-dlp) now fail fast on an unreachable upstream, which is the
/// intended behavior — full-provision acceptance for live tools lives behind
/// the network-gated online paths.
#[tokio::test]
async fn sync_accepts_recheck_seconds_on_any_tool() {
    let mut runtime = MediaRuntimeStorage::default();
    runtime.tools.insert(
        "media-tagger".to_string(),
        ToolRequirement { recheck_seconds: 30, ..ToolRequirement::default() },
    );

    let (mut service, _root, _cache_root) =
        service_with_cache(runtime).await.expect("service creation");

    let result = service
        .sync_tools_with_progress_overrides(false, false, test_sync_progress_overrides())
        .await;
    assert!(
        result.is_ok(),
        "recheck_seconds on non-ffmpeg tool should be accepted: {:?}",
        result.err()
    );
}

/// Provisioning failures fail fast: an unresolvable tool aborts the whole sync
/// with the phase-tagged cause, and nothing is applied or written (no
/// `state.json`, no `conductor.generated.ncl`). This pins the fail-fast
/// contract that replaced the old warn-continue behavior, hermetically (an
/// unknown tool id resolves to a provider error with no network access).
#[tokio::test]
async fn sync_unresolvable_tool_fails_fast_and_writes_nothing() {
    let mut runtime = MediaRuntimeStorage::default();
    runtime.tools.insert("nonexistent-tool".to_string(), ToolRequirement::default());

    let (mut service, _root, _cache_root) =
        service_with_cache(runtime).await.expect("service creation");

    let result = service
        .sync_tools_with_progress_overrides(false, false, test_sync_progress_overrides())
        .await;
    let err = result.expect_err("an unresolvable tool must abort the whole sync");
    let message = err.to_string();
    assert!(
        message.contains("tool sync failed: nonexistent-tool (resolve)"),
        "the error must name the failing tool and resolve phase: {message}"
    );

    let paths = service.paths();
    assert!(!paths.mediapm_state_json.exists(), "a failed sync must not write state.json");
    assert!(
        !paths.conductor_generated_ncl.exists(),
        "a failed sync must not write conductor.generated.ncl"
    );
}
