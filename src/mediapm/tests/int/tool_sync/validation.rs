use mediapm::{MediaRuntimeStorage, ToolRequirement};

use crate::common::service_with_cache;

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
    let result = service.sync_tools().await;
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

    let result = service.sync_tools().await;
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
