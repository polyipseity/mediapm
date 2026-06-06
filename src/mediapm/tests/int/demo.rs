//! Integration guardrails for the local `demo` example wiring.
//!
//! `examples/demo.rs` is compile-only in Cargo (`test = false`), so these
//! source-level checks enforce durable behavior without requiring runtime
//! execution in every automated test pass.

/// Verifies `demo` relies on managed media-tagger input defaults for strict
/// identification instead of restating the default in step options.
#[test]
fn demo_relies_on_media_tagger_default_strict_identification() {
    let source = include_str!("../../examples/mediapm_demo.rs");

    assert!(
        !source.contains("\"strict_identification\".to_string()")
            && source.contains("\"recording_mbid\".to_string()")
            && source.contains("\"release_mbid\".to_string()")
            && source.contains("tool: MediaStepTool::MediaTagger"),
        "demo should omit explicit strict_identification step options and rely on managed input defaults"
    );
}

/// Verifies local `demo` writes explicit runtime defaults so generated
/// `mediapm.ncl` documents all runtime knobs (not just tool-cache toggle).
#[test]
fn demo_writes_explicit_runtime_defaults() {
    let source = include_str!("../../examples/mediapm_demo.rs");

    assert!(
        source.contains("mediapm_dir: Some(\".mediapm\".to_string())")
            && source.contains("hierarchy_root_dir: Some(\"media\".to_string())")
            && source.contains(
                "materialization_preference_order: Some(DEMO_MATERIALIZATION_PREFERENCE_ORDER.to_vec())"
            )
            && source.contains("conductor_config: Some(\"mediapm.conductor.ncl\".to_string())")
            && source.contains(
                "conductor_machine_config: Some(\"mediapm.conductor.machine.ncl\".to_string())"
            )
            && source.contains(
                "conductor_state_config: Some(\".mediapm/state.conductor.ncl\".to_string())"
            )
            && source
                .contains("conductor_schema_dir: Some(\".mediapm/config/conductor\".to_string())")
            && source.contains(
                "inherited_env_vars: Some(default_runtime_inherited_env_vars_for_host())"
            )
            && source.contains("media_state_config: Some(\".mediapm/state.ncl\".to_string())")
            && source.contains("env_file: Some(\".mediapm/.env\".to_string())")
            && source.contains("env_generated_file: Some(\".mediapm/.env.generated\".to_string())")
            && source.contains(
                "mediapm_schema_dir: Some(Some(\".mediapm/config/mediapm\".to_string()))"
            )
            && source.contains("profiler_enabled: Some(true)"),
        "demo should write explicit runtime defaults for mediapm_dir/hierarchy/conductor paths/env/schema/inherited env vars and profiler/materialization"
    );
}

/// Verifies local `demo` explicitly declares the `import` tool
/// requirement and import step.
#[test]
fn demo_declares_import_and_dollar_zero_metadata_transforms() {
    let source = include_str!("../../examples/mediapm_demo.rs");

    assert!(
        source.contains("\"import\".to_string()") && source.contains("MediaStepTool::Import"),
        "demo should explicitly declare import tool requirement and import step"
    );
}

/// Verifies local `demo` defaults to sync-enabled mode and avoids
/// test-target special-casing.
#[test]
fn demo_defaults_to_sync_enabled_without_test_target_special_casing() {
    let source = include_str!("../../examples/mediapm_demo.rs");

    assert!(
        !source.contains("fn running_as_test_binary() -> bool")
            && source.contains(
                "sync_enabled_from_env_value(std::env::var(DEMO_RUN_SYNC_ENV_VAR).ok().as_deref())"
            )
            && source.contains("MEDIAPM_DEMO_RUN_SYNC"),
        "demo should default to sync enabled and keep env override support without test-target branching"
    );
}

/// Verifies local `demo` wires one explicit playlist hierarchy node with
/// duplicated ids and per-item absolute path override.
#[test]
fn demo_configures_playlist_hierarchy_entry() {
    let source = include_str!("../../examples/mediapm_demo.rs");

    assert!(
        source.contains("path: HierarchyPath::from(\"playlists\")")
            && source.contains("path: HierarchyPath::from(\"local-demo.m3u8\")")
            && source.contains("kind: HierarchyNodeKind::Playlist")
            && source.contains("PlaylistItemRef {")
            && source.contains("id: DEMO_PLAYLIST_TARGET_HIERARCHY_ID.to_string()")
            && source.contains("id: Some(DEMO_MEDIA_FOLDER_HIERARCHY_ID.to_string())")
            && source.contains("variant: Some(\"video_untagged\".to_string())")
            && source.contains("variant: Some(\"audio\".to_string())")
            && !source.contains("audio_tagged")
            && source.contains("path: PlaylistEntryPathMode::Relative")
            && source.contains("path: PlaylistEntryPathMode::Absolute")
            && source.contains("children: media_hierarchy_children")
            && source.contains("document.hierarchy = vec!["),
        "demo should configure nested playlist hierarchy entries with duplicated target ids, media-folder id, and aligned tagged/untagged variants"
    );
}
