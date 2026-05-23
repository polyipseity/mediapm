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
            && source.contains("hierarchy_root_dir: Some(\".\".to_string())")
            && source.contains("mediapm_tmp_dir: Some(\"tmp\".to_string())")
            && source.contains("conductor_config: Some(\"mediapm.conductor.ncl\".to_string())")
            && source.contains(
                "conductor_machine_config: Some(\"mediapm.conductor.machine.ncl\".to_string())"
            )
            && source.contains(
                "conductor_state_config: Some(\".mediapm/state.conductor.ncl\".to_string())"
            )
            && source.contains("conductor_tmp_dir: Some(\".mediapm/tmp\".to_string())")
            && source
                .contains("conductor_schema_dir: Some(\".mediapm/config/conductor\".to_string())")
            && source.contains(
                "inherited_env_vars: Some(default_runtime_inherited_env_vars_for_host())"
            )
            && source.contains("media_state_config: Some(\".mediapm/state.ncl\".to_string())")
            && source.contains("env_file: Some(\".mediapm/.env\".to_string())")
            && source.contains(
                "mediapm_schema_dir: Some(Some(\".mediapm/config/mediapm\".to_string()))"
            )
            && source.contains("use_user_tool_cache: Some(true)"),
        "demo should write explicit runtime defaults for mediapm_dir/hierarchy/tmp/conductor paths/env/schema/inherited env vars and cache toggle"
    );
}

/// Verifies local `demo` explicitly declares the `import` tool
/// requirement and uses `$0` whole-match replacement for metadata extension
/// transforms.
#[test]
fn demo_declares_import_and_dollar_zero_metadata_transforms() {
    let source = include_str!("../../examples/mediapm_demo.rs");

    assert!(
        source.contains("\"import\".to_string()")
            && source.contains("replacement: \".$0\".to_string()")
            && source.contains("MediaStepTool::Import"),
        "demo should explicitly require import and keep $0 whole-match transform semantics"
    );
}

/// Verifies local `demo` defaults to config-only mode when compiled as a
/// Cargo test-target binary.
#[test]
fn demo_defaults_to_config_only_when_built_as_test_target() {
    let source = include_str!("../../examples/mediapm_demo.rs");

    assert!(
        source.contains("fn running_as_test_binary() -> bool")
            && source.contains("cfg!(test)")
            && source.contains("!running_as_test_binary()")
            && source.contains("MEDIAPM_DEMO_RUN_SYNC"),
        "demo should auto-disable full sync in test-target runs while keeping explicit env override support"
    );
}

/// Verifies local `demo` wires one explicit playlist hierarchy node with
/// duplicated ids and per-item absolute path override.
#[test]
fn demo_configures_playlist_hierarchy_entry() {
    let source = include_str!("../../examples/mediapm_demo.rs");

    assert!(
        source.contains("path: \"playlists\".to_string()")
            && source.contains("path: \"local-demo.m3u8\".to_string()")
            && source.contains("kind: HierarchyNodeKind::Playlist")
            && source.contains("PlaylistItemRef {")
            && source.contains("id: DEMO_PLAYLIST_TARGET_HIERARCHY_ID.to_string()")
            && source.contains("path: PlaylistEntryPathMode::Relative")
            && source.contains("path: PlaylistEntryPathMode::Absolute")
            && source.contains("children: media_hierarchy_children")
            && source.contains("document.hierarchy = vec!["),
        "demo should configure nested playlist hierarchy entries with duplicated target ids and relative+absolute path modes"
    );
}
