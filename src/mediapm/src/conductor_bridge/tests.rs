//! Unit tests for `conductor_bridge` policy helpers.

use std::collections::BTreeMap;

use mediapm_cas::Hash;
use mediapm_conductor::{
    InputBinding, MachineNickelDocument, RuntimeStorageConfig, ToolConfigSpec, ToolKindSpec,
    ToolSpec, default_runtime_inherited_env_vars_for_host,
};

use crate::lockfile::{MediaLockFile, ToolRegistryStatus};
use crate::paths::MediaPmPaths;
use crate::tools::catalog::tool_catalog_entry;
use crate::tools::downloader::{ProvisionedToolPayload, ResolvedToolIdentity};

use super::documents::{
    ensure_conductor_documents, list_tools, load_machine_document, save_machine_document,
};
use super::runtime_storage::normalize_runtime_storage_defaults;
use super::tool_runtime::{
    FfmpegSlotLimits, build_tool_command, build_tool_env, default_max_concurrent_calls,
    default_max_retries, default_tool_config_description, extract_platform_conditional_paths,
    media_tagger_launcher_mediapm_env_var_for_host, merge_tool_config_defaults,
    success_codes_for_tool, tool_spec_has_binary, validate_tool_command,
};

/// Returns default ffmpeg slot limits used by helper-level tests.
#[must_use]
fn default_ffmpeg_slot_limits() -> FfmpegSlotLimits {
    FfmpegSlotLimits::default()
}

/// Protects bootstrap invariant that phase-2 builtins are always available.
#[test]
fn ensure_conductor_documents_registers_builtin_tools_for_new_workspace() {
    let temp = tempfile::tempdir().expect("tempdir");
    let paths = MediaPmPaths::from_root(temp.path());

    ensure_conductor_documents(&paths).expect("bootstrap should succeed");
    let machine = load_machine_document(&paths.conductor_machine_ncl).expect("load machine");

    for tool_id in crate::registered_builtin_ids() {
        let Some(spec) = machine.tools.get(tool_id) else {
            panic!("expected builtin tool '{tool_id}' to be registered");
        };

        assert!(
            matches!(spec.kind, ToolKindSpec::Builtin { .. }),
            "tool '{tool_id}' should remain builtin"
        );
    }
}

/// Protects self-healing behavior for workspaces that predate builtin seeding.
#[test]
fn ensure_conductor_documents_backfills_missing_builtin_tools() {
    let temp = tempfile::tempdir().expect("tempdir");
    let paths = MediaPmPaths::from_root(temp.path());

    let mut machine = MachineNickelDocument::default();
    machine.tools.insert(
        "mediapm.tools.ffmpeg+github-btbn@latest".to_string(),
        ToolSpec {
            kind: ToolKindSpec::Executable {
                command: vec!["ffmpeg".to_string()],
                env_vars: BTreeMap::new(),
                success_codes: vec![0],
            },
            ..ToolSpec::default()
        },
    );
    save_machine_document(&paths.conductor_machine_ncl, &machine).expect("seed machine doc");

    ensure_conductor_documents(&paths).expect("bootstrap should backfill builtins");
    let machine = load_machine_document(&paths.conductor_machine_ncl).expect("reload machine");

    assert!(
        machine.tools.contains_key("mediapm.tools.ffmpeg+github-btbn@latest"),
        "existing tool entries should stay"
    );
    for tool_id in crate::registered_builtin_ids() {
        assert!(
            machine.tools.contains_key(tool_id),
            "expected missing builtin '{tool_id}' to be backfilled"
        );
    }
}

/// Protects listing behavior and lockfile-status overlay semantics.
#[test]
fn list_tools_reports_binary_and_status_fields() {
    let temp = tempfile::tempdir().expect("tempdir");
    let paths = MediaPmPaths::from_root(temp.path());
    let mut machine = MachineNickelDocument::default();
    machine.tools.insert(
        "mediapm.tools.ffmpeg+github-btbn@latest".to_string(),
        ToolSpec {
            kind: ToolKindSpec::Executable {
                command: vec!["ffmpeg".to_string()],
                env_vars: BTreeMap::new(),
                success_codes: vec![0],
            },
            ..ToolSpec::default()
        },
    );
    machine.tool_configs.insert(
        "mediapm.tools.ffmpeg+github-btbn@latest".to_string(),
        ToolConfigSpec {
            max_concurrent_calls: -1,
            max_retries: -1,
            description: Some("ffmpeg tool config".to_string()),
            input_defaults: BTreeMap::new(),
            env_vars: BTreeMap::new(),
            content_map: Some(BTreeMap::from([(
                "ffmpeg".to_string(),
                Hash::from_content(b"ffmpeg"),
            )])),
        },
    );
    machine.external_data.insert(
        Hash::from_content(b"ffmpeg"),
        mediapm_conductor::ExternalContentRef {
            description: Some("ffmpeg binary payload".to_string()),
        },
    );

    save_machine_document(&paths.conductor_machine_ncl, &machine).expect("save machine doc");

    let lock = MediaLockFile::default();
    let rows = list_tools(&paths, &lock).expect("list tools");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].tool_id, "mediapm.tools.ffmpeg+github-btbn@latest");
    assert_eq!(rows[0].status, ToolRegistryStatus::Active);
}

/// Protects default description formatting for generated tool configs.
#[test]
fn default_tool_config_description_contains_identity_fields() {
    let description = default_tool_config_description(
        "yt-dlp",
        &ResolvedToolIdentity {
            git_hash: Some("abc123def456".to_string()),
            version: Some("2026.04.01".to_string()),
            tag: Some("v2026.04.01".to_string()),
            release_description: Some("Nightly downloader release".to_string()),
        },
        "yt-dlp remote media downloader",
    );

    assert!(description.contains("git_hash: abc123def456"));
    assert!(description.contains("version: 2026.04.01"));
    assert!(description.contains("tag: v2026.04.01"));
    assert!(description.contains("catalog_description: yt-dlp remote media downloader"));
}

/// Protects default download-pressure policy for yt-dlp process fan-out.
#[test]
fn default_max_concurrent_calls_limits_yt_dlp_to_one() {
    assert_eq!(default_max_concurrent_calls("yt-dlp"), 1);
    assert_eq!(default_max_concurrent_calls("YT-DLP"), 1);
}

/// Protects default behavior by leaving non-download tools unbounded.
#[test]
fn default_max_concurrent_calls_keeps_other_tools_unbounded() {
    assert_eq!(default_max_concurrent_calls("ffmpeg"), -1);
    assert_eq!(default_max_concurrent_calls("rsgain"), -1);
}

/// Protects default retry policy so yt-dlp uses one outer retry while other
/// tools keep retries disabled.
#[test]
fn default_max_retries_sets_yt_dlp_to_one_only() {
    assert_eq!(default_max_retries("yt-dlp"), 1);
    assert_eq!(default_max_retries("YT-DLP"), 1);
    assert_eq!(default_max_retries("ffmpeg"), -1);
    assert_eq!(default_max_retries("rsgain"), -1);
}

/// Protects default success-code policy for native media-tagger execution.
#[test]
fn success_codes_for_media_tagger_match_default_policy() {
    let success_codes = success_codes_for_tool("media-tagger");

    assert_eq!(success_codes, vec![0]);
}

/// Protects default tool-config merge when no prior config exists.
#[test]
fn merge_tool_config_defaults_backfills_yt_dlp_limit_and_description() {
    let content_map = BTreeMap::from([("yt-dlp".to_string(), Hash::from_content(b"yt"))]);
    let merged = merge_tool_config_defaults(
        None,
        "yt-dlp",
        content_map.clone(),
        "yt description".to_string(),
        default_ffmpeg_slot_limits(),
    );

    assert_eq!(merged.max_concurrent_calls, 1);
    assert_eq!(merged.max_retries, 1);
    assert_eq!(merged.description.as_deref(), Some("yt description"));
    assert_eq!(merged.content_map, Some(content_map));
}

/// Protects explicit machine overrides from being silently clobbered.
#[test]
fn merge_tool_config_defaults_preserves_explicit_override_values() {
    let existing = ToolConfigSpec {
        max_concurrent_calls: 2,
        max_retries: 3,
        description: Some("operator provided".to_string()),
        input_defaults: BTreeMap::new(),
        env_vars: BTreeMap::new(),
        content_map: None,
    };
    let merged = merge_tool_config_defaults(
        Some(&existing),
        "yt-dlp",
        BTreeMap::from([("yt-dlp".to_string(), Hash::from_content(b"yt-new"))]),
        "generated description".to_string(),
        default_ffmpeg_slot_limits(),
    );

    assert_eq!(merged.max_concurrent_calls, 2);
    assert_eq!(merged.max_retries, 3);
    assert_eq!(merged.description.as_deref(), Some("operator provided"));
    assert!(merged.content_map.as_ref().is_some_and(|map| map.contains_key("yt-dlp")));
}

/// Protects value-centric default-input ergonomics by preserving scalar
/// option defaults and list-typed `option_args` defaults.
#[test]
fn merge_tool_config_defaults_preserves_value_only_option_bindings() {
    let existing = ToolConfigSpec {
        max_concurrent_calls: -1,
        max_retries: -1,
        description: Some("operator defaults".to_string()),
        input_defaults: BTreeMap::from([
            ("audio_quality".to_string(), InputBinding::String("2".to_string())),
            ("vn".to_string(), InputBinding::String("true".to_string())),
            ("option_args".to_string(), InputBinding::StringList(vec!["-hide_banner".to_string()])),
        ]),
        env_vars: BTreeMap::new(),
        content_map: None,
    };

    let merged = merge_tool_config_defaults(
        Some(&existing),
        "ffmpeg",
        BTreeMap::from([("ffmpeg".to_string(), Hash::from_content(b"ffmpeg"))]),
        "ffmpeg description".to_string(),
        default_ffmpeg_slot_limits(),
    );

    assert_eq!(
        merged.input_defaults.get("audio_quality"),
        Some(&InputBinding::String("2".to_string())),
    );
    assert_eq!(merged.input_defaults.get("vn"), Some(&InputBinding::String("true".to_string())),);
    assert_eq!(
        merged.input_defaults.get("option_args"),
        Some(&InputBinding::StringList(vec!["-hide_banner".to_string()])),
    );
}

/// Protects platform-selector parsing used for command validation.
#[test]
fn extract_platform_conditional_paths_parses_expected_targets() {
    let parsed = extract_platform_conditional_paths(
        "${context.os == \"windows\" ? windows/yt-dlp.exe | ''}${context.os == \"linux\" ? linux/yt-dlp | ''}${context.os == \"macos\" ? macos/yt-dlp | ''}",
    )
    .expect("selector parsing should succeed");

    assert_eq!(parsed.get("windows").map(String::as_str), Some("windows/yt-dlp.exe"));
    assert_eq!(parsed.get("linux").map(String::as_str), Some("linux/yt-dlp"));
    assert_eq!(parsed.get("macos").map(String::as_str), Some("macos/yt-dlp"));
}

/// Protects command validation for folder-form ZIP content-map entries.
#[test]
fn validate_tool_command_accepts_directory_content_map_keys() {
    let content_map = BTreeMap::from([("windows/".to_string(), Hash::from_content(b"zip"))]);

    validate_tool_command("ffmpeg", &["windows/bin/ffmpeg.exe".to_string()], &content_map)
        .expect("directory key should satisfy nested command path");
}

/// Protects command validation for root ZIP unpack content-map entries.
#[test]
fn validate_tool_command_accepts_root_zip_content_map_key() {
    let content_map = BTreeMap::from([("./".to_string(), Hash::from_content(b"zip"))]);

    validate_tool_command("yt-dlp", &["yt-dlp.exe".to_string()], &content_map)
        .expect("root ZIP key should satisfy command path");
}

/// Protects ffmpeg execution ordering so output-format args apply to output path.
#[test]
fn build_tool_command_places_ffmpeg_trailing_args_before_output_path() {
    let payload = ProvisionedToolPayload {
        tool_id: "mediapm.tools.ffmpeg+github-btbn@latest".to_string(),
        command_selector: "ffmpeg".to_string(),
        content_entries: BTreeMap::new(),
        identity: ResolvedToolIdentity::default(),
        source_label: "GitHub Releases".to_string(),
        source_identifier: "github-btbn".to_string(),
        catalog: tool_catalog_entry("ffmpeg").expect("catalog entry"),
        warnings: Vec::new(),
    };

    let command = build_tool_command("ffmpeg", &payload, default_ffmpeg_slot_limits());

    let trailing_index = command
        .iter()
        .position(|token| token == "${*inputs.trailing_args}")
        .expect("ffmpeg command should include trailing args placeholder");
    let output_index = command
        .iter()
        .position(|token| token == "${*inputs.output_path_0}")
        .expect("ffmpeg command should include managed output path");

    assert!(
        command.iter().any(|token| token == "${*inputs.audio_codec ? -c:a | ''}"),
        "ffmpeg command should expose conditional key token for audio_codec"
    );
    assert!(
        command.iter().any(|token| token == "${*inputs.audio_codec}"),
        "ffmpeg command should expose scalar value token for audio_codec"
    );
    assert!(
        trailing_index < output_index,
        "trailing args must remain before output path so users can override defaults"
    );
}

/// Protects internal media-tagger command synthesis for in-place tagging.
#[test]
fn build_tool_command_sets_media_tagger_flags() {
    let payload = ProvisionedToolPayload {
        tool_id: "mediapm.tools.media-tagger+mediapm-internal@latest".to_string(),
        command_selector: "windows/media-tagger.cmd".to_string(),
        content_entries: BTreeMap::new(),
        identity: ResolvedToolIdentity::default(),
        source_label: "mediapm internal launcher".to_string(),
        source_identifier: "mediapm-internal".to_string(),
        catalog: tool_catalog_entry("media-tagger").expect("catalog entry"),
        warnings: Vec::new(),
    };

    let command = build_tool_command("media-tagger", &payload, default_ffmpeg_slot_limits());

    assert!(
        command.iter().any(|token| token == "--input"),
        "media-tagger command should pass explicit input path argument"
    );
    assert!(
        command.iter().any(|token| token == "--output"),
        "media-tagger command should pass explicit output path argument"
    );
    assert!(
        command.iter().any(|token| {
            token == "${*inputs.strict_identification == \"true\" ? --strict-identification | ''}"
        }),
        "media-tagger command should expose strict_identification option key"
    );
    assert!(command.iter().any(|token| token == "${*inputs.trailing_args}"));
}

/// Protects simplified boolean rendering by ensuring generated templates only
/// gate on exact `"true"` and no longer emit synonym/false-branch checks.
#[test]
fn build_tool_command_bool_templates_check_only_true() {
    let yt_payload = ProvisionedToolPayload {
        tool_id: "mediapm.tools.yt-dlp+github-releases@latest".to_string(),
        command_selector: "yt-dlp".to_string(),
        content_entries: BTreeMap::new(),
        identity: ResolvedToolIdentity::default(),
        source_label: "GitHub Releases".to_string(),
        source_identifier: "github-releases".to_string(),
        catalog: tool_catalog_entry("yt-dlp").expect("catalog entry"),
        warnings: Vec::new(),
    };
    let media_tagger_payload = ProvisionedToolPayload {
        tool_id: "mediapm.tools.media-tagger+mediapm-internal@latest".to_string(),
        command_selector: "windows/media-tagger.cmd".to_string(),
        content_entries: BTreeMap::new(),
        identity: ResolvedToolIdentity::default(),
        source_label: "mediapm internal launcher".to_string(),
        source_identifier: "mediapm-internal".to_string(),
        catalog: tool_catalog_entry("media-tagger").expect("catalog entry"),
        warnings: Vec::new(),
    };

    let yt_command = build_tool_command("yt-dlp", &yt_payload, default_ffmpeg_slot_limits());
    let media_tagger_command =
        build_tool_command("media-tagger", &media_tagger_payload, default_ffmpeg_slot_limits());
    let combined = yt_command
        .iter()
        .chain(media_tagger_command.iter())
        .map(String::as_str)
        .collect::<Vec<_>>();

    assert!(combined.iter().any(|token| token.contains("== \"true\"")));
    assert!(combined.iter().all(|token| !token.contains("== \"1\"")));
    assert!(combined.iter().all(|token| !token.contains("== \"yes\"")));
    assert!(combined.iter().all(|token| !token.contains("== \"on\"")));
    assert!(combined.iter().all(|token| !token.contains("== \"false\"")));
    assert!(combined.iter().all(|token| !token.contains("== \"0\"")));
    assert!(combined.iter().all(|token| !token.contains("== \"no\"")));
    assert!(combined.iter().all(|token| !token.contains("== \"off\"")));
    assert!(combined.iter().all(|token| !token.contains("--yes-playlist")));
    assert!(combined.iter().all(|token| !token.contains("--no-embed-metadata")));
}

/// Protects rsgain command synthesis so managed runs execute custom mode and
/// pass options before file operands.
#[test]
fn build_tool_command_sets_rsgain_custom_mode_with_trailing_options() {
    let payload = ProvisionedToolPayload {
        tool_id: "mediapm.tools.rsgain+github-releases@latest".to_string(),
        command_selector: "rsgain".to_string(),
        content_entries: BTreeMap::new(),
        identity: ResolvedToolIdentity::default(),
        source_label: "GitHub Releases".to_string(),
        source_identifier: "github-releases".to_string(),
        catalog: tool_catalog_entry("rsgain").expect("catalog entry"),
        warnings: Vec::new(),
    };

    let command = build_tool_command("rsgain", &payload, default_ffmpeg_slot_limits());

    let trailing_index = command
        .iter()
        .position(|token| token == "${*inputs.trailing_args}")
        .expect("rsgain command should include trailing args placeholder");
    let input_index = command
        .iter()
        .position(|token| token == "${inputs.input_content:file(inputs/input.mp3)}")
        .expect("rsgain command should include input content materialization");

    assert!(
        command.iter().any(|token| token == "${*inputs.tagmode ? --tagmode | ''}"),
        "rsgain command should expose explicit tagmode option input"
    );
    assert!(
        trailing_index < input_index,
        "rsgain trailing args should remain before the file operand"
    );
}

/// Protects generated tool-default policy so each managed media tool starts
/// with high-quality/content-preserving argument defaults.
#[test]
fn merge_tool_config_defaults_seeds_curated_media_tool_input_defaults() {
    let yt = merge_tool_config_defaults(
        None,
        "yt-dlp",
        BTreeMap::from([("yt-dlp".to_string(), Hash::from_content(b"yt"))]),
        "yt description".to_string(),
        default_ffmpeg_slot_limits(),
    );
    let ffmpeg = merge_tool_config_defaults(
        None,
        "ffmpeg",
        BTreeMap::from([("ffmpeg".to_string(), Hash::from_content(b"ffmpeg"))]),
        "ffmpeg description".to_string(),
        default_ffmpeg_slot_limits(),
    );
    let rsgain = merge_tool_config_defaults(
        None,
        "rsgain",
        BTreeMap::from([("rsgain".to_string(), Hash::from_content(b"rsgain"))]),
        "rsgain description".to_string(),
        default_ffmpeg_slot_limits(),
    );
    let media_tagger = merge_tool_config_defaults(
        None,
        "media-tagger",
        BTreeMap::from([("media-tagger".to_string(), Hash::from_content(b"media-tagger"))]),
        "media-tagger description".to_string(),
        default_ffmpeg_slot_limits(),
    );

    assert_eq!(
        yt.input_defaults.get("format"),
        Some(&mediapm_conductor::InputBinding::String("best".to_string()))
    );
    assert_eq!(
        yt.input_defaults.get("write_description"),
        Some(&mediapm_conductor::InputBinding::String("true".to_string()))
    );
    assert_eq!(
        yt.input_defaults.get("write_info_json"),
        Some(&mediapm_conductor::InputBinding::String("true".to_string()))
    );
    assert_eq!(
        yt.input_defaults.get("ffmpeg_location"),
        Some(&mediapm_conductor::InputBinding::String("ffmpeg".to_string()))
    );
    assert_eq!(
        ffmpeg.input_defaults.get("audio_quality"),
        Some(&mediapm_conductor::InputBinding::String("2".to_string()))
    );
    assert_eq!(
        rsgain.input_defaults.get("tagmode"),
        Some(&mediapm_conductor::InputBinding::String("i".to_string()))
    );
    assert_eq!(
        media_tagger.input_defaults.get("strict_identification"),
        Some(&mediapm_conductor::InputBinding::String("true".to_string()))
    );
    assert_eq!(
        yt.input_defaults.get("leading_args"),
        Some(&mediapm_conductor::InputBinding::StringList(Vec::new()))
    );
    assert_eq!(
        yt.input_defaults.get("trailing_args"),
        Some(&mediapm_conductor::InputBinding::StringList(Vec::new()))
    );
}

/// Protects media-tagger launcher policy by wiring the current mediapm
/// executable path through a host-specific tool-config environment key.
#[test]
fn build_tool_env_exposes_media_tagger_launcher_binding() {
    let temp = tempfile::tempdir().expect("tempdir");
    let paths = MediaPmPaths::from_root(temp.path());

    let env =
        build_tool_env(&paths, "media-tagger").expect("media-tagger env build should succeed");

    let key = media_tagger_launcher_mediapm_env_var_for_host()
        .expect("host platform should support media-tagger launcher env mapping");
    let value = env.get(key).expect("expected launcher env key to be populated");
    assert!(!value.trim().is_empty());
}

/// Protects strict runtime-storage behavior by preserving explicit legacy
/// values instead of silently rewriting them.
#[test]
fn runtime_storage_normalization_keeps_explicit_legacy_values() {
    let root = tempfile::tempdir().expect("tempdir");
    let paths = MediaPmPaths::from_root(root.path());
    let mut runtime_storage = RuntimeStorageConfig {
        conductor_dir: Some(".conductor".to_string()),
        state_config: Some(".conductor/state.ncl".to_string()),
        cas_store_dir: Some(".conductor/store/".to_string()),
        inherited_env_vars: Some(BTreeMap::new()),
    };

    assert!(!normalize_runtime_storage_defaults(&paths, &mut runtime_storage));
    assert_eq!(runtime_storage.conductor_dir.as_deref(), Some(".conductor"));
    assert_eq!(runtime_storage.state_config.as_deref(), Some(".conductor/state.ncl"));
    assert_eq!(runtime_storage.cas_store_dir.as_deref(), Some(".conductor/store/"));
    assert_eq!(runtime_storage.inherited_env_vars, Some(BTreeMap::new()));
}

/// Protects runtime defaulting by materializing inherited env-name defaults
/// when omitted from runtime storage.
#[test]
fn runtime_storage_normalization_backfills_inherited_env_var_defaults() {
    let root = tempfile::tempdir().expect("tempdir");
    let paths = MediaPmPaths::from_root(root.path());
    let mut runtime_storage = RuntimeStorageConfig {
        conductor_dir: Some(".conductor".to_string()),
        state_config: Some(".conductor/state.ncl".to_string()),
        cas_store_dir: Some(".conductor/store/".to_string()),
        inherited_env_vars: None,
    };

    let changed = normalize_runtime_storage_defaults(&paths, &mut runtime_storage);
    let expected = default_runtime_inherited_env_vars_for_host();

    if expected.is_empty() {
        assert!(!changed);
        assert!(runtime_storage.inherited_env_vars.is_none());
    } else {
        assert!(changed);
        assert_eq!(runtime_storage.inherited_env_vars, Some(expected));
    }
}

/// Protects tool-row binary detection from regressing to content-map-only checks.
#[test]
fn tool_spec_has_binary_reads_executable_path() {
    let binary = std::env::current_exe().expect("current exe");
    let spec = ToolSpec {
        kind: ToolKindSpec::Executable {
            command: vec![binary.to_string_lossy().to_string()],
            env_vars: BTreeMap::new(),
            success_codes: vec![0],
        },
        ..ToolSpec::default()
    };

    assert!(tool_spec_has_binary(&spec));
}
