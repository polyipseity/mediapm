//! Unit tests for managed workflow synthesis and variant binding behavior.

use std::collections::BTreeMap;

use mediapm_cas::Hash;
use mediapm_conductor::{
    InputBinding, MachineNickelDocument, OutputPolicy, OutputSaveMode, ToolConfigSpec,
    ToolKindSpec, ToolSpec, WorkflowSpec, WorkflowStepSpec,
};
use serde_json::{Value, json};

use crate::config::{
    ManagedFileRecord, ManagedWorkflowStepState, MediaPmDocument, MediaPmImpureTimestamp,
    MediaSourceSpec, MediaStep, MediaStepTool, ToolRequirement, ToolRequirementDependencies,
    TransformInputValue,
};
use crate::lockfile::MediaLockFile;

use super::{
    MANAGED_EXTERNAL_DESCRIPTION_PREFIX, MANAGED_WORKFLOW_PREFIX, build_media_workflow_plan,
    build_media_workflow_plan_and_update_state,
    collect_managed_external_data_from_machine_and_lock, resolve_media_variant_output_binding,
    resolve_media_variant_output_binding_with_limits, step_option_input_bindings,
    upsert_managed_external_data,
};

fn generic_output_variant(kind: &str) -> Value {
    json!({ "kind": kind, "save": "full" })
}

fn ffmpeg_output_variant(idx: u32) -> Value {
    json!({ "kind": "primary", "save": "full", "idx": idx })
}

fn ffmpeg_output_variant_with_extension(idx: u32, extension: &str) -> Value {
    json!({
        "kind": "primary",
        "save": "full",
        "idx": idx,
        "extension": extension,
    })
}

fn yt_dlp_output_variant(kind: &str) -> Value {
    json!({ "kind": kind, "save": "full" })
}

fn executable_tool_spec(command: &str) -> ToolSpec {
    ToolSpec {
        kind: ToolKindSpec::Executable {
            command: vec![command.to_string()],
            env_vars: BTreeMap::new(),
            success_codes: vec![0],
        },
        ..ToolSpec::default()
    }
}

fn machine_with_active_tool_specs(lock: &MediaLockFile) -> MachineNickelDocument {
    let mut machine = MachineNickelDocument::default();

    for (logical_name, tool_id) in &lock.active_tools {
        let command = match logical_name.as_str() {
            "yt-dlp" => "yt-dlp",
            "ffmpeg" => "ffmpeg",
            "rsgain" => "rsgain",
            "sd" => "sd",
            "media-tagger" => "media-tagger",
            _ => "tool",
        };

        machine.tools.insert(tool_id.clone(), executable_tool_spec(command));
    }

    machine
}

fn single_step_yt_dlp_source(output_kind: &str) -> MediaSourceSpec {
    MediaSourceSpec {
        id: None,
        description: None,
        title: None,
        workflow_id: None,
        metadata: None,
        variant_hashes: BTreeMap::new(),
        steps: vec![MediaStep {
            tool: MediaStepTool::YtDlp,
            input_variants: Vec::new(),
            output_variants: BTreeMap::from([(
                "default".to_string(),
                yt_dlp_output_variant(output_kind),
            )]),
            options: BTreeMap::from([(
                "uri".to_string(),
                TransformInputValue::String("https://example.com/video".to_string()),
            )]),
        }],
    }
}

/// Protects one-workflow-per-media synthesis and managed id namespace.
#[test]
fn plan_builds_exactly_one_workflow_per_media() {
    let document = MediaPmDocument {
        media: BTreeMap::from([
            (
                "media-a".to_string(),
                MediaSourceSpec {
                    id: None,
                    description: None,
                    title: None,
                    workflow_id: None,
                    metadata: None,
                    variant_hashes: BTreeMap::from([(
                        "default".to_string(),
                        "blake3:0000000000000000000000000000000000000000000000000000000000000000"
                            .to_string(),
                    )]),
                    steps: Vec::new(),
                },
            ),
            (
                "media-b".to_string(),
                MediaSourceSpec {
                    id: None,
                    description: Some("custom media description".to_string()),
                    title: None,
                    workflow_id: Some("custom.workflow.media-b".to_string()),
                    metadata: None,
                    variant_hashes: BTreeMap::from([(
                        "default".to_string(),
                        "blake3:1111111111111111111111111111111111111111111111111111111111111111"
                            .to_string(),
                    )]),
                    steps: Vec::new(),
                },
            ),
        ]),
        ..MediaPmDocument::default()
    };

    let lock = MediaLockFile::default();
    let machine = machine_with_active_tool_specs(&lock);
    let plan = build_media_workflow_plan(&document, &lock, &machine).expect("plan");

    assert_eq!(plan.workflows.len(), 2);
    assert!(plan.workflows.contains_key("mediapm.media.media-a"));
    assert!(plan.workflows.contains_key("custom.workflow.media-b"));
    assert_eq!(
        plan.workflows.get("mediapm.media.media-a").and_then(|workflow| workflow.name.as_deref()),
        Some("media-a")
    );
    assert_eq!(
        plan.workflows.get("custom.workflow.media-b").and_then(|workflow| workflow.name.as_deref()),
        Some("media-b")
    );
    assert_eq!(
        plan.workflows
            .get("custom.workflow.media-b")
            .and_then(|workflow| workflow.description.as_deref()),
        Some("custom media description")
    );
    assert!(plan.external_data.keys().all(|hash| hash.to_string().starts_with("blake3:")));
    assert!(plan.external_data.values().all(|reference| {
        reference
            .description
            .as_deref()
            .is_some_and(|description| description.starts_with(MANAGED_EXTERNAL_DESCRIPTION_PREFIX))
    }));
    assert!(
        plan.external_data
            .values()
            .all(|reference| { reference.save == Some(OutputSaveMode::Saved) })
    );
    assert!(
        plan.workflows.keys().any(|workflow_id| workflow_id.starts_with(MANAGED_WORKFLOW_PREFIX))
    );
}

/// Protects mediapm incremental behavior for folder-style outputs by keeping
/// prior immutable tool ids when explicit step config is unchanged and
/// mediapm step impure timestamp is present.
#[test]
fn unchanged_step_config_with_timestamp_keeps_previous_tool_identity() {
    let old_tool = "mediapm.tools.yt-dlp+github-releases-yt-dlp-yt-dlp@old".to_string();
    let new_tool = "mediapm.tools.yt-dlp+github-releases-yt-dlp-yt-dlp@new".to_string();
    let media_id = "archive-a".to_string();
    let source = single_step_yt_dlp_source("subtitles");
    let explicit_snapshot =
        serde_json::to_value(&source.steps[0]).expect("serialize explicit step config");
    let preserved_timestamp = MediaPmImpureTimestamp { epoch_seconds: 10, subsec_nanos: 20 };

    let document = MediaPmDocument {
        media: BTreeMap::from([(media_id.clone(), source)]),
        ..MediaPmDocument::default()
    };

    let mut lock = MediaLockFile {
        active_tools: BTreeMap::from([("yt-dlp".to_string(), new_tool.clone())]),
        workflow_step_state: BTreeMap::from([(
            media_id.clone(),
            BTreeMap::from([(
                "step-0".to_string(),
                ManagedWorkflowStepState {
                    explicit_config: explicit_snapshot.clone(),
                    impure_timestamp: Some(preserved_timestamp),
                },
            )]),
        )]),
        ..MediaLockFile::default()
    };

    let mut machine = machine_with_active_tool_specs(&lock);
    machine.tools.insert(old_tool.clone(), executable_tool_spec("yt-dlp"));
    machine.workflows.insert(
        format!("{MANAGED_WORKFLOW_PREFIX}{media_id}"),
        WorkflowSpec {
            steps: vec![WorkflowStepSpec {
                id: "0-0-yt-dlp".to_string(),
                tool: old_tool.clone(),
                inputs: BTreeMap::new(),
                depends_on: Vec::new(),
                outputs: BTreeMap::from([(
                    "yt_dlp_subtitle_artifacts".to_string(),
                    OutputPolicy { save: None },
                )]),
            }],
            ..WorkflowSpec::default()
        },
    );

    let plan = build_media_workflow_plan_and_update_state(&document, &mut lock, &machine)
        .expect("plan should succeed");
    let workflow =
        plan.workflows.get(&format!("{MANAGED_WORKFLOW_PREFIX}{media_id}")).expect("workflow");

    assert_eq!(workflow.steps.len(), 1);
    assert_eq!(workflow.steps[0].tool, old_tool);

    let stored = lock
        .workflow_step_state
        .get(&media_id)
        .and_then(|steps| steps.get("step-0"))
        .expect("stored step refresh state");
    assert_eq!(stored.explicit_config, explicit_snapshot);
    assert_eq!(stored.impure_timestamp, Some(preserved_timestamp));
}

/// Protects refresh gating by forcing refresh when explicit user-facing step
/// config changes.
#[test]
fn changed_step_config_forces_refresh_to_active_tool() {
    let old_tool = "mediapm.tools.yt-dlp+github-releases-yt-dlp-yt-dlp@old".to_string();
    let new_tool = "mediapm.tools.yt-dlp+github-releases-yt-dlp-yt-dlp@new".to_string();
    let media_id = "refresh-on-config-change".to_string();

    let old_source = single_step_yt_dlp_source("subtitles");
    let old_snapshot =
        serde_json::to_value(&old_source.steps[0]).expect("serialize old explicit step config");
    let new_source = single_step_yt_dlp_source("primary");
    let new_snapshot =
        serde_json::to_value(&new_source.steps[0]).expect("serialize new explicit step config");
    assert_ne!(old_snapshot, new_snapshot);

    let document = MediaPmDocument {
        media: BTreeMap::from([(media_id.clone(), new_source)]),
        ..MediaPmDocument::default()
    };

    let mut lock = MediaLockFile {
        active_tools: BTreeMap::from([("yt-dlp".to_string(), new_tool.clone())]),
        workflow_step_state: BTreeMap::from([(
            media_id.clone(),
            BTreeMap::from([(
                "step-0".to_string(),
                ManagedWorkflowStepState {
                    explicit_config: old_snapshot,
                    impure_timestamp: Some(MediaPmImpureTimestamp {
                        epoch_seconds: 1,
                        subsec_nanos: 2,
                    }),
                },
            )]),
        )]),
        ..MediaLockFile::default()
    };

    let mut machine = machine_with_active_tool_specs(&lock);
    machine.tools.insert(old_tool.clone(), executable_tool_spec("yt-dlp"));
    machine.workflows.insert(
        format!("{MANAGED_WORKFLOW_PREFIX}{media_id}"),
        WorkflowSpec {
            steps: vec![WorkflowStepSpec {
                id: "0-0-yt-dlp".to_string(),
                tool: old_tool,
                inputs: BTreeMap::new(),
                depends_on: Vec::new(),
                outputs: BTreeMap::from([("primary".to_string(), OutputPolicy { save: None })]),
            }],
            ..WorkflowSpec::default()
        },
    );

    let plan = build_media_workflow_plan_and_update_state(&document, &mut lock, &machine)
        .expect("plan should succeed");
    let workflow =
        plan.workflows.get(&format!("{MANAGED_WORKFLOW_PREFIX}{media_id}")).expect("workflow");

    assert_eq!(workflow.steps.len(), 1);
    assert_eq!(workflow.steps[0].tool, new_tool);

    let stored = lock
        .workflow_step_state
        .get(&media_id)
        .and_then(|steps| steps.get("step-0"))
        .expect("stored step refresh state");
    assert_eq!(stored.explicit_config, new_snapshot);
    assert!(stored.impure_timestamp.is_some());
    assert_ne!(
        stored.impure_timestamp,
        Some(MediaPmImpureTimestamp { epoch_seconds: 1, subsec_nanos: 2 })
    );
}

/// Protects refresh gating by forcing refresh when mediapm step impure
/// timestamp is missing even when explicit step config is unchanged.
#[test]
fn missing_step_timestamp_forces_refresh_to_active_tool() {
    let old_tool = "mediapm.tools.yt-dlp+github-releases-yt-dlp-yt-dlp@old".to_string();
    let new_tool = "mediapm.tools.yt-dlp+github-releases-yt-dlp-yt-dlp@new".to_string();
    let media_id = "refresh-on-missing-timestamp".to_string();

    let source = single_step_yt_dlp_source("subtitles");
    let explicit_snapshot =
        serde_json::to_value(&source.steps[0]).expect("serialize explicit step config");
    let document = MediaPmDocument {
        media: BTreeMap::from([(media_id.clone(), source)]),
        ..MediaPmDocument::default()
    };

    let mut lock = MediaLockFile {
        active_tools: BTreeMap::from([("yt-dlp".to_string(), new_tool.clone())]),
        workflow_step_state: BTreeMap::from([(
            media_id.clone(),
            BTreeMap::from([(
                "step-0".to_string(),
                ManagedWorkflowStepState {
                    explicit_config: explicit_snapshot.clone(),
                    impure_timestamp: None,
                },
            )]),
        )]),
        ..MediaLockFile::default()
    };

    let mut machine = machine_with_active_tool_specs(&lock);
    machine.tools.insert(old_tool.clone(), executable_tool_spec("yt-dlp"));
    machine.workflows.insert(
        format!("{MANAGED_WORKFLOW_PREFIX}{media_id}"),
        WorkflowSpec {
            steps: vec![WorkflowStepSpec {
                id: "0-0-yt-dlp".to_string(),
                tool: old_tool,
                inputs: BTreeMap::new(),
                depends_on: Vec::new(),
                outputs: BTreeMap::from([(
                    "yt_dlp_subtitle_artifacts".to_string(),
                    OutputPolicy { save: None },
                )]),
            }],
            ..WorkflowSpec::default()
        },
    );

    let plan = build_media_workflow_plan_and_update_state(&document, &mut lock, &machine)
        .expect("plan should succeed");
    let workflow =
        plan.workflows.get(&format!("{MANAGED_WORKFLOW_PREFIX}{media_id}")).expect("workflow");

    assert_eq!(workflow.steps.len(), 1);
    assert_eq!(workflow.steps[0].tool, new_tool);

    let stored = lock
        .workflow_step_state
        .get(&media_id)
        .and_then(|steps| steps.get("step-0"))
        .expect("stored step refresh state");
    assert_eq!(stored.explicit_config, explicit_snapshot);
    assert!(stored.impure_timestamp.is_some());
}

/// Protects managed external-data dedupe by merging overlapping hash policies
/// so `full` dominates `saved` when the same hash is rooted from multiple
/// managed sources.
#[test]
fn managed_external_data_dedupe_merges_save_policy_to_full() {
    let hash = Hash::from_content(b"shared-external-hash");
    let mut external_data = BTreeMap::new();

    upsert_managed_external_data(
        &mut external_data,
        hash,
        "managed external data: tool content 'demo-tool' path 'windows/tool.exe'".to_string(),
        OutputSaveMode::Saved,
    );
    upsert_managed_external_data(
        &mut external_data,
        hash,
        "managed external data: materialized output 'library/demo.bin' (media 'demo', variant 'video')".to_string(),
        OutputSaveMode::Full,
    );

    let reference = external_data.get(&hash).expect("merged external-data row");
    assert_eq!(reference.save, Some(OutputSaveMode::Full));
}

/// Protects managed-state persistence by rooting managed file CAS hashes in
/// machine external-data with minimum `save = "full"`.
#[test]
fn managed_external_data_collection_roots_lock_managed_file_hashes() {
    let hash = Hash::from_content(b"managed-file-hash");
    let lock = MediaLockFile {
        managed_files: BTreeMap::from([(
            "music videos/demo.mkv".to_string(),
            ManagedFileRecord {
                media_id: "demo-media".to_string(),
                variant: "video_tagged".to_string(),
                hash: hash.to_string(),
                last_synced_unix_millis: 1,
            },
        )]),
        ..MediaLockFile::default()
    };
    let machine = MachineNickelDocument::default();
    let mut external_data = BTreeMap::new();

    collect_managed_external_data_from_machine_and_lock(&machine, &lock, &mut external_data)
        .expect("managed external-data collection should succeed");

    let reference = external_data.get(&hash).expect("managed-file hash should be rooted");
    assert_eq!(reference.save, Some(OutputSaveMode::Full));
    assert!(reference.description.as_deref().is_some_and(|description| {
        description.contains("materialized output 'music videos/demo.mkv'")
            && description.contains("media 'demo-media'")
            && description.contains("variant 'video_tagged'")
    }));
}

/// Protects hash dedupe by escalating shared tool-content/managed-file roots
/// to full-save persistence.
#[test]
fn managed_external_data_collection_escalates_shared_hash_to_full() {
    let shared_hash = Hash::from_content(b"shared-tool-and-managed-file");
    let machine = MachineNickelDocument {
        tool_configs: BTreeMap::from([(
            "mediapm.tools.demo@latest".to_string(),
            ToolConfigSpec {
                content_map: Some(BTreeMap::from([("windows/demo.exe".to_string(), shared_hash)])),
                ..ToolConfigSpec::default()
            },
        )]),
        ..MachineNickelDocument::default()
    };
    let lock = MediaLockFile {
        managed_files: BTreeMap::from([(
            "sidecars/demo.info.json".to_string(),
            ManagedFileRecord {
                media_id: "demo-media".to_string(),
                variant: "infojson".to_string(),
                hash: shared_hash.to_string(),
                last_synced_unix_millis: 1,
            },
        )]),
        ..MediaLockFile::default()
    };
    let mut external_data = BTreeMap::new();

    collect_managed_external_data_from_machine_and_lock(&machine, &lock, &mut external_data)
        .expect("managed external-data collection should dedupe shared hash");

    assert_eq!(external_data.len(), 1);
    let reference = external_data.get(&shared_hash).expect("shared hash row should exist");
    assert_eq!(reference.save, Some(OutputSaveMode::Full));
}

/// Protects dependency synthesis for ordered variant-flow step chains.
#[test]
#[expect(
    clippy::too_many_lines,
    reason = "this regression keeps full variant-flow dependency assertions together for readability"
)]
fn variant_flow_creates_explicit_step_dependencies() {
    let document = MediaPmDocument {
        media: BTreeMap::from([(
            "remote-a".to_string(),
            MediaSourceSpec {
                id: None,
                description: None,
                title: None,
                workflow_id: None,
                metadata: None,
                variant_hashes: BTreeMap::new(),
                steps: vec![
                    MediaStep {
                        tool: MediaStepTool::YtDlp,
                        input_variants: Vec::new(),
                        output_variants: BTreeMap::from([(
                            "default".to_string(),
                            yt_dlp_output_variant("primary"),
                        )]),
                        options: BTreeMap::from([(
                            "uri".to_string(),
                            TransformInputValue::String("https://example.com/video".to_string()),
                        )]),
                    },
                    MediaStep {
                        tool: MediaStepTool::Ffmpeg,
                        input_variants: vec!["default".to_string()],
                        output_variants: BTreeMap::from([(
                            "aac".to_string(),
                            ffmpeg_output_variant(0),
                        )]),
                        options: BTreeMap::new(),
                    },
                    MediaStep {
                        tool: MediaStepTool::Rsgain,
                        input_variants: vec!["aac".to_string()],
                        output_variants: BTreeMap::from([(
                            "aac".to_string(),
                            generic_output_variant("primary"),
                        )]),
                        options: BTreeMap::from([(
                            "target_lufs".to_string(),
                            TransformInputValue::String("-14".to_string()),
                        )]),
                    },
                ],
            },
        )]),
        ..MediaPmDocument::default()
    };

    let lock = MediaLockFile {
        active_tools: BTreeMap::from([
            (
                "yt-dlp".to_string(),
                "mediapm.tools.yt-dlp+github-releases-yt-dlp-yt-dlp@latest".to_string(),
            ),
            (
                "ffmpeg".to_string(),
                "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@latest".to_string(),
            ),
            (
                "rsgain".to_string(),
                "mediapm.tools.rsgain+github-releases-complexlogic-rsgain@latest".to_string(),
            ),
            ("sd".to_string(), "mediapm.tools.sd+conductor-common@latest".to_string()),
        ]),
        ..MediaLockFile::default()
    };
    let machine = machine_with_active_tool_specs(&lock);

    let plan = build_media_workflow_plan(&document, &lock, &machine).expect("plan");
    let workflow = plan.workflows.get("mediapm.media.remote-a").expect("managed workflow");

    assert_eq!(workflow.steps.len(), 8);
    let download = &workflow.steps[0];
    let ffmpeg = &workflow.steps[1];
    let rsgain_extract = &workflow.steps[2];
    let rsgain = &workflow.steps[3];
    let metadata_export = &workflow.steps[4];
    let replaygain_metadata_rewrite = &workflow.steps[5];
    let r128_metadata_rewrite = &workflow.steps[6];
    let apply = &workflow.steps[7];

    assert!(download.depends_on.is_empty());
    assert_eq!(ffmpeg.depends_on, vec![download.id.clone()]);
    assert_eq!(rsgain_extract.depends_on, vec![ffmpeg.id.clone()]);
    assert_eq!(rsgain.depends_on, vec![rsgain_extract.id.clone()]);
    assert_eq!(metadata_export.depends_on, vec![rsgain.id.clone()]);
    assert_eq!(replaygain_metadata_rewrite.depends_on, vec![metadata_export.id.clone()]);
    assert_eq!(r128_metadata_rewrite.depends_on, vec![replaygain_metadata_rewrite.id.clone()]);
    assert!(apply.depends_on.contains(&r128_metadata_rewrite.id));
    assert!(apply.depends_on.contains(&ffmpeg.id));
    assert_eq!(
        replaygain_metadata_rewrite.inputs.get("pattern"),
        Some(&InputBinding::String("(?i)REPLAYGAIN_".to_string()))
    );
    assert_eq!(
        replaygain_metadata_rewrite.inputs.get("replacement"),
        Some(&InputBinding::String("replaygain_".to_string()))
    );
    assert_eq!(
        r128_metadata_rewrite.inputs.get("pattern"),
        Some(&InputBinding::String("(?i)R128_".to_string()))
    );
    assert_eq!(
        r128_metadata_rewrite.inputs.get("replacement"),
        Some(&InputBinding::String("R128_".to_string()))
    );
    assert_eq!(rsgain.inputs.get("album"), None);
    assert_eq!(rsgain.inputs.get("album_mode"), None);
    assert_eq!(rsgain.inputs.get("map_chapters"), None);
}

/// Protects expanded step-id numbering so each conductor step emitted from one
/// mediapm step gets a unique `<mediapm_step>-<expanded_step>` prefix.
#[test]
fn expanded_step_ids_increment_within_each_mediapm_step() {
    let document = MediaPmDocument {
        media: BTreeMap::from([(
            "expanded-id-order".to_string(),
            MediaSourceSpec {
                id: None,
                description: None,
                title: None,
                workflow_id: None,
                metadata: None,
                variant_hashes: BTreeMap::from([(
                    "default".to_string(),
                    "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        .to_string(),
                )]),
                steps: vec![
                    MediaStep {
                        tool: MediaStepTool::MediaTagger,
                        input_variants: vec!["default".to_string()],
                        output_variants: BTreeMap::from([(
                            "tagged".to_string(),
                            generic_output_variant("primary"),
                        )]),
                        options: BTreeMap::new(),
                    },
                    MediaStep {
                        tool: MediaStepTool::Rsgain,
                        input_variants: vec!["tagged".to_string()],
                        output_variants: BTreeMap::from([(
                            "normalized".to_string(),
                            generic_output_variant("primary"),
                        )]),
                        options: BTreeMap::new(),
                    },
                ],
            },
        )]),
        ..MediaPmDocument::default()
    };

    let lock = MediaLockFile {
        active_tools: BTreeMap::from([
            (
                "media-tagger".to_string(),
                "mediapm.tools.media-tagger+mediapm-internal@latest".to_string(),
            ),
            (
                "ffmpeg".to_string(),
                "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@latest".to_string(),
            ),
            (
                "rsgain".to_string(),
                "mediapm.tools.rsgain+github-releases-complexlogic-rsgain@latest".to_string(),
            ),
            ("sd".to_string(), "mediapm.tools.sd+conductor-common@latest".to_string()),
        ]),
        ..MediaLockFile::default()
    };

    let machine = machine_with_active_tool_specs(&lock);
    let plan = build_media_workflow_plan(&document, &lock, &machine).expect("plan");
    let workflow = plan.workflows.get("mediapm.media.expanded-id-order").expect("managed workflow");

    let step_prefixes = workflow
        .steps
        .iter()
        .map(|step| {
            let mut parts = step.id.splitn(3, '-');
            let first = parts.next().expect("first step-id segment");
            let second = parts.next().expect("second step-id segment");
            format!("{first}-{second}")
        })
        .collect::<Vec<_>>();

    assert_eq!(step_prefixes, vec!["0-0", "0-1", "1-0", "1-1", "1-2", "1-3", "1-4", "1-5"],);
}

/// Protects media-tagger synthesis expansion into metadata-fetch and
/// ffmpeg-apply step pair with deterministic dependency wiring.
#[test]
#[expect(
    clippy::too_many_lines,
    reason = "this item intentionally keeps end-to-end control flow together so ordering invariants remain explicit during maintenance"
)]
fn media_tagger_step_expands_to_metadata_and_apply_steps() {
    let document = MediaPmDocument {
        media: BTreeMap::from([(
            "tag-a".to_string(),
            MediaSourceSpec {
                id: None,
                description: None,
                title: None,
                workflow_id: None,
                metadata: None,
                variant_hashes: BTreeMap::from([(
                    "default".to_string(),
                    "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        .to_string(),
                )]),
                steps: vec![MediaStep {
                    tool: MediaStepTool::MediaTagger,
                    input_variants: vec!["default".to_string()],
                    output_variants: BTreeMap::from([(
                        "tagged".to_string(),
                        generic_output_variant("primary"),
                    )]),
                    options: BTreeMap::from([
                        (
                            "strict_identification".to_string(),
                            TransformInputValue::String("false".to_string()),
                        ),
                        (
                            "output_container".to_string(),
                            TransformInputValue::String("mp4".to_string()),
                        ),
                    ]),
                }],
            },
        )]),
        tools: BTreeMap::from([(
            "media-tagger".to_string(),
            ToolRequirement {
                version: None,
                tag: Some("latest".to_string()),
                dependencies: ToolRequirementDependencies {
                    ffmpeg_version: Some("inherit".to_string()),
                    sd_version: None,
                },
                recheck_seconds: None,
                max_input_slots: None,
                max_output_slots: None,
            },
        )]),
        ..MediaPmDocument::default()
    };

    let lock = MediaLockFile {
        active_tools: BTreeMap::from([
            (
                "media-tagger".to_string(),
                "mediapm.tools.media-tagger+mediapm-internal@latest".to_string(),
            ),
            (
                "ffmpeg".to_string(),
                "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@latest".to_string(),
            ),
        ]),
        ..MediaLockFile::default()
    };
    let mut machine = machine_with_active_tool_specs(&lock);
    machine.tools.insert(
        "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@latest".to_string(),
        ToolSpec {
            kind: ToolKindSpec::Executable {
                command: vec!["${context.os == \"windows\" ? windows/ffmpeg.exe | ''}${context.os == \"linux\" ? linux/ffmpeg | ''}${context.os == \"macos\" ? macos/ffmpeg | ''}".to_string()],
                env_vars: BTreeMap::new(),
                success_codes: vec![0],
            },
            ..ToolSpec::default()
        },
    );

    let plan = build_media_workflow_plan(&document, &lock, &machine).expect("plan");
    let workflow = plan.workflows.get("mediapm.media.tag-a").expect("managed workflow");

    assert_eq!(workflow.steps.len(), 2);

    let metadata = &workflow.steps[0];
    let apply = &workflow.steps[1];

    assert_eq!(metadata.tool, "mediapm.tools.media-tagger+mediapm-internal@latest");
    assert_eq!(metadata.outputs.get("content"), Some(&OutputPolicy { save: None }));
    assert_eq!(metadata.outputs.get("sandbox_artifacts"), Some(&OutputPolicy { save: None }));
    assert!(!metadata.inputs.contains_key("ffmpeg_version"));
    assert!(!metadata.inputs.contains_key("output_container"));

    assert_eq!(apply.tool, "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@latest");
    assert!(apply.depends_on.contains(&metadata.id));
    assert_eq!(
        apply.inputs.get("ffmetadata_content"),
        Some(&InputBinding::String(format!("${{step_output.{}.content}}", metadata.id)))
    );
    assert_eq!(
        apply.inputs.get("input_content_1"),
        Some(&InputBinding::String(format!(
            "${{step_output.{}.sandbox_artifacts:zip(coverart-slot-1.bin)}}",
            metadata.id
        )))
    );
    assert_eq!(
        apply.inputs.get("cover_art_slot_enabled_1"),
        Some(&InputBinding::String(format!(
            "${{step_output.{}.sandbox_artifacts:zip(coverart-slot-1.flag)}}",
            metadata.id
        )))
    );
    assert_eq!(apply.inputs.get("map_chapters"), None);
    assert_eq!(
        apply.inputs.get("trailing_args"),
        Some(&InputBinding::StringList(vec!["-map".to_string(), "0".to_string()]))
    );
    assert_eq!(apply.inputs.get("container"), Some(&InputBinding::String("mp4".to_string())));
    assert_eq!(
        apply.outputs.get("primary"),
        Some(&OutputPolicy { save: Some(OutputSaveMode::Full) })
    );

    let binding = resolve_media_variant_output_binding(
        document.media.get("tag-a").expect("tag-a source"),
        "tagged",
    )
    .expect("resolve tagged binding")
    .expect("tagged binding should exist");
    assert_eq!(binding.step_id, apply.id);
    assert_eq!(binding.output_name, "primary");
}

/// Protects media-tagger apply synthesis by preserving a supported upstream
/// extension when the output variant does not override it.
#[test]
fn media_tagger_apply_preserves_upstream_supported_extension_by_default() {
    let document = MediaPmDocument {
        media: BTreeMap::from([(
            "tag-preserve-ext".to_string(),
            MediaSourceSpec {
                id: None,
                description: None,
                title: None,
                workflow_id: None,
                metadata: None,
                variant_hashes: BTreeMap::from([(
                    "default".to_string(),
                    "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        .to_string(),
                )]),
                steps: vec![
                    MediaStep {
                        tool: MediaStepTool::Ffmpeg,
                        input_variants: vec!["default".to_string()],
                        output_variants: BTreeMap::from([(
                            "audio_m4a".to_string(),
                            ffmpeg_output_variant_with_extension(0, "m4a"),
                        )]),
                        options: BTreeMap::new(),
                    },
                    MediaStep {
                        tool: MediaStepTool::MediaTagger,
                        input_variants: vec!["audio_m4a".to_string()],
                        output_variants: BTreeMap::from([(
                            "tagged".to_string(),
                            generic_output_variant("primary"),
                        )]),
                        options: BTreeMap::new(),
                    },
                ],
            },
        )]),
        ..MediaPmDocument::default()
    };

    let lock = MediaLockFile {
        active_tools: BTreeMap::from([
            (
                "media-tagger".to_string(),
                "mediapm.tools.media-tagger+mediapm-internal@latest".to_string(),
            ),
            (
                "ffmpeg".to_string(),
                "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@latest".to_string(),
            ),
        ]),
        ..MediaLockFile::default()
    };

    let machine = machine_with_active_tool_specs(&lock);
    let plan = build_media_workflow_plan(&document, &lock, &machine).expect("plan");
    let workflow = plan.workflows.get("mediapm.media.tag-preserve-ext").expect("managed workflow");
    let apply = workflow.steps.last().expect("media-tagger apply step");

    assert_eq!(
        apply.inputs.get("output_path_0"),
        Some(&InputBinding::String("output-0.m4a".to_string()))
    );
}

/// Protects `tools.media-tagger.dependencies.ffmpeg_version = "inherit"`
/// behavior by
/// requiring an active logical ffmpeg tool in lock state.
#[test]
fn media_tagger_inherit_ffmpeg_version_requires_active_ffmpeg_tool() {
    let document = MediaPmDocument {
        media: BTreeMap::from([(
            "tag-inherit-missing-ffmpeg".to_string(),
            MediaSourceSpec {
                id: None,
                description: None,
                title: None,
                workflow_id: None,
                metadata: None,
                variant_hashes: BTreeMap::from([(
                    "default".to_string(),
                    "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        .to_string(),
                )]),
                steps: vec![MediaStep {
                    tool: MediaStepTool::MediaTagger,
                    input_variants: vec!["default".to_string()],
                    output_variants: BTreeMap::from([(
                        "tagged".to_string(),
                        generic_output_variant("primary"),
                    )]),
                    options: BTreeMap::from([(
                        "recording_mbid".to_string(),
                        TransformInputValue::String(
                            "8f3471b5-7e6a-48da-86a9-c1c07a0f47ae".to_string(),
                        ),
                    )]),
                }],
            },
        )]),
        tools: BTreeMap::from([(
            "media-tagger".to_string(),
            ToolRequirement {
                version: None,
                tag: Some("latest".to_string()),
                dependencies: ToolRequirementDependencies {
                    ffmpeg_version: Some("inherit".to_string()),
                    sd_version: None,
                },
                recheck_seconds: None,
                max_input_slots: None,
                max_output_slots: None,
            },
        )]),
        ..MediaPmDocument::default()
    };

    let lock = MediaLockFile {
        active_tools: BTreeMap::from([(
            "media-tagger".to_string(),
            "mediapm.tools.media-tagger+mediapm-internal@latest".to_string(),
        )]),
        ..MediaLockFile::default()
    };

    let machine = machine_with_active_tool_specs(&lock);
    let error = build_media_workflow_plan(&document, &lock, &machine)
        .expect_err("inherit mode should fail when active ffmpeg tool is missing");
    let text = error.to_string();
    assert!(
        text.contains("tools.media-tagger.dependencies.ffmpeg_version='inherit'"),
        "unexpected error: {text}"
    );
}

/// Protects metadata-preserving media-tagger behavior by always forwarding
/// source input into metadata fetch stages, even when MBID identity is
/// explicitly provided.
#[test]
fn media_tagger_metadata_step_keeps_input_when_recording_mbid_is_set() {
    let document = MediaPmDocument {
        media: BTreeMap::from([(
            "tag-no-input".to_string(),
            MediaSourceSpec {
                id: None,
                description: None,
                title: None,
                workflow_id: None,
                metadata: None,
                variant_hashes: BTreeMap::from([(
                    "default".to_string(),
                    "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        .to_string(),
                )]),
                steps: vec![MediaStep {
                    tool: MediaStepTool::MediaTagger,
                    input_variants: vec!["default".to_string()],
                    output_variants: BTreeMap::from([(
                        "tagged".to_string(),
                        generic_output_variant("primary"),
                    )]),
                    options: BTreeMap::from([(
                        "recording_mbid".to_string(),
                        TransformInputValue::String(
                            "f4ec5f46-5f50-4f95-9f8d-2df2ec2fd2bc".to_string(),
                        ),
                    )]),
                }],
            },
        )]),
        ..MediaPmDocument::default()
    };

    let lock = MediaLockFile {
        active_tools: BTreeMap::from([
            (
                "media-tagger".to_string(),
                "mediapm.tools.media-tagger+mediapm-internal@latest".to_string(),
            ),
            (
                "ffmpeg".to_string(),
                "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@latest".to_string(),
            ),
        ]),
        ..MediaLockFile::default()
    };
    let machine = machine_with_active_tool_specs(&lock);

    let plan = build_media_workflow_plan(&document, &lock, &machine).expect("plan");
    let workflow = plan.workflows.get("mediapm.media.tag-no-input").expect("managed workflow");
    let metadata_step =
        workflow.steps.iter().find(|step| step.id.ends_with("-metadata")).expect("metadata step");

    let input_content = metadata_step
        .inputs
        .get("input_content")
        .and_then(|binding| match binding {
            InputBinding::String(value) => Some(value),
            InputBinding::StringList(_) => None,
        })
        .expect("input_content scalar binding");
    assert!(
        input_content.starts_with("${external_data.blake3:"),
        "expected metadata step to keep upstream content binding"
    );
    assert!(metadata_step.depends_on.is_empty());
    assert_eq!(
        metadata_step.inputs.get("recording_mbid"),
        Some(&InputBinding::String("f4ec5f46-5f50-4f95-9f8d-2df2ec2fd2bc".to_string()))
    );
    assert_eq!(
        metadata_step.inputs.get("strict_identification"),
        None,
        "media-tagger workflow inputs should omit strict_identification when callers rely on managed input defaults"
    );
}

/// Protects local import-step synthesis using builtin import output wiring.
#[test]
fn import_step_synthesizes_builtin_import_binding() {
    let document = MediaPmDocument {
        media: BTreeMap::from([(
            "local-a".to_string(),
            MediaSourceSpec {
                id: None,
                description: None,
                title: None,
                workflow_id: None,
                metadata: None,
                variant_hashes: BTreeMap::new(),
                steps: vec![MediaStep {
                    tool: MediaStepTool::Import,
                    input_variants: Vec::new(),
                    output_variants: BTreeMap::from([(
                        "source".to_string(),
                        generic_output_variant("primary"),
                    )]),
                    options: BTreeMap::from([
                        (
                            "kind".to_string(),
                            TransformInputValue::String("cas_hash".to_string()),
                        ),
                        (
                            "hash".to_string(),
                            TransformInputValue::String(
                                "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                                    .to_string(),
                            ),
                        ),
                    ]),
                }],
            },
        )]),
        ..MediaPmDocument::default()
    };

    let lock = MediaLockFile::default();
    let mut machine = MachineNickelDocument::default();
    machine.tools.insert(
        "import@1.0.0".to_string(),
        ToolSpec {
            kind: ToolKindSpec::Builtin {
                name: "import".to_string(),
                version: "1.0.0".to_string(),
            },
            ..ToolSpec::default()
        },
    );

    let plan = build_media_workflow_plan(&document, &lock, &machine).expect("plan");
    let workflow = plan.workflows.get("mediapm.media.local-a").expect("managed workflow");
    assert_eq!(workflow.steps.len(), 1);

    let step = &workflow.steps[0];
    assert_eq!(step.tool, "import@1.0.0");
    assert!(step.depends_on.is_empty());
    assert_eq!(step.inputs.get("kind"), Some(&InputBinding::String("cas_hash".to_string())));
    assert_eq!(
        step.outputs.get("primary").and_then(|policy| policy.save),
        Some(OutputSaveMode::Full),
    );
}

/// Protects per-variant output policy mapping from mediapm schema into
/// generated conductor workflow-step output overrides.
#[test]
fn step_output_variant_policy_maps_to_workflow_output_policy() {
    let document = MediaPmDocument {
        media: BTreeMap::from([(
            "policy-a".to_string(),
            MediaSourceSpec {
                id: None,
                description: None,
                title: None,
                workflow_id: None,
                metadata: None,
                variant_hashes: BTreeMap::from([(
                    "source".to_string(),
                    "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        .to_string(),
                )]),
                steps: vec![MediaStep {
                    tool: MediaStepTool::Ffmpeg,
                    input_variants: vec!["source".to_string()],
                    output_variants: BTreeMap::from([(
                        "normalized".to_string(),
                        json!({ "kind": "primary", "save": "full", "idx": 0 }),
                    )]),
                    options: BTreeMap::new(),
                }],
            },
        )]),
        ..MediaPmDocument::default()
    };

    let lock = MediaLockFile {
        active_tools: BTreeMap::from([(
            "ffmpeg".to_string(),
            "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@latest".to_string(),
        )]),
        ..MediaLockFile::default()
    };
    let machine = machine_with_active_tool_specs(&lock);

    let plan = build_media_workflow_plan(&document, &lock, &machine).expect("plan");
    let workflow = plan.workflows.get("mediapm.media.policy-a").expect("managed workflow");
    let step = workflow.steps.first().expect("workflow step");

    assert_eq!(
        step.outputs.get("primary"),
        Some(&OutputPolicy { save: Some(OutputSaveMode::Full) }),
    );
}

/// Protects ffmpeg per-variant extension wiring by mapping output
/// extension config into generated `output_path_<idx>` bindings.
#[test]
fn ffmpeg_output_variant_extension_updates_output_path_binding() {
    let document = MediaPmDocument {
        media: BTreeMap::from([(
            "ffmpeg-extension".to_string(),
            MediaSourceSpec {
                id: None,
                description: None,
                title: None,
                workflow_id: None,
                metadata: None,
                variant_hashes: BTreeMap::from([(
                    "source".to_string(),
                    "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        .to_string(),
                )]),
                steps: vec![MediaStep {
                    tool: MediaStepTool::Ffmpeg,
                    input_variants: vec!["source".to_string()],
                    output_variants: BTreeMap::from([(
                        "normalized".to_string(),
                        ffmpeg_output_variant_with_extension(0, "webm"),
                    )]),
                    options: BTreeMap::new(),
                }],
            },
        )]),
        ..MediaPmDocument::default()
    };

    let lock = MediaLockFile {
        active_tools: BTreeMap::from([(
            "ffmpeg".to_string(),
            "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@latest".to_string(),
        )]),
        ..MediaLockFile::default()
    };

    let machine = machine_with_active_tool_specs(&lock);
    let plan = build_media_workflow_plan(&document, &lock, &machine).expect("plan");
    let workflow = plan.workflows.get("mediapm.media.ffmpeg-extension").expect("workflow");
    let step = workflow.steps.first().expect("workflow step");

    assert_eq!(
        step.inputs.get("output_path_0"),
        Some(&InputBinding::String("output-0.webm".to_string()))
    );
}

/// Protects yt-dlp artifact variants by mapping non-primary outputs to
/// artifact-bundle capture outputs instead of `content`.
#[test]
fn yt_dlp_artifact_variant_maps_output_policy_to_artifact_capture() {
    let document = MediaPmDocument {
        media: BTreeMap::from([(
            "policy-ytdlp".to_string(),
            MediaSourceSpec {
                id: None,
                description: None,
                title: None,
                workflow_id: None,
                metadata: None,
                variant_hashes: BTreeMap::new(),
                steps: vec![MediaStep {
                    tool: MediaStepTool::YtDlp,
                    input_variants: Vec::new(),
                    output_variants: BTreeMap::from([(
                        "subtitles".to_string(),
                        json!({
                            "kind": "subtitles",
                            "save": true
                        }),
                    )]),
                    options: BTreeMap::from([(
                        "uri".to_string(),
                        TransformInputValue::String("https://example.com/video".to_string()),
                    )]),
                }],
            },
        )]),
        ..MediaPmDocument::default()
    };

    let lock = MediaLockFile {
        active_tools: BTreeMap::from([(
            "yt-dlp".to_string(),
            "mediapm.tools.yt-dlp+github-releases-yt-dlp-yt-dlp@latest".to_string(),
        )]),
        ..MediaLockFile::default()
    };
    let machine = machine_with_active_tool_specs(&lock);

    let plan = build_media_workflow_plan(&document, &lock, &machine).expect("plan");
    let workflow = plan.workflows.get("mediapm.media.policy-ytdlp").expect("managed workflow");
    let step = workflow.steps.first().expect("workflow step");

    assert_eq!(step.outputs.get("yt_dlp_subtitle_artifacts"), Some(&OutputPolicy { save: None }),);
    assert!(!step.outputs.contains_key("content"));
}

/// Protects sidecar capture routing by forcing an explicit output key even
/// when per-variant save/force overrides are omitted.
#[test]
fn yt_dlp_sidecar_variant_without_policy_still_emits_artifact_output_key() {
    let document = MediaPmDocument {
        media: BTreeMap::from([(
            "policy-ytdlp-default-sidecar".to_string(),
            MediaSourceSpec {
                id: None,
                description: None,
                title: None,
                workflow_id: None,
                metadata: None,
                variant_hashes: BTreeMap::new(),
                steps: vec![MediaStep {
                    tool: MediaStepTool::YtDlp,
                    input_variants: Vec::new(),
                    output_variants: BTreeMap::from([(
                        "thumbnail".to_string(),
                        yt_dlp_output_variant("thumbnails"),
                    )]),
                    options: BTreeMap::from([(
                        "uri".to_string(),
                        TransformInputValue::String("https://example.com/video".to_string()),
                    )]),
                }],
            },
        )]),
        ..MediaPmDocument::default()
    };

    let lock = MediaLockFile {
        active_tools: BTreeMap::from([(
            "yt-dlp".to_string(),
            "mediapm.tools.yt-dlp+github-releases-yt-dlp-yt-dlp@latest".to_string(),
        )]),
        ..MediaLockFile::default()
    };
    let machine = machine_with_active_tool_specs(&lock);

    let plan = build_media_workflow_plan(&document, &lock, &machine).expect("plan");
    let workflow =
        plan.workflows.get("mediapm.media.policy-ytdlp-default-sidecar").expect("managed workflow");
    let step = workflow.steps.first().expect("workflow step");

    assert!(step.outputs.contains_key("yt_dlp_thumbnail_artifacts"));
    assert!(!step.outputs.contains_key("content"));
}

/// Protects value-centric option binding policy by keeping non-`option_args`
/// option values as scalar `string` bindings.
#[test]
fn step_option_bindings_keep_non_option_args_values_scalar() {
    let bindings = step_option_input_bindings(
        MediaStepTool::YtDlp,
        &BTreeMap::from([
            ("merge_output_format".to_string(), TransformInputValue::String("mkv".to_string())),
            ("no_playlist".to_string(), TransformInputValue::String("true".to_string())),
        ]),
    )
    .expect("bindings");

    assert!(bindings.get("merge_output_format") == Some(&InputBinding::String("mkv".to_string())));
    assert!(bindings.get("no_playlist") == Some(&InputBinding::String("true".to_string())));
}

/// Protects `option_args` escape-hatch behavior, which remains `string_list` and
/// splits scalar input on whitespace.
#[test]
fn step_option_bindings_split_option_args_to_string_list() {
    let bindings = step_option_input_bindings(
        MediaStepTool::YtDlp,
        &BTreeMap::from([(
            "option_args".to_string(),
            TransformInputValue::String("--foo --bar=baz".to_string()),
        )]),
    )
    .expect("bindings");

    assert_eq!(
        bindings.get("option_args"),
        Some(&InputBinding::StringList(vec!["--foo".to_string(), "--bar=baz".to_string()])),
    );
}

/// Protects scalar-first option typing by rejecting list values for
/// non-`option_args` option inputs.
#[test]
fn step_option_bindings_reject_string_list_for_non_option_args_option() {
    let error = step_option_input_bindings(
        MediaStepTool::YtDlp,
        &BTreeMap::from([(
            "merge_output_format".to_string(),
            TransformInputValue::StringList(vec!["mkv".to_string()]),
        )]),
    )
    .expect_err("non-option_args list option should fail");

    assert!(error.to_string().contains("must be a string"));
    assert!(error.to_string().contains("merge_output_format"));
}

/// Protects yt-dlp source URI routing so workflow synthesis does not bind
/// `options.uri` as a tool option input.
#[test]
fn step_option_bindings_skip_yt_dlp_uri_option() {
    let bindings = step_option_input_bindings(
        MediaStepTool::YtDlp,
        &BTreeMap::from([(
            "uri".to_string(),
            TransformInputValue::String("https://example.com/v".to_string()),
        )]),
    )
    .expect("bindings");

    assert!(!bindings.contains_key("uri"));
}

/// Protects hierarchy variant resolution so any variant exposed by any
/// step remains selectable by name.
#[test]
fn variant_binding_resolves_non_latest_variant_name_when_still_unique() {
    let source = MediaSourceSpec {
        id: None,
        description: None,
        title: None,
        workflow_id: None,
        metadata: None,
        variant_hashes: BTreeMap::new(),
        steps: vec![
            MediaStep {
                tool: MediaStepTool::YtDlp,
                input_variants: Vec::new(),
                output_variants: BTreeMap::from([
                    ("downloaded".to_string(), yt_dlp_output_variant("primary")),
                    ("subtitles".to_string(), yt_dlp_output_variant("subtitles")),
                ]),
                options: BTreeMap::from([(
                    "uri".to_string(),
                    TransformInputValue::String("https://example.com/video".to_string()),
                )]),
            },
            MediaStep {
                tool: MediaStepTool::Ffmpeg,
                input_variants: vec!["downloaded".to_string()],
                output_variants: BTreeMap::from([(
                    "video_144p".to_string(),
                    ffmpeg_output_variant(0),
                )]),
                options: BTreeMap::new(),
            },
        ],
    };

    let binding =
        resolve_media_variant_output_binding(&source, "subtitles").expect("resolve binding");
    let binding = binding.expect("binding should exist for subtitles variant");

    assert_eq!(binding.step_id, "0-0-yt-dlp");
    assert_eq!(binding.output_name, "yt_dlp_subtitle_artifacts");
}

/// Protects duplicate output-variant semantics by selecting the latest
/// producer when multiple steps expose the same variant name.
#[test]
fn variant_binding_uses_last_producer_for_duplicate_output_variant() {
    let source = MediaSourceSpec {
        id: None,
        description: None,
        title: None,
        workflow_id: None,
        metadata: None,
        variant_hashes: BTreeMap::new(),
        steps: vec![
            MediaStep {
                tool: MediaStepTool::YtDlp,
                input_variants: Vec::new(),
                output_variants: BTreeMap::from([(
                    "downloaded".to_string(),
                    yt_dlp_output_variant("primary"),
                )]),
                options: BTreeMap::from([(
                    "uri".to_string(),
                    TransformInputValue::String("https://example.com/video".to_string()),
                )]),
            },
            MediaStep {
                tool: MediaStepTool::Ffmpeg,
                input_variants: vec!["downloaded".to_string()],
                output_variants: BTreeMap::from([(
                    "normalized".to_string(),
                    ffmpeg_output_variant(0),
                )]),
                options: BTreeMap::new(),
            },
            MediaStep {
                tool: MediaStepTool::Rsgain,
                input_variants: vec!["normalized".to_string()],
                output_variants: BTreeMap::from([(
                    "normalized".to_string(),
                    generic_output_variant("primary"),
                )]),
                options: BTreeMap::new(),
            },
        ],
    };

    let binding =
        resolve_media_variant_output_binding(&source, "normalized").expect("resolve binding");
    let binding = binding.expect("binding should exist for normalized variant");

    assert_eq!(binding.step_id, "2-5-rsgain-ffmpeg-apply");
    assert_eq!(binding.output_name, "primary");
}

/// Protects rsgain synthesis by reusing a supported upstream tagged extension
/// instead of falling back to FLAC extraction.
#[test]
fn rsgain_chain_reuses_supported_upstream_extension_to_avoid_transcoding() {
    let document = MediaPmDocument {
        media: BTreeMap::from([(
            "rsgain-preserve-ext".to_string(),
            MediaSourceSpec {
                id: None,
                description: None,
                title: None,
                workflow_id: None,
                metadata: None,
                variant_hashes: BTreeMap::from([(
                    "default".to_string(),
                    "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        .to_string(),
                )]),
                steps: vec![
                    MediaStep {
                        tool: MediaStepTool::Ffmpeg,
                        input_variants: vec!["default".to_string()],
                        output_variants: BTreeMap::from([(
                            "audio_m4a".to_string(),
                            ffmpeg_output_variant_with_extension(0, "m4a"),
                        )]),
                        options: BTreeMap::new(),
                    },
                    MediaStep {
                        tool: MediaStepTool::MediaTagger,
                        input_variants: vec!["audio_m4a".to_string()],
                        output_variants: BTreeMap::from([(
                            "tagged".to_string(),
                            generic_output_variant("primary"),
                        )]),
                        options: BTreeMap::new(),
                    },
                    MediaStep {
                        tool: MediaStepTool::Rsgain,
                        input_variants: vec!["tagged".to_string()],
                        output_variants: BTreeMap::from([(
                            "normalized".to_string(),
                            generic_output_variant("primary"),
                        )]),
                        options: BTreeMap::new(),
                    },
                ],
            },
        )]),
        ..MediaPmDocument::default()
    };

    let lock = MediaLockFile {
        active_tools: BTreeMap::from([
            (
                "media-tagger".to_string(),
                "mediapm.tools.media-tagger+mediapm-internal@latest".to_string(),
            ),
            (
                "ffmpeg".to_string(),
                "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@latest".to_string(),
            ),
            (
                "rsgain".to_string(),
                "mediapm.tools.rsgain+github-releases-complexlogic-rsgain@latest".to_string(),
            ),
            ("sd".to_string(), "mediapm.tools.sd+conductor-common@latest".to_string()),
        ]),
        ..MediaLockFile::default()
    };

    let machine = machine_with_active_tool_specs(&lock);
    let plan = build_media_workflow_plan(&document, &lock, &machine).expect("plan");
    let workflow =
        plan.workflows.get("mediapm.media.rsgain-preserve-ext").expect("managed workflow");

    let rsgain_extract = workflow
        .steps
        .iter()
        .find(|step| step.id.ends_with("-ffmpeg-extract"))
        .expect("rsgain extract step");
    let rsgain =
        workflow.steps.iter().find(|step| step.id.ends_with("-rsgain")).expect("rsgain step");
    let apply = workflow
        .steps
        .iter()
        .find(|step| step.id.ends_with("-ffmpeg-apply"))
        .expect("rsgain apply step");

    assert_eq!(
        rsgain_extract.inputs.get("output_path_0"),
        Some(&InputBinding::String("output-0.m4a".to_string()))
    );
    assert_eq!(
        rsgain_extract.inputs.get("codec_copy"),
        Some(&InputBinding::String("true".to_string()))
    );
    assert_eq!(
        rsgain.inputs.get("input_extension"),
        Some(&InputBinding::String("m4a".to_string()))
    );
    assert_eq!(
        apply.inputs.get("output_path_0"),
        Some(&InputBinding::String("output-0.m4a".to_string()))
    );
}

/// Protects ffmpeg runtime-limit configurability for high-index outputs.
#[test]
fn variant_binding_supports_custom_ffmpeg_output_limit() {
    let source = MediaSourceSpec {
        id: None,
        description: None,
        title: None,
        workflow_id: None,
        metadata: None,
        variant_hashes: BTreeMap::from([(
            "default".to_string(),
            "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
        )]),
        steps: vec![MediaStep {
            tool: MediaStepTool::Ffmpeg,
            input_variants: vec!["default".to_string()],
            output_variants: BTreeMap::from([(
                "hi".to_string(),
                serde_json::json!({
                    "kind": "primary",
                    "save": "full",
                    "idx": 70
                }),
            )]),
            options: BTreeMap::new(),
        }],
    };

    let default_error =
        resolve_media_variant_output_binding(&source, "hi").expect_err("default limit should fail");
    assert!(default_error.to_string().contains("tools.ffmpeg.max_output_slots"));

    let binding = resolve_media_variant_output_binding_with_limits(&source, "hi", 128, 128)
        .expect("custom limits should resolve")
        .expect("binding should exist");
    assert_eq!(binding.output_name, "primary_70");
}

/// Protects yt-dlp description sidecar semantics by binding directly to
/// file captures with no implicit ZIP member selector.
#[test]
fn yt_dlp_description_binding_uses_file_capture_without_zip_member() {
    let source = MediaSourceSpec {
        id: None,
        description: None,
        title: None,
        workflow_id: None,
        metadata: None,
        variant_hashes: BTreeMap::new(),
        steps: vec![MediaStep {
            tool: MediaStepTool::YtDlp,
            input_variants: Vec::new(),
            output_variants: BTreeMap::from([(
                "description".to_string(),
                serde_json::json!({ "kind": "description", "save": "full" }),
            )]),
            options: BTreeMap::from([(
                "uri".to_string(),
                TransformInputValue::String("https://example.com/video".to_string()),
            )]),
        }],
    };

    let binding = resolve_media_variant_output_binding(&source, "description")
        .expect("resolve description binding")
        .expect("binding should exist");

    assert_eq!(binding.output_name, "yt_dlp_description_file");
    assert!(binding.zip_member.is_none());
}

/// Protects yt-dlp annotation sidecar semantics by binding singular
/// `annotation` variants directly to file captures.
#[test]
fn yt_dlp_annotation_binding_uses_file_capture_without_zip_member() {
    let source = MediaSourceSpec {
        id: None,
        description: None,
        title: None,
        workflow_id: None,
        metadata: None,
        variant_hashes: BTreeMap::new(),
        steps: vec![MediaStep {
            tool: MediaStepTool::YtDlp,
            input_variants: Vec::new(),
            output_variants: BTreeMap::from([(
                "annotation".to_string(),
                serde_json::json!({ "kind": "annotation", "save": "full" }),
            )]),
            options: BTreeMap::from([(
                "uri".to_string(),
                TransformInputValue::String("https://example.com/video".to_string()),
            )]),
        }],
    };

    let binding = resolve_media_variant_output_binding(&source, "annotation")
        .expect("resolve annotation binding")
        .expect("annotation binding should exist");

    assert_eq!(binding.output_name, "yt_dlp_annotation_file");
    assert!(binding.zip_member.is_none());
}

/// Protects capture-kind override semantics by routing description
/// variants to folder capture outputs when explicitly requested.
#[test]
fn yt_dlp_description_binding_honors_folder_capture_kind() {
    let source = MediaSourceSpec {
        id: None,
        description: None,
        title: None,
        workflow_id: None,
        metadata: None,
        variant_hashes: BTreeMap::new(),
        steps: vec![MediaStep {
            tool: MediaStepTool::YtDlp,
            input_variants: Vec::new(),
            output_variants: BTreeMap::from([(
                "description".to_string(),
                serde_json::json!({
                    "kind": "description",
                    "capture_kind": "folder",
                    "save": "full"
                }),
            )]),
            options: BTreeMap::from([(
                "uri".to_string(),
                TransformInputValue::String("https://example.com/video".to_string()),
            )]),
        }],
    };

    let binding = resolve_media_variant_output_binding(&source, "description")
        .expect("resolve description binding")
        .expect("binding should exist");

    assert_eq!(binding.output_name, "sandbox_artifacts");
}

/// Protects multi-output yt-dlp synthesis by generating one workflow step
/// that enables all required sidecar toggles.
#[test]
fn yt_dlp_description_and_infojson_outputs_share_one_step() {
    let document = MediaPmDocument {
        media: BTreeMap::from([(
            "sidecar-flags".to_string(),
            MediaSourceSpec {
                id: None,
                description: None,
                title: None,
                workflow_id: None,
                metadata: None,
                variant_hashes: BTreeMap::new(),
                steps: vec![MediaStep {
                    tool: MediaStepTool::YtDlp,
                    input_variants: Vec::new(),
                    output_variants: BTreeMap::from([
                        (
                            "description".to_string(),
                            json!({ "kind": "description", "save": "full" }),
                        ),
                        ("info_json".to_string(), json!({ "kind": "infojson", "save": "full" })),
                    ]),
                    options: BTreeMap::from([(
                        "uri".to_string(),
                        TransformInputValue::String("https://example.com/video".to_string()),
                    )]),
                }],
            },
        )]),
        ..MediaPmDocument::default()
    };

    let lock = MediaLockFile {
        active_tools: BTreeMap::from([(
            "yt-dlp".to_string(),
            "mediapm.tools.yt-dlp+github-releases-yt-dlp-yt-dlp@latest".to_string(),
        )]),
        ..MediaLockFile::default()
    };

    let machine = machine_with_active_tool_specs(&lock);
    let plan = build_media_workflow_plan(&document, &lock, &machine).expect("plan");
    let workflow = plan.workflows.get("mediapm.media.sidecar-flags").expect("workflow");

    assert_eq!(workflow.steps.len(), 1);
    let step = workflow.steps.first().expect("yt-dlp step");
    assert_eq!(step.id, "0-0-yt-dlp");

    assert_eq!(
        step.inputs.get("write_description"),
        Some(&InputBinding::String("true".to_string()))
    );
    assert_eq!(step.inputs.get("write_info_json"), Some(&InputBinding::String("true".to_string())));

    let description_binding = resolve_media_variant_output_binding(
        document.media.get("sidecar-flags").expect("source"),
        "description",
    )
    .expect("resolve description binding")
    .expect("description binding should exist");
    let infojson_binding = resolve_media_variant_output_binding(
        document.media.get("sidecar-flags").expect("source"),
        "info_json",
    )
    .expect("resolve infojson binding")
    .expect("infojson binding should exist");

    assert_eq!(description_binding.step_id, "0-0-yt-dlp");
    assert_eq!(infojson_binding.step_id, "0-0-yt-dlp");
}

/// Protects thumbnail output synthesis by enabling thumbnail toggles while
/// leaving caller defaults overrideable.
#[test]
fn yt_dlp_thumbnail_step_enables_thumbnail_outputs() {
    let document = MediaPmDocument {
        media: BTreeMap::from([(
            "thumbnail-only".to_string(),
            MediaSourceSpec {
                id: None,
                description: None,
                title: None,
                workflow_id: None,
                metadata: None,
                variant_hashes: BTreeMap::new(),
                steps: vec![MediaStep {
                    tool: MediaStepTool::YtDlp,
                    input_variants: Vec::new(),
                    output_variants: BTreeMap::from([(
                        "thumbnails/".to_string(),
                        json!({ "kind": "thumbnails", "save": "full" }),
                    )]),
                    options: BTreeMap::from([(
                        "uri".to_string(),
                        TransformInputValue::String("https://example.com/video".to_string()),
                    )]),
                }],
            },
        )]),
        ..MediaPmDocument::default()
    };

    let lock = MediaLockFile {
        active_tools: BTreeMap::from([(
            "yt-dlp".to_string(),
            "mediapm.tools.yt-dlp+github-releases-yt-dlp-yt-dlp@latest".to_string(),
        )]),
        ..MediaLockFile::default()
    };

    let machine = machine_with_active_tool_specs(&lock);
    let plan = build_media_workflow_plan(&document, &lock, &machine).expect("plan");
    let workflow = plan.workflows.get("mediapm.media.thumbnail-only").expect("workflow");
    let step = workflow.steps.first().expect("thumbnail step");

    assert_eq!(step.inputs.get("write_thumbnail"), Some(&InputBinding::String("true".to_string())));
    assert_eq!(
        step.inputs.get("write_all_thumbnails"),
        Some(&InputBinding::String("false".to_string()))
    );
}

/// Protects subtitle output synthesis by enabling subtitle capture and
/// avoiding forced disables for unrelated toggles.
#[test]
fn yt_dlp_subtitle_step_enables_subtitle_capture() {
    let document = MediaPmDocument {
        media: BTreeMap::from([(
            "subtitle-only".to_string(),
            MediaSourceSpec {
                id: None,
                description: None,
                title: None,
                workflow_id: None,
                metadata: None,
                variant_hashes: BTreeMap::new(),
                steps: vec![MediaStep {
                    tool: MediaStepTool::YtDlp,
                    input_variants: Vec::new(),
                    output_variants: BTreeMap::from([(
                        "subtitles/".to_string(),
                        json!({ "kind": "subtitles", "save": "full" }),
                    )]),
                    options: BTreeMap::from([(
                        "uri".to_string(),
                        TransformInputValue::String("https://example.com/video".to_string()),
                    )]),
                }],
            },
        )]),
        ..MediaPmDocument::default()
    };

    let lock = MediaLockFile {
        active_tools: BTreeMap::from([(
            "yt-dlp".to_string(),
            "mediapm.tools.yt-dlp+github-releases-yt-dlp-yt-dlp@latest".to_string(),
        )]),
        ..MediaLockFile::default()
    };

    let machine = machine_with_active_tool_specs(&lock);
    let plan = build_media_workflow_plan(&document, &lock, &machine).expect("plan");
    let workflow = plan.workflows.get("mediapm.media.subtitle-only").expect("workflow");
    let step = workflow.steps.first().expect("subtitle step");

    assert_eq!(step.inputs.get("write_subs"), Some(&InputBinding::String("true".to_string())));
    assert_eq!(step.inputs.get("skip_download"), Some(&InputBinding::String("true".to_string())));
    assert_eq!(
        step.inputs.get("write_thumbnail"),
        Some(&InputBinding::String("false".to_string()))
    );
    assert_eq!(step.inputs.get("write_comments"), Some(&InputBinding::String("false".to_string())));
}

/// Protects key-agnostic producer resolution by requiring exact producer
/// matches for scoped input variants.
#[test]
fn scoped_input_variant_requires_exact_producer_without_folder_fallback() {
    let document = MediaPmDocument {
        media: BTreeMap::from([(
            "scoped-folder".to_string(),
            MediaSourceSpec {
                id: None,
                description: None,
                title: None,
                workflow_id: None,
                metadata: None,
                variant_hashes: BTreeMap::new(),
                steps: vec![
                    MediaStep {
                        tool: MediaStepTool::YtDlp,
                        input_variants: Vec::new(),
                        output_variants: BTreeMap::from([(
                            "subtitles/".to_string(),
                            yt_dlp_output_variant("subtitles"),
                        )]),
                        options: BTreeMap::from([(
                            "uri".to_string(),
                            TransformInputValue::String("https://example.com/video".to_string()),
                        )]),
                    },
                    MediaStep {
                        tool: MediaStepTool::Ffmpeg,
                        input_variants: vec!["subtitles/en".to_string()],
                        output_variants: BTreeMap::from([(
                            "normalized".to_string(),
                            ffmpeg_output_variant(0),
                        )]),
                        options: BTreeMap::new(),
                    },
                ],
            },
        )]),
        ..MediaPmDocument::default()
    };

    let lock = MediaLockFile {
        active_tools: BTreeMap::from([
            (
                "yt-dlp".to_string(),
                "mediapm.tools.yt-dlp+github-releases-yt-dlp-yt-dlp@latest".to_string(),
            ),
            (
                "ffmpeg".to_string(),
                "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@latest".to_string(),
            ),
        ]),
        ..MediaLockFile::default()
    };

    let machine = machine_with_active_tool_specs(&lock);
    let error = build_media_workflow_plan(&document, &lock, &machine)
        .expect_err("plan should fail without exact scoped producer");
    assert!(error.to_string().contains("subtitles/en") && error.to_string().contains("unknown"));
}

/// Protects producer selection precedence so exact scoped outputs resolve
/// successfully when both scoped and folder-like keys exist.
#[test]
fn scoped_input_variant_prefers_exact_output_over_folder_fallback() {
    let document = MediaPmDocument {
        media: BTreeMap::from([(
            "scoped-exact".to_string(),
            MediaSourceSpec {
                id: None,
                description: None,
                title: None,
                workflow_id: None,
                metadata: None,
                variant_hashes: BTreeMap::new(),
                steps: vec![
                    MediaStep {
                        tool: MediaStepTool::YtDlp,
                        input_variants: Vec::new(),
                        output_variants: BTreeMap::from([
                            ("subtitles/".to_string(), yt_dlp_output_variant("subtitles")),
                            (
                                "subtitles/en".to_string(),
                                json!({
                                    "kind": "subtitles",
                                    "save": "full",
                                    "langs": "en"
                                }),
                            ),
                        ]),
                        options: BTreeMap::from([(
                            "uri".to_string(),
                            TransformInputValue::String("https://example.com/video".to_string()),
                        )]),
                    },
                    MediaStep {
                        tool: MediaStepTool::Ffmpeg,
                        input_variants: vec!["subtitles/en".to_string()],
                        output_variants: BTreeMap::from([(
                            "normalized".to_string(),
                            ffmpeg_output_variant(0),
                        )]),
                        options: BTreeMap::new(),
                    },
                ],
            },
        )]),
        ..MediaPmDocument::default()
    };

    let lock = MediaLockFile {
        active_tools: BTreeMap::from([
            (
                "yt-dlp".to_string(),
                "mediapm.tools.yt-dlp+github-releases-yt-dlp-yt-dlp@latest".to_string(),
            ),
            (
                "ffmpeg".to_string(),
                "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@latest".to_string(),
            ),
        ]),
        ..MediaLockFile::default()
    };

    let machine = machine_with_active_tool_specs(&lock);
    let plan = build_media_workflow_plan(&document, &lock, &machine).expect("plan");
    let workflow = plan.workflows.get("mediapm.media.scoped-exact").expect("managed workflow");

    let exact_producer_step = &workflow.steps[0];
    let consumer_step = &workflow.steps[1];
    assert_eq!(
        consumer_step.inputs.get("input_content_0"),
        Some(&InputBinding::String(format!(
            "${{step_output.{}.yt_dlp_subtitle_artifacts}}",
            exact_producer_step.id
        ))),
    );
}

/// Protects downloader language-selection ownership by keeping
/// `sub_langs` sourced from step options instead of output-variant `langs`.
#[test]
fn yt_dlp_scoped_subtitle_variant_keeps_step_sub_langs_authoritative() {
    let document = MediaPmDocument {
        media: BTreeMap::from([(
            "auto-inputs".to_string(),
            MediaSourceSpec {
                id: None,
                description: None,
                title: None,
                workflow_id: None,
                metadata: None,
                variant_hashes: BTreeMap::new(),
                steps: vec![MediaStep {
                    tool: MediaStepTool::YtDlp,
                    input_variants: Vec::new(),
                    output_variants: BTreeMap::from([(
                        "subtitles/en".to_string(),
                        json!({
                            "kind": "subtitles",
                            "save": "full",
                            "langs": "en"
                        }),
                    )]),
                    options: BTreeMap::from([
                        (
                            "uri".to_string(),
                            TransformInputValue::String("https://example.com/video".to_string()),
                        ),
                        ("sub_langs".to_string(), TransformInputValue::String("en,es".to_string())),
                    ]),
                }],
            },
        )]),
        ..MediaPmDocument::default()
    };

    let lock = MediaLockFile {
        active_tools: BTreeMap::from([(
            "yt-dlp".to_string(),
            "mediapm.tools.yt-dlp+github-releases-yt-dlp-yt-dlp@latest".to_string(),
        )]),
        ..MediaLockFile::default()
    };

    let machine = machine_with_active_tool_specs(&lock);
    let plan = build_media_workflow_plan(&document, &lock, &machine).expect("plan");
    let workflow = plan.workflows.get("mediapm.media.auto-inputs").expect("managed workflow");
    let step = workflow.steps.first().expect("yt-dlp step");

    assert_eq!(step.inputs.get("write_subs"), Some(&InputBinding::String("true".to_string())),);
    assert_eq!(step.inputs.get("sub_langs"), Some(&InputBinding::String("en,es".to_string())),);
    assert_eq!(step.inputs.get("skip_download"), Some(&InputBinding::String("true".to_string())));
    assert!(!step.inputs.contains_key("output"));
}

/// Protects primary yt-dlp variant behavior by keeping download-enabled
/// defaults for media outputs.
#[test]
fn yt_dlp_primary_variant_does_not_auto_inject_skip_download() {
    let document = MediaPmDocument {
        media: BTreeMap::from([(
            "primary-output".to_string(),
            MediaSourceSpec {
                id: None,
                description: None,
                title: None,
                workflow_id: None,
                metadata: None,
                variant_hashes: BTreeMap::new(),
                steps: vec![MediaStep {
                    tool: MediaStepTool::YtDlp,
                    input_variants: Vec::new(),
                    output_variants: BTreeMap::from([(
                        "video".to_string(),
                        yt_dlp_output_variant("primary"),
                    )]),
                    options: BTreeMap::from([(
                        "uri".to_string(),
                        TransformInputValue::String("https://example.com/video".to_string()),
                    )]),
                }],
            },
        )]),
        ..MediaPmDocument::default()
    };

    let lock = MediaLockFile {
        active_tools: BTreeMap::from([(
            "yt-dlp".to_string(),
            "mediapm.tools.yt-dlp+github-releases-yt-dlp-yt-dlp@latest".to_string(),
        )]),
        ..MediaLockFile::default()
    };

    let machine = machine_with_active_tool_specs(&lock);
    let plan = build_media_workflow_plan(&document, &lock, &machine).expect("plan");
    let workflow = plan.workflows.get("mediapm.media.primary-output").expect("managed workflow");
    let step = workflow.steps.first().expect("yt-dlp step");

    assert!(!step.inputs.contains_key("skip_download"));
}
