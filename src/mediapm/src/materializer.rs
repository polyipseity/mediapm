//! Atomic staging + commit materializer for Phase 3 hierarchy sync.
//!
//! The materializer enforces path invariants, stages all outputs under
//! the resolved runtime staging directory, and then commits with atomic rename
//! operations into the resolved library directory.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Read;
use std::path::Path;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use mediapm_cas::{CasApi, FileSystemCas, Hash};
use mediapm_conductor::{
    InputBinding, MachineNickelDocument, OrchestrationState, ToolCallInstance, ToolKindSpec,
    ToolSpec, decode_state_document,
};
use unicode_normalization::UnicodeNormalization;

use crate::conductor_bridge::{
    managed_workflow_id_for_media, resolve_ffmpeg_slot_limits,
    resolve_media_variant_output_binding_with_limits,
};
use crate::config::{
    HierarchyEntry, MediaMetadataValue, MediaPmDocument, MediaSourceSpec,
    hierarchy_metadata_placeholder_keys, media_source_uri,
};
use crate::error::MediaPmError;
use crate::lockfile::{ManagedFileRecord, MediaLockFile};
use crate::paths::MediaPmPaths;

/// Summary of one materialization pass.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MaterializeReport {
    /// Number of hierarchy entries staged and committed.
    pub materialized_paths: usize,
    /// Number of previously managed outputs removed as stale.
    pub removed_paths: usize,
    /// Link/copy fallback notices captured during materialization.
    pub notices: Vec<String>,
}

/// Per-workflow step output hash table (`step_id -> output_name -> CAS hash`).
type StepOutputHashes = BTreeMap<String, BTreeMap<String, Hash>>;

/// Shared lookup context for resolving materialization-time variant payloads.
///
/// The materializer repeatedly resolves variant bytes from either local CAS
/// pointers or managed workflow outputs. This context groups immutable lookup
/// dependencies so helper signatures remain compact and consistent.
struct MaterializationLookupContext<'a> {
    /// Conductor CAS store used for payload reads.
    cas: &'a FileSystemCas,
    /// Resolved conductor machine document for tool/workflow metadata.
    machine: &'a MachineNickelDocument,
    /// Optional persisted orchestration state loaded from runtime pointer.
    orchestration_state: Option<&'a OrchestrationState>,
    /// Effective ffmpeg input-slot limit used for output-binding resolution.
    ffmpeg_max_input_slots: usize,
    /// Effective ffmpeg output-slot limit used for output-binding resolution.
    ffmpeg_max_output_slots: usize,
}

/// Synchronizes desired hierarchy entries using stage-verify-commit flow.
pub async fn sync_hierarchy(
    paths: &MediaPmPaths,
    document: &MediaPmDocument,
    machine: &MachineNickelDocument,
    conductor_cas_root: &Path,
    lock: &mut MediaLockFile,
) -> Result<MaterializeReport, MediaPmError> {
    let ffmpeg_slot_limits = resolve_ffmpeg_slot_limits(&document.tools)?;
    let ffmpeg_max_input_slots = ffmpeg_slot_limits.max_input_slots;
    let ffmpeg_max_output_slots = ffmpeg_slot_limits.max_output_slots;

    fs::create_dir_all(&paths.tmp_dir).map_err(|source| MediaPmError::Io {
        operation: "creating .mediapm/tmp".to_string(),
        path: paths.tmp_dir.clone(),
        source,
    })?;
    fs::create_dir_all(&paths.library_dir).map_err(|source| MediaPmError::Io {
        operation: "creating resolved library directory".to_string(),
        path: paths.library_dir.clone(),
        source,
    })?;

    let cas = FileSystemCas::open(conductor_cas_root).await.map_err(|source| {
        MediaPmError::Workflow(format!(
            "opening conductor CAS store '{}' for materialization failed: {source}",
            conductor_cas_root.display()
        ))
    })?;
    let orchestration_state = load_runtime_orchestration_state(paths, &cas).await?;
    let lookup = MaterializationLookupContext {
        cas: &cas,
        machine,
        orchestration_state: orchestration_state.as_ref(),
        ffmpeg_max_input_slots,
        ffmpeg_max_output_slots,
    };

    let staging_root = paths.tmp_dir.join(format!("sync-{}", now_unix_seconds()));
    fs::create_dir_all(&staging_root).map_err(|source| MediaPmError::Io {
        operation: "creating sync staging directory".to_string(),
        path: staging_root.clone(),
        source,
    })?;

    let mut report = MaterializeReport::default();
    let mut desired_paths = BTreeSet::new();

    for (relative_path_template, entry) in &document.hierarchy {
        let source = resolve_hierarchy_source(document, entry)?;
        let relative_path =
            resolve_hierarchy_relative_path(relative_path_template, entry, source, &lookup).await?;
        validate_hierarchy_path(&relative_path)?;

        if entry.variants.is_empty() {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy path '{relative_path}' must define at least one variant"
            )));
        }

        let is_directory_target = relative_path.ends_with('/') || relative_path.ends_with('\\');
        let fs_relative_path = if is_directory_target {
            relative_path.trim_end_matches(['/', '\\'])
        } else {
            relative_path.as_str()
        };

        if fs_relative_path.is_empty() {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy path '{relative_path}' must not resolve to an empty filesystem path"
            )));
        }

        let staged_path = staging_root.join(fs_relative_path);

        if is_directory_target {
            fs::create_dir_all(&staged_path).map_err(|source_err| MediaPmError::Io {
                operation: "creating staged output directory".to_string(),
                path: staged_path.clone(),
                source: source_err,
            })?;

            let mut extracted_entries = BTreeMap::new();
            for variant in &entry.variants {
                let (variant_bytes, notice) =
                    resolve_variant_source_bytes(&lookup, &entry.media_id, source, variant).await?;
                if let Some(message) = notice {
                    report.notices.push(message);
                }

                extract_zip_folder_variant_bytes(
                    &variant_bytes,
                    &staged_path,
                    &relative_path,
                    &entry.media_id,
                    variant,
                    &mut extracted_entries,
                )?;
            }
        } else {
            if entry.variants.len() != 1 {
                return Err(MediaPmError::Workflow(format!(
                    "hierarchy file path '{relative_path}' must reference exactly one variant"
                )));
            }
            let variant = entry
                .variants
                .first()
                .expect("checked non-empty and len==1 for hierarchy file path");

            if let Some(parent) = staged_path.parent() {
                fs::create_dir_all(parent).map_err(|source_err| MediaPmError::Io {
                    operation: "creating staged parent directory".to_string(),
                    path: parent.to_path_buf(),
                    source: source_err,
                })?;
            }

            let (variant_bytes, notice) =
                resolve_variant_source_bytes(&lookup, &entry.media_id, source, variant).await?;
            if let Some(message) = notice {
                report.notices.push(message);
            }

            fs::write(&staged_path, variant_bytes).map_err(|source_err| MediaPmError::Io {
                operation: "writing workflow-produced variant bytes from CAS".to_string(),
                path: staged_path.clone(),
                source: source_err,
            })?;
        }

        desired_paths.insert(relative_path.clone());

        let final_path = paths.library_dir.join(fs_relative_path);
        if let Some(parent) = final_path.parent() {
            fs::create_dir_all(parent).map_err(|source_err| MediaPmError::Io {
                operation: "creating final output parent directory".to_string(),
                path: parent.to_path_buf(),
                source: source_err,
            })?;
        }
        if final_path.exists() {
            remove_path(&final_path)?;
        }

        fs::rename(&staged_path, &final_path).map_err(|source_err| MediaPmError::Io {
            operation: "committing staged output via rename".to_string(),
            path: final_path.clone(),
            source: source_err,
        })?;
        ensure_managed_path_readonly(&final_path)?;

        lock.managed_files.insert(
            relative_path.clone(),
            ManagedFileRecord {
                media_id: entry.media_id.clone(),
                variant: entry.variants.join("+"),
                last_synced_unix_millis: unix_epoch_millis(),
            },
        );

        report.materialized_paths += 1;
    }

    let stale_paths = lock
        .managed_files
        .keys()
        .filter(|path| !desired_paths.contains(*path))
        .cloned()
        .collect::<Vec<_>>();

    for stale in stale_paths {
        let final_path = paths.library_dir.join(&stale);
        if final_path.exists() {
            remove_path(&final_path)?;
            report.removed_paths += 1;
        }
        lock.managed_files.remove(&stale);
    }

    let _ = fs::remove_dir_all(&staging_root);
    Ok(report)
}

/// Resolves `${media.metadata.*}` placeholders for one hierarchy key template.
async fn resolve_hierarchy_relative_path(
    relative_path_template: &str,
    entry: &HierarchyEntry,
    source: &MediaSourceSpec,
    lookup: &MaterializationLookupContext<'_>,
) -> Result<String, MediaPmError> {
    let placeholder_keys =
        hierarchy_metadata_placeholder_keys(relative_path_template).map_err(|reason| {
            MediaPmError::Workflow(format!(
                "hierarchy path '{}' has invalid metadata placeholder syntax: {reason}",
                relative_path_template
            ))
        })?;

    if placeholder_keys.is_empty() {
        return Ok(relative_path_template.to_string());
    }

    let metadata = source.metadata.as_ref().ok_or_else(|| {
        MediaPmError::Workflow(format!(
            "hierarchy path '{}' references metadata placeholders but media '{}' does not define metadata",
            relative_path_template, entry.media_id
        ))
    })?;

    let mut resolved_values = BTreeMap::new();
    for metadata_key in placeholder_keys {
        if resolved_values.contains_key(&metadata_key) {
            continue;
        }

        let metadata_value = metadata.get(metadata_key.as_str()).ok_or_else(|| {
            MediaPmError::Workflow(format!(
                "hierarchy path '{}' references undefined metadata key '{}' for media '{}'",
                relative_path_template, metadata_key, entry.media_id
            ))
        })?;

        let resolved = resolve_media_metadata_string_value(
            &entry.media_id,
            metadata_key.as_str(),
            metadata_value,
            source,
            lookup,
        )
        .await?;
        resolved_values.insert(metadata_key, resolved);
    }

    let mut resolved_path = relative_path_template.to_string();
    for (metadata_key, metadata_value) in resolved_values {
        let placeholder = format!("${{media.metadata.{metadata_key}}}");
        resolved_path = resolved_path.replace(&placeholder, metadata_value.as_str());
    }

    Ok(resolved_path)
}

/// Resolves one media metadata value into a concrete string.
async fn resolve_media_metadata_string_value(
    media_id: &str,
    metadata_key: &str,
    metadata_value: &MediaMetadataValue,
    source: &MediaSourceSpec,
    lookup: &MaterializationLookupContext<'_>,
) -> Result<String, MediaPmError> {
    match metadata_value {
        MediaMetadataValue::Literal(value) => Ok(value.clone()),
        MediaMetadataValue::Variant(binding) => {
            let (variant_bytes, _notice) =
                resolve_variant_source_bytes(lookup, media_id, source, binding.variant.as_str())
                    .await?;

            let parsed = serde_json::from_slice::<serde_json::Value>(variant_bytes.as_slice())
                .map_err(|error| {
                    MediaPmError::Workflow(format!(
                        "media '{media_id}' metadata '{metadata_key}' variant '{}' must resolve to JSON for metadata extraction: {error}",
                        binding.variant
                    ))
                })?;

            let object = parsed.as_object().ok_or_else(|| {
                MediaPmError::Workflow(format!(
                    "media '{media_id}' metadata '{metadata_key}' variant '{}' must resolve to a JSON object",
                    binding.variant
                ))
            })?;

            let extracted = object.get(binding.metadata_key.as_str()).ok_or_else(|| {
                MediaPmError::Workflow(format!(
                    "media '{media_id}' metadata '{metadata_key}' variant '{}' is missing metadata key '{}'",
                    binding.variant, binding.metadata_key
                ))
            })?;

            extracted.as_str().map(ToString::to_string).ok_or_else(|| {
                MediaPmError::Workflow(format!(
                    "media '{media_id}' metadata '{metadata_key}' key '{}' from variant '{}' must resolve to a string",
                    binding.metadata_key, binding.variant
                ))
            })
        }
    }
}

/// Loads persisted orchestration state referenced by runtime state pointer.
///
/// Returns `None` when the volatile runtime state document is absent, empty, or
/// does not carry a state pointer yet. A missing pointed CAS object is also
/// treated as unavailable state so sync can continue in mixed-backend flows
/// (for example in-memory conductor bootstrapping with filesystem materializer).
async fn load_runtime_orchestration_state(
    paths: &MediaPmPaths,
    cas: &FileSystemCas,
) -> Result<Option<OrchestrationState>, MediaPmError> {
    if !paths.conductor_state_config.exists() {
        return Ok(None);
    }

    let state_bytes =
        fs::read(&paths.conductor_state_config).map_err(|source| MediaPmError::Io {
            operation: "reading conductor runtime state document for materialization".to_string(),
            path: paths.conductor_state_config.clone(),
            source,
        })?;

    if state_bytes.iter().all(u8::is_ascii_whitespace) {
        return Ok(None);
    }

    let state_document = decode_state_document(&state_bytes).map_err(|error| {
        MediaPmError::Workflow(format!(
            "decoding conductor runtime state document '{}' failed: {error}",
            paths.conductor_state_config.display()
        ))
    })?;
    let Some(state_pointer) = state_document.state_pointer else {
        return Ok(None);
    };

    let state_blob = match cas.get(state_pointer).await {
        Ok(bytes) => bytes,
        Err(_) => {
            return Ok(None);
        }
    };

    let orchestration_state =
        serde_json::from_slice::<OrchestrationState>(&state_blob).map_err(|error| {
            MediaPmError::Serialization(format!(
                "decoding persisted orchestration-state blob '{}' failed: {error}",
                state_pointer
            ))
        })?;

    Ok(Some(orchestration_state))
}

/// Resolves one hierarchy source reference.
fn resolve_hierarchy_source<'a>(
    document: &'a MediaPmDocument,
    entry: &HierarchyEntry,
) -> Result<&'a MediaSourceSpec, MediaPmError> {
    document.media.get(&entry.media_id).ok_or_else(|| {
        MediaPmError::Workflow(format!(
            "hierarchy references unknown media id '{}'",
            entry.media_id
        ))
    })
}

/// Resolves one source variant into concrete bytes for staging.
async fn resolve_variant_source_bytes(
    lookup: &MaterializationLookupContext<'_>,
    media_id: &str,
    source: &MediaSourceSpec,
    variant: &str,
) -> Result<(Vec<u8>, Option<String>), MediaPmError> {
    let source_uri = media_source_uri(media_id, source);

    if let Some(state) = lookup.orchestration_state
        && let Some((workflow_hash, fallback_notice)) =
            resolve_variant_hash_from_workflow_state(lookup, state, media_id, source, variant)
                .await?
    {
        let bytes = lookup.cas.get(workflow_hash).await.map_err(|source| {
            MediaPmError::Workflow(format!(
                "workflow output hash '{}' for '{}' variant '{}' is missing from CAS: {source}",
                workflow_hash, source_uri, variant
            ))
        })?;

        let materialized_bytes = if let Some(binding) =
            resolve_media_variant_output_binding_with_limits(
                source,
                variant,
                lookup.ffmpeg_max_input_slots,
                lookup.ffmpeg_max_output_slots,
            )? {
            if let Some(zip_member) = binding.zip_member.as_deref() {
                extract_zip_member_bytes(bytes.as_ref(), zip_member).map_err(|error| {
                    MediaPmError::Workflow(format!(
                        "extracting ZIP member '{zip_member}' for '{}' variant '{}' failed: {error}",
                        source_uri, variant
                    ))
                })?
            } else {
                bytes.as_ref().to_vec()
            }
        } else {
            bytes.as_ref().to_vec()
        };

        return Ok((materialized_bytes, fallback_notice));
    }

    if !source.variant_hashes.is_empty() {
        let hash_string = source
            .variant_hashes
            .get(variant)
            .or_else(|| source.variant_hashes.get("default"))
            .ok_or_else(|| {
                MediaPmError::Workflow(format!(
                    "local source '{}' does not define hash pointer for variant '{}'",
                    source_uri, variant
                ))
            })?
            .clone();

        let hash = Hash::from_str(&hash_string).map_err(|_| {
            MediaPmError::Workflow(format!(
                "local source '{}' variant '{}' has invalid CAS hash '{}': expected multihash string",
                source_uri, variant, hash_string
            ))
        })?;

        match lookup.cas.get(hash).await {
            Ok(bytes) => {
                if source.variant_hashes.contains_key(variant) {
                    Ok((bytes.as_ref().to_vec(), None))
                } else {
                    Ok((
                        bytes.as_ref().to_vec(),
                        Some(format!(
                            "variant '{}' missing for '{}'; used fallback variant 'default'",
                            variant, source_uri
                        )),
                    ))
                }
            }
            Err(source) => Err(MediaPmError::Workflow(format!(
                "CAS hash '{}' for '{}' variant '{}' is missing from CAS: {source}",
                hash, source_uri, variant
            ))),
        }
    } else {
        Err(MediaPmError::Workflow(format!(
            "source '{}' variant '{}' has no local variant hashes and no workflow output hash resolved from runtime state",
            source_uri, variant
        )))
    }
}

/// Resolves one materialization variant hash from workflow runtime outputs.
///
/// Returns `None` when the target variant is not produced by a workflow step,
/// when no managed workflow exists for the source, or when matching runtime
/// step instances are unavailable in current orchestration state.
async fn resolve_variant_hash_from_workflow_state(
    lookup: &MaterializationLookupContext<'_>,
    state: &OrchestrationState,
    media_id: &str,
    source: &MediaSourceSpec,
    variant: &str,
) -> Result<Option<(Hash, Option<String>)>, MediaPmError> {
    let Some(binding) = resolve_media_variant_output_binding_with_limits(
        source,
        variant,
        lookup.ffmpeg_max_input_slots,
        lookup.ffmpeg_max_output_slots,
    )?
    else {
        return Ok(None);
    };

    let workflow_id = managed_workflow_id_for_media(media_id, source);
    let Some(workflow) = lookup.machine.workflows.get(&workflow_id) else {
        return Ok(None);
    };

    let Some(step_output_hashes) =
        resolve_workflow_step_output_hashes(lookup.cas, lookup.machine, state, workflow).await?
    else {
        return Ok(None);
    };

    let output_hash = step_output_hashes
        .get(&binding.step_id)
        .and_then(|outputs| outputs.get(&binding.output_name))
        .copied();

    let Some(hash) = output_hash else {
        return Ok(None);
    };

    let fallback_notice = if binding.used_default_variant {
        Some(format!(
            "variant '{}' missing for media '{}'; used workflow fallback variant 'default'",
            variant, media_id
        ))
    } else {
        None
    };

    Ok(Some((hash, fallback_notice)))
}

/// Resolves concrete output hashes for each workflow step using orchestration state.
///
/// Steps are matched by immutable tool id, canonical tool metadata, and exact
/// resolved input hash identities. `None` is returned when a required step
/// instance cannot be resolved from the current persisted orchestration state.
async fn resolve_workflow_step_output_hashes(
    cas: &FileSystemCas,
    machine: &MachineNickelDocument,
    state: &OrchestrationState,
    workflow: &mediapm_conductor::WorkflowSpec,
) -> Result<Option<StepOutputHashes>, MediaPmError> {
    let mut step_outputs = StepOutputHashes::new();

    for step in &workflow.steps {
        let expected_inputs =
            resolve_expected_input_hashes(cas, machine, &step.inputs, &step_outputs).await?;
        let Some(expected_inputs) = expected_inputs else {
            return Ok(None);
        };

        let expected_metadata = machine.tools.get(&step.tool).ok_or_else(|| {
            MediaPmError::Workflow(format!(
                "workflow step '{}' references unknown tool '{}' in machine config",
                step.id, step.tool
            ))
        })?;

        let matching_instances = state
            .instances
            .iter()
            .filter_map(|(instance_id, instance)| {
                (instance.tool_name == step.tool
                    && tool_metadata_matches(expected_metadata, &instance.metadata)
                    && instance_matches_expected_inputs(instance, &expected_inputs)
                    && instance_matches_expected_output_names(instance, &step.outputs))
                .then_some((instance_id, instance))
            })
            .collect::<Vec<_>>();

        let Some((_, instance)) = matching_instances.first().copied() else {
            return Ok(None);
        };

        let output_hashes = instance
            .outputs
            .iter()
            .map(|(name, output)| (name.clone(), output.hash))
            .collect::<BTreeMap<_, _>>();
        step_outputs.insert(step.id.clone(), output_hashes);
    }

    Ok(Some(step_outputs))
}

/// Returns true when two tool metadata specs represent the same runtime tool.
///
/// Builtin metadata persisted in orchestration state intentionally remains in a
/// strict minimal wire shape (`kind`/`name`/`version`) while decoded machine
/// config builtins may carry runtime defaults for `is_impure` and outputs.
/// Materializer instance matching must therefore compare builtin identity by
/// name/version only, while executable tools keep full-struct equality.
fn tool_metadata_matches(expected: &ToolSpec, actual: &ToolSpec) -> bool {
    match (&expected.kind, &actual.kind) {
        (
            ToolKindSpec::Builtin { name: expected_name, version: expected_version },
            ToolKindSpec::Builtin { name: actual_name, version: actual_version },
        ) => expected_name == actual_name && expected_version == actual_version,
        (ToolKindSpec::Executable { .. }, ToolKindSpec::Executable { .. }) => expected == actual,
        _ => false,
    }
}

/// Resolves expected input hashes for one workflow step from concrete bindings.
///
/// Returns `None` when the step depends on unresolved prior step output hashes.
async fn resolve_expected_input_hashes(
    cas: &FileSystemCas,
    machine: &MachineNickelDocument,
    step_inputs: &BTreeMap<String, InputBinding>,
    step_outputs: &StepOutputHashes,
) -> Result<Option<BTreeMap<String, Hash>>, MediaPmError> {
    let mut expected = BTreeMap::new();

    for (input_name, binding) in step_inputs {
        let Some(hash) = resolve_input_binding_hash(cas, machine, binding, step_outputs).await?
        else {
            return Ok(None);
        };
        expected.insert(input_name.clone(), hash);
    }

    Ok(Some(expected))
}

/// Resolves one workflow input binding into deterministic input hash identity.
///
/// This mirrors conductor runtime semantics:
/// - scalar values hash raw UTF-8 bytes,
/// - list values hash canonical JSON encoding,
/// - `${external_data.<hash>}` resolves directly to the declared CAS hash,
/// - `${step_output.<step_id>.<output_name>}` resolves from prior step outputs.
/// - `${step_output.<step_id>.<output_name>:zip(<member>)}` resolves ZIP
///   member bytes from that output before hashing.
async fn resolve_input_binding_hash(
    cas: &FileSystemCas,
    machine: &MachineNickelDocument,
    binding: &InputBinding,
    step_outputs: &StepOutputHashes,
) -> Result<Option<Hash>, MediaPmError> {
    match binding {
        InputBinding::String(value) => {
            if let Some(reference) = parse_step_output_reference(value) {
                let Some(hash) = step_outputs
                    .get(reference.step_id)
                    .and_then(|outputs| outputs.get(reference.output_name))
                    .copied()
                else {
                    return Ok(None);
                };

                if let Some(zip_member) = reference.zip_member {
                    let zip_bytes = cas.get(hash).await.map_err(|source| {
                        MediaPmError::Workflow(format!(
                            "reading step output '{}' from step '{}' for ZIP selector failed: {source}",
                            reference.output_name, reference.step_id
                        ))
                    })?;
                    let member_bytes = extract_zip_member_bytes(zip_bytes.as_ref(), zip_member)
                        .map_err(|error| {
                            MediaPmError::Workflow(format!(
                                "extracting ZIP member '{zip_member}' from '${{step_output.{}.{}}}' failed: {error}",
                                reference.step_id, reference.output_name
                            ))
                        })?;
                    return Ok(Some(Hash::from_content(member_bytes.as_slice())));
                }

                return Ok(Some(hash));
            }

            if let Some(external_hash) = parse_external_data_reference(value)? {
                return machine.external_data.contains_key(&external_hash).then_some(external_hash).map(Some).ok_or_else(|| {
                    MediaPmError::Workflow(format!(
                        "workflow binding references unknown external_data hash '{external_hash}'"
                    ))
                });
            }

            Ok(Some(Hash::from_content(value.as_bytes())))
        }
        InputBinding::StringList(values) => {
            let encoded = serde_json::to_vec(values).map_err(|error| {
                MediaPmError::Serialization(format!(
                    "encoding workflow string-list binding for deterministic hash resolution failed: {error}"
                ))
            })?;
            Ok(Some(Hash::from_content(encoded.as_slice())))
        }
    }
}

/// Returns true when one runtime instance contains all expected input hashes.
///
/// Runtime may inject additional resolved inputs from tool-level defaults.
/// Materialization matching therefore treats step-declared bindings as a
/// required subset instead of requiring exact key-set equality.
fn instance_matches_expected_inputs(
    instance: &ToolCallInstance,
    expected_inputs: &BTreeMap<String, Hash>,
) -> bool {
    expected_inputs.iter().all(|(name, hash)| {
        instance.inputs.get(name).map(|resolved| resolved.hash == *hash).unwrap_or(false)
    })
}

/// Returns true when one runtime instance exposes all step-declared outputs.
///
/// Multiple equivalent tool calls can share tool identity and resolved input
/// hashes while persisting different output families (for example `yt-dlp`
/// primary content vs sidecar-only captures). Materialization must constrain
/// matching to instances that provide the workflow step's expected output keys.
fn instance_matches_expected_output_names(
    instance: &ToolCallInstance,
    expected_outputs: &BTreeMap<String, mediapm_conductor::OutputPolicy>,
) -> bool {
    expected_outputs.keys().all(|output_name| instance.outputs.contains_key(output_name))
}

/// Parsed `${step_output...}` binding reference metadata.
struct StepOutputReference<'a> {
    /// Producer step id.
    step_id: &'a str,
    /// Producer output name.
    output_name: &'a str,
    /// Optional ZIP-member selector.
    zip_member: Option<&'a str>,
}

/// Parses exact `${step_output.<step_id>.<output_name>}` references with
/// optional `${step_output.<step_id>.<output_name>:zip(<member>)}` selector.
fn parse_step_output_reference(value: &str) -> Option<StepOutputReference<'_>> {
    let content = value.strip_prefix("${step_output.")?.strip_suffix('}')?;

    let (selector, zip_member) = if let Some(without_suffix) = content.strip_suffix(')') {
        if let Some((prefix, member)) = without_suffix.rsplit_once(":zip(") {
            if member.is_empty() || member.contains('/') || member.contains('\\') {
                return None;
            }
            (prefix, Some(member))
        } else {
            (content, None)
        }
    } else {
        (content, None)
    };

    let (step_id, output_name) = selector.rsplit_once('.')?;
    if step_id.is_empty() || output_name.is_empty() {
        return None;
    }

    Some(StepOutputReference { step_id, output_name, zip_member })
}

/// Extracts one file payload from ZIP bytes using one flat member key.
fn extract_zip_member_bytes(zip_bytes: &[u8], member_key: &str) -> Result<Vec<u8>, String> {
    if member_key.is_empty() || member_key.contains('/') || member_key.contains('\\') {
        return Err(
            "ZIP member key must be non-empty and must not contain path separators".to_string()
        );
    }

    let reader = std::io::Cursor::new(zip_bytes);
    let mut archive = zip::ZipArchive::new(reader)
        .map_err(|error| format!("decoding ZIP payload failed: {error}"))?;

    let mut index = 0usize;
    while index < archive.len() {
        let mut entry = archive
            .by_index(index)
            .map_err(|error| format!("reading ZIP entry #{index} failed: {error}"))?;
        let entry_name = entry.name().replace('\\', "/");
        if entry_name == member_key {
            if entry.is_dir() {
                return Err(format!("ZIP member '{member_key}' resolves to a directory"));
            }
            let mut bytes = Vec::new();
            entry
                .read_to_end(&mut bytes)
                .map_err(|error| format!("reading ZIP member '{member_key}' failed: {error}"))?;
            return Ok(bytes);
        }
        index = index.saturating_add(1);
    }

    Err(format!("ZIP member '{member_key}' not found"))
}

/// Extracts one ZIP folder payload into a staged directory with merge checks.
///
/// Multiple hierarchy variants may contribute archive entries into the same
/// destination directory. This helper enforces strict path-collision rules so
/// no file or directory path can be overwritten by a later variant.
fn extract_zip_folder_variant_bytes(
    zip_bytes: &[u8],
    target_dir: &Path,
    hierarchy_path: &str,
    media_id: &str,
    variant: &str,
    extracted_entries: &mut BTreeMap<String, bool>,
) -> Result<(), MediaPmError> {
    let reader = std::io::Cursor::new(zip_bytes);
    let mut archive = zip::ZipArchive::new(reader).map_err(|error| {
        MediaPmError::Workflow(format!(
            "hierarchy path '{hierarchy_path}' media '{media_id}' variant '{variant}' is expected to be a ZIP folder payload: {error}"
        ))
    })?;

    for index in 0..archive.len() {
        let mut entry = archive.by_index(index).map_err(|error| {
            MediaPmError::Workflow(format!(
                "reading ZIP entry #{index} for hierarchy path '{hierarchy_path}' media '{media_id}' variant '{variant}' failed: {error}"
            ))
        })?;

        let normalized = normalize_zip_entry_relative_path(entry.name()).map_err(|reason| {
            MediaPmError::Workflow(format!(
                "invalid ZIP entry '{}' for hierarchy path '{hierarchy_path}' media '{media_id}' variant '{variant}': {reason}",
                entry.name()
            ))
        })?;

        if normalized.is_empty() {
            continue;
        }

        if entry.is_dir() {
            register_zip_directory_entry(&normalized, extracted_entries).map_err(|reason| {
                MediaPmError::Workflow(format!(
                    "directory merge conflict for hierarchy path '{hierarchy_path}' media '{media_id}' variant '{variant}': {reason}"
                ))
            })?;

            let directory_path = target_dir.join(&normalized);
            fs::create_dir_all(&directory_path).map_err(|source| MediaPmError::Io {
                operation: "creating staged hierarchy directory from ZIP payload".to_string(),
                path: directory_path,
                source,
            })?;
        } else {
            register_zip_file_entry(&normalized, extracted_entries).map_err(|reason| {
                MediaPmError::Workflow(format!(
                    "file merge conflict for hierarchy path '{hierarchy_path}' media '{media_id}' variant '{variant}': {reason}"
                ))
            })?;

            let file_path = target_dir.join(&normalized);
            if let Some(parent) = file_path.parent() {
                fs::create_dir_all(parent).map_err(|source| MediaPmError::Io {
                    operation: "creating staged hierarchy file parent from ZIP payload".to_string(),
                    path: parent.to_path_buf(),
                    source,
                })?;
            }

            let mut bytes = Vec::new();
            entry.read_to_end(&mut bytes).map_err(|error| {
                MediaPmError::Workflow(format!(
                    "reading ZIP file entry '{}' for hierarchy path '{hierarchy_path}' media '{media_id}' variant '{variant}' failed: {error}",
                    entry.name()
                ))
            })?;

            fs::write(&file_path, bytes).map_err(|source| MediaPmError::Io {
                operation: "writing staged hierarchy file from ZIP payload".to_string(),
                path: file_path,
                source,
            })?;
        }
    }

    Ok(())
}

/// Normalizes one ZIP entry path into a safe relative path.
fn normalize_zip_entry_relative_path(entry_name: &str) -> Result<String, String> {
    let mut normalized = entry_name.replace('\\', "/");
    while let Some(stripped) = normalized.strip_prefix('/') {
        normalized = stripped.to_string();
    }
    while let Some(stripped) = normalized.strip_prefix("./") {
        normalized = stripped.to_string();
    }

    let mut components = Vec::new();
    for segment in normalized.split('/') {
        if segment.is_empty() {
            continue;
        }
        if segment == "." || segment == ".." {
            return Err("contains '.' or '..' path components".to_string());
        }
        if segment.contains(':') {
            return Err("contains ':' path segment characters".to_string());
        }
        components.push(segment);
    }

    Ok(components.join("/"))
}

/// Registers one ZIP directory path, rejecting file/directory collisions.
fn register_zip_directory_entry(
    entry_path: &str,
    extracted_entries: &mut BTreeMap<String, bool>,
) -> Result<(), String> {
    let mut cursor = String::new();

    for segment in entry_path.split('/') {
        if !cursor.is_empty() {
            cursor.push('/');
        }
        cursor.push_str(segment);

        match extracted_entries.get(&cursor).copied() {
            Some(true) => {}
            Some(false) => {
                return Err(format!(
                    "directory '{entry_path}' conflicts with existing file '{cursor}'"
                ));
            }
            None => {
                extracted_entries.insert(cursor.clone(), true);
            }
        }
    }

    Ok(())
}

/// Registers one ZIP file path, rejecting duplicates and file/dir collisions.
fn register_zip_file_entry(
    entry_path: &str,
    extracted_entries: &mut BTreeMap<String, bool>,
) -> Result<(), String> {
    let mut parts = entry_path.split('/').collect::<Vec<_>>();
    if parts.is_empty() {
        return Err("file entry path is empty".to_string());
    }

    let file_name = parts.pop().expect("checked non-empty split result");
    let mut parent = String::new();

    for segment in parts {
        if !parent.is_empty() {
            parent.push('/');
        }
        parent.push_str(segment);

        match extracted_entries.get(&parent).copied() {
            Some(true) => {}
            Some(false) => {
                return Err(format!(
                    "file '{entry_path}' has parent '{parent}' that is already a file"
                ));
            }
            None => {
                extracted_entries.insert(parent.clone(), true);
            }
        }
    }

    let full_file_path =
        if parent.is_empty() { file_name.to_string() } else { format!("{parent}/{file_name}") };

    match extracted_entries.get(&full_file_path).copied() {
        Some(true) => {
            Err(format!("file '{entry_path}' conflicts with existing directory '{full_file_path}'"))
        }
        Some(false) => {
            Err(format!("file '{entry_path}' duplicates existing file '{full_file_path}'"))
        }
        None => {
            extracted_entries.insert(full_file_path, false);
            Ok(())
        }
    }
}

/// Parses exact `${external_data.<hash>}` references.
fn parse_external_data_reference(value: &str) -> Result<Option<Hash>, MediaPmError> {
    let Some(hash_text) =
        value.strip_prefix("${external_data.").and_then(|text| text.strip_suffix('}'))
    else {
        return Ok(None);
    };

    if hash_text.is_empty() {
        return Err(MediaPmError::Workflow(
            "workflow binding '${external_data.<hash>}' requires a non-empty hash".to_string(),
        ));
    }

    let hash = Hash::from_str(hash_text).map_err(|source| {
        MediaPmError::Workflow(format!(
            "workflow binding references invalid external_data hash '{hash_text}': {source}"
        ))
    })?;
    Ok(Some(hash))
}

/// Removes one path recursively when it is a directory, or as one file otherwise.
fn remove_path(path: &Path) -> Result<(), MediaPmError> {
    clear_path_readonly_recursively(path)?;

    let metadata = fs::symlink_metadata(path).map_err(|source| MediaPmError::Io {
        operation: "reading path metadata before removal".to_string(),
        path: path.to_path_buf(),
        source,
    })?;

    if metadata.is_dir() {
        fs::remove_dir_all(path).map_err(|source| MediaPmError::Io {
            operation: "removing stale directory".to_string(),
            path: path.to_path_buf(),
            source,
        })
    } else {
        fs::remove_file(path).map_err(|source| MediaPmError::Io {
            operation: "removing stale file".to_string(),
            path: path.to_path_buf(),
            source,
        })
    }
}

/// Marks one managed output path as read-only after successful commit.
///
/// For directory outputs, this recursively marks descendant files/directories
/// read-only. For symlinks, permissions are applied to the resolved target.
fn ensure_managed_path_readonly(path: &Path) -> Result<(), MediaPmError> {
    let metadata = fs::symlink_metadata(path).map_err(|source| MediaPmError::Io {
        operation: "reading managed output metadata before readonly enforcement".to_string(),
        path: path.to_path_buf(),
        source,
    })?;

    if metadata.is_dir() {
        for entry in fs::read_dir(path).map_err(|source| MediaPmError::Io {
            operation: "reading managed output directory before readonly enforcement".to_string(),
            path: path.to_path_buf(),
            source,
        })? {
            let entry = entry.map_err(|source| MediaPmError::Io {
                operation: "iterating managed output directory before readonly enforcement"
                    .to_string(),
                path: path.to_path_buf(),
                source,
            })?;
            ensure_managed_path_readonly(&entry.path())?;
        }
    }

    let mut permissions = fs::metadata(path)
        .map_err(|source| MediaPmError::Io {
            operation: "reading managed output permissions before readonly enforcement".to_string(),
            path: path.to_path_buf(),
            source,
        })?
        .permissions();
    if !permissions.readonly() {
        permissions.set_readonly(true);
        fs::set_permissions(path, permissions).map_err(|source| MediaPmError::Io {
            operation: "marking managed output path readonly".to_string(),
            path: path.to_path_buf(),
            source,
        })?;
    }

    Ok(())
}

/// Clears read-only bit recursively so stale managed paths can be removed.
fn clear_path_readonly_recursively(path: &Path) -> Result<(), MediaPmError> {
    let metadata = fs::symlink_metadata(path).map_err(|source| MediaPmError::Io {
        operation: "reading path metadata before readonly clear".to_string(),
        path: path.to_path_buf(),
        source,
    })?;

    if metadata.is_dir() {
        for entry in fs::read_dir(path).map_err(|source| MediaPmError::Io {
            operation: "reading directory before readonly clear".to_string(),
            path: path.to_path_buf(),
            source,
        })? {
            let entry = entry.map_err(|source| MediaPmError::Io {
                operation: "iterating directory before readonly clear".to_string(),
                path: path.to_path_buf(),
                source,
            })?;
            clear_path_readonly_recursively(&entry.path())?;
        }
    }

    let mut permissions = fs::metadata(path)
        .map_err(|source| MediaPmError::Io {
            operation: "reading path permissions before readonly clear".to_string(),
            path: path.to_path_buf(),
            source,
        })?
        .permissions();
    if permissions.readonly() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let mode = permissions.mode();
            let writable_mode = mode | 0o200;
            if writable_mode != mode {
                permissions.set_mode(writable_mode);
            }
        }

        #[cfg(not(unix))]
        {
            #[allow(clippy::permissions_set_readonly_false)]
            {
                permissions.set_readonly(false);
            }
        }

        fs::set_permissions(path, permissions).map_err(|source| MediaPmError::Io {
            operation: "clearing readonly bit before managed-path removal".to_string(),
            path: path.to_path_buf(),
            source,
        })?;
    }

    Ok(())
}

/// Validates one relative hierarchy path against Phase 3 invariants.
fn validate_hierarchy_path(relative_path: &str) -> Result<(), MediaPmError> {
    let path = Path::new(relative_path);

    if path.is_absolute() {
        return Err(MediaPmError::Workflow(format!(
            "hierarchy path '{}' must be relative",
            relative_path
        )));
    }

    for component in path.components() {
        let segment = component.as_os_str().to_string_lossy();
        if segment == "." || segment == ".." {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy path '{}' must not contain '.' or '..' components",
                relative_path
            )));
        }

        if segment.chars().any(is_rejected_char) {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy path '{}' contains forbidden characters",
                relative_path
            )));
        }

        let nfd = segment.nfd().collect::<String>();
        if nfd != segment {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy path '{}' is not Unicode NFD normalized",
                relative_path
            )));
        }
    }

    Ok(())
}

/// Returns whether one character is forbidden by cross-platform filename rules.
fn is_rejected_char(ch: char) -> bool {
    matches!(ch, '<' | '>' | ':' | '"' | '|' | '?' | '*')
}

/// Returns current Unix epoch timestamp in seconds.
fn now_unix_seconds() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

/// Returns current Unix epoch timestamp in milliseconds.
fn unix_epoch_millis() -> u64 {
    let millis = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis();
    u64::try_from(millis).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use mediapm_cas::{CasApi, FileSystemCas, Hash};
    use mediapm_conductor::model::config::ToolInputKind;
    use mediapm_conductor::{
        InputBinding, MachineNickelDocument, OrchestrationState, OutputCaptureSpec, OutputPolicy,
        OutputRef, PersistenceFlags, StateNickelDocument, ToolCallInstance, ToolInputSpec,
        ToolKindSpec, ToolOutputSpec, ToolSpec, WorkflowSpec, WorkflowStepSpec,
        encode_state_document,
    };

    use crate::config::{
        HierarchyEntry, MediaMetadataValue, MediaMetadataVariantBinding, MediaPmDocument,
        MediaSourceSpec, MediaStep, MediaStepTool, TransformInputValue,
    };
    use crate::lockfile::MediaLockFile;
    use crate::paths::MediaPmPaths;

    use super::{instance_matches_expected_inputs, sync_hierarchy, validate_hierarchy_path};

    fn yt_dlp_output_variant(kind: &str) -> serde_json::Value {
        serde_json::json!({ "kind": kind, "save": true, "save_full": true })
    }

    /// Protects strict forbidden-character enforcement in hierarchy paths.
    #[test]
    fn hierarchy_path_rejects_forbidden_characters() {
        let err = validate_hierarchy_path("movies/Star:Wars.mkv").expect_err("path should fail");
        assert!(err.to_string().contains("forbidden characters"));
    }

    /// Protects NFD-only normalization policy for hierarchy path segments.
    #[test]
    fn hierarchy_path_rejects_non_nfd_segments() {
        let err = validate_hierarchy_path("movies/épisode.mkv").expect_err("NFD should fail");
        assert!(err.to_string().contains("NFD"));
    }

    /// Protects stage-commit materialization for local source entries.
    #[tokio::test]
    async fn sync_hierarchy_materializes_local_source_from_cas_variant_pointer() {
        let temp = tempfile::tempdir().expect("tempdir");
        let paths = MediaPmPaths::from_root(temp.path());
        let cas_root = paths.root_dir.join(".mediapm").join("store");
        let cas = FileSystemCas::open(&cas_root).await.expect("open cas");
        let hash = cas.put(b"abc".to_vec()).await.expect("put local bytes");

        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "media-a".to_string(),
                MediaSourceSpec {
                    description: Some("file: source.bin".to_string()),
                    workflow_id: None,
                    metadata: None,
                    variant_hashes: BTreeMap::from([("default".to_string(), hash.to_string())]),
                    steps: Vec::new(),
                },
            )]),
            hierarchy: BTreeMap::from([(
                "library/media-a.bin".to_string(),
                HierarchyEntry {
                    media_id: "media-a".to_string(),
                    variants: vec!["default".to_string()],
                },
            )]),
            ..MediaPmDocument::default()
        };

        let mut lock = MediaLockFile::default();
        let report = sync_hierarchy(
            &paths,
            &document,
            &MachineNickelDocument::default(),
            &cas_root,
            &mut lock,
        )
        .await
        .expect("sync hierarchy");

        assert_eq!(report.materialized_paths, 1);
        assert!(paths.library_dir.join("library/media-a.bin").exists());
        let metadata =
            std::fs::metadata(paths.library_dir.join("library/media-a.bin")).expect("metadata");
        assert!(metadata.permissions().readonly(), "managed file should be readonly");
        assert_eq!(
            std::fs::read(paths.library_dir.join("library/media-a.bin")).expect("read output"),
            b"abc"
        );
        let record = lock.managed_files.get("library/media-a.bin").expect("managed record");
        assert_eq!(record.media_id, "media-a");
    }

    /// Protects hierarchy placeholder interpolation for literal metadata values.
    #[tokio::test]
    async fn sync_hierarchy_interpolates_literal_media_metadata_placeholders() {
        let temp = tempfile::tempdir().expect("tempdir");
        let paths = MediaPmPaths::from_root(temp.path());
        let cas_root = paths.root_dir.join(".mediapm").join("store");
        let cas = FileSystemCas::open(&cas_root).await.expect("open cas");
        let hash = cas.put(b"abc".to_vec()).await.expect("put local bytes");

        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "media-a".to_string(),
                MediaSourceSpec {
                    description: Some("file: source.bin".to_string()),
                    workflow_id: None,
                    metadata: Some(BTreeMap::from([(
                        "title".to_string(),
                        MediaMetadataValue::Literal("Demo Title".to_string()),
                    )])),
                    variant_hashes: BTreeMap::from([("default".to_string(), hash.to_string())]),
                    steps: Vec::new(),
                },
            )]),
            hierarchy: BTreeMap::from([(
                "library/${media.metadata.title}.bin".to_string(),
                HierarchyEntry {
                    media_id: "media-a".to_string(),
                    variants: vec!["default".to_string()],
                },
            )]),
            ..MediaPmDocument::default()
        };

        let mut lock = MediaLockFile::default();
        let report = sync_hierarchy(
            &paths,
            &document,
            &MachineNickelDocument::default(),
            &cas_root,
            &mut lock,
        )
        .await
        .expect("sync hierarchy");

        assert_eq!(report.materialized_paths, 1);
        assert!(paths.library_dir.join("library/Demo Title.bin").exists());
    }

    /// Protects hierarchy placeholder interpolation for variant-backed
    /// metadata extraction from JSON sidecar payloads.
    #[tokio::test]
    async fn sync_hierarchy_interpolates_variant_backed_media_metadata_placeholders() {
        let temp = tempfile::tempdir().expect("tempdir");
        let paths = MediaPmPaths::from_root(temp.path());
        let cas_root = paths.root_dir.join(".mediapm").join("store");
        let cas = FileSystemCas::open(&cas_root).await.expect("open cas");
        let audio_hash = cas.put(b"audio-bytes".to_vec()).await.expect("put audio bytes");
        let infojson_hash =
            cas.put(br#"{"title":"Variant Title"}"#.to_vec()).await.expect("put infojson bytes");

        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "media-a".to_string(),
                MediaSourceSpec {
                    description: Some("file: source.bin".to_string()),
                    workflow_id: None,
                    metadata: Some(BTreeMap::from([(
                        "title".to_string(),
                        MediaMetadataValue::Variant(MediaMetadataVariantBinding {
                            variant: "infojson".to_string(),
                            metadata_key: "title".to_string(),
                        }),
                    )])),
                    variant_hashes: BTreeMap::from([
                        ("audio".to_string(), audio_hash.to_string()),
                        ("infojson".to_string(), infojson_hash.to_string()),
                    ]),
                    steps: Vec::new(),
                },
            )]),
            hierarchy: BTreeMap::from([(
                "library/${media.metadata.title}.bin".to_string(),
                HierarchyEntry {
                    media_id: "media-a".to_string(),
                    variants: vec!["audio".to_string()],
                },
            )]),
            ..MediaPmDocument::default()
        };

        let mut lock = MediaLockFile::default();
        let report = sync_hierarchy(
            &paths,
            &document,
            &MachineNickelDocument::default(),
            &cas_root,
            &mut lock,
        )
        .await
        .expect("sync hierarchy");

        assert_eq!(report.materialized_paths, 1);
        assert!(paths.library_dir.join("library/Variant Title.bin").exists());
        assert_eq!(
            std::fs::read(paths.library_dir.join("library/Variant Title.bin"))
                .expect("read output"),
            b"audio-bytes"
        );
    }

    /// Protects stale-path removal for readonly managed files on Windows/Linux.
    #[test]
    fn remove_path_handles_readonly_files() {
        let temp = tempfile::tempdir().expect("tempdir");
        let file_path = temp.path().join("readonly.txt");
        std::fs::write(&file_path, b"x").expect("write file");

        let mut permissions = std::fs::metadata(&file_path).expect("metadata").permissions();
        permissions.set_readonly(true);
        std::fs::set_permissions(&file_path, permissions).expect("set readonly");

        super::remove_path(&file_path).expect("remove readonly file");
        assert!(!file_path.exists());
    }

    /// Protects online-source materialization by resolving workflow output hashes
    /// from persisted orchestration state instead of writing placeholders.
    #[tokio::test]
    async fn sync_hierarchy_materializes_online_variant_from_workflow_state() {
        let temp = tempfile::tempdir().expect("tempdir");
        let paths = MediaPmPaths::from_root(temp.path());
        let cas_root = paths.root_dir.join(".mediapm").join("store");
        let cas = FileSystemCas::open(&cas_root).await.expect("open cas");

        let media_id = "remote-a";
        let source_uri = "https://example.com/audio";
        let output_bytes = b"ID3workflow-output".to_vec();
        let output_hash = cas.put(output_bytes.clone()).await.expect("put output bytes");

        let tool_id = "mediapm.tools.yt-dlp+github-releases@latest".to_string();
        let tool_spec = ToolSpec {
            kind: ToolKindSpec::Executable {
                command: vec!["yt-dlp.exe".to_string()],
                env_vars: BTreeMap::new(),
                success_codes: vec![0],
            },
            inputs: BTreeMap::from([
                ("source_url".to_string(), ToolInputSpec::default()),
                ("leading_args".to_string(), ToolInputSpec { kind: ToolInputKind::StringList }),
                ("trailing_args".to_string(), ToolInputSpec { kind: ToolInputKind::StringList }),
            ]),
            outputs: BTreeMap::from([(
                "output_content".to_string(),
                ToolOutputSpec {
                    capture: OutputCaptureSpec::File {
                        path: "downloads/yt-dlp-output.media".to_string(),
                    },
                },
            )]),
            ..ToolSpec::default()
        };

        let step_id = "step-0-0-yt-dlp-normalized-to-normalized".to_string();
        let workflow = WorkflowSpec {
            name: Some(media_id.to_string()),
            description: Some("online source".to_string()),
            steps: vec![WorkflowStepSpec {
                id: step_id.clone(),
                tool: tool_id.clone(),
                inputs: BTreeMap::from([
                    ("source_url".to_string(), InputBinding::String(source_uri.to_string())),
                    (
                        "leading_args".to_string(),
                        InputBinding::StringList(vec![
                            "--format".to_string(),
                            "bestaudio/best".to_string(),
                        ]),
                    ),
                    ("trailing_args".to_string(), InputBinding::StringList(Vec::new())),
                ]),
                depends_on: Vec::new(),
                outputs: BTreeMap::new(),
            }],
        };

        let mut machine = MachineNickelDocument::default();
        machine.tools.insert(tool_id.clone(), tool_spec.clone());
        let workflow_id = crate::conductor_bridge::managed_workflow_id_for_media(
            media_id,
            &MediaSourceSpec {
                description: Some("online source".to_string()),
                workflow_id: None,
                metadata: None,
                variant_hashes: BTreeMap::new(),
                steps: vec![MediaStep {
                    tool: MediaStepTool::YtDlp,
                    input_variants: Vec::new(),
                    output_variants: BTreeMap::from([(
                        "normalized".to_string(),
                        yt_dlp_output_variant("primary"),
                    )]),
                    options: BTreeMap::from([
                        ("uri".to_string(), TransformInputValue::String(source_uri.to_string())),
                        (
                            "leading_args".to_string(),
                            TransformInputValue::StringList(vec![
                                "--format".to_string(),
                                "bestaudio/best".to_string(),
                            ]),
                        ),
                    ]),
                }],
            },
        );
        machine.workflows.insert(workflow_id, workflow);

        let instance_inputs = BTreeMap::from([
            (
                "source_url".to_string(),
                mediapm_conductor::ResolvedInput::from_hash(Hash::from_content(
                    source_uri.as_bytes(),
                )),
            ),
            (
                "leading_args".to_string(),
                mediapm_conductor::ResolvedInput::from_string_list(vec![
                    "--format".to_string(),
                    "bestaudio/best".to_string(),
                ])
                .expect("list hash"),
            ),
            (
                "trailing_args".to_string(),
                mediapm_conductor::ResolvedInput::from_string_list(Vec::new())
                    .expect("empty list hash"),
            ),
        ]);

        let state = OrchestrationState {
            version: 1,
            instances: BTreeMap::from([(
                "instance-a".to_string(),
                mediapm_conductor::ToolCallInstance {
                    tool_name: tool_id.clone(),
                    metadata: tool_spec,
                    impure_timestamp: None,
                    inputs: instance_inputs,
                    outputs: BTreeMap::from([(
                        "output_content".to_string(),
                        OutputRef { hash: output_hash, persistence: PersistenceFlags::default() },
                    )]),
                },
            )]),
        };

        let state_blob = serde_json::to_vec(&state).expect("encode state blob");
        let state_pointer = cas.put(state_blob).await.expect("put state blob");
        let encoded_state_document = encode_state_document(StateNickelDocument {
            impure_timestamps: BTreeMap::new(),
            state_pointer: Some(state_pointer),
        })
        .expect("encode state document");

        std::fs::create_dir_all(paths.conductor_state_config.parent().expect("state parent"))
            .expect("create state parent");
        std::fs::write(&paths.conductor_state_config, encoded_state_document)
            .expect("write state document");

        let source = MediaSourceSpec {
            description: Some("online source".to_string()),
            workflow_id: None,
            metadata: None,
            variant_hashes: BTreeMap::new(),
            steps: vec![MediaStep {
                tool: MediaStepTool::YtDlp,
                input_variants: Vec::new(),
                output_variants: BTreeMap::from([(
                    "normalized".to_string(),
                    yt_dlp_output_variant("primary"),
                )]),
                options: BTreeMap::from([
                    ("uri".to_string(), TransformInputValue::String(source_uri.to_string())),
                    (
                        "leading_args".to_string(),
                        TransformInputValue::StringList(vec![
                            "--format".to_string(),
                            "bestaudio/best".to_string(),
                        ]),
                    ),
                ]),
            }],
        };

        let binding =
            crate::conductor_bridge::resolve_media_variant_output_binding(&source, "normalized")
                .expect("resolve variant binding")
                .expect("binding exists");
        assert_eq!(binding.step_id, step_id);
        assert_eq!(binding.output_name, "output_content");

        let document = MediaPmDocument {
            media: BTreeMap::from([(media_id.to_string(), source)]),
            hierarchy: BTreeMap::from([(
                "demo/online.bin".to_string(),
                HierarchyEntry {
                    media_id: media_id.to_string(),
                    variants: vec!["normalized".to_string()],
                },
            )]),
            ..MediaPmDocument::default()
        };

        let mut lock = MediaLockFile::default();
        let report = sync_hierarchy(&paths, &document, &machine, &cas_root, &mut lock)
            .await
            .expect("sync hierarchy");

        assert_eq!(report.materialized_paths, 1);
        assert!(report.notices.is_empty());
        assert_eq!(
            std::fs::read(paths.library_dir.join("demo/online.bin")).expect("read output"),
            output_bytes
        );
    }

    /// Protects strict local-source materialization by failing when the
    /// declared CAS hash pointer is unavailable.
    #[tokio::test]
    async fn sync_hierarchy_fails_when_local_variant_hash_is_missing_from_cas() {
        let temp = tempfile::tempdir().expect("tempdir");
        let paths = MediaPmPaths::from_root(temp.path());
        let cas_root = paths.root_dir.join(".mediapm").join("store");

        let missing_hash = Hash::from_content(b"missing-local-payload");
        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "local-missing".to_string(),
                MediaSourceSpec {
                    description: Some("local source".to_string()),
                    workflow_id: None,
                    metadata: None,
                    variant_hashes: BTreeMap::from([(
                        "default".to_string(),
                        missing_hash.to_string(),
                    )]),
                    steps: Vec::new(),
                },
            )]),
            hierarchy: BTreeMap::from([(
                "demo/local.bin".to_string(),
                HierarchyEntry {
                    media_id: "local-missing".to_string(),
                    variants: vec!["default".to_string()],
                },
            )]),
            ..MediaPmDocument::default()
        };

        let mut lock = MediaLockFile::default();
        let error = sync_hierarchy(
            &paths,
            &document,
            &MachineNickelDocument::default(),
            &cas_root,
            &mut lock,
        )
        .await
        .expect_err("missing local CAS payload must fail materialization");

        let error_text = error.to_string();
        assert!(error_text.contains("variant 'default'"), "unexpected error: {error_text}");
        assert!(error_text.contains("missing") || error_text.contains("not found"));
        assert!(!paths.library_dir.join("demo/local.bin").exists());
    }

    /// Protects strict online-source materialization by failing when no
    /// workflow output hash can be resolved from runtime state.
    #[tokio::test]
    async fn sync_hierarchy_fails_when_online_variant_hash_is_unresolved() {
        let temp = tempfile::tempdir().expect("tempdir");
        let paths = MediaPmPaths::from_root(temp.path());
        let cas_root = paths.root_dir.join(".mediapm").join("store");

        let source_uri = "https://example.com/audio";
        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "remote-unresolved".to_string(),
                MediaSourceSpec {
                    description: Some("online source".to_string()),
                    workflow_id: None,
                    metadata: None,
                    variant_hashes: BTreeMap::new(),
                    steps: vec![MediaStep {
                        tool: MediaStepTool::YtDlp,
                        input_variants: Vec::new(),
                        output_variants: BTreeMap::from([(
                            "normalized".to_string(),
                            yt_dlp_output_variant("primary"),
                        )]),
                        options: BTreeMap::from([(
                            "uri".to_string(),
                            TransformInputValue::String(source_uri.to_string()),
                        )]),
                    }],
                },
            )]),
            hierarchy: BTreeMap::from([(
                "demo/online.bin".to_string(),
                HierarchyEntry {
                    media_id: "remote-unresolved".to_string(),
                    variants: vec!["normalized".to_string()],
                },
            )]),
            ..MediaPmDocument::default()
        };

        let mut lock = MediaLockFile::default();
        let error = sync_hierarchy(
            &paths,
            &document,
            &MachineNickelDocument::default(),
            &cas_root,
            &mut lock,
        )
        .await
        .expect_err("online source without resolved workflow output hash must fail");

        let error_text = error.to_string();
        assert!(error_text.contains(source_uri), "unexpected error: {error_text}");
        assert!(error_text.contains("runtime state") || error_text.contains("workflow"));
        assert!(!paths.library_dir.join("demo/online.bin").exists());
    }

    /// Protects runtime-instance matching by allowing extra runtime-injected
    /// default inputs while still requiring all step-declared input hashes.
    #[test]
    fn instance_matching_allows_extra_runtime_inputs() {
        let expected_text_hash = Hash::from_content(b"hello");
        let expected = BTreeMap::from([("text".to_string(), expected_text_hash)]);

        let instance = ToolCallInstance {
            tool_name: "echo@1.0.0".to_string(),
            metadata: ToolSpec {
                kind: ToolKindSpec::Builtin {
                    name: "echo".to_string(),
                    version: "1.0.0".to_string(),
                },
                ..ToolSpec::default()
            },
            impure_timestamp: None,
            inputs: BTreeMap::from([
                (
                    "text".to_string(),
                    mediapm_conductor::ResolvedInput::from_hash(expected_text_hash),
                ),
                (
                    "leading_args".to_string(),
                    mediapm_conductor::ResolvedInput::from_string_list(vec![
                        "--verbose".to_string(),
                    ])
                    .expect("list hash"),
                ),
            ]),
            outputs: BTreeMap::new(),
        };

        assert!(instance_matches_expected_inputs(&instance, &expected));
    }

    /// Protects runtime step resolution against equivalent-call collisions by
    /// requiring matched instances to expose step-declared output names.
    #[tokio::test]
    async fn resolve_step_output_hashes_matches_instance_with_expected_output_names() {
        let temp = tempfile::tempdir().expect("tempdir");
        let cas = FileSystemCas::open(temp.path()).await.expect("open cas");

        let tool_id = "mediapm.tools.yt-dlp+github-releases@latest".to_string();
        let tool_spec = ToolSpec {
            kind: ToolKindSpec::Executable {
                command: vec!["yt-dlp.exe".to_string()],
                env_vars: BTreeMap::new(),
                success_codes: vec![0],
            },
            inputs: BTreeMap::from([("source_url".to_string(), ToolInputSpec::default())]),
            outputs: BTreeMap::from([
                (
                    "output_content".to_string(),
                    ToolOutputSpec {
                        capture: OutputCaptureSpec::File {
                            path: "downloads/yt-dlp-output.media".to_string(),
                        },
                    },
                ),
                (
                    "yt_dlp_thumbnail_artifacts".to_string(),
                    ToolOutputSpec {
                        capture: OutputCaptureSpec::Folder {
                            path: "downloads".to_string(),
                            include_topmost_folder: false,
                        },
                    },
                ),
            ]),
            ..ToolSpec::default()
        };

        let step_id = "step-0-primary".to_string();
        let source_url = "https://example.com/video".to_string();
        let source_url_hash = Hash::from_content(source_url.as_bytes());

        let workflow = WorkflowSpec {
            name: Some("demo".to_string()),
            description: None,
            steps: vec![WorkflowStepSpec {
                id: step_id.clone(),
                tool: tool_id.clone(),
                inputs: BTreeMap::from([(
                    "source_url".to_string(),
                    InputBinding::String(source_url.clone()),
                )]),
                depends_on: Vec::new(),
                outputs: BTreeMap::from([(
                    "output_content".to_string(),
                    OutputPolicy { save: Some(true), force_full: Some(true) },
                )]),
            }],
        };

        let mut machine = MachineNickelDocument::default();
        machine.tools.insert(tool_id.clone(), tool_spec.clone());

        let state = OrchestrationState {
            version: 1,
            instances: BTreeMap::from([
                (
                    "a-thumbnail-first".to_string(),
                    ToolCallInstance {
                        tool_name: tool_id.clone(),
                        metadata: tool_spec.clone(),
                        impure_timestamp: None,
                        inputs: BTreeMap::from([(
                            "source_url".to_string(),
                            mediapm_conductor::ResolvedInput::from_hash(source_url_hash),
                        )]),
                        outputs: BTreeMap::from([(
                            "yt_dlp_thumbnail_artifacts".to_string(),
                            OutputRef {
                                hash: Hash::from_content(b"thumb-zip"),
                                persistence: PersistenceFlags::default(),
                            },
                        )]),
                    },
                ),
                (
                    "z-primary-second".to_string(),
                    ToolCallInstance {
                        tool_name: tool_id,
                        metadata: tool_spec,
                        impure_timestamp: None,
                        inputs: BTreeMap::from([(
                            "source_url".to_string(),
                            mediapm_conductor::ResolvedInput::from_hash(source_url_hash),
                        )]),
                        outputs: BTreeMap::from([(
                            "output_content".to_string(),
                            OutputRef {
                                hash: Hash::from_content(b"primary-media"),
                                persistence: PersistenceFlags::default(),
                            },
                        )]),
                    },
                ),
            ]),
        };

        let step_output_hashes =
            super::resolve_workflow_step_output_hashes(&cas, &machine, &state, &workflow)
                .await
                .expect("resolve step outputs")
                .expect("step outputs should resolve");

        let output_hash = step_output_hashes
            .get(&step_id)
            .and_then(|outputs| outputs.get("output_content"))
            .copied()
            .expect("output_content hash should resolve from primary instance");
        assert_eq!(output_hash, Hash::from_content(b"primary-media"));
    }
}
