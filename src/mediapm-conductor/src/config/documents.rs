//! Multi-document configuration model.
//!
//! In the simplified multi-doc model, conductor accepts zero to many user
//! configuration documents plus one volatile state document.  Each document is
//! a versioned Nickel envelope parsed independently by its embedded schema
//! version marker and merged in declaration order (conflicts produce errors).
//!
//! This replaces the old three-document model (`UserNickelDocument`,
//! `MachineNickelDocument`, `StateNickelDocument`) with a single unified type.
//!
//! Merging operates on the presence-preserving wire envelope
//! ([`NickelEnvelopeLatest`]) so that explicit values never lose to implicit
//! serde defaults, and human-readable fields (`external_data` descriptions,
//! workflow `display_name`/`description`) are never merged or compared across
//! documents.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use mediapm_cas::Hash;
use serde::{Deserialize, Serialize};

use super::versions::v_latest::{
    ConductorRuntimeConfigLatest, NICKEL_VERSION_LATEST, NickelEnvelopeLatest, OutputPolicyLatest,
    WorkflowSpecLatest,
};
use super::{
    ConductorRuntimeConfig, ToolKindSpec, ToolSpec, WorkflowSpec,
    default_runtime_inherited_env_vars,
};
use crate::error::ConductorError;
use crate::orchestration::protocol::{UnifiedNickelDocument, UnifiedToolSpec};

/// A single evaluated Nickel configuration document.
///
/// In the multi-doc model, each user config file and the volatile state
/// document produce one `NickelDocument`.  These are merged in order during
/// configuration loading.
///
/// Runtime-only fields (concurrency, retries, `content_map`, env overrides)
/// live inline on each [`ToolSpec`] via its [`ToolRuntime`] — there is no
/// separate `tool_runtimes` map.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct NickelDocument {
    /// Tool definitions in this document keyed by tool name.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tools: BTreeMap<String, ToolSpec>,
    /// Workflow definitions in this document.
    #[serde(default)]
    pub workflows: Vec<WorkflowSpec>,
    /// Conductor-level runtime configuration.
    #[serde(default)]
    pub runtime: ConductorRuntimeConfig,
    /// External data entries keyed by CAS hash.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub external_data: BTreeMap<Hash, super::ExternalDataEntry>,
}

/// Prefix reserved for managed external-data descriptions that root tool
/// content-map CAS hashes against pruning.
const MANAGED_TOOL_CONTENT_DESCRIPTION_PREFIX: &str = "managed tool content CAS root for";

/// Collects all CAS hashes referenced by tool content maps AND external data
/// entries in one document.
///
/// Returns a set of every CAS hash value found in any tool's
/// `runtime.content_map` entries plus the keys of the `external_data` map.
#[must_use]
pub fn collect_config_content_hashes(
    tools: &BTreeMap<String, ToolSpec>,
    external_data: &BTreeMap<Hash, super::ExternalDataEntry>,
) -> BTreeSet<Hash> {
    let mut hashes: BTreeSet<Hash> = tools
        .values()
        .flat_map(|spec| spec.runtime.content_map.values())
        .filter_map(|value| {
            // Attempt to parse each content_map value as a CAS hash.
            // Non-hash values (inline descriptions, base64) are skipped.
            value.parse::<Hash>().ok()
        })
        .collect();
    // Include external data entry keys directly.
    hashes.extend(external_data.keys().copied());
    hashes
}

/// A wire envelope paired with its source file path.
///
/// Used during configuration loading to track which file each document
/// originated from — critical for error reporting on merge conflicts.  The
/// envelope is the presence-preserving wire form (boundary defaults NOT yet
/// applied) so merging can distinguish explicit from implicit values.
#[derive(Debug, Clone)]
pub(crate) struct SourceDocument {
    /// Absolute path to the `.ncl` file this document was loaded from.
    pub path: PathBuf,
    /// The parsed latest-schema wire envelope.
    pub envelope: NickelEnvelopeLatest,
}

/// A conflict between two source documents during merge.
#[derive(Debug, Clone)]
pub enum MergeConflict {
    /// Two documents declare the same tool name with incompatible specs.
    DuplicateTool { name: String, first_path: PathBuf, second_path: PathBuf },
    /// Two documents declare the same workflow name with CONFLICTING
    /// definitions. Identical re-declarations (ignoring the human-readable
    /// `display_name`/`description`) merge cleanly instead.
    DuplicateWorkflow { name: String, first_path: PathBuf, second_path: PathBuf },
    /// Two documents explicitly set the same runtime field to conflicting
    /// values (explicit beats implicit; two explicit conflicts error).
    ConflictingRuntime { field: &'static str, first_path: PathBuf, second_path: PathBuf },
}

/// Merges multiple source documents into one unified document.
///
/// Documents are merged in declaration order. The merge is symmetric and
/// order-independent:
///
/// - **Tools** — two documents defining the same tool name error
///   ([`MergeConflict::DuplicateTool`]).
/// - **Workflows** — keyed by name. Two documents declaring the same name
///   with IDENTICAL definitions (ignoring the human-readable
///   `display_name`/`description`, which are never compared) merge into one
///   entry; conflicting definitions error
///   ([`MergeConflict::DuplicateWorkflow`]).
/// - **External data** — unioned by CAS hash. `save` policies merge to the
///   most-permissive explicitly-specified value (`Full` > `Saved` >
///   `Unsaved`); absent `save` is implicit `Saved` and never beats an
///   explicit value. Descriptions are never merged and always stay `None` in
///   the merged result.
/// - **Runtime** — per-field merge of `retry_impure` and
///   `platform_inherited_env_vars` where an explicit value wins over an
///   implicit (absent) one; two explicit CONFLICTING values error
///   ([`MergeConflict::ConflictingRuntime`]).
///
/// Boundary defaults (`save_mode = Saved`, `retry_impure = false`) are
/// applied once at the end, so the returned document is definite for all
/// merge-relevant fields.
///
/// # Errors
///
/// Returns `ConductorError::Workflow` containing all merge conflicts found.
pub(crate) fn merge_documents(docs: &[SourceDocument]) -> Result<NickelDocument, ConductorError> {
    let mut state = MergeState::new();
    for source in docs {
        state.merge_source(source);
    }
    state.finish()
}

/// Accumulated state while folding [`SourceDocument`]s in declaration order.
struct MergeState {
    /// Merged presence-preserving envelope (defaults applied at the end).
    merged: NickelEnvelopeLatest,
    /// Path each tool name was first declared in.
    tool_sources: BTreeMap<String, PathBuf>,
    /// Path each workflow name was first declared in.
    workflow_sources: BTreeMap<String, PathBuf>,
    /// Path of the first document that explicitly set `retry_impure`.
    runtime_retry_source: Option<PathBuf>,
    /// Path of the first document that explicitly set platform env vars.
    runtime_platform_source: Option<PathBuf>,
    /// Conflicts accumulated while merging.
    conflicts: Vec<MergeConflict>,
}

impl MergeState {
    fn new() -> Self {
        Self {
            merged: NickelEnvelopeLatest {
                version: NICKEL_VERSION_LATEST,
                tools: BTreeMap::new(),
                workflows: Vec::new(),
                external_data: BTreeMap::new(),
                runtime: ConductorRuntimeConfigLatest::default(),
            },
            tool_sources: BTreeMap::new(),
            workflow_sources: BTreeMap::new(),
            runtime_retry_source: None,
            runtime_platform_source: None,
            conflicts: Vec::new(),
        }
    }

    /// Folds one source document into the accumulated merge state.
    fn merge_source(&mut self, source: &SourceDocument) {
        self.merge_tools(source);
        self.merge_workflows(source);
        self.merge_external_data(source);
        self.merge_runtime(source);
    }

    fn merge_tools(&mut self, source: &SourceDocument) {
        for (tool_name, tool_spec) in &source.envelope.tools {
            if let Some(first_path) = self.tool_sources.get(tool_name) {
                self.conflicts.push(MergeConflict::DuplicateTool {
                    name: tool_name.clone(),
                    first_path: first_path.clone(),
                    second_path: source.path.clone(),
                });
            } else {
                self.tool_sources.insert(tool_name.clone(), source.path.clone());
                self.merged.tools.insert(tool_name.clone(), tool_spec.clone());
            }
        }
    }

    fn merge_workflows(&mut self, source: &SourceDocument) {
        for workflow in &source.envelope.workflows {
            if let Some(first_path) = self.workflow_sources.get(&workflow.name) {
                // A conflicting duplicate is an error; an identical duplicate
                // is a no-op (the first definition already stands).
                let conflicting = self
                    .merged
                    .workflows
                    .iter()
                    .find(|w| w.name == workflow.name)
                    .is_some_and(|previous| workflow_conflicts(previous, workflow));
                if conflicting {
                    self.conflicts.push(MergeConflict::DuplicateWorkflow {
                        name: workflow.name.clone(),
                        first_path: first_path.clone(),
                        second_path: source.path.clone(),
                    });
                }
            } else {
                self.workflow_sources.insert(workflow.name.clone(), source.path.clone());
                // Human-readable fields are never merged: the merged
                // definition carries none (rule 1).
                let mut merged_workflow = workflow.clone();
                merged_workflow.display_name = None;
                merged_workflow.description = None;
                self.merged.workflows.push(merged_workflow);
            }
        }
    }

    fn merge_external_data(&mut self, source: &SourceDocument) {
        // Union by hash, most-permissive explicit `save`, descriptions never
        // merged.
        for (hash, entry) in &source.envelope.external_data {
            if let Some(existing) = self.merged.external_data.get_mut(hash) {
                existing.save_mode =
                    merge_save_modes(existing.save_mode.take(), entry.save_mode.clone());
            } else {
                let mut merged_entry = entry.clone();
                merged_entry.description = None;
                self.merged.external_data.insert(*hash, merged_entry);
            }
        }
    }

    fn merge_runtime(&mut self, source: &SourceDocument) {
        // Per-field, explicit wins over implicit; two explicit conflicting
        // values error.
        let rt = &source.envelope.runtime;
        match (self.runtime_retry_source.as_ref(), rt.retry_impure) {
            (Some(first_path), Some(value)) if self.merged.runtime.retry_impure != Some(value) => {
                self.conflicts.push(MergeConflict::ConflictingRuntime {
                    field: "retry_impure",
                    first_path: first_path.clone(),
                    second_path: source.path.clone(),
                });
            }
            (None, Some(value)) => {
                self.runtime_retry_source = Some(source.path.clone());
                self.merged.runtime.retry_impure = Some(value);
            }
            _ => {}
        }
        if !rt.platform_inherited_env_vars.is_empty() {
            match self.runtime_platform_source.as_ref() {
                Some(first_path)
                    if self.merged.runtime.platform_inherited_env_vars
                        != rt.platform_inherited_env_vars =>
                {
                    self.conflicts.push(MergeConflict::ConflictingRuntime {
                        field: "platform_inherited_env_vars",
                        first_path: first_path.clone(),
                        second_path: source.path.clone(),
                    });
                }
                Some(_) => {}
                None => {
                    self.runtime_platform_source = Some(source.path.clone());
                    self.merged.runtime.platform_inherited_env_vars =
                        rt.platform_inherited_env_vars.clone();
                }
            }
        }
    }

    /// Applies boundary defaults once and returns the merged live document.
    fn finish(self) -> Result<NickelDocument, ConductorError> {
        if !self.conflicts.is_empty() {
            let detail: Vec<String> = self
                .conflicts
                .iter()
                .map(|c| match c {
                    MergeConflict::DuplicateTool { name, first_path, second_path } => format!(
                        "tool '{name}' defined in '{}' and '{}'",
                        first_path.display(),
                        second_path.display()
                    ),
                    MergeConflict::DuplicateWorkflow { name, first_path, second_path } => format!(
                        "workflow '{name}' defined in '{}' and '{}'",
                        first_path.display(),
                        second_path.display()
                    ),
                    MergeConflict::ConflictingRuntime { field, first_path, second_path } => {
                        format!(
                            "runtime field '{field}' set to conflicting values in '{}' and '{}'",
                            first_path.display(),
                            second_path.display()
                        )
                    }
                })
                .collect();
            return Err(ConductorError::Workflow(format!(
                "merge conflicts in config documents: {}",
                detail.join("; ")
            )));
        }
        // Defaults applied once at the end: the merged document is definite
        // for all merge-relevant fields (save_mode and retry_impure).
        let doc: NickelDocument = self.merged.into();
        doc.validate_external_data_invariant()?;
        Ok(doc)
    }
}

/// Two workflow definitions conflict when any field EXCEPT the human-readable
/// `display_name`/`description` differs (those are never compared, rule 1).
fn workflow_conflicts(a: &WorkflowSpecLatest, b: &WorkflowSpecLatest) -> bool {
    a.name != b.name || a.impure != b.impure || a.steps != b.steps
}

/// Merges two optional save policies to the most-permissive explicit one;
/// `None` (implicit) never beats an explicit value.
fn merge_save_modes(
    a: Option<OutputPolicyLatest>,
    b: Option<OutputPolicyLatest>,
) -> Option<OutputPolicyLatest> {
    match (a, b) {
        (Some(a), Some(b)) => Some(most_permissive_save_mode(a, b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

/// Most-permissive save policy: `Full` > `Saved` > `Unsaved`.
fn most_permissive_save_mode(a: OutputPolicyLatest, b: OutputPolicyLatest) -> OutputPolicyLatest {
    match (a, b) {
        (OutputPolicyLatest::Full, _) | (_, OutputPolicyLatest::Full) => OutputPolicyLatest::Full,
        (OutputPolicyLatest::Bool(true), _) | (_, OutputPolicyLatest::Bool(true)) => {
            OutputPolicyLatest::Bool(true)
        }
        _ => OutputPolicyLatest::Bool(false),
    }
}

impl NickelDocument {
    /// Converts this document into a [`UnifiedNickelDocument`] for the
    /// orchestration runtime.
    ///
    /// Each tool spec is mapped to a [`UnifiedToolSpec`] by combining the
    /// tool definition with its runtime configuration. Content-map hashes
    /// and external-data hashes are collected into a deduplicated set.
    #[must_use]
    pub(crate) fn to_unified(&self) -> UnifiedNickelDocument {
        let config_content_hashes = collect_config_content_hashes(&self.tools, &self.external_data);

        let tools: BTreeMap<String, UnifiedToolSpec> = self
            .tools
            .iter()
            .map(|(name, spec)| {
                let id = name.clone();
                let (command_parts, success_codes) = match &spec.kind {
                    ToolKindSpec::Executable { command, env_vars: _, success_codes } => {
                        (command.clone(), success_codes.clone())
                    }
                    ToolKindSpec::Builtin { .. } => (Vec::new(), vec![0]),
                };

                let builtin_id = match &spec.kind {
                    ToolKindSpec::Builtin { builtin_id } => Some(builtin_id.clone()),
                    ToolKindSpec::Executable { .. } => None,
                };
                let unified = UnifiedToolSpec {
                    name: spec.name.clone(),
                    is_impure: spec.runtime.impure,
                    max_concurrent_calls: spec.runtime.max_concurrent_calls,
                    max_retries: spec.runtime.max_retries,
                    builtin_id,
                    command_parts,
                    success_codes,
                    inputs: spec.inputs.clone(),
                    default_inputs: spec.default_inputs.clone(),
                    execution_env_vars: {
                        // 1. Hardcoded platform defaults
                        let mut env_vars = default_runtime_inherited_env_vars();

                        // 2. Inherit additional env var names from host, selected by current platform
                        let current_platform = if cfg!(target_os = "windows") {
                            "windows"
                        } else if cfg!(target_os = "linux") {
                            "linux"
                        } else if cfg!(target_os = "macos") {
                            "macos"
                        } else {
                            "unknown"
                        };
                        if let Some(platform_names) =
                            self.runtime.platform_inherited_env_vars.env_names_for(current_platform)
                        {
                            for name in platform_names {
                                if let Ok(val) = std::env::var(name) {
                                    env_vars.insert(name.clone(), val);
                                }
                            }
                        }

                        // 3. Tool-level inherited env var names
                        for name in &spec.runtime.inherited_env_vars {
                            if let Ok(val) = std::env::var(name) {
                                env_vars.insert(name.clone(), val);
                            }
                        }
                        env_vars
                    },
                    outputs: BTreeMap::new(),
                    tool_content_map: spec.runtime.content_map.clone(),
                };
                (id, unified)
            })
            .collect();

        let workflows: BTreeMap<String, WorkflowSpec> =
            self.workflows.iter().map(|w| (w.name.clone(), w.clone())).collect();

        let external_data_policies =
            self.external_data.iter().map(|(hash, entry)| (*hash, entry.save_mode)).collect();

        UnifiedNickelDocument {
            tools,
            workflows,
            tool_content_hashes: config_content_hashes,
            external_data_policies,
            runtime: self.runtime.clone(),
        }
    }

    /// Validates that every CAS hash referenced in any tool's `content_map` has
    /// a corresponding entry in `external_data`.
    ///
    /// This enforces the `content_map ⊆ external_data` invariant that
    /// prevents CAS GC from pruning hashes that tools actively depend on.
    ///
    /// # Errors
    ///
    /// Returns `ConductorError::Workflow` with a message listing all
    /// content-map hashes that are missing from `external_data`.
    pub(crate) fn validate_external_data_invariant(&self) -> Result<(), ConductorError> {
        let mut missing: Vec<String> = Vec::new();

        for (tool_name, spec) in &self.tools {
            for (path, value) in &spec.runtime.content_map {
                if let Ok(hash) = value.parse::<Hash>()
                    && !self.external_data.contains_key(&hash)
                {
                    missing.push(format!(
                        "tool '{tool_name}' content_map entry '{path}' references hash \
                             {hash} which is not declared in external_data"
                    ));
                }
            }
        }

        if missing.is_empty() {
            Ok(())
        } else {
            Err(ConductorError::Workflow(format!(
                "content_map references hashes not in external_data: {}",
                missing.join("; ")
            )))
        }
    }
}

/// Returns true when one external-data description marks managed tool content.
#[must_use]
pub fn is_tool_content_description(description: Option<&str>) -> bool {
    description.is_some_and(|text| text.starts_with(MANAGED_TOOL_CONTENT_DESCRIPTION_PREFIX))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use mediapm_cas::Hash;

    use super::*;
    use crate::config::ToolRuntime;
    use crate::config::versions::v_latest::{
        ExternalDataEntryLatest, ToolKindLatest, ToolRuntimeLatest, ToolSpecLatest,
        WorkflowStepSpecLatest,
    };

    /// Verifies `collect_config_content_hashes` collects hashes from tool
    /// content maps and external data entries.
    #[test]
    fn collect_config_content_hashes_finds_referenced_hashes() {
        let hash_a = Hash::from_content(b"payload-a");
        let hash_b = Hash::from_content(b"payload-b");

        let tools = BTreeMap::from([
            (
                "tool-a".to_string(),
                ToolSpec {
                    kind: ToolKindSpec::Executable {
                        command: vec!["tool-a".to_string()],
                        env_vars: BTreeMap::new(),
                        success_codes: vec![0],
                    },
                    name: "tool-a".to_string(),
                    inputs: BTreeMap::new(),
                    default_inputs: BTreeMap::new(),
                    outputs: BTreeMap::new(),
                    runtime: ToolRuntime {
                        content_map: BTreeMap::from([
                            ("file-a.bin".to_string(), hash_a.to_string()),
                            ("file-b.bin".to_string(), hash_b.to_string()),
                        ]),
                        ..ToolRuntime::default()
                    },
                },
            ),
            (
                "tool-b".to_string(),
                ToolSpec {
                    kind: ToolKindSpec::Executable {
                        command: vec!["tool-b".to_string()],
                        env_vars: BTreeMap::new(),
                        success_codes: vec![0],
                    },
                    name: "tool-b".to_string(),
                    inputs: BTreeMap::new(),
                    default_inputs: BTreeMap::new(),
                    outputs: BTreeMap::new(),
                    runtime: ToolRuntime {
                        content_map: BTreeMap::from([(
                            "file-c.bin".to_string(),
                            hash_a.to_string(),
                        )]),
                        ..ToolRuntime::default()
                    },
                },
            ),
        ]);

        let hashes = collect_config_content_hashes(&tools, &BTreeMap::new());
        assert!(hashes.contains(&hash_a));
        assert!(hashes.contains(&hash_b));
        assert_eq!(hashes.len(), 2);
    }

    /// Verifies `collect_config_content_hashes` skips non-hash values.
    #[test]
    fn collect_config_content_hashes_skips_inline_values() {
        let tools = BTreeMap::from([(
            "echo".to_string(),
            ToolSpec {
                kind: ToolKindSpec::Builtin { builtin_id: "echo@v1".to_string() },
                name: "echo".to_string(),
                inputs: BTreeMap::new(),
                default_inputs: BTreeMap::new(),
                outputs: BTreeMap::new(),
                runtime: ToolRuntime {
                    content_map: BTreeMap::from([(
                        "payload.txt".to_string(),
                        "not-a-hash".to_string(),
                    )]),
                    ..ToolRuntime::default()
                },
            },
        )]);

        let hashes = collect_config_content_hashes(&tools, &BTreeMap::new());
        assert!(hashes.is_empty());
    }

    /// Verifies `is_tool_content_description` matches the expected prefix.
    #[test]
    fn is_tool_content_description_matches_prefix() {
        assert!(is_tool_content_description(Some(
            "managed tool content CAS root for 00000000000000000000000000000000"
        )));
        assert!(!is_tool_content_description(Some("user-provided content")));
        assert!(!is_tool_content_description(None));
    }

    /// Verifies `NickelDocument::default` produces an empty document.
    #[test]
    fn nickel_document_default_is_empty() {
        let doc = NickelDocument::default();
        assert!(doc.tools.is_empty());
        assert!(doc.workflows.is_empty());
    }

    /// Verifies `validate_external_data_invariant` passes when all
    /// content-map hashes have matching `external_data` entries.
    #[test]
    fn validate_external_data_invariant_passes_when_all_hashes_covered() {
        let hash_a = Hash::from_content(b"payload-a");
        let doc = NickelDocument {
            tools: BTreeMap::from([(
                "tool-a".to_string(),
                ToolSpec {
                    kind: ToolKindSpec::Executable {
                        command: vec!["tool-a".to_string()],
                        env_vars: BTreeMap::new(),
                        success_codes: vec![0],
                    },
                    name: "tool-a".to_string(),
                    inputs: BTreeMap::new(),
                    default_inputs: BTreeMap::new(),
                    outputs: BTreeMap::new(),
                    runtime: ToolRuntime {
                        content_map: BTreeMap::from([("file.bin".to_string(), hash_a.to_string())]),
                        ..ToolRuntime::default()
                    },
                },
            )]),
            workflows: vec![],
            runtime: crate::config::ConductorRuntimeConfig::default(),
            external_data: BTreeMap::from([(
                hash_a,
                super::super::ExternalDataEntry {
                    description: Some("test payload".to_string()),
                    save_mode: crate::state::OutputSaveMode::Saved,
                },
            )]),
        };

        assert!(doc.validate_external_data_invariant().is_ok());
    }

    /// Verifies `validate_external_data_invariant` fails when a tool's
    /// `content_map` references a hash not declared in `external_data`.
    #[test]
    fn validate_external_data_invariant_rejects_missing_hash() {
        let hash_a = Hash::from_content(b"payload-a");
        let hash_b = Hash::from_content(b"payload-b");
        let doc = NickelDocument {
            tools: BTreeMap::from([(
                "tool-a".to_string(),
                ToolSpec {
                    kind: ToolKindSpec::Executable {
                        command: vec!["tool-a".to_string()],
                        env_vars: BTreeMap::new(),
                        success_codes: vec![0],
                    },
                    name: "tool-a".to_string(),
                    inputs: BTreeMap::new(),
                    default_inputs: BTreeMap::new(),
                    outputs: BTreeMap::new(),
                    runtime: ToolRuntime {
                        content_map: BTreeMap::from([
                            ("file-a.bin".to_string(), hash_a.to_string()),
                            ("file-b.bin".to_string(), hash_b.to_string()),
                        ]),
                        ..ToolRuntime::default()
                    },
                },
            )]),
            workflows: vec![],
            runtime: crate::config::ConductorRuntimeConfig::default(),
            // Only hash_a is declared — hash_b is missing.
            external_data: BTreeMap::from([(
                hash_a,
                super::super::ExternalDataEntry {
                    description: Some("test payload".to_string()),
                    save_mode: crate::state::OutputSaveMode::Saved,
                },
            )]),
        };

        let err = doc.validate_external_data_invariant().unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("not in external_data"), "error should mention missing hash: {msg}");
        assert!(msg.contains(hash_b.to_string().as_str()), "error should mention hash_b: {msg}");
    }

    /// Verifies `validate_external_data_invariant` ignores non-hash
    /// `content_map` values (inline descriptions, base64).
    #[test]
    fn validate_external_data_invariant_skips_non_hash_values() {
        let doc = NickelDocument {
            tools: BTreeMap::from([(
                "echo".to_string(),
                ToolSpec {
                    kind: ToolKindSpec::Builtin { builtin_id: "echo@v1".to_string() },
                    name: "echo".to_string(),
                    inputs: BTreeMap::new(),
                    default_inputs: BTreeMap::new(),
                    outputs: BTreeMap::new(),
                    runtime: ToolRuntime {
                        content_map: BTreeMap::from([(
                            "payload.txt".to_string(),
                            "not-a-hash".to_string(),
                        )]),
                        ..ToolRuntime::default()
                    },
                },
            )]),
            workflows: vec![],
            runtime: crate::config::ConductorRuntimeConfig::default(),
            external_data: BTreeMap::new(),
        };

        assert!(doc.validate_external_data_invariant().is_ok());
    }

    /// Verifies `merge_documents` with an empty list returns a default document.
    #[test]
    fn merge_documents_empty_list() {
        let doc = merge_documents(&[]).unwrap();
        assert!(doc.tools.is_empty());
        assert!(doc.workflows.is_empty());
    }

    /// Builds an empty latest-schema wire envelope.
    fn envelope() -> NickelEnvelopeLatest {
        NickelEnvelopeLatest {
            version: NICKEL_VERSION_LATEST,
            tools: BTreeMap::new(),
            workflows: Vec::new(),
            external_data: BTreeMap::new(),
            runtime: ConductorRuntimeConfigLatest::default(),
        }
    }

    /// Wraps an envelope in a `SourceDocument` under a dummy path.
    fn source(path: &str, envelope: NickelEnvelopeLatest) -> SourceDocument {
        SourceDocument { path: PathBuf::from(path), envelope }
    }

    /// Builds a minimal builtin-tool wire entry.
    fn builtin_tool(name: &str) -> ToolSpecLatest {
        ToolSpecLatest {
            kind: ToolKindLatest::Builtin { builtin_id: "echo@v1".into() },
            name: name.into(),
            inputs: BTreeMap::new(),
            default_inputs: BTreeMap::new(),
            outputs: BTreeMap::new(),
            runtime: ToolRuntimeLatest::default(),
        }
    }

    /// Builds a minimal workflow wire entry with the given step ids.
    fn workflow(name: &str, step_ids: &[&str]) -> WorkflowSpecLatest {
        WorkflowSpecLatest {
            name: name.into(),
            display_name: None,
            description: None,
            impure: false,
            steps: step_ids
                .iter()
                .map(|id| WorkflowStepSpecLatest {
                    id: (*id).into(),
                    tool: "echo".into(),
                    inputs: BTreeMap::new(),
                    outputs: BTreeMap::new(),
                    max_retries: 0,
                    depends_on: Vec::new(),
                })
                .collect(),
        }
    }

    /// Builds an external-data wire entry.
    fn external_data(
        description: Option<&str>,
        save_mode: Option<OutputPolicyLatest>,
    ) -> ExternalDataEntryLatest {
        ExternalDataEntryLatest {
            hash: None,
            description: description.map(ToOwned::to_owned),
            save_mode,
        }
    }

    /// Verifies `merge_documents` with one source passes through its tool.
    #[test]
    fn merge_documents_single_passthrough() {
        let mut env = envelope();
        env.tools.insert("echo".to_string(), builtin_tool("echo"));
        let result = merge_documents(&[source("/dummy/a.ncl", env)]).unwrap();
        assert!(result.tools.contains_key("echo"));
    }

    /// Verifies `merge_documents` merges two documents with disjoint tool sets.
    #[test]
    fn merge_documents_disjoint_tools_merge() {
        let mut env1 = envelope();
        env1.tools.insert("tool-a".to_string(), builtin_tool("tool-a"));
        let mut env2 = envelope();
        env2.tools.insert("tool-b".to_string(), builtin_tool("tool-b"));
        let result =
            merge_documents(&[source("/dummy/a.ncl", env1), source("/dummy/b.ncl", env2)]).unwrap();
        assert!(result.tools.contains_key("tool-a"));
        assert!(result.tools.contains_key("tool-b"));
    }

    /// Verifies `merge_documents` rejects duplicate tool names with a
    /// merge-conflict error.
    #[test]
    fn merge_documents_duplicate_tool_rejected() {
        let mut env1 = envelope();
        env1.tools.insert("echo".to_string(), builtin_tool("echo"));
        let mut env2 = envelope();
        env2.tools.insert("echo".to_string(), builtin_tool("echo"));
        let err = merge_documents(&[source("/dummy/a.ncl", env1), source("/dummy/b.ncl", env2)])
            .unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("merge conflicts"), "error should mention merge conflicts: {msg}");
        assert!(msg.contains("tool 'echo'"), "error should mention tool name: {msg}");
        assert!(msg.contains("defined in"), "error should mention duplicate tool: {msg}");
    }

    /// Verifies `merge_documents` merges identical workflow re-declarations
    /// (same name, impure flag, and steps) into a single entry.
    #[test]
    fn merge_documents_identical_workflows_merge_cleanly() {
        let mut env1 = envelope();
        env1.workflows.push(workflow("w", &["s1"]));
        let mut env2 = envelope();
        env2.workflows.push(workflow("w", &["s1"]));
        let result =
            merge_documents(&[source("/dummy/a.ncl", env1), source("/dummy/b.ncl", env2)]).unwrap();
        assert_eq!(result.workflows.len(), 1);
        assert_eq!(result.workflows[0].name, "w");
        assert_eq!(result.workflows[0].steps.len(), 1);
    }

    /// Verifies `merge_documents` never compares the human-readable workflow
    /// `display_name`/`description` when deciding conflicts.
    #[test]
    fn merge_documents_human_fields_not_compared() {
        let mut env1 = envelope();
        env1.workflows.push(workflow("w", &["s1"]));
        let mut env2 = envelope();
        let mut humanized = workflow("w", &["s1"]);
        humanized.display_name = Some("Human name".into());
        humanized.description = Some("Human description".into());
        env2.workflows.push(humanized);
        let result =
            merge_documents(&[source("/dummy/a.ncl", env1), source("/dummy/b.ncl", env2)]).unwrap();
        // Merged entry carries no human fields (rule 1: never merged).
        assert_eq!(result.workflows[0].display_name, None);
        assert_eq!(result.workflows[0].description, None);
    }

    /// Verifies `merge_documents` rejects duplicate workflow names with
    /// CONFLICTING definitions.
    #[test]
    fn merge_documents_duplicate_workflow_rejected() {
        let mut env1 = envelope();
        env1.workflows.push(workflow("w", &["s1"]));
        let mut env2 = envelope();
        env2.workflows.push(workflow("w", &["s1", "s2"]));
        let err = merge_documents(&[source("/dummy/a.ncl", env1), source("/dummy/b.ncl", env2)])
            .unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("merge conflicts"), "error should mention merge conflicts: {msg}");
        assert!(msg.contains("workflow 'w'"), "error should mention workflow name: {msg}");
        assert!(msg.contains("defined in"), "error should mention duplicate workflow: {msg}");
    }

    /// Verifies `merge_documents` unions external data by hash across
    /// documents and applies the implicit `Saved` default.
    #[test]
    fn merge_documents_external_data_union() {
        let hash_a = Hash::from_content(b"payload-a");
        let hash_b = Hash::from_content(b"payload-b");
        let mut env1 = envelope();
        env1.external_data.insert(hash_a, external_data(None, None));
        let mut env2 = envelope();
        env2.external_data.insert(hash_b, external_data(None, None));
        let result =
            merge_documents(&[source("/dummy/a.ncl", env1), source("/dummy/b.ncl", env2)]).unwrap();
        assert_eq!(result.external_data.len(), 2);
        // Implicit `save` defaults to Saved (definite final document).
        assert_eq!(
            result.external_data.get(&hash_a).unwrap().save_mode,
            crate::state::OutputSaveMode::Saved
        );
        assert_eq!(
            result.external_data.get(&hash_b).unwrap().save_mode,
            crate::state::OutputSaveMode::Saved
        );
    }

    /// Verifies `merge_documents` keeps the most-permissive explicit `save`
    /// policy when the same hash appears in multiple documents.
    #[test]
    fn merge_documents_external_data_most_permissive() {
        let hash = Hash::from_content(b"shared-payload");
        // Saved (explicit) beats Unsaved.
        let mut env1 = envelope();
        env1.external_data.insert(hash, external_data(None, Some(OutputPolicyLatest::Bool(false))));
        let mut env2 = envelope();
        env2.external_data.insert(hash, external_data(None, Some(OutputPolicyLatest::Bool(true))));
        let result =
            merge_documents(&[source("/dummy/a.ncl", env1), source("/dummy/b.ncl", env2)]).unwrap();
        assert_eq!(
            result.external_data.get(&hash).unwrap().save_mode,
            crate::state::OutputSaveMode::Saved
        );
        // Full beats Saved.
        let mut env3 = envelope();
        env3.external_data.insert(hash, external_data(None, Some(OutputPolicyLatest::Bool(true))));
        let mut env4 = envelope();
        env4.external_data.insert(hash, external_data(None, Some(OutputPolicyLatest::Full)));
        let result =
            merge_documents(&[source("/dummy/c.ncl", env3), source("/dummy/d.ncl", env4)]).unwrap();
        assert_eq!(
            result.external_data.get(&hash).unwrap().save_mode,
            crate::state::OutputSaveMode::Full
        );
    }

    /// Verifies an explicit `save` policy beats an implicit (absent) one,
    /// in either document order.
    #[test]
    fn merge_documents_external_data_save_mode_explicit_beats_implicit() {
        let hash = Hash::from_content(b"explicit-payload");
        let mut implicit_env = envelope();
        implicit_env.external_data.insert(hash, external_data(None, None));
        let mut explicit_env = envelope();
        explicit_env
            .external_data
            .insert(hash, external_data(None, Some(OutputPolicyLatest::Bool(false))));
        let result = merge_documents(&[
            source("/dummy/a.ncl", implicit_env.clone()),
            source("/dummy/b.ncl", explicit_env.clone()),
        ])
        .unwrap();
        assert_eq!(
            result.external_data.get(&hash).unwrap().save_mode,
            crate::state::OutputSaveMode::Unsaved
        );
        // Order-independence: explicit document first yields the same result.
        let result = merge_documents(&[
            source("/dummy/b.ncl", explicit_env),
            source("/dummy/a.ncl", implicit_env),
        ])
        .unwrap();
        assert_eq!(
            result.external_data.get(&hash).unwrap().save_mode,
            crate::state::OutputSaveMode::Unsaved
        );
    }

    /// Verifies external-data descriptions are never merged: the merged
    /// document always carries `None` regardless of the source descriptions.
    #[test]
    fn merge_documents_external_data_descriptions_stay_none() {
        let hash = Hash::from_content(b"described-payload");
        let mut env1 = envelope();
        env1.external_data.insert(hash, external_data(Some("first description"), None));
        let mut env2 = envelope();
        env2.external_data.insert(hash, external_data(Some("second description"), None));
        let result =
            merge_documents(&[source("/dummy/a.ncl", env1), source("/dummy/b.ncl", env2)]).unwrap();
        assert_eq!(result.external_data.get(&hash).unwrap().description, None);
    }

    /// Verifies runtime `retry_impure` merges explicitly-set values and
    /// applies the `false` default to implicit ones.
    #[test]
    fn merge_documents_runtime_explicit_beats_implicit() {
        let env1 = envelope();
        let mut env2 = envelope();
        env2.runtime.retry_impure = Some(true);
        let result =
            merge_documents(&[source("/dummy/a.ncl", env1), source("/dummy/b.ncl", env2)]).unwrap();
        assert!(result.runtime.retry_impure);
        // No explicit value anywhere → default false.
        let env3 = envelope();
        let result = merge_documents(&[source("/dummy/c.ncl", env3)]).unwrap();
        assert!(!result.runtime.retry_impure);
    }

    /// Verifies runtime `platform_inherited_env_vars` merges explicitly-set
    /// platform maps and skips empty (implicit) ones.
    #[test]
    fn merge_documents_runtime_platform_explicit_beats_implicit() {
        let env1 = envelope();
        let mut env2 = envelope();
        env2.runtime.platform_inherited_env_vars = super::super::PlatformInheritedEnvVars {
            windows: Vec::new(),
            linux: vec!["PATH".into()],
            macos: Vec::new(),
        };
        let result =
            merge_documents(&[source("/dummy/a.ncl", env1), source("/dummy/b.ncl", env2)]).unwrap();
        assert_eq!(
            result.runtime.platform_inherited_env_vars.env_names_for("linux"),
            Some(&vec!["PATH".to_string()])
        );
    }

    /// Verifies two documents explicitly setting conflicting `retry_impure`
    /// values error with a runtime-field conflict.
    #[test]
    fn merge_documents_runtime_conflicting_explicit_values_rejected() {
        let mut env1 = envelope();
        env1.runtime.retry_impure = Some(true);
        let mut env2 = envelope();
        env2.runtime.retry_impure = Some(false);
        let err = merge_documents(&[source("/dummy/a.ncl", env1), source("/dummy/b.ncl", env2)])
            .unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("merge conflicts"), "error should mention merge conflicts: {msg}");
        assert!(msg.contains("runtime field 'retry_impure'"), "error should name the field: {msg}");
    }

    /// Verifies two documents explicitly setting conflicting
    /// `platform_inherited_env_vars` error with a runtime-field conflict.
    #[test]
    fn merge_documents_runtime_conflicting_platform_rejected() {
        let mut env1 = envelope();
        env1.runtime.platform_inherited_env_vars = super::super::PlatformInheritedEnvVars {
            windows: Vec::new(),
            linux: vec!["PATH".into()],
            macos: Vec::new(),
        };
        let mut env2 = envelope();
        env2.runtime.platform_inherited_env_vars = super::super::PlatformInheritedEnvVars {
            windows: Vec::new(),
            linux: Vec::new(),
            macos: vec!["HOME".into()],
        };
        let err = merge_documents(&[source("/dummy/a.ncl", env1), source("/dummy/b.ncl", env2)])
            .unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("merge conflicts"), "error should mention merge conflicts: {msg}");
        assert!(
            msg.contains("runtime field 'platform_inherited_env_vars'"),
            "error should name the field: {msg}"
        );
    }

    /// Verifies the merge result is order-independent for a mixed document
    /// set (identical workflow, external data, runtime).
    #[test]
    fn merge_documents_order_independent() {
        let hash = Hash::from_content(b"order-payload");
        let mut env_user = envelope();
        env_user.workflows.push(workflow("w", &["s1"]));
        env_user
            .external_data
            .insert(hash, external_data(None, Some(OutputPolicyLatest::Bool(true))));
        env_user.runtime.retry_impure = Some(true);
        let mut env_generated = envelope();
        env_generated.workflows.push(workflow("w", &["s1"]));
        env_generated.external_data.insert(hash, external_data(None, None));

        let forward = merge_documents(&[
            source("/dummy/user.ncl", env_user.clone()),
            source("/dummy/generated.ncl", env_generated.clone()),
        ])
        .unwrap();
        let reverse = merge_documents(&[
            source("/dummy/generated.ncl", env_generated),
            source("/dummy/user.ncl", env_user),
        ])
        .unwrap();
        // NickelDocument has no PartialEq; compare field-wise. Workflows are a
        // Vec so compare element-wise (both docs declare the same single
        // workflow, so order is identical).
        assert_eq!(forward.tools, reverse.tools);
        assert_eq!(forward.external_data, reverse.external_data);
        assert_eq!(forward.runtime, reverse.runtime);
        assert_eq!(forward.workflows.len(), reverse.workflows.len());
        for (a, b) in forward.workflows.iter().zip(reverse.workflows.iter()) {
            assert_eq!(a.name, b.name);
            assert_eq!(a.impure, b.impure);
            assert_eq!(a.steps, b.steps);
        }
    }

    /// Verifies `collect_config_content_hashes` returns an empty set for empty inputs.
    #[test]
    fn collect_config_content_hashes_empty_tools() {
        let hashes = collect_config_content_hashes(&BTreeMap::new(), &BTreeMap::new());
        assert!(hashes.is_empty());
    }

    /// Verifies `collect_config_content_hashes` deduplicates the same hash across tools.
    #[test]
    fn collect_config_content_hashes_deduplicates_across_tools() {
        let hash_a = Hash::from_content(b"same-payload");
        let tools = BTreeMap::from([
            (
                "tool-a".to_string(),
                ToolSpec {
                    kind: ToolKindSpec::Executable {
                        command: vec!["tool-a".to_string()],
                        env_vars: BTreeMap::new(),
                        success_codes: vec![0],
                    },
                    name: "tool-a".to_string(),
                    inputs: BTreeMap::new(),
                    default_inputs: BTreeMap::new(),
                    outputs: BTreeMap::new(),
                    runtime: ToolRuntime {
                        content_map: BTreeMap::from([("file.bin".to_string(), hash_a.to_string())]),
                        ..ToolRuntime::default()
                    },
                },
            ),
            (
                "tool-b".to_string(),
                ToolSpec {
                    kind: ToolKindSpec::Executable {
                        command: vec!["tool-b".to_string()],
                        env_vars: BTreeMap::new(),
                        success_codes: vec![0],
                    },
                    name: "tool-b".to_string(),
                    inputs: BTreeMap::new(),
                    default_inputs: BTreeMap::new(),
                    outputs: BTreeMap::new(),
                    runtime: ToolRuntime {
                        content_map: BTreeMap::from([(
                            "other.bin".to_string(),
                            hash_a.to_string(),
                        )]),
                        ..ToolRuntime::default()
                    },
                },
            ),
        ]);
        let hashes = collect_config_content_hashes(&tools, &BTreeMap::new());
        assert_eq!(hashes.len(), 1);
    }

    /// Verifies `collect_config_content_hashes` includes external-data keys.
    #[test]
    fn collect_config_content_hashes_includes_external_data_keys() {
        let hash_a = Hash::from_content(b"external-payload");
        let external_data = BTreeMap::from([(
            hash_a,
            super::super::ExternalDataEntry {
                description: Some("external data".to_string()),
                save_mode: crate::state::OutputSaveMode::Saved,
            },
        )]);
        let hashes = collect_config_content_hashes(&BTreeMap::new(), &external_data);
        assert_eq!(hashes.len(), 1);
        assert!(hashes.contains(&hash_a));
    }
}
