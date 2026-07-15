//! Multi-document configuration model.
//!
//! In the simplified multi-doc model, conductor accepts zero to many user
//! configuration documents plus one volatile state document.  Each document is
//! a [`NickelDocument`] parsed independently by its embedded schema version
//! marker and merged in declaration order (conflicts produce errors).
//!
//! This replaces the old three-document model (`UserNickelDocument`,
//! `MachineNickelDocument`, `StateNickelDocument`) with a single unified type.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use mediapm_cas::Hash;
use serde::{Deserialize, Serialize};

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

/// A `NickelDocument` paired with its source file path.
///
/// Used during configuration loading to track which file each document
/// originated from — critical for error reporting on merge conflicts.
#[derive(Debug, Clone)]
pub struct SourceDocument {
    /// Absolute path to the `.ncl` file this document was loaded from.
    pub path: PathBuf,
    /// The parsed document.
    pub document: NickelDocument,
}

/// A conflict between two source documents during merge.
#[derive(Debug, Clone)]
pub enum MergeConflict {
    /// Two documents declare the same tool name with incompatible specs.
    DuplicateTool { name: String, first_path: PathBuf, second_path: PathBuf },
    /// Two documents declare the same workflow name.
    DuplicateWorkflow { name: String, first_path: PathBuf, second_path: PathBuf },
}

/// Merges multiple source documents into one unified document.
///
/// Documents are merged in declaration order. If two documents define the same
/// tool name, workflow name, or tool-runtime key, the merge fails with a
/// [`MergeConflict`] error.
///
/// # Errors
///
/// Returns `ConductorError::Workflow` containing all merge conflicts found.
pub fn merge_documents(docs: &[SourceDocument]) -> Result<NickelDocument, ConductorError> {
    let mut merged = NickelDocument::default();
    // Track which path each merged name was first seen in.
    let mut tool_sources: BTreeMap<String, &PathBuf> = BTreeMap::new();
    let mut workflow_sources: BTreeMap<String, &PathBuf> = BTreeMap::new();
    let mut conflicts: Vec<MergeConflict> = Vec::new();

    for source in docs {
        for (tool_name, tool_spec) in &source.document.tools {
            if let Some(first_path) = tool_sources.get(tool_name) {
                conflicts.push(MergeConflict::DuplicateTool {
                    name: tool_name.clone(),
                    first_path: (*first_path).clone(),
                    second_path: source.path.clone(),
                });
            } else {
                tool_sources.insert(tool_name.clone(), &source.path);
                merged.tools.insert(tool_name.clone(), tool_spec.clone());
            }
        }

        for workflow in &source.document.workflows {
            if let Some(first_path) = workflow_sources.get(&workflow.name) {
                conflicts.push(MergeConflict::DuplicateWorkflow {
                    name: workflow.name.clone(),
                    first_path: (*first_path).clone(),
                    second_path: source.path.clone(),
                });
            } else {
                workflow_sources.insert(workflow.name.clone(), &source.path);
                merged.workflows.push(workflow.clone());
            }
        }
    }

    if conflicts.is_empty() {
        Ok(merged)
    } else {
        let detail: Vec<String> = conflicts
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
            })
            .collect();
        Err(ConductorError::Workflow(format!(
            "merge conflicts in config documents: {}",
            detail.join("; ")
        )))
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
                            self.runtime.platform_inherited_env_vars.get(current_platform)
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

    /// Validates that every CAS hash referenced in any tool's content_map has
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
                if let Ok(hash) = value.parse::<Hash>() {
                    if !self.external_data.contains_key(&hash) {
                        missing.push(format!(
                            "tool '{tool_name}' content_map entry '{path}' references hash \
                             {hash} which is not declared in external_data"
                        ));
                    }
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
    /// content-map hashes have matching external_data entries.
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
                    description: "test payload".to_string(),
                    save_mode: crate::state::OutputSaveMode::Saved,
                },
            )]),
        };

        assert!(doc.validate_external_data_invariant().is_ok());
    }

    /// Verifies `validate_external_data_invariant` fails when a tool's
    /// content_map references a hash not declared in external_data.
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
                    description: "test payload".to_string(),
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
    /// content_map values (inline descriptions, base64).
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

    /// Verifies `merge_documents` with one source passes through its tool.
    #[test]
    fn merge_documents_single_passthrough() {
        let doc = NickelDocument {
            tools: BTreeMap::from([(
                "echo".to_string(),
                ToolSpec {
                    kind: ToolKindSpec::Builtin { builtin_id: "echo@v1".to_string() },
                    name: "echo".to_string(),
                    inputs: BTreeMap::new(),
                    default_inputs: BTreeMap::new(),
                    outputs: BTreeMap::new(),
                    runtime: ToolRuntime::default(),
                },
            )]),
            ..NickelDocument::default()
        };
        let source = SourceDocument { path: PathBuf::from("/dummy/a.ncl"), document: doc };
        let result = merge_documents(&[source]).unwrap();
        assert!(result.tools.contains_key("echo"));
    }

    /// Verifies `merge_documents` merges two documents with disjoint tool sets.
    #[test]
    fn merge_documents_disjoint_tools_merge() {
        let doc1 = NickelDocument {
            tools: BTreeMap::from([(
                "tool-a".to_string(),
                ToolSpec {
                    kind: ToolKindSpec::Builtin { builtin_id: "echo@v1".to_string() },
                    name: "tool-a".to_string(),
                    inputs: BTreeMap::new(),
                    default_inputs: BTreeMap::new(),
                    outputs: BTreeMap::new(),
                    runtime: ToolRuntime::default(),
                },
            )]),
            ..NickelDocument::default()
        };
        let doc2 = NickelDocument {
            tools: BTreeMap::from([(
                "tool-b".to_string(),
                ToolSpec {
                    kind: ToolKindSpec::Builtin { builtin_id: "echo@v1".to_string() },
                    name: "tool-b".to_string(),
                    inputs: BTreeMap::new(),
                    default_inputs: BTreeMap::new(),
                    outputs: BTreeMap::new(),
                    runtime: ToolRuntime::default(),
                },
            )]),
            ..NickelDocument::default()
        };
        let source1 = SourceDocument { path: PathBuf::from("/dummy/a.ncl"), document: doc1 };
        let source2 = SourceDocument { path: PathBuf::from("/dummy/b.ncl"), document: doc2 };
        let result = merge_documents(&[source1, source2]).unwrap();
        assert!(result.tools.contains_key("tool-a"));
        assert!(result.tools.contains_key("tool-b"));
    }

    /// Verifies `merge_documents` rejects duplicate tool names with a merge-conflict error.
    #[test]
    fn merge_documents_duplicate_tool_rejected() {
        let spec = ToolSpec {
            kind: ToolKindSpec::Builtin { builtin_id: "echo@v1".to_string() },
            name: "echo".to_string(),
            inputs: BTreeMap::new(),
            default_inputs: BTreeMap::new(),
            outputs: BTreeMap::new(),
            runtime: ToolRuntime::default(),
        };
        let doc1 = NickelDocument {
            tools: BTreeMap::from([("echo".to_string(), spec.clone())]),
            ..NickelDocument::default()
        };
        let doc2 = NickelDocument {
            tools: BTreeMap::from([("echo".to_string(), spec)]),
            ..NickelDocument::default()
        };
        let source1 = SourceDocument { path: PathBuf::from("/dummy/a.ncl"), document: doc1 };
        let source2 = SourceDocument { path: PathBuf::from("/dummy/b.ncl"), document: doc2 };
        let err = merge_documents(&[source1, source2]).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("merge conflicts"), "error should mention merge conflicts: {msg}");
        assert!(msg.contains("tool 'echo'"), "error should mention tool name: {msg}");
        assert!(msg.contains("defined in"), "error should mention duplicate tool: {msg}");
    }

    /// Verifies `merge_documents` rejects duplicate workflow names with a merge-conflict error.
    #[test]
    fn merge_documents_duplicate_workflow_rejected() {
        let workflow = WorkflowSpec {
            name: "w".to_string(),
            display_name: String::new(),
            description: String::new(),
            impure: false,
            steps: vec![],
        };
        let doc1 =
            NickelDocument { workflows: vec![workflow.clone()], ..NickelDocument::default() };
        let doc2 = NickelDocument { workflows: vec![workflow], ..NickelDocument::default() };
        let source1 = SourceDocument { path: PathBuf::from("/dummy/a.ncl"), document: doc1 };
        let source2 = SourceDocument { path: PathBuf::from("/dummy/b.ncl"), document: doc2 };
        let err = merge_documents(&[source1, source2]).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("merge conflicts"), "error should mention merge conflicts: {msg}");
        assert!(msg.contains("workflow 'w'"), "error should mention workflow name: {msg}");
        assert!(msg.contains("defined in"), "error should mention duplicate workflow: {msg}");
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
                description: "external data".to_string(),
                save_mode: crate::state::OutputSaveMode::Saved,
            },
        )]);
        let hashes = collect_config_content_hashes(&BTreeMap::new(), &external_data);
        assert_eq!(hashes.len(), 1);
        assert!(hashes.contains(&hash_a));
    }
}
