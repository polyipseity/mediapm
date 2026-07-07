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

                let unified = UnifiedToolSpec {
                    is_impure: spec.runtime.impure,
                    max_concurrent_calls: spec.runtime.max_concurrent_calls,
                    max_retries: spec.runtime.max_retries,
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
}
