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
//! Merging operates on the presence-preserving wire envelope so that explicit
//! values never lose to implicit serde defaults, and human-readable fields
//! (`external_data` descriptions, workflow `display_name`/`description`) are
//! never merged or compared across documents. The merge itself is version
//! code and lives in `super::versions`, reachable only through the
//! unversioned `super::versions::merge_document_sources` entry point.

use std::collections::{BTreeMap, BTreeSet};

use mediapm_cas::Hash;
use serde::{Deserialize, Serialize};

use super::{
    ConductorRuntimeConfig, InputBinding, ToolKindSpec, ToolSpec, WorkflowSpec,
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
/// live inline on each [`ToolSpec`] via its
/// [`ToolRuntime`](super::ToolRuntime) — there is no
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

/// Returns the pattern one `${external_data.<hash>}` template reference is
/// written with.
///
/// The capture is the reference payload, which the engine parses as a `Hash`
/// in either display form (`blake3:<hex>` or bare hex). A match whose payload
/// does not parse is a reference the engine rejects at resolve time, with a
/// message about the reference itself; that is not this pass's verdict to make,
/// so such matches are skipped rather than reported here.
fn external_data_reference_pattern() -> &'static regex::Regex {
    static PATTERN: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    PATTERN.get_or_init(|| {
        regex::Regex::new(r"\$\{external_data\.([^}]+)\}")
            .expect("external-data reference pattern is a valid regex")
    })
}

/// Extracts every CAS hash one template string names through
/// `${external_data.<hash>}`.
#[must_use]
fn external_data_references(value: &str) -> Vec<Hash> {
    external_data_reference_pattern()
        .captures_iter(value)
        .filter_map(|captures| captures[1].trim().parse::<Hash>().ok())
        .collect()
}

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
                    version: spec.version.clone(),
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
                        if let Some(platform_names) = self
                            .runtime
                            .environment
                            .platform_inherited_env_vars
                            .env_names_for(current_platform)
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
                    // Bridged: a tool's capture defaults are live. The step
                    // worker merges them with the step's own declarations
                    // before capturing — the tool owns the capture identity of
                    // a name it declares and the step contributes only `save`,
                    // while a step-only name is kept whole — so one instance
                    // holds every name a step with this work can bind. They are
                    // not key material: the v4 instance key derives from inputs
                    // alone, and a step reads the names it binds out of the
                    // instance it found.
                    outputs: spec.outputs.clone(),
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

    /// Validates that every CAS hash the document names has a matching entry in
    /// `external_data`.
    ///
    /// Three shapes name a hash, and all three name bytes the CAS GC must keep:
    ///
    /// - a tool's `runtime.content_map` value, which is the payload the tool
    ///   runs on;
    /// - a `${external_data.<hash>}` reference in a step input or in a tool's
    ///   default input, which the template engine resolves by reading those
    ///   bytes out of CAS; and
    /// - an input value that *is* a hash, which a builtin such as `import`
    ///   consumes as one.
    ///
    /// The declaration is what makes the bytes reachable, and it has to be here
    /// rather than inferred: the GC's root set is built from `external_data`
    /// keys and from recorded instance outputs, so a hash named only inside an
    /// input is an orphan to the next sweep. A step that needs bytes kept alive
    /// declares them; the document that forgets fails here, at load, instead of
    /// losing the bytes at the next reclamation.
    ///
    /// # Errors
    ///
    /// Returns `ConductorError::Workflow` listing every hash that is named
    /// without being declared, with the tool, workflow, step, and input key that
    /// named it.
    pub(crate) fn validate_external_data_invariant(&self) -> Result<(), ConductorError> {
        let mut missing: Vec<String> = Vec::new();

        for (tool_name, spec) in &self.tools {
            for (path, value) in &spec.runtime.content_map {
                if let Ok(hash) = value.parse::<Hash>()
                    && !self.external_data.contains_key(&hash)
                {
                    missing.push(format!(
                        "tool '{tool_name}' content_map entry '{path}' references hash \
                         {hash}, which is not in external_data"
                    ));
                }
            }
        }

        for workflow in &self.workflows {
            for step in &workflow.steps {
                for (key, value) in &step.inputs {
                    self.collect_input_hash_gaps(
                        &format!("workflow '{}' step '{}' input '{key}'", workflow.name, step.id),
                        value,
                        &mut missing,
                    );
                }
            }
        }

        for (tool_name, spec) in &self.tools {
            for (key, binding) in &spec.default_inputs {
                match binding {
                    InputBinding::String(value) => self.collect_input_hash_gaps(
                        &format!("tool '{tool_name}' default input '{key}'"),
                        value,
                        &mut missing,
                    ),
                    InputBinding::Vec(values) => {
                        for (index, value) in values.iter().enumerate() {
                            self.collect_input_hash_gaps(
                                &format!("tool '{tool_name}' default input '{key}[{index}]'"),
                                value,
                                &mut missing,
                            );
                        }
                    }
                }
            }
        }

        if missing.is_empty() {
            Ok(())
        } else {
            Err(ConductorError::Workflow(format!(
                "document references hashes not in external_data: {}",
                missing.join("; ")
            )))
        }
    }

    /// Records every hash one input value names that `external_data` omits.
    ///
    /// `location` names the place in the document that carries the value, so a
    /// rejection points at the input to fix rather than at the document as a
    /// whole.
    fn collect_input_hash_gaps(&self, location: &str, value: &str, missing: &mut Vec<String>) {
        for hash in external_data_references(value) {
            if !self.external_data.contains_key(&hash) {
                missing.push(format!(
                    "{location} references hash {hash} through ${{external_data.…}}, which is \
                     not in external_data"
                ));
            }
        }

        let trimmed = value.trim();
        if let Ok(hash) = trimmed.parse::<Hash>()
            && !self.external_data.contains_key(&hash)
        {
            missing
                .push(format!("{location} is the CAS hash {hash}, which is not in external_data"));
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
    use crate::config::{ToolRuntime, WorkflowStepSpec};

    /// One workflow with a single step whose inputs are the given pairs.
    ///
    /// The step is the only place a document can name a hash outside a tool's
    /// `content_map`, which is what the input-side invariant rules are about.
    fn workflow_with_inputs(inputs: &[(&str, &str)]) -> WorkflowSpec {
        WorkflowSpec {
            name: "default".to_string(),
            steps: vec![WorkflowStepSpec {
                id: "s1".to_string(),
                tool: "echo".to_string(),
                inputs: inputs
                    .iter()
                    .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
                    .collect(),
                outputs: BTreeMap::new(),
                max_retries: 0,
                depends_on: Vec::new(),
            }],
            ..WorkflowSpec::default()
        }
    }

    /// One document carrying a single workflow step with the given inputs and
    /// the given `external_data` declarations.
    fn document_with_step_inputs(inputs: &[(&str, &str)], declared: &[Hash]) -> NickelDocument {
        NickelDocument {
            tools: BTreeMap::new(),
            workflows: vec![workflow_with_inputs(inputs)],
            runtime: ConductorRuntimeConfig::default(),
            external_data: declared
                .iter()
                .map(|hash| {
                    (
                        *hash,
                        super::super::ExternalDataEntry {
                            description: None,
                            save_mode: crate::state::OutputSaveMode::Saved,
                        },
                    )
                })
                .collect(),
        }
    }

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
                    version: None,
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
                    version: None,
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
                version: None,
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
                    version: None,
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
                    version: None,
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
                    version: None,
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

    /// Verifies `validate_external_data_invariant` fails when a step input
    /// names a hash through `${external_data.<hash>}` without declaring it.
    #[test]
    fn validate_external_data_invariant_rejects_undeclared_template_reference() {
        let hash_a = Hash::from_content(b"payload-a");
        let reference = format!("${{external_data.{hash_a}}}");
        let doc = document_with_step_inputs(&[("text", reference.as_str())], &[]);

        let err = doc.validate_external_data_invariant().unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("not in external_data"), "error should state the invariant: {msg}");
        assert!(msg.contains(hash_a.to_string().as_str()), "error should name the hash: {msg}");
        assert!(
            msg.contains("workflow 'default' step 's1' input 'text'"),
            "error should name the input that carried the reference: {msg}"
        );
    }

    /// Verifies `validate_external_data_invariant` fails when a step input *is*
    /// a CAS hash the document does not declare.
    ///
    /// This is the import builtin's shape: it reads its payload by hash from an
    /// input value, so the value is a CAS reference even though it carries no
    /// template syntax.
    #[test]
    fn validate_external_data_invariant_rejects_undeclared_bare_hash_input() {
        let hash_a = Hash::from_content(b"payload-a");
        let doc = document_with_step_inputs(&[("hash", hash_a.to_string().as_str())], &[]);

        let err = doc.validate_external_data_invariant().unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("not in external_data"), "error should state the invariant: {msg}");
        assert!(msg.contains(hash_a.to_string().as_str()), "error should name the hash: {msg}");
        assert!(
            msg.contains("workflow 'default' step 's1' input 'hash'"),
            "error should name the input that carried the hash: {msg}"
        );
    }

    /// Verifies both input-side shapes pass once the hash is declared.
    #[test]
    fn validate_external_data_invariant_passes_when_input_hashes_are_declared() {
        let hash_a = Hash::from_content(b"payload-a");
        let reference = format!("${{external_data.{hash_a}}}");
        let doc = document_with_step_inputs(
            &[("text", reference.as_str()), ("hash", hash_a.to_string().as_str())],
            &[hash_a],
        );

        assert!(doc.validate_external_data_invariant().is_ok());
    }

    /// Verifies a tool's default input is held to the same rule as a step's own.
    ///
    /// A step that omits the key inherits the default, so the default names the
    /// hash just as directly as the step would have.
    #[test]
    fn validate_external_data_invariant_rejects_undeclared_default_input() {
        let hash_a = Hash::from_content(b"payload-a");
        let reference = format!("${{external_data.{hash_a}}}");
        let doc = NickelDocument {
            tools: BTreeMap::from([(
                "echo".to_string(),
                ToolSpec {
                    version: None,
                    kind: ToolKindSpec::Builtin { builtin_id: "echo@v1".to_string() },
                    name: "echo".to_string(),
                    inputs: BTreeMap::new(),
                    default_inputs: BTreeMap::from([(
                        "text".to_string(),
                        InputBinding::String(reference),
                    )]),
                    outputs: BTreeMap::new(),
                    runtime: ToolRuntime::default(),
                },
            )]),
            workflows: vec![],
            runtime: ConductorRuntimeConfig::default(),
            external_data: BTreeMap::new(),
        };

        let err = doc.validate_external_data_invariant().unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("tool 'echo' default input 'text'"),
            "error should name the default input that carried the reference: {msg}"
        );
        assert!(msg.contains(hash_a.to_string().as_str()), "error should name the hash: {msg}");
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
                    version: None,
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
                    version: None,
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

    /// A tool's declared output captures reach the unified spec.
    ///
    /// They are the defaults the step worker merges under a step's own
    /// declarations before capturing, so a bridge that dropped them would
    /// leave every tool-level capture inert and a step that reuses another
    /// step's work unable to find the names it binds.
    #[test]
    fn to_unified_carries_a_tools_declared_output_captures() {
        let content = super::super::OutputCaptureSpec {
            name: "content".to_string(),
            capture: "file:out.mkv".to_string(),
            ..super::super::OutputCaptureSpec::default()
        };
        let document = NickelDocument {
            tools: BTreeMap::from([(
                "ffmpeg".to_string(),
                ToolSpec {
                    version: None,
                    kind: ToolKindSpec::Builtin { builtin_id: "ffmpeg@v1".to_string() },
                    name: "ffmpeg".to_string(),
                    outputs: BTreeMap::from([("content".to_string(), content.clone())]),
                    ..ToolSpec::default()
                },
            )]),
            ..NickelDocument::default()
        };

        let unified = document.to_unified();

        assert_eq!(
            unified.tools["ffmpeg"].outputs.get("content"),
            Some(&content),
            "a tool's declared capture defaults must reach the unified spec",
        );
    }

    /// A version declared in the persisted document reaches the unified spec
    /// a workflow row reads, and an undeclared one stays absent.
    ///
    /// The row can only render what the merge carries, so this pins the whole
    /// decode-then-unify path rather than one hop of it.
    #[test]
    fn declared_tool_version_reaches_the_unified_spec() {
        let document = NickelDocument {
            tools: BTreeMap::from([
                (
                    "ffmpeg".to_string(),
                    ToolSpec {
                        version: Some(
                            "autobuild-2025-07-15+1759500000_N-127260-g7bc3576910".to_string(),
                        ),
                        kind: ToolKindSpec::Executable {
                            command: vec!["ffmpeg".to_string()],
                            env_vars: BTreeMap::new(),
                            success_codes: vec![0],
                        },
                        name: "ffmpeg".to_string(),
                        ..ToolSpec::default()
                    },
                ),
                (
                    "echo".to_string(),
                    ToolSpec {
                        version: None,
                        kind: ToolKindSpec::Builtin { builtin_id: "echo@v1".to_string() },
                        name: "echo".to_string(),
                        ..ToolSpec::default()
                    },
                ),
            ]),
            ..NickelDocument::default()
        };

        let bytes = super::super::versions::encode_document(document.clone())
            .expect("encode document declaring a version");
        let decoded = super::super::versions::decode_document(&bytes).expect("decode document");

        assert_eq!(decoded.tools["ffmpeg"].version, document.tools["ffmpeg"].version);
        assert_eq!(decoded.tools["echo"].version, None);
        let unified = decoded.to_unified();
        assert_eq!(
            unified.tools["ffmpeg"].version.as_deref(),
            Some("autobuild-2025-07-15+1759500000_N-127260-g7bc3576910")
        );
        assert_eq!(unified.tools["echo"].version, None);
    }
}
