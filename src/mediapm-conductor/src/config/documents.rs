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

/// Extracts every CAS hash one template string reads through
/// `${external_data.<hash>}`.
///
/// The scan is the resolver's own parse of the string, so it agrees with what a
/// step reads by construction: a reference that carries post-processing
/// selectors (`${external_data.<hash>:file(name)}`) and a reference inside a
/// conditional branch are both found, because both are resolved. A reference the
/// resolver would reject, a `${…}` after a `\${` escape, and a `${…}` inside a
/// selector argument name nothing here, because the resolver never reads them.
///
/// Both this crate's document invariant and the mediapm application's
/// declaration pass call this function, so the two cannot disagree with the
/// engine, or with each other, about which strings name bytes.
#[must_use]
pub fn external_data_reference_hashes(value: &str) -> Vec<Hash> {
    crate::orchestration::step_worker::template::external_data_hashes(value)
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
                    // The staging decision travels with the payload it stages:
                    // an `in_place` or `hard_link` step reads the provisioned
                    // tree, so it needs the root the document recorded beside it.
                    payload_root: spec.runtime.payload_root.clone(),
                    payload_staging: spec.runtime.payload_staging,
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
    /// - a `${external_data.<hash>}` reference in a tool's command part, which is
    ///   the one surface the template engine resolves with a CAS handle, so the
    ///   step reads those bytes out of CAS; and
    /// - an input value that *is* a hash, which a builtin such as `import`
    ///   consumes as one.
    ///
    /// A `${external_data.<hash>}` reference in a step input or in a tool's
    /// `default_inputs` is checked as well, and there the engine cannot resolve
    /// it: step inputs are resolved without a CAS handle, so such a reference
    /// fails its step at run time instead of reading anything. The rule stays on
    /// that surface because it errs toward keeping bytes — a declaration costs
    /// nothing while nothing can read them, and it is already in place for the
    /// run that can.
    ///
    /// A reference is found by parsing the template with the engine's own
    /// scanner, so a reference that carries post-processing selectors
    /// (`${external_data.<hash>:file(name)}`) and one inside either branch of a
    /// conditional are both found; a `${…}` after a `\${` escape or inside a
    /// selector argument is not, because the resolver never interpolates those.
    ///
    /// Step output captures are deliberately not checked: a capture selector is
    /// matched literally against the sandbox, never interpolated, so a
    /// `${external_data.…}` inside one resolves to nothing.
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
    /// without being declared, with the tool, workflow, step, and input key or
    /// command part that named it.
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
            if let ToolKindSpec::Executable { command, .. } = &spec.kind {
                for (index, part) in command.iter().enumerate() {
                    self.collect_reference_gaps(
                        &format!("tool '{tool_name}' command part {index}"),
                        part,
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

    /// Validates that every tool whose payload staging reads a payload root
    /// names one.
    ///
    /// A tool with a non-empty `content_map` and an empty `payload_root` cannot
    /// be staged `in_place` or `hard_link`, because both strategies read that
    /// directory. The check is on the document rather than on the step so the
    /// failure names the tool at load, and it never downgrades the strategy: a
    /// document that names neither a root nor `copy` is a document that never
    /// decided how its payload reaches the step.
    ///
    /// `copy` needs no root, and a tool with no content map stages nothing, so
    /// both load with an empty root.
    ///
    /// # Errors
    ///
    /// Returns [`ConductorError::Workflow`] listing every tool whose strategy
    /// needs the root it does not name.
    pub(crate) fn validate_payload_staging_invariant(&self) -> Result<(), ConductorError> {
        let mut unusable: Vec<String> = Vec::new();

        for (tool_name, spec) in &self.tools {
            let runtime = &spec.runtime;
            let needs_root = matches!(
                runtime.payload_staging,
                crate::config::PayloadStaging::InPlace | crate::config::PayloadStaging::HardLink
            );
            if needs_root && !runtime.content_map.is_empty() && runtime.payload_root.is_empty() {
                unusable.push(format!(
                    "tool '{tool_name}' stages its payload by '{}' but names no payload_root",
                    runtime.payload_staging.as_str()
                ));
            }
        }

        if unusable.is_empty() {
            Ok(())
        } else {
            Err(ConductorError::Workflow(format!(
                "document stages payloads without a payload root: {}",
                unusable.join("; ")
            )))
        }
    }

    /// Validates that every `builtin` tool spec names an id this build
    /// registers.
    ///
    /// The registry answers what this build can dispatch, so the check is
    /// build-dependent by design: a build that compiles the test doubles
    /// resolves them, and a build that does not rejects a document naming one.
    /// Running it when the document loads makes an unavailable builtin fail
    /// before any step that needs it runs, and the two loaders turn that
    /// failure into different outcomes: a user document fails the run that
    /// reads it, while a `mediapm` generated document is discarded and
    /// regenerated from the registry, which is why `encode_document` leaves the
    /// check out.
    ///
    /// A tool key is not reserved: an executable spec may be keyed with any
    /// string, including one spelled like a builtin id.
    ///
    /// # Errors
    ///
    /// Returns [`ConductorError::Workflow`] listing every tool whose builtin id
    /// this build does not register.
    pub(crate) fn validate_builtin_tool_ids(&self) -> Result<(), ConductorError> {
        let registered = crate::tools::registered_builtin_ids();
        let mut unknown: Vec<String> = Vec::new();

        for (tool_name, spec) in &self.tools {
            let ToolKindSpec::Builtin { builtin_id } = &spec.kind else {
                continue;
            };
            if !registered.contains(builtin_id) {
                unknown.push(format!("tool '{tool_name}' names builtin '{builtin_id}'"));
            }
        }

        if unknown.is_empty() {
            Ok(())
        } else {
            Err(ConductorError::Workflow(format!(
                "document names builtin ids this build does not register: {}",
                unknown.join("; ")
            )))
        }
    }

    /// Records every hash one input value names that `external_data` omits.
    ///
    /// `location` names the place in the document that carries the value, so a
    /// rejection points at the input to fix rather than at the document as a
    /// whole.
    fn collect_input_hash_gaps(&self, location: &str, value: &str, missing: &mut Vec<String>) {
        self.collect_reference_gaps(location, value, missing);

        let trimmed = value.trim();
        if let Ok(hash) = trimmed.parse::<Hash>()
            && !self.external_data.contains_key(&hash)
        {
            missing
                .push(format!("{location} is the CAS hash {hash}, which is not in external_data"));
        }
    }

    /// Records every `${external_data.<hash>}` reference one value makes that
    /// `external_data` omits.
    ///
    /// Separate from [`Self::collect_input_hash_gaps`] because only an input
    /// value is read as a hash by a builtin: a command part that happens to
    /// spell out a hash is a literal argument, not a CAS read.
    fn collect_reference_gaps(&self, location: &str, value: &str, missing: &mut Vec<String>) {
        for hash in external_data_reference_hashes(value) {
            if !self.external_data.contains_key(&hash) {
                missing.push(format!(
                    "{location} references hash {hash} through ${{external_data.…}}, which is \
                     not in external_data"
                ));
            }
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
    /// Steps are the surface the input-side rules are about: a bare hash input
    /// and a `${external_data.<hash>}` reference both reach the document through
    /// one. Tests that need a hash named outside a step build the tool or the
    /// command part they want instead.
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

    /// Verifies a reference that carries a post-processing selector is checked
    /// by the hash it reads, not by the selector text after it.
    ///
    /// The engine strips `:file(…)` before it resolves the base reference, so
    /// `${external_data.<hash>:file(name)}` reads those bytes out of CAS and the
    /// GC must keep them.
    #[test]
    fn validate_external_data_invariant_rejects_selector_carrying_reference() {
        let hash_a = Hash::from_content(b"payload-a");
        let reference = format!("${{external_data.{hash_a}:file(payload.bin)}}");
        let doc = document_with_step_inputs(&[("text", reference.as_str())], &[]);

        let err = doc.validate_external_data_invariant().unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains(hash_a.to_string().as_str()),
            "error should name the hash the reference reads, not the selector text: {msg}"
        );
    }

    /// Verifies the same reference passes once the hash it reads is declared.
    #[test]
    fn validate_external_data_invariant_accepts_declared_selector_carrying_reference() {
        let hash_a = Hash::from_content(b"payload-a");
        let reference = format!("${{external_data.{hash_a}:file(payload.bin)}}");
        let doc = document_with_step_inputs(&[("text", reference.as_str())], &[hash_a]);

        doc.validate_external_data_invariant().expect("a declared selector reference is rooted");
    }

    /// Verifies a reference inside a tool command part is checked.
    ///
    /// Command parts are interpolated before the process runs, so a reference
    /// there reads CAS exactly as an input does.
    #[test]
    fn validate_external_data_invariant_rejects_undeclared_command_part_reference() {
        let hash_a = Hash::from_content(b"payload-a");
        let reference = format!("${{external_data.{hash_a}}}");
        let doc = NickelDocument {
            tools: BTreeMap::from([(
                "probe".to_string(),
                ToolSpec {
                    version: None,
                    kind: ToolKindSpec::Executable {
                        command: vec!["probe".to_string(), reference],
                        env_vars: BTreeMap::new(),
                        success_codes: vec![0],
                    },
                    name: "probe".to_string(),
                    inputs: BTreeMap::new(),
                    default_inputs: BTreeMap::new(),
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
            msg.contains("tool 'probe' command part 1"),
            "error should name the command part that carried the reference: {msg}"
        );
        assert!(msg.contains(hash_a.to_string().as_str()), "error should name the hash: {msg}");
    }

    /// Verifies a command part that merely spells out a hash is not treated as a
    /// CAS read.
    ///
    /// Only an input value is read as a hash, by a builtin such as `import`; a
    /// command part is an argument, and requiring a declaration for every hex
    /// string a command mentions would reject documents that name no bytes.
    #[test]
    fn validate_external_data_invariant_ignores_bare_hash_command_parts() {
        let hash_a = Hash::from_content(b"payload-a");
        let doc = NickelDocument {
            tools: BTreeMap::from([(
                "probe".to_string(),
                ToolSpec {
                    version: None,
                    kind: ToolKindSpec::Executable {
                        command: vec!["probe".to_string(), hash_a.to_string()],
                        env_vars: BTreeMap::new(),
                        success_codes: vec![0],
                    },
                    name: "probe".to_string(),
                    inputs: BTreeMap::new(),
                    default_inputs: BTreeMap::new(),
                    outputs: BTreeMap::new(),
                    runtime: ToolRuntime::default(),
                },
            )]),
            workflows: vec![],
            runtime: ConductorRuntimeConfig::default(),
            external_data: BTreeMap::new(),
        };

        doc.validate_external_data_invariant()
            .expect("a bare hash argument names no bytes the engine reads");
    }

    /// Verifies a reference inside a conditional branch is checked.
    ///
    /// The resolver picks a branch at run time and wraps a branch that names a
    /// reference, so either branch can read CAS and both have to be declared.
    #[test]
    fn validate_external_data_invariant_rejects_reference_in_conditional_branch() {
        let hash_a = Hash::from_content(b"payload-a");
        let reference =
            format!("${{*inputs.probe ? external_data.{hash_a}:file(payload.bin) | ''}}");
        let doc = document_with_step_inputs(&[("text", reference.as_str())], &[]);

        let err = doc.validate_external_data_invariant().unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains(hash_a.to_string().as_str()),
            "error should name the hash the branch reads: {msg}"
        );
    }

    /// Verifies an escaped reference names nothing, because the resolver reads it
    /// as literal text.
    #[test]
    fn validate_external_data_invariant_ignores_escaped_reference() {
        let hash_a = Hash::from_content(b"payload-a");
        let reference = format!("\\${{external_data.{hash_a}}}");
        let doc = document_with_step_inputs(&[("text", reference.as_str())], &[]);

        doc.validate_external_data_invariant()
            .expect("an escaped reference is literal text, not a CAS read");
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

    /// One document carrying the given tool specs, keyed as given.
    fn document_with_tools(tools: BTreeMap<String, ToolSpec>) -> NickelDocument {
        NickelDocument { tools, ..NickelDocument::default() }
    }

    /// A `builtin` spec naming `builtin_id`, under the tool name `name`.
    fn builtin_spec(name: &str, builtin_id: &str) -> ToolSpec {
        ToolSpec {
            version: None,
            kind: ToolKindSpec::Builtin { builtin_id: builtin_id.to_string() },
            name: name.to_string(),
            ..ToolSpec::default()
        }
    }

    /// An `executable` spec whose command is its own name.
    fn executable_spec(name: &str) -> ToolSpec {
        ToolSpec {
            version: None,
            kind: ToolKindSpec::Executable {
                command: vec![name.to_string()],
                env_vars: BTreeMap::new(),
                success_codes: vec![0],
            },
            name: name.to_string(),
            ..ToolSpec::default()
        }
    }

    /// Verifies a `builtin` spec naming an id this build does not register is
    /// rejected, naming both the tool and the id so the document is fixable
    /// from the message.
    #[test]
    fn strict_a_builtin_spec_naming_an_unregistered_id_is_rejected() {
        let doc = document_with_tools(BTreeMap::from([(
            "ghost".to_string(),
            builtin_spec("ghost", "ghost@v1"),
        )]));

        let error = doc.validate_builtin_tool_ids().expect_err("ghost@v1 is not registered");
        let message = error.to_string();
        assert!(message.contains("tool 'ghost'"), "{message}");
        assert!(message.contains("ghost@v1"), "{message}");
    }

    /// Verifies one error names every offending tool, so a document with two
    /// misses does not need two runs to fix.
    #[test]
    fn strict_every_unregistered_builtin_is_reported_not_just_the_first() {
        let doc = document_with_tools(BTreeMap::from([
            ("first".to_string(), builtin_spec("first", "absent@v1")),
            ("second".to_string(), builtin_spec("second", "missing@v2")),
        ]));

        let error = doc.validate_builtin_tool_ids().expect_err("neither id is registered");
        let message = error.to_string();
        assert!(message.contains("absent@v1"), "{message}");
        assert!(message.contains("missing@v2"), "{message}");
    }

    /// Verifies a tool key is not reserved: an executable spec keyed with a
    /// builtin id is a tool named that way, not a builtin collision.
    #[test]
    fn strict_an_executable_spec_may_take_a_builtin_id_as_its_key() {
        let doc = document_with_tools(BTreeMap::from([(
            "echo@v1".to_string(),
            executable_spec("echo@v1"),
        )]));

        assert!(
            doc.validate_builtin_tool_ids().is_ok(),
            "a tool key carries no reserved meaning, so an executable may spell one like a builtin id"
        );
    }

    /// Verifies every real builtin loads, which is the positive half of the
    /// check: the five ids this build always registers.
    #[test]
    fn strict_the_five_production_builtins_load() {
        let ids = ["archive@v1", "echo@v1", "export@v1", "fs@v1", "import@v1"];
        let tools = ids.iter().map(|id| ((*id).to_string(), builtin_spec(id, id))).collect();
        let doc = document_with_tools(tools);

        assert!(doc.validate_builtin_tool_ids().is_ok(), "the five real builtins are registered");
    }

    /// Verifies the check is build-dependent by design: a build carrying the
    /// `test-builtins` feature registers the doubles, so a document a
    /// production build rejects loads here.
    #[cfg(feature = "test-builtins")]
    #[test]
    fn strict_a_test_build_resolves_the_doubles() {
        let doc = document_with_tools(BTreeMap::from([(
            "flaky".to_string(),
            builtin_spec("flaky", "flaky@v1"),
        )]));

        assert!(
            doc.validate_builtin_tool_ids().is_ok(),
            "a test build registers flaky@v1, so the same document a production build rejects loads here"
        );
    }
}
