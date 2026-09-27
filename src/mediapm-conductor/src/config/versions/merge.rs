//! Presence-preserving merge of multiple configuration documents.
//!
//! Merging operates on the raw latest-schema wire envelope
//! ([`super::v_latest::NickelEnvelopeLatest`]) so that explicit values never
//! lose to implicit serde defaults: this module must live inside `versions/`
//! because its input type is a versioned boundary type that no caller outside
//! `versions/` may name. [`super::merge_document_sources`] is the sole
//! unversioned entry point; it loads and merges in one step so callers never
//! construct a [`SourceDocument`] themselves.
//!
//! Human-readable fields (`external_data` descriptions, workflow
//! `display_name`/`description`) are never merged or compared across
//! documents.
//!
//! ## DO NOT REMOVE: versions policy guard
//!
//! - This file is envelope-shaped by necessity, not by convenience: merging
//!   runs on the presence-preserving wire envelope, so it lives inside
//!   `versions/` where that type is nameable. No unversioned struct is
//!   declared here.
//! - `super::merge_document_sources` is the only way in from outside
//!   `versions/`; a caller outside must never reach `SourceDocument`,
//!   `MergeState`, or `merge_documents` directly.
//! - Do not re-export anything from this file. A re-export would move a
//!   versioned symbol back out of the boundary these entry points protect.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::config::NickelDocument;
use crate::error::ConductorError;

use super::v_latest::{
    ConductorRuntimeConfigLatest, NICKEL_VERSION_LATEST, NickelEnvelopeLatest, OutputPolicyLatest,
    WorkflowSpecLatest,
};

/// Reads and evaluates one `.ncl` file into a presence-preserving source
/// document.
///
/// The envelope keeps boundary defaults UNAPPLIED so a later merge can tell an
/// explicitly written field from an implicitly defaulted one.
///
/// # Errors
///
/// Returns [`ConductorError::Io`] when the file cannot be read, and
/// [`ConductorError::Serialization`] when the bytes are not UTF-8 or the
/// Nickel evaluation/migration pipeline fails.
pub(super) fn load_source(path: &Path) -> Result<SourceDocument, ConductorError> {
    let bytes = std::fs::read(path).map_err(|source| ConductorError::Io {
        operation: "reading config document".to_string(),
        path: path.to_path_buf(),
        source,
    })?;
    let envelope = super::decode_document_envelope(&bytes)?;
    Ok(SourceDocument { path: path.to_path_buf(), envelope })
}

/// Copies human-readable fields from the old wire envelope onto the outgoing
/// document for entries that already exist there; fresh entries keep `None`.
///
/// Only fills fields that are currently `None` — an explicit value in the
/// outgoing document always wins (explicit beats implicit).
pub(super) fn preserve_readable_fields(outgoing: &mut NickelDocument, old: &NickelEnvelopeLatest) {
    for (hash, old_entry) in &old.external_data {
        if let Some(entry) =
            outgoing.external_data.get_mut(hash).filter(|entry| entry.description.is_none())
        {
            entry.description.clone_from(&old_entry.description);
        }
    }
    for old_workflow in &old.workflows {
        if let Some(new_workflow) =
            outgoing.workflows.iter_mut().find(|w| w.name == old_workflow.name)
        {
            if new_workflow.display_name.is_none() {
                new_workflow.display_name.clone_from(&old_workflow.display_name);
            }
            if new_workflow.description.is_none() {
                new_workflow.description.clone_from(&old_workflow.description);
            }
        }
    }
}

/// A wire envelope paired with its source file path.
///
/// Used during configuration loading to track which file each document
/// originated from — critical for error reporting on merge conflicts.  The
/// envelope is the presence-preserving wire form (boundary defaults NOT yet
/// applied) so merging can distinguish explicit from implicit values.
#[derive(Debug, Clone)]
pub(super) struct SourceDocument {
    /// Absolute path to the `.ncl` file this document was loaded from.
    pub path: PathBuf,
    /// The parsed latest-schema wire envelope.
    pub envelope: NickelEnvelopeLatest,
}

/// A conflict between two source documents during merge.
#[derive(Debug, Clone)]
pub(super) enum MergeConflict {
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
pub(super) fn merge_documents(docs: &[SourceDocument]) -> Result<NickelDocument, ConductorError> {
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
        if !rt.environment.platform_inherited_env_vars.is_empty() {
            match self.runtime_platform_source.as_ref() {
                Some(first_path)
                    if self.merged.runtime.environment.platform_inherited_env_vars
                        != rt.environment.platform_inherited_env_vars =>
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
                    self.merged.runtime.environment.platform_inherited_env_vars =
                        rt.environment.platform_inherited_env_vars.clone();
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use mediapm_cas::Hash;

    use super::*;
    use crate::config::PlatformInheritedEnvVars;
    use crate::config::versions::v_latest::{
        ConductorRuntimeConfigLatest, ExternalDataEntryLatest, NICKEL_VERSION_LATEST,
        NickelEnvelopeLatest, OutputPolicyLatest, RuntimePlatformEnvConfigLatest, ToolKindLatest,
        ToolRuntimeLatest, ToolSpecLatest, WorkflowSpecLatest, WorkflowStepSpecLatest,
    };

    /// Verifies `merge_documents` with an empty list returns a default document.
    #[test]
    fn merge_documents_empty_list() {
        let doc = merge_documents(&[]).unwrap();
        assert!(doc.tools.is_empty());
        assert!(doc.workflows.is_empty());
    }

    /// Documents that grouped-field merge across two documents keeps both
    /// the scalar `retry_impure` flag and the nested platform env-var config:
    /// one document sets `retry_impure = true`, the other sets
    /// `platform_inherited_env_vars.macos = ["PATH"]`, and the merged result
    /// carries both.
    #[test]
    fn merge_runtime_merges_grouped_fields() {
        let env_retry = NickelEnvelopeLatest {
            version: NICKEL_VERSION_LATEST,
            tools: BTreeMap::new(),
            workflows: Vec::new(),
            external_data: BTreeMap::new(),
            runtime: ConductorRuntimeConfigLatest {
                environment: RuntimePlatformEnvConfigLatest::default(),
                retry_impure: Some(true),
            },
        };
        let mut env_platform = env_retry.clone();
        env_platform.runtime = ConductorRuntimeConfigLatest {
            environment: RuntimePlatformEnvConfigLatest {
                platform_inherited_env_vars: PlatformInheritedEnvVars {
                    macos: vec!["PATH".to_string()],
                    ..Default::default()
                },
            },
            retry_impure: None,
        };

        let doc = merge_documents(&[
            SourceDocument { path: PathBuf::from("a.ncl"), envelope: env_retry },
            SourceDocument { path: PathBuf::from("b.ncl"), envelope: env_platform },
        ])
        .expect("merge succeeds");

        assert!(doc.runtime.retry_impure, "retry_impure must survive merge");
        assert!(
            doc.runtime.environment.platform_inherited_env_vars.macos.contains(&"PATH".to_string()),
            "platform env vars must survive merge"
        );
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
        env2.runtime.environment.platform_inherited_env_vars = PlatformInheritedEnvVars {
            windows: Vec::new(),
            linux: vec!["PATH".into()],
            macos: Vec::new(),
        };
        let result =
            merge_documents(&[source("/dummy/a.ncl", env1), source("/dummy/b.ncl", env2)]).unwrap();
        assert_eq!(
            result.runtime.environment.platform_inherited_env_vars.env_names_for("linux"),
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
        env1.runtime.environment.platform_inherited_env_vars = PlatformInheritedEnvVars {
            windows: Vec::new(),
            linux: vec!["PATH".into()],
            macos: Vec::new(),
        };
        let mut env2 = envelope();
        env2.runtime.environment.platform_inherited_env_vars = PlatformInheritedEnvVars {
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
}
