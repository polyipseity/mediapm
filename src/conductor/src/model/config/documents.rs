//! Machine and state Nickel document types and mutation helpers.
//!
//! These types carry the full persisted conductor configuration surface for
//! machine-managed and volatile-state documents, together with the document
//! mutation helpers that keep user and machine document semantics aligned.

use std::collections::{BTreeMap, BTreeSet};

use mediapm_cas::Hash;
use serde::{Deserialize, Serialize};

use super::{
    AddExternalDataOptions, AddToolConfigMode, AddToolOptions, ExternalContentRef, ImpureTimestamp,
    NickelDocumentMetadata, RuntimeStorageConfig, ToolConfigSpec, ToolKindSpec, ToolSpec,
    UserNickelDocument, WorkflowSpec,
};
use crate::error::ConductorError;
use crate::model::state::OutputSaveMode;

/// Validates one external-data save mode for machine-document insertion.
fn validate_external_data_save_mode(
    save_mode: Option<OutputSaveMode>,
) -> Result<(), ConductorError> {
    if matches!(save_mode, Some(OutputSaveMode::Unsaved)) {
        return Err(ConductorError::Workflow(
            "external_data save policy cannot be false/unsaved; use true/saved or \"full\""
                .to_string(),
        ));
    }

    Ok(())
}

/// Nickel document loaded from `conductor.machine.ncl`.
///
/// This document shares the same schema surface as `conductor.ncl`. The only
/// special behavior is that runtime writes flow back to this file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct MachineNickelDocument {
    /// Runtime-only metadata (not persisted in `v1.ncl`).
    #[serde(default)]
    pub metadata: NickelDocumentMetadata,
    /// Grouped runtime storage path configuration persisted under `runtime`.
    #[serde(default, skip_serializing_if = "RuntimeStorageConfig::is_empty")]
    pub runtime: RuntimeStorageConfig,
    /// External content metadata keyed by CAS hash identity.
    #[serde(default)]
    pub external_data: BTreeMap<Hash, ExternalContentRef>,
    /// Tool definitions keyed by logical tool name.
    #[serde(default)]
    pub tools: BTreeMap<String, ToolSpec>,
    /// Workflow DAG definitions keyed by workflow id.
    #[serde(default)]
    pub workflows: BTreeMap<String, WorkflowSpec>,
    /// Runtime-only tool execution configuration (`tool_name -> config`).
    #[serde(default)]
    pub tool_configs: BTreeMap<String, ToolConfigSpec>,
    /// Machine-injected timestamps for impure tool calls.
    ///
    /// Layout: `workflow_id -> (step_id -> timestamp)`.
    #[serde(default)]
    pub impure_timestamps: BTreeMap<String, BTreeMap<String, ImpureTimestamp>>,
    /// Current orchestration-state CAS pointer.
    #[serde(default)]
    pub state_pointer: Option<Hash>,
}

/// Nickel document loaded from `.conductor/state.ncl`.
///
/// This document stores volatile runtime-managed state only.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct StateNickelDocument {
    /// Machine-injected timestamps for impure tool calls.
    ///
    /// Layout: `workflow_id -> (step_id -> timestamp)`.
    #[serde(default)]
    pub impure_timestamps: BTreeMap<String, BTreeMap<String, ImpureTimestamp>>,
    /// Current orchestration-state CAS pointer.
    #[serde(default)]
    pub state_pointer: Option<Hash>,
}

impl UserNickelDocument {
    /// Adds one tool definition (and optional tool config) to user document state.
    ///
    /// Validation rules:
    /// - `tool_name` must be non-empty after trimming,
    /// - duplicates fail unless `overwrite_existing = true`,
    /// - builtin tools cannot end up with `content_map` in effective config,
    /// - content-map hashes are reconciled into managed `external_data` roots.
    ///
    /// # Errors
    ///
    /// Returns an error when validation fails (for example empty tool names,
    /// conflicting entries without overwrite mode, or invalid builtin/config
    /// combinations).
    pub fn add_tool(
        &mut self,
        tool_name: impl Into<String>,
        options: AddToolOptions,
    ) -> Result<(), ConductorError> {
        add_tool_to_maps(
            &mut self.tools,
            &mut self.tool_configs,
            &mut self.external_data,
            tool_name.into(),
            options,
        )
    }

    /// Reconciles managed external-data CAS roots with current tool content maps.
    ///
    /// This helper guarantees that every hash referenced from
    /// `tool_configs.<tool>.content_map` appears in `external_data` and removes
    /// stale managed tool-content root entries when no configured tool refers
    /// to those hashes anymore.
    pub fn sync_tool_content_external_data_roots(&mut self) {
        sync_tool_content_external_data_roots(&mut self.external_data, &self.tool_configs);
    }
}

impl MachineNickelDocument {
    /// Adds one tool definition (and optional tool config) to machine document state.
    ///
    /// Validation rules mirror [`UserNickelDocument::add_tool`].
    ///
    /// # Errors
    ///
    /// Returns an error when validation fails (for example empty tool names,
    /// conflicting entries without overwrite mode, or invalid builtin/config
    /// combinations).
    pub fn add_tool(
        &mut self,
        tool_name: impl Into<String>,
        options: AddToolOptions,
    ) -> Result<(), ConductorError> {
        add_tool_to_maps(
            &mut self.tools,
            &mut self.tool_configs,
            &mut self.external_data,
            tool_name.into(),
            options,
        )
    }

    /// Reconciles managed external-data CAS roots with current tool content maps.
    ///
    /// This helper guarantees that every hash referenced from
    /// `tool_configs.<tool>.content_map` appears in `external_data` and removes
    /// stale managed tool-content root entries when no configured tool refers
    /// to those hashes anymore.
    pub fn sync_tool_content_external_data_roots(&mut self) {
        sync_tool_content_external_data_roots(&mut self.external_data, &self.tool_configs);
    }

    /// Adds one external-data entry to machine document state.
    ///
    /// Validation rules:
    /// - `hash` is the external-data map key,
    /// - duplicates fail unless `overwrite_existing = true`.
    ///
    /// # Errors
    ///
    /// Returns an error when `hash` already exists and overwrite mode is not
    /// enabled.
    pub fn add_external_data(
        &mut self,
        hash: Hash,
        options: AddExternalDataOptions,
    ) -> Result<(), ConductorError> {
        validate_external_data_save_mode(options.reference.save)?;

        if !options.overwrite_existing && self.external_data.contains_key(&hash) {
            return Err(ConductorError::Workflow(format!(
                "external data '{hash}' already exists in machine config; set overwrite_existing=true to replace it"
            )));
        }

        self.external_data.insert(hash, options.reference);
        Ok(())
    }
}

/// Prefix reserved for managed external-data descriptions that root tool
/// content-map CAS hashes against pruning.
const MANAGED_TOOL_CONTENT_DESCRIPTION_PREFIX: &str = "managed tool content CAS root for";

/// Collects all CAS hashes referenced by configured tool content maps.
fn collect_tool_content_map_hashes(
    tool_configs: &BTreeMap<String, ToolConfigSpec>,
) -> BTreeSet<Hash> {
    tool_configs
        .values()
        .flat_map(|config| config.content_map.iter().flat_map(|map| map.values().copied()))
        .collect()
}

/// Returns true when one external-data description marks managed tool content.
fn is_managed_tool_content_description(description: Option<&str>) -> bool {
    description.is_some_and(|text| text.starts_with(MANAGED_TOOL_CONTENT_DESCRIPTION_PREFIX))
}

/// Reconciles managed external-data roots against current tool content-map hashes.
///
/// Behavior:
/// - ensures each referenced content-map hash appears at least once in
///   `external_data`,
/// - removes stale managed tool-content entries whose hash no longer appears
///   in any configured tool content map,
/// - preserves non-managed `external_data` entries even when their hashes are
///   unrelated to tool content maps.
fn sync_tool_content_external_data_roots(
    external_data: &mut BTreeMap<Hash, ExternalContentRef>,
    tool_configs: &BTreeMap<String, ToolConfigSpec>,
) {
    let referenced_hashes = collect_tool_content_map_hashes(tool_configs);

    external_data.retain(|hash, reference| {
        referenced_hashes.contains(hash)
            || !is_managed_tool_content_description(reference.description.as_deref())
    });

    for hash in referenced_hashes {
        external_data.entry(hash).or_insert_with(|| ExternalContentRef {
            description: Some(format!("{MANAGED_TOOL_CONTENT_DESCRIPTION_PREFIX} {hash}")),
            save: None,
        });
    }
}

/// Validates and applies one add-tool request against document maps.
///
/// This helper keeps user/machine document add-tool semantics identical.
fn add_tool_to_maps(
    tools: &mut BTreeMap<String, ToolSpec>,
    tool_configs: &mut BTreeMap<String, ToolConfigSpec>,
    external_data: &mut BTreeMap<Hash, ExternalContentRef>,
    tool_name: String,
    options: AddToolOptions,
) -> Result<(), ConductorError> {
    if tool_name.trim().is_empty() {
        return Err(ConductorError::Workflow(
            "tool name cannot be empty when adding a tool".to_string(),
        ));
    }

    if !options.overwrite_existing
        && (tools.contains_key(&tool_name) || tool_configs.contains_key(&tool_name))
    {
        return Err(ConductorError::Workflow(format!(
            "tool '{tool_name}' already exists; set overwrite_existing=true to replace it"
        )));
    }

    validate_add_tool_config_mode(&tool_name, &options.spec, &options.config_mode, tool_configs)?;

    tools.insert(tool_name.clone(), options.spec);
    match options.config_mode {
        AddToolConfigMode::KeepExisting => {}
        AddToolConfigMode::Replace(config) => {
            tool_configs.insert(tool_name, config);
        }
        AddToolConfigMode::Remove => {
            tool_configs.remove(&tool_name);
        }
    }

    sync_tool_content_external_data_roots(external_data, tool_configs);

    Ok(())
}

/// Validates builtin/content-map invariants for one add-tool request.
fn validate_add_tool_config_mode(
    tool_name: &str,
    spec: &ToolSpec,
    config_mode: &AddToolConfigMode,
    existing_configs: &BTreeMap<String, ToolConfigSpec>,
) -> Result<(), ConductorError> {
    let has_content_map = match config_mode {
        AddToolConfigMode::KeepExisting => existing_configs
            .get(tool_name)
            .is_some_and(|config| config.content_map.as_ref().is_some()),
        AddToolConfigMode::Replace(config) => config.content_map.as_ref().is_some(),
        AddToolConfigMode::Remove => false,
    };

    if has_content_map && matches!(&spec.kind, ToolKindSpec::Builtin { .. }) {
        return Err(ConductorError::Workflow(format!(
            "tool '{tool_name}' is builtin and cannot have tool_configs.content_map"
        )));
    }

    let has_input_defaults = match config_mode {
        AddToolConfigMode::KeepExisting => {
            existing_configs.get(tool_name).is_some_and(|config| !config.input_defaults.is_empty())
        }
        AddToolConfigMode::Replace(config) => !config.input_defaults.is_empty(),
        AddToolConfigMode::Remove => false,
    };

    if has_input_defaults && matches!(&spec.kind, ToolKindSpec::Builtin { .. }) {
        return Err(ConductorError::Workflow(format!(
            "tool '{tool_name}' is builtin and cannot have tool_configs.input_defaults"
        )));
    }

    let has_env_vars = match config_mode {
        AddToolConfigMode::KeepExisting => {
            existing_configs.get(tool_name).is_some_and(|config| !config.env_vars.is_empty())
        }
        AddToolConfigMode::Replace(config) => !config.env_vars.is_empty(),
        AddToolConfigMode::Remove => false,
    };

    if has_env_vars && matches!(&spec.kind, ToolKindSpec::Builtin { .. }) {
        return Err(ConductorError::Workflow(format!(
            "tool '{tool_name}' is builtin and cannot have tool_configs.env_vars"
        )));
    }

    Ok(())
}
