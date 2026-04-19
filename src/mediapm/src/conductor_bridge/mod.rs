//! Conductor-facing tool lifecycle helpers used by Phase 3 `mediapm`.
//!
//! This folder-module keeps Phase 3 orchestration glue readable by separating:
//! - Nickel document bootstrapping and persistence,
//! - runtime-storage policy/default handling,
//! - desired-tool reconciliation and prune logic,
//! - tool command/config normalization and validation.

mod documents;
mod runtime_storage;
mod sync;
mod tool_runtime;
mod util;
mod workflows;

#[cfg(test)]
mod tests;

pub(crate) use documents::{ensure_conductor_documents, list_tools, load_machine_document};
pub(crate) use sync::{prune_tool_binary, reconcile_desired_tools};
pub(crate) use tool_runtime::resolve_ffmpeg_slot_limits;
#[allow(unused_imports)]
pub(crate) use workflows::{
    managed_workflow_id_for_media, reconcile_media_workflows,
    resolve_media_variant_output_binding_with_limits,
};

#[cfg(test)]
pub(crate) use workflows::resolve_media_variant_output_binding;

/// Summary of one desired-tool reconciliation pass.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ToolSyncReport {
    /// Tool ids newly inserted into conductor machine config.
    pub added_tool_ids: Vec<String>,
    /// Tool ids promoted/replaced due desired version change.
    pub updated_tool_ids: Vec<String>,
    /// Tool ids that already matched desired state.
    pub unchanged_tool_ids: Vec<String>,
    /// Non-fatal reconciliation notices.
    pub warnings: Vec<String>,
}

/// One conductor tool row for `mediapm tools list` output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConductorToolRow {
    /// Immutable tool id.
    pub tool_id: String,
    /// Whether binary content-map entries are currently present.
    pub has_binary: bool,
    /// Current lifecycle status tracked by lock state.
    pub status: crate::lockfile::ToolRegistryStatus,
}
