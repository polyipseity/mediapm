//! Phase 2 Conductor orchestration contracts and deterministic runtime.
//!
//! The crate is organized in CAS-inspired modules:
//! - `api` for public contracts,
//! - `error` for error taxonomy,
//! - `model` for persisted schemas,
//! - `orchestration` for runtime execution behavior.

pub mod api;
pub mod cli;
pub mod error;
pub mod model;
pub mod orchestration;

pub use api::{
    ConductorApi, ResolvedRuntimeStoragePaths, RunSummary, RunWorkflowOptions, RuntimeDiagnostics,
    RuntimeStoragePaths, SchedulerDiagnostics, SchedulerTraceEvent, SchedulerTraceKind,
    ToolRuntimeEstimate, WorkerQueueDiagnostics, default_cas_store_path, default_state_paths,
    default_volatile_state_path, resolve_runtime_storage_paths,
};
pub use error::ConductorError;
pub use model::config::{
    AddExternalDataOptions, AddToolConfigMode, AddToolOptions, ExternalContentRef, InputBinding,
    MachineNickelDocument, NickelDocumentMetadata, NickelIdentity, OutputCaptureSpec, OutputPolicy,
    ProcessSpec, RuntimeStorageConfig, StateNickelDocument, ToolConfigSpec, ToolInputSpec,
    ToolKindSpec, ToolOutputSpec, ToolSpec, UserNickelDocument, WorkflowSpec, WorkflowStepSpec,
    decode_machine_document, decode_state_document, decode_user_document, encode_machine_document,
    encode_state_document, encode_user_document, evaluate_total_configuration_sources,
};
pub use model::state::{
    OrchestrationState, OutputRef, PersistenceFlags, ResolvedInput, ToolCallInstance,
    merge_persistence_flags, persisted_state_json_pretty, persisted_state_json_value,
};
pub use orchestration::SimpleConductor;

/// Returns built-in tool ids known by the conductor runtime.
///
/// This exposes builtin identity from Phase 2 so higher layers (such as
/// `mediapm`) can inspect builtin registration without depending directly on
/// individual builtin crates.
#[must_use]
pub const fn registered_builtin_ids() -> [&'static str; 5] {
    [
        mediapm_conductor_builtin_echo::TOOL_ID,
        mediapm_conductor_builtin_fs::TOOL_ID,
        mediapm_conductor_builtin_import::TOOL_ID,
        mediapm_conductor_builtin_archive::TOOL_ID,
        mediapm_conductor_builtin_export::TOOL_ID,
    ]
}
