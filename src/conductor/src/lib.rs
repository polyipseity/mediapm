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
};
pub use model::state::{
    OrchestrationState, OutputRef, PersistenceFlags, ResolvedInput, ToolCallInstance,
    merge_persistence_flags, persisted_state_json_pretty, persisted_state_json_value,
};
pub use orchestration::SimpleConductor;
