//! # mediapm-conductor
//!
//! Orchestration contracts and state model for mediapm.
//!
//! This crate provides the runtime configuration model, the orchestration
//! state model, and the deterministic workflow coordinator that drives
//! step execution across a pool of actor-backed workers.
//!
// Targeted dead-code suppressions for intentional placeholder items.

pub mod api;
pub mod cache;
pub mod cache_user_level;
pub mod config;
pub mod defaults;
pub mod error;
pub mod gc;
pub mod orchestration;
pub mod provision;
pub mod runtime_env;
pub mod simple_conductor;
pub mod state;
pub mod tools;

// CLI sub-modules (flattened from cli/ directory).
#[cfg(feature = "cli")]
pub mod cli;
pub mod cli_document_io;
#[cfg(feature = "cli")]
pub mod cli_tools;

// Re-exports for the public API surface.
pub use api::{
    ConductorApi, ManagedToolExecutableResolution, RunSummary, RunWorkflowOptions,
    RuntimeDiagnostics, RuntimeStoragePaths, resolve_managed_tool_executable_with_filesystem_cas,
};
pub use config::documents::NickelDocument;
pub use config::versions::{decode_document, encode_document};
pub use config::{
    ImpureTimestamp, InputBinding, OutputCaptureSpec, OutputPolicy, SaveMode, ToolInputKind,
    ToolInputSpec, ToolKindSpec, ToolRuntime, ToolSpec, WorkflowSpec, WorkflowStepSpec,
    default_runtime_inherited_env_vars,
};
pub use error::ConductorError;
pub use simple_conductor::SimpleConductor;
pub use state::OrchestrationState;
pub use state::versions::{decode_state_json, encode_state_json};
pub use state::{
    AuxData, OutputRef, OutputSaveMode, PersistenceFlags, ResolvedInput, ToolCallInstance,
};
pub use tools::registered_builtin_ids;
