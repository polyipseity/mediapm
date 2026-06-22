//! # mediapm-conductor
//!
//! Orchestration contracts and state model for mediapm.
//!
//! This crate provides the runtime configuration model, the orchestration
//! state model, and the deterministic workflow coordinator that drives
//! step execution across a pool of actor-backed workers.
//!
//! Allow dead code during incremental implementation — most orchestration
//! scaffolding will be wired as feature implementation progresses.
#![allow(dead_code)]

pub mod api;
pub mod cache;
pub mod defaults;
pub mod error;
pub mod gc;
pub mod model;
pub mod orchestration;
pub mod provision;
pub mod runtime_env;
pub mod simple_conductor;
pub mod tools;

#[cfg(feature = "cli")]
pub mod cli;

// Re-exports for the public API surface.
pub use api::{
    ConductorApi, ManagedToolExecutableResolution, RunSummary, RunWorkflowOptions,
    RuntimeDiagnostics, RuntimeStoragePaths, resolve_managed_tool_executable_with_filesystem_cas,
};
pub use error::ConductorError;
pub use model::config::documents::NickelDocument;
pub use model::config::versions::{decode_document, encode_document};
pub use model::config::{
    ImpureTimestamp, NickelDocumentMetadata, NickelIdentity, OutputCaptureSpec, OutputPolicy,
    ToolInputKind, ToolInputSpec, ToolKindSpec, ToolRuntime, ToolSpec, WorkflowSpec,
    WorkflowStepSpec, default_runtime_inherited_env_vars,
};
pub use model::state::OrchestrationState;
pub use model::state::versions::{decode_state_json, encode_state_json};
pub use model::state::{
    AuxData, OutputRef, OutputSaveMode, PersistenceFlags, ResolvedInput, ToolCallInstance,
};
pub use simple_conductor::SimpleConductor;
pub use tools::registered_builtin_ids;
