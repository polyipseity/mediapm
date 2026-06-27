//! Conductor bridge — MediaPM ↔ Conductor integration layer.
//!
//! This module translates mediapm's configuration and lifecycle model into
//! conductor tool specifications, runtime settings, and Nickel documents.
//!
//! It owns:
//! - [`ToolSyncReport`] — output of one `mediapm tool sync` pass
//! - [`reconcile_desired_tools`] — top-level async reconciliation entry point
//! - Document load/save helpers for generated and state documents
//! - Runtime-storage path defaults
//! - Tool-spec and runtime builders for managed executables
//! - Shared string constants and utility helpers
//!
//! Sub-modules:
//! - [`constants`] — shared string constants for input/output keys and tool IDs
//! - [`documents`] — conductor NCL document loading, saving, and builtin registration
//! - [`runtime_storage`] — runtime-storage path resolution and normalization
//! - [`sync`] — tool reconciliation coordinator and sub-phases
//! - [`tool_runtime`] — managed-tool runtime contract builders
//! - [`util`] — shared IO and time helpers

pub(crate) mod constants;
pub(crate) mod documents;
pub(crate) mod runtime_storage;
pub(crate) mod sync;
pub(crate) mod tool_runtime;
pub(crate) mod util;

// (no re-exports currently)
