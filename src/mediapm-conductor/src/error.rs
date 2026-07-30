//! # CND error code catalog
//!
//! | Code | Title | Description | Corresponding variant |
//! |------|-------|-------------|-----------------------|
//! | CND-E001 | Workflow error | Invalid config, missing tools, orchestration failure | `Workflow` |
//! | CND-E002 | CAS error | Error forwarded from the CAS layer | `Cas` |
//! | CND-E003 | Serialization error | JSON or Nickel encode/decode failure | `Serialization` |
//! | CND-E004 | I/O error | Filesystem operation failed | `Io` |
//! | CND-E005 | Internal error | Unexpected conductor state | `Internal` |
//!
//! See `.agents/instructions/error-codes.instructions.md` for the
//! workspace-wide error code reference.

use std::path::PathBuf;

use mediapm_cas::CasError;
use thiserror::Error;

/// Error type for all conductor operations.
#[derive(Debug, Error)]
pub enum ConductorError {
    /// Workflow-level error (invalid config, missing tools, etc.).
    #[error("{0}")]
    Workflow(String),

    /// CAS-level error forwarded from the CAS implementation.
    #[error("CAS error: {0}")]
    Cas(#[from] CasError),

    /// Serialization error (JSON encode/decode, Nickel eval, etc.).
    #[error("serialization error: {0}")]
    Serialization(String),

    /// I/O error with context.
    #[error("I/O error {operation:?} on `{path}`: {source}")]
    Io {
        /// Description of the failed operation.
        operation: String,
        /// Path involved in the I/O operation.
        path: PathBuf,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },

    /// Internal conductor error (unexpected state, invariants, etc.).
    #[error("internal conductor error: {0}")]
    Internal(String),
}

impl ConductorError {
    /// Builds an `Io` variant with the given operation description, path, and source.
    #[must_use]
    pub fn io(
        operation: impl Into<String>,
        path: impl Into<PathBuf>,
        source: std::io::Error,
    ) -> Self {
        Self::Io { operation: operation.into(), path: path.into(), source }
    }

    /// Builds an `Internal` variant with a formatted RPC error message.
    #[must_use]
    pub fn rpc_error(service: &str, err: impl std::fmt::Display) -> Self {
        Self::Internal(format!("RPC call to '{service}' failed: {err}"))
    }
}
