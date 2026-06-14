//! Conductor crate error types.

use std::path::PathBuf;

use mediapm_cas::CasError;
use thiserror::Error;

/// Conductor-level error category.
#[derive(Debug, Error)]
pub enum ConductorError {
    /// Workflow references could not be loaded or interpreted.
    #[error("workflow file error: {0}")]
    Workflow(String),
    /// CAS operation failed while materializing state.
    #[error("cas operation failed: {0}")]
    Cas(#[from] CasError),
    /// Serialization/deserialization failed.
    #[error("serialization error: {0}")]
    Serialization(String),
    /// Local filesystem I/O failed.
    #[error("io error while {operation} at {path}: {source}")]
    Io {
        /// Operation name.
        operation: String,
        /// Path involved.
        path: PathBuf,
        /// Underlying I/O source.
        #[source]
        source: std::io::Error,
    },
    /// Internal synchronization or invariant failure.
    #[error("internal conductor error: {0}")]
    Internal(String),
}

impl ConductorError {
    /// Constructs an I/O error with operation name and path.
    pub fn io(
        operation: impl Into<String>,
        path: impl Into<PathBuf>,
        source: std::io::Error,
    ) -> Self {
        Self::Io { operation: operation.into(), path: path.into(), source }
    }

    /// Constructs an internal RPC error with operation name and underlying error.
    pub(crate) fn rpc_error(operation: &'static str, err: impl std::fmt::Display) -> Self {
        Self::Internal(format!("{operation} RPC failed: {err}"))
    }
}
