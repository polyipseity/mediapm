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
