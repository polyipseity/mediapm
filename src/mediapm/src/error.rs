//! Phase 3 `mediapm` error taxonomy.
//!
//! This module centralizes crate-level error variants so all submodules share
//! one consistent error contract while preserving operation/path context.

use std::path::PathBuf;

use mediapm_conductor::ConductorError;
use thiserror::Error;

/// Error category for Phase 3 orchestration and runtime coordination.
#[derive(Debug, Error)]
pub enum MediaPmError {
    /// Source URI does not satisfy scheme requirements.
    #[error("invalid source URI: {0}")]
    InvalidSource(String),
    /// Workflow/state consistency violation.
    #[error("workflow error: {0}")]
    Workflow(String),
    /// Serialization or schema conversion failure.
    #[error("serialization error: {0}")]
    Serialization(String),
    /// Filesystem I/O failure with operation context.
    #[error("I/O error while {operation} at '{path}': {source}")]
    Io {
        /// Human-readable operation label.
        operation: String,
        /// Filesystem target path.
        path: PathBuf,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },
    /// Error propagated from Phase 2 conductor.
    #[error("conductor error: {0}")]
    Conductor(#[from] ConductorError),
}
