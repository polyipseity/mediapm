//! Conductor crate error types.

use std::path::PathBuf;

use mediapm_cas::{CasError, Hash};
use thiserror::Error;

/// Structured context for one `${step_output...}` corruption-read failure.
///
/// This payload is intentionally stored behind a `Box` in [`ConductorError`]
/// so the top-level error enum remains small enough for strict `clippy`
/// `result_large_err` checks while still preserving the full recovery context
/// needed by coordinator retry logic.
#[derive(Debug, Error)]
#[error(
    "workflow '{workflow_name}' step '{consumer_step_id}' failed to read output '{producer_step_id}.{output_name}' at hash '{output_hash}' due to CAS corruption: {detail}"
)]
pub struct CorruptWorkflowOutputContext {
    /// Workflow id where the consumer step was executing.
    pub workflow_name: String,
    /// Consumer step id that attempted to read the output binding.
    pub consumer_step_id: String,
    /// Producer step id referenced by `${step_output...}`.
    pub producer_step_id: String,
    /// Producer output name referenced by `${step_output...}`.
    pub output_name: String,
    /// Corrupt output hash that failed CAS read verification.
    pub output_hash: Hash,
    /// Original CAS corruption detail.
    pub detail: String,
}

/// Conductor-level error category.
#[derive(Debug, Error)]
pub enum ConductorError {
    /// Workflow references could not be loaded or interpreted.
    #[error("workflow file error: {0}")]
    Workflow(String),
    /// CAS operation failed while materializing state.
    #[error("cas operation failed: {0}")]
    Cas(#[from] CasError),
    /// Cached workflow output bytes are present but failed CAS integrity checks.
    ///
    /// This variant preserves structured recovery context so the coordinator can
    /// decide whether one pure-workflow retry is allowed.
    #[error(transparent)]
    CorruptWorkflowOutput(Box<CorruptWorkflowOutputContext>),
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
