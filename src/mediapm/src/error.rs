//! `mediapm` error taxonomy.
//!
//! This module centralizes crate-level error variants so all submodules share
//! one consistent error contract while preserving operation/path context.
//!
//! # MPM error code catalog
//!
//! Every error and warning emitted by the `mediapm` crate carries a crate-prefixed
//! code in its display message, following the format `<severity>[<CODE>]: <message>`.
//! Codes use `MPM-` prefix for errors native to this crate and `CND-` for errors
//! forwarded from the conductor. The code suffix distinguishes errors (`E`) from
//! warnings (`W`).
//!
//! | Code | Title | Description | Suggested fix |
//! |------|-------|-------------|---------------|
//! | MPM-E001 | Unknown dependency key | A dependency key does not match any known dep type or configured tool | Use a bare tool ID as the key; check valid deps in the error message |
//! | MPM-E002 | Inherit with unconfigured tool | A dependency uses "inherit" but the target tool is not in tools section | Add the tool to the `tools` section, or use `"latest"` or an explicit version spec |
//! | MPM-E003 | Circular inherit | A dependency and its target both use "inherit" | Set an explicit version for the target |
//! | MPM-E004 | Config parse failure | A config value failed to deserialize | Check the expected shape and available fields |
//! | MPM-W001 | Silenced serde error | A ToolRequirement deserialization silently dropped during transitive deps | Add structured warning to emitted diagnostics |

use std::path::PathBuf;

use mediapm_conductor::ConductorError;
use thiserror::Error;

/// Error category for mediapm orchestration and runtime coordination.
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
    /// Error propagated from the conductor.
    #[error("conductor error: {0}")]
    Conductor(#[from] ConductorError),
    /// Conductor document I/O failure with operation context.
    #[error("conductor document error while {operation} at '{path}': {detail}")]
    ConductorDocument {
        /// Human-readable operation label.
        operation: String,
        /// Filesystem target path.
        path: PathBuf,
        /// Underlying error description.
        detail: String,
    },
    /// Config validation failure with error code, context, detail, and suggestion.
    #[error("error[{code}] {context}: {detail}\n  suggestion: {suggestion}")]
    ConfigValidation {
        /// Error code string (e.g. "MPM-E001").
        code: &'static str,
        /// What was being validated.
        context: String,
        /// What went wrong.
        detail: String,
        /// Actionable fix suggestion.
        suggestion: String,
    },
}

impl MediaPmError {
    /// Returns the error code string for this error.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::ConfigValidation { code, .. } => code,
            Self::InvalidSource(_) => "MPM-E005",
            Self::Workflow(_) => "MPM-E006",
            Self::Serialization(_) => "MPM-E004",
            Self::Io { .. } => "MPM-E007",
            Self::ConductorDocument { .. } => "MPM-E008",
            Self::Conductor(_) => "CND-",
        }
    }
}
