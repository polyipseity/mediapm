//! Errors shared by the unified auth file and auth services.

use std::path::PathBuf;

use thiserror::Error;

/// Failures while reading, writing, or acting on unified credentials.
#[derive(Debug, Error)]
pub enum AuthError {
    /// The file declares a schema version this build cannot decode.
    #[error("unsupported auth file version {0}; expected 1")]
    UnknownVersion(u32),
    /// The auth file could not be read.
    #[error("failed to read auth file {path}: {source}")]
    Read {
        /// File that could not be read.
        path: PathBuf,
        /// Underlying I/O failure.
        #[source]
        source: std::io::Error,
    },
    /// The auth file or its temporary sibling could not be written.
    #[error("failed to write auth file {path}: {source}")]
    Write {
        /// File that could not be written.
        path: PathBuf,
        /// Underlying I/O failure.
        #[source]
        source: std::io::Error,
    },
    /// The auth file's parent directory could not be created.
    #[error("failed to create auth directory {path}: {source}")]
    CreateDir {
        /// Directory that could not be created.
        path: PathBuf,
        /// Underlying I/O failure.
        #[source]
        source: std::io::Error,
    },
    /// The file does not match the strict shape of its declared version.
    #[error("invalid auth file: {detail}")]
    Decode {
        /// Serde diagnostic, prefixed with the file path when known.
        detail: String,
    },
    /// The auth file could not be serialized.
    #[error("failed to encode auth file: {detail}")]
    Encode {
        /// Serde diagnostic.
        detail: String,
    },
    /// A service-specific operation failed.
    #[error("{0}")]
    Service(String),
    /// A prompted secret was empty after trimming.
    #[error("secret cannot be empty")]
    EmptySecret,
}
