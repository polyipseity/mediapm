//! # CAS error code catalog
//!
//! | Code | Title | Description | Corresponding variant |
//! |------|-------|-------------|-----------------------|
//! | CAS-E001 | Object not found | The requested CAS object is missing from the store | `NotFound` |
//! | CAS-E002 | Invalid argument | An argument violates CAS invariants | `InvalidArgument` |
//! | CAS-E003 | Internal error | Unexpected internal state in the CAS layer | `Internal` |
//! | CAS-E004 | I/O error | Filesystem operation failed | `Io` |
//! | CAS-E005 | Corrupt object | Data integrity check failed (hash mismatch) | `CorruptObject` |
//! | CAS-E006 | Object too large | Object exceeds the operation size limits | `TooLarge` |
//! | CAS-E007 | Lock contention | Another process holds the CAS directory lock | `LockContention` |
//!
//! See `.agents/instructions/error-codes.instructions.md` for the
//! workspace-wide error code reference.

use std::path::PathBuf;

use thiserror::Error;

use crate::hash::Hash;

/// Errors returned by CAS operations.
#[derive(Error, Debug)]
pub enum CasError {
    /// The requested object was not found.
    NotFound(Hash),

    /// Invalid argument (e.g. self-referencing constraint).
    InvalidArgument(String),

    /// Internal error.
    Internal(String),

    /// I/O error.
    Io(#[from] std::io::Error),

    /// Data corruption detected (e.g. invalid delta envelope, hash mismatch).
    CorruptObject {
        /// The hash of the corrupted object, if known.
        hash: Option<Hash>,
        /// Human-readable corruption detail.
        details: String,
    },

    /// Object too large for the requested operation.
    ///
    /// Returned when a delta chain exceeds `MAX_DELTA_CHAIN_DEPTH`.
    TooLarge {
        /// The hash of the object.
        hash: Hash,
        /// The actual size of the object.
        size: u64,
        /// The maximum size allowed for this operation.
        limit: u64,
    },

    /// Another process or thread already holds an exclusive lock on this
    /// CAS directory.
    LockContention {
        /// Path to the lock file where contention was detected.
        path: PathBuf,
    },
}

/// Display implementation for [`CasError`].
impl std::fmt::Display for CasError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CorruptObject { hash, details } => match hash {
                Some(h) => write!(f, "corrupt object {h}: {details}"),
                None => write!(f, "corrupt object: {details}"),
            },
            Self::NotFound(h) => write!(f, "object not found: {h}"),
            Self::InvalidArgument(msg) => write!(f, "invalid argument: {msg}"),
            Self::Internal(msg) => write!(f, "internal error: {msg}"),
            Self::TooLarge { hash, size, limit } => {
                write!(f, "object {hash} too large ({size} bytes, limit {limit})")
            }
            Self::LockContention { path } => {
                write!(f, "CAS directory '{}' is locked by another process", path.display())
            }
            Self::Io(inner) => write!(f, "I/O error: {inner}"),
        }
    }
}

// Manual Clone: `std::io::Error` does not implement Clone, so we
// reconstruct it from its kind + display string.
impl Clone for CasError {
    fn clone(&self) -> Self {
        match self {
            Self::NotFound(h) => Self::NotFound(*h),
            Self::InvalidArgument(s) => Self::InvalidArgument(s.clone()),
            Self::Internal(s) => Self::Internal(s.clone()),
            Self::TooLarge { hash, size, limit } => {
                Self::TooLarge { hash: *hash, size: *size, limit: *limit }
            }
            Self::LockContention { path } => Self::LockContention { path: path.clone() },
            Self::Io(e) => Self::Io(std::io::Error::new(e.kind(), format!("{e}"))),
            Self::CorruptObject { hash, details } => {
                Self::CorruptObject { hash: *hash, details: details.clone() }
            }
        }
    }
}

impl CasError {
    /// Convenience constructor for internal errors.
    pub fn internal(msg: impl Into<String>) -> Self {
        CasError::Internal(msg.into())
    }

    /// Convenience constructor for corruption errors without a known hash.
    pub fn corrupt_object(detail: impl Into<String>) -> Self {
        CasError::CorruptObject { hash: None, details: detail.into() }
    }

    /// Convenience constructor for reconstruction corruption errors.
    pub fn corrupt_reconstruction(
        target: Hash,
        current: Hash,
        base_hash: Hash,
        detail: impl Into<String>,
    ) -> Self {
        CasError::CorruptObject {
            hash: Some(target),
            details: format!(
                "failed to reconstruct from base {base_hash} at step {current}: {}",
                detail.into()
            ),
        }
    }

    /// Convenience constructor for codec-layer errors.
    pub fn codec(source: impl std::fmt::Display) -> Self {
        CasError::corrupt_object(format!("codec error: {source}"))
    }
}
