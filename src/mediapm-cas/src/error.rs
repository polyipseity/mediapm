//! CAS error types.

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
}

/// Display implementation for CasError.
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
            Self::Io(inner) => write!(f, "I/O error: {inner}"),
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
