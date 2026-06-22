//! Document I/O helpers for loading and saving conductor documents.
//!
//! Each document is a `.ncl` file that wraps a versioned Nickel envelope.
//! Loading evaluates the file through `nickel-lang-core`, saving renders
//! the document back through the latest-schema envelope.

use std::path::Path;

use crate::config::documents::NickelDocument;
use crate::error::ConductorError;

/// Loads a `NickelDocument` from a `.ncl` file path.
///
/// Reads the file, evaluates it through the versioned Nickel migration
/// pipeline, and returns the decoded document.
///
/// # Errors
///
/// Returns [`ConductorError::Io`] when the file cannot be read, or wraps
/// any Nickel evaluation or version‑migration error.
pub(crate) fn load_document(path: &Path) -> Result<NickelDocument, ConductorError> {
    let source = std::fs::read_to_string(path).map_err(|source| ConductorError::Io {
        operation: "reading config document".to_string(),
        path: path.to_path_buf(),
        source,
    })?;
    let bytes = source.as_bytes();
    crate::config::versions::decode_document(bytes)
}

/// Saves a `NickelDocument` to a `.ncl` file.
///
/// Encodes the document through the latest‑schema envelope and writes the
/// resulting Nickel source to the given path.
///
/// # Errors
///
/// Returns [`ConductorError::Io`] when the file cannot be written, or wraps
/// any encoding error.
pub(crate) fn save_document(path: &Path, document: &NickelDocument) -> Result<(), ConductorError> {
    let bytes = crate::config::versions::encode_document(document.clone())?;
    std::fs::write(path, &bytes).map_err(|source| ConductorError::Io {
        operation: "writing config document".to_string(),
        path: path.to_path_buf(),
        source,
    })
}
