//! Shared utility helpers for conductor bridge modules.

use std::fs;
use std::path::Path;
use std::time::SystemTime;

use crate::error::MediaPmError;

/// Writes bytes to disk with parent-directory creation and IO context.
pub(super) fn write_bytes(path: &Path, bytes: &[u8], operation: &str) -> Result<(), MediaPmError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| MediaPmError::Io {
            operation: format!("creating parent directory for {}", path.display()),
            path: parent.to_path_buf(),
            source,
        })?;
    }

    fs::write(path, bytes).map_err(|source| MediaPmError::Io {
        operation: operation.to_string(),
        path: path.to_path_buf(),
        source,
    })
}

/// Returns current Unix timestamp in seconds.
pub(super) fn now_unix_seconds() -> u64 {
    SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default().as_secs()
}
