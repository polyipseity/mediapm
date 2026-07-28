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

/// Writes bytes only when target content differs.
pub(super) fn write_bytes_if_changed(
    path: &Path,
    bytes: &[u8],
    operation: &str,
) -> Result<(), MediaPmError> {
    if let Ok(existing) = fs::read(path)
        && existing == bytes
    {
        return Ok(());
    }

    write_bytes(path, bytes, operation)
}

/// Returns current Unix timestamp in seconds.
#[allow(dead_code)]
pub(super) fn now_unix_seconds() -> u64 {
    SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default().as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::Duration;

    #[test]
    fn write_bytes_if_changed_writes_when_content_differs() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("test.txt");

        // First write: file doesn't exist yet, should create it.
        write_bytes_if_changed(&path, b"hello", "test").unwrap();
        assert!(path.exists(), "file should exist after first write");
        assert_eq!(fs::read(&path).unwrap(), b"hello", "content mismatch");

        // Second write with different content: should overwrite.
        write_bytes_if_changed(&path, b"world", "test").unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"world", "content should be updated");
    }

    #[test]
    fn write_bytes_if_changed_skips_write_when_content_identical() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("test.txt");

        // Write initial content and record mtime (with sub-second precision).
        fs::write(&path, b"hello").unwrap();
        let original_meta = fs::metadata(&path).unwrap();
        let original_mtime = original_meta.modified().unwrap();

        // Brief sleep to ensure mtime would differ if a write occurs.
        std::thread::sleep(Duration::from_millis(10));

        // Write identical content — should skip, mtime unchanged.
        write_bytes_if_changed(&path, b"hello", "test").unwrap();
        let after_meta = fs::metadata(&path).unwrap();
        let after_mtime = after_meta.modified().unwrap();

        assert_eq!(
            original_mtime, after_mtime,
            "mtime should not change when content is identical (write was skipped)",
        );
        assert!(path.exists(), "file should still exist");
        assert_eq!(fs::read(&path).unwrap(), b"hello", "content should be unchanged");
    }

    #[test]
    fn write_bytes_if_changed_creates_parent_directories() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("a/b/c/test.txt");

        // File is in a deeply nested path that doesn't exist yet.
        assert!(!path.parent().unwrap().exists(), "parent dir should not exist before write");
        write_bytes_if_changed(&path, b"nested", "test").unwrap();

        assert!(path.exists(), "file should exist after write to nested path");
        assert_eq!(fs::read(&path).unwrap(), b"nested", "content mismatch for nested path");
        assert!(path.parent().unwrap().exists(), "parent directory should have been created");
    }
}
