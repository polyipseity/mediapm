//! Shared utility helpers for conductor bridge modules.

use std::fs;
use std::path::Path;

use crate::error::MediaPmError;

/// Writes bytes to disk with parent-directory creation and IO context.
///
/// The bytes are published through a sibling temporary and a rename, so a
/// document this workspace later parses is either the previous one or the new
/// one. A save that cannot finish leaves the previous document at the path.
pub(super) fn write_bytes(path: &Path, bytes: &[u8], operation: &str) -> Result<(), MediaPmError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| MediaPmError::Io {
            operation: format!("creating parent directory for {}", path.display()),
            path: parent.to_path_buf(),
            source,
        })?;
    }

    mediapm_utils::fs::write_bytes_atomically(path, bytes).map_err(|source| MediaPmError::Io {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::Duration;

    #[test]
    fn write_bytes_if_changed_writes_when_content_differs() {
        let tmp = mediapm_utils::temp::artifact_dir().unwrap();
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
        let tmp = mediapm_utils::temp::artifact_dir().unwrap();
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
        let tmp = mediapm_utils::temp::artifact_dir().unwrap();
        let path = tmp.path().join("a/b/c/test.txt");

        // File is in a deeply nested path that doesn't exist yet.
        assert!(!path.parent().unwrap().exists(), "parent dir should not exist before write");
        write_bytes_if_changed(&path, b"nested", "test").unwrap();

        assert!(path.exists(), "file should exist after write to nested path");
        assert_eq!(fs::read(&path).unwrap(), b"nested", "content mismatch for nested path");
        assert!(path.parent().unwrap().exists(), "parent directory should have been created");
    }

    /// A publish that cannot finish leaves the previous document where the
    /// next run reads it, and that document's own reader still accepts it.
    ///
    /// The directory holding the document loses its write permission, so the
    /// temporary file cannot be created. A writer that opened the target
    /// directly would have truncated the previous document before discovering
    /// it could not write the new one, and a torn document is what the next
    /// run refuses to load.
    ///
    /// The document here is `mediapm.ncl` rather than the generated conductor
    /// document, because the conductor's document type is private to its own
    /// crate and this test lives in mediapm. The writer is the same one, so the
    /// property it states is the same property.
    #[cfg(unix)]
    #[test]
    fn a_failed_publish_leaves_a_document_the_decoder_accepts() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = mediapm_utils::temp::artifact_dir().unwrap();
        let path = tmp.path().join("mediapm.ncl");
        let previous = crate::config::MediaPmDocument::default();
        crate::config::nickel_io::save_mediapm_document(&path, &previous)
            .expect("publish the document the test starts from");
        let expected =
            mediapm_utils::nickel::render_document_as_nickel(&previous, "mediapm document")
                .expect("render the document the test starts from");

        let directory = path.parent().expect("the document's directory").to_path_buf();
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o500))
            .expect("make the document's directory unwritable");
        let failed = write_bytes(&path, b"a document that never finishes", "test");
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700))
            .expect("restore the document's directory");

        let error = failed.expect_err("a publish into an unwritable directory must be reported");
        assert_eq!(error.code(), "MPM-E007", "the failure is an IO error the caller can act on");
        assert_eq!(
            fs::read(&path).expect("read the document back"),
            expected,
            "the previous document must survive a publish that never finished",
        );
        crate::config::nickel_io::load_mediapm_document(&path)
            .expect("the document left behind must be one the loader accepts");
    }
}
