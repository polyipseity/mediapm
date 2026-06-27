//! Content-map file import into CAS.
//!
//! This module handles importing tool payload artifacts (individual files or
//! directory zips) from content-map sources into the CAS store.

use std::collections::BTreeMap;
use std::path::Path;

use bytes::Bytes;
use mediapm_cas::{CasApi, Hash};
use zip::write::SimpleFileOptions;

use crate::error::MediaPmError;

/// Source of content-map entries for CAS import.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(super) enum ContentMapSource {
    /// A single file at the given path.
    FilePath(std::path::PathBuf),
    /// A directory to be zipped before importing.
    DirectoryZip(std::path::PathBuf),
}

/// Cache key for deduplicating content-map source imports.
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct ContentMapSourceCacheKey {
    /// Source path string.
    pub(super) source: String,
    /// Last modified timestamp (seconds since epoch).
    pub(super) mtime_secs: u64,
}

/// Imports tool content files from a content map into the CAS store.
///
/// Reads each file referenced by the content map, verifies its content hash
/// matches the expected value, and stores it in the CAS. Returns a mapping
/// of content-map keys to their verified CAS hashes.
pub(super) async fn import_tool_content_files_into_cas(
    cas: &impl CasApi,
    content_map: &BTreeMap<String, String>,
    content_dir: &Path,
) -> Result<BTreeMap<String, Hash>, MediaPmError> {
    let mut result = BTreeMap::new();

    for (key, expected_hash_str) in content_map {
        let file_path = content_dir.join(key);
        let bytes = tokio::fs::read(&file_path).await.map_err(|source| MediaPmError::Io {
            operation: format!("reading content file for key '{key}'"),
            path: file_path.clone(),
            source,
        })?;
        let actual_hash = cas
            .put(Bytes::from(bytes))
            .await
            .map_err(|e| MediaPmError::Workflow(format!("CAS put failed for key '{key}': {e}")))?;

        let expected_hash = expected_hash_str.parse::<Hash>().map_err(|_| {
            MediaPmError::Workflow(format!(
                "invalid hash string for key '{key}': '{expected_hash_str}'"
            ))
        })?;

        if actual_hash != expected_hash {
            return Err(MediaPmError::Workflow(format!(
                "content hash mismatch for key '{key}': expected {expected_hash}, got {actual_hash}"
            )));
        }

        result.insert(key.clone(), actual_hash);
    }

    Ok(result)
}

/// Imports one content-map source entry into the CAS store.
#[allow(dead_code)]
pub(super) async fn import_tool_content_source_into_cas(
    cas: &impl CasApi,
    source: &ContentMapSource,
) -> Result<Hash, MediaPmError> {
    match source {
        ContentMapSource::FilePath(path) => {
            let bytes = tokio::fs::read(path).await.map_err(|source| MediaPmError::Io {
                operation: "reading content source file".to_string(),
                path: path.clone(),
                source,
            })?;
            let hash = cas.put(Bytes::from(bytes)).await.map_err(|e| {
                MediaPmError::Workflow(format!("CAS put failed for content source: {e}"))
            })?;
            Ok(hash)
        }
        ContentMapSource::DirectoryZip(dir) => {
            let zip_bytes = pack_directory_to_uncompressed_zip_bytes(dir)?;
            let hash = cas.put(Bytes::from(zip_bytes)).await.map_err(|e| {
                MediaPmError::Workflow(format!("CAS put failed for directory zip: {e}"))
            })?;
            Ok(hash)
        }
    }
}

/// Packs one directory tree into uncompressed ZIP bytes.
#[allow(dead_code)]
fn pack_directory_to_uncompressed_zip_bytes(dir: &Path) -> Result<Vec<u8>, MediaPmError> {
    use zip::CompressionMethod;

    let mut buf = Vec::new();
    {
        let mut writer = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
        let options = SimpleFileOptions::default().compression_method(CompressionMethod::Stored);

        pack_directory_entries(&mut writer, dir, dir, &options)?;

        writer
            .finish()
            .map_err(|e| MediaPmError::Workflow(format!("failed to finalize zip archive: {e}")))?;
    }
    Ok(buf)
}

/// Recursively adds directory entries to the zip writer.
#[allow(dead_code)]
fn pack_directory_entries(
    writer: &mut zip::ZipWriter<std::io::Cursor<&mut Vec<u8>>>,
    root: &Path,
    dir: &Path,
    options: &SimpleFileOptions,
) -> Result<(), MediaPmError> {
    use std::io::{Read, Write};

    for entry in std::fs::read_dir(dir).map_err(|source| MediaPmError::Io {
        operation: format!("reading directory '{}'", dir.display()),
        path: dir.to_path_buf(),
        source,
    })? {
        let entry = entry.map_err(|source| MediaPmError::Io {
            operation: format!("reading directory entry in '{}'", dir.display()),
            path: dir.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        if path.is_dir() {
            pack_directory_entries(writer, root, &path, options)?;
        } else {
            let relative = path.strip_prefix(root).unwrap_or(&path).to_string_lossy().to_string();
            let mut file = std::fs::File::open(&path).map_err(|source| MediaPmError::Io {
                operation: format!("opening file '{}' for zip", path.display()),
                path: path.clone(),
                source,
            })?;
            writer.start_file(relative.clone(), *options).map_err(|e| {
                MediaPmError::Workflow(format!("failed to start zip entry '{relative}': {e}"))
            })?;
            let mut contents = Vec::new();
            file.read_to_end(&mut contents).map_err(|source| MediaPmError::Io {
                operation: format!("reading file '{}' for zip", path.display()),
                path: path.clone(),
                source,
            })?;
            writer.write_all(&contents).map_err(|e| {
                MediaPmError::Workflow(format!("failed to write zip entry '{relative}': {e}"))
            })?;
        }
    }
    Ok(())
}
