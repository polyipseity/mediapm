use std::collections::BTreeMap;
use std::fs;
use std::io::{Cursor, Write};
use std::path::{Path, PathBuf};

use mediapm_cas::{CasApi, FileSystemCas, Hash};

use crate::error::MediaPmError;
use crate::tools::downloader::ContentMapSource;

pub(super) async fn import_tool_content_files_into_cas(
    cas: &FileSystemCas,
    content_entries: &BTreeMap<String, ContentMapSource>,
) -> Result<BTreeMap<String, Hash>, MediaPmError> {
    let mut map = BTreeMap::new();
    let mut source_hash_cache = BTreeMap::<ContentMapSourceCacheKey, Hash>::new();

    for (relative_path, entry) in content_entries {
        let hash = import_tool_content_source_into_cas(
            cas,
            relative_path.as_str(),
            entry,
            &mut source_hash_cache,
        )
        .await?;
        map.insert(relative_path.clone(), hash);
    }

    Ok(map)
}

/// Cache key for one materialized content-map source import.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum ContentMapSourceCacheKey {
    /// Raw file payload imported directly from one absolute path.
    FilePath(PathBuf),
    /// Directory payload imported as one deterministic uncompressed ZIP blob.
    DirectoryZip(PathBuf),
}

/// Returns one stable source-cache key for content-map deduplication.
fn content_map_source_cache_key(source: &ContentMapSource) -> ContentMapSourceCacheKey {
    match source {
        ContentMapSource::FilePath(path) => ContentMapSourceCacheKey::FilePath(path.clone()),
        ContentMapSource::DirectoryZip { root_dir } => {
            ContentMapSourceCacheKey::DirectoryZip(root_dir.clone())
        }
    }
}

/// Imports one content-map source into CAS with per-pass source-hash caching.
///
/// Blocking file I/O is offloaded to `spawn_blocking` so the async executor
/// remains available for progress rendering and other tasks while large tool
/// payloads (e.g. ffmpeg directory ZIPs) are read and serialized.
pub(super) async fn import_tool_content_source_into_cas(
    cas: &FileSystemCas,
    relative_path: &str,
    source: &ContentMapSource,
    source_hash_cache: &mut BTreeMap<ContentMapSourceCacheKey, Hash>,
) -> Result<Hash, MediaPmError> {
    let cache_key = content_map_source_cache_key(source);
    if let Some(hash) = source_hash_cache.get(&cache_key) {
        return Ok(*hash);
    }

    let bytes = match source {
        ContentMapSource::FilePath(absolute_path) => {
            let path = absolute_path.clone();
            tokio::task::spawn_blocking(move || {
                fs::read(&path).map_err(|source| MediaPmError::Io {
                    operation: format!(
                        "reading tool payload file '{}' before CAS import",
                        path.display()
                    ),
                    path: path.clone(),
                    source,
                })
            })
            .await
            .map_err(|e| {
                MediaPmError::Workflow(format!("tool payload file read task panicked: {e}"))
            })??
        }
        ContentMapSource::DirectoryZip { root_dir } => {
            let dir = root_dir.clone();
            tokio::task::spawn_blocking(move || build_uncompressed_zip_bytes_from_directory(&dir))
                .await
                .map_err(|e| {
                    MediaPmError::Workflow(format!("tool payload directory ZIP task panicked: {e}"))
                })??
        }
    };

    let hash = cas.put(bytes).await.map_err(|source| {
        MediaPmError::Workflow(format!(
            "importing tool payload entry '{relative_path}' into CAS failed: {source}",
        ))
    })?;
    source_hash_cache.insert(cache_key, hash);

    Ok(hash)
}

/// Serializes one directory tree as an uncompressed ZIP payload.
///
/// This encoding keeps conductor `content_map` compact for archive-style tools:
/// one folder key can carry a complete tool payload without one hash per file.
fn build_uncompressed_zip_bytes_from_directory(root_dir: &Path) -> Result<Vec<u8>, MediaPmError> {
    if !root_dir.exists() || !root_dir.is_dir() {
        return Err(MediaPmError::Workflow(format!(
            "cannot build ZIP payload: '{}' is not a directory",
            root_dir.display()
        )));
    }

    let mut files = Vec::<PathBuf>::new();
    let mut stack = vec![root_dir.to_path_buf()];
    while let Some(next) = stack.pop() {
        let entries = fs::read_dir(&next).map_err(|source| MediaPmError::Io {
            operation: "enumerating tool payload directory for ZIP serialization".to_string(),
            path: next.clone(),
            source,
        })?;

        for entry in entries {
            let entry = entry.map_err(|source| MediaPmError::Io {
                operation: "reading tool payload directory entry for ZIP serialization".to_string(),
                path: next.clone(),
                source,
            })?;
            let path = entry.path();
            let ty = entry.file_type().map_err(|source| MediaPmError::Io {
                operation: "reading tool payload entry type for ZIP serialization".to_string(),
                path: path.clone(),
                source,
            })?;

            if ty.is_dir() {
                stack.push(path);
            } else if ty.is_file() {
                files.push(path);
            }
        }
    }

    files.sort();

    let mut buffer = Cursor::new(Vec::new());
    let mut zip = zip::ZipWriter::new(&mut buffer);
    let options = zip::write::SimpleFileOptions::default()
        .compression_method(zip::CompressionMethod::Stored)
        .unix_permissions(0o644);

    for path in files {
        let relative = path.strip_prefix(root_dir).map_err(|_| {
            MediaPmError::Workflow(format!(
                "failed deriving ZIP entry path from '{}' under '{}'",
                path.display(),
                root_dir.display()
            ))
        })?;
        let entry_name = relative.to_string_lossy().replace('\\', "/");

        zip.start_file(entry_name, options).map_err(|source| {
            MediaPmError::Workflow(format!(
                "creating ZIP entry for '{}' failed: {source}",
                path.display()
            ))
        })?;

        let bytes = fs::read(&path).map_err(|source| MediaPmError::Io {
            operation: "reading tool payload file for ZIP serialization".to_string(),
            path: path.clone(),
            source,
        })?;
        zip.write_all(&bytes).map_err(|source| {
            MediaPmError::Workflow(format!(
                "writing ZIP entry bytes for '{}' failed: {source}",
                path.display()
            ))
        })?;
    }

    zip.finish().map_err(|source| {
        MediaPmError::Workflow(format!(
            "finalizing uncompressed ZIP payload for '{}' failed: {source}",
            root_dir.display()
        ))
    })?;

    Ok(buffer.into_inner())
}
