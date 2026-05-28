//! Runtime-local cache helpers for directory-form tool-content-map entries.
//!
//! The step worker uses this cache to avoid repeatedly unpacking the same ZIP
//! payloads when a workflow step reuses the same managed tool content. The
//! cache lives under the conductor runtime root's `tools/` directory so
//! `mediapm` inherits the same layout automatically when it invokes conductor
//! with its own runtime root.

use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use crate::error::ConductorError;

const TOOL_CONTENT_CACHE_ENTRY_TTL_SECONDS: u64 = 24 * 60 * 60;
const TOOL_CONTENT_CACHE_METADATA_FILE_NAME: &str = "metadata.json";
const TOOL_CONTENT_CACHE_PAYLOAD_DIR_NAME: &str = "payload";
const TOOL_CONTENT_CACHE_VERSION: u32 = 1;
const TOOL_CONTENT_CACHE_ROOT_DIR_NAME: &str = "tools";
const TOOL_CONTENT_CACHE_ROOT_SENTINEL: &str = "__root__";

/// Returns the runtime-local tool cache root for one conductor invocation.
#[must_use]
pub(super) fn runtime_tool_cache_root(runtime_tmp_dir: &Path) -> PathBuf {
    runtime_tmp_dir
        .parent()
        .map_or_else(|| runtime_tmp_dir.to_path_buf(), Path::to_path_buf)
        .join(TOOL_CONTENT_CACHE_ROOT_DIR_NAME)
}

/// Removes stale runtime-local tool cache entries.
///
/// Cache entries that have not been used for at least 24 hours are removed
/// best-effort. Missing or malformed entries are ignored so cache cleanup never
/// blocks workflow execution.
pub(super) fn prune_expired_tool_content_cache_entries(
    cache_root: &Path,
) -> Result<(), ConductorError> {
    if !cache_root.exists() {
        return Ok(());
    }

    let now = now_unix_seconds();
    let cutoff = now.saturating_sub(TOOL_CONTENT_CACHE_ENTRY_TTL_SECONDS);

    let entries = fs::read_dir(cache_root).map_err(|source| ConductorError::Io {
        operation: "enumerating tool-content cache root".to_string(),
        path: cache_root.to_path_buf(),
        source,
    })?;

    for entry in entries {
        let entry = entry.map_err(|source| ConductorError::Io {
            operation: "reading tool-content cache entry".to_string(),
            path: cache_root.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        if !entry.file_type().is_ok_and(|ty| ty.is_dir()) {
            continue;
        }

        let metadata_path = path.join(TOOL_CONTENT_CACHE_METADATA_FILE_NAME);
        if !metadata_path.exists() {
            // Orphaned directory — left over from a previous nested cache key
            // scheme or a partially-created entry. Remove it best-effort.
            let _ = fs::remove_dir_all(&path);
            continue;
        }
        let Ok(raw) = fs::read_to_string(&metadata_path) else {
            continue;
        };
        let Ok(metadata) = serde_json::from_str::<ToolContentCacheMetadata>(&raw) else {
            continue;
        };
        if metadata.version != TOOL_CONTENT_CACHE_VERSION {
            continue;
        }
        if metadata.last_used_unix_seconds > cutoff {
            continue;
        }

        let _ = fs::remove_dir_all(&path);
    }

    Ok(())
}

/// Prepares one cached directory payload and returns the cache entry payload
/// directory.
///
/// A cache hit refreshes the last-used timestamp and returns the existing
/// extracted payload directory. A cache miss unpacks the ZIP payload into a new
/// cache entry and then returns the extracted payload directory.
pub(super) fn prepare_cached_tool_content_directory(
    cache_root: &Path,
    raw_relative_path: &str,
    relative_dir: &Path,
    zip_content: &[u8],
) -> Result<PathBuf, ConductorError> {
    fs::create_dir_all(cache_root).map_err(|source| ConductorError::Io {
        operation: "creating tool-content cache root".to_string(),
        path: cache_root.to_path_buf(),
        source,
    })?;

    let payload_hash = blake3::hash(zip_content).to_hex().to_string();
    let cache_entry_dir =
        cache_root.join(cache_key_path(raw_relative_path, relative_dir, &payload_hash)?);
    let payload_dir = cache_entry_dir.join(TOOL_CONTENT_CACHE_PAYLOAD_DIR_NAME);
    let metadata_path = cache_entry_dir.join(TOOL_CONTENT_CACHE_METADATA_FILE_NAME);
    let now = now_unix_seconds();

    if let Ok(raw) = fs::read_to_string(&metadata_path)
        && let Ok(metadata) = serde_json::from_str::<ToolContentCacheMetadata>(&raw)
        && metadata.version == TOOL_CONTENT_CACHE_VERSION
        && metadata.payload_hash == payload_hash
        && payload_dir.is_dir()
    {
        persist_cache_metadata(
            &metadata_path,
            &ToolContentCacheMetadata {
                version: TOOL_CONTENT_CACHE_VERSION,
                payload_hash,
                last_used_unix_seconds: now,
            },
        )?;
        return Ok(payload_dir);
    }

    if cache_entry_dir.exists() {
        let _ = fs::remove_dir_all(&cache_entry_dir);
    }
    fs::create_dir_all(&payload_dir).map_err(|source| ConductorError::Io {
        operation: "creating tool-content cache payload directory".to_string(),
        path: payload_dir.clone(),
        source,
    })?;

    mediapm_conductor_builtin_archive::unpack_zip_bytes_to_directory(zip_content, &payload_dir)
        .map_err(|err| {
            ConductorError::Workflow(format!(
                "tool content map directory key '{raw_relative_path}' expects ZIP payload, but unpack failed: {err}"
            ))
        })?;

    persist_cache_metadata(
        &metadata_path,
        &ToolContentCacheMetadata {
            version: TOOL_CONTENT_CACHE_VERSION,
            payload_hash,
            last_used_unix_seconds: now,
        },
    )?;

    Ok(payload_dir)
}

/// Copies one cached payload directory into the execution sandbox.
pub(super) fn copy_cached_tool_content_directory(
    cached_dir: &Path,
    target_dir: &Path,
) -> Result<(), String> {
    if !cached_dir.exists() {
        return Err(format!(
            "cached tool-content directory '{}' does not exist",
            cached_dir.display()
        ));
    }
    copy_directory_recursive(cached_dir, target_dir)
}

/// Collects sandbox-relative file paths from one directory tree.
pub(super) fn collect_relative_files_recursive(
    root_dir: &Path,
    scan_dir: &Path,
) -> Result<BTreeSet<PathBuf>, ConductorError> {
    let mut out = BTreeSet::new();
    collect_relative_files_recursive_into(root_dir, scan_dir, &mut out)?;
    Ok(out)
}

fn collect_relative_files_recursive_into(
    root_dir: &Path,
    scan_dir: &Path,
    out: &mut BTreeSet<PathBuf>,
) -> Result<(), ConductorError> {
    if !scan_dir.exists() {
        return Ok(());
    }

    let entries = fs::read_dir(scan_dir).map_err(|source| ConductorError::Io {
        operation: "reading tool-content cache directory".to_string(),
        path: scan_dir.to_path_buf(),
        source,
    })?;

    for entry in entries {
        let entry = entry.map_err(|source| ConductorError::Io {
            operation: "iterating tool-content cache directory".to_string(),
            path: scan_dir.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        let file_type = entry.file_type().map_err(|source| ConductorError::Io {
            operation: "reading tool-content cache entry type".to_string(),
            path: path.clone(),
            source,
        })?;

        if file_type.is_dir() {
            collect_relative_files_recursive_into(root_dir, &path, out)?;
            continue;
        }

        if file_type.is_file() {
            let relative = path.strip_prefix(root_dir).map_err(|err| {
                ConductorError::Internal(format!(
                    "failed deriving relative path for cached tool-content file '{}': {err}",
                    path.display()
                ))
            })?;
            out.insert(relative.to_path_buf());
        }
    }

    Ok(())
}

fn copy_directory_recursive(source_dir: &Path, target_dir: &Path) -> Result<(), String> {
    fs::create_dir_all(target_dir)
        .map_err(|err| format!("creating destination '{}' failed: {err}", target_dir.display()))?;

    let entries = fs::read_dir(source_dir).map_err(|err| {
        format!("reading source directory '{}' failed: {err}", source_dir.display())
    })?;

    for entry in entries {
        let entry = entry.map_err(|err| format!("reading source directory entry failed: {err}"))?;
        let path = entry.path();
        let target_path = target_dir.join(entry.file_name());
        let file_type = entry
            .file_type()
            .map_err(|err| format!("reading file type for '{}' failed: {err}", path.display()))?;

        if file_type.is_dir() {
            copy_directory_recursive(&path, &target_path)?;
            continue;
        }

        if let Some(parent) = target_path.parent() {
            fs::create_dir_all(parent).map_err(|err| {
                format!("creating parent directory '{}' failed: {err}", parent.display())
            })?;
        }
        fs::copy(&path, &target_path).map_err(|err| {
            format!("copying '{}' to '{}' failed: {err}", path.display(), target_path.display())
        })?;

        let source_permissions = fs::metadata(&path)
            .map_err(|err| format!("reading permissions for '{}' failed: {err}", path.display()))?
            .permissions();
        fs::set_permissions(&target_path, source_permissions).map_err(|err| {
            format!("setting permissions on '{}' failed: {err}", target_path.display())
        })?;
    }

    Ok(())
}

/// Derives a flat, hash-disambiguated cache directory name for one content-map
/// directory entry.
///
/// The key is `<path_part>@<hash_prefix>` where `<path_part>` joins the
/// normalized path components with `+` (never `/`) so all cache entries sit
/// directly under the cache root regardless of nesting depth. `<hash_prefix>`
/// is the first 16 hex characters of the payload hash, ensuring that two tools
/// whose `content_map` keys share the same relative path but carry different
/// payloads never share a cache directory.
fn cache_key_path(
    raw_relative_path: &str,
    relative_dir: &Path,
    payload_hash: &str,
) -> Result<PathBuf, ConductorError> {
    let path_part = if relative_dir.as_os_str().is_empty() {
        TOOL_CONTENT_CACHE_ROOT_SENTINEL.to_string()
    } else {
        let mut components = Vec::new();
        for component in relative_dir.components() {
            let raw = component.as_os_str().to_string_lossy();
            let sanitized = sanitize_cache_key_component(&raw);
            if sanitized.is_empty() {
                return Err(ConductorError::Workflow(format!(
                    "tool content map directory key '{raw_relative_path}' normalizes to an empty cache key"
                )));
            }
            components.push(sanitized);
        }
        components.join("+")
    };
    let hash_prefix = payload_hash.get(..16).unwrap_or(payload_hash);
    Ok(PathBuf::from(format!("{path_part}@{hash_prefix}")))
}

fn sanitize_cache_key_component(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() { TOOL_CONTENT_CACHE_ROOT_SENTINEL.to_string() } else { out }
}

fn persist_cache_metadata(
    metadata_path: &Path,
    metadata: &ToolContentCacheMetadata,
) -> Result<(), ConductorError> {
    if let Some(parent) = metadata_path.parent() {
        fs::create_dir_all(parent).map_err(|source| ConductorError::Io {
            operation: "creating tool-content cache metadata parent directory".to_string(),
            path: parent.to_path_buf(),
            source,
        })?;
    }

    let rendered = serde_json::to_string_pretty(metadata).map_err(|error| {
        ConductorError::Serialization(format!("encoding tool-content cache metadata: {error}"))
    })?;
    let temp_path = metadata_path.with_extension("json.tmp");
    fs::write(&temp_path, format!("{rendered}\n")).map_err(|source| ConductorError::Io {
        operation: "writing temporary tool-content cache metadata".to_string(),
        path: temp_path.clone(),
        source,
    })?;
    if metadata_path.exists() {
        let _ = fs::remove_file(metadata_path);
    }
    fs::rename(&temp_path, metadata_path).map_err(|source| ConductorError::Io {
        operation: "replacing tool-content cache metadata".to_string(),
        path: metadata_path.to_path_buf(),
        source,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ToolContentCacheMetadata {
    version: u32,
    payload_hash: String,
    last_used_unix_seconds: u64,
}

#[must_use]
fn now_unix_seconds() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_tool_cache_root_follows_runtime_tmp_parent() {
        let runtime_tmp_dir = Path::new("/tmp/example/.conductor/tmp");
        let cache_root = runtime_tool_cache_root(runtime_tmp_dir);

        assert_eq!(cache_root, PathBuf::from("/tmp/example/.conductor/tools"));
    }

    #[test]
    fn cache_key_path_flattens_components_and_appends_hash_prefix() {
        // Simple single-component path.
        let key = cache_key_path("macos/", Path::new("macos"), "83e87ee4a2609f5babc123").unwrap();
        assert_eq!(key, PathBuf::from("macos@83e87ee4a2609f5b"));

        // Multi-component path must be flattened with '+' so the cache entry
        // sits directly under cache_root rather than in a nested subdirectory.
        let key =
            cache_key_path("ffmpeg/macos/", Path::new("ffmpeg/macos"), "83e87ee4a2609f5babc123")
                .unwrap();
        assert_eq!(key, PathBuf::from("ffmpeg+macos@83e87ee4a2609f5b"));

        // Empty relative_dir uses the sentinel.
        let key = cache_key_path("", Path::new(""), "deadbeef12345678xyz").unwrap();
        assert_eq!(key, PathBuf::from("__root__@deadbeef12345678"));
    }

    #[test]
    fn cache_key_path_different_hashes_produce_different_dirs() {
        // Two tools may use the same content_map key path (e.g. both have a
        // `macos/` entry) but carry different payloads. Their cache entries
        // must be in different directories.
        let key_a = cache_key_path("macos/", Path::new("macos"), "aaaa1111bbbb2222ccc").unwrap();
        let key_b = cache_key_path("macos/", Path::new("macos"), "bbbb2222cccc3333ddd").unwrap();
        assert_ne!(key_a, key_b);
    }

    #[test]
    fn prepare_cached_tool_content_directory_reuses_existing_entry() {
        let root = tempfile::tempdir().expect("tempdir");
        let cache_root = root.path().join("tools");
        let zip_bytes = build_zip_bytes();

        let first = prepare_cached_tool_content_directory(
            &cache_root,
            "tool-content/assets/",
            Path::new("tool-content/assets"),
            &zip_bytes,
        )
        .expect("first extraction");
        let second = prepare_cached_tool_content_directory(
            &cache_root,
            "tool-content/assets/",
            Path::new("tool-content/assets"),
            &zip_bytes,
        )
        .expect("cache hit");

        assert_eq!(first, second);
        assert!(first.join("nested/file.txt").exists());
        assert!(first.join("nested").join("inner.txt").exists());
    }

    fn build_zip_bytes() -> Vec<u8> {
        let mut buffer = Vec::new();
        {
            let mut writer = zip::ZipWriter::new(std::io::Cursor::new(&mut buffer));
            let options = zip::write::SimpleFileOptions::default()
                .compression_method(zip::CompressionMethod::Stored);
            writer.add_directory("nested/", options).expect("add directory");
            writer.start_file("nested/file.txt", options).expect("start file");
            std::io::Write::write_all(&mut writer, b"alpha").expect("write file");
            writer.start_file("nested/inner.txt", options).expect("start inner file");
            std::io::Write::write_all(&mut writer, b"beta").expect("write inner file");
            writer.finish().expect("finish zip");
        }
        buffer
    }
}
