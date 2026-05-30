//! Persistent tool-content cache for the conductor step worker.
//!
//! Each conductor tool that declares a `tool_content_map` gets one cache entry
//! under `<tools_dir>/<sanitized_tool_id>/`.  An entry contains:
//!
//! - `metadata.json` — version marker, the full `content_map` from the
//!   conductor config (the cache-validity key), and `last_used_unix_seconds`
//!   for TTL-based expiry.
//! - `payload/` — the fully-extracted, ready-to-execute tool content tree.
//!   Archive (`./` or `dir/`) keys are unpacked here; raw-file keys are
//!   written here verbatim.  The payload tree mirrors the execution sandbox
//!   layout, so sandbox setup is just a hard-link pass over `payload/`.
//!
//! # Cache lifecycle
//!
//! - **Hit** — `metadata.json` is present, its `content_map` equals the
//!   current tool config entry-for-entry, and `payload/` exists as a
//!   directory.  The entry's `last_used_unix_seconds` is refreshed before
//!   returning the payload path.
//! - **Miss** — CAS bytes for every entry are fetched concurrently; any
//!   previous entry directory is removed; all content is extracted into a
//!   fresh `payload/` tree; `metadata.json` is written atomically.
//! - **Expiry** — entries not used within 24 hours are pruned by
//!   [`prune_expired_tool_content_cache_entries`] on a best-effort basis that
//!   never blocks workflow execution.
//!
//! # Multi-step sharing
//!
//! The cache is keyed by tool identity, not by step or sandbox.  All steps in
//! one run that reference the same conductor tool share a single `payload/`
//! tree.  Hard-linking from `payload/` into each sandbox means sandbox setup
//! is a metadata-only operation regardless of payload size.
//!
//! # Module ownership
//!
//! This module is intentionally `pub(super)` — only the step worker and its
//! direct collaborators may call it.  `mediapm` accesses the cache indirectly
//! by supplying its own `tools_dir` to conductor via
//! [`RuntimeStoragePaths`][crate::api::RuntimeStoragePaths].

use std::collections::BTreeMap;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use futures_util::future::try_join_all;
use mediapm_cas::{CasApi, Hash};
use serde::{Deserialize, Serialize};
use tokio::task;

use crate::error::ConductorError;

/// Seconds since last use after which a cache entry is eligible for pruning.
const TOOL_CONTENT_CACHE_ENTRY_TTL_SECONDS: u64 = 24 * 60 * 60;
/// Minimum interval between best-effort prune scans.
///
/// Prune scans traverse the full `tools/` tree; running them on every step can
/// add avoidable latency once the cache has many entries.
const TOOL_CONTENT_CACHE_PRUNE_COOLDOWN_SECONDS: u64 = 5 * 60;
/// Marker file used to remember when prune last ran.
const TOOL_CONTENT_CACHE_PRUNE_MARKER_FILE_NAME: &str = ".prune-last-used-unix-seconds";
/// File name of the per-entry JSON metadata document.
const TOOL_CONTENT_CACHE_METADATA_FILE_NAME: &str = "metadata.json";
/// Subdirectory name under the entry root that holds extracted tool content.
const TOOL_CONTENT_CACHE_PAYLOAD_DIR_NAME: &str = "payload";
/// Schema version marker for `metadata.json`.
const TOOL_CONTENT_CACHE_VERSION: u32 = 1;

/// Persistent metadata stored alongside every cache entry.
///
/// `content_map` is the canonical cache-validity key: two entries are
/// considered equivalent when their `content_map` values compare equal
/// key-for-key and hash-for-hash.  Any change in the tool config — new key,
/// updated hash, removed key — produces a cache miss and triggers full
/// re-extraction.
///
/// `last_used_unix_seconds` is updated on every cache hit to drive TTL expiry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ToolContentCacheMetadata {
    version: u32,
    /// Full `tool_content_map` as it appeared in the conductor config at the
    /// time the cache entry was last extracted or validated.
    ///
    /// [`Hash`] serializes as `"blake3:<hex>"` via its [`Serialize`] impl.
    content_map: BTreeMap<String, Hash>,
    /// Unix timestamp (seconds) of the most recent cache access.
    last_used_unix_seconds: u64,
    /// Set to `true` after the payload tree has been fully extracted and its
    /// execute bits have been verified.  Allows the cache-hit path to skip the
    /// `O(n_files)` permission walk on subsequent steps.
    ///
    /// Defaults to `false` so that older entries (written before this field
    /// existed) trigger one re-verification on next access.
    #[serde(default)]
    execute_bits_verified: bool,
}

/// Classified kind for one raw `content_map` key.
#[derive(Debug)]
enum ContentMapKeyKind {
    /// Regular file: bytes are written verbatim to `payload/<relative_path>`.
    File {
        /// Normalized sandbox-relative destination path.
        relative_path: PathBuf,
    },
    /// Directory ZIP: archive bytes are unpacked into `payload/<relative_dir>`.
    ///
    /// `relative_dir` is empty (`PathBuf::new()`) for the special `./` root
    /// key, meaning the ZIP content is unpacked directly at `payload/`.
    Directory {
        /// Normalized destination directory, empty for root.
        relative_dir: PathBuf,
    },
}

/// Prepares the persistent cache entry for one conductor tool and returns the
/// `payload/` directory path.
///
/// # Cache-hit path
///
/// When the entry's `metadata.json` contains a `content_map` that equals
/// `content_map` entry-for-entry and `payload/` exists as a directory, the
/// entry is reused: `last_used_unix_seconds` is refreshed and the existing
/// payload path is returned without touching the CAS.
///
/// # Cache-miss path
///
/// CAS bytes for every entry in `content_map` are fetched concurrently.  Any
/// previous entry directory is removed.  All content is then extracted into a
/// fresh `payload/` tree (CPU-bound ZIP extraction runs in a blocking task).
/// Finally, `metadata.json` is written atomically via a temp-file rename.
///
/// When the tool executable resolves directly inside the returned payload
/// path, the caller may execute from it without populating the sandbox.
/// Otherwise, the caller populates the execution sandbox from the returned
/// path via [`link_payload_to_sandbox`].
///
/// # Errors
///
/// Returns [`ConductorError`] when:
/// - CAS retrieval fails for any entry,
/// - a `content_map` key is invalid (absolute path, sandbox escape, or
///   malformed directory key),
/// - two entries claim the same target file path,
/// - ZIP extraction fails for a directory-form key, or
/// - filesystem operations fail.
#[expect(
    clippy::too_many_lines,
    reason = "preflight classification, concurrent CAS fetch, blocking extraction, and metadata persistence each need full context; splitting them across helpers would obscure the sequential invariants the function encodes"
)]
pub(crate) async fn prepare_tool_content_cache<C>(
    tools_dir: &Path,
    tool_id: &str,
    content_map: &BTreeMap<String, Hash>,
    cas: &Arc<C>,
) -> Result<PathBuf, ConductorError>
where
    C: CasApi + Send + Sync + 'static,
{
    let entry_dir = tools_dir.join(sanitize_tool_id(tool_id));
    let payload_dir = entry_dir.join(TOOL_CONTENT_CACHE_PAYLOAD_DIR_NAME);
    let metadata_path = entry_dir.join(TOOL_CONTENT_CACHE_METADATA_FILE_NAME);
    let now = now_unix_seconds();

    // Best-effort expiry pruning: errors are intentionally swallowed so
    // cleanup never blocks workflow execution.
    let _ = maybe_prune_expired_tool_content_cache_entries(tools_dir, now);

    // --- Cache hit ---
    if payload_dir.is_dir()
        && let Ok(raw) = fs::read_to_string(&metadata_path)
        && let Ok(metadata) = serde_json::from_str::<ToolContentCacheMetadata>(&raw)
        && metadata.version == TOOL_CONTENT_CACHE_VERSION
        && metadata.content_map == *content_map
    {
        // Skip the O(n_files) permission walk when it has already been done
        // for this payload tree.  The flag defaults to false so entries
        // written by older code trigger one re-verification on first access.
        if !metadata.execute_bits_verified {
            ensure_payload_tree_user_execute_bits(&payload_dir)?;
        }
        // Refresh last-used timestamp and record verified status (best-effort;
        // miss is harmless).
        let _ = persist_cache_metadata(
            &metadata_path,
            &ToolContentCacheMetadata {
                version: TOOL_CONTENT_CACHE_VERSION,
                content_map: content_map.clone(),
                last_used_unix_seconds: now,
                execute_bits_verified: true,
            },
        );
        return Ok(payload_dir);
    }

    // --- Cache miss: classify keys ---
    let classified: Vec<(String, Hash, ContentMapKeyKind)> = content_map
        .iter()
        .map(|(key, hash)| {
            let kind = classify_content_map_key(key)?;
            Ok::<_, ConductorError>((key.clone(), *hash, kind))
        })
        .collect::<Result<Vec<_>, _>>()?;

    // --- Fetch all CAS bytes concurrently ---
    let entries_with_bytes: Vec<(String, ContentMapKeyKind, Vec<u8>)> =
        try_join_all(classified.into_iter().map(|(key, hash, kind)| {
            let cas = cas.clone();
            async move {
                let bytes = cas.get(hash).await?.to_vec();
                Ok::<_, ConductorError>((key, kind, bytes))
            }
        }))
        .await?;

    // --- Extract in a blocking task (CPU-bound + sync I/O) ---
    let entry_dir_for_task = entry_dir;
    let payload_dir_for_task = payload_dir;
    let metadata_path_for_task = metadata_path;
    let content_map_for_task = content_map.clone();

    task::spawn_blocking(move || {
        // Remove stale entry so extraction starts clean.
        if entry_dir_for_task.exists() {
            fs::remove_dir_all(&entry_dir_for_task).map_err(|source| ConductorError::Io {
                operation: "removing stale tool-content cache entry".to_string(),
                path: entry_dir_for_task.clone(),
                source,
            })?;
        }
        fs::create_dir_all(&payload_dir_for_task).map_err(|source| ConductorError::Io {
            operation: "creating tool-content cache payload directory".to_string(),
            path: payload_dir_for_task.clone(),
            source,
        })?;

        // Phase 1 — collision detection.
        //
        // Build a map of every file path that will be written to `payload/`.
        // File entries contribute a single path; directory entries contribute
        // all member file paths from their ZIP archive.  Overlapping paths
        // across two distinct content-map entries are rejected before any
        // extraction starts.
        let mut claimed: BTreeMap<PathBuf, String> = BTreeMap::new();

        for (key, kind, bytes) in &entries_with_bytes {
            match kind {
                ContentMapKeyKind::File { relative_path } => {
                    if let Some(prev) = claimed.insert(relative_path.clone(), key.clone()) {
                        return Err(ConductorError::Workflow(format!(
                            "tool content map entries '{}' and '{}' both materialize '{}' and would overwrite each other",
                            prev,
                            key,
                            relative_path.display()
                        )));
                    }
                }
                ContentMapKeyKind::Directory { relative_dir } => {
                    let members =
                        mediapm_conductor_builtin_archive::list_zip_member_file_paths(bytes)
                            .map_err(|err| {
                                ConductorError::Workflow(format!(
                                    "tool content map directory key '{key}' expects ZIP payload, but member listing failed: {err}"
                                ))
                            })?;
                    for member in members {
                        let full_path = if relative_dir.as_os_str().is_empty() {
                            PathBuf::from(&member)
                        } else {
                            relative_dir.join(&member)
                        };
                        if let Some(prev) = claimed.insert(full_path.clone(), key.clone()) {
                            return Err(ConductorError::Workflow(format!(
                                "tool content map entries '{}' and '{}' both materialize '{}' and would overwrite each other",
                                prev,
                                key,
                                full_path.display()
                            )));
                        }
                    }
                }
            }
        }

        // Phase 2 — extraction.
        for (key, kind, bytes) in entries_with_bytes {
            match kind {
                ContentMapKeyKind::File { relative_path } => {
                    let target_path = payload_dir_for_task.join(&relative_path);
                    if let Some(parent) = target_path.parent() {
                        fs::create_dir_all(parent).map_err(|source| ConductorError::Io {
                            operation: "creating tool-content file parent directories".to_string(),
                            path: parent.to_path_buf(),
                            source,
                        })?;
                    }
                    fs::write(&target_path, &bytes).map_err(|source| ConductorError::Io {
                        operation: "writing tool-content file to cache payload".to_string(),
                        path: target_path.clone(),
                        source,
                    })?;
                    ensure_user_execute_bit(&target_path)?;
                }
                ContentMapKeyKind::Directory { relative_dir } => {
                    let unpack_dir = if relative_dir.as_os_str().is_empty() {
                        payload_dir_for_task.clone()
                    } else {
                        payload_dir_for_task.join(&relative_dir)
                    };
                    mediapm_conductor_builtin_archive::unpack_zip_bytes_to_directory(
                        &bytes,
                        &unpack_dir,
                    )
                    .map_err(|err| {
                        ConductorError::Workflow(format!(
                            "tool content map directory key '{key}' expects ZIP payload, but unpack failed: {err}"
                        ))
                    })?;
                }
            }
        }

        ensure_payload_tree_user_execute_bits(&payload_dir_for_task)?;

        // Phase 3 — write metadata atomically.
        persist_cache_metadata(
            &metadata_path_for_task,
            &ToolContentCacheMetadata {
                version: TOOL_CONTENT_CACHE_VERSION,
                content_map: content_map_for_task,
                last_used_unix_seconds: now,
                execute_bits_verified: true,
            },
        )?;

        Ok::<PathBuf, ConductorError>(payload_dir_for_task)
    })
    .await
    .map_err(|join_err| {
        ConductorError::Internal(format!(
            "joining tool-content cache extraction task failed: {join_err}"
        ))
    })?
}

/// Ensures one extracted payload file is executable by the current user.
///
/// On Unix hosts this sets `u+x` while preserving all existing permission
/// bits. Non-Unix hosts leave permissions unchanged.
fn ensure_user_execute_bit(path: &Path) -> Result<(), ConductorError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let metadata = fs::metadata(path).map_err(|source| ConductorError::Io {
            operation: "reading extracted tool-content file permissions".to_string(),
            path: path.to_path_buf(),
            source,
        })?;
        let mut permissions = metadata.permissions();
        let mode = permissions.mode();
        if mode & 0o100 == 0 {
            permissions.set_mode(mode | 0o100);
            fs::set_permissions(path, permissions).map_err(|source| ConductorError::Io {
                operation: "marking extracted tool-content file executable".to_string(),
                path: path.to_path_buf(),
                source,
            })?;
        }
    }

    Ok(())
}

/// Recursively ensures all regular files under one payload tree have owner
/// execute permissions so bundled companion binaries remain runnable.
fn ensure_payload_tree_user_execute_bits(root: &Path) -> Result<(), ConductorError> {
    if !root.exists() {
        return Ok(());
    }

    #[cfg(unix)]
    {
        let entries = fs::read_dir(root).map_err(|source| ConductorError::Io {
            operation: "enumerating tool-content payload tree for permission refresh".to_string(),
            path: root.to_path_buf(),
            source,
        })?;

        for entry in entries {
            let entry = entry.map_err(|source| ConductorError::Io {
                operation: "reading tool-content payload directory entry".to_string(),
                path: root.to_path_buf(),
                source,
            })?;
            let path = entry.path();
            let file_type = entry.file_type().map_err(|source| ConductorError::Io {
                operation: "reading tool-content payload entry file type".to_string(),
                path: path.clone(),
                source,
            })?;

            if file_type.is_dir() {
                ensure_payload_tree_user_execute_bits(&path)?;
            } else if file_type.is_file() {
                ensure_user_execute_bit(&path)?;
            }
        }
    }

    Ok(())
}

/// Platform directory names that are never relevant on the current OS.
///
/// Top-level `payload/` subdirectories whose name matches one of these strings
/// are skipped during sandbox hard-linking to avoid materialising hundreds of
/// files that will never be executed.  The names are the conventional
/// sub-directory names used by mediapm tool bundles (`linux`, `macos`,
/// `windows`).
#[cfg(target_os = "macos")]
const FOREIGN_PLATFORM_DIRS: &[&str] = &["linux", "windows"];
#[cfg(target_os = "linux")]
const FOREIGN_PLATFORM_DIRS: &[&str] = &["macos", "windows"];
#[cfg(target_os = "windows")]
const FOREIGN_PLATFORM_DIRS: &[&str] = &["linux", "macos"];
#[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
const FOREIGN_PLATFORM_DIRS: &[&str] = &[];

/// Hard-links (or copies as fallback) all files from a cached `payload/`
/// directory into an execution sandbox directory.
///
/// Because all steps that use the same conductor tool share a single
/// `payload/` tree, this is the only step-specific operation: files are
/// linked into the per-step sandbox without re-extracting or copying bytes.
///
/// Hard links are attempted first (near-zero-cost metadata operation).  If the
/// link fails (e.g. cross-device), a byte-for-byte copy is used as fallback.
///
/// Top-level subdirectories whose name matches a known foreign-platform
/// identifier (`linux`, `macos`, `windows` — whichever do not apply to the
/// current OS) are skipped entirely.  This avoids materialising hundreds of
/// files that will never execute on the host.
///
/// # Errors
///
/// Returns a descriptive `String` on failure.  Callers should wrap this into
/// an appropriate [`ConductorError`] variant.
pub(super) fn link_payload_to_sandbox(
    payload_dir: &Path,
    sandbox_dir: &Path,
) -> Result<(), String> {
    if !payload_dir.exists() {
        return Err(format!(
            "tool-content cache payload directory '{}' does not exist",
            payload_dir.display()
        ));
    }

    fs::create_dir_all(sandbox_dir).map_err(|err| {
        format!("creating sandbox directory '{}' failed: {err}", sandbox_dir.display())
    })?;

    let entries = fs::read_dir(payload_dir).map_err(|err| {
        format!("reading payload directory '{}' failed: {err}", payload_dir.display())
    })?;

    for entry in entries {
        let entry =
            entry.map_err(|err| format!("reading payload directory entry failed: {err}"))?;
        let file_name = entry.file_name();
        let name = file_name.to_string_lossy();
        let path = entry.path();
        let target_path = sandbox_dir.join(&file_name);

        let file_type = entry
            .file_type()
            .map_err(|err| format!("reading file type for '{}' failed: {err}", path.display()))?;

        // Skip top-level directories that belong to a foreign platform.
        // This avoids hard-linking hundreds of binary files that can never
        // run on the current OS (e.g. Linux/Windows ffmpeg on macOS).
        if file_type.is_dir() && FOREIGN_PLATFORM_DIRS.contains(&name.as_ref()) {
            continue;
        }

        if file_type.is_dir() {
            copy_directory_recursive(&path, &target_path)?;
        } else {
            // Non-directory top-level entry — hard-link or copy as usual.
            if let Some(parent) = target_path.parent() {
                fs::create_dir_all(parent).map_err(|err| {
                    format!("creating parent directory '{}' failed: {err}", parent.display())
                })?;
            }
            if fs::hard_link(&path, &target_path).is_ok() {
                continue;
            }
            fs::copy(&path, &target_path).map_err(|err| {
                format!("copying '{}' to '{}' failed: {err}", path.display(), target_path.display())
            })?;
            let source_permissions = fs::metadata(&path)
                .map_err(|err| {
                    format!("reading permissions for '{}' failed: {err}", path.display())
                })?
                .permissions();
            fs::set_permissions(&target_path, source_permissions).map_err(|err| {
                format!("setting permissions on '{}' failed: {err}", target_path.display())
            })?;
        }
    }

    Ok(())
}

/// Runs TTL pruning when the cooldown interval has elapsed.
///
/// Any metadata-read/write error is intentionally ignored by callers so prune
/// scheduling never blocks workflow execution.
fn maybe_prune_expired_tool_content_cache_entries(
    tools_dir: &Path,
    now_unix_seconds: u64,
) -> Result<(), ConductorError> {
    let marker_path = tools_dir.join(TOOL_CONTENT_CACHE_PRUNE_MARKER_FILE_NAME);
    if let Ok(raw) = fs::read_to_string(&marker_path)
        && let Ok(last_prune) = raw.trim().parse::<u64>()
        && now_unix_seconds.saturating_sub(last_prune) < TOOL_CONTENT_CACHE_PRUNE_COOLDOWN_SECONDS
    {
        return Ok(());
    }

    prune_expired_tool_content_cache_entries(tools_dir)?;
    let _ = fs::create_dir_all(tools_dir);
    let _ = fs::write(&marker_path, format!("{now_unix_seconds}\n"));
    Ok(())
}

/// Removes tool-content cache entries that have not been used within 24 hours.
///
/// Skips missing entries, unreadable metadata, or version-mismatched entries
/// silently so cleanup never blocks workflow execution.
pub(super) fn prune_expired_tool_content_cache_entries(
    tools_dir: &Path,
) -> Result<(), ConductorError> {
    if !tools_dir.exists() {
        return Ok(());
    }

    let now = now_unix_seconds();
    let cutoff = now.saturating_sub(TOOL_CONTENT_CACHE_ENTRY_TTL_SECONDS);

    let entries = fs::read_dir(tools_dir).map_err(|source| ConductorError::Io {
        operation: "enumerating tool-content cache root".to_string(),
        path: tools_dir.to_path_buf(),
        source,
    })?;

    for entry in entries {
        let entry = entry.map_err(|source| ConductorError::Io {
            operation: "reading tool-content cache directory entry".to_string(),
            path: tools_dir.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        if !entry.file_type().is_ok_and(|ty| ty.is_dir()) {
            continue;
        }
        let metadata_path = path.join(TOOL_CONTENT_CACHE_METADATA_FILE_NAME);
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

/// Returns a filesystem-safe directory name derived from one conductor tool identifier.
///
/// Characters unsafe on major platforms (`/`, `\`, `:`, `?`, `*`, `<`, `>`,
/// `|`, `"`) are replaced with `_` so the name can be used directly as a
/// subdirectory under `tools/`.
///
/// Typical tool IDs (e.g.
/// `ffmpeg+evermeet-ffmpeg@6e66d4d1e81f75b5f34dc2a369cc341e12edc531`) contain
/// only `.`, `+`, `@`, `-`, and alphanumeric characters, none of which require
/// sanitization.
fn sanitize_tool_id(tool_id: &str) -> String {
    tool_id
        .chars()
        .map(|ch| {
            if matches!(ch, '/' | '\\' | ':' | '?' | '*' | '<' | '>' | '|' | '"') {
                '_'
            } else {
                ch
            }
        })
        .collect()
}

/// Classifies one raw `content_map` key into a file or directory extraction
/// target.
///
/// Rules:
/// - Keys ending with `/` or `\` are directory ZIP targets.
/// - The special `./` (or `.\`) key unpacks a ZIP directly at `payload/` root.
/// - All other keys are regular file targets.
///
/// Absolute paths, sandbox-escaping paths (`../`), and trailing-slash keys
/// without a concrete path component are rejected.
fn classify_content_map_key(raw: &str) -> Result<ContentMapKeyKind, ConductorError> {
    if raw.ends_with('/') || raw.ends_with('\\') {
        let trimmed = raw.trim_end_matches(['/', '\\']);
        if trimmed == "." {
            // `./` or `.\` — unpack ZIP at payload root.
            return Ok(ContentMapKeyKind::Directory { relative_dir: PathBuf::new() });
        }
        if trimmed.trim().is_empty() {
            return Err(ConductorError::Workflow(format!(
                "tool content map directory key '{raw}' must contain at least one path component before trailing slash"
            )));
        }
        let relative_dir = normalize_sandbox_relative_path(trimmed, raw)?;
        return Ok(ContentMapKeyKind::Directory { relative_dir });
    }

    let relative_path = normalize_sandbox_relative_path(raw, raw)?;
    Ok(ContentMapKeyKind::File { relative_path })
}

/// Normalizes and validates one sandbox-relative path string.
///
/// `raw` is the path string to normalize; `context_key` is included in error
/// messages for diagnostic clarity.
///
/// Accepts only relative paths that do not escape the sandbox root:
/// absolute paths, `..` components, and empty-after-normalization paths are
/// all rejected.
fn normalize_sandbox_relative_path(
    raw: &str,
    context_key: &str,
) -> Result<PathBuf, ConductorError> {
    if raw.trim().is_empty() {
        return Err(ConductorError::Workflow(format!(
            "tool content map key '{context_key}' path must be non-empty"
        )));
    }
    let parsed = Path::new(raw);
    if parsed.is_absolute() {
        return Err(ConductorError::Workflow(format!(
            "tool content map key '{context_key}' path must be relative"
        )));
    }
    let mut normalized = PathBuf::new();
    for component in parsed.components() {
        match component {
            Component::Normal(part) => normalized.push(part),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(ConductorError::Workflow(format!(
                    "tool content map key '{context_key}' must not escape the tool sandbox"
                )));
            }
        }
    }
    if normalized.as_os_str().is_empty() {
        return Err(ConductorError::Workflow(format!(
            "tool content map key '{context_key}' must contain a concrete path component"
        )));
    }
    Ok(normalized)
}

/// Copies (preferring hard links) all files from `source_dir` into `target_dir`.
///
/// The directory tree under `source_dir` is reproduced under `target_dir`.
/// Hard links are attempted first (near-zero-cost metadata-only operation);
/// when a link fails (e.g. cross-device), a byte-for-byte copy with permission
/// preservation is used as fallback.
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

        // Prefer hard links (metadata-only) to avoid copying bytes when
        // cache and sandbox share the same filesystem.
        if fs::hard_link(&path, &target_path).is_ok() {
            continue;
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

/// Writes cache metadata atomically via a temporary-file rename.
///
/// The metadata is written to a `.json.tmp` sibling first, then renamed into
/// place so any interrupted write leaves the old metadata intact.
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

/// Returns the current Unix timestamp in whole seconds.
#[must_use]
fn now_unix_seconds() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Confirms that tool IDs containing only safe characters are not altered.
    #[test]
    fn sanitize_tool_id_preserves_safe_characters() {
        let safe = "ffmpeg+evermeet-ffmpeg@6e66d4d1e81f75b5f34dc2a369cc341e12edc531";
        assert_eq!(sanitize_tool_id(safe), safe);
    }

    /// Confirms that all filesystem-unsafe characters are replaced with `_`.
    #[test]
    fn sanitize_tool_id_replaces_unsafe_characters() {
        let input = r#"tool/a:b*c?d<e>f|g"h\i"#;
        let sanitized = sanitize_tool_id(input);
        assert_eq!(sanitized, "tool_a_b_c_d_e_f_g_h_i");
    }

    /// `./` or `.\` key must classify as Directory with an empty `relative_dir`.
    #[test]
    fn classify_dot_slash_key_maps_to_empty_relative_dir() {
        match classify_content_map_key("./").expect("classify ./") {
            ContentMapKeyKind::Directory { relative_dir } => {
                assert!(relative_dir.as_os_str().is_empty(), "expected empty relative_dir for ./");
            }
            ContentMapKeyKind::File { .. } => panic!("expected Directory for ./"),
        }
    }

    /// A named directory key (`linux/`) must classify as Directory with the
    /// named component as `relative_dir`.
    #[test]
    fn classify_named_directory_key_maps_to_relative_dir() {
        match classify_content_map_key("linux/").expect("classify linux/") {
            ContentMapKeyKind::Directory { relative_dir } => {
                assert_eq!(relative_dir, PathBuf::from("linux"));
            }
            ContentMapKeyKind::File { .. } => panic!("expected Directory for linux/"),
        }
    }

    /// A file-form key must classify as File with the normalized path.
    #[test]
    fn classify_file_key_maps_to_relative_path() {
        match classify_content_map_key("bin/tool").expect("classify bin/tool") {
            ContentMapKeyKind::File { relative_path } => {
                assert_eq!(relative_path, PathBuf::from("bin/tool"));
            }
            ContentMapKeyKind::Directory { .. } => panic!("expected File for bin/tool"),
        }
    }

    /// A bare `/` key (no component before the trailing slash) must be rejected.
    #[test]
    fn classify_bare_slash_key_is_rejected() {
        let err = classify_content_map_key("/").expect_err("bare slash should be rejected");
        match err {
            ConductorError::Workflow(msg) => {
                assert!(msg.contains("must contain at least one path component"), "{msg}");
            }
            other => panic!("unexpected error kind: {other:?}"),
        }
    }

    /// Extracted raw-file entries should gain `u+x` so bundled companion
    /// executables remain runnable from payload cache trees.
    #[cfg(unix)]
    #[test]
    fn ensure_user_execute_bit_sets_owner_execute_permission() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().expect("tempdir");
        let file_path = temp.path().join("tool.bin");
        fs::write(&file_path, b"tool-bytes").expect("write file");

        let mut permissions = fs::metadata(&file_path).expect("metadata").permissions();
        permissions.set_mode(0o644);
        fs::set_permissions(&file_path, permissions).expect("set non-executable mode");

        ensure_user_execute_bit(&file_path).expect("set execute bit");

        let mode = fs::metadata(&file_path).expect("metadata after").permissions().mode();
        assert_ne!(mode & 0o100, 0, "owner execute bit should be set");
    }
}
