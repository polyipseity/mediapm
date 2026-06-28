//! CAS pre-fetch and synchronous extraction logic.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use futures_util::future::try_join_all;
use mediapm_cas::{CasApi, Hash};

use crate::error::ConductorError;

use super::VERSION;
use super::helpers::{
    classify_content_map_key, ensure_payload_tree_user_execute_bits, ensure_user_execute_bit,
    now_unix_seconds, persist_cache_metadata,
};
use super::types::{ContentMapKeyKind, Metadata, ProvisionedTool};

/// Writes all cache entries (files and directories) into the payload directory.
fn write_cache_entries(
    payload_dir: &Path,
    entries_with_bytes: &[(String, ContentMapKeyKind, Vec<u8>)],
) -> Result<(), ConductorError> {
    for (key, kind, bytes) in entries_with_bytes {
        match kind {
            ContentMapKeyKind::File { relative_path } => {
                let target_path = payload_dir.join(relative_path);
                if let Some(parent) = target_path.parent() {
                    fs::create_dir_all(parent).map_err(|source| ConductorError::Io {
                        operation: "creating tool-content file parent directories".to_string(),
                        path: parent.to_path_buf(),
                        source,
                    })?;
                }
                fs::write(&target_path, bytes).map_err(|source| ConductorError::Io {
                    operation: "writing tool-content file to cache payload".to_string(),
                    path: target_path.clone(),
                    source,
                })?;
                ensure_user_execute_bit(&target_path)?;
            }
            ContentMapKeyKind::Directory { relative_dir } => {
                let unpack_dir = if relative_dir.as_os_str().is_empty() {
                    payload_dir.to_path_buf()
                } else {
                    payload_dir.join(relative_dir)
                };
                mediapm_conductor_builtin_archive::unpack_zip_bytes_to_directory(
                    bytes,
                    &unpack_dir,
                )
                .map_err(|err| {
                    ConductorError::Workflow(format!(
                        "tool content map directory key '{key}' expects ZIP payload, \
                         but unpack failed: {err}"
                    ))
                })?;
            }
        }
    }
    Ok(())
}

/// Fetches all bytes referenced by `content_map` from the CAS concurrently.
pub(crate) async fn fetch_all_cas_entries<C: CasApi + Send + Sync + 'static>(
    cas: Arc<C>,
    content_map: &BTreeMap<String, Hash>,
) -> Result<Vec<(String, ContentMapKeyKind, Vec<u8>)>, ConductorError> {
    let classified: Vec<(String, Hash, ContentMapKeyKind)> = content_map
        .iter()
        .map(|(key, hash)| {
            let kind = classify_content_map_key(key)?;
            Ok::<_, ConductorError>((key.clone(), *hash, kind))
        })
        .collect::<Result<Vec<_>, _>>()?;

    try_join_all(classified.into_iter().map(|(key, hash, kind)| {
        let cas = Arc::clone(&cas);
        async move {
            let bytes = cas.get(hash).await.map_err(ConductorError::Cas)?;
            Ok::<_, ConductorError>((key, kind, bytes.to_vec()))
        }
    }))
    .await
}

/// Groups the four paths that describe one tool-content cache entry.
pub(crate) struct ExtractPaths {
    pub entry_dir: PathBuf,
    pub lock_path: PathBuf,
    pub payload_dir: PathBuf,
    pub metadata_path: PathBuf,
}

/// Validates content-map entries do not collide on the same target path.
fn build_collision_map(
    entries_with_bytes: &[(String, ContentMapKeyKind, Vec<u8>)],
) -> Result<(), ConductorError> {
    let mut claimed: BTreeMap<PathBuf, String> = BTreeMap::new();
    for (key, kind, bytes) in entries_with_bytes {
        match kind {
            ContentMapKeyKind::File { relative_path } => {
                if let Some(prev) = claimed.insert(relative_path.clone(), key.clone()) {
                    return Err(ConductorError::Workflow(format!(
                        "tool content map entries '{prev}' and '{key}' both materialize \
                         '{}' and would overwrite each other",
                        relative_path.display()
                    )));
                }
            }
            ContentMapKeyKind::Directory { relative_dir } => {
                let members = mediapm_conductor_builtin_archive::list_zip_member_file_paths(bytes)
                    .map_err(|err| {
                        ConductorError::Workflow(format!(
                            "tool content map directory key '{key}' expects ZIP payload, \
                             but member listing failed: {err}"
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
                            "tool content map entries '{prev}' and '{key}' both materialize \
                             '{}' and would overwrite each other",
                            full_path.display()
                        )));
                    }
                }
            }
        }
    }
    Ok(())
}

/// Synchronous extraction: acquires exclusive lock, extracts payloads, writes
/// metadata, then downgrades to a shared lock.
pub(crate) fn extract_sync(
    paths: &ExtractPaths,
    content_map: &BTreeMap<String, Hash>,
    entries_with_bytes: &[(String, ContentMapKeyKind, Vec<u8>)],
) -> Result<ProvisionedTool, ConductorError> {
    let now = now_unix_seconds();

    // Ensure the entry directory exists before opening the lock file.
    fs::create_dir_all(&paths.entry_dir).map_err(|source| ConductorError::Io {
        operation: "creating tool-content cache entry directory".to_string(),
        path: paths.entry_dir.clone(),
        source,
    })?;

    // Open/create the lock file and acquire an exclusive (write) lock.
    // This blocks until any other worker's extraction finishes.
    let excl_file = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(&paths.lock_path)
        .map_err(|source| ConductorError::Io {
            operation: "opening tool-content cache lock file".to_string(),
            path: paths.lock_path.clone(),
            source,
        })?;
    excl_file.lock().map_err(|source| ConductorError::Io {
        operation: "acquiring exclusive lock on tool-content cache entry".to_string(),
        path: paths.lock_path.clone(),
        source,
    })?;

    // Double-check: another process may have populated the entry while we
    // were waiting for the exclusive lock.
    if let Some(guard) = double_check_hit(
        &paths.payload_dir,
        &paths.metadata_path,
        &paths.lock_path,
        &excl_file,
        content_map,
        now,
    )? {
        drop(excl_file);
        return Ok(guard);
    }

    // --- Cache miss: validate and extract ---
    build_collision_map(entries_with_bytes)?;

    // Phase 2: remove stale entry, create fresh payload dir, extract.
    if paths.entry_dir.exists() {
        fs::remove_dir_all(&paths.entry_dir).map_err(|source| ConductorError::Io {
            operation: "removing stale tool-content cache entry".to_string(),
            path: paths.entry_dir.clone(),
            source,
        })?;
    }
    fs::create_dir_all(&paths.payload_dir).map_err(|source| ConductorError::Io {
        operation: "creating tool-content cache payload directory".to_string(),
        path: paths.payload_dir.clone(),
        source,
    })?;

    write_cache_entries(&paths.payload_dir, entries_with_bytes)?;
    ensure_payload_tree_user_execute_bits(&paths.payload_dir)?;

    // Phase 3: recreate the lock file (removed by remove_dir_all above) and
    // persist metadata atomically.
    std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(true)
        .open(&paths.lock_path)
        .map_err(|source| ConductorError::Io {
            operation: "recreating tool-content cache lock file after extraction".to_string(),
            path: paths.lock_path.clone(),
            source,
        })?;

    persist_cache_metadata(
        &paths.metadata_path,
        &Metadata {
            version: VERSION,
            content_map: content_map.clone(),
            last_used_unix_seconds: now,
            execute_bits_verified: true,
        },
    )?;

    // Acquire a shared lock on the new lock file while still holding the
    // exclusive lock on the old inode (orphaned by remove_dir_all).
    let shared_lock = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(true)
        .open(&paths.lock_path)
        .map_err(|source| ConductorError::Io {
            operation: "opening new tool-content cache lock file for shared lock".to_string(),
            path: paths.lock_path.clone(),
            source,
        })?;
    shared_lock.lock_shared().map_err(|source| ConductorError::Io {
        operation: "acquiring shared lock on new tool-content cache entry".to_string(),
        path: paths.lock_path.clone(),
        source,
    })?;

    // Drop the exclusive lock fd — the old inode is now orphaned and the new
    // .lock file holds the shared lock via shared_lock.
    drop(excl_file);

    Ok(ProvisionedTool { payload_dir: paths.payload_dir.clone(), _lock_file: shared_lock })
}

/// Checks whether another process completed extraction while we waited for
/// the exclusive lock.
fn double_check_hit(
    payload_dir: &Path,
    metadata_path: &Path,
    lock_path: &Path,
    excl_file: &std::fs::File,
    content_map: &BTreeMap<String, Hash>,
    now: u64,
) -> Result<Option<ProvisionedTool>, ConductorError> {
    if !payload_dir.is_dir() {
        return Ok(None);
    }
    let Ok(raw) = fs::read_to_string(metadata_path) else {
        return Ok(None);
    };
    let Ok(metadata) = serde_json::from_str::<Metadata>(&raw) else {
        return Ok(None);
    };
    if metadata.version != VERSION || metadata.content_map != *content_map {
        return Ok(None);
    }

    // Double-check hit — another worker finished extraction while we waited.
    if !metadata.execute_bits_verified {
        ensure_payload_tree_user_execute_bits(payload_dir)?;
    }
    let _ = persist_cache_metadata(
        metadata_path,
        &Metadata {
            version: VERSION,
            content_map: content_map.clone(),
            last_used_unix_seconds: now,
            execute_bits_verified: true,
        },
    );
    // Clone fd and downgrade to shared.
    let shared_file = excl_file.try_clone().map_err(|source| ConductorError::Io {
        operation: "duplicating lock fd for shared downgrade".to_string(),
        path: lock_path.to_path_buf(),
        source,
    })?;
    shared_file.lock_shared().map_err(|source| ConductorError::Io {
        operation: "downgrading exclusive lock to shared after double-check hit".to_string(),
        path: lock_path.to_path_buf(),
        source,
    })?;
    Ok(Some(ProvisionedTool { payload_dir: payload_dir.to_path_buf(), _lock_file: shared_file }))
}
