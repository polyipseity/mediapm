//! Payload download, extraction, and content-map enumeration helpers.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs;
use std::io::{Cursor, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use flate2::read::GzDecoder;
use mediapm_conductor::fetch_common_executable_tool_payload;
use tar::Archive as TarArchive;
use xz2::read::XzDecoder;
use zip::ZipArchive;

use crate::error::MediaPmError;
use crate::tools::catalog::{ToolCatalogEntry, ToolOs};

use super::ToolDownloadCache;
use super::http::fetch_bytes_from_candidates;
use super::http::probe_content_length_from_candidates;
use super::models::ContentMapSource;
use super::models::{OsDownloadAction, ResolvedDownloadPlan};
use super::{DownloadProgressCallback, DownloadProgressSnapshot};

/// Materializes resolved download actions into one install root.
pub(super) async fn materialize_download_plan(
    entry: &ToolCatalogEntry,
    plan: &ResolvedDownloadPlan,
    install_root: &Path,
    download_progress: Option<DownloadProgressCallback>,
    download_cache: Option<Arc<ToolDownloadCache>>,
) -> Result<(), MediaPmError> {
    if plan.internal_launcher {
        materialize_internal_launcher(entry, plan, install_root)?;
        if let Some(callback) = download_progress {
            callback(DownloadProgressSnapshot { downloaded_bytes: 0, total_bytes: Some(0) });
        }
        return Ok(());
    }

    if let Some(common_tool) = plan.common_executable_tool {
        let payload_size =
            materialize_conductor_common_executable(entry, plan, install_root, common_tool)?;
        if let Some(callback) = download_progress {
            callback(DownloadProgressSnapshot {
                downloaded_bytes: payload_size,
                total_bytes: Some(payload_size),
            });
        }
        return Ok(());
    }

    let cache_identity = download_cache_identity(plan);

    if plan.shared_package {
        let first_action = plan.per_os_actions.values().next().ok_or_else(|| {
            MediaPmError::Workflow("resolved downloader plan has no actions".to_string())
        })?;
        materialize_one_os_payload(
            entry,
            first_action,
            install_root,
            download_progress,
            download_cache,
            &cache_identity,
        )
        .await?;
        return Ok(());
    }

    let mut cumulative_downloaded_bytes = 0_u64;
    let planned_total_bytes = planned_total_download_bytes(plan).await;

    if let Some(callback) = download_progress.as_ref() {
        callback(DownloadProgressSnapshot {
            downloaded_bytes: 0,
            total_bytes: planned_total_bytes,
        });
    }

    for action in plan.per_os_actions.values() {
        let os_root = install_root.join(action.os.as_str());

        let action_snapshot = Arc::new(Mutex::new(DownloadProgressSnapshot {
            downloaded_bytes: 0,
            total_bytes: Some(0),
        }));
        let progress_callback = download_progress.as_ref().map(|callback| {
            let callback = Arc::clone(callback);
            let action_snapshot = Arc::clone(&action_snapshot);
            let base_downloaded = cumulative_downloaded_bytes;

            Arc::new(move |snapshot: DownloadProgressSnapshot| {
                if let Ok(mut current) = action_snapshot.lock() {
                    *current = snapshot;
                }

                callback(aggregate_progress_snapshot(
                    base_downloaded,
                    snapshot,
                    planned_total_bytes,
                ));
            }) as DownloadProgressCallback
        });

        materialize_one_os_payload(
            entry,
            action,
            &os_root,
            progress_callback,
            download_cache.clone(),
            &cache_identity,
        )
        .await?;

        let completed_snapshot = action_snapshot
            .lock()
            .map(|snapshot| *snapshot)
            .unwrap_or(DownloadProgressSnapshot { downloaded_bytes: 0, total_bytes: None });
        cumulative_downloaded_bytes =
            cumulative_downloaded_bytes.saturating_add(completed_snapshot.downloaded_bytes);
        if let Some(callback) = download_progress.as_ref() {
            let mut downloaded_bytes = cumulative_downloaded_bytes;
            let total_bytes = planned_total_bytes;
            if let Some(total_bytes) = total_bytes {
                downloaded_bytes = downloaded_bytes.min(total_bytes);
            }

            callback(DownloadProgressSnapshot { downloaded_bytes, total_bytes });
        }
    }

    Ok(())
}

/// Materializes one conductor common-source executable payload.
fn materialize_conductor_common_executable(
    entry: &ToolCatalogEntry,
    plan: &ResolvedDownloadPlan,
    install_root: &Path,
    common_tool: mediapm_conductor::CommonExecutableTool,
) -> Result<u64, MediaPmError> {
    let payload = fetch_common_executable_tool_payload(common_tool).map_err(|error| {
        MediaPmError::Workflow(format!(
            "materializing common executable tool '{}' failed: {error}",
            entry.name
        ))
    })?;

    for action in plan.per_os_actions.values() {
        let destination =
            install_root.join(action.os.as_str()).join(entry.executable_name_for_os(action.os));
        write_binary_file(&destination, &payload.executable_bytes)?;
    }

    Ok(u64::try_from(payload.executable_bytes.len()).unwrap_or(u64::MAX))
}

/// Materializes locally generated command-launcher shims for internal tools.
fn materialize_internal_launcher(
    entry: &ToolCatalogEntry,
    plan: &ResolvedDownloadPlan,
    install_root: &Path,
) -> Result<(), MediaPmError> {
    for action in plan.per_os_actions.values() {
        let os_root = install_root.join(action.os.as_str());
        fs::create_dir_all(&os_root).map_err(|source| MediaPmError::Io {
            operation: format!(
                "creating internal launcher directory for '{}' ({})",
                entry.name,
                action.os.as_str()
            ),
            path: os_root.clone(),
            source,
        })?;

        let executable_path = os_root.join(entry.executable_name_for_os(action.os));
        let launcher_env_key = media_tagger_launcher_env_var(action.os);
        let content = if action.os == ToolOs::Windows {
            format!(
                concat!(
                    "@echo off\r\n",
                    "setlocal\r\n",
                    "if \"%{launcher_env_key}%\"==\"\" (\r\n",
                    "  echo internal media-tagger launcher requires %{launcher_env_key}% to be set>&2\r\n",
                    "  exit /b 1\r\n",
                    ")\r\n",
                    "\"%{launcher_env_key}%\" builtins media-tagger %*\r\n"
                ),
                launcher_env_key = launcher_env_key,
            )
        } else {
            format!(
                concat!(
                    "#!/usr/bin/env sh\n",
                    "if [ -z \"${launcher_env_key}\" ]; then\n",
                    "  printf '%s\\n' \"internal media-tagger launcher requires {launcher_env_key} to be set\" >&2\n",
                    "  exit 1\n",
                    "fi\n",
                    "exec \"${launcher_env_key}\" builtins media-tagger \"$@\"\n"
                ),
                launcher_env_key = launcher_env_key,
            )
        };

        write_binary_file(&executable_path, content.as_bytes())?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let metadata = fs::metadata(&executable_path).map_err(|source| MediaPmError::Io {
                operation: "reading internal launcher permissions".to_string(),
                path: executable_path.clone(),
                source,
            })?;
            let mut permissions = metadata.permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&executable_path, permissions).map_err(|source| {
                MediaPmError::Io {
                    operation: "setting internal launcher executable permissions".to_string(),
                    path: executable_path.clone(),
                    source,
                }
            })?;
        }
    }

    Ok(())
}

/// Returns internal media-tagger launcher env var key by target OS.
#[must_use]
fn media_tagger_launcher_env_var(os: ToolOs) -> &'static str {
    match os {
        ToolOs::Windows => "MEDIAPM_MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_WINDOWS",
        ToolOs::Linux => "MEDIAPM_MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_LINUX",
        ToolOs::Macos => "MEDIAPM_MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_MACOS",
    }
}

/// Resolves planned total download bytes across all per-OS actions.
///
/// Returns `None` when any action cannot provide `Content-Length` during
/// best-effort probing.
async fn planned_total_download_bytes(plan: &ResolvedDownloadPlan) -> Option<u64> {
    let mut total = Some(0_u64);

    for action in plan.per_os_actions.values() {
        let action_total = probe_content_length_from_candidates(&action.urls).await;
        total = match (total, action_total) {
            (Some(base), Some(current)) => Some(base.saturating_add(current)),
            _ => None,
        };

        if total.is_none() {
            break;
        }
    }

    total
}

/// Aggregates one action-local snapshot into multi-action cumulative progress.
fn aggregate_progress_snapshot(
    base_downloaded: u64,
    current_snapshot: DownloadProgressSnapshot,
    planned_total: Option<u64>,
) -> DownloadProgressSnapshot {
    let mut downloaded_bytes = base_downloaded.saturating_add(current_snapshot.downloaded_bytes);
    let total_bytes = planned_total;

    if let Some(total_bytes) = total_bytes {
        downloaded_bytes = downloaded_bytes.min(total_bytes);
    }

    DownloadProgressSnapshot { downloaded_bytes, total_bytes }
}

/// Downloads and materializes one OS payload into the destination root.
async fn materialize_one_os_payload(
    entry: &ToolCatalogEntry,
    action: &OsDownloadAction,
    destination_root: &Path,
    download_progress: Option<DownloadProgressCallback>,
    download_cache: Option<Arc<ToolDownloadCache>>,
    cache_identity: &str,
) -> Result<(), MediaPmError> {
    fs::create_dir_all(destination_root).map_err(|source| MediaPmError::Io {
        operation: format!(
            "creating tool payload directory for '{}' ({})",
            entry.name,
            action.os.as_str()
        ),
        path: destination_root.to_path_buf(),
        source,
    })?;

    let mode_label = match action.mode {
        crate::tools::catalog::DownloadPayloadMode::DirectBinary => "direct-binary",
        crate::tools::catalog::DownloadPayloadMode::ZipArchive => "zip-archive",
        crate::tools::catalog::DownloadPayloadMode::TarGzArchive => "tar-gz-archive",
        crate::tools::catalog::DownloadPayloadMode::TarXzArchive => "tar-xz-archive",
    };
    let cache_key = format!(
        "identity={cache_identity}|tool={}|os={}|mode={mode_label}|urls={}",
        entry.name,
        action.os.as_str(),
        action.urls.join("\n")
    );

    let (payload, _source_url) =
        fetch_bytes_from_candidates(&action.urls, download_progress, download_cache, cache_key)
            .await?;
    match action.mode {
        crate::tools::catalog::DownloadPayloadMode::DirectBinary => {
            let target = destination_root.join(entry.executable_name_for_os(action.os));
            write_binary_file(&target, &payload)?;
        }
        crate::tools::catalog::DownloadPayloadMode::ZipArchive => {
            extract_zip_payload(&payload, destination_root)?;
        }
        crate::tools::catalog::DownloadPayloadMode::TarGzArchive => {
            extract_tar_gz_payload(&payload, destination_root)?;
        }
        crate::tools::catalog::DownloadPayloadMode::TarXzArchive => {
            extract_tar_xz_payload(&payload, destination_root)?;
        }
    }

    Ok(())
}

/// Builds one stable cache-identity token for all actions in a resolved plan.
///
/// The token prefers immutable release selectors (`git_hash`, `version`, `tag`)
/// and falls back to source labels when explicit selectors are unavailable.
fn download_cache_identity(plan: &ResolvedDownloadPlan) -> String {
    let selector = plan
        .identity
        .git_hash
        .clone()
        .or_else(|| plan.identity.version.clone())
        .or_else(|| plan.identity.tag.clone())
        .unwrap_or_else(|| "unknown".to_string());

    format!("source={}+selector={selector}", plan.source_identifier)
}

/// Resolves executable relative paths from one install root.
pub(super) fn resolve_executable_paths(
    entry: &ToolCatalogEntry,
    plan: &ResolvedDownloadPlan,
    install_root: &Path,
) -> Result<BTreeMap<ToolOs, String>, MediaPmError> {
    let mut paths = BTreeMap::new();
    let target_oses = plan.per_os_actions.keys().copied().collect::<Vec<_>>();

    if plan.shared_package {
        for os in &target_oses {
            let executable_name = entry.executable_name_for_os(*os);
            let discovered = find_file_named(install_root, &executable_name).ok_or_else(|| {
                MediaPmError::Workflow(format!(
                    "shared payload for '{}' did not contain expected executable '{}' for {}",
                    entry.name,
                    executable_name,
                    os.as_str()
                ))
            })?;

            let relative = discovered.strip_prefix(install_root).map_err(|_| {
                MediaPmError::Workflow(format!(
                    "failed to derive executable relative path for '{}' ({})",
                    entry.name,
                    os.as_str()
                ))
            })?;

            paths.insert(*os, normalize_relative_path_text(relative));
        }
        return Ok(paths);
    }

    for os in target_oses {
        let os_root = install_root.join(os.as_str());
        let executable_name = entry.executable_name_for_os(os);
        let discovered = find_file_named(&os_root, &executable_name).ok_or_else(|| {
            MediaPmError::Workflow(format!(
                "payload for '{}' ({}) did not contain expected executable '{}'",
                entry.name,
                os.as_str(),
                executable_name
            ))
        })?;

        let relative = discovered.strip_prefix(install_root).map_err(|_| {
            MediaPmError::Workflow(format!(
                "failed to derive executable relative path for '{}' ({})",
                entry.name,
                os.as_str()
            ))
        })?;
        paths.insert(os, normalize_relative_path_text(relative));
    }

    Ok(paths)
}

/// Builds command-selector text from per-OS executable relative paths.
pub(super) fn build_command_selector(
    paths: &BTreeMap<ToolOs, String>,
) -> Result<String, MediaPmError> {
    if paths.is_empty() {
        return Err(MediaPmError::Workflow(
            "tool provisioning did not resolve any executable paths".to_string(),
        ));
    }

    if paths.len() == 1
        && let Some(path) = paths.values().next()
    {
        return Ok(path.clone());
    }

    let mut unique_paths = paths.values().collect::<Vec<_>>();
    unique_paths.sort_unstable();
    unique_paths.dedup();
    if unique_paths.len() == 1 {
        return Ok(unique_paths[0].clone());
    }

    let mut selector = String::new();
    for os in [ToolOs::Windows, ToolOs::Linux, ToolOs::Macos] {
        if let Some(path) = paths.get(&os) {
            let _ = write!(selector, "${{context.os == \"{}\" ? {path} | ''}}", os.as_str());
        }
    }

    if selector.is_empty() {
        return Err(MediaPmError::Workflow(
            "tool provisioning produced no OS selector expressions".to_string(),
        ));
    }

    Ok(selector)
}

/// Collects content-map payload sources from one materialized install root.
///
/// Archive payloads are represented as one directory key (for shared packages)
/// or one per-OS directory key, allowing conductor to unpack ZIP bytes without
/// storing one hash per extracted file.
pub(super) fn collect_materialized_content_entries(
    plan: &ResolvedDownloadPlan,
    root: &Path,
) -> Result<BTreeMap<String, ContentMapSource>, MediaPmError> {
    let archive_only = plan.per_os_actions.values().all(|action| is_archive_mode(action.mode));

    if archive_only {
        return Ok(collect_archive_directory_entries(plan, root));
    }

    collect_regular_file_entries(root)
}

/// Returns whether payload mode expands one archive into a directory tree.
#[must_use]
fn is_archive_mode(mode: crate::tools::catalog::DownloadPayloadMode) -> bool {
    matches!(
        mode,
        crate::tools::catalog::DownloadPayloadMode::ZipArchive
            | crate::tools::catalog::DownloadPayloadMode::TarGzArchive
            | crate::tools::catalog::DownloadPayloadMode::TarXzArchive
    )
}

/// Collects directory-form content-map entries for archive payloads.
fn collect_archive_directory_entries(
    plan: &ResolvedDownloadPlan,
    root: &Path,
) -> BTreeMap<String, ContentMapSource> {
    if plan.shared_package {
        return BTreeMap::from([(
            "./".to_string(),
            ContentMapSource::DirectoryZip { root_dir: root.to_path_buf() },
        )]);
    }

    let mut entries = BTreeMap::new();
    for action in plan.per_os_actions.values() {
        let key = format!("{}/", action.os.as_str());
        entries.insert(
            key,
            ContentMapSource::DirectoryZip { root_dir: root.join(action.os.as_str()) },
        );
    }

    entries
}

/// Collects all regular files in one install root as direct file entries.
fn collect_regular_file_entries(
    root: &Path,
) -> Result<BTreeMap<String, ContentMapSource>, MediaPmError> {
    let mut files = BTreeMap::new();
    let mut stack = vec![root.to_path_buf()];

    while let Some(next) = stack.pop() {
        let entries = fs::read_dir(&next).map_err(|source| MediaPmError::Io {
            operation: "enumerating materialized tool payload directory".to_string(),
            path: next.clone(),
            source,
        })?;

        for entry in entries {
            let entry = entry.map_err(|source| MediaPmError::Io {
                operation: "reading materialized tool payload directory entry".to_string(),
                path: next.clone(),
                source,
            })?;
            let path = entry.path();
            let ty = entry.file_type().map_err(|source| MediaPmError::Io {
                operation: "reading materialized tool payload entry type".to_string(),
                path: path.clone(),
                source,
            })?;

            if ty.is_dir() {
                stack.push(path);
                continue;
            }
            if !ty.is_file() {
                continue;
            }

            let relative = path.strip_prefix(root).map_err(|_| {
                MediaPmError::Workflow(format!(
                    "failed deriving relative payload path for '{}'",
                    path.display()
                ))
            })?;
            files.insert(normalize_relative_path_text(relative), ContentMapSource::FilePath(path));
        }
    }

    Ok(files)
}

/// Normalizes one relative path to slash-separated text.
fn normalize_relative_path_text(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

/// Extracts ZIP payload bytes into one destination directory.
fn extract_zip_payload(bytes: &[u8], destination: &Path) -> Result<(), MediaPmError> {
    let cursor = Cursor::new(bytes.to_vec());
    let mut archive = ZipArchive::new(cursor).map_err(|source| {
        MediaPmError::Workflow(format!("opening ZIP payload failed: {source}"))
    })?;

    for index in 0..archive.len() {
        let mut file = archive.by_index(index).map_err(|source| {
            MediaPmError::Workflow(format!("reading ZIP entry at index {index} failed: {source}"))
        })?;

        let Some(enclosed) = file.enclosed_name().as_deref().map(Path::to_path_buf) else {
            continue;
        };
        let output_path = destination.join(enclosed);

        if file.is_dir() {
            fs::create_dir_all(&output_path).map_err(|source| MediaPmError::Io {
                operation: "creating extracted ZIP directory".to_string(),
                path: output_path,
                source,
            })?;
            continue;
        }

        if let Some(parent) = output_path.parent() {
            fs::create_dir_all(parent).map_err(|source| MediaPmError::Io {
                operation: "creating extracted ZIP file parent directory".to_string(),
                path: parent.to_path_buf(),
                source,
            })?;
        }

        let mut out = fs::File::create(&output_path).map_err(|source| MediaPmError::Io {
            operation: "creating extracted ZIP file".to_string(),
            path: output_path.clone(),
            source,
        })?;
        std::io::copy(&mut file, &mut out).map_err(|source| {
            MediaPmError::Workflow(format!(
                "writing extracted ZIP payload to '{}' failed: {source}",
                output_path.display()
            ))
        })?;
    }

    Ok(())
}

/// Extracts TAR.XZ payload bytes into one destination directory.
fn extract_tar_xz_payload(bytes: &[u8], destination: &Path) -> Result<(), MediaPmError> {
    let decoder = XzDecoder::new(Cursor::new(bytes));
    let mut archive = TarArchive::new(decoder);
    let entries = archive.entries().map_err(|source| {
        MediaPmError::Workflow(format!("opening TAR.XZ payload entries failed: {source}"))
    })?;

    for entry_result in entries {
        let mut entry = entry_result.map_err(|source| {
            MediaPmError::Workflow(format!("reading TAR.XZ entry failed: {source}"))
        })?;

        let unpacked = entry.unpack_in(destination).map_err(|source| {
            MediaPmError::Workflow(format!(
                "extracting TAR.XZ payload into '{}' failed: {source}",
                destination.display()
            ))
        })?;

        if !unpacked {
            return Err(MediaPmError::Workflow(format!(
                "TAR.XZ payload entry resolved outside destination '{}'",
                destination.display()
            )));
        }
    }

    Ok(())
}

/// Extracts TAR.GZ payload bytes into one destination directory.
fn extract_tar_gz_payload(bytes: &[u8], destination: &Path) -> Result<(), MediaPmError> {
    let decoder = GzDecoder::new(Cursor::new(bytes));
    let mut archive = TarArchive::new(decoder);
    let entries = archive.entries().map_err(|source| {
        MediaPmError::Workflow(format!("opening TAR.GZ payload entries failed: {source}"))
    })?;

    for entry_result in entries {
        let mut entry = entry_result.map_err(|source| {
            MediaPmError::Workflow(format!("reading TAR.GZ entry failed: {source}"))
        })?;

        let unpacked = entry.unpack_in(destination).map_err(|source| {
            MediaPmError::Workflow(format!(
                "extracting TAR.GZ payload into '{}' failed: {source}",
                destination.display()
            ))
        })?;

        if !unpacked {
            return Err(MediaPmError::Workflow(format!(
                "TAR.GZ payload entry resolved outside destination '{}'",
                destination.display()
            )));
        }
    }

    Ok(())
}

/// Writes one downloaded binary and ensures executable permissions where needed.
fn write_binary_file(path: &Path, bytes: &[u8]) -> Result<(), MediaPmError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| MediaPmError::Io {
            operation: "creating binary output parent directory".to_string(),
            path: parent.to_path_buf(),
            source,
        })?;
    }

    let mut file = fs::File::create(path).map_err(|source| MediaPmError::Io {
        operation: "creating downloaded binary output file".to_string(),
        path: path.to_path_buf(),
        source,
    })?;
    file.write_all(bytes).map_err(|source| MediaPmError::Io {
        operation: "writing downloaded binary output bytes".to_string(),
        path: path.to_path_buf(),
        source,
    })?;

    ensure_executable_permissions(path)
}

/// Ensures executable permission bits are present on Unix-like platforms.
#[cfg_attr(
    not(unix),
    expect(
        clippy::unnecessary_wraps,
        reason = "on non-Unix hosts executable bit mutation is skipped by design"
    )
)]
fn ensure_executable_permissions(path: &Path) -> Result<(), MediaPmError> {
    #[cfg(not(unix))]
    let _ = path;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mut perms = fs::metadata(path)
            .map_err(|source| MediaPmError::Io {
                operation: "reading executable metadata for chmod".to_string(),
                path: path.to_path_buf(),
                source,
            })?
            .permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms).map_err(|source| MediaPmError::Io {
            operation: "setting executable permission bits".to_string(),
            path: path.to_path_buf(),
            source,
        })?;
    }

    Ok(())
}

/// Recursively finds the first file whose name matches `needle`.
fn find_file_named(root: &Path, needle: &str) -> Option<PathBuf> {
    let mut stack = vec![root.to_path_buf()];

    while let Some(next) = stack.pop() {
        let entries = fs::read_dir(&next).ok()?;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
                continue;
            };
            if cfg!(windows) {
                if name.eq_ignore_ascii_case(needle) {
                    return Some(path);
                }
            } else if name == needle {
                return Some(path);
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;
    use std::io::Cursor;

    use tar::{Builder as TarBuilder, Header as TarHeader};
    use tempfile::tempdir;
    use xz2::write::XzEncoder;

    use super::{
        aggregate_progress_snapshot, collect_materialized_content_entries, extract_tar_xz_payload,
        materialize_internal_launcher, media_tagger_launcher_env_var,
    };
    use crate::tools::catalog::{DownloadPayloadMode, ToolOs, tool_catalog_entry};
    use crate::tools::downloader::DownloadProgressSnapshot;
    use crate::tools::downloader::models::{
        OsDownloadAction, ResolvedDownloadPlan, ResolvedToolIdentity,
    };

    /// Protects Linux archive provisioning by ensuring TAR.XZ payloads can be
    /// extracted and discovered as regular files for executable resolution.
    #[test]
    fn extract_tar_xz_payload_materializes_expected_file_tree() {
        let mut tar_builder = TarBuilder::new(Vec::<u8>::new());

        let mut header = TarHeader::new_gnu();
        let payload = b"#!/bin/sh\necho ffmpeg\n";
        header.set_size(payload.len() as u64);
        header.set_mode(0o755);
        header.set_cksum();
        tar_builder
            .append_data(&mut header, "ffmpeg-master/bin/ffmpeg", Cursor::new(payload))
            .expect("append tar entry");

        let tar_bytes = tar_builder.into_inner().expect("finalize tar archive");
        let mut encoder = XzEncoder::new(Vec::new(), 6);
        std::io::Write::write_all(&mut encoder, &tar_bytes).expect("write xz payload");
        let tar_xz = encoder.finish().expect("finalize xz stream");

        let root = tempdir().expect("tempdir");
        extract_tar_xz_payload(&tar_xz, root.path()).expect("extract tar.xz payload");

        let extracted = root.path().join("ffmpeg-master").join("bin").join("ffmpeg");
        assert!(extracted.is_file(), "expected extracted ffmpeg file at {extracted:?}");
    }

    /// Protects aggregate progress rendering so precomputed totals remain
    /// stable and downloaded bytes are clamped to known total size.
    #[test]
    fn aggregate_progress_snapshot_prefers_planned_total_and_clamps_downloaded() {
        let snapshot = aggregate_progress_snapshot(
            180,
            DownloadProgressSnapshot { downloaded_bytes: 80, total_bytes: Some(120) },
            Some(240),
        );

        assert_eq!(snapshot.total_bytes, Some(240));
        assert_eq!(snapshot.downloaded_bytes, 240);
    }

    /// Protects non-growing total-byte semantics by keeping totals unknown
    /// when precompute probes cannot determine one stable aggregate size.
    #[test]
    fn aggregate_progress_snapshot_keeps_total_unknown_without_precomputed_size() {
        let snapshot = aggregate_progress_snapshot(
            64,
            DownloadProgressSnapshot { downloaded_bytes: 16, total_bytes: Some(32) },
            None,
        );

        assert_eq!(snapshot.total_bytes, None);
        assert_eq!(snapshot.downloaded_bytes, 80);
    }

    /// Protects Windows launcher reliability by ensuring generated scripts
    /// rely only on the platform-prefixed launcher env var.
    #[test]
    fn materialize_internal_launcher_uses_platform_prefixed_env_var() {
        let root = tempdir().expect("tempdir");

        let install_root = root
            .path()
            .join("runtime")
            .join(".mediapm")
            .join("tools")
            .join("mediapm.tools.media-tagger+mediapm-internal@latest");

        let mut per_os_actions = BTreeMap::new();
        per_os_actions.insert(
            ToolOs::Windows,
            OsDownloadAction {
                os: ToolOs::Windows,
                urls: Vec::new(),
                mode: DownloadPayloadMode::DirectBinary,
            },
        );
        let plan = ResolvedDownloadPlan {
            per_os_actions,
            shared_package: false,
            internal_launcher: true,
            common_executable_tool: None,
            identity: ResolvedToolIdentity::default(),
            source_label: "mediapm internal launcher".to_string(),
            source_identifier: "mediapm-internal".to_string(),
            warnings: Vec::new(),
        };

        materialize_internal_launcher(
            &tool_catalog_entry("media-tagger").expect("media-tagger entry"),
            &plan,
            &install_root,
        )
        .expect("materialize launcher");

        let script = fs::read_to_string(install_root.join("windows").join("media-tagger.cmd"))
            .expect("read launcher script");
        let launcher_env_key = media_tagger_launcher_env_var(ToolOs::Windows);
        assert!(
            script.contains(&format!("%{launcher_env_key}%")),
            "expected host launcher env var reference in launcher script"
        );
        assert!(!script.contains("cargo run"), "launcher must not depend on cargo fallback");
        assert!(!script.contains("where mediapm"), "launcher must not use ambient command lookup");
    }

    /// Verifies direct-binary materialization publishes one content-map entry
    /// per supported platform with normalized, sandbox-relative keys.
    #[test]
    fn collect_materialized_content_entries_emits_all_platform_keys_for_direct_binaries() {
        let root = tempdir().expect("tempdir");
        let install_root = root.path().join("tool-root");

        let mut per_os_actions = BTreeMap::new();
        for os in ToolOs::all() {
            per_os_actions.insert(
                os,
                OsDownloadAction { os, urls: Vec::new(), mode: DownloadPayloadMode::DirectBinary },
            );
        }

        let plan = ResolvedDownloadPlan {
            per_os_actions,
            shared_package: false,
            internal_launcher: false,
            common_executable_tool: None,
            identity: ResolvedToolIdentity::default(),
            source_label: "fixture direct binary".to_string(),
            source_identifier: "fixture-direct".to_string(),
            warnings: Vec::new(),
        };

        fs::create_dir_all(install_root.join("windows")).expect("create windows folder");
        fs::create_dir_all(install_root.join("linux")).expect("create linux folder");
        fs::create_dir_all(install_root.join("macos")).expect("create macos folder");
        fs::write(install_root.join("windows").join("sd.exe"), b"windows-sd")
            .expect("write windows binary");
        fs::write(install_root.join("linux").join("sd"), b"linux-sd").expect("write linux binary");
        fs::write(install_root.join("macos").join("sd"), b"macos-sd").expect("write macos binary");

        let _entry = tool_catalog_entry("sd").expect("sd entry");
        let content_entries =
            collect_materialized_content_entries(&plan, &install_root).expect("collect");

        let keys = content_entries.keys().map(std::string::String::as_str).collect::<Vec<_>>();

        assert!(keys.contains(&"windows/sd.exe"));
        assert!(keys.contains(&"linux/sd"));
        assert!(keys.contains(&"macos/sd"));
        for key in keys {
            assert!(
                !key.contains("..") && !key.starts_with('/'),
                "content-map key should be sandbox-relative and traversal-safe: {key}"
            );
        }
    }
}
