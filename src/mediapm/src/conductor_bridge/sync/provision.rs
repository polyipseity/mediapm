//! Tool payload provisioning: download, extract, pack to ZIP, and import to CAS.
//!
//! This module provides [`fetch_and_import_tool_payload`] which handles the
//! full lifecycle for one tool: look up catalog → resolve download plan →
//! download → extract → pack to uncompressed ZIP → CAS import → content map.
//!
//! # All-platform download principle
//!
//! Tool payloads are downloaded for **all supported platforms** regardless of
//! the host OS. Each platform's archive is extracted to a separate
//! `{os}/` subdirectory. For archive formats (ZIP, tar.gz, tar.xz) the
//! extracted directory is repacked into a single uncompressed ZIP and
//! imported to CAS as one blob, producing a single trailing-slash entry
//! (`./{os}/` → ZIP hash). The [`ARCHIVE_BINARY`] format keeps a file-level
//! entry (`./{os}/{tool_id}` → binary hash). The command selector is emitted
//! as a `${context.os == "…" ? ./…/… : …}` template expression so the
//! conductor resolves the correct executable at runtime. The conductor's
//! [`link_to_sandbox`] then skips foreign-platform directories via
//! [`FOREIGN_PLATFORM_DIRS`], materialising only the host-native payload.

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use bytes::Bytes;
use mediapm_cas::CasApi;
use zip::write::SimpleFileOptions;

use crate::error::MediaPmError;
use crate::output::ProgressHandle;
use crate::tools::catalog::{ARCHIVE_BINARY, tool_catalog_entry};
use crate::tools::downloader::{
    ToolDownloadCache, extract_archive, fetch_bytes_from_candidates, resolve_download_plan,
};
use crate::tools::models::DownloadProgressCallback;

/// Result of fetching and importing a tool payload into CAS.
#[derive(Debug, Clone)]
pub(super) struct FetchedToolPayload {
    /// Content map: sandbox-relative path → CAS hash hex string.
    /// For archive formats the key is a trailing-slash directory entry
    /// (`./{os}/`); for [`ARCHIVE_BINARY`] it is a file-level entry
    /// (`./{os}/{tool_id}`).  The conductor's platform filtering resolves
    /// files through the unpacked directory tree.
    pub(super) content_map: BTreeMap<String, String>,
    /// Sandbox-relative path to the main executable, emitted as a
    /// `${context.os == "…" ? ./…/… : …}` template expression when multiple
    /// platforms are provisioned.
    pub(super) command_selector: String,
}

/// Fetches a tool payload for **all** platforms, extracts each to a
/// per-OS temp directory, imports files to CAS with `./{os}/` key prefixes,
/// and builds an OS-conditional command-selector template.
///
/// `progress_handle` is a [`ProgressHandle`] whose message, total, and
/// position are updated per-OS download to show per-tool progress.
///
/// Returns `Ok(None)` when the tool has no catalog entry or is an internal
/// launcher.
pub(super) async fn fetch_and_import_tool_payload(
    cas: &impl CasApi,
    tool_id: &str,
    cache: &ToolDownloadCache,
    progress_handle: &ProgressHandle,
) -> Result<Option<FetchedToolPayload>, MediaPmError> {
    let Some(entry) = tool_catalog_entry(tool_id) else {
        tracing::warn!("tool {tool_id}: no catalog entry found, skipping provisioning");
        return Ok(None);
    };

    let plan = resolve_download_plan(entry, cache).await.map_err(|e| {
        MediaPmError::Workflow(format!("tool {tool_id}: failed to resolve download plan: {e}"))
    })?;

    if plan.internal_launcher {
        return Ok(None);
    }

    if plan.per_os_actions.is_empty() {
        tracing::warn!("tool {tool_id}: no download actions defined, skipping provisioning");
        return Ok(None);
    }

    let temp_root = tempfile::tempdir().map_err(|source| MediaPmError::Io {
        operation: "creating temp directory for tool extraction".to_string(),
        path: PathBuf::new(),
        source,
    })?;

    let mut content_map: BTreeMap<String, String> = BTreeMap::new();
    // Maps OS label → executable path relative to that OS extraction root.
    let mut per_os_exec: BTreeMap<String, String> = BTreeMap::new();

    for (os, action) in &plan.per_os_actions {
        let os_label = os.as_str();
        progress_handle.set_message(os_label.to_string());

        // ── download (per-OS cache key) ────────────────────────────────
        let cache_key = format!("{}_{}_{}", entry.id, os_label, entry.latest);
        let bytes = if let Some(cached) = cache.lookup_bytes(&cache_key).await {
            cached
        } else {
            let cb: DownloadProgressCallback = {
                let pb = progress_handle.clone();
                std::sync::Arc::new(move |snap| {
                    if let Some(total) = snap.total_bytes {
                        pb.set_total(total);
                    }
                    pb.set_position(snap.downloaded_bytes);
                })
            };
            let downloaded =
                fetch_bytes_from_candidates(&action.urls, Some(cb)).await.map_err(|e| {
                    MediaPmError::Workflow(format!(
                        "tool {tool_id}: download failed for {os_label}: {e}"
                    ))
                })?;
            cache.store_bytes(&cache_key, &downloaded).await;
            downloaded
        };

        // ── extract, import, and find executable ───────────────────────
        let os_dir = temp_root.path().join(os_label);
        let (os_content_map, exec_path) = process_downloaded_archive(
            &bytes,
            action.archive_format,
            os_label,
            tool_id,
            &os_dir,
            cas,
        )
        .await?;
        content_map.extend(os_content_map);
        per_os_exec.insert(os_label.to_string(), exec_path);
        progress_handle.advance(1);
    }
    progress_handle.finish();

    // Build ${context.os == "..." ? ./.../... : ...} template.
    let command_selector = build_os_conditional_selector(&per_os_exec);

    Ok(Some(FetchedToolPayload { content_map, command_selector }))
}

/// Packs one directory tree into uncompressed ZIP bytes.
///
/// # Errors
///
/// Returns [`MediaPmError`] when directory reading or ZIP writing fails.
fn pack_directory_to_uncompressed_zip_bytes(dir: &Path) -> Result<Vec<u8>, MediaPmError> {
    let mut buf = Vec::new();
    {
        let mut writer = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
        let options =
            SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);

        pack_directory_entries(&mut writer, dir, dir, &options)?;

        writer
            .finish()
            .map_err(|e| MediaPmError::Workflow(format!("failed to finalize zip archive: {e}")))?;
    }
    Ok(buf)
}

/// Recursively adds directory entries to the zip writer.
fn pack_directory_entries(
    writer: &mut zip::ZipWriter<std::io::Cursor<&mut Vec<u8>>>,
    root: &Path,
    dir: &Path,
    options: &SimpleFileOptions,
) -> Result<(), MediaPmError> {
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

/// Searches for an executable named `{tool_id}` or `{tool_id}.exe` inside
/// `os_dir` and returns its path relative to `os_dir`. Returns `None` if
/// neither variant is found.
fn find_os_executable(os_dir: &Path, tool_id: &str) -> Option<String> {
    let candidates = [tool_id.to_string(), format!("{tool_id}.exe")];
    for name in &candidates {
        if let Some(rel) = find_file_relative(os_dir, os_dir, name) {
            return Some(rel.to_string_lossy().to_string());
        }
    }
    None
}

/// Recursively searches for a file with the given name, returning its path
/// relative to `root`.
fn find_file_relative(root: &Path, dir: &Path, target: &str) -> Option<PathBuf> {
    for entry in std::fs::read_dir(dir).ok()? {
        let entry = entry.ok()?;
        let path = entry.path();
        if path.is_dir() {
            if let found @ Some(_) = find_file_relative(root, &path, target) {
                return found;
            }
        } else if path.file_name().and_then(|n| n.to_str()) == Some(target) {
            return path.strip_prefix(root).ok().map(|p| p.to_path_buf());
        }
    }
    None
}

/// Processes one already-downloaded archive through extraction,
/// CAS import, and executable lookup for a single OS.
///
/// For archive formats (ZIP, tar.gz, tar.xz) the extracted directory is
/// repacked into a single uncompressed ZIP and imported to CAS, producing
/// a single trailing-slash content-map key (`./{os_label}/`).  For
/// [`ARCHIVE_BINARY`] the single binary is imported as a file-level entry
/// (`./{os_label}/{tool_id}`).
///
/// # Returns
///
/// A tuple `(content_map_entries, exec_path)` where:
/// - `content_map_entries` maps content-map keys → CAS hash hex strings.
/// - `exec_path` is the path to the executable relative to `os_dir`.
async fn process_downloaded_archive(
    bytes: &[u8],
    archive_format: &str,
    os_label: &str,
    tool_id: &str,
    os_dir: &Path,
    cas: &impl CasApi,
) -> Result<(BTreeMap<String, String>, String), MediaPmError> {
    std::fs::create_dir_all(os_dir).map_err(|source| MediaPmError::Io {
        operation: format!("creating temp directory for {os_label} tool extraction"),
        path: os_dir.to_path_buf(),
        source,
    })?;

    extract_archive(bytes, archive_format, os_dir)?;

    if archive_format == ARCHIVE_BINARY {
        // ── binary format: single file entry ────────────────────────
        let exe_name = if cfg!(target_os = "windows") { "tool.exe" } else { "tool" };
        let old_path = os_dir.join(exe_name);
        let tool_path = os_dir.join(tool_id);
        if old_path.exists() {
            std::fs::rename(&old_path, &tool_path).map_err(|source| MediaPmError::Io {
                operation: format!(
                    "renaming extracted binary from {exe_name} to {tool_id} for {os_label}"
                ),
                path: old_path,
                source,
            })?;
        }

        let file_bytes = std::fs::read(&tool_path).map_err(|source| MediaPmError::Io {
            operation: format!("reading binary '{tool_id}' for CAS import"),
            path: tool_path,
            source,
        })?;
        let hash = cas.put(Bytes::from(file_bytes)).await.map_err(|e| {
            MediaPmError::Workflow(format!("CAS put failed for binary '{tool_id}': {e}"))
        })?;
        let key = format!("{os_label}/{tool_id}");
        let mut content_map = BTreeMap::new();
        content_map.insert(key, hash.to_hex());
        Ok((content_map, tool_id.to_string()))
    } else {
        // ── archive format: pack OS directory, single dir entry ─────
        let exec_path = find_os_executable(os_dir, tool_id).unwrap_or_else(|| tool_id.to_string());

        let zip_bytes = pack_directory_to_uncompressed_zip_bytes(os_dir)?;
        let hash = cas.put(Bytes::from(zip_bytes)).await.map_err(|e| {
            MediaPmError::Workflow(format!(
                "CAS put failed for tool '{tool_id}' {os_label} zip: {e}"
            ))
        })?;
        let key = format!("{os_label}/");
        let mut content_map = BTreeMap::new();
        content_map.insert(key, hash.to_hex());
        Ok((content_map, exec_path))
    }
}

/// Builds a `${context.os == "linux" ? linux/sd : context.os == "macos" ? macos/sd : windows/sd}`
/// template string from the per-OS executable suffix map.
///
/// When only one OS is provisioned the template collapses to a plain path.
fn build_os_conditional_selector(per_os_exec: &BTreeMap<String, String>) -> String {
    if per_os_exec.is_empty() {
        return String::new();
    }
    let mut iter = per_os_exec.iter();
    let (first_os, first_path) = iter.next().expect("non-empty per_os_exec");
    if per_os_exec.len() == 1 {
        return format!("{first_os}/{first_path}");
    }
    let mut result = format!("${{context.os == \"{first_os}\" ? {first_os}/{first_path}");
    for (os, path) in iter.by_ref() {
        result.push_str(&format!(" : context.os == \"{os}\" ? {os}/{path}"));
    }
    result.push('}');
    result
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use mediapm_cas::InMemoryCas;

    use super::*;

    // ── Synthetic archive helpers ─────────────────────────────────────

    fn synthetic_zip(entries: &[(&str, &[u8])]) -> Vec<u8> {
        use zip::write::SimpleFileOptions;
        let cursor = std::io::Cursor::new(Vec::new());
        let mut writer = zip::ZipWriter::new(cursor);
        let options = SimpleFileOptions::default();
        for (name, content) in entries {
            writer.start_file(*name, options.clone()).unwrap();
            writer.write_all(content).unwrap();
        }
        let cursor = writer.finish().unwrap();
        cursor.into_inner()
    }

    fn synthetic_tar_gz(entries: &[(&str, &[u8])]) -> Vec<u8> {
        use flate2::Compression;
        use flate2::write::GzEncoder;
        let buf = Vec::new();
        let encoder = GzEncoder::new(buf, Compression::fast());
        let mut tar = tar::Builder::new(encoder);
        for (name, content) in entries {
            let mut header = tar::Header::new_gnu();
            header.set_path(name).unwrap();
            header.set_size(content.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            tar.append(&header, *content).unwrap();
        }
        let encoder = tar.into_inner().unwrap();
        encoder.finish().unwrap()
    }

    fn synthetic_tar_xz(entries: &[(&str, &[u8])]) -> Vec<u8> {
        use xz2::write::XzEncoder;
        let buf = Vec::new();
        let encoder = XzEncoder::new(buf, 6);
        let mut tar = tar::Builder::new(encoder);
        for (name, content) in entries {
            let mut header = tar::Header::new_gnu();
            header.set_path(name).unwrap();
            header.set_size(content.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            tar.append(&header, *content).unwrap();
        }
        let encoder = tar.into_inner().unwrap();
        encoder.finish().unwrap()
    }

    const EXEC_BYTES: &[u8] = b"#!/bin/sh\necho mocked\n";

    // ── build_os_conditional_selector ─────────────────────────────────

    #[test]
    fn selector_empty_map_returns_empty_string() {
        assert_eq!(build_os_conditional_selector(&BTreeMap::new()), "");
    }

    #[test]
    fn selector_single_os_returns_plain_path() {
        let mut map: BTreeMap<String, String> = BTreeMap::new();
        map.insert("linux".into(), "sd".into());
        assert_eq!(build_os_conditional_selector(&map), "linux/sd");
    }

    #[test]
    fn selector_two_oses_produces_template() {
        let mut map: BTreeMap<String, String> = BTreeMap::new();
        map.insert("linux".into(), "sd".into());
        map.insert("macos".into(), "sd".into());
        let result = build_os_conditional_selector(&map);
        assert!(result.starts_with("${context.os == \""));
        assert!(result.contains("linux/sd"));
        assert!(result.contains("macos/sd"));
        assert!(result.ends_with('}'));
        // The first condition should be Windows-free: macOS sorts after linux
        assert!(!result.contains("windows/sd"));
    }

    #[test]
    fn selector_three_oses_produces_full_template() {
        let mut map: BTreeMap<String, String> = BTreeMap::new();
        map.insert("linux".into(), "sd".into());
        map.insert("macos".into(), "sd".into());
        map.insert("windows".into(), "sd.exe".into());
        let result = build_os_conditional_selector(&map);
        assert!(result.contains("linux/sd"));
        assert!(result.contains("macos/sd"));
        assert!(result.contains("windows/sd.exe"));
        // linux first, then macos, then windows (BTreeMap order)
        assert!(result.starts_with("${context.os == \"linux\""));
    }

    #[test]
    fn selector_with_subdir_paths() {
        let mut map: BTreeMap<String, String> = BTreeMap::new();
        map.insert("linux".into(), "bin/sd".into());
        let result = build_os_conditional_selector(&map);
        assert_eq!(result, "linux/bin/sd");
    }

    // ── find_file_relative ────────────────────────────────────────────

    #[test]
    fn find_file_at_root() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("sd");
        std::fs::write(&file_path, "").unwrap();
        assert_eq!(find_file_relative(dir.path(), dir.path(), "sd"), Some(PathBuf::from("sd")));
    }

    #[test]
    fn find_file_in_nested_dir() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("bin");
        std::fs::create_dir(&sub).unwrap();
        std::fs::write(sub.join("sd"), "").unwrap();
        assert_eq!(find_file_relative(dir.path(), dir.path(), "sd"), Some(PathBuf::from("bin/sd")));
    }

    #[test]
    fn find_file_absent_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        assert!(find_file_relative(dir.path(), dir.path(), "nonexistent").is_none());
    }

    #[test]
    fn find_file_skips_directories() {
        let dir = tempfile::tempdir().unwrap();
        let nested = dir.path().join("sd");
        std::fs::create_dir(&nested).unwrap();
        std::fs::write(nested.join("other"), "").unwrap();
        // "sd" is a directory, not a file
        assert!(find_file_relative(dir.path(), dir.path(), "sd").is_none());
    }

    // ── find_os_executable ────────────────────────────────────────────

    #[test]
    fn find_os_exec_direct_match() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("sd"), "").unwrap();
        assert_eq!(find_os_executable(dir.path(), "sd"), Some("sd".into()));
    }

    #[test]
    fn find_os_exec_finds_exe_variant() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("sd.exe"), "").unwrap();
        assert_eq!(find_os_executable(dir.path(), "sd"), Some("sd.exe".into()));
    }

    #[test]
    fn find_os_exec_finds_nested() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("bin");
        std::fs::create_dir(&sub).unwrap();
        std::fs::write(sub.join("sd"), "").unwrap();
        assert_eq!(find_os_executable(dir.path(), "sd"), Some("bin/sd".into()));
    }

    #[test]
    fn find_os_exec_not_found_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        assert!(find_os_executable(dir.path(), "nonexistent").is_none());
    }

    // ── process_downloaded_archive (with InMemoryCas) ─────────────────

    #[tokio::test]
    async fn process_zip_archive_linux_label() {
        let zip = synthetic_zip(&[("sd", EXEC_BYTES)]);
        let cas = InMemoryCas::default();
        let os_dir = tempfile::tempdir().unwrap();
        let (cmap, exec) = process_downloaded_archive(
            &zip,
            crate::tools::catalog::ARCHIVE_ZIP,
            "linux",
            "sd",
            os_dir.path(),
            &cas,
        )
        .await
        .unwrap();
        assert_eq!(cmap.len(), 1);
        assert!(cmap.contains_key("linux/"));
        assert_eq!(exec, "sd");
    }

    #[tokio::test]
    async fn process_tar_gz_archive_macos_label() {
        let tgz = synthetic_tar_gz(&[("sd", EXEC_BYTES)]);
        let cas = InMemoryCas::default();
        let os_dir = tempfile::tempdir().unwrap();
        let (cmap, exec) = process_downloaded_archive(
            &tgz,
            crate::tools::catalog::ARCHIVE_TAR_GZ,
            "macos",
            "sd",
            os_dir.path(),
            &cas,
        )
        .await
        .unwrap();
        assert_eq!(cmap.len(), 1);
        assert!(cmap.contains_key("macos/"));
        assert_eq!(exec, "sd");
    }

    #[tokio::test]
    async fn process_tar_xz_archive_windows_label() {
        let txz = synthetic_tar_xz(&[("sd.exe", EXEC_BYTES)]);
        let cas = InMemoryCas::default();
        let os_dir = tempfile::tempdir().unwrap();
        let (cmap, exec) = process_downloaded_archive(
            &txz,
            crate::tools::catalog::ARCHIVE_TAR_XZ,
            "windows",
            "sd",
            os_dir.path(),
            &cas,
        )
        .await
        .unwrap();
        assert_eq!(cmap.len(), 1);
        assert!(cmap.contains_key("windows/"));
        assert_eq!(exec, "sd.exe");
    }

    #[tokio::test]
    async fn process_binary_format_renames_tool() {
        use crate::tools::catalog::ARCHIVE_BINARY;
        let cas = InMemoryCas::default();
        let os_dir = tempfile::tempdir().unwrap();
        let (cmap, exec) = process_downloaded_archive(
            EXEC_BYTES,
            ARCHIVE_BINARY,
            "linux",
            "sd",
            os_dir.path(),
            &cas,
        )
        .await
        .unwrap();
        // ARCHIVE_BINARY writes bytes as "tool" (non-Windows) / "tool.exe" (Windows)
        // then renames to tool_id ("sd"). The walk finds the renamed file.
        assert!(cmap.contains_key("linux/sd"));
        assert_eq!(exec, "sd");
    }

    #[tokio::test]
    async fn process_archive_with_nested_directories() {
        let zip = synthetic_zip(&[("bin/sd", EXEC_BYTES)]);
        let cas = InMemoryCas::default();
        let os_dir = tempfile::tempdir().unwrap();
        let (cmap, exec) = process_downloaded_archive(
            &zip,
            crate::tools::catalog::ARCHIVE_ZIP,
            "linux",
            "sd",
            os_dir.path(),
            &cas,
        )
        .await
        .unwrap();
        assert!(cmap.contains_key("linux/"));
        assert_eq!(exec, "bin/sd");
    }

    /// Verifies that each OS label produces the correct trailing-slash content-map key.
    #[tokio::test]
    async fn process_all_three_os_labels_independently() {
        let zip = synthetic_zip(&[("sd", EXEC_BYTES)]);
        let cas = InMemoryCas::default();

        for os_label in ["linux", "macos", "windows"] {
            let os_dir = tempfile::tempdir().unwrap();
            let (cmap, exec) = process_downloaded_archive(
                &zip,
                crate::tools::catalog::ARCHIVE_ZIP,
                os_label,
                "sd",
                os_dir.path(),
                &cas,
            )
            .await
            .unwrap();
            let expected_key = format!("{}/", os_label);
            assert!(
                cmap.contains_key(&expected_key),
                "missing key '{expected_key}' for os '{os_label}'"
            );
            assert_eq!(exec, "sd", "exec mismatch for os '{os_label}'");
        }
    }

    /// A multi-file archive (3 files) produces exactly one trailing-slash entry.
    #[tokio::test]
    async fn process_multi_file_archive_produces_one_entry() {
        let zip = synthetic_zip(&[
            ("bin/sd", EXEC_BYTES),
            ("config/default.toml", b"setting=true\n"),
            ("share/help.txt", b"usage\n"),
        ]);
        let cas = InMemoryCas::default();
        let os_dir = tempfile::tempdir().unwrap();
        let (cmap, exec) = process_downloaded_archive(
            &zip,
            crate::tools::catalog::ARCHIVE_ZIP,
            "linux",
            "sd",
            os_dir.path(),
            &cas,
        )
        .await
        .unwrap();
        assert_eq!(cmap.len(), 1);
        assert!(cmap.contains_key("linux/"), "expected single directory entry");
        assert_eq!(exec, "bin/sd");
    }

    /// An empty directory produces a valid ZIP with a single trailing-slash entry.
    #[tokio::test]
    async fn process_empty_archive_produces_valid_zip_entry() {
        let zip = synthetic_zip(&[]);
        let cas = InMemoryCas::default();
        let os_dir = tempfile::tempdir().unwrap();
        let (cmap, exec) = process_downloaded_archive(
            &zip,
            crate::tools::catalog::ARCHIVE_ZIP,
            "linux",
            "sd",
            os_dir.path(),
            &cas,
        )
        .await
        .unwrap();
        assert_eq!(cmap.len(), 1);
        assert!(cmap.contains_key("linux/"), "expected directory entry for empty archive");
        assert_eq!(exec, "sd");
    }

    /// Each tool content map has exactly one trailing-slash directory entry.
    #[tokio::test]
    async fn process_two_tools_each_has_single_dir_entry() {
        let zip_a = synthetic_zip(&[("alpha", EXEC_BYTES)]);
        let zip_b = synthetic_zip(&[("beta", b"#!/bin/sh\necho other\n")]);
        let cas = InMemoryCas::default();

        let dir_a = tempfile::tempdir().unwrap();
        let dir_b = tempfile::tempdir().unwrap();

        let (cmap_a, _) = process_downloaded_archive(
            &zip_a,
            crate::tools::catalog::ARCHIVE_ZIP,
            "linux",
            "tool-a",
            dir_a.path(),
            &cas,
        )
        .await
        .unwrap();
        let (cmap_b, _) = process_downloaded_archive(
            &zip_b,
            crate::tools::catalog::ARCHIVE_ZIP,
            "linux",
            "tool-b",
            dir_b.path(),
            &cas,
        )
        .await
        .unwrap();

        // Each tool map has exactly one key: "linux/"
        assert_eq!(cmap_a.len(), 1);
        assert!(cmap_a.contains_key("linux/"));
        assert_eq!(cmap_b.len(), 1);
        assert!(cmap_b.contains_key("linux/"));
    }
}
