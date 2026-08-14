//! Shared scaffolding for integration tests.

use std::io::Write;
use std::path::Path;

use mediapm::{MediaPmService, MediaRuntimeStorage};
use zip::write::FileOptions;

/// Loads a `MediaPmDocument` from a persisted `mediapm.ncl` file.
pub(crate) fn read_doc(path: &Path) -> mediapm::MediaPmDocument {
    mediapm::load_mediapm_document(path).expect("mediapm.ncl should load")
}

/// Creates a `MediaPmService` with a hermetic cache override: a fresh
/// artifact root plus a dedicated cache root, so sync never touches the real
/// OS user cache. The cache root is returned alongside the service so
/// callers can inspect the initialized cache files.
pub(crate) async fn service_with_cache(
    mut runtime: MediaRuntimeStorage,
) -> Result<
    (MediaPmService<mediapm_cas::FileSystemCas>, tempfile::TempDir, tempfile::TempDir),
    mediapm::MediaPmError,
> {
    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    let cache_root = mediapm_utils::temp::cache_dir().expect("cache tempdir");
    runtime.cache_root_override = Some(cache_root.path().to_path_buf());
    let service =
        MediaPmService::new_fs_at_with_runtime_storage_overrides(root.path(), runtime).await?;
    Ok((service, root, cache_root))
}

/// Creates a filesystem service whose tool download cache lives inside the
/// workspace root, so parallel tests never contend on the OS-level cache.
/// `hierarchy_root_dir` optionally points the hierarchy materializer at a
/// subdirectory of the artifact root (mirrors the demo-online layout).
pub(crate) async fn service_at(
    root: &Path,
    hierarchy_root_dir: Option<&str>,
) -> Result<MediaPmService<mediapm_cas::FileSystemCas>, mediapm::MediaPmError> {
    let runtime_storage = MediaRuntimeStorage {
        cache_root_override: Some(root.join("tool-cache")),
        hierarchy_root_dir: hierarchy_root_dir.map(str::to_string),
        ..MediaRuntimeStorage::default()
    };
    MediaPmService::new_fs_at_with_runtime_storage_overrides(root, runtime_storage).await
}

/// Builds an in-memory zip archive from name/content pairs.
pub(crate) fn make_zip(entries: &[(&str, &[u8])]) -> Vec<u8> {
    let mut buffer = std::io::Cursor::new(Vec::new());
    let mut zip = zip::ZipWriter::new(&mut buffer);
    for (name, data) in entries {
        zip.start_file::<&str, ()>(*name, FileOptions::default()).expect("zip entry");
        zip.write_all(data).expect("zip bytes");
    }
    zip.finish().expect("zip finish");
    buffer.into_inner()
}
