//! Shared scaffolding for integration tests.

use mediapm::{MediaPmService, MediaRuntimeStorage};

/// Creates a `MediaPmService` with a hermetic cache override: a fresh
/// artifact root plus a dedicated cache root, so sync never touches the real
/// OS user cache. The cache root is returned alongside the service so
/// callers can inspect the initialized cache files.
pub(super) async fn service_with_cache(
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
