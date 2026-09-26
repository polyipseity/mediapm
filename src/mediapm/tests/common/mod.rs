//! Shared scaffolding for integration tests.

use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use indicatif::{InMemoryTerm, MultiProgress, ProgressDrawTarget};
use mediapm::{MediaPmService, MediaRuntimeStorage};
use mediapm_cas::CasApi;
use mediapm_conductor::{NickelDocument, decode_document};
use mediapm_utils::progress::{DimensionSource, ProgressTerminal, TestDimensionSource};
use zip::write::FileOptions;

/// Terminal height of a test-drawn terminal, in rows.
const TEST_TERM_ROWS: u16 = 24;
/// Terminal width of a test-drawn terminal, in columns.
const TEST_TERM_COLS: u16 = 80;

/// Builds a live [`ProgressTerminal`] whose draw target is an in-memory grid.
///
/// The default build targets the process's real stderr, so a test that lets a
/// sync open its own terminal paints progress frames into the test log. This
/// terminal keeps the production draw path — write gate, layout, style, join —
/// but routes every byte into an [`InMemoryTerm`] the caller can inspect or
/// drop. Pre-roll capture is given a second in-memory term because the default
/// swaps only the pre-roll term and would still write to stderr.
pub(crate) fn test_progress_terminal() -> ProgressTerminal {
    let grid = InMemoryTerm::new(TEST_TERM_ROWS, TEST_TERM_COLS);
    let dims = Arc::new(TestDimensionSource::new((TEST_TERM_ROWS, TEST_TERM_COLS)));
    ProgressTerminal::builder()
        .with_multi_progress(MultiProgress::with_draw_target(ProgressDrawTarget::term_like(
            Box::new(grid),
        )))
        .with_dim_source(Arc::clone(&dims) as Arc<dyn DimensionSource>)
        .with_pre_roll_capture(Box::new(InMemoryTerm::new(TEST_TERM_ROWS, TEST_TERM_COLS)))
        .with_ticker_enabled(false)
        .capacity(TEST_TERM_ROWS as usize)
        .build()
}

/// Builds a [`SyncProgressOverrides`] whose terminal is an in-memory grid.
///
/// Passed to [`MediaPmService::sync_tools_with_progress_overrides`] by tests
/// that call the tool sync directly. The sync still builds its screen, writes
/// its frames and joins it — the frames just land in a [`InMemoryTerm`]
/// instead of the test log, so the assertion set around the call is untouched
/// while the log stays clean.
pub(crate) fn test_sync_progress_overrides() -> mediapm::SyncProgressOverrides {
    mediapm::SyncProgressOverrides { terminal: Some(test_progress_terminal()), no_progress: false }
}

/// Runs a library sync with the given verification flag through an injected
/// in-memory terminal.
///
/// Equivalent to [`MediaPmService::sync_library`] (same options, no observer)
/// except that the sync renders into a memory grid instead of the real stderr,
/// so its frames never reach the test log. The sync still runs every phase.
pub(crate) async fn sync_library_with_test_terminal(
    service: &mut MediaPmService<mediapm_cas::FileSystemCas>,
    verify_materialization: bool,
) -> Result<mediapm::SyncSummary, mediapm::MediaPmError> {
    service
        .sync_library_with_progress_overrides(
            mediapm::SyncLibraryOptions {
                verify_materialization,
                ..mediapm::SyncLibraryOptions::default()
            },
            mediapm::SyncProgressOverrides {
                terminal: Some(test_progress_terminal()),
                no_progress: false,
            },
        )
        .await
}

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

/// Creates a filesystem service rooted at a fresh temp artifact dir.
///
/// The returned `TempDir` must be kept alive alongside the service (dropping
/// it deletes the workspace); config-only tests can bind it to `_root`.
pub(crate) async fn service_in_tempdir()
-> Result<(MediaPmService<mediapm_cas::FileSystemCas>, tempfile::TempDir), mediapm::MediaPmError> {
    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    let service = MediaPmService::new_fs_at(root.path()).await?;
    Ok((service, root))
}

/// Creates a filesystem service whose tool download cache lives inside the
/// workspace root, so parallel tests never contend on the OS-level cache.
/// `hierarchy_root_dir` optionally points the hierarchy materializer at a
/// subdirectory of the artifact root (mirrors the demo-online layout).
pub(crate) async fn service_at(
    root: &Path,
    _hierarchy_root_dir: Option<&str>,
) -> Result<MediaPmService<mediapm_cas::FileSystemCas>, mediapm::MediaPmError> {
    let runtime_storage = MediaRuntimeStorage {
        cache_root_override: Some(root.join("tool-cache")),
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

/// Writes `payload` into the service's CAS and returns the content hash,
/// mapping CAS errors to a `Workflow` error carrying `label`.
pub(crate) async fn seed_cas(
    service: &MediaPmService<mediapm_cas::FileSystemCas>,
    payload: Bytes,
    label: &str,
) -> Result<mediapm_cas::Hash, mediapm::MediaPmError> {
    let cas = service.conductor().cas().clone();
    cas.put(payload)
        .await
        .map_err(|e| mediapm::MediaPmError::Workflow(format!("seed {label}: {e}")))
}

/// Reads and decodes the machine-generated conductor document after a sync.
pub(crate) fn read_generated_doc(
    service: &MediaPmService<mediapm_cas::FileSystemCas>,
) -> NickelDocument {
    let bytes = std::fs::read(&service.paths().conductor_generated_ncl)
        .expect("conductor.generated.ncl should be readable");
    decode_document(&bytes).expect("valid Nickel document")
}
