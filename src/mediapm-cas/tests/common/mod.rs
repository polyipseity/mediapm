//! Shared test utilities for `mediapm-cas` integration tests.

use std::ops::Deref;
use std::time::Duration;

use bytes::Bytes;

use mediapm_cas::api::CasApi;

/// Puts static bytes into `cas` and returns the content-addressable hash.
pub(crate) async fn put_static(cas: &impl CasApi, data: &'static [u8]) -> mediapm_cas::Hash {
    cas.put(Bytes::from_static(data)).await.unwrap()
}

/// Creates a fresh artifact tempdir (RAII guard dropped at end of test).
pub(crate) fn artifact_dir() -> mediapm_utils::temp::ManagedTempDir {
    mediapm_utils::temp::artifact_dir().unwrap()
}

/// A file-system CAS together with the temp directory that owns it.
///
/// The field order is the contract, not a convention: Rust drops struct fields
/// in declaration order, so `cas` is dropped — and dropping the last handle
/// cancels the background maintenance task and closes the store's mutation
/// gate — *before* `dir` removes the tree. Returning a
/// `(TempDir, FileSystemCas)` tuple instead would make that ordering something
/// every call site has to remember, and getting it backwards resurrects the
/// tree from a write the aborted task had already dispatched, leaking a
/// `$TMPDIR/mediapm-artifact-*` directory holding one orphaned object.
///
/// Call sites use the fixture as the store: it derefs to
/// [`FileSystemCas`](mediapm_cas::FileSystemCas), so `cas.put(..)` and
/// `put_static(&cas, ..)` work unchanged.
pub(crate) struct FileCasFixture {
    /// Declared first so it drops first: the store must be quiesced before the
    /// directory it writes into disappears.
    cas: mediapm_cas::FileSystemCas,
    /// The artifact root the store was opened on. Drops after `cas`. Held for
    /// its `Drop` alone — the leading underscore says the value is never read.
    _dir: mediapm_utils::temp::ManagedTempDir,
}

impl Deref for FileCasFixture {
    type Target = mediapm_cas::FileSystemCas;

    fn deref(&self) -> &Self::Target {
        &self.cas
    }
}

/// Creates a fresh artifact tempdir and opens a `FileSystemCas` on it.
///
/// Returns the fixture so the directory outlives every operation performed
/// during the test, and is removed only after the store is quiesced.
pub(crate) async fn open_file_cas() -> FileCasFixture {
    let dir = artifact_dir();
    let cas = mediapm_cas::FileSystemCas::open(dir.path()).await.unwrap();
    FileCasFixture { cas, _dir: dir }
}

impl FileCasFixture {
    /// Drops the store and opens a new one on the same directory.
    ///
    /// A test that asserts what survives a restart needs the store's findings to
    /// come from disk, not from the memory the first handle held, and a second
    /// handle on the same root would hit the store lock. The directory outlives
    /// both handles, so it is the fixture that reopens and not the caller.
    pub(crate) async fn reopen(self) -> FileCasFixture {
        let FileCasFixture { cas, _dir: dir } = self;
        drop(cas);
        let cas = mediapm_cas::FileSystemCas::open(dir.path()).await.unwrap();
        FileCasFixture { cas, _dir: dir }
    }
}

/// Creates a fresh artifact tempdir and opens a `FileSystemCas` whose background
/// WAL consumer is stopped before the caller writes anything.
///
/// # Why a test that counts consumed entries needs this
///
/// [`open_file_cas`] arms the store's background consumer, which waits 500 ms and
/// then drains the WAL on its own. A test that calls `run_wal_consumer` itself
/// and asserts on the returned count is then racing that consumer for the same
/// entries: whichever call reaches the store's `consume_lock` first claims the
/// whole range, and the loser reports a short count. `consume_lock` makes replay
/// exclusive but says nothing about *who* drains the range, so the background
/// consumer wins the race whenever it wakes up before the foreground call does
/// — which is any time the test has been running for 500 ms.
///
/// Stopping the consumer makes the foreground call the only consumer, so the
/// count is a property of the code under test rather than of scheduling.
///
/// The stop is `BackgroundMaintenanceGuard::shutdown`, awaited: it sets the
/// cooperative flag, aborts the task, and awaits the handle, so the task has
/// observably stopped rather than merely been asked to. It runs here — before
/// the first `put`, on a WAL that is still empty — so the consumer cannot have
/// claimed part of a range even if its task was already inside
/// `run_wal_consumer` when the abort landed. `FileSystemCas::close` is
/// deliberately not used: it also closes the store's mutation gate, after which
/// every leased write fails with `BrokenPipe`.
pub(crate) async fn open_file_cas_with_quiesced_consumer() -> FileCasFixture {
    let cas = open_file_cas().await;
    cas.bg_guard_ref().shutdown().await;
    cas
}

/// Creates a fresh artifact tempdir and opens a `FileSystemCas` whose
/// background WAL consumer runs every `bg_interval`.
pub(crate) async fn open_file_cas_with_background(bg_interval: Duration) -> FileCasFixture {
    let dir = artifact_dir();
    let cas = mediapm_cas::FileSystemCas::open_with_strategies_and_interval(
        dir.path(),
        vec![],
        bg_interval,
    )
    .await
    .unwrap();
    FileCasFixture { cas, _dir: dir }
}

/// Creates a fresh artifact tempdir and opens a `FileSystemCas` that verifies
/// every payload it reads.
///
/// [`open_file_cas`] passes no strategies, so a payload corrupted on disk reads
/// back silently and a test of corruption behaviour would assert nothing. This
/// opens the store the way a configuration with `verify_on_read` does.
pub(crate) async fn open_file_cas_verifying() -> FileCasFixture {
    let dir = artifact_dir();
    let cas = mediapm_cas::FileSystemCas::open_with_strategies(
        dir.path(),
        vec![mediapm_cas::VerifyTriggerStrategy::Always],
    )
    .await
    .unwrap();
    FileCasFixture { cas, _dir: dir }
}

/// Creates a fresh artifact tempdir and opens a `FileSystemCas` that verifies
/// a payload only when its on-disk mtime has changed.
///
/// This is the regression surface for form-aware mtime checks: the evaluator
/// must compare against the physical form being read, not an unrelated one.
pub(crate) async fn open_file_cas_modified() -> FileCasFixture {
    let dir = artifact_dir();
    let cas = mediapm_cas::FileSystemCas::open_with_strategies(
        dir.path(),
        vec![mediapm_cas::VerifyTriggerStrategy::Modified],
    )
    .await
    .unwrap();
    FileCasFixture { cas, _dir: dir }
}

/// Open a verifying file-system CAS over a directory the caller owns.
///
/// [`open_file_cas_verifying`] returns a fixture that owns its directory, which
/// a restart test cannot use: it needs to drop one store and open another over
/// the same root. The caller keeps the directory, so it outlives every handle.
pub(crate) async fn open_file_cas_verifying_at(
    root: &std::path::Path,
) -> mediapm_cas::FileSystemCas {
    mediapm_cas::FileSystemCas::open_with_strategies(
        root,
        vec![mediapm_cas::VerifyTriggerStrategy::Always],
    )
    .await
    .unwrap()
}

/// The full-blob path and the delta-blob path for one hash.
pub(crate) fn form_paths(
    cas: &mediapm_cas::FileSystemCas,
    hash: mediapm_cas::Hash,
) -> (std::path::PathBuf, std::path::PathBuf) {
    let full = cas.object_path_for_hash(hash).expect("a file-system store exposes object paths");
    let mut delta = full.clone();
    delta.set_extension(full.extension().map_or_else(
        || std::ffi::OsString::from("diff"),
        |extension| {
            let mut suffix = extension.to_os_string();
            suffix.push(".diff");
            suffix
        },
    ));
    (full, delta)
}

/// Two similar buffers with one marker replaced, large enough that VCDIFF finds
/// a payload worth storing as a delta.
pub(crate) fn similar(fill: u8, marker: &[u8]) -> Vec<u8> {
    let mut buffer = vec![fill; 4096];
    let start = buffer.len() - marker.len();
    buffer[start..].copy_from_slice(marker);
    buffer
}

/// Overwrite a committed payload with bytes that cannot be the right content.
///
/// Objects are committed read-only, so the write bit has to come back before the
/// file can be made wrong, which is exactly what an external tamper does.
/// `Permissions::set_readonly(false)` is the cross-platform spelling: it clears
/// the Windows attribute and grants the write bits on Unix, and the file it is
/// applied to lives in a temp directory either way.
///
/// A file that is not there is created corrupt: an interrupted promotion can
/// leave the full payload absent, and a test that stages that state must not
/// need a second helper to describe it.
pub(crate) fn corrupt(path: &std::path::Path) {
    match std::fs::metadata(path) {
        Ok(metadata) => {
            let mut permissions = metadata.permissions();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                permissions.set_mode(permissions.mode() | 0o200);
            }
            #[cfg(windows)]
            {
                #[expect(
                    clippy::permissions_set_readonly_false,
                    reason = "clearing the read-only attribute is how a Windows test tampers with a committed blob"
                )]
                permissions.set_readonly(false);
            }
            std::fs::set_permissions(path, permissions).expect("make the payload writable");
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => panic!("payload metadata: {error}"),
    }
    std::fs::write(path, b"not the bytes this hash names").expect("corrupt the payload");
}

/// Clears the read-only attribute a committed object carries.
///
/// Test setup sometimes has to stage a corrupt or legacy file in place of a
/// committed object. That is what a tamper does, and the attribute has to come
/// back before the write can land.
pub(crate) fn make_writable(path: &std::path::Path) {
    let metadata = std::fs::metadata(path).expect("payload metadata");
    let mut permissions = metadata.permissions();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        permissions.set_mode(permissions.mode() | 0o200);
    }
    #[cfg(windows)]
    {
        #[expect(
            clippy::permissions_set_readonly_false,
            reason = "clearing the read-only attribute is how a Windows test tampers with a committed blob"
        )]
        permissions.set_readonly(false);
    }
    std::fs::set_permissions(path, permissions).expect("make the payload writable");
}

/// Re-encodes a current delta envelope as a legacy V1 envelope.
///
/// Integration tests use this to stage legacy on-disk bytes without naming a
/// version module. The returned bytes carry no integrity field, which is the
/// state a verifying store is meant to repair on first read.
pub(crate) fn legacy_delta_envelope(current: &[u8]) -> Vec<u8> {
    mediapm_cas::legacy_delta_v1_wire_from_current(current)
}
