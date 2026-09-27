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
pub(crate) fn artifact_dir() -> tempfile::TempDir {
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
    _dir: tempfile::TempDir,
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
