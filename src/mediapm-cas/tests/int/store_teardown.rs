//! Tests for store teardown: what a dropped [`FileSystemCas`] guarantees about
//! the directory it was opened on.
//!
//! The guarantee under test is the one the janitor gate depends on. The
//! background WAL consumer holds an owned `Arc` to the store, so it can outlive
//! the caller's handle, and `tokio::fs` runs every operation on
//! `spawn_blocking` — work that an abort does not cancel. Without the store's
//! mutation gate, a `create_dir_all` dispatched by the consumer lands *after*
//! the test removed its temp directory and leaves an orphaned
//! `$TMPDIR/mediapm-artifact-*` tree holding exactly one object.

use std::time::Duration;

use bytes::Bytes;

use mediapm_cas::FileSystemCas;
use mediapm_cas::api::CasApi;

use crate::common::artifact_dir;

/// Entries under `root` after a teardown, as a sorted list of relative paths.
///
/// A resurrected tree always shows up here as a directory chain, whether or not
/// the write that recreated it also completed. A removed root has no entries.
fn surviving_entries(root: &std::path::Path) -> Vec<String> {
    let mut entries = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(children) = std::fs::read_dir(&dir) else {
            continue;
        };
        for child in children.flatten() {
            let path = child.path();
            let relative = path
                .strip_prefix(root)
                .expect("child is under root")
                .to_string_lossy()
                .into_owned();
            if path.is_dir() {
                stack.push(path);
            }
            entries.push(relative);
        }
    }
    entries.sort();
    entries
}

/// Dropping the last handle must leave the store's directory inert.
///
/// The consumer is armed with a 1 ms interval and given more entries than one
/// cycle drains, so a maintenance write is in flight when the handle is
/// dropped. Dropping the last handle cancels the task and closes the mutation
/// gate, which waits for the write the aborted task already dispatched and
/// refuses any that follow; removing the tree straight afterwards must
/// therefore succeed and stay removed.
///
/// The sleep below only widens the observation window — it lets the runtime
/// poll the aborted task and the blocking pool run anything already accepted,
/// so a surviving write would have landed by now. It is not part of the fix,
/// and the assertion cannot pass because of it: after `drop` returns, the gate
/// is closed synchronously, so there is no later write left to wait for.
#[tokio::test]
async fn dropping_the_last_handle_leaves_the_store_directory_inert() {
    let dir = artifact_dir();
    let cas = FileSystemCas::open_with_strategies_and_interval(
        dir.path(),
        vec![],
        Duration::from_millis(1),
    )
    .await
    .expect("open");

    for index in 0..64 {
        let data = Bytes::from(format!("teardown-race-{index}"));
        cas.put(data).await.expect("put");
    }

    drop(cas);

    std::fs::remove_dir_all(dir.path()).expect("remove the store tree right after drop");
    assert!(!dir.path().exists(), "store tree removed after the last handle dropped");

    tokio::time::sleep(Duration::from_millis(100)).await;

    assert_eq!(
        surviving_entries(dir.path()),
        Vec::<String>::new(),
        "a write dispatched by the cancelled consumer recreated the store tree after teardown"
    );
}

/// Dropping a clone must not close the store out from under the other handles.
///
/// The teardown work in `Drop` is gated on the dropped value being the last
/// live handle. A store whose clone merely went away would otherwise have its
/// mutation gate closed while a sibling handle is still writing, and every
/// later write would fail with `BrokenPipe`.
#[tokio::test]
async fn dropping_a_clone_keeps_the_store_writable() {
    let dir = artifact_dir();
    let cas = FileSystemCas::open(dir.path()).await.expect("open");
    let clone = FileSystemCas::clone(&cas);

    drop(cas);

    let data = Bytes::from_static(b"still writable through the surviving clone");
    let hash = clone.put(data.clone()).await.expect("put through the surviving clone");
    assert_eq!(clone.get(hash).await.expect("get through the surviving clone"), data);
}

/// `close` must stop the consumer deterministically, not merely request it.
///
/// The guard's `cancelled` flag is observable through the store, so this pins
/// that the awaited shutdown has already set it by the time `close` returns —
/// a plain abort request would leave the task free to run one more cycle.
#[tokio::test]
async fn close_awaits_the_background_consumer() {
    let dir = artifact_dir();
    let cas = FileSystemCas::open_with_strategies_and_interval(
        dir.path(),
        vec![],
        Duration::from_millis(1),
    )
    .await
    .expect("open");

    for index in 0..16 {
        let data = Bytes::from(format!("close-then-remove-{index}"));
        cas.put(data).await.expect("put");
    }

    let cancelled = cas.bg_guard_ref().cancelled.clone();
    cas.close().await;
    assert!(
        cancelled.load(std::sync::atomic::Ordering::SeqCst),
        "close must have cancelled the consumer"
    );

    std::fs::remove_dir_all(dir.path()).expect("remove the store tree right after close");
    assert!(!dir.path().exists(), "store tree removed after close");

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        surviving_entries(dir.path()),
        Vec::<String>::new(),
        "a write dispatched before close recreated the store tree after teardown"
    );
}
