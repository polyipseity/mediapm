//! What `mediapm sync` returns to the shell.
//!
//! `sync_exit_status` is unit-tested against counters the test wrote by hand.
//! No test ran the binary against a workspace whose sync failed, so the path
//! from a real run to a process status rested on a predicate plus a library
//! test. A predicate cannot notice the process never calling it, a status
//! leaving by another route, or the summary an operator reads disagreeing with
//! the number a script sees.
//!
//! Every test here spawns the built binary and reads the status the operating
//! system gives it. Both workspaces carry the same document and differ in one
//! fact: the CAS holds the blob the document names, or it does not. That is
//! what separates status 0 from status 4, so a run that got there some other way
//! fails one of the two.
//!
//! | status | meaning |
//! | --- | --- |
//! | 0 | the run left the library the way it was asked to, a normal skip included |
//! | 3 | the run finished and warned |
//! | 4 | the run left the library short of what the config asked for |
//! | 1 | the binary never got as far as a sync outcome |
//!
//! Nothing here reaches 3. Every warning the CLI can carry needs a tool
//! download to reach, and these workspaces are offline. The unit test
//! `sync_exits_three_when_the_run_only_warned` holds that half.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use super::subprocess::run_sync;
use crate::common::seed_cas;
use bytes::Bytes;
use mediapm::{
    HierarchyNode, HierarchyNodeKind, HierarchyPath, MediaPmDocument, MediaPmService,
    MediaRuntimeStorage, MediaSourceSpec, PlaylistFormat, save_mediapm_document,
};

/// Bytes stored in the CAS and then materialized.
const PAYLOAD: &[u8] = b"mediapm exit-status subprocess payload\n";

/// Media id of the single source, and the directory its id materializes as.
const MEDIA_ID: &str = "exit-status-media";

/// The variant the single blob is published under.
const VARIANT: &str = "default";

/// Path template that puts the media id itself in the materialized path, so the
/// entry's output is observable on disk after each run.
const HIERARCHY_PATH: &str = "${media.id}/track.bin";

/// Hash naming content the library does not hold.
///
/// Sixty-four zeros in the canonical `blake3:<hex>` form. Variant resolution
/// reads the blob out of the CAS before it hands a hash to the materializer, so
/// a well-formed hash for an absent blob resolves to no content. The document
/// then names content the library never received, which is how this reaches the
/// missing-content branch: nothing on disk is edited and no record corrupted.
const UNRESOLVABLE_HASH: &str =
    "blake3:0000000000000000000000000000000000000000000000000000000000000000";

/// Field the summary prints for entries whose target already held the resolved
/// bytes. It appears only above a count of zero, so its presence is proof the
/// run found an already-correct entry.
const SKIPPED_FIELD: &str = "skipped=";

/// Substring the post-sync summary prints on stdout once the sync finishes.
const SYNC_COMPLETE_MARKER: &str = "sync complete";

/// Builds the document whose single media entry names `variant_hash` and
/// declares no step to produce any.
///
/// Both kinds of workspace share this one document, so the two statuses come
/// out of one code path under one changing condition rather than out of two
/// workspaces that happen to end differently.
fn document_naming_one_hash(variant_hash: &str) -> MediaPmDocument {
    MediaPmDocument {
        media: BTreeMap::from([(
            MEDIA_ID.to_string(),
            MediaSourceSpec {
                variant_hashes: BTreeMap::from([(VARIANT.to_string(), variant_hash.to_string())]),
                ..MediaSourceSpec::default()
            },
        )]),
        hierarchy: vec![HierarchyNode {
            path: HierarchyPath::from(HIERARCHY_PATH),
            kind: HierarchyNodeKind::Media,
            id: None,
            media_id: Some(MEDIA_ID.to_string()),
            variant: Some(VARIANT.to_string()),
            variants: Vec::new(),
            rename_files: Vec::new(),
            format: PlaylistFormat::M3u8,
            ids: Vec::new(),
            sanitize_names: None,
            children: Vec::new(),
        }],
        ..MediaPmDocument::default()
    }
}

/// Stores `payload` in the workspace CAS and returns its canonical hash string.
///
/// No CLI command ingests a host file into the workspace CAS. The `import`
/// builtin takes a hash that is already stored, so no run of the binary could
/// put this payload there, and the blob is written through [`MediaPmService`]
/// instead. Both syncs under test are still real processes.
///
/// The service drops before this returns. The filesystem CAS holds an exclusive
/// lock on the store, and a second open while the first is alive fails with a
/// store-locked error rather than running the sync.
async fn seed_payload(workspace: &Path, payload: Bytes) -> String {
    let cache_root = mediapm_utils::temp::cache_dir().expect("cache dir");
    let runtime = MediaRuntimeStorage {
        cache_root_override: Some(cache_root.path().to_path_buf()),
        ..MediaRuntimeStorage::default()
    };
    let service = MediaPmService::new_fs_at_with_runtime_storage_overrides(workspace, runtime)
        .await
        .expect("the workspace CAS opens for seeding");

    let hash = seed_cas(&service, payload, "the exit-status payload")
        .await
        .expect("the payload is stored");
    // The materializer's first method is a hardlink, which needs the blob on
    // disk as a file rather than as a packed object.
    service
        .conductor()
        .cas()
        .ensure_blob_materialized(hash)
        .await
        .expect("the payload is materialized as a file");

    let hash = hash.to_string();
    drop(service);
    hash
}

/// Creates a workspace whose library holds the blob its document names.
async fn workspace_holding_payload() -> tempfile::TempDir {
    let workspace = mediapm_utils::temp::artifact_dir().expect("artifact dir");
    let hash = seed_payload(workspace.path(), Bytes::from_static(PAYLOAD)).await;
    save_mediapm_document(&workspace.path().join("mediapm.ncl"), &document_naming_one_hash(&hash))
        .expect("the document is written into the workspace");
    workspace
}

/// Creates a workspace whose document names a blob the library does not hold.
///
/// Nothing is seeded, so the two workspaces differ only in whether the named
/// blob is readable out of the CAS.
fn workspace_missing_content() -> tempfile::TempDir {
    let workspace = mediapm_utils::temp::artifact_dir().expect("artifact dir");
    save_mediapm_document(
        &workspace.path().join("mediapm.ncl"),
        &document_naming_one_hash(UNRESOLVABLE_HASH),
    )
    .expect("the document is written into the workspace");
    workspace
}

/// Absolute path the single entry materializes to.
fn materialized(workspace: &Path) -> PathBuf {
    workspace.join(MEDIA_ID).join("track.bin")
}

/// Runs a sync over `workspace` with progress suppressed, so the captured
/// streams carry summary lines and nothing else.
fn sync(workspace: &Path, cache_home: &Path) -> std::process::Output {
    run_sync(workspace, cache_home, true)
}

/// Renders an [`std::process::Output`] as a diagnostic naming both streams and
/// the status.
fn describe(what: &str, output: &std::process::Output) -> String {
    format!(
        "{what}\nstatus: {:?}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    )
}

/// A run that lands every entry the config asked for exits zero.
///
/// The control the other tests read against. A rule that turned every sync red
/// fails here first, so the rest of this file cannot pass on a scheme that
/// simply refuses to report success.
#[tokio::test]
async fn sync_exits_zero_when_every_entry_landed() {
    let workspace = workspace_holding_payload().await;
    let cache_home = mediapm_utils::temp::cache_dir().expect("cache dir");

    let output = sync(workspace.path(), cache_home.path());

    assert_eq!(output.status.code(), Some(0), "{}", describe("a clean sync must exit 0", &output));
    assert_eq!(
        std::fs::read(materialized(workspace.path())).ok(),
        Some(PAYLOAD.to_vec()),
        "the run has to land the entry, or the exit status is reporting success over an empty \
         library"
    );
}

/// A second run over a library nothing changed exits zero, and leaves the entry
/// where the first run put it.
///
/// This is the direction a wrong fix breaks. The second run resolves the hash the
/// first one wrote, finds the target already holding it, and declines the entry.
/// A scheme reading a declined entry as damage turns a good re-sync red, and only
/// a real process over a real re-sync catches that.
///
/// The `skipped=` field keeps this from passing for the wrong reason. Exit zero
/// over a workspace that still had work to do is the same number, and the
/// summary prints the field only above a count of zero.
#[tokio::test]
async fn sync_exits_zero_when_every_entry_was_already_correct() {
    let workspace = workspace_holding_payload().await;
    let cache_home = mediapm_utils::temp::cache_dir().expect("cache dir");
    let target = materialized(workspace.path());

    let first = sync(workspace.path(), cache_home.path());
    assert_eq!(first.status.code(), Some(0), "{}", describe("the first run must exit 0", &first));
    let landed = std::fs::read(&target).expect("the first run must land the entry");

    let second = sync(workspace.path(), cache_home.path());
    let stdout = String::from_utf8_lossy(&second.stdout);

    assert_eq!(
        second.status.code(),
        Some(0),
        "{}",
        describe(
            "a re-sync over an unchanged library is the best outcome available, not a \
                  damage report",
            &second
        )
    );
    assert!(
        stdout.contains(SYNC_COMPLETE_MARKER) && stdout.contains(SKIPPED_FIELD),
        "the second run must reach its summary and report at least one already-correct entry, so \
         exit zero reads as a skip and not as a run that never reached the library; \
         stdout:\n{stdout}"
    );
    assert_eq!(
        std::fs::read(&target).ok(),
        Some(landed),
        "the declined entry must still hold the bytes the first run put there"
    );
}

/// A variant whose content resolves to nothing ends the command on the error
/// status.
///
/// The document names a well-formed hash for a blob the library never received,
/// so there is nothing to write and the library is short an entry the config
/// asked for. The counts on stderr are asserted alongside the status because a
/// status alone cannot say which failure produced it.
#[test]
fn sync_exits_four_when_a_variant_resolves_to_no_content() {
    let workspace = workspace_missing_content();
    let cache_home = mediapm_utils::temp::cache_dir().expect("cache dir");
    let target = materialized(workspace.path());

    let output = sync(workspace.path(), cache_home.path());
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert_eq!(
        output.status.code(),
        Some(4),
        "{}",
        describe(
            "a variant with no content leaves the library short, so the run must not report \
                  success",
            &output
        )
    );
    assert!(
        stderr.contains("missing=1") && stderr.contains("failed_steps=0"),
        "the error line has to name the counts behind it, so a reader can tell a missing entry \
         from a failed step; stderr:\n{stderr}"
    );
    assert!(
        !target.exists(),
        "nothing can be materialized from an absent blob, so the target must not exist: {}",
        target.display()
    );
}

/// A workspace the binary refuses to sync exits 1, which no sync outcome uses.
///
/// The missing-`cli`-feature arm in `main.rs` writes that status as a literal
/// and cannot be spawned: the `mediapm` bin target declares
/// `required-features = ["cli"]`, so no build produces a binary without the
/// feature and the arm has no process to run in. This reaches the status from
/// the other side, by handing the real binary a document it cannot load. The run
/// never reaches a sync outcome, which is the band status 1 is reserved for, so
/// a script reading the number can tell a binary that did not sync from one that
/// synced and found the library short.
#[test]
fn sync_exits_one_when_the_workspace_never_reached_a_sync() {
    let workspace = mediapm_utils::temp::artifact_dir().expect("artifact dir");
    std::fs::write(workspace.path().join("mediapm.ncl"), "this is not a Nickel document")
        .expect("the unreadable document is written");
    let cache_home = mediapm_utils::temp::cache_dir().expect("cache dir");

    let output = sync(workspace.path(), cache_home.path());

    assert_eq!(
        output.status.code(),
        Some(1),
        "{}",
        describe("a run that never reached a sync outcome must stay off 3 and 4", &output)
    );
}
