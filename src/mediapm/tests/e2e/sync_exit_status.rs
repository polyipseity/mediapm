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
//! system gives it. The workspaces carry one document and differ in what the
//! entry asks for: a variant whose bytes the CAS holds, a variant whose hash no
//! blob answers for, or a variant nothing produces at all. That is what
//! separates status `0` from `1` and from `4`, so a run that got there some
//! other way fails one of the three.
//!
//! | status | meaning |
//! | --- | --- |
//! | 0 | the run left the library the way it was asked to, a normal skip included |
//! | 3 | the run finished and warned |
//! | 4 | the run finished and left the library short of what the config asked for |
//! | 1 | the run failed without a summary, a lost CAS object included |
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
/// Sixty-four zeros in the canonical `blake3:<hex>` form. The document declares
/// it, so the variant has resolved to a hash and the store cannot answer for it,
/// which is the lost-object failure: nothing on disk is edited and no record is
/// corrupted to reach it.
const UNRESOLVABLE_HASH: &str =
    "blake3:0000000000000000000000000000000000000000000000000000000000000000";

/// Field the summary prints for entries whose target already matched the
/// resolved hash and length. It appears only above a count of zero, so its
/// presence is proof the run found an already-correct entry.
const SKIPPED_FIELD: &str = "skipped=";

/// Substring the post-sync summary prints on stdout once the sync finishes.
const SYNC_COMPLETE_MARKER: &str = "sync complete";

/// Builds the document whose single entry declares no step.
///
/// Every workspace here shares this document, so the statuses come out of one
/// code path rather than out of workspaces that happen to end differently. The
/// kind, the hashes and the selector are the conditions that decide the status,
/// and each workspace builder below states the one it asks for.
///
/// The selector is a condition rather than a fixed field: a media node's
/// selector naming a variant the source does not declare fails the plan build
/// before materialization, so the workspace whose entry asks for a variant
/// nothing produces takes the folder kind, whose empty selector list means every
/// available variant.
fn document_naming(
    kind: HierarchyNodeKind,
    variant_hashes: BTreeMap<String, String>,
    variant: Option<String>,
) -> MediaPmDocument {
    MediaPmDocument {
        media: BTreeMap::from([(
            MEDIA_ID.to_string(),
            MediaSourceSpec { variant_hashes, ..MediaSourceSpec::default() },
        )]),
        hierarchy: vec![HierarchyNode {
            path: HierarchyPath::from(HIERARCHY_PATH),
            kind,
            id: None,
            media_id: Some(MEDIA_ID.to_string()),
            variant,
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

/// The document naming one variant at `variant_hash`.
fn document_naming_one_hash(variant_hash: &str) -> MediaPmDocument {
    document_naming(
        HierarchyNodeKind::Media,
        BTreeMap::from([(VARIANT.to_string(), variant_hash.to_string())]),
        Some(VARIANT.to_string()),
    )
}

/// The document whose entry asks for every variant its source has, and whose
/// source has none.
///
/// A media node cannot express this, for the reason given on [`document_naming`]:
/// it names exactly one variant, and that name has to be one the source declares.
/// A folder asks for every available variant, so a source that declares neither a
/// hash nor a step gives it nothing to write while still asking for the entry.
fn document_naming_no_variant() -> MediaPmDocument {
    document_naming(HierarchyNodeKind::MediaFolder, BTreeMap::new(), None)
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

/// Creates a workspace whose config asks for variants nothing produces.
///
/// Nothing is seeded and the document declares no hash and no step, so the entry
/// resolves to no variant at all: it is a path the config asked for and no
/// workflow has written, which the run reports as a miss rather than as a
/// failure.
fn workspace_without_content() -> tempfile::TempDir {
    let workspace = mediapm_utils::temp::artifact_dir().expect("artifact dir");
    save_mediapm_document(&workspace.path().join("mediapm.ncl"), &document_naming_no_variant())
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

/// A variant whose declared hash names a blob the library never received ends
/// the command on the failure status.
///
/// The declaration and the store disagree, and no later run repairs that by
/// itself: the workflow recorded the hash as its output, so a path it "has not
/// written yet" is the wrong reading. The message is asserted alongside the
/// status because a status alone cannot say which failure produced it, and the
/// hash it names is what a reader needs to find the record that promised it.
#[test]
fn sync_exits_one_when_a_variant_names_bytes_the_store_lost() {
    let workspace = workspace_missing_content();
    let cache_home = mediapm_utils::temp::cache_dir().expect("cache dir");
    let target = materialized(workspace.path());

    let output = sync(workspace.path(), cache_home.path());
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert_eq!(
        output.status.code(),
        Some(1),
        "{}",
        describe(
            "a variant whose bytes the store lost fails the run, so it must not report a finished \
                  sync",
            &output
        )
    );
    for expected in [UNRESOLVABLE_HASH, MEDIA_ID, VARIANT] {
        assert!(
            stderr.contains(expected),
            "the failure has to name '{expected}', or a reader cannot tell which record promised \
             the bytes; stderr:\n{stderr}"
        );
    }
    assert!(
        !target.exists(),
        "nothing can be materialized from an absent blob, so the target must not exist: {}",
        target.display()
    );
}

/// An entry nothing produces finishes the command on the incomplete-library
/// status, with the counts behind it.
///
/// The document asks for variants no hash and no step supplies, so the entry is a
/// path no workflow has written: the run finishes, reports it as missing, and
/// exits `4`. This is the band a lost object must not swallow, because a missing
/// path is a library a later run may complete while lost bytes are a
/// disagreement no later run repairs by itself. The two are separate assertions
/// in this file, and the counters are what tell them apart in a script.
#[test]
fn sync_exits_four_when_no_variant_has_a_producer() {
    let workspace = workspace_without_content();
    let cache_home = mediapm_utils::temp::cache_dir().expect("cache dir");
    let target = materialized(workspace.path());

    let output = sync(workspace.path(), cache_home.path());
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert_eq!(
        output.status.code(),
        Some(4),
        "{}",
        describe(
            "a variant nothing produces leaves the library short, so the run must finish and \
             report it rather than fail without a summary",
            &output
        )
    );
    assert!(
        stderr.contains("missing=1") && stderr.contains("failed_steps=0"),
        "the error line has to name the counts behind it, so a reader can tell a missing entry \
         from a failed step; stderr:\n{stderr}"
    );
    // A folder entry creates its directory even when it writes nothing, so the
    // claim is about files rather than about the path: a missing variant must
    // leave the folder empty rather than absent.
    let written = std::fs::read_dir(&target).map_or(0, std::iter::Iterator::count);
    assert_eq!(
        written,
        0,
        "no variant resolved, so the folder must hold no file: {}",
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
