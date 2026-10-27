//! A local media source stages the bytes its hash names.
//!
//! `media add --local` writes an import step whose `hash` option names the
//! file's content. Naming a hash is a claim that the bytes are in the store,
//! so the same call stages them: a sync immediately afterwards then resolves
//! the import step instead of failing on a missing object.
//!
//! The add and the sync are separate processes, so the claim is about a store
//! that outlives the process that wrote to it. One test covers the in-process
//! pair; the other covers survival across a service lifetime, which is as far
//! as a single-process test can carry it. Cross-process survival rests on the
//! CLI reproduction recorded in this task's report, not on either test.

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use mediapm::{AddInsertPosition, MediaPmService, MediaRuntimeStorage, TransformInputValue};
use mediapm_cas::{CasApi, Hash};

use crate::common::read_doc;

/// The bytes the local source fixture holds.
const LOCAL_SOURCE_BYTES: &[u8] = b"local source bytes";

/// The media id the fixture registers, so the document key is deterministic.
const LOCAL_MEDIA_ID: &str = "local-fixture";

/// An ffprobe command that cannot resolve.
///
/// The metadata stage must be served by the seeded cache: a probe attempt
/// would fail on this name, so a passing test proves the cache was consulted
/// rather than that the host happens to have ffprobe installed.
const FFPROBE_NOT_INSTALLED: &str = "ffprobe-not-installed-in-this-test";

/// A local source's hash names bytes that are in the store.
///
/// The document is read back rather than the return value inspected, because
/// the claim being tested is about the pair: the hash the import step names,
/// and the bytes that hash resolves to.
#[tokio::test]
async fn a_local_source_is_staged_before_its_hash_is_written() {
    let root = mediapm_utils::temp::artifact_dir().expect("artifact dir");
    let cache_root = mediapm_utils::temp::cache_dir().expect("cache dir");
    let payload = root.path().join("local.mkv");
    std::fs::write(&payload, LOCAL_SOURCE_BYTES).expect("write payload");
    seed_ffprobe_cache(root.path(), &payload);

    let storage = MediaRuntimeStorage {
        cache_root_override: Some(cache_root.path().to_path_buf()),
        ..MediaRuntimeStorage::default()
    };
    let mut service =
        MediaPmService::new_fs_at_with_runtime_storage_overrides(root.path(), storage)
            .await
            .expect("service");

    let media_id = service
        .add_local_source_with_position(
            &payload,
            FFPROBE_NOT_INSTALLED,
            Some(LOCAL_MEDIA_ID.to_string()),
            AddInsertPosition::End,
            false,
        )
        .await
        .expect("add local source");
    assert_eq!(media_id, LOCAL_MEDIA_ID, "the fixture registers its own media id");

    let document = read_doc(&service.paths().mediapm_ncl);
    let source = document.media.get(LOCAL_MEDIA_ID).expect("source is in the document");
    let import_step = source.steps.first().expect("the source has an import step");
    let named = match import_step.options.get("hash") {
        Some(TransformInputValue::String(value)) => value.clone(),
        other => panic!("the import step must name a hash, got {other:?}"),
    };

    let expected = Hash::from_content(LOCAL_SOURCE_BYTES);
    assert_eq!(named, expected.to_hex(), "the import step must name the file's content hash");
    let stored = service
        .conductor()
        .cas()
        .get(expected)
        .await
        .expect("the named hash must resolve to staged bytes");
    assert_eq!(stored.as_ref(), LOCAL_SOURCE_BYTES, "the staged bytes are the file's");
}

/// Seeds the ffprobe metadata cache so the add runs without an ffprobe binary.
///
/// The cache is keyed by `ffprobe:<path>` and read before the probe runs, so
/// a seeded entry makes the metadata stage hermetic instead of depending on a
/// tool the test host may not have. The file must be written in the cache's own
/// shape, version included, or the cache treats it as holding nothing, and must
/// exist before the service opens the cache.
fn seed_ffprobe_cache(root: &Path, payload: &Path) {
    let cache_dir = root.join(".mediapm").join("cache").join("mediapm");
    std::fs::create_dir_all(&cache_dir).expect("create metadata cache dir");
    let now = SystemTime::now().duration_since(UNIX_EPOCH).expect("clock after epoch").as_secs();
    let entries = serde_json::json!({
        format!("ffprobe:{}", payload.display()): {
            "value": {
                "title": "Local Fixture",
                "artist": "",
                "description": "",
            },
            "lastAccessUnixSeconds": now,
        }
    });
    let cache_file = serde_json::json!({ "version": 1, "entries": entries });
    std::fs::write(
        cache_dir.join("metadata.json"),
        serde_json::to_vec(&cache_file).expect("encode metadata cache"),
    )
    .expect("write metadata cache");
}

/// A reopened workspace resolves the hash the document records.
///
/// `media add --local` writes the document and exits, and `mediapm sync` later
/// opens the same store and reads the hash that document names. Staging that
/// did not survive the first process would leave the document promising bytes
/// no store holds, which is a missing-object failure at the import step rather
/// than at the add. This test opens the workspace a second time and resolves
/// the hash the first service recorded, so it fails if the staged bytes live
/// only in the process that staged them.
///
/// Dropping the first service before the second open is required rather than
/// stylistic: the store holds a directory lock for its service's lifetime, and
/// a second open at the same root fails while that lock is held.
///
/// The limit of this test: it resolves the hash through the conductor's CAS
/// handle, so it pins that the store outlives the service rather than that the
/// materializer is handed that same store. That hand-off is a call site
/// (`materializer::sync_hierarchy(..., self.conductor.cas(), ...)`), and the
/// read a sync performs for this step is the worker's own `cas.get` for a
/// `kind = "cas_hash"` import; both are named in this task's report rather than
/// asserted here.
#[tokio::test]
async fn a_reopened_workspace_resolves_the_staged_hash() {
    let root = mediapm_utils::temp::artifact_dir().expect("artifact dir");
    let cache_root = mediapm_utils::temp::cache_dir().expect("cache dir");
    let payload = root.path().join("local.mkv");
    std::fs::write(&payload, LOCAL_SOURCE_BYTES).expect("write payload");
    seed_ffprobe_cache(root.path(), &payload);

    let storage = MediaRuntimeStorage {
        cache_root_override: Some(cache_root.path().to_path_buf()),
        ..MediaRuntimeStorage::default()
    };

    let mut first =
        MediaPmService::new_fs_at_with_runtime_storage_overrides(root.path(), storage.clone())
            .await
            .expect("first service");
    first
        .add_local_source_with_position(
            &payload,
            FFPROBE_NOT_INSTALLED,
            Some(LOCAL_MEDIA_ID.to_string()),
            AddInsertPosition::End,
            false,
        )
        .await
        .expect("add local source");
    let named = match read_doc(&first.paths().mediapm_ncl)
        .media
        .get(LOCAL_MEDIA_ID)
        .and_then(|source| source.steps.first())
        .and_then(|step| step.options.get("hash"))
    {
        Some(TransformInputValue::String(text)) => text.clone(),
        other => panic!("the import step must name a hash, got {other:?}"),
    };
    let expected = Hash::from_content(LOCAL_SOURCE_BYTES);
    assert_eq!(named, expected.to_hex(), "the import step must name the file's content hash");
    drop(first);

    let second = MediaPmService::new_fs_at_with_runtime_storage_overrides(root.path(), storage)
        .await
        .expect("the workspace reopens after the first service is dropped");
    let stored = second
        .conductor()
        .cas()
        .get(expected)
        .await
        .expect("a reopened workspace resolves the hash the document records");
    assert_eq!(
        stored.as_ref(),
        LOCAL_SOURCE_BYTES,
        "the bytes staged by the first process survive it"
    );
}
