//! A local media source stages the bytes its hash names.
//!
//! `media add --local` writes an import step whose `hash` option names the
//! file's content. Naming a hash is a claim that the bytes are in the store,
//! so the same call stages them: a sync immediately afterwards then resolves
//! the import step instead of failing on a missing object.

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
/// tool the test host may not have. The file must be written before the
/// service opens the cache.
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
    std::fs::write(
        cache_dir.join("metadata.cache.json"),
        serde_json::to_vec(&entries).expect("encode metadata cache"),
    )
    .expect("write metadata cache");
}
