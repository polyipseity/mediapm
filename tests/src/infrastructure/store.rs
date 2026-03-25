use serde_json::json;
use tempfile::tempdir;

use mediapm::{
    domain::model::{MediaRecord, VariantLineage, VariantRecord},
    infrastructure::store::{
        WorkspacePaths, ensure_object, hash_file, load_sidecar_index, object_relpath, read_sidecar,
        write_sidecar,
    },
};

#[tokio::test]
async fn ensure_object_is_deduplicated_on_repeated_calls() {
    let workspace = tempdir().expect("temp workspace should create");
    let paths = WorkspacePaths::new(workspace.path());
    paths.ensure_store_dirs().await.expect("store dirs should create");

    let source_file = workspace.path().join("song.flac");
    std::fs::write(&source_file, b"audio-data").expect("source file should write");

    let hash = hash_file(&source_file).await.expect("hash should compute");
    let first_relpath =
        ensure_object(&paths, &source_file, &hash).await.expect("object should store");
    let second_relpath =
        ensure_object(&paths, &source_file, &hash).await.expect("object should dedupe");

    assert_eq!(first_relpath, second_relpath);
    assert!(workspace.path().join(&first_relpath).exists());
    let relpath = object_relpath(&hash).to_string_lossy().replace('\\', "/");
    assert!(relpath.contains(".mediapm/objects/blake3/"));
}

#[tokio::test]
async fn write_and_read_sidecar_round_trip() {
    let workspace = tempdir().expect("temp workspace should create");
    let paths = WorkspacePaths::new(workspace.path());
    paths.ensure_store_dirs().await.expect("store dirs should create");

    let source_file = workspace.path().join("song.flac");
    std::fs::write(&source_file, b"audio-data").expect("source file should write");
    let hash = hash_file(&source_file).await.expect("hash should compute");

    let record = MediaRecord::new_initial(
        "file:///tmp/song.flac".to_owned(),
        "2026-01-01T00:00:00Z".to_owned(),
        VariantRecord {
            variant_hash: hash,
            object_relpath: ".mediapm/objects/blake3/aa/example".to_owned(),
            byte_size: 9,
            container: Some("flac".to_owned()),
            probe: json!({"probe": true}),
            metadata: json!({"tags": {}}),
            lineage: VariantLineage { parent_variant_hash: None, edit_event_ids: vec![] },
        },
        json!({"seed": true}),
    );

    write_sidecar(&paths, &record).await.expect("sidecar should write");

    let loaded = read_sidecar(&paths, "file:///tmp/song.flac")
        .await
        .expect("sidecar read should succeed")
        .expect("sidecar should exist");

    assert_eq!(loaded.canonical_uri, "file:///tmp/song.flac");
    assert_eq!(loaded.variants.len(), 1);
}

#[tokio::test]
async fn load_sidecar_index_contains_written_records() {
    let workspace = tempdir().expect("temp workspace should create");
    let paths = WorkspacePaths::new(workspace.path());
    paths.ensure_store_dirs().await.expect("store dirs should create");

    let source_file = workspace.path().join("song.flac");
    std::fs::write(&source_file, b"audio-data").expect("source file should write");
    let hash = hash_file(&source_file).await.expect("hash should compute");

    let record = MediaRecord::new_initial(
        "file:///tmp/song.flac".to_owned(),
        "2026-01-01T00:00:00Z".to_owned(),
        VariantRecord {
            variant_hash: hash,
            object_relpath: ".mediapm/objects/blake3/aa/example".to_owned(),
            byte_size: 9,
            container: Some("flac".to_owned()),
            probe: json!({}),
            metadata: json!({}),
            lineage: VariantLineage { parent_variant_hash: None, edit_event_ids: vec![] },
        },
        json!({}),
    );

    write_sidecar(&paths, &record).await.expect("sidecar should write");

    let index = load_sidecar_index(&paths).await.expect("sidecar index should load");
    assert!(index.contains_key("file:///tmp/song.flac"));
}
