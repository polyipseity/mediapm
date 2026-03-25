use serde_json::Value;
use tempfile::tempdir;

use mediapm::{domain::metadata::probe_media_file, infrastructure::store::hash_file};

#[tokio::test]
async fn probe_media_file_returns_expected_shapes() {
    let workspace = tempdir().expect("temp workspace should create");
    let source_file = workspace.path().join("song.flac");
    std::fs::write(&source_file, b"flac-payload").expect("source should write");

    let hash = hash_file(&source_file).await.expect("hash should compute");
    let (container, probe, normalized) =
        probe_media_file(&source_file, hash).await.expect("probe should succeed");

    assert_eq!(container.as_deref(), Some("flac"));
    assert_eq!(probe.get("byte_size").and_then(Value::as_u64), Some(12));
    assert_eq!(normalized["technical"]["container"], Value::String("flac".to_owned()));
}
