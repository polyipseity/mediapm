use serde_json::json;
use tempfile::tempdir;

use mediapm::{
    application::{
        executor::execute_plan,
        history::{
            MetadataEditRequest, TranscodeRecordRequest, record_metadata_edit,
            record_transcode_event,
        },
        planner::build_plan,
    },
    configuration::config::AppConfig,
    domain::model::EditKind,
    infrastructure::{store::WorkspacePaths, verify::verify_workspace},
};

fn setup_imported_sidecar() -> (tempfile::TempDir, WorkspacePaths, String) {
    let workspace = tempdir().expect("temp workspace should create");
    let source_file = workspace.path().join("inbox/song.flac");

    std::fs::create_dir_all(source_file.parent().expect("parent should exist"))
        .expect("source directory should create");
    std::fs::write(&source_file, b"seed-audio").expect("source file should write");

    let config: AppConfig = serde_json::from_value(json!({
        "sources": [{"uri": "inbox/song.flac"}],
        "links": []
    }))
    .expect("config should deserialize");

    let paths = WorkspacePaths::new(workspace.path());
    let plan = build_plan(&config, workspace.path()).expect("plan should build");
    execute_plan(&paths, &config, &plan, true).expect("sync should succeed");

    let canonical_uri =
        mediapm::domain::canonical::canonicalize_uri("inbox/song.flac", workspace.path())
            .expect("uri should canonicalize")
            .into_string();

    (workspace, paths, canonical_uri)
}

#[test]
fn record_metadata_edit_updates_variant_and_history() {
    let (_workspace, paths, canonical_uri) = setup_imported_sidecar();

    let summary = record_metadata_edit(
        &paths,
        MetadataEditRequest {
            canonical_uri: canonical_uri.clone(),
            target_variant_hash: None,
            kind: EditKind::Revertable,
            operation: "metadata_update".to_owned(),
            metadata_patch: json!({"tags": {"artist": "Test Artist"}}),
            message: Some("manual correction".to_owned()),
            details: json!({"actor": "integration-test"}),
        },
    )
    .expect("metadata edit should record");

    assert_eq!(summary.canonical_uri, canonical_uri);
    assert_eq!(summary.from_variant_hash, summary.to_variant_hash);

    let sidecar = mediapm::infrastructure::store::read_sidecar(&paths, &summary.canonical_uri)
        .expect("sidecar should read")
        .expect("sidecar should exist");

    assert!(
        sidecar.variants.iter().any(|variant| {
            variant.metadata["tags"]["artist"]
                == serde_json::Value::String("Test Artist".to_owned())
        }),
        "metadata patch should be applied"
    );
    assert!(sidecar.edits.iter().any(|event| event.event_id == summary.event_id));
}

#[test]
fn record_transcode_event_appends_non_revertable_lineage() {
    let (workspace, paths, canonical_uri) = setup_imported_sidecar();

    let output_file = workspace.path().join("out/song.mp3");
    std::fs::create_dir_all(output_file.parent().expect("parent should exist"))
        .expect("output directory should create");
    std::fs::write(&output_file, b"transcoded-bytes").expect("output file should write");

    let summary = record_transcode_event(
        &paths,
        TranscodeRecordRequest {
            canonical_uri: canonical_uri.clone(),
            from_variant_hash: None,
            kind: EditKind::NonRevertable,
            output_path: output_file,
            operation: "transcode".to_owned(),
            details: json!({"tool": "ffmpeg", "preset": "v2"}),
        },
    )
    .expect("transcode event should record");

    assert_eq!(summary.canonical_uri, canonical_uri);
    assert_ne!(summary.from_variant_hash, summary.to_variant_hash);
    assert!(summary.variant_created);

    let sidecar = mediapm::infrastructure::store::read_sidecar(&paths, &summary.canonical_uri)
        .expect("sidecar should read")
        .expect("sidecar should exist");

    assert!(sidecar.edits.iter().any(|event| {
        event.event_id == summary.event_id
            && event.kind == mediapm::domain::model::EditKind::NonRevertable
    }));

    let verify_report = verify_workspace(&paths).expect("verify should run");
    assert!(verify_report.is_clean(), "recorded transcode should preserve integrity");
}

#[test]
fn record_transcode_event_reuses_existing_variant_hash_when_present() {
    let (workspace, paths, canonical_uri) = setup_imported_sidecar();

    let output_file = workspace.path().join("out/song-copy.flac");
    std::fs::create_dir_all(output_file.parent().expect("parent should exist"))
        .expect("output directory should create");
    std::fs::write(&output_file, b"seed-audio").expect("output file should write");

    let summary = record_transcode_event(
        &paths,
        TranscodeRecordRequest {
            canonical_uri: canonical_uri.clone(),
            from_variant_hash: None,
            kind: EditKind::NonRevertable,
            output_path: output_file,
            operation: "transcode".to_owned(),
            details: json!({"tool": "ffmpeg", "mode": "copy"}),
        },
    )
    .expect("transcode event should record");

    assert!(!summary.variant_created, "same-bytes output should reuse existing variant");

    let sidecar = mediapm::infrastructure::store::read_sidecar(&paths, &summary.canonical_uri)
        .expect("sidecar should read")
        .expect("sidecar should exist");

    assert_eq!(sidecar.variants.len(), 1);
    assert!(sidecar.edits.iter().any(|event| event.event_id == summary.event_id));
}

#[test]
fn record_metadata_edit_supports_non_revertable_history_only_event() {
    let (_workspace, paths, canonical_uri) = setup_imported_sidecar();

    let summary = record_metadata_edit(
        &paths,
        MetadataEditRequest {
            canonical_uri: canonical_uri.clone(),
            target_variant_hash: None,
            kind: EditKind::NonRevertable,
            operation: "history_annotation".to_owned(),
            metadata_patch: json!({}),
            message: Some("manual irreversible decision".to_owned()),
            details: json!({"reason": "audit-note"}),
        },
    )
    .expect("history-only metadata edit should record");

    assert_eq!(summary.kind, EditKind::NonRevertable);
    assert!(!summary.variant_created);

    let sidecar = mediapm::infrastructure::store::read_sidecar(&paths, &summary.canonical_uri)
        .expect("sidecar should read")
        .expect("sidecar should exist");

    let event = sidecar
        .edits
        .iter()
        .find(|event| event.event_id == summary.event_id)
        .expect("recorded event should exist");

    assert_eq!(event.kind, EditKind::NonRevertable);
    assert_eq!(event.operation, "history_annotation");
    assert_eq!(event.details["reason"], "audit-note");
    assert_eq!(event.details["metadata_changed"], false);
}
