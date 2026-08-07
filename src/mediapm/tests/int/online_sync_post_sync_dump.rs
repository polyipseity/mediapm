//! Post-sync diagnostic for online demo variant resolution (`#[ignore]`).
//!
//! Run:
//! `cargo test -p mediapm online_sync_post_sync_dump -- --ignored --nocapture --test-threads=1`

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use mediapm::{MediaPmService, MediaRuntimeStorage, load_mediapm_document};
use mediapm_conductor::decode_state_json;

fn fixture_mediapm_ncl() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/artifacts/demo-online/mediapm.ncl")
}

fn list_files_under(root: &Path) -> Vec<String> {
    let mut files = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        if let Ok(entries) = fs::read_dir(&dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    pending.push(path);
                } else if let Ok(relative) = path.strip_prefix(root) {
                    files.push(relative.to_string_lossy().to_string());
                }
            }
        }
    }
    files.sort();
    files
}

#[tokio::test]
#[ignore = "requires network, external tools, and several minutes"]
async fn online_sync_post_sync_dump() {
    let fixture = fixture_mediapm_ncl();
    assert!(
        fixture.is_file(),
        "generate fixture first: MEDIAPM_DEMO_ONLINE_RUN_SYNC=false cargo run --example mediapm_demo_online"
    );

    let root = tempfile::tempdir().expect("tempdir");
    fs::copy(&fixture, root.path().join("mediapm.ncl")).expect("copy mediapm.ncl");

    let runtime_storage = MediaRuntimeStorage {
        cache_root_override: Some(root.path().join("tool-cache")),
        hierarchy_root_dir: Some("media".to_string()),
        ..MediaRuntimeStorage::default()
    };

    let mut service =
        MediaPmService::new_fs_at_with_runtime_storage_overrides(root.path(), runtime_storage)
            .await
            .expect("create service");

    let summary = service.sync_library(false).await.expect("sync_library");

    let paths = service.resolve_effective_paths().expect("paths");
    let document = load_mediapm_document(&paths.mediapm_ncl).expect("load document");
    let source = document.media.get("youtube.dQw4w9WgXcQ").expect("youtube source");

    let generated_bytes = fs::read(&paths.conductor_generated_ncl).expect("read generated");
    let generated_doc =
        mediapm_conductor::decode_document(&generated_bytes).expect("decode generated");

    let state_bytes = fs::read(&paths.conductor_state_config).expect("read state");
    let conductor_state = decode_state_json(&state_bytes).expect("decode state");

    let workflow_name = format!("mediapm.media.youtube.dQw4w9WgXcQ");
    let workflow = generated_doc
        .workflows
        .iter()
        .find(|workflow| workflow.name == workflow_name)
        .expect("managed workflow");

    let yt_dlp_instances: BTreeMap<String, Vec<String>> = conductor_state
        .tool_call_instances
        .values()
        .filter(|instance| instance.tool_call_id.contains("yt-dlp"))
        .map(|instance| (instance.tool_call_id.clone(), instance.outputs.keys().cloned().collect()))
        .collect();

    let infojson_steps: Vec<String> = workflow
        .steps
        .iter()
        .filter(|step| step.id.contains("infojson"))
        .map(|step| step.id.clone())
        .collect();

    let hierarchy_root = paths.hierarchy_root_dir;
    let media_folder = PathBuf::from("music videos")
        .join("Rick Astley - Never Gonna Give You Up [youtube.dQw4w9WgXcQ]");
    let sidecar_info = hierarchy_root.join(&media_folder).join("sidecars/info.json");

    panic!(
        "ONLINE_SYNC_POST_SYNC_DUMP\n\
warnings: {warnings:?}\n\
materialized_paths: {materialized}\n\
variant_hashes ({vh_len}): {variant_keys:?}\n\
infojson hash: {infojson_hash:?}\n\
infojson workflow steps: {infojson_steps:?}\n\
yt-dlp instance output keys: {yt_dlp_instances:#?}\n\
sidecars/info.json exists: {sidecar_exists}\n\
files under hierarchy ({file_count}):\n{files:#?}",
        warnings = summary.warnings,
        materialized = summary.materialized_paths,
        vh_len = source.variant_hashes.len(),
        variant_keys = source.variant_hashes.keys().collect::<Vec<_>>(),
        infojson_hash = source.variant_hashes.get("infojson"),
        sidecar_exists = sidecar_info.is_file(),
        file_count = list_files_under(&hierarchy_root).len(),
        files = list_files_under(&hierarchy_root),
    );
}
