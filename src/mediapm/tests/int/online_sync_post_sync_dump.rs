//! Regression test for online demo variant resolution.
//!
//! This is a real regression test (not a diagnostic dump). It exercises the
//! full online sync path for the demo-online fixture and asserts concrete
//! post-sync invariants: yt-dlp tool keys are present, their content maps are
//! non-empty, the infojson sidecar was materialized, and conductor recorded
//! executed tool-call instances.
//!
//! The actual `YouTube` video download only happens on an explicit opt-in run.
//! The test skips before any network call unless `MEDIAPM_RUN_LARGE_TESTS` is
//! set to an enabled token (see `large_tests_enabled`). `scripts/run-all-tests.sh
//! --large` and CI set that variable; a plain `cargo test` skips.
//!
//! Run explicitly:
//! `MEDIAPM_RUN_LARGE_TESTS=1 cargo test -p mediapm online_sync_post_sync_dump -- --nocapture --test-threads=1`

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::common::service_at;
use mediapm::load_mediapm_document;
use mediapm_conductor::decode_state_json;
use tracing_subscriber::EnvFilter;

/// Opt-in env var that enables network/external-tool-heavy tests. Set by
/// `scripts/run-all-tests.sh --large` and CI; unset in a plain `cargo test`.
const LARGE_TESTS_ENV: &str = "MEDIAPM_RUN_LARGE_TESTS";

/// Returns true only when large/online tests are explicitly opted in. Any
/// other value (including unset) means the test must skip before touching the
/// network.
fn large_tests_enabled() -> bool {
    std::env::var(LARGE_TESTS_ENV).is_ok_and(|value| {
        matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on")
    })
}

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

fn list_dir_names(root: &Path) -> Vec<String> {
    fs::read_dir(root)
        .map(|entries| {
            entries.flatten().map(|entry| entry.file_name().to_string_lossy().to_string()).collect()
        })
        .unwrap_or_default()
}

fn read_env_generated_yt_dlp_path(env_file: &Path) -> Option<String> {
    fs::read_to_string(env_file).ok().and_then(|content| {
        content
            .lines()
            .find(|line| line.starts_with("MEDIAPM_TOOLS_YT_DLP="))
            .map(|line| line.split('=').nth(1).unwrap_or("").to_string())
    })
}

/// Concrete post-sync facts collected from the synced workspace, used by the
/// regression assertions so the test body stays under the line-length lint.
struct PostSyncFacts<'a> {
    summary: &'a mediapm::SyncSummary,
    yt_dlp_tool_keys: Vec<String>,
    yt_dlp_content_map_lens: BTreeMap<String, usize>,
    tools_dir_names: Vec<String>,
    yt_dlp_env_exists: bool,
    instance_count: usize,
    sidecar_exists: bool,
    file_count: usize,
    infojson_hash_present: bool,
    infojson_step_count: usize,
    workflow_step_count: usize,
}

/// Asserts the post-sync invariants that prove the online demo resolved and
/// materialized its yt-dlp variant correctly.
fn assert_post_sync_facts(facts: &PostSyncFacts<'_>) {
    assert!(
        !facts.yt_dlp_tool_keys.is_empty(),
        "yt-dlp tool keys must be present in generated doc"
    );
    assert!(
        facts.yt_dlp_content_map_lens.values().all(|len| *len > 0),
        "every yt-dlp tool content_map must be non-empty: {:#?}",
        facts.yt_dlp_content_map_lens
    );
    assert!(facts.summary.added_tools > 0, "sync must register managed tools");
    assert!(
        facts.summary.executed_instances > 0,
        "yt-dlp must have executed at least one instance"
    );
    assert!(facts.instance_count > 0, "conductor must record tool-call instances");
    assert!(facts.infojson_hash_present, "source variant_hashes must contain infojson");
    assert!(facts.infojson_step_count > 0, "workflow must contain infojson steps");
    assert!(facts.workflow_step_count > 0, "generated workflow must contain steps");
    assert!(facts.yt_dlp_env_exists, ".env.generated must point at a real yt-dlp payload path");
    assert!(!facts.tools_dir_names.is_empty(), "tools_dir must contain provisioned tool dirs");
    assert!(facts.sidecar_exists, "sidecars/info.json must be materialized");
    assert!(facts.file_count > 0, "hierarchy must contain materialized files");
    assert!(facts.summary.materialized_paths > 0, "sync must materialize hierarchy paths");
}

#[tokio::test]
async fn online_sync_post_sync_dump() {
    if !large_tests_enabled() {
        eprintln!(
            "[online_sync_post_sync_dump] skipping: set {LARGE_TESTS_ENV}=1 to run \
             (requires network, external tools, and several minutes)"
        );
        return;
    }

    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .try_init();
    let fixture = fixture_mediapm_ncl();
    assert!(
        fixture.is_file(),
        "generate fixture first: MEDIAPM_DEMO_ONLINE_RUN_SYNC=false cargo run --example mediapm_demo_online"
    );

    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    fs::copy(&fixture, root.path().join("mediapm.ncl")).expect("copy mediapm.ncl");

    let mut service = service_at(root.path(), Some("media")).await.expect("create service");

    let summary = service.sync_library(false).await.expect("sync_library");

    let paths = service.resolve_effective_paths().expect("paths");
    let document = load_mediapm_document(&paths.mediapm_ncl).expect("load document");
    let source = document.media.get("youtube.dQw4w9WgXcQ").expect("youtube source");

    let generated_bytes = fs::read(&paths.conductor_generated_ncl).expect("read generated");
    let generated_doc =
        mediapm_conductor::decode_document(&generated_bytes).expect("decode generated");

    let state_bytes = fs::read(&paths.conductor_state_config).expect("read state");
    let conductor_state = decode_state_json(&state_bytes).expect("decode state");

    let workflow_name = "mediapm.media.youtube.dQw4w9WgXcQ".to_string();
    let workflow = generated_doc
        .workflows
        .iter()
        .find(|workflow| workflow.name == workflow_name)
        .expect("managed workflow");

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

    let yt_dlp_tool_keys: Vec<String> =
        generated_doc.tools.keys().filter(|key| key.contains("yt-dlp")).cloned().collect();
    let yt_dlp_content_map_lens: BTreeMap<String, usize> = generated_doc
        .tools
        .iter()
        .filter(|(key, _)| key.contains("yt-dlp"))
        .map(|(key, spec)| (key.clone(), spec.runtime.content_map.len()))
        .collect();
    let tools_dir_names = list_dir_names(&paths.tools_dir);
    let yt_dlp_env_path = read_env_generated_yt_dlp_path(&paths.env_generated_file);
    let yt_dlp_env_exists = yt_dlp_env_path.as_ref().is_some_and(|path| Path::new(path).is_file());
    let instance_count = conductor_state.tool_call_instances.len();

    let facts = PostSyncFacts {
        summary: &summary,
        yt_dlp_tool_keys,
        yt_dlp_content_map_lens,
        tools_dir_names,
        yt_dlp_env_exists,
        instance_count,
        sidecar_exists: sidecar_info.is_file(),
        file_count: list_files_under(&hierarchy_root).len(),
        infojson_hash_present: source.variant_hashes.contains_key("infojson"),
        infojson_step_count: infojson_steps.len(),
        workflow_step_count: workflow.steps.len(),
    };

    assert_post_sync_facts(&facts);
}
