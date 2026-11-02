//! Real-upstream regression for the online demo's `YouTube` download.
//!
//! Nothing here may run by default: the run downloads from `YouTube`, needs the external
//! tools, and reads the config fixture the demo writes into
//! `examples/artifacts/demo-online/mediapm.ncl` — a path `.gitignore` excludes, so a fresh
//! clone has none. That is why the test carries `#[ignore]` rather than a skip ladder, and
//! why the offline twin is the coverage that runs everywhere: the `mediapm_demo_online`
//! example's embedded test drives this same pipeline over the stub chain, and
//! `tests/e2e/demo_online_hierarchy_materialization.rs` pins the tree it materializes.
//!
//! Run the real upstream on demand:
//! `cargo test-pkg mediapm -- --ignored youtube_regression`

use std::fs;
use std::path::{Path, PathBuf};

use crate::common::service_at;
use mediapm::demo_hierarchy_spec::{
    DemoHierarchyGoldenSpec, ONLINE_DEMO_MEDIA_ID, ONLINE_DEMO_YT_DLP_VIDEO_ID, assert_tree_under,
    load_demo_hierarchy_golden_document,
};
use mediapm::{MediaPmService, SyncSummary, load_mediapm_state_document};
use mediapm_cas::{FileSystemCas, Hash};
use mediapm_conductor::decode_state_json;

/// Managed tool that performs the download.
const YT_DLP_TOOL: &str = "yt-dlp";

/// Variant name the online demo's yt-dlp `write_info_json` step declares.
///
/// The demo's document names it; `demo_hierarchy_spec` exports the naming helpers but not
/// the variant names, so the regression spells it here — the same literal
/// `tests/e2e/demo_online_hierarchy_materialization.rs` seeds.
const INFOJSON_VARIANT: &str = "infojson";

/// Link extensions the online demo's yt-dlp `write_link` step produces.
///
/// yt-dlp names these files itself from its own output template
/// (`%(title)s [%(id)s].{ext}`, per `demo_hierarchy_spec`'s "Online link naming" section),
/// so the set is the formats the step asks for rather than a name mediapm renders.
const SIDECAR_LINK_EXTENSIONS: [&str; 3] = ["desktop", "url", "webloc"];

fn fixture_mediapm_ncl() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/artifacts/demo-online/mediapm.ncl")
}

/// Whether a tool identity names the [`YT_DLP_TOOL`] tool.
///
/// A generated tool key and a recorded tool-call id share one spelling, `<tool>@<identity>`
/// (`{name}@{content_map_hash}` in `conductor_bridge::sync`), so the same read answers both
/// questions. A host builtin records a bare tool name and therefore never matches.
fn names_yt_dlp(identity: &str) -> bool {
    identity.split_once('@').is_some_and(|(tool, _)| tool == YT_DLP_TOOL)
}

/// Reads the upstream provider title yt-dlp recorded in its `info.json` sidecar.
///
/// The sidecar is yt-dlp's own document, and the template that names the link files reads
/// the same `title` field, so this is the run's record of what its link family carries.
fn read_infojson_title(info_json: &Path) -> String {
    let bytes = fs::read(info_json).expect("read the yt-dlp info.json sidecar");
    let document: serde_json::Value =
        serde_json::from_slice(&bytes).expect("decode the yt-dlp info.json sidecar");
    document
        .get("title")
        .and_then(serde_json::Value::as_str)
        .filter(|title| !title.is_empty())
        .expect("the yt-dlp info.json sidecar must carry a non-empty 'title'")
        .to_string()
}

/// Asserts the facts a `sync_library` over the online demo's document establishes.
///
/// `service` is the synced service and `summary` that sync's summary; both describe a run
/// into a root the caller created, so no earlier record can account for anything on disk.
/// `expectations` is the canonical post-sync tree the hierarchy contract promises — the
/// golden `online` block of `demo_hierarchy_spec`.
fn assert_youtube_regression_post_sync(
    service: &MediaPmService<FileSystemCas>,
    summary: &SyncSummary,
    expectations: &DemoHierarchyGoldenSpec,
) {
    assert_summary_reports_a_first_sync(summary);

    let paths = service.resolve_effective_paths().expect("resolve the effective paths");
    let hierarchy_root = &paths.hierarchy_root_dir;
    let media_folder = assert_canonical_media_root(hierarchy_root, expectations);

    let sidecar_info = media_folder.join("sidecars/info.json");
    assert!(
        sidecar_info.is_file(),
        "the yt-dlp info.json sidecar must be materialized at '{}'",
        sidecar_info.display()
    );

    assert_infojson_variant_recorded(hierarchy_root, &paths.mediapm_state_json);
    assert_yt_dlp_instances_recorded_outputs(&paths.conductor_state_config);
    assert_yt_dlp_deployed_with_a_payload(&paths.conductor_generated_ncl);
    assert_contract_tree(hierarchy_root, expectations, &sidecar_info);
}

/// Asserts the summary describes a first sync into a root it created.
///
/// A fresh root holds no instance the cache could serve and no path an earlier run could
/// have written, so a run that executed nothing or materialized nothing did not do the work
/// the rest of the assertions read.
fn assert_summary_reports_a_first_sync(summary: &SyncSummary) {
    assert_eq!(
        summary.cached_steps, 0,
        "a sync into a fresh root must execute every step rather than serve one from the \
         instance cache: {summary:?}"
    );
    assert!(
        summary.executed_steps >= 1,
        "the sync must have executed the media's workflow: {summary:?}"
    );
    assert!(
        summary.materialized_paths >= 1,
        "the sync must have materialized the hierarchy it resolved: {summary:?}"
    );
    assert!(
        summary.warnings.is_empty(),
        "a download the sync carried through must surface nothing it wants the user to read: \
         {summary:?}"
    );
}

/// Asserts the media root carries the artist-prefixed canonical name, and returns it.
///
/// The folder template renders `${media.metadata.artist} - ${media.metadata.title}
/// [${media.id}]`, and the title is media-tagger's `MusicBrainz` recording title, which
/// carries no artist of its own. A run that never landed the tagging step resolves the
/// upstream provider title instead, which appends its own suffixes.
fn assert_canonical_media_root(
    hierarchy_root: &Path,
    expectations: &DemoHierarchyGoldenSpec,
) -> PathBuf {
    let media_folder = hierarchy_root.join(&expectations.media_folder);
    assert!(
        media_folder.is_dir(),
        "the media root must exist at the canonical artist-prefixed path '{}'",
        media_folder.display()
    );
    media_folder
}

/// Asserts the materialization state records the infojson variant's hash.
///
/// The state records one entry per materialized path, naming the media, the variant and the
/// CAS hash. The media document's `variant_hashes` is a pre-seeded field that a sync fills
/// only in its own in-memory copy for hierarchy resolution, so a document read back after a
/// sync still holds none.
fn assert_infojson_variant_recorded(hierarchy_root: &Path, state_path: &Path) {
    let state = load_mediapm_state_document(state_path).expect("load the mediapm state");
    let mut infojson_records = state
        .managed_files
        .iter()
        .filter(|(_, record)| record.variant == INFOJSON_VARIANT)
        .peekable();
    assert!(
        infojson_records.peek().is_some(),
        "the run must record the '{INFOJSON_VARIANT}' variant's hash for the paths it \
         materialized from it, and '{}' records none",
        state_path.display()
    );
    for (relative_path, record) in infojson_records {
        assert_eq!(
            record.media_id, ONLINE_DEMO_MEDIA_ID,
            "the '{INFOJSON_VARIANT}' record for '{relative_path}' must name the demo's media"
        );
        let materialized = hierarchy_root.join(relative_path);
        let bytes = fs::read(&materialized).unwrap_or_else(|error| {
            panic!("the recorded infojson path '{}' must exist: {error}", materialized.display())
        });
        assert_eq!(
            record.hash,
            Hash::from_content(&bytes).to_string(),
            "the '{INFOJSON_VARIANT}' record for '{relative_path}' must name the bytes on disk"
        );
    }
}

/// Asserts the download steps left tool-call instances, each with outputs.
fn assert_yt_dlp_instances_recorded_outputs(conductor_state_path: &Path) {
    let conductor_state =
        decode_state_json(&fs::read(conductor_state_path).expect("read the conductor state"))
            .expect("decode the conductor state");
    let output_counts: Vec<usize> = conductor_state
        .tool_call_instances
        .values()
        .filter(|instance| names_yt_dlp(&instance.tool_call_id))
        .map(|instance| instance.outputs.len())
        .collect();
    assert!(
        !output_counts.is_empty(),
        "the download steps must leave at least one yt-dlp tool-call instance in '{}'",
        conductor_state_path.display()
    );
    assert!(
        output_counts.iter().all(|outputs| *outputs > 0),
        "every yt-dlp instance must have recorded the outputs it produced, got output counts \
         {output_counts:?}"
    );
}

/// Asserts the sync deployed the downloader as one tool entry that carries a payload.
///
/// The sync reconciles managed tools by one identity each, and the content map is what the
/// entry materializes into the execution sandbox — the provider binary and its bundled
/// dependencies. A builtin's entry is the empty case, so an empty map here would be a tool
/// the workflow could invoke and not run.
fn assert_yt_dlp_deployed_with_a_payload(generated_path: &Path) {
    let generated = mediapm_conductor::decode_document(
        &fs::read(generated_path).expect("read the generated conductor document"),
    )
    .expect("decode the generated conductor document");
    let yt_dlp_tool_keys: Vec<&str> =
        generated.tools.keys().filter(|key| names_yt_dlp(key)).map(String::as_str).collect();
    assert_eq!(
        yt_dlp_tool_keys.len(),
        1,
        "the generated document must hold exactly one yt-dlp entry, and it holds \
         {yt_dlp_tool_keys:?}"
    );
    let yt_dlp_key = yt_dlp_tool_keys[0];
    let content_map = &generated.tools[yt_dlp_key].runtime.content_map;
    assert!(
        !content_map.is_empty(),
        "the deployed entry '{yt_dlp_key}' must name the payload files it materializes, and \
         its content map is empty"
    );
}

/// Asserts the library holds the tree the hierarchy contract promises.
///
/// The golden's three `sidecars/links/` entries spell the provider title the e2e test
/// seeds, while a real download is named by yt-dlp from the title upstream reports, so
/// those three files are read off the run's own `info.json` sidecar instead.
fn assert_contract_tree(
    hierarchy_root: &Path,
    expectations: &DemoHierarchyGoldenSpec,
    sidecar_info: &Path,
) {
    let sidecar_links_prefix = format!("{}/sidecars/links/", expectations.media_folder);
    let provider_title = read_infojson_title(sidecar_info);
    let mut expected_tree = expectations.clone();
    expected_tree.required_files.retain(|path| !path.starts_with(&sidecar_links_prefix));
    for extension in SIDECAR_LINK_EXTENSIONS {
        expected_tree.required_files.push(format!(
            "{sidecar_links_prefix}{provider_title} [{ONLINE_DEMO_YT_DLP_VIDEO_ID}].{extension}"
        ));
    }
    assert_tree_under(hierarchy_root, &expected_tree).unwrap_or_else(|error| {
        panic!("the synced library does not hold the tree the hierarchy contract promises: {error}")
    });
}

#[tokio::test]
#[ignore = "downloads from YouTube; run with --ignored when validating the real upstream"]
async fn youtube_regression_against_the_real_upstream() {
    let fixture = fixture_mediapm_ncl();
    assert!(
        fixture.is_file(),
        "the regression's config fixture '{}' is gitignored, because it is written by the \
         demo (`**/examples/artifacts/` in `.gitignore`), so a fresh clone has none: write it \
         with `cargo run --all-features -p mediapm --example mediapm_demo_online` and run \
         this test with `--ignored`",
        fixture.display()
    );

    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    fs::copy(&fixture, root.path().join("mediapm.ncl")).expect("copy mediapm.ncl");

    let mut service = service_at(root.path(), Some("media")).await.expect("create service");
    let summary = service.sync_library(false).await.expect("sync_library");

    let expectations = load_demo_hierarchy_golden_document().online;
    assert_youtube_regression_post_sync(&service, &summary, &expectations);
}
