//! The service hands its workspace metadata cache to the materializer probe.
//!
//! `MediaPmService` opens one `MetadataCache` at construction over the
//! workspace `metadata.json` and threads that same instance into
//! `materializer::sync_hierarchy` during `sync_library`. This module pins the
//! hop end to end: a `${media.metadata.title}` placeholder whose probe answer
//! is already seeded in that file must resolve without spawning `ffprobe`,
//! because the materializer reads the service's cache rather than bypassing
//! it. A service that stops handing its cache over fails the run here - the
//! seeded bytes are deliberately unprobeable - instead of quietly paying a
//! probe per repeat.

use std::collections::BTreeMap;

use crate::common::{seed_cas, sync_library_with_test_terminal};
use bytes::Bytes;
use mediapm::{
    HierarchyNode, HierarchyNodeKind, HierarchyPath, MediaMetadataValue,
    MediaMetadataVariantBinding, MediaPmError, MediaPmPaths, MediaPmService, MediaRuntimeStorage,
    MediaSourceSpec, MediaStep, MediaStepTool, OutputVariantValue, PlaylistFormat,
    SanitizeNamesConfig, TransformInputValue, YtDlpOutputVariantConfig, load_mediapm_document,
    media_id_from_uri, save_mediapm_document,
};
use url::Url;

/// Variant bytes no probe can answer: not JSON, not parseable media.
///
/// Only the seeded cache entry can resolve the template over this payload,
/// which is what makes the test bite when the service stops handing its cache
/// to the materializer: the probe is asked for garbage, fails, and the sync
/// errors instead of passing quietly.
const PROBE_UNANSWERABLE: &[u8] = b"definitely not a JSON probe document";

/// Output variant the import step publishes, which the metadata binding probes.
const VARIANT: &str = "primary";

/// The full service sync resolves a probe-bound template from the workspace
/// cache the service itself opened.
///
/// User-level guarantee: a repeat run's probe answers come from the workspace
/// `metadata.json` through the service, so a `${media.metadata.*}` path does
/// not respawn `ffprobe` for content an earlier run already probed. The seed
/// is written before the service is constructed because `MediaPmService` opens
/// its `MetadataCache` once at construction and the open handle does not
/// re-read the file, so a later write would never reach the instance the
/// materializer is handed. Failure here means the service-to-materializer hop
/// carries no cache: the probe would be asked for unprobeable bytes and the
/// sync would error.
#[tokio::test]
async fn the_service_hands_its_metadata_cache_to_the_materializer() -> Result<(), MediaPmError> {
    let hash = mediapm_cas::Hash::from_content(PROBE_UNANSWERABLE);

    let root = mediapm_utils::temp::artifact_dir().expect("artifact dir");
    let cache_root = mediapm_utils::temp::cache_dir().expect("cache dir");

    let seed_dir = MediaPmPaths::from_root(root.path()).workspace_mediapm_cache_dir();
    std::fs::create_dir_all(&seed_dir).expect("workspace metadata cache dir");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after epoch")
        .as_secs();
    let seeded = serde_json::json!({
        "version": 2,
        "entries": {
            format!("ffprobe:{hash}"): {
                "value": { "format": { "tags": { "title": "ServedFromCache" } } },
                // Fresh enough to survive the TTL prune at open: an expired
                // seed would be discarded before the hop under test reads it.
                "last_access_unix_seconds": now,
            }
        }
    });
    std::fs::write(
        seed_dir.join("metadata.json"),
        serde_json::to_vec_pretty(&seeded).expect("encode seeded metadata cache"),
    )
    .expect("write seeded metadata cache");

    let mut runtime = MediaRuntimeStorage::default();
    runtime.cache_root_override = Some(cache_root.path().to_path_buf());
    let mut service =
        MediaPmService::new_fs_at_with_runtime_storage_overrides(root.path(), runtime).await?;

    seed_cas(&service, Bytes::from_static(PROBE_UNANSWERABLE), "probe payload").await?;

    let uri = Url::parse("local:metadata-probe-service-hop").expect("source URI must parse");
    let media_id = media_id_from_uri(&uri);
    let mut spec = MediaSourceSpec {
        steps: vec![MediaStep {
            tool: MediaStepTool::Import,
            input_variants: Vec::new(),
            output_variants: BTreeMap::from([(
                VARIANT.to_string(),
                OutputVariantValue::YtDlp(YtDlpOutputVariantConfig::default()),
            )]),
            options: BTreeMap::from([
                ("kind".to_string(), TransformInputValue::String("cas_hash".to_string())),
                ("hash".to_string(), TransformInputValue::String(hash.to_string())),
            ]),
        }],
        ..MediaSourceSpec::default()
    };
    spec.metadata = BTreeMap::from([(
        "title".to_string(),
        MediaMetadataValue::Variant(MediaMetadataVariantBinding {
            variant: VARIANT.to_string(),
            metadata_key: "title".to_string(),
            transform: None,
        }),
    )]);
    service.add_media_source(&spec, media_id.clone(), &uri, None, None)?;

    let paths = service.paths().clone();
    let mut document = load_mediapm_document(&paths.mediapm_ncl)?;
    document.hierarchy.push(HierarchyNode {
        path: HierarchyPath::simple("${media.metadata.title}"),
        kind: HierarchyNodeKind::Media,
        id: None,
        media_id: Some(media_id),
        variant: Some(VARIANT.to_string()),
        variants: Vec::new(),
        rename_files: Vec::new(),
        format: PlaylistFormat::M3u8,
        ids: Vec::new(),
        sanitize_names: Some(SanitizeNamesConfig::Inherit),
        children: Vec::new(),
    });
    save_mediapm_document(&paths.mediapm_ncl, &document)?;

    let summary = sync_library_with_test_terminal(&mut service, false).await?;
    assert_eq!(
        summary.missing_paths, 0,
        "the probe-bound entry must be produced by the sync: {summary:?}"
    );

    let materialized = root.path().join("ServedFromCache");
    assert!(
        materialized.is_file(),
        "the service must hand its workspace metadata cache to the materializer, so the seeded \
         answer names the materialized entry instead of ffprobe being asked for unprobeable \
         bytes; workspace {}",
        root.path().display()
    );
    Ok(())
}
