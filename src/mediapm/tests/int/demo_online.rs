//! Integration guardrails for the `demo_online` example wiring.
//!
//! These checks intentionally validate source-level invariants in the example
//! because `demo_online` is compile-only in automated tests (`test = false`) to
//! avoid network/tool-provider dependencies during CI.

/// Verifies `demo_online` uses in-memory service construction so tool sync can
/// open the persistent runtime CAS store without redb lock contention.
#[test]
fn demo_online_uses_in_memory_service_wiring() {
    let source = include_str!("../../examples/demo_online.rs");

    assert!(
        source.contains("MediaPmService::new_in_memory_at("),
        "demo_online must construct MediaPmService with in-memory conductor wiring"
    );

    assert!(
        !source.contains("FileSystemCas::open(&workspace_root.join(\".mediapm\").join(\"store\"))"),
        "demo_online must not pre-open runtime CAS store directly; that can lock redb during tool sync"
    );

    assert!(
        !source.contains("acoustid_api_key"),
        "demo_online must not pass acoustid_api_key through mediapm media step options"
    );

    assert!(
        source.contains("input_variants: Vec::new()"),
        "demo_online should keep yt-dlp input_variants empty"
    );

    assert!(
        source.contains("\"rsgain\".to_string()")
            && source.contains("\"media-tagger\".to_string()"),
        "demo_online should declare managed tool requirements for rsgain and media-tagger"
    );

    assert!(
        source.contains("tool: MediaStepTool::Rsgain")
            && source.contains("tool: MediaStepTool::MediaTagger"),
        "demo_online should include rsgain and media-tagger steps after ffmpeg transforms"
    );

    assert!(
        source.contains("strict_identification") && source.contains("\"true\".to_string()"),
        "demo_online should keep strict media-tagger identification behavior enabled"
    );

    assert!(
        source.contains("output_container") && source.contains("\"mp4\".to_string()"),
        "demo_online should force media-tagger metadata apply to emit mp4 output"
    );

    assert!(
        source.contains("video_codec")
            && source.contains("libx264")
            && source.contains("video_filters")
            && source.contains("scale=-2:144"),
        "demo_online should transcode with x264 at 144p"
    );

    assert!(
        source.contains("const DEMO_SIDECAR_VARIANT_SUFFIXES: [(&str, &str); 12] =")
            && source.contains("(\"thumbnails\", \"thumbnails/\")")
            && source.contains("(\"description\", \"description.txt\")")
            && source.contains("(\"infojson\", \"info.json\")")
            && source.contains("(\"comments\", \"comments/\")")
            && source.contains("(\"playlist_infojson\", \"playlist-infojson/\")"),
        "demo_online should materialize all supported downloader sidecar families"
    );

    assert!(
        source.contains("MediaMetadataValue::Literal")
            && source.contains("MediaMetadataValue::Variant")
            && source.contains("metadata_key: DEMO_METADATA_VIDEO_ID_KEY.to_string()")
            && source.contains("format!(\"{DEMO_HIERARCHY_ROOT_TEMPLATE}/rickroll-144p.mp4\")")
            && source.contains("${media.metadata.title}")
            && source.contains("${media.metadata.video_id}"),
        "demo_online should demonstrate strict metadata object values and hierarchy metadata placeholders"
    );

    assert!(
        !source.contains("\"filename_template\"") && !source.contains("\"output_template\""),
        "demo_online should avoid legacy yt-dlp filename template/output template fields"
    );

    assert!(
        source.contains("demo/Rickroll Demo/dQw4w9WgXcQ/rickroll-144p.mp4")
            && source.contains("demo/Rickroll Demo/dQw4w9WgXcQ/rickroll-144p-tagged.mp4")
            && source.contains("DEMO_HIERARCHY_ROOT_RESOLVED")
            && source.contains("DEMO_EXPECTED_VIDEO_ID"),
        "demo_online should materialize both video outputs and metadata-interpolated downloader sidecars"
    );
}
