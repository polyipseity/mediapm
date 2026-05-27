use std::collections::BTreeMap;

use super::{
    HierarchyEntry, HierarchyEntryKind, MEDIAPM_DOCUMENT_VERSION, MaterializationMethod,
    MediaMetadataValue, MediaMetadataValueCandidate, MediaPmDocument, MediaPmImpureTimestamp,
    MediaPmState, MediaRuntimeStorage, MediaSourceSpec, MediaStep, MediaStepTool, OutputSaveConfig,
    PlaylistEntryPathMode, PlaylistFormat, ToolRequirement, TransformInputValue, Value,
    flatten_hierarchy_nodes_for_runtime, load_mediapm_document, load_mediapm_state_document,
    media_source_uri, resolve_step_variant_flow, save_mediapm_document,
    save_mediapm_state_document,
};

fn hierarchy_flat_map(document: &MediaPmDocument) -> BTreeMap<String, HierarchyEntry> {
    flatten_hierarchy_nodes_for_runtime(&document.hierarchy)
        .expect("flatten hierarchy")
        .into_iter()
        .map(|flattened| (flattened.path, flattened.entry))
        .collect()
}

fn hierarchy_nodes(entries: BTreeMap<String, HierarchyEntry>) -> Vec<super::HierarchyNode> {
    entries
        .into_iter()
        .map(|(path, entry)| match entry.kind {
            HierarchyEntryKind::Media if path.ends_with('/') || path.ends_with('\\') => {
                super::HierarchyNode {
                    path: path.trim_end_matches(['/', '\\']).to_string(),
                    kind: super::HierarchyNodeKind::MediaFolder,
                    id: Some(entry.media_id.clone()),
                    media_id: Some(entry.media_id),
                    variant: None,
                    variants: entry.variants,
                    rename_files: entry.rename_files,
                    format: PlaylistFormat::M3u8,
                    ids: Vec::new(),
                    children: Vec::new(),
                }
            }
            HierarchyEntryKind::Media => super::HierarchyNode {
                path,
                kind: super::HierarchyNodeKind::Media,
                id: Some(entry.media_id.clone()),
                media_id: Some(entry.media_id),
                variant: entry.variants.first().cloned(),
                variants: Vec::new(),
                rename_files: Vec::new(),
                format: PlaylistFormat::M3u8,
                ids: Vec::new(),
                children: Vec::new(),
            },
            HierarchyEntryKind::MediaFolder => super::HierarchyNode {
                path,
                kind: super::HierarchyNodeKind::MediaFolder,
                id: Some(entry.media_id.clone()),
                media_id: Some(entry.media_id),
                variant: None,
                variants: entry.variants,
                rename_files: entry.rename_files,
                format: PlaylistFormat::M3u8,
                ids: Vec::new(),
                children: Vec::new(),
            },
            HierarchyEntryKind::Playlist => super::HierarchyNode {
                path,
                kind: super::HierarchyNodeKind::Playlist,
                id: None,
                media_id: None,
                variant: None,
                variants: Vec::new(),
                rename_files: Vec::new(),
                format: entry.format,
                ids: entry.ids,
                children: Vec::new(),
            },
        })
        .collect()
}

/// Protects flat-to-node conversion helper semantics used by migration-
/// period Rust callsites by covering `media`, `media_folder`, and
/// `playlist`
/// entry mapping behavior.
#[test]
fn hierarchy_nodes_from_flat_entries_converts_all_supported_kinds() {
    let entries = BTreeMap::from([
        (
            "library/video.mkv".to_string(),
            HierarchyEntry {
                kind: HierarchyEntryKind::Media,
                media_id: "demo".to_string(),
                variants: vec!["video".to_string()],
                rename_files: Vec::new(),
                format: PlaylistFormat::M3u8,
                ids: Vec::new(),
            },
        ),
        (
            "library/subtitles/".to_string(),
            HierarchyEntry {
                kind: HierarchyEntryKind::MediaFolder,
                media_id: "demo".to_string(),
                variants: vec!["subtitles".to_string()],
                rename_files: Vec::new(),
                format: PlaylistFormat::M3u8,
                ids: Vec::new(),
            },
        ),
        (
            "library/mixed.m3u8".to_string(),
            HierarchyEntry {
                kind: HierarchyEntryKind::Playlist,
                media_id: String::new(),
                variants: Vec::new(),
                rename_files: Vec::new(),
                format: PlaylistFormat::M3u8,
                ids: vec![super::PlaylistItemRef {
                    id: "demo".to_string(),
                    path: PlaylistEntryPathMode::Relative,
                }],
            },
        ),
    ]);

    let nodes = super::hierarchy_nodes_from_flat_entries(&entries)
        .expect("flat hierarchy entries should convert to node-list form");

    assert_eq!(nodes.len(), 3);

    let media = nodes
        .iter()
        .find(|node| node.path == "library/video.mkv")
        .expect("media node should exist");
    assert!(matches!(media.kind, super::HierarchyNodeKind::Media));
    assert_eq!(media.media_id.as_deref(), Some("demo"));
    assert_eq!(media.variant.as_deref(), Some("video"));

    let media_folder = nodes
        .iter()
        .find(|node| node.path == "library/subtitles")
        .expect("media_folder node should exist");
    assert!(matches!(media_folder.kind, super::HierarchyNodeKind::MediaFolder));
    assert_eq!(media_folder.media_id.as_deref(), Some("demo"));
    assert_eq!(media_folder.variants, vec!["subtitles".to_string()]);

    let playlist = nodes
        .iter()
        .find(|node| node.path == "library/mixed.m3u8")
        .expect("playlist node should exist");
    assert!(matches!(playlist.kind, super::HierarchyNodeKind::Playlist));
    assert!(playlist.media_id.is_none());
    assert_eq!(playlist.ids.len(), 1);
    assert_eq!(playlist.ids[0].id(), "demo");
}

/// Protects round-trip persistence semantics for `mediapm.ncl` defaults.
#[test]
fn mediapm_document_round_trip_preserves_schema_version() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let document = MediaPmDocument::default();

    save_mediapm_document(&path, &document).expect("save mediapm.ncl");
    let decoded = load_mediapm_document(&path).expect("load mediapm.ncl");

    assert_eq!(decoded.version, MEDIAPM_DOCUMENT_VERSION);
}

/// Protects Nickel rendering by quoting reserved field names such as
/// `import` so saved documents round-trip through Nickel evaluation.
#[test]
fn save_document_quotes_nickel_reserved_tool_key_import() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let mut document = MediaPmDocument::default();
    document.tools.insert(
        "import".to_string(),
        ToolRequirement {
            version: None,
            tag: None,
            dependencies: super::ToolRequirementDependencies::default(),
            recheck_seconds: None,
            max_input_slots: None,
            max_output_slots: None,
        },
    );

    save_mediapm_document(&path, &document).expect("save mediapm.ncl");
    let rendered = std::fs::read_to_string(&path).expect("read rendered mediapm.ncl");
    assert!(
        rendered.contains("\"import\" = {") || rendered.contains("'import' = {"),
        "reserved key must be quoted in rendered Nickel"
    );

    let decoded = load_mediapm_document(&path).expect("load mediapm.ncl");
    assert!(decoded.tools.contains_key("import"));
}

/// Protects Nickel numeric rendering by emitting integral ffmpeg `idx`
/// values without trailing decimal notation.
#[test]
fn save_document_renders_integral_output_variant_idx_without_decimal() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let mut document = MediaPmDocument::default();

    document.media.insert(
        "demo".to_string(),
        MediaSourceSpec {
            id: None,
            description: None,
            title: None,
            workflow_id: None,
            metadata: None,
            variant_hashes: BTreeMap::new(),
            steps: vec![MediaStep {
                tool: MediaStepTool::Ffmpeg,
                input_variants: vec!["video".to_string()],
                output_variants: BTreeMap::from([(
                    "video".to_string(),
                    serde_json::json!({
                        "kind": "primary",
                        "idx": 0,
                        "extension": "mkv",
                    }),
                )]),
                options: BTreeMap::new(),
            }],
        },
    );

    save_mediapm_document(&path, &document).expect("save mediapm.ncl");
    let rendered = std::fs::read_to_string(&path).expect("read rendered mediapm.ncl");

    assert!(rendered.contains("idx = 0,"));
    assert!(!rendered.contains("idx = 0.0"));
}

/// Protects machine-managed state persistence shape and round-trip decode.
#[test]
fn mediapm_state_document_round_trip_is_state_only() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("state.ncl");
    let mut state = MediaPmState::default();
    state.active_tools.insert("ffmpeg".to_string(), "tool-id".to_string());

    save_mediapm_state_document(&path, &state).expect("save state.ncl");
    let decoded = load_mediapm_state_document(&path).expect("load state.ncl");
    let rendered = std::fs::read_to_string(&path).expect("read state.ncl");

    assert_eq!(decoded, state);
    assert!(rendered.contains("version = 1"));
    assert!(rendered.contains("state = {"));
    assert!(!rendered.lines().any(|line| line.trim_start().starts_with("runtime =")));
    assert!(!rendered.lines().any(|line| line.trim_start().starts_with("tools =")));
    assert!(!rendered.lines().any(|line| line.trim_start().starts_with("media =")));
    assert!(!rendered.lines().any(|line| line.trim_start().starts_with("hierarchy =")));
}

/// Protects workflow-step refresh state persistence by round-tripping
/// explicit step config snapshots and mediapm-managed impure timestamps.
#[test]
fn mediapm_state_round_trip_preserves_workflow_states() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("state.ncl");
    let mut state = MediaPmState::default();
    state.workflow_states.insert(
        "demo-media".to_string(),
        vec![
            super::ManagedWorkflowStepState {
                explicit_config: serde_json::json!({
                    "tool": "yt-dlp",
                    "output_variants": {
                        "default": { "kind": "primary", "save": "full" }
                    },
                    "options": { "uri": "https://example.com/video" }
                }),
                impure_timestamp: Some(MediaPmImpureTimestamp {
                    epoch_seconds: 123,
                    subsec_nanos: 456,
                }),
            },
            super::ManagedWorkflowStepState {
                explicit_config: serde_json::json!({
                    "tool": "rsgain",
                    "input_variants": ["default"],
                    "output_variants": {
                        "default": { "kind": "primary", "save": "full" }
                    },
                    "options": {}
                }),
                impure_timestamp: None,
            },
        ],
    );

    save_mediapm_state_document(&path, &state).expect("save state.ncl");
    let decoded = load_mediapm_state_document(&path).expect("load state.ncl");
    let rendered = std::fs::read_to_string(&path).expect("read state.ncl");

    assert_eq!(decoded, state);
    assert!(rendered.contains("workflow_states = {"));
}

/// Protects strict state-file shape by rejecting non-state top-level keys.
#[test]
fn mediapm_state_document_rejects_non_state_top_level_fields() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("state.ncl");
    let source = r#"
{
version = 1,
runtime = {
    mediapm_dir = ".mediapm-custom",
},
state = {
    active_tools = {
        ffmpeg = "tool-id",
    },
},
}
"#;

    std::fs::write(&path, source).expect("write state.ncl");
    let err = load_mediapm_state_document(&path)
        .expect_err("state.ncl with runtime section must fail shape validation");

    assert!(
        err.to_string().contains("must contain only top-level 'version' and 'state' properties")
    );
}

/// Protects node-list hierarchy decode by flattening recursive folder nodes
/// into runtime flat-path entries while preserving directory/file targets.
#[test]
fn hierarchy_nested_nodes_flatten_into_runtime_paths() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    video = { kind = "primary", save = "full" },
                    subtitles = { kind = "subtitles", save = "full" },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
hierarchy = [
    {
        path = "library",
        children = [
            {
                path = "artist",
                children = [
                    {
                        path = "video.mkv",
                        kind = "media",
                        id = "demo-video",
                        media_id = "demo",
                        variant = "video",
                    },
                    {
                        path = "subtitles",
                        kind = "media_folder",
                        id = "demo-subtitles",
                        media_id = "demo",
                        variants = ["subtitles"],
                    },
                ],
            },
        ],
    },
],
}
"#;

    std::fs::write(&path, source).expect("write source");
    let document = load_mediapm_document(&path).expect("decode nested hierarchy document");

    let hierarchy = hierarchy_flat_map(&document);
    assert!(hierarchy.contains_key("library/artist/video.mkv"));
    assert!(hierarchy.contains_key("library/artist/subtitles"));
}

/// Protects hierarchy defaults by treating omitted `kind` as structural
/// folder nodes.
#[test]
fn hierarchy_nested_nodes_default_to_folder_kind_when_kind_is_omitted() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    video = { kind = "primary", save = "full" },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
hierarchy = [
    {
        path = "top",
        children = [
            {
                path = "middle",
                children = [
                    {
                        path = "final.mkv",
                        kind = "media",
                        id = "demo-final",
                        media_id = "demo",
                        variant = "video",
                    },
                ],
            },
        ],
    },
],
}
"#;

    std::fs::write(&path, source).expect("write source");
    let document = load_mediapm_document(&path).expect("decode nested hierarchy document");

    assert!(hierarchy_flat_map(&document).contains_key("top/middle/final.mkv"));
}

/// Protects node-kind typing by requiring media leaf declarations to set
/// `kind = "media"`.
#[test]
fn hierarchy_nested_leaf_requires_kind_marker_for_media_or_playlist() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    video = { kind = "primary", save = "full" },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
hierarchy = [
    {
        path = "library/video.mkv",
        media_id = "demo",
        variant = "video",
    },
],
}
"#;

    std::fs::write(&path, source).expect("write source");
    let err =
        load_mediapm_document(&path).expect_err("media leaf without explicit kind should fail");

    assert!(err.to_string().contains("kind 'folder' must not define 'variant'"));
}

/// Ensures configured hierarchy path literals are rejected when segments
/// are not Unicode NFD normalized.
#[test]
fn hierarchy_path_rejects_non_nfd_literal_segments() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    video = { kind = "primary", save = "full" },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
hierarchy = [
    {
        path = "library/épisode.mkv",
        kind = "media",
        id = "demo-video",
        media_id = "demo",
        variant = "video",
    },
],
}
"#;

    std::fs::write(&path, source).expect("write source");
    let err = load_mediapm_document(&path).expect_err("non-NFD hierarchy path should fail decode");
    assert!(err.to_string().contains("must be Unicode NFD normalized"));
}

/// Ensures configured hierarchy path templates still enforce NFD on the
/// literal path text around placeholders.
#[test]
fn hierarchy_path_template_rejects_non_nfd_literal_segments() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    demo = {
        metadata = {
            artist = "demo",
        },
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    video = { kind = "primary", save = "full" },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
hierarchy = [
    {
        path = "library/${media.metadata.artist}/épisode.mkv",
        kind = "media",
        id = "demo-video",
        media_id = "demo",
        variant = "video",
    },
],
}
"#;

    std::fs::write(&path, source).expect("write source");
    let err = load_mediapm_document(&path)
        .expect_err("non-NFD template literal path segment should fail decode");
    assert!(err.to_string().contains("must be Unicode NFD normalized"));
}

/// Protects persistence rendering by serializing hierarchy as ordered node
/// arrays with explicit `kind`/`path` fields.
#[test]
fn save_mediapm_document_emits_nested_hierarchy_kind_field() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let mut document = MediaPmDocument::default();

    document.media.insert(
        "demo".to_string(),
        MediaSourceSpec {
            id: None,
            description: None,
            title: None,
            workflow_id: None,
            metadata: None,
            variant_hashes: BTreeMap::from([(
                "video".to_string(),
                "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_string(),
            )]),
            steps: Vec::new(),
        },
    );

    document.hierarchy = hierarchy_nodes(BTreeMap::from([(
        "library/demo.mkv".to_string(),
        HierarchyEntry {
            kind: HierarchyEntryKind::Media,
            media_id: "demo".to_string(),
            variants: vec!["video".to_string()],
            rename_files: Vec::new(),
            format: PlaylistFormat::M3u8,
            ids: Vec::new(),
        },
    )]));

    save_mediapm_document(&path, &document).expect("save hierarchy node-list document");
    let rendered = std::fs::read_to_string(&path).expect("read rendered mediapm.ncl");

    assert!(rendered.contains("kind = \"media\""));
    assert!(rendered.contains("path = \"library/demo.mkv\""));

    let decoded = load_mediapm_document(&path).expect("decode rendered hierarchy node-list");
    assert!(hierarchy_flat_map(&decoded).contains_key("library/demo.mkv"));
}

/// Protects tool requirement decoding for explicit version/tag selectors.
#[test]
fn tool_requirements_decode_with_version_or_tag_selectors() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
  version = 1,
  tools = {
        ffmpeg = { version = "8.2" },
                    rsgain = { version = "3.7.0", tag = "v3.7.0", dependencies = { ffmpeg_version = "inherit", sd_version = "inherit" } },
                                            "media-tagger" = { tag = "latest", dependencies = { ffmpeg_version = "inherit" } },
                                            "yt-dlp" = { tag = "v2026.04.01", dependencies = { ffmpeg_version = "inherit" }, recheck_seconds = 3600 },
  },
}
"#;

    std::fs::write(&path, source).expect("write source");
    let document = load_mediapm_document(&path).expect("decode document");

    assert_eq!(document.tools["ffmpeg"].version.as_deref(), Some("8.2"));
    assert!(document.tools["ffmpeg"].tag.is_none());
    assert!(document.tools["yt-dlp"].version.is_none());
    assert_eq!(document.tools["yt-dlp"].tag.as_deref(), Some("v2026.04.01"));
    assert_eq!(document.tools["yt-dlp"].recheck_seconds, Some(3600));
    assert_eq!(document.tools["yt-dlp"].dependencies.ffmpeg_version.as_deref(), Some("inherit"));
    assert_eq!(document.tools["rsgain"].version.as_deref(), Some("3.7.0"));
    assert_eq!(document.tools["rsgain"].tag.as_deref(), Some("v3.7.0"));
    assert_eq!(document.tools["rsgain"].dependencies.ffmpeg_version.as_deref(), Some("inherit"));
    assert_eq!(document.tools["rsgain"].dependencies.sd_version.as_deref(), Some("inherit"));
    assert_eq!(
        document.tools["media-tagger"].dependencies.ffmpeg_version.as_deref(),
        Some("inherit")
    );
}

/// Protects tool-requirement schema by rejecting ffmpeg selector overrides
/// on unsupported logical tools.
#[test]
fn unsupported_tool_rejects_ffmpeg_version_selector() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
  version = 1,
  tools = {
archive = { tag = "latest", dependencies = { ffmpeg_version = "inherit" } },
  },
}
"#;

    std::fs::write(&path, source).expect("write source");
    let err =
        load_mediapm_document(&path).expect_err("ffmpeg_version on unsupported tool should fail");
    assert!(
        err.to_string().contains("must not define dependency selector overrides"),
        "unexpected error: {err}"
    );
}

/// Protects grouped dependency selector support for rsgain workflows.
#[test]
fn rsgain_accepts_grouped_dependency_selectors() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
  version = 1,
  tools = {
rsgain = { tag = "latest", dependencies = { ffmpeg_version = "inherit", sd_version = "inherit" } },
  },
}
"#;

    std::fs::write(&path, source).expect("write source");
    let document =
        load_mediapm_document(&path).expect("rsgain dependencies should pass validation");
    assert_eq!(document.tools["rsgain"].dependencies.ffmpeg_version.as_deref(), Some("inherit"));
    assert_eq!(document.tools["rsgain"].dependencies.sd_version.as_deref(), Some("inherit"));
}

/// Protects yt-dlp output-variant schema by requiring `format` to be set
/// in step `options`, not inside output-variant config objects.
#[test]
fn yt_dlp_output_variant_rejects_format_field() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    video = {
                        kind = "primary",
                        save = "full",
                        format = "bestvideo*+bestaudio/best",
                    },
                },
                options = { uri = "https://example.com/video" },
            },
        ],
    },
},
}
"#;

    std::fs::write(&path, source).expect("write source");
    let err = load_mediapm_document(&path).expect_err("format field must be rejected");
    assert!(err.to_string().contains("unknown field `format`"));
}

/// Protects runtime-storage decode for shared user-cache policy toggle.
#[test]
fn runtime_storage_decodes_use_user_tool_cache_toggle() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r"
{
version = 1,
runtime = {
    use_user_tool_cache = false,
},
}
";

    std::fs::write(&path, source).expect("write source");
    let document = load_mediapm_document(&path).expect("decode document");

    assert_eq!(document.runtime.use_user_tool_cache, Some(false));
    assert!(!document.runtime.use_user_tool_cache_enabled());
}

/// Protects runtime-storage decode for explicit dotenv file overrides.
#[test]
fn runtime_storage_decodes_env_file_override() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
runtime = {
    env_file = ".mediapm/.env.custom",
},
}
"#;

    std::fs::write(&path, source).expect("write source");
    let document = load_mediapm_document(&path).expect("decode document");

    assert_eq!(document.runtime.env_file.as_deref(), Some(".mediapm/.env.custom"));
}

/// Protects tool-requirement decode for ffmpeg slot-limit overrides.
#[test]
fn tool_requirements_decode_ffmpeg_slot_limits_on_ffmpeg_tool() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
tools = {
    ffmpeg = {
        version = "latest",
        max_input_slots = 96,
        max_output_slots = 80,
    },
},
}
"#;

    std::fs::write(&path, source).expect("write source");
    let document = load_mediapm_document(&path).expect("decode document");

    assert_eq!(document.tools["ffmpeg"].max_input_slots, Some(96));
    assert_eq!(document.tools["ffmpeg"].max_output_slots, Some(80));
    assert_eq!(document.tools["ffmpeg"].max_input_slots_or_default(), 96);
    assert_eq!(document.tools["ffmpeg"].max_output_slots_or_default(), 80);
}

/// Protects runtime-storage decode for platform-keyed inherited env vars.
#[test]
fn runtime_storage_decodes_platform_inherited_env_vars() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
runtime = {
    inherited_env_vars = {
        windows = ["ComSpec", "Path"],
        linux = ["PATH"],
    },
},
}
"#;

    std::fs::write(&path, source).expect("write source");
    let document = load_mediapm_document(&path).expect("decode document");

    let inherited =
        document.runtime.inherited_env_vars.as_ref().expect("inherited env map should decode");
    assert_eq!(inherited.get("windows"), Some(&vec!["ComSpec".to_string(), "Path".to_string()]));
    assert_eq!(inherited.get("linux"), Some(&vec!["PATH".to_string()]));
}

/// Protects runtime materialization policy decoding for ordered methods.
#[test]
fn runtime_storage_decodes_materialization_preference_order() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
runtime = {
    materialization_preference_order = ["copy", "hardlink"],
},
}
"#;

    std::fs::write(&path, source).expect("write source");
    let document = load_mediapm_document(&path).expect("decode document");

    assert_eq!(
        document.runtime.materialization_preference_order,
        Some(vec![MaterializationMethod::Copy, MaterializationMethod::Hardlink])
    );
}

/// Protects runtime materialization policy by rejecting duplicate methods.
#[test]
fn runtime_storage_rejects_duplicate_materialization_preference_order() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
runtime = {
    materialization_preference_order = ["hardlink", "copy", "hardlink"],
},
}
"#;

    std::fs::write(&path, source).expect("write source");
    let error =
        load_mediapm_document(&path).expect_err("duplicate materialization methods must fail");
    assert!(
        error
            .to_string()
            .contains("runtime.materialization_preference_order contains duplicate method")
    );
}

/// Protects runtime materialization policy by rejecting empty method lists.
#[test]
fn runtime_storage_rejects_empty_materialization_preference_order() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r"
{
version = 1,
runtime = {
    materialization_preference_order = [],
},
}
";

    std::fs::write(&path, source).expect("write source");
    let error = load_mediapm_document(&path).expect_err("empty materialization methods must fail");
    assert!(
        error
            .to_string()
            .contains("runtime.materialization_preference_order must contain at least one method")
    );
}

/// Protects host-platform filtering when resolving inherited env names.
#[test]
fn inherited_env_vars_with_defaults_reads_only_host_platform() {
    let runtime = MediaRuntimeStorage {
        inherited_env_vars: Some(BTreeMap::from([
            ("windows".to_string(), vec!["SYSTEMROOT".to_string(), "ComSpec".to_string()]),
            ("linux".to_string(), vec!["LD_LIBRARY_PATH".to_string()]),
            ("macos".to_string(), vec!["DYLD_LIBRARY_PATH".to_string()]),
        ])),
        ..MediaRuntimeStorage::default()
    };

    let resolved = runtime.inherited_env_vars_with_defaults();

    if cfg!(windows) {
        assert!(resolved.iter().any(|value| value.eq_ignore_ascii_case("ComSpec")));
        assert!(!resolved.iter().any(|value| value.eq_ignore_ascii_case("LD_LIBRARY_PATH")));
        assert!(!resolved.iter().any(|value| value.eq_ignore_ascii_case("DYLD_LIBRARY_PATH")));
    } else if cfg!(target_os = "linux") {
        assert!(resolved.iter().any(|value| value.eq_ignore_ascii_case("LD_LIBRARY_PATH")));
        assert!(!resolved.iter().any(|value| value.eq_ignore_ascii_case("ComSpec")));
        assert!(!resolved.iter().any(|value| value.eq_ignore_ascii_case("DYLD_LIBRARY_PATH")));
    } else if cfg!(target_os = "macos") {
        assert!(resolved.iter().any(|value| value.eq_ignore_ascii_case("DYLD_LIBRARY_PATH")));
        assert!(!resolved.iter().any(|value| value.eq_ignore_ascii_case("ComSpec")));
        assert!(!resolved.iter().any(|value| value.eq_ignore_ascii_case("LD_LIBRARY_PATH")));
    }
}

/// Protects default cache policy when runtime-storage toggle is omitted.
#[test]
fn runtime_storage_defaults_to_enabled_shared_download_cache() {
    let runtime_storage = MediaRuntimeStorage::default();
    assert!(runtime_storage.use_user_tool_cache_enabled());
}

/// Protects runtime materialization policy defaults when runtime value is omitted.
#[test]
fn runtime_storage_defaults_materialization_preference_order() {
    let runtime_storage = MediaRuntimeStorage::default();
    assert_eq!(
        runtime_storage.materialization_preference_order_with_defaults(),
        vec![
            MaterializationMethod::Hardlink,
            MaterializationMethod::Symlink,
            MaterializationMethod::Reflink,
            MaterializationMethod::Copy,
        ]
    );
}

/// Protects ffmpeg slot-limit validation by rejecting zero input slots.
#[test]
fn tool_requirements_reject_zero_ffmpeg_input_slots() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
tools = {
    ffmpeg = {
        version = "latest",
        max_input_slots = 0,
    },
},
}
"#;

    std::fs::write(&path, source).expect("write source");
    let err = load_mediapm_document(&path).expect_err("zero input slots must fail");
    assert!(err.to_string().contains("tools.ffmpeg.max_input_slots"));
}

/// Protects ffmpeg slot-limit validation by rejecting zero output slots.
#[test]
fn tool_requirements_reject_zero_ffmpeg_output_slots() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
tools = {
    ffmpeg = {
        version = "latest",
        max_output_slots = 0,
    },
},
}
"#;

    std::fs::write(&path, source).expect("write source");
    let err = load_mediapm_document(&path).expect_err("zero output slots must fail");
    assert!(err.to_string().contains("tools.ffmpeg.max_output_slots"));
}

/// Protects tool-requirement validation by rejecting ffmpeg slot settings
/// on non-ffmpeg logical tools.
#[test]
fn non_ffmpeg_tools_reject_ffmpeg_slot_settings() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
tools = {
    "yt-dlp" = {
        version = "latest",
        max_input_slots = 72,
    },
},
}
"#;

    std::fs::write(&path, source).expect("write source");
    let err = load_mediapm_document(&path).expect_err("non-ffmpeg slot settings must be rejected");
    assert!(err.to_string().contains("must not define ffmpeg slot settings"));
}

/// Protects no-backward-compat policy by rejecting legacy ffmpeg slot key
/// names under `tools.ffmpeg`.
#[test]
fn tools_ffmpeg_rejects_legacy_ffmpeg_slot_key_names() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
tools = {
    ffmpeg = {
        version = "latest",
        ffmpeg_max_input_slots = 96,
    },
},
}
"#;

    std::fs::write(&path, source).expect("write source");
    let err =
        load_mediapm_document(&path).expect_err("legacy tools.ffmpeg slot key should be rejected");
    assert!(err.to_string().contains("ffmpeg_max_input_slots"));
}

/// Protects no-backward-compat migration policy by rejecting legacy
/// ffmpeg slot settings under `runtime` via strict unknown-field decoding.
#[test]
fn runtime_rejects_legacy_ffmpeg_slot_keys() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
runtime = {
    ffmpeg_max_input_slots = 96,
},
tools = {
    ffmpeg = { version = "latest" },
},
}
"#;

    std::fs::write(&path, source).expect("write source");
    let err = load_mediapm_document(&path).expect_err("legacy runtime ffmpeg key must be rejected");
    assert!(err.to_string().contains("ffmpeg_max_input_slots"));
}

/// Protects renamed runtime key policy by rejecting legacy key spelling
/// through strict top-level unknown-field decoding.
#[test]
fn runtime_storage_key_is_rejected_after_runtime_rename() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r"
{
version = 1,
runtime_storage = {
    use_user_tool_cache = false,
},
}
";

    std::fs::write(&path, source).expect("write source");
    let err = load_mediapm_document(&path).expect_err("legacy runtime_storage key must fail");
    assert!(err.to_string().contains("runtime_storage"));
}

/// Protects no-backward-compat policy by rejecting removed
/// yt-dlp output-variant `filename_template` fields.
#[test]
fn yt_dlp_output_variant_rejects_filename_template_field() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    "subtitles/" = {
                        kind = "subtitles",
                        save = "full",
                        filename_template = "%(title)s [%(id)s].%(ext)s",
                    },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
}
"#;

    std::fs::write(&path, source).expect("write source");
    let err = load_mediapm_document(&path).expect_err("filename_template must be rejected");

    assert!(err.to_string().contains("unknown field `filename_template`"));
}

/// Protects selector validation by requiring at least one version/tag entry.
#[test]
fn tool_requirements_reject_missing_version_and_tag() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r"
{
  version = 1,
  tools = {
  ffmpeg = {},
  },
}
";

    std::fs::write(&path, source).expect("write source");
    let err = load_mediapm_document(&path).expect_err("document should fail validation");
    assert!(err.to_string().contains("must define at least one selector"));
}

/// Protects selector validation by rejecting mismatched version/tag pairs.
#[test]
fn tool_requirements_reject_mismatched_version_and_tag() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
  version = 1,
  tools = {
  ffmpeg = { version = "8.2", tag = "v8.1" },
  },
}
"#;

    std::fs::write(&path, source).expect("write source");
    let err = load_mediapm_document(&path).expect_err("document should fail validation");
    assert!(err.to_string().contains("mismatched version"));
}

/// Protects online-step schema by requiring explicit `options.uri`.
#[test]
fn online_step_requires_options_uri() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = { default = { kind = "primary", save = "full" } },
            },
        ],
    },
},
}
"#;

    std::fs::write(&path, source).expect("write source");
    let err = load_mediapm_document(&path).expect_err("document should fail validation");
    assert!(err.to_string().contains("must define options.uri"));
}

/// Protects simplified boolean-option semantics by accepting non-`true`
/// values and deferring enablement checks to runtime command templates.
#[test]
fn online_step_write_description_accepts_non_true_values() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = { default = { kind = "primary", save = "full" } },
                options = {
                    uri = "https://example.com/video",
                    write_description = "false",
                },
            },
        ],
    },
},
}
"#;

    std::fs::write(&path, source).expect("write source");
    let document = load_mediapm_document(&path).expect("document should decode");

    assert_eq!(
        document.media["demo"].steps[0].options.get("write_description").map(|value| match value {
            TransformInputValue::String(value) => value.as_str(),
        }),
        Some("false"),
    );
}

/// Protects step option validation by rejecting undeclared keys.
#[test]
fn step_options_reject_unknown_keys() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    local_demo = {
        variant_hashes = { default = "blake3:abc" },
        steps = [
            {
                tool = "ffmpeg",
                input_variants = ["default"],
                output_variants = { default = { kind = "primary", save = "full", idx = 0 } },
                options = { unsupported = "yes" },
            },
        ],
    },
},
}
"#;

    std::fs::write(&path, source).expect("write source");
    let err = load_mediapm_document(&path).expect_err("document should fail validation");
    assert!(err.to_string().contains("unsupported option 'unsupported'"));
}

/// Protects unified subtitle option semantics by rejecting legacy
/// `write_auto_subs` step options.
#[test]
fn step_options_reject_legacy_write_auto_subs_key() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    remote_demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = { downloaded = { kind = "primary", save = "full" } },
                options = {
                    uri = "https://example.com/video",
                    write_auto_subs = "true",
                },
            },
        ],
    },
},
}
"#;

    std::fs::write(&path, source).expect("write source");
    let err = load_mediapm_document(&path).expect_err("legacy write_auto_subs option must fail");
    assert!(err.to_string().contains("unsupported option 'write_auto_subs'"));
}

/// Protects expanded step-option allowlists so audited CLI keys are
/// accepted for all managed media tools.
#[test]
fn step_options_accept_expanded_tool_keys() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    remote_demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = { downloaded = { kind = "primary", save = "full" } },
                options = {
                    uri = "https://example.com/video",
                    merge_output_format = "mkv",
                    format_sort = "res,codec",
                    cache_dir = "./cache/yt-dlp",
                    playlist_items = "1:3",
                    sleep_subtitles = "60",
                    skip_download = "true",
                },
            },
            {
                tool = "ffmpeg",
                input_variants = ["downloaded"],
                output_variants = { normalized = { kind = "primary", save = "full", idx = 0 } },
                options = {
                    audio_quality = "2",
                    map = "0:a:0",
                    map_channel = "0.0.0",
                    id3v2_version = "3",
                },
            },
            {
                tool = "rsgain",
                input_variants = ["normalized"],
                output_variants = { gained = { kind = "primary", save = "full" } },
                options = {
                    tagmode = "i",
                    clip_mode = "p",
                    true_peak = "true",
                    preserve_mtimes = "true",
                },
            },
            {
                tool = "media-tagger",
                input_variants = ["gained"],
                output_variants = { tagged = { kind = "primary", save = "full" } },
                options = {
                    strict_identification = "false",
                    cache_dir = "./cache",
                    cache_expiry_seconds = "86400",
                    musicbrainz_endpoint = "https://musicbrainz.org/ws/2",
                    output_container = "mp4",
                },
            },
        ],
    },
},
}
"#;

    std::fs::write(&path, source).expect("write source");
    let document = load_mediapm_document(&path).expect("document should decode");

    assert_eq!(document.media["remote_demo"].steps.len(), 4);
}

/// Protects scalar-first option typing by rejecting list values for
/// non-list option keys.
#[test]
fn step_options_reject_list_value_for_scalar_option_key() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    local_demo = {
        variant_hashes = { source = "blake3:abc" },
        steps = [
            {
                tool = "ffmpeg",
                input_variants = ["source"],
                output_variants = { normalized = { kind = "primary", save = "full", idx = 0 } },
                options = {
                    audio_quality = ["2"],
                },
            },
        ],
    },
},
}
"#;

    std::fs::write(&path, source).expect("write source");
    let err = load_mediapm_document(&path).expect_err("document should fail validation");
    assert!(!err.to_string().trim().is_empty());
}

/// Protects strict output-variant schema by rejecting non-object values
/// for non-yt-dlp tools.
#[test]
fn non_yt_dlp_output_variant_rejects_string_shorthand() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    local_demo = {
        variant_hashes = { source = "blake3:abc" },
        steps = [
            {
                tool = "ffmpeg",
                input_variants = ["source"],
                output_variants = { normalized = "primary" },
            },
        ],
    },
},
}
"#;

    std::fs::write(&path, source).expect("write source");
    let err = load_mediapm_document(&path).expect_err("document should fail validation");
    assert!(err.to_string().contains("must be an object with at least field 'kind'"));
}

/// Protects value-explicit output semantics by rejecting empty-object
/// output-variant values for single-output simple tools.
#[test]
fn single_output_simple_tool_rejects_empty_object_output_variant() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    local_demo = {
        variant_hashes = { source = "blake3:abc" },
        steps = [
            {
                tool = "ffmpeg",
                input_variants = ["source"],
                output_variants = { normalized = {} },
            },
        ],
    },
},
}
"#;

    std::fs::write(&path, source).expect("write source");
    let err = load_mediapm_document(&path).expect_err("document should fail validation");
    let error_text = err.to_string();
    assert!(error_text.contains("required fields") || error_text.contains("missing field `kind`"));
}

/// Protects per-step variant-flow decoding and string option decoding.
#[test]
fn media_step_supports_variant_flow_and_string_options() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    local_demo = {
        variant_hashes = { source = "blake3:abc" },
        steps = [
            {
                tool = "ffmpeg",
                input_variants = ["source"],
                output_variants = { aac = { kind = "primary", save = "full", idx = 0 } },
                options = {
                    option_args = "-vn",
                    leading_args = "-hide_banner",
                    trailing_args = "-c:a aac",
                },
            },
        ],
    },
},
}
"#;

    std::fs::write(&path, source).expect("write source");
    let document = load_mediapm_document(&path).expect("decode document");
    let step = &document.media["local_demo"].steps[0];
    let flow = resolve_step_variant_flow(step).expect("resolve flow");

    assert_eq!(flow.len(), 1);
    assert_eq!(flow[0].input, "source");
    assert_eq!(flow[0].output, "aac");
    assert!(step.options.contains_key("leading_args"));
    assert!(step.options.contains_key("trailing_args"));
}

/// Protects key-agnostic semantics by allowing deep slash-separated output
/// variant names when values are valid.
#[test]
fn output_variants_allow_more_than_one_slash_in_keys() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = { "subtitles/en/srt" = { kind = "primary", save = "full" } },
                options = { uri = "https://example.com/video" },
            },
        ],
    },
},
}
"#;

    std::fs::write(&path, source).expect("write source");
    let document = load_mediapm_document(&path).expect("document should decode");
    assert!(document.media["demo"].steps[0].output_variants.contains_key("subtitles/en/srt"));
}

/// Protects yt-dlp output config decoding by requiring object values.
#[test]
fn yt_dlp_output_variants_reject_non_object_values() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = { video = "audio" },
                options = { uri = "https://example.com/video" },
            },
        ],
    },
},
}
"#;

    std::fs::write(&path, source).expect("write source");
    let err = load_mediapm_document(&path).expect_err("document should fail validation");
    assert!(err.to_string().contains("must be an object"));
}

/// Protects strict value schema by rejecting legacy yt-dlp
/// `*_artifacts` kind names.
#[test]
fn yt_dlp_legacy_artifact_kind_aliases_are_rejected() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    "subtitles/" = { kind = "subtitle_artifacts", save = "full", langs = "en" },
                },
                options = { uri = "https://example.com/video" },
            },
        ],
    },
},
}
"#;

    std::fs::write(&path, source).expect("write source");
    let err = load_mediapm_document(&path).expect_err("legacy kind aliases should fail");
    assert!(err.to_string().contains("invalid yt-dlp config"));
}

/// Protects key-agnostic semantics by allowing folder and scoped keys to
/// coexist in the same output map when filename templates are not used.
#[test]
fn output_variants_allow_scoped_and_folder_keys_together() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    "subtitles/" = { kind = "subtitles", save = "full" },
                    "subtitles/en" = { kind = "subtitles", save = "full" },
                },
                options = { uri = "https://example.com/video" },
            },
        ],
    },
},
}
"#;

    std::fs::write(&path, source).expect("write source");
    let document = load_mediapm_document(&path).expect("document should decode");
    let output_variants = &document.media["demo"].steps[0].output_variants;
    assert!(output_variants.contains_key("subtitles/"));
    assert!(output_variants.contains_key("subtitles/en"));
}

/// Protects yt-dlp value schema by allowing `langs`/`sub_format` on
/// non-subtitle kinds for capture-side filtering semantics.
#[test]
fn yt_dlp_non_subtitle_variant_allows_langs_and_sub_format() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    "thumbnails/" = {
                        kind = "thumbnails",
                        save = "full",
                        langs = "all",
                        sub_format = "vtt",
                    },
                },
                options = { uri = "https://example.com/video" },
            },
        ],
    },
},
}
"#;

    std::fs::write(&path, source).expect("write source");
    let document = load_mediapm_document(&path).expect("document should decode");
    let step = &document.media["demo"].steps[0];
    let decoded = super::decode_output_variant_config(
        MediaStepTool::YtDlp,
        "thumbnails/",
        step.output_variants.get("thumbnails/").expect("thumbnails output variant should exist"),
    )
    .expect("yt-dlp output variant should decode");

    match decoded {
        super::DecodedOutputVariantConfig::YtDlp(config) => {
            assert_eq!(config.langs.as_deref(), Some("all"));
            assert_eq!(config.sub_format.as_deref(), Some("vtt"));
        }
        super::DecodedOutputVariantConfig::Generic(config) => {
            panic!("expected yt-dlp config, got Generic({config:?})")
        }
    }
}

/// Protects hierarchy file-target semantics by keeping subtitle variants
/// folder-captured by default.
#[test]
fn hierarchy_file_target_rejects_default_folder_subtitle_capture() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    subtitles = {
                        kind = "subtitles",
                        save = "full",
                    },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
hierarchy = [
    {
        path = "demo/subtitles.srt",
        kind = "media",
        id = "demo-subtitles-file",
        media_id = "demo",
        variant = "subtitles",
    },
],
}
"#;

    std::fs::write(&path, source).expect("write source");
    let err = load_mediapm_document(&path)
        .expect_err("default subtitle capture should remain folder output");
    assert!(err.to_string().contains("requires file variants"));
}

/// Protects capture-kind override semantics by allowing subtitle
/// variants to opt into file capture behavior.
#[test]
fn hierarchy_file_target_accepts_subtitle_capture_kind_file() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    subtitles = {
                        kind = "subtitles",
                        capture_kind = "file",
                        save = "full",
                    },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
hierarchy = [
    {
        path = "demo/subtitles.srt",
        kind = "media",
        id = "demo-subtitles-file",
        media_id = "demo",
        variant = "subtitles",
    },
],
}
"#;

    std::fs::write(&path, source).expect("write source");
    let document = load_mediapm_document(&path)
        .expect("capture_kind=file should permit file hierarchy target");
    assert!(hierarchy_flat_map(&document).contains_key("demo/subtitles.srt"));
}

/// Protects generalized capture-kind semantics by allowing generic
/// transform outputs to opt into folder validation behavior.
#[test]
fn hierarchy_file_target_rejects_generic_capture_kind_folder() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    demo = {
        variant_hashes = {
            source = "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        },
        steps = [
            {
                tool = "ffmpeg",
                input_variants = ["source"],
                output_variants = {
                    result = {
                        kind = "primary",
                        idx = 0,
                        capture_kind = "folder",
                        save = "full",
                    },
                },
                options = {},
            },
        ],
    },
},
hierarchy = [
    {
        path = "demo/result.mkv",
        kind = "media",
        id = "demo-result-file",
        media_id = "demo",
        variant = "result",
    },
],
}
"#;

    std::fs::write(&path, source).expect("write source");
    let err = load_mediapm_document(&path)
        .expect_err("generic capture_kind=folder should reject file target");
    assert!(err.to_string().contains("requires file variants"));
}

/// Protects generalized capture-kind semantics by allowing generic
/// transform outputs to target directory hierarchy paths when set to
/// folder capture behavior.
#[test]
fn hierarchy_directory_target_accepts_generic_capture_kind_folder() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    demo = {
        variant_hashes = {
            source = "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        },
        steps = [
            {
                tool = "ffmpeg",
                input_variants = ["source"],
                output_variants = {
                    result = {
                        kind = "primary",
                        idx = 0,
                        capture_kind = "folder",
                        save = "full",
                    },
                },
                options = {},
            },
        ],
    },
},
hierarchy = [
    {
        path = "demo/result",
        kind = "media_folder",
        id = "demo-result-folder",
        media_id = "demo",
        variants = ["result"],
    },
],
}
"#;

    std::fs::write(&path, source).expect("write source");
    let document = load_mediapm_document(&path)
        .expect("generic capture_kind=folder should permit directory target");
    assert!(hierarchy_flat_map(&document).contains_key("demo/result"));
}

/// Protects hierarchy rename semantics by rejecting file-target usage.
#[test]
fn hierarchy_file_target_rejects_rename_files_rules() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    subtitles = {
                        kind = "subtitles",
                        capture_kind = "file",
                        save = "full",
                    },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
hierarchy = [
    {
        path = "demo/subtitles.vtt",
        kind = "media",
        id = "demo-subtitles-file",
        media_id = "demo",
        variant = "subtitles",
        rename_files = [
            { pattern = "^(.+)\\.vtt$", replacement = "$1.en.vtt" },
        ],
    },
],
}
"#;

    std::fs::write(&path, source).expect("write source");
    let err = load_mediapm_document(&path).expect_err("file-target rename_files must be rejected");
    assert!(err.to_string().contains("rename_files"));
}

/// Protects hierarchy rename semantics by allowing directory-target usage.
#[test]
fn hierarchy_directory_target_accepts_rename_files_rules() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    subtitles = {
                        kind = "subtitles",
                        save = "full",
                    },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
hierarchy = [
    {
        path = "demo/subtitles",
        kind = "media_folder",
        id = "demo-subtitles-folder",
        media_id = "demo",
        variants = ["subtitles"],
        rename_files = [
            { pattern = "^(.+)\\.vtt$", replacement = "$1.en.vtt" },
        ],
    },
],
}
"#;

    std::fs::write(&path, source).expect("write source");
    let document =
        load_mediapm_document(&path).expect("directory-target rename_files should decode");
    assert!(hierarchy_flat_map(&document).contains_key("demo/subtitles"));
}

/// Protects rename replacement interpolation by accepting `${media.id}`
/// and `${media.metadata.*}` placeholders in directory-target rules.
#[test]
fn hierarchy_directory_target_accepts_rename_files_replacement_placeholders() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    demo = {
        metadata = {
            title = "Demo Title",
        },
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    subtitles = {
                        kind = "subtitles",
                        save = "full",
                    },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
hierarchy = [
    {
        path = "demo/subtitles",
        kind = "media_folder",
        id = "demo-subtitles-folder",
        media_id = "demo",
        variants = ["subtitles"],
        rename_files = [
            { pattern = "^(.+)\\.vtt$", replacement = "${media.metadata.title} [${media.id}]$1.vtt" },
        ],
    },
],
}
"#;

    std::fs::write(&path, source).expect("write source");
    let document =
        load_mediapm_document(&path).expect("rename_files replacement placeholders should decode");
    assert!(hierarchy_flat_map(&document).contains_key("demo/subtitles"));
}

/// Protects rename replacement placeholder validation by rejecting
/// undefined metadata references.
#[test]
fn hierarchy_directory_target_rejects_rename_files_replacement_unknown_metadata_key() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    demo = {
        metadata = {
            title = "Demo Title",
        },
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    subtitles = {
                        kind = "subtitles",
                        save = "full",
                    },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
hierarchy = [
    {
        path = "demo/subtitles",
        kind = "media_folder",
        id = "demo-subtitles-folder",
        media_id = "demo",
        variants = ["subtitles"],
        rename_files = [
            { pattern = "^(.+)\\.vtt$", replacement = "${media.metadata.artist} [${media.id}]$1.vtt" },
        ],
    },
],
}
"#;

    std::fs::write(&path, source).expect("write source");
    let err = load_mediapm_document(&path)
        .expect_err("unknown rename_files replacement metadata key must fail validation");
    assert!(err.to_string().contains("undefined metadata key 'artist'"));
}

/// Protects downloader schema by allowing omitted input variants.
#[test]
fn yt_dlp_step_allows_omitted_input_variants() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = { downloaded = { kind = "primary", save = "full" } },
                options = { uri = "https://example.com/video" },
            },
        ],
    },
},
}
"#;

    std::fs::write(&path, source).expect("write source");
    let document = load_mediapm_document(&path).expect("document should decode");
    assert!(document.media["demo"].steps[0].input_variants.is_empty());
}

/// Protects yt-dlp schema by rejecting explicit input variant wiring.
#[test]
fn yt_dlp_step_rejects_non_empty_input_variants() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "yt-dlp",
                input_variants = ["source"],
                output_variants = { downloaded = { kind = "primary", save = "full" } },
                options = { uri = "https://example.com/video" },
            },
        ],
    },
},
}
"#;

    std::fs::write(&path, source).expect("write source");
    let err = load_mediapm_document(&path).expect_err("yt-dlp input_variants must be rejected");
    assert!(
        err.to_string().contains("must not define input_variants for source-ingest tool 'yt-dlp'")
    );
}

/// Protects source-ingest schema by rejecting explicit input variants for
/// import-style ingest steps.
#[test]
fn import_step_rejects_non_empty_input_variants() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "import",
                input_variants = ["default"],
                output_variants = { default = { kind = "primary", save = "full" } },
                options = {
                    kind = "cas_hash",
                    hash = "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                },
            },
        ],
    },
},
}
"#;

    std::fs::write(&path, source).expect("write source");
    let err = load_mediapm_document(&path).expect_err("import input_variants must be rejected");
    assert!(
        err.to_string().contains("must not define input_variants for source-ingest tool 'import'")
    );
}

/// Protects step graph validation by requiring top-to-bottom variant wiring.
#[test]
fn step_graph_rejects_unknown_input_variant() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    local_demo = {
        steps = [
            {
                tool = "ffmpeg",
                input_variants = ["default"],
                output_variants = { aac = { kind = "primary", save = "full", idx = 0 } },
            },
        ],
    },
},
}
"#;

    std::fs::write(&path, source).expect("write source");
    let err = load_mediapm_document(&path).expect_err("document should fail validation");
    assert!(err.to_string().contains("default") && err.to_string().contains("unknown"));
}

/// Protects local-import source validation for required `cas_hash` options.
#[test]
fn import_step_requires_cas_hash_kind_and_hash() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    local_demo = {
        steps = [
            {
                tool = "import",
                output_variants = { default = { kind = "primary", save = "full" } },
                options = { kind = "cas_hash" },
            },
        ],
    },
},
}
"#;

    std::fs::write(&path, source).expect("write source");
    let err = load_mediapm_document(&path).expect_err("document should fail validation");
    assert!(err.to_string().contains("must define options.hash"));
}

/// Protects source-uri bookkeeping helper for online and local media specs.
#[test]
fn media_source_uri_prefers_online_uri_and_falls_back_to_local() {
    let online = MediaSourceSpec {
        id: None,
        description: None,
        title: None,
        workflow_id: None,
        metadata: None,
        variant_hashes: BTreeMap::new(),
        steps: vec![MediaStep {
            tool: MediaStepTool::YtDlp,
            input_variants: Vec::new(),
            output_variants: BTreeMap::from([(
                "default".to_string(),
                Value::Object(serde_json::Map::new()),
            )]),
            options: BTreeMap::from([(
                "uri".to_string(),
                TransformInputValue::String("https://example.com/video.mkv".to_string()),
            )]),
        }],
    };
    let local = MediaSourceSpec {
        id: None,
        description: None,
        title: None,
        workflow_id: None,
        metadata: None,
        variant_hashes: BTreeMap::new(),
        steps: vec![MediaStep {
            tool: MediaStepTool::Import,
            input_variants: Vec::new(),
            output_variants: BTreeMap::from([(
                "default".to_string(),
                Value::Object(serde_json::Map::new()),
            )]),
            options: BTreeMap::from([
                ("kind".to_string(), TransformInputValue::String("cas_hash".to_string())),
                (
                    "hash".to_string(),
                    TransformInputValue::String(
                        "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                            .to_string(),
                    ),
                ),
            ]),
        }],
    };

    assert_eq!(media_source_uri("remote-id", &online), "https://example.com/video.mkv");
    assert_eq!(media_source_uri("local-id", &local), "local:local-id");
}

/// Protects strict metadata schema by accepting literal and
/// variant-binding metadata values.
#[test]
fn media_source_metadata_accepts_literal_and_variant_bindings() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    demo = {
        metadata = {
            curator = "alice",
            title = {
                variant = "infojson",
                metadata_key = "title",
            },
        },
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    infojson = {
                        kind = "infojson",
                    },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
}
"#;

    std::fs::write(&path, source).expect("write source");
    let document = load_mediapm_document(&path).expect("decode document");
    let metadata = document
        .media
        .get("demo")
        .and_then(|spec| spec.metadata.as_ref())
        .expect("metadata should decode as object");

    assert_eq!(metadata.get("curator"), Some(&MediaMetadataValue::Literal("alice".to_string())));

    match metadata.get("title") {
        Some(MediaMetadataValue::Variant(binding)) => {
            assert_eq!(binding.variant, "infojson");
            assert_eq!(binding.metadata_key, "title");
            assert!(binding.transform.is_none());
        }
        other => panic!("expected metadata variant binding, got {other:?}"),
    }
}

/// Protects metadata decode by accepting ordered fallback lists that mix
/// variant bindings and literal values.
#[test]
fn media_source_metadata_accepts_fallback_candidate_lists() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    demo = {
        metadata = {
            title = [
                {
                    variant = "infojson",
                    metadata_key = "title",
                },
                "Unknown Title",
            ],
        },
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    infojson = {
                        kind = "infojson",
                    },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
}
"#;

    std::fs::write(&path, source).expect("write source");
    let document = load_mediapm_document(&path).expect("decode document");
    let metadata = document
        .media
        .get("demo")
        .and_then(|spec| spec.metadata.as_ref())
        .expect("metadata should decode as object");

    match metadata.get("title") {
        Some(MediaMetadataValue::Fallback(candidates)) => {
            assert_eq!(candidates.len(), 2);
            assert!(matches!(
                candidates.first(),
                Some(MediaMetadataValueCandidate::Variant(binding))
                    if binding.variant == "infojson" && binding.metadata_key == "title"
            ));
            assert_eq!(
                candidates.get(1),
                Some(&MediaMetadataValueCandidate::Literal("Unknown Title".to_string()))
            );
        }
        other => panic!("expected metadata fallback list, got {other:?}"),
    }
}

/// Protects metadata validation by rejecting empty fallback lists.
#[test]
fn media_source_metadata_rejects_empty_fallback_lists() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    demo = {
        metadata = {
            title = [],
        },
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    infojson = {
                        kind = "infojson",
                    },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
}
"#;

    std::fs::write(&path, source).expect("write source");
    let err = load_mediapm_document(&path).expect_err("empty metadata fallback list must fail");
    assert!(err.to_string().contains("fallback list must be non-empty"));
}

/// Protects metadata binding decode by accepting regex transform settings
/// for variant-backed placeholders.
#[test]
fn media_source_metadata_variant_binding_accepts_regex_transform() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    demo = {
        metadata = {
            video_ext = {
                variant = "infojson",
                metadata_key = "ext",
                transform = {
                    pattern = "(.+)",
                    replacement = ".$1",
                },
            },
        },
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    infojson = {
                        kind = "infojson",
                    },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
}
"#;

    std::fs::write(&path, source).expect("write source");
    let document = load_mediapm_document(&path).expect("decode document");
    let metadata = document
        .media
        .get("demo")
        .and_then(|spec| spec.metadata.as_ref())
        .expect("metadata should decode as object");

    match metadata.get("video_ext") {
        Some(MediaMetadataValue::Variant(binding)) => {
            assert_eq!(binding.variant, "infojson");
            assert_eq!(binding.metadata_key, "ext");
            let transform = binding.transform.as_ref().expect("transform should decode");
            assert_eq!(transform.pattern, "(.+)");
            assert_eq!(transform.replacement, ".$1");
        }
        other => panic!("expected metadata variant binding, got {other:?}"),
    }
}

/// Protects output-variant extension policy by allowing extension only for
/// ffmpeg/rsgain/media-tagger outputs.
#[test]
fn output_variant_extension_rejects_unsupported_tools() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "import",
                output_variants = {
                    default = {
                        kind = "primary",
                        extension = "mkv",
                    },
                },
                options = {
                    kind = "cas_hash",
                    hash = "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                },
            },
        ],
    },
},
}
"#;

    std::fs::write(&path, source).expect("write source");
    let err = load_mediapm_document(&path).expect_err("import extension should be rejected");
    assert!(err.to_string().contains("must not define extension"));
}

/// Protects source metadata top-level shape policy by rejecting
/// non-object metadata values.
#[test]
fn media_source_metadata_rejects_non_object_values() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    demo = {
        metadata = "invalid",
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    default = {
                        kind = "primary",
                        save = "full",
                    },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
}
"#;

    std::fs::write(&path, source).expect("write source");
    let err = load_mediapm_document(&path).expect_err("string metadata should be rejected");

    assert!(err.to_string().contains("invalid type: string \"invalid\""));
}

/// Protects strict metadata schema by rejecting folder-output variant
/// bindings for metadata lookup.
#[test]
fn media_source_metadata_rejects_folder_output_binding() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    demo = {
        metadata = {
            title = {
                variant = "subtitles",
                metadata_key = "title",
            },
        },
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    subtitles = {
                        kind = "subtitles",
                    },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
}
"#;

    std::fs::write(&path, source).expect("write source");
    let err = load_mediapm_document(&path)
        .expect_err("folder variants should be rejected for metadata binding");

    assert!(err.to_string().contains("metadata bindings require file variants"));
}

/// Protects output policy defaults by treating omitted save as `true`.
#[test]
fn output_variant_policy_defaults_apply_when_save_fields_are_omitted() {
    let yt_dlp = super::decode_output_variant_policy(
        MediaStepTool::YtDlp,
        "video",
        &serde_json::json!({ "kind": "primary" }),
    )
    .expect("decode yt-dlp output policy");
    assert_eq!(yt_dlp.save, OutputSaveConfig::Bool(true));

    let ffmpeg = super::decode_output_variant_policy(
        MediaStepTool::Ffmpeg,
        "audio",
        &serde_json::json!({ "kind": "primary", "idx": 0 }),
    )
    .expect("decode ffmpeg output policy");
    assert_eq!(ffmpeg.save, OutputSaveConfig::Bool(true));
}

/// Protects hierarchy validation by allowing file variants to keep the
/// default `save=true` policy when materialized to file paths.
#[test]
fn hierarchy_file_variant_allows_default_save_true() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    video = {
                        kind = "primary",
                    },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
hierarchy = [
    {
        path = "demo/video.mp4",
        kind = "media",
        id = "demo-video",
        media_id = "demo",
        variant = "video",
    },
],
}
"#;

    std::fs::write(&path, source).expect("write source");
    let document = load_mediapm_document(&path).expect("hierarchy file variant should be allowed");
    assert!(hierarchy_flat_map(&document).contains_key("demo/video.mp4"));
}

/// Protects hierarchy interpolation policy by requiring every
/// `${media.metadata.*}` placeholder key to be declared in source metadata.
#[test]
fn hierarchy_metadata_placeholder_requires_declared_metadata_key() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    demo = {
        metadata = {
            artist = "The Artist",
        },
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    video = {
                        kind = "primary",
                        save = "full",
                    },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
hierarchy = [
    {
        path = "library/${media.metadata.title}/demo.mp4",
        kind = "media",
        id = "demo-video",
        media_id = "demo",
        variant = "video",
    },
],
}
"#;

    std::fs::write(&path, source).expect("write source");
    let err = load_mediapm_document(&path)
        .expect_err("undefined metadata placeholder keys should be rejected");
    assert!(err.to_string().contains("undefined metadata key 'title'"));
}

/// Protects hierarchy interpolation grammar by rejecting unsupported
/// placeholder expressions.
#[test]
fn hierarchy_metadata_placeholder_rejects_unsupported_expression() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    demo = {
        metadata = {
            title = "Demo",
        },
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    video = {
                        kind = "primary",
                        save = "full",
                    },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
hierarchy = [
    {
        path = "library/${media.title}/demo.mp4",
        kind = "media",
        id = "demo-video",
        media_id = "demo",
        variant = "video",
    },
],
}
"#;

    std::fs::write(&path, source).expect("write source");
    let err = load_mediapm_document(&path)
        .expect_err("unsupported placeholder expressions should be rejected");
    assert!(err.to_string().contains("unsupported placeholder"));
}

/// Protects hierarchy interpolation grammar by allowing `${media.id}`
/// placeholders without requiring metadata declarations.
#[test]
fn hierarchy_placeholder_allows_media_id_expression() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    video = {
                        kind = "primary",
                        save = "full",
                    },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
hierarchy = [
    {
        path = "library/${media.id}/demo.mp4",
        kind = "media",
        id = "demo-video",
        media_id = "demo",
        variant = "video",
    },
],
}
"#;

    std::fs::write(&path, source).expect("write source");
    let document = load_mediapm_document(&path).expect("media.id placeholder should decode");
    assert!(hierarchy_flat_map(&document).contains_key("library/${media.id}/demo.mp4"));
}

/// Protects playlist hierarchy decoding by preserving ordered id entries,
/// default format policy, and per-item absolute-path overrides.
#[test]
fn hierarchy_playlist_entry_decodes_ordered_ids_and_path_modes() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    a = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    video = {
                        kind = "primary",
                        save = "full",
                    },
                },
                options = {
                    uri = "https://example.com/a",
                },
            },
        ],
    },
    b = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    video = {
                        kind = "primary",
                        save = "full",
                    },
                },
                options = {
                    uri = "https://example.com/b",
                },
            },
        ],
    },
},
hierarchy = [
    {
        path = "library/a.mp4",
        kind = "media",
        id = "playlist-a",
        media_id = "a",
        variant = "video",
    },
    {
        path = "library/b.mp4",
        kind = "media",
        id = "b",
        media_id = "b",
        variant = "video",
    },
    {
        path = "playlists/demo.m3u8",
        kind = "playlist",
        ids = [
            "playlist-a",
            {
                id = "b",
                path = "absolute",
            },
        ],
    },
],
}
"#;

    std::fs::write(&path, source).expect("write source");
    let document = load_mediapm_document(&path).expect("playlist hierarchy should decode");
    let hierarchy = hierarchy_flat_map(&document);
    let playlist_entry = hierarchy.get("playlists/demo.m3u8").expect("playlist entry exists");

    assert!(matches!(playlist_entry.kind, HierarchyEntryKind::Playlist));
    assert!(matches!(playlist_entry.format, PlaylistFormat::M3u8));
    assert_eq!(playlist_entry.ids.len(), 2);
    assert_eq!(playlist_entry.ids[0].id(), "playlist-a");
    assert!(matches!(playlist_entry.ids[0].path_mode(), PlaylistEntryPathMode::Relative));
    assert_eq!(playlist_entry.ids[1].id(), "b");
    assert!(matches!(playlist_entry.ids[1].path_mode(), PlaylistEntryPathMode::Absolute));
}

/// Protects playlist hierarchy decoding by preserving explicit non-default
/// format selections and duplicate id ordering semantics.
#[test]
fn hierarchy_playlist_entry_decodes_explicit_format_and_duplicate_ids() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    a = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    video = {
                        kind = "primary",
                        save = "full",
                    },
                },
                options = {
                    uri = "https://example.com/a",
                },
            },
        ],
    },
},
hierarchy = [
    {
        path = "library/a.mp4",
        kind = "media",
        id = "playlist-a",
        media_id = "a",
        variant = "video",
    },
    {
        path = "playlists/demo.xspf",
        kind = "playlist",
        format = "xspf",
        ids = [
            "playlist-a",
            {
                id = "playlist-a",
            },
            "playlist-a",
        ],
    },
],
}
"#;

    std::fs::write(&path, source).expect("write source");
    let document =
        load_mediapm_document(&path).expect("playlist hierarchy with xspf should decode");
    let hierarchy = hierarchy_flat_map(&document);
    let playlist_entry = hierarchy.get("playlists/demo.xspf").expect("playlist entry exists");

    assert!(matches!(playlist_entry.kind, HierarchyEntryKind::Playlist));
    assert!(matches!(playlist_entry.format, PlaylistFormat::Xspf));
    assert_eq!(playlist_entry.ids.len(), 3);
    assert_eq!(playlist_entry.ids[0].id(), "playlist-a");
    assert_eq!(playlist_entry.ids[1].id(), "playlist-a");
    assert_eq!(playlist_entry.ids[2].id(), "playlist-a");
    assert!(matches!(playlist_entry.ids[0].path_mode(), PlaylistEntryPathMode::Relative));
    assert!(matches!(playlist_entry.ids[1].path_mode(), PlaylistEntryPathMode::Relative));
    assert!(matches!(playlist_entry.ids[2].path_mode(), PlaylistEntryPathMode::Relative));
}

/// Protects playlist hierarchy validation by rejecting unknown referenced
/// ids.
#[test]
fn hierarchy_playlist_entry_rejects_unknown_referenced_id() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    video = {
                        kind = "primary",
                        save = "full",
                    },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
hierarchy = [
    {
        path = "library/demo.mp4",
        kind = "media",
        id = "demo-video",
        media_id = "demo",
        variant = "video",
    },
    {
        path = "playlists/demo.m3u8",
        kind = "playlist",
        ids = ["unknown-id"],
    },
],
}
"#;

    std::fs::write(&path, source).expect("write source");
    let error =
        load_mediapm_document(&path).expect_err("playlist should reject unknown referenced ids");
    assert!(error.to_string().contains("unknown hierarchy id 'unknown-id'"));
}

/// Protects hierarchy id uniqueness by rejecting duplicate `hierarchy[*].id`
/// assignments across media nodes.
#[test]
fn media_hierarchy_id_rejects_duplicates_across_media_entries() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    a = {
        variant_hashes = {
            video = "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        },
    },
    b = {
        variant_hashes = {
            video = "blake3:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        },
    },
},
hierarchy = [
    {
        path = "library/a.mp4",
        kind = "media",
        id = "duplicate",
        media_id = "a",
        variant = "video",
    },
    {
        path = "library/b.mp4",
        kind = "media",
        id = "duplicate",
        media_id = "b",
        variant = "video",
    },
],
}
"#;

    std::fs::write(&path, source).expect("write source");
    let error =
        load_mediapm_document(&path).expect_err("duplicate hierarchy ids should be rejected");
    assert!(error.to_string().contains("hierarchy id 'duplicate'"));
    assert!(error.to_string().contains("duplicated") || error.to_string().contains("duplicates"));
}

/// Protects hierarchy validation by allowing folder variants to keep the
/// default `save=true` policy when materialized to directory paths.
#[test]
fn hierarchy_directory_variant_allows_default_save_true() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    "subtitles" = {
                        kind = "subtitles",
                    },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
hierarchy = [
    {
        path = "demo/subtitles",
        kind = "media_folder",
        id = "demo-subtitles-folder",
        media_id = "demo",
        variants = ["subtitles"],
    },
],
}
"#;

    std::fs::write(&path, source).expect("write source");
    let document =
        load_mediapm_document(&path).expect("hierarchy folder variant should be allowed");
    assert!(hierarchy_flat_map(&document).contains_key("demo/subtitles"));
}

/// Protects hierarchy typing by rejecting folder variants for file paths.
#[test]
fn hierarchy_file_path_rejects_folder_variant_output() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    subtitles = {
                        kind = "subtitles",
                        save = "full",
                    },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
hierarchy = [
    {
        path = "demo/subtitles.txt",
        kind = "media",
        id = "demo-subtitles-file",
        media_id = "demo",
        variant = "subtitles",
    },
],
}
"#;

    std::fs::write(&path, source).expect("write source");
    let err =
        load_mediapm_document(&path).expect_err("file paths must reject folder output variants");
    assert!(err.to_string().contains("requires file variants"));
}

/// Protects selector-object support by allowing regex object syntax in
/// both `input_variants` and `media_folder` hierarchy `variants`.
#[test]
fn regex_selector_objects_are_supported_for_steps_and_hierarchy() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "import",
                output_variants = {
                    source = {
                        kind = "result",
                        save = "full",
                    },
                },
                options = {
                    kind = "cas_hash",
                    hash = "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                },
            },
            {
                tool = "ffmpeg",
                input_variants = [{ regex = "^source$" }],
                output_variants = {
                    video = {
                        kind = "primary",
                        idx = 0,
                        capture_kind = "folder",
                        save = "full",
                        extension = "mkv",
                    },
                },
            },
        ],
    },
},
hierarchy = [
    {
        path = "library/demo",
        kind = "media_folder",
        id = "demo-folder",
        media_id = "demo",
        variants = [{ regex = "^video$" }],
    },
],
}
"#;

    std::fs::write(&path, source).expect("write source");
    let document = load_mediapm_document(&path).expect("regex selector objects should decode");

    assert!(document.media["demo"].steps[1].input_variants[0].contains("source"));
    let hierarchy = hierarchy_flat_map(&document);
    let media_folder = hierarchy
        .get("library/demo")
        .expect("media_folder hierarchy entry should flatten without trailing slash");
    assert_eq!(media_folder.variants.len(), 1);
    assert!(media_folder.variants[0].contains("video"));
}

/// Protects selector decode by rejecting malformed regex selector objects.
#[test]
fn regex_selector_object_rejects_invalid_pattern() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("mediapm.ncl");
    let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    video = {
                        kind = "primary",
                        save = "full",
                    },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
hierarchy = [
    {
        path = "library/demo",
        kind = "media_folder",
        media_id = "demo",
        variants = [{ regex = "[" }],
    },
],
}
"#;

    std::fs::write(&path, source).expect("write source");
    let err =
        load_mediapm_document(&path).expect_err("invalid regex selector object must be rejected");
    assert!(err.to_string().contains("regex selector"));
}
