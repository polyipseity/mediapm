//! Integration guardrails for the `demo_online` example wiring.
//!
//! These checks intentionally validate source-level invariants in the example
//! because `demo_online` is compile-only in automated tests (`test = false`) to
//! avoid network/tool-provider dependencies during CI.

/// Verifies `demo_online` writes explicit runtime defaults so generated
/// `mediapm.ncl` documents all runtime knobs (not just tool-cache toggle).
#[test]
fn demo_online_writes_explicit_runtime_defaults() {
    let source = include_str!("../../examples/demo_online.rs");

    assert!(
        source.contains("mediapm_dir: Some(\".mediapm\".to_string())")
            && source.contains("hierarchy_root_dir: Some(\".\".to_string())")
            && source.contains("mediapm_tmp_dir: Some(\"tmp\".to_string())")
            && source.contains("conductor_config: Some(\"mediapm.conductor.ncl\".to_string())")
            && source.contains(
                "conductor_machine_config: Some(\"mediapm.conductor.machine.ncl\".to_string())"
            )
            && source.contains(
                "conductor_state_config: Some(\".mediapm/state.conductor.ncl\".to_string())"
            )
            && source.contains("conductor_tmp_dir: Some(\".mediapm/tmp\".to_string())")
            && source
                .contains("conductor_schema_dir: Some(\".mediapm/config/conductor\".to_string())")
            && source.contains(
                "inherited_env_vars: Some(default_runtime_inherited_env_vars_for_host())"
            )
            && source.contains("media_state_config: Some(\".mediapm/state.ncl\".to_string())")
            && source.contains("env_file: Some(\".mediapm/.env\".to_string())")
            && source.contains(
                "mediapm_schema_dir: Some(Some(\".mediapm/config/mediapm\".to_string()))"
            )
            && source.contains("use_user_tool_cache: Some(true)"),
        "demo_online should write explicit runtime defaults for mediapm_dir/hierarchy/tmp/conductor paths/env/schema/inherited env vars and cache toggle"
    );
}

/// Verifies `demo_online` uses in-memory service construction so tool sync can
/// open the persistent runtime CAS store without redb lock contention.
#[test]
#[expect(
    clippy::too_many_lines,
    reason = "this item intentionally keeps end-to-end control flow together so ordering invariants remain explicit during maintenance"
)]
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
            && source.contains("\"sd\".to_string()")
            && source.contains("\"media-tagger\".to_string()")
            && source.contains("\"import\".to_string()"),
        "demo_online should declare managed tool requirements for rsgain, sd, media-tagger, and import"
    );

    assert!(
        source.contains("tool: MediaStepTool::Rsgain")
            && source.contains("tool: MediaStepTool::MediaTagger"),
        "demo_online should include rsgain and media-tagger steps after ffmpeg transforms"
    );

    assert!(
        source.contains("fn final_demo_output_variant() -> &'static str")
            && source.contains("\"video\"")
            && source.contains("input_variants: vec![\"video\".to_string()]")
            && source.contains("input_variants: vec![\"video_tagged\".to_string()]")
            && !source.contains("regex_variant_selector(\"^video$\")")
            && !source.contains("regex_variant_selector(\"^video_tagged$\")")
            && source.contains("\"video_tagged\".to_string()")
            && !source.contains("\"video_mkv\"")
            && !source.contains("\"tagged_metadata\""),
        "demo_online should keep exact-string video/video_tagged variant selectors and remove legacy names"
    );

    assert!(
        !source.contains("\"strict_identification\".to_string()")
            && source.contains("\"recording_mbid\".to_string()"),
        "demo_online should rely on managed media-tagger input defaults for strict identification and only set explicit non-default options"
    );

    assert!(
        source.contains("\"extension\": \"mkv\"")
            && source.contains("\"format\".to_string()")
            && source.contains("height<=144")
            && !source.contains("height<=720")
            && !source.contains("\"format\": \"bestvideo*+bestaudio/best\""),
        "demo_online should keep yt-dlp format selection in step options (not output variants) and force 144p outputs"
    );

    assert!(
        source.contains("\"codec_copy\".to_string()")
            && source.contains("\"true\".to_string()")
            && source.contains("\"container\".to_string()")
            && source.contains("matroska")
            && !source
                .contains("\"vn\".to_string(), TransformInputValue::String(\"true\".to_string())")
            && !source.contains("libmp3lame")
            && !source.contains("audio_bitrate"),
        "demo_online should preserve full video+audio streams in MKV instead of forcing audio-only MP3 transcodes"
    );

    assert!(
        source.contains("tool: MediaStepTool::Rsgain")
            && !source.contains("\"input_extension\".to_string()")
            && !source.contains("TransformInputValue::String(\"mp3\".to_string())"),
        "demo_online should keep rsgain options minimal and let workflow synthesis handle metadata-export/apply details"
    );

    assert!(
        source.contains("const DEMO_SIDECAR_VARIANT_PATHS: [(&str, &str, &str); 9] =")
            && source.contains("(\"subtitles_sidecars\", \"subtitles\", \"sidecars/subtitles/\")")
            && source.contains("\"subtitles_en\".to_string()")
            && source.contains("\"langs\": \"en\"")
            && source.contains("\"capture_kind\": \"file\"")
            && !source.contains("\"save\": \"full\"")
            && !source.contains("\"langs\": \"en,es\"")
            && !source.contains("regex_variant_selector(\"^subtitles(?:_en)?$\")")
            && source
                .contains("(\"thumbnails_sidecars\", \"thumbnails\", \"sidecars/thumbnails/\")")
            && source.contains("(\"links_sidecars\", \"links\", \"sidecars/links/\")")
            && source.contains("(\"archive_sidecars\", \"archive\", \"sidecars/archive.txt\")")
            && source.contains(
                "(\"description_sidecars\", \"description\", \"sidecars/description.txt\")"
            )
            && source.contains("(\"infojson_sidecars\", \"infojson\", \"sidecars/info.json\")")
            && source.contains("\"description_media\"")
            && source.contains(
                "${media.metadata.artist} - ${media.metadata.title} [${media.id}].description.txt"
            )
            && source.contains("\"infojson_media\"")
            && source.contains(
                "${media.metadata.artist} - ${media.metadata.title} [${media.id}].info.json"
            )
            && source.contains("\"subtitles_en_media\"")
            && source.contains(
                "${media.metadata.artist} - ${media.metadata.title} [${media.id}].en.vtt"
            )
            && !source.contains(
                "${media.metadata.artist} - ${media.metadata.title} [${media.id}]-description.txt"
            )
            && !source.contains(
                "${media.metadata.artist} - ${media.metadata.title} [${media.id}]-info.json"
            )
            && !source.contains("\"playlist_infojson\".to_string()")
            && !source.contains("sidecars/playlist-info.json")
            && !source.contains("\"playlist_description\".to_string()")
            && !source.contains("sidecars/playlist-description.txt")
            && !source.contains("\"playlist_video\"")
            && !source.contains("\"playlist_thumbnail\"")
            && !source.contains("\"kind\": \"annotations\"")
            && !source.contains("(\"chapters\", \"chapters/\")"),
        "demo_online should materialize bounded downloader sidecar families, skip comment projections by default, and keep annotation/playlist sidecars opt-in"
    );

    assert!(
        source.contains("\"yt-dlp\".to_string()")
            && source.contains("\"rsgain\".to_string()")
            && source.contains("\"sd\".to_string()")
            && source.contains("\"media-tagger\".to_string()")
            && source.contains("dependencies: ToolRequirementDependencies {")
            && source.contains("ffmpeg_version: Some(\"inherit\".to_string())")
            && source.contains("\"rsgain\".to_string(),\n            ToolRequirement {\n                version: None,\n                tag: Some(\"latest\".to_string()),\n                dependencies: ToolRequirementDependencies {\n                    ffmpeg_version: Some(\"inherit\".to_string()),\n                    sd_version: Some(\"inherit\".to_string()),")
            && !source.contains("\"ffmpeg_version\".to_string()"),
        "demo_online should demonstrate inherit dependencies for yt-dlp/media-tagger/rsgain via tool requirements, not media step options"
    );

    assert!(
        source.contains("DEMO_UNTAGGED_MEDIA_FILE_NAME")
            && source.contains("DEMO_TAGGED_MEDIA_FILE_NAME")
            && source.contains("${media.metadata.artist} - ${media.metadata.title} [${media.id}].untagged${media.metadata.video_ext}")
            && source.contains("${media.metadata.artist} - ${media.metadata.title} [${media.id}]${media.metadata.video_ext}")
            && !source.contains("-audio.mp3")
            && !source.contains("-audio-tagged.mp3"),
        "demo_online should materialize metadata-driven MKV output filename templates for untagged + tagged outputs"
    );

    assert!(
        !source.contains(
            "\"write_subs\".to_string(), TransformInputValue::String(\"false\".to_string())"
        ),
        "demo_online should not disable subtitle defaults in yt-dlp options"
    );

    assert!(
        !source.contains("\"write_comments\".to_string()")
            && !source.contains("comment payload")
            && !source.contains("\"comment\" =>"),
        "demo_online should rely on managed yt-dlp defaults for disabled comment extraction and avoid comment-specific demo validation branches"
    );

    assert!(
        source.contains("DEMO_SAFE_SUB_LANGS")
            && source.contains("\"sub_langs\".to_string()")
            && source.contains("en-en,en-AU,en-CA,en-IN,en-IE,en-GB,en-US,en-orig"),
        "demo_online should pin safe subtitle language selectors that avoid broad translated subtitle downloads"
    );

    assert!(
        !source.contains("DEMO_SAFE_EXTRACTOR_ARGS")
            && !source.contains("\"extractor_args\".to_string()"),
        "demo_online must not override extractor_args explicitly; the tool default (YT_DLP_DEFAULT_EXTRACTOR_ARGS) already covers translated-subtitle avoidance"
    );

    assert!(
        !source.contains(
            "\"no_playlist\".to_string(), TransformInputValue::String(\"false\".to_string())"
        ),
        "demo_online must not disable the no_playlist tool default; single-video flow relies on the default no_playlist = true to avoid unbounded playlist fan-out"
    );

    assert!(
        source.contains("MediaMetadataValue::Literal")
            && source.contains("MediaMetadataValue::Variant")
            && source.contains("variant: \"video_tagged\".to_string()")
            && !source.contains("variant: \"infojson\".to_string(),\n                        metadata_key: DEMO_METADATA_TITLE_KEY.to_string()")
            && source.contains("metadata_key: DEMO_METADATA_ARTIST_KEY.to_string()")
            && source.contains("metadata_key: DEMO_METADATA_TITLE_KEY.to_string()")
            && source.contains("metadata_key: DEMO_METADATA_VIDEO_ID_KEY.to_string()")
            && source.contains("metadata_key: DEMO_METADATA_VIDEO_EXT_KEY.to_string()")
            && source.contains("${media.id}")
            && source.contains("${media.metadata.video_ext}"),
        "demo_online should derive hierarchy title/artist/video-ext metadata from tagged media bindings with strict object values"
    );

    assert!(
        !source.contains("\"filename_template\"") && !source.contains("\"output_template\""),
        "demo_online should avoid legacy yt-dlp filename template/output template fields"
    );

    assert!(
        !source.contains(
            "\"write_all_thumbnails\".to_string(), TransformInputValue::String(\"true\".to_string())"
        ),
        "demo_online must not enable write_all_thumbnails; the tool default is false (single best thumbnail) and enabling all thumbnails would increase download footprint"
    );

    assert!(
        source.contains("music videos/")
            && source.contains("sidecars/")
            && source.contains("(\"links_sidecars\", \"links\", \"sidecars/links/\")")
            && source.contains("DEMO_MEDIA_ROOT_FLAT_VARIANTS")
            && source.contains("DEMO_ROOT_SELECTED_SUBTITLE_VARIANT")
            && source.contains("DEMO_ROOT_SELECTED_SUBTITLE_FILE_NAME")
            && source.contains("DEMO_MEDIA_ROOT_RENAME_PATTERN")
            && source.contains("DEMO_MEDIA_ROOT_RENAME_REPLACEMENT")
            && source.contains(r"^.*\\.([^.]*)$")
            && source.contains(
                "\"${media.metadata.artist} - ${media.metadata.title} [${media.id}].$1\""
            )
            && !source.contains("^.+ \\[{DEMO_MEDIA_ID}\\](\\.[^/\\\\]+)$")
            && !source
                .contains("{DEMO_EXPECTED_ARTIST} - {DEMO_EXPECTED_TITLE} [{DEMO_MEDIA_ID}]$1")
            && source.contains(".en.vtt")
            && !source.contains("format!(\"{DEMO_HIERARCHY_ROOT_TEMPLATE}/subtitles/\")")
            && source.contains("DEMO_HIERARCHY_ROOT_TEMPLATE")
            && source.contains("DEMO_HIERARCHY_MEDIA_ROOT_TEMPLATE")
            && source.contains("DEMO_LIBRARY_ROOT")
            && source.contains("must not contain dedicated root subtitles folder")
            && source.contains("rename_files: vec![")
            && source.contains("HierarchyFolderRenameRule {")
            && source.contains(
                "assert_flat_media_root_sidecar_families(&interpolated_root, &resolved_output_base)"
            )
            && source.contains("resolve_interpolated_demo_root")
            && source.contains("DEMO_EXPECTED_VIDEO_ID"),
        "demo_online should materialize both primary/tagged media outputs, keep sidecar hierarchy, and mirror one language-selected subtitle file at media root"
    );

    assert!(
        source.contains("assert_sidecar_directory_family_content")
            && source.contains("assert_sidecar_file_content_shape")
            && source.contains("assert_flat_media_root_sidecar_families")
            && source.contains("collect_regular_files_recursive")
            && source.contains("thumbnails"),
        "demo_online should validate sidecar artifact content families, not only path existence"
    );

    assert!(
        source.contains("bytes_look_like_matroska")
            && source.contains("assert_mkv_video_audio_with_ffprobe")
            && source.contains("ffprobe_payload_has_mkv_video_and_audio")
            && source.contains("configure_demo_ffprobe_command(&machine, &tool_binaries)?")
            && source.contains("derive_ffprobe_path_from_ffmpeg_command")
            && source.contains("demo_ffprobe_command()")
            && !source.contains("Command::new(\"ffprobe\")")
            && !source.contains("bytes_look_like_mp3"),
        "demo_online should validate MKV container identity and video+audio stream preservation using managed ffprobe resolution (not PATH-only ffprobe or MP3 signatures)"
    );

    assert!(
        source.contains("assert_tagged_media_replaygain_tags")
            && source.contains("probe_replaygain_tags_with_ffprobe")
            && source.contains("ffprobe_payload_has_required_replaygain_tags")
            && source.contains("DEMO_REPLAYGAIN_REQUIRED_TAG_KEYS")
            && source.contains("replaygain_track_gain")
            && source.contains("replaygain_track_peak")
            && source.contains("replaygain_reference_loudness")
            && source.contains("DEMO_REPLAYGAIN_DISALLOWED_TAG_KEYS")
            && source.contains("DEMO_REPLAYGAIN_FFPROBE_MAX_ATTEMPTS"),
        "demo_online should poll ffprobe on tagged media until ReplayGain tags are visible"
    );

    assert!(
        source.contains("assert_tagged_media_replaygain_tags(&output_tagged_video_path).await?")
            && !source.contains("DEMO_REPLAYGAIN_OPTIONAL_TAG_KEYS")
            && !source.contains("ffprobe_payload_missing_optional_replaygain_tags")
            && !source.contains("warning: ReplayGain tags were not observed"),
        "demo_online should fail fast when required ReplayGain tags are missing"
    );

    assert!(
        source.contains("store_size_without_delta_bytes")
            && source.contains("store_size_with_delta_bytes")
            && source.contains("store_size_ratio_with_delta_over_without")
            && source.contains("ratio_with_delta_over_without"),
        "demo_online should serialize store-size stats via shared ratio logic"
    );

    assert!(
        source.contains("DemoOnlineTimeoutError")
            && source.contains("spawn_hard_timeout_guard")
            && source.contains("process::exit(124)")
            && source.contains("emit_watchdog_notice")
            && source.contains("wait briefly, then rerun")
            && !source.contains("DEMO_ONLINE_SYNC_HEARTBEAT_SECS")
            && !source.contains("tokio::time::interval(")
            && !source.contains("\\u{1b}[2K"),
        "demo_online should preserve timeout watchdog exit code 124 while keeping timeout notices plain text and free of heartbeat/progress-row duplication"
    );
}

/// Verifies online `demo_online` includes one playlist hierarchy example with
/// duplicated id references and per-item path mode fields.
#[test]
fn demo_online_configures_playlist_hierarchy_entry() {
    let source = include_str!("../../examples/demo_online.rs");

    assert!(
        source.contains("path: \"playlists\".to_string()")
            && source.contains("path: \"rickroll.m3u8\".to_string()")
            && source.contains("kind: HierarchyNodeKind::Playlist")
            && source.contains("PlaylistItemRef {")
            && source.contains("id: DEMO_TAGGED_HIERARCHY_ID.to_string()")
            && source.contains("path: PlaylistEntryPathMode::Relative")
            && source.contains("path: PlaylistEntryPathMode::Absolute")
            && source.contains("children: media_root_children")
            && source.contains("document.hierarchy = vec!["),
        "demo_online should configure nested playlist hierarchy with duplicated ids and relative+absolute path modes"
    );
}
