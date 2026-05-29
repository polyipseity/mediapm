use std::fs;

use std::collections::BTreeMap;

use mediapm_cas::Hash;
use mediapm_conductor::OutputCaptureSpec;
use tempfile::tempdir;

use mediapm_conductor::InputBinding;

use super::{
    FfmpegSlotLimits, INPUT_LEADING_ARGS, INPUT_TRAILING_ARGS, OUTPUT_CONTENT,
    OUTPUT_SANDBOX_ARTIFACTS, OUTPUT_YT_DLP_ANNOTATION_FILE, OUTPUT_YT_DLP_ARCHIVE_FILE,
    OUTPUT_YT_DLP_CHAPTER_ARTIFACTS, OUTPUT_YT_DLP_DESCRIPTION_FILE, OUTPUT_YT_DLP_INFOJSON_FILE,
    OUTPUT_YT_DLP_LINK_ARTIFACTS, OUTPUT_YT_DLP_PLAYLIST_DESCRIPTION_FILE,
    OUTPUT_YT_DLP_PLAYLIST_INFOJSON_FILE, OUTPUT_YT_DLP_SUBTITLE_ARTIFACTS,
    OUTPUT_YT_DLP_THUMBNAIL_ARTIFACTS, SANDBOX_DOWNLOADS_DIR, YT_DLP_DEFAULT_EXTRACTOR_ARGS,
    YT_DLP_DEFAULT_OUTPUT_TEMPLATE, YT_DLP_OPTION_INPUTS, build_tool_outputs,
    default_input_defaults_for_tool, option_input_names_for_tool, option_tokens_for_input,
    rsgain_output_file_regex, validate_tool_command,
};

use super::launcher::{
    find_workspace_root_for_target_dir, resolve_profile_adjacent_mediapm_binary_for_example,
};

/// Verifies generated input defaults include every declared managed-tool
/// option key so runtime config remains explicit and override-friendly.
#[test]
fn input_defaults_include_all_declared_option_inputs() {
    for tool_name in ["yt-dlp", "ffmpeg", "rsgain", "media-tagger"] {
        let defaults = default_input_defaults_for_tool(tool_name, FfmpegSlotLimits::default());
        for option_name in option_input_names_for_tool(tool_name) {
            assert!(
                defaults.contains_key(*option_name),
                "missing input_defaults entry '{option_name}' for tool '{tool_name}'"
            );
        }

        assert!(defaults.contains_key(INPUT_LEADING_ARGS));
        assert!(defaults.contains_key(INPUT_TRAILING_ARGS));
    }
}

/// Verifies default rsgain options keep single-track mode with the
/// expected loudness profile and explicit peak-safety behavior.
#[test]
fn rsgain_defaults_match_expected_loudness_profile() {
    let defaults = default_input_defaults_for_tool("rsgain", FfmpegSlotLimits::default());

    assert_eq!(defaults.get("target_lufs"), Some(&InputBinding::String("-18".to_string())));
    assert_eq!(defaults.get("album"), Some(&InputBinding::String("false".to_string())));
    assert_eq!(defaults.get("album_mode"), Some(&InputBinding::String("false".to_string())));
    assert_eq!(defaults.get("tagmode"), Some(&InputBinding::String("i".to_string())));
    assert_eq!(defaults.get("true_peak"), Some(&InputBinding::String("true".to_string())));
    assert_eq!(defaults.get("clip_mode"), Some(&InputBinding::String("p".to_string())));
    assert_eq!(defaults.get("max_peak"), Some(&InputBinding::String("0".to_string())));
}

/// Verifies yt-dlp defaults prioritize one best thumbnail while keeping
/// unified subtitle capture enabled.
#[test]
fn yt_dlp_defaults_prefer_single_best_thumbnail_with_unified_subtitles() {
    let defaults = default_input_defaults_for_tool("yt-dlp", FfmpegSlotLimits::default());

    assert_eq!(defaults.get("write_subs"), Some(&InputBinding::String("true".to_string())));
    assert_eq!(defaults.get("sub_langs"), Some(&InputBinding::String("all".to_string())));
    assert!(
        YT_DLP_DEFAULT_EXTRACTOR_ARGS.contains("skip=translated_subs"),
        "yt-dlp managed defaults should skip auto-translated subtitles"
    );
    assert_eq!(
        defaults.get("paths"),
        Some(&InputBinding::String(SANDBOX_DOWNLOADS_DIR.to_string()))
    );
    assert_eq!(
        defaults.get("output"),
        Some(&InputBinding::String(YT_DLP_DEFAULT_OUTPUT_TEMPLATE.to_string()))
    );
    assert_eq!(
        defaults.get("extractor_args"),
        Some(&InputBinding::String(YT_DLP_DEFAULT_EXTRACTOR_ARGS.to_string()))
    );
    assert!(
        YT_DLP_DEFAULT_OUTPUT_TEMPLATE.contains("%(playlist_index|)s"),
        "yt-dlp default template should include playlist-index marker slot"
    );
    assert_eq!(defaults.get("write_thumbnail"), Some(&InputBinding::String("true".to_string())));
    assert_eq!(
        defaults.get("write_all_thumbnails"),
        Some(&InputBinding::String("false".to_string()))
    );
    assert_eq!(defaults.get("clean_info_json"), Some(&InputBinding::String("true".to_string())));
    assert_eq!(defaults.get("write_comments"), Some(&InputBinding::String("false".to_string())));
    assert_eq!(defaults.get("write_annotations"), Some(&InputBinding::String("false".to_string())));
    assert_eq!(defaults.get("write_chapters"), Some(&InputBinding::String("true".to_string())));
    assert_eq!(defaults.get("write_url_link"), Some(&InputBinding::String("true".to_string())));
    assert_eq!(defaults.get("write_webloc_link"), Some(&InputBinding::String("true".to_string())));
    assert_eq!(defaults.get("write_desktop_link"), Some(&InputBinding::String("true".to_string())));
    assert_eq!(
        defaults.get("download_archive"),
        Some(&InputBinding::String("downloads/archive.txt".to_string()))
    );
    assert_eq!(defaults.get("cache_dir"), Some(&InputBinding::String(String::new())));
}

/// Verifies unified subtitle input wiring controls both manual and
/// automatic yt-dlp subtitle switches through `write_subs`.
#[test]
fn yt_dlp_write_subs_tokens_cover_manual_and_automatic_switches() {
    assert!(!YT_DLP_OPTION_INPUTS.contains(&"write_auto_subs"));

    let tokens = option_tokens_for_input("yt-dlp", "write_subs");
    assert!(tokens.contains(&"${*inputs.write_subs == \"true\" ? --write-subs | ''}".to_string()));
    assert!(
        tokens.contains(&"${*inputs.write_subs == \"false\" ? --no-write-subs | ''}".to_string())
    );
    assert!(
        tokens.contains(&"${*inputs.write_subs == \"true\" ? --write-auto-subs | ''}".to_string())
    );
    assert!(
        tokens.contains(
            &"${*inputs.write_subs == \"false\" ? --no-write-auto-subs | ''}".to_string()
        )
    );
}

/// Verifies media-tagger defaults include strict-identification behavior,
/// runtime-root cache location, and one-day cache expiry budget.
#[test]
fn media_tagger_defaults_include_workspace_cache_and_expiry() {
    let defaults = default_input_defaults_for_tool("media-tagger", FfmpegSlotLimits::default());

    assert_eq!(
        defaults.get("strict_identification"),
        Some(&InputBinding::String("true".to_string()))
    );
    assert_eq!(
        defaults.get("embed_only_one_front_image"),
        Some(&InputBinding::String("true".to_string()))
    );

    assert_eq!(defaults.get("cache_dir"), Some(&InputBinding::String(String::new())));
    assert_eq!(
        defaults.get("cache_expiry_seconds"),
        Some(&InputBinding::String(
            crate::builtins::media_tagger::DEFAULT_CACHE_EXPIRY_SECONDS.to_string()
        ))
    );
}

/// Verifies rsgain output capture accepts the full managed supported
/// extension set and no longer exposes legacy unsupported placeholders.
#[test]
fn rsgain_output_capture_supports_expected_sandbox_path_variants() {
    let outputs = build_tool_outputs("rsgain", FfmpegSlotLimits::default());
    let output = outputs.get("content").expect("missing content capture");

    let OutputCaptureSpec::FileRegex { path_regex } = &output.capture else {
        panic!("expected file-regex capture for rsgain content");
    };

    assert_eq!(path_regex, &rsgain_output_file_regex());
    assert!(!path_regex.contains("mkv"));
    assert!(!path_regex.contains("mka"));
}

/// Verifies yt-dlp sidecar-family outputs use regex folder captures so
/// one shared downloader run can publish isolated artifact bundles.
#[test]
fn yt_dlp_sidecar_outputs_use_regex_folder_captures() {
    let outputs = build_tool_outputs("yt-dlp", FfmpegSlotLimits::default());

    for output_name in [OUTPUT_YT_DLP_SUBTITLE_ARTIFACTS, OUTPUT_YT_DLP_LINK_ARTIFACTS] {
        let output = outputs
            .get(output_name)
            .unwrap_or_else(|| panic!("missing output capture '{output_name}'"));
        match &output.capture {
            OutputCaptureSpec::FolderRegex { path_regex } => {
                assert!(
                    path_regex.contains("(.+?)"),
                    "expected capture-group rename regex for '{output_name}', got '{path_regex}'"
                );
            }
            other => {
                panic!("expected folder_regex capture for '{output_name}', got '{other:?}'")
            }
        }
    }
}

/// Verifies yt-dlp optional sidecar/runtime captures allow empty matches
/// while primary media outputs stay required.
#[test]
fn yt_dlp_optional_outputs_allow_empty_but_primary_outputs_remain_required() {
    let outputs = build_tool_outputs("yt-dlp", FfmpegSlotLimits::default());

    for required_output in [OUTPUT_CONTENT, "primary"] {
        let output = outputs
            .get(required_output)
            .unwrap_or_else(|| panic!("missing output capture '{required_output}'"));
        assert!(
            !output.allow_empty,
            "required yt-dlp output '{required_output}' must not allow empty capture"
        );
    }

    for optional_output in [
        OUTPUT_SANDBOX_ARTIFACTS,
        "stdout",
        "stderr",
        "process_code",
        OUTPUT_YT_DLP_SUBTITLE_ARTIFACTS,
        OUTPUT_YT_DLP_THUMBNAIL_ARTIFACTS,
        OUTPUT_YT_DLP_LINK_ARTIFACTS,
        OUTPUT_YT_DLP_CHAPTER_ARTIFACTS,
        OUTPUT_YT_DLP_DESCRIPTION_FILE,
        OUTPUT_YT_DLP_ANNOTATION_FILE,
        OUTPUT_YT_DLP_INFOJSON_FILE,
        OUTPUT_YT_DLP_ARCHIVE_FILE,
        OUTPUT_YT_DLP_PLAYLIST_DESCRIPTION_FILE,
        OUTPUT_YT_DLP_PLAYLIST_INFOJSON_FILE,
    ] {
        let output = outputs
            .get(optional_output)
            .unwrap_or_else(|| panic!("missing output capture '{optional_output}'"));
        assert!(
            output.allow_empty,
            "optional yt-dlp output '{optional_output}' should allow empty capture"
        );
    }
}

/// Verifies chapter-family regex supports optional split suffixes so
/// marker stripping also applies to unsplit yt-dlp output names.
#[test]
fn yt_dlp_chapter_regex_supports_unsplit_outputs_without_marker_leak() {
    let outputs = build_tool_outputs("yt-dlp", FfmpegSlotLimits::default());
    let chapter_output =
        outputs.get(OUTPUT_YT_DLP_CHAPTER_ARTIFACTS).expect("missing chapter output capture");

    let OutputCaptureSpec::FolderRegex { path_regex } = &chapter_output.capture else {
        panic!("expected folder_regex capture for chapter artifacts");
    };

    assert!(
        path_regex.contains("(?: - .+)?"),
        "chapter regex should allow optional split suffix for unsplit outputs: {path_regex}"
    );
    assert!(
        path_regex.contains("__mediapm__(") && path_regex.contains("|(.+?)("),
        "chapter regex should include marker-present vs marker-absent alternation captures: {path_regex}"
    );
}

/// Verifies thumbnail-family regex strips `__mediapm__` and supports an
/// optional numeric index (e.g. `.0.jpg`) produced by yt-dlp when
/// `write_all_thumbnails` is enabled.
#[test]
fn yt_dlp_thumbnail_regex_strips_marker_and_supports_numeric_index() {
    let outputs = build_tool_outputs("yt-dlp", FfmpegSlotLimits::default());
    let thumbnail_output =
        outputs.get(OUTPUT_YT_DLP_THUMBNAIL_ARTIFACTS).expect("missing thumbnail output capture");

    let OutputCaptureSpec::FolderRegex { path_regex } = &thumbnail_output.capture else {
        panic!("expected folder_regex capture for thumbnail artifacts");
    };

    let re = regex::Regex::new(path_regex).expect("thumbnail regex must be valid");

    // Single thumbnail with marker — no numeric index.
    let captures = re
        .captures("downloads/Title [abc123]__mediapm__.jpg")
        .expect("single-thumbnail with marker should match");
    let parts: Vec<&str> = captures.iter().skip(1).flatten().map(|m| m.as_str()).collect();
    assert_eq!(parts.join(""), "Title [abc123].jpg", "marker must be stripped");

    // Numbered thumbnail with marker and numeric index.
    let captures = re
        .captures("downloads/Title [abc123]__mediapm__.0.jpg")
        .expect("indexed thumbnail with marker should match");
    let parts: Vec<&str> = captures.iter().skip(1).flatten().map(|m| m.as_str()).collect();
    assert_eq!(
        parts.join(""),
        "Title [abc123].0.jpg",
        "marker must be stripped while numeric index is preserved"
    );

    // Thumbnail without marker — passthrough behavior.
    let captures =
        re.captures("downloads/Title [abc123].jpg").expect("thumbnail without marker should match");
    let parts: Vec<&str> = captures.iter().skip(1).flatten().map(|m| m.as_str()).collect();
    assert_eq!(parts.join(""), "Title [abc123].jpg", "passthrough path must be preserved");
}
/// Verifies singular yt-dlp annotation output uses file capture semantics
/// rather than folder-regex artifact bundling.
#[test]
fn yt_dlp_annotation_output_uses_file_capture() {
    let outputs = build_tool_outputs("yt-dlp", FfmpegSlotLimits::default());
    let annotation_output =
        outputs.get(OUTPUT_YT_DLP_ANNOTATION_FILE).expect("missing annotation output capture");

    let OutputCaptureSpec::FileRegex { path_regex } = &annotation_output.capture else {
        panic!("expected file_regex capture for annotation output");
    };

    assert!(
        path_regex.contains("annotation"),
        "annotation capture regex should target annotation sidecar files: {path_regex}"
    );
}

/// Verifies playlist sidecar file captures require a playlist index marker
/// so single-item/non-playlist downloads do not leak into playlist file
/// outputs.
#[test]
fn yt_dlp_playlist_file_regexes_require_playlist_index_marker() {
    let outputs = build_tool_outputs("yt-dlp", FfmpegSlotLimits::default());

    for output_name in
        [OUTPUT_YT_DLP_PLAYLIST_DESCRIPTION_FILE, OUTPUT_YT_DLP_PLAYLIST_INFOJSON_FILE]
    {
        let playlist_output = outputs
            .get(output_name)
            .unwrap_or_else(|| panic!("missing playlist output capture '{output_name}'"));

        let OutputCaptureSpec::FileRegex { path_regex } = &playlist_output.capture else {
            panic!("expected file_regex capture for playlist output '{output_name}'");
        };

        assert!(
            path_regex.contains("\\x5B") && path_regex.contains("\\x5D[0-9]+"),
            "playlist capture regex should require bracket-terminated id followed by playlist index: {path_regex}"
        );
    }
}

/// Verifies platform-conditional command validation accepts a
/// cross-platform content-map that satisfies all selector branches.
#[test]
fn validate_tool_command_accepts_all_platform_selector_targets() {
    let command = "${context.os == \"windows\" ? windows/tool.exe | ''}${context.os == \"linux\" ? linux/tool | ''}${context.os == \"macos\" ? macos/tool | ''}".to_string();

    let content_map = BTreeMap::from([
        ("windows/tool.exe".to_string(), Hash::from_content(b"windows-tool")),
        ("linux/tool".to_string(), Hash::from_content(b"linux-tool")),
        ("macos/tool".to_string(), Hash::from_content(b"macos-tool")),
    ]);

    validate_tool_command("fixture", &[command], &content_map)
        .expect("cross-platform platform content should validate");
}

/// Verifies platform-conditional command validation still fails when the
/// non-host selector branch target is missing from `content_map`.
#[test]
fn validate_tool_command_rejects_missing_non_host_selector_target() {
    let command = "${context.os == \"windows\" ? windows/tool.exe | ''}${context.os == \"linux\" ? linux/tool | ''}${context.os == \"macos\" ? macos/tool | ''}".to_string();
    let content_map = BTreeMap::from([
        ("windows/tool.exe".to_string(), Hash::from_content(b"windows-tool")),
        ("linux/tool".to_string(), Hash::from_content(b"linux-tool")),
    ]);

    let error = validate_tool_command("fixture", &[command], &content_map)
        .expect_err("missing selector target should fail validation");
    let message = error.to_string();
    assert!(
        message.contains("command selector") && message.contains("content_map"),
        "unexpected validation error message: {message}"
    );
}

/// Verifies launcher resolution can reuse a profile-adjacent mediapm
/// binary when examples run from `target/<profile>/examples`.
#[test]
fn resolve_profile_adjacent_binary_for_example_uses_neighboring_mediapm_binary() {
    let temp = tempdir().expect("tempdir");
    let target_dir = temp.path().join("target");
    let profile_dir = target_dir.join("debug");
    let examples_dir = profile_dir.join("examples");
    fs::create_dir_all(&examples_dir).expect("create examples directory");

    let current_exe = examples_dir.join(if cfg!(windows) { "demo.exe" } else { "demo" });
    fs::write(&current_exe, b"example").expect("write current exe fixture");

    let expected_binary = profile_dir.join(if cfg!(windows) { "mediapm.exe" } else { "mediapm" });
    fs::write(&expected_binary, b"mediapm").expect("write mediapm binary fixture");

    let resolved = resolve_profile_adjacent_mediapm_binary_for_example(&current_exe)
        .expect("resolution should succeed");
    assert_eq!(resolved.as_deref(), Some(expected_binary.as_path()));
}

/// Verifies non-example executables do not trigger profile-adjacent
/// launcher path resolution.
#[test]
fn resolve_profile_adjacent_binary_for_non_example_returns_none() {
    let temp = tempdir().expect("tempdir");
    let current_exe = temp.path().join(if cfg!(windows) { "demo.exe" } else { "demo" });
    fs::write(&current_exe, b"example").expect("write current exe fixture");

    let resolved = resolve_profile_adjacent_mediapm_binary_for_example(&current_exe)
        .expect("resolution should succeed");
    assert!(resolved.is_none());
}

/// Verifies workspace-root inference climbs from target directories to the
/// nearest Cargo workspace manifest.
#[test]
fn find_workspace_root_for_target_dir_detects_manifest_ancestor() {
    let temp = tempdir().expect("tempdir");
    let workspace_root = temp.path().join("workspace");
    fs::create_dir_all(&workspace_root).expect("create workspace root");
    fs::write(workspace_root.join("Cargo.toml"), b"[workspace]\nmembers=[]\n")
        .expect("write workspace manifest");

    let target_dir = workspace_root.join("target").join("custom-profile");
    fs::create_dir_all(&target_dir).expect("create target dir");

    let inferred =
        find_workspace_root_for_target_dir(&target_dir).expect("workspace root should resolve");
    assert_eq!(inferred, workspace_root);
}
