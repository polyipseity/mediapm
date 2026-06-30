//! CLI token specs and generators for managed tool option inputs.
//!
//! Each tool's options are mapped to CLI tokens via [`TokenSpec`] entries.
//! Cover-art slot names and container-condition helpers are also defined here.

#![allow(dead_code)]

/// Describes how one option input is rendered as CLI tokens.
///
/// Each variant carries the CLI flag string(s) to emit when the input is
/// present. The flag strings use the actual tool CLI conventions (e.g.
/// dashes for long flags) rather than deriving from the input name.
#[derive(Debug, Clone, Copy)]
pub(super) enum TokenSpec {
    /// `--flag=value` — emitted when the input has a non-empty value.
    Pair(&'static str),
    /// `--flag` — emitted only when the input value is `"true"`.
    Bool(&'static str),
    /// `--flag value` — emitted as a pair when the input is `"true"`.
    BoolPair(&'static str, &'static str),
    /// No token (e.g. list inputs like `option_args`).
    None,
}

// ── Token specs per tool ─────────────────────────────────────────────────

/// Map from option input name to [`TokenSpec`] for yt-dlp.
const YT_DLP_TOKEN_SPECS: &[(&str, TokenSpec)] = &[
    ("format", TokenSpec::Pair("-f")),
    ("format_sort", TokenSpec::Pair("-S")),
    ("extract_audio", TokenSpec::Bool("--extract-audio")),
    ("audio_format", TokenSpec::Pair("--audio-format")),
    ("audio_quality", TokenSpec::Pair("--audio-quality")),
    ("remux_video", TokenSpec::Pair("--remux-video")),
    ("recode_video", TokenSpec::Pair("--recode-video")),
    ("merge_output_format", TokenSpec::Pair("--merge-output-format")),
    ("embed_thumbnail", TokenSpec::Bool("--embed-thumbnail")),
    ("embed_metadata", TokenSpec::Bool("--embed-metadata")),
    ("embed_subs", TokenSpec::Bool("--embed-subs")),
    ("embed_chapters", TokenSpec::Bool("--embed-chapters")),
    ("embed_info_json", TokenSpec::Bool("--embed-info-json")),
    ("write_subs", TokenSpec::Bool("--write-subs")),
    ("write_auto_subs", TokenSpec::Bool("--write-auto-subs")),
    ("sub_langs", TokenSpec::Pair("--sub-langs")),
    ("sub_format", TokenSpec::Pair("--sub-format")),
    ("convert_subs", TokenSpec::Pair("--convert-subs")),
    ("write_thumbnail", TokenSpec::Bool("--write-thumbnail")),
    ("write_all_thumbnails", TokenSpec::Bool("--write-all-thumbnails")),
    ("convert_thumbnails", TokenSpec::Pair("--convert-thumbnails")),
    ("write_info_json", TokenSpec::Bool("--write-info-json")),
    ("clean_info_json", TokenSpec::Bool("--clean-info-json")),
    ("write_comments", TokenSpec::Bool("--write-comments")),
    ("write_description", TokenSpec::Bool("--write-description")),
    ("write_annotations", TokenSpec::Bool("--write-annotations")),
    ("write_chapters", TokenSpec::Bool("--write-chapters")),
    ("write_link", TokenSpec::Bool("--write-link")),
    ("write_url_link", TokenSpec::Bool("--write-url-link")),
    ("write_webloc_link", TokenSpec::Bool("--write-webloc-link")),
    ("write_desktop_link", TokenSpec::Bool("--write-desktop-link")),
    ("split_chapters", TokenSpec::Bool("--split-chapters")),
    ("playlist_items", TokenSpec::Pair("--playlist-items")),
    ("no_playlist", TokenSpec::Bool("--no-playlist")),
    ("skip_download", TokenSpec::Bool("--skip-download")),
    ("retries", TokenSpec::Pair("--retries")),
    ("limit_rate", TokenSpec::Pair("--limit-rate")),
    ("concurrent_fragments", TokenSpec::Pair("--concurrent-fragments")),
    ("proxy", TokenSpec::Pair("--proxy")),
    ("socket_timeout", TokenSpec::Pair("--socket-timeout")),
    ("sleep_subtitles", TokenSpec::Pair("--sleep-subtitles")),
    ("user_agent", TokenSpec::Pair("--user-agent")),
    ("referer", TokenSpec::Pair("--referer")),
    ("add_header", TokenSpec::Pair("--add-header")),
    ("cookies", TokenSpec::Pair("--cookies")),
    ("cookies_from_browser", TokenSpec::Pair("--cookies-from-browser")),
    ("paths", TokenSpec::Pair("--paths")),
    ("js_runtimes", TokenSpec::Pair("--js-runtimes")),
    ("output", TokenSpec::Pair("--output")),
    ("parse_metadata", TokenSpec::Pair("--parse-metadata")),
    ("replace_in_metadata", TokenSpec::Pair("--replace-in-metadata")),
    ("download_sections", TokenSpec::Pair("--download-sections")),
    ("postprocessor_args", TokenSpec::Pair("--postprocessor-args")),
    ("extractor_args", TokenSpec::Pair("--extractor-args")),
    ("http_chunk_size", TokenSpec::Pair("--http-chunk-size")),
    ("download_archive", TokenSpec::Pair("--download-archive")),
    ("sponsorblock_mark", TokenSpec::Pair("--sponsorblock-mark")),
    ("sponsorblock_remove", TokenSpec::Pair("--sponsorblock-remove")),
];

/// Map from option input name to [`TokenSpec`] for ffmpeg.
const FFMPEG_TOKEN_SPECS: &[(&str, TokenSpec)] = &[
    ("audio_codec", TokenSpec::Pair("-c:a")),
    ("video_codec", TokenSpec::Pair("-c:v")),
    ("container", TokenSpec::Pair("-f")),
    ("audio_bitrate", TokenSpec::Pair("-b:a")),
    ("video_bitrate", TokenSpec::Pair("-b:v")),
    ("audio_quality", TokenSpec::Pair("-q:a")),
    ("video_quality", TokenSpec::Pair("-q:v")),
    ("crf", TokenSpec::Pair("-crf")),
    ("preset", TokenSpec::Pair("-preset")),
    ("threads", TokenSpec::Pair("-threads")),
    ("log_level", TokenSpec::Pair("-loglevel")),
    ("progress", TokenSpec::Pair("-progress")),
    ("tune", TokenSpec::Pair("-tune")),
    ("profile", TokenSpec::Pair("-profile:v")),
    ("level", TokenSpec::Pair("-level")),
    ("pixel_format", TokenSpec::Pair("-pix_fmt")),
    ("frame_rate", TokenSpec::Pair("-r")),
    ("sample_rate", TokenSpec::Pair("-ar")),
    ("channels", TokenSpec::Pair("-ac")),
    ("audio_filters", TokenSpec::Pair("-af")),
    ("video_filters", TokenSpec::Pair("-vf")),
    ("filter_complex", TokenSpec::Pair("-filter_complex")),
    ("start_time", TokenSpec::Pair("-ss")),
    ("duration", TokenSpec::Pair("-t")),
    ("to", TokenSpec::Pair("-to")),
    ("movflags", TokenSpec::Pair("-movflags")),
    ("cues_to_front", TokenSpec::BoolPair("-cues_to_front", "1")),
    ("map_metadata", TokenSpec::Pair("-map_metadata")),
    ("map_chapters", TokenSpec::Pair("-map_chapters")),
    ("map", TokenSpec::Pair("-map")),
    ("map_channel", TokenSpec::Pair("-map_channel")),
    ("copy_ts", TokenSpec::Bool("-copyts")),
    ("start_at_zero", TokenSpec::Bool("-start_at_zero")),
    ("stats", TokenSpec::Bool("-stats")),
    ("no_overwrite", TokenSpec::Bool("-n")),
    ("codec_copy", TokenSpec::BoolPair("-c", "copy")),
    ("faststart", TokenSpec::BoolPair("-movflags", "+faststart")),
    ("hwaccel", TokenSpec::Pair("-hwaccel")),
    ("sample_format", TokenSpec::Pair("-sample_fmt")),
    ("channel_layout", TokenSpec::Pair("-channel_layout")),
    ("metadata", TokenSpec::Pair("-metadata")),
    ("timestamp", TokenSpec::Pair("-timestamp")),
    ("disposition", TokenSpec::Pair("-disposition")),
    ("fps_mode", TokenSpec::Pair("-fps_mode")),
    ("force_key_frames", TokenSpec::Pair("-force_key_frames")),
    ("aspect", TokenSpec::Pair("-aspect")),
    ("stream_loop", TokenSpec::Pair("-stream_loop")),
    ("max_muxing_queue_size", TokenSpec::Pair("-max_muxing_queue_size")),
    ("strict", TokenSpec::Pair("-strict")),
    ("maxrate", TokenSpec::Pair("-maxrate")),
    ("bufsize", TokenSpec::Pair("-bufsize")),
    ("bitstream_filter", TokenSpec::Pair("-bsf")),
    ("id3v2_version", TokenSpec::Pair("-id3v2_version")),
    ("shortest", TokenSpec::Bool("-shortest")),
    ("vn", TokenSpec::Bool("-vn")),
    ("an", TokenSpec::Bool("-an")),
    ("sn", TokenSpec::Bool("-sn")),
    ("dn", TokenSpec::Bool("-dn")),
    ("hide_banner", TokenSpec::Bool("-hide_banner")),
];

/// Map from option input name to [`TokenSpec`] for rsgain.
const RSGAIN_TOKEN_SPECS: &[(&str, TokenSpec)] = &[
    ("input_extension", TokenSpec::None),
    ("mode", TokenSpec::None),
    ("album", TokenSpec::Bool("--album")),
    ("album_mode", TokenSpec::Bool("--album")),
    ("album_aes77", TokenSpec::Bool("--album-aes77")),
    ("skip_existing", TokenSpec::Bool("--skip-existing")),
    ("tagmode", TokenSpec::Pair("--tagmode")),
    ("target_lufs", TokenSpec::Pair("--loudness")),
    ("loudness", TokenSpec::Pair("--loudness")),
    ("clip_mode", TokenSpec::Pair("--clip-mode")),
    ("true_peak", TokenSpec::Bool("--true-peak")),
    ("dual_mono", TokenSpec::Bool("--dual-mono")),
    ("max_peak", TokenSpec::Pair("--max-peak")),
    ("lowercase", TokenSpec::Bool("--lowercase")),
    ("id3v2_version", TokenSpec::Pair("--id3v2-version")),
    ("opus_mode", TokenSpec::Pair("--opus-mode")),
    ("jobs", TokenSpec::Pair("--multithread")),
    ("multithread", TokenSpec::Pair("--multithread")),
    ("preset", TokenSpec::Pair("--preset")),
    ("dry_run", TokenSpec::Bool("--dry-run")),
    ("output", TokenSpec::Pair("--output")),
    ("quiet", TokenSpec::Bool("--quiet")),
    ("skip_tags", TokenSpec::BoolPair("--tagmode", "s")),
    ("preserve_mtime", TokenSpec::Bool("--preserve-mtimes")),
    ("preserve_mtimes", TokenSpec::Bool("--preserve-mtimes")),
];

/// Map from option input name to [`TokenSpec`] for media-tagger.
const MEDIA_TAGGER_TOKEN_SPECS: &[(&str, TokenSpec)] = &[
    ("acoustid_endpoint", TokenSpec::Pair("--acoustid-endpoint")),
    ("musicbrainz_endpoint", TokenSpec::Pair("--musicbrainz-endpoint")),
    ("cache_dir", TokenSpec::Pair("--cache-dir")),
    ("cache_expiry_seconds", TokenSpec::Pair("--cache-expiry-seconds")),
    ("strict_identification", TokenSpec::Bool("--strict-identification")),
    ("write_all_tags", TokenSpec::Bool("--write-all-tags")),
    ("write_all_images", TokenSpec::Bool("--write-all-images")),
    ("save_images_to_tags", TokenSpec::Bool("--save-images-to-tags")),
    ("embed_only_one_front_image", TokenSpec::Bool("--embed-only-one-front-image")),
    ("ca_providers", TokenSpec::Pair("--ca-providers")),
    ("caa_image_types", TokenSpec::Pair("--caa-image-types")),
    ("caa_image_size", TokenSpec::Pair("--caa-image-size")),
    ("caa_approved_only", TokenSpec::Bool("--caa-approved-only")),
    ("preserve_images", TokenSpec::Bool("--preserve-images")),
    ("clear_existing_tags", TokenSpec::Bool("--clear-existing-tags")),
    ("enable_tag_saving", TokenSpec::Bool("--enable-tag-saving")),
    ("release_ars", TokenSpec::Bool("--release-ars")),
    ("cover_art_slot_count", TokenSpec::Pair("--cover-art-slot-count")),
    ("recording_mbid", TokenSpec::Pair("--recording-mbid")),
    ("release_mbid", TokenSpec::Pair("--release-mbid")),
];

// ── Private helpers ──────────────────────────────────────────────────────

/// Returns the token specs for a tool, or an empty slice.
#[must_use]
fn token_specs_for_tool(tool_name: &str) -> &'static [(&'static str, TokenSpec)] {
    if tool_name.eq_ignore_ascii_case("yt-dlp") {
        YT_DLP_TOKEN_SPECS
    } else if tool_name.eq_ignore_ascii_case("ffmpeg") {
        FFMPEG_TOKEN_SPECS
    } else if tool_name.eq_ignore_ascii_case("rsgain") {
        RSGAIN_TOKEN_SPECS
    } else if tool_name.eq_ignore_ascii_case("media-tagger") {
        MEDIA_TAGGER_TOKEN_SPECS
    } else {
        &[]
    }
}

/// Builds one unpack conditional token gated on non-empty scalar presence.
#[must_use]
fn unpack_if_truthy(input_name: &str, rendered_argument: &str) -> String {
    format!("${{*inputs.{input_name} ? {rendered_argument} | ''}}")
}

/// Builds one unpack conditional token gated on scalar equality.
#[must_use]
fn unpack_if_equals(input_name: &str, expected_value: &str, rendered_argument: &str) -> String {
    format!("${{*inputs.{input_name} == \"{expected_value}\" ? {rendered_argument} | ''}}")
}

/// Builds one scalar unpack token `${*inputs.<name>}`.
#[must_use]
fn unpack_scalar(input_name: &str) -> String {
    format!("${{*inputs.{input_name}}}")
}

// ── Template generation ──────────────────────────────────────────────────

/// Renders option argument templates for ordered option inputs.
#[must_use]
pub(super) fn command_option_tokens_for_tool(tool_name: &str, input_names: &[&str]) -> Vec<String> {
    input_names
        .iter()
        .flat_map(|input_name| option_tokens_for_input(tool_name, input_name))
        .collect()
}

/// Resolves option templates for one logical tool option input.
#[must_use]
pub(super) fn option_tokens_for_input(tool_name: &str, input_name: &str) -> Vec<String> {
    if input_name == "option_args" {
        return vec![unpack_scalar(input_name)];
    }

    let spec = token_specs_for_tool(tool_name)
        .iter()
        .find(|(name, _)| *name == input_name)
        .map(|(_, spec)| *spec);

    match spec {
        Some(TokenSpec::Pair(flag)) => pair_option_tokens(input_name, flag),
        Some(TokenSpec::Bool(flag)) => bool_flag_tokens(input_name, flag),
        Some(TokenSpec::BoolPair(flag, value)) => bool_value_pair_tokens(input_name, flag, value),
        Some(TokenSpec::None) => Vec::new(),
        None => pair_option_tokens(input_name, &format!("--{}", input_name.replace('_', "-"))),
    }
}

/// Builds `${*inputs.<name> ? <flag> | ''}` + `${*inputs.<name>}` tokens.
#[must_use]
fn pair_option_tokens(input_name: &str, flag: &str) -> Vec<String> {
    vec![unpack_if_truthy(input_name, flag), unpack_scalar(input_name)]
}

/// Builds conditional tokens that emit one flag when the option is `"true"`.
#[must_use]
fn bool_flag_tokens(input_name: &str, flag: &str) -> Vec<String> {
    vec![unpack_if_equals(input_name, "true", flag)]
}

/// Builds conditional tokens that emit `flag value` when the option is `"true"`.
#[must_use]
fn bool_value_pair_tokens(input_name: &str, flag: &str, value: &str) -> Vec<String> {
    vec![unpack_if_equals(input_name, "true", flag), unpack_if_equals(input_name, "true", value)]
}

// ── Cover-art slot helpers ───────────────────────────────────────────────

/// Builds ffmpeg cover-art map/disposition templates for managed media-tagger
/// apply workflows.
#[must_use]
pub(super) fn ffmpeg_cover_art_tokens(max_input_slots: u32, max_output_slots: u32) -> Vec<String> {
    let _ = max_output_slots;
    let mut tokens = Vec::new();

    for slot_index in 1..max_input_slots {
        let enabled_input = format!("cover_slot_{slot_index}_enabled");
        let ffmpeg_input_index = slot_index + 1;
        let output_video_index = slot_index - 1;

        tokens.push(unpack_if_equals(&enabled_input, "true", "-map"));
        tokens.push(format!(
            "${{*inputs.{enabled_input} == \"true\" ? \"{ffmpeg_input_index}:v:0?\" | ''}}"
        ));
        tokens.push(unpack_if_equals(
            &enabled_input,
            "true",
            &format!("-disposition:v:{output_video_index}"),
        ));
        tokens.push(unpack_if_equals(&enabled_input, "true", "attached_pic"));
    }

    tokens
}

/// Builds one OR-joined container-equality condition string.
#[must_use]
pub(super) fn ffmpeg_container_any_of_condition(containers: &[&str]) -> String {
    containers
        .iter()
        .map(|container| format!("inputs.container == \"{container}\""))
        .collect::<Vec<_>>()
        .join(" || ")
}

// ── Slot path helpers ────────────────────────────────────────────────────

/// Returns the sandbox-path for one ffmpeg cover-art slot input.
#[must_use]
pub(super) fn ffmpeg_cover_slot_input_name(index: u32) -> String {
    format!("cover_art_{index}")
}

/// Returns the sandbox-path for one ffmpeg cover-art slot output capture.
#[must_use]
pub(super) fn ffmpeg_cover_slot_capture_name(index: u32) -> String {
    format!("cover_art_{index}")
}

/// Returns the output capture name for one ffmpeg indexed output file.
#[must_use]
pub(super) fn ffmpeg_output_capture_name(index: u32) -> String {
    if index == 0 { "primary".to_string() } else { format!("primary_{index}") }
}

/// Returns the sandbox-path input name for one ffmpeg indexed output path.
#[must_use]
pub(super) fn ffmpeg_output_path_input_name(index: u32) -> String {
    format!("output_path_{index}")
}

/// Returns a `FileRegex` pattern for one ffmpeg output slot.
#[must_use]
pub(super) fn ffmpeg_output_file_regex(index: u32) -> String {
    format!(r"^output-{index}(?:[.][^/\\]+)?$")
}
