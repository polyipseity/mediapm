//! CLI token specs and generators for managed tool option inputs.
//!
//! Each tool's options are mapped to CLI tokens via [`TokenSpec`] entries.
//! Cover-art slot names and container-condition helpers are also defined here.

#![allow(dead_code)]
// TODO: Stream A stubs — wired when provisioning pipeline is complete.

/// Describes how one option input is rendered as CLI tokens.
#[derive(Debug, Clone, Copy)]
pub(super) enum TokenSpec {
    /// `--flag=value`
    Pair,
    /// `--flag` / absent
    Bool,
    /// `--flag=value` / absent (where value is computed from context)
    BoolPair,
    /// No token (e.g. list inputs like `option_args`).
    None,
}

// ── Token specs per tool ─────────────────────────────────────────────────

/// Map from option input name to [`TokenSpec`] for yt-dlp.
const YT_DLP_TOKEN_SPECS: &[(&str, TokenSpec)] = &[
    ("option_args", TokenSpec::None),
    ("format", TokenSpec::Pair),
    ("output", TokenSpec::Pair),
    ("paths", TokenSpec::Pair),
    ("playlist_start", TokenSpec::Pair),
    ("playlist_end", TokenSpec::Pair),
    ("playlist_reverse", TokenSpec::Bool),
    ("playlist_random", TokenSpec::Bool),
    ("match_title", TokenSpec::Pair),
    ("reject_title", TokenSpec::Pair),
    ("max_downloads", TokenSpec::Pair),
    ("min_filesize", TokenSpec::Pair),
    ("max_filesize", TokenSpec::Pair),
    ("date", TokenSpec::Pair),
    ("dateafter", TokenSpec::Pair),
    ("datebefore", TokenSpec::Pair),
    ("sub_langs", TokenSpec::Pair),
    ("write_subs", TokenSpec::Bool),
    ("write_auto_subs", TokenSpec::Bool),
    ("embed_subs", TokenSpec::Bool),
    ("sub_format", TokenSpec::Pair),
    ("merge_output_format", TokenSpec::Pair),
    ("extract_audio", TokenSpec::Bool),
    ("audio_format", TokenSpec::Pair),
    ("audio_quality", TokenSpec::Pair),
    ("remux_video", TokenSpec::Pair),
    ("recode_video", TokenSpec::Pair),
    ("postprocessor_args", TokenSpec::Pair),
    ("embed_thumbnail", TokenSpec::Bool),
    ("embed_metadata", TokenSpec::Bool),
    ("embed_chapters", TokenSpec::Bool),
    ("embed_info_json", TokenSpec::Bool),
    ("write_thumbnail", TokenSpec::Bool),
    ("write_all_thumbnails", TokenSpec::Bool),
    ("write_info_json", TokenSpec::Bool),
    ("clean_info_json", TokenSpec::Bool),
    ("write_comments", TokenSpec::Bool),
    ("write_description", TokenSpec::Bool),
    ("write_annotations", TokenSpec::Bool),
    ("write_chapters", TokenSpec::Bool),
    ("write_link", TokenSpec::Bool),
    ("write_url_link", TokenSpec::Bool),
    ("write_webloc_link", TokenSpec::Bool),
    ("write_desktop_link", TokenSpec::Bool),
    ("download_archive", TokenSpec::Pair),
    ("split_chapters", TokenSpec::Bool),
    ("no_playlist", TokenSpec::Bool),
    ("yes_playlist", TokenSpec::Bool),
    ("limit_rate", TokenSpec::Pair),
    ("retries", TokenSpec::Pair),
    ("fragment_retries", TokenSpec::Pair),
    ("skip_unavailable_fragments", TokenSpec::Bool),
    ("abort_on_unavailable_fragment", TokenSpec::Bool),
    ("extractor_args", TokenSpec::Pair),
    ("cache_dir", TokenSpec::Pair),
    ("cookies", TokenSpec::Pair),
    ("cookies_from_browser", TokenSpec::Pair),
    ("ffmpeg_location", TokenSpec::Pair),
    ("concurrent_fragment_downloads", TokenSpec::Pair),
];

/// Map from option input name to [`TokenSpec`] for ffmpeg.
const FFMPEG_TOKEN_SPECS: &[(&str, TokenSpec)] = &[
    ("option_args", TokenSpec::None),
    ("vn", TokenSpec::Bool),
    ("an", TokenSpec::Bool),
    ("sn", TokenSpec::Bool),
    ("dn", TokenSpec::Bool),
    ("codec_copy", TokenSpec::Bool),
    ("codec_video", TokenSpec::Pair),
    ("codec_audio", TokenSpec::Pair),
    ("codec_subtitle", TokenSpec::Pair),
    ("pixel_format", TokenSpec::Pair),
    ("max_muxing_queue_size", TokenSpec::Pair),
    ("threads", TokenSpec::Pair),
    ("preset", TokenSpec::Pair),
    ("crf", TokenSpec::Pair),
    ("bitrate_video", TokenSpec::Pair),
    ("bitrate_audio", TokenSpec::Pair),
    ("sample_rate", TokenSpec::Pair),
    ("channels", TokenSpec::Pair),
    ("volume", TokenSpec::Pair),
    ("filter_complex", TokenSpec::Pair),
    ("filter_video", TokenSpec::Pair),
    ("filter_audio", TokenSpec::Pair),
    ("filter_subtitle", TokenSpec::Pair),
    ("map_metadata", TokenSpec::Pair),
    ("map_chapters", TokenSpec::Pair),
    ("metadata", TokenSpec::Pair),
    ("timestamp", TokenSpec::Pair),
    ("ss", TokenSpec::Pair),
    ("t", TokenSpec::Pair),
    ("to", TokenSpec::Pair),
    ("frames", TokenSpec::Pair),
    ("frame_rate", TokenSpec::Pair),
    ("resolution", TokenSpec::Pair),
    ("aspect", TokenSpec::Pair),
    ("crop", TokenSpec::Pair),
    ("rotate", TokenSpec::Pair),
    ("hflip", TokenSpec::Bool),
    ("vflip", TokenSpec::Bool),
    ("color_balance", TokenSpec::Pair),
    ("color_brightness", TokenSpec::Pair),
    ("color_contrast", TokenSpec::Pair),
    ("color_saturation", TokenSpec::Pair),
    ("color_gamma", TokenSpec::Pair),
    ("deinterlace", TokenSpec::Bool),
    ("denoise", TokenSpec::Pair),
    ("sharpness", TokenSpec::Pair),
    ("subtitles", TokenSpec::Pair),
    ("ass_subtitles", TokenSpec::Pair),
    ("watermark", TokenSpec::Pair),
    ("watermark_position", TokenSpec::Pair),
    ("movflags", TokenSpec::Pair),
    ("cues_to_front", TokenSpec::Pair),
    ("audio_codec", TokenSpec::Pair),
    ("audio_bitrate", TokenSpec::Pair),
    ("audio_sample_rate", TokenSpec::Pair),
    ("audio_channels", TokenSpec::Pair),
    ("hide_banner", TokenSpec::Bool),
    ("overwrite_output", TokenSpec::Bool),
];

/// Map from option input name to [`TokenSpec`] for rsgain.
const RSGAIN_TOKEN_SPECS: &[(&str, TokenSpec)] = &[
    ("option_args", TokenSpec::None),
    ("input_extension", TokenSpec::Pair),
    ("album", TokenSpec::Bool),
    ("album_mode", TokenSpec::Bool),
    ("target_lufs", TokenSpec::Pair),
    ("tagmode", TokenSpec::Pair),
    ("clip_mode", TokenSpec::Pair),
    ("true_peak", TokenSpec::Bool),
    ("max_peak", TokenSpec::Pair),
    ("preserve_mtimes", TokenSpec::Bool),
    ("loudness_range", TokenSpec::Pair),
    ("integrated_loudness", TokenSpec::Pair),
    ("true_peak_level", TokenSpec::Pair),
    ("lra_loudness", TokenSpec::Pair),
    ("loudness_correction", TokenSpec::Pair),
    ("sample_peak", TokenSpec::Pair),
    ("bit_depth", TokenSpec::Pair),
    ("dynamic_range", TokenSpec::Pair),
    ("dynamic_range_max", TokenSpec::Pair),
    ("dynamic_range_count", TokenSpec::Pair),
    ("dynamic_range_avg", TokenSpec::Pair),
    ("dynamic_range_stdev", TokenSpec::Pair),
    ("dynamic_range_threshold", TokenSpec::Pair),
    ("dynamic_range_histogram", TokenSpec::Pair),
    ("dynamic_range_histogram_count", TokenSpec::Pair),
    ("dynamic_range_histogram_bins", TokenSpec::Pair),
];

/// Map from option input name to [`TokenSpec`] for media-tagger.
const MEDIA_TAGGER_TOKEN_SPECS: &[(&str, TokenSpec)] = &[
    ("option_args", TokenSpec::None),
    ("strict_identification", TokenSpec::Bool),
    ("write_all_tags", TokenSpec::Bool),
    ("write_all_images", TokenSpec::Bool),
    ("save_images_to_tags", TokenSpec::Bool),
    ("enable_tag_saving", TokenSpec::Bool),
    ("embed_only_one_front_image", TokenSpec::Bool),
    ("ca_providers", TokenSpec::Pair),
    ("caa_image_types", TokenSpec::Pair),
    ("caa_image_size", TokenSpec::Pair),
    ("caa_approved_only", TokenSpec::Bool),
    ("preserve_images", TokenSpec::Bool),
    ("clear_existing_tags", TokenSpec::Bool),
    ("release_ars", TokenSpec::Bool),
    ("cover_art_slot_count", TokenSpec::Pair),
    ("acoustid_endpoint", TokenSpec::Pair),
    ("musicbrainz_endpoint", TokenSpec::Pair),
    ("cache_dir", TokenSpec::Pair),
    ("cache_expiry_seconds", TokenSpec::Pair),
    ("acoustid_api_key", TokenSpec::Pair),
    ("enable_acoustid", TokenSpec::Bool),
];

// ── Public helpers ───────────────────────────────────────────────────────

/// Returns the [`TokenSpec`] map for the named tool.
#[must_use]
pub(super) fn command_option_tokens_for_tool(
    tool_name: &str,
) -> &'static [(&'static str, TokenSpec)] {
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

/// Resolves the list of CLI tokens for one option input of one tool.
///
/// Returns `None` when the input is unknown or has [`TokenSpec::None`].
#[must_use]
pub(super) fn option_tokens_for_input(
    tool_name: &str,
    input_key: &str,
    input_value: &str,
) -> Option<Vec<String>> {
    let specs = command_option_tokens_for_tool(tool_name);
    for (name, spec) in specs {
        if *name == input_key {
            return match spec {
                TokenSpec::Pair => {
                    if input_value.is_empty() {
                        None
                    } else {
                        Some(vec![format!("--{name}={input_value}")])
                    }
                }
                TokenSpec::Bool => {
                    if input_value.eq_ignore_ascii_case("true") {
                        Some(vec![format!("--{name}")])
                    } else {
                        None
                    }
                }
                TokenSpec::BoolPair => {
                    if input_value.is_empty() {
                        None
                    } else {
                        Some(vec![format!("--{name}={input_value}")])
                    }
                }
                TokenSpec::None => None,
            };
        }
    }
    None
}

// ── Cover-art slot helpers ───────────────────────────────────────────────

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
    format!("output_{index}")
}

/// Returns the sandbox-path input name for one ffmpeg indexed output path.
#[must_use]
pub(super) fn ffmpeg_output_path_input_name(index: u32) -> String {
    format!("output_path_{index}")
}

/// Returns a FileRegex pattern for one ffmpeg output slot.
#[must_use]
pub(super) fn ffmpeg_output_file_regex(index: u32) -> String {
    format!(r"^{index}_output\.\w+$")
}
