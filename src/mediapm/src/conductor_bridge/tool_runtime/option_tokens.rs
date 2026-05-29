//! Option-token template generators for managed tool commands.
//!
//! Each managed tool command contains `${{...}}` template tokens that expand to
//! CLI flags at runtime. This module generates those token lists from logical
//! input names and tool-specific flag mappings.
//!
//! Callers typically invoke [`command_option_tokens_for_tool`] or
//! [`option_input_names_for_tool`]; the individual token-shape helpers below
//! are private implementation details.

use super::option_constants::{
    FFMPEG_OPTION_INPUTS, MEDIA_TAGGER_OPTION_INPUTS, RSGAIN_OPTION_INPUTS, YT_DLP_OPTION_INPUTS,
};
use super::{FfmpegSlotLimits, ffmpeg_cover_slot_enabled_input_name};

/// Returns ordered option-input names for the provided managed tool.
#[must_use]
pub(super) fn option_input_names_for_tool(tool_name: &str) -> &'static [&'static str] {
    if tool_name.eq_ignore_ascii_case("yt-dlp") {
        YT_DLP_OPTION_INPUTS
    } else if tool_name.eq_ignore_ascii_case("ffmpeg") {
        FFMPEG_OPTION_INPUTS
    } else if tool_name.eq_ignore_ascii_case("rsgain") {
        RSGAIN_OPTION_INPUTS
    } else if tool_name.eq_ignore_ascii_case("media-tagger") {
        MEDIA_TAGGER_OPTION_INPUTS
    } else {
        &[]
    }
}

/// Renders option argument templates for ordered option inputs.
///
/// The generated templates rely on conductor conditional-unpack semantics so
/// `mediapm` can keep option bindings value-centric (`string` values) while
/// still producing correct CLI key/value argv forms at runtime.
#[must_use]
pub(super) fn command_option_tokens_for_tool(tool_name: &str, input_names: &[&str]) -> Vec<String> {
    input_names
        .iter()
        .flat_map(|input_name| option_tokens_for_input(tool_name, input_name))
        .collect::<Vec<_>>()
}

/// Resolves option templates for one logical tool option input.
#[must_use]
#[expect(
    clippy::too_many_lines,
    reason = "option-token mapping is intentionally exhaustive and declarative"
)]
pub(super) fn option_tokens_for_input(tool_name: &str, input_name: &str) -> Vec<String> {
    if input_name == "option_args" {
        return vec![format!("${{*inputs.{input_name}}}")];
    }

    if tool_name.eq_ignore_ascii_case("yt-dlp") {
        return match input_name {
            "format" => pair_option_tokens(input_name, "-f"),
            "format_sort" => pair_option_tokens(input_name, "-S"),
            "extract_audio" => bool_flag_tokens(input_name, "--extract-audio"),
            "audio_format" => pair_option_tokens(input_name, "--audio-format"),
            "audio_quality" => pair_option_tokens(input_name, "--audio-quality"),
            "remux_video" => pair_option_tokens(input_name, "--remux-video"),
            "recode_video" => pair_option_tokens(input_name, "--recode-video"),
            "merge_output_format" => pair_option_tokens(input_name, "--merge-output-format"),
            "embed_thumbnail" => {
                bool_switch_tokens(input_name, "--embed-thumbnail", "--no-embed-thumbnail")
            }
            "embed_metadata" => {
                bool_switch_tokens(input_name, "--embed-metadata", "--no-embed-metadata")
            }
            "embed_subs" => bool_switch_tokens(input_name, "--embed-subs", "--no-embed-subs"),
            "embed_chapters" => {
                bool_switch_tokens(input_name, "--embed-chapters", "--no-embed-chapters")
            }
            "embed_info_json" => {
                bool_switch_tokens(input_name, "--embed-info-json", "--no-embed-info-json")
            }
            "write_subs" => vec![
                "${*inputs.write_subs == \"true\" ? --write-subs | ''}".to_string(),
                "${*inputs.write_subs == \"false\" ? --no-write-subs | ''}".to_string(),
                "${*inputs.write_subs == \"true\" ? --write-auto-subs | ''}".to_string(),
                "${*inputs.write_subs == \"false\" ? --no-write-auto-subs | ''}".to_string(),
            ],
            "sub_langs" => pair_option_tokens(input_name, "--sub-langs"),
            "sub_format" => pair_option_tokens(input_name, "--sub-format"),
            "convert_subs" => pair_option_tokens(input_name, "--convert-subs"),
            "write_thumbnail" => {
                bool_switch_tokens(input_name, "--write-thumbnail", "--no-write-thumbnail")
            }
            "write_all_thumbnails" => bool_switch_tokens(
                input_name,
                "--write-all-thumbnails",
                "--no-write-all-thumbnails",
            ),
            "convert_thumbnails" => pair_option_tokens(input_name, "--convert-thumbnails"),
            "write_info_json" => {
                bool_switch_tokens(input_name, "--write-info-json", "--no-write-info-json")
            }
            "clean_info_json" => {
                bool_switch_tokens(input_name, "--clean-info-json", "--no-clean-info-json")
            }
            "write_comments" => {
                bool_switch_tokens(input_name, "--write-comments", "--no-write-comments")
            }
            "write_description" => {
                bool_switch_tokens(input_name, "--write-description", "--no-write-description")
            }
            "write_annotations" => {
                bool_switch_tokens(input_name, "--write-annotations", "--no-write-annotations")
            }
            "write_chapters" => {
                bool_switch_tokens(input_name, "--write-chapters", "--no-write-chapters")
            }
            "write_link" => bool_switch_tokens(input_name, "--write-link", "--no-write-link"),
            "write_url_link" => {
                bool_switch_tokens(input_name, "--write-url-link", "--no-write-url-link")
            }
            "write_webloc_link" => {
                bool_switch_tokens(input_name, "--write-webloc-link", "--no-write-webloc-link")
            }
            "write_desktop_link" => {
                bool_switch_tokens(input_name, "--write-desktop-link", "--no-write-desktop-link")
            }
            "split_chapters" => {
                bool_switch_tokens(input_name, "--split-chapters", "--no-split-chapters")
            }
            "playlist_items" => pair_option_tokens(input_name, "--playlist-items"),
            "no_playlist" => bool_flag_tokens(input_name, "--no-playlist"),
            "skip_download" => bool_flag_tokens(input_name, "--skip-download"),
            "retries" => pair_option_tokens(input_name, "--retries"),
            "limit_rate" => pair_option_tokens(input_name, "--limit-rate"),
            "concurrent_fragments" => pair_option_tokens(input_name, "--concurrent-fragments"),
            "proxy" => pair_option_tokens(input_name, "--proxy"),
            "socket_timeout" => pair_option_tokens(input_name, "--socket-timeout"),
            "sleep_subtitles" => pair_option_tokens(input_name, "--sleep-subtitles"),
            "user_agent" => pair_option_tokens(input_name, "--user-agent"),
            "referer" => pair_option_tokens(input_name, "--referer"),
            "add_header" => pair_option_tokens(input_name, "--add-header"),
            "cookies" => pair_option_tokens(input_name, "--cookies"),
            "cookies_from_browser" => pair_option_tokens(input_name, "--cookies-from-browser"),
            "paths" => pair_option_tokens(input_name, "--paths"),
            "js_runtimes" => pair_option_tokens(input_name, "--js-runtimes"),
            "output" => pair_option_tokens(input_name, "--output"),
            "parse_metadata" => pair_option_tokens(input_name, "--parse-metadata"),
            "replace_in_metadata" => pair_option_tokens(input_name, "--replace-in-metadata"),
            "download_sections" => pair_option_tokens(input_name, "--download-sections"),
            "postprocessor_args" => pair_option_tokens(input_name, "--postprocessor-args"),
            "extractor_args" => pair_option_tokens(input_name, "--extractor-args"),
            "http_chunk_size" => pair_option_tokens(input_name, "--http-chunk-size"),
            "download_archive" => pair_option_tokens(input_name, "--download-archive"),
            "sponsorblock_mark" => pair_option_tokens(input_name, "--sponsorblock-mark"),
            "sponsorblock_remove" => pair_option_tokens(input_name, "--sponsorblock-remove"),
            _ => pair_option_tokens(input_name, &format!("--{}", input_name.replace('_', "-"))),
        };
    }

    if tool_name.eq_ignore_ascii_case("ffmpeg") {
        return match input_name {
            "audio_codec" => pair_option_tokens(input_name, "-c:a"),
            "video_codec" => pair_option_tokens(input_name, "-c:v"),
            "container" => pair_option_tokens(input_name, "-f"),
            "audio_bitrate" => pair_option_tokens(input_name, "-b:a"),
            "video_bitrate" => pair_option_tokens(input_name, "-b:v"),
            "audio_quality" => pair_option_tokens(input_name, "-q:a"),
            "video_quality" => pair_option_tokens(input_name, "-q:v"),
            "crf" => pair_option_tokens(input_name, "-crf"),
            "preset" => pair_option_tokens(input_name, "-preset"),
            "threads" => pair_option_tokens(input_name, "-threads"),
            "log_level" => pair_option_tokens(input_name, "-loglevel"),
            "progress" => pair_option_tokens(input_name, "-progress"),
            "tune" => pair_option_tokens(input_name, "-tune"),
            "profile" => pair_option_tokens(input_name, "-profile:v"),
            "level" => pair_option_tokens(input_name, "-level"),
            "pixel_format" => pair_option_tokens(input_name, "-pix_fmt"),
            "frame_rate" => pair_option_tokens(input_name, "-r"),
            "sample_rate" => pair_option_tokens(input_name, "-ar"),
            "channels" => pair_option_tokens(input_name, "-ac"),
            "audio_filters" => pair_option_tokens(input_name, "-af"),
            "video_filters" => pair_option_tokens(input_name, "-vf"),
            "filter_complex" => pair_option_tokens(input_name, "-filter_complex"),
            "start_time" => pair_option_tokens(input_name, "-ss"),
            "duration" => pair_option_tokens(input_name, "-t"),
            "to" => pair_option_tokens(input_name, "-to"),
            "movflags" => pair_option_tokens(input_name, "-movflags"),
            "cues_to_front" => bool_value_pair_tokens(input_name, "-cues_to_front", "1"),
            "map_metadata" => pair_option_tokens(input_name, "-map_metadata"),
            "map_chapters" => pair_option_tokens(input_name, "-map_chapters"),
            "map" => pair_option_tokens(input_name, "-map"),
            "map_channel" => pair_option_tokens(input_name, "-map_channel"),
            "copy_ts" => bool_flag_tokens(input_name, "-copyts"),
            "start_at_zero" => bool_flag_tokens(input_name, "-start_at_zero"),
            "stats" => bool_flag_tokens(input_name, "-stats"),
            "no_overwrite" => bool_flag_tokens(input_name, "-n"),
            "codec_copy" => bool_value_pair_tokens(input_name, "-c", "copy"),
            "faststart" => bool_value_pair_tokens(input_name, "-movflags", "+faststart"),
            "hwaccel" => pair_option_tokens(input_name, "-hwaccel"),
            "sample_format" => pair_option_tokens(input_name, "-sample_fmt"),
            "channel_layout" => pair_option_tokens(input_name, "-channel_layout"),
            "metadata" => pair_option_tokens(input_name, "-metadata"),
            "timestamp" => pair_option_tokens(input_name, "-timestamp"),
            "disposition" => pair_option_tokens(input_name, "-disposition"),
            "fps_mode" => pair_option_tokens(input_name, "-fps_mode"),
            "force_key_frames" => pair_option_tokens(input_name, "-force_key_frames"),
            "aspect" => pair_option_tokens(input_name, "-aspect"),
            "stream_loop" => pair_option_tokens(input_name, "-stream_loop"),
            "max_muxing_queue_size" => pair_option_tokens(input_name, "-max_muxing_queue_size"),
            "strict" => pair_option_tokens(input_name, "-strict"),
            "maxrate" => pair_option_tokens(input_name, "-maxrate"),
            "bufsize" => pair_option_tokens(input_name, "-bufsize"),
            "bitstream_filter" => pair_option_tokens(input_name, "-bsf"),
            "id3v2_version" => pair_option_tokens(input_name, "-id3v2_version"),
            "shortest" => bool_flag_tokens(input_name, "-shortest"),
            "vn" => bool_flag_tokens(input_name, "-vn"),
            "an" => bool_flag_tokens(input_name, "-an"),
            "sn" => bool_flag_tokens(input_name, "-sn"),
            "dn" => bool_flag_tokens(input_name, "-dn"),
            "hide_banner" => bool_flag_tokens(input_name, "-hide_banner"),
            _ => pair_option_tokens(input_name, &format!("--{}", input_name.replace('_', "-"))),
        };
    }

    if tool_name.eq_ignore_ascii_case("rsgain") {
        return match input_name {
            "input_extension" | "mode" => Vec::new(),
            "album" | "album_mode" => bool_flag_tokens(input_name, "--album"),
            "album_aes77" => bool_flag_tokens(input_name, "--album-aes77"),
            "skip_existing" => bool_flag_tokens(input_name, "--skip-existing"),
            "tagmode" => pair_option_tokens(input_name, "--tagmode"),
            "target_lufs" | "loudness" => pair_option_tokens(input_name, "--loudness"),
            "clip_mode" => pair_option_tokens(input_name, "--clip-mode"),
            "true_peak" => bool_flag_tokens(input_name, "--true-peak"),
            "dual_mono" => bool_flag_tokens(input_name, "--dual-mono"),
            "max_peak" => pair_option_tokens(input_name, "--max-peak"),
            "lowercase" => bool_flag_tokens(input_name, "--lowercase"),
            "id3v2_version" => pair_option_tokens(input_name, "--id3v2-version"),
            "opus_mode" => pair_option_tokens(input_name, "--opus-mode"),
            "jobs" | "multithread" => pair_option_tokens(input_name, "--multithread"),
            "preset" => pair_option_tokens(input_name, "--preset"),
            "dry_run" => bool_flag_tokens(input_name, "--dry-run"),
            "output" => pair_option_tokens(input_name, "--output"),
            "quiet" => bool_flag_tokens(input_name, "--quiet"),
            "skip_tags" => bool_value_pair_tokens(input_name, "--tagmode", "s"),
            "preserve_mtime" | "preserve_mtimes" => {
                bool_flag_tokens(input_name, "--preserve-mtimes")
            }
            _ => pair_option_tokens(input_name, &format!("--{}", input_name.replace('_', "-"))),
        };
    }

    if tool_name.eq_ignore_ascii_case("media-tagger") {
        return match input_name {
            "acoustid_endpoint" => pair_option_tokens(input_name, "--acoustid-endpoint"),
            "musicbrainz_endpoint" => pair_option_tokens(input_name, "--musicbrainz-endpoint"),
            "cache_dir" => pair_option_tokens(input_name, "--cache-dir"),
            "cache_expiry_seconds" => pair_option_tokens(input_name, "--cache-expiry-seconds"),
            "strict_identification" => bool_flag_tokens(input_name, "--strict-identification"),
            "write_all_tags" => bool_flag_tokens(input_name, "--write-all-tags"),
            "write_all_images" => bool_flag_tokens(input_name, "--write-all-images"),
            "embed_only_one_front_image" => {
                bool_flag_tokens(input_name, "--embed-only-one-front-image")
            }
            "cover_art_slot_count" => pair_option_tokens(input_name, "--cover-art-slot-count"),
            "recording_mbid" => pair_option_tokens(input_name, "--recording-mbid"),
            "release_mbid" => pair_option_tokens(input_name, "--release-mbid"),
            _ => pair_option_tokens(input_name, &format!("--{}", input_name.replace('_', "-"))),
        };
    }

    pair_option_tokens(input_name, &format!("--{}", input_name.replace('_', "-")))
}

/// Builds `${*inputs.<name> ? <flag> | ''}` + `${*inputs.<name>}` tokens for
/// one key/value CLI option.
#[must_use]
fn pair_option_tokens(input_name: &str, flag: &str) -> Vec<String> {
    vec![unpack_if_truthy(input_name, flag), unpack_scalar(input_name)]
}

/// Builds conditional tokens that emit one flag only when the option value is
/// exactly `"true"`.
///
/// Any other value is treated as disabled for CLI flag rendering.
#[must_use]
fn bool_flag_tokens(input_name: &str, flag: &str) -> Vec<String> {
    vec![unpack_if_equals(input_name, "true", flag)]
}

/// Builds conditional tokens that emit `true_flag` only when the option value
/// is exactly `"true"`.
///
/// The `false` branch is intentionally omitted in mediapm for simpler
/// value-centric option behavior: non-`"true"` values produce no boolean CLI
/// toggle token.
#[must_use]
fn bool_switch_tokens(input_name: &str, true_flag: &str, _false_flag: &str) -> Vec<String> {
    bool_flag_tokens(input_name, true_flag)
}

/// Builds conditional tokens that emit one `flag value` pair only when the
/// option value is exactly `"true"`.
#[must_use]
fn bool_value_pair_tokens(input_name: &str, flag: &str, value: &str) -> Vec<String> {
    vec![unpack_if_equals(input_name, "true", flag), unpack_if_equals(input_name, "true", value)]
}

/// Builds ffmpeg cover-art map/disposition templates for managed media-tagger
/// apply workflows.
#[must_use]
pub(super) fn ffmpeg_cover_art_tokens(ffmpeg_slot_limits: FfmpegSlotLimits) -> Vec<String> {
    let mut tokens = Vec::new();

    for slot_index in 1..ffmpeg_slot_limits.max_input_slots {
        let enabled_input = ffmpeg_cover_slot_enabled_input_name(slot_index);
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

/// Builds one OR-joined container-equality condition string for conductor
/// template expressions.
#[must_use]
pub(super) fn ffmpeg_container_any_of_condition(containers: &[&str]) -> String {
    containers
        .iter()
        .map(|container| format!("inputs.container == \"{container}\""))
        .collect::<Vec<_>>()
        .join(" || ")
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
