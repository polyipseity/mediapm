//! Media source and step-level validation.

use std::str::FromStr;

use mediapm_cas::Hash;
use url::Url;

use crate::error::MediaPmError;

use super::super::{
    DecodedOutputVariantConfig, MediaSourceSpec, MediaStep, MediaStepTool, TransformInputValue,
    decode_output_variant_config, expand_variant_selectors, has_step_option_scalar,
    resolve_step_variant_flow, step_option_scalar,
};

use super::hierarchy::validate_media_metadata_entries;

/// Validates one media source entry.
#[expect(
    clippy::too_many_lines,
    reason = "this item intentionally keeps end-to-end control flow together so ordering invariants remain explicit during maintenance"
)]
pub(super) fn validate_media_source(
    media_id: &str,
    source: &MediaSourceSpec,
) -> Result<(), MediaPmError> {
    if source.steps.is_empty() && source.variant_hashes.is_empty() {
        return Err(MediaPmError::Workflow(format!(
            "media '{media_id}' must define at least one step or at least one variant_hashes entry"
        )));
    }

    if let Some(workflow_id) = source.workflow_id.as_deref()
        && workflow_id.trim().is_empty()
    {
        return Err(MediaPmError::Workflow(format!(
            "media '{media_id}' defines an empty workflow_id override"
        )));
    }

    if source.id.is_some() {
        return Err(MediaPmError::Workflow(format!(
            "media '{media_id}' must not define id; playlist references now resolve through hierarchy node ids"
        )));
    }

    let mut available_variants = source
        .variant_hashes
        .keys()
        .map(ToString::to_string)
        .collect::<std::collections::BTreeSet<_>>();

    for (variant, hash) in &source.variant_hashes {
        if variant.trim().is_empty() {
            return Err(MediaPmError::Workflow(format!(
                "media '{media_id}' has an empty variant name in variant_hashes"
            )));
        }
        if hash.trim().is_empty() {
            return Err(MediaPmError::Workflow(format!(
                "media '{media_id}' variant '{variant}' has an empty CAS hash pointer"
            )));
        }
    }

    for (index, step) in source.steps.iter().enumerate() {
        let mut resolved_step = step.clone();
        if !step.tool.is_source_ingest_tool() {
            resolved_step.input_variants =
                expand_variant_selectors(&step.input_variants, &available_variants).map_err(
                    |reason| {
                        MediaPmError::Workflow(format!("media '{media_id}' step #{index} {reason}"))
                    },
                )?;
        }

        let flow = resolve_step_variant_flow(&resolved_step).map_err(|reason| {
            MediaPmError::Workflow(format!("media '{media_id}' step #{index} {reason}"))
        })?;

        validate_step_output_variant_configs(media_id, index, &resolved_step)?;

        for key in resolved_step.options.keys() {
            if !is_allowed_step_option(resolved_step.tool, key) {
                return Err(MediaPmError::Workflow(format!(
                    "media '{media_id}' step #{index} uses unsupported option '{key}' for tool '{}'",
                    resolved_step.tool.as_str()
                )));
            }
        }

        if resolved_step.tool.is_online_media_downloader() {
            let uri = step_option_scalar(&resolved_step, "uri").ok_or_else(|| {
                MediaPmError::Workflow(format!(
                    "media '{media_id}' step #{index} uses tool '{}' and must define options.uri",
                    resolved_step.tool.as_str()
                ))
            })?;

            let uri = Url::parse(uri).map_err(|err| {
                MediaPmError::Workflow(format!(
                    "media '{media_id}' step #{index} has invalid options.uri '{uri}': {err}"
                ))
            })?;
            if !matches!(uri.scheme(), "http" | "https") {
                return Err(MediaPmError::Workflow(format!(
                    "media '{media_id}' step #{index} options.uri must use http/https, observed '{}'",
                    uri.scheme()
                )));
            }
        } else if matches!(resolved_step.tool, MediaStepTool::Import) {
            let kind = step_option_scalar(&resolved_step, "kind").ok_or_else(|| {
                MediaPmError::Workflow(format!(
                    "media '{media_id}' step #{index} uses tool '{}' and must define options.kind",
                    resolved_step.tool.as_str()
                ))
            })?;

            if kind != "cas_hash" {
                return Err(MediaPmError::Workflow(format!(
                    "media '{media_id}' step #{index} options.kind must be 'cas_hash' for tool '{}', observed '{kind}'",
                    resolved_step.tool.as_str()
                )));
            }

            let hash_text = step_option_scalar(&resolved_step, "hash").ok_or_else(|| {
                MediaPmError::Workflow(format!(
                    "media '{media_id}' step #{index} uses tool '{}' and must define options.hash",
                    resolved_step.tool.as_str()
                ))
            })?;
            Hash::from_str(hash_text).map_err(|_| {
                MediaPmError::Workflow(format!(
                    "media '{media_id}' step #{index} has invalid options.hash '{hash_text}'"
                ))
            })?;
        } else if has_step_option_scalar(&resolved_step, "uri") {
            return Err(MediaPmError::Workflow(format!(
                "media '{media_id}' step #{index} uses tool '{}' and must not define options.uri",
                resolved_step.tool.as_str()
            )));
        }

        if matches!(resolved_step.tool, MediaStepTool::Ffmpeg) {
            for input_variant in &resolved_step.input_variants {
                if !available_variants.contains(input_variant.trim()) {
                    return Err(MediaPmError::Workflow(format!(
                        "media '{media_id}' step #{index} references unknown input variant '{input_variant}'"
                    )));
                }
            }
        }

        for mapping in &flow {
            if !resolved_step.tool.is_source_ingest_tool()
                && !available_variants.contains(&mapping.input)
            {
                return Err(MediaPmError::Workflow(format!(
                    "media '{media_id}' step #{index} references unknown input variant '{}'",
                    mapping.input
                )));
            }

            available_variants.insert(mapping.output.clone());
        }

        for (key, value) in &resolved_step.options {
            if key.trim().is_empty() {
                return Err(MediaPmError::Workflow(format!(
                    "media '{media_id}' step #{index} has an empty options key"
                )));
            }

            let TransformInputValue::String(text) = value;
            let _ = text;
        }
    }

    validate_media_metadata_entries(media_id, source)?;

    Ok(())
}

/// Validates tool-specific output-variant configuration object schemas.
fn validate_step_output_variant_configs(
    media_id: &str,
    step_index: usize,
    step: &MediaStep,
) -> Result<(), MediaPmError> {
    for (key, value) in &step.output_variants {
        let normalized_key = key.trim();
        let decoded =
            decode_output_variant_config(step.tool, normalized_key, value).map_err(|reason| {
                MediaPmError::Workflow(format!("media '{media_id}' step #{step_index} {reason}"))
            })?;

        if matches!(step.tool, MediaStepTool::Ffmpeg)
            && matches!(decoded, DecodedOutputVariantConfig::Generic(ref config) if config.kind != "primary")
        {
            return Err(MediaPmError::Workflow(format!(
                "media '{media_id}' step #{step_index} ffmpeg output variant '{normalized_key}' must use kind 'primary'"
            )));
        }
    }

    Ok(())
}

/// Returns whether one step option key is supported for the given tool.
#[must_use]
#[expect(
    clippy::too_many_lines,
    reason = "this item intentionally keeps end-to-end control flow together so ordering invariants remain explicit during maintenance"
)]
fn is_allowed_step_option(tool: MediaStepTool, key: &str) -> bool {
    match tool {
        MediaStepTool::YtDlp => matches!(
            key,
            "uri"
                | "leading_args"
                | "trailing_args"
                | "option_args"
                | "format"
                | "format_sort"
                | "extract_audio"
                | "audio_format"
                | "audio_quality"
                | "remux_video"
                | "recode_video"
                | "convert_subs"
                | "convert_thumbnails"
                | "merge_output_format"
                | "embed_thumbnail"
                | "embed_metadata"
                | "embed_subs"
                | "embed_chapters"
                | "embed_info_json"
                | "write_subs"
                | "sub_langs"
                | "sub_format"
                | "write_thumbnail"
                | "write_all_thumbnails"
                | "write_info_json"
                | "clean_info_json"
                | "write_comments"
                | "write_description"
                | "write_annotations"
                | "write_link"
                | "write_url_link"
                | "write_webloc_link"
                | "write_desktop_link"
                | "write_chapters"
                | "split_chapters"
                | "playlist_items"
                | "no_playlist"
                | "skip_download"
                | "retries"
                | "limit_rate"
                | "concurrent_fragments"
                | "proxy"
                | "socket_timeout"
                | "sleep_subtitles"
                | "user_agent"
                | "referer"
                | "add_header"
                | "cookies"
                | "cookies_from_browser"
                | "cache_dir"
                | "js_runtimes"
                | "ffmpeg_location"
                | "paths"
                | "output"
                | "parse_metadata"
                | "replace_in_metadata"
                | "download_sections"
                | "postprocessor_args"
                | "extractor_args"
                | "http_chunk_size"
                | "download_archive"
                | "sponsorblock_mark"
                | "sponsorblock_remove"
        ),
        MediaStepTool::Import => matches!(key, "kind" | "hash"),
        MediaStepTool::Ffmpeg => matches!(
            key,
            "leading_args"
                | "trailing_args"
                // common options
                | "option_args"
                | "audio_codec"
                | "video_codec"
                | "container"
                | "audio_bitrate"
                | "video_bitrate"
                | "audio_quality"
                | "video_quality"
                | "crf"
                | "preset"
                | "threads"
                | "log_level"
                | "progress"
                // less-common but useful options
                | "tune"
                | "profile"
                | "level"
                | "pixel_format"
                | "frame_rate"
                | "sample_rate"
                | "channels"
                | "audio_filters"
                | "video_filters"
                | "filter_complex"
                | "start_time"
                | "duration"
                | "to"
                | "movflags"
                | "map_metadata"
                | "map_chapters"
                | "map"
                | "map_channel"
                | "copy_ts"
                | "start_at_zero"
                | "stats"
                | "no_overwrite"
                | "codec_copy"
                | "faststart"
                | "hwaccel"
                | "sample_format"
                | "channel_layout"
                | "metadata"
                | "timestamp"
                | "disposition"
                | "fps_mode"
                | "force_key_frames"
                | "aspect"
                | "stream_loop"
                | "max_muxing_queue_size"
                | "strict"
                | "maxrate"
                | "bufsize"
                | "bitstream_filter"
                | "shortest"
                | "vn"
                | "an"
                | "sn"
                | "dn"
                | "id3v2_version"
        ),
        MediaStepTool::Rsgain => matches!(
            key,
            "leading_args"
                | "trailing_args"
                | "option_args"
                | "mode"
                | "album"
                | "album_aes77"
                | "skip_existing"
                | "tagmode"
                | "loudness"
                | "target_lufs"
                | "clip_mode"
                | "true_peak"
                | "dual_mono"
                | "album_mode"
                | "max_peak"
                | "lowercase"
                | "id3v2_version"
                | "opus_mode"
                | "jobs"
                | "multithread"
                | "preset"
                | "dry_run"
                | "output"
                | "quiet"
                | "skip_tags"
                | "preserve_mtime"
                | "preserve_mtimes"
                | "input_extension"
        ),
        MediaStepTool::MediaTagger => matches!(
            key,
            "leading_args"
                | "trailing_args"
                | "option_args"
                | "acoustid_endpoint"
                | "musicbrainz_endpoint"
                | "cache_dir"
                | "cache_expiry_seconds"
                | "strict_identification"
                | "write_all_tags"
                | "write_all_images"
                | "embed_only_one_front_image"
                | "cover_art_slot_count"
                | "recording_mbid"
                | "release_mbid"
                | "output_container"
        ),
    }
}
