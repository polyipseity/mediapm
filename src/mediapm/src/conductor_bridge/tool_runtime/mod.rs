//! Managed-tool runtime contract builders.
//!
//! This module produces [`ToolSpec`] and [`ToolRuntime`] entries for each
//! mediapm managed tool (yt-dlp, ffmpeg, rsgain, media-tagger, sd). It does
//! NOT produce workflow steps — step synthesis lives upstream in the
//! materializer/hierarchy modules.
//!
//! Sub-modules:
//! - [`option_constants`] — ordered option name definitions used for CLI token generation
//! - [`option_tokens`] — token-spec mappings and cover-art / container-any-of conditions
//! - [`template`] — command-template validation, environment-variable substitution, sandbox-path normalization
//! - [`launcher`] — media-tagger launcher binary path resolution

pub(crate) mod launcher;
pub(crate) mod option_constants;
pub(crate) mod option_tokens;
pub(crate) mod template;

use std::collections::BTreeMap;

use mediapm_conductor::{
    OutputCaptureSpec, ToolInputKind, ToolInputSpec, ToolKindSpec, ToolRuntime, ToolSpec,
};

use crate::conductor_bridge::constants::{
    DEFAULT_FFMPEG_MAX_INPUT_SLOTS, DEFAULT_FFMPEG_MAX_OUTPUT_SLOTS, INPUT_CONTENT,
    INPUT_FFMETADATA_CONTENT, INPUT_LEADING_ARGS, INPUT_SOURCE_URL, INPUT_TRAILING_ARGS,
    OUTPUT_CONTENT, OUTPUT_SANDBOX_ARTIFACTS,
};

/// ffmpeg slot-limit configuration derived from tool requirements.
#[derive(Debug, Clone, Copy)]
pub(crate) struct FfmpegSlotLimits {
    /// Maximum number of ffmpeg input content / cover-art slots.
    #[allow(dead_code)]
    pub(crate) max_input_slots: u32,
    /// Maximum number of ffmpeg indexed output-file slots.
    #[allow(dead_code)]
    pub(crate) max_output_slots: u32,
}

/// Resolves ffmpeg slot limits from config default or overrides.
#[must_use]
#[allow(clippy::cast_possible_truncation)]
pub(crate) fn resolve_ffmpeg_slot_limits(
    max_input: Option<u32>,
    max_output: Option<u32>,
) -> FfmpegSlotLimits {
    FfmpegSlotLimits {
        max_input_slots: max_input.unwrap_or(DEFAULT_FFMPEG_MAX_INPUT_SLOTS as u32),
        max_output_slots: max_output.unwrap_or(DEFAULT_FFMPEG_MAX_OUTPUT_SLOTS as u32),
    }
}

/// Builds a full [`ToolSpec`] and [`ToolRuntime`] for one managed tool.
///
/// `content_map` maps sandbox-relative paths to CAS hash hex strings
/// (output of the fetch + CAS-import step in the sync pipeline).
/// `command_path` is the sandbox-relative path to the main executable.
#[allow(clippy::too_many_lines)]
pub(crate) fn build_tool_spec(
    tool_name: &str,
    content_map: BTreeMap<String, String>,
    command_path: &str,
    ffmpeg_slot_limits: FfmpegSlotLimits,
) -> (ToolSpec, ToolRuntime) {
    let inputs = build_tool_inputs(tool_name, ffmpeg_slot_limits);
    let outputs = build_tool_outputs(tool_name, ffmpeg_slot_limits);
    let default_inputs = build_default_input_defaults(tool_name, ffmpeg_slot_limits);

    let runtime = ToolRuntime {
        content_map,
        impure: false,
        inherited_env_vars: Vec::new(),
        max_concurrent_calls: default_max_concurrent_calls(tool_name),
        max_retries: default_max_retries(tool_name),
    };

    let spec = ToolSpec {
        kind: ToolKindSpec::Executable {
            command: vec![command_path.to_string()],
            env_vars: BTreeMap::new(),
            success_codes: vec![0],
        },
        name: tool_name.to_string(),
        version: String::new(),
        inputs,
        default_inputs,
        outputs,
        runtime: runtime.clone(),
    };

    (spec, runtime)
}

// ── Default policy helpers ───────────────────────────────────────────────

#[must_use]
fn default_max_concurrent_calls(tool_name: &str) -> usize {
    usize::from(tool_name.eq_ignore_ascii_case("yt-dlp"))
}

#[must_use]
fn default_max_retries(tool_name: &str) -> usize {
    usize::from(tool_name.eq_ignore_ascii_case("yt-dlp"))
}

// ── Input / output spec builders ─────────────────────────────────────────

fn build_tool_inputs(
    tool_name: &str,
    _ffmpeg_slot_limits: FfmpegSlotLimits,
) -> BTreeMap<String, ToolInputSpec> {
    // Stub: returns minimal inputs. Full implementation should mirror
    // src_old/conductor_bridge/tool_runtime/mod.rs `build_tool_inputs`.
    let mut inputs = BTreeMap::new();
    inputs.insert(
        INPUT_CONTENT.to_string(),
        ToolInputSpec { kind: ToolInputKind::String, description: String::new(), required: false },
    );
    inputs.insert(
        INPUT_SOURCE_URL.to_string(),
        ToolInputSpec { kind: ToolInputKind::String, description: String::new(), required: false },
    );
    inputs.insert(
        INPUT_LEADING_ARGS.to_string(),
        ToolInputSpec {
            kind: ToolInputKind::StringList,
            description: String::new(),
            required: false,
        },
    );
    inputs.insert(
        INPUT_TRAILING_ARGS.to_string(),
        ToolInputSpec {
            kind: ToolInputKind::StringList,
            description: String::new(),
            required: false,
        },
    );

    if tool_name.eq_ignore_ascii_case("ffmpeg") {
        inputs.insert(
            INPUT_FFMETADATA_CONTENT.to_string(),
            ToolInputSpec {
                kind: ToolInputKind::String,
                description: String::new(),
                required: false,
            },
        );
    }

    inputs
}

fn build_tool_outputs(
    tool_name: &str,
    _ffmpeg_slot_limits: FfmpegSlotLimits,
) -> BTreeMap<String, OutputCaptureSpec> {
    let mut outputs = BTreeMap::new();

    let main_capture = if tool_name.eq_ignore_ascii_case("yt-dlp") {
        format!("file_regex:{YT_DLP_OUTPUT_CONTENT_REGEX}")
    } else {
        format!("file:{SANDBOX_INPUT_FILE}")
    };

    outputs.insert(
        OUTPUT_CONTENT.to_string(),
        OutputCaptureSpec { name: OUTPUT_CONTENT.to_string(), capture: main_capture, save: true },
    );
    outputs.insert(
        OUTPUT_SANDBOX_ARTIFACTS.to_string(),
        OutputCaptureSpec {
            name: OUTPUT_SANDBOX_ARTIFACTS.to_string(),
            capture: format!("folder:{SANDBOX_INPUTS_DIR}"),
            save: true,
        },
    );

    outputs
}

fn build_default_input_defaults(
    tool_name: &str,
    _ffmpeg_slot_limits: FfmpegSlotLimits,
) -> BTreeMap<String, String> {
    let mut defaults = BTreeMap::new();

    // Apply static defaults per tool.
    let static_defaults: &[(&str, &str)] = match tool_name {
        n if n.eq_ignore_ascii_case("yt-dlp") => YT_DLP_INPUT_DEFAULTS,
        n if n.eq_ignore_ascii_case("rsgain") => RSGAIN_INPUT_DEFAULTS,
        n if n.eq_ignore_ascii_case("media-tagger") => MEDIA_TAGGER_INPUT_DEFAULTS,
        _ => &[],
    };

    for (key, value) in static_defaults {
        defaults.insert(key.to_string(), value.to_string());
    }

    defaults
}

// ── Static defaults tables ───────────────────────────────────────────────

const YT_DLP_OUTPUT_CONTENT_REGEX: &str = r"^downloads/.+__mediapm__\.\w+$";

const SANDBOX_INPUT_FILE: &str = "inputs/sandbox_input";
const SANDBOX_INPUTS_DIR: &str = "inputs";

const YT_DLP_INPUT_DEFAULTS: &[(&str, &str)] = &[
    ("paths", "downloads"),
    ("output", "%(title)s [%(id)s]%(playlist_index|)s__mediapm__.%(ext)s"),
    ("format", "bestvideo*+bestaudio/best"),
    ("sub_langs", "all"),
    ("merge_output_format", "mkv"),
    ("embed_metadata", "true"),
    ("embed_chapters", "true"),
    ("write_subs", "true"),
    ("write_thumbnail", "true"),
    ("write_info_json", "true"),
    ("clean_info_json", "true"),
    ("write_comments", "false"),
    ("embed_info_json", "true"),
    ("extractor_args", "youtube:skip=translated_subs"),
    ("cache_dir", ""),
    ("ffmpeg_location", "ffmpeg"),
];

const RSGAIN_INPUT_DEFAULTS: &[(&str, &str)] = &[
    ("input_extension", "flac"),
    ("album", "false"),
    ("album_mode", "false"),
    ("target_lufs", "-18"),
    ("tagmode", "i"),
    ("clip_mode", "p"),
    ("true_peak", "true"),
    ("preserve_mtimes", "true"),
];

const MEDIA_TAGGER_INPUT_DEFAULTS: &[(&str, &str)] = &[
    ("strict_identification", "true"),
    ("write_all_tags", "true"),
    ("write_all_images", "true"),
    ("save_images_to_tags", "true"),
    ("embed_only_one_front_image", "false"),
    ("ca_providers", "caa_release,url_relationships,caa_release_group"),
    ("caa_image_types", "all,-matrix/runout,-raw/unedited,-watermark"),
    ("caa_image_size", "full"),
    ("caa_approved_only", "false"),
    ("enable_tag_saving", "true"),
    ("release_ars", "true"),
    ("cover_art_slot_count", "16"),
    ("cache_dir", ""),
    ("cache_expiry_seconds", "86400"),
];

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn build_tool_spec_returns_executable_kind() {
        let mut content_map = BTreeMap::new();
        content_map.insert("linux/sd".into(), "hash123".into());
        content_map.insert("macos/sd".into(), "hash456".into());
        content_map.insert("windows/sd.exe".into(), "hash789".into());

        let limits = FfmpegSlotLimits { max_input_slots: 2, max_output_slots: 2 };
        let (spec, runtime) = build_tool_spec("sd", content_map.clone(), "sd", limits);

        let ToolKindSpec::Executable { command, .. } = &spec.kind else {
            panic!("expected Executable kind");
        };
        assert_eq!(command, &vec!["sd".to_string()]);
        assert_eq!(runtime.content_map, content_map);
        assert!(!runtime.impure);
        assert_eq!(spec.name, "sd");
    }

    #[test]
    fn build_tool_spec_preserves_content_map() {
        let mut content_map = BTreeMap::new();
        content_map.insert("linux/sd".into(), "abc".into());
        content_map.insert("macos/sd".into(), "def".into());
        content_map.insert("windows/sd.exe".into(), "ghi".into());

        let limits = FfmpegSlotLimits { max_input_slots: 2, max_output_slots: 2 };
        let (_spec, runtime) = build_tool_spec("sd", content_map.clone(), "sd", limits);

        assert_eq!(runtime.content_map.len(), 3);
        assert_eq!(runtime.content_map["linux/sd"], "abc");
        assert_eq!(runtime.content_map["macos/sd"], "def");
        assert_eq!(runtime.content_map["windows/sd.exe"], "ghi");
    }

    #[test]
    fn build_tool_spec_sets_runtime_defaults() {
        let content_map = BTreeMap::new();
        let limits = FfmpegSlotLimits { max_input_slots: 2, max_output_slots: 2 };
        let (_spec, runtime) = build_tool_spec("ffmpeg", content_map, "ffmpeg", limits);

        assert_eq!(runtime.max_concurrent_calls, 0);
        assert_eq!(runtime.max_retries, 0);
        assert!(runtime.inherited_env_vars.is_empty());
    }

    #[test]
    fn yt_dlp_increases_concurrency_and_retries() {
        let content_map = BTreeMap::new();
        let limits = FfmpegSlotLimits { max_input_slots: 2, max_output_slots: 2 };
        let (_spec, runtime) = build_tool_spec("yt-dlp", content_map, "yt-dlp", limits);

        assert_eq!(runtime.max_concurrent_calls, 1);
        assert_eq!(runtime.max_retries, 1);
    }
}
