//! Shared [`conductor_bridge`](super) string constants.
//!
//! These constants are shared between the workflow-synthesis and tool-runtime
//! modules to avoid defining the same string literal in multiple places.
//! The canonical definitions live here; all consumers import from this module.

#![allow(dead_code)]

// ── Reserved input keys ──────────────────────────────────────────────────

/// Reserved list-input name injected right after executable token.
pub(crate) const INPUT_LEADING_ARGS: &str = "leading_args";
/// Reserved list-input name injected after all generated operation arguments.
pub(crate) const INPUT_TRAILING_ARGS: &str = "trailing_args";
/// Common scalar input carrying upstream bytes for non-downloader tools.
pub(crate) const INPUT_CONTENT: &str = "input_content";
/// Optional scalar input carrying `FFmetadata` bytes for ffmpeg metadata merge.
pub(crate) const INPUT_FFMETADATA_CONTENT: &str = "ffmetadata_content";
/// Required regex pattern input for `sd` text replacement operations.
pub(crate) const INPUT_SD_PATTERN: &str = "pattern";
/// Required replacement-string input for `sd` text replacement operations.
pub(crate) const INPUT_SD_REPLACEMENT: &str = "replacement";
/// Scalar URL input used by download tools.
pub(crate) const INPUT_SOURCE_URL: &str = "source_url";

// ── Output capture names ─────────────────────────────────────────────────

/// Output capture name exposing one tool's primary file content payload.
pub(crate) const OUTPUT_CONTENT: &str = "content";
/// Output name exposing full sandbox artifact bundles.
pub(crate) const OUTPUT_SANDBOX_ARTIFACTS: &str = "sandbox_artifacts";
/// yt-dlp subtitle artifact bundle output.
pub(crate) const OUTPUT_YT_DLP_SUBTITLE_ARTIFACTS: &str = "yt_dlp_subtitle_artifacts";
/// yt-dlp thumbnail artifact bundle output.
pub(crate) const OUTPUT_YT_DLP_THUMBNAIL_ARTIFACTS: &str = "yt_dlp_thumbnail_artifacts";
/// yt-dlp description file output.
pub(crate) const OUTPUT_YT_DLP_DESCRIPTION_FILE: &str = "yt_dlp_description_file";
/// yt-dlp annotation file output.
pub(crate) const OUTPUT_YT_DLP_ANNOTATION_FILE: &str = "yt_dlp_annotation_file";
/// yt-dlp infojson file output.
pub(crate) const OUTPUT_YT_DLP_INFOJSON_FILE: &str = "yt_dlp_infojson_file";
/// yt-dlp download-archive file output.
pub(crate) const OUTPUT_YT_DLP_ARCHIVE_FILE: &str = "yt_dlp_archive_file";
/// yt-dlp internet-shortcut artifact bundle output.
pub(crate) const OUTPUT_YT_DLP_LINK_ARTIFACTS: &str = "yt_dlp_link_artifacts";
/// yt-dlp split-chapter artifact bundle output.
pub(crate) const OUTPUT_YT_DLP_CHAPTER_ARTIFACTS: &str = "yt_dlp_chapter_artifacts";
/// yt-dlp playlist-description file output.
pub(crate) const OUTPUT_YT_DLP_PLAYLIST_DESCRIPTION_FILE: &str = "yt_dlp_playlist_description_file";
/// yt-dlp playlist-infojson file output.
pub(crate) const OUTPUT_YT_DLP_PLAYLIST_INFOJSON_FILE: &str = "yt_dlp_playlist_infojson_file";

// ── Conductor logical tool IDs ───────────────────────────────────────────

/// Canonical tool id prefix for mediapm managed tools.
pub(crate) const MEDIAPM_TOOL_ID_PREFIX: &str = "mediapm.tools";

/// Suffix for tools resolved from GitHub Releases.
pub(crate) const GITHUB_RELEASES_SUFFIX: &str = "github-releases";

/// Tool id for yt-dlp media downloader.
pub(crate) const TOOL_YT_DLP: &str = "yt-dlp";
/// Tool id for ffmpeg media transform.
pub(crate) const TOOL_FFMPEG: &str = "ffmpeg";
/// Tool id for rsgain loudness scanner.
pub(crate) const TOOL_RSGAIN: &str = "rsgain";
/// Tool id for media-tagger metadata tagger.
pub(crate) const TOOL_MEDIA_TAGGER: &str = "media-tagger";
/// Tool id for sd in-place text replacement.
pub(crate) const TOOL_SD: &str = "sd";

// ── Slot limits ──────────────────────────────────────────────────────────

/// Default maximum number of indexed ffmpeg content input slots.
pub(crate) const DEFAULT_FFMPEG_MAX_INPUT_SLOTS: usize = 16;
/// Default maximum number of indexed ffmpeg output slots.
pub(crate) const DEFAULT_FFMPEG_MAX_OUTPUT_SLOTS: usize = 4;

// ── Config key names used in NCL templates ───────────────────────────────

/// Config key for the generated conductor document tools section.
pub(crate) const CONFIG_TOOLS_KEY: &str = "tools";
