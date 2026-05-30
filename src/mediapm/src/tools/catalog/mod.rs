//! Built-in downloader catalog for managed mediapm tool entries.
//!
//! Catalog entries are split into one Rust file per logical tool so source
//! details remain isolated and easy to review.

mod deno;
mod ffmpeg;
mod media_tagger;
mod rsgain;
mod sd;
mod yt_dlp;

pub(crate) use crate::config::ToolRequirementDependencies;
use crate::error::MediaPmError;

/// Supported operating-system targets for tool payload selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum ToolOs {
    /// Windows target.
    Windows,
    /// Linux target.
    Linux,
    /// macOS target.
    Macos,
}

impl ToolOs {
    /// Returns canonical lower-case label for this OS value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Windows => "windows",
            Self::Linux => "linux",
            Self::Macos => "macos",
        }
    }

    /// Returns every supported downloader OS target in deterministic order.
    #[must_use]
    pub const fn all() -> [Self; 3] {
        [Self::Windows, Self::Linux, Self::Macos]
    }
}

/// Returns current host OS used for runtime-local policy decisions.
#[must_use]
pub(crate) const fn current_tool_os() -> ToolOs {
    #[cfg(target_os = "windows")]
    {
        ToolOs::Windows
    }

    #[cfg(target_os = "macos")]
    {
        ToolOs::Macos
    }

    #[cfg(not(any(target_os = "windows", target_os = "macos")))]
    {
        ToolOs::Linux
    }
}

/// Payload handling mode used when persisting downloaded tool content.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DownloadPayloadMode {
    /// Response bytes are treated as the executable payload itself.
    DirectBinary,
    /// Response bytes are treated as ZIP archive content.
    ZipArchive,
    /// Response bytes are treated as TAR.GZ archive content.
    TarGzArchive,
    /// Response bytes are treated as TAR.XZ archive content.
    TarXzArchive,
}

/// OS-specific helper value used by declarative catalog metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PlatformValue<T>
where
    T: Copy,
{
    /// Value used on Windows.
    pub windows: T,
    /// Value used on macOS.
    pub macos: T,
    /// Value used on Linux/other Unix-like targets.
    pub linux: T,
}

impl<T> PlatformValue<T>
where
    T: Copy,
{
    /// Returns value selected for one explicit operating-system target.
    #[must_use]
    pub const fn for_os(self, os: ToolOs) -> T {
        match os {
            ToolOs::Windows => self.windows,
            ToolOs::Linux => self.linux,
            ToolOs::Macos => self.macos,
        }
    }
}

/// Declarative download strategy for one managed tool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ToolDownloadDescriptor {
    /// Static URL candidates selected by host OS.
    StaticUrls {
        /// Payload handling mode per operating-system target.
        modes: PlatformValue<DownloadPayloadMode>,
        /// Candidate URL list per OS, tried in-order.
        urls: PlatformValue<&'static [&'static str]>,
        /// Optional GitHub release repo used only for identity metadata.
        release_repo: Option<&'static str>,
    },
    /// GitHub latest release ZIP asset lookup by marker matching.
    GitHubLatestZipAsset {
        /// `<owner>/<repo>` identifier.
        repo: &'static str,
        /// Marker list used to rank ZIP assets per OS.
        markers: PlatformValue<&'static [&'static str]>,
    },
    /// Internal executable shim generated locally by `mediapm`.
    InternalLauncher,
}

/// Optional additional source downloaded alongside the main tool payload.
///
/// Use this when one managed tool must combine multiple upstream payload
/// sources in the same install root. For example, `evermeet.cx` provides
/// `ffmpeg` and `ffprobe` as separate ZIP downloads on macOS.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ToolAdditionalDownloadSource {
    /// Per-OS download URL candidates, tried in-order. Empty slice means this
    /// additional source is not required for that OS.
    pub urls: PlatformValue<&'static [&'static str]>,
    /// Payload handling mode per OS.
    pub mode: PlatformValue<DownloadPayloadMode>,
    /// Expected executable file name per OS, used to verify this source has
    /// been materialized before skipping re-provisioning.
    pub expected_executable_name: PlatformValue<&'static str>,
}

/// Catalog entry for one logical tool declared in `mediapm.ncl`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ToolCatalogEntry {
    /// Logical tool name as used by users and workflow synthesis.
    pub name: &'static str,
    /// Human-readable summary for diagnostics and generated descriptions.
    pub description: &'static str,
    /// Catalog default release-track label for docs and fallbacks.
    pub registry_track: &'static str,
    /// Human-readable source label used in lock metadata.
    pub source_label: PlatformValue<&'static str>,
    /// Stable source identifier fragment used in immutable tool ids.
    pub source_identifier: PlatformValue<&'static str>,
    /// Executable file name selected per operating-system target.
    pub executable_name: PlatformValue<&'static str>,
    /// Download strategy for this tool.
    pub download: ToolDownloadDescriptor,
    /// Additional payload sources that are merged into the same install root.
    ///
    /// Use an empty slice when the main tool download already contains all
    /// required binaries/content for every target OS.
    pub additional_download_sources: &'static [ToolAdditionalDownloadSource],
}

impl ToolCatalogEntry {
    /// Returns executable file name expected for one host OS.
    #[must_use]
    pub fn executable_name_for_os(&self, os: ToolOs) -> String {
        self.executable_name.for_os(os).to_string()
    }

    /// Returns source label selected for one host OS.
    #[must_use]
    pub fn source_label_for_os(&self, os: ToolOs) -> &'static str {
        self.source_label.for_os(os)
    }

    /// Returns source identifier selected for one host OS.
    #[must_use]
    pub fn source_identifier_for_os(&self, os: ToolOs) -> &'static str {
        self.source_identifier.for_os(os)
    }
}

/// In-memory catalog used for requirement reconciliation and downloads.
const TOOL_CATALOG: [ToolCatalogEntry; 6] =
    [ffmpeg::ENTRY, yt_dlp::ENTRY, deno::ENTRY, rsgain::ENTRY, media_tagger::ENTRY, sd::ENTRY];

/// Resolves one catalog entry for a logical tool name.
pub(crate) fn tool_catalog_entry(tool_name: &str) -> Result<ToolCatalogEntry, MediaPmError> {
    let normalized = tool_name.trim();
    TOOL_CATALOG
        .iter()
        .cloned()
        .find(|entry| entry.name.eq_ignore_ascii_case(normalized))
        .ok_or_else(|| {
            let names = TOOL_CATALOG
                .iter()
                .map(|entry| entry.name)
                .collect::<Vec<_>>()
                .join(", ");
            MediaPmError::Workflow(format!(
                "tool '{tool_name}' is not supported by mediapm downloader catalog; supported tools: {names}"
            ))
        })
}

/// Returns the default dependency selectors seeded by `mediapm tool add` for
/// one managed logical tool.
#[must_use]
pub(crate) fn default_tool_requirement_dependencies(
    tool_name: &str,
) -> ToolRequirementDependencies {
    match tool_name.trim().to_ascii_lowercase().as_str() {
        "yt-dlp" => ToolRequirementDependencies {
            ffmpeg_version: Some("inherit".to_string()),
            deno_version: Some("inherit".to_string()),
            sd_version: None,
        },
        "media-tagger" => ToolRequirementDependencies {
            ffmpeg_version: Some("inherit".to_string()),
            deno_version: None,
            sd_version: None,
        },
        "rsgain" => ToolRequirementDependencies {
            ffmpeg_version: Some("inherit".to_string()),
            deno_version: None,
            sd_version: Some("inherit".to_string()),
        },
        _ => ToolRequirementDependencies::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::{DownloadPayloadMode, ToolDownloadDescriptor, tool_catalog_entry};

    /// Verifies catalog resolves every currently managed logical tool name.
    #[test]
    fn catalog_resolves_all_current_tool_names() {
        let ffmpeg = tool_catalog_entry("ffmpeg").expect("ffmpeg entry");
        let yt_dlp = tool_catalog_entry("yt-dlp").expect("yt-dlp entry");
        let deno = tool_catalog_entry("deno").expect("deno entry");
        let rsgain = tool_catalog_entry("rsgain").expect("rsgain entry");
        let media_tagger = tool_catalog_entry("media-tagger").expect("media-tagger entry");
        let sd = tool_catalog_entry("sd").expect("sd entry");

        assert_eq!(ffmpeg.registry_track, "latest");
        assert_eq!(yt_dlp.registry_track, "latest");
        assert_eq!(deno.registry_track, "latest");
        assert_eq!(rsgain.registry_track, "latest");
        assert_eq!(media_tagger.registry_track, "latest");
        assert_eq!(sd.registry_track, "latest");
    }

    /// Verifies unknown logical tool names include supported-name diagnostics.
    #[test]
    fn catalog_rejects_unknown_tool_names() {
        let err = tool_catalog_entry("unknown-tool").expect_err("unknown should fail");
        assert!(err.to_string().contains("supported tools"));
    }

    /// Verifies ffmpeg catalog keeps static ZIP mode with metadata repo.
    #[test]
    fn ffmpeg_entry_prefers_static_zip_strategy() {
        let entry = tool_catalog_entry("ffmpeg").expect("ffmpeg entry");

        match entry.download {
            ToolDownloadDescriptor::StaticUrls { modes, release_repo, .. } => {
                assert_eq!(modes.windows, DownloadPayloadMode::ZipArchive);
                assert_eq!(modes.linux, DownloadPayloadMode::TarXzArchive);
                assert_eq!(modes.macos, DownloadPayloadMode::ZipArchive);
                assert_eq!(release_repo, Some("BtbN/FFmpeg-Builds"));
            }
            other => panic!("expected static ffmpeg strategy, got {other:?}"),
        }
    }

    /// Verifies yt-dlp static strategy keeps GitHub repo metadata for
    /// immutable id hash derivation.
    #[test]
    fn yt_dlp_entry_keeps_release_metadata_repo() {
        let entry = tool_catalog_entry("yt-dlp").expect("yt-dlp entry");

        match entry.download {
            ToolDownloadDescriptor::StaticUrls { release_repo, .. } => {
                assert_eq!(release_repo, Some("yt-dlp/yt-dlp"));
            }
            other => panic!("expected static yt-dlp strategy, got {other:?}"),
        }
    }

    /// Verifies managed executable tools keep cross-platform provisioning
    /// metadata so planner/reconciler logic can build all-platform selectors
    /// and content-map entries.
    #[test]
    fn managed_executable_catalog_entries_define_all_platform_metadata() {
        for tool_name in ["ffmpeg", "yt-dlp", "deno", "rsgain", "sd"] {
            let entry = tool_catalog_entry(tool_name).unwrap_or_else(|error| {
                panic!("missing tool catalog entry '{tool_name}': {error}")
            });

            for os in super::ToolOs::all() {
                assert!(
                    !entry.executable_name.for_os(os).trim().is_empty(),
                    "tool '{tool_name}' executable name for '{}' must be non-empty",
                    os.as_str()
                );
            }

            match entry.download {
                ToolDownloadDescriptor::StaticUrls { urls, .. } => {
                    for os in super::ToolOs::all() {
                        assert!(
                            !urls.for_os(os).is_empty(),
                            "tool '{tool_name}' static URLs for '{}' must be non-empty",
                            os.as_str()
                        );
                    }
                }
                ToolDownloadDescriptor::GitHubLatestZipAsset { markers, .. } => {
                    for os in super::ToolOs::all() {
                        assert!(
                            !markers.for_os(os).is_empty(),
                            "tool '{tool_name}' GitHub markers for '{}' must be non-empty",
                            os.as_str()
                        );
                    }
                }
                ToolDownloadDescriptor::InternalLauncher => {}
            }
        }
    }
}
