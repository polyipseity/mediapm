//! Golden hierarchy layout contract shared by demo examples and integration tests.
//!
//! Paths mirror `mediapm_demo` / `mediapm_demo_online` post-sync assertions and git
//! golden commits `962cb5fe`, `849d0051`, `28f38e64`.
//!
//! ## Online link naming (two formats)
//!
//! Sidecar links under `sidecars/links/` use the **yt-dlp output basename** (`%(title)s [%(id)s].{ext}`).
//! Root link projections use the **mediapm hierarchy rename** shape (`… [{media.id}].link.{ext}`).
//! Helpers encode both exact forms for golden JSON, e2e seeds, and live demo asserts — see
//! `.agents/instructions/demo-hierarchy-golden.instructions.md`.

use std::path::{Path, PathBuf};

use regex::Regex;
use serde::Deserialize;

/// Shared demo metadata used to resolve hierarchy folder names.
pub const DEMO_METADATA_ARTIST: &str = "Rick Astley";
pub const DEMO_METADATA_TITLE: &str = "Never Gonna Give You Up";
pub const DEMO_LIBRARY_ROOT: &str = "music videos";

/// Offline demo media id (`mediapm_demo`).
pub const OFFLINE_DEMO_MEDIA_ID: &str = "demo.local.dQw4w9WgXcQ";
/// Online demo media id (`mediapm_demo_online`).
pub const ONLINE_DEMO_MEDIA_ID: &str = "youtube.dQw4w9WgXcQ";

/// Offline demo playlist leaf under `playlists/`.
pub const OFFLINE_DEMO_PLAYLIST: &str = "local-demo.m3u8";
/// Online demo playlist leaf under `playlists/`.
pub const ONLINE_DEMO_PLAYLIST: &str = "rickroll.m3u8";

/// Returns Jellyfin-style media folder name for one media id.
#[must_use]
pub fn demo_media_folder_name(artist: &str, title: &str, media_id: &str) -> String {
    format!("{artist} - {title} [{media_id}]")
}

/// Returns offline demo media folder relative path under hierarchy root.
#[must_use]
pub fn offline_demo_media_folder_relative() -> String {
    format!(
        "{DEMO_LIBRARY_ROOT}/{}",
        demo_media_folder_name(DEMO_METADATA_ARTIST, DEMO_METADATA_TITLE, OFFLINE_DEMO_MEDIA_ID)
    )
}

/// Returns online demo media folder relative path under hierarchy root.
#[must_use]
pub fn online_demo_media_folder_relative() -> String {
    format!(
        "{DEMO_LIBRARY_ROOT}/{}",
        demo_media_folder_name(DEMO_METADATA_ARTIST, DEMO_METADATA_TITLE, ONLINE_DEMO_MEDIA_ID)
    )
}

/// Raw yt-dlp video id for the online demo Rick Astley URL.
pub const ONLINE_DEMO_YT_DLP_VIDEO_ID: &str = "dQw4w9WgXcQ";

/// Yt-dlp provider title for the online demo (`%(title)s` in the managed output template).
#[must_use]
pub fn online_demo_yt_dlp_provider_title() -> String {
    format!("{DEMO_METADATA_ARTIST} - {DEMO_METADATA_TITLE}")
}

/// Public artifact basename: `{provider_title} [{video_id}].{ext}` (no `__mediapm__`).
#[must_use]
pub fn online_demo_public_artifact_filename(
    provider_title: &str,
    video_id: &str,
    extension: &str,
) -> String {
    format!("{provider_title} [{video_id}].{extension}")
}

/// yt-dlp-format sidecar link basename under `sidecars/links/` (`{title} [{video_id}].{ext}`).
#[must_use]
pub fn online_demo_sidecar_link_filename(extension: &str) -> String {
    online_demo_public_artifact_filename(
        &online_demo_yt_dlp_provider_title(),
        ONLINE_DEMO_YT_DLP_VIDEO_ID,
        extension,
    )
}

/// mediapm root link projection basename (`…[{media.id}].link.{ext}`).
#[must_use]
pub fn online_demo_root_link_filename(extension: &str) -> String {
    format!("{} [{}].link.{}", online_demo_yt_dlp_provider_title(), ONLINE_DEMO_MEDIA_ID, extension)
}

/// Golden-relative path for one online demo sidecar link file under `sidecars/links/`.
#[must_use]
pub fn online_demo_sidecar_link_relative_path(extension: &str) -> String {
    format!(
        "{}/sidecars/links/{}",
        online_demo_media_folder_relative(),
        online_demo_sidecar_link_filename(extension)
    )
}

/// Golden-relative path for one online demo root link projection at the media folder root.
#[must_use]
pub fn online_demo_root_link_relative_path(extension: &str) -> String {
    format!("{}/{}", online_demo_media_folder_relative(), online_demo_root_link_filename(extension))
}

/// Sandbox-only yt-dlp filename with `__mediapm__` marker (conductor fixtures only).
#[must_use]
pub fn yt_dlp_sandbox_artifact_filename(
    provider_title: &str,
    video_id: &str,
    extension: &str,
) -> String {
    format!("{provider_title} [{video_id}]__mediapm__.{extension}")
}

/// One demo hierarchy golden tree specification.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct DemoHierarchyGoldenSpec {
    /// Primary media folder path relative to hierarchy root.
    pub media_folder: String,
    /// Required file paths relative to hierarchy root.
    pub required_files: Vec<String>,
    /// Directories that must exist and contain at least one regular file.
    pub required_nonempty_dirs: Vec<String>,
    /// Glob patterns (relative to hierarchy root) that must match at least one file.
    pub glob_file_patterns: Vec<String>,
}

/// Offline + online golden hierarchy specs loaded from fixture JSON.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct DemoHierarchyGoldenDocument {
    /// Offline (`mediapm_demo`) golden tree.
    pub offline: DemoHierarchyGoldenSpec,
    /// Online (`mediapm_demo_online`) golden tree.
    pub online: DemoHierarchyGoldenSpec,
}

/// Loads golden hierarchy specs from the workspace test fixture.
#[must_use]
pub fn load_demo_hierarchy_golden_document() -> DemoHierarchyGoldenDocument {
    let fixture_path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/demo_hierarchy_golden.json");
    let bytes = std::fs::read(&fixture_path)
        .unwrap_or_else(|error| panic!("read demo hierarchy golden fixture: {error}"));
    serde_json::from_slice(&bytes)
        .unwrap_or_else(|error| panic!("decode demo hierarchy golden fixture: {error}"))
}

/// Asserts that `hierarchy_root` contains every path required by `spec`.
pub fn assert_tree_under(
    hierarchy_root: &Path,
    spec: &DemoHierarchyGoldenSpec,
) -> Result<(), String> {
    for relative_path in &spec.required_files {
        let path = hierarchy_root.join(relative_path);
        if !path.is_file() {
            return Err(format!("expected required file '{}' to exist", path.display()));
        }
    }

    for relative_dir in &spec.required_nonempty_dirs {
        let dir = hierarchy_root.join(relative_dir);
        if !dir.is_dir() {
            return Err(format!("expected required directory '{}' to exist", dir.display()));
        }
        let file_count = count_regular_files_recursive(&dir)?;
        if file_count == 0 {
            return Err(format!(
                "expected directory '{}' to contain at least one file",
                dir.display()
            ));
        }
    }

    for pattern in &spec.glob_file_patterns {
        let glob_regex = glob_pattern_to_regex(pattern)?;
        let matches = collect_regular_files_recursive(hierarchy_root)?
            .into_iter()
            .filter(|path| {
                let relative = path
                    .strip_prefix(hierarchy_root)
                    .unwrap_or(path)
                    .to_string_lossy()
                    .replace('\\', "/");
                glob_regex.is_match(&relative)
            })
            .collect::<Vec<_>>();
        if matches.is_empty() {
            return Err(format!(
                "expected glob pattern '{pattern}' to match at least one file under '{}'",
                hierarchy_root.display()
            ));
        }
    }

    let media_folder = hierarchy_root.join(&spec.media_folder);
    if !media_folder.is_dir() {
        return Err(format!("expected media folder '{}' to exist", media_folder.display()));
    }

    Ok(())
}

fn count_regular_files_recursive(directory: &Path) -> Result<usize, String> {
    Ok(collect_regular_files_recursive(directory)?.len())
}

fn collect_regular_files_recursive(root: &Path) -> Result<Vec<PathBuf>, String> {
    let mut pending = vec![root.to_path_buf()];
    let mut files = Vec::new();

    while let Some(path) = pending.pop() {
        let entries = std::fs::read_dir(&path)
            .map_err(|error| format!("read directory '{}': {error}", path.display()))?;
        for entry in entries {
            let entry = entry.map_err(|error| {
                format!("read directory entry in '{}': {error}", path.display())
            })?;
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else if path.is_file() {
                files.push(path);
            }
        }
    }

    Ok(files)
}

fn glob_pattern_to_regex(pattern: &str) -> Result<Regex, String> {
    let mut regex = String::from("^");
    for ch in pattern.chars() {
        match ch {
            '*' => regex.push_str(".*"),
            '?' => regex.push('.'),
            '.' | '+' | '(' | ')' | '|' | '^' | '$' | '[' | ']' | '{' | '}' | '\\' => {
                regex.push('\\');
                regex.push(ch);
            }
            _ => regex.push(ch),
        }
    }
    regex.push('$');
    Regex::new(&regex).map_err(|error| format!("invalid glob pattern '{pattern}': {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn online_demo_sidecar_link_filenames_are_canonical() {
        assert_eq!(
            online_demo_sidecar_link_filename("url"),
            "Rick Astley - Never Gonna Give You Up [dQw4w9WgXcQ].url"
        );
        assert_eq!(
            online_demo_sidecar_link_filename("webloc"),
            "Rick Astley - Never Gonna Give You Up [dQw4w9WgXcQ].webloc"
        );
        assert_eq!(
            online_demo_sidecar_link_filename("desktop"),
            "Rick Astley - Never Gonna Give You Up [dQw4w9WgXcQ].desktop"
        );
    }

    #[test]
    fn online_demo_root_link_filenames_are_canonical() {
        assert_eq!(
            online_demo_root_link_filename("url"),
            "Rick Astley - Never Gonna Give You Up [youtube.dQw4w9WgXcQ].link.url"
        );
        assert_eq!(
            online_demo_root_link_filename("webloc"),
            "Rick Astley - Never Gonna Give You Up [youtube.dQw4w9WgXcQ].link.webloc"
        );
        assert_eq!(
            online_demo_root_link_filename("desktop"),
            "Rick Astley - Never Gonna Give You Up [youtube.dQw4w9WgXcQ].link.desktop"
        );
    }

    #[test]
    fn yt_dlp_sandbox_artifact_filename_includes_mediapm_marker() {
        assert_eq!(
            yt_dlp_sandbox_artifact_filename(
                "Rick Astley - Never Gonna Give You Up",
                ONLINE_DEMO_YT_DLP_VIDEO_ID,
                "url"
            ),
            "Rick Astley - Never Gonna Give You Up [dQw4w9WgXcQ]__mediapm__.url"
        );
    }
}
