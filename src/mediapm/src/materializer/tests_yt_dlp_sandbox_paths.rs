//! Path and link-file naming from a yt-dlp sandbox tree, and what stale pruning spares.

use super::*;

#[test]
fn normalize_yt_dlp_sandbox_zip_member_path_strips_downloads_and_mediapm_marker() {
    let path = Path::new("downloads/Rick Astley [dQw4w9WgXcQ]__mediapm__.url");
    assert_eq!(
        normalize_yt_dlp_sandbox_zip_member_path(path),
        PathBuf::from("Rick Astley [dQw4w9WgXcQ].url")
    );
}

#[test]
fn normalize_yt_dlp_sandbox_zip_member_path_preserves_paths_without_marker() {
    let path = Path::new("downloads/subtitles/foo.en.vtt");
    assert_eq!(
        normalize_yt_dlp_sandbox_zip_member_path(path),
        PathBuf::from("subtitles/foo.en.vtt")
    );
}

#[test]
fn normalize_yt_dlp_sandbox_zip_member_path_strips_marker_from_nested_components() {
    let path = Path::new("downloads/links/Rick Astley [dQw4w9WgXcQ]__mediapm__.desktop");
    assert_eq!(
        normalize_yt_dlp_sandbox_zip_member_path(path),
        PathBuf::from("links/Rick Astley [dQw4w9WgXcQ].desktop")
    );
}

#[test]
fn regression_desktop_link_name_strips_downloads_prefix() {
    let input = "[Desktop Entry]\nName=downloads/Rick Astley - Never Gonna Give You Up\nURL=https://example.com";
    let expected =
        "[Desktop Entry]\nName=Rick Astley - Never Gonna Give You Up\nURL=https://example.com";
    assert_eq!(rewrite_desktop_link_content(input), expected);
}

#[test]
fn regression_desktop_link_name_strips_mediapm_marker() {
    let input =
        "[Desktop Entry]\nName=Rick Astley [dQw4w9WgXcQ]__mediapm__\nURL=https://example.com";
    let expected = "[Desktop Entry]\nName=Rick Astley [dQw4w9WgXcQ]\nURL=https://example.com";
    assert_eq!(rewrite_desktop_link_content(input), expected);
}

#[test]
fn regression_desktop_link_name_unescapes_spaces() {
    let input = "Name=downloads/Rick\\sAstley\\s[dQw4w9WgXcQ]__mediapm__";
    let expected = "Name=Rick Astley [dQw4w9WgXcQ]";
    assert_eq!(rewrite_desktop_link_content(input), expected);
}

#[test]
fn regression_desktop_link_name_full_ytdlp_template() {
    let input = "[Desktop Entry]\nName=downloads/Rick\\sAstley\\s-\\sNever\\sGonna\\sGive\\sYou\\sUp\\s[dQw4w9WgXcQ]__mediapm__\nURL=https://example.com";
    let expected = "[Desktop Entry]\nName=Rick Astley - Never Gonna Give You Up [dQw4w9WgXcQ]\nURL=https://example.com";
    assert_eq!(rewrite_desktop_link_content(input), expected);
}

#[test]
fn regression_desktop_link_name_preserves_non_name_lines() {
    let input =
        "[Desktop Entry]\nType=Link\nName=downloads/Test__mediapm__\nURL=https://example.com";
    let expected = "[Desktop Entry]\nType=Link\nName=Test\nURL=https://example.com";
    assert_eq!(rewrite_desktop_link_content(input), expected);
}

#[test]
fn is_protected_hierarchy_path_accepts_managed_descendants() {
    let current_paths = BTreeSet::from(["music videos/demo [id]/sidecars/links".to_string()]);
    let managed_paths =
        BTreeSet::from(
            ["music videos/demo [id]/sidecars/links/Rick [dQw4w9WgXcQ].url".to_string()],
        );
    assert!(is_protected_hierarchy_path(
        "music videos/demo [id]/sidecars/links/Rick [dQw4w9WgXcQ].url",
        &current_paths,
        &managed_paths,
    ));
    assert!(is_protected_hierarchy_path(
        "music videos/demo [id]/Rick [id].link.url",
        &BTreeSet::from(["music videos/demo [id]".to_string()]),
        &BTreeSet::new(),
    ));
    assert!(!is_protected_hierarchy_path(
        "music videos/other [id]/stale.txt",
        &current_paths,
        &managed_paths,
    ));
}
