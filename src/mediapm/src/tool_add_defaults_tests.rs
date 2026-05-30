//! Regression tests for managed tool-add dependency defaults.

use crate::MediaPmService;
use crate::config::load_mediapm_document_without_validation;
use tempfile::tempdir;

/// Ensures `tool add` seeds each managed tool with its catalog default
/// dependency selectors instead of hardcoding one-off special cases.
#[test]
fn add_tool_requirement_uses_catalog_default_dependency_selectors() {
    let cases = [
        ("ffmpeg", None, None, None),
        ("deno", None, None, None),
        ("sd", None, None, None),
        ("yt-dlp", Some("inherit"), Some("inherit"), None),
        ("media-tagger", Some("inherit"), None, None),
        ("rsgain", Some("inherit"), None, Some("inherit")),
    ];

    for (tool_name, ffmpeg_version, deno_version, sd_version) in cases {
        let root = tempdir().expect("tempdir");
        let service = MediaPmService::new_in_memory_at(root.path());

        let added = service.add_tool_requirement(tool_name).expect("add managed tool requirement");
        assert!(added, "{tool_name} should be added on a fresh workspace");

        let document = load_mediapm_document_without_validation(&service.paths().mediapm_ncl)
            .expect("load seeded mediapm.ncl without validation");
        let dependencies = &document.tools[tool_name].dependencies;

        assert_eq!(dependencies.ffmpeg_version.as_deref(), ffmpeg_version, "{tool_name}");
        assert_eq!(dependencies.deno_version.as_deref(), deno_version, "{tool_name}");
        assert_eq!(dependencies.sd_version.as_deref(), sd_version, "{tool_name}");
    }
}
