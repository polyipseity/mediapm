//! Collecting same-step dependencies, inlining their payloads, stripping the inlined keys.

use crate::config::ToolRequirement;
use crate::tools::dependency::{DependencyTypes, known_dependency_type};
use std::collections::BTreeMap;

use super::*;

#[test]
fn collect_same_step_dep_ids_empty_deps() {
    let req = ToolRequirement::default();
    let ids = collect_same_step_dep_ids("ffmpeg", &req, known_dependency_type);
    assert!(ids.is_empty());
}

#[test]
fn collect_same_step_dep_ids_yt_dlp_ffmpeg() {
    let deps = BTreeMap::from([("ffmpeg".to_string(), ConfigVersionSpec::Latest)]);
    let req = ToolRequirement {
        version_spec: ConfigVersionSpec::Latest,
        dependencies: deps,
        ..Default::default()
    };
    let ids = collect_same_step_dep_ids("yt-dlp", &req, known_dependency_type);
    assert_eq!(ids, vec!["ffmpeg"]);
}

#[test]
fn collect_same_step_dep_ids_rsgain_ffmpeg() {
    let deps = BTreeMap::from([("ffmpeg".to_string(), ConfigVersionSpec::Latest)]);
    let req = ToolRequirement {
        version_spec: ConfigVersionSpec::Latest,
        dependencies: deps,
        ..Default::default()
    };
    // rsgain has CrossStep dep on ffmpeg → NOT in same-step list
    let ids = collect_same_step_dep_ids("rsgain", &req, known_dependency_type);
    assert!(ids.is_empty());
}

#[allow(clippy::unnecessary_wraps)] // must match the `fn(&str, &str) -> Option<DependencyTypes>` parameter
fn both_roles_dep_type(_tool_id: &str, _dep_id: &str) -> Option<DependencyTypes> {
    Some(DependencyTypes::SAME_STEP.combine(DependencyTypes::CROSS_STEP))
}

#[test]
fn collect_same_step_dep_ids_combined_roles() {
    // A dependency carrying both roles contributes its same-step role
    // (replaces the removed `DependencyType::Both` semantics).
    let deps = BTreeMap::from([("ffmpeg".to_string(), ConfigVersionSpec::Latest)]);
    let req = ToolRequirement {
        version_spec: ConfigVersionSpec::Latest,
        dependencies: deps,
        ..Default::default()
    };
    let ids = collect_same_step_dep_ids("tool", &req, both_roles_dep_type);
    assert_eq!(ids, vec!["ffmpeg"]);
}

#[test]
fn inline_same_step_deps_empty_deps() {
    let req = ToolRequirement::default();
    let maps = BTreeMap::new();
    let result = inline_same_step_deps("yt-dlp", &req, &maps, known_dependency_type);
    assert!(result.is_empty());
}

#[test]
fn inline_same_step_deps_yt_dlp_ffmpeg_deno() {
    let deps = BTreeMap::from([
        ("ffmpeg".to_string(), ConfigVersionSpec::Latest),
        ("deno".to_string(), ConfigVersionSpec::Latest),
    ]);
    let req = ToolRequirement {
        version_spec: ConfigVersionSpec::Latest,
        dependencies: deps,
        ..Default::default()
    };
    let mut maps: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    maps.insert(
        "ffmpeg".to_string(),
        BTreeMap::from([
            ("linux/ffmpeg".to_string(), "blake3:a".to_string()),
            ("macos/ffmpeg".to_string(), "blake3:b".to_string()),
        ]),
    );
    maps.insert(
        "deno".to_string(),
        BTreeMap::from([("linux/deno".to_string(), "blake3:c".to_string())]),
    );
    let result = inline_same_step_deps("yt-dlp", &req, &maps, known_dependency_type);
    assert_eq!(
        result,
        BTreeMap::from([
            ("deps/ffmpeg/linux/ffmpeg".to_string(), "blake3:a".to_string()),
            ("deps/ffmpeg/macos/ffmpeg".to_string(), "blake3:b".to_string()),
            ("deps/deno/linux/deno".to_string(), "blake3:c".to_string()),
        ]),
    );
}

#[test]
fn inline_same_step_deps_cross_step_excluded() {
    // rsgain's ffmpeg dep is CrossStep → never inlined.
    let deps = BTreeMap::from([("ffmpeg".to_string(), ConfigVersionSpec::Latest)]);
    let req = ToolRequirement {
        version_spec: ConfigVersionSpec::Latest,
        dependencies: deps,
        ..Default::default()
    };
    let mut maps: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    maps.insert(
        "ffmpeg".to_string(),
        BTreeMap::from([("linux/ffmpeg".to_string(), "blake3:a".to_string())]),
    );
    let result = inline_same_step_deps("rsgain", &req, &maps, known_dependency_type);
    assert!(result.is_empty());
}

#[test]
fn inline_same_step_deps_dep_absent_skipped() {
    // Dep listed but not provisioned this pass (skipped/failed) → nothing.
    let deps = BTreeMap::from([("ffmpeg".to_string(), ConfigVersionSpec::Latest)]);
    let req = ToolRequirement {
        version_spec: ConfigVersionSpec::Latest,
        dependencies: deps,
        ..Default::default()
    };
    let maps = BTreeMap::new();
    let result = inline_same_step_deps("yt-dlp", &req, &maps, known_dependency_type);
    assert!(result.is_empty());
}

#[test]
fn inline_same_step_deps_no_recursion() {
    // A dep's stored own map may (defensively) contain `deps/...` keys;
    // those must never be re-inlined — deps are non-transitive, so the
    // output never contains nested `deps/` paths like
    // `deps/ffmpeg/deps/x/...`.
    let deps = BTreeMap::from([("ffmpeg".to_string(), ConfigVersionSpec::Latest)]);
    let req = ToolRequirement {
        version_spec: ConfigVersionSpec::Latest,
        dependencies: deps,
        ..Default::default()
    };
    let mut maps: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    maps.insert(
        "ffmpeg".to_string(),
        BTreeMap::from([
            ("linux/ffmpeg".to_string(), "blake3:a".to_string()),
            ("deps/x/linux/x".to_string(), "blake3:b".to_string()),
        ]),
    );
    let result = inline_same_step_deps("yt-dlp", &req, &maps, known_dependency_type);
    assert_eq!(
        result,
        BTreeMap::from([("deps/ffmpeg/linux/ffmpeg".to_string(), "blake3:a".to_string())]),
        "only the dep's own payload keys are inlined; deps/ keys are never re-inlined",
    );
    assert!(
        result.keys().all(|k| !k.contains("/deps/")),
        "no nested deps/ paths allowed: {result:?}",
    );
}

#[test]
fn inline_same_step_deps_own_keys_untouched() {
    // Inlining returns only `deps/`-prefixed entries; the requester's own
    // keys live in the payload content map, never in the inlined set.
    let deps = BTreeMap::from([("ffmpeg".to_string(), ConfigVersionSpec::Latest)]);
    let req = ToolRequirement {
        version_spec: ConfigVersionSpec::Latest,
        dependencies: deps,
        ..Default::default()
    };
    let mut maps: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    maps.insert(
        "ffmpeg".to_string(),
        BTreeMap::from([("linux/ffmpeg".to_string(), "blake3:a".to_string())]),
    );
    let result = inline_same_step_deps("yt-dlp", &req, &maps, known_dependency_type);
    assert!(result.keys().all(|k| k.starts_with("deps/")));
}

#[test]
fn strip_inlined_deps_keys_removes_deps_prefix() {
    let map = BTreeMap::from([
        ("linux/yt-dlp".to_string(), "blake3:a".to_string()),
        ("deps/ffmpeg/linux/ffmpeg".to_string(), "blake3:b".to_string()),
    ]);
    assert_eq!(
        strip_inlined_deps_keys(&map),
        BTreeMap::from([("linux/yt-dlp".to_string(), "blake3:a".to_string())]),
    );
}

#[test]
fn strip_inlined_deps_keys_keeps_own_keys() {
    let map = BTreeMap::from([
        ("linux/yt-dlp".to_string(), "blake3:a".to_string()),
        ("macos/yt-dlp".to_string(), "blake3:c".to_string()),
    ]);
    assert_eq!(strip_inlined_deps_keys(&map), map);
}

#[test]
fn strip_inlined_deps_keys_empty_when_only_deps() {
    let map = BTreeMap::from([
        ("deps/ffmpeg/linux/ffmpeg".to_string(), "blake3:b".to_string()),
        ("deps/deno/linux/deno".to_string(), "blake3:c".to_string()),
    ]);
    assert!(strip_inlined_deps_keys(&map).is_empty());
}
