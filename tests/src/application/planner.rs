use serde_json::json;

use mediapm::{
    application::planner::{build_plan, render_plan_human},
    configuration::config::AppConfig,
};

#[test]
fn builds_non_empty_plan_for_sources_and_links() {
    let workspace = tempfile::tempdir().expect("temp workspace should create");
    let source_file = workspace.path().join("inbox/song.flac");
    std::fs::create_dir_all(source_file.parent().expect("parent should exist"))
        .expect("source parent should create");
    std::fs::write(&source_file, b"audio").expect("source file should write");

    let config: AppConfig = serde_json::from_value(json!({
        "sources": [{"uri": "inbox/song.flac"}],
        "links": [{"path": "library/song.flac", "from_uri": "inbox/song.flac"}]
    }))
    .expect("config should deserialize");

    let plan = build_plan(&config, workspace.path()).expect("plan should build");

    assert_eq!(plan.effects.len(), 2);
    assert!(!plan.is_empty());
}

#[test]
fn render_plan_human_mentions_effect_count() {
    let workspace = tempfile::tempdir().expect("temp workspace should create");
    let source_file = workspace.path().join("song.flac");
    std::fs::write(&source_file, b"audio").expect("source file should write");

    let config: AppConfig = serde_json::from_value(json!({
        "sources": [{"uri": "song.flac"}],
        "links": []
    }))
    .expect("config should deserialize");

    let plan = build_plan(&config, workspace.path()).expect("plan should build");
    let rendered = render_plan_human(&plan);

    assert!(rendered.contains("Planned 1 effect(s):"));
    assert!(rendered.contains("- import"));
}

#[test]
fn planning_is_deterministic_for_identical_inputs() {
    let workspace = tempfile::tempdir().expect("temp workspace should create");
    std::fs::write(workspace.path().join("a.flac"), b"a").expect("a should write");
    std::fs::write(workspace.path().join("b.flac"), b"b").expect("b should write");

    let config: AppConfig = serde_json::from_value(json!({
        "sources": [{"uri": "b.flac"}, {"uri": "a.flac"}],
        "links": []
    }))
    .expect("config should deserialize");

    let left = build_plan(&config, workspace.path()).expect("left plan should build");
    let right = build_plan(&config, workspace.path()).expect("right plan should build");

    assert_eq!(left, right);
}
