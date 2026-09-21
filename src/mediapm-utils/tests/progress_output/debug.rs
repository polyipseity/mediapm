//! Tests for progress debug instrumentation (JSONL output per tick).
//!
//! These tests validate that `ProgressDebugSink` produces valid JSONL lines
//! containing the expected bar state fields. The ticker thread may race with
//! manual `group.tick()` calls, so we check *at least* the expected output
//! rather than exact counts.

use std::time::Duration;

use super::common::*;

/// Helper: create a `ProgressDebugSink` backed by a temp file, return the
/// sink, the path, and the `TempDir` (kept alive so the file persists).
fn debug_sink_to_file() -> (ProgressDebugSink, std::path::PathBuf, tempfile::TempDir) {
    let dir = mediapm_utils::temp::artifact_dir().unwrap();
    let path = dir.path().join("debug.jsonl");
    let file = std::fs::File::create(&path).unwrap();
    (ProgressDebugSink::new(Box::new(file)), path, dir)
}

/// Helper: create a minimal `ProgressGroup` with a debug sink, a single bar,
/// and manual tick control.
fn make_debug_group(sink: ProgressDebugSink) -> (ProgressGroup, TrackedHandle) {
    let (mp, _term) = mk();
    let group = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(4)
        .with_progress_debug_sink(sink)
        .build();
    let bar = group.add_bar(4, "test-label");
    (group, bar)
}

#[test]
fn progress_debug_emits_one_line_per_tick() {
    let (sink, path, _dir) = debug_sink_to_file();
    let (group, _bar) = make_debug_group(sink);

    // Force enough ticks so at least 2 land in the file.
    for _ in 0..10 {
        group.tick();
        std::thread::sleep(Duration::from_millis(5));
    }

    drop(group);

    let contents = std::fs::read_to_string(&path).unwrap();
    let lines: Vec<&str> = contents.lines().collect();

    // At least 2 lines from manual ticks (ticker thread may add more).
    assert!(lines.len() >= 2, "expected ≥2 JSONL lines for 10 forced ticks, got {}", lines.len());

    // Every line must be a JSON object with `"type":"tick"`.
    for (i, line) in lines.iter().enumerate() {
        assert!(line.contains(r#""type":"tick""#), "line {i} missing type field: {line}");
    }
}

#[test]
fn progress_debug_shows_bar_state() {
    let (sink, path, _dir) = debug_sink_to_file();
    let (group, bar) = make_debug_group(sink);

    // Advance the bar to a known state.
    bar.set_position(2);
    group.tick();
    std::thread::sleep(Duration::from_millis(10));
    group.tick();
    std::thread::sleep(Duration::from_millis(10));

    drop(group);

    let contents = std::fs::read_to_string(&path).unwrap();
    let first_line = contents.lines().next().expect("expected at least one JSONL line");

    // Verify key bar-state fields are present in snake_case.
    assert!(first_line.contains(r#""slot":0"#), "missing slot: {first_line}");
    assert!(first_line.contains(r#""bound":true"#), "missing bound: {first_line}");
    assert!(first_line.contains(r#""label""#), "missing label field: {first_line}");
    assert!(first_line.contains(r#""position""#), "missing position field: {first_line}");
    assert!(first_line.contains(r#""total""#), "missing total field: {first_line}");
    assert!(first_line.contains(r#""status""#), "missing status field: {first_line}");
    assert!(first_line.contains(r#""dirty""#), "missing dirty field: {first_line}");
    assert!(first_line.contains(r#""test-label""#), "missing expected label value: {first_line}");

    // Verify tick metadata.
    assert!(first_line.contains(r#""type":"tick""#), "missing type: {first_line}");
    assert!(first_line.contains(r#""tick""#), "missing tick counter: {first_line}");
    assert!(first_line.contains(r#""elapsed_secs""#), "missing elapsed_secs: {first_line}");
    assert!(first_line.contains(r#""bars""#), "missing bars array: {first_line}");
}

#[test]
fn progress_debug_no_bars_shows_empty_bars_array() {
    let (sink, path, _dir) = debug_sink_to_file();
    let (mp, _term) = mk();
    let group = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(4)
        .with_progress_debug_sink(sink)
        .build();

    group.tick();
    std::thread::sleep(Duration::from_millis(10));
    group.tick();

    drop(group);

    let contents = std::fs::read_to_string(&path).unwrap();
    let first_line = contents.lines().next().expect("expected at least one JSONL line");

    // Empty bars array: slots still exist (capacity=4) but none bound.
    assert!(first_line.contains(r#""bars":"#), "missing bars in output: {first_line}");
    assert!(first_line.contains(r#""type":"tick""#), "missing type field: {first_line}");
}

#[test]
fn progress_debug_env_auto_creates_file() {
    // Write under a managed temp dir so panics don't leak files into the crate root.
    let dir = mediapm_utils::temp::artifact_dir().unwrap();
    let debug_path = dir.path().join("debug-env.jsonl");
    let debug_path_str = debug_path.to_str().unwrap().to_string();

    // Serialize on ENV_LOCK: parallel tests in the same process share the
    // process-global env var. Without this, a sibling test's set_var/remove_var
    // between our build() and tick() causes the sink to point at the wrong file
    // or to be absent entirely.
    let _lock = ENV_LOCK.lock().expect("env lock poisoned");
    // SAFETY: held under ENV_LOCK — single-threaded access.
    let _guard = unsafe { EnvVarGuard::set("MEDIAPM_PROGRESS_DEBUG", &debug_path_str) };

    // Ensure no stale file from a previous run.
    let _ = std::fs::remove_file(&debug_path);

    let (mp, _term) = mk();
    let group = ProgressGroup::builder().with_multi_progress(mp).build();

    group.tick();
    std::thread::sleep(Duration::from_millis(10));

    drop(group);

    assert!(debug_path.exists(), "expected debug file at {}", debug_path.display());

    let contents = std::fs::read_to_string(&debug_path).unwrap();
    assert!(!contents.is_empty(), "debug file should contain at least one tick line");

    // Cleanup handled by dir Drop (TempDir removes the whole tree).
}

#[test]
fn progress_debug_append_across_groups() {
    // Two sequential `ProgressGroup::builder().build()` calls in the same
    // process must both contribute lines to the same debug file. The sink must
    // APPEND (not truncate) so the earlier group's ticks survive.
    let dir = mediapm_utils::temp::artifact_dir().unwrap();
    let debug_path = dir.path().join("debug-append.jsonl");
    let debug_path_str = debug_path.to_str().unwrap().to_string();

    // Serialize on ENV_LOCK: without this, a sibling test's set_var/remove_var
    // between our two build() calls makes group 2 write to a different file or
    // nowhere, leaving only group 1's lines (5 < 6 → false failure).
    let _lock = ENV_LOCK.lock().expect("env lock poisoned");
    // SAFETY: held under ENV_LOCK — single-threaded access.
    let _guard = unsafe { EnvVarGuard::set("MEDIAPM_PROGRESS_DEBUG", &debug_path_str) };

    let _ = std::fs::remove_file(&debug_path);

    // First group: writes its ticks to the file.
    {
        let (mp, _term) = mk();
        let group = ProgressGroup::builder().with_multi_progress(mp).build();
        for _ in 0..5 {
            group.tick();
            std::thread::sleep(Duration::from_millis(5));
        }
        drop(group);
    }

    // Second group: must APPEND, not truncate, so the first group's lines
    // remain. Keep the env var set so build() re-detects the same path.
    {
        let (mp, _term) = mk();
        let group = ProgressGroup::builder().with_multi_progress(mp).build();
        for _ in 0..5 {
            group.tick();
            std::thread::sleep(Duration::from_millis(5));
        }
        drop(group);
    }

    // Release env lock and guard — restore env before file read.
    drop(_guard);
    drop(_lock);

    let contents = std::fs::read_to_string(&debug_path).unwrap();
    let lines: Vec<&str> = contents.lines().filter(|l| !l.is_empty()).collect();

    // Both groups contributed ticks; the file must hold more than a single
    // group's worth (truncation would leave only the second group's lines).
    assert!(
        lines.len() >= 6,
        "expected ≥6 JSONL lines from two sequential groups (append), got {}: {contents}",
        lines.len()
    );
    for (i, line) in lines.iter().enumerate() {
        assert!(line.contains(r#""type":"tick""#), "line {i} missing type field: {line}");
    }

    // Cleanup handled by dir Drop (TempDir removes the whole tree).
}
