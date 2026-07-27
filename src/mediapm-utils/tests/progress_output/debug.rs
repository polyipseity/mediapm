//! Tests for progress debug instrumentation (JSONL output per tick).
//!
//! These tests validate that `ProgressDebugSink` produces valid JSONL lines
//! containing the expected bar state fields. The ticker thread may race with
//! manual `group.tick()` calls, so we check *at least* the expected output
//! rather than exact counts.

use indicatif::{InMemoryTerm, MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use mediapm_utils::progress::{ProgressDebugSink, ProgressGroup, TrackedHandle};
use std::io::Write;
use std::time::Duration;

use super::common::*;

/// Helper: create a `ProgressDebugSink` backed by a temp file, return the
/// sink, the path, and the `TempDir` (kept alive so the file persists).
fn debug_sink_to_file() -> (ProgressDebugSink, std::path::PathBuf, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
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
    // Set the env var to `auto` before creating the group.
    // SAFETY: single-threaded test — no concurrent env access.
    unsafe {
        std::env::set_var("MEDIAPM_PROGRESS_DEBUG", "auto");
    }
    // Make sure we clean up after the test.
    let _cleanup = EnvGuard("MEDIAPM_PROGRESS_DEBUG");

    let pid = std::process::id();
    let expected_path = std::path::PathBuf::from(format!("progress-debug-{pid}.jsonl"));

    // Ensure no stale file from a previous run.
    let _ = std::fs::remove_file(&expected_path);

    let (mp, _term) = mk();
    let group = ProgressGroup::builder().with_multi_progress(mp).build();

    group.tick();
    std::thread::sleep(Duration::from_millis(10));

    drop(group);

    assert!(
        expected_path.exists(),
        "expected auto-named debug file at {}",
        expected_path.display()
    );

    let contents = std::fs::read_to_string(&expected_path).unwrap();
    assert!(!contents.is_empty(), "debug file should contain at least one tick line");

    // Clean up.
    let _ = std::fs::remove_file(&expected_path);
}

// ---------------------------------------------------------------------------
// RAII guard to unset an env var on drop (regardless of test outcome)
// ---------------------------------------------------------------------------

struct EnvGuard(&'static str);

impl Drop for EnvGuard {
    fn drop(&mut self) {
        // SAFETY: single-threaded test — no concurrent env access.
        unsafe {
            std::env::remove_var(self.0);
        }
    }
}
