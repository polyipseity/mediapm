//! Integration tests for the tool provider pipeline
//! (resolve → fetch → postprocess).
//!
//! These tests validate the full 3-phase pipeline end-to-end using
//! launcher-based (GenerateLauncher) tools that don't require network
//! access. The echo builtin is used because it produces launcher scripts
//! in memory rather than downloading payloads.

use mediapm_cas::CasApi;
use mediapm_cas::InMemoryCas;
use mediapm_conductor::cache_user_level::UserLevelCache;
use mediapm_conductor::tools::provider::{
    fetch_tool_sources, postprocess_tool_sources, resolve_tool_fetch,
};
use std::str::FromStr;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Expected OS labels produced by the echo provider.
const ECHO_OS_LABELS: &[&str] = &["linux", "macos", "windows"];

// ---------------------------------------------------------------------------
// Phase 1 — Resolve
// ---------------------------------------------------------------------------

/// Resolving echo returns a `ResolvedToolFetch` with one `GenerateLauncher`
/// source per platform.
#[tokio::test]
async fn resolve_echo_returns_three_launcher_sources() {
    let fetch = resolve_tool_fetch("echo").await.expect("resolve echo");

    assert_eq!(fetch.tool_id, "echo");
    assert_eq!(fetch.sources.len(), ECHO_OS_LABELS.len());

    let oses: Vec<&str> = fetch.sources.iter().map(|s| s.os.as_str()).collect();
    for expected_os in ECHO_OS_LABELS {
        assert!(oses.contains(expected_os), "missing source for OS {expected_os}");
    }

    for source in &fetch.sources {
        let producer = &source.producer;
        match producer {
            mediapm_conductor::tools::provider::SourceProducer::GenerateLauncher { builtin_id } => {
                assert_eq!(builtin_id, "echo@v1", "all launchers should reference echo@v1");
            }
            _ => panic!("echo sources should all be GenerateLauncher"),
        }
    }
}

/// Resolving an unknown tool returns an error.
#[tokio::test]
async fn resolve_unknown_tool_returns_error() {
    let result = resolve_tool_fetch("nonexistent-tool").await;
    assert!(result.is_err(), "unknown tool should fail to resolve");
}

// ---------------------------------------------------------------------------
// Phase 2 — Fetch
// ---------------------------------------------------------------------------

/// Fetching echo sources returns launcher scripts (shell/batch) for each OS,
/// cached in `UserLevelCache`.
#[tokio::test]
async fn fetch_echo_produces_launcher_scripts_via_cache() {
    let fetch = resolve_tool_fetch("echo").await.expect("resolve echo");

    let cache_root = tempfile::tempdir().expect("tempdir for cache");
    let (cache, _guard) = UserLevelCache::open(cache_root.path(), "tools.json", 30 * 24 * 60 * 60)
        .await
        .expect("open UserLevelCache");

    let downloaded = fetch_tool_sources(&fetch, &cache, None).await.expect("fetch echo sources");

    assert_eq!(downloaded.tool_id, "echo");
    assert_eq!(downloaded.entries.len(), ECHO_OS_LABELS.len());

    for entry in &downloaded.entries {
        assert!(ECHO_OS_LABELS.contains(&entry.os.as_str()), "unexpected OS: {}", entry.os);

        let text = String::from_utf8_lossy(&entry.bytes);
        assert!(
            text.contains("MEDIAPM_EXECUTABLE"),
            "launcher script should reference MEDIAPM_EXECUTABLE (os: {})",
            entry.os
        );
        assert!(
            text.contains("echo@v1"),
            "launcher script should reference echo@v1 (os: {})",
            entry.os
        );

        // Platform-specific checks
        if entry.os == "windows" {
            assert!(text.contains("%*"), "Windows launcher should use %* for args");
            assert!(text.contains("@echo off"), "Windows launcher should start with @echo off");
        } else {
            assert!(text.contains("$@"), "Unix launcher should use $@ for args");
            assert!(text.contains("#!/bin/sh"), "Unix launcher should have shebang");
        }
    }
}

/// Fetching from cache returns the same bytes on the second call.
#[tokio::test]
async fn fetch_echo_is_cached_idempotently() {
    let fetch = resolve_tool_fetch("echo").await.expect("resolve echo");
    let cache_root = tempfile::tempdir().expect("tempdir for cache");
    let (cache, _guard) = UserLevelCache::open(cache_root.path(), "tools.json", 30 * 24 * 60 * 60)
        .await
        .expect("open UserLevelCache");

    let first = fetch_tool_sources(&fetch, &cache, None).await.expect("first fetch");
    let second = fetch_tool_sources(&fetch, &cache, None).await.expect("second fetch");

    assert_eq!(first.entries.len(), second.entries.len());
    for (a, b) in first.entries.iter().zip(second.entries.iter()) {
        assert_eq!(a.os, b.os);
        assert_eq!(a.bytes, b.bytes, "cached bytes for OS {} should match", a.os);
    }
}

/// Postprocessing echo launchers produces a content map with one entry per OS
/// (binary format → `{os_label}/{tool_id}` keys) and an `os_exec_paths` map
/// with one entry per OS.
#[tokio::test]
async fn postprocess_echo_produces_correct_content_map_and_os_exec_paths() {
    let fetch = resolve_tool_fetch("echo").await.expect("resolve echo");
    let cache_root = tempfile::tempdir().expect("tempdir for cache");
    let (cache, _guard) = UserLevelCache::open(cache_root.path(), "tools.json", 30 * 24 * 60 * 60)
        .await
        .expect("open UserLevelCache");
    let downloaded = fetch_tool_sources(&fetch, &cache, None).await.expect("fetch echo");
    let cas = InMemoryCas::default();

    let result = postprocess_tool_sources(&downloaded, &cas, None).await.expect("postprocess echo");

    // Content map: one binary-format entry per OS
    assert_eq!(result.content_map.len(), ECHO_OS_LABELS.len(), "content map size");
    for os in ECHO_OS_LABELS {
        let key = format!("{os}/echo");
        assert!(
            result.content_map.contains_key(&key),
            "content map should contain key {key:?}, got keys: {:?}",
            result.content_map.keys().collect::<Vec<_>>(),
        );
    }

    // os_exec_paths: one entry per OS
    assert_eq!(result.os_exec_paths.len(), ECHO_OS_LABELS.len(), "os_exec_paths size");
    for os in ECHO_OS_LABELS {
        assert!(
            result.os_exec_paths.contains_key(*os),
            "os_exec_paths should contain OS {os:?}, got keys: {:?}",
            result.os_exec_paths.keys().collect::<Vec<_>>(),
        );
        assert_eq!(
            result.os_exec_paths[*os], "echo",
            "os_exec_paths for {os:?} should be \"echo\"",
        );
    }
}

// ---------------------------------------------------------------------------
// Full pipeline (resolve → fetch → postprocess)
// ---------------------------------------------------------------------------

/// The full pipeline for echo produces a usable `ProvisionResult` where
/// every content-map hash is retrievable from CAS.
#[tokio::test]
async fn full_pipeline_echo_all_hashes_retrievable_from_cas() {
    let fetch = resolve_tool_fetch("echo").await.expect("resolve echo");
    let cache_root = tempfile::tempdir().expect("tempdir for cache");
    let (cache, _guard) = UserLevelCache::open(cache_root.path(), "tools.json", 30 * 24 * 60 * 60)
        .await
        .expect("open UserLevelCache");
    let downloaded = fetch_tool_sources(&fetch, &cache, None).await.expect("fetch echo");
    let cas = InMemoryCas::default();

    let result = postprocess_tool_sources(&downloaded, &cas, None).await.expect("postprocess echo");

    // Every content-map hash should be retrievable
    for (key, hex_hash) in &result.content_map {
        let hash = mediapm_cas::Hash::from_str(hex_hash).expect("valid hash hex");
        let bytes = cas.get(hash).await.expect("hash should be retrievable from CAS");
        assert!(!bytes.is_empty(), "retrieved bytes for key {key:?} should not be empty");
    }

    // Verify binary content: each OS launcher script
    for os in ECHO_OS_LABELS {
        let key = format!("{os}/echo");
        let hex_hash = result.content_map.get(&key).expect("key in content map");
        let hash = mediapm_cas::Hash::from_str(hex_hash).expect("valid hash");
        let bytes = cas.get(hash).await.expect("retrievable from CAS");
        let text = String::from_utf8_lossy(&bytes);
        assert!(text.contains("MEDIAPM_EXECUTABLE"), "CAS content should be launcher script");
    }
}

/// Every registered conductor provider returns at least one source —
/// `sources.len()` must always be a valid positive count for progress bars.
#[tokio::test]
async fn resolve_tool_fetch_matches_sources_len_for_all_providers() {
    for tool_id in &["echo", "fs", "archive", "import", "export", "sd"] {
        let fetch =
            resolve_tool_fetch(tool_id).await.unwrap_or_else(|e| panic!("resolve {tool_id}: {e}"));
        assert!(!fetch.sources.is_empty(), "{tool_id}: expected at least one source, got empty",);
    }
}
