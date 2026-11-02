//! Integration tests for the tool provider pipeline
//! (resolve → fetch → process).
//!
//! The builtin tools (`echo`, `fs`, `archive`, `import`, `export`) resolve with
//! no sources: a builtin step carries no command parts and its implementation
//! runs in-process, so the pipeline has nothing to download for it. These tests
//! pin that end to end and offline — resolve answers with an empty source list,
//! and fetching and processing that list succeeds without touching the network.

use mediapm_cas::InMemoryCas;
use mediapm_conductor::cache::ENTRY_TTL_SECONDS;
use mediapm_conductor::cache_user_level::UserLevelCache;
use mediapm_conductor::tools::provider::{
    ResolvedToolFetch, fetch_tool_sources, process_tool_sources, resolve_tool_fetch,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Resolves the echo tool.
async fn resolve_echo() -> ResolvedToolFetch {
    resolve_tool_fetch("echo").await.expect("resolve echo")
}

/// Opens a fresh user-level cache in a temp dir.
async fn open_echo_cache() -> (UserLevelCache, tempfile::TempDir) {
    let cache_root = mediapm_utils::temp::cache_dir().expect("cache dir");
    let cache = UserLevelCache::open(cache_root.path(), "tools.json", ENTRY_TTL_SECONDS)
        .await
        .expect("open UserLevelCache");
    (cache, cache_root)
}

// ---------------------------------------------------------------------------
// Phase 1 — Resolve
// ---------------------------------------------------------------------------

/// Resolving echo returns no sources.
///
/// A builtin's implementation is code the host runs, so its `ResolvedToolFetch`
/// carries the tool id and an empty source list — neither a per-OS download nor
/// a script to generate.
#[tokio::test]
async fn resolve_echo_returns_no_sources() {
    let fetch = resolve_echo().await;

    assert_eq!(fetch.tool_id, "echo");
    assert!(fetch.sources.is_empty(), "a builtin has no payload, got {:?}", fetch.sources);
}

/// Resolving an unknown tool returns an error.
#[tokio::test]
async fn resolve_unknown_tool_returns_error() {
    let result = resolve_tool_fetch("nonexistent-tool").await;
    assert!(result.is_err(), "unknown tool should fail to resolve");
}

/// Every registered provider answers for the tool id it was asked about.
///
/// `sd` is a managed tool and still declares per-OS `Fetch` sources, which is
/// what keeps the empty lists above a statement about builtins rather than about
/// the registry as a whole.
#[tokio::test]
async fn every_registered_provider_resolves() {
    for tool_id in &["echo", "fs", "archive", "import", "export"] {
        let fetch =
            resolve_tool_fetch(tool_id).await.unwrap_or_else(|e| panic!("resolve {tool_id}: {e}"));
        assert_eq!(fetch.tool_id, *tool_id, "{tool_id}: resolve must answer under its own id");
        assert!(fetch.sources.is_empty(), "{tool_id}: a builtin declares no source");
    }

    let sd = resolve_tool_fetch("sd").await.expect("resolve sd");
    assert_eq!(sd.tool_id, "sd");
    assert!(!sd.sources.is_empty(), "sd is a managed tool and fetches a payload");
}

// ---------------------------------------------------------------------------
// Phases 2 and 3 — Fetch and process a tool with no sources
// ---------------------------------------------------------------------------

/// A builtin provisions without fetching: no entries, no cache traffic, and an
/// empty content map.
///
/// The pipeline is the one a managed tool walks, so this is what makes a
/// builtin's empty source list a pass-through rather than a special case.
#[tokio::test]
async fn a_builtin_provisions_without_fetching() {
    let fetch = resolve_echo().await;
    let (cache, _cache_root) = open_echo_cache().await;

    let downloaded = fetch_tool_sources(&fetch, &cache, "default", None)
        .await
        .expect("fetching no sources succeeds");
    assert!(downloaded.entries.is_empty(), "no source, no entry: {:?}", downloaded.entries);
    assert_eq!(downloaded.cached_count, 0, "nothing was looked up in the cache");
    assert!(downloaded.warnings.is_empty(), "nothing was tried: {:?}", downloaded.warnings);

    let cas = InMemoryCas::default();
    let result = process_tool_sources(&downloaded, &cas, None)
        .await
        .expect("processing no sources succeeds");
    assert!(
        result.content_map.is_empty(),
        "a builtin has no content to map: {:?}",
        result.content_map
    );
    assert!(
        result.os_exec_paths.is_empty(),
        "a builtin names no executable: {:?}",
        result.os_exec_paths
    );
}
