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
    ResolvedSource, ResolvedToolFetch, SourceProducer, fetch_tool_sources, process_tool_sources,
    resolve_tool_fetch,
};
use mediapm_utils::progress::{ProviderProgressCallback, ProviderProgressSnapshot};

use crate::{EnvVarGuard, rpc_env_lock};

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

/// Fetch progress aggregates every source's bytes: `items` counts completed
/// sources, and the byte aggregate ends at the payloads' summed size.
///
/// The fetch is serialized through `MEDIAPM_SOURCE_FETCH_CONCURRENCY=1` so the
/// snapshot sequence is the pipeline's schedule rather than a race between two
/// concurrent downloads. Two sources in flight at once can push a snapshot that
/// read the completed count before the other task's increment and so appears
/// after it, which is why the crate's own unit test asserts a maximum rather
/// than a monotone run; one source at a time makes monotonicity a property of
/// the pipeline instead of the machine's timing.
#[tokio::test]
async fn progress_aggregation_over_a_mocked_fetch_source_covers_every_byte() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    // One source in flight at a time, so the snapshot sequence is the
    // pipeline's schedule rather than a race between two concurrent downloads.
    // The guard restores any prior value on drop, and the lock is what makes
    // the process-wide mutation safe while sibling tests read the environment;
    // see `rpc_env_lock`.
    let _lock = rpc_env_lock().await;
    let _concurrency = EnvVarGuard::set("MEDIAPM_SOURCE_FETCH_CONCURRENCY", "1");

    let first = vec![0x11u8; 3_000];
    let second = vec![0x22u8; 5_000];
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/first.bin"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(first.clone()))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/second.bin"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(second.clone()))
        .mount(&server)
        .await;

    let source = |path: &str, len: usize| ResolvedSource {
        os: "linux".to_string(),
        arch: "x86_64".to_string(),
        producer: SourceProducer::Fetch { urls: vec![format!("{}/{path}", server.uri())] },
        expected_size: Some(len as u64),
        size_hint_bytes: None,
        identity: None,
    };
    let fetch = ResolvedToolFetch {
        tool_id: "mocked".to_string(),
        sources: vec![source("first.bin", first.len()), source("second.bin", second.len())],
    };

    let (cache, _cache_root) = open_echo_cache().await;
    let snapshots: std::sync::Arc<std::sync::Mutex<Vec<ProviderProgressSnapshot>>> =
        std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let recorder = std::sync::Arc::clone(&snapshots);
    let cb: ProviderProgressCallback = std::sync::Arc::new(move |snap| {
        recorder.lock().expect("the snapshot recorder is never poisoned").push(snap);
    });

    let downloaded = fetch_tool_sources(&fetch, &cache, "default", Some(cb))
        .await
        .expect("both mocked sources serve their bytes");
    assert_eq!(downloaded.entries.len(), 2, "each source yields its own entry");

    let all = snapshots.lock().expect("the snapshot recorder is never poisoned").clone();
    assert!(!all.is_empty(), "a fetch fires at least one snapshot");

    let mut completed_so_far = 0u64;
    for (index, snapshot) in all.iter().enumerate() {
        let (completed, total) = snapshot.items;
        assert_eq!(total, 2, "snapshot {index} counts both resolved sources: {snapshot:?}");
        assert!(
            completed >= completed_so_far,
            "the completed count decreased at snapshot {index}: {snapshot:?}"
        );
        completed_so_far = completed;
        let (done, expected) = snapshot.bytes;
        assert!(
            done <= expected,
            "the byte aggregate {done} exceeds its total {expected} at snapshot {index}: \
             {snapshot:?}"
        );
    }
    assert_eq!(completed_so_far, 2, "both sources must be counted as completed");

    let expected_bytes = (first.len() + second.len()) as u64;
    let last = all.last().expect("at least one snapshot was recorded");
    assert_eq!(
        last.bytes,
        (expected_bytes, expected_bytes),
        "the final snapshot's byte aggregate is the payloads' summed size: {last:?}"
    );
}
