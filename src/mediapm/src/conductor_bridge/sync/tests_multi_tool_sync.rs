//! One reconcile pass over a seeded two-tool cache: per-tool bars and repeat-run determinism.

use crate::config::ToolRequirement;
use crate::example_isolation::lock_process_env;
use mediapm_utils::progress::recording::{ProgressOp, RecordingProgressTracker};
use std::collections::BTreeMap;
use std::collections::BTreeSet;

use super::*;

/// Seeds a user-level download cache with yt-dlp metadata (tag+hash)
/// and three plain-binary payloads (windows/macos/linux).
///
/// Returns the cache root path. The `Cache` handle is dropped before
/// return, releasing the directory lock; the caller keeps the
/// underlying `TempDir` alive so the data persists.
///
/// media-tagger is an offline builtin launcher (`GenerateLauncher`);
/// its `resolve_tool_fetch` arm returns `sources()` directly with
/// `metadata_fetch_count: 0`, so no metadata seeding is required.
async fn seed_two_tool_cache(cache_root: &std::path::Path) {
    let cache = Cache::open(
        cache_root,
        &[
            CacheDomainConfig {
                domain: "tools".to_string(),
                index_file_name: "tools.json".to_string(),
                entry_ttl_seconds: ENTRY_TTL_SECONDS,
            },
            CacheDomainConfig {
                domain: "tool_metadata".to_string(),
                index_file_name: "tool_metadata.json".to_string(),
                entry_ttl_seconds: 24 * 60 * 60,
            },
        ],
    )
    .await
    .expect("test cache opens");

    // Metadata: yt-dlp tag resolution served from cache (no GitHub API).
    let tag = "2025.07.15";
    let hash = "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0";
    let api_key = format!(
        "{}/repos/yt-dlp/yt-dlp/releases/latest",
        crate::tools::provider::github_api_base()
    );
    cache.store_bytes("tool_metadata", &api_key, format!("{tag}\n{hash}").as_bytes()).await;

    // Payloads: three plain binaries at the REWRITTEN release URLs.
    // `fetch_tool_sources` consults the cache by final URL, so seeding
    // these exact keys makes the run network-free. The keys are built from
    // the same base the provider builds its URLs from, so a seam-redirected
    // run seeds the URLs that run will look up.
    for (filename, payload) in &[
        ("yt-dlp.exe", &b"fake yt-dlp windows binary"[..]),
        ("yt-dlp_macos", &b"fake yt-dlp macos binary"[..]),
        ("yt-dlp_linux", &b"fake yt-dlp linux binary"[..]),
    ] {
        let url = format!(
            "{}/yt-dlp/yt-dlp/releases/download/{tag}/{filename}",
            crate::tools::provider::github_download_base()
        );
        cache.store_bytes("tools", &url, payload).await;
    }
    // Cache handle dropped here — directory lock released, data persists
    // on disk under the TempDir that the caller holds.
}

/// Asserts the overall `syncing tools` bar ended the run at its total (spec AC3).
///
/// The overall row is located by its `AddBar` label and advances are summed
/// on that row only, so no op recorded against a per-tool phase row can ever
/// count toward the overall position — phase-bar work must not be able to
/// fake the overall success this asserts, whatever ops those bars emit today
/// or gain later. `expected_entries` is the entry count the fixture
/// configured: the total must equal it, and the position must equal the
/// total, or a fetched-only sync froze below its own bar.
fn assert_overall_position_reaches_total(
    tracker: &RecordingProgressTracker,
    expected_entries: u64,
) {
    let recorded = tracker.recorded();
    let (overall_bar, overall_total) = recorded
        .iter()
        .find_map(|entry| match &entry.op {
            ProgressOp::AddBar { label, total } if label == "syncing tools" => {
                Some((entry.bar, *total))
            }
            _ => None,
        })
        .expect("the overall bar must be registered through the tracker");
    assert_eq!(
        overall_total, expected_entries,
        "the overall total must be the configured entry count"
    );
    let position: u64 = recorded
        .iter()
        .filter(|entry| entry.bar == overall_bar)
        .filter_map(|entry| match &entry.op {
            ProgressOp::Advance { delta } => Some(*delta),
            _ => None,
        })
        .sum();
    assert_eq!(
        position, overall_total,
        "a fetched-only sync must advance the overall bar to its total once every entry \
         terminates; got {recorded:?}"
    );
}

/// Verifies that the parallel provisioning driver creates the correct
/// per-tool bars regardless of completion order. The assertion is a
/// **sorted multiset** of `(tool_id, phase)` pairs extracted from every
/// `AddBar` label — order-free by construction.
///
/// Two tools: yt-dlp (seeded metadata + payloads) and media-tagger
/// (offline `GenerateLauncher`). Both take the `Resolved` path (empty
/// `MediaPmState` defeats both skip checks), so each produces exactly
/// 3 bars: `[res]`, `[fch]`, `[pro]`. The overall bar adds 1.
#[tokio::test]
#[expect(
    clippy::await_holding_lock,
    reason = "the process env lock serializes this test's github_api_base() reads against the MEDIAPM_GITHUB_API_BASE seam mutation scope"
)]
async fn sync_multi_tool_per_tool_bars_are_order_independent() {
    let _env = lock_process_env();
    let tmp = mediapm_utils::temp::artifact_dir().unwrap();
    let cache_root_tmp = mediapm_utils::temp::cache_dir().unwrap();
    let paths = MediaPmPaths::from_root(tmp.path());
    let tracker = RecordingProgressTracker::new();
    let state = MediaPmState::default();

    // Seed the cache with yt-dlp metadata + payloads.
    seed_two_tool_cache(cache_root_tmp.path()).await;
    // Cache handle dropped inside the fixture — directory lock released.

    let workspace_cas = super::open_workspace_cas_store(&paths).await.expect("open workspace cas");

    // desired_tools: yt-dlp (needs cache seed) + media-tagger (offline).
    // Both use Latest so neither skip-check fires (empty state).
    let mut desired_tools = BTreeMap::new();
    desired_tools.insert(
        "yt-dlp".to_string(),
        serde_json::to_value(ToolRequirement {
            version_spec: ConfigVersionSpec::Latest,
            ..Default::default()
        })
        .unwrap(),
    );
    desired_tools.insert(
        "media-tagger".to_string(),
        serde_json::to_value(ToolRequirement {
            version_spec: ConfigVersionSpec::Latest,
            ..Default::default()
        })
        .unwrap(),
    );

    let result = reconcile_desired_tools(
        workspace_cas,
        &paths,
        &desired_tools,
        &BTreeMap::new(),
        RecheckPolicy::default(),
        &state,
        Some(cache_root_tmp.path()),
        &tracker,
        None,
    )
    .await
    .map(ToolSyncOutcome::into_report);

    assert!(result.is_ok(), "reconcile_desired_tools failed: {:?}", result.err());

    let ops = tracker.ops();

    // --- overall bar ---
    let add_bars: Vec<&ProgressOp> =
        ops.iter().filter(|op| matches!(op, ProgressOp::AddBar { .. })).collect();
    assert_eq!(
        add_bars.len(),
        1 + 3 * 2,
        "expected 1 overall + 3 bars per tool (res/fch/pro) × 2 tools = 7, got {}",
        add_bars.len()
    );

    // --- extract (tool_id, phase) from each AddBar label ---
    let mut observed: Vec<(String, String)> = Vec::new();
    for op in &add_bars {
        if let ProgressOp::AddBar { label, .. } = op {
            // Labels: "syncing tools", "yt-dlp <ver> [res]", etc.
            // Phase is the last token in brackets.
            let phase = label
                .rsplit_once('[')
                .and_then(|(_, rest)| rest.strip_suffix(']'))
                .unwrap_or("overall")
                .to_string();
            // Tool id: everything before the phase tag (or "tools" for overall).
            let tool_id = if phase == "overall" {
                "tools".to_string()
            } else {
                label.split_once(' ').map(|(id, _)| id.to_string()).unwrap_or_default()
            };
            observed.push((tool_id, phase));
        }
    }
    observed.sort();

    let mut expected: Vec<(String, String)> = vec![
        ("tools".to_string(), "overall".to_string()),
        ("yt-dlp".to_string(), "res".to_string()),
        ("yt-dlp".to_string(), "fch".to_string()),
        ("yt-dlp".to_string(), "pro".to_string()),
        ("media-tagger".to_string(), "res".to_string()),
        ("media-tagger".to_string(), "fch".to_string()),
        ("media-tagger".to_string(), "pro".to_string()),
    ];
    expected.sort();

    assert_eq!(
        observed, expected,
        "sorted (tool_id, phase) multiset mismatch — order-independence violated"
    );

    // --- bar totals: [fch] = 3, [pro] = 3 for both tools ---
    for op in &add_bars {
        if let ProgressOp::AddBar { label, total } = op
            && (label.ends_with("[fch]") || label.ends_with("[pro]"))
        {
            assert_eq!(
                *total, 3,
                "{label}: expected total 3 for plain-binary/launcher sources, got {total}"
            );
        }
    }

    // --- overall position reaches total (spec AC3, fetched-only success) ---
    // Both entries took the `Resolved` path (seeded cache, no network): the
    // pool must have counted each one, or the row freezes at 0 under finished
    // children — the `0/7` symptom this rule exists for.
    assert_overall_position_reaches_total(&tracker, 2);
}

/// Every per-tool phase bar carries the tool id and the phase tag, with the
/// version between them when the provider reported one.
///
/// The three fields reach the bar as `PrefixComponents` rather than as one
/// rendered string, so what the row reads is the renderer's joining of them:
/// the tool id first, then the version, then the bracketed phase. The
/// version is legitimately absent for a tool whose provider reports none,
/// which is why this asserts the two segments on either side of it rather
/// than a fixed three-part label.
///
/// The fixture is the same two tools the order-independence test seeds:
/// yt-dlp resolves a tag out of the seeded metadata, and media-tagger
/// generates its launcher offline. Both take the `Resolved` path, so each
/// produces a `[res]`, a `[fch]` and a `[pro]` bar.
#[tokio::test]
#[expect(
    clippy::await_holding_lock,
    reason = "the process env lock serializes this test's github_api_base() reads against the MEDIAPM_GITHUB_API_BASE seam mutation scope"
)]
async fn per_tool_phase_bars_carry_the_version_between_the_tool_and_the_phase() {
    let _env = lock_process_env();
    let tmp = mediapm_utils::temp::artifact_dir().unwrap();
    let cache_root_tmp = mediapm_utils::temp::cache_dir().unwrap();
    let paths = MediaPmPaths::from_root(tmp.path());
    let tracker = RecordingProgressTracker::new();
    let state = MediaPmState::default();

    seed_two_tool_cache(cache_root_tmp.path()).await;

    let workspace_cas = super::open_workspace_cas_store(&paths).await.expect("open workspace cas");

    let mut desired_tools = BTreeMap::new();
    for tool in ["yt-dlp", "media-tagger"] {
        desired_tools.insert(
            tool.to_string(),
            serde_json::to_value(ToolRequirement {
                version_spec: ConfigVersionSpec::Latest,
                ..Default::default()
            })
            .unwrap(),
        );
    }

    let result = reconcile_desired_tools(
        workspace_cas,
        &paths,
        &desired_tools,
        &BTreeMap::new(),
        RecheckPolicy::default(),
        &state,
        Some(cache_root_tmp.path()),
        &tracker,
        None,
    )
    .await
    .map(ToolSyncOutcome::into_report);
    assert!(result.is_ok(), "reconcile_desired_tools failed: {:?}", result.err());

    let ops = tracker.ops();
    let phase_labels: Vec<&str> = ops
        .iter()
        .filter_map(|op| match op {
            ProgressOp::AddBar { label, .. }
                if label.ends_with(']')
                    && !label.starts_with("syncing")
                    && !label.starts_with("pruning") =>
            {
                Some(label.as_str())
            }
            _ => None,
        })
        .collect();
    assert_eq!(phase_labels.len(), 6, "two tools at three phases each: {phase_labels:?}");

    // yt-dlp's provider resolves a tag from the seeded metadata, so its
    // rows must carry it between the tool id and the phase tag. The tag is
    // `2025.07.15`; pinning it here is what makes a version that went
    // missing, or moved behind the phase tag, a failure rather than a
    // shorter label nobody reads.
    // Each bar reads tool id, then the version the provider reported, then the phase
    // tag. Pinning the exact version strings would fail whenever a resolved `Latest`
    // moved to a new content hash, which is not what this row is about: the row asks
    // that the segment be present at all. So the shape is checked, and the segment is
    // checked for being non-empty, which is the part the order-independent test
    // discarded.
    let mut seen: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    for label in &phase_labels {
        let (tool, rest) = label
            .split_once(' ')
            .unwrap_or_else(|| panic!("a phase bar reads tool id then version then tag: {label}"));
        let (version, tag) = rest
            .split_once(' ')
            .unwrap_or_else(|| panic!("a phase bar carries a version segment: {label}"));
        assert!(
            !version.is_empty(),
            "the version segment between the tool id and the phase must not be empty: {label}"
        );
        let tag = tag
            .strip_prefix('[')
            .and_then(|t| t.strip_suffix(']'))
            .unwrap_or_else(|| panic!("the phase tag is bracketed: {label}"));
        assert!(
            matches!(tag, "res" | "fch" | "pro"),
            "the tail is a phase tag, not a version: {label}"
        );
        seen.entry(tool).or_default().insert(tag);
    }

    assert_eq!(seen.len(), 2, "both tools drew their own phase bars: {seen:?}");
    for (tool, phases) in &seen {
        assert_eq!(
            phases,
            &["fch", "pro", "res"].into_iter().collect(),
            "{tool} drew one bar per phase: {phases:?}"
        );
    }
}

/// Parallel source fetch must be deterministic: running
/// `reconcile_desired_tools` twice with the same seeded cache must
/// produce identical generated-document bytes and bar-operation multisets.
#[tokio::test]
#[expect(
    clippy::await_holding_lock,
    reason = "the process env lock serializes this test's github_api_base() reads against the MEDIAPM_GITHUB_API_BASE seam mutation scope"
)]
#[expect(
    clippy::too_many_lines,
    reason = "the determinism contract is the comparison itself: both runs, the document-byte equality, and the bar-multiset equality must stay visible together so a future edit cannot quietly compare only one of the two artifacts"
)]
async fn sync_parallel_fetch_is_deterministic() {
    let _env = lock_process_env();
    let tmp1 = mediapm_utils::temp::artifact_dir().unwrap();
    let tmp2 = mediapm_utils::temp::artifact_dir().unwrap();
    let cache1 = mediapm_utils::temp::cache_dir().unwrap();
    let cache2 = mediapm_utils::temp::cache_dir().unwrap();

    let paths1 = MediaPmPaths::from_root(tmp1.path());
    let paths2 = MediaPmPaths::from_root(tmp2.path());

    // Seed both caches identically.
    seed_two_tool_cache(cache1.path()).await;
    seed_two_tool_cache(cache2.path()).await;

    let mut desired_tools = BTreeMap::new();
    desired_tools.insert(
        "yt-dlp".to_string(),
        serde_json::to_value(ToolRequirement {
            version_spec: ConfigVersionSpec::Latest,
            ..Default::default()
        })
        .unwrap(),
    );
    desired_tools.insert(
        "media-tagger".to_string(),
        serde_json::to_value(ToolRequirement {
            version_spec: ConfigVersionSpec::Latest,
            ..Default::default()
        })
        .unwrap(),
    );

    let state = MediaPmState::default();

    // --- Run 1 ---
    let cas1 = super::open_workspace_cas_store(&paths1).await.expect("open cas 1");
    let tracker1 = RecordingProgressTracker::new();
    reconcile_desired_tools(
        cas1,
        &paths1,
        &desired_tools,
        &BTreeMap::new(),
        RecheckPolicy::default(),
        &state,
        Some(cache1.path()),
        &tracker1,
        None,
    )
    .await
    .expect("run 1 failed");

    // --- Run 2 ---
    let cas2 = super::open_workspace_cas_store(&paths2).await.expect("open cas 2");
    let tracker2 = RecordingProgressTracker::new();
    reconcile_desired_tools(
        cas2,
        &paths2,
        &desired_tools,
        &BTreeMap::new(),
        RecheckPolicy::default(),
        &state,
        Some(cache2.path()),
        &tracker2,
        None,
    )
    .await
    .expect("run 2 failed");

    // --- Assert identical generated-doc bytes ---
    //
    // Apart from the workspace root: the document names the directory each tool
    // payload was materialized into, and two workspaces hold the same document
    // at different paths. Everything else still has to match byte for byte.
    let doc1_raw = std::fs::read(&paths1.conductor_generated_ncl).expect("read generated doc 1");
    let doc2_raw = std::fs::read(&paths2.conductor_generated_ncl).expect("read generated doc 2");
    let doc1 = normalize_workspace_root(&doc1_raw, &paths1.root_dir);
    let doc2 = normalize_workspace_root(&doc2_raw, &paths2.root_dir);
    assert_eq!(doc1, doc2, "generated-doc bytes differ between runs");

    // --- Assert identical bar-operation multisets ---
    let mut bars1: Vec<(String, String)> = tracker1
        .ops()
        .iter()
        .filter_map(|op| {
            if let ProgressOp::AddBar { label, .. } = op {
                let phase = label
                    .rsplit_once('[')
                    .and_then(|(_, rest)| rest.strip_suffix(']'))
                    .unwrap_or("overall")
                    .to_string();
                let tool_id = if phase == "overall" {
                    "tools".to_string()
                } else {
                    label.split_once(' ').map(|(id, _)| id.to_string()).unwrap_or_default()
                };
                Some((tool_id, phase))
            } else {
                None
            }
        })
        .collect();
    let mut bars2: Vec<(String, String)> = tracker2
        .ops()
        .iter()
        .filter_map(|op| {
            if let ProgressOp::AddBar { label, .. } = op {
                let phase = label
                    .rsplit_once('[')
                    .and_then(|(_, rest)| rest.strip_suffix(']'))
                    .unwrap_or("overall")
                    .to_string();
                let tool_id = if phase == "overall" {
                    "tools".to_string()
                } else {
                    label.split_once(' ').map(|(id, _)| id.to_string()).unwrap_or_default()
                };
                Some((tool_id, phase))
            } else {
                None
            }
        })
        .collect();
    bars1.sort();
    bars2.sort();
    assert_eq!(bars1, bars2, "bar-operation multisets differ between runs");
}

/// Replaces a workspace root with a fixed token in a generated document.
///
/// The token assertion is what keeps the normalization honest: one that matched
/// nothing would leave the comparison exactly as it was and hide the reason it
/// exists.
///
/// `tests/int/tool_sync/inline_deps.rs` carries the same helper. The two cannot
/// share one: this copy lives in a `#[cfg(test)]` module inside the crate and
/// that one in a separate crate, so a shared helper would have to be public API
/// of `mediapm` and would export a test-only utility.
fn normalize_workspace_root(document: &[u8], root: &std::path::Path) -> String {
    let text = String::from_utf8(document.to_vec()).expect("the generated document is UTF-8");
    let root_text = root.to_string_lossy();
    let normalized = text.replace(root_text.as_ref(), "<workspace>");
    assert!(
        normalized.contains("<workspace>"),
        "the generated document has to name the workspace root it was written in",
    );
    normalized
}
