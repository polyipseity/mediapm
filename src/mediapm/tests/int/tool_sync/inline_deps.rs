use super::seeded_three_tool_skip_service;
use crate::common::stub_upstream::{
    STUB_COMMIT_SHA, STUB_DENO_COMMIT_SHA, binary_asset, mock_all_managed_tools,
};
use crate::common::{
    read_generated_doc, read_state, service_with_cache, test_sync_progress_overrides,
};
use mediapm::{
    MediaPmService, MediaPmState, MediaRuntimeStorage, ToolRegistryEntry, ToolRequirement,
};
use mediapm_conductor::{
    NickelDocument, PayloadStaging, ToolKindSpec, ToolRuntime, ToolSpec, encode_document,
};

// ---------------------------------------------------------------------------
// Same-step dependency payload inlining (deps/<mediapm tool id>/)
// ---------------------------------------------------------------------------
//
// The inlining itself (`inline_same_step_deps`) is covered by the unit tests
// in `conductor_bridge/sync/mod.rs`. The integration tests below cover the
// observable pipeline contracts around it:
//
// - `sync_skip_preserves_inlined_deps` — a generated-doc runtime that already
//   carries `deps/` keys survives a spec-based skip intact; the reserved
//   prefix is never stripped, renamed, or re-inlined on the skip path.
// - `sync_env_has_no_deps_garbage` — inlined `deps/` keys never leak into
//   `.env.generated` (no `_DEPS` / `_DIR` / `_COMPANIONS_` lines).
// - `sync_composite_non_transitive` — a dep whose stored `canonical_version`
//   is itself composite never triggers a false reprovision of the requester;
//   composite segments reference each dep's OWN version segment only.
//
// - `sync_inlines_same_step_deps_into_content_map` — a fresh sync of a
//   requester (yt-dlp) with a same-step dep (deno) inlines the dep's payload
//   under `deps/deno/<key>` into the generated-doc runtime, mirroring the
//   dep's own content-map values exactly, with no recursion and no
//   `companions/` residue.
// - `sync_dep_version_change_reprovisions_requester` — a requester whose
//   recorded composite names a dep version upstream no longer serves
//   reprovisions with freshly inlined payloads, while its own payload keys
//   keep byte-identical hashes.
// - `sync_level_ordering_applies_deps_before_requesters` — every dep key
//   reaches the requester, which holds only if the dep was applied first.
// - `sync_parallel_driver_is_deterministic` — two independent syncs of the
//   same fixture produce byte-identical generated documents.
//
// Every provisioning test here runs against the mocked upstream
// (`mock_all_managed_tools`), which serves yt-dlp's release metadata, its tag
// ref, and one bare binary per OS slot. The mock is what makes a
// download-backed provider exercisable end to end: the payload bytes arrive
// over HTTP from the fixture, so the download, extraction and content-import
// stages all run. yt-dlp + deno are the pair used throughout because deno is
// the same-step dependency the inlining assertions name; the fixture mounts
// every managed tool's traffic, so a test that later needs ffmpeg needs no
// fixture change.

/// Hermetic: seeds a generated doc whose `yt-dlp` runtime already carries
/// inlined `deps/ffmpeg/...` and `deps/deno/...` keys (as a previous network
/// sync would have produced) plus matching state entries, then runs sync with
/// exact version specs so all three tools spec-skip (no network).
///
/// Asserts the inlined structure survives the skip path: the generated doc
/// keeps the `deps/` keys alongside the requester's own keys, the state entry
/// keeps its non-transitive composite `canonical_version`, and no
/// `companions/` prefix ever appears.
#[tokio::test]
async fn sync_skip_preserves_inlined_deps() -> Result<(), mediapm::MediaPmError> {
    let (mut service, _root, _cache_root) = seeded_three_tool_skip_service().await?;

    service
        .sync_tools_with_progress_overrides(false, false, test_sync_progress_overrides())
        .await?;

    // The exact specs match the seeded resolved versions → all three tools
    // spec-skip; the inlined structure must survive intact.
    let doc = read_generated_doc(&service);
    let spec = doc
        .tools
        .values()
        .find(|s| s.name == "yt-dlp")
        .expect("yt-dlp entry must survive the skip path");
    let content_map = &spec.runtime.content_map;
    assert!(
        content_map.contains_key("deps/ffmpeg/linux/ffmpeg"),
        "inlined ffmpeg key must be preserved: {content_map:?}"
    );
    assert!(
        content_map.contains_key("deps/deno/linux/deno"),
        "inlined deno key must be preserved: {content_map:?}"
    );
    assert!(
        content_map.contains_key("linux/yt-dlp"),
        "requester's own key must be preserved: {content_map:?}"
    );
    assert!(
        content_map.keys().all(|k| !k.contains("companions")),
        "no companions/ prefix may appear: {content_map:?}"
    );

    // State entries are preserved unchanged on the skip path.
    let state_bytes =
        std::fs::read(&service.paths().mediapm_state_json).expect("state.json readable");
    let state: MediaPmState =
        serde_json::from_slice(&state_bytes).expect("state.json should deserialize");
    let entry =
        state.managed_tools.iter().find(|e| e.tool_id == "yt-dlp").expect("yt-dlp entry in state");
    assert_eq!(
        entry.canonical_version, "yt-dlp-v2024.01.01;deno:deno-v1.46.0;ffmpeg:ffmpeg-v7.1",
        "stored composite must stay non-transitive and unchanged"
    );
    assert_eq!(
        entry.content_map_hash, "blake3:abc123",
        "content_map_hash must be preserved on skip"
    );
    Ok(())
}

/// Hermetic: same seeding as `sync_skip_preserves_inlined_deps`; asserts
/// `.env.generated` never leaks inlined `deps/` keys.
///
/// Without the Phase 3 skip, `content_key_to_env_name` would split
/// `deps/ffmpeg/linux/ffmpeg` on the first `/` and emit `MEDIAPM_YT_DLP_DEPS_*`
/// garbage pointing at `.../payload/deps/...`. Inlined companions are
/// referenced via the predictable `deps/<tool_id>/` path, never env vars.
#[tokio::test]
async fn sync_env_has_no_deps_garbage() -> Result<(), mediapm::MediaPmError> {
    let (mut service, _root, _cache_root) = seeded_three_tool_skip_service().await?;

    service
        .sync_tools_with_progress_overrides(false, false, test_sync_progress_overrides())
        .await?;

    let content = std::fs::read_to_string(&service.paths().env_generated_file)
        .expect("env file should be readable");

    // The requester's own keys still emit (binary + dir entries).
    assert!(
        content.lines().any(|line| line.starts_with("MEDIAPM_YT_DLP_LINUX=")),
        "own binary var missing:\n{content}"
    );
    assert!(
        content.lines().any(|line| line.starts_with("MEDIAPM_YT_DLP_LINUX_DIR=")),
        "own dir var missing:\n{content}"
    );

    // Inlined deps keys never leak: no `deps/` path segments, no `_DEPS_*`
    // or `_COMPANIONS_*` var names.
    for line in content.lines() {
        assert!(!line.contains("deps/"), "env must not reference deps/ paths: {line}\n{content}");
        assert!(
            !line.contains("DEPS") && !line.contains("COMPANIONS"),
            "env must not leak companion vars: {line}\n{content}"
        );
    }
    Ok(())
}

/// `logical_tool_requires_sync` stays stable when a dep's stored
/// `canonical_version` is itself composite (the state a sync leaves when the
/// dep had same-step deps of its own).
///
/// Guards the wiring through `service.rs`: composite segments reference each
/// dep's OWN version segment (`own_version_segment`), so a composite-bearing
/// dep entry never changes the requester's computed composite and never
/// triggers a false reprovision. The requester is yt-dlp because its `deno`
/// dependency carries the `SameStep` role, which is what puts a dep into the
/// composite at all; a `CrossStep` dep is excluded from it and would make this
/// assertion hold whether or not the own-segment rule worked.
#[tokio::test]
async fn sync_composite_non_transitive() -> Result<(), mediapm::MediaPmError> {
    let (_mock, _seams) = mock_all_managed_tools().await;
    let (service, _root, _cache_root) = yt_dlp_with_deno_service().await?;

    let mut state = MediaPmState::default();
    // deno stored with a COMPOSITE canonical_version — as if it had been
    // network-synced with a same-step dep of its own in a prior pass.
    state.managed_tools.push(ToolRegistryEntry {
        tool_id: "deno".to_string(),
        version: String::new(),
        canonical_version: format!("{STUB_DENO_COMMIT_SHA};other-tool:deadbeef"),
        content_map_hash: "blake3:abc".to_string(),
        deployed_at: mediapm_utils::Timestamp::default(),
        resolved_tag: None,
        resolved_version: None,
        resolved_vcs_hash: None,
    });
    state.managed_tools.push(ToolRegistryEntry {
        tool_id: "yt-dlp".to_string(),
        version: String::new(),
        canonical_version: format!("{STUB_COMMIT_SHA};deno:{STUB_DENO_COMMIT_SHA}"),
        content_map_hash: "blake3:abc".to_string(),
        deployed_at: mediapm_utils::Timestamp::default(),
        resolved_tag: None,
        resolved_version: None,
        resolved_vcs_hash: None,
    });

    assert!(
        !service.logical_tool_requires_sync("yt-dlp", &state).await?,
        "a composite-bearing dep entry must contribute its own version segment only"
    );
    Ok(())
}

/// Builds a service whose `yt-dlp` requirement carries `deno` as a same-step
/// dependency, over a fresh workspace and a hermetic download cache.
///
/// yt-dlp is the requester whose payload is inlined, and deno is the dep whose
/// payload is inlined into it. Both are pure downloads, so against
/// [`mock_all_managed_tools`] the pair exercises the download, extraction and
/// content-import stages end to end. The caller installs the mock before
/// syncing, and keeps the returned workspace and cache roots alive for as long
/// as the service reads them.
async fn yt_dlp_with_deno_service() -> Result<
    (MediaPmService<mediapm_cas::FileSystemCas>, tempfile::TempDir, tempfile::TempDir),
    mediapm::MediaPmError,
> {
    let mut runtime = MediaRuntimeStorage::default();
    runtime.tools.insert(
        "yt-dlp".to_string(),
        ToolRequirement {
            dependencies: std::collections::BTreeMap::from([(
                "deno".to_string(),
                mediapm::ConfigVersionSpec::Latest,
            )]),
            ..Default::default()
        },
    );
    service_with_cache(runtime).await
}

/// Fresh sync of the provisioning path: yt-dlp (requester) with a same-step
/// dep (deno), both provisioned from the mocked upstream. Asserts the
/// generated-doc runtime carries the dep's payload inlined under
/// `deps/deno/<key>` with values mirroring the dep's own content map exactly,
/// no recursion into `deps/deno/deps/`, no `companions/` residue, and a
/// composite `canonical_version` in state.
#[tokio::test]
async fn sync_inlines_same_step_deps_into_content_map() -> Result<(), mediapm::MediaPmError> {
    let (_mock, _seams) = mock_all_managed_tools().await;
    let (mut service, _root, _cache_root) = yt_dlp_with_deno_service().await?;
    service
        .sync_tools_with_progress_overrides(false, false, test_sync_progress_overrides())
        .await?;

    // Generated doc: the requester's ACTIVE spec (name match + non-empty
    // content map — pruned stale keys keep the name with a cleared map).
    let doc = read_generated_doc(&service);
    let yt_dlp_spec = doc
        .tools
        .values()
        .find(|s| s.name == "yt-dlp" && !s.runtime.content_map.is_empty())
        .expect("active yt-dlp spec");
    let deno_spec = doc
        .tools
        .values()
        .find(|s| s.name == "deno" && !s.runtime.content_map.is_empty())
        .expect("active deno spec");

    // deno's own map: exactly the three `{os}/` dir keys.
    assert_eq!(
        deno_spec.runtime.content_map.len(),
        3,
        "deno own map must have exactly 3 OS entries"
    );
    for os in ["windows", "macos", "linux"] {
        let key = format!("{os}/");
        assert!(deno_spec.runtime.content_map.contains_key(&key), "deno missing own key {key}");
        // The requester inlines the dep payload under `deps/deno/{os}/` with
        // values mirroring the dep's own content map exactly.
        let inlined_key = format!("deps/deno/{os}/");
        assert_eq!(
            yt_dlp_spec.runtime.content_map.get(&inlined_key),
            deno_spec.runtime.content_map.get(&key),
            "inlined {inlined_key} must mirror deno's own {key}"
        );
    }
    // The requester keeps its own binary keys.
    for (filename, os) in
        [("yt-dlp.exe", "windows"), ("yt-dlp_macos", "macos"), ("yt-dlp_linux", "linux")]
    {
        let key = format!("{os}/{filename}");
        assert!(yt_dlp_spec.runtime.content_map.contains_key(&key), "yt-dlp missing own key {key}");
    }
    // Non-transitive: no `deps/deno/deps/` recursion; no `companions/`.
    for key in yt_dlp_spec.runtime.content_map.keys() {
        assert!(!key.starts_with("deps/deno/deps/"), "inlining must not recurse: {key}");
        assert!(!key.contains("companions/"), "no companions residue: {key}");
    }

    // State: yt-dlp carries the composite canonical version referencing the
    // dep's OWN version segment; deno carries its bare hash.
    let state = read_state(&service);
    let expected_composite = format!("{STUB_COMMIT_SHA};deno:{STUB_DENO_COMMIT_SHA}");
    assert!(
        state
            .managed_tools
            .iter()
            .any(|e| e.tool_id == "yt-dlp" && e.canonical_version == expected_composite),
        "yt-dlp must carry composite canonical {expected_composite}"
    );
    assert!(
        state
            .managed_tools
            .iter()
            .any(|e| e.tool_id == "deno" && e.canonical_version == STUB_DENO_COMMIT_SHA),
        "deno must carry bare canonical {STUB_DENO_COMMIT_SHA}"
    );
    Ok(())
}

/// Dep-version-change reprovision: a workspace whose recorded composite names
/// a deno version upstream no longer serves reprovisions the requester instead
/// of skipping it.
///
/// The composite skip is gated on three conditions in `sync/mod.rs`: the
/// recorded composite must match the freshly computed one, an active
/// generated-doc spec must exist for the tool, and that spec's content map
/// must be available in the workspace CAS. The fixture satisfies the last two
/// and fails the first, so the skip is reachable and is rejected on the
/// composite alone — which is the property under test. Without the seeded
/// document the skip could never fire, and the assertions below would hold for
/// any fresh sync, so the seeded document is part of the fixture rather than
/// decoration.
///
/// The seeded state:
/// - yt-dlp's entry carries the OLD composite
///   `{STUB_COMMIT_SHA};deno:{STALE_DENO_VERSION}` with a non-empty
///   placeholder hash, so `should_skip` is false.
/// - deno's entry carries an EMPTY `content_map_hash` and is inert in this
///   path: a sync takes a dependency's version from the pool's published
///   `gates.dep_version` (`provision_entry` in `sync/mod.rs`), not from
///   persisted state, so the seeded entry contributes no segment to the
///   requester's computed composite. `dep_segments_from_live_state` reads
///   persisted state the same way, but only from
///   `logical_tool_requires_sync`, which this test does not reach.
///
/// Asserts yt-dlp re-provisions: nothing is reported skipped, the active spec
/// carries freshly downloaded payload hashes rather than the seeded
/// placeholders, the inlined dep payloads mirror deno's own map, and the
/// recorded composite is the new one.
#[expect(
    clippy::too_many_lines,
    reason = "the fixture is the point here: a reachable-but-rejected composite skip needs a seeded generated document, a seeded state entry for the requester and for its dep, and the assertions that together tell a re-provision apart from a skip"
)]
#[tokio::test]
async fn sync_dep_version_change_reprovisions_requester() -> Result<(), mediapm::MediaPmError> {
    let (_mock, _seams) = mock_all_managed_tools().await;
    let (mut service, _root, _cache_root) = yt_dlp_with_deno_service().await?;

    // A deno commit hash no upstream serves, standing in for the version a
    // previous sync recorded before the release moved.
    let stale_deno_version = "9999999999999999999999999999999999999999";

    // Seed an active yt-dlp spec whose content map is non-empty, so the
    // composite skip is reachable. The values are non-hash placeholders, which
    // `workspace_content_map_is_available` ignores.
    let mut doc = NickelDocument::default();
    doc.tools.insert(
        "yt-dlp@blake3:stale".to_string(),
        ToolSpec {
            version: None,
            name: "yt-dlp".to_string(),
            kind: ToolKindSpec::default(),
            runtime: ToolRuntime {
                content_map: std::collections::BTreeMap::from([
                    ("linux/yt-dlp".to_string(), "provisioned".to_string()),
                    ("deps/deno/linux/".to_string(), "provisioned".to_string()),
                ]),
                payload_staging: PayloadStaging::Copy,
                ..Default::default()
            },
            ..Default::default()
        },
    );
    let generated_path = service.paths().conductor_generated_ncl.clone();
    std::fs::write(&generated_path, encode_document(doc).expect("seeded doc encodes"))
        .expect("write seeded generated doc");

    // Seed the PREVIOUS deployment records, so the recorded composite is the
    // stale one the skip check compares against.
    let mut state = MediaPmState::default();
    state.managed_tools.push(ToolRegistryEntry {
        tool_id: "deno".to_string(),
        version: String::new(),
        canonical_version: stale_deno_version.to_string(),
        content_map_hash: String::new(),
        deployed_at: mediapm_utils::Timestamp::default(),
        resolved_tag: None,
        resolved_version: None,
        resolved_vcs_hash: None,
    });
    state.managed_tools.push(ToolRegistryEntry {
        tool_id: "yt-dlp".to_string(),
        version: String::new(),
        canonical_version: format!("{STUB_COMMIT_SHA};deno:{stale_deno_version}"),
        content_map_hash: "blake3:old".to_string(),
        deployed_at: mediapm_utils::Timestamp::default(),
        resolved_tag: None,
        resolved_version: None,
        resolved_vcs_hash: None,
    });
    let state_path = service.paths().mediapm_state_json.clone();
    std::fs::create_dir_all(state_path.parent().expect("state parent dir"))
        .expect("create state dir");
    std::fs::write(&state_path, serde_json::to_vec(&state).expect("state serializes"))
        .expect("write seeded state");

    let summary = service
        .sync_tools_with_progress_overrides(false, false, test_sync_progress_overrides())
        .await?;

    // The skip was reachable and the composite was stale, so the entry had to
    // provision. A skip here would mean the composite check accepted the old
    // version.
    assert_eq!(
        summary.skipped_tools, 0,
        "a stale composite must not skip when an active spec and its content map are present",
    );

    // Generated doc: yt-dlp's ACTIVE spec carries freshly inlined deno
    // payloads; deno's own map reflects the NEW zip contents.
    let doc = read_generated_doc(&service);
    let yt_dlp_spec = doc
        .tools
        .values()
        .find(|s| s.name == "yt-dlp" && !s.runtime.content_map.is_empty())
        .expect("active yt-dlp spec");
    let deno_spec = doc
        .tools
        .values()
        .find(|s| s.name == "deno" && !s.runtime.content_map.is_empty())
        .expect("active deno spec");

    // The seeded placeholders are gone: the spec was rebuilt from the
    // downloaded payloads rather than kept from the seeded document.
    assert!(
        yt_dlp_spec.runtime.content_map.values().all(|v| v != "provisioned"),
        "the seeded placeholder content map must be replaced by provisioned hashes: {:?}",
        yt_dlp_spec.runtime.content_map,
    );

    // yt-dlp's own keys are UNCHANGED: the requester's payload is the same
    // fixture bytes as before the dep bump, so the content addresses match.
    // The expectation is derived from the same builder the mock mounts.
    for (asset_name, os) in
        [("yt-dlp.exe", "windows"), ("yt-dlp_macos", "macos"), ("yt-dlp_linux", "linux")]
    {
        let key = format!("{os}/{asset_name}");
        let expected_hash =
            blake3::hash(&binary_asset(asset_name, "yt-dlp", os).bytes).to_hex().to_string();
        assert_eq!(
            yt_dlp_spec.runtime.content_map.get(&key),
            Some(&expected_hash),
            "yt-dlp own key {key} must be byte-stable after dep bump"
        );
    }
    // Inlined dep payloads mirror deno's NEW own map.
    for os in ["windows", "macos", "linux"] {
        let key = format!("{os}/");
        let inlined_key = format!("deps/deno/{os}/");
        assert_eq!(
            yt_dlp_spec.runtime.content_map.get(&inlined_key),
            deno_spec.runtime.content_map.get(&key),
            "inlined {inlined_key} must mirror deno's new own {key}"
        );
    }
    // Non-transitive: no recursion; no companions residue.
    for key in yt_dlp_spec.runtime.content_map.keys() {
        assert!(!key.starts_with("deps/deno/deps/"), "inlining must not recurse: {key}");
        assert!(!key.contains("companions/"), "no companions residue: {key}");
    }

    // State: yt-dlp reprovisioned with the NEW composite; deno carries the
    // NEW bare hash.
    let state = read_state(&service);
    let expected_composite = format!("{STUB_COMMIT_SHA};deno:{STUB_DENO_COMMIT_SHA}");
    assert!(
        state
            .managed_tools
            .iter()
            .any(|e| e.tool_id == "yt-dlp" && e.canonical_version == expected_composite),
        "yt-dlp must reprovision to composite canonical {expected_composite}"
    );
    assert!(
        state
            .managed_tools
            .iter()
            .any(|e| e.tool_id == "deno" && e.canonical_version == STUB_DENO_COMMIT_SHA),
        "deno must reprovision to bare canonical {STUB_DENO_COMMIT_SHA}"
    );
    Ok(())
}

/// Integration test proving level-0 (dep) entries are applied before
/// level-1 (requester) entries in the parallel provisioning driver.
///
/// The level ordering is load-bearing: level-1 inlining reads
/// `provisioned_own_maps` populated by level-0 apply. If level-0
/// were not applied first, the requester's content map would lack
/// `deps/deno/...` keys. This test makes the ordering property
/// explicit (rather than inferred from a single key's presence) by
/// asserting:
///   1. The dep's own content map has zero `deps/` keys (non-transitive).
///   2. Every dep key appears as a `deps/<dep_id>/` subset in the requester.
///   3. Exactly one state entry per distinct tool id.
#[tokio::test]
async fn sync_level_ordering_applies_deps_before_requesters() -> Result<(), mediapm::MediaPmError> {
    let (_mock, _seams) = mock_all_managed_tools().await;
    let (mut service, _root, _cache_root) = yt_dlp_with_deno_service().await?;
    service
        .sync_tools_with_progress_overrides(false, false, test_sync_progress_overrides())
        .await?;

    let doc = read_generated_doc(&service);
    let deno_spec = doc
        .tools
        .values()
        .find(|s| s.name == "deno" && !s.runtime.content_map.is_empty())
        .expect("active deno spec");
    let yt_dlp_spec = doc
        .tools
        .values()
        .find(|s| s.name == "yt-dlp" && !s.runtime.content_map.is_empty())
        .expect("active yt-dlp spec");

    // Property 1: dep's own content map has no deps/ keys (non-transitive).
    for key in deno_spec.runtime.content_map.keys() {
        assert!(
            !key.starts_with("deps/"),
            "dep entry must not carry deps/ keys (non-transitive): {key}"
        );
    }

    // Property 2: every dep key appears as a deps/<dep_id>/ subset in the requester.
    for (key, hash) in &deno_spec.runtime.content_map {
        let inlined_key = format!("deps/deno/{key}");
        assert_eq!(
            yt_dlp_spec.runtime.content_map.get(&inlined_key),
            Some(hash),
            "requester must inline dep key {key} as {inlined_key}"
        );
    }

    // Property 3: exactly one state entry per distinct tool id.
    let state = read_state(&service);
    let mut tool_ids: Vec<_> = state.managed_tools.iter().map(|e| e.tool_id.as_str()).collect();
    tool_ids.sort_unstable();
    tool_ids.dedup();
    assert_eq!(
        tool_ids.len(),
        state.managed_tools.len(),
        "each tool id must appear exactly once in managed_tools, got {tool_ids:?}"
    );

    // The two tool ids are yt-dlp and deno.
    assert!(tool_ids.contains(&"yt-dlp"), "yt-dlp must be in managed_tools");
    assert!(tool_ids.contains(&"deno"), "deno must be in managed_tools");

    Ok(())
}

/// Determinism test for the parallel provisioning driver: two independent
/// syncs of the same fixture produce byte-identical generated docs and
/// identical summary fields.
#[tokio::test]
async fn sync_parallel_driver_is_deterministic() -> Result<(), mediapm::MediaPmError> {
    // One mock serves both runs: the guard holds the process environment lock,
    // which is not reentrant, so a second mock in one test would deadlock.
    let (_mock, _seams) = mock_all_managed_tools().await;

    // Run 1
    let (mut svc1, _root1, _cache1) = yt_dlp_with_deno_service().await?;
    let summary1 = svc1
        .sync_tools_with_progress_overrides(false, false, test_sync_progress_overrides())
        .await?;
    let doc1_bytes = std::fs::read(svc1.paths().conductor_generated_ncl.clone())
        .expect("generated doc readable");

    // Run 2 (separate workspace, separate cache)
    let (mut svc2, _root2, _cache2) = yt_dlp_with_deno_service().await?;
    let summary2 = svc2
        .sync_tools_with_progress_overrides(false, false, test_sync_progress_overrides())
        .await?;
    let doc2_bytes = std::fs::read(svc2.paths().conductor_generated_ncl.clone())
        .expect("generated doc readable");

    // Summary fields must match.
    assert_eq!(summary1.added_tools, summary2.added_tools, "added_tools differ");
    assert_eq!(summary1.updated_tools, summary2.updated_tools, "updated_tools differ");
    assert_eq!(summary1.pruned_tools, summary2.pruned_tools, "pruned_tools differ");
    assert_eq!(summary1.removed_tools, summary2.removed_tools, "removed_tools differ");
    assert_eq!(summary1.warnings, summary2.warnings, "warnings differ");

    // Generated doc bytes must be identical.
    assert_eq!(doc1_bytes, doc2_bytes, "generated doc bytes differ between runs");

    Ok(())
}
