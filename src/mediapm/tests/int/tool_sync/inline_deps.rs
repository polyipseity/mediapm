use super::seeded_three_tool_skip_service;
use crate::common::{make_zip, service_with_cache};
use mediapm::{
    MediaPmService, MediaPmState, MediaRuntimeStorage, ToolRegistryEntry, ToolRequirement,
};
use mediapm_conductor::cache::{Cache, CacheDomainConfig, ENTRY_TTL_SECONDS};
use mediapm_conductor::{NickelDocument, decode_document};

// ---------------------------------------------------------------------------
// Same-step dependency payload inlining (deps/<mediapm tool id>/)
// ---------------------------------------------------------------------------
//
// The inlining itself (`inline_same_step_deps`) is covered by the unit tests
// in `conductor_bridge/sync/mod.rs`. The integration tests below are hermetic
// (no network) and cover the observable pipeline contracts around it:
//
// - `sync_skip_preserves_inlined_deps` — a generated-doc runtime that already
//   carries `deps/` keys survives a spec-based skip intact; the reserved
//   prefix is never stripped, renamed, or re-inlined on the skip path.
// - `sync_env_has_no_deps_garbage` — inlined `deps/` keys never leak into
//   `.env.generated` (no `_DEPS` / `_DIR` / `_COMPANIONS_` lines).
// - `sync_composite_non_transitive` — a dep whose stored `canonical_version`
//   is itself composite (realistic post-network-sync state) never triggers a
//   false reprovision of the requester; composite segments reference each
//   dep's OWN version segment only (Phase 1 wiring through `service.rs`).
//
// - `sync_inlines_same_step_deps_into_content_map` — a fresh sync of a
//   requester (yt-dlp) with a same-step dep (deno) inlines the dep's payload
//   under `deps/deno/<key>` into the generated-doc runtime, mirroring the
//   dep's own content-map values exactly, with no recursion and no
//   `companions/` residue.
// - `sync_dep_version_change_reprovisions_requester` — bumping a dep's
//   resolved version changes the requester's composite `canonical_version`
//   and forces a reprovision with freshly inlined payloads, while the
//   requester's own payload keys stay byte-identical.
//
// The provisioning-path tests above are hermetic via download-cache
// pre-seeding (metadata + payload domains), mirroring
// `fetch_and_import_ytdlp_full_pipeline` in
// `conductor_bridge/sync/provision.rs`. yt-dlp + deno are the only
// network-resolved managed tools whose payloads are pure downloads with
// tooling mediapm already has (`zip`); ffmpeg is excluded because its linux
// payload is tar.xz (mediapm has no tar/xz decoder). One tolerated network
// touch remains: `prefetch_expected_sizes` fires tolerant HEAD probes at the
// rewritten GitHub URLs before fetch; failures are silently ignored, so
// offline runs only pay a fast DNS failure.

/// Hermetic: seeds a generated doc whose `yt-dlp` runtime already carries
/// inlined `deps/ffmpeg/...` and `deps/deno/...` keys (as a previous network
/// sync would have produced) plus matching state entries, then runs sync with
/// exact version specs so all three tools spec-skip (no network).
///
/// Asserts the inlined structure survives the skip path: the generated doc
/// keeps the `deps/` keys alongside the requester's own keys, the state entry
/// keeps its non-transitive composite `canonical_version`, and no
/// `companions/` prefix ever appears.
#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn sync_skip_preserves_inlined_deps() -> Result<(), mediapm::MediaPmError> {
    let (mut service, _root, _cache_root) = seeded_three_tool_skip_service().await?;

    service.sync_tools().await?;

    // The exact specs match the seeded resolved versions → all three tools
    // spec-skip; the inlined structure must survive intact.
    let doc_bytes =
        std::fs::read(&service.paths().conductor_generated_ncl).expect("generated doc readable");
    let doc: NickelDocument = decode_document(&doc_bytes).expect("valid Nickel document");
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
#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn sync_env_has_no_deps_garbage() -> Result<(), mediapm::MediaPmError> {
    let (mut service, _root, _cache_root) = seeded_three_tool_skip_service().await?;

    service.sync_tools().await?;

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
/// `canonical_version` is itself composite (realistic state after a network
/// sync in which the dep had its own same-step deps).
///
/// Guards the Phase 1 wiring through `service.rs`: composite segments
/// reference each dep's OWN version segment (`own_version_segment`), so a
/// composite-bearing dep entry never changes the requester's computed
/// composite and never triggers a false reprovision. (The same-step
/// version-segment math itself is unit-tested in
/// `compute_composite_canonical_version_non_transitive`; no same-step tool
/// resolves without network, so the integration check uses media-tagger's
/// `CrossStep` dep to assert composite-dep tolerance.)
#[tokio::test]
async fn sync_composite_non_transitive() -> Result<(), mediapm::MediaPmError> {
    let mut runtime = MediaRuntimeStorage::default();
    runtime.tools.insert(
        "media-tagger".to_string(),
        ToolRequirement {
            dependencies: std::collections::BTreeMap::from([(
                "ffmpeg".to_string(),
                mediapm::ConfigVersionSpec::Latest,
            )]),
            ..Default::default()
        },
    );
    let (service, _root, _cache_root) = service_with_cache(runtime).await?;

    let mut state = MediaPmState::default();
    // ffmpeg stored with a COMPOSITE canonical_version — as if it had been
    // network-synced with its own same-step dep (`deno`) in a prior pass.
    state.managed_tools.push(ToolRegistryEntry {
        tool_id: "ffmpeg".to_string(),
        version: String::new(),
        canonical_version: "ffmpeg-v7.1;deno:deno-v1.46.0".to_string(),
        content_map_hash: "blake3:abc".to_string(),
        deployed_at: mediapm_utils::Timestamp::default(),
        resolved_tag: None,
        resolved_version: None,
        resolved_vcs_hash: None,
    });
    state.managed_tools.push(ToolRegistryEntry {
        tool_id: "media-tagger".to_string(),
        version: String::new(),
        canonical_version: mediapm::MEDIAPM_GIT_HASH.to_string(),
        content_map_hash: "blake3:abc".to_string(),
        deployed_at: mediapm_utils::Timestamp::default(),
        resolved_tag: None,
        resolved_version: None,
        resolved_vcs_hash: None,
    });

    assert!(
        !service.logical_tool_requires_sync("media-tagger", &state).await?,
        "composite-bearing dep entries must not trigger reprovision of the requester"
    );
    Ok(())
}

/// Opens a user-level download cache with the two domains the sync pipeline
/// uses (`tools` payload domain + `tool_metadata` tag domain), mirroring the
/// `CacheDomainConfig`s from `reconcile_desired_tools`.
async fn open_test_cache(root: &std::path::Path) -> Cache {
    Cache::open(
        root,
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
    .expect("test cache opens")
}

/// Hermetic fresh-sync of the provisioning path: yt-dlp (requester) with a
/// same-step dep (deno) is provisioned entirely from a pre-seeded
/// user-level download cache (no network). Asserts the generated-doc
/// runtime carries the dep's payload inlined under `deps/deno/<key>` with
/// values mirroring the dep's own content map exactly, no recursion into
/// `deps/deno/deps/`, no `companions/` residue, and a composite
/// `canonical_version` in state.
///
/// Hermeticity recipe (mirrors `fetch_and_import_ytdlp_full_pipeline` in
/// `conductor_bridge/sync/provision.rs`): pre-seed the `tool_metadata`
/// domain with `"{tag}\n{hash}"` entries (tag resolution never touches the
/// GitHub API), pre-seed the `tools` domain under the REWRITTEN download
/// URLs (`/download/{tag}/...` — `resolve_tool_fetch` substitutes the
/// `latest/download/` placeholder), then drop the cache handle BEFORE sync
/// (two open `Cache` handles at the same root contend for the directory
/// lock). ffmpeg is excluded because its linux payload is tar.xz (mediapm
/// has no tar/xz decoder); deno payloads are zips built with the `zip`
/// crate. The only tolerated network touch is `prefetch_expected_sizes`
/// firing tolerant HEAD probes at the rewritten URLs; failures are silently
/// ignored.
#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn sync_inlines_same_step_deps_into_content_map() -> Result<(), mediapm::MediaPmError> {
    use std::collections::BTreeMap;

    let yt_dlp_tag = "2025.07.15";
    let yt_dlp_hash = "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0";
    let deno_tag = "1.46.0";
    let deno_hash = "b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1";
    let yt_dlp_payloads = [
        ("yt-dlp.exe", &b"fake yt-dlp windows binary"[..]),
        ("yt-dlp_macos", &b"fake yt-dlp macos binary"[..]),
        ("yt-dlp_linux", &b"fake yt-dlp linux binary"[..]),
    ];
    let deno_zip_payloads = [
        ("windows", &b"fake deno 1.46.0 windows binary"[..]),
        ("macos", &b"fake deno 1.46.0 macos binary"[..]),
        ("linux", &b"fake deno 1.46.0 linux binary"[..]),
    ];

    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    let cache_root = mediapm_utils::temp::cache_dir().expect("cache tempdir");
    let cache = open_test_cache(cache_root.path()).await;
    // Metadata cache: tag resolution is served from cache (no GitHub API).
    let yt_dlp_metadata_key = "https://api.github.com/repos/yt-dlp/yt-dlp/releases/latest";
    cache
        .store_bytes(
            "tool_metadata",
            yt_dlp_metadata_key,
            format!("{yt_dlp_tag}\n{yt_dlp_hash}").as_bytes(),
        )
        .await;
    let deno_metadata_key = "https://api.github.com/repos/denoland/deno/releases/latest";
    cache
        .store_bytes(
            "tool_metadata",
            deno_metadata_key,
            format!("{deno_tag}\n{deno_hash}").as_bytes(),
        )
        .await;
    // Payload cache: REWRITTEN download URLs (`/download/{tag}/`).
    for (filename, payload) in &yt_dlp_payloads {
        let url =
            format!("https://github.com/yt-dlp/yt-dlp/releases/download/{yt_dlp_tag}/{filename}");
        cache.store_bytes("tools", &url, payload).await;
    }
    for (os, content) in &deno_zip_payloads {
        let zip_name = match *os {
            "windows" => "deno-x86_64-pc-windows-msvc.zip",
            "macos" => "deno-aarch64-apple-darwin.zip",
            _ => "deno-aarch64-unknown-linux-gnu.zip",
        };
        let zip_bytes = make_zip(&[("deno", content)]);
        let url =
            format!("https://github.com/denoland/deno/releases/download/{deno_tag}/{zip_name}");
        cache.store_bytes("tools", &url, &zip_bytes).await;
    }
    // Drop BEFORE sync: a second open `Cache` at the same root would contend
    // for the directory lock.
    drop(cache);

    let mut runtime = MediaRuntimeStorage {
        cache_root_override: Some(cache_root.path().to_path_buf()),
        ..MediaRuntimeStorage::default()
    };
    runtime.tools.insert(
        "yt-dlp".to_string(),
        ToolRequirement {
            version_spec: mediapm::ConfigVersionSpec::Latest,
            dependencies: BTreeMap::from([(
                "deno".to_string(),
                mediapm::ConfigVersionSpec::Latest,
            )]),
            ..Default::default()
        },
    );
    let mut service =
        MediaPmService::new_fs_at_with_runtime_storage_overrides(root.path(), runtime).await?;
    service.sync_tools().await?;

    // Generated doc: the requester's ACTIVE spec (name match + non-empty
    // content map — pruned stale keys keep the name with a cleared map).
    let doc_bytes =
        std::fs::read(&service.paths().conductor_generated_ncl).expect("generated doc readable");
    let doc: NickelDocument = decode_document(&doc_bytes).expect("valid Nickel document");
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
    for (filename, _) in &yt_dlp_payloads {
        let os = match *filename {
            "yt-dlp.exe" => "windows",
            "yt-dlp_macos" => "macos",
            _ => "linux",
        };
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
    let state_bytes = std::fs::read(&service.paths().mediapm_state_json).expect("state readable");
    let state: MediaPmState = serde_json::from_slice(&state_bytes).expect("valid state json");
    let expected_composite = format!("{yt_dlp_hash};deno:{deno_hash}");
    assert!(
        state
            .managed_tools
            .iter()
            .any(|e| e.tool_id == "yt-dlp" && e.canonical_version == expected_composite),
        "yt-dlp must carry composite canonical {expected_composite}"
    );
    assert!(
        state.managed_tools.iter().any(|e| e.tool_id == "deno" && e.canonical_version == deno_hash),
        "deno must carry bare canonical {deno_hash}"
    );
    Ok(())
}

/// Hermetic dep-version-change reprovision: bumps deno's resolved tag/hash
/// (1.46.0 → 1.47.0) while yt-dlp's payload stays identical, then syncs a
/// workspace whose `state.json` was seeded with the PREVIOUS deployment
/// records. Asserts yt-dlp reprovisions (composite canonical becomes
/// `{yt_dlp_hash};deno:{new_deno_hash}`) with freshly inlined deno payloads,
/// while its own binary keys keep byte-identical hashes (same cached
/// payloads → same blake3 content addresses).
///
/// The seeded state drives the composite skip check:
/// - deno's entry carries an EMPTY `content_map_hash`. The composite
///   computation (`compute_composite_canonical_version`) matches only
///   non-empty-hash entries via `find()`, so the stale deno record is
///   ignored — this dodges the find-first hazard where an accumulated stale
///   entry with a non-empty hash would be matched first and suppress
///   yt-dlp's reprovision — and deno itself cannot skip either.
/// - yt-dlp's entry carries the OLD composite `{yt_dlp_hash};deno:{old_deno_hash}`
///   with a non-empty placeholder hash: the composite skip check compares it
///   against the freshly computed `{yt_dlp_hash};deno:{new_deno_hash}` and
///   falls through to reprovision.
#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn sync_dep_version_change_reprovisions_requester() -> Result<(), mediapm::MediaPmError> {
    use std::collections::BTreeMap;

    let yt_dlp_tag = "2025.07.15";
    let yt_dlp_hash = "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0";
    let old_deno_hash = "b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1";
    let new_deno_tag = "1.47.0";
    let new_deno_hash = "c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2";
    let yt_dlp_payloads = [
        ("yt-dlp.exe", &b"fake yt-dlp windows binary"[..]),
        ("yt-dlp_macos", &b"fake yt-dlp macos binary"[..]),
        ("yt-dlp_linux", &b"fake yt-dlp linux binary"[..]),
    ];
    let deno_zip_payloads = [
        ("windows", &b"fake deno 1.47.0 windows binary"[..]),
        ("macos", &b"fake deno 1.47.0 macos binary"[..]),
        ("linux", &b"fake deno 1.47.0 linux binary"[..]),
    ];

    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    let cache_root = mediapm_utils::temp::cache_dir().expect("cache tempdir");
    let cache = open_test_cache(cache_root.path()).await;
    // Metadata cache: deno resolves to the NEW tag/hash; yt-dlp stays put.
    let yt_dlp_metadata_key = "https://api.github.com/repos/yt-dlp/yt-dlp/releases/latest";
    cache
        .store_bytes(
            "tool_metadata",
            yt_dlp_metadata_key,
            format!("{yt_dlp_tag}\n{yt_dlp_hash}").as_bytes(),
        )
        .await;
    let deno_metadata_key = "https://api.github.com/repos/denoland/deno/releases/latest";
    cache
        .store_bytes(
            "tool_metadata",
            deno_metadata_key,
            format!("{new_deno_tag}\n{new_deno_hash}").as_bytes(),
        )
        .await;
    // Payload cache: REWRITTEN download URLs (`/download/{tag}/`).
    for (filename, payload) in &yt_dlp_payloads {
        let url =
            format!("https://github.com/yt-dlp/yt-dlp/releases/download/{yt_dlp_tag}/{filename}");
        cache.store_bytes("tools", &url, payload).await;
    }
    for (os, content) in &deno_zip_payloads {
        let zip_name = match *os {
            "windows" => "deno-x86_64-pc-windows-msvc.zip",
            "macos" => "deno-aarch64-apple-darwin.zip",
            _ => "deno-aarch64-unknown-linux-gnu.zip",
        };
        let zip_bytes = make_zip(&[("deno", content)]);
        let url =
            format!("https://github.com/denoland/deno/releases/download/{new_deno_tag}/{zip_name}");
        cache.store_bytes("tools", &url, &zip_bytes).await;
    }
    drop(cache);

    let mut runtime = MediaRuntimeStorage {
        cache_root_override: Some(cache_root.path().to_path_buf()),
        ..MediaRuntimeStorage::default()
    };
    runtime.tools.insert(
        "yt-dlp".to_string(),
        ToolRequirement {
            version_spec: mediapm::ConfigVersionSpec::Latest,
            dependencies: BTreeMap::from([(
                "deno".to_string(),
                mediapm::ConfigVersionSpec::Latest,
            )]),
            ..Default::default()
        },
    );
    let mut service =
        MediaPmService::new_fs_at_with_runtime_storage_overrides(root.path(), runtime).await?;

    // Seed the PREVIOUS deployment records (as a prior network sync would
    // have left them). The generated doc is left empty — this sync pass
    // rebuilds it from scratch.
    let mut state = MediaPmState::default();
    state.managed_tools.push(ToolRegistryEntry {
        tool_id: "deno".to_string(),
        version: String::new(),
        canonical_version: old_deno_hash.to_string(),
        content_map_hash: String::new(),
        deployed_at: mediapm_utils::Timestamp::default(),
        resolved_tag: None,
        resolved_version: None,
        resolved_vcs_hash: None,
    });
    state.managed_tools.push(ToolRegistryEntry {
        tool_id: "yt-dlp".to_string(),
        version: String::new(),
        canonical_version: format!("{yt_dlp_hash};deno:{old_deno_hash}"),
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

    service.sync_tools().await?;

    // Generated doc: yt-dlp's ACTIVE spec carries freshly inlined deno
    // payloads; deno's own map reflects the NEW zip contents.
    let doc_bytes =
        std::fs::read(&service.paths().conductor_generated_ncl).expect("generated doc readable");
    let doc: NickelDocument = decode_document(&doc_bytes).expect("valid Nickel document");
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

    // yt-dlp's own keys are UNCHANGED: identical cached payloads → identical
    // blake3 content addresses (binary format imports the raw bytes as-is).
    for (filename, payload) in &yt_dlp_payloads {
        let os = match *filename {
            "yt-dlp.exe" => "windows",
            "yt-dlp_macos" => "macos",
            _ => "linux",
        };
        let key = format!("{os}/{filename}");
        let expected_hash = blake3::hash(payload).to_hex().to_string();
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
    let state_bytes = std::fs::read(&service.paths().mediapm_state_json).expect("state readable");
    let state: MediaPmState = serde_json::from_slice(&state_bytes).expect("valid state json");
    let expected_composite = format!("{yt_dlp_hash};deno:{new_deno_hash}");
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
            .any(|e| e.tool_id == "deno" && e.canonical_version == new_deno_hash),
        "deno must reprovision to bare canonical {new_deno_hash}"
    );
    Ok(())
}
