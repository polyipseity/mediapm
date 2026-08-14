use mediapm::{MediaPmState, MediaRuntimeStorage, ToolRegistryEntry, ToolRequirement};
use mediapm_conductor::tools::provider::VersionSpecFields;
use mediapm_conductor::{NickelDocument, ToolKindSpec, ToolRuntime, ToolSpec, encode_document};

use crate::common::service_with_cache;

// ---------------------------------------------------------------------------
// Resolved-field population (resolved_tag / resolved_version / resolved_vcs_hash)
// ---------------------------------------------------------------------------
//
// These tests validate the Phase 3 wiring: the provider's resolved metadata
// is persisted into `state.json` on provision, backfilled in place for
// skipped tools, and matched by exact version specs. Media-tagger is used
// throughout because it resolves without network.

/// Sync persists provider-resolved fields into the managed-tool registry.
///
/// Uses media-tagger (builtin launcher, no network). Expected per the
/// provider matrix: `resolved_tag` stays `None` (no upstream tag —
/// why-empty invariant), `resolved_version` is the mediapm crate version,
/// and `resolved_vcs_hash` is the mediapm git hash.
#[tokio::test]
async fn sync_populates_resolved_fields_in_state() -> Result<(), mediapm::MediaPmError> {
    let mut runtime = MediaRuntimeStorage::default();
    runtime.tools.insert("media-tagger".to_string(), ToolRequirement::default());
    let (mut service, _root, _cache_root) = service_with_cache(runtime).await?;

    service.sync_tools().await?;

    let bytes = std::fs::read(&service.paths().mediapm_state_json).expect("state.json after sync");
    let state: MediaPmState =
        serde_json::from_slice(&bytes).expect("state.json should deserialize");
    let entry = state
        .managed_tools
        .iter()
        .find(|e| e.tool_id == "media-tagger")
        .expect("media-tagger entry should exist after sync");

    // WHY: media-tagger is a builtin launcher shipped inside mediapm; there
    // is no upstream tag that identifies the artifact set.
    assert_eq!(entry.resolved_tag, None, "media-tagger has no upstream tag");
    assert_eq!(
        entry.resolved_version.as_deref(),
        Some(env!("CARGO_PKG_VERSION")),
        "resolved_version should be the mediapm crate version"
    );
    assert_eq!(
        entry.resolved_vcs_hash.as_deref(),
        Some(mediapm::MEDIAPM_GIT_HASH),
        "resolved_vcs_hash should be the mediapm git hash"
    );
    assert_eq!(
        entry.canonical_version,
        mediapm::MEDIAPM_GIT_HASH,
        "canonical_version should equal MEDIAPM_GIT_HASH for builtin launcher"
    );
    Ok(())
}

/// Skipped tools get `None` resolved fields backfilled in place from fresh
/// provider metadata, while identity fields are preserved.
///
/// Seeds state.json with a media-tagger entry whose resolved fields are all
/// `None` plus a non-empty `content_map_hash` (so the skip check fires). After
/// re-sync the resolved fields are filled, but `content_map_hash`,
/// `deployed_at`, and `version` are untouched — proving no re-provision.
#[tokio::test]
async fn sync_skip_backfills_resolved_fields() -> Result<(), mediapm::MediaPmError> {
    let mut runtime = MediaRuntimeStorage::default();
    runtime.tools.insert("media-tagger".to_string(), ToolRequirement::default());
    let (mut service, _root, _cache_root) = service_with_cache(runtime).await?;

    // First sync provisions media-tagger, populates workspace CAS, and writes
    // conductor.generated.ncl so the skip path can verify content-map bytes.
    service.sync_tools().await?;

    let state_path = service.paths().mediapm_state_json.clone();
    let bytes = std::fs::read(&state_path).expect("state.json after provision");
    let provisioned_state: MediaPmState =
        serde_json::from_slice(&bytes).expect("state.json should deserialize");
    let provisioned = provisioned_state
        .managed_tools
        .iter()
        .find(|entry| entry.tool_id == "media-tagger")
        .expect("media-tagger entry should exist after provision");

    // Reset resolved fields while preserving identity fields that trigger skip.
    let mut state = MediaPmState::default();
    state.managed_tools.push(ToolRegistryEntry {
        tool_id: "media-tagger".to_string(),
        version: "seeded-version".to_string(),
        canonical_version: provisioned.canonical_version.clone(),
        content_map_hash: provisioned.content_map_hash.clone(),
        deployed_at: mediapm_utils::Timestamp::from_unix_secs(42),
        resolved_tag: None,
        resolved_version: None,
        resolved_vcs_hash: None,
    });
    let bytes = serde_json::to_vec(&state).expect("state serializes");
    std::fs::write(&state_path, bytes).expect("write seeded state");

    service.sync_tools().await?;

    let bytes = std::fs::read(&service.paths().mediapm_state_json).expect("state.json after sync");
    let state: MediaPmState =
        serde_json::from_slice(&bytes).expect("state.json should deserialize");
    let entry = state
        .managed_tools
        .iter()
        .find(|e| e.tool_id == "media-tagger")
        .expect("media-tagger entry should exist after sync");

    // Backfilled resolved fields (why-empty preserved for tag).
    assert_eq!(entry.resolved_tag, None, "why-empty tag must stay None");
    assert_eq!(
        entry.resolved_version.as_deref(),
        Some(env!("CARGO_PKG_VERSION")),
        "backfilled resolved_version should be the mediapm crate version"
    );
    assert_eq!(
        entry.resolved_vcs_hash.as_deref(),
        Some(mediapm::MEDIAPM_GIT_HASH),
        "backfilled resolved_vcs_hash should be the mediapm git hash"
    );

    // Identity fields preserved — skip path never re-provisions.
    assert_eq!(
        entry.content_map_hash, provisioned.content_map_hash,
        "content_map_hash must be preserved"
    );
    assert_eq!(
        entry.deployed_at,
        mediapm_utils::Timestamp::from_unix_secs(42),
        "deployed_at must be preserved"
    );
    assert_eq!(entry.version, "seeded-version", "version must be preserved");
    assert_eq!(
        entry.canonical_version,
        mediapm::MEDIAPM_GIT_HASH,
        "canonical_version must be preserved"
    );
    Ok(())
}

/// An exact version spec whose fields match stored resolved fields skips the
/// tool instead of re-provisioning (regression: `spec_matches_entry` with
/// `None` stored fields never matched, forcing re-provision).
///
/// Seeds a media-tagger entry with `Some` resolved fields matching an
/// `Exact { version, vcs_hash }` spec. Sync must skip: `added_tools == 0`,
/// and `deployed_at`/`version` stay at seeded values (no new record).
#[tokio::test]
async fn sync_exact_version_spec_skips_when_stored_fields_match()
-> Result<(), mediapm::MediaPmError> {
    let mut runtime = MediaRuntimeStorage::default();
    runtime.tools.insert(
        "media-tagger".to_string(),
        ToolRequirement {
            version_spec: mediapm::ConfigVersionSpec::Exact(VersionSpecFields {
                vcs_hash: Some(mediapm::MEDIAPM_GIT_HASH.to_string()),
                version: Some(env!("CARGO_PKG_VERSION").to_string()),
                tag: None,
            }),
            ..Default::default()
        },
    );
    let (mut service, _root, _cache_root) = service_with_cache(runtime).await?;

    // Seed state.json with matching resolved fields.
    let state_path = service.paths().mediapm_state_json.clone();
    std::fs::create_dir_all(state_path.parent().expect("state parent dir"))
        .expect("create state parent dir");
    let mut state = MediaPmState::default();
    state.managed_tools.push(ToolRegistryEntry {
        tool_id: "media-tagger".to_string(),
        version: "seeded-version".to_string(),
        canonical_version: mediapm::MEDIAPM_GIT_HASH.to_string(),
        content_map_hash: "blake3:abc".to_string(),
        deployed_at: mediapm_utils::Timestamp::from_unix_secs(42),
        resolved_tag: None,
        resolved_version: Some(env!("CARGO_PKG_VERSION").to_string()),
        resolved_vcs_hash: Some(mediapm::MEDIAPM_GIT_HASH.to_string()),
    });
    let bytes = serde_json::to_vec(&state).expect("state serializes");
    std::fs::write(&state_path, bytes).expect("write seeded state");

    // Seed the generated doc with a media-tagger entry so the workspace-CAS
    // skip path can find the active tool spec. Placeholder content-map
    // values (non-hash) pass the CAS availability check.
    let mut doc = NickelDocument::default();
    doc.tools.insert(
        "media-tagger@blake3:abc".to_string(),
        ToolSpec {
            name: "media-tagger".to_string(),
            kind: ToolKindSpec::default(),
            runtime: ToolRuntime {
                content_map: std::collections::BTreeMap::from([(
                    "macos/media-tagger".to_string(),
                    "provisioned".to_string(),
                )]),
                ..Default::default()
            },
            ..Default::default()
        },
    );
    let generated_path = service.paths().conductor_generated_ncl.clone();
    let bytes = encode_document(doc).expect("seeded doc encodes");
    std::fs::write(&generated_path, bytes).expect("write seeded generated doc");

    let summary = service.sync_tools().await?;

    assert_eq!(
        summary.added_tools, 0,
        "exact spec matching stored fields must skip, not re-provision"
    );
    let bytes = std::fs::read(&service.paths().mediapm_state_json).expect("state.json after sync");
    let state: MediaPmState =
        serde_json::from_slice(&bytes).expect("state.json should deserialize");
    let entry = state
        .managed_tools
        .iter()
        .find(|e| e.tool_id == "media-tagger")
        .expect("media-tagger entry should exist after sync");

    // No re-provision: identity and resolved fields unchanged.
    assert_eq!(
        entry.deployed_at,
        mediapm_utils::Timestamp::from_unix_secs(42),
        "deployed_at must be preserved on skip"
    );
    assert_eq!(entry.version, "seeded-version", "version must be preserved on skip");
    assert_eq!(
        entry.resolved_version.as_deref(),
        Some(env!("CARGO_PKG_VERSION")),
        "stored resolved_version must be preserved on skip"
    );
    assert_eq!(
        entry.resolved_vcs_hash.as_deref(),
        Some(mediapm::MEDIAPM_GIT_HASH),
        "stored resolved_vcs_hash must be preserved on skip"
    );
    Ok(())
}

/// Env payload paths in `.env.generated` are keyed by the **mediapm
/// conductor tool id** (the generated doc `tools` map key,
/// `{name}@{content_map_hash}`), matching the `ProvisionCache` deployment
/// layout (`tools_dir/<sanitize_tool_id(conductor_tool_id)>/payload/`).
/// The bare mediapm tool id (`ffmpeg`) must never appear as the path
/// segment.
///
/// Hermetic: seeds a matching state entry and a conductor-keyed generated
/// doc entry, then runs sync with an exact version spec so the spec-based
/// skip fires (no network). The skip path reconstructs the runtime from
/// the generated doc under its conductor key, and env generation must emit
/// paths under the sanitized conductor id.
#[tokio::test]
async fn sync_env_paths_use_conductor_tool_id() -> Result<(), mediapm::MediaPmError> {
    use std::collections::BTreeMap;

    let mut runtime = MediaRuntimeStorage::default();
    runtime.tools.insert(
        "ffmpeg".to_string(),
        ToolRequirement {
            version_spec: mediapm::ConfigVersionSpec::Exact(VersionSpecFields {
                version: Some("7.1".to_string()),
                vcs_hash: None,
                tag: None,
            }),
            ..Default::default()
        },
    );
    let (mut service, _root, _cache_root) = service_with_cache(runtime).await?;

    // Seed state.json with a matching ffmpeg entry (exact version "7.1").
    let state_path = service.paths().mediapm_state_json.clone();
    std::fs::create_dir_all(state_path.parent().expect("state parent dir"))
        .expect("create state parent dir");
    let mut state = MediaPmState::default();
    state.managed_tools.push(ToolRegistryEntry {
        tool_id: "ffmpeg".to_string(),
        version: "seeded-version".to_string(),
        canonical_version: "ffmpeg-v7.1".to_string(),
        content_map_hash: "blake3:abc123".to_string(),
        deployed_at: mediapm_utils::Timestamp::from_unix_secs(42),
        resolved_tag: None,
        resolved_version: Some("7.1".to_string()),
        resolved_vcs_hash: None,
    });
    std::fs::write(&state_path, serde_json::to_vec(&state).expect("state serializes"))
        .expect("write seeded state");

    // Seed the generated doc with a conductor-keyed ffmpeg entry. Content
    // map values are non-hash placeholders (external_data invariant skips
    // values that do not parse as `Hash`).
    let mut doc = NickelDocument::default();
    doc.tools.insert(
        "ffmpeg@blake3:abc123".to_string(),
        ToolSpec {
            name: "ffmpeg".to_string(),
            kind: ToolKindSpec::default(),
            runtime: ToolRuntime {
                content_map: BTreeMap::from([
                    ("linux/ffmpeg".to_string(), "provisioned".to_string()),
                    ("linux/".to_string(), "provisioned".to_string()),
                ]),
                ..Default::default()
            },
            ..Default::default()
        },
    );
    let generated_path = service.paths().conductor_generated_ncl.clone();
    let bytes = encode_document(doc).expect("seeded doc encodes");
    std::fs::write(&generated_path, bytes).expect("write seeded generated doc");

    service.sync_tools().await?;

    // The exact spec matches the seeded resolved version → skip fires.
    let content = std::fs::read_to_string(&service.paths().env_generated_file)
        .expect("env file should be readable");

    // Binary entry: payload path keyed by the sanitized conductor id.
    let binary_line = content
        .lines()
        .find(|line| line.starts_with("MEDIAPM_FFMPEG_LINUX="))
        .unwrap_or_else(|| panic!("missing MEDIAPM_FFMPEG_LINUX in env file:\n{content}"));
    assert!(
        binary_line.contains("/ffmpeg@blake3_abc123/payload/linux/ffmpeg"),
        "binary env path must use the sanitized conductor tool id: {binary_line}"
    );

    // Dir entry: payload dir path keyed by the sanitized conductor id.
    let dir_line = content
        .lines()
        .find(|line| line.starts_with("MEDIAPM_FFMPEG_LINUX_DIR="))
        .unwrap_or_else(|| panic!("missing MEDIAPM_FFMPEG_LINUX_DIR in env file:\n{content}"));
    assert!(
        dir_line.contains("/ffmpeg@blake3_abc123/payload/linux/"),
        "dir env path must use the sanitized conductor tool id: {dir_line}"
    );

    // Regression: the bare mediapm tool id must never be the path segment.
    assert!(
        !content.contains("/ffmpeg/payload/"),
        "env paths must not use the bare mediapm tool id:\n{content}"
    );

    // Env var names stay hash-free (plain mediapm id stem).
    assert!(
        content.lines().all(|line| {
            line.starts_with('#') || line.split('=').next().is_none_or(|name| !name.contains('@'))
        }),
        "env var names must not contain @:\n{content}"
    );
    Ok(())
}
