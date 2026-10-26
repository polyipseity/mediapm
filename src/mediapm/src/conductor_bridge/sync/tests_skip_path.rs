//! The skip path: the runtime it rebuilds and the registry record it writes.

use crate::config::ToolRequirement;
use crate::output::ProgressScreen;
use mediapm_conductor::tools::provider::VersionSpecFields;
use mediapm_conductor::{ConfigPayloadStaging, NickelDocument, ToolKindSpec, ToolSpec};
use std::collections::BTreeMap;

use super::*;

#[tokio::test]
async fn reconcile_desired_tools_skipped_tool_preserves_env_entries() {
    let tmp = mediapm_utils::temp::artifact_dir().unwrap();
    let cache_root = mediapm_utils::temp::cache_dir().unwrap();
    let paths = MediaPmPaths::from_root(tmp.path());
    // Pre-populate generated doc with a tool that has content_map entries.
    // The skip branch should reconstruct the runtime from this doc.
    let mut content_map = BTreeMap::new();
    content_map.insert("linux/media-tagger".to_string(), "blake3:abc123".to_string());
    content_map.insert("macos/media-tagger".to_string(), "blake3:def456".to_string());
    let tool_spec = ToolSpec {
        name: "media-tagger".to_string(),
        kind: ToolKindSpec::default(),
        runtime: ToolRuntime {
            content_map,
            payload_staging: ConfigPayloadStaging::Copy,
            ..Default::default()
        },
        ..Default::default()
    };
    let mut tools = BTreeMap::new();
    tools.insert("media-tagger".to_string(), tool_spec);
    let doc = NickelDocument { tools, ..Default::default() };
    save_conductor_generated_document(&paths, &doc).expect("pre-save generated doc");

    // State with matching canonical_version and content_map_hash → triggers skip.
    let mut state = MediaPmState::default();
    state.managed_tools.push(ToolRegistryEntry {
        tool_id: "media-tagger".to_string(),
        version: format!("{}+{}", env!("CARGO_PKG_VERSION"), crate::global::MEDIAPM_GIT_HASH),
        canonical_version: crate::global::MEDIAPM_GIT_HASH.to_string(),
        content_map_hash: "blake3:abc".to_string(),
        deployed_at: mediapm_utils::Timestamp::default(),
        resolved_tag: None,
        resolved_version: None,
        resolved_vcs_hash: None,
    });

    // Desired tools with media-tagger.
    let mut desired_tools = BTreeMap::new();
    let req = ToolRequirement::default();
    desired_tools.insert("media-tagger".to_string(), serde_json::to_value(req).unwrap());

    let workspace_cas = super::open_workspace_cas_store(&paths).await.expect("open workspace cas");
    let result = reconcile_desired_tools(
        workspace_cas,
        &paths,
        &desired_tools,
        &BTreeMap::new(),
        PayloadStaging::InPlace,
        RecheckPolicy::default(),
        &state,
        Some(cache_root.path()),
        &ProgressScreen::disabled(),
        None,
    )
    .await
    .map(ToolSyncOutcome::into_report);

    assert!(result.is_ok(), "reconcile_desired_tools failed: {:?}", result.err());

    // Verify env file has entries reconstructed from the generated doc.
    let env_path = &paths.env_generated_file;
    assert!(env_path.exists(), ".env.generated should exist");
    let content = std::fs::read_to_string(env_path).expect("env file readable");
    assert!(
        content.contains("MEDIAPM_MEDIA_TAGGER_LINUX"),
        "env file should have MEDIAPM_MEDIA_TAGGER_LINUX\n--- content:\n{content}",
    );
    assert!(
        content.contains("MEDIAPM_MEDIA_TAGGER_LINUX_DIR"),
        "env file should have MEDIAPM_MEDIA_TAGGER_LINUX_DIR\n--- content:\n{content}",
    );
    assert!(
        content.contains("MEDIAPM_MEDIA_TAGGER_MACOS"),
        "env file should have MEDIAPM_MEDIA_TAGGER_MACOS\n--- content:\n{content}",
    );
    assert!(
        content.contains("MEDIAPM_MEDIA_TAGGER_MACOS_DIR"),
        "env file should have MEDIAPM_MEDIA_TAGGER_MACOS_DIR\n--- content:\n{content}",
    );
    assert!(
        content.contains("/media-tagger/payload/"),
        "env file paths should contain /media-tagger/payload/\n--- content:\n{content}",
    );
}

/// The spec-based skip path reconstructs the runtime under its conductor
/// tool id (the generated doc key), so env payload paths match the
/// `ProvisionCache` deployment layout.
#[tokio::test]
async fn reconcile_keys_tool_runtimes_by_conductor_tool_id() {
    let tmp = mediapm_utils::temp::artifact_dir().unwrap();
    let cache_root = mediapm_utils::temp::cache_dir().unwrap();
    let paths = MediaPmPaths::from_root(tmp.path());
    // Pre-populate generated doc with an active `{name}@{hash}` key.
    let mut content_map = BTreeMap::new();
    content_map.insert("linux/yt-dlp".to_string(), "blake3:abc".to_string());
    // The record must carry the hash of the map the document actually holds:
    // the skip path refuses a record whose content map the active spec no
    // longer describes.
    let recorded_hash = content_map_hash(&content_map);
    let tool_spec = ToolSpec {
        name: "yt-dlp".to_string(),
        kind: ToolKindSpec::default(),
        runtime: ToolRuntime {
            content_map,
            payload_staging: ConfigPayloadStaging::Copy,
            ..Default::default()
        },
        ..Default::default()
    };
    let mut tools = BTreeMap::new();
    tools.insert("yt-dlp@blake3:abc".to_string(), tool_spec);
    let doc = NickelDocument { tools, ..Default::default() };
    save_conductor_generated_document(&paths, &doc).expect("pre-save generated doc");

    // State whose resolved version matches the exact spec → spec-based
    // skip fires without any network access.
    let mut state = MediaPmState::default();
    state.managed_tools.push(ToolRegistryEntry {
        tool_id: "yt-dlp".to_string(),
        version: "seeded-version".to_string(),
        canonical_version: "yt-dlp-2024.01.01".to_string(),
        content_map_hash: recorded_hash,
        deployed_at: mediapm_utils::Timestamp::default(),
        resolved_tag: None,
        resolved_version: Some("2024.01.01".to_string()),
        resolved_vcs_hash: None,
    });

    let mut desired_tools = BTreeMap::new();
    let req = ToolRequirement {
        version_spec: mediapm_conductor::tools::provider::ConfigVersionSpec::Exact(
            VersionSpecFields {
                version: Some("2024.01.01".to_string()),
                vcs_hash: None,
                tag: None,
            },
        ),
        ..Default::default()
    };
    desired_tools.insert("yt-dlp".to_string(), serde_json::to_value(req).unwrap());

    let workspace_cas = super::open_workspace_cas_store(&paths).await.expect("open workspace cas");
    let result = reconcile_desired_tools(
        workspace_cas,
        &paths,
        &desired_tools,
        &BTreeMap::new(),
        PayloadStaging::InPlace,
        RecheckPolicy::default(),
        &state,
        Some(cache_root.path()),
        &ProgressScreen::disabled(),
        None,
    )
    .await
    .map(ToolSyncOutcome::into_report);

    assert!(result.is_ok(), "reconcile_desired_tools failed: {:?}", result.err());
    assert_eq!(result.unwrap().tools_skipped, 1, "exact spec matching stored fields must skip");

    // Env paths must be keyed by the conductor tool id, not the plain id.
    let env_path = &paths.env_generated_file;
    let content = std::fs::read_to_string(env_path).expect("env file readable");
    assert!(
        content.contains("MEDIAPM_YT_DLP_LINUX="),
        "env file should have MEDIAPM_YT_DLP_LINUX\n--- content:\n{content}",
    );
    assert!(
        content.contains("/yt-dlp@blake3_abc/payload/linux/yt-dlp"),
        "env path must use the sanitized conductor tool id\n--- content:\n{content}",
    );
    assert!(
        !content.contains("/yt-dlp/payload/"),
        "env path must not use the plain mediapm tool id\n--- content:\n{content}",
    );
}

/// Regression: the skip path MUST register the skipped tool in
/// `report.tool_records` (and therefore in `state.managed_tools`), not
/// only push a `resolved_field_backfill`. A provisioned-but-unregistered
/// tool is illegal state — the post-sync warning check would otherwise
/// flag it as needing sync on every pass.
///
/// Uses `media-tagger`, whose provider resolves to `MEDIAPM_GIT_HASH`
/// without network access, so the skip path fires hermetically.
#[tokio::test]
async fn regression_skip_path_registers_managed_tool() {
    let tmp = mediapm_utils::temp::artifact_dir().unwrap();
    let cache_root = mediapm_utils::temp::cache_dir().unwrap();
    let paths = MediaPmPaths::from_root(tmp.path());

    // Generated doc carries the tool with a non-empty content map.
    // Placeholder values pass the CAS availability check without real
    // CAS bytes.
    let mut content_map = BTreeMap::new();
    content_map.insert("linux/media-tagger".to_string(), "provisioned".to_string());
    let tool_spec = ToolSpec {
        name: "media-tagger".to_string(),
        kind: ToolKindSpec::default(),
        runtime: ToolRuntime {
            content_map,
            payload_staging: ConfigPayloadStaging::Copy,
            ..Default::default()
        },
        ..Default::default()
    };
    let mut tools = BTreeMap::new();
    tools.insert("media-tagger@blake3:mt1".to_string(), tool_spec);
    let doc = NickelDocument { tools, ..Default::default() };
    save_conductor_generated_document(&paths, &doc).expect("pre-save generated doc");

    // State with a matching canonical_version (media-tagger resolves to
    // the git hash without network) and non-empty content_map_hash → the
    // skip path fires.
    let mut state = MediaPmState::default();
    state.managed_tools.push(ToolRegistryEntry {
        tool_id: "media-tagger".to_string(),
        version: format!("{}+{}", env!("CARGO_PKG_VERSION"), crate::global::MEDIAPM_GIT_HASH),
        canonical_version: crate::global::MEDIAPM_GIT_HASH.to_string(),
        content_map_hash: "blake3:mt1".to_string(),
        deployed_at: mediapm_utils::Timestamp::default(),
        resolved_tag: None,
        resolved_version: None,
        resolved_vcs_hash: None,
    });

    let mut desired_tools = BTreeMap::new();
    let req = ToolRequirement::default();
    desired_tools.insert("media-tagger".to_string(), serde_json::to_value(req).unwrap());

    let workspace_cas = super::open_workspace_cas_store(&paths).await.expect("open workspace cas");
    let result = reconcile_desired_tools(
        workspace_cas,
        &paths,
        &desired_tools,
        &BTreeMap::new(),
        PayloadStaging::InPlace,
        RecheckPolicy::default(),
        &state,
        Some(cache_root.path()),
        &ProgressScreen::disabled(),
        None,
    )
    .await
    .map(ToolSyncOutcome::into_report);

    assert!(result.is_ok(), "reconcile_desired_tools failed: {:?}", result.err());
    let report = result.unwrap();
    assert_eq!(report.tools_skipped, 1, "skip path must fire for matching state");

    // The skip path MUST contribute a tool_records entry with a non-empty
    // content_map_hash — currently it does NOT, which is the bug.
    let registered = report
        .tool_records
        .iter()
        .find(|e| e.tool_id == "media-tagger")
        .expect("skip path must register the tool in tool_records");
    assert!(
        !registered.content_map_hash.is_empty(),
        "skip-path registration must carry a non-empty content_map_hash",
    );
}

/// Seeds a generated document holding one `media-tagger` spec (whose provider
/// resolves without network access) and runs the sync against the given
/// persisted records and exact pin.
///
/// The document's spec is the build the newest record is supposed to describe
/// unless a test overrides it, so a test can make the record and the spec agree
/// or disagree by construction.
async fn reconcile_media_tagger(
    records: Vec<ToolRegistryEntry>,
    pinned_version: &str,
    spec_content_map: BTreeMap<String, String>,
) -> Result<ToolSyncReport, crate::error::MediaPmError> {
    let tmp = mediapm_utils::temp::artifact_dir().unwrap();
    let cache_root = mediapm_utils::temp::cache_dir().unwrap();
    let paths = MediaPmPaths::from_root(tmp.path());
    let spec = ToolSpec {
        name: "media-tagger".to_string(),
        kind: ToolKindSpec::default(),
        runtime: ToolRuntime {
            payload_staging: ConfigPayloadStaging::Copy,
            content_map: spec_content_map,
            ..Default::default()
        },
        ..Default::default()
    };
    let mut tools = BTreeMap::new();
    tools.insert("media-tagger".to_string(), spec);
    let doc = NickelDocument { tools, ..Default::default() };
    save_conductor_generated_document(&paths, &doc).expect("pre-save generated doc");

    let mut state = MediaPmState::default();
    state.managed_tools.extend(records);

    let req = ToolRequirement {
        version_spec: mediapm_conductor::tools::provider::ConfigVersionSpec::Exact(
            VersionSpecFields {
                version: Some(pinned_version.to_string()),
                vcs_hash: None,
                tag: None,
            },
        ),
        ..Default::default()
    };
    let mut desired_tools = BTreeMap::new();
    desired_tools.insert("media-tagger".to_string(), serde_json::to_value(req).unwrap());

    let workspace_cas = super::open_workspace_cas_store(&paths).await.expect("open workspace cas");
    reconcile_desired_tools(
        workspace_cas,
        &paths,
        &desired_tools,
        &BTreeMap::new(),
        PayloadStaging::InPlace,
        RecheckPolicy::default(),
        &state,
        Some(cache_root.path()),
        &ProgressScreen::disabled(),
        None,
    )
    .await
    .map(ToolSyncOutcome::into_report)
}

/// The version string `media-tagger` resolves to without network access: the
/// crate version plus the mediapm build hash, which is what its provider reports
/// as both the human-readable and the exact-match version.
fn media_tagger_version() -> String {
    format!("{}+{}", env!("CARGO_PKG_VERSION"), crate::global::MEDIAPM_GIT_HASH)
}

/// One persisted record for `media-tagger` describing a build.
fn media_tagger_record(
    content_map_hash: String,
    resolved_version: &str,
    deployed_at_nanos: u64,
) -> ToolRegistryEntry {
    ToolRegistryEntry {
        tool_id: "media-tagger".to_string(),
        version: resolved_version.to_string(),
        // Deliberately not the canonical version the provider resolves to: were
        // it equal, the post-resolve skip would fire and a run that took the
        // first skip check would be indistinguishable from one that did not.
        canonical_version: "stale-canonical".to_string(),
        content_map_hash,
        deployed_at: mediapm_utils::Timestamp::from_unix_nanos(deployed_at_nanos),
        resolved_tag: None,
        resolved_version: Some(resolved_version.to_string()),
        resolved_vcs_hash: None,
    }
}

/// Regression: an exact pin is matched against the **newest** record for the
/// tool, not the first one the registry happens to hold.
///
/// The registry keeps every deployment, in insertion order, so a record for an
/// older build can precede the newest one. Matching the pin against that older
/// record while activating the document's spec (the newest build) reports a
/// skip for a build the user did not ask for.
#[tokio::test]
async fn regression_a_pinned_tool_matches_the_newest_record_not_the_first() {
    // The document holds the build the older record describes, so every
    // condition the pre-fix check read is satisfied by that record: the pin
    // matches its version fields and its content hash matches the spec. Only
    // the newest record tells the pass that the tool has moved on since.
    let mut spec_content_map = BTreeMap::new();
    spec_content_map.insert("linux/media-tagger".to_string(), "blake3:old".to_string());
    let spec_hash = content_map_hash(&spec_content_map);

    let older_pin = media_tagger_version();
    // Insertion order is what the defect read: the matching (older) record
    // first, the newest build second.
    let records = vec![
        media_tagger_record(spec_hash, &older_pin, 1_000_000_000),
        media_tagger_record("blake3:newer".to_string(), "2099.01.01", 2_000_000_000),
    ];

    let report = reconcile_media_tagger(records, &older_pin, spec_content_map)
        .await
        .expect("the sync provisions media-tagger hermetically");

    assert_eq!(
        report.tools_skipped, 0,
        "the pin matches the older record, so the newest build must not be reported as skipped",
    );
    assert!(
        report.tool_records.iter().any(|entry| entry.tool_id == "media-tagger"),
        "the pass must provision the pinned build instead of skipping",
    );
}

/// Regression: a record whose content map the active spec no longer holds
/// describes another build, so the pin cannot be reported as satisfied.
///
/// The version fields match, which is all the pre-fix check read; the document
/// nonetheless holds a build the record was never written for.
#[tokio::test]
async fn regression_a_record_describing_another_build_is_not_skipped() {
    let mut spec_content_map = BTreeMap::new();
    spec_content_map.insert("linux/media-tagger".to_string(), "blake3:new".to_string());

    let pinned = media_tagger_version();
    let records = vec![media_tagger_record("blake3:stale".to_string(), &pinned, 1_000_000_000)];

    let report = reconcile_media_tagger(records, &pinned, spec_content_map)
        .await
        .expect("the sync provisions media-tagger hermetically");

    assert_eq!(report.tools_skipped, 0, "a spec holding another build must not satisfy the pin");
    assert!(
        report.tool_records.iter().any(|entry| entry.tool_id == "media-tagger"),
        "the pass must provision the pinned build instead of skipping",
    );
}
