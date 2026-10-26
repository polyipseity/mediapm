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
    // The record states the hash of the map the document holds: a skip only
    // fires when the deployed spec is the build the record describes.
    let recorded_hash = content_map_hash(&content_map);
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
        content_map_hash: recorded_hash,
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
    let conductor_tool_id = format!("yt-dlp@{recorded_hash}");
    tools.insert(conductor_tool_id.clone(), tool_spec);
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
        content.contains(&expected_payload_binary(&paths, &conductor_tool_id, "linux/yt-dlp")),
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
    // The document key and the record's hash are one identity: a skip fires
    // when the map the document holds is the one the record was written for.
    let recorded_hash = content_map_hash(&content_map);
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
    tools.insert(format!("media-tagger@{recorded_hash}"), tool_spec);
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
        content_map_hash: recorded_hash,
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

/// Runs one `media-tagger` reconcile pass and hands back what it wrote.
///
/// `media-tagger` is the fixture tool because its provider is a builtin
/// launcher: a resolve needs neither network nor a seeded download cache, yet
/// the launcher it generates is a real payload, so the pass reaches the fetch
/// branch instead of the no-payload one.
///
/// The document is filed under `{name}@{hash}` the way a real pass files it, and
/// `pinned_vcs_hash` is the field the user pins, because `vcs_hash` is what this
/// provider fills for its build: its `resolved_version` is the bare crate
/// version and its `resolved_vcs_hash` the mediapm git hash. A test makes the
/// record and the document agree or disagree by construction — the document
/// holds `spec_content_map`, and each record states the hash of the map it
/// describes.
///
/// The temporaries come back beside the paths: `TempDir` removes its tree when
/// it drops, so a caller that kept only the paths would read a directory that no
/// longer exists.
async fn reconcile_media_tagger(
    records: Vec<ToolRegistryEntry>,
    pinned_vcs_hash: &str,
    spec_content_map: BTreeMap<String, String>,
) -> (tempfile::TempDir, tempfile::TempDir, MediaPmPaths, ToolSyncReport) {
    let tmp = mediapm_utils::temp::artifact_dir().expect("artifact dir");
    let cache_root = mediapm_utils::temp::cache_dir().expect("cache dir");
    let paths = MediaPmPaths::from_root(tmp.path());
    let spec_hash = content_map_hash(&spec_content_map);
    let conductor_tool_id = if spec_hash.is_empty() {
        "media-tagger".to_string()
    } else {
        format!("media-tagger@{spec_hash}")
    };
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
    let doc =
        NickelDocument { tools: BTreeMap::from([(conductor_tool_id, spec)]), ..Default::default() };
    save_conductor_generated_document(&paths, &doc).expect("pre-save generated doc");

    let mut state = MediaPmState::default();
    state.managed_tools.extend(records);

    let req = ToolRequirement {
        version_spec: mediapm_conductor::tools::provider::ConfigVersionSpec::Exact(
            VersionSpecFields {
                version: None,
                vcs_hash: Some(pinned_vcs_hash.to_string()),
                tag: None,
            },
        ),
        ..Default::default()
    };
    let mut desired_tools = BTreeMap::new();
    desired_tools.insert("media-tagger".to_string(), serde_json::to_value(req).unwrap());

    let workspace_cas = super::open_workspace_cas_store(&paths).await.expect("open workspace cas");
    let report = reconcile_desired_tools(
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
    .expect("the sync provisions media-tagger hermetically")
    .into_report();
    (tmp, cache_root, paths, report)
}

/// The absolute path a conductor tool id's payload entry is materialized at.
///
/// `provisioned_payload_dir` is the production rule for that layout — the one
/// `ProvisionCache::materialize` writes and the generated dotenv reads — so a
/// fixture that names the directory reads the rule instead of re-spelling it.
fn expected_payload_binary(paths: &MediaPmPaths, conductor_tool_id: &str, key: &str) -> String {
    mediapm_conductor::runtime_env::provisioned_payload_dir(&paths.tools_dir, conductor_tool_id)
        .join(key)
        .display()
        .to_string()
}

/// The human-readable version `media-tagger` reports: the crate version plus the
/// mediapm build hash.
fn media_tagger_version() -> String {
    format!("{}+{}", env!("CARGO_PKG_VERSION"), crate::global::MEDIAPM_GIT_HASH)
}

/// One persisted record for `media-tagger` describing the build `content_map_hash`
/// belongs to.
///
/// The provenance fields are the ones the provider writes: `resolved_version` is
/// the bare crate version and `resolved_vcs_hash` the mediapm git hash, while the
/// `+{hash}` form lives in the informational `version` field that no skip
/// compares.
fn media_tagger_record(
    content_map_hash: String,
    canonical_version: &str,
    deployed_at_nanos: u64,
) -> ToolRegistryEntry {
    ToolRegistryEntry {
        tool_id: "media-tagger".to_string(),
        version: media_tagger_version(),
        canonical_version: canonical_version.to_string(),
        content_map_hash,
        deployed_at: mediapm_utils::Timestamp::from_unix_nanos(deployed_at_nanos),
        resolved_tag: None,
        resolved_version: Some(env!("CARGO_PKG_VERSION").to_string()),
        resolved_vcs_hash: Some(crate::global::MEDIAPM_GIT_HASH.to_string()),
    }
}

/// Asserts the document the pass wrote and the record it pushed describe one
/// build.
///
/// A document key, a record's `content_map_hash` and the active spec's map are
/// one identity; a pass that writes one without the other leaves a tool whose
/// identity depends on which half a reader consults, which is what the skip
/// checks exist to prevent.
fn assert_document_and_record_agree(paths: &MediaPmPaths, report: &ToolSyncReport) {
    let doc = load_conductor_generated_document(paths).expect("load the generated document");
    let (key, spec) = find_active_tool_spec(&doc, "media-tagger")
        .expect("the freshly provisioned tool is active");
    let recorded = report
        .tool_records
        .iter()
        .find(|entry| entry.tool_id == "media-tagger")
        .expect("the pass records the tool it provisioned");
    let document_hash = content_map_hash(&spec.runtime.content_map);
    let expected_key = if document_hash.is_empty() {
        "media-tagger".to_string()
    } else {
        format!("media-tagger@{document_hash}")
    };
    assert_eq!(
        document_hash, recorded.content_map_hash,
        "the document's active spec and the record must state one identity",
    );
    assert_eq!(key, &expected_key, "the active entry is keyed by the identity the record states");
    assert_eq!(
        recorded.canonical_version,
        crate::global::MEDIAPM_GIT_HASH,
        "the run records the build the pin asked for",
    );
}

/// Regression: an exact pin is matched against the **newest** record for the
/// tool, not the first one the registry happens to hold.
///
/// The registry keeps every deployment, in insertion order, so a record for an
/// older build can precede the newest one. Matching the pin against that older
/// record while activating the document's spec (the build the older record
/// agrees with) reports a skip for a build that is not the one deployed.
#[tokio::test]
async fn regression_a_pinned_tool_matches_the_newest_record_not_the_first() {
    // The document holds the build the older record describes, so every
    // condition the pre-fix check read is satisfied by that record: the pin
    // matches its version fields and its content hash matches the spec. Only
    // the newest record tells the pass that the tool has moved on since.
    let mut spec_content_map = BTreeMap::new();
    spec_content_map.insert("linux/media-tagger".to_string(), "blake3:old".to_string());
    let spec_hash = content_map_hash(&spec_content_map);

    // The two canonical versions differ because the loader collapses records
    // that share `(tool_id, canonical_version)`, and neither is the version the
    // provider resolves, which keeps the post-resolve skip out of this test.
    let records = vec![
        media_tagger_record(spec_hash, "canonical-of-the-documented-build", 1_000_000_000),
        media_tagger_record(
            "blake3:newer".to_string(),
            "canonical-of-the-newest-build",
            2_000_000_000,
        ),
    ];

    let (_tmp, _cache, paths, report) =
        reconcile_media_tagger(records, crate::global::MEDIAPM_GIT_HASH, spec_content_map).await;

    assert_eq!(
        report.tools_skipped, 0,
        "the newest record describes another build than the document holds, so the pin is not satisfied",
    );
    assert_document_and_record_agree(&paths, &report);
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

    let records = vec![media_tagger_record(
        "blake3:stale".to_string(),
        "canonical-of-another-build",
        1_000_000_000,
    )];

    let (_tmp, _cache, paths, report) =
        reconcile_media_tagger(records, crate::global::MEDIAPM_GIT_HASH, spec_content_map).await;

    assert_eq!(report.tools_skipped, 0, "a spec holding another build must not satisfy the pin");
    assert_document_and_record_agree(&paths, &report);
}

/// Regression: the skip decided **after** the resolve must also require the
/// document to hold the build its record describes.
///
/// The state's record names the version the provider resolves, so the
/// post-resolve check sees a composite match; its content hash describes a build
/// the document does not hold. Skipping on the version alone activates the
/// document's own spec while recording the resolved version, so the record it
/// writes names two builds at once and the run reports a build the workspace
/// does not hold as skipped.
#[tokio::test]
async fn regression_a_post_resolve_skip_requires_the_document_to_hold_the_recorded_build() {
    let mut spec_content_map = BTreeMap::new();
    spec_content_map.insert("linux/media-tagger".to_string(), "blake3:documented".to_string());
    let mut recorded_map = BTreeMap::new();
    recorded_map.insert("linux/media-tagger".to_string(), "blake3:recorded".to_string());

    let records = vec![media_tagger_record(
        content_map_hash(&recorded_map),
        crate::global::MEDIAPM_GIT_HASH,
        1_000_000_000,
    )];

    let (_tmp, _cache, paths, report) =
        reconcile_media_tagger(records, crate::global::MEDIAPM_GIT_HASH, spec_content_map).await;

    assert_eq!(
        report.tools_skipped, 0,
        "the record's content hash describes a build the document does not hold, so the resolve does not license a skip",
    );
    assert_document_and_record_agree(&paths, &report);
}

/// Regression: the post-resolve skip reads the newest record for the tool, not
/// the newest record that happens to carry the resolved composite.
///
/// State keeps every deployment. When the newest one has moved on, an older
/// record can still carry the composite this pass resolved and the content map
/// the document holds; selecting by composite match alone therefore reaches that
/// older record and skips, which reports a build the newest deployment does not
/// describe. Both arms read the newest deployment, so both refuse here and the
/// pass re-provisions.
#[tokio::test]
async fn regression_a_post_resolve_skip_reads_the_newest_deployment() {
    let mut spec_content_map = BTreeMap::new();
    spec_content_map.insert("linux/media-tagger".to_string(), "blake3:documented".to_string());

    // The older record is the one every condition agrees with: its composite is
    // the version the provider resolves and its content hash is the document's.
    // The newest record has moved to another build, which is what tells the pass
    // the tool is no longer the build the older record describes.
    let records = vec![
        media_tagger_record(
            content_map_hash(&spec_content_map),
            crate::global::MEDIAPM_GIT_HASH,
            1_000_000_000,
        ),
        media_tagger_record(
            "blake3:newest".to_string(),
            "canonical-of-an-earlier-build",
            2_000_000_000,
        ),
    ];

    let (_tmp, _cache, paths, report) =
        reconcile_media_tagger(records, crate::global::MEDIAPM_GIT_HASH, spec_content_map).await;

    assert_eq!(
        report.tools_skipped, 0,
        "the newest deployment is another build, so neither record licenses a skip",
    );
    assert_document_and_record_agree(&paths, &report);
}
