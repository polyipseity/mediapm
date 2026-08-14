//! # Tool-sync integration tests
//!
//! Tests for [`MediaPmService::sync_tools()`] — the managed-tool
//! reconciliation pipeline (download, register, provision, content-import,
//! lifecycle, env generation).
//!
//! **Do NOT add workflow-sync or state-sync tests here.** This file is
//! exclusively for the tool provisioning / syncing subset of the mediapm
//! sync pipeline. Other sync concerns (hierarchy, materialization,
//! conductor orchestration) belong in separate test modules.
//!
//! These tests focus on file-creation guarantees, document structure,
//! idempotency, and pure-function logic — not on counter values
//! (`added_tools`, `updated_tools`, etc.).

use mediapm::{
    MediaPmService, MediaPmState, MediaRuntimeStorage, ToolRegistryEntry, ToolRequirement,
};
use mediapm_conductor::tools::provider::VersionSpecFields;
use mediapm_conductor::{NickelDocument, ToolKindSpec, ToolRuntime, ToolSpec, encode_document};

use crate::common::service_with_cache;

mod basics;
mod composite;
mod inline_deps;
mod requires_sync;
mod resolved;
mod validation;

// ---------------------------------------------------------------------------
// Shared test scaffolding
// ---------------------------------------------------------------------------

/// The v2 `state.json` fixture: map-form `managed_tools` with a single
/// ffmpeg entry. `resolved` selects the pre-v3 wire format (string
/// provenance fields) versus explicit `null`s.
fn v2_state_json(resolved: bool) -> serde_json::Value {
    let resolved_tag = if resolved {
        serde_json::Value::String("v7.1".to_string())
    } else {
        serde_json::Value::Null
    };
    let resolved_version = if resolved {
        serde_json::Value::String("7.1".to_string())
    } else {
        serde_json::Value::Null
    };
    let resolved_vcs_hash = if resolved {
        serde_json::Value::String("abc".to_string())
    } else {
        serde_json::Value::Null
    };
    serde_json::json!({
        "version": 2,
        "managed_files": {},
        "managed_tools": {
            "ffmpeg": {
                "version": "7.1",
                "canonical_version": "ffmpeg-v7.1",
                "content_map_hash": "blake3:abc123",
                "deployed_at": 1_700_000_000,
                "resolved_tag": resolved_tag,
                "resolved_version": resolved_version,
                "resolved_vcs_hash": resolved_vcs_hash,
            }
        },
        "workflow_states": {}
    })
}

/// Creates a service whose sync skip-path is fully hermetic for the three
/// yt-dlp/ffmpeg/deno tools: exact version specs, a seeded `state.json` with
/// matching entries, and a seeded generated doc that already carries the
/// inlined `deps/` keys a previous network sync would have produced.
#[allow(clippy::too_many_lines)]
async fn seeded_three_tool_skip_service() -> Result<
    (MediaPmService<mediapm_cas::FileSystemCas>, tempfile::TempDir, tempfile::TempDir),
    mediapm::MediaPmError,
> {
    use std::collections::BTreeMap;

    let mut runtime = MediaRuntimeStorage::default();
    runtime.tools.insert(
        "yt-dlp".to_string(),
        ToolRequirement {
            version_spec: mediapm::ConfigVersionSpec::Exact(VersionSpecFields {
                version: Some("v2024.01.01".to_string()),
                vcs_hash: None,
                tag: None,
            }),
            dependencies: BTreeMap::from([
                (
                    "ffmpeg".to_string(),
                    mediapm::ConfigVersionSpec::Exact(VersionSpecFields {
                        version: Some("v7.1".to_string()),
                        vcs_hash: None,
                        tag: None,
                    }),
                ),
                (
                    "deno".to_string(),
                    mediapm::ConfigVersionSpec::Exact(VersionSpecFields {
                        version: Some("v1.46.0".to_string()),
                        vcs_hash: None,
                        tag: None,
                    }),
                ),
            ]),
            ..Default::default()
        },
    );
    let (service, root, cache_root) = service_with_cache(runtime).await?;

    // Seed state.json with matching entries for all three tools so the
    // spec-based skip fires without network.
    let state_path = service.paths().mediapm_state_json.clone();
    std::fs::create_dir_all(state_path.parent().expect("state parent dir"))
        .expect("create state parent dir");
    let mut state = MediaPmState::default();
    state.managed_tools.push(ToolRegistryEntry {
        tool_id: "yt-dlp".to_string(),
        version: "seeded-version".to_string(),
        // Non-transitive composite: segments reference each dep's OWN
        // version segment (sorted by dep_id); composite-bearing dep entries
        // never nest into the requester's composite.
        canonical_version: "yt-dlp-v2024.01.01;deno:deno-v1.46.0;ffmpeg:ffmpeg-v7.1".to_string(),
        content_map_hash: "blake3:abc123".to_string(),
        deployed_at: mediapm_utils::Timestamp::from_unix_secs(42),
        resolved_tag: None,
        resolved_version: Some("v2024.01.01".to_string()),
        resolved_vcs_hash: None,
    });
    state.managed_tools.push(ToolRegistryEntry {
        tool_id: "ffmpeg".to_string(),
        version: "seeded-version".to_string(),
        canonical_version: "ffmpeg-v7.1".to_string(),
        content_map_hash: "blake3:ffmpeg1".to_string(),
        deployed_at: mediapm_utils::Timestamp::from_unix_secs(42),
        resolved_tag: None,
        resolved_version: Some("v7.1".to_string()),
        resolved_vcs_hash: None,
    });
    state.managed_tools.push(ToolRegistryEntry {
        tool_id: "deno".to_string(),
        version: "seeded-version".to_string(),
        canonical_version: "deno-v1.46.0".to_string(),
        content_map_hash: "blake3:deno1".to_string(),
        deployed_at: mediapm_utils::Timestamp::from_unix_secs(42),
        resolved_tag: None,
        resolved_version: Some("v1.46.0".to_string()),
        resolved_vcs_hash: None,
    });
    std::fs::write(&state_path, serde_json::to_vec(&state).expect("state serializes"))
        .expect("write seeded state");

    // Seed the generated doc with entries for ALL three tools: the
    // workspace-CAS skip path requires `find_active_tool_spec` for each tool
    // (deps get their own generated-doc specs too, not just the requester).
    // Content map values are non-hash placeholders (external_data invariant
    // skips them, and they pass the CAS availability check).
    let mut doc = NickelDocument::default();
    doc.tools.insert(
        "yt-dlp@blake3:abc123".to_string(),
        ToolSpec {
            name: "yt-dlp".to_string(),
            kind: ToolKindSpec::default(),
            runtime: ToolRuntime {
                content_map: BTreeMap::from([
                    ("linux/yt-dlp".to_string(), "provisioned".to_string()),
                    ("linux/".to_string(), "provisioned".to_string()),
                    ("deps/ffmpeg/linux/ffmpeg".to_string(), "provisioned".to_string()),
                    ("deps/deno/linux/deno".to_string(), "provisioned".to_string()),
                ]),
                ..Default::default()
            },
            ..Default::default()
        },
    );
    doc.tools.insert(
        "ffmpeg@blake3:ffmpeg1".to_string(),
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
    doc.tools.insert(
        "deno@blake3:deno1".to_string(),
        ToolSpec {
            name: "deno".to_string(),
            kind: ToolKindSpec::default(),
            runtime: ToolRuntime {
                content_map: BTreeMap::from([
                    ("linux/deno".to_string(), "provisioned".to_string()),
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

    Ok((service, root, cache_root))
}
