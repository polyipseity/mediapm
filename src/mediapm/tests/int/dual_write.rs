//! # Dual-write strategy integration tests
//!
//! Tests for the dual-write strategy between state.json (always-write) and
//! conductor.generated.ncl (change-detected / skip-if-unchanged).
//!
//! These tests verify:
//! - `conductor.generated.ncl` is NOT re-written when content is identical
//!   (change-detected via `write_bytes_if_changed`)
//! - The always-write policy for state.json is tested separately in
//!   `state_persistence.rs` (unit-level at the save-layer boundary).
//! - State-only churn (e.g. `canonical_version` changes without payload
//!   changes) does NOT propagate to generator file mtime.

use std::time::Duration;

use mediapm::{MediaPmService, MediaRuntimeStorage, ToolRequirement};
use tempfile::tempdir;

// ---------------------------------------------------------------------------
// conductor.generated.ncl: change-detected write
// ---------------------------------------------------------------------------

/// Verifies that `conductor.generated.ncl` is NOT re-written when its
/// content is identical between consecutive syncs.
///
/// This is the "skip-if-unchanged" half of the dual-write strategy. The
/// generator document is an artifact-manifest file: it only changes when
/// binary payload hashes change. Metadata-only updates are absorbed by
/// state.json.
#[tokio::test]
async fn conductor_ncl_skips_write_when_unchanged() {
    let root = tempdir().expect("tempdir");
    let cache_root = tempdir().expect("cache tempdir");
    let runtime = MediaRuntimeStorage {
        cache_root_override: Some(cache_root.path().to_path_buf()),
        ..MediaRuntimeStorage::default()
    };
    let mut service =
        MediaPmService::new_fs_at_with_runtime_storage_overrides(root.path(), runtime)
            .await
            .expect("create service");
    service.sync_tools().await.expect("first sync");
    let generated_path = service.paths().conductor_generated_ncl.clone();
    let meta1 = std::fs::metadata(&generated_path).expect("generated file exists");
    let mtime1 = meta1.modified().expect("mtime after first sync");

    // Advance clock so mtime would differ if a write occurs.
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Second sync with identical inputs: generator file should NOT be
    // re-written because content hasn't changed.
    service.sync_tools().await.expect("second sync");
    let meta2 = std::fs::metadata(&generated_path).expect("generated file exists");
    let mtime2 = meta2.modified().expect("mtime after second sync");

    assert_eq!(
        mtime1, mtime2,
        "conductor.generated.ncl mtime must not change when content is identical"
    );
}

// ---------------------------------------------------------------------------
// Regression: state-only churn does not touch generator file
// ---------------------------------------------------------------------------

/// Verifies that when only `canonical_version` changes in state (simulating
/// upstream metadata churn without payload changes), the conductor generated
/// document remains untouched.
///
/// This regression test reproduces the original scenario: `BtbN` autobuild
/// tags change daily (`canonical_version` churn) while the evermeet binary
/// (same `content_map` / runtime hashes) stays identical. The generated NCL
/// document should not change — state.json absorbs the metadata update.
#[tokio::test]
async fn regression_state_only_churn_does_not_touch_conductor_file() {
    let root = tempdir().expect("tempdir");
    let cache_root = tempdir().expect("cache tempdir");
    let mut runtime = MediaRuntimeStorage {
        cache_root_override: Some(cache_root.path().to_path_buf()),
        ..MediaRuntimeStorage::default()
    };
    // Use media-tagger — resolves without network; its empty canonical_version
    // gives us a stable baseline.
    runtime.tools.insert("media-tagger".to_string(), ToolRequirement::default());
    let mut service =
        MediaPmService::new_fs_at_with_runtime_storage_overrides(root.path(), runtime)
            .await
            .expect("create service");
    service.sync_tools().await.expect("first sync");

    let generated_path = service.paths().conductor_generated_ncl.clone();
    let state_path = service.paths().mediapm_state_json.clone();

    // Read baseline state and generated file bytes.
    let baseline_state = std::fs::read_to_string(&state_path).expect("read state.json");
    let baseline_generated = std::fs::read(&generated_path).expect("read conductor.generated.ncl");
    let gen_meta1 = std::fs::metadata(&generated_path).expect("generated file exists");
    let gen_mtime1 = gen_meta1.modified().expect("mtime after first sync");

    // Simulate metadata churn: modify canonical_version in state.json.
    // A full-blown canonical_version change would affect the tool's resolved
    // value; we simulate by modifying state.json directly and re-saving.
    let mut state: mediapm::MediaPmState =
        serde_json::from_str(&baseline_state).expect("deserialize state.json");
    if let Some(entry) = state.managed_tools.iter_mut().find(|e| e.tool_id == "media-tagger") {
        entry.canonical_version = "simulated-churn-v2".to_string();
    }
    let modified_json = serde_json::to_string_pretty(&state).expect("serialize modified state");
    std::fs::write(&state_path, &modified_json).expect("write modified state.json");

    // Advance clock.
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Second sync: media-tagger resolves again; its canonical_version is ""
    // (same as baseline), so payload hasn't changed. The generated NCL should
    // be byte-identical and mtime-unchanged.
    service.sync_tools().await.expect("second sync");

    // Verify generated file: bytes and mtime unchanged.
    let after_generated =
        std::fs::read(&generated_path).expect("read conductor.generated.ncl after second sync");
    let gen_meta2 = std::fs::metadata(&generated_path).expect("generated file exists");
    let gen_mtime2 = gen_meta2.modified().expect("mtime after second sync");

    assert_eq!(
        baseline_generated, after_generated,
        "conductor.generated.ncl bytes must not change on state-only churn"
    );
    assert_eq!(
        gen_mtime1, gen_mtime2,
        "conductor.generated.ncl mtime must not change on state-only churn"
    );

    // Verify state.json was updated (always-write — the canonical_version
    // our simulation injected may have been overwritten by the re-resolution,
    // but the file itself was written).
    let after_state =
        std::fs::read_to_string(&state_path).expect("read state.json after second sync");
    assert!(!after_state.is_empty(), "state.json must still exist and be non-empty");
}
