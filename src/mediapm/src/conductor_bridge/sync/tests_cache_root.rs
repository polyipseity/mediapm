//! Behaviour of the cache root override and the user-level cache it bypasses.

use crate::output::ProgressScreen;
use mediapm_conductor::cache_user_level::default_mediapm_user_download_cache_root;
use std::collections::BTreeMap;

use super::*;

#[tokio::test]
async fn reconcile_desired_tools_with_override_does_not_touch_real_cache() {
    let tmp = mediapm_utils::temp::artifact_dir().unwrap();
    let cache_root = mediapm_utils::temp::cache_dir().unwrap();
    let paths = MediaPmPaths::from_root(tmp.path());
    // Record real cache state before the call.
    let real_cache_mtime = default_mediapm_user_download_cache_root()
        .and_then(|p| std::fs::metadata(p.join("tools.json")).ok())
        .and_then(|m| m.modified().ok());

    let state = MediaPmState::default();
    let workspace_cas = super::open_workspace_cas_store(&paths).await.expect("open workspace cas");
    let result = reconcile_desired_tools(
        workspace_cas,
        &paths,
        &BTreeMap::new(),
        &BTreeMap::new(),
        RecheckPolicy::default(),
        &state,
        Some(cache_root.path()),
        &ProgressScreen::disabled(),
        None,
    )
    .await;

    assert!(result.is_ok(), "reconcile_desired_tools failed: {:?}", result.err());
    let report = result.unwrap();
    assert_eq!(report.tools_added, 0, "no tools should be added");
    assert_eq!(report.tools_updated, 0, "no tools should be updated");
    assert_eq!(report.tools_skipped, 0, "no tools should be skipped");
    assert!(report.warnings.is_empty(), "no warnings expected: {:?}", report.warnings);

    // Verify the override path was used (cache files initialized there).
    assert!(
        cache_root.path().join("tools.json").exists() || cache_root.path().join("store").exists(),
        "override cache dir should have been initialized",
    );

    // Verify the real cache was not modified by the call (mtime unchanged).
    let real_cache_mtime_after = default_mediapm_user_download_cache_root()
        .and_then(|p| std::fs::metadata(p.join("tools.json")).ok())
        .and_then(|m| m.modified().ok());
    assert_eq!(
        real_cache_mtime, real_cache_mtime_after,
        "real cache directory must not be modified when cache_root_override is set",
    );
}

#[tokio::test]
async fn reconcile_desired_tools_cache_override_supports_explicit_paths() {
    let tmp = mediapm_utils::temp::artifact_dir().unwrap();
    let cache_root = mediapm_utils::temp::cache_dir().unwrap();
    let paths = MediaPmPaths::from_root(tmp.path());
    // Pre-populate the cache dir with an empty store/ dir so the CAS
    // opens cleanly at the override path.
    std::fs::create_dir_all(cache_root.path().join("store")).unwrap();

    let state = MediaPmState::default();
    let workspace_cas = super::open_workspace_cas_store(&paths).await.expect("open workspace cas");
    let result = reconcile_desired_tools(
        workspace_cas,
        &paths,
        &BTreeMap::new(),
        &BTreeMap::new(),
        RecheckPolicy::default(),
        &state,
        Some(cache_root.path()),
        &ProgressScreen::disabled(),
        None,
    )
    .await;

    assert!(
        result.is_ok(),
        "reconcile_desired_tools with pre-populated cache dir failed: {:?}",
        result.err()
    );
    let report = result.unwrap();
    assert!(report.warnings.is_empty(), "no warnings expected: {:?}", report.warnings);
}
