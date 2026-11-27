//! Reconcile-level proof that the post-apply clearing pass is wired into
//! `reconcile_desired_tools`.

use mediapm::{MediaRuntimeStorage, ToolDeployments, ToolRequirement, find_active_tool_spec};
use mediapm_conductor::encode_document;

use crate::common::stub_upstream::mock_all_managed_tools;
use crate::common::{
    read_generated_doc, read_state, service_with_cache, test_sync_progress_overrides,
};

/// The post-apply pass runs inside `reconcile_desired_tools`, so this test
/// drives a real sync rather than the helper. A payload-bearing tool is the
/// fixture because only a non-empty content map can shadow the live key.
#[tokio::test]
async fn a_skipping_sync_clears_a_stale_sibling_map_through_reconcile()
-> Result<(), mediapm::MediaPmError> {
    let mut runtime = MediaRuntimeStorage::default();
    runtime.tools.insert("yt-dlp".to_string(), ToolRequirement::default());
    let (_mock, _seams) = mock_all_managed_tools().await;
    let (mut service, _root, _cache_root) = service_with_cache(runtime).await?;

    // First pass: provision everything, so the second pass can skip.
    service.sync_tools_with_progress_overrides(None, false, test_sync_progress_overrides()).await?;

    // Seed a stale sibling beside the live key, carrying a real content map.
    let mut doc = read_generated_doc(&service);
    let (live_key, live_spec) = find_active_tool_spec(
        &doc,
        &ToolDeployments::from_records(&read_state(&service).managed_tools),
        "yt-dlp",
    )
    .expect("the first sync must leave an active yt-dlp entry");
    let live_key = live_key.clone();
    let live_content_map = live_spec.runtime.content_map.clone();
    assert!(!live_content_map.is_empty(), "the fixture tool must carry a payload");
    let mut stale_spec = live_spec.clone();
    stale_spec.runtime.content_map = live_content_map.clone();
    let stale_key = format!("yt-dlp@{}", "0".repeat(64));
    doc.tools.insert(stale_key.clone(), stale_spec);
    let encoded = encode_document(doc).expect("seed the stale sibling");
    std::fs::write(&service.paths().conductor_generated_ncl, encoded)
        .expect("write the seeded stale sibling");

    // Second pass: everything matches, so the skip path fires and the
    // post-apply pass is the only thing that can remove the stale sibling.
    let second = service
        .sync_tools_with_progress_overrides(None, false, test_sync_progress_overrides())
        .await?;
    assert!(second.skipped_tools >= 1, "the pass must skip, not reprovision: {second:?}");

    let after = read_generated_doc(&service);
    assert!(
        !after.tools.contains_key(&stale_key),
        "the stale sibling must be dropped: {:?}",
        after.tools.keys().collect::<Vec<_>>()
    );
    assert_eq!(
        after.tools[&live_key].runtime.content_map, live_content_map,
        "the live key keeps its content map"
    );

    // One pass, then nothing: the third sync leaves the document byte-identical.
    let before_bytes = std::fs::read(&service.paths().conductor_generated_ncl).expect("read doc");
    let third = service
        .sync_tools_with_progress_overrides(None, false, test_sync_progress_overrides())
        .await?;
    let after_bytes = std::fs::read(&service.paths().conductor_generated_ncl).expect("read doc");
    assert_eq!(third.pruned_tools, 0, "a second run must have nothing left to prune: {third:?}");
    assert_eq!(
        after_bytes, before_bytes,
        "an unchanged library leaves the document byte-identical"
    );
    service.shutdown().await.expect("service shutdown");
    Ok(())
}
