//! # Explicit invalidation is reachable from the mediapm binary
//!
//! Conductor instance keys are content-derived: an unchanged step is served
//! from cache however much wall-clock time passed. That makes explicit
//! invalidation the only way to re-execute a step whose external
//! circumstances changed, and this module pins the service-side half of it:
//! `MediaPmService::invalidate_media_step_tool_calls`.
//!
//! The invalidation path had quietly become a no-op. It cleared the media's
//! recorded step progress and then reported invented `removed` instances,
//! while the conductor served the step from cache on the next sync. The
//! test below walks the whole sequence — free re-run, invalidation, forced
//! re-run, free again — because each assertion is meaningless without the
//! one before it: without a proven cache hit first, a "re-executed" step
//! could just be work the first run never finished.

use mediapm::{MediaPmError, MediaRuntimeStorage, media_id_from_uri};
use url::Url;

use crate::common::{service_with_cache, sync_library_with_test_terminal};
use crate::int::sync_idempotency_double_run::{SOURCE_URI, seed_probe_workspace};

/// Invalidating a media step arms the next sync to re-execute the workflow.
///
/// The user-level guarantee: `mediapm media invalidate` followed by a sync
/// re-runs the targeted media's workflow even though its inputs are
/// unchanged. The report says regeneration was requested and carries no
/// invented instance deletions, because the content-derived key keeps the
/// instance rather than removing it.
#[tokio::test]
async fn invalidating_a_media_step_forces_the_next_sync_to_reexecute() -> Result<(), MediaPmError> {
    let (mut service, _root, _cache) = service_with_cache(MediaRuntimeStorage::default()).await?;

    let uri = Url::parse(SOURCE_URI).expect("source URI must parse");
    let media_id = media_id_from_uri(&uri);
    seed_probe_workspace(&mut service, &uri).await?;

    let first = sync_library_with_test_terminal(&mut service, false).await?;
    assert!(
        first.executed_instances >= 1,
        "the first sync must execute the import workflow before invalidation means anything: \
         {first:?}"
    );

    // Prove the step is cached before invalidating, so the forced run below
    // is re-executing a cached instance rather than doing first-run work.
    let cached_run = sync_library_with_test_terminal(&mut service, false).await?;
    assert_eq!(
        cached_run.executed_instances, 0,
        "an unchanged re-run must be served from cache before invalidation: {cached_run:?}"
    );

    let summary = service.invalidate_media_step_tool_calls(&media_id, 0, true, false)?;
    assert!(
        summary.regeneration_requested,
        "invalidating tool calls must arm the next sync to re-execute them: {summary:?}"
    );
    assert!(
        summary.warnings.is_empty(),
        "a successful invalidation carries no warnings: {summary:?}"
    );

    // The invalidation is what makes this run re-execute: the material is
    // still the same bytes the cached run was served from.
    let forced = sync_library_with_test_terminal(&mut service, false).await?;
    assert!(
        forced.executed_instances >= 1,
        "invalidating a media step must re-execute its workflow on the next sync: {forced:?} \
         (cached run: {cached_run:?})"
    );

    // Explicit invalidation is run-scoped: once the forced run re-records the
    // instance, an unforced sync is free again.
    let after = sync_library_with_test_terminal(&mut service, false).await?;
    assert_eq!(
        after.executed_instances, 0,
        "the sync after a forced one must be served from cache again: {after:?}"
    );

    // Release the CAS store lock before the workspace TempDir removes the tree.
    drop(service);
    Ok(())
}
