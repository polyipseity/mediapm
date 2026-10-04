//! # Re-sync over an unchanged library
//!
//! The materializer has no "already up to date" path. An entry that runs and
//! writes is counted as materialized, and the only way an entry reaches
//! `skipped_paths` is a variant that resolved no content hash. Making a skip
//! fail the run is therefore only safe while a second `sync_library` over a
//! workspace nothing changed resolves the same hash the first run resolved.
//! If it stopped doing so, every repeat run of the command would fail.
//!
//! `resolve_variant_hash` reads the document and the conductor state, so the
//! second run has no reason to answer differently. That is an argument, and
//! this file is the check on it.
//!
//! The test asserts the first run materialized a path before it reads the
//! second run's skip count. A second run over a workspace that never wrote
//! anything reports zero skips because there was nothing to decline, which
//! would make the assertion pass for the wrong reason.

use std::collections::BTreeMap;

use crate::common::{seed_cas, service_with_cache, sync_library_with_test_terminal};
use bytes::Bytes;
use mediapm::{
    HierarchyNode, HierarchyNodeKind, HierarchyPath, MediaPmDocument, MediaRuntimeStorage,
    MediaSourceSpec, PlaylistFormat, save_mediapm_document,
};

/// Media id of the single source, and the directory its id materializes as.
const MEDIA_ID: &str = "resync-media";

/// The variant the single CAS payload is published under.
const VARIANT: &str = "default";

/// Hierarchy path template that puts the media id itself in the materialized
/// path, so the entry's output is observable on disk after each run.
const HIERARCHY_PATH: &str = "${media.id}/track.mp4";

/// Payload stored in CAS for [`VARIANT`].
const PAYLOAD: &[u8] = b"resync payload";

/// Builds a document with one media source bound to `media_id` at
/// [`HIERARCHY_PATH`], its variant pointing at `variant_hash`.
///
/// The source carries no steps, so both syncs resolve the variant from the
/// hash the document declares and no workflow has to run to produce one.
fn document_with_one_media_entry(variant_hash: &str) -> MediaPmDocument {
    MediaPmDocument {
        media: BTreeMap::from([(
            MEDIA_ID.to_string(),
            MediaSourceSpec {
                variant_hashes: BTreeMap::from([(VARIANT.to_string(), variant_hash.to_string())]),
                steps: Vec::new(),
                ..MediaSourceSpec::default()
            },
        )]),
        hierarchy: vec![HierarchyNode {
            path: HierarchyPath::from(HIERARCHY_PATH),
            kind: HierarchyNodeKind::Media,
            id: None,
            media_id: Some(MEDIA_ID.to_string()),
            variant: Some(VARIANT.to_string()),
            variants: Vec::new(),
            rename_files: Vec::new(),
            format: PlaylistFormat::M3u8,
            ids: Vec::new(),
            sanitize_names: None,
            children: Vec::new(),
        }],
        ..MediaPmDocument::default()
    }
}

/// A second `sync_library` over an unchanged library succeeds and reports zero
/// skipped paths.
///
/// The two run-1 assertions carry the weight of the run-2 one. Without
/// `first.materialized_paths > 0` the second sync could decline the only entry
/// and still report zero skips, because a workspace that wrote nothing has
/// nothing to decline. Without the on-disk check the run-1 counter could be
/// counting an entry whose bytes never landed, and the same hole opens up for
/// run 2.
///
/// The run-2 `materialized_paths` assertion says the entry was visited again
/// rather than quietly dropped from the plan, which is what makes its zero skip
/// count a statement about resolution rather than about an empty work list.
#[tokio::test]
async fn resync_over_unchanged_library_skips_no_paths() -> Result<(), mediapm::MediaPmError> {
    let (mut service, root, _cache) = service_with_cache(MediaRuntimeStorage::default()).await?;

    let hash = seed_cas(&service, Bytes::from_static(PAYLOAD), "resync variant").await?;
    let cas = service.conductor().cas().clone();
    cas.ensure_blob_materialized(hash).await.map_err(|source| {
        mediapm::MediaPmError::Workflow(format!("ensure blob materialized: {source}"))
    })?;

    save_mediapm_document(
        &service.paths().mediapm_ncl,
        &document_with_one_media_entry(&hash.to_string()),
    )?;

    let first = sync_library_with_test_terminal(&mut service, false).await?;
    assert!(
        first.materialized_paths > 0,
        "the first run must materialize the entry, or the second run's skip count proves \
         nothing about an unchanged library: {first:?}"
    );
    let materialized = root.path().join(MEDIA_ID).join("track.mp4");
    assert!(
        materialized.exists(),
        "the first run's materialized path must exist on disk before the re-sync is read: {}",
        materialized.display()
    );

    let second = sync_library_with_test_terminal(&mut service, false).await?;
    assert_eq!(
        second.skipped_paths, 0,
        "a re-sync over an unchanged library must decline no path: {second:?}"
    );
    assert!(
        second.materialized_paths > 0,
        "the re-sync must still visit the entry, or zero skips only says the work list was \
         empty: {second:?}"
    );
    assert!(
        materialized.exists(),
        "the re-sync must leave the previously materialized path in place: {}",
        materialized.display()
    );

    // Release the CAS `store/lock` before the workspace `TempDir` removes the
    // tree it guards.
    drop(service);
    Ok(())
}
