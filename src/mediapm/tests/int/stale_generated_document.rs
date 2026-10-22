//! # A sync over a generated document this build cannot read
//!
//! The generated conductor document is machine-owned: mediapm rewrites it in
//! full from the user document on every sync, and the file says so at the top.
//! A build that tightens what a document may name — as the external-data
//! invariant did when it grew the reference shapes it checks — therefore leaves
//! workspaces holding a file this build refuses to decode, and the sync that
//! owns the file is the thing that would replace it.
//!
//! So a generated document that fails to decode is discarded and regenerated,
//! while the user-owned document beside it still fails loudly. This test pins
//! both halves: the stale file is unreadable before the run (or the test proves
//! nothing), the run succeeds anyway, and what it leaves on disk decodes and
//! declares the payload the media document names.

use bytes::Bytes;
use mediapm::{
    HierarchyNode, HierarchyNodeKind, HierarchyPath, MediaPmDocument, MediaRuntimeStorage,
    MediaSourceSpec, save_mediapm_document,
};

use crate::common::{
    read_generated_doc, seed_cas, service_with_cache, sync_library_with_test_terminal,
};

/// Media id of the single source.
const MEDIA_ID: &str = "stale-generated-media";

/// The variant the seeded payload is published under.
const VARIANT: &str = "default";

/// Payload stored in CAS for [`VARIANT`].
const PAYLOAD: &[u8] = b"stale generated document payload";

/// Hierarchy path template that puts the media id in the materialized path, so
/// the run has an output to observe.
const HIERARCHY_PATH: &str = "${media.id}/track.mp4";

/// Builds the document with one media source bound to `media_id` at
/// [`HIERARCHY_PATH`], its variant pointing at `variant_hash`.
///
/// The source carries no steps, so the sync resolves the variant from the hash
/// the document declares and no workflow has to run to produce one.
fn document_with_one_media_entry(variant_hash: &str) -> MediaPmDocument {
    MediaPmDocument {
        media: std::collections::BTreeMap::from([(
            MEDIA_ID.to_string(),
            MediaSourceSpec {
                variant_hashes: std::collections::BTreeMap::from([(
                    VARIANT.to_string(),
                    variant_hash.to_string(),
                )]),
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
            format: mediapm::PlaylistFormat::M3u8,
            ids: Vec::new(),
            sanitize_names: None,
            children: Vec::new(),
        }],
        ..MediaPmDocument::default()
    }
}

/// Renders the shape an earlier build wrote: a workflow step whose `hash` input
/// is a bare CAS hash, with no `external_data` entry naming it.
///
/// The current encoder rejects exactly this document, which is the point: the
/// file cannot be produced through the API any more, so it is written as the
/// text the earlier build left behind.
fn stale_generated_document_text(hash: &str) -> String {
    format!(
        r#"{{
  version = 2,
  external_data = {{}},
  tools = {{}},
  workflows = [
    {{
      name = "mediapm.media.stale",
      steps = [
        {{
          id = "step-0-import",
          inputs = {{
            hash = "{hash}",
            kind = "cas_hash",
          }},
          tool = "import",
        }},
      ],
    }},
  ],
}}
"#
    )
}

/// A generated document this build cannot decode is regenerated, and the sync
/// that owns it still runs.
#[tokio::test]
async fn a_stale_generated_document_is_regenerated() -> Result<(), mediapm::MediaPmError> {
    let (mut service, root, _cache) = service_with_cache(MediaRuntimeStorage::default()).await?;

    let hash = seed_cas(&service, Bytes::from_static(PAYLOAD), "stale variant").await?;
    save_mediapm_document(
        &service.paths().mediapm_ncl,
        &document_with_one_media_entry(&hash.to_string()),
    )?;

    let generated_path = service.paths().conductor_generated_ncl.clone();
    std::fs::write(&generated_path, stale_generated_document_text(&hash.to_string()))
        .expect("the stale generated document should be writable");

    let stale = std::fs::read(&generated_path).expect("the stale document should be readable");
    let decode_error = mediapm_conductor::decode_document(&stale)
        .expect_err("the stale shape must be one this build refuses, or this test proves nothing");
    assert!(
        decode_error.to_string().contains("not in external_data"),
        "the stale shape must be refused by the external-data invariant: {decode_error}"
    );

    let summary = sync_library_with_test_terminal(&mut service, false).await?;
    assert_eq!(
        (summary.materialized_paths, summary.missing_paths),
        (1, 0),
        "the run must regenerate the document and materialize the entry: {summary:?}"
    );

    let materialized = root.path().join(MEDIA_ID).join("track.mp4");
    assert!(
        materialized.is_file(),
        "the regenerated document must resolve the entry: {}",
        materialized.display()
    );

    let regenerated = read_generated_doc(&service);
    assert!(
        regenerated.external_data.contains_key(&hash),
        "the regenerated document must declare the payload the media document names"
    );

    Ok(())
}
