//! End-to-end proof of the *fix* half of the two-stage NFD contract.
//!
//! A hierarchy component may only reach the materializer through metadata the
//! user does not control: tag values, ffprobe output, and upstream title
//! strings all arrive after the config-level NFD rejection has already run.
//! The materializer therefore normalizes instead of rejecting, and the entry
//! still commits.

use std::collections::BTreeMap;

use crate::common::{seed_cas, service_at, sync_library_with_test_terminal};
use bytes::Bytes;
use mediapm::{
    HierarchyNode, HierarchyNodeKind, HierarchyPath, MediaMetadataValue, MediaPmDocument,
    MediaSourceSpec, PlaylistFormat, SanitizeNamesConfig, save_mediapm_document,
};

/// Media id of the single source under test.
const MEDIA_ID: &str = "nfd.meta.source";

/// Artist metadata in NFC (`e` + U+00E9 precomposed) — the form a tagger or an
/// upstream title string realistically carries, and the form the config-level
/// check would reject if it were applied to resolved components.
const NFC_ARTIST: &str = "Caf\u{e9}";

/// The same artist in NFD (`e` + U+0301 combining acute) — the form the
/// materializer must commit.
const NFD_ARTIST: &str = "Cafe\u{301}";

/// Hierarchy path template: the artist folder comes from metadata, the file
/// name is literal. Only the resolved component carries the accented text.
const HIERARCHY_PATH: &str = "${media.metadata.artist}/track.mp4";

/// Payload stored in CAS for the `default` variant.
const PAYLOAD: &[u8] = b"nfd materialization payload";

/// A media source whose `artist` metadata is NFC and whose single variant
/// points at pre-seeded CAS content.
///
/// The declared hierarchy path is a template, so the config-level validation
/// sees the ASCII placeholder `${media.metadata.artist}` and passes; the NFC
/// text only appears once the placeholder is resolved during materialization.
fn document_with_nfc_metadata(variant_hash: &str) -> MediaPmDocument {
    MediaPmDocument {
        media: BTreeMap::from([(
            MEDIA_ID.to_string(),
            MediaSourceSpec {
                metadata: BTreeMap::from([(
                    "artist".to_string(),
                    MediaMetadataValue::Literal(NFC_ARTIST.to_string()),
                )]),
                variant_hashes: BTreeMap::from([("default".to_string(), variant_hash.to_string())]),
                steps: Vec::new(),
                ..MediaSourceSpec::default()
            },
        )]),
        hierarchy: vec![HierarchyNode {
            path: HierarchyPath::from(HIERARCHY_PATH),
            kind: HierarchyNodeKind::Media,
            id: None,
            media_id: Some(MEDIA_ID.to_string()),
            variant: Some("default".to_string()),
            variants: Vec::new(),
            rename_files: Vec::new(),
            format: PlaylistFormat::M3u8,
            ids: Vec::new(),
            sanitize_names: Some(SanitizeNamesConfig::Inherit),
            children: Vec::new(),
        }],
        ..MediaPmDocument::default()
    }
}

/// NFC external metadata is normalized to NFD on the way to a materialized
/// path, and the entry still commits with its CAS payload.
///
/// The guarantee protected here: the materializer *fixes* the components it
/// cannot ask the user to fix, so a source tagged with a precomposed `é`
/// materializes under a decomposed name instead of failing the sync.
#[tokio::test]
async fn sync_hierarchy_normalizes_nfc_metadata_to_nfd_materialized_path()
-> Result<(), mediapm::MediaPmError> {
    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    let mut service = service_at(root.path(), None).await?;
    let hash = seed_cas(&service, Bytes::from_static(PAYLOAD), "default variant").await?;
    service.conductor().cas().ensure_blob_materialized(hash).await.map_err(|source| {
        mediapm::MediaPmError::Workflow(format!("ensure blob materialized for '{hash}': {source}"))
    })?;

    let document = document_with_nfc_metadata(&hash.to_string());
    save_mediapm_document(&service.paths().mediapm_ncl, &document)?;

    let summary = sync_library_with_test_terminal(&mut service, false).await?;
    assert!(summary.materialized_paths > 0, "expected the entry to materialize: {summary:?}");

    let hierarchy_root = service.resolve_effective_paths()?.hierarchy_root_dir;
    let committed_names = std::fs::read_dir(&hierarchy_root)
        .map_err(|error| {
            mediapm::MediaPmError::Workflow(format!(
                "read hierarchy root '{}': {error}",
                hierarchy_root.display()
            ))
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| {
            mediapm::MediaPmError::Workflow(format!("list hierarchy root entries: {error}"))
        })?
        .into_iter()
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    assert!(
        committed_names.contains(&NFD_ARTIST.to_string()),
        "the committed artist folder must be NFD-normalized; got {committed_names:?}"
    );
    assert!(
        !committed_names.contains(&NFC_ARTIST.to_string()),
        "the committed artist folder must not keep the NFC spelling; got {committed_names:?}"
    );

    let materialized = hierarchy_root.join(NFD_ARTIST).join("track.mp4");
    let bytes = std::fs::read(&materialized).map_err(|error| {
        mediapm::MediaPmError::Workflow(format!(
            "read materialized '{}': {error}",
            materialized.display()
        ))
    })?;
    assert_eq!(bytes, PAYLOAD, "materialized file must carry the CAS payload");

    Ok(())
}
