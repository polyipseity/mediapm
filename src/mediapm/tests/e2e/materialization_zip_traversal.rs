//! ZIP member names are attacker-controlled input on the folder-variant
//! extraction path.
//!
//! A tool-produced archive (or anything that can write into the CAS) decides
//! the member names the materializer later writes to disk. These tests pin the
//! two guarantees that makes safe:
//!
//! 1. A member whose declared name escapes the archive root (`../evil.txt`,
//!    `..\..\evil.txt`) or is absolute (`/etc/evil.txt`) is **refused** with an
//!    error that names the member, and **nothing is written outside the target
//!    media folder**.
//! 2. A rename rule's regex replacement cannot re-introduce traversal *after*
//!    normalization has already run, and an ordinary replacement still works.
//!
//! Absence on disk is asserted alongside the error, because a rejected member
//! and a silently dropped member are indistinguishable from the error alone.

use std::collections::BTreeMap;

use crate::common::{make_zip, seed_cas, service_at, sync_library_with_test_terminal};
use bytes::Bytes;
use mediapm::{
    HierarchyFolderRenameRule, HierarchyNode, HierarchyNodeKind, HierarchyPath, MediaPmDocument,
    MediaSourceSpec, PlaylistFormat, SanitizeNamesConfig, save_mediapm_document,
};

/// Media id of the single source under test.
const MEDIA_ID: &str = "zip.traversal.source";

/// Variant name carrying the ZIP folder payload.
const ARCHIVE_VARIANT: &str = "archive";

/// Hierarchy folder the archive is extracted into. A `../` member escapes
/// exactly one level, landing in the workspace root next to it.
const TARGET_FOLDER: &str = "album";

/// Name the escaping member tries to create one level above [`TARGET_FOLDER`].
const ESCAPED_FILE: &str = "evil.txt";

/// Bytes written by the escaping member if the traversal is not refused.
const ESCAPED_BODY: &[u8] = b"zip-slip payload";

/// Bytes of the legitimate nested member used by the positive controls.
const NESTED_BODY: &[u8] = b"nested payload";

/// Builds the document under test: one media source whose `archive` variant
/// is `archive_zip`, materialized as a [`HierarchyNodeKind::MediaFolder`] at
/// [`TARGET_FOLDER`] with the given folder rename rules.
fn document_with_zip_variant(
    archive_zip: &str,
    rename_files: Vec<HierarchyFolderRenameRule>,
) -> MediaPmDocument {
    MediaPmDocument {
        media: BTreeMap::from([(
            MEDIA_ID.to_string(),
            MediaSourceSpec {
                variant_hashes: BTreeMap::from([(
                    ARCHIVE_VARIANT.to_string(),
                    archive_zip.to_string(),
                )]),
                steps: Vec::new(),
                ..MediaSourceSpec::default()
            },
        )]),
        hierarchy: vec![HierarchyNode {
            path: HierarchyPath::from(TARGET_FOLDER),
            kind: HierarchyNodeKind::MediaFolder,
            id: None,
            media_id: Some(MEDIA_ID.to_string()),
            variant: None,
            variants: vec![ARCHIVE_VARIANT.to_string()],
            rename_files,
            format: PlaylistFormat::M3u8,
            ids: Vec::new(),
            sanitize_names: Some(SanitizeNamesConfig::Inherit),
            children: Vec::new(),
        }],
        ..MediaPmDocument::default()
    }
}

/// Seeds `entries` as the `archive` variant, saves the document, and returns
/// the sync outcome together with the workspace [`TempDir`] the escaped file
/// would land in.
///
/// The `TempDir` is returned rather than its path so the caller keeps the
/// directory alive: dropping it would delete the very tree whose contents the
/// escape assertions inspect, turning a real check into a vacuous one.
async fn sync_with_zip_members(
    entries: &[(&str, &[u8])],
    rename_files: Vec<HierarchyFolderRenameRule>,
) -> Result<
    (Result<mediapm::SyncSummary, mediapm::MediaPmError>, tempfile::TempDir),
    mediapm::MediaPmError,
> {
    let root = mediapm_utils::temp::artifact_dir().expect("tempdir");
    let mut service = service_at(root.path(), None).await?;
    let zip_bytes = make_zip(entries);
    let hash = seed_cas(&service, Bytes::from(zip_bytes), "archive variant").await?;
    service.conductor().cas().ensure_blob_materialized(hash).await.map_err(|source| {
        mediapm::MediaPmError::Workflow(format!("ensure blob materialized: {source}"))
    })?;

    let document = document_with_zip_variant(&hash.to_string(), rename_files);
    save_mediapm_document(&service.paths().mediapm_ncl, &document)?;
    let outcome = sync_library_with_test_terminal(&mut service, false).await;
    // Release the CAS `store/lock` before the caller inspects the tree.
    drop(service);
    Ok((outcome, root))
}

/// Asserts the sync refused the archive and that no file escaped [`TARGET_FOLDER`].
///
/// The escaped path is the workspace root, one level above the media folder the
/// archive is extracted into, so its absence is the on-disk proof that the
/// traversal was stopped before any write.
fn assert_traversal_refused_and_nothing_escaped(
    outcome: &Result<mediapm::SyncSummary, mediapm::MediaPmError>,
    workspace_root: &std::path::Path,
    offending_member: &str,
) {
    let escaped = workspace_root.join(ESCAPED_FILE);
    let escape_evidence =
        format!("escaped path '{}' exists: {}", escaped.display(), escaped.exists());

    let error = outcome.as_ref().err().unwrap_or_else(|| {
        panic!(
            "archive with member '{offending_member}' must be refused, got {outcome:?}; {escape_evidence}"
        )
    });
    let rendered = error.to_string();
    assert!(
        rendered.contains(offending_member),
        "the error must name the refused member '{offending_member}'; got: {rendered}"
    );

    assert!(
        !escaped.exists(),
        "member '{offending_member}' escaped the target folder: '{}' was written",
        escaped.display()
    );
    assert_eq!(
        std::fs::read(&escaped).ok().as_deref(),
        None,
        "member '{offending_member}' must not have written a body outside the target folder"
    );
}

/// A `../` member name must be refused, and nothing may be written one level
/// above the target media folder.
///
/// Before the fix this wrote `{workspace}/evil.txt` and reported a successful
/// sync; the error is the primary assertion, the missing file the proof.
#[tokio::test]
async fn zip_folder_variant_rejects_dotdot_member_and_writes_nothing_outside_target()
-> Result<(), mediapm::MediaPmError> {
    let (outcome, root) =
        sync_with_zip_members(&[("../evil.txt", ESCAPED_BODY)], Vec::new()).await?;
    assert_traversal_refused_and_nothing_escaped(&outcome, root.path(), "../evil.txt");
    Ok(())
}

/// The Windows-separator form `..\..\evil.txt` must be refused too: splitting
/// only on `/` would leave the whole string as one component that no longer
/// looks like traversal, yet still resolves outside the target on Windows.
#[tokio::test]
async fn zip_folder_variant_rejects_backslash_traversal_member() -> Result<(), mediapm::MediaPmError>
{
    let (outcome, root) =
        sync_with_zip_members(&[("..\\..\\evil.txt", ESCAPED_BODY)], Vec::new()).await?;
    assert_traversal_refused_and_nothing_escaped(&outcome, root.path(), "..\\..\\evil.txt");
    Ok(())
}

/// An absolute member name must be refused rather than silently re-rooted.
///
/// The pre-fix normalizer stripped the leading `/` and treated the member as
/// relative, so `/etc/evil.txt` wrote inside the media folder — an archive was
/// accepted whose declared layout was never honored.
#[tokio::test]
async fn zip_folder_variant_rejects_absolute_member_name() -> Result<(), mediapm::MediaPmError> {
    let (outcome, root) =
        sync_with_zip_members(&[("/etc/evil.txt", ESCAPED_BODY)], Vec::new()).await?;
    assert_traversal_refused_and_nothing_escaped(&outcome, root.path(), "/etc/evil.txt");
    Ok(())
}

/// A nested but legitimate member must still be extracted at its declared depth.
///
/// Guards the fix against over-rejection: the `..` rule must reject the
/// traversal component specifically, not any multi-component path.
#[tokio::test]
async fn zip_folder_variant_extracts_nested_member_under_target()
-> Result<(), mediapm::MediaPmError> {
    let (outcome, root) = sync_with_zip_members(&[("a/b/c.txt", NESTED_BODY)], Vec::new()).await?;
    let summary = outcome?;
    assert!(summary.materialized_paths > 0, "the nested member must materialize: {summary:?}");

    let extracted = root.path().join(TARGET_FOLDER).join("a").join("b").join("c.txt");
    let bytes = std::fs::read(&extracted).map_err(|error| {
        mediapm::MediaPmError::Workflow(format!(
            "read nested member '{}': {error}",
            extracted.display()
        ))
    })?;
    assert_eq!(bytes, NESTED_BODY, "nested member must land at a/b/c.txt under the target folder");
    Ok(())
}

/// A rename rule whose replacement emits `..` must be refused.
///
/// The replacement runs *after* path normalization, so it can re-introduce a
/// traversal component that no earlier stage ever saw.
#[tokio::test]
async fn zip_folder_variant_rejects_rename_rule_emitting_traversal()
-> Result<(), mediapm::MediaPmError> {
    let (outcome, root) = sync_with_zip_members(
        &[("cover.jpg", ESCAPED_BODY)],
        vec![HierarchyFolderRenameRule {
            pattern: "^cover".to_string(),
            replacement: "../$1".to_string(),
        }],
    )
    .await?;
    assert_traversal_refused_and_nothing_escaped(&outcome, root.path(), "../");
    Ok(())
}

/// An ordinary rename rule must still rewrite the leaf component.
///
/// The rejection added for the traversal case must not fire on a replacement
/// that stays inside one path component.
#[tokio::test]
async fn zip_folder_variant_applies_ordinary_rename_rule() -> Result<(), mediapm::MediaPmError> {
    let (outcome, root) = sync_with_zip_members(
        &[("cover.jpg", NESTED_BODY)],
        vec![HierarchyFolderRenameRule {
            pattern: "\\.jpg$".to_string(),
            replacement: ".jpeg".to_string(),
        }],
    )
    .await?;
    let summary = outcome?;
    assert!(summary.materialized_paths > 0, "the renamed member must materialize: {summary:?}");

    let renamed = root.path().join(TARGET_FOLDER).join("cover.jpeg");
    let bytes = std::fs::read(&renamed).map_err(|error| {
        mediapm::MediaPmError::Workflow(format!(
            "read renamed member '{}': {error}",
            renamed.display()
        ))
    })?;
    assert_eq!(bytes, NESTED_BODY, "the rename rule must rewrite the leaf component in place");
    assert!(
        !root.path().join(TARGET_FOLDER).join("cover.jpg").exists(),
        "the pre-rename name must not survive alongside the renamed member"
    );
    Ok(())
}
