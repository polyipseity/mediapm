//! What a run does when a variant resolves to a hash whose bytes are gone.
//!
//! Two failures look alike from the document's side and mean opposite things. A
//! variant no step has produced yet is a workflow that has not run: the entry
//! counts a miss and the run reports it as a missing path. A variant that
//! resolved to a hash the store does not hold is the opposite — the document and
//! the recorded state both promise bytes the library is supposed to have — so
//! the run fails instead of reporting a library that is quietly short a file.
//!
//! This module pins the single-file arm, which resolves one hash for one target.
//! The media-folder arm, which resolves the bytes of every variant it writes,
//! carries the same distinction and proves it where its rows are asserted:
//! `tests_overall_row_finish::the_run_fails_when_a_folder_variant_names_bytes_the_store_lost`.

use super::tests_common::{open_hierarchy_cas, resolvable_media_document};
use super::*;
use crate::paths::MediaPmPaths;

/// Media id the shared fixtures bind their entries to.
const MEDIA_ID: &str = "src1";

/// Hierarchy path of the single-file fixture's entry, which is also the key the
/// error names as the library target.
const FILE_ENTRY_PATH: &str = "song";

/// A hash nothing ever stores, so every read of it is the failure under test.
///
/// Derived rather than spelled out: a literal hash that happened to appear in a
/// fixture the run also stores would turn this test into a different one.
fn absent_hash() -> Hash {
    Hash::from_content(b"a-variant-nobody-stored")
}

/// Runs `sync_hierarchy` over `document` with no progress output.
///
/// Returns the result rather than one arm of it, because the point of the test
/// is which arm a lost object lands in.
async fn run_sync_result(
    paths: &MediaPmPaths,
    document: &MediaPmDocument,
    cas: &FileSystemCas,
) -> Result<MaterializeReport, MediaPmError> {
    sync_hierarchy(
        paths,
        document,
        &mut MediaPmState::default(),
        cas,
        false,
        &ConductorState::new_empty(),
        &NickelDocument::default(),
        None,
        None,
    )
    .await
}

/// A single-file entry whose variant resolves to bytes the store lost, fails
/// the run and names what is missing.
///
/// The entry is otherwise writable: it has one declared variant, the hierarchy
/// names it, and the only thing wrong is that the hash behind it is absent. A
/// run that counted this as a missing path would report a library the document
/// asked for and could not get, which is the shape the retention rule must never
/// leave behind.
#[tokio::test]
async fn a_missing_store_object_for_a_resolved_variant_is_an_error() {
    let root = mediapm_utils::temp::artifact_dir().expect("artifact dir");
    let paths = MediaPmPaths::from_root(root.path());
    let cas = open_hierarchy_cas(&paths).await;
    let absent = absent_hash();
    let document = resolvable_media_document(&absent.to_string());

    let result = run_sync_result(&paths, &document, &cas).await;

    let error = result.expect_err(
        "a declared hash with no bytes behind it must fail the run rather than count a miss",
    );
    let text = error.to_string();
    for expected in [absent.to_string(), MEDIA_ID.to_string(), "default".to_string()] {
        assert!(
            text.contains(&expected),
            "the error must name '{expected}' so the lost object can be found; got: {text}"
        );
    }
    assert!(
        text.contains(FILE_ENTRY_PATH),
        "the error must name the library target the bytes were headed for; got: {text}"
    );
}
