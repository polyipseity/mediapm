//! # Every CAS hash a generated step names is declared as external data
//!
//! The conductor reclaims every CAS object that neither a recorded instance
//! output nor an `external_data` entry names. A managed import step names its
//! payload with a bare CAS hash rather than a reference the conductor can root
//! on its own, so the generated document has to declare that hash too —
//! otherwise the first full GC pass deletes the bytes the step reads.
//!
//! The assertion is made from outside the crate, on the document a real sync
//! left on disk: decode it, and require every step input that parses as a CAS
//! hash to have an `external_data` entry. That catches an unrooted bare hash
//! anywhere in the synthesized tree, not only in the import step this fixture
//! happens to exercise. A reference-shaped name is the conductor's own rule to
//! enforce, and it rejects the document when it is encoded.

use mediapm::MediaRuntimeStorage;
use mediapm_cas::{FileSystemCas, Hash};
use url::Url;

use crate::common::{read_generated_doc, service_with_cache, sync_library_with_test_terminal};
use crate::int::sync_idempotency_double_run::{SOURCE_URI, seed_probe_workspace};

/// A synced import source roots the payload hash its step is given, and keeps
/// rooting it across a re-sync.
///
/// The user-level guarantee: bytes an import step ingests survive the
/// conductor's garbage collection for as long as the document that names them
/// exists, so a library does not lose its source material to a maintenance pass.
/// The second sync is the load-bearing half: the tools phase rebuilds
/// `external_data` from usage on every run, so a declaration that survives only
/// the first run would leave the payload unrooted from the next run onward.
#[tokio::test]
async fn a_synced_import_source_declares_its_payload_hash() {
    let (mut service, _root, _cache) =
        service_with_cache(MediaRuntimeStorage::default()).await.expect("service");
    let uri = Url::parse(SOURCE_URI).expect("source URI parses");
    seed_probe_workspace(&mut service, &uri).await.expect("seed workspace");

    sync_library_with_test_terminal(&mut service, false).await.expect("first sync");
    assert_every_hash_reference_is_declared(&service);

    sync_library_with_test_terminal(&mut service, false).await.expect("second sync");
    assert_every_hash_reference_is_declared(&service);
}

/// Asserts the document a sync left on disk declares every CAS hash its steps
/// name as an input value.
///
/// The reference shape counts: an input value that is a bare hash, which the
/// `import` builtin reads as its payload. The counter exists so the assertion
/// cannot pass over a document with no such input.
fn assert_every_hash_reference_is_declared(service: &mediapm::MediaPmService<FileSystemCas>) {
    let document = read_generated_doc(service);

    let mut bare_hash_inputs = 0;
    for workflow in &document.workflows {
        for step in &workflow.steps {
            for (key, value) in &step.inputs {
                let Ok(hash) = value.parse::<Hash>() else { continue };
                bare_hash_inputs += 1;
                assert!(
                    document.external_data.contains_key(&hash),
                    "workflow '{}' step '{}' input '{key}' names CAS hash {hash}, which \
                     external_data does not declare",
                    workflow.name,
                    step.id,
                );
            }
        }
    }

    assert!(
        bare_hash_inputs > 0,
        "the fixture must produce at least one bare-hash step input, or this test proves nothing",
    );
    assert!(
        !document.external_data.is_empty(),
        "a document whose steps name CAS hashes must declare them",
    );
}
