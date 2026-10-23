//! Pins the teardown order every `TestConductor` owner follows.
//!
//! A fixture owns the directory its conductor writes into, so it stops that
//! conductor before the directory goes. Nothing at run time witnesses an
//! omission: this test crate installs no tracing subscriber, so the maintenance
//! loop's failure warning is never printed, and a missed teardown surfaces only
//! as a leftover temp directory when the race happens to land. This scan is the
//! witness instead, and it fails on the source that would forget.

use std::path::{Path, PathBuf};

/// Every test that builds a `TestConductor` stops it before its directory goes.
///
/// Each file is split at its test attributes, so a chunk runs from one test
/// attribute to the next and a chunk that constructs the fixture must also stop
/// it. The constructor call is what marks an owner: helpers take the fixture by
/// reference, so they never contain it. The stop is matched by its prefix rather
/// than one method name, because the fixture exposes a strict stop and a stop
/// that tolerates the client a test has deliberately broken, and both leave the
/// background pass stopped.
#[test]
fn every_fixture_owner_stops_its_conductor() {
    let tests_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    let mut offenders = Vec::new();
    for path in rust_sources(&tests_root) {
        let source = std::fs::read_to_string(&path).expect("read test source");
        for chunk in test_chunks(&source) {
            let fixtures = chunk.matches("TestConductor::").count();
            let stops = chunk.matches(".shutdown").count();
            if fixtures > stops {
                offenders.push(format!(
                    "{}: {} builds {fixtures} fixture(s) and stops {stops}",
                    path.display(),
                    chunk_name(&chunk),
                ));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "a fixture owns the directory its conductor writes into, so it must be \
         stopped before the directory goes: {offenders:#?}",
    );
}

/// Collects every `.rs` file under `dir`, depth first.
fn rust_sources(dir: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(current) = pending.pop() {
        for entry in std::fs::read_dir(&current).expect("read test directory") {
            let path = entry.expect("directory entry").path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                found.push(path);
            }
        }
    }
    found.sort();
    found
}

/// Splits `source` into one chunk per test attribute.
///
/// A chunk is the text from its own attribute to the next one, which is the
/// span a test's body lives in for the shape these files use: the attribute
/// opens the item and no later attribute belongs to it.
fn test_chunks(source: &str) -> Vec<String> {
    let mut chunks: Vec<String> = Vec::new();
    for line in source.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("#[test]") || trimmed.starts_with("#[tokio::test]") {
            chunks.push(String::new());
        }
        if let Some(current) = chunks.last_mut() {
            current.push_str(line);
            current.push('\n');
        }
    }
    chunks
}

/// Names the test a chunk holds, for the failure message.
///
/// The name is read from the first `fn` the chunk declares, which is the item
/// its attribute annotates.
fn chunk_name(chunk: &str) -> String {
    for line in chunk.lines() {
        if let Some(rest) = line.split("fn ").nth(1) {
            let name: String =
                rest.chars().take_while(|c| c.is_alphanumeric() || *c == '_').collect();
            if !name.is_empty() {
                return name;
            }
        }
    }
    "<unnamed>".to_string()
}
