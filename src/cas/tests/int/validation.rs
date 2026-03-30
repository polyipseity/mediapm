//! Integration tests for input validation and parser edge cases.
//!
//! Covers invalid constraint and hash parsing paths that should fail with
//! deterministic errors.

use std::collections::BTreeSet;
use std::str::FromStr;

use bytes::Bytes;
use mediapm_cas::{CasApi, Constraint, FileSystemCas, Hash};
use tempfile::tempdir;

#[tokio::test]
/// Ensures self-referential constraint rows are rejected.
async fn rejects_self_referential_constraint_candidate() {
    // Arrange
    let dir = tempdir().expect("tempdir");
    let cas = FileSystemCas::open_for_tests(dir.path()).await.expect("open cas");
    let target = cas.put(Bytes::from_static(b"target")).await.expect("put target");

    // Act
    let result = cas
        .set_constraint(Constraint {
            target_hash: target,
            potential_bases: BTreeSet::from([target]),
        })
        .await;

    // Assert
    assert!(result.is_err());
}

#[test]
/// Ensures hash parser rejects unsupported algorithm prefixes.
fn parsing_rejects_unknown_algorithm_name() {
    // Arrange + Act
    let result =
        Hash::from_str("sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

    // Assert
    assert!(result.is_err());
}

#[tokio::test]
/// Ensures empty constraint sets are treated as implicit unconstrained rows.
async fn empty_only_constraint_is_implicit_and_omitted() {
    // Arrange
    let dir = tempdir().expect("tempdir");
    let cas = FileSystemCas::open_for_tests(dir.path()).await.expect("open cas");
    let target = cas.put(Bytes::from_static(b"implicit-empty")).await.expect("put target");

    // Act
    let result = cas
        .set_constraint(Constraint { target_hash: target, potential_bases: BTreeSet::new() })
        .await;

    // Assert
    assert!(result.is_ok());
}
