use bytes::Bytes;
use mediapm_cas::api::{CasApi, ConstraintApi};
use mediapm_cas::hash::Hash;
use mediapm_cas::storage::in_memory::InMemoryCas;
/// Minimal test to isolate put/get hang.
use std::collections::BTreeSet;

#[tokio::test]
async fn put_then_get_works() {
    eprintln!("=== Starting put_then_get_works ===");
    let cas = InMemoryCas::new();
    eprintln!("=== CAS created ===");

    let data = Bytes::from("hello world");
    let hash = cas.put(data.clone()).await.expect("put should succeed");
    eprintln!("=== Put succeeded, hash={hash} ===");

    let retrieved = cas.get(hash).await.expect("get should succeed");
    eprintln!("=== Get succeeded ===");

    assert_eq!(retrieved, data);
    eprintln!("=== Test passed ===");
}

#[tokio::test]
async fn put_get_constraint_works() {
    eprintln!("=== Starting put_get_constraint_works ===");
    let cas = InMemoryCas::new();
    eprintln!("=== CAS created ===");

    let data = Bytes::from("test data");
    let hash = cas.put(data.clone()).await.expect("put should succeed");
    eprintln!("=== Put succeeded, hash={hash} ===");

    let retrieved = cas.get(hash).await.expect("get should succeed");
    eprintln!("=== Get succeeded, len={} ===", retrieved.len());

    eprintln!("=== About to call set_constraint ===");
    cas.set_constraint(hash, BTreeSet::from([Hash::empty()]))
        .await
        .expect("set_constraint should succeed");
    eprintln!("=== set_constraint succeeded ===");

    let constraint = cas.get_constraint(hash).await.expect("get_constraint should succeed");
    eprintln!("=== get_constraint returned {constraint:?} ===");
    assert_eq!(constraint, BTreeSet::from([Hash::empty()]));
    eprintln!("=== Test passed ===");
}
