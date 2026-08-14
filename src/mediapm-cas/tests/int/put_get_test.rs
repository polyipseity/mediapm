use bytes::Bytes;
use std::collections::BTreeSet;

use mediapm_cas::Hash;
use mediapm_cas::api::{CasApi, ConstraintApi};
use mediapm_cas::new_in_memory_cas;

#[tokio::test]
async fn put_then_get_works() {
    let cas = new_in_memory_cas();

    let data = Bytes::from("hello world");
    let hash = cas.put(data.clone()).await.expect("put should succeed");

    let retrieved = cas.get(hash).await.expect("get should succeed");

    assert_eq!(retrieved, data);
}

#[tokio::test]
async fn put_get_constraint_works() {
    let cas = new_in_memory_cas();

    let data = Bytes::from("test data");
    let hash = cas.put(data.clone()).await.expect("put should succeed");

    let retrieved = cas.get(hash).await.expect("get should succeed");
    assert_eq!(retrieved, data);

    cas.set_constraint(hash, BTreeSet::from([Hash::empty()]))
        .await
        .expect("set_constraint should succeed");

    let constraint = cas.get_constraint(hash).await.expect("get_constraint should succeed");
    assert_eq!(constraint, BTreeSet::from([Hash::empty()]));
}
