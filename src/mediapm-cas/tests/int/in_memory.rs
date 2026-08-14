use bytes::Bytes;

use mediapm_cas::api::CasApi;
use mediapm_cas::new_in_memory_cas;

use crate::common::put_static;

#[tokio::test]
async fn new_in_memory_cas_factory_works() {
    let cas = new_in_memory_cas();
    let data = Bytes::from_static(b"factory test");
    let hash = cas.put(data.clone()).await.unwrap();
    let retrieved = cas.get(hash).await.unwrap();
    assert_eq!(retrieved, data);
}

#[tokio::test]
async fn clone_shares_state() {
    let cas1 = new_in_memory_cas();
    let cas2 = cas1.clone();

    let data = Bytes::from_static(b"shared");
    let hash = cas1.put(data.clone()).await.unwrap();
    // cas2 should see what cas1 put.
    let retrieved = cas2.get(hash).await.unwrap();
    assert_eq!(retrieved, data);
}

#[tokio::test]
async fn multi_step_workflow() {
    let cas = new_in_memory_cas();

    // Phase 1: store a few objects.
    let h1 = put_static(&cas, b"object-a").await;
    let h2 = put_static(&cas, b"object-b").await;
    let h3 = put_static(&cas, b"object-c").await;

    // Phase 2: verify all present.
    assert_eq!(cas.get(h1).await.unwrap(), Bytes::from_static(b"object-a"));
    assert_eq!(cas.get(h2).await.unwrap(), Bytes::from_static(b"object-b"));
    assert_eq!(cas.get(h3).await.unwrap(), Bytes::from_static(b"object-c"));

    // Phase 3: delete one.
    cas.delete(h2).await.unwrap();
    assert!(cas.get(h2).await.is_err());
    assert!(cas.get(h1).await.is_ok());
    assert!(cas.get(h3).await.is_ok());
}
