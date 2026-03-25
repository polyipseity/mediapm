//! End-to-end workflow tests for mediapm phase composition.

use mediapm::{MediaPmApi, MediaPmService};
use url::Url;

#[tokio::test]
async fn sync_library_executes_a_conductor_pass() {
    let service = MediaPmService::new_in_memory();

    let summary = service.sync_library().await.expect("sync should succeed");

    assert_eq!(summary.executed_instances, 1);
}

#[tokio::test]
async fn source_scheme_validation_is_enforced() {
    let service = MediaPmService::new_in_memory();
    let invalid = Url::parse("ftp://example.com/file.mp4").expect("url must parse");

    let result = service.process_source(invalid, true).await;

    assert!(result.is_err());
}
