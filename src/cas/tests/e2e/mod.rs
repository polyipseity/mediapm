//! End-to-end tests exercising realistic multi-step CAS workflows.

use std::future::Future;
use std::time::Duration;

/// Runs an async test body with a hard timeout to prevent hangs.
///
/// The helper name is historical; the current timeout is 25 seconds to allow
/// slower CI environments enough headroom for filesystem + optimization tests.
pub(super) async fn run_with_15s_timeout<F, T>(future: F) -> T
where
    F: Future<Output = T>,
{
    tokio::time::timeout(Duration::from_secs(25), future).await.expect("test exceeded 25s timeout")
}

/// High-load and burst-workload scenarios.
mod high_load;
/// Full lifecycle optimize/reconstruct scenarios.
mod lifecycle;
/// Performance-regression profile scenarios.
mod perf_profile;
/// Recovery and repair scenarios.
mod recovery;
/// Domain-grouped long-form scenario coverage.
mod scenarios;
