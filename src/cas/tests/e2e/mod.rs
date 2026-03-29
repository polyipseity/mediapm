//! End-to-end tests exercising realistic multi-step CAS workflows.

use std::future::Future;
use std::time::Duration;

pub(super) async fn run_with_15s_timeout<F, T>(future: F) -> T
where
    F: Future<Output = T>,
{
    tokio::time::timeout(Duration::from_secs(25), future).await.expect("test exceeded 25s timeout")
}

mod high_load;
mod lifecycle;
mod perf_profile;
mod recovery;
