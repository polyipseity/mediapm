//! Shared test utilities for the `mediapm` crate.

/// Runs one async future on a single-thread Tokio runtime.
pub(crate) fn run_async<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime")
        .block_on(future)
}
