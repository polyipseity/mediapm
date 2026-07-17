//! Shared test utilities for `mediapm-cas` integration tests.
//!
//! Provides a lazily-initialized shared tokio runtime so simple async tests
//! can avoid per-test runtime creation overhead.

use std::sync::LazyLock;

/// Returns a reference to the shared lazily-initialized tokio runtime.
///
/// Multiple threads may call `block_on` concurrently on the same runtime
/// — `Runtime::block_on` takes `&self`, so each OS thread enters the
/// scheduler independently.
pub(crate) fn shared_runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: LazyLock<tokio::runtime::Runtime> =
        LazyLock::new(|| tokio::runtime::Runtime::new().expect("build shared tokio runtime"));
    &RUNTIME
}
