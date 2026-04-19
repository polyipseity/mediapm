//! Process-wide shared async HTTP client for `mediapm`.
//!
//! The downloader and metadata/tagging integrations intentionally reuse one
//! `reqwest::Client` instance for the lifetime of the process so connection
//! pooling, TLS session reuse, and DNS caching remain stable under concurrent
//! sync operations.

use std::sync::OnceLock;
use std::time::Duration;

use reqwest::Client;

use crate::error::MediaPmError;

/// User-Agent header used for outbound HTTP requests.
pub(crate) const MEDIAPM_USER_AGENT: &str = "mediapm/0.0.0 (+https://github.com/mediapm/mediapm)";

/// Default TCP connect timeout used for outbound HTTP requests.
const DEFAULT_CONNECT_TIMEOUT_SECONDS: u64 = 30;

/// Default request timeout used for payload and metadata requests.
///
/// The value is intentionally generous to support large payloads across slow
/// links, and can be overridden per environment.
const DEFAULT_REQUEST_TIMEOUT_SECONDS: u64 = 60 * 30;

/// Environment variable used to override request timeout seconds.
const REQUEST_TIMEOUT_ENV: &str = "MEDIAPM_DOWNLOAD_TIMEOUT_SECONDS";

/// Process-wide shared `reqwest::Client` initialization state.
static SHARED_HTTP_CLIENT: OnceLock<Result<Client, String>> = OnceLock::new();

/// Returns the process-wide shared async HTTP client.
///
/// The first successful call constructs the client and all later calls return
/// the same instance, guaranteeing one client object for the entire process
/// lifetime.
pub(crate) fn shared_http_client() -> Result<&'static Client, MediaPmError> {
    match SHARED_HTTP_CLIENT.get_or_init(build_shared_http_client) {
        Ok(client) => Ok(client),
        Err(message) => Err(MediaPmError::Workflow(message.clone())),
    }
}

/// Builds the shared HTTP client once.
fn build_shared_http_client() -> Result<Client, String> {
    let timeout_seconds = std::env::var(REQUEST_TIMEOUT_ENV)
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .filter(|value| *value >= 30)
        .unwrap_or(DEFAULT_REQUEST_TIMEOUT_SECONDS);

    Client::builder()
        .connect_timeout(Duration::from_secs(DEFAULT_CONNECT_TIMEOUT_SECONDS))
        .timeout(Duration::from_secs(timeout_seconds))
        .user_agent(MEDIAPM_USER_AGENT)
        .build()
        .map_err(|source| format!("building shared HTTP client for mediapm failed: {source}"))
}
