//! Shared async HTTP client for conductor tool-fetch operations.
//!
//! # Decoupling contract
//!
//! This module is **fully self-contained**: it depends on external crates only
//! (`reqwest`, `std`) and intentionally imports **nothing from `crate::`**.
//! It defines its own error type [`HttpClientError`] rather than using
//! [`ConductorError`].
//!
//! This design makes the module independently extractable into a standalone
//! crate. If you ever need to share this client outside `mediapm-conductor`,
//! copy the directory and adjust the `Cargo.toml` dependencies — no code
//! changes to the module body are required.
//!
//! # Error boundary
//!
//! The unit of encapsulation is [`HttpClientError`]. Every function returns
//! this type. The caller in `crate::tools::provider` maps it to
//! [`ConductorError`] at the call site, never inside this module.

use std::sync::OnceLock;
use std::time::Duration;

use reqwest::Client;

/// User-Agent header used for outbound HTTP requests.
pub(crate) const MEDIAPM_USER_AGENT: &str =
    concat!("mediapm/", env!("CARGO_PKG_VERSION"), " (+https://github.com/mediapm/mediapm)");

/// Default TCP connect timeout used for outbound HTTP requests.
const DEFAULT_CONNECT_TIMEOUT_SECONDS: u64 = 30;

/// Default request timeout used for payload downloads.
const DEFAULT_REQUEST_TIMEOUT_SECONDS: u64 = 60 * 30;

/// Environment variable used to override request timeout seconds.
const REQUEST_TIMEOUT_ENV: &str = "MEDIAPM_HTTP_TIMEOUT_SECONDS";

/// Errors produced by the HTTP client module.
///
/// This is a self-contained error type that does **not** reference
/// [`ConductorError`]. Callers map this to their own error domain at
/// the call site.
#[derive(Debug)]
pub enum HttpClientError {
    /// Building the `reqwest::Client` failed.
    ClientBuildFailed(String),
}

impl std::fmt::Display for HttpClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HttpClientError::ClientBuildFailed(msg) => {
                write!(f, "HTTP client build failed: {msg}")
            }
        }
    }
}

impl std::error::Error for HttpClientError {}

/// Process-wide shared `reqwest::Client` initialization state (follows redirects).
static SHARED_HTTP_CLIENT: OnceLock<Result<Client, HttpClientError>> = OnceLock::new();

/// Returns the process-wide shared async HTTP client (follows redirects).
pub(crate) fn shared_http_client() -> Result<&'static Client, &'static HttpClientError> {
    match SHARED_HTTP_CLIENT.get_or_init(|| build_shared_http_client(true)) {
        Ok(client) => Ok(client),
        Err(err) => Err(err),
    }
}

/// Process-wide shared no-redirect `reqwest::Client` initialization state.
#[allow(dead_code)]
static SHARED_NO_REDIRECT_HTTP_CLIENT: OnceLock<Result<Client, HttpClientError>> = OnceLock::new();

/// Returns the process-wide shared async HTTP client that does **not** follow
/// redirects. Use this when the caller needs to inspect redirect responses
/// (e.g. capture the `Location` header) rather than transparently following
/// them to the final destination.
#[allow(dead_code)]
pub(crate) fn shared_no_redirect_http_client() -> Result<&'static Client, &'static HttpClientError>
{
    match SHARED_NO_REDIRECT_HTTP_CLIENT.get_or_init(|| build_shared_http_client(false)) {
        Ok(client) => Ok(client),
        Err(err) => Err(err),
    }
}

/// Builds an HTTP client with optional redirect following.
fn build_shared_http_client(follow_redirects: bool) -> Result<Client, HttpClientError> {
    let timeout_seconds = std::env::var(REQUEST_TIMEOUT_ENV)
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .filter(|value| *value >= 30)
        .unwrap_or(DEFAULT_REQUEST_TIMEOUT_SECONDS);

    let mut builder = Client::builder()
        .connect_timeout(Duration::from_secs(DEFAULT_CONNECT_TIMEOUT_SECONDS))
        .timeout(Duration::from_secs(timeout_seconds))
        .user_agent(MEDIAPM_USER_AGENT);

    if !follow_redirects {
        builder = builder.redirect(reqwest::redirect::Policy::none());
    }

    builder.build().map_err(|source| {
        HttpClientError::ClientBuildFailed(format!(
            "building shared HTTP client for conductor failed: {source}"
        ))
    })
}
