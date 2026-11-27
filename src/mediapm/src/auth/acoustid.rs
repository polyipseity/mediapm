//! `AcoustID` API key auth service.
//!
//! Login probes the key with the strict lookup operation unless the caller
//! explicitly chooses the offline path. The key lives only in the unified
//! auth file.

use std::path::Path;

use mediapm_conductor::http::client::shared_http_client;
use serde::{Deserialize, Serialize};

use super::error::AuthError;
use super::prompt::SecretPrompt;
use super::service::{AuthOutcome, AuthServiceId};
use super::store::{AcoustIdCredential, load_auth_file, remove_service, save_auth_file};
use crate::builtins::media_tagger::DEFAULT_ACOUSTID_ENDPOINT;

/// Debug-only seam redirecting the lookup probe to a test server.
const ENDPOINT_ENV: &str = "MEDIAPM_ACOUSTID_BASE";

/// Fixed fingerprint used only to validate a key.
const PROBE_FINGERPRINT: &str = "1";

/// Fixed duration paired with [`PROBE_FINGERPRINT`] for the validation probe.
const PROBE_DURATION_SECONDS: u32 = 1;

/// Returns the effective `AcoustID` lookup endpoint.
///
/// In a debug build the value is the `MEDIAPM_ACOUSTID_BASE` override when
/// set, read per call (never cached in a `OnceLock`). A release build ignores
/// the variable (`cfg!(debug_assertions)` is false), so the environment cannot
/// redirect the validation probe in a shipped binary.
fn endpoint() -> String {
    if cfg!(debug_assertions) {
        std::env::var(ENDPOINT_ENV).unwrap_or_else(|_| DEFAULT_ACOUSTID_ENDPOINT.to_owned())
    } else {
        DEFAULT_ACOUSTID_ENDPOINT.to_owned()
    }
}

/// Wire-format query parameters for the key-validation probe.
///
/// `Debug` is deliberately not derived: `client` carries the secret and must
/// never enter a log or diagnostic dump.
#[derive(Serialize)]
struct ProbeQuery<'a> {
    /// API key issued by `AcoustID`.
    client: &'a str,
    /// Fixed fingerprint used for the probe.
    fingerprint: &'a str,
    /// Fixed duration paired with the fingerprint.
    duration: u32,
    /// Requested lookup metadata projections.
    meta: &'a str,
    /// Output response format.
    format: &'a str,
}

/// Minimal lookup response shape used by the strict probe.
#[derive(Debug, Deserialize)]
struct ProbeResponse {
    /// API status string (`ok` on success).
    status: String,
}

/// Validates one `AcoustID` API key through the strict lookup probe.
///
/// # HTTP client policy
///
/// Uses the process-wide shared client from
/// [`shared_http_client`].
/// Connection pooling, TLS reuse, and DNS caching are managed centrally.
/// Do NOT create a [`reqwest::Client`] locally — always use the shared instance.
///
/// # Errors
///
/// Returns [`AuthError::Service`] on HTTP client, transport, non-2xx, JSON
/// decode, or non-`ok` status failures. The key is never included in the
/// error text.
pub async fn validate_api_key(endpoint: &str, api_key: &str) -> Result<(), AuthError> {
    let http_client = shared_http_client().map_err(|error| {
        AuthError::Service(format!("AcoustID HTTP client unavailable: {error}"))
    })?;
    let payload = http_client
        .get(endpoint)
        .query(&ProbeQuery {
            client: api_key,
            fingerprint: PROBE_FINGERPRINT,
            duration: PROBE_DURATION_SECONDS,
            meta: "recordings releases",
            format: "json",
        })
        .send()
        .await
        .map_err(|error| AuthError::Service(format!("AcoustID request failed: {error}")))?
        .error_for_status()
        .map_err(|error| {
            AuthError::Service(format!("AcoustID returned a non-success status: {error}"))
        })?
        .text()
        .await
        .map_err(|error| AuthError::Service(format!("AcoustID response body failed: {error}")))?;
    let response = serde_json::from_str::<ProbeResponse>(&payload).map_err(|error| {
        AuthError::Service(format!("AcoustID response JSON was invalid: {error}"))
    })?;
    if !response.status.eq_ignore_ascii_case("ok") {
        return Err(AuthError::Service(format!(
            "AcoustID rejected the API key with status '{}'",
            response.status
        )));
    }
    Ok(())
}

/// Prompts for an `AcoustID` API key, optionally probes it, and stores it.
///
/// # Errors
///
/// Returns [`AuthError`] when the prompt, probe, load, or save fails.
pub async fn login(
    auth_file: &Path,
    prompt: &dyn SecretPrompt,
    offline: bool,
) -> Result<AuthOutcome, AuthError> {
    let api_key = prompt.read_secret("AcoustID API key: ")?;
    if !offline {
        validate_api_key(&endpoint(), &api_key).await?;
    }
    let mut auth = load_auth_file(auth_file)?.unwrap_or_default();
    auth.services.acoustid = Some(AcoustIdCredential { api_key });
    save_auth_file(auth_file, &auth)?;
    let message = if offline {
        "AcoustID API key stored without validation".to_owned()
    } else {
        "AcoustID API key validated and stored".to_owned()
    };
    Ok(AuthOutcome { message })
}

/// Reports whether the auth file holds an `AcoustID` key, without printing it.
///
/// # Errors
///
/// Returns [`AuthError`] when the auth file cannot be read or decoded.
pub fn status(auth_file: &Path) -> Result<AuthOutcome, AuthError> {
    let logged_in = load_auth_file(auth_file)?.is_some_and(|auth| auth.services.acoustid.is_some());
    let message = if logged_in { "AcoustID: logged in" } else { "AcoustID: not logged in" };
    Ok(AuthOutcome { message: message.to_owned() })
}

/// Removes the stored `AcoustID` key. There is no remote revocation step.
///
/// # Errors
///
/// Returns [`AuthError`] when the auth file cannot be read, decoded, or saved.
pub fn logout(auth_file: &Path) -> Result<AuthOutcome, AuthError> {
    remove_service(auth_file, AuthServiceId::Acoustid.as_str())?;
    Ok(AuthOutcome { message: "AcoustID: logged out".to_owned() })
}

/// Returns the stored `AcoustID` key, or `None` when not logged in.
///
/// # Errors
///
/// Returns [`AuthError`] when the auth file cannot be read or decoded.
pub fn api_key(auth_file: &Path) -> Result<Option<String>, AuthError> {
    Ok(load_auth_file(auth_file)?.and_then(|auth| auth.services.acoustid).map(|auth| auth.api_key))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Mutex;

    use crate::auth::store::{AuthFile, AuthServices};

    // Only the seam test mutates the process env; probe tests pass their
    // endpoint explicitly and never read the seam.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[tokio::test]
    async fn strict_probe_accepts_an_ok_response() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::query_param("client", "valid-key"))
            .and(wiremock::matchers::query_param("fingerprint", PROBE_FINGERPRINT))
            .and(wiremock::matchers::query_param("duration", PROBE_DURATION_SECONDS.to_string()))
            .and(wiremock::matchers::query_param("meta", "recordings releases"))
            .and(wiremock::matchers::query_param("format", "json"))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "ok", "results": []
            })))
            .mount(&server)
            .await;
        validate_api_key(&server.uri(), "valid-key").await.expect("valid key");
    }

    #[tokio::test]
    async fn strict_probe_rejects_an_error_response() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .respond_with(wiremock::ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "status": "error", "error": { "code": 4, "message": "invalid API key" }
            })))
            .mount(&server)
            .await;
        assert!(validate_api_key(&server.uri(), "bad-key").await.is_err());
    }

    #[tokio::test]
    async fn offline_login_stores_a_key_without_a_probe() {
        let dir = mediapm_utils::temp::output_dir().expect("temp dir");
        let path = dir.path().join("auth.json");
        let prompt = crate::auth::prompt::FixedPrompt::new(vec!["offline-key".to_owned()]);
        let outcome = login(&path, &prompt, true).await.expect("offline login");
        assert!(outcome.message.contains("stored without validation"));
        let auth = load_auth_file(&path).expect("load").expect("present");
        assert_eq!(auth.services.acoustid.expect("key").api_key, "offline-key");
    }

    #[test]
    fn status_api_key_and_logout_use_only_the_auth_file() {
        let dir = mediapm_utils::temp::output_dir().expect("temp dir");
        let path = dir.path().join("auth.json");
        assert_eq!(status(&path).expect("status").message, "AcoustID: not logged in");
        assert!(api_key(&path).expect("api key").is_none());

        let auth = AuthFile {
            services: AuthServices {
                acoustid: Some(AcoustIdCredential { api_key: "top-secret".to_owned() }),
                ..AuthServices::default()
            },
        };
        save_auth_file(&path, &auth).expect("seed");
        let outcome = status(&path).expect("status");
        assert_eq!(outcome.message, "AcoustID: logged in");
        assert!(!format!("{outcome:?}").contains("top-secret"));
        assert_eq!(api_key(&path).expect("api key").as_deref(), Some("top-secret"));

        let outcome = logout(&path).expect("logout");
        assert!(outcome.message.contains("logged out"));
        assert!(api_key(&path).expect("api key").is_none());
    }

    #[cfg(debug_assertions)]
    #[test]
    fn endpoint_override_is_read_per_call_in_debug_builds() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        // SAFETY: the process env lock serializes this mutation scope against
        // the other readers and writers of MEDIAPM_ACOUSTID_BASE.
        unsafe {
            std::env::set_var(ENDPOINT_ENV, "http://first.invalid");
        }
        assert_eq!(endpoint(), "http://first.invalid");
        unsafe {
            std::env::set_var(ENDPOINT_ENV, "http://second.invalid");
        }
        assert_eq!(endpoint(), "http://second.invalid");
        unsafe {
            std::env::remove_var(ENDPOINT_ENV);
        }
        assert_eq!(endpoint(), DEFAULT_ACOUSTID_ENDPOINT);
    }
}
