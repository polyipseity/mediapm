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
/// error text: `reqwest::Error::Display` appends the request URL, so the
/// transport and status paths strip it with [`reqwest::Error::without_url`].
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
        .map_err(|error| {
            let error = error.without_url();
            AuthError::Service(format!("AcoustID request failed: {error}"))
        })?
        .error_for_status()
        .map_err(|error| {
            let error = error.without_url();
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
    login_with_endpoint(auth_file, prompt, offline, &endpoint()).await
}

/// Same as [`login`] with an explicit probe endpoint for hermetic callers.
///
/// # Errors
///
/// Returns [`AuthError`] when the prompt, probe, load, or save fails.
async fn login_with_endpoint(
    auth_file: &Path,
    prompt: &dyn SecretPrompt,
    offline: bool,
    endpoint: &str,
) -> Result<AuthOutcome, AuthError> {
    let api_key = prompt.read_secret("AcoustID API key: ")?;
    if !offline {
        validate_api_key(endpoint, &api_key).await?;
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

    use std::ffi::OsString;

    use crate::auth::store::{AuthFile, AuthServices};

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
        const PROBE_KEY: &str = "probe-key-that-must-not-leak";
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::query_param("client", PROBE_KEY))
            .respond_with(wiremock::ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "status": "error", "error": { "code": 4, "message": "invalid API key" }
            })))
            .mount(&server)
            .await;
        let error = validate_api_key(&server.uri(), PROBE_KEY).await.expect_err("probe must fail");
        let message = error.to_string();
        assert!(message.contains("AcoustID"), "{message}");
        assert!(!message.contains(PROBE_KEY), "probe key leaked into error: {message}");
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

    #[tokio::test]
    async fn online_login_validates_and_stores_a_key() {
        const KEY: &str = "online-key";
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::query_param("client", KEY))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "ok", "results": []
            })))
            .mount(&server)
            .await;
        let dir = mediapm_utils::temp::output_dir().expect("temp dir");
        let path = dir.path().join("auth.json");
        let prompt = crate::auth::prompt::FixedPrompt::new(vec![KEY.to_owned()]);
        let outcome =
            login_with_endpoint(&path, &prompt, false, &server.uri()).await.expect("online login");
        assert_eq!(outcome.message, "AcoustID API key validated and stored");
        assert_eq!(api_key(&path).expect("api key").as_deref(), Some(KEY));
    }

    #[tokio::test]
    async fn failed_probe_leaves_no_auth_file() {
        const KEY: &str = "rejected-key";
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::query_param("client", KEY))
            .respond_with(wiremock::ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "status": "error", "error": { "code": 4, "message": "invalid API key" }
            })))
            .mount(&server)
            .await;
        let dir = mediapm_utils::temp::output_dir().expect("temp dir");
        let path = dir.path().join("auth.json");
        let prompt = crate::auth::prompt::FixedPrompt::new(vec![KEY.to_owned()]);
        let error = login_with_endpoint(&path, &prompt, false, &server.uri())
            .await
            .expect_err("probe must reject the key");
        let message = error.to_string();
        assert!(message.contains("AcoustID"), "{message}");
        assert!(!message.contains(KEY), "probe key leaked into error: {message}");
        assert!(!path.exists(), "a failed probe must not create an auth file");
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

    /// Restores `ENDPOINT_ENV` to `previous` under the process env lock.
    ///
    /// Test scaffolding for seeding a pre-existing value before an
    /// [`EndpointOverride`] is installed; it takes no lock until drop, so the
    /// two guards never nest a non-reentrant mutex acquisition.
    #[cfg(debug_assertions)]
    struct RestoreEndpoint(Option<OsString>);

    #[cfg(debug_assertions)]
    impl Drop for RestoreEndpoint {
        fn drop(&mut self) {
            let _lock = crate::example_isolation::lock_process_env();
            // SAFETY: the process env lock is held for this restore, and no
            // other guard can be alive when this drop runs.
            unsafe {
                match &self.0 {
                    Some(value) => std::env::set_var(ENDPOINT_ENV, value),
                    None => std::env::remove_var(ENDPOINT_ENV),
                }
            }
        }
    }

    /// Installs an endpoint override under the process-wide env lock.
    ///
    /// The guard owns the lock for its whole lifetime; its drop restores the
    /// value read before the first mutation, including during unwinding.
    #[cfg(debug_assertions)]
    struct EndpointOverride {
        previous: Option<OsString>,
        _env_lock: parking_lot::MutexGuard<'static, ()>,
    }

    #[cfg(debug_assertions)]
    impl EndpointOverride {
        fn install(value: &str) -> Self {
            let env_lock = crate::example_isolation::lock_process_env();
            let previous = std::env::var_os(ENDPOINT_ENV);
            // SAFETY: the process env lock is held for this write, and every
            // reader/writer of this seam shares that lock.
            unsafe { std::env::set_var(ENDPOINT_ENV, value) };
            Self { previous, _env_lock: env_lock }
        }
    }

    #[cfg(debug_assertions)]
    impl Drop for EndpointOverride {
        fn drop(&mut self) {
            // SAFETY: the guard still owns the process env lock; its fields
            // are dropped only after this method returns.
            unsafe {
                match &self.previous {
                    Some(value) => std::env::set_var(ENDPOINT_ENV, value),
                    None => std::env::remove_var(ENDPOINT_ENV),
                }
            }
        }
    }

    #[cfg(debug_assertions)]
    #[test]
    fn endpoint_override_is_read_per_call_in_debug_builds() {
        let override_env = EndpointOverride::install("http://first.invalid");
        let previous = override_env.previous.clone();
        assert_eq!(endpoint(), "http://first.invalid");
        // SAFETY: `override_env` owns the process env lock for this write.
        unsafe { std::env::set_var(ENDPOINT_ENV, "http://second.invalid") };
        assert_eq!(endpoint(), "http://second.invalid");
        drop(override_env);

        // A pre-existing override is restored, not silently replaced by the
        // default; the default applies only when there was no prior value.
        let _lock = crate::example_isolation::lock_process_env();
        assert_eq!(std::env::var_os(ENDPOINT_ENV), previous);
        if previous.is_none() {
            assert_eq!(endpoint(), DEFAULT_ACOUSTID_ENDPOINT);
        }
    }

    #[cfg(debug_assertions)]
    #[test]
    fn endpoint_override_restores_preexisting_value() {
        const PRIOR: &str = "http://caller.invalid";
        let ambient = {
            let _lock = crate::example_isolation::lock_process_env();
            let ambient = std::env::var_os(ENDPOINT_ENV);
            // SAFETY: the process env lock is held for this write.
            unsafe { std::env::set_var(ENDPOINT_ENV, PRIOR) };
            ambient
        };
        let _restore_ambient = RestoreEndpoint(ambient);

        let override_env = EndpointOverride::install("http://inside.invalid");
        assert_eq!(override_env.previous, Some(OsString::from(PRIOR)));
        assert_eq!(endpoint(), "http://inside.invalid");
        drop(override_env);

        let _lock = crate::example_isolation::lock_process_env();
        assert_eq!(std::env::var_os(ENDPOINT_ENV), Some(OsString::from(PRIOR)));
    }

    #[cfg(debug_assertions)]
    #[test]
    fn endpoint_override_restores_preexisting_value_on_panic() {
        let ambient = {
            let _lock = crate::example_isolation::lock_process_env();
            std::env::var_os(ENDPOINT_ENV)
        };
        let result = std::panic::catch_unwind(|| {
            let _override = EndpointOverride::install("http://panic.invalid");
            assert_eq!(endpoint(), "http://panic.invalid");
            panic!("intentional endpoint override test panic");
        });
        assert!(result.is_err(), "the intentionally panicking scope must unwind");

        let _lock = crate::example_isolation::lock_process_env();
        assert_eq!(std::env::var_os(ENDPOINT_ENV), ambient);
    }
}
