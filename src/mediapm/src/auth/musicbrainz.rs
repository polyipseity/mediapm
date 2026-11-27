//! `MusicBrainz` OAuth (PKCE + loopback) implementation.
//!
//! Client credentials and the user token live only in the unified auth file.

use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use rand::RngCore;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::error::AuthError;
use super::prompt::SecretPrompt;
use super::service::{AuthOutcome, AuthServiceId};
use super::store::{
    MusicBrainzCredential, MusicBrainzToken, load_auth_file, remove_service, save_auth_file,
};

/// Default `MusicBrainz` OAuth base URL.
pub const DEFAULT_MUSICBRAINZ_OAUTH_BASE_URL: &str = "https://metabrainz.org";

/// Registration URL shown when no stored client credentials exist.
pub const MUSICBRAINZ_APP_REGISTRATION_URL: &str = "https://metabrainz.org/profile/applications";

/// OAuth scopes mediapm requests for read-only `MusicBrainz` user data.
pub const MUSICBRAINZ_OAUTH_SCOPES: &str =
    "profile musicbrainz:tag musicbrainz:rating musicbrainz:collection";

/// Errors produced by the `MusicBrainz` HTTP and loopback flows.
#[derive(Debug, Error)]
pub enum MusicBrainzAuthError {
    /// No user token is stored.
    #[error("not logged in to MusicBrainz; run 'mediapm auth musicbrainz login'")]
    NotLoggedIn,
    /// Refresh token was rejected and the stored record must be cleared.
    #[error("MusicBrainz login expired; run 'mediapm auth musicbrainz login' again")]
    LoginExpired,
    /// HTTP client could not be obtained.
    #[error("MusicBrainz HTTP client unavailable: {message}")]
    HttpClient {
        /// Error message.
        message: String,
    },
    /// HTTP request failed.
    #[error("MusicBrainz HTTP request failed: {message}")]
    HttpRequest {
        /// Error message.
        message: String,
    },
    /// Token endpoint returned an unexpected response.
    #[error("MusicBrainz token endpoint error: {message}")]
    TokenEndpoint {
        /// Error message.
        message: String,
    },
    /// OAuth callback flow failed.
    #[error("MusicBrainz OAuth callback failed: {message}")]
    Callback {
        /// Error message.
        message: String,
    },
    /// The authorization server returned an error.
    #[error("MusicBrainz OAuth authorization failed: {message}")]
    Authorization {
        /// Error message.
        message: String,
    },
    /// The loopback server could not bind.
    #[error("failed to bind MusicBrainz OAuth loopback listener: {source}")]
    BindLoopback {
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },
    /// Opening the browser failed.
    #[error("failed to open browser for MusicBrainz login: {message}")]
    OpenBrowser {
        /// Error message.
        message: String,
    },
}

impl std::fmt::Debug for MusicBrainzOAuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MusicBrainzOAuthConfig")
            .field("base_url", &self.base_url)
            .field("client_id", &self.client_id)
            .field("client_secret", &"[REDACTED]")
            .finish()
    }
}

/// OAuth parameters for one `MusicBrainz` request.
#[derive(Clone)]
pub struct MusicBrainzOAuthConfig {
    /// Base URL, e.g. `https://metabrainz.org`.
    pub base_url: String,
    /// OAuth client id from the unified auth file.
    pub client_id: String,
    /// OAuth client secret from the unified auth file.
    pub client_secret: String,
}

/// Returns the stored client id and secret, or an error naming the login command.
///
/// # Errors
///
/// Returns [`AuthError`] when the auth file cannot be read or the record has
/// no client credentials. There is no environment or dotenv fallback.
fn credentials(auth_file: &Path) -> Result<(String, String), AuthError> {
    let Some(credential) = load_auth_file(auth_file)?.and_then(|auth| auth.services.musicbrainz)
    else {
        return Err(missing_credentials_error());
    };
    if credential.client_id.trim().is_empty() || credential.client_secret.trim().is_empty() {
        return Err(missing_credentials_error());
    }
    Ok((credential.client_id, credential.client_secret))
}

fn missing_credentials_error() -> AuthError {
    AuthError::Service(format!(
        "MusicBrainz client credentials are missing; run 'mediapm auth musicbrainz login' and register an application at {MUSICBRAINZ_APP_REGISTRATION_URL}"
    ))
}

fn cannot_revoke_error() -> AuthError {
    AuthError::Service(
        "cannot revoke without stored client credentials; use --offline to remove locally"
            .to_owned(),
    )
}

fn not_logged_in_error() -> AuthError {
    AuthError::Service(
        "not logged in to MusicBrainz; run 'mediapm auth musicbrainz login'".to_owned(),
    )
}

fn service_error(error: &MusicBrainzAuthError) -> AuthError {
    AuthError::Service(error.to_string())
}

/// PKCE verifier/challenge pair (S256).
#[derive(Clone)]
pub struct PkcePair {
    /// URL-safe verifier string.
    pub verifier: String,
    /// URL-safe S256 challenge string.
    pub challenge: String,
}

impl std::fmt::Debug for PkcePair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PkcePair")
            .field("verifier", &"[REDACTED]")
            .field("challenge", &self.challenge)
            .finish()
    }
}

/// Generates a PKCE S256 verifier/challenge pair.
#[must_use]
pub fn generate_pkce_pair() -> PkcePair {
    let mut bytes = [0_u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    let verifier = URL_SAFE_NO_PAD.encode(bytes);
    let digest = Sha256::digest(verifier.as_bytes());
    let challenge = URL_SAFE_NO_PAD.encode(digest);
    PkcePair { verifier, challenge }
}

fn current_unix_time() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs())
}

fn token_endpoint(base_url: &str) -> String {
    format!("{}/oauth2/token", base_url.trim_end_matches('/'))
}

fn authorize_endpoint(base_url: &str) -> String {
    format!("{}/oauth2/authorize", base_url.trim_end_matches('/'))
}

fn revoke_endpoint(base_url: &str) -> String {
    format!("{}/oauth2/revoke", base_url.trim_end_matches('/'))
}

fn build_authorize_url(
    config: &MusicBrainzOAuthConfig,
    redirect_uri: &str,
    state: &str,
    pkce: &PkcePair,
) -> String {
    let mut url = reqwest::Url::parse(&authorize_endpoint(&config.base_url))
        .expect("valid base URL for authorize endpoint");
    url.query_pairs_mut()
        .append_pair("response_type", "code")
        .append_pair("client_id", &config.client_id)
        .append_pair("redirect_uri", redirect_uri)
        .append_pair("scope", MUSICBRAINZ_OAUTH_SCOPES)
        .append_pair("state", state)
        .append_pair("code_challenge", &pkce.challenge)
        .append_pair("code_challenge_method", "S256");
    url.to_string()
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    #[serde(default)]
    refresh_token: Option<String>,
    #[serde(default)]
    expires_in: Option<u64>,
    #[serde(default)]
    scope: Option<String>,
    #[serde(default)]
    token_type: Option<String>,
}

fn parse_scopes(scope: Option<&str>) -> Vec<String> {
    scope
        .unwrap_or(MUSICBRAINZ_OAUTH_SCOPES)
        .split_whitespace()
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect()
}

fn token_response_to_token(resp: TokenResponse, now_unix: u64) -> MusicBrainzToken {
    let expires_in = resp.expires_in.unwrap_or(3600);
    MusicBrainzToken {
        access_token: resp.access_token,
        refresh_token: resp.refresh_token,
        expires_at_unix: now_unix.saturating_add(expires_in),
        scopes: parse_scopes(resp.scope.as_deref()),
        token_type: resp.token_type.unwrap_or_else(|| "Bearer".to_owned()),
    }
}

async fn post_token_form(
    endpoint: &str,
    form: &[(&str, &str)],
) -> Result<reqwest::Response, MusicBrainzAuthError> {
    let client = mediapm_conductor::http::client::shared_http_client()
        .map_err(|err| MusicBrainzAuthError::HttpClient { message: err.to_string() })?;
    client
        .post(endpoint)
        .form(form)
        .send()
        .await
        .map_err(|err| MusicBrainzAuthError::HttpRequest { message: err.to_string() })
}

/// Exchanges an authorization code for tokens.
///
/// # Errors
///
/// Returns [`MusicBrainzAuthError`] on HTTP or token endpoint failures.
pub async fn exchange_authorization_code(
    config: &MusicBrainzOAuthConfig,
    code: &str,
    redirect_uri: &str,
    code_verifier: &str,
) -> Result<MusicBrainzToken, MusicBrainzAuthError> {
    let endpoint = token_endpoint(&config.base_url);
    let form = [
        ("grant_type", "authorization_code"),
        ("code", code),
        ("redirect_uri", redirect_uri),
        ("client_id", config.client_id.as_str()),
        ("client_secret", config.client_secret.as_str()),
        ("code_verifier", code_verifier),
    ];
    let response = post_token_form(&endpoint, &form).await?;
    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|err| MusicBrainzAuthError::HttpRequest { message: err.to_string() })?;
    if !status.is_success() {
        return Err(MusicBrainzAuthError::TokenEndpoint { message: format!("status {status}") });
    }
    let token: TokenResponse = serde_json::from_str(&body).map_err(|err| {
        MusicBrainzAuthError::TokenEndpoint { message: format!("invalid token response: {err}") }
    })?;
    Ok(token_response_to_token(token, current_unix_time()))
}

/// Refreshes an access token using a refresh token.
///
/// On HTTP 400, the caller should treat the refresh token as invalid.
///
/// # Errors
///
/// Returns [`MusicBrainzAuthError`] on HTTP or token endpoint failures.
pub async fn refresh_access_token(
    config: &MusicBrainzOAuthConfig,
    refresh_token: &str,
) -> Result<(MusicBrainzToken, bool), MusicBrainzAuthError> {
    let endpoint = token_endpoint(&config.base_url);
    let form = [
        ("grant_type", "refresh_token"),
        ("refresh_token", refresh_token),
        ("client_id", config.client_id.as_str()),
        ("client_secret", config.client_secret.as_str()),
    ];
    let response = post_token_form(&endpoint, &form).await?;
    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|err| MusicBrainzAuthError::HttpRequest { message: err.to_string() })?;
    if status == reqwest::StatusCode::BAD_REQUEST {
        return Err(MusicBrainzAuthError::LoginExpired);
    }
    if !status.is_success() {
        return Err(MusicBrainzAuthError::TokenEndpoint { message: format!("status {status}") });
    }
    let token: TokenResponse = serde_json::from_str(&body).map_err(|err| {
        MusicBrainzAuthError::TokenEndpoint { message: format!("invalid token response: {err}") }
    })?;
    let rotated = token.refresh_token.is_some();
    let mut auth = token_response_to_token(token, current_unix_time());
    if auth.refresh_token.is_none() {
        auth.refresh_token = Some(refresh_token.to_owned());
    }
    Ok((auth, rotated))
}

/// Revokes a token at the `MusicBrainz` revoke endpoint. Best-effort for callers.
///
/// # Errors
///
/// Returns [`MusicBrainzAuthError`] on HTTP client/request failures.
pub async fn revoke_token(
    config: &MusicBrainzOAuthConfig,
    token: &str,
) -> Result<(), MusicBrainzAuthError> {
    let endpoint = revoke_endpoint(&config.base_url);
    let form = [
        ("token", token),
        ("client_id", config.client_id.as_str()),
        ("client_secret", config.client_secret.as_str()),
    ];
    let response = post_token_form(&endpoint, &form).await?;
    if response.status().is_success() || response.status() == reqwest::StatusCode::BAD_REQUEST {
        return Ok(());
    }
    Err(MusicBrainzAuthError::TokenEndpoint {
        message: format!("revoke status {}", response.status()),
    })
}

/// Ensures a valid access token is available, reading and writing only the
/// unified auth file.
///
/// # Errors
///
/// Returns [`AuthError`] when not logged in, stored credentials are missing,
/// refresh fails, or storage fails.
pub async fn ensure_access_token(auth_file: &Path) -> Result<String, AuthError> {
    ensure_access_token_with_base(auth_file, DEFAULT_MUSICBRAINZ_OAUTH_BASE_URL).await
}

async fn ensure_access_token_with_base(
    auth_file: &Path,
    base_url: &str,
) -> Result<String, AuthError> {
    let Some(mut auth) = load_auth_file(auth_file)? else {
        return Err(not_logged_in_error());
    };
    let Some(mut credential) = auth.services.musicbrainz else {
        return Err(not_logged_in_error());
    };
    let Some(token) = credential.token.clone() else {
        return Err(not_logged_in_error());
    };
    if !token.is_expired(current_unix_time()) {
        return Ok(token.access_token);
    }
    let (client_id, client_secret) = credentials(auth_file)?;
    let config = MusicBrainzOAuthConfig { base_url: base_url.to_owned(), client_id, client_secret };
    let Some(refresh_token) = token.refresh_token else {
        remove_service(auth_file, AuthServiceId::Musicbrainz.as_str())?;
        return Err(service_error(&MusicBrainzAuthError::LoginExpired));
    };
    match refresh_access_token(&config, &refresh_token).await {
        Ok((refreshed, _rotated)) => {
            let access_token = refreshed.access_token.clone();
            credential.token = Some(refreshed);
            auth.services.musicbrainz = Some(credential);
            save_auth_file(auth_file, &auth)?;
            Ok(access_token)
        }
        Err(MusicBrainzAuthError::LoginExpired) => {
            remove_service(auth_file, AuthServiceId::Musicbrainz.as_str())?;
            Err(service_error(&MusicBrainzAuthError::LoginExpired))
        }
        Err(err) => Err(service_error(&err)),
    }
}

/// Reports stored credential and token state without printing a secret.
///
/// # Errors
///
/// Returns [`AuthError`] when the auth file cannot be read or decoded.
pub fn status(auth_file: &Path) -> Result<AuthOutcome, AuthError> {
    let Some(credential) = load_auth_file(auth_file)?.and_then(|auth| auth.services.musicbrainz)
    else {
        return Ok(AuthOutcome { message: "MusicBrainz: not logged in".to_owned() });
    };
    let message = match credential.token {
        None => "MusicBrainz: credentials configured; no user token".to_owned(),
        Some(token) => {
            let state = if token.is_expired(current_unix_time()) { "expired" } else { "logged in" };
            let scopes = if token.scopes.is_empty() {
                "(no scopes)".to_owned()
            } else {
                token.scopes.join(" ")
            };
            format!("MusicBrainz: {state}; scopes: {scopes}")
        }
    };
    Ok(AuthOutcome { message })
}

/// Logs in through the browser PKCE flow, writing credentials before the
/// browser opens and the returned token into the same auth-file record.
///
/// # Errors
///
/// Returns [`AuthError`] when prompting, storage, or the OAuth flow fails.
pub async fn login(
    auth_file: &Path,
    prompt: &dyn SecretPrompt,
    offline: bool,
) -> Result<AuthOutcome, AuthError> {
    login_with_base(auth_file, prompt, offline, DEFAULT_MUSICBRAINZ_OAUTH_BASE_URL, open_browser)
        .await
}

async fn login_with_base(
    auth_file: &Path,
    prompt: &dyn SecretPrompt,
    offline: bool,
    base_url: &str,
    open_browser_fn: impl FnOnce(&str) -> Result<(), MusicBrainzAuthError>,
) -> Result<AuthOutcome, AuthError> {
    let mut auth = load_auth_file(auth_file)?.unwrap_or_default();
    let mut credential = auth.services.musicbrainz.take().unwrap_or(MusicBrainzCredential {
        client_id: String::new(),
        client_secret: String::new(),
        token: None,
    });
    if credential.client_id.trim().is_empty() {
        credential.client_id = prompt.read_secret("MusicBrainz client ID: ")?;
    }
    if credential.client_secret.trim().is_empty() {
        credential.client_secret = prompt.read_secret("MusicBrainz client secret: ")?;
    }
    let config = MusicBrainzOAuthConfig {
        base_url: base_url.to_owned(),
        client_id: credential.client_id.clone(),
        client_secret: credential.client_secret.clone(),
    };
    credential.token = None;
    auth.services.musicbrainz = Some(credential);
    save_auth_file(auth_file, &auth)?;

    if offline {
        tracing::info!(
            "--offline has no separate MusicBrainz validation step; OAuth exchange still runs"
        );
    }

    let token = perform_oauth_login(&config, open_browser_fn)
        .await
        .map_err(|error| service_error(&error))?;
    let scopes = if token.scopes.is_empty() { "(none)".to_owned() } else { token.scopes.join(" ") };
    let message = format!("MusicBrainz: logged in; scopes: {scopes}");
    let credential = auth.services.musicbrainz.as_mut().expect("credentials saved above");
    credential.token = Some(token);
    save_auth_file(auth_file, &auth)?;
    Ok(AuthOutcome { message })
}

/// Logs out: revokes the stored token unless `--offline`, then removes the
/// `MusicBrainz` record from the unified auth file. A failed revocation keeps
/// the record so the caller can retry or pass `--offline`.
///
/// # Errors
///
/// Returns [`AuthError`] when revocation is impossible without stored
/// credentials, revocation fails, or the auth file cannot be read, decoded,
/// or saved.
pub async fn logout(auth_file: &Path, offline: bool) -> Result<AuthOutcome, AuthError> {
    logout_with_base(auth_file, offline, DEFAULT_MUSICBRAINZ_OAUTH_BASE_URL).await
}

async fn logout_with_base(
    auth_file: &Path,
    offline: bool,
    base_url: &str,
) -> Result<AuthOutcome, AuthError> {
    if offline {
        remove_service(auth_file, AuthServiceId::Musicbrainz.as_str())?;
        return Ok(AuthOutcome {
            message: "MusicBrainz: removed local record without revocation".to_owned(),
        });
    }
    let Some(credential) = load_auth_file(auth_file)?.and_then(|auth| auth.services.musicbrainz)
    else {
        return Err(cannot_revoke_error());
    };
    let Some(token) = credential.token.as_ref() else {
        return Err(cannot_revoke_error());
    };
    if credential.client_id.trim().is_empty() || credential.client_secret.trim().is_empty() {
        return Err(cannot_revoke_error());
    }
    let config = MusicBrainzOAuthConfig {
        base_url: base_url.to_owned(),
        client_id: credential.client_id,
        client_secret: credential.client_secret,
    };
    let revoked = token.refresh_token.as_deref().unwrap_or(&token.access_token).to_owned();
    // The underlying error can carry request context, so report a fixed
    // message and keep the record for a retry or an explicit offline removal.
    revoke_token(&config, &revoked).await.map_err(|_| {
        AuthError::Service(
            "MusicBrainz revocation failed; local record kept for retry or use --offline to remove it"
                .to_owned(),
        )
    })?;
    remove_service(auth_file, AuthServiceId::Musicbrainz.as_str())?;
    Ok(AuthOutcome { message: "MusicBrainz: revoked token and removed local record".to_owned() })
}

/// Parsed OAuth callback query.
#[derive(Clone, PartialEq, Eq)]
pub struct OAuthCallback {
    /// Authorization code.
    pub code: String,
    /// State parameter returned by the server.
    pub state: String,
}

impl std::fmt::Debug for OAuthCallback {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OAuthCallback")
            .field("code", &"[REDACTED]")
            .field("state", &self.state)
            .finish()
    }
}

/// Parses an OAuth callback path/query (e.g. `/callback?code=...&state=...`).
///
/// # Errors
///
/// Returns [`MusicBrainzAuthError`] when required parameters are missing or an error is present.
pub fn parse_oauth_callback(request_target: &str) -> Result<OAuthCallback, MusicBrainzAuthError> {
    let parsed =
        reqwest::Url::parse(&format!("http://127.0.0.1{request_target}")).map_err(|err| {
            MusicBrainzAuthError::Callback { message: format!("invalid callback request: {err}") }
        })?;
    let mut code = None;
    let mut state = None;
    let mut error = None;
    let mut error_description = None;
    for (k, v) in parsed.query_pairs() {
        match k.as_ref() {
            "code" => code = Some(v.into_owned()),
            "state" => state = Some(v.into_owned()),
            "error" => error = Some(v.into_owned()),
            "error_description" => error_description = Some(v.into_owned()),
            _ => {}
        }
    }
    if let Some(err) = error {
        let detail = error_description.unwrap_or(err);
        return Err(MusicBrainzAuthError::Authorization { message: detail });
    }
    let code = code.ok_or_else(|| MusicBrainzAuthError::Callback {
        message: "missing code in OAuth callback".to_owned(),
    })?;
    let state = state.ok_or_else(|| MusicBrainzAuthError::Callback {
        message: "missing state in OAuth callback".to_owned(),
    })?;
    Ok(OAuthCallback { code, state })
}

/// Receives one OAuth callback on a loopback TCP listener.
///
/// # Errors
///
/// Returns [`MusicBrainzAuthError`] when the callback cannot be read, parsed, or times out.
pub fn receive_loopback_callback(
    listener: &TcpListener,
    expected_state: &str,
    timeout: std::time::Duration,
) -> Result<OAuthCallback, MusicBrainzAuthError> {
    listener.set_nonblocking(true).map_err(|source| MusicBrainzAuthError::Callback {
        message: format!("failed to configure callback listener: {source}"),
    })?;
    let deadline = std::time::Instant::now() + timeout;
    let (mut stream, _) = loop {
        match listener.accept() {
            Ok(pair) => break pair,
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                if std::time::Instant::now() >= deadline {
                    return Err(MusicBrainzAuthError::Callback {
                        message: "timed out waiting for OAuth callback".to_owned(),
                    });
                }
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
            Err(source) => {
                return Err(MusicBrainzAuthError::Callback {
                    message: format!("failed to accept callback connection: {source}"),
                });
            }
        }
    };
    stream.set_nonblocking(false).map_err(|source| MusicBrainzAuthError::Callback {
        message: format!("failed to configure callback stream: {source}"),
    })?;
    let _ = stream
        .set_read_timeout(Some(deadline.saturating_duration_since(std::time::Instant::now())));
    let mut reader = BufReader::new(&mut stream);
    let mut request_line = String::new();
    reader.read_line(&mut request_line).map_err(|source| MusicBrainzAuthError::Callback {
        message: format!("failed to read callback request: {source}"),
    })?;
    let target = request_line.split_whitespace().nth(1).ok_or_else(|| {
        MusicBrainzAuthError::Callback { message: "malformed callback request".to_owned() }
    })?;
    let result = parse_oauth_callback(target);
    let (status, body) = match &result {
        Ok(_) => ("200 OK", "MusicBrainz login complete. You can close this tab."),
        Err(_) => ("400 Bad Request", "MusicBrainz login failed. You can close this tab."),
    };
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes());
    let _ = stream.flush();
    let callback = result?;
    if callback.state != expected_state {
        return Err(MusicBrainzAuthError::Callback { message: "OAuth state mismatch".to_owned() });
    }
    Ok(callback)
}

/// Binds a loopback listener on 127.0.0.1:0 for the OAuth callback.
///
/// # Errors
///
/// Returns [`MusicBrainzAuthError`] when the listener cannot be bound.
pub fn bind_loopback_listener() -> Result<TcpListener, MusicBrainzAuthError> {
    TcpListener::bind(("127.0.0.1", 0))
        .map_err(|source| MusicBrainzAuthError::BindLoopback { source })
}

/// Opens a URL in the system browser. Best-effort; test callers can inject a no-op.
///
/// # Errors
///
/// Returns [`MusicBrainzAuthError`] when the browser command cannot be spawned.
pub fn open_browser(url: &str) -> Result<(), MusicBrainzAuthError> {
    #[cfg(target_os = "macos")]
    let mut cmd = {
        let mut c = std::process::Command::new("open");
        c.arg(url);
        c
    };
    #[cfg(target_os = "windows")]
    let mut cmd = {
        let mut c = std::process::Command::new("cmd");
        c.args(["/C", "start", "", url]);
        c
    };
    #[cfg(all(not(target_os = "macos"), not(target_os = "windows")))]
    let mut cmd = {
        let mut c = std::process::Command::new("xdg-open");
        c.arg(url);
        c
    };
    // The launcher is a runtime child too: give it only the host platform's
    // inherited names (which include the desktop/display variables it needs),
    // never the ambient credentials of the `mediapm` process.
    let env_pairs = crate::child_env::default_host_inherited_env_pairs();
    crate::child_env::apply_std(&mut cmd, &env_pairs);
    cmd.stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .map(|_| ())
        .map_err(|err| MusicBrainzAuthError::OpenBrowser { message: err.to_string() })
}

/// Runs the PKCE loopback exchange and returns the token; storage belongs to
/// [`login`], which saves credentials before this browser flow begins.
///
/// # Errors
///
/// Returns [`MusicBrainzAuthError`] on any OAuth flow failure.
async fn perform_oauth_login(
    config: &MusicBrainzOAuthConfig,
    open_browser_fn: impl FnOnce(&str) -> Result<(), MusicBrainzAuthError>,
) -> Result<MusicBrainzToken, MusicBrainzAuthError> {
    let listener = bind_loopback_listener()?;
    let port = listener
        .local_addr()
        .map_err(|source| MusicBrainzAuthError::BindLoopback { source })?
        .port();
    let redirect_uri = format!("http://127.0.0.1:{port}/callback");
    let pkce = generate_pkce_pair();
    let mut state_bytes = [0_u8; 16];
    rand::thread_rng().fill_bytes(&mut state_bytes);
    let state = URL_SAFE_NO_PAD.encode(state_bytes);
    let authorize_url = build_authorize_url(config, &redirect_uri, &state, &pkce);

    open_browser_fn(&authorize_url)?;

    let expected_state = state.clone();
    let callback = tokio::task::spawn_blocking(move || {
        receive_loopback_callback(&listener, &expected_state, std::time::Duration::from_secs(300))
    })
    .await
    .map_err(|err| MusicBrainzAuthError::Callback {
        message: format!("callback task failed: {err}"),
    })??;

    exchange_authorization_code(config, &callback.code, &redirect_uri, &pkce.verifier).await
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::ffi::OsString;
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::auth::error::AuthError;
    use crate::auth::prompt::SecretPrompt;
    use crate::auth::store::{AuthFile, AuthServices, MusicBrainzCredential, MusicBrainzToken};
    use mediapm_test_support::MockMusicBrainz;
    use tracing::instrument::WithSubscriber;
    use wiremock::{Mock, ResponseTemplate};

    fn sample_token(expires_at_unix: u64) -> MusicBrainzToken {
        MusicBrainzToken {
            access_token: "access-token".to_owned(),
            refresh_token: Some("refresh-token".to_owned()),
            expires_at_unix,
            scopes: vec![
                "profile".to_owned(),
                "musicbrainz:tag".to_owned(),
                "musicbrainz:rating".to_owned(),
                "musicbrainz:collection".to_owned(),
            ],
            token_type: "Bearer".to_owned(),
        }
    }

    fn seeded_auth(token: Option<MusicBrainzToken>) -> AuthFile {
        let mut auth = AuthFile { services: AuthServices::default() };
        auth.services.musicbrainz = Some(MusicBrainzCredential {
            client_id: "stored-id".to_owned(),
            client_secret: "stored-secret".to_owned(),
            token,
        });
        auth
    }

    /// Collects formatted tracing output for one scoped subscriber.
    ///
    /// Reuses the in-memory writer shape of the existing scoped capture in
    /// `src/mediapm/src/metadata_cache.rs`; no global subscriber is installed.
    #[derive(Clone)]
    struct CollectedLogs(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for CollectedLogs {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("log buffer").write(bytes)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// Holds the process env lock while conflicting credential env values are
    /// installed and restores the prior values on drop.
    ///
    /// The owning test is synchronous; the guard is never carried across an
    /// await boundary.
    struct CredentialEnvOverride {
        previous_id: Option<OsString>,
        previous_secret: Option<OsString>,
        _env_lock: parking_lot::MutexGuard<'static, ()>,
    }

    impl Drop for CredentialEnvOverride {
        fn drop(&mut self) {
            // SAFETY: this guard still owns the process env lock; no reader
            // can observe a half-restored credential value.
            unsafe {
                match &self.previous_id {
                    Some(value) => std::env::set_var("MUSICBRAINZ_OAUTH_CLIENT_ID", value),
                    None => std::env::remove_var("MUSICBRAINZ_OAUTH_CLIENT_ID"),
                }
                match &self.previous_secret {
                    Some(value) => std::env::set_var("MUSICBRAINZ_OAUTH_CLIENT_SECRET", value),
                    None => std::env::remove_var("MUSICBRAINZ_OAUTH_CLIENT_SECRET"),
                }
            }
        }
    }

    #[derive(Debug)]
    struct RecordingPrompt {
        secrets: RefCell<Vec<String>>,
        labels: RefCell<Vec<&'static str>>,
    }

    impl RecordingPrompt {
        fn new(secrets: Vec<String>) -> Self {
            Self { secrets: RefCell::new(secrets), labels: RefCell::new(Vec::new()) }
        }
    }

    impl SecretPrompt for RecordingPrompt {
        fn read_secret(&self, label: &'static str) -> Result<String, AuthError> {
            self.labels.borrow_mut().push(label);
            let mut secrets = self.secrets.borrow_mut();
            let secret = secrets.remove(0);
            if secret.trim().is_empty() {
                return Err(AuthError::EmptySecret);
            }
            Ok(secret)
        }
    }

    #[test]
    fn pkce_s256_challenge_matches_verifier() {
        let pair = generate_pkce_pair();
        let digest = Sha256::digest(pair.verifier.as_bytes());
        let expected = URL_SAFE_NO_PAD.encode(digest);
        assert_eq!(pair.challenge, expected);
        assert_ne!(pair.verifier, "");
        assert_ne!(pair.challenge, "");
    }

    #[test]
    fn credentials_read_only_the_unified_auth_file() {
        let dir = mediapm_utils::temp::output_dir().expect("temp dir");
        let path = dir.path().join("auth.json");
        save_auth_file(&path, &seeded_auth(None)).expect("seed");
        assert_eq!(
            credentials(&path).expect("stored credentials"),
            ("stored-id".to_owned(), "stored-secret".to_owned())
        );

        let missing = dir.path().join("missing-auth.json");
        let error = credentials(&missing).expect_err("no credential source besides the auth file");
        assert!(
            error.to_string().contains("mediapm auth musicbrainz login"),
            "the error must name the login command: {error}"
        );
    }

    #[test]
    fn credentials_ignore_conflicting_client_credential_env_values() {
        let dir = mediapm_utils::temp::output_dir().expect("temp dir");
        let path = dir.path().join("auth.json");
        save_auth_file(&path, &seeded_auth(None)).expect("seed");

        let env_lock = crate::example_isolation::lock_process_env();
        let previous_id = std::env::var_os("MUSICBRAINZ_OAUTH_CLIENT_ID");
        let previous_secret = std::env::var_os("MUSICBRAINZ_OAUTH_CLIENT_SECRET");
        // SAFETY: `env_lock` is held from here until `override_env` drops.
        unsafe {
            std::env::set_var("MUSICBRAINZ_OAUTH_CLIENT_ID", "env-conflicting-id");
            std::env::set_var("MUSICBRAINZ_OAUTH_CLIENT_SECRET", "env-conflicting-secret");
        }
        let override_env = CredentialEnvOverride {
            previous_id: previous_id.clone(),
            previous_secret: previous_secret.clone(),
            _env_lock: env_lock,
        };

        assert_eq!(
            credentials(&path).expect("the auth file is the only credential source"),
            ("stored-id".to_owned(), "stored-secret".to_owned())
        );
        drop(override_env);

        let _lock = crate::example_isolation::lock_process_env();
        assert_eq!(std::env::var_os("MUSICBRAINZ_OAUTH_CLIENT_ID"), previous_id);
        assert_eq!(std::env::var_os("MUSICBRAINZ_OAUTH_CLIENT_SECRET"), previous_secret);
    }

    #[test]
    fn auth_file_round_trip_and_permissions() {
        let dir = mediapm_utils::temp::output_dir().expect("temp dir");
        let path = dir.path().join("auth.json");
        let mut auth = AuthFile { services: AuthServices::default() };
        auth.services.musicbrainz = Some(MusicBrainzCredential {
            client_id: "id".to_owned(),
            client_secret: "secret".to_owned(),
            token: Some(sample_token(1_790_000_000)),
        });
        save_auth_file(&path, &auth).expect("save");
        assert_eq!(load_auth_file(&path).unwrap().unwrap(), auth);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).expect("metadata").permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "auth file must be 0600");
        }
    }

    #[test]
    fn status_reports_token_state_without_secrets() {
        let dir = mediapm_utils::temp::output_dir().expect("temp dir");
        let path = dir.path().join("auth.json");
        assert!(status(&path).expect("status").message.contains("not logged in"));

        save_auth_file(&path, &seeded_auth(None)).expect("seed credentials");
        let status_without_token = status(&path).expect("status").message;
        assert!(status_without_token.contains("no user token"));
        assert!(!status_without_token.contains("stored-secret"));

        save_auth_file(&path, &seeded_auth(Some(sample_token(current_unix_time() + 3600))))
            .expect("seed token");
        let status_with_token = status(&path).expect("status").message;
        assert!(status_with_token.contains("logged in"));
        assert!(status_with_token.contains("musicbrainz:tag"));
        assert!(!status_with_token.contains("stored-secret"));
        assert!(!status_with_token.contains("access-token"));
    }

    #[tokio::test]
    async fn login_prompts_saves_credentials_before_browser_flow_and_stores_token() {
        let mock = MockMusicBrainz::start().await;
        mock.mount_oauth_authorize_redirect("test-code").await;
        mock.mount_oauth_token(
            "access-1",
            Some("refresh-1"),
            3600,
            "profile musicbrainz:tag musicbrainz:rating musicbrainz:collection",
        )
        .await;

        let dir = mediapm_utils::temp::output_dir().expect("temp dir");
        let path = dir.path().join("auth.json");
        let prompt =
            RecordingPrompt::new(vec!["prompted-id".to_owned(), "prompted-secret".to_owned()]);
        let browser_path = path.clone();
        let collected = CollectedLogs(Arc::new(Mutex::new(Vec::new())));
        let subscriber = tracing_subscriber::fmt()
            .with_writer({
                let sink = collected.clone();
                move || sink.clone()
            })
            .with_max_level(tracing::Level::INFO)
            .finish();
        let outcome = login_with_base(&path, &prompt, true, &mock.server().uri(), |url| {
            let stored =
                load_auth_file(&browser_path).expect("browser reads auth file").expect("present");
            let credential = stored.services.musicbrainz.expect("credentials saved before browser");
            assert_eq!(credential.client_id, "prompted-id");
            assert_eq!(credential.client_secret, "prompted-secret");
            assert!(credential.token.is_none(), "no token before the exchange");

            let url = url.to_owned();
            std::thread::spawn(move || {
                let client = reqwest::blocking::Client::builder()
                    .redirect(reqwest::redirect::Policy::limited(5))
                    .build()
                    .expect("client");
                let _ = client.get(url).send();
            });
            Ok(())
        })
        .with_subscriber(subscriber)
        .await
        .expect("login");

        let logged = String::from_utf8(collected.0.lock().expect("log buffer").clone())
            .expect("captured logs are UTF-8");
        let expected_line =
            "--offline has no separate MusicBrainz validation step; OAuth exchange still runs";
        assert!(
            logged.lines().any(|line| line.ends_with(expected_line)),
            "offline login must log exactly {expected_line:?}; captured: {logged}"
        );

        assert_eq!(
            *prompt.labels.borrow(),
            ["MusicBrainz client ID: ", "MusicBrainz client secret: "]
        );
        assert!(!outcome.message.contains("prompted-secret"));
        let stored = load_auth_file(&path).expect("load").expect("stored");
        let credential = stored.services.musicbrainz.expect("record");
        assert_eq!(credential.client_id, "prompted-id");
        assert_eq!(credential.client_secret, "prompted-secret");
        let token = credential.token.expect("token");
        assert_eq!(token.access_token, "access-1");
        assert_eq!(token.refresh_token.as_deref(), Some("refresh-1"));
    }

    #[tokio::test]
    async fn refreshes_when_near_expiry_and_persists_rotation() {
        let mock = MockMusicBrainz::start().await;
        mock.mount_oauth_token(
            "access-2",
            Some("refresh-2"),
            3600,
            "profile musicbrainz:tag musicbrainz:rating musicbrainz:collection",
        )
        .await;

        let dir = mediapm_utils::temp::output_dir().expect("temp dir");
        let path = dir.path().join("auth.json");
        let mut expired = seeded_auth(Some(sample_token(current_unix_time().saturating_sub(10))));
        if let Some(token) = expired.services.musicbrainz.as_mut().and_then(|c| c.token.as_mut()) {
            token.refresh_token = Some("refresh-1".to_owned());
        }
        save_auth_file(&path, &expired).expect("save");

        let token =
            ensure_access_token_with_base(&path, &mock.server().uri()).await.expect("token");
        assert_eq!(token, "access-2");

        let stored = load_auth_file(&path).expect("load").expect("stored");
        let credential = stored.services.musicbrainz.expect("record");
        assert_eq!(credential.client_id, "stored-id");
        assert_eq!(credential.client_secret, "stored-secret");
        let token = credential.token.expect("token");
        assert_eq!(token.access_token, "access-2");
        assert_eq!(token.refresh_token.as_deref(), Some("refresh-2"));
    }

    #[tokio::test]
    async fn refresh_400_drops_token_and_reports_login_again() {
        let mock = MockMusicBrainz::start().await;
        Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/oauth2/token"))
            .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "error": "invalid_grant"
            })))
            .mount(mock.server())
            .await;

        let dir = mediapm_utils::temp::output_dir().expect("temp dir");
        let path = dir.path().join("auth.json");
        let mut expired = seeded_auth(Some(sample_token(current_unix_time().saturating_sub(10))));
        if let Some(token) = expired.services.musicbrainz.as_mut().and_then(|c| c.token.as_mut()) {
            token.refresh_token = Some("refresh-1".to_owned());
        }
        save_auth_file(&path, &expired).expect("save");

        let error = ensure_access_token_with_base(&path, &mock.server().uri())
            .await
            .expect_err("expired refresh must fail");
        assert!(
            matches!(&error, AuthError::Service(message) if message.contains("login expired")),
            "unexpected error: {error}"
        );
        let loaded = load_auth_file(&path).expect("load").expect("file");
        assert!(loaded.services.musicbrainz.is_none(), "expired refresh should drop the record");
    }

    #[tokio::test]
    async fn logout_revokes_and_deletes_file() {
        let mock = MockMusicBrainz::start().await;
        mock.mount_oauth_revoke().await;

        let dir = mediapm_utils::temp::output_dir().expect("temp dir");
        let path = dir.path().join("auth.json");
        save_auth_file(&path, &seeded_auth(Some(sample_token(1_790_000_000)))).expect("seed");

        let outcome = logout_with_base(&path, false, &mock.server().uri()).await.expect("logout");
        assert!(outcome.message.contains("revoked"));

        let requests = mock.server().received_requests().await.expect("request log");
        let revoke = requests
            .iter()
            .find(|request| request.url.path() == "/oauth2/revoke")
            .expect("revocation request");
        let body = String::from_utf8_lossy(&revoke.body);
        assert!(body.contains("client_id=stored-id"), "revoke must use the stored id: {body}");
        assert!(
            body.contains("client_secret=stored-secret"),
            "revoke must use the stored secret: {body}"
        );

        let loaded = load_auth_file(&path).expect("load").expect("file");
        assert!(loaded.services.musicbrainz.is_none());
    }

    #[tokio::test]
    async fn failed_revocation_keeps_the_record_and_reports_a_secret_free_error() {
        let mock = MockMusicBrainz::start().await;
        Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/oauth2/revoke"))
            .respond_with(ResponseTemplate::new(500))
            .mount(mock.server())
            .await;

        let dir = mediapm_utils::temp::output_dir().expect("temp dir");
        let path = dir.path().join("auth.json");
        let seeded = seeded_auth(Some(sample_token(current_unix_time() + 3600)));
        save_auth_file(&path, &seeded).expect("seed");

        let error = logout_with_base(&path, false, &mock.server().uri())
            .await
            .expect_err("a failed revocation must not claim success");
        let AuthError::Service(message) = &error else {
            panic!("revocation failure must surface a service error: {error:?}");
        };
        assert_eq!(
            message,
            "MusicBrainz revocation failed; local record kept for retry or use --offline to remove it"
        );
        assert!(!message.contains("stored-secret"), "client secret leaked: {message}");
        assert!(!message.contains("access-token"), "access token leaked: {message}");
        assert!(!message.contains("refresh-token"), "refresh token leaked: {message}");

        let loaded = load_auth_file(&path).expect("load").expect("record kept for retry");
        assert_eq!(loaded.services.musicbrainz, seeded.services.musicbrainz);
    }

    #[tokio::test]
    async fn offline_logout_skips_revocation_and_removes_the_record() {
        let dir = mediapm_utils::temp::output_dir().expect("temp dir");
        let path = dir.path().join("auth.json");
        let mut auth = AuthFile { services: AuthServices::default() };
        auth.services.musicbrainz = Some(MusicBrainzCredential {
            client_id: "id".to_owned(),
            client_secret: "secret".to_owned(),
            token: Some(sample_token(1_790_000_000)),
        });
        save_auth_file(&path, &auth).expect("seed");
        let outcome = logout(&path, true).await.expect("offline logout");
        assert!(outcome.message.contains("without revocation"));
        let loaded = load_auth_file(&path).unwrap().unwrap();
        assert!(loaded.services.musicbrainz.is_none());
    }

    #[tokio::test]
    async fn online_logout_without_stored_credentials_errors() {
        let dir = mediapm_utils::temp::output_dir().expect("temp dir");
        let missing = dir.path().join("missing-auth.json");
        let error = logout(&missing, false).await.expect_err("no credential fallback");
        assert_eq!(
            error.to_string(),
            "cannot revoke without stored client credentials; use --offline to remove locally"
        );

        let credentials_only = dir.path().join("credentials-only.json");
        save_auth_file(&credentials_only, &seeded_auth(None)).expect("seed");
        let error = logout(&credentials_only, false).await.expect_err("no token to revoke");
        assert_eq!(
            error.to_string(),
            "cannot revoke without stored client credentials; use --offline to remove locally"
        );
    }
}
