//! `MusicBrainz` OAuth (PKCE + loopback) implementation.
//!
//! Tokens are stored at `<runtime_root>/auth/musicbrainz.json` (not in state).

use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Default `MusicBrainz` OAuth base URL.
pub const DEFAULT_MUSICBRAINZ_OAUTH_BASE_URL: &str = "https://metabrainz.org";

/// Registration URL shown in error messages when OAuth client credentials are missing.
pub const MUSICBRAINZ_APP_REGISTRATION_URL: &str = "https://metabrainz.org/profile/applications";

/// OAuth scopes mediapm requests for read-only `MusicBrainz` user data.
pub const MUSICBRAINZ_OAUTH_SCOPES: &str =
    "profile musicbrainz:tag musicbrainz:rating musicbrainz:collection";

/// Errors produced by `MusicBrainz` OAuth flows.
#[derive(Debug, Error)]
pub enum MusicBrainzAuthError {
    /// Required OAuth client credentials are missing.
    #[error(
        "MusicBrainz OAuth client credentials are missing; register an application at {registration_url} and set MUSICBRAINZ_OAUTH_CLIENT_ID and MUSICBRAINZ_OAUTH_CLIENT_SECRET"
    )]
    MissingClientCredentials {
        /// URL where the user can register an OAuth application.
        registration_url: &'static str,
    },
    /// The auth file could not be read.
    #[error("failed to read MusicBrainz auth file {path}: {source}")]
    ReadAuthFile {
        /// Path that could not be read.
        path: PathBuf,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },
    /// The auth file could not be written.
    #[error("failed to write MusicBrainz auth file {path}: {source}")]
    WriteAuthFile {
        /// Path that could not be written.
        path: PathBuf,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },
    /// The auth file could not be parsed.
    #[error("failed to parse MusicBrainz auth file {path}: {source}")]
    ParseAuthFile {
        /// Path that could not be parsed.
        path: PathBuf,
        /// Underlying parse error.
        #[source]
        source: serde_json::Error,
    },
    /// The auth file could not be serialized.
    #[error("failed to serialize MusicBrainz auth file: {source}")]
    SerializeAuthFile {
        /// Underlying serialization error.
        #[source]
        source: serde_json::Error,
    },
    /// The auth file parent directory could not be created.
    #[error("failed to create MusicBrainz auth directory {path}: {source}")]
    CreateAuthDir {
        /// Path that could not be created.
        path: PathBuf,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },
    /// Removing the auth file failed.
    #[error("failed to remove MusicBrainz auth file {path}: {source}")]
    RemoveAuthFile {
        /// Path that could not be removed.
        path: PathBuf,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },
    /// No auth session is present.
    #[error("not logged in to MusicBrainz; run 'mediapm auth musicbrainz login'")]
    NotLoggedIn,
    /// Refresh token was rejected and the stored auth was cleared.
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

/// Stored `MusicBrainz` OAuth token data.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MusicBrainzAuthFile {
    /// OAuth access token.
    pub access_token: String,
    /// OAuth refresh token, if provided.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_token: Option<String>,
    /// Unix timestamp (seconds) when the access token expires.
    pub expires_at_unix: u64,
    /// Granted scopes.
    #[serde(default)]
    pub scopes: Vec<String>,
    /// Token type (usually "Bearer").
    #[serde(default = "default_token_type")]
    pub token_type: String,
}

fn default_token_type() -> String {
    "Bearer".to_owned()
}

impl MusicBrainzAuthFile {
    /// Returns true if the access token is expired or near expiry (within 60 seconds).
    #[must_use]
    pub fn is_expired(&self, now_unix: u64) -> bool {
        now_unix >= self.expires_at_unix.saturating_sub(60)
    }
}

/// Runtime configuration for `MusicBrainz` OAuth.
#[derive(Clone)]
pub struct MusicBrainzOAuthConfig {
    /// Base URL, e.g. `https://metabrainz.org`.
    pub base_url: String,
    /// OAuth client id.
    pub client_id: String,
    /// OAuth client secret.
    pub client_secret: String,
}

impl MusicBrainzOAuthConfig {
    /// Creates a config from environment variables.
    ///
    /// # Errors
    ///
    /// Returns [`MusicBrainzAuthError::MissingClientCredentials`] when either
    /// credential is missing or empty.
    pub fn from_env() -> Result<Self, MusicBrainzAuthError> {
        Self::from_env_with_base(DEFAULT_MUSICBRAINZ_OAUTH_BASE_URL)
    }

    /// Creates a config from environment variables with a custom base URL (for tests).
    ///
    /// # Errors
    ///
    /// Returns [`MusicBrainzAuthError::MissingClientCredentials`] when either
    /// credential is missing or empty.
    pub fn from_env_with_base(base_url: impl Into<String>) -> Result<Self, MusicBrainzAuthError> {
        let client_id = std::env::var("MUSICBRAINZ_OAUTH_CLIENT_ID").unwrap_or_default();
        let client_secret = std::env::var("MUSICBRAINZ_OAUTH_CLIENT_SECRET").unwrap_or_default();
        let client_id = client_id.trim().to_owned();
        let client_secret = client_secret.trim().to_owned();
        if client_id.is_empty() || client_secret.is_empty() {
            return Err(MusicBrainzAuthError::MissingClientCredentials {
                registration_url: MUSICBRAINZ_APP_REGISTRATION_URL,
            });
        }
        Ok(Self { base_url: base_url.into(), client_id, client_secret })
    }
}

impl std::fmt::Debug for MusicBrainzAuthFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MusicBrainzAuthFile")
            .field("access_token", &"[REDACTED]")
            .field("refresh_token", &self.refresh_token.as_ref().map(|_| "[REDACTED]"))
            .field("expires_at_unix", &self.expires_at_unix)
            .field("scopes", &self.scopes)
            .field("token_type", &self.token_type)
            .finish()
    }
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

/// Returns the path to the `MusicBrainz` auth file under the runtime root.
#[must_use]
pub fn musicbrainz_auth_file_path(runtime_root: &Path) -> PathBuf {
    runtime_root.join("auth").join("musicbrainz.json")
}

/// Loads the `MusicBrainz` auth file if it exists.
///
/// # Errors
///
/// Returns [`MusicBrainzAuthError`] when the file exists but cannot be read or parsed.
pub fn load_auth_file(path: &Path) -> Result<Option<MusicBrainzAuthFile>, MusicBrainzAuthError> {
    match std::fs::read(path) {
        Ok(bytes) => {
            let auth: MusicBrainzAuthFile = serde_json::from_slice(&bytes).map_err(|source| {
                MusicBrainzAuthError::ParseAuthFile { path: path.to_path_buf(), source }
            })?;
            Ok(Some(auth))
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(MusicBrainzAuthError::ReadAuthFile { path: path.to_path_buf(), source }),
    }
}

/// Saves the `MusicBrainz` auth file atomically with mode 0600 on Unix.
///
/// # Errors
///
/// Returns [`MusicBrainzAuthError`] when directory creation, serialization, or write fails.
pub fn save_auth_file(path: &Path, auth: &MusicBrainzAuthFile) -> Result<(), MusicBrainzAuthError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| MusicBrainzAuthError::CreateAuthDir {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    let mut json = serde_json::to_vec_pretty(auth)
        .map_err(|source| MusicBrainzAuthError::SerializeAuthFile { source })?;
    json.push(b'\n');
    let tmp_path = path.with_extension("json.tmp");
    std::fs::write(&tmp_path, &json)
        .map_err(|source| MusicBrainzAuthError::WriteAuthFile { path: tmp_path.clone(), source })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let perms = std::fs::Permissions::from_mode(0o600);
        if let Err(source) = std::fs::set_permissions(&tmp_path, perms) {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(MusicBrainzAuthError::WriteAuthFile { path: tmp_path, source });
        }
    }
    if let Err(source) = std::fs::rename(&tmp_path, path) {
        let _ = std::fs::remove_file(&tmp_path);
        return Err(MusicBrainzAuthError::WriteAuthFile { path: path.to_path_buf(), source });
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let perms = std::fs::Permissions::from_mode(0o600);
        if let Err(source) = std::fs::set_permissions(path, perms) {
            return Err(MusicBrainzAuthError::WriteAuthFile { path: path.to_path_buf(), source });
        }
    }
    Ok(())
}

/// Deletes the `MusicBrainz` auth file if it exists. Returns true if deleted.
///
/// # Errors
///
/// Returns [`MusicBrainzAuthError`] when the file exists but cannot be removed.
pub fn delete_auth_file(path: &Path) -> Result<bool, MusicBrainzAuthError> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(source) => {
            Err(MusicBrainzAuthError::RemoveAuthFile { path: path.to_path_buf(), source })
        }
    }
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

fn token_response_to_auth_file(resp: TokenResponse, now_unix: u64) -> MusicBrainzAuthFile {
    let expires_in = resp.expires_in.unwrap_or(3600);
    MusicBrainzAuthFile {
        access_token: resp.access_token,
        refresh_token: resp.refresh_token,
        expires_at_unix: now_unix.saturating_add(expires_in),
        scopes: parse_scopes(resp.scope.as_deref()),
        token_type: resp.token_type.unwrap_or_else(default_token_type),
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
) -> Result<MusicBrainzAuthFile, MusicBrainzAuthError> {
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
    Ok(token_response_to_auth_file(token, current_unix_time()))
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
) -> Result<(MusicBrainzAuthFile, bool), MusicBrainzAuthError> {
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
    let mut auth = token_response_to_auth_file(token, current_unix_time());
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

/// Ensures a valid access token is available, refreshing if needed.
///
/// # Errors
///
/// Returns [`MusicBrainzAuthError`] when not logged in, refresh fails, or storage fails.
pub async fn ensure_access_token(
    runtime_root: &Path,
    config: &MusicBrainzOAuthConfig,
) -> Result<String, MusicBrainzAuthError> {
    let path = musicbrainz_auth_file_path(runtime_root);
    let Some(mut auth) = load_auth_file(&path)? else {
        return Err(MusicBrainzAuthError::NotLoggedIn);
    };
    if !auth.is_expired(current_unix_time()) {
        return Ok(auth.access_token);
    }
    let Some(refresh_token) = auth.refresh_token.clone() else {
        let _ = delete_auth_file(&path)?;
        return Err(MusicBrainzAuthError::LoginExpired);
    };
    match refresh_access_token(config, &refresh_token).await {
        Ok((refreshed, _rotated)) => {
            auth = refreshed;
            save_auth_file(&path, &auth)?;
            Ok(auth.access_token)
        }
        Err(MusicBrainzAuthError::LoginExpired) => {
            let _ = delete_auth_file(&path)?;
            Err(MusicBrainzAuthError::LoginExpired)
        }
        Err(err) => Err(err),
    }
}

/// Returns a human-readable status for the stored auth file.
///
/// # Errors
///
/// Returns [`MusicBrainzAuthError`] when the auth file exists but cannot be read.
pub fn auth_status(runtime_root: &Path) -> Result<String, MusicBrainzAuthError> {
    let path = musicbrainz_auth_file_path(runtime_root);
    let Some(auth) = load_auth_file(&path)? else {
        return Ok("not logged in".to_owned());
    };
    let now = current_unix_time();
    let state = if auth.is_expired(now) { "expired" } else { "logged in" };
    let scopes =
        if auth.scopes.is_empty() { "(no scopes)".to_owned() } else { auth.scopes.join(" ") };
    Ok(format!("{state}; scopes: {scopes}; file: {}", path.display()))
}

/// Logs out: revokes the stored token (best-effort) and deletes the auth file.
///
/// # Errors
///
/// Returns [`MusicBrainzAuthError`] when deletion fails.
pub async fn logout_musicbrainz(
    runtime_root: &Path,
    config: Option<&MusicBrainzOAuthConfig>,
) -> Result<bool, MusicBrainzAuthError> {
    let path = musicbrainz_auth_file_path(runtime_root);
    let Some(auth) = load_auth_file(&path)? else {
        return Ok(false);
    };
    if let Some(config) = config {
        let token = auth.refresh_token.as_deref().unwrap_or(&auth.access_token);
        let _ = revoke_token(config, token).await;
    }
    delete_auth_file(&path)
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

/// Performs the interactive `MusicBrainz` OAuth login flow.
///
/// Binds a loopback listener, opens the browser (via `open_browser_fn`), waits for
/// the callback, exchanges the code, and saves tokens.
///
/// # Errors
///
/// Returns [`MusicBrainzAuthError`] on any login failure.
pub async fn login_musicbrainz(
    runtime_root: &Path,
    config: &MusicBrainzOAuthConfig,
    open_browser_fn: impl FnOnce(&str) -> Result<(), MusicBrainzAuthError>,
) -> Result<MusicBrainzAuthFile, MusicBrainzAuthError> {
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

    let auth =
        exchange_authorization_code(config, &callback.code, &redirect_uri, &pkce.verifier).await?;
    let path = musicbrainz_auth_file_path(runtime_root);
    save_auth_file(&path, &auth)?;
    Ok(auth)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mediapm_test_support::MockMusicBrainz;
    use wiremock::{Mock, ResponseTemplate};

    fn sample_auth(expires_at_unix: u64) -> MusicBrainzAuthFile {
        MusicBrainzAuthFile {
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
    fn missing_client_credentials_reports_registration_url() {
        let err = MusicBrainzOAuthConfig::from_env_with_base("https://metabrainz.org").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("MUSICBRAINZ_OAUTH_CLIENT_ID"));
        assert!(msg.contains("MUSICBRAINZ_OAUTH_CLIENT_SECRET"));
        assert!(msg.contains("https://metabrainz.org/profile/applications"));
    }

    #[test]
    fn auth_file_round_trip_and_permissions() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("auth").join("musicbrainz.json");
        let auth = sample_auth(current_unix_time() + 3600);
        save_auth_file(&path, &auth).expect("save");
        let loaded = load_auth_file(&path).expect("load").expect("present");
        assert_eq!(loaded, auth);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).expect("metadata").permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "auth file must be 0600");
        }
    }

    #[tokio::test]
    async fn login_exchanges_code_and_stores_auth_file() {
        let mock = MockMusicBrainz::start().await;
        mock.mount_oauth_authorize_redirect("test-code").await;
        mock.mount_oauth_token(
            "access-1",
            Some("refresh-1"),
            3600,
            "profile musicbrainz:tag musicbrainz:rating musicbrainz:collection",
        )
        .await;

        let dir = tempfile::tempdir().expect("tempdir");
        let runtime_root = dir.path();
        let config = MusicBrainzOAuthConfig {
            base_url: mock.server().uri(),
            client_id: "client-id".to_owned(),
            client_secret: "client-secret".to_owned(),
        };

        let auth = login_musicbrainz(runtime_root, &config, |url| {
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
        .await
        .expect("login");

        assert_eq!(auth.access_token, "access-1");
        assert_eq!(auth.refresh_token.as_deref(), Some("refresh-1"));
        let stored = load_auth_file(&musicbrainz_auth_file_path(runtime_root))
            .expect("load")
            .expect("stored");
        assert_eq!(stored.access_token, "access-1");
    }

    #[test]
    fn status_reports_logged_out_and_logged_in() {
        let dir = tempfile::tempdir().expect("tempdir");
        let runtime_root = dir.path();
        let status = auth_status(runtime_root).expect("status");
        assert!(status.contains("not logged in"));

        let path = musicbrainz_auth_file_path(runtime_root);
        save_auth_file(&path, &sample_auth(current_unix_time() + 3600)).expect("save");
        let status = auth_status(runtime_root).expect("status");
        assert!(status.contains("logged in"));
        assert!(status.contains("musicbrainz:tag"));
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

        let dir = tempfile::tempdir().expect("tempdir");
        let runtime_root = dir.path();
        let path = musicbrainz_auth_file_path(runtime_root);
        let mut expired = sample_auth(current_unix_time().saturating_sub(10));
        expired.refresh_token = Some("refresh-1".to_owned());
        save_auth_file(&path, &expired).expect("save");

        let config = MusicBrainzOAuthConfig {
            base_url: mock.server().uri(),
            client_id: "client-id".to_owned(),
            client_secret: "client-secret".to_owned(),
        };
        let token = ensure_access_token(runtime_root, &config).await.expect("token");
        assert_eq!(token, "access-2");

        let stored = load_auth_file(&path).expect("load").expect("stored");
        assert_eq!(stored.access_token, "access-2");
        assert_eq!(stored.refresh_token.as_deref(), Some("refresh-2"));
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

        let dir = tempfile::tempdir().expect("tempdir");
        let runtime_root = dir.path();
        let path = musicbrainz_auth_file_path(runtime_root);
        let mut expired = sample_auth(current_unix_time().saturating_sub(10));
        expired.refresh_token = Some("refresh-1".to_owned());
        save_auth_file(&path, &expired).expect("save");

        let config = MusicBrainzOAuthConfig {
            base_url: mock.server().uri(),
            client_id: "client-id".to_owned(),
            client_secret: "client-secret".to_owned(),
        };
        let err = ensure_access_token(runtime_root, &config).await.unwrap_err();
        assert!(matches!(err, MusicBrainzAuthError::LoginExpired));
        let loaded = load_auth_file(&path).expect("load");
        assert!(loaded.is_none(), "expired refresh should drop stored auth");
    }

    #[tokio::test]
    async fn logout_revokes_and_deletes_file() {
        let mock = MockMusicBrainz::start().await;
        mock.mount_oauth_revoke().await;

        let dir = tempfile::tempdir().expect("tempdir");
        let runtime_root = dir.path();
        let path = musicbrainz_auth_file_path(runtime_root);
        save_auth_file(&path, &sample_auth(current_unix_time() + 3600)).expect("save");

        let config = MusicBrainzOAuthConfig {
            base_url: mock.server().uri(),
            client_id: "client-id".to_owned(),
            client_secret: "client-secret".to_owned(),
        };
        let removed = logout_musicbrainz(runtime_root, Some(&config)).await.expect("logout");
        assert!(removed);
        assert!(!path.exists());
    }
}
