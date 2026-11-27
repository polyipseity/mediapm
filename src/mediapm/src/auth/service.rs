//! Auth command dispatch over the two supported services.
//!
//! Every CLI action maps to exactly one pair of service and action through
//! [`run_auth_action`]. The six service functions below are temporary stubs
//! until Tasks 5 and 6 replace their bodies with the real service-module
//! calls; each stub returns [`AuthError::Service`] instead of claiming
//! success or touching the auth file.

use std::path::Path;

use super::error::AuthError;
use super::prompt::SecretPrompt;

/// Service whose credentials an auth command acts on.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthServiceId {
    /// `MusicBrainz` OAuth credentials.
    Musicbrainz,
    /// `AcoustID` API key.
    Acoustid,
}

impl AuthServiceId {
    /// Returns the wire key understood by the versioned auth file store.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Musicbrainz => "musicbrainz",
            Self::Acoustid => "acoustid",
        }
    }
}

/// Action to perform for one auth service.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthAction {
    /// Store credentials, prompting for any missing secret.
    Login,
    /// Report credential state without printing secrets.
    Status,
    /// Remove the stored credentials.
    Logout,
}

/// Options shared by every auth action.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AuthOptions {
    /// Skip remote validation or revocation for this action.
    pub offline: bool,
}

/// Paths and injected seams an auth action needs.
pub struct AuthRuntime<'a> {
    /// Effective path of the unified `auth.json`.
    pub auth_file: &'a Path,
    /// Source of secrets supplied through hidden prompts.
    pub prompt: &'a dyn SecretPrompt,
}

/// Result of an auth action, safe to print verbatim.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthOutcome {
    /// Human-readable outcome containing no secret value.
    pub message: String,
}

/// Runs one service/action pair against the effective auth file.
///
/// # Errors
///
/// Returns the error from the selected service action. All six pairs have an
/// explicit arm; there is no fallback that can silently succeed.
// The dispatch signature is async by contract. Tasks 5 and 6 replace the
// synchronous stubs with async service calls, which restores a real await.
#[expect(
    clippy::unused_async,
    reason = "run_auth_action is async by contract while the six service stubs are temporary synchronous placeholders; Tasks 5 and 6 remove this expectation when the real async service calls land"
)]
pub async fn run_auth_action(
    service: AuthServiceId,
    action: AuthAction,
    options: AuthOptions,
    runtime: &AuthRuntime<'_>,
) -> Result<AuthOutcome, AuthError> {
    match (service, action) {
        (AuthServiceId::Musicbrainz, AuthAction::Login) => {
            musicbrainz_login(runtime.auth_file, runtime.prompt, options.offline)
        }
        (AuthServiceId::Musicbrainz, AuthAction::Status) => musicbrainz_status(runtime.auth_file),
        (AuthServiceId::Musicbrainz, AuthAction::Logout) => {
            musicbrainz_logout(runtime.auth_file, options.offline)
        }
        (AuthServiceId::Acoustid, AuthAction::Login) => {
            acoustid_login(runtime.auth_file, runtime.prompt, options.offline)
        }
        (AuthServiceId::Acoustid, AuthAction::Status) => acoustid_status(runtime.auth_file),
        (AuthServiceId::Acoustid, AuthAction::Logout) => acoustid_logout(runtime.auth_file),
    }
}

/// Builds the loud failure used by every not-yet-implemented service action.
fn stub_error(action: &'static str) -> AuthError {
    AuthError::Service(format!("{action} is not available in this build"))
}

/// Temporary placeholder for `MusicBrainz` login; Task 6 replaces it with the real service call.
fn musicbrainz_login(
    _auth_file: &Path,
    _prompt: &dyn SecretPrompt,
    _offline: bool,
) -> Result<AuthOutcome, AuthError> {
    Err(stub_error("MusicBrainz login"))
}

/// Temporary placeholder for `MusicBrainz` status; Task 6 replaces it with the real service call.
fn musicbrainz_status(_auth_file: &Path) -> Result<AuthOutcome, AuthError> {
    Err(stub_error("MusicBrainz status"))
}

/// Temporary placeholder for `MusicBrainz` logout; Task 6 replaces it with the real service call.
fn musicbrainz_logout(_auth_file: &Path, _offline: bool) -> Result<AuthOutcome, AuthError> {
    Err(stub_error("MusicBrainz logout"))
}

/// Temporary placeholder for `AcoustID` login; Task 5 replaces it with the real service call.
fn acoustid_login(
    _auth_file: &Path,
    _prompt: &dyn SecretPrompt,
    _offline: bool,
) -> Result<AuthOutcome, AuthError> {
    Err(stub_error("AcoustID login"))
}

/// Temporary placeholder for `AcoustID` status; Task 5 replaces it with the real service call.
fn acoustid_status(_auth_file: &Path) -> Result<AuthOutcome, AuthError> {
    Err(stub_error("AcoustID status"))
}

/// Temporary placeholder for `AcoustID` logout; Task 5 replaces it with the real service call.
fn acoustid_logout(_auth_file: &Path) -> Result<AuthOutcome, AuthError> {
    Err(stub_error("AcoustID logout"))
}

#[cfg(test)]
mod tests {
    use super::AuthServiceId;

    #[test]
    fn auth_service_ids_use_the_wire_keys() {
        assert_eq!(AuthServiceId::Musicbrainz.as_str(), "musicbrainz");
        assert_eq!(AuthServiceId::Acoustid.as_str(), "acoustid");
    }
}
