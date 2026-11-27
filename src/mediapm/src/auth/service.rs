//! Auth command dispatch over the two supported services.
//!
//! Every CLI action maps to exactly one pair of service and action through
//! [`run_auth_action`], which calls the matching `musicbrainz` or `acoustid`
//! service function. All six pairs are explicit; there is no fallback.

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
pub async fn run_auth_action(
    service: AuthServiceId,
    action: AuthAction,
    options: AuthOptions,
    runtime: &AuthRuntime<'_>,
) -> Result<AuthOutcome, AuthError> {
    match (service, action) {
        (AuthServiceId::Musicbrainz, AuthAction::Login) => {
            super::musicbrainz::login(runtime.auth_file, runtime.prompt, options.offline).await
        }
        (AuthServiceId::Musicbrainz, AuthAction::Status) => {
            super::musicbrainz::status(runtime.auth_file)
        }
        (AuthServiceId::Musicbrainz, AuthAction::Logout) => {
            super::musicbrainz::logout(runtime.auth_file, options.offline).await
        }
        (AuthServiceId::Acoustid, AuthAction::Login) => {
            super::acoustid::login(runtime.auth_file, runtime.prompt, options.offline).await
        }
        (AuthServiceId::Acoustid, AuthAction::Status) => super::acoustid::status(runtime.auth_file),
        (AuthServiceId::Acoustid, AuthAction::Logout) => {
            let outcome = super::acoustid::logout(runtime.auth_file)?;
            if options.offline {
                Ok(AuthOutcome {
                    message: format!(
                        "{} (--offline has no separate revocation step)",
                        outcome.message
                    ),
                })
            } else {
                Ok(outcome)
            }
        }
    }
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
