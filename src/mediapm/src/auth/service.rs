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

/// Appends the shared `--offline` notice when an action has no remote step.
///
/// Both `message` and `absent_step` are caller-controlled fixed strings, so
/// no credential value can enter the notice.
fn with_offline_notice(outcome: AuthOutcome, offline: bool, absent_step: &str) -> AuthOutcome {
    if !offline {
        return outcome;
    }
    AuthOutcome {
        message: format!("{} (--offline has no separate {absent_step} step)", outcome.message),
    }
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
            let outcome = super::musicbrainz::status(runtime.auth_file)?;
            Ok(with_offline_notice(outcome, options.offline, "remote"))
        }
        (AuthServiceId::Musicbrainz, AuthAction::Logout) => {
            super::musicbrainz::logout(runtime.auth_file, options.offline).await
        }
        (AuthServiceId::Acoustid, AuthAction::Login) => {
            super::acoustid::login(runtime.auth_file, runtime.prompt, options.offline).await
        }
        (AuthServiceId::Acoustid, AuthAction::Status) => {
            let outcome = super::acoustid::status(runtime.auth_file)?;
            Ok(with_offline_notice(outcome, options.offline, "remote"))
        }
        (AuthServiceId::Acoustid, AuthAction::Logout) => {
            let outcome = super::acoustid::logout(runtime.auth_file)?;
            Ok(with_offline_notice(outcome, options.offline, "revocation"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{AuthAction, AuthOptions, AuthRuntime, AuthServiceId, run_auth_action};
    use crate::auth::prompt::FixedPrompt;
    use crate::auth::store::{
        AcoustIdCredential, AuthFile, AuthServices, MusicBrainzCredential, save_auth_file,
    };

    const OFFLINE_STATUS_NOTICE: &str = "(--offline has no separate remote step)";

    #[test]
    fn auth_service_ids_use_the_wire_keys() {
        assert_eq!(AuthServiceId::Musicbrainz.as_str(), "musicbrainz");
        assert_eq!(AuthServiceId::Acoustid.as_str(), "acoustid");
    }

    #[tokio::test]
    async fn musicbrainz_status_reports_offline_notice_without_secrets() {
        const SECRET: &str = "musicbrainz-client-secret-that-must-not-leak";
        let dir = mediapm_utils::temp::output_dir().expect("temp dir");
        let auth_file = dir.path().join("auth.json");
        let auth = AuthFile {
            services: AuthServices {
                musicbrainz: Some(MusicBrainzCredential {
                    client_id: "client-id".to_owned(),
                    client_secret: SECRET.to_owned(),
                    token: None,
                }),
                ..AuthServices::default()
            },
        };
        save_auth_file(&auth_file, &auth).expect("seed");
        let prompt = FixedPrompt::new(Vec::new());
        let runtime = AuthRuntime { auth_file: &auth_file, prompt: &prompt };
        let base = "MusicBrainz: credentials configured; no user token";

        let online = run_auth_action(
            AuthServiceId::Musicbrainz,
            AuthAction::Status,
            AuthOptions { offline: false },
            &runtime,
        )
        .await
        .expect("online status");
        assert_eq!(online.message, base);

        let offline = run_auth_action(
            AuthServiceId::Musicbrainz,
            AuthAction::Status,
            AuthOptions { offline: true },
            &runtime,
        )
        .await
        .expect("offline status");
        assert_eq!(offline.message, format!("{base} {OFFLINE_STATUS_NOTICE}"));
        assert!(!offline.message.contains(SECRET), "status leaked the secret: {}", offline.message);
    }

    #[tokio::test]
    async fn acoustid_status_reports_offline_notice_without_secrets() {
        const SECRET: &str = "acoustid-key-that-must-not-leak";
        let dir = mediapm_utils::temp::output_dir().expect("temp dir");
        let auth_file = dir.path().join("auth.json");
        let auth = AuthFile {
            services: AuthServices {
                acoustid: Some(AcoustIdCredential { api_key: SECRET.to_owned() }),
                ..AuthServices::default()
            },
        };
        save_auth_file(&auth_file, &auth).expect("seed");
        let prompt = FixedPrompt::new(Vec::new());
        let runtime = AuthRuntime { auth_file: &auth_file, prompt: &prompt };
        let base = "AcoustID: logged in";

        let online = run_auth_action(
            AuthServiceId::Acoustid,
            AuthAction::Status,
            AuthOptions { offline: false },
            &runtime,
        )
        .await
        .expect("online status");
        assert_eq!(online.message, base);

        let offline = run_auth_action(
            AuthServiceId::Acoustid,
            AuthAction::Status,
            AuthOptions { offline: true },
            &runtime,
        )
        .await
        .expect("offline status");
        assert_eq!(offline.message, format!("{base} {OFFLINE_STATUS_NOTICE}"));
        assert!(!offline.message.contains(SECRET), "status leaked the key: {}", offline.message);
    }
}
