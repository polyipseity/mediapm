//! Offline `AcoustID` auth commands reach the same auth-file-backed API the
//! CLI dispatches to.
//!
//! The library's fixed prompt is `cfg(test)` and is not visible to an
//! integration test, so this file owns a prompt double over the public seam.

use std::cell::RefCell;

use mediapm::auth::error::AuthError;
use mediapm::auth::prompt::SecretPrompt;
use mediapm::auth::service::{
    AuthAction, AuthOptions, AuthRuntime, AuthServiceId, run_auth_action,
};
use mediapm::auth::store::load_auth_file;

/// Feeds queued secrets to auth actions without a terminal.
struct IntegrationPrompt {
    secrets: RefCell<Vec<String>>,
}

impl IntegrationPrompt {
    fn new(secrets: Vec<String>) -> Self {
        Self { secrets: RefCell::new(secrets) }
    }
}

impl SecretPrompt for IntegrationPrompt {
    fn read_secret(&self, _label: &'static str) -> Result<String, AuthError> {
        let secret = self.secrets.borrow_mut().remove(0);
        let trimmed = secret.trim();
        if trimmed.is_empty() {
            return Err(AuthError::EmptySecret);
        }
        Ok(trimmed.to_owned())
    }
}

#[tokio::test]
async fn acoustid_login_status_logout_use_the_configured_auth_file() {
    const SECRET: &str = "integration-key-that-must-not-leak";

    let dir = mediapm_utils::temp::output_dir().expect("temp dir");
    let auth_file = dir.path().join("auth.json");
    let prompt = IntegrationPrompt::new(vec![SECRET.to_owned()]);
    let runtime = AuthRuntime { auth_file: &auth_file, prompt: &prompt };
    let options = AuthOptions { offline: true };
    let service = AuthServiceId::Acoustid;

    let login =
        run_auth_action(service, AuthAction::Login, options, &runtime).await.expect("login");
    assert!(!login.message.contains(SECRET), "login outcome leaked the key: {}", login.message);
    let stored = load_auth_file(&auth_file).expect("load").expect("auth file");
    assert_eq!(stored.services.acoustid.expect("stored key").api_key, SECRET);

    let status =
        run_auth_action(service, AuthAction::Status, options, &runtime).await.expect("status");
    assert_eq!(status.message, "AcoustID: logged in");
    assert!(!status.message.contains(SECRET), "status leaked the key: {}", status.message);

    let outcome =
        run_auth_action(service, AuthAction::Logout, options, &runtime).await.expect("logout");
    assert!(
        outcome.message.contains("--offline has no separate revocation step"),
        "offline logout must explain the missing revocation step: {}",
        outcome.message
    );
    assert!(
        !outcome.message.contains(SECRET),
        "logout outcome leaked the key: {}",
        outcome.message
    );

    let loaded = load_auth_file(&auth_file).expect("load").expect("auth file");
    assert!(loaded.services.acoustid.is_none());
}
