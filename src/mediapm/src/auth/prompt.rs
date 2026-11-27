//! Hidden prompt seam shared by the auth services.
//!
//! Real secrets come from a terminal without echo; tests inject a fixed
//! sequence through [`SecretPrompt`] instead of driving a real TTY.

#[cfg(test)]
use std::cell::RefCell;

use console::Term;

use super::error::AuthError;

/// Reads one secret without echoing it to the user.
pub trait SecretPrompt {
    /// Writes `label` to the prompt target and returns the trimmed secret.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::EmptySecret`] when the trimmed input is empty and
    /// [`AuthError::Service`] when the prompt cannot be written or read.
    fn read_secret(&self, label: &'static str) -> Result<String, AuthError>;
}

/// Secret prompt backed by stderr and `console`'s no-echo line reader.
///
/// `Term::stderr()` is unbuffered, so `write_str` reaches stderr before
/// `read_secure_line` blocks (`console` 0.15.11 `src/term.rs:161`, `:237`,
/// `:373`). That reader returns `Ok("")` when stderr is not a TTY, which this
/// seam turns into [`AuthError::EmptySecret`] rather than a default secret.
pub struct ConsoleSecretPrompt {
    term: Term,
}

impl ConsoleSecretPrompt {
    /// Creates a prompt bound to the current process's stderr.
    #[must_use]
    pub fn new() -> Self {
        Self { term: Term::stderr() }
    }
}

impl Default for ConsoleSecretPrompt {
    fn default() -> Self {
        Self::new()
    }
}

impl SecretPrompt for ConsoleSecretPrompt {
    fn read_secret(&self, label: &'static str) -> Result<String, AuthError> {
        self.term.write_str(label).map_err(|source| {
            AuthError::Service(format!("failed to write secret prompt: {source}"))
        })?;
        let line = self
            .term
            .read_secure_line()
            .map_err(|source| AuthError::Service(format!("failed to read secret: {source}")))?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            return Err(AuthError::EmptySecret);
        }
        Ok(trimmed.to_owned())
    }
}

/// Test-only prompt that yields queued secrets in request order.
#[cfg(test)]
pub(crate) struct FixedPrompt {
    secrets: RefCell<Vec<String>>,
}

#[cfg(test)]
impl FixedPrompt {
    /// Queues secrets in the order they will be requested.
    pub(crate) fn new(secrets: Vec<String>) -> Self {
        Self { secrets: RefCell::new(secrets) }
    }
}

#[cfg(test)]
impl SecretPrompt for FixedPrompt {
    fn read_secret(&self, _label: &'static str) -> Result<String, AuthError> {
        let mut secrets = self.secrets.borrow_mut();
        if secrets.is_empty() {
            return Err(AuthError::Service("test prompt has no queued secrets left".to_owned()));
        }
        let trimmed = secrets.remove(0).trim().to_owned();
        if trimmed.is_empty() {
            return Err(AuthError::EmptySecret);
        }
        Ok(trimmed)
    }
}

#[cfg(test)]
impl std::fmt::Debug for FixedPrompt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FixedPrompt")
    }
}

#[cfg(test)]
mod tests {
    use super::{FixedPrompt, SecretPrompt};

    #[test]
    fn fixed_prompt_returns_secrets_in_order() {
        let prompt = FixedPrompt::new(vec!["id".to_owned(), "secret".to_owned()]);
        assert_eq!(prompt.read_secret("client id").unwrap(), "id");
        assert_eq!(prompt.read_secret("client secret").unwrap(), "secret");
        assert!(!format!("{prompt:?}").contains("secret"));
    }
}
