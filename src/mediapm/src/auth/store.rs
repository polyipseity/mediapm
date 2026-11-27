//! Unversioned auth domain types and strict atomic file IO.
//!
//! The versioned representation is private to [`super::versions`]; every
//! filesystem entry point in this module goes through that bridge.

use std::fmt;
use std::path::Path;

use super::error::AuthError;
use super::versions;

/// All credentials in one unified auth file.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct AuthFile {
    /// Credential records keyed by service.
    pub services: AuthServices,
}

/// Closed set of services stored in an [`AuthFile`].
#[derive(Clone, Default, PartialEq, Eq)]
pub struct AuthServices {
    /// `MusicBrainz` client credentials and optional user token.
    pub musicbrainz: Option<MusicBrainzCredential>,
    /// `AcoustID` API credential.
    pub acoustid: Option<AcoustIdCredential>,
}

/// `MusicBrainz` OAuth client credentials and optional user token.
#[derive(Clone, PartialEq, Eq)]
pub struct MusicBrainzCredential {
    /// OAuth client identifier.
    pub client_id: String,
    /// OAuth client secret.
    pub client_secret: String,
    /// User token, absent until login completes.
    pub token: Option<MusicBrainzToken>,
}

/// Stored `MusicBrainz` OAuth token.
#[derive(Clone, PartialEq, Eq)]
pub struct MusicBrainzToken {
    /// Access token used for authenticated requests.
    pub access_token: String,
    /// Refresh token, when the authorization server issued one.
    pub refresh_token: Option<String>,
    /// Unix timestamp in seconds when the access token expires.
    pub expires_at_unix: u64,
    /// Scopes granted to the access token.
    pub scopes: Vec<String>,
    /// Token type, normally `Bearer`.
    pub token_type: String,
}

impl MusicBrainzToken {
    /// Returns true when the token is expired or within 60 seconds of expiry.
    #[must_use]
    pub fn is_expired(&self, now_unix: u64) -> bool {
        now_unix >= self.expires_at_unix.saturating_sub(60)
    }
}

/// Stored `AcoustID` API credential.
#[derive(Clone, PartialEq, Eq)]
pub struct AcoustIdCredential {
    /// `AcoustID` API key.
    pub api_key: String,
}

impl fmt::Debug for AuthFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AuthFile").field("services", &self.services).finish()
    }
}

impl fmt::Debug for AuthServices {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AuthServices")
            .field("musicbrainz", &self.musicbrainz)
            .field("acoustid", &self.acoustid)
            .finish()
    }
}

impl fmt::Debug for MusicBrainzCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MusicBrainzCredential")
            .field("client_id", &self.client_id)
            .field("client_secret", &"[REDACTED]")
            .field("token", &self.token)
            .finish()
    }
}

impl fmt::Debug for MusicBrainzToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MusicBrainzToken")
            .field("access_token", &"[REDACTED]")
            .field("refresh_token", &self.refresh_token.as_ref().map(|_| "[REDACTED]"))
            .field("expires_at_unix", &self.expires_at_unix)
            .field("scopes", &self.scopes)
            .field("token_type", &self.token_type)
            .finish()
    }
}

impl fmt::Debug for AcoustIdCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AcoustIdCredential").field("api_key", &"[REDACTED]").finish()
    }
}

/// Writes an auth file atomically with mode `0600` on Unix.
///
/// The serialized bytes go to `path.with_extension("json.tmp")` first; that
/// file is created with mode `0600` on Unix, then chmodded, then renamed over
/// `path`, and the mode is reasserted on the destination. A failed write,
/// chmod, or rename removes the temporary sibling so a retry starts clean.
///
/// # Errors
///
/// Returns [`AuthError`] when directory creation, serialization, write,
/// rename, or permission setting fails.
pub fn save_auth_file(path: &Path, auth: &AuthFile) -> Result<(), AuthError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|source| AuthError::CreateDir { path: parent.to_path_buf(), source })?;
    }
    let bytes = versions::encode(auth)?;
    let tmp_path = path.with_extension("json.tmp");
    if let Err(source) = write_auth_bytes(&tmp_path, &bytes) {
        let _ = std::fs::remove_file(&tmp_path);
        return Err(AuthError::Write { path: tmp_path, source });
    }
    #[cfg(unix)]
    {
        if let Err(source) = restrict_permissions(&tmp_path) {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(AuthError::Write { path: tmp_path, source });
        }
    }
    if let Err(source) = std::fs::rename(&tmp_path, path) {
        let _ = std::fs::remove_file(&tmp_path);
        return Err(AuthError::Write { path: path.to_path_buf(), source });
    }
    #[cfg(unix)]
    {
        restrict_permissions(path)
            .map_err(|source| AuthError::Write { path: path.to_path_buf(), source })?;
    }
    Ok(())
}

/// Creates and fills the temporary auth file with owner-only permissions on
/// Unix, so credential bytes are never briefly readable by other users.
///
/// The explicit chmod in [`save_auth_file`] still covers a pre-existing
/// temporary file, whose creation mode is not controlled by this function.
#[cfg(unix)]
fn write_auth_bytes(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(bytes)
}

/// Creates and fills the temporary auth file on non-Unix platforms.
#[cfg(not(unix))]
fn write_auth_bytes(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    std::fs::write(path, bytes)
}

/// Restricts a file to owner read/write on Unix.
#[cfg(unix)]
fn restrict_permissions(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
}

/// Loads an auth file, returning `None` only when the file is absent.
///
/// # Errors
///
/// Returns [`AuthError`] for I/O failures, malformed JSON, unknown fields, or
/// an unsupported schema version. None of those cases are treated as logged
/// out.
pub fn load_auth_file(path: &Path) -> Result<Option<AuthFile>, AuthError> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(AuthError::Read { path: path.to_path_buf(), source }),
    };
    versions::decode(&bytes)
        .map_err(|error| match error {
            AuthError::Decode { detail } => {
                AuthError::Decode { detail: format!("{}: {detail}", path.display()) }
            }
            other => other,
        })
        .map(Some)
}

/// Clears one named service record and saves the resulting auth file.
///
/// Callers name services with their wire keys, `musicbrainz` and `acoustid`,
/// so this store stays independent of the auth command enum. A missing file is
/// treated as an empty auth file and written with the service cleared.
///
/// # Errors
///
/// Returns [`AuthError`] for an unknown service name or a failed load/save.
pub fn remove_service(path: &Path, service: impl AsRef<str>) -> Result<(), AuthError> {
    let mut auth = load_auth_file(path)?.unwrap_or_default();
    match service.as_ref() {
        "musicbrainz" => auth.services.musicbrainz = None,
        "acoustid" => auth.services.acoustid = None,
        other => return Err(AuthError::Service(format!("unknown auth service: {other}"))),
    }
    save_auth_file(path, &auth)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_file_write_is_atomic_and_redacts_secrets_in_debug() {
        let dir = mediapm_utils::temp::output_dir().expect("temp dir");
        let path = dir.path().join("auth.json");
        let auth = AuthFile {
            services: AuthServices {
                musicbrainz: None,
                acoustid: Some(AcoustIdCredential { api_key: "super-secret".to_owned() }),
            },
        };
        save_auth_file(&path, &auth).expect("save");
        assert_eq!(load_auth_file(&path).expect("load").expect("present"), auth);
        assert!(!format!("{auth:?}").contains("super-secret"));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600);
        }
    }

    #[cfg(unix)]
    #[test]
    fn failed_auth_write_removes_the_temporary_sibling() {
        let dir = mediapm_utils::temp::output_dir().expect("temp dir");
        let path = dir.path().join("auth.json");
        let tmp_path = path.with_extension("json.tmp");
        // Opening this symlink for creation fails because its target parent
        // does not exist, so no regular temp file is created.
        std::os::unix::fs::symlink(dir.path().join("missing-parent/auth.json.tmp"), &tmp_path)
            .expect("seed broken temp symlink");
        assert!(save_auth_file(&path, &AuthFile::default()).is_err());
        assert!(
            std::fs::symlink_metadata(&tmp_path).is_err(),
            "failed write must remove the temporary sibling"
        );
        assert!(!path.exists(), "failed write must not publish the auth file");
    }

    #[cfg(unix)]
    #[test]
    fn auth_temp_file_is_created_with_owner_only_permissions() {
        use std::os::unix::fs::PermissionsExt;
        let dir = mediapm_utils::temp::output_dir().expect("temp dir");
        let tmp_path = dir.path().join("auth.json.tmp");
        write_auth_bytes(&tmp_path, b"credential bytes").expect("write temp");
        let mode =
            std::fs::metadata(&tmp_path).expect("temp metadata").permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "temp file must be 0600 at creation, before chmod");
    }

    #[test]
    fn auth_file_missing_is_none_and_malformed_is_an_error() {
        let dir = mediapm_utils::temp::output_dir().expect("temp dir");
        let path = dir.path().join("auth.json");
        assert!(load_auth_file(&path).expect("load").is_none());
        std::fs::write(&path, b"{not json").expect("seed malformed file");
        assert!(load_auth_file(&path).is_err());
    }

    #[test]
    fn remove_service_clears_one_service_and_saves() {
        let dir = mediapm_utils::temp::output_dir().expect("temp dir");
        let path = dir.path().join("auth.json");
        let auth = AuthFile {
            services: AuthServices {
                musicbrainz: Some(MusicBrainzCredential {
                    client_id: "id".to_owned(),
                    client_secret: "secret".to_owned(),
                    token: None,
                }),
                acoustid: Some(AcoustIdCredential { api_key: "key".to_owned() }),
            },
        };
        save_auth_file(&path, &auth).expect("seed");
        remove_service(&path, "acoustid").expect("remove acoustid");
        let loaded = load_auth_file(&path).expect("load").expect("present");
        assert!(loaded.services.acoustid.is_none());
        assert_eq!(loaded.services.musicbrainz, auth.services.musicbrainz);
    }

    #[test]
    fn remove_service_rejects_an_unknown_service() {
        let dir = mediapm_utils::temp::output_dir().expect("temp dir");
        let path = dir.path().join("auth.json");
        let error = remove_service(&path, "lastfm").expect_err("unknown service must fail");
        assert!(matches!(error, super::AuthError::Service(_)), "{error:?}");
    }
}
