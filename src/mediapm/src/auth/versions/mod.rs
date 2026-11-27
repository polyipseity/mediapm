//! Serialization bridge between versioned auth wire records and domain types.
//!
//! This is the only module that knows both representations: callers work with
//! [`AuthFile`], while disk bytes always carry an integer schema version.
//!
//! ## DO NOT REMOVE: versions policy guard
//!
//! - `vX.rs` files own only version-local wire records and must never import
//!   unversioned domain structs from outside `versions/`.
//! - A `vX` file may only reference the most recent previous version, and only
//!   for version-to-version migration.
//! - This `mod.rs` is the only place where versioned auth records are bridged
//!   to unversioned domain types, through `decode` and `encode`.
//! - Files outside `versions/` must interact with the versioned records only
//!   through this module's `decode`/`encode` entry points, never through a
//!   direct `versions::vX` path.
//! - Do not re-export `versions::vX` structs. The `versions` module is private
//!   to `auth`, and its wire types stay inside this directory.

mod v1;

use serde::Deserialize;

use super::error::AuthError;
use super::store::{
    AcoustIdCredential, AuthFile, AuthServices, MusicBrainzCredential, MusicBrainzToken,
};

/// The single auth file schema version this build reads and writes.
pub(super) const AUTH_FILE_VERSION: u32 = 1;

/// Version field read before selecting a wire schema.
#[derive(Deserialize)]
struct VersionOnly {
    /// Integer schema version in the file.
    version: u32,
}

/// Decodes auth file bytes into unversioned domain types.
///
/// # Errors
///
/// Returns [`AuthError::UnknownVersion`] for any version other than 1 and
/// [`AuthError::Decode`] for malformed or unexpected records.
pub(super) fn decode(bytes: &[u8]) -> Result<AuthFile, AuthError> {
    let declared: VersionOnly = serde_json::from_slice(bytes)
        .map_err(|source| AuthError::Decode { detail: source.to_string() })?;
    match declared.version {
        AUTH_FILE_VERSION => {
            let wire: v1::AuthFileV1 = serde_json::from_slice(bytes)
                .map_err(|source| AuthError::Decode { detail: source.to_string() })?;
            Ok(AuthFile {
                services: AuthServices {
                    musicbrainz: wire.services.musicbrainz.map(|service| MusicBrainzCredential {
                        client_id: service.client_id,
                        client_secret: service.client_secret,
                        token: service.token.map(|token| MusicBrainzToken {
                            access_token: token.access_token,
                            refresh_token: token.refresh_token,
                            expires_at_unix: token.expires_at_unix,
                            scopes: token.scopes,
                            token_type: token.token_type,
                        }),
                    }),
                    acoustid: wire
                        .services
                        .acoustid
                        .map(|service| AcoustIdCredential { api_key: service.api_key }),
                },
            })
        }
        other => Err(AuthError::UnknownVersion(other)),
    }
}

/// Encodes domain types as pretty version-1 JSON terminated by a newline.
///
/// # Errors
///
/// Returns [`AuthError::Encode`] if serialization fails.
pub(super) fn encode(auth: &AuthFile) -> Result<Vec<u8>, AuthError> {
    let wire = v1::AuthFileV1 {
        version: AUTH_FILE_VERSION,
        services: v1::ServicesV1 {
            musicbrainz: auth.services.musicbrainz.as_ref().map(|service| v1::MusicBrainzV1 {
                client_id: service.client_id.clone(),
                client_secret: service.client_secret.clone(),
                token: service.token.as_ref().map(|token| v1::MusicBrainzTokenV1 {
                    access_token: token.access_token.clone(),
                    refresh_token: token.refresh_token.clone(),
                    expires_at_unix: token.expires_at_unix,
                    scopes: token.scopes.clone(),
                    token_type: token.token_type.clone(),
                }),
            }),
            acoustid: auth
                .services
                .acoustid
                .as_ref()
                .map(|service| v1::AcoustIdV1 { api_key: service.api_key.clone() }),
        },
    };
    let mut bytes = serde_json::to_vec_pretty(&wire)
        .map_err(|source| AuthError::Encode { detail: source.to_string() })?;
    bytes.push(b'\n');
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_decode_rejects_an_unknown_version() {
        let error = decode(br#"{"version":2,"services":{}}"#).expect_err("version 2 must fail");
        assert!(matches!(error, AuthError::UnknownVersion(2)), "{error:?}");
    }

    #[test]
    fn auth_round_trip_keeps_every_service() {
        let auth = AuthFile {
            services: AuthServices {
                musicbrainz: Some(MusicBrainzCredential {
                    client_id: "client-id-value".to_owned(),
                    client_secret: "client-secret-value".to_owned(),
                    token: Some(MusicBrainzToken {
                        access_token: "access-value".to_owned(),
                        refresh_token: Some("refresh-value".to_owned()),
                        expires_at_unix: 1_790_000_000,
                        scopes: vec!["profile".to_owned()],
                        token_type: "Bearer".to_owned(),
                    }),
                }),
                acoustid: Some(AcoustIdCredential { api_key: "api-key-value".to_owned() }),
            },
        };
        let bytes = encode(&auth).expect("encode");
        assert!(bytes.ends_with(b"\n"), "auth file ends with a newline");
        assert_eq!(decode(&bytes).expect("decode"), auth);
        let debug = format!("{auth:?}");
        for secret in ["client-secret-value", "access-value", "refresh-value", "api-key-value"] {
            assert!(!debug.contains(secret), "debug output contains a secret: {secret}");
        }
    }
}
