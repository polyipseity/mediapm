//! Version 1 wire records for the unified auth file.
//!
//! These structs describe the on-disk shape only. Every record is closed: an
//! unknown field or service name fails deserialization rather than being
//! ignored. Domain conversion lives in the parent module.

use serde::{Deserialize, Serialize};

/// Root record of an `auth.json` file at schema version 1.
#[derive(Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct AuthFileV1 {
    /// Integer schema version.
    pub(super) version: u32,
    /// Credentials keyed by service name.
    pub(super) services: ServicesV1,
}

/// Closed set of services that version 1 can store.
#[derive(Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct ServicesV1 {
    /// `MusicBrainz` client credentials and optional user token.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) musicbrainz: Option<MusicBrainzV1>,
    /// `AcoustID` API credential.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) acoustid: Option<AcoustIdV1>,
}

/// Stored `MusicBrainz` OAuth client credentials and user token.
#[derive(Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct MusicBrainzV1 {
    /// OAuth client identifier.
    pub(super) client_id: String,
    /// OAuth client secret.
    pub(super) client_secret: String,
    /// User token, absent when no login has completed.
    #[serde(default)]
    pub(super) token: Option<MusicBrainzTokenV1>,
}

/// Stored `MusicBrainz` OAuth access token.
#[derive(Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct MusicBrainzTokenV1 {
    /// Access token used for authenticated requests.
    pub(super) access_token: String,
    /// Refresh token, when the authorization server issued one.
    #[serde(default)]
    pub(super) refresh_token: Option<String>,
    /// Unix timestamp in seconds when the access token expires.
    pub(super) expires_at_unix: u64,
    /// Scopes granted to the access token.
    #[serde(default)]
    pub(super) scopes: Vec<String>,
    /// Token type, defaulting to `Bearer` when omitted.
    #[serde(default = "default_token_type")]
    pub(super) token_type: String,
}

/// Stored `AcoustID` API credential.
#[derive(Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct AcoustIdV1 {
    /// `AcoustID` API key.
    pub(super) api_key: String,
}

/// Default token type for records that omit `token_type`.
fn default_token_type() -> String {
    "Bearer".to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_v1_rejects_unknown_fields() {
        let json = r#"{"version":1,"services":{},"surprise":true}"#;
        assert!(serde_json::from_str::<AuthFileV1>(json).is_err());
    }

    #[test]
    fn auth_v1_rejects_unknown_services() {
        let json = r#"{"version":1,"services":{"lastfm":{"api_key":"x"}}}"#;
        assert!(serde_json::from_str::<AuthFileV1>(json).is_err());
    }

    #[test]
    fn auth_v1_rejects_a_non_integer_version() {
        let json = r#"{"version":1.5,"services":{}}"#;
        assert!(serde_json::from_str::<AuthFileV1>(json).is_err());
    }
}
