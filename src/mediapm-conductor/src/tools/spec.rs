//! Desired-tool specification matching.
//!
//! Tools for checking whether a registry entry satisfies a [`VersionSpec`]
//! for skip-if-already-deployed decisions.
//!
//! All matching uses **exact string comparison** — no semver, no prefix
//! stripping, no normalization beyond trimming whitespace.

#![cfg(feature = "tool-presets")]

use crate::tools::provider::VersionSpec;

#[cfg(test)]
use crate::tools::provider::VersionSpecFields;

/// Check whether a registry entry's resolved fields satisfy a [`VersionSpec`].
///
/// Returns `true` if the entry already matches the spec (meaning the tool
/// deployment can be skipped — no re-resolve needed).
///
/// For [`VersionSpec::Latest`] and [`VersionSpec::Inherit`], always returns
/// `false` — the caller must re-resolve.
///
/// # Exact string match
///
/// All comparisons are exact string match. Fields are compared as-is after
/// trimming whitespace.
#[must_use]
pub fn spec_matches_entry(
    spec: &VersionSpec,
    resolved_tag: &str,
    resolved_version: &str,
    resolved_vcs_hash: &str,
) -> bool {
    match spec {
        VersionSpec::Latest => false,
        VersionSpec::Inherit => false, // inherit always re-resolves
        VersionSpec::Exact(fields) => {
            // All specified fields must match their resolved counterpart.
            // Unspecified fields are not checked.
            let hash_ok =
                fields.vcs_hash.as_ref().map_or(true, |h| resolved_vcs_hash.trim() == h.trim());
            let ver_ok =
                fields.version.as_ref().map_or(true, |v| resolved_version.trim() == v.trim());
            let tag_ok = fields.tag.as_ref().map_or(true, |t| resolved_tag.trim() == t.trim());
            hash_ok && ver_ok && tag_ok
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spec_matches_vcs_hash() {
        let spec = VersionSpec::Exact(VersionSpecFields {
            vcs_hash: Some("abc123".into()),
            version: None,
            tag: None,
        });
        assert!(spec_matches_entry(&spec, "", "", "abc123"));
        assert!(!spec_matches_entry(&spec, "", "", "def456"));
    }

    #[test]
    fn spec_matches_tag() {
        let spec = VersionSpec::Exact(VersionSpecFields {
            vcs_hash: None,
            version: None,
            tag: Some("v1.2.3".into()),
        });
        assert!(spec_matches_entry(&spec, "v1.2.3", "", ""));
        assert!(!spec_matches_entry(&spec, "v2.0.0", "", ""));
    }

    #[test]
    fn spec_matches_version() {
        let spec = VersionSpec::Exact(VersionSpecFields {
            vcs_hash: None,
            version: Some("2025.01.15".into()),
            tag: None,
        });
        assert!(spec_matches_entry(&spec, "", "2025.01.15", ""));
        assert!(!spec_matches_entry(&spec, "", "2024.12.01", ""));
    }

    #[test]
    fn spec_matches_multi_field_all_must_match() {
        let spec = VersionSpec::Exact(VersionSpecFields {
            vcs_hash: Some("abc".into()),
            version: Some("1.0".into()),
            tag: None,
        });
        assert!(spec_matches_entry(&spec, "", "1.0", "abc"));
        assert!(!spec_matches_entry(&spec, "", "1.0", "def"));
        assert!(!spec_matches_entry(&spec, "", "2.0", "abc"));
    }

    #[test]
    fn spec_matches_latest_never() {
        let spec = VersionSpec::Latest;
        assert!(!spec_matches_entry(&spec, "anything", "anything", "anything"));
    }

    #[test]
    fn spec_matches_inherit_never() {
        let spec = VersionSpec::Inherit;
        assert!(!spec_matches_entry(&spec, "anything", "anything", "anything"));
    }

    #[test]
    fn spec_matches_exact_string_no_semver() {
        // "v1.2.3" does NOT match "1.2.3" — exact string comparison.
        let spec = VersionSpec::Exact(VersionSpecFields {
            vcs_hash: None,
            version: None,
            tag: Some("v1.2.3".into()),
        });
        assert!(!spec_matches_entry(&spec, "1.2.3", "", ""));
    }

    #[test]
    fn spec_matches_trims_whitespace() {
        let spec = VersionSpec::Exact(VersionSpecFields {
            vcs_hash: None,
            version: None,
            tag: Some("v1.2.3".into()),
        });
        assert!(spec_matches_entry(&spec, "  v1.2.3  ", "", ""));
    }

    #[test]
    fn spec_matches_unspecified_field_not_checked() {
        // version field not specified — should not be checked
        let spec = VersionSpec::Exact(VersionSpecFields {
            vcs_hash: None,
            version: None,
            tag: Some("v1.0".into()),
        });
        assert!(spec_matches_entry(&spec, "v1.0", "anything", ""));
    }

    // ---------------------------------------------------------------------------
    // Phase 6 — VersionSpec serde round-trip
    //
    // VersionSpec uses custom Serialize/Deserialize so unit variants serialize
    // to/from JSON strings "latest" and "inherit" (matching the Nickel schema).
    // ---------------------------------------------------------------------------

    #[test]
    fn version_spec_serde_latest() {
        let json = serde_json::json!("latest");
        let spec: VersionSpec = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(spec, VersionSpec::Latest);
        let back = serde_json::to_value(&spec).unwrap();
        assert_eq!(back, json);
    }

    #[test]
    fn version_spec_serde_inherit() {
        let json = serde_json::json!("inherit");
        let spec: VersionSpec = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(spec, VersionSpec::Inherit);
        let back = serde_json::to_value(&spec).unwrap();
        assert_eq!(back, json);
    }

    #[test]
    fn version_spec_serde_exact_vcs_hash() {
        let json = serde_json::json!({"vcs_hash": "abc123"});
        let spec: VersionSpec = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(
            spec,
            VersionSpec::Exact(VersionSpecFields {
                vcs_hash: Some("abc123".into()),
                version: None,
                tag: None,
            })
        );
        let back = serde_json::to_value(&spec).unwrap();
        assert_eq!(back, json);
    }

    #[test]
    fn version_spec_serde_exact_version() {
        let json = serde_json::json!({"version": "1.0"});
        let spec: VersionSpec = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(
            spec,
            VersionSpec::Exact(VersionSpecFields {
                vcs_hash: None,
                version: Some("1.0".into()),
                tag: None,
            })
        );
        let back = serde_json::to_value(&spec).unwrap();
        assert_eq!(back, json);
    }

    #[test]
    fn version_spec_serde_exact_tag() {
        let json = serde_json::json!({"tag": "v1.2.3"});
        let spec: VersionSpec = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(
            spec,
            VersionSpec::Exact(VersionSpecFields {
                vcs_hash: None,
                version: None,
                tag: Some("v1.2.3".into()),
            })
        );
        let back = serde_json::to_value(&spec).unwrap();
        assert_eq!(back, json);
    }

    #[test]
    fn version_spec_serde_exact_multi_field() {
        let json = serde_json::json!({"vcs_hash": "abc", "version": "1.0", "tag": "v1.0"});
        let spec: VersionSpec = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(
            spec,
            VersionSpec::Exact(VersionSpecFields {
                vcs_hash: Some("abc".into()),
                version: Some("1.0".into()),
                tag: Some("v1.0".into()),
            })
        );
        let back = serde_json::to_value(&spec).unwrap();
        assert_eq!(back, json);
    }

    #[test]
    fn version_spec_serde_empty_object_error() {
        let json = serde_json::json!({});
        let result: Result<VersionSpec, _> = serde_json::from_value(json);
        assert!(result.is_err(), "empty object should fail deserialization");
        let err = result.unwrap_err().to_string();
        assert!(err.contains("at least one"), "error should mention at least one field: {err}");
    }

    #[test]
    fn version_spec_serde_deny_unknown_fields() {
        // Unknown field in object causes VersionSpecFields to reject it.
        // With untagged enum, the final error is "data did not match any variant".
        let json = serde_json::json!({"vcs_hash": "abc", "unknown": "x"});
        let result: Result<VersionSpec, _> = serde_json::from_value(json);
        assert!(result.is_err(), "unknown fields should be rejected");
    }
}
