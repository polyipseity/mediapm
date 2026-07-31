//! Desired-tool specification matching.
//!
//! Tools for checking whether a registry entry satisfies a [`VersionSpec`]
//! for skip-if-already-deployed decisions.
//!
//! All matching uses **exact string comparison** — no semver, no prefix
//! stripping, no whitespace normalization. `None` resolved fields never
//! match (a missing resolved value cannot satisfy a specified field).

#![cfg(feature = "tool-presets")]

use crate::tools::provider::VersionSpec;

#[cfg(test)]
use crate::tools::provider::VersionSpecFields;

/// Check whether a registry entry's resolved fields satisfy a [`VersionSpec`].
///
/// Returns `true` if the entry already matches the spec (meaning the tool
/// deployment can be skipped — no re-resolve needed).
///
/// For [`VersionSpec::Latest`], always returns `false` — the caller must
/// re-resolve.
///
/// # Exact string match
///
/// All comparisons are exact string match with no whitespace normalization.
/// A `None` resolved field never matches — the entry is treated as not
/// having that provenance, so an exact spec requiring it triggers a
/// re-provision.
#[must_use]
pub fn spec_matches_entry(
    spec: &VersionSpec,
    resolved_tag: Option<&str>,
    resolved_version: Option<&str>,
    resolved_vcs_hash: Option<&str>,
) -> bool {
    match spec {
        VersionSpec::Latest => false,
        VersionSpec::Exact(fields) => {
            // All specified fields must match their resolved counterpart.
            // Unspecified fields are not checked. `None` never matches.
            let hash_ok =
                fields.vcs_hash.as_ref().map_or(true, |h| resolved_vcs_hash == Some(h.as_str()));
            let ver_ok =
                fields.version.as_ref().map_or(true, |v| resolved_version == Some(v.as_str()));
            let tag_ok = fields.tag.as_ref().map_or(true, |t| resolved_tag == Some(t.as_str()));
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
        assert!(spec_matches_entry(&spec, None, None, Some("abc123")));
        assert!(!spec_matches_entry(&spec, None, None, Some("def456")));
    }

    #[test]
    fn spec_matches_tag() {
        let spec = VersionSpec::Exact(VersionSpecFields {
            vcs_hash: None,
            version: None,
            tag: Some("v1.2.3".into()),
        });
        assert!(spec_matches_entry(&spec, Some("v1.2.3"), None, None));
        assert!(!spec_matches_entry(&spec, Some("v2.0.0"), None, None));
    }

    #[test]
    fn spec_matches_version() {
        let spec = VersionSpec::Exact(VersionSpecFields {
            vcs_hash: None,
            version: Some("2025.01.15".into()),
            tag: None,
        });
        assert!(spec_matches_entry(&spec, None, Some("2025.01.15"), None));
        assert!(!spec_matches_entry(&spec, None, Some("2024.12.01"), None));
    }

    #[test]
    fn spec_matches_multi_field_all_must_match() {
        let spec = VersionSpec::Exact(VersionSpecFields {
            vcs_hash: Some("abc".into()),
            version: Some("1.0".into()),
            tag: None,
        });
        assert!(spec_matches_entry(&spec, None, Some("1.0"), Some("abc")));
        assert!(!spec_matches_entry(&spec, None, Some("1.0"), Some("def")));
        assert!(!spec_matches_entry(&spec, None, Some("2.0"), Some("abc")));
    }

    #[test]
    fn spec_matches_latest_never() {
        let spec = VersionSpec::Latest;
        assert!(!spec_matches_entry(&spec, Some("anything"), Some("anything"), Some("anything")));
    }

    #[test]
    fn spec_matches_exact_string_no_semver() {
        // "v1.2.3" does NOT match "1.2.3" — exact string comparison.
        let spec = VersionSpec::Exact(VersionSpecFields {
            vcs_hash: None,
            version: None,
            tag: Some("v1.2.3".into()),
        });
        assert!(!spec_matches_entry(&spec, Some("1.2.3"), None, None));
    }

    #[test]
    fn spec_matches_whitespace_not_normalized() {
        // Whitespace is preserved — "  v1.2.3  " does NOT match "v1.2.3".
        let spec = VersionSpec::Exact(VersionSpecFields {
            vcs_hash: None,
            version: None,
            tag: Some("v1.2.3".into()),
        });
        assert!(!spec_matches_entry(&spec, Some("  v1.2.3  "), None, None));
    }

    #[test]
    fn spec_matches_unspecified_field_not_checked() {
        // version field not specified — should not be checked
        let spec = VersionSpec::Exact(VersionSpecFields {
            vcs_hash: None,
            version: None,
            tag: Some("v1.0".into()),
        });
        assert!(spec_matches_entry(&spec, Some("v1.0"), Some("anything"), None));
    }

    #[test]
    fn spec_matches_none_never_matches() {
        // A `None` resolved field cannot satisfy a specified field — the
        // entry must be re-provisioned.
        let spec = VersionSpec::Exact(VersionSpecFields {
            vcs_hash: None,
            version: None,
            tag: Some("v1.2.3".into()),
        });
        assert!(!spec_matches_entry(&spec, None, None, None));
        let hash_spec = VersionSpec::Exact(VersionSpecFields {
            vcs_hash: Some("abc123".into()),
            version: None,
            tag: None,
        });
        assert!(!spec_matches_entry(&hash_spec, None, None, None));
    }
}
