//! Desired-tool specification matching.
//!
//! Tools for parsing user-facing desired-spec fields
//! (`desired_git_hash`, `desired_tag`, `desired_version`) into
//! [`VersionSpec`] and checking whether a registry entry satisfies a spec
//! for skip-if-already-deployed decisions.
//!
//! All matching uses **exact string comparison** — no semver, no prefix
//! stripping, no normalization beyond trimming whitespace.

#![cfg(feature = "tool-presets")]

use crate::tools::provider::VersionSpec;

/// Error from resolving deselection fields.
#[derive(Debug, Clone, thiserror::Error)]
pub enum SpecConflictError {
    /// Multiple non-empty desired fields were specified simultaneously.
    #[error("tool requirement: cannot specify {fields} simultaneously")]
    MultipleNonEmpty { fields: String },
}

/// Parse `desired_git_hash`, `desired_tag`, `desired_version` strings into a
/// [`VersionSpec`].
///
/// Returns [`SpecConflictError`] when more than one non-empty field is set.
/// Returns [`VersionSpec::Latest`] when all fields are empty (no constraint).
///
/// # Exact string comparison
///
/// Version comparison throughout this module uses **exact string match**.
/// No semver parsing, no prefix stripping, no normalization.
/// The user's config value is compared byte-for-byte against the resolved value.
pub fn resolve_desired_spec(
    desired_git_hash: &str,
    desired_tag: &str,
    desired_version: &str,
) -> Result<VersionSpec, SpecConflictError> {
    let mut non_empty: Vec<&str> = Vec::new();
    if !desired_git_hash.trim().is_empty() {
        non_empty.push("desired_git_hash");
    }
    if !desired_tag.trim().is_empty() {
        non_empty.push("desired_tag");
    }
    if !desired_version.trim().is_empty() {
        non_empty.push("desired_version");
    }

    if non_empty.len() > 1 {
        return Err(SpecConflictError::MultipleNonEmpty { fields: non_empty.join(", ") });
    }

    if !desired_git_hash.trim().is_empty() {
        Ok(VersionSpec::GitHash(desired_git_hash.trim().to_string()))
    } else if !desired_tag.trim().is_empty() {
        // Map "tag" to the conductor's "GitTag" variant — they mean the same thing.
        Ok(VersionSpec::GitTag(desired_tag.trim().to_string()))
    } else if !desired_version.trim().is_empty() {
        Ok(VersionSpec::Version(desired_version.trim().to_string()))
    } else {
        Ok(VersionSpec::Latest)
    }
}

/// Check whether a registry entry's resolved fields satisfy a [`VersionSpec`].
///
/// Returns `true` if the entry already matches the spec (meaning the tool
/// deployment can be skipped — no re-resolve needed).
///
/// For [`VersionSpec::Latest`], always returns `false` — the caller must
/// re-resolve to determine if a newer version exists.
///
/// # Exact string match
///
/// All comparisons are exact string match. Fields are compared as-is after
/// trimming whitespace.
pub fn spec_matches_entry(
    spec: &VersionSpec,
    resolved_tag: &str,
    resolved_version: &str,
    resolved_git_hash: &str,
) -> bool {
    match spec {
        VersionSpec::GitHash(hash) => resolved_git_hash.trim() == hash.trim(),
        VersionSpec::GitTag(tag) => resolved_tag.trim() == tag.trim(),
        VersionSpec::Version(ver) => resolved_version.trim() == ver.trim(),
        VersionSpec::Latest => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_single_git_hash() {
        let spec = resolve_desired_spec("abc123", "", "").unwrap();
        assert_eq!(spec, VersionSpec::GitHash("abc123".into()));
    }

    #[test]
    fn resolve_single_tag() {
        let spec = resolve_desired_spec("", "v1.2.3", "").unwrap();
        assert_eq!(spec, VersionSpec::GitTag("v1.2.3".into()));
    }

    #[test]
    fn resolve_single_version() {
        let spec = resolve_desired_spec("", "", "2025.01.15").unwrap();
        assert_eq!(spec, VersionSpec::Version("2025.01.15".into()));
    }

    #[test]
    fn resolve_all_empty_is_latest() {
        let spec = resolve_desired_spec("", "", "").unwrap();
        assert_eq!(spec, VersionSpec::Latest);
    }

    #[test]
    fn resolve_conflict_errors() {
        let err = resolve_desired_spec("abc", "v1", "").unwrap_err();
        assert!(err.to_string().contains("desired_git_hash"));
        assert!(err.to_string().contains("desired_tag"));
    }

    #[test]
    fn resolve_triple_conflict_errors() {
        let err = resolve_desired_spec("abc", "v1", "1.0").unwrap_err();
        assert!(err.to_string().contains("cannot specify"));
    }

    #[test]
    fn spec_matches_git_hash() {
        let spec = VersionSpec::GitHash("abc123".into());
        assert!(spec_matches_entry(&spec, "", "", "abc123"));
        assert!(!spec_matches_entry(&spec, "", "", "def456"));
    }

    #[test]
    fn spec_matches_tag() {
        let spec = VersionSpec::GitTag("v1.2.3".into());
        assert!(spec_matches_entry(&spec, "v1.2.3", "", ""));
        assert!(!spec_matches_entry(&spec, "v2.0.0", "", ""));
    }

    #[test]
    fn spec_matches_version() {
        let spec = VersionSpec::Version("2025.01.15".into());
        assert!(spec_matches_entry(&spec, "", "2025.01.15", ""));
        assert!(!spec_matches_entry(&spec, "", "2024.12.01", ""));
    }

    #[test]
    fn spec_matches_latest_never() {
        let spec = VersionSpec::Latest;
        assert!(!spec_matches_entry(&spec, "anything", "anything", "anything"));
    }

    #[test]
    fn spec_matches_exact_string_no_semver() {
        // "v1.2.3" does NOT match "1.2.3" — exact string comparison.
        let spec = VersionSpec::GitTag("v1.2.3".into());
        assert!(!spec_matches_entry(&spec, "1.2.3", "", ""));
    }

    #[test]
    fn spec_matches_trims_whitespace() {
        let spec = VersionSpec::GitTag("v1.2.3".into());
        assert!(spec_matches_entry(&spec, "  v1.2.3  ", "", ""));
    }
}
