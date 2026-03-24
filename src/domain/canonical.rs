//! URI and path canonicalization.
//!
//! The central identity rule in mediapm is: **source identity is canonical URI**.
//! This module converts user input (`path`, `file://...`, or remote URL) into a
//! stable representation used throughout planning and storage.
//!
//! Why this matters: users can refer to the same file in many ways
//! (`../x.flac`, absolute path, `file://` URI). Without canonicalization, those
//! forms would appear as different media entries and break deduplication,
//! linking, and sidecar history consistency.

use std::path::{Path, PathBuf};

use anyhow::{Result, anyhow};
use path_clean::PathClean;
use serde::{Deserialize, Serialize};
use url::Url;

/// Canonicalized URI wrapper.
///
/// This is intentionally a transparent newtype so it serializes as a plain
/// string while still allowing type-safe APIs.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CanonicalUri(pub String);

impl CanonicalUri {
    /// Borrow the canonical URI string.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consume this wrapper and return the inner URI string.
    pub fn into_string(self) -> String {
        self.0
    }

    /// Convert a canonical URI into a local filesystem path.
    ///
    /// Returns an error when the URI is not `file://...`.
    pub fn to_file_path(&self) -> Result<PathBuf> {
        let parsed = Url::parse(&self.0)?;
        if parsed.scheme() != "file" {
            return Err(anyhow!("URI is not a file URI: {}", self.0));
        }

        parsed.to_file_path().map_err(|_| anyhow!("failed to convert URI to path: {}", self.0))
    }
}

/// Canonicalize user input into a URI identity.
///
/// Behavior:
/// - If input parses as `file://`, normalize path components and return a
///   canonical file URI.
/// - If input parses as another URL scheme, normalize host casing and strip
///   fragments.
/// - Otherwise, treat input as a local path relative to `cwd`.
///
/// This function is used at the boundary between user-facing configuration and
/// domain identity. Keeping all URI normalization in one place makes identity
/// rules easier to audit and test.
pub fn canonicalize_uri(input: &str, cwd: &Path) -> Result<CanonicalUri> {
    if let Ok(url) = Url::parse(input) {
        if url.scheme() == "file" {
            let as_path = url.to_file_path().map_err(|_| anyhow!("invalid file URI: {input}"))?;
            return canonicalize_path_uri(&as_path, cwd);
        }

        let mut normalized = url;
        normalized.set_fragment(None);

        if let Some(host) = normalized.host_str() {
            let lowered = host.to_ascii_lowercase();
            normalized.set_host(Some(&lowered))?;
        }

        return Ok(CanonicalUri(normalized.to_string()));
    }

    canonicalize_path_uri(Path::new(input), cwd)
}

fn canonicalize_path_uri(path_like: &Path, cwd: &Path) -> Result<CanonicalUri> {
    let path = normalize_path(path_like, cwd)?;
    let uri = Url::from_file_path(&path).map_err(|_| anyhow!("failed to build file URI"))?;
    Ok(CanonicalUri(uri.to_string()))
}

fn normalize_path(path_like: &Path, cwd: &Path) -> Result<PathBuf> {
    let joined =
        if path_like.is_absolute() { path_like.to_path_buf() } else { cwd.join(path_like) }.clean();

    if joined.exists() {
        return Ok(std::fs::canonicalize(joined)?);
    }

    Ok(joined)
}

#[cfg(test)]
mod tests {
    use super::canonicalize_uri;

    #[test]
    fn canonicalizes_relative_path() {
        let cwd = std::env::current_dir().expect("cwd should resolve");
        let uri = canonicalize_uri("src/main.rs", &cwd).expect("path should canonicalize");

        assert!(uri.as_str().starts_with("file://"));
    }
}
