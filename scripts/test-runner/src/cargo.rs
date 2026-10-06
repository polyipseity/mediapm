//! Typed access to `cargo metadata`.
//!
//! The runner needs exactly two facts from cargo: where the workspace root
//! is, and which features each member declares. Both come from
//! `cargo metadata --no-deps`, parsed into [`Metadata`] and [`Package`] so
//! no later task has to reach into `serde_json::Value`.
//!
//! The module also owns [`status_code`], the one place that turns a child's
//! exit status into a reportable number. Keeping it here means the
//! signal-death case is handled once rather than at each spawn site.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::ExitStatus;

use anyhow::{Context, Result, bail};
use serde::Deserialize;

/// The subset of `cargo metadata --no-deps --format-version 1` the runner
/// reads. Everything else in cargo's document is ignored.
#[derive(Debug, Deserialize)]
pub struct Metadata {
    /// Absolute path of the workspace root cargo resolved.
    pub workspace_root: PathBuf,
    /// Every workspace member.
    pub packages: Vec<Package>,
}

/// One workspace member and the features it declares.
#[derive(Debug, Deserialize)]
pub struct Package {
    /// The crate's package name.
    pub name: String,
    /// Declared features keyed by name. The implicit `default` feature
    /// appears here when the crate declares one, which is why the feature
    /// matrix filters it out before sweeping single features.
    pub features: BTreeMap<String, Vec<String>>,
}

/// Runs `cargo metadata --no-deps --format-version 1` rooted at `dir` and
/// parses the result.
///
/// `dir` should be this crate's manifest directory: cargo walks up to the
/// workspace root from there regardless of the process working directory,
/// so the caller never has to guess where it was invoked from.
pub fn metadata(dir: &Path) -> Result<Metadata> {
    let output = std::process::Command::new(cargo_binary())
        .current_dir(dir)
        .args(["metadata", "--no-deps", "--format-version", "1"])
        .output()
        .with_context(|| format!("run cargo metadata in {}", dir.display()))?;
    if !output.status.success() {
        bail!(
            "cargo metadata failed in {}: {}",
            dir.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    serde_json::from_slice(&output.stdout)
        .with_context(|| format!("parse cargo metadata for {}", dir.display()))
}

/// The cargo executable to spawn.
///
/// Honours `CARGO` so a nested toolchain or a test stub can redirect it,
/// matching what `build-utils` already does.
fn cargo_binary() -> std::ffi::OsString {
    std::env::var_os("CARGO").unwrap_or_else(|| std::ffi::OsString::from("cargo"))
}

/// A child's exit code, or an error when the process died from a signal.
///
/// A signal death has no code, and reading it as `0` would report success
/// for a run that never finished.
pub fn status_code(status: ExitStatus, label: &str) -> Result<i32> {
    match status.code() {
        Some(code) => Ok(code),
        None => Err(anyhow::anyhow!("{label} was terminated by a signal")),
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::Metadata;

    /// Guards the shape [`metadata`] deserializes: without this the only
    /// coverage of the wire format is a live cargo invocation, so a
    /// mismatch in a field name would surface as a runtime parse failure
    /// in whichever subcommand happened to run first.
    #[test]
    fn metadata_parses_the_documented_shape() {
        let doc = br#"{
            "workspace_root": "/repo",
            "packages": [
                {"name": "alpha", "features": {"cli": [], "default": ["cli"]}},
                {"name": "beta", "features": {}}
            ]
        }"#;
        let parsed: Metadata = serde_json::from_slice(doc).expect("parse");
        assert_eq!(parsed.workspace_root, Path::new("/repo"));
        assert_eq!(parsed.packages.len(), 2);
        assert!(parsed.packages[1].features.is_empty());
    }
}
