//! Hermetic client for the vendored Picard 3.0.4 parity driver.
//!
//! The driver is a Python script under `scripts/picard-parity/`. This helper
//! runs it once, parses its canonical JSON, and hands typed values to Rust
//! tests. It never reaches the network: only the driver's explicit `--refresh`
//! mode can do that, and this helper never passes it.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Deserializer, de};

/// The exact diagnostic a host without Python must see.
///
/// This is a hard failure, not a skip: a gate nobody ran is not a gate.
pub const MISSING_PYTHON_MESSAGE: &str = "python3 is required for the Picard parity gate";

/// One canonical tag entry: the key followed by all of its values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PicardTag {
    /// FFmetadata-style tag key.
    pub key: String,
    /// Values in the order Picard emits them.
    pub values: Vec<String>,
}

impl<'de> Deserialize<'de> for PicardTag {
    /// Reads the driver's `[key, value, ...]` array form.
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let parts = Vec::<String>::deserialize(deserializer)?;
        let (key, values) = parts
            .split_first()
            .ok_or_else(|| de::Error::custom("a Picard tag entry needs at least a key"))?;
        Ok(Self { key: key.clone(), values: values.to_vec() })
    }
}

/// One entity's `inc` token set, restricted to the shared contract vocabulary.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct PicardInc {
    /// Entity the request targets (`recording` or `release`).
    pub entity: String,
    /// Sorted `inc` tokens Picard's vendored load path produced.
    pub values: Vec<String>,
}

/// One selected cover-art image descriptor.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct PicardCoverArt {
    /// Chosen thumbnail or original URL.
    pub url: String,
    /// Primary normalized image kind.
    pub maintype: String,
    /// All normalized image kinds, in Picard's order.
    pub types: Vec<String>,
    /// Normalized image comment.
    pub comment: String,
}

/// The whole canonical JSON contract emitted by the driver.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct PicardContract {
    /// Touched tag keys and values.
    pub tags: Vec<PicardTag>,
    /// Recording and release `inc` sets.
    pub inc: Vec<PicardInc>,
    /// Every approved, type-matched cover-art image in priority order.
    pub cover_art: Vec<PicardCoverArt>,
    /// The subset Picard would embed when only one front image is allowed.
    #[serde(default)]
    pub embedded: Vec<PicardCoverArt>,
}

/// Locates the workspace root from this crate's compile-time manifest path.
fn workspace_root() -> Result<PathBuf> {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let crate_dir = manifest.parent().context("test-support manifest has no parent directory")?;
    crate_dir.parent().map(Path::to_path_buf).context("test-support crate has no workspace root")
}

/// Interpreter names to try, in host-specific preference order.
fn python_candidates() -> &'static [&'static str] {
    if cfg!(windows) { &["python", "python3"] } else { &["python3", "python"] }
}

/// Spawns the driver with the first interpreter that can be found.
///
/// A missing candidate falls through to the next; a driver that starts and
/// fails is never retried. If no candidate exists, the exact missing-Python
/// message is returned.
fn run_driver_with(root: &Path, driver: &Path, candidates: &[&str]) -> Result<Output> {
    for python in candidates {
        match Command::new(python).arg(driver).current_dir(root).output() {
            Ok(output) => return Ok(output),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("running {python} {}", driver.display()));
            }
        }
    }
    bail!("{MISSING_PYTHON_MESSAGE}")
}

/// Runs the offline driver and parses its canonical JSON output.
///
/// # Errors
///
/// Returns [`MISSING_PYTHON_MESSAGE`] when no interpreter can be spawned, and
/// an error carrying the driver's stdout and stderr for every other failure.
/// A driver that writes non-canonical JSON fails rather than being skipped.
pub fn run_picard_parity_gate() -> Result<PicardContract> {
    let root = workspace_root()?;
    let driver = root.join("scripts/picard-parity/driver.py");
    let output = run_driver_with(&root, &driver, python_candidates())?;
    if !output.status.success() {
        bail!(
            "Picard parity driver failed: {}\nstdout: {}\nstderr: {}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let stdout =
        String::from_utf8(output.stdout).context("Picard parity driver output is not UTF-8")?;
    let text = stdout.trim_end();
    let value: serde_json::Value = serde_json::from_str(text)
        .with_context(|| format!("parsing Picard parity driver JSON: {stdout}"))?;
    let canonical = serde_json::to_string(&value).context("canonicalizing Picard contract")?;
    if canonical != text {
        bail!("Picard parity driver did not emit canonical JSON");
    }
    serde_json::from_value(value).context("decoding the Picard contract")
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{MISSING_PYTHON_MESSAGE, run_driver_with};

    /// A missing interpreter must fail the helper with the required message;
    /// this injects a command name that cannot resolve on any host.
    #[test]
    fn an_unresolvable_interpreter_returns_the_missing_python_message() {
        let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
        let root = manifest.ancestors().nth(2).expect("test-support manifest has a workspace root");
        let driver = root.join("scripts/picard-parity/driver.py");
        let error =
            run_driver_with(root, &driver, &["mediapm-picard-no-such-python-interpreter-7f3a"])
                .expect_err("an absent interpreter must fail");
        assert!(error.to_string().contains(MISSING_PYTHON_MESSAGE));
    }
}
