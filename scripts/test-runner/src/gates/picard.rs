//! Vendored Picard parity gate.
//!
//! `test-runner test` runs this after the suite. The driver verifies its own
//! vendored sources, but the runner repeats that check in Rust so a changed
//! vendored byte cannot be waved through by a locally edited driver. The gate
//! then checks that stdout is one canonical JSON object with the contract's
//! four keys.

use std::path::Path;
use std::process::{Command, Output};

use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};

/// Diagnostic required when the host has no Python interpreter.
const MISSING_PYTHON: &str = "python3 is required for the Picard parity gate";

/// Collapses CRLF and lone CR to LF before hashing.
///
/// Provenance hashes describe committed LF bytes; a checkout with host line
/// endings must verify against the same hashes.
fn normalized_line_endings(bytes: &[u8]) -> Vec<u8> {
    let mut normalized = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'\r' {
            normalized.push(b'\n');
            if bytes.get(index + 1) == Some(&b'\n') {
                index += 1;
            }
        } else {
            normalized.push(bytes[index]);
        }
        index += 1;
    }
    normalized
}

/// Interpreter names to try, in host-specific preference order.
fn python_candidates() -> &'static [&'static str] {
    if cfg!(windows) { &["python", "python3"] } else { &["python3", "python"] }
}

/// Reads the vendored provenance table.
fn provenance(root: &Path) -> Result<Vec<(String, String)>> {
    let path = root.join("scripts/picard-parity/vendored/provenance.json");
    let text =
        std::fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
    let value: serde_json::Value =
        serde_json::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
    let files = value
        .get("files")
        .and_then(serde_json::Value::as_array)
        .context("provenance has no files array")?;
    files
        .iter()
        .map(|entry| {
            let rel = entry
                .get("path")
                .and_then(serde_json::Value::as_str)
                .context("provenance entry has no path")?;
            let sha = entry
                .get("sha256")
                .and_then(serde_json::Value::as_str)
                .context("provenance entry has no sha256")?;
            Ok((rel.to_owned(), sha.to_owned()))
        })
        .collect()
}

/// Recomputes every vendored sha256 before the driver is allowed to run.
fn verify_vendored_hashes(root: &Path) -> Result<()> {
    for (rel, expected) in provenance(root)? {
        let path = root.join("scripts/picard-parity/vendored").join(&rel);
        let bytes = std::fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
        let actual = format!("{:x}", Sha256::digest(normalized_line_endings(&bytes)));
        if actual != expected {
            bail!(
                "vendored Picard source hash mismatch for {rel}: expected {expected}, got {actual}"
            );
        }
    }
    Ok(())
}

/// Checks the driver's stdout shape and canonical spelling.
fn parse_canonical_contract(stdout: &str) -> Result<serde_json::Value> {
    let text = stdout.trim_end();
    let value: serde_json::Value =
        serde_json::from_str(text).context("parsing the Picard parity contract")?;
    let canonical = serde_json::to_string(&value).context("canonicalizing the Picard contract")?;
    if canonical != text {
        bail!("Picard parity driver did not emit canonical JSON");
    }
    for key in ["tags", "inc", "cover_art", "embedded"] {
        if !value.get(key).is_some_and(serde_json::Value::is_array) {
            bail!("Picard parity contract needs an array named {key}");
        }
    }
    Ok(value)
}

/// Spawns the driver with the first interpreter that can be found.
///
/// A candidate that is absent falls through to the next; a spawned driver
/// that fails is never retried with another interpreter. When no candidate
/// exists the exact missing-Python diagnostic is returned.
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
    bail!("{MISSING_PYTHON}")
}

/// Runs the offline driver and checks its verdict.
///
/// # Errors
///
/// Returns [`MISSING_PYTHON`] when no interpreter can be spawned, the
/// driver's output when it fails, and a contract error when its JSON is not
/// canonical or lacks a required array. No failure is skipped.
pub fn enforce(root: &Path) -> Result<()> {
    verify_vendored_hashes(root)?;
    let driver = root.join("scripts/picard-parity/driver.py");
    let output = run_driver_with(root, &driver, python_candidates())?;
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
    parse_canonical_contract(&stdout)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use sha2::{Digest, Sha256};

    use super::{
        MISSING_PYTHON, enforce, normalized_line_endings, parse_canonical_contract, run_driver_with,
    };

    /// A missing interpreter has to produce the required diagnostic, not a
    /// skipped gate. The injected name cannot resolve on any host.
    #[test]
    fn an_unresolvable_interpreter_returns_the_missing_python_diagnostic() {
        let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
        let root = manifest.ancestors().nth(2).expect("test-runner manifest has a workspace root");
        let driver = root.join("scripts/picard-parity/driver.py");
        let error =
            run_driver_with(root, &driver, &["mediapm-picard-no-such-python-interpreter-7f3a"])
                .expect_err("an absent interpreter must fail");
        assert_eq!(error.to_string(), MISSING_PYTHON);
    }

    /// Provenance hashes are LF bytes, so CRLF and CR copies must hash the
    /// same as LF before any mismatch is reported.
    #[test]
    fn vendored_hashes_ignore_host_line_endings() {
        let digest = |bytes: &[u8]| format!("{:x}", Sha256::digest(normalized_line_endings(bytes)));
        let lf = b"first\nsecond\n";
        assert_eq!(digest(lf), digest(b"first\r\nsecond\r\n"));
        assert_eq!(digest(lf), digest(b"first\rsecond\r"));
    }

    /// Runs the real gate against the committed tree so a broken driver,
    /// vendored hash, or contract shape fails a test-runner test run too.
    #[test]
    fn enforced_gate_accepts_the_committed_vendored_sources() {
        let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
        let root = manifest.ancestors().nth(2).expect("test-runner manifest has a workspace root");
        enforce(root).expect("the committed parity gate must pass");
    }

    /// A reordered or padded contract must be rejected, because the gate's
    /// output is the byte-level artifact a failing CI run reports.
    #[test]
    fn contract_requires_canonical_json_and_all_arrays() {
        let good = r#"{"cover_art":[],"embedded":[],"inc":[],"tags":[]}"#;
        assert!(parse_canonical_contract(good).is_ok());
        assert!(parse_canonical_contract(&format!("{good}\n")).is_ok());
        assert!(
            parse_canonical_contract(r#"{"tags": [], "inc": [], "cover_art": [], "embedded": []}"#)
                .is_err()
        );
        assert!(parse_canonical_contract(r#"{"tags":[],"inc":[],"embedded":[]}"#).is_err());
        assert!(parse_canonical_contract(r#"{"tags":[],"inc":[],"cover_art":[]}"#).is_err());
    }
}
