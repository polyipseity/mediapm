//! Temp-directory janitor gate.
//!
//! The janitor itself stays a shell and pwsh script pair under
//! `scripts/clean-mediapm-temp.{sh,ps1}`: it is a user-facing command with
//! its own output contract and its own self-tests. This module only runs
//! it in dry-run mode and reads the answer.
//!
//! The exit status is read before the output, and for a reason the old
//! shell runner documented: a temp root that cannot be scanned prints
//! nothing that reads as a leftover, so a text-only gate would read the
//! failure as a clean sweep.

use std::path::Path;
use std::process::Command;

use anyhow::{Context, Result, bail};

/// The janitor's own output contract.
///
/// These are the only lines a dry run is allowed to print. Anything else
/// is a diagnostic, and it is forwarded to stderr rather than swallowed,
/// because a warning the janitor grows later must not vanish into a
/// captured variable.
fn is_contract_line(line: &str) -> bool {
    if line == "no mediapm temp directories found" {
        return true;
    }
    if is_path_line("would remove: ", line) || is_path_line("removed: ", line) {
        return true;
    }
    is_count_line("would remove", line) || is_count_line("removed", line)
}

/// Matches a per-directory line, `<prefix><path>`.
///
/// The path has to be there and has to be one token. A line whose tail
/// carries whitespace is prose appended to a path, not a path, so it is a
/// diagnostic: `is_contract_line` swallows whatever it accepts, and the
/// gate's leftover verdict does not come from these lines, so the safe
/// direction is to forward a line the janitor may not have written rather
/// than swallow one it may have.
fn is_path_line(prefix: &str, line: &str) -> bool {
    let Some(rest) = line.strip_prefix(prefix) else {
        return false;
    };
    !rest.is_empty() && !rest.chars().any(char::is_whitespace)
}

/// Matches the janitor's summary line, `<word> <n> mediapm temp director(ies)`.
///
/// The count must be all ASCII digits and non-empty, so a truncated line
/// does not pass as a contract line.
fn is_count_line(word: &str, line: &str) -> bool {
    let Some(rest) = line.strip_prefix(word) else {
        return false;
    };
    let Some(rest) = rest.strip_prefix(' ') else {
        return false;
    };
    let Some(digits) = rest.strip_suffix(" mediapm temp director(ies)") else {
        return false;
    };
    !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit())
}

/// The command that runs the janitor for this platform.
fn janitor_command(root: &Path) -> (std::ffi::OsString, Vec<std::ffi::OsString>) {
    if cfg!(windows) {
        (
            std::ffi::OsString::from("pwsh"),
            vec![
                std::ffi::OsString::from("-NoProfile"),
                std::ffi::OsString::from("-File"),
                root.join("scripts").join("clean-mediapm-temp.ps1").into_os_string(),
                std::ffi::OsString::from("--dry-run"),
            ],
        )
    } else {
        (
            std::ffi::OsString::from("sh"),
            vec![
                root.join("scripts").join("clean-mediapm-temp.sh").into_os_string(),
                std::ffi::OsString::from("--dry-run"),
            ],
        )
    }
}

/// Fails when the test run left a mediapm-owned temp directory behind.
///
/// Runs the janitor in dry-run mode, so it reports and never deletes.
///
/// Caveat inherited from the shell runner this gate replaces: a concurrent
/// local mediapm process can leave a temp directory of its own and trip the
/// gate spuriously.
///
/// # Errors
///
/// Returns an error when the janitor cannot be spawned, when it exits
/// non-zero (a sweep it could not perform is never reported as a clean
/// one), or when its dry-run output lists anything it would remove.
pub fn enforce(root: &Path) -> Result<()> {
    let (program, args) = janitor_command(root);
    let output = Command::new(&program)
        .args(&args)
        .output()
        .with_context(|| format!("run the janitor: {} {args:?}", program.to_string_lossy()))?;

    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let trimmed = combined.trim_end();

    if !output.status.success() {
        bail!("mediapm temp-dir sweep failed: {trimmed}");
    }

    let mut leftover = false;
    for line in combined.lines() {
        if line.starts_with("would remove") {
            leftover = true;
        }
        if !is_contract_line(line) {
            eprintln!("{line}");
        }
    }
    if leftover {
        bail!("test suite left mediapm temp dirs behind");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::is_contract_line;

    /// Pins the janitor's output contract from the side of the gate: every
    /// line a dry run may print must be recognised, or the gate forwards it
    /// to stderr as if it were a diagnostic.
    #[test]
    fn contract_lines_are_recognised() {
        for line in [
            "no mediapm temp directories found",
            "would remove: /tmp/mediapm-artifact-abc",
            "removed: /tmp/mediapm-artifact-abc",
            "would remove 3 mediapm temp director(ies)",
            "removed 3 mediapm temp director(ies)",
            "would remove 0 mediapm temp director(ies)",
        ] {
            assert!(is_contract_line(line), "expected contract line: {line}");
        }
    }

    /// The summary line's count has to be digits. A line that lost its
    /// count to truncation, or that carries a word where the number belongs,
    /// is a malformed line and must not pass as contract.
    #[test]
    fn a_count_line_needs_digits() {
        assert!(!is_contract_line("would remove some mediapm temp director(ies)"));
        assert!(!is_contract_line("removed  mediapm temp director(ies)"));
    }

    /// The contract is a whole-line shape, not a prefix one: a line that
    /// merely starts like a contract line and then says more is a
    /// diagnostic, and the gate forwards it rather than swallowing it.
    #[test]
    fn contract_lines_must_match_wholly() {
        assert!(!is_contract_line("would remove: /tmp/x and then some"));
        assert!(!is_contract_line("no such directory: /nope"));
    }
}
