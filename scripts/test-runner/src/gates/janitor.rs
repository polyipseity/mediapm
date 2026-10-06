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
///
/// The cost is an echo rather than a wrong verdict: on Windows a `%TEMP%`
/// containing a space is the common case rather than an edge, so there the
/// per-directory lines routinely take this branch and are re-printed on
/// stderr. That is noise on a clean pass and never changes the verdict,
/// which is decided by the separate `would remove` check.
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
///
/// On POSIX the script is spawned **by path, with no interpreter named**,
/// and that is load-bearing rather than incidental. `clean-mediapm-temp.sh`
/// is a bash script (`#!/usr/bin/env bash`, `set -euo pipefail`, `[[ ]]`,
/// `read -r -d ''`), while the POSIX `sh` is dash on Debian and ubuntu,
/// which rejects it on the `pipefail` line:
///
/// ```text
/// $ dash scripts/clean-mediapm-temp.sh --dry-run
/// scripts/clean-mediapm-temp.sh: 3: set: Illegal option -o pipefail
/// $ /bin/sh scripts/clean-mediapm-temp.sh --dry-run   # macOS: bash in POSIX mode
/// ```
///
/// Naming `sh` here therefore fails the gate on every `ubuntu-latest` run
/// and passes on macOS, which is the worst shape a portability defect can
/// take. Executing the path lets the kernel read the shebang and pick the
/// interpreter, which is what `run-all-tests.sh` did before this gate
/// existed. Do not "tidy" the bare path back into an interpreter name.
///
/// The pwsh branch names its interpreter because `-File` requires it; that
/// is the platform's own script-invocation form.
fn janitor_command(root: &Path) -> (std::ffi::OsString, Vec<std::ffi::OsString>) {
    let script = root.join("scripts").join("clean-mediapm-temp.sh");
    if cfg!(windows) {
        (
            std::ffi::OsString::from("pwsh"),
            vec![
                std::ffi::OsString::from("-NoProfile"),
                std::ffi::OsString::from("-File"),
                script.with_file_name("clean-mediapm-temp.ps1").into_os_string(),
                std::ffi::OsString::from("--dry-run"),
            ],
        )
    } else {
        (script.into_os_string(), vec![std::ffi::OsString::from("--dry-run")])
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
///
/// The status is read first on purpose: a temp root that cannot be
/// scanned prints nothing that reads as a leftover, so a text-only gate
/// would read that failure as a clean sweep.
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

    // The trigger stays the broad `would remove` form because the janitor's
    // own summary line (`would remove 3 mediapm temp director(ies)`) also
    // carries it, and a sweep that only reported a count is still a
    // leftover. The evidence collected for the verdict is narrower: only
    // the per-directory lines, so the verdict names paths rather than
    // repeating the count the gate already implies.
    let mut leftover = false;
    let mut leftovers: Vec<&str> = Vec::new();
    for line in combined.lines() {
        if line.starts_with("would remove") {
            leftover = true;
            if line.starts_with("would remove:") {
                leftovers.push(line);
            }
        }
        if !is_contract_line(line) {
            eprintln!("{line}");
        }
    }
    if !leftover {
        return Ok(());
    }
    let detail =
        if leftovers.is_empty() { String::new() } else { format!(": {}", leftovers.join("; ")) };
    bail!("test suite left mediapm temp dirs behind{detail}");
}

#[cfg(test)]
mod program_tests {
    use super::janitor_command;
    use std::path::Path;

    /// The unix branch must spawn the janitor by path and pass `--dry-run`.
    ///
    /// `clean-mediapm-temp.sh` is bash: it uses `set -euo pipefail`, `[[ ]]`
    /// and `read -r -d ''`, none of which dash parses. On `ubuntu-latest` the
    /// resolved `sh` is dash, so naming an interpreter fails the gate on every
    /// run while passing on macOS, where `sh` is bash 3.2. The stub-based
    /// tests cannot catch that on a bash-as-sh host, because the stub's own
    /// `set -euo pipefail` is legal there. This assertion is what closes it
    /// everywhere, with no spawn and no interpreter dependency.
    ///
    /// The args are the other half of that contract, for the same style of
    /// reason: `--dry-run` is what makes the janitor report rather than
    /// delete. Drop it and the gate removes exactly the directories it exists
    /// to report, destroying the evidence of the failure it catches, while
    /// every other assertion here keeps passing. The flag is checked against
    /// the whole arg list rather than a fixed position, so reordering the
    /// command cannot lose it silently.
    #[test]
    #[cfg(unix)]
    fn the_unix_branch_spawns_the_janitor_by_path() {
        let (program, args) = janitor_command(Path::new("/repo"));
        assert!(
            program.to_string_lossy().ends_with("clean-mediapm-temp.sh"),
            "the janitor must be spawned by path, not through an interpreter; got {program:?}"
        );
        assert!(
            args.iter().any(|arg| arg == "--dry-run"),
            "the gate must pass --dry-run to the janitor; without it the janitor deletes \
             the temp directories this gate exists to report instead of reporting them; \
             got args {args:?}"
        );
    }

    /// The Windows branch must name `pwsh` and pass `--dry-run`.
    ///
    /// The flag is why this test exists, and nothing else can observe it on
    /// Windows: the stub-based tests spawn the command built here through
    /// `write_stub`'s Windows twin, whose script has no `param()` block, so
    /// `pwsh -File` hands `--dry-run` through `$args` where it lands
    /// unexamined — `write_stub`'s own doc comment records that. The stub
    /// tests therefore see nothing of the flag on either platform, and the
    /// unix twin above observes it only for the unix branch, so this is the
    /// only assertion anywhere that reads the Windows arg list.
    ///
    /// Without `--dry-run`, `clean-mediapm-temp.ps1` deletes exactly the
    /// temp directories the gate exists to report, destroying the evidence
    /// of the failure the gate catches — and every other test in this file
    /// keeps passing, because nothing else looks at the args. The remaining
    /// assertions pin the invocation form `janitor_command` documents:
    /// `pwsh` with `-File` is how the platform runs a script, and
    /// `-NoProfile` keeps user profile scripts from perturbing the sweep.
    ///
    /// Every arg is checked against the whole list rather than a fixed
    /// position, so reordering the command cannot silently drop the flag,
    /// matching the unix twin.
    #[test]
    #[cfg(windows)]
    fn the_windows_branch_passes_dry_run() {
        let (program, args) = janitor_command(Path::new("/repo"));
        assert_eq!(
            program.to_string_lossy(),
            "pwsh",
            "the Windows branch must name the pwsh interpreter; got {program:?}"
        );
        assert!(
            args.iter().any(|arg| arg == "-NoProfile"),
            "the Windows branch must pass -NoProfile; got args {args:?}"
        );
        assert!(
            args.iter().any(|arg| arg == "-File"),
            "the Windows branch must pass -File; got args {args:?}"
        );
        assert!(
            args.iter().any(|arg| arg.to_string_lossy().ends_with("clean-mediapm-temp.ps1")),
            "the Windows branch must pass the ps1 janitor script; got args {args:?}"
        );
        assert!(
            args.iter().any(|arg| arg == "--dry-run"),
            "the gate must pass --dry-run to the janitor; without it the janitor deletes \
             the temp directories this gate exists to report instead of reporting them; \
             got args {args:?}"
        );
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;

    use super::{enforce, is_contract_line};
    use crate::gates::test_scratch::scratch;

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

    /// A janitor that exits non-zero reports a sweep it did not perform.
    /// The status is read before the output, so the gate has to name that
    /// diagnostic in its own error: this is the case a broken spawn shares
    /// with a genuinely unscannable temp root, and reporting it as a clean
    /// sweep is what the status-first read exists to prevent.
    #[test]
    fn a_failed_sweep_names_its_own_diagnostic() {
        let root = scratch("failed-sweep");
        write_stub(&root, &["no such directory: /nonexistent-root"], 1);
        let err = enforce(&root).expect_err("a failed sweep must fail the gate");
        assert_eq!(
            err.to_string(),
            "mediapm temp-dir sweep failed: no such directory: /nonexistent-root"
        );
    }

    /// A successful sweep that lists a leftover fails, and the verdict
    /// names the directory. A bare verdict told the developer that a
    /// directory survived without saying which one, so the first thing a
    /// pre-push author sees is a path they have to reconstruct from
    /// `$TMPDIR` by hand.
    ///
    /// The stub emits the janitor's summary line alongside the path, and
    /// the verdict must carry only the path: the evidence collected is the
    /// per-directory lines, not everything the broad trigger matched.
    #[test]
    fn a_leftover_fails_the_gate() {
        let root = scratch("leftover");
        write_stub(
            &root,
            &[
                "would remove: /tmp/mediapm-artifact-abc",
                "would remove 1 mediapm temp director(ies)",
            ],
            0,
        );
        let err = enforce(&root).expect_err("a leftover must fail the gate");
        let text = err.to_string();
        assert!(
            text.contains("test suite left mediapm temp dirs behind"),
            "unexpected message: {text}"
        );
        assert!(
            text.contains("/tmp/mediapm-artifact-abc"),
            "the verdict must name the leftover directory; got: {text}"
        );
        assert!(
            !text.contains("director(ies)"),
            "the verdict carries the per-directory paths, not the janitor's count; got: {text}"
        );
    }

    /// A sweep that reports only a count is still a leftover.
    ///
    /// The trigger is the broad `would remove` rather than the colon form
    /// for exactly this case: the janitor's own summary line
    /// `would remove 1 mediapm temp director(ies)` carries the prefix but
    /// names no directory, and a colon-only trigger would read a sweep
    /// that found leftovers as a clean run. The old pwsh runner had that
    /// colon-only trigger, so this is the defect it had.
    ///
    /// Nothing collects a path here, so the verdict carries no evidence
    /// clause at all — asserted exactly, because the absence of a trailing
    /// `:` and an empty detail is the whole shape of this branch.
    #[test]
    fn a_count_only_sweep_fails_with_the_bare_verdict() {
        let root = scratch("count-only-leftover");
        write_stub(&root, &["would remove 1 mediapm temp director(ies)"], 0);
        let err = enforce(&root).expect_err("a count-only leftover must fail the gate");
        assert_eq!(err.to_string(), "test suite left mediapm temp dirs behind");
    }

    /// The empty-root case, which nothing else covers: a sweep with
    /// nothing to remove passes. A gate that failed here would report a
    /// broken janitor as a dirty repository on every clean run.
    #[test]
    fn a_clean_sweep_passes() {
        let root = scratch("clean-sweep");
        write_stub(&root, &["no mediapm temp directories found"], 0);
        enforce(&root).expect("a clean sweep must pass the gate");
    }

    /// Writes an executable stub janitor into `root`'s `scripts/`.
    ///
    /// The stub is a bash script because the gate now executes the script
    /// by path: the kernel reads the shebang, so a stub without one, or
    /// without the executable bit, would fail to spawn rather than fail to
    /// assert. Its own `set -euo pipefail` is what makes the by-path form
    /// observable — the stub is rejected outright by any POSIX `sh`.
    #[cfg(unix)]
    fn write_stub(root: &Path, lines: &[&str], exit_code: i32) {
        use std::fmt::Write as _;
        use std::os::unix::fs::PermissionsExt as _;

        let dir = root.join("scripts");
        fs::create_dir_all(&dir).expect("create the stub's scripts dir");
        let mut body = String::from("#!/usr/bin/env bash\nset -euo pipefail\n");
        for line in lines {
            writeln!(body, "echo \"{line}\"").expect("render the stub body");
        }
        writeln!(body, "exit {exit_code}").expect("render the stub exit");
        let path = dir.join("clean-mediapm-temp.sh");
        fs::write(&path, body).expect("write the stub janitor");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755))
            .expect("make the stub executable");
    }

    /// The Windows twin of [`write_stub`]. `pwsh -File <script>` hands a
    /// script with no `param()` block any trailing token through `$args`,
    /// so the `--dry-run` the gate passes lands there and is ignored.
    #[cfg(windows)]
    fn write_stub(root: &Path, lines: &[&str], exit_code: i32) {
        use std::fmt::Write as _;

        let dir = root.join("scripts");
        fs::create_dir_all(&dir).expect("create the stub's scripts dir");
        let mut body = String::new();
        for line in lines {
            writeln!(body, "Write-Output \"{line}\"").expect("render the stub body");
        }
        writeln!(body, "exit {exit_code}").expect("render the stub exit");
        fs::write(dir.join("clean-mediapm-temp.ps1"), body).expect("write the stub janitor");
    }
}
