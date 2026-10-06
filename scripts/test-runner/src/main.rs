//! Central runner for this workspace's tests.
//!
//! One binary owns every step the repository used to spell out four times:
//! the nextest run, the doctest run, the rustdoc link gate, the
//! temp-directory janitor gate, the unprefixed-tempdir invariant gate and
//! the per-feature build matrix. `.cargo/config.toml`, `prek.toml` and
//! `.github/workflows/ci.yml` call this binary instead of repeating the
//! steps.
//!
//! The runner never resolves paths through git. It anchors on
//! `CARGO_MANIFEST_DIR` and asks cargo where the workspace root is, so it
//! works in a checkout without a `.git` directory.

mod cargo;
mod cli;
mod doc;
mod gates;
mod matrix;
mod nextest;
mod test;

use std::process::ExitCode;

use clap::Parser;

use crate::cli::{Cli, Command};

fn main() -> ExitCode {
    let cli = Cli::parse();
    match dispatch(cli.command) {
        Ok(code) => process_exit_code(code),
        Err(err) => {
            eprintln!("error: {err:#}");
            ExitCode::FAILURE
        }
    }
}

/// The status the runner exits with when a subcommand reported a value
/// outside the range a process exit status can carry.
///
/// `EX_SOFTWARE` from `sysexits.h`: the subcommand reported something the
/// runner could not represent, which is a defect in the runner rather than
/// a verdict on the suite, so it must not borrow a status the suite itself
/// uses. It is clear of the statuses this workspace already assigns: `0`
/// clean, `1` the `missing-cli` and generic-failure paths, `3` warning and
/// `4` error from `mediapm sync`, and `2`, which POSIX reserves for shell
/// misuse.
const UNREPRESENTABLE_EXIT_CODE: u8 = 70;

/// Narrows a subcommand's reported code to what a process can exit with.
///
/// Returns `None` for anything a POSIX exit status cannot carry, so the
/// caller reports the value instead of silently substituting another one.
fn narrow_exit_code(code: i32) -> Option<u8> {
    u8::try_from(code).ok()
}

/// Turns a subcommand's reported code into this process's exit status.
///
/// A POSIX exit status is a single unsigned byte, so a code outside
/// `0..=255` cannot be forwarded to a caller reading `$?`. Such a value is
/// named on stderr and replaced with [`UNREPRESENTABLE_EXIT_CODE`]:
/// collapsing it to `1` instead would report a run that failed for one
/// reason as if it failed for another, which is the kind of masked value
/// this repository's typing conventions reject.
fn process_exit_code(code: i32) -> ExitCode {
    if let Some(narrowed) = narrow_exit_code(code) {
        return ExitCode::from(narrowed);
    }
    eprintln!(
        "error: a subcommand reported exit code {code}, which is outside the range 0..=255 a process can exit with; \
         reporting {UNREPRESENTABLE_EXIT_CODE} instead"
    );
    ExitCode::from(UNREPRESENTABLE_EXIT_CODE)
}

/// Routes a parsed subcommand to its implementation.
///
fn dispatch(command: Command) -> anyhow::Result<i32> {
    match command {
        Command::Test(selection) => test::run(&selection),
        Command::Doc(selection) => doc::run(&selection),
        Command::FeatureMatrix(args) => matrix::run(&args),
    }
}

#[cfg(test)]
mod tests {
    use super::{UNREPRESENTABLE_EXIT_CODE, narrow_exit_code};

    /// Guards the narrowing seam the runner's own exit status depends on:
    /// every status a subcommand reports in practice must pass through
    /// unchanged, because a status silently rewritten here is a verdict the
    /// suite never produced.
    #[test]
    fn exit_codes_a_process_can_carry_are_forwarded() {
        assert_eq!(narrow_exit_code(0), Some(0));
        assert_eq!(narrow_exit_code(1), Some(1));
        assert_eq!(narrow_exit_code(255), Some(255));
    }

    /// A negative status and one past the byte boundary both have to be
    /// refused rather than truncated, since truncating either reports a
    /// different run than the one that happened.
    #[test]
    fn exit_codes_a_process_cannot_carry_are_refused() {
        assert_eq!(narrow_exit_code(256), None);
        assert_eq!(narrow_exit_code(-1), None);
        assert_eq!(narrow_exit_code(i32::MAX), None);
        assert_eq!(narrow_exit_code(i32::MIN), None);
    }

    /// Pins the replacement status. `EX_SOFTWARE` is distinct from every
    /// status the suite itself reports, so a caller can tell "the runner
    /// could not represent this" from "the suite failed".
    #[test]
    fn unrepresentable_exit_code_is_ex_software() {
        assert_eq!(UNREPRESENTABLE_EXIT_CODE, 70);
    }
}
