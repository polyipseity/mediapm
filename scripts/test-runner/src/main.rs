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

// `dispatch` is still a set of `todo!()` arms, so nothing calls these two
// modules yet and rustc reports every item in them as dead. Each expectation
// is removed by the task that makes its last item live: Task 5 for both.
// `cargo` is gated by `Metadata::packages` and the `Package` struct, which
// are feature-matrix inputs; Task 2 reads `metadata()` and `status_code()`
// but leaves those two unread, so removing the expectation there would be a
// hard build error under `warnings = "deny"`.
#[expect(
    dead_code,
    reason = "the feature-matrix and test subcommands are still todo!() arms, so nothing calls the typed metadata view yet; gated by Metadata::packages and Package, which Task 5 consumes, so Task 5 removes this"
)]
mod cargo;
#[expect(
    dead_code,
    reason = "package_flags and MatrixArgs::wants_lock are consumed by subcommands that are still todo!() arms; Task 5 removes this"
)]
mod cli;

use std::process::ExitCode;

use clap::Parser;

use crate::cli::{Cli, Command};

fn main() -> ExitCode {
    let cli = Cli::parse();
    match dispatch(cli.command) {
        Ok(code) => ExitCode::from(u8::try_from(code).unwrap_or(1)),
        Err(err) => {
            eprintln!("error: {err:#}");
            ExitCode::FAILURE
        }
    }
}

/// Routes a parsed subcommand to its implementation.
///
/// Takes `command` by value because every arm binds and moves its own
/// payload. While the arms are still `todo!()` no move is visible to
/// clippy, so the by-value signature is expected here and the expectation
/// lapses once Task 2 replaces the first arm.
#[expect(
    clippy::needless_pass_by_value,
    reason = "each dispatch arm moves its own payload out of the command; the todo!() placeholder arms show clippy no move yet"
)]
fn dispatch(command: Command) -> anyhow::Result<i32> {
    match command {
        Command::Test(_) => todo!("Task 2"),
        Command::Doc(_) => todo!("Task 4"),
        Command::FeatureMatrix(_) => todo!("Task 5"),
    }
}
