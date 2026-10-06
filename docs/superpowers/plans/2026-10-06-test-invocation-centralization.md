# Test invocation centralization implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `scripts/run-all-tests.{sh,ps1}` with a Rust `test-runner` crate that owns the workspace test pipeline, and point `.cargo/config.toml`, `prek.toml` and `ci.yml` at it.

**Architecture:** One new workspace member `scripts/test-runner` (package `test-runner`) exposing three subcommands: `test` (nextest + janitor gate + tempdir gate), `doc` (doctests + rustdoc link gate), and `feature-matrix` (derives and checks every package/feature combination from `cargo metadata`). Two cargo aliases, `test-all` and `test-doc-all`, forward to it. The janitor stays a shell/pwsh script pair; the runner shells out to it.

**Tech Stack:** Rust 2024, clap 4 (derive), serde + serde_json, anyhow, cargo-nextest via `cargo-bin`.

**Spec:** `docs/superpowers/specs/2026-10-06-test-invocation-centralization-design.md`

## Global Constraints

- Every commit message uses Conventional Commits with a mandatory scope: `type(scope): subject`. Scope is mandatory and must NOT be a bare crate/tool prefix like `mediapm:` or `test-runner:`. Use `type(tests):`, `type(ci):`, `type(ci):`, `type(docs):`, `type(refactor):`.
- This environment exports `RUSTC_WRAPPER=sccache` and sccache fails with `Operation not permitted (os error 1)`. Prefix every cargo invocation with `RUSTC_WRAPPER=""`.
- Workspace lints are `[lints] workspace = true` with `clippy::all = deny`, `clippy::pedantic = deny`, `rust warnings = deny`. Code must be clippy-pedantic clean. Prefer `#[expect(lint, reason = "...")]` with a substantive reason over bare `#[allow(...)]`, and never crate-wide.
- Every crate `Cargo.toml` inherits `version.workspace = true`, `edition.workspace = true`, `rust-version.workspace = true`, `license-file.workspace = true`, `publish.workspace = true`.
- Dependencies use `x = { workspace = true }`.
- Line endings: `.sh` and `.md` use LF, `.ps1` uses CRLF.
- Markdown under `.agents/` uses 3-space bullets, 5-space sub-bullets, one-line paragraphs, sentence-case headings.
- No subagent runs `git commit` or `git push`. The controller commits. Implementers leave changes staged-ready and unstaged in the working tree; the controller commits after review.
- Never run `git reset`, `git commit --amend`, `git revert`, or `git clean`.
- Error messages that the old shell runner emitted are part of the contract. Reproduce them exactly: `error: mediapm temp-dir sweep failed: <output>`, `error: test suite left mediapm temp dirs behind`, `error: unprefixed tempdir/prefix use outside src/mediapm-utils/src/temp.rs`.
- The runner resolves the workspace root from `cargo metadata`'s own `workspace_root` field, not from `git rev-parse`.

## Rulings recorded before execution

- **R1: leftover detection uses `starts_with("would remove")`, not `starts_with("would remove:")`.** The current `run-all-tests.sh` uses `grep -q 'would remove'` and its pwsh twin uses `-like 'would remove:*'`. The twin's colon requirement misses the janitor's own summary line `would remove 3 mediapm temp director(ies)`, so the Windows runner would report a clean sweep while leftovers existed. The shell runner's behaviour is the correct one; the Rust port matches it. Cost if wrong: none, this is strictly the more careful of the two.
- **R2: the janitor's contract-line match is a full match, not a prefix.** `run-all-tests.sh` uses `^would remove:` (prefix only) while the pw1 twin anchors both ends. The Rust port anchors both ends, so a line that looks like contract output but carries extra trailing content is forwarded to stderr instead of swallowed. Cost if wrong: one extra stderr line on a passing run.
- **R3: an unknown argument exits 2, not 1.** The shell runner exited 1; clap exits 2 for a usage error. The spec was updated to match. Cost if wrong: a caller asserting exactly 1.
- **R4: `feature-matrix` derives rows from `cargo metadata` minus the implicit `default` feature.** Verified against the tree before planning: `cargo metadata --no-deps --format-version 1` yields 55 candidate rows, and removing `default` from the 8 packages that declare one yields exactly the 47 rows the current `ci.yml` matrix lists. Cost if wrong: the matrix grows by 8 redundant probes, which is harmless but noisy.
- **R5: the tempdir gate runs before the janitor gate.** The spec's CLI table listed janitor first, copied from the old shell script's order. The Rust `test::run` calls tempdir first because that gate is a pure in-process source scan with no subprocess and no filesystem mutation, so failing fast on it is free; the janitor spawns `sh` or `pwsh`. The spec was updated to match the code. Cost if wrong: when both gates fail, the user sees the two messages in the other order.
- **R12: `--workspace` is emitted only when no package is named, in both `nextest::argv` and `doc::doctest_argv`.** Cargo resolves `--workspace -p <pkg>` as a union in which `--workspace` wins silently and with no warning, so emitting both made `-p` an accepted flag that did nothing. The old `cargo test-pkg` alias carried no `--workspace`, which is what made its `-p` narrow, so the runner was losing a capability the thing it replaces had. `rustdoc_argv` stays workspace-wide unconditionally, because the link gate must not be narrowable by an off-by-default module. Ruled after the Task 4 implementer's F1 report and the Task 4 review both found the composition. The review corrected the implementer's cost estimate: every pinned vector in both already-reviewed files is built from a selection with no `-p`, so the change breaks none of them. Cost if wrong: none for any caller the plan creates, since none passes `-p`; a developer passing `-p` gets the narrow run they asked for rather than a silent whole-workspace one.
- **R11: an exit code a process cannot carry becomes 70 (`EX_SOFTWARE`), reported on stderr, not a silent 1.** The runner converts a subcommand's `i32` into `std::process::ExitCode`, which only carries a `u8`. `unwrap_or(1)` would silently rewrite anything above 255 to 1, which is the value-masking default `.agents/instructions/typing-conventions.instructions.md` bans, and 1 collides with the generic-failure path. 70 says "the runner could not represent what happened", which is a defect in the runner rather than a verdict on the suite, and it is distinguishable from every status a real failure produces: 1 is the generic and `missing-cli` path in `mediapm/src/main.rs`, 2 is POSIX shell misuse, 3 is sync-warning, 4 is sync-error. It is also a plain numeric status on Windows, where `sysexits.h` does not exist. Ruled by the Task 2 implementer and ratified here. Cost if wrong: a caller sees 70 rather than the real code, but the stderr line names the actual value, so nothing is lost.
- **R6: `--locked` and `--no-locked` do not conflict, and `--no-locked` wins.** The cargo aliases hardcode `--locked`, so a `conflicts_with` between the two flags would make the escape hatch unreachable through the very aliases people use. Both flags being present is resolved in `Selection::wants_lock` by matching the pair `(self.locked, self.no_locked)` and letting `no_locked` win. The match reads both fields, which is deliberate: a field nothing reads is `dead_code` under `warnings = "deny"`, and an accepted-but-inert flag whose rationale lives only in a plan reads as a bug. The obvious one-liner `self.locked || !self.no_locked` is wrong and must not be substituted: with both flags present it returns true, so `--no-locked` loses. This was caught by the Task 1 implementer, who was handed that exact expression along with the two invariants it violates. Cost if wrong: a caller passing both gets locked instead of unlocked, which is the wrong way round for an escape hatch.

---

### Task 1: Scaffold the `test-runner` crate with its CLI and workspace metadata

**Files:**

- Create: `scripts/test-runner/Cargo.toml`
- Create: `scripts/test-runner/src/main.rs`
- Create: `scripts/test-runner/src/cli.rs`
- Create: `scripts/test-runner/src/cargo.rs`
- Modify: `Cargo.toml` (add `"scripts/test-runner"` to `members`)

**Interfaces:**

- Consumes: nothing.
- Produces: `cli::{Cli, Command, Selection}` where `Command` is `Test(Selection) | Doc(Selection) | FeatureMatrix(MatrixArgs)`; `Selection::{features, all_features, no_default_features, package, locked, no_locked, wants_lock(), feature_flags()}`; `cargo::{Metadata, Package, metadata(), status_code()}` where `Metadata { workspace_root: PathBuf, packages: Vec<Package> }` and `Package { name: String, features: BTreeMap<String, Vec<String>> }`.
- [ ] **Step 1: Write the failing CLI tests**

In `scripts/test-runner/src/cli.rs`, add a `#[cfg(test)] mod tests` beginning with `use clap::Parser;` and `use super::{Cli, Command};` (without the trait in scope none of these compile), covering:

```rust
#[test]
fn locked_is_the_default() {
    let cli = Cli::parse_from(["test-runner", "test"]);
    let Command::Test(sel) = cli.command else { panic!("expected test") };
    assert!(sel.wants_lock());
}

#[test]
fn no_locked_opts_out() {
    let cli = Cli::parse_from(["test-runner", "test", "--no-locked"]);
    let Command::Test(sel) = cli.command else { panic!("expected test") };
    assert!(!sel.wants_lock());
}

#[test]
fn no_locked_wins_over_locked() {
    let cli = Cli::parse_from(["test-runner", "test", "--locked", "--no-locked"]);
    let Command::Test(sel) = cli.command else { panic!("expected test") };
    assert!(!sel.wants_lock());
}

#[test]
fn locked_flag_alone_requests_locking() {
    let cli = Cli::parse_from(["test-runner", "test", "--locked"]);
    let Command::Test(sel) = cli.command else { panic!("expected test") };
    assert!(sel.wants_lock());
}

#[test]
fn feature_flags_emit_all_features() {
    let cli = Cli::parse_from(["test-runner", "test", "--all-features"]);
    let Command::Test(sel) = cli.command else { panic!("expected test") };
    assert_eq!(sel.feature_flags(), vec![OsString::from("--all-features")]);
}

#[test]
fn feature_flags_are_empty_by_default() {
    let cli = Cli::parse_from(["test-runner", "test"]);
    let Command::Test(sel) = cli.command else { panic!("expected test") };
    assert!(sel.feature_flags().is_empty());
}

#[test]
fn feature_flags_reject_no_default_features_with_all_features() {
    assert!(Cli::try_parse_from([
        "test-runner", "test", "--all-features", "--no-default-features"
    ]).is_err());
}

#[test]
fn feature_flags_reject_features_with_all_features() {
    assert!(Cli::try_parse_from([
        "test-runner", "test", "--all-features", "--features", "cli"
    ]).is_err());
}
```

- [ ] **Step 2: Run the tests and watch them fail**

Run: `RUSTC_WRAPPER="" cargo test -p test-runner 2>&1`
Expected: failure, because `scripts/test-runner/Cargo.toml` and the `main.rs`/`cli.rs` sources do not exist yet.

- [ ] **Step 3: Write `scripts/test-runner/Cargo.toml`**

```toml
[package]
name = "test-runner"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
license-file.workspace = true
publish.workspace = true
description = "Central runner for workspace tests, doctests and the feature matrix"

[lints]
workspace = true

[dependencies]
anyhow = { workspace = true }
# The workspace clap entry is `default-features = false` with only
# ["derive", "env", "std"], which leaves out `help`, `usage`,
# `error-context` and `suggestions`. Without them the binary has no
# `--help` at all and parse errors carry no usage line. These are clap
# 4's defaults minus `std` (already on) and `color`, added here rather
# than to the workspace entry so no other crate's surface moves.
clap = { workspace = true, features = [
    "help",
    "usage",
    "error-context",
    "suggestions",
] }
serde = { workspace = true }
serde_json = { workspace = true }
```

- [ ] **Step 4: Write `scripts/test-runner/src/cli.rs`**

Declare the `all_features` / `no_default_features` conflict on `all_features` only. `conflicts_with_all` on `features` covers `--features` against both, but it does not make those two conflict with each other, and cargo rejects that pair. Clap's conflicts are symmetric, so one side is enough. Omitting it makes the test above fail.

Module doc explaining that the module owns the command-line surface and that feature selection mirrors cargo's three mutually exclusive flags. Then:

```rust
use std::ffi::OsString;

use clap::{Args, Parser, Subcommand};

/// Parsed command line.
#[derive(Debug, Parser)]
#[command(
    name = "test-runner",
    version,
    about = "Central runner for workspace tests, doctests and the feature matrix."
)]
pub struct Cli {
    /// The subcommand to run.
    #[command(subcommand)]
    pub command: Command,
}

/// The three things the runner can do.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run the test suite, then the janitor and tempdir gates.
    Test(Selection),
    /// Run doctests, then the rustdoc link gate.
    Doc(Selection),
    /// Check every package and feature combination.
    FeatureMatrix(MatrixArgs),
}

/// Cargo-style package and feature selection.
///
/// The three feature flags are mutually exclusive and mirror cargo's own.
/// `--locked` is the default because CI runs this; `--no-locked` is the
/// local escape for a tree whose `Cargo.lock` still needs updating.
#[derive(Debug, Args, Clone)]
pub struct Selection {
    /// Space or comma separated list of features to activate.
    #[arg(
        long,
        value_name = "FEATURES",
        conflicts_with_all = ["all_features", "no_default_features"]
    )]
    pub features: Option<String>,
    /// Activate all available features.
    #[arg(long, conflicts_with = "no_default_features")]
    pub all_features: bool,
    /// Do not activate the `default` feature.
    #[arg(long)]
    pub no_default_features: bool,
    /// Package to check; defaults to the whole workspace.
    #[arg(short, long, value_name = "PKG")]
    pub package: Option<String>,
    /// Require an up-to-date `Cargo.lock`. This is the default.
    ///
    /// The cargo aliases that wrap this binary hardcode `--locked`, so the
    /// flag has to parse when a caller appends it. `--no-locked` is the only
    /// one of the two that changes the answer.
    #[arg(long)]
    pub locked: bool,
    /// Permit `Cargo.lock` to be updated. Wins over `--locked`, so a caller
    /// that appended it to an alias carrying `--locked` still gets the
    /// escape hatch.
    #[arg(long)]
    pub no_locked: bool,
}

impl Selection {
    /// Whether child cargo invocations must pass `--locked`.
    ///
    /// The match is on the tuple rather than on `no_locked` alone so that
    /// both fields are read: a field nothing reads is `dead_code` under
    /// `warnings = "deny"`. The precedence is `--no-locked` over `--locked`,
    /// so appending `--no-locked` to an alias that already carries
    /// `--locked` still unlocks the run.
    ///
    /// Note that `self.locked || !self.no_locked` is the tempting one-liner
    /// and it is wrong: with both flags it returns true, so `--no-locked`
    /// would lose.
    pub fn wants_lock(&self) -> bool {
        match (self.locked, self.no_locked) {
            (_, true) => false,
            (_, false) => true,
        }
    }

    /// The feature flags to append to a child cargo invocation.
    ///
    /// Order is fixed (`--features`, then `--all-features`, then
    /// `--no-default-features`) so two runs of the same selection produce
    /// byte-identical argv.
    pub fn feature_flags(&self) -> Vec<OsString> {
        let mut flags = Vec::new();
        if let Some(features) = &self.features {
            flags.push(OsString::from("--features"));
            flags.push(OsString::from(features));
        }
        if self.all_features {
            flags.push(OsString::from("--all-features"));
        }
        if self.no_default_features {
            flags.push(OsString::from("--no-default-features"));
        }
        flags
    }

    /// The `-p <PKG>` fragment, empty when the selection covers everything.
    pub fn package_flags(&self) -> Vec<OsString> {
        match &self.package {
            Some(pkg) => vec![OsString::from("-p"), OsString::from(pkg)],
            None => Vec::new(),
        }
    }
}

/// Arguments for the feature-matrix sweep.
#[derive(Debug, Args, Clone)]
pub struct MatrixArgs {
    /// Permit `Cargo.lock` to be updated. Wins over `--locked`.
    #[arg(long)]
    pub no_locked: bool,
    /// Require an up-to-date `Cargo.lock`. This is the default.
    ///
    /// Accepted for the same reason as `Selection::locked`: the CI step that
    /// runs the matrix appends `--locked`.
    #[arg(long)]
    pub locked: bool,
    /// Print every derived combination without running any of them.
    #[arg(long)]
    pub print: bool,
}

impl MatrixArgs {
    /// Whether each `cargo check` must pass `--locked`.
    ///
    /// Matches the tuple for the same reason as `Selection::wants_lock`, and
    /// resolves the precedence the same way.
    pub fn wants_lock(&self) -> bool {
        match (self.locked, self.no_locked) {
            (_, true) => false,
            (_, false) => true,
        }
    }
}
```

- [ ] **Step 5: Write `scripts/test-runner/src/cargo.rs`**

Module doc explaining that the module owns the typed view of `cargo metadata` and the helper that turns a child's exit status into a reportable code. Then:

```rust
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
```

- [ ] **Step 6: Write `scripts/test-runner/src/main.rs` as a minimal dispatcher**

```rust
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
fn dispatch(command: Command) -> anyhow::Result<i32> {
    match command {
        Command::Test(_) => todo!("Task 2"),
        Command::Doc(_) => todo!("Task 4"),
        Command::FeatureMatrix(_) => todo!("Task 5"),
    }
}
```

The `todo!` arms are intentional: each later task replaces one, and the crate must compile between tasks. Replace each arm in its own task.

- [ ] **Step 7: Register the crate in the workspace**

In the root `Cargo.toml`, add `"scripts/test-runner",` to `members`, keeping the list alphabetical: it goes after `"scripts/cargo-bin",` and before `"src/mediapm",`.

- [ ] **Step 8: Run the tests**

Run: `RUSTC_WRAPPER="" cargo test -p test-runner 2>&1`
Expected: PASS, 8 tests in `cli::tests`.

- [ ] **Step 8b: Lint**

Run: `RUSTC_WRAPPER="" cargo clippy -p test-runner --all-targets --all-features 2>&1`
Expected: exit 0 with no diagnostics.

`dispatch` is three `todo!()` arms at this point, so nothing calls
`cargo::metadata`, `cargo::status_code`, `Selection::package_flags` or
`MatrixArgs::wants_lock`, and `warnings = "deny"` turns each into a
`dead_code` error. Silence them with item-scoped `#[expect]` carrying a
substantive reason, never a bare `#[allow]`:

- `#[expect(dead_code, reason = "...")]` on `mod cargo;`. **Task 5 deletes it,**
  not Task 2: Task 2 calls `metadata()` and `status_code()`, but `Metadata::packages`
  and the whole `Package` struct are feature-matrix inputs and stay unread until
  Task 5, so `dead_code` is still firing in between and removing the expectation
  early turns Task 2 into a hard build error.
- `#[expect(dead_code, reason = "...")]` on `mod cli;`. **Task 5 deletes it.**
- `#[expect(clippy::struct_excessive_bools, reason = "...")]` on
  `Selection`, permanent: it mirrors cargo's flags one for one, and
  collapsing them into enums would diverge from cargo's own CLI.
- `#[expect(clippy::needless_pass_by_value, reason = "...")]` on
  `dispatch`. **Task 2 deletes it**, when the first arm moves its payload.

`#[expect]` fires `unfulfilled_lint_expectations` once it is fulfilled, so
leaving one of the three time-limited expectations in place is a hard build
error, not a warning. Tasks 2 and 5 must delete theirs.

---

### Task 2: The `test` subcommand, part one, nextest

**Files:**

- Create: `scripts/test-runner/src/nextest.rs`
- Create: `scripts/test-runner/src/test.rs`
- Modify: `scripts/test-runner/src/main.rs` (add `mod nextest; mod test;`, implement the `Test` arm)

**Interfaces:**

- Consumes: `cli::Selection`, `cargo::metadata`, `cargo::status_code` from Task 1.
- Produces: `test::run(selection: &Selection) -> anyhow::Result<i32>`; `nextest::argv(selection: &Selection) -> Vec<OsString>`.
- [ ] **Step 1: Write the failing argv test**

In `scripts/test-runner/src/nextest.rs`, add:

```rust
#[cfg(test)]
mod tests {
    use super::argv;
    use crate::cli::{Cli, Command};
    use clap::Parser;
    use std::ffi::OsString;

    fn selection(args: &[&str]) -> crate::cli::Selection {
        let mut argv = vec!["test-runner", "test"];
        argv.extend_from_slice(args);
        let Command::Test(sel) = Cli::parse_from(argv).command else {
            panic!("expected the test subcommand");
        };
        sel
    }

    #[test]
    fn argv_routes_through_cargo_bin_and_locks_both_levels() {
        assert_eq!(
            argv(&selection(&["--all-features"])),
            vec![
                OsString::from("run"),
                OsString::from("--quiet"),
                OsString::from("--package"),
                OsString::from("cargo-bin"),
                OsString::from("--locked"),
                OsString::from("--"),
                OsString::from("cargo-nextest"),
                OsString::from("run"),
                OsString::from("--locked"),
                OsString::from("--workspace"),
                OsString::from("--all-targets"),
                OsString::from("--all-features"),
            ]
        );
    }

    #[test]
    fn argv_omits_locked_when_opted_out() {
        let got = argv(&selection(&["--no-locked"]));
        assert!(!got.iter().any(|a| a == "--locked"));
        // The absence check alone would still pass with an outer `--locked`
        // misplaced before the separator, because it would then be absent
        // under `--no-locked` yet present otherwise. Pinning the separator's
        // position constrains the unlocked shape's prefix contiguity, which
        // the locked-shape exact-vector test does not cover.
        let separator = got.iter().position(|a| a == "--").expect("separator present");
        assert_eq!(separator, 4);
    }

    #[test]
    fn argv_appends_the_package_selector() {
        let got = argv(&selection(&["-p", "mediapm-utils"]));
        let idx = got.iter().position(|a| a == "-p").expect("-p present");
        assert_eq!(got[idx + 1], OsString::from("mediapm-utils"));
    }
}
```

- [ ] **Step 2: Run and watch it fail**

Run: `RUSTC_WRAPPER="" cargo test -p test-runner nextest 2>&1`
Expected: failure, because `src/nextest.rs` does not exist.

- [ ] **Step 2b: Note a plan defect this task's later code will hit**

`assert_eq!(parsed.workspace_root, Path::from("/repo"));` does not compile.
`std::path::Path` is unsized, a `#[repr(transparent)]` wrapper over `OsStr`,
so it has no `From` impl and the call additionally errors on an unknown size.
Use `Path::new("/repo")`.

- [ ] **Step 2c: Add the teardown this task's placeholders create**

The gate placeholders written in Step 6 return `Ok(())` unconditionally, which
`clippy::unnecessary_wraps` fires on. Silence each with an item-scoped
`#[expect(clippy::unnecessary_wraps, reason = "...")]` naming Task 3 as the task
that makes the return fallible, and record here that **Task 3 must delete
both**. A fulfilled `#[expect]` is an `unfulfilled_lint_expectations` build
error, the same trap as the `dispatch` expectation this task removes in Step 7.

- [ ] **Step 3: Write `scripts/test-runner/src/nextest.rs`**

Module doc explaining that nextest is reached through `cargo-bin` so it is auto-installed on first use, and that `run-all-tests.sh` used to call `cargo nextest` directly with nothing in `.github/` installing it.

```rust
use std::ffi::OsString;

use crate::cli::Selection;

/// The argv that runs the nextest suite for `selection`.
///
/// Nextest is reached as `cargo run --package cargo-bin -- cargo-nextest`,
/// which is the route the `bin` alias in `.cargo/config.toml` installs
/// through on first use. The
/// old shell runner called `cargo nextest` directly while nothing in
/// `.github/` or `rust-toolchain.toml` provisioned the binary, so CI
/// depended on it being ambient.
///
/// Both levels carry `--locked`, and they mean different things. Before the
/// `--` separator it is a cargo flag guarding the `cargo-bin` build; after
/// it, it is a nextest flag guarding the workspace build that the suite
/// runs. Omitting the outer one lets cargo rewrite `Cargo.lock` while
/// building `cargo-bin`, which the runner has no reason to permit: the old
/// shell runner passed `--locked` to cargo on every invocation.
pub fn argv(selection: &Selection) -> Vec<OsString> {
    let locked = selection.wants_lock();
    let mut argv: Vec<OsString> = vec![
        OsString::from("run"),
        OsString::from("--quiet"),
        OsString::from("--package"),
        OsString::from("cargo-bin"),
    ];
    if locked {
        argv.push(OsString::from("--locked"));
    }
    argv.push(OsString::from("--"));
    argv.push(OsString::from("cargo-nextest"));
    argv.push(OsString::from("run"));
    if locked {
        argv.push(OsString::from("--locked"));
    }
    argv.push(OsString::from("--workspace"));
    argv.push(OsString::from("--all-targets"));
    argv.extend(selection.feature_flags());
    argv.extend(selection.package_flags());
    argv
}
```

- [ ] **Step 4: Write the failing metadata test**

In `scripts/test-runner/src/cargo.rs`, extend the existing test module with a parse test that does not shell out:

```rust
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
```

- [ ] **Step 5: Write `scripts/test-runner/src/test.rs`**

Module doc explaining that the subcommand owns the suite run plus the two gates that follow it, and that the gates land in Task 3.

```rust
use std::path::Path;

use anyhow::Result;

use crate::cargo;
use crate::cli::Selection;

/// Runs the nextest suite for `selection`, then every gate that guards it.
///
/// A non-zero nextest exit returns immediately: running the temp-directory
/// gates after a failed suite would report leftovers belonging to a run
/// that had already been rejected.
pub fn run(selection: &Selection) -> Result<i32> {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let metadata = cargo::metadata(manifest_dir)?;
    let root = metadata.workspace_root;

    let code = run_nextest(selection)?;
    if code != 0 {
        return Ok(code);
    }

    crate::gates::tempdir::enforce(&root)?;
    crate::gates::janitor::enforce(&root)?;
    Ok(0)
}

/// Spawns the nextest suite and returns its exit code.
fn run_nextest(selection: &Selection) -> Result<i32> {
    let status = std::process::Command::new(std::env::var_os("CARGO").unwrap_or_else(|| std::ffi::OsString::from("cargo")))
        .current_dir(Path::new(env!("CARGO_MANIFEST_DIR")))
        .args(crate::nextest::argv(selection))
        .status()
        .context("spawn the nextest suite")?;
    cargo::status_code(status, "the nextest suite")
}
```

- [ ] **Step 6: Declare the gate modules as empty placeholders so the crate compiles**

Create `scripts/test-runner/src/gates/mod.rs`:

```rust
//! Repository hygiene gates that run after the test suite.
//!
//! Both gates answer one question: did the suite leave the tree in a state
//! the next person can work with. They used to exist twice, once in
//! `run-all-tests.sh` and once in `run-all-tests.ps1`; keeping them here
//! removes the parity requirement instead of maintaining it.

pub mod janitor;
pub mod tempdir;
```

Create `scripts/test-runner/src/gates/janitor.rs`:

```rust
//! Temp-directory janitor gate.

use std::path::Path;

use anyhow::Result;

/// Runs the janitor in dry-run mode and fails when the suite left a
/// mediapm-owned temp directory behind.
///
/// Placeholder body, replaced in Task 3.
pub fn enforce(_root: &Path) -> Result<()> {
    Ok(())
}
```

Create `scripts/test-runner/src/gates/tempdir.rs`:

```rust
//! Unprefixed-tempdir invariant gate.

use std::path::Path;

use anyhow::Result;

/// Rejects bare `tempfile::tempdir()` and `.prefix(` use outside the role
/// helpers.
///
/// Placeholder body, replaced in Task 3.
pub fn enforce(_root: &Path) -> Result<()> {
    Ok(())
}
```

- [ ] **Step 7: Wire the `Test` arm**

In `main.rs`, add `mod gates;`, `mod nextest;`, `mod test;` to the module list, then replace the `Test` arm:

```rust
Command::Test(selection) => test::run(&selection),
```

Delete the `#[expect(clippy::needless_pass_by_value, ...)]` on `dispatch`
that Task 1 added. It is now fulfilled, and a fulfilled `#[expect]` is an
`unfulfilled_lint_expectations` error.

Leave both `#[expect(dead_code, ...)]` attributes alone. The one on
`mod cargo;` must stay until Task 5, because `Metadata::packages` and
`Package` are still unread at the end of this task, so the lint is still
firing and the expectation is still required.

- [ ] **Step 8: Run the tests**

Run: `RUSTC_WRAPPER="" cargo test -p test-runner 2>&1`
Expected: PASS, including the three `nextest::tests` and the new `cargo::tests` parse test.

- [ ] **Step 9: Lint**

Run: `RUSTC_WRAPPER="" cargo clippy -p test-runner --all-targets --all-features 2>&1`
Expected: exit 0.

---

### Task 3: The two gates

**Files:**

- Modify: `scripts/test-runner/src/gates/janitor.rs`
- Modify: `scripts/test-runner/src/gates/tempdir.rs`

**Interfaces:**

- Consumes: nothing new.
- Produces: `janitor::{enforce, janitor_program, is_contract_line}` (the latter two `pub(crate)` or private, exercised by the module's own tests) and `tempdir::enforce`.
- [ ] **Step 1: Write the failing janitor contract tests**

In `janitor.rs`, add these tests:

```rust
#[cfg(test)]
mod tests {
    use super::is_contract_line;

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

    #[test]
    fn a_count_line_needs_digits() {
        assert!(!is_contract_line("would remove some mediapm temp director(ies)"));
        assert!(!is_contract_line("removed  mediapm temp director(ies)"));
    }

    #[test]
    fn contract_lines_must_match_wholly() {
        assert!(!is_contract_line("would remove: /tmp/x and then some"));
        assert!(!is_contract_line("no such directory: /nope"));
    }
}
```

- [ ] **Step 2: Run and watch them fail**

Run: `RUSTC_WRAPPER="" cargo test -p test-runner janitor 2>&1`
Expected: failure, `is_contract_line` is undefined.

- [ ] **Step 3: Implement `janitor.rs`**

First, delete the `#[expect(clippy::unnecessary_wraps, ...)]` that Task 2
placed on `enforce` in this file. The placeholder body returned `Ok(())`
unconditionally, which is what the lint fires on; the real body below can
fail, so the lint stops firing and a fulfilled `#[expect]` is an
`unfulfilled_lint_expectations` build error. The same deletion is required in
`tempdir.rs` at Step 6. Leave the two `#[expect(dead_code, ...)]` attributes
in `main.rs` alone; Task 5 removes those.

```rust
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
///
/// The per-directory lines are matched by [`is_path_line`] rather than by a
/// bare prefix. A prefix test cannot satisfy the two tests this module
/// requires at once: `would remove: /tmp/mediapm-artifact-abc` must be
/// recognised while `would remove: /tmp/x and then some` must not, and the
/// two share their first fourteen characters. Keeping the test bodies and
/// splitting the match on "is the remainder one token" is the direction
/// that leaves the safe default in place, since an ambiguous line is then
/// forwarded rather than swallowed. Note that the verdict on the tree does
/// not depend on any of this: it comes from the separate
/// `starts_with("would remove")` check, so this only decides what is echoed.
fn is_contract_line(line: &str) -> bool {
    if line == "no mediapm temp directories found"
        || is_path_line("would remove: ", line)
        || is_path_line("removed: ", line)
    {
        return true;
    }
    is_count_line("would remove", line) || is_count_line("removed", line)
}

/// Matches a per-directory line, `<prefix><path>`.
///
/// The path must be non-empty and a single token. A remainder carrying
/// whitespace is prose appended to a path, which means the line is not the
/// janitor's own output and belongs on stderr. The accepted edge is a
/// `TMPDIR` containing a space: such a path line is forwarded rather than
/// swallowed, which echoes one line and does not change the verdict.
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
/// On unix the script is spawned BY PATH, with no interpreter named. That is
/// deliberate and load-bearing: `scripts/clean-mediapm-temp.sh` carries a
/// `#!/usr/bin/env bash` shebang and uses `set -euo pipefail`, `[[ ]]` and
/// `read -r -d ''`, none of which dash parses. `sh` is bash on macOS, so the
/// mistake is invisible there and fatal on `ubuntu-latest`, where `/bin/sh`
/// is dash and the first line it reaches is `set -euo pipefail`. Letting the
/// shebang choose the interpreter is what the shell runner did, and it is
/// the only form that is correct on both.
fn janitor_command(root: &Path) -> (std::ffi::OsString, Vec<std::ffi::OsString>) {
    if cfg!(windows) {
        (
            std::ffi::OsString::from("pwsh"),
            vec![
                std::ffi::OsString::from("-NoProfile"),
                std::ffi::OsString::from("-File"),
                root.join("scripts")
                    .join("clean-mediapm-temp.ps1")
                    .into_os_string(),
                std::ffi::OsString::from("--dry-run"),
            ],
        )
    } else {
        (
            root.join("scripts")
                .join("clean-mediapm-temp.sh")
                .into_os_string(),
            vec![std::ffi::OsString::from("--dry-run")],
        )
    }
}

/// Fails when the test run left a mediapm-owned temp directory behind.
///
/// Runs the janitor in dry-run mode, so it reports and never deletes.
pub fn enforce(root: &Path) -> Result<()> {
    let (program, args) = janitor_command(root);
    let output = Command::new(&program)
        .args(&args)
        .output()
        .with_context(|| format!("run the janitor: {program:?} {args:?}"))?;

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
```

Note on the leftover test: it matches `starts_with("would remove")`, not `"would remove:"`. The old pwsh runner used the colon form and therefore missed the janitor's own `would remove 3 mediapm temp director(ies)` summary, reporting a clean sweep while leftovers existed. This matches the shell runner, which was the careful one.

- [ ] **Step 4: Write the failing tempdir gate tests**

In `tempdir.rs`, add:

```rust
#[cfg(test)]
mod tests {
    use super::violations_in;
    use std::fs;
    use std::path::Path;

    #[test]
    fn temp_rs_is_the_only_allowed_site() {
        let root = scratch("allowed");
        fs::create_dir_all(root.join("src/mediapm-utils/src")).expect("mkdir");
        fs::write(
            root.join("src/mediapm-utils/src/temp.rs"),
            "let _ = tempfile::Builder::new().prefix(\"mediapm-\");\nlet _ = tempfile::tempdir();\n",
        )
        .expect("write");
        assert!(violations_in(&root).expect("scan").is_empty());
    }

    #[test]
    fn a_bare_tempdir_elsewhere_is_reported() {
        let root = scratch("bare");
        fs::create_dir_all(root.join("src/mediapm/src")).expect("mkdir");
        fs::write(
            root.join("src/mediapm/src/lib.rs"),
            "fn f() { let _ = tempfile::tempdir(); }\n",
        )
        .expect("write");
        assert_eq!(violations_in(&root).expect("scan").len(), 1);
    }

    #[test]
    fn a_prefix_call_elsewhere_is_reported() {
        let root = scratch("prefix");
        fs::create_dir_all(root.join("tests/src")).expect("mkdir");
        fs::write(
            root.join("tests/src/mod.rs"),
            "fn f() { let _ = tempfile::Builder::new().prefix(\"x\"); }\n",
        )
        .expect("write");
        assert_eq!(violations_in(&root).expect("scan").len(), 1);
    }

    #[test]
    fn a_clean_tree_has_no_violations() {
        let root = scratch("clean");
        fs::create_dir_all(root.join("src/mediapm/src")).expect("mkdir");
        fs::write(root.join("src/mediapm/src/lib.rs"), "fn f() {}\n").expect("write");
        assert!(violations_in(&root).expect("scan").is_empty());
    }

    /// Creates a unique scratch tree under the managed temp prefix.
    fn scratch(tag: &str) -> std::path::PathBuf {
        static SEQ: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
        let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "mediapm-test-runner-gate-{}-{}-{seq}",
            std::process::id(),
            tag
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("create scratch root");
        dir
    }
}
```

- [ ] **Step 5: Run and watch them fail**

Run: `RUSTC_WRAPPER="" cargo test -p test-runner tempdir 2>&1`
Expected: failure, `violations_in` is undefined.

- [ ] **Step 6: Implement `tempdir.rs`**

Delete the `#[expect(clippy::unnecessary_wraps, ...)]` that Task 2 placed on
`enforce` here, for the same reason given in Step 3.

```rust
//! Unprefixed-tempdir invariant gate.
//!
//! Every mediapm-owned temp directory carries the `mediapm-` prefix so an
//! orphan is identifiable and the janitor can reclaim it. The role helpers
//! in `src/mediapm-utils/src/temp.rs` are the only place allowed to build
//! one, and this gate is what keeps that true.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

/// The single file permitted to construct a temp directory.
const ALLOWED: &str = "src/mediapm-utils/src/temp.rs";

/// The two patterns that build a temp directory outside the role helpers.
const FORBIDDEN: [&str; 2] = ["tempfile::tempdir(", ".prefix("];

/// Roots the walk covers.
const SCAN_ROOTS: [&str; 2] = ["src", "tests"];

/// Fails when a temp directory is built outside the role helpers.
///
/// Each violating file is printed with its path, then one summary line is
/// returned as the error so the caller owns the `error: ` prefix.
pub fn enforce(root: &Path) -> Result<()> {
    let violations = violations_in(root)?;
    if violations.is_empty() {
        return Ok(());
    }
    for violation in &violations {
        eprintln!("{violation}");
    }
    bail!("unprefixed tempdir/prefix use outside {ALLOWED}");
}

/// Every `file:line` in the tree that builds a temp directory illegally.
fn violations_in(root: &Path) -> Result<Vec<String>> {
    let allowed = root.join(ALLOWED);
    let mut found = Vec::new();
    for dir in SCAN_ROOTS {
        let start = root.join(dir);
        if start.is_dir() {
            walk(&start, &allowed, &mut found)?;
        }
    }
    Ok(found)
}

/// Recurses into `dir`, recording every `*.rs` file that matches.
///
/// `root` is the workspace root, kept so a violation prints the same
/// repo-relative path a reader would type, not an absolute one.
fn walk(root: &Path, dir: &Path, allowed: &Path, found: &mut Vec<String>) -> Result<()> {
    for entry in std::fs::read_dir(dir).with_context(|| format!("read {}", dir.display()))? {
        let entry = entry.with_context(|| format!("read entry in {}", dir.display()))?;
        let path = entry.path();
        if path.is_dir() {
            walk(root, &path, allowed, found)?;
            continue;
        }
        if path.extension().and_then(|e| e.to_str()) != Some("rs") || path == allowed {
            continue;
        }
        let text = std::fs::read_to_string(&path)
            .with_context(|| format!("read {}", path.display()))?;
        for (number, line) in text.lines().enumerate() {
            if FORBIDDEN.iter().any(|needle| line.contains(needle)) {
                let relative = path.strip_prefix(root).unwrap_or(&path).display();
                found.push(format!("{relative}:{}: {line}", number + 1));
            }
        }
    }
    Ok(())
}
```

The call site passes `root` through, so update the recursive call in `violations_in` to `walk(root, &start, &allowed, &mut found)?;`.

- [ ] **Step 6b: Test `enforce` itself, not just its parsers**

The tests in Steps 1 and 4 cover `is_contract_line` and `violations_in`.
Nothing yet exercises the two functions the crate exists to provide, and
Task 9 deletes `tests/scripts/test-run-all-tests.{sh,ps1}`, which covered
all three of `enforce`'s real behaviours against the real gate. Deleting
those files without restoring the coverage is a net loss, so restore it
here.

`enforce` takes the workspace root as a parameter, so a hermetic test needs
neither the real janitor nor a real temp root. Build a scratch root under
the managed `mediapm-` prefix containing a stub `scripts/clean-mediapm-temp.sh`
that prints canned lines and exits with a chosen status. Guard the stub with
`#[cfg(unix)]` and write a `#[cfg(windows)]` twin emitting `.ps1`, so the
`pwsh` branch is covered too; today nothing at all exercises it, because the
Windows CI job runs only `cargo --locked test-pkg mediapm-tests`.

Cover these three cases for the janitor gate, matching what the deleted shell
self-test asserted:

1. the stub exits non-zero and prints `no such directory: <path>`: `enforce`
   fails, and the error message names the cause, so a caller can tell a
   failed sweep from a dirty tree.
2. the stub exits 0 and prints `would remove: <path>`: `enforce` fails with
   `test suite left mediapm temp dirs behind`.
3. the stub exits 0 and prints `no mediapm temp directories found`: `enforce`
   passes.

Case 3 is the one that must not be lost. It is the only assertion that a
clean tree is not falsely reported dirty, and a broken spawn on a clean tree
produces the same message as case 1, so only case 3 distinguishes them.

For the tempdir gate, add one test that `enforce` fails and its error message
is `unprefixed tempdir/prefix use outside src/mediapm-utils/src/temp.rs` when
the scanned tree contains a violating file, and that it passes on a clean
tree. Reuse the `Scratch` guard from Step 4 so nothing is left at the temp
root.

- [ ] **Step 7: Run the tests**

Run: `RUSTC_WRAPPER="" cargo test -p test-runner 2>&1`
Expected: PASS, 4 tempdir tests and 4 janitor tests included.

- [ ] **Step 8: Verify the gate against the real tree**

Run: `RUSTC_WRAPPER="" cargo run -q -p test-runner -- --help 2>&1`
Expected: usage text listing `test`, `doc` and `feature-matrix`.

Do NOT run `cargo run -p test-runner -- test` in this task: it runs the full workspace suite, which the controller holds as its own gate.

- [ ] **Step 9: Lint**

Run: `RUSTC_WRAPPER="" cargo clippy -p test-runner --all-targets --all-features 2>&1`
Expected: exit 0.

---

### Task 4: The `doc` subcommand

**Files:**

- Create: `scripts/test-runner/src/doc.rs`
- Modify: `scripts/test-runner/src/main.rs` (implement the `Doc` arm)

**Interfaces:**

- Consumes: `cli::Selection`, `cargo::status_code` from Task 1.
- Produces: `doc::{run, doctest_argv, rustdoc_argv}`.
- [ ] **Step 1: Write the failing argv tests**

In `doc.rs`, add:

```rust
#[cfg(test)]
mod tests {
    use super::{doctest_argv, rustdoc_argv};
    use crate::cli::{Cli, Command};
    use clap::Parser;
    use std::ffi::OsString;

    fn selection(options: &[&str]) -> crate::cli::Selection {
        let mut argv = vec!["test-runner", "doc"];
        argv.extend_from_slice(options);
        let Command::Doc(sel) = Cli::parse_from(argv).command else {
            panic!("expected the doc subcommand");
        };
        sel
    }

    #[test]
    fn doctest_argv_covers_the_workspace_and_locks() {
        assert_eq!(
            doctest_argv(&selection(&["--all-features"])),
            vec![
                OsString::from("test"),
                OsString::from("--doc"),
                OsString::from("--workspace"),
                OsString::from("--locked"),
                OsString::from("--all-features"),
            ]
        );
    }

    #[test]
    fn doctest_argv_omits_locked_when_opted_out() {
        assert_eq!(
            doctest_argv(&selection(&["--no-locked"])),
            vec![
                OsString::from("test"),
                OsString::from("--doc"),
                OsString::from("--workspace"),
            ]
        );
    }

    #[test]
    fn rustdoc_argv_documents_private_items() {
        assert_eq!(
            rustdoc_argv(),
            vec![
                OsString::from("doc"),
                OsString::from("--locked"),
                OsString::from("--no-deps"),
                OsString::from("--workspace"),
                OsString::from("--all-features"),
                OsString::from("--document-private-items"),
            ]
        );
    }
}
```

- [ ] **Step 2: Run and watch them fail**

Run: `RUSTC_WRAPPER="" cargo test -p test-runner doc:: 2>&1`
Expected: failure, `src/doc.rs` does not exist.

- [ ] **Step 3: Write `src/doc.rs`**

```rust
//! The `doc` subcommand: doctests, then the rustdoc link gate.
//!
//! Doctests and the rustdoc gate were separate concerns before this crate
//! existed, which is why prek ran a `test docs` hook and a `rustdoc` hook
//! while CI ran the same work once inside the shell runner and once again
//! in its build step. They are one concern here: the link gate only means
//! something if the docs it reads are the ones under test.

use std::ffi::OsString;
use std::path::Path;

use anyhow::{Context, Result};

use crate::cargo;
use crate::cli::Selection;

/// Runs the doctests for `selection`, then the rustdoc link gate.
pub fn run(selection: &Selection) -> Result<i32> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let cargo_bin = std::env::var_os("CARGO").unwrap_or_else(|| OsString::from("cargo"));

    let status = std::process::Command::new(&cargo_bin)
        .current_dir(dir)
        .args(doctest_argv(selection))
        .status()
        .context("run the doctests")?;
    let code = cargo::status_code(status, "the doctest run")?;
    if code != 0 {
        return Ok(code);
    }

    let status = std::process::Command::new(&cargo_bin)
        .current_dir(dir)
        .args(rustdoc_argv())
        .status()
        .context("run the rustdoc link gate")?;
    cargo::status_code(status, "the rustdoc link gate")
}

/// The argv for the doctest run.
///
/// nextest cannot run doctests, which is the whole reason this step exists
/// as something separate from `test`.
pub fn doctest_argv(selection: &Selection) -> Vec<OsString> {
    let mut argv = vec![OsString::from("test"), OsString::from("--doc")];
    // Same reason as `nextest::argv`: `--workspace` wins silently over `-p`,
    // so emitting both made `-p` a no-op. Emitted only when no package was
    // named. It comes BEFORE the lock flag, because the pinned vector in
    // Step 1 puts it there. The rustdoc step below is deliberately
    // unaffected: it always runs workspace-wide so an off-by-default module
    // cannot hide a broken link.
    if selection.package.is_none() {
        argv.push(OsString::from("--workspace"));
    }
    if selection.wants_lock() {
        argv.push(OsString::from("--locked"));
    }
    argv.extend(selection.feature_flags());
    argv.extend(selection.package_flags());
    argv
}

/// The argv for the rustdoc link gate.
///
/// `--document-private-items` documents the private doc comments that hold
/// most cross-references in this workspace, and `--all-features` stops an
/// off-by-default module from hiding behind its feature flag. Without
/// both, a broken link here would pass.
pub fn rustdoc_argv() -> Vec<OsString> {
    vec![
        OsString::from("doc"),
        OsString::from("--locked"),
        OsString::from("--no-deps"),
        OsString::from("--workspace"),
        OsString::from("--all-features"),
        OsString::from("--document-private-items"),
    ]
}
```

- [ ] **Step 4: Wire the `Doc` arm**

In `main.rs`, add `mod doc;` and replace:

```rust
Command::Doc(selection) => doc::run(&selection),
```

- [ ] **Step 5: Run the tests**

Run: `RUSTC_WRAPPER="" cargo test -p test-runner 2>&1`
Expected: PASS, including the three `doc::tests`.

- [ ] **Step 6: Lint**

Run: `RUSTC_WRAPPER="" cargo clippy -p test-runner --all-targets --all-features 2>&1`
Expected: exit 0.

---

### Task 5: The `feature-matrix` subcommand

**Files:**

- Create: `scripts/test-runner/src/matrix.rs`
- Modify: `scripts/test-runner/src/main.rs` (implement the `FeatureMatrix` arm)

**Interfaces:**

- Consumes: `cli::MatrixArgs`, `cargo::Metadata`, `cargo::status_code` from Task 1.
- Produces: `matrix::{run, probes, Probe}` where `Probe { package: String, flags: Vec<String> }`.
- [ ] **Step 1: Write the failing derivation tests**

In `matrix.rs`, add:

```rust
#[cfg(test)]
mod tests {
    use super::{probes, Probe};
    use crate::cargo::Metadata;
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    fn metadata(names: &[(&str, &[&str])]) -> Metadata {
        let raw = format!(
            r#"{{"workspace_root":"/repo","packages":[{}]}}"#,
            names
                .iter()
                .map(|(name, features)| {
                    let entries: Vec<String> = features
                        .iter()
                        .map(|f| format!("\"{f}\":[]"))
                        .collect();
                    format!(r#"{{"name":"{name}","features":{{{}}}}}"#, entries.join(","))
                })
                .collect::<Vec<_>>()
                .join(",")
        );
        serde_json::from_str(&raw).expect("build metadata")
    }

    #[test]
    fn a_package_without_features_gets_two_endpoints() {
        let got = probes(&metadata(&[("alpha", &[])]));
        assert_eq!(
            got,
            vec![
                Probe { package: "alpha".into(), flags: vec!["--no-default-features".into()] },
                Probe { package: "alpha".into(), flags: vec!["--all-features".into()] },
            ]
        );
    }

    #[test]
    fn the_implicit_default_feature_is_not_swept_on_its_own() {
        let got = probes(&metadata(&[("alpha", &["cli", "default", "proptest"])]));
        assert_eq!(got.len(), 4);
        assert_eq!(got[1].flags, vec!["--features".to_string(), "cli".to_string()]);
        assert_eq!(got[2].flags, vec!["--features".to_string(), "proptest".to_string()]);
        assert!(got.iter().all(|p| p.flags != vec!["--features".to_string(), "default".to_string()]));
    }

    #[test]
    fn packages_are_ordered_by_name() {
        let got = probes(&metadata(&[("zulu", &[]), ("alpha", &[]), ("mike", &[])]));
        let names: Vec<&str> = got.iter().map(|p| p.package.as_str()).collect();
        assert_eq!(names, vec!["alpha", "alpha", "mike", "mike", "zulu", "zulu"]);
    }

    #[test]
    fn a_real_workspace_shape_deserializes() {
        let parsed: Metadata = serde_json::from_str(
            r#"{"workspace_root":"/repo","packages":[{"name":"beta","features":{}}]}"#,
        )
        .expect("parse");
        let _: BTreeMap<String, Vec<String>> = parsed.packages[0].features.clone();
        assert_eq!(parsed.workspace_root, PathBuf::from("/repo"));
    }
}
```

- [ ] **Step 2: Run and watch them fail**

Run: `RUSTC_WRAPPER="" cargo test -p test-runner matrix 2>&1`
Expected: failure, `src/matrix.rs` does not exist.

- [ ] **Step 3: Write `src/matrix.rs`**

```rust
//! The `feature-matrix` subcommand.
//!
//! Every workspace member is checked with `--no-default-features`, then
//! each single feature it declares, then `--all-features`. The CI workflow
//! used to carry the resulting combination list as a 47-row YAML matrix,
//! which is a list that silently rots: a crate gains a feature and the
//! matrix stops covering it. Deriving the list from `cargo metadata` makes
//! that impossible.
//!
//! Three things a reader of the old matrix had to be told, and which now
//! hold as properties of the derivation:
//!
//! 1. `--no-default-features` is not a minimal build of `mediapm-utils`
//!    through `mediapm` or `mediapm-conductor`. Both force features on
//!    regardless, so only probing the crate directly gives a minimal build.
//! 2. Eight bin targets need `required-features = ["cli"]`, so cargo skips
//!    them on every probe without it. Expected, not a failure.
//! 3. No `--all-targets`, so `#[cfg(test)]` code depending on an optional
//!    dependency goes unchecked. Adding it would unify dev-dependency
//!    features across all probes and mask a genuinely missing dependency.

use std::ffi::OsString;

use anyhow::{Context, Result};

use crate::cargo::{self, Metadata};
use crate::cli::MatrixArgs;

/// One `cargo check` probe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Probe {
    /// The workspace member to check.
    pub package: String,
    /// The feature flags to check it with.
    pub flags: Vec<String>,
}

/// Runs every derived probe, reporting all failures rather than the first.
pub fn run(args: &MatrixArgs) -> Result<i32> {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let metadata = cargo::metadata(dir)?;
    let derived = probes(&metadata);

    if args.print {
        for probe in &derived {
            println!("{} {}", probe.package, probe.flags.join(" "));
        }
        return Ok(0);
    }

    let cargo_bin = std::env::var_os("CARGO").unwrap_or_else(|| OsString::from("cargo"));
    let mut failures = Vec::new();
    for probe in &derived {
        let mut argv: Vec<OsString> = vec![OsString::from("check")];
        if args.wants_lock() {
            argv.push(OsString::from("--locked"));
        }
        argv.push(OsString::from("--package"));
        argv.push(OsString::from(&probe.package));
        argv.extend(probe.flags.iter().map(OsString::from));

        let status = std::process::Command::new(&cargo_bin)
            .current_dir(dir)
            .args(&argv)
            .status()
            .with_context(|| format!("run cargo check for {}", probe.package))?;
        match cargo::status_code(status, &probe.package)? {
            0 => {}
            code => failures.push(format!("{} {} (exit {code})", probe.package, probe.flags.join(" "))),
        }
    }

    if failures.is_empty() {
        println!("feature matrix: {} combinations, all clean", derived.len());
        return Ok(0);
    }
    for failure in &failures {
        eprintln!("error: {failure}");
    }
    eprintln!(
        "error: {} of {} feature combinations failed",
        failures.len(),
        derived.len()
    );
    Ok(1)
}

/// Derives every probe from `metadata`, ordered by package name.
///
/// Per package: `--no-default-features`, then each declared feature in
/// sorted order except the implicit `default`, then `--all-features`.
/// `cargo metadata` reports `default` alongside the real features, and
/// sweeping it alone is redundant, so it is filtered out.
pub fn probes(metadata: &Metadata) -> Vec<Probe> {
    let mut packages: Vec<_> = metadata.packages.iter().collect();
    packages.sort_by(|a, b| a.name.cmp(&b.name));

    let mut derived = Vec::new();
    for package in packages {
        derived.push(Probe {
            package: package.name.clone(),
            flags: vec!["--no-default-features".to_string()],
        });
        for feature in package.features.keys().filter(|name| name.as_str() != "default") {
            derived.push(Probe {
                package: package.name.clone(),
                flags: vec!["--features".to_string(), feature.clone()],
            });
        }
        derived.push(Probe {
            package: package.name.clone(),
            flags: vec!["--all-features".to_string()],
        });
    }
    derived
}
```

- [ ] **Step 4: Wire the `FeatureMatrix` arm**

In `main.rs`, add `mod matrix;` and replace:

```rust
Command::FeatureMatrix(args) => matrix::run(&args),
```

Delete both `#[expect(dead_code, ...)]` attributes that Task 1 added, the
one on `mod cargo;` and the one on `mod cli;`. This is the last task that
consumes anything from either module: `Metadata::packages` and `Package`
belong to this task's derivation, and `MatrixArgs::wants_lock` is the final
unconsumed item in `cli`. Both lints stop firing here, so both expectations
become `unfulfilled_lint_expectations` errors if left in place. Leave the
permanent `#[expect(clippy::struct_excessive_bools, ...)]` on `Selection`
alone; the `needless_pass_by_value` expectation that once sat on `dispatch`
was already deleted in Task 2 and must not be recreated.

- [ ] **Step 5: Run the tests**

Run: `RUSTC_WRAPPER="" cargo test -p test-runner 2>&1`
Expected: PASS, including the four `matrix::tests`.

- [ ] **Step 6: Check the derivation against the real workspace**

Run: `RUSTC_WRAPPER="" cargo run -q -p test-runner -- feature-matrix --print 2>&1 | wc -l`
Expected: 49. The old YAML matrix had 47 rows covering 12 packages; `test-runner` is the 13th workspace member and brings its own two endpoint rows.

Run: `RUSTC_WRAPPER="" cargo run -q -p test-runner -- feature-matrix --print 2>&1 | grep -c "features default"`
Expected: 0. Any hit means the implicit `default` feature leaked into the single-feature sweep.

- [ ] **Step 7: Lint**

Run: `RUSTC_WRAPPER="" cargo clippy -p test-runner --all-targets --all-features 2>&1`
Expected: exit 0.

---

### Task 6: Point the cargo aliases at the runner

**Files:**

- Modify: `.cargo/config.toml`

**Interfaces:**

- Consumes: the `test` and `doc` subcommands from Tasks 2 and 4.
- Produces: `cargo test-all` and `cargo test-doc-all` as the public entry points.
- [ ] **Step 1: Replace the two aliases**

In `.cargo/config.toml`, replace the `test-all` line and the `doc-test` block with:

```toml
test-all = ["run", "--package", "test-runner", "--", "test", "--all-features", "--locked"]
test-doc-all = ["run", "--package", "test-runner", "--", "doc", "--all-features", "--locked"]
```

Delete the four-line comment above `doc-test` that explains why doctests are a separate alias. It described a cargo alias's single-argv limitation, which is no longer the reason: `test-doc-all` is a separate subcommand, not a separate alias standing in for what an alias cannot express.

Leave `build-all`, `clippy-all`, `fmt-check`, `build-pkg`, `clippy-pkg`, `test-pkg` and `bin` exactly as they are.

- [ ] **Step 2: Verify the alias parses and dispatches**

Run: `RUSTC_WRAPPER="" cargo test-all --help 2>&1`
Expected: the runner's usage text, exit 0. `--help` reaches the runner because cargo forwards unrecognised trailing arguments to a `run` alias's target.

If `--help` is swallowed by cargo instead, run `RUSTC_WRAPPER="" cargo test-all 2>&1` and confirm the nextest run starts, then record that `--help` passes through and move on.

- [ ] **Step 3: Verify the doc alias dispatches**

Run: `RUSTC_WRAPPER="" cargo test-doc-all --help 2>&1`
Expected: usage text listing `test`, `doc` and `feature-matrix`.

---

### Task 7: Point prek.toml at the runner

**Files:**

- Modify: `prek.toml`

**Interfaces:**

- Consumes: the `test` and `doc` subcommands.
- Produces: a pre-push stage that runs the same gates CI runs.
- [ ] **Step 1: Replace the local test hook**

In the `[[repos]] repo = "local"` block, replace the `test` hook entry with:

```toml
  { id = "test", name = "test", entry = "cargo run --package test-runner -- test --all-features --locked", language = "system", pass_filenames = false, stages = [
    "pre-push",
  ] },
```

- [ ] **Step 2: Delete the standalone rustdoc hook**

Delete this hook from the same block:

```toml
  { id = "rustdoc", name = "rustdoc", entry = "cargo doc --locked --no-deps --workspace --all-features --document-private-items", language = "system", pass_filenames = false, stages = [
    "pre-push",
  ] },
```

Its work now runs inside the `doc` subcommand.

- [ ] **Step 3: Retarget the doctest hook**

Replace the FeryET `test docs` hook:

```toml
  { id = "test", name = "test docs", args = ["--doc", "--workspace"], pass_filenames = false, stages = [
    "pre-push",
  ] },
```

with a local hook, and add it to the `repo = "local"` block so both pre-push test hooks read the same way:

```toml
  { id = "test-docs", name = "test docs", entry = "cargo run --package test-runner -- doc --all-features --locked", language = "system", pass_filenames = false, stages = [
    "pre-push",
  ] },
```

Note the hook id changes from `test` to `test-docs` so the two hooks in the same file are distinct. `SKIP=test-docs` still skips it. `SKIP=rustdoc` no longer works, because that hook id is gone.

- [ ] **Step 4: Update the stale comment above the deleted rustdoc hook**

Delete the four-line comment block that begins `# rustdoc: unresolved intra-doc links, the same gate `run-all-tests.sh` runs.` It describes a hook that no longer exists and names a script that Task 9 deletes.

- [ ] **Step 5: Verify prek parses the config**

Run: `prek validate-config 2>&1 || prek run test --all-files --dry-run 2>&1 | head -20`
Expected: prek reports the config as valid, or lists the pre-push hooks it would run.

- [ ] **Step 6: Verify the hooks resolve**

Run: `prek run test --hook-stage pre-push 2>&1 | head -30`
Expected: prek resolves `cargo run --package test-runner -- test --all-features --locked` and starts the suite. Interrupt it once the suite is visibly running; do not let it run to completion, the controller holds that gate.

---

### Task 8: Point ci.yml at the runner

**Files:**

- Modify: `.github/workflows/ci.yml`

**Interfaces:**

- Consumes: the `test`, `doc` and `feature-matrix` subcommands.
- Produces: a CI workflow with no duplicated gate and no hand-maintained matrix.
- [ ] **Step 1: Replace the two test steps**

In the `build` job, replace:

```yaml
      - name: Run tests
        run: scripts/run-all-tests.sh

      - name: Run large tests
        run: scripts/run-all-tests.sh --large
```

with:

```yaml
      - name: Run tests
        run: cargo --locked test-all

      - name: Run doctests and rustdoc
        run: cargo --locked test-doc-all
```

- [ ] **Step 2: Drop the duplicated doc line from the build step**

In the same job's `Build project` step, delete the `cargo --locked doc --no-deps --workspace --all-features --document-private-items` line. `test-doc-all` already runs it, so it was running twice per push.

- [ ] **Step 3: Replace the feature-matrix job body**

In the `feature-matrix` job, delete the entire `strategy:` block including `fail-fast: false` and the 47-row `matrix.include` list, along with the three-paragraph comment above it that explains the matrix gotchas (that comment now lives in `scripts/test-runner/src/matrix.rs`).

Replace the `Check feature combination` step with:

```yaml
      - name: Check every feature combination
        run: cargo --locked run --package test-runner -- feature-matrix --locked
```

Keep the job's `name`, `runs-on`, checkout step and toolchain step. Rename the job's display name to `Feature Matrix` since it no longer carries a matrix.

- [ ] **Step 4: Verify the YAML parses and no stale reference remains**

Run: `python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/ci.yml'))" && echo parses`
Expected: `parses`.

Run: `grep -n "run-all-tests\|matrix:" .github/workflows/ci.yml`
Expected: no output.

- [ ] **Step 5: Confirm the remaining steps still name real aliases**

Run: `grep -oE "cargo (bin|clippy-all|fmt-check|build-all|test-all|test-doc-all|test-pkg)" .github/workflows/ci.yml | sort -u`
Expected: every name printed is present in `.cargo/config.toml`'s `[alias]` section.

---

### Task 9: Delete the old runner scripts and their self-tests

**Files:**

- Delete: `scripts/run-all-tests.sh`
- Delete: `scripts/run-all-tests.ps1`
- Delete: `tests/scripts/test-run-all-tests.sh`
- Delete: `tests/scripts/test-run-all-tests.ps1`
- Modify: `tests/scripts/mod.rs`

**Interfaces:**

- Consumes: the runner from Tasks 1 to 5.
- Produces: a tree with one implementation of each gate.
- [ ] **Step 1: Confirm nothing references the deleted scripts**

Run: `grep -rn "run-all-tests" --include="*.toml" --include="*.yml" --include="*.yaml" --include="*.sh" --include="*.ps1" --include="*.json" --include="*.jsonc" . 2>/dev/null | grep -v "^./target/" | grep -v "^./.superpowers/" | grep -v "^./.pi/" | grep -v "^./docs/superpowers/"`
Expected: only `tests/scripts/mod.rs` (which Step 4 edits) and the doc files (which Task 10 edits).

- [ ] **Step 2: Delete the four files**

Run: `git rm scripts/run-all-tests.sh scripts/run-all-tests.ps1 tests/scripts/test-run-all-tests.sh tests/scripts/test-run-all-tests.ps1`
Expected: all four staged as deletions.

- [ ] **Step 3: Shrink `TEST_SCRIPTS` in `tests/scripts/mod.rs`**

Replace:

```rust
const TEST_SCRIPTS: [&str; 4] = [
    "test-clean-mediapm-temp.sh",
    "test-clean-mediapm-temp.ps1",
    "test-run-all-tests.sh",
    "test-run-all-tests.ps1",
];
```

with:

```rust
const TEST_SCRIPTS: [&str; 2] = [
    "test-clean-mediapm-temp.sh",
    "test-clean-mediapm-temp.ps1",
];
```

- [ ] **Step 4: Delete the two self-test runner tests**

In the same file, delete every `#[test]` function that invokes `test-run-all-tests.sh` or `test-run-all-tests.ps1`, and any helper they alone used (read around lines 270 to 300 of the file as it stands before editing). Keep every janitor test.

If a deleted test used a helper that a janitor test also uses, keep the helper.

- [ ] **Step 5: Update the module doc**

Replace the first paragraph of the file's `//!` doc:

```rust
//! Integration tests for repository scripts: the temp-janitor production
//! scripts (`scripts/clean-mediapm-temp.{sh,ps1}`), their self-tests, and
//! the run-all-tests runner self-tests (`tests/scripts/test-run-all-tests.*`).
```

with:

```rust
//! Integration tests for repository scripts: the temp-janitor production
//! scripts (`scripts/clean-mediapm-temp.{sh,ps1}`) and their self-tests.
//!
//! The test pipeline itself is not tested from here. It is the
//! `test-runner` crate, and it carries its own gate tests in Rust.
```

- [ ] **Step 6: Run the crate's tests**

Run: `RUSTC_WRAPPER="" cargo test -p mediapm-tests 2>&1`
Expected: PASS. The `script_files_exist_and_are_executable` test now expects two self-test scripts, not four.

- [ ] **Step 7: Lint the workspace member**

Run: `RUSTC_WRAPPER="" cargo clippy -p mediapm-tests --all-targets --all-features 2>&1`
Expected: exit 0.

---

### Task 10: Update every doc that names the old entry points

**Files:**

- Modify: `AGENTS.md`
- Modify: `README.md`
- Modify: `src/mediapm/AGENTS.md`
- Modify: `.agents/instructions/rust-conventions.instructions.md`
- Modify: `.agents/instructions/ci-workflow.instructions.md`
- Modify: `.agents/instructions/temp-directory-spec.instructions.md`
- Modify: `.agents/instructions/scripts.instructions.md`
- Modify: `.vscode/settings.json`
- Modify: `.agents/coverage-matrix.md` (only if it tracks a spec row about the gate)

**Interfaces:**

- Consumes: everything above.
- Produces: docs that name the new entry points.
- [ ] **Step 1: Fix the quick-start line in `AGENTS.md`**

Replace:

```text
- **Tests/build**: `cargo test -p <crate>`; `cargo build-pkg <crate>`; full validation via `cargo fmt-check`, `cargo clippy-all`, `cargo test-all`.
```

with:

```text
- **Tests/build**: `cargo test -p <crate>`; `cargo build-pkg <crate>`; full validation via `cargo test-all` (nextest plus the janitor and tempdir gates) and `cargo test-doc-all` (doctests plus the rustdoc link gate), alongside `cargo fmt-check` and `cargo clippy-all`.
```

- [ ] **Step 2: Fix the two development lines in `src/mediapm/AGENTS.md`**

Replace `cargo test-pkg mediapm` / `cargo build-pkg mediapm` on the Development line only if it names a removed alias. `test-pkg` and `build-pkg` both still exist, so that line stays.

Replace the full-workspace line:

```text
Full workspace: `cargo fmt-check && cargo clippy-all && cargo test-all`.
```

with:

```text
Full workspace: `cargo fmt-check && cargo clippy-all && cargo test-all && cargo test-doc-all`.
```

- [ ] **Step 3: Fix `README.md`**

Replace `cargo test-all    # test entire workspace` with:

```text
cargo test-all      # nextest + janitor and tempdir gates
cargo test-doc-all  # doctests + rustdoc link gate
```

Keep the `cargo test-pkg <crate>` line, which still works.

- [ ] **Step 4: Fix the verification-commands section in `rust-conventions.instructions.md`**

That section currently claims `scripts/run-all-tests.sh` is the gate and calls `cargo test-all` a weaker check. Both are now wrong in the same direction. Rewrite it to state:

- `cargo test-all` is the workspace test gate: nextest over `--all-features`, then the janitor dry-run gate, then the unprefixed-tempdir gate. It is no longer a bare nextest alias.
- `cargo test-doc-all` is the separate doctest and rustdoc gate, kept separate because nextest cannot run doctests.
- CI runs both. prek's pre-push runs both. The two are the same gate now, not two runners that drifted.

Delete any sentence naming `scripts/run-all-tests.sh` and any sentence describing `cargo test-all` as nextest-only.

- [ ] **Step 5: Fix `ci-workflow.instructions.md`**

Update the validation-gates and CI-parity sections:

- pre-push hooks are now `cargo-check`, `clippy`, `test` (the runner's `test`), and `test docs` (the runner's `doc`). The `rustdoc` hook id is gone.
- pre-push runs the janitor and tempdir gates, which it did not before.
- CI's `Run tests` step is `cargo --locked test-all`; `Run doctests and rustdoc` is `cargo --locked test-doc-all`. There is no `Run large tests` step, because `--all-features` already activates `large-tests`.
- The feature-matrix job derives its combinations from `cargo metadata` instead of carrying them in YAML.
- [ ] **Step 6: Fix `temp-directory-spec.instructions.md`**

The janitor-contract and regression-gate sections name `run-all-tests.{sh,ps1}` as the thing that runs the gates. Replace those references with `scripts/test-runner` and its `test` subcommand. State that the gate now exists once in Rust rather than twice as `grep -rn` and `Get-ChildItem | Select-String`.

Also correct the janitor self-test bullet: `tests/scripts/test-clean-mediapm-temp.{sh,ps1}` still live in the root `tests` crate, but `test-run-all-tests.{sh,ps1}` no longer exist.

- [ ] **Step 7: Fix `scripts.instructions.md`**

The file requires cross-platform helpers to ship as `.sh` + `.ps1` twins. Add one line to that section: `scripts/test-runner` is a crate rather than a script pair, because its twin-free existence is the point: the gates it holds are implemented once. The janitor remains a twin pair because it is a user-facing command.

- [ ] **Step 8: Fix the auto-approve regex in `.vscode/settings.json`**

The regex currently lists `test-all`, `test-pkg` and other cargo subcommands. Add `test-doc-all` and `run` is already there. Confirm the final regex still matches `cargo test-doc-all --all-features --locked` and `cargo run --locked run --package test-runner -- feature-matrix --locked`.

- [ ] **Step 9: Check the coverage matrix**

Run: `grep -n "run-all-tests\|test-all\|doc-test" .agents/coverage-matrix.md`
Expected: no output, or rows that Task 10's edits should update. Update any row that names a removed entry point.

- [ ] **Step 10: Lint every markdown file touched**

Run: `RUSTC_WRAPPER="" cargo bin rumdl check AGENTS.md README.md src/mediapm/AGENTS.md .agents/instructions/rust-conventions.instructions.md .agents/instructions/ci-workflow.instructions.md .agents/instructions/temp-directory-spec.instructions.md .agents/instructions/scripts.instructions.md 2>&1`
Expected: exit 0.

- [ ] **Step 11: Confirm no stale reference survives anywhere**

Run: `grep -rn "run-all-tests\|doc-test" --include="*.md" --include="*.toml" --include="*.yml" --include="*.json" --include="*.jsonc" . 2>/dev/null | grep -v "^./target/" | grep -v "^./.superpowers/" | grep -v "^./.pi/" | grep -v "^./docs/superpowers/"`
Expected: no output.

---

## Controller-held closing gate

After every task is reviewed and committed, the controller runs, in this order:

1. `RUSTC_WRAPPER="" cargo fmt --all -- --check`
2. `RUSTC_WRAPPER="" cargo clippy -p test-runner --all-targets --all-features`
3. `RUSTC_WRAPPER="" cargo test-all`
4. `RUSTC_WRAPPER="" cargo test-doc-all`
5. `RUSTC_WRAPPER="" cargo run --package test-runner -- feature-matrix --print | wc -l` (expect 49)
6. `prek validate-config`
