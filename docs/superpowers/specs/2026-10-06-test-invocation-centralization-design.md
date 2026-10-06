# Test invocation centralization design

**Date:** 2026-10-06
**Status:** approved
**Plan:** `docs/superpowers/plans/2026-10-06-test-invocation-centralization.md`

## Problem

Four files each describe how to run this workspace's tests, and they disagree.

| Surface | nextest features | doctests | rustdoc | janitor gate | tempdir gate |
| --- | --- | --- | --- | --- | --- |
| `scripts/run-all-tests.sh` (what CI runs) | default | yes, no `--all-features` | yes | yes | yes |
| `prek.toml` pre-push | `--all-features` | yes, no `--all-features` | yes | no | no |
| `cargo test-all` alias | `--all-features` | no | no | no | no |
| `ci.yml` build step | n/a | n/a | yes, a second time | no | no |

`--locked` is passed in CI and absent in prek. The Windows job runs something else entirely (`cargo --locked test-pkg mediapm-tests`).

A structural fact constrains the fix. A cargo alias is a single argv: it cannot sequence five steps, stop on the first failure, or run a filesystem gate after the tests. Centralizing in `.cargo/config.toml` therefore means either one alias per step (which leaves the ordering duplicated in two other files) or one alias routing through a binary that owns the sequencing. The second form is what this spec builds.

## Decision

One new workspace member, `scripts/test-runner/` (package `test-runner`), owns the test pipeline. `.cargo/config.toml`, `prek.toml` and `ci.yml` each become thin callers.

`scripts/run-all-tests.sh`, `scripts/run-all-tests.ps1`, `tests/scripts/test-run-all-tests.sh` and `tests/scripts/test-run-all-tests.ps1` are deleted. The janitor pair `scripts/clean-mediapm-temp.{sh,ps1}` stays as scripts, because it is a user-facing command with its own output contract and its own self-tests.

### CLI surface

```sh
cargo run -p test-runner -- <subcommand> [selection flags]
```

| Subcommand | Runs |
| --- | --- |
| `test` | nextest over the selection, then the unprefixed-tempdir gate, then the janitor dry-run gate |
| `doc` | doctests over the selection, then the rustdoc link gate |
| `feature-matrix` | every package x feature combination, checked one at a time |

Selection flags mirror cargo and are mutually exclusive: `--features <FEATURES>`, `--all-features`, `--no-default-features`, plus `-p/--package`. Omitted means workspace defaults. `--locked` defaults on; `--no-locked` is the local escape. `--large` is removed, because `--all-features` already activates `large-tests` on `mediapm-cas`.

### Aliases

```toml
test-all     = ["run", "--package", "test-runner", "--", "test", "--all-features", "--locked"]
test-doc-all = ["run", "--package", "test-runner", "--", "doc",  "--all-features", "--locked"]
```

`build-all`, `clippy-all`, `fmt-check`, `build-pkg`, `clippy-pkg`, `test-pkg` and `bin` are untouched.

### prek.toml

`cargo-check` and `clippy` stay as FeryET hooks. `test` and `test docs` become runner calls. The `rustdoc` hook is deleted because its work moved into the `doc` subcommand. `SKIP=test` and `SKIP=test-docs` keep working; `SKIP=rustdoc` does not, because that hook id is gone.

Pre-push gains the janitor and tempdir gates, which it never ran.

### ci.yml

The two test steps become `cargo --locked test-all` and `cargo --locked test-doc-all`. The `Run large tests` step goes. The `cargo doc` line in `Build project` goes, because `test-doc-all` already does that work. rumdl, `clippy-all`, `fmt-check` and `build-all` stay.

The `feature-matrix` job keeps its own job but loses `strategy.matrix.include`, replaced by one step running the runner's `feature-matrix` subcommand. The Windows job is untouched.

### Feature matrix derivation

`cargo metadata --no-deps --format-version 1`. Per package: `--no-default-features`, each single feature from that package's own `features` map excluding `default`, then `--all-features`. Packages with no features get the two endpoints only.

This reproduces the current 47 rows exactly. `cargo metadata` reports 55 because it includes the implicit `default` feature on the eight packages that have one; removing those gives 47.

### Reaching nextest

The runner shells out to `cargo run --quiet --package cargo-bin -- cargo-nextest run <args>`, which auto-installs nextest through binstall. This is the pattern `test-all` uses today. Nesting `cargo` inside a `cargo run` binary in the same target directory was probed and does not deadlock; `build-utils` needs a nested `CARGO_TARGET_DIR` for the different reason that it nests during compile, from a `build.rs`.

This also closes a gap: nothing in `.github/`, `rust-toolchain.toml` or the workflow installs nextest, while the old `run-all-tests.sh` invoked `cargo --locked nextest` directly.

## Gates

The janitor gate keeps today's behaviour and its exact messages:

- run the janitor in dry-run mode, reading its exit status before its output
- non-zero exit: `error: mediapm temp-dir sweep failed: <output>`, exit 1
- forward every line that does not match the janitor's output contract to stderr
- any `would remove:` line: `error: test suite left mediapm temp dirs behind`, exit 1

The tempdir invariant gate walks `src/` and `tests/` for `.rs` files and rejects `tempfile::tempdir(` or `.prefix(` outside `src/mediapm-utils/src/temp.rs`, reporting `unprefixed tempdir/prefix use outside src/mediapm-utils/src/temp.rs`.

Both gates exist twice today, once as `grep -rn` and once as `Get-ChildItem | Select-String`. In Rust they exist once, so the parity requirement between the twins disappears instead of being maintained.

## Tests

The hand-written shell self-tests go away, replaced by Rust tests in the crate:

- `--help` exits 0; an unknown argument exits 2, which is clap's usage-error code and a change from the shell runner's 1
- the janitor gate against a stub janitor: missing root gives the sweep-failed message, a root holding a leftover `mediapm-*` gives the leftover message, an empty root passes
- the tempdir invariant gate against a fixture that violates the rule, and the `temp.rs` case that must pass
- feature-matrix derivation against a synthetic metadata document, asserting the exact combination list

`tests/scripts/test-clean-mediapm-temp.{sh,ps1}` stay in the root `mediapm-tests` crate, because the janitor is still a script.

## Files updated for accuracy

`AGENTS.md` quick start, `src/mediapm/AGENTS.md`, `README.md`, `.agents/instructions/rust-conventions.instructions.md`, `.agents/instructions/ci-workflow.instructions.md`, `.agents/instructions/temp-directory-spec.instructions.md`, `.agents/instructions/scripts-and-permissions.instructions.md`, and the `.vscode/settings.json` terminal auto-approve regex, which lists subcommand names and needs `test-doc-all` in place of `doc-test`.

## Accepted cost

`--all-features` on the main gate means CI now runs `large-tests`, the network-heavy `mediapm-cas` work CI skips today. Dropping `--large` removes the duplicate step, not the cost, because `--all-features` already implied it. Pre-push and CI get slower and CI gains a network dependency. This is prek's policy winning over CI's.
