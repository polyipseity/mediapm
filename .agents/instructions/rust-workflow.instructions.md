---
description: "Use when editing Rust code, Cargo/config, CI, prek, or nextest files. Covers validation workflow, hooks, CI parity, Rust conventions, tests, and doc/lint rules."
name: "Rust Workflow and Conventions"
applyTo: "**/*.rs, Cargo.toml, Cargo.lock, rust-toolchain.toml, .rustfmt.toml, .clippy.toml, .config/**/*.toml, .cargo/**/*.toml, prek.toml, .github/workflows/**/*.yml, .github/workflows/**/*.yaml"
---

# Rust workflow and conventions

## Source of truth

- `Cargo.toml`, `.cargo/config.toml`, `.config/nextest.toml`, `rust-toolchain.toml`, `rustfmt.toml`/`clippy.toml`, `.github/workflows/ci.yml`, `prek.toml`.
- `AGENTS.md` and `.agents/instructions/*.instructions.md` define the active contract. Workspace members: `src/mediapm-cas/`, `src/mediapm-conductor/`, `src/mediapm-conductor-builtins/*/`, `src/mediapm/`.

## Validation workflow

Default to a fast inner loop during edits. Treat full workspace and parity gates as explicit checkpoints.

- **Inner loop.** Use the narrowest check that covers the change: `cargo test -p <crate> <test_name>`, `cargo test-pkg <crate>`, `cargo clippy-pkg <crate>`, `cargo build-pkg <crate>`, or targeted `cargo check`/`cargo fmt-check`/`cargo bin rumdl check`. Do not run slow full-workspace gates on every edit.
- **Checkpoints.** For pre-push/CI parity, run `cargo fmt-check`, `cargo bin rumdl check`, `cargo clippy-all`, `cargo test-all`, `cargo test-doc-all`, and `cargo build-all`/feature-matrix as required. `test-all` and `test-doc-all` go through the `test-runner` crate; `test-doc-all` runs doctests and then the rustdoc link gate as one command. `prek.toml` pre-push runs `cargo-check`, `clippy`, `test-runner test`, `test-runner doc`; CI mirrors and extends it.
- **Warnings.** `-D warnings` holds in CI and locally (`.cargo/config.toml` `[build] rustflags = ["-Dwarnings"]`). Fix warnings at the root; bare lint suppression is banned.
- libtest args go after `--`: `cargo test -p mediapm-utils --features progress -- --test-threads=8`.
- Stale-artifact gotcha: `passthrough_conductor_tool_run_help_is_routable` in `src/mediapm/src/main.rs` can fail from stale incremental artifacts; `cargo clean -p mediapm -p mediapm-conductor`.
- Before completion, run `cargo run --package mediapm --example mediapm_demo` and `mediapm_demo_online`. For `src/mediapm/**` edits, avoid full online demo runs during normal development; reserve for push/pre-push unless requested. If source or configs are incomplete, report gaps explicitly.

## Hooks and CI parity

- **prek stages.** pre-commit: conflict/private-key/whitespace checks, `rumdl-fmt`, `fmt` on changed `.rs`. commit-msg: `commitlint` (Conventional Commits, self-provisioned). pre-push: `cargo-check --workspace --all-targets --all-features`, `clippy --workspace --all-targets --all-features`, `test` (`test-runner test --all-features --locked`: nextest, unprefixed-tempdir gate, janitor dry-run gate), `test docs` (`test-runner doc --all-features --locked`: doctests, rustdoc link gate). No `rustdoc` hook id; use `SKIP=test-docs`.
- **Commitlint.** The commitlint workflow lints the push range only, so a scope-less subject in an older commit cannot re-block a later push. `.commitlintrc.mjs` is the single config; CI passes it explicitly (`configFile: .commitlintrc.mjs` in `.github/workflows/commitlint.yml`), and no second config file exists to shadow it.
- **CI parity.** `.github/workflows/ci.yml` mirrors pre-push and extends it. `build` job: a `ubuntu-latest` and a `windows-latest` leg (the Windows leg is where `cfg(windows)` code compiles and runs natively; `fail-fast: false` keeps one leg's break from cancelling the other), cheap checks (`rumdl`, `fmt-check`, `clippy-all`), required setup, then `test-all`, `test-doc-all`, `build-all`. `feature-matrix` derives crate/feature combinations from `cargo metadata` (`test-runner feature-matrix`); it and `build-all` are CI-only, not pre-push. `release` builds `mediapm-cas`, `mediapm-conductor`, `mediapm` with `--release` and exercises completion rendering. `online` workflow is manual dispatch against real hosts. `cargo platform` lints foreign-target code locally (`test-runner platform`): `x86_64-unknown-linux-gnu`, `x86_64-apple-darwin`, and `x86_64-pc-windows-msvc`, each with an explicit package list, because `ring`/`blake3` C build scripts block cross builds for five packages. The three targets are declared in `rust-toolchain.toml`, so `rustup` and CI install them with no separate command. Run it with `cargo clippy-all` before pushing; the native `windows-latest` and `ubuntu-latest` legs still cover the excluded packages.
- **Hook recovery.** `fmt` and `rumdl-fmt` may auto-fix and fail the commit. Verify HEAD unmoved (`git rev-parse HEAD`), `git add` modified files, retry with a fresh `git commit`, never `--amend`. prek stashes unstaged changes around hooks and restores them.

## Nextest caveats

- No doctest support; pair nextest with doctests via `test-doc-all`.
- Example `main()` is not executed by nextest; embedded example tests cover it.
- `#[should_panic]` can hit `slow-timeout`; adjust `.config/nextest.toml` only when needed.
- Leak detection is real. cargo-nextest is pinned in `Cargo.toml` `[workspace.metadata.bin]`; do not tune `leak-timeout` away, do not set `result = "fail"`, and do not add broad per-test overrides.
- No `--nocapture` by default; use `cargo nextest run --show-output`.
- Network-heavy tests are serialized (`test-groups network`, `max-threads = 1`, longer `slow-timeout`) as ordering insurance. The group is hermetic today; real-upstream runs are manual/online workflow only. Keep filter/budget aligned with `.config/nextest.toml`.

## Editing conventions

- Keep changes minimal, deterministic, and aligned with the functional-core direction. Keep planner pure and deterministic; keep side effects in boundary modules.
- Keep dependency/feature surfaces explicit. Prefer existing workspace deps, remove unused direct deps, gate optional behavior behind Cargo features, avoid default-feature fan-out.
- Avoid hidden mutable state or databases unless explicitly requested. Preserve managed-file record and migration-provenance semantics. Keep object-store and state-document writes atomic.
- Keep stack-specific detail in focused instructions, not root `AGENTS.md`. Verify real workspace shape before changing workspace-wide guidance.
- CLI/API parity: new CLI operations route through library/API entry points; programmatic APIs keep the same validation/failure semantics as CLI paths. Capability gaps between CLI and API are not acceptable. See `AGENTS.md`.

## Module split and naming

- When a module outgrows one file, move `foo.rs` to `foo/mod.rs`, put submodules in `foo/<submodule>.rs`, keep unit tests inline as `#[cfg(test)]`, and split a themed `foo_<theme>.rs` sibling only past about 300 lines. Prefer `mod tests;`, never keep both `foo.rs` and `foo/mod.rs`, and keep `//!` docs on `foo/mod.rs`. After a split, run `cargo test -p <crate> <focused_test_name>` and `cargo build-pkg <crate>`.
- All workspace examples use crate-name prefix: `<crate_name>_<example_name>.rs`. Invoke with the full target name, for example `cargo run --package mediapm --example mediapm_demo_online`.

## Docstrings and lint suppression

- Add module-level `//!` docs and item-level `///` docs for touched Rust code, including private items in touched files. Explain purpose, invariants, side effects, error behavior, and performance notes when relevant. Do not restate item names. Missing docs on touched helpers/constants/types are regressions.
- Do not add bare `#[allow(...)]` or `#![allow(...)]` for rustc/clippy. Fix the lint first. If suppression is unavoidable, use narrow item-scoped `#[expect(<lint>, reason = "<why required now>")]`; keep scope minimal, give a concrete reason, treat as debt, and investigate `unfulfilled_lint_expectations`.

## Test expectations

- Tests are executable specifications. Cover planner determinism, sync idempotency, record/object integrity, and GC safety where relevant.
- Workspace-crate integration tests use `tests/mod.rs` plus grouped `tests/e2e/`, `tests/int/`, `tests/prop/` modules.
- Keep tests deterministic, isolated, and hermetic. Use `mediapm_utils::temp::output_dir()`/`cache_dir()` or scoped helpers; never write fixtures into source; never use real OS user cache in tests. Use `cache_root_override`/`MEDIAPM_EXAMPLE_CACHE_ROOT` isolation and hold process env locks as required by `temp-and-paths.instructions.md`.
- Prefer explicit arrange/act/assert. Use `assert_eq!(actual, expected)` with clear messages. Normalize path separators for platform-safe assertions. Avoid nondeterministic collection ordering in assertions; use ordered collections or sort.
- Behavior changes need atomic test updates. Demo behavior changes require verifying/updating `mediapm_demo` and `mediapm_demo_online` as applicable. Every example has an embedded test that exercises `main()`.
- Terminal-rendered tests use exact `assert_eq!(term.contents(), concat!(...))` for deterministic rendered output. Substring/count-only assertions are acceptable only for non-deterministic timing, resize-dependent content where dimension is under test, or a preliminary check followed by an exact assertion.
- For async tests use `tokio::test`; prefer `start_paused = true` for timeouts/intervals and await spawned handles. Control ambient credentials/env explicitly in tests that depend on absence.

## Pointers

- `.config/nextest.toml`, `prek.toml`, `.github/workflows/ci.yml`, `.cargo/config.toml`
- `AGENTS.md`, `rust-workflow.instructions.md` (this file), `temp-and-paths.instructions.md`, `examples-and-demos.instructions.md`, `sdd-tdd.instructions.md`
- Code: `scripts/test-runner/src/gates/`, `src/mediapm/src/example_isolation.rs`, `src/mediapm-utils/src/temp.rs`
