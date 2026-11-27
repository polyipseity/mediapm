---
description: "Use for Rust, Cargo/config, CI, prek, nextest, tests and lint."
name: "Rust Workflow and Conventions"
applyTo: "**/*.rs, Cargo.toml, Cargo.lock, rust-toolchain.toml, .rustfmt.toml, .clippy.toml, .config/**/*.toml, .cargo/**/*.toml, prek.toml, .github/workflows/**/*.yml, .github/workflows/**/*.yaml"
---

# Rust workflow and conventions

## Source of truth

`Cargo.toml`, `.cargo/config.toml`, `.config/nextest.toml`, `rust-toolchain.toml`, `rustfmt.toml`/`clippy.toml`, `.github/workflows/ci.yml`, `prek.toml`; contract in `AGENTS.md` + `.agents/instructions/*.instructions.md`. Members: `src/mediapm-cas/`, `src/mediapm-conductor/`, `src/mediapm-conductor-builtins/*/`, `src/mediapm/`.

## Validation workflow

Fast inner loop during edits; full/parity gates are checkpoints.

- Inner loop: narrowest check — `cargo test -p <crate> <test_name>`, `cargo test-pkg <crate>`, `cargo clippy-pkg <crate>`, `cargo build-pkg <crate>`, targeted `cargo check`/`cargo fmt-check`/`cargo bin rumdl check`.
- Checkpoints: `cargo fmt-check`, `cargo bin rumdl check`, `cargo clippy-all`, `cargo test-all`, `cargo test-doc-all`, `cargo build-all`/feature-matrix. `test-all`/`test-doc-all` via `test-runner`; `test-doc-all` = doctests + rustdoc link gate. Pre-push: `cargo-check`, `clippy`, `test-runner test`, `test-runner doc`.
- Disk: <1% free/out-of-space -> `cargo clean` here only; still <2% -> clear this repo's `examples/outputs/` only; still <2% -> stop. Nothing else.
- `-D warnings` in CI and locally (`.cargo/config.toml` `[build] rustflags = ["-Dwarnings"]`); fix root cause, no bare suppression.
- libtest args after `--`: `cargo test -p mediapm-utils --features progress -- --test-threads=8`.
- Stale artifact: `passthrough_conductor_tool_run_help_is_routable` in `src/mediapm/src/main.rs`; `cargo clean -p mediapm -p mediapm-conductor`.
- Before completion run `cargo run --package mediapm --example mediapm_demo` and `mediapm_demo_online`; full online runs for `src/mediapm/**` only at push/pre-push unless requested. Report source/config gaps.

## Hooks and CI parity

- prek: pre-commit conflict/private-key/whitespace, `rumdl-fmt`, `fmt` on changed `.rs`; commit-msg `commitlint`; pre-push `cargo-check --workspace --all-targets --all-features`, `clippy --workspace --all-targets --all-features`, `test` (`test-runner test --all-features --locked`: nextest, unprefixed-tempdir, janitor), `test docs` (`test-runner doc --all-features --locked`: doctests, rustdoc links). No `rustdoc` hook id; use `SKIP=test-docs`.
- Commitlint lints push range only; `.commitlintrc.mjs` single config passed by CI (`configFile: .commitlintrc.mjs`, `.github/workflows/commitlint.yml`), no shadow.
- `.github/workflows/ci.yml` mirrors pre-push: `build` on `ubuntu-latest`/`windows-latest` (`cfg(windows)` native, `fail-fast: false`), cheap checks (`rumdl`, `fmt-check`, `clippy-all`), `test-all`, `test-doc-all`, `build-all`. `feature-matrix` (`test-runner feature-matrix`, from `cargo metadata`) and `build-all` CI-only. `release` builds `mediapm-cas`, `mediapm-conductor`, `mediapm` with `--release` + completion; `online` manual. `cargo platform` (`test-runner platform`) lints `x86_64-unknown-linux-gnu`, `x86_64-apple-darwin`, `x86_64-pc-windows-msvc` with explicit lists (`ring`/`blake3` C scripts block cross builds for five packages); targets in `rust-toolchain.toml`, installed by `rustup`/CI; run before pushing with `cargo clippy-all`, native legs cover excluded packages. GitHub-side settings (branch protection, required checks, reviews) out of scope: never propose as work, owner action, or risk. Workflow change ends at the workflow file.
- Hook recovery: `fmt`/`rumdl-fmt` auto-fix may fail commit; verify HEAD (`git rev-parse HEAD`), `git add` modified files, fresh `git commit`, never `--amend`; prek restores stashed changes.

## Nextest caveats

No doctests (`test-doc-all`); example `main()` not executed (embedded tests cover it). `#[should_panic]` can hit `slow-timeout`; edit `.config/nextest.toml` only when needed. Leak detection real; cargo-nextest pinned in `Cargo.toml` `[workspace.metadata.bin]`; no `leak-timeout` tuning, `result = "fail"`, or broad overrides. No `--nocapture` by default; use `cargo nextest run --show-output`. Network tests serialized (`test-groups network`, `max-threads = 1`, longer `slow-timeout`); real upstream only manual/online.

## Editing conventions

- Minimal deterministic changes; pure deterministic planners, side effects at boundaries.
- Explicit deps/features: workspace deps first, remove unused direct deps, features gate optional behavior, avoid default-feature fan-out.
- No hidden mutable state/databases unless requested; preserve managed-file/migration provenance; atomic object/state writes.
- Stack detail in focused instructions, not root `AGENTS.md`; verify shape before changing workspace-wide guidance.
- CLI/API parity via library/API entry points and identical validation/failure semantics; no capability gaps (`AGENTS.md`).

## Module split and naming

Outgrown module: `foo.rs` -> `foo/mod.rs`, submodules in `foo/<submodule>.rs`, unit tests inline `#[cfg(test)]`, themed `foo_<theme>.rs` only past ~300 lines. Prefer `mod tests;`; never both `foo.rs` and `foo/mod.rs`; `//!` docs on `foo/mod.rs`. After split: `cargo test -p <crate> <focused_test_name>`, `cargo build-pkg <crate>`. Examples use `<crate_name>_<example_name>.rs`, invoked by full target name (e.g. `cargo run --package mediapm --example mediapm_demo_online`).

## Docstrings and lint suppression

Module `//!`/item `///` docs on touched Rust code, including private items: purpose, invariants, side effects, errors, performance when relevant; no restating names. Missing docs on touched helpers/constants/types are regressions. No bare `#[allow(...)]`/`#![allow(...)]`: fix lint; if unavoidable use narrow `#[expect(<lint>, reason = "<why required now>")]` with concrete reason, treat as debt, investigate `unfulfilled_lint_expectations`.

## Test expectations

- Tests are executable specs: planner determinism, sync idempotency, record/object integrity, GC safety. Integration layout `tests/mod.rs` + `tests/e2e/`, `tests/int/`, `tests/prop/`; deterministic hermetic tests via `mediapm_utils::temp::output_dir()`/`cache_dir()`, `cache_root_override`/`MEDIAPM_EXAMPLE_CACHE_ROOT`, env locks (`temp-and-paths.instructions.md`); no fixtures in source or real OS cache. Arrange/act/assert, `assert_eq!(actual, expected)`, normalized separators, ordered collections.
- Repository text reads compare line-ending-insensitively: `.gitattributes` pins `eol` for markdown/shell only, so JSON/TOML/Nickel carry host endings; use `mediapm_test_support::normalize::normalize_line_endings` both sides, CRLF via `crlf_checkout_of`, never replacing `\n` in possibly-CRLF files. Behavior changes update tests and demos atomically; every example has embedded `main()` test.
- Terminal-rendered tests: exact `assert_eq!(term.contents(), concat!(...))`; substring/count only for nondeterministic timing, resize-dependent dimension, or preliminary check followed by exact assertion.
- Async: `tokio::test`, `start_paused = true` for timeouts/intervals, await spawned handles; control ambient credentials/env explicitly.

## Pointers

`.config/nextest.toml`, `prek.toml`, `.github/workflows/ci.yml`, `.cargo/config.toml`, `AGENTS.md`, `temp-and-paths.instructions.md`, `examples-and-demos.instructions.md`, `sdd-tdd.instructions.md`, `scripts/test-runner/src/gates/`, `src/mediapm/src/example_isolation.rs`, `src/mediapm-utils/src/temp.rs`.
