---
description: "Use when editing CI workflow files, prek.toml, or nextest configuration. Covers validation gates, CI parity, git hooks/pre-commit, and known nextest caveats."
name: "CI and Validation Workflow"
applyTo: ".github/workflows/**/*.yml, .github/workflows/**/*.yaml, prek.toml, .config/nextest.toml"
---

# CI and validation workflow

## Validation gates

Local git hooks (configured via `prek.toml`) enforce code quality at three stages:

- **pre-commit stage** (on `git commit`): `check-case-conflict`, `check-executables-have-shebangs`, `check-illegal-windows-names`, `check-merge-conflict`, `check-shebang-scripts-are-executable`, `check-symlinks`, `destroyed-symlinks`, `detect-private-key`, `end-of-file-fixer`, `fix-byte-order-marker`, `name-tests-test`, `trailing-whitespace`, `rumdl-fmt`, and `fmt` (cargo fmt on changed `.rs` files).
- **commit-msg stage**: runs `commitlint` (Conventional Commits enforcement via `@commitlint/config-conventional`).
- **pre-push stage** (on `git push`): `cargo-check --workspace --all-targets --all-features`, `clippy --workspace --all-targets --all-features`, `test docs` (doctests via `cargo test --doc --workspace`), and `test` (nextest: `cargo-nextest run --workspace --all-targets --all-features`).

Treat these hooks as the canonical lint/format/check gate. During normal development, prefer selective test/build runs and rely on commit/push hooks for full validation.

## CI parity

GitHub Actions (`.github/workflows/ci.yml`) mirrors the pre-push gate set:

- `scripts/run-all-tests.sh` runs nextest (`cargo-nextest run --workspace --all-targets --all-features`) followed by `cargo test --doc --workspace` for doctests.
- `cargo clippy-all`, `cargo fmt-check`, `cargo build-all`.
- `cargo bin rumdl check` (project-specific markdown linting).

CI triggers on pull requests and pushes to any branch. Uses concurrency deduplication so only the latest run per PR/commit executes.

## Explicit forms

When cargo aliases are unavailable, use explicit equivalents:

- `cargo test -p <crate> --all-targets --all-features` → same as `cargo test-pkg <crate>`.
- `cargo clippy -p <crate> --all-targets --all-features` → same as `cargo clippy-pkg <crate>`.

## Hook management

Install or update hooks: `pre-commit install`. Run all hooks manually: `pre-commit run --all-files`. Run a specific hook: `pre-commit run <hook-name>` (e.g., `pre-commit run test` for nextest, `pre-commit run test-docs` for doctests). Skip hooks temporarily: `SKIP=test git commit -m "msg"` or `SKIP=test-docs git commit -m "msg"`.

## Known nextest caveats

1. **No doctest support.** Nextest does not run doctests. Always pair nextest with `cargo test --doc --workspace` — this is why `scripts/run-all-tests.sh` and pre-push hooks include a separate doctest step.
2. **Binary/test executable detection only.** Nextest only discovers binary and test crate targets. It does not run examples or benchmarks. Use `cargo build --examples` or `cargo bench` separately.
3. **`#[should_panic]` tests may timeout.** Nextest applies a per-test timeout (configured via `slow-timeout` in `.config/nextest.toml`). A `#[should_panic]` test that deadlocks or loops infinitely will be killed by the timeout rather than hanging indefinitely. Adjust `slow-timeout` if needed.
4. **Leak detection is experimental.** The `leak-timeout` setting in `.config/nextest.toml` warns on unresolved child processes. Can produce false positives for tests holding OS resources (file descriptors, sockets). Disable globally or per-test if it causes CI flakiness.
5. **No `--nocapture` by default.** Nextest captures stdout/stderr per test and displays it grouped by pass/fail. To see live output, use `cargo nextest run --show-output`. The `test-all` alias does not pass `--show-output`; use `cargo bin cargo-nextest run --show-output` for debugging.
