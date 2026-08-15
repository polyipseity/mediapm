---
description: "Use when editing CI workflow files, prek.toml, or nextest configuration. Covers validation gates, CI parity, git hooks/pre-commit, and known nextest caveats."
name: "CI and Validation Workflow"
applyTo: ".github/workflows/**/*.yml, .github/workflows/**/*.yaml, prek.toml, .config/nextest.toml"
---

# CI and validation workflow

## Validation gates

Local git hooks (configured via `prek.toml`) enforce code quality at three stages:

- **pre-commit stage** (on `git commit`): `check-case-conflict`, `check-executables-have-shebangs`, `check-illegal-windows-names`, `check-merge-conflict`, `check-shebang-scripts-are-executable`, `check-symlinks`, `destroyed-symlinks`, `detect-private-key`, `end-of-file-fixer`, `fix-byte-order-marker`, `name-tests-test`, `trailing-whitespace`, `rumdl-fmt`, and `fmt` (cargo fmt on changed `.rs` files).
- **commit-msg stage**: runs `commitlint` (Conventional Commits enforcement via `@commitlint/config-conventional`). commitlint is self-provisioned by the pre-commit hook (`additional_dependencies`) and the CI action; no repo-level `package.json` is used.
- **pre-push stage** (on `git push`): `cargo-check --workspace --all-targets --all-features`, `clippy --workspace --all-targets --all-features`, `test docs` (doctests via `cargo test --doc --workspace`), and `test` (nextest: `cargo-nextest run --workspace --all-targets --all-features`).

Treat these hooks as the canonical lint/format/check gate. During normal development, prefer selective test/build runs and rely on commit/push hooks for full validation.

## CI parity

GitHub Actions (`.github/workflows/ci.yml`) mirrors the pre-push gate set:

- `scripts/run-all-tests.sh` runs nextest (`cargo-nextest run --workspace --all-targets --all-features`) followed by `cargo test --doc --workspace` for doctests.
- `cargo clippy-all`, `cargo fmt-check`, `cargo build-all`.
- `cargo bin rumdl check` (project-specific markdown linting).
- A separate `windows` job (windows-latest) runs ONLY `cargo --locked test-pkg mediapm-tests` (the root script-test crate), covering the script self-tests (`tests/scripts/test-clean-mediapm-temp.*` janitors, `tests/scripts/test-run-all-tests.*` runners). pwsh is preinstalled on the runner; bash-based tests run via the Git Bash `bash` probe and skip with a printed reason if absent. No full-suite parity, no `run-all-tests.ps1`, no extra gates on Windows.

CI triggers on pull requests and pushes to any branch. Uses concurrency deduplication so only the latest run per PR/commit executes.

## Explicit forms

When cargo aliases are unavailable, use explicit equivalents:

- `cargo test -p <crate> --all-targets --all-features` → same as `cargo test-pkg <crate>`.
- `cargo clippy -p <crate> --all-targets --all-features` → same as `cargo clippy-pkg <crate>`.

## Hook management

Install or update hooks: `pre-commit install`. Run all hooks manually: `pre-commit run --all-files`. Run a specific hook: `pre-commit run <hook-name>` (e.g., `pre-commit run test` for nextest, `pre-commit run test-docs` for doctests). Skip hooks temporarily: `SKIP=test git commit -m "msg"` or `SKIP=test-docs git commit -m "msg"`.

## Hook failure recovery

- The `fmt` (rustfmt) and `rumdl-fmt` hooks auto-fix changed files and **fail the commit** when a fix was applied (exit 1, "files were modified by this hook"). Recovery: verify HEAD unmoved (`git rev-parse HEAD`), `git add` the modified files, and retry with a fresh `git commit` — never `--amend`.
- `rumdl-fmt` enforces list indentation (MD007/MD032) and flattens nested lists indented otherwise. Restore hierarchy with 3-space bullets and 5-space sub-bullets — the style `commit-staged.prompt.md` uses, which the hook leaves untouched.
- prek stashes unstaged working-tree changes to `~/.cache/prek/patches/*.patch` around each hook run and restores them afterward, so working tree state is preserved across commits.

## Known nextest caveats

1. **No doctest support.** Nextest does not run doctests. Always pair nextest with `cargo test --doc --workspace` — this is why `scripts/run-all-tests.sh` and pre-push hooks include a separate doctest step.
2. **Example `main()` is not executed.** Nextest compiles examples and runs their embedded `#[cfg(test)]` modules when invoked with `--all-targets` (as `test-all` does), but it never executes an example's `main()` on its own. Example `main()` execution is the responsibility of embedded tests per `example-execution-policy.instructions.md`.
3. **`#[should_panic]` tests may timeout.** Nextest applies a per-test timeout (configured via `slow-timeout` in `.config/nextest.toml`). A `#[should_panic]` test that deadlocks or loops infinitely will be killed by the timeout rather than hanging indefinitely. Adjust `slow-timeout` if needed.
4. **Leak detection is experimental.** The `leak-timeout` setting in `.config/nextest.toml` warns on unresolved child processes. Can produce false positives for tests holding OS resources (file descriptors, sockets). Disable globally or per-test if it causes CI flakiness.
5. **No `--nocapture` by default.** Nextest captures stdout/stderr per test and displays it grouped by pass/fail. To see live output, use `cargo nextest run --show-output`. The `test-all` alias does not pass `--show-output`; use `cargo bin cargo-nextest run --show-output` for debugging.
6. **Network-heavy tests are serialized with a long timeout.** Demo example tests and the `all_platform` integration tests each download the full managed-tool set (ffmpeg ~163M, etc.) into a fresh empty cache; 3-7 running in parallel saturate bandwidth and trip the default slow-timeout. They are grouped via `[test-groups] network = { max-threads = 1 }` with `slow-timeout = { period = "60s", terminate-after = 600 }` in both `default` and `ci` profiles; the group filter is `binary(mediapm_demo) or binary(mediapm_demo_online) or test(all_platform)` — `binary()` exact-matches example binary names, `test()` contains-matches integration tests (see `.config/nextest.toml`). Fast non-network tests in those binaries simply join the group. Do not remove this override without addressing the download-contention flake. Watchdog sync invariant: `terminate-after = 600` must stay equal to `DEMO_ONLINE_HARD_TIMEOUT_TOTAL_SECS` in `src/mediapm/examples/mediapm_demo_online.rs` — update both together. Demos remain network-dependent: YouTube intermittently returns HTTP 403 to yt-dlp ("No supported JavaScript runtime" — EJS deprecation), a transient anti-bot flake rather than a code regression — re-run the failing demo test in isolation to confirm.
