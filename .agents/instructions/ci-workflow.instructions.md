---
description: "Use when editing CI workflow files, prek.toml, or nextest configuration. Covers validation gates, CI parity, git hooks/pre-commit, and known nextest caveats."
name: "CI and Validation Workflow"
applyTo: ".github/workflows/**/*.yml, .github/workflows/**/*.yaml, prek.toml, .config/nextest.toml"
---

# CI and validation workflow

## Validation gates

Local git hooks (via `prek.toml`) enforce quality at three stages:

- **pre-commit** (`git commit`): `check-case-conflict`, `check-executables-have-shebangs`, `check-illegal-windows-names`, `check-merge-conflict`, `check-shebang-scripts-are-executable`, `check-symlinks`, `destroyed-symlinks`, `detect-private-key`, `end-of-file-fixer`, `fix-byte-order-marker`, `name-tests-test`, `trailing-whitespace`, `rumdl-fmt`, and `fmt` (cargo fmt on changed `.rs`).
- **commit-msg**: `commitlint` (Conventional Commits via `@commitlint/config-conventional`), self-provisioned by the hook and CI action — no repo `package.json`.
- **pre-push** (`git push`): `cargo-check --workspace --all-targets --all-features`, `clippy --workspace --all-targets --all-features`, `test` (`cargo run --package test-runner -- test --all-features --locked`: nextest plus the tempdir and janitor gates), and `test docs` (`cargo run --package test-runner -- doc --all-features --locked`: doctests plus the rustdoc link gate). Both test hooks are local hooks that call the `test-runner` crate, so the pre-push gate is the CI gate rather than an approximation of it — including the temp-directory gates, which pre-push did not run before the runner existed. The separate `rustdoc` hook id is gone; its work is the second half of `test docs`, so `SKIP=test-docs` is the handle for it.

Treat these as the canonical lint/format/check gate; use selective runs during development.

## CI parity

GitHub Actions (`.github/workflows/ci.yml`) mirrors the pre-push gate:

- The `Run tests` step is `cargo --locked test-all` and `Run doctests and rustdoc` is `cargo --locked test-doc-all`. Both are thin aliases over the `test-runner` crate, so the suite runs under `--all-features` — which already activates the opt-in `large-tests` feature (`mediapm-cas`), so there is no separate large-tests step and no `--large` flag. The runner's `test` subcommand runs nextest, then the unprefixed-tempdir gate, then the janitor dry-run gate; its `doc` subcommand runs the doctests, then the rustdoc link gate.
- The online demo YouTube regression (`online_sync_post_sync_dump`) is gated by the **3-level mechanism** (`example_isolation::ci_mode_detected()` skip + `MEDIAPM_RUN_ONLINE_SYNC=1` opt-in), not by feature selection. The two are orthogonal: `--all-features` activates `large-tests` and still does not run the YouTube test.
- `cargo clippy-all`, `cargo fmt-check`, `cargo build-all`.
- `cargo bin rumdl check` (project markdown lint).
- The `feature-matrix` job runs `cargo --locked run --package test-runner -- feature-matrix --locked`: one `cargo check` per crate per feature combination, with the combinations derived from `cargo metadata` by the runner rather than listed in YAML. A hand-maintained list is a list that silently rots when a crate gains a feature; the three gotchas the old inline list needed documented (`--no-default-features` for `mediapm-utils`, `required-features = ["cli"]` bin targets, and the deliberate absence of `--all-targets`) are properties of the derivation in `scripts/test-runner/src/matrix.rs`.
- A `windows` job (windows-latest) runs ONLY `cargo --locked test-pkg mediapm-tests` (root script-test crate): the janitor tests (`tests/scripts/test-clean-mediapm-temp.{sh,ps1}` and the dry-run/real-run drivers around them). pwsh is preinstalled; bash tests run via Git Bash `bash` probe and skip if absent. It does not run the `test-runner` crate, so the temp-directory gates and the doctest/rustdoc gate are not exercised on Windows. No full-suite parity and no extra gates.

CI triggers on PRs and pushes to any branch, with concurrency deduplication (latest run per PR/commit).

## Explicit forms

When cargo aliases are unavailable:

- `cargo test -p <crate> --all-targets --all-features` = `cargo test-pkg <crate>`
- `cargo clippy -p <crate> --all-targets --all-features` = `cargo clippy-pkg <crate>`

## Hook management

Install/update: `pre-commit install`. Run all: `pre-commit run --all-files`. One hook: `pre-commit run <hook-name>` (`test` for the suite plus its gates, `test-docs` for doctests plus rustdoc). Skip: `SKIP=test git commit -m "msg"` / `SKIP=test-docs git commit -m "msg"`. There is no `rustdoc` hook id to skip; `SKIP=rustdoc` matches nothing.

## Hook failure recovery

- `fmt` (rustfmt) and `rumdl-fmt` auto-fix changed files and **fail the commit** when a fix was applied (exit 1, "files were modified by this hook"). Recovery: verify HEAD unmoved (`git rev-parse HEAD`), `git add` the modified files, retry with a fresh `git commit` — never `--amend`.
- `rumdl-fmt` enforces list indentation (MD007/MD032) and flattens nested lists. Restore hierarchy with 3-space bullets and 5-space sub-bullets (the `commit-staged.prompt.md` style, which the hook leaves untouched).
- prek stashes unstaged changes to `~/.cache/prek/patches/*.patch` around each hook and restores them after, so working-tree state is preserved.

## Known nextest caveats

1. **No doctest support.** Pair nextest with `cargo test --doc --workspace` (why `cargo test-all` and pre-push's `test-docs` hook are a second command rather than one).
2. **Example `main()` is not executed.** Nextest compiles examples and runs embedded `#[cfg(test)]` modules with `--all-targets` but never runs an example's `main()`; that is the embedded tests' job (see `example-execution-policy.instructions.md`).
3. **`#[should_panic]` tests may timeout.** Nextest applies a per-test timeout (`slow-timeout` in `.config/nextest.toml`); a deadlocking `#[should_panic]` is killed by the timeout. Adjust `slow-timeout` if needed.
4. **Leak detection is experimental.** `leak-timeout` warns on unresolved child processes; false positives for tests holding OS resources. Disable globally or per-test if flaky.
5. **No `--nocapture` by default.** Use `cargo nextest run --show-output` for live output; `test-all` does not pass it.
6. **Network-heavy tests are serialized with a long timeout.** The `mediapm_demo_online` example tests and `all_platform` integration tests each download the full managed-tool set (ffmpeg ~163M) into a fresh empty cache; parallel runs saturate bandwidth and trip the default slow-timeout. The offline `mediapm_demo` downloads nothing, so it is in the group only to stay serialized with its network-using siblings. They use `[test-groups] network = { max-threads = 1 }` with `slow-timeout = { period = "60s", terminate-after = 600 }` in both `default` and `ci` profiles; filter is `binary(mediapm_demo) or binary(mediapm_demo_online) or test(all_platform)` (`binary()` exact-matches example binary names, `test()` contains-matches integration tests — see `.config/nextest.toml`). Fast non-network tests in those binaries join the group. Do not remove this override without fixing the download-contention flake. `terminate-after = 600` must equal `DEMO_ONLINE_HARD_TIMEOUT_TOTAL_SECS` in `src/mediapm/examples/mediapm_demo_online.rs` — update both together. YouTube intermittently returns HTTP 403 to yt-dlp ("No supported JavaScript runtime" — EJS deprecation), a transient anti-bot flake, not a code regression — re-run the failing demo test in isolation to confirm.
