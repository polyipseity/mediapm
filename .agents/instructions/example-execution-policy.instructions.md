---
description: "Use when authoring or editing example files or example-execution tests. Covers the policy that every example main must be exercised by an embedded test, and that CI detection for nondeterministic examples lives in the test that calls main, never inside main itself."
name: "Example Execution Policy"
applyTo: "src/**/examples/**/*.rs"
---

# Example execution policy

## Every example main is exercised by an embedded test

Each example file under `src/*/examples/` must contain at least one `#[cfg(test)]` test that executes the example's `main()` via `super::main()`. This guarantees the documented example entry point is compiled and runnable, not just its helper functions. Tests that call a `#[tokio::main]`-wrapped `main()` must be plain `#[test]` functions, because `#[tokio::main]` expands to a synchronous `fn main()` that creates and drives its own runtime; a `#[tokio::test]` wrapper would create a nested runtime and panic.

Nextest (via `cargo test-all` / `--all-targets`) compiles examples and runs their embedded `#[cfg(test)]` modules; it never executes an example `main()` on its own.

## Deterministic examples always run their full path; nondeterministic examples follow a three-level run model

Examples that require nondeterministic access (network, external services, managed-tool downloads) must detect CI in the test that calls `main()`, never inside `main()` itself. The three-level run model below applies to all examples that make use of the user-level tool download cache (`mediapm_cli_add_tools`, `mediapm_cli_add_hierarchy`, `mediapm_demo`, `mediapm_demo_online`), not only the online demo; deterministic examples such as the offline demo still run their full path in tests with no CI detection. The policy:

1. **Deterministic examples** (e.g. the offline demo) always run their full path in tests — no reduced mode, no CI detection.
2. **Nondeterministic examples** (e.g. the online demo) follow a three-level run model:
   - **Level 1 — CI test run** (`cargo test` with any of `CI`, `GITHUB_ACTIONS`, `GITLAB_CI`, `CIRCLECI`, `TRAVIS`, `BUILDKITE`, `DRONE` set): the embedded test skips with a documented message via `ci_mode_detected()` — the full path is nondeterministic and must not run in CI.
   - **Level 2 — test harness outside CI** (`cargo test` / `cargo test-all` / pre-push hooks, no CI env): the test runs `main()` in a deterministic reduced mode (e.g. config-only) selected by the documented environment variables, never touching the network.
   - **Level 3 — explicit run** (`cargo run --example <name>` or executing the built example binary): no test harness exists (`cfg(test)` is not compiled), so the full path runs — the only execution level that exercises network and external tools. With the env overrides unset, cache-using examples resolve the real persistent user-level tool download cache via `example_isolation::user_level_cache_root()` (`<os-cache-dir>/mediapm/cache`), so downloaded tools persist across runs.

`main()` itself must be deterministic given environment inputs: no CI detection, no network probing, and no conditional behavior other than what the documented environment variables select.

## The 3-level mechanism also gates non-example regression tests

The three-level model is not limited to example `main()` tests. Any non-deterministic test that performs real network downloads (not just example entry points) must apply the same gating so it never runs in CI or in the normal harness by accident. The canonical example is `src/mediapm/tests/int/online_sync_post_sync_dump.rs`, a YouTube-download regression test:

- **Level 1 — CI skip:** the test calls `ci_mode_detected()` (same CI-env set as above) and returns early with a documented message.
- **Level 2 — harness skip:** absent an explicit opt-in, the test returns early — the normal `cargo test` / `cargo test-all` / pre-push harness never performs the download.
- **Level 3 — explicit opt-in:** set `MEDIAPM_RUN_ONLINE_SYNC=1` (tokens `1|true|yes|on`) to run the full download and assertions.

This gate is **orthogonal** to the `large-tests` Cargo feature: enabling `--large` (or `--all-features`) does not run the YouTube test, and the test does not require the feature. Keep the two mechanisms separate — do not conflate feature gating with the 3-level mechanism. The single shared env var `MEDIAPM_RUN_ONLINE_SYNC` (defined as `example_isolation::RUN_ONLINE_SYNC_ENV`) is the only Level 3 gate: the online demo `main()` uses it as a disable toggle (unset/enabled = full sync; disabled = reduced mode), and the non-example regression test uses it as an enable toggle (unset/disabled/unknown = skip). The offline demo (`mediapm_demo`) has no Level 3 env var and always runs a full sync.

## Examples-as-tests must be isolated

Examples compile into test binaries (nextest `--all-targets`), so any example `main()` that persists state runs concurrently with sibling tests in the same suite. It must therefore treat itself as a test: never share canonical on-disk locations between tests and never touch the real OS user cache.

See **`example-temp-isolation.instructions.md`** for the full temp-directory model (directory classes, lifecycle, conductor sandbox teardown, parallelism constraints, and janitor script). Summary:

Two env-var overrides exist for this, set by tests through the shared [`IsolatedExampleRoots`](../../src/mediapm/src/example_isolation.rs) guard (`mediapm::example_isolation`):

- `MEDIAPM_EXAMPLE_ARTIFACT_ROOT` — artifact root (the workspace dir the example mutates). Tests set it to a unique tempdir. Never share the canonical `examples/artifacts/<name>` dir between tests in the same suite (CAS `store/lock` flock races otherwise).
- `MEDIAPM_EXAMPLE_CACHE_ROOT` — user-level tool download cache root. Tests set it to a unique tempdir and map it to `MediaRuntimeStorage.cache_root_override`, so `sync_tools()`/tool provisioning never touches the real OS cache (`default_mediapm_user_download_cache_root()`). When unset (explicit `cargo run --example`), the example resolves the real user-level tool download cache via `example_isolation::user_level_cache_root()` (`<os-cache-dir>/mediapm/cache`), so explicit runs share the cache with regular mediapm syncs and persist downloads across runs; tests always set the env var to a unique tempdir via `IsolatedExampleRoots::with_cache`, keeping the real cache untouched. `example_isolation::uses_isolated_cache_root()` lets examples and test code detect whether the hermetic isolated cache is active.

When canonical artifact roots are locked (Windows share violations), examples may call `example_isolation::isolated_artifact_dir()` and keep the returned `TempDir` alive for the rest of the test or `main()` scope.

To remove stale mediapm temp trees manually, run `scripts/clean-mediapm-temp.sh` (POSIX) or `scripts/clean-mediapm-temp.ps1` (Windows) — both sweep `mediapm-artifact-*`, `mediapm-cache-*`, and `mediapm-runtime-*` at depth 1 under the OS temp dir, and nothing else. See `temp-directory-spec.instructions.md` (Janitor contract).

`main()` must honor these env vars when set and fall back to canonical paths when unset, so manual `cargo run --example` behavior stays unchanged.
