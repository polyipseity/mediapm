---
description: "Use when authoring or editing example files or example-execution tests. Covers the policy that every example main must be exercised by an embedded test, and that CI detection for nondeterministic examples lives in the test that calls main, never inside main itself."
name: "Example Execution Policy"
applyTo: "src/**/examples/**/*.rs"
---

# Example execution policy

Each example under `src/*/examples/` must contain a `#[cfg(test)]` test that runs `main()` via `super::main()`. Tests calling a `#[tokio::main]`-wrapped `main()` must be plain `#[test]` (a `#[tokio::test]` wrapper would nest runtimes and panic). Nextest compiles examples and runs their embedded `#[cfg(test)]` modules but never executes `main()` itself.

## Deterministic vs nondeterministic examples

Nondeterministic examples (network, external services, managed-tool downloads) detect CI in the test that calls `main()`, never inside `main()`. This applies to all cache-using examples (`mediapm_cli_add_tools`, `mediapm_cli_add_hierarchy`, `mediapm_demo`, `mediapm_demo_online`); deterministic examples (the offline demo) run their full path in tests with no CI detection.

- **Deterministic** (offline demo): always full path in tests — no reduced mode, no CI detection.
- **Nondeterministic** (online demo) — three-level model:
  - **Level 1 — CI:** `cargo test` with any of `CI`, `GITHUB_ACTIONS`, `GITLAB_CI`, `CIRCLECI`, `TRAVIS`, `BUILDKITE`, `DRONE` set → embedded test skips via `ci_mode_detected()`.
  - **Level 2 — harness outside CI:** `cargo test`/`test-all`/pre-push (no CI env) → runs `main()` in a deterministic reduced mode (e.g. config-only) via documented env vars, never touching the network.
  - **Level 3 — explicit run:** `cargo run --example <name>` (no `cfg(test)`) → full path, the only level exercising network and external tools. With env overrides unset, cache-using examples resolve the real persistent cache via `example_isolation::user_level_cache_root()` (`<os-cache-dir>/mediapm/cache`), so downloads persist across runs.

`main()` must be deterministic given env inputs: no CI detection, no network probing, only the documented env vars select behavior.

## The 3-level mechanism also gates non-example regression tests

Any non-deterministic test doing real network downloads must apply the same gating. Canonical case: `src/mediapm/tests/int/online_sync_post_sync_dump.rs` (YouTube-download regression):

- **Level 1 — CI skip:** `ci_mode_detected()` returns early with a documented message.
- **Level 2 — harness skip:** absent opt-in, returns early — normal `cargo test`/`test-all`/pre-push never downloads.
- **Level 3 — explicit opt-in:** `MEDIAPM_RUN_ONLINE_SYNC=1` (tokens `1|true|yes|on`) runs the full download and assertions.

This gate is **orthogonal** to the `large-tests` feature: `--large`/`--all-features` does not run the YouTube test, and the test needs no feature. The single shared env var `MEDIAPM_RUN_ONLINE_SYNC` (`example_isolation::RUN_ONLINE_SYNC_ENV`) is the only Level 3 gate: the online demo `main()` uses it as a disable toggle (unset/enabled = full sync; disabled = reduced mode), the non-example regression test as an enable toggle (unset/disabled/unknown = skip). The offline demo (`mediapm_demo`) has no Level 3 env var and always runs a full sync.

## Examples-as-tests must be isolated

Examples compile into test binaries (nextest `--all-targets`), so any `main()` persisting state runs concurrently with sibling tests. It must treat itself as a test: never share canonical on-disk locations, never touch the real OS user cache. See `example-temp-isolation.instructions.md` for the full temp model. Summary:

Two env-var overrides, set by tests through the shared [`IsolatedExampleRoots`](../../src/mediapm/src/example_isolation.rs) guard (`mediapm::example_isolation`):

- `MEDIAPM_EXAMPLE_ARTIFACT_ROOT` — artifact root the example mutates. Tests set it to a unique tempdir. Never share the canonical `examples/artifacts/<name>` dir between tests (CAS `store/lock` flock races).
- `MEDIAPM_EXAMPLE_CACHE_ROOT` — user-level tool download cache root. Tests set it to a unique tempdir mapped to `MediaRuntimeStorage.cache_root_override`, so `sync_tools()`/provisioning never touches the real OS cache (`default_mediapm_user_download_cache_root()`). When unset (explicit `cargo run --example`), the example resolves the real cache via `example_isolation::user_level_cache_root()` (`<os-cache-dir>/mediapm/cache`), sharing it with regular syncs; tests always set it via `IsolatedExampleRoots::with_cache`. `example_isolation::uses_isolated_cache_root()` reports whether the hermetic cache is active.

When canonical artifact roots are locked (Windows share violations), call `example_isolation::isolated_artifact_dir()` and keep the returned `TempDir` alive for the rest of the test or `main()` scope.

`main()` must honor these env vars when set and fall back to canonical paths when unset, so `cargo run --example` behavior is unchanged.
