---
description: "Use when authoring or editing example files or example-execution tests. Covers the policy that every example main must be exercised by an embedded test, and that CI detection for nondeterministic examples lives in the test that calls main, never inside main itself."
name: "Example Execution Policy"
applyTo: "src/**/examples/**/*.rs"
---

# Example execution policy

## Every example main is exercised by an embedded test

Each example file under `src/*/examples/` must contain at least one `#[cfg(test)]` test that executes the example's `main()` via `super::main()`. This guarantees the documented example entry point is compiled and runnable, not just its helper functions. Tests that call a `#[tokio::main]`-wrapped `main()` must be plain `#[test]` functions, because `#[tokio::main]` expands to a synchronous `fn main()` that creates and drives its own runtime; a `#[tokio::test]` wrapper would create a nested runtime and panic.

Nextest (via `cargo test-all` / `--all-targets`) compiles examples and runs their embedded `#[cfg(test)]` modules; it never executes an example `main()` on its own.

## Deterministic examples always run their full path; nondeterministic examples skip in CI only

Examples that require nondeterministic access (network, external services, managed-tool downloads) must detect CI in the test that calls `main()`, never inside `main()` itself. The policy:

1. **Deterministic examples** (e.g. the offline demo) always run their full path in tests — no reduced mode, no CI detection.
2. **Nondeterministic examples** (e.g. the online demo) detect CI using the standard CI environment variables (`CI`, `GITHUB_ACTIONS`, `GITLAB_CI`, `CIRCLECI`, `TRAVIS`, `BUILDKITE`, `DRONE`):
   - In CI: the test skips with a documented message (the full path is nondeterministic and must not run in CI).
   - Outside CI: the test runs the full path (no reduced mode).

`main()` itself must be deterministic given environment inputs: no CI detection, no network probing, and no conditional behavior other than what the documented environment variables select.

## Examples-as-tests must be isolated

Examples compile into test binaries (nextest `--all-targets`), so any example `main()` that persists state runs concurrently with sibling tests in the same suite. It must therefore treat itself as a test: never share canonical on-disk locations between tests and never touch the real OS user cache.

Two env-var overrides exist for this, set by tests through RAII guard structs (`tempfile::TempDir` + `unsafe { std::env::set_var/remove_var }`; sound under nextest because each test runs in its own process):

- `MEDIAPM_EXAMPLE_ARTIFACT_ROOT` — artifact root (the workspace dir the example mutates). Tests set it to a unique tempdir. Never share the canonical `examples/artifacts/<name>` dir between tests in the same suite (CAS `store/lock` flock races otherwise).
- `MEDIAPM_EXAMPLE_CACHE_ROOT` — user-level tool download cache root. Tests set it to a unique tempdir and map it to `MediaRuntimeStorage.cache_root_override`, so `sync_tools()`/tool provisioning never touches the real OS cache (`default_mediapm_user_download_cache_root()`).

`main()` must honor these env vars when set and fall back to canonical paths when unset, so manual `cargo run --example` behavior stays unchanged.
