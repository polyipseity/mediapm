---
description: "Use when authoring or editing example files or example-execution tests. Covers the policy that every example main must be exercised by an embedded test, and that CI detection for nondeterministic examples lives in the test that calls main, never inside main itself."
name: "Example Execution Policy"
applyTo: "src/**/examples/**/*.rs"
---

# Example execution policy

## Every example main is exercised by an embedded test

Each example file under `src/*/examples/` must contain at least one `#[cfg(test)]` test that executes the example's `main()` via `super::main()`. This guarantees the documented example entry point is compiled and runnable, not just its helper functions. Tests that call a `#[tokio::main]`-wrapped `main()` must be plain `#[test]` functions, because `#[tokio::main]` expands to a synchronous `fn main()` that creates and drives its own runtime; a `#[tokio::test]` wrapper would create a nested runtime and panic.

Nextest (via `cargo test-all` / `--all-targets`) compiles examples and runs their embedded `#[cfg(test)]` modules; it never executes an example `main()` on its own.

## Nondeterministic examples reduce their run in CI, detected in the test

Examples that require nondeterministic access (network, external services, managed-tool downloads) must detect CI in the test that calls `main()`, never inside `main()` itself. The test body:

1. Detects CI using the standard CI environment variables (`CI`, `GITHUB_ACTIONS`, `GITLAB_CI`, `CIRCLECI`, `TRAVIS`, `BUILDKITE`, `DRONE`).
2. When CI is detected, sets the example's reduced-mode environment variable and then calls `super::main()`, which runs the config-only path (no network, no external tools).
3. When CI is not detected, it does not set the reduced-mode variable (no reduced-mode) and skips with a documented message when the full path is unsafe or blocked; full runs stay manual via `cargo run --example`.

`main()` itself must be deterministic given environment inputs: no CI detection, no network probing, and no conditional behavior other than what the documented environment variables select.
