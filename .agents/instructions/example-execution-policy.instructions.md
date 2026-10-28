---
description: "Use when authoring or editing example files or example-execution tests. Covers the policy that every example main must be exercised by an embedded test, and that CI detection for nondeterministic examples lives in the test that calls main, never inside main itself."
name: "Example Execution Policy"
applyTo: "src/**/examples/**/*.rs"
---

# Example execution policy

Each example under `src/*/examples/` must contain a `#[cfg(test)]` test that runs `main()` via `super::main()`. Tests calling a `#[tokio::main]`-wrapped `main()` must be plain `#[test]` (a `#[tokio::test]` wrapper would nest runtimes and panic). Nextest compiles examples and runs their embedded `#[cfg(test)]` modules but never executes `main()` itself.

## Deterministic vs nondeterministic examples

Nondeterministic examples (network, external services, managed-tool downloads) detect CI in the test that calls `main()`, never inside `main()`. Of the examples only `mediapm_demo_online` carries that gate: `mediapm_cli_add_tools` and `mediapm_cli_add_hierarchy` carry **no** gate at all, because neither reaches an upstream — the tools example syncs before it writes any tool requirement and then fabricates its own dummy payloads, and the hierarchy example declares no managed tool. Deterministic examples (the offline demo) run their full path in tests with no CI detection.

- **Deterministic** (offline demo): always full path in tests (no reduced mode, no CI detection). Its two `import` steps provision with no network — one captures the media fixture the hierarchy links, the other a scratch payload nothing links, so the document shows both halves of the save policy — so the full path is genuinely offline. The demo therefore covers no real managed binary: `ffmpeg`, `sd`, and `rsgain` fetch real payloads, and `media-tagger` is classified impure because it performs network lookups at run time, so none of them can appear in a demo whose defining property is that it needs no network. Real-binary coverage of the transform and tagging stages lives behind the `MEDIAPM_RUN_ONLINE_SYNC` gate (see the three-level skip model below), exercised by `online_sync_post_sync_dump`. Excluding them moved that coverage from the default suite, where it failed at payload fetch, to the opt-in gate, where it runs.
- **Nondeterministic** (online demo) (three-level model:
  - **Level 1 (CI:)** `cargo test` with any of `CI`, `GITHUB_ACTIONS`, `GITLAB_CI`, `CIRCLECI`, `TRAVIS`, `BUILDKITE`, `DRONE` set → embedded test skips via `ci_mode_detected()`.
  - **Level 2 (harness outside CI:)** `cargo test`/`test-all`/pre-push (no CI env) → runs `main()` in a deterministic reduced mode (e.g. config-only) via documented env vars, never touching the network.
  - **Level 3 (explicit run:)** `cargo run --example <name>` (no `cfg(test)`) → full path, the only level exercising network and external tools. With env overrides unset, cache-using examples resolve the real persistent cache via `example_isolation::user_level_cache_root()` (`<os-cache-dir>/mediapm/cache`), so downloads persist across runs.

`main()` must be deterministic given env inputs: no CI detection, no network probing, only the documented env vars select behavior.

## A network-bound test gets two paths, not a skip ladder

Where the subject is an upstream, prefer a mocked upstream over a skip. The shape is:

- **One shared assertion function** holds the contract, so the two paths cannot end up asserting different things.
- **A hermetic test** runs it against a mocked upstream and always executes — in CI, in `test-all`, and in pre-push — so the gate reports a real pass instead of a skip.
- **A real-network twin** runs the same assertion against the real hosts, carries `#[ignore]` with its reason, and is reached only by an explicit ignored run (`cargo test -p mediapm --all-features -- --ignored`, as the twin's own doc names). An ignored test is reported as ignored, so it can never be mistaken for coverage.

`src/mediapm/tests/int/all_platform.rs` is the worked example: `every_managed_tool_provisions_a_payload_for_every_os_slot` asserts `assert_every_managed_tool_covers_every_os` against the mock, and `every_managed_tool_provisions_a_payload_for_every_os_slot_online` is its `#[ignore]`d twin, kept ignored by owner decision on 2026-10-27: one run downloads every managed tool for every OS slot — hundreds of megabytes, minutes of wall time — and the upstreams behind them flake, so the hermetic sibling carries the contract in every gate and the twin is reached only by an explicit ignored run. The `tool_sync` modules answer their upstream the same way, through the stub in `tests/common/stub_upstream.rs` — a mock copied per module is a mock that drifts, and a drifted mock is the silent-coverage failure this shape exists to remove.

`MANAGED_TOOLS` in `all_platform.rs` names the managed-tool set a full sync must provision. It is hand-maintained, and the repository keeps equivalent lists at three further sites — `resolve_tool_fetch_routes_all_tools` and `resolve_tool_fetch_each_fetched_tool_covers_three_oses` (unit, `tools/provider/mod.rs`) and `every_latest_payload_url_is_built_from_the_download_seam` — so nothing derives the set from `resolve_tool_fetch`'s dispatch; a shared production constant is a separate change.

## Seams: four upstream base URLs, behind `test-seams`

A mock is only reachable when production code reads its host from somewhere a test can set:

| Env var                        | Default                          |
| ------------------------------ | -------------------------------- |
| `MEDIAPM_GITHUB_API_BASE`      | `https://api.github.com`         |
| `MEDIAPM_GITHUB_DOWNLOAD_BASE` | `https://github.com`             |
| `MEDIAPM_MARTIN_RIEDL_BASE`    | `https://ffmpeg.martin-riedl.de` |
| `MEDIAPM_JELLYFIN_BASE`        | `https://repo.jellyfin.org`      |

Each is read per call and never cached. Each reader is gated `#[cfg(feature = "test-seams")]` on its **own body** rather than at its call sites, so no call site carries a conditional, a build without the feature returns the literal default, and the environment cannot redirect a release binary.

`mediapm-test-support` (`src/mediapm-test-support/`) owns the mock: `MockUpstream` serves a mounted tool's release assets, and `SeamGuard` installs the seams for its lifetime, taking its lock as a required argument so no call site can pick the wrong mutex by omission. It is a **dev-dependency** of `mediapm` and `mediapm-conductor` and must never depend back on either, which would close a dev-dependency cycle; its `wiremock` dependency is a normal one because the mount API is part of its public surface. A build of that crate without its own `test-seams` feature refuses to install a seam, naming the feature and the command to run instead.

Seam-dependent tests are gated `#[cfg(feature = "test-seams")]`, so they need an `--all-features` build, which every gate passes. A bare `cargo test -p mediapm` compiles the seams out, and `tests/int/mod.rs` refuses to build with a `compile_error!` naming the command to run instead, so the mistake fails at the compiler rather than quietly reaching the real hosts.

The env-lock rule a test that sets a seam must follow is in `example-temp-isolation.instructions.md` ("Parallelism constraints").

## The three-level skip model still applies where no mock exists

Two tests still reach the real network and have no mock to answer them: the online demo, whose subject is the real upstream, and the YouTube regression below. They keep the skip model; the two-path model above is what a new upstream-dependent test should use.

Any non-deterministic test doing real network downloads must apply the same gating. Canonical case: `src/mediapm/tests/int/online_sync_post_sync_dump.rs` (YouTube-download regression):

- **Level 1 (CI skip:)** `ci_mode_detected()` returns early with a documented message.
- **Level 2 (harness skip:)** absent opt-in, returns early — normal `cargo test`/`test-all`/pre-push never downloads.
- **Level 3 (explicit opt-in:)** `MEDIAPM_RUN_ONLINE_SYNC=1` (tokens `1|true|yes|on`) runs the full download and assertions.

This gate is **orthogonal** to the `large-tests` feature: `--all-features` does not run the YouTube test, and the test needs no feature. The single shared env var `MEDIAPM_RUN_ONLINE_SYNC` (`example_isolation::RUN_ONLINE_SYNC_ENV`) is the only Level 3 gate: the online demo `main()` uses it as a disable toggle (unset/enabled = full sync; disabled = reduced mode), the non-example regression test as an enable toggle (unset/disabled/unknown = skip). The offline demo (`mediapm_demo`) has no Level 3 env var and always runs a full sync.

## Examples-as-tests must be isolated

Examples compile into test binaries (nextest `--all-targets`), so any `main()` persisting state runs concurrently with sibling tests. It must treat itself as a test: never share canonical on-disk locations, never touch the real OS user cache. See `example-temp-isolation.instructions.md` for the full temp model. Summary:

Two env-var overrides, set by tests through the shared [`IsolatedExampleRoots`](../../src/mediapm/src/example_isolation.rs) guard (`mediapm::example_isolation`):

- `MEDIAPM_EXAMPLE_ARTIFACT_ROOT` — artifact root the example mutates. Tests set it to a unique tempdir. Never share the canonical `examples/artifacts/<name>` dir between tests (CAS `store/lock` flock races).
- `MEDIAPM_EXAMPLE_CACHE_ROOT` — user-level tool download cache root. Tests set it to a unique tempdir mapped to `MediaRuntimeStorage.cache_root_override`, so `sync_tools()`/provisioning never touches the real OS cache (`default_mediapm_user_download_cache_root()`). When unset (explicit `cargo run --example`), the example resolves the real cache via `example_isolation::user_level_cache_root()` (`<os-cache-dir>/mediapm/cache`), sharing it with regular syncs; tests always set it via `IsolatedExampleRoots::with_cache`. `example_isolation::uses_isolated_cache_root()` reports whether the hermetic cache is active.

When canonical artifact roots are locked (Windows share violations), call `example_isolation::isolated_artifact_dir()` and keep the returned `TempDir` alive for the rest of the test or `main()` scope.

`main()` must honor these env vars when set and fall back to canonical paths when unset, so `cargo run --example` behavior is unchanged.

**An example whose demo takes no arguments refuses any argument it receives.** Managed-tool launcher scripts dispatch through `MEDIAPM_EXECUTABLE`, so an example binary can be executed as `builtin <tool> …`; an example that ignores argv runs a second demo from inside a workflow step, resetting the artifact root and re-provisioning every tool while the step waits. Such an example rejects any argument it receives, naming the first argument and `MEDIAPM_EXECUTABLE`, while one whose demo takes flags by design parses argv as usual (`mediapm_progress_tool_sync`, `mediapm_progress_workflow` and `mediapm_progress_materialize` accept `--scenario`, `--width` and `--height`). An example that needs the CLI resolves it through `mediapm::ensure_mediapm_executable()` before it provisions a tool or removes an artifact root, so a missing CLI fails immediately instead of after a long download. The check in `main` is gated on `!cfg!(test)`, because under `cargo test` the harness owns argv and the embedded test calls `main` itself; the rejection helper is unit-tested directly.
