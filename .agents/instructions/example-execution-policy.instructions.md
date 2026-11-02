---
description: "Use when authoring or editing example files or example-execution tests. Covers the policy that every example main must be exercised by an embedded test, and that path selection for a nondeterministic example lives in the test that calls main, never inside main itself."
name: "Example Execution Policy"
applyTo: "src/**/examples/**/*.rs"
---

# Example execution policy

Each example under `src/*/examples/` must contain a `#[cfg(test)]` test that runs `main()` via `super::main()`. Tests calling a `#[tokio::main]`-wrapped `main()` must be plain `#[test]` (a `#[tokio::test]` wrapper would nest runtimes and panic). Nextest compiles examples and runs their embedded `#[cfg(test)]` modules but never executes `main()` itself.

## Deterministic vs nondeterministic examples

Nondeterministic examples (network, external services, managed-tool downloads) select their path in the test that calls `main()`, never inside `main()`. Of the examples only `mediapm_demo_online` can reach an upstream at all, and it no longer detects CI: its default path is offline and hermetic (see its two paths below). `mediapm_cli_add_tools` and `mediapm_cli_add_hierarchy` reach no upstream and carry **no** gate — the tools example syncs before it writes any tool requirement and then fabricates its own dummy payloads, and the hierarchy example declares no managed tool. Deterministic examples run their full path in tests with no CI detection.

- **Deterministic** (offline demo): always full path in tests (no reduced mode, no CI detection). Its two `import` steps provision with no network — one captures the media fixture the hierarchy links, the other a scratch payload nothing links, so the document shows both halves of the save policy — so the full path is genuinely offline. The demo therefore covers no real managed binary: `ffmpeg`, `sd`, and `rsgain` fetch real payloads, and `media-tagger` is classified impure because it performs network lookups at run time, so none of them can appear in a demo whose defining property is that it needs no network. Real-binary coverage of the transform and tagging stages lives on the online demo's opt-in path (see "The demo's two paths" below) and behind the YouTube regression's Level 3 opt-in.
- **Two paths** (online demo): offline by default, online on request. Neither path is skipped and no CI variable is read; `MEDIAPM_RUN_ONLINE_SYNC` only selects. See "The demo's two paths" below.

`main()` must be deterministic given env inputs: no CI detection, no network probing, only the documented env vars select behavior.

## The demo's two paths: offline by default, online on request

`mediapm_demo_online` is one `main` with two paths, and the switch does nothing but select between them.

- **Offline (default).** Unset, empty, `false`, and every other token but `1|true|yes|on` select the stub tool chain: each managed tool provisions from a mocked upstream whose payload is a chain executable that copies its input, and the tagging step names a mocked `MusicBrainz` endpoint. The run is the full pipeline — tool sync, materialization, the idempotency re-run — and needs no network, which is why the embedded `main_is_exercised` runs it in every gate.
- **Offline needs `--all-features`.** The provider base-URL seams the offline path redirects are compiled behind `test-seams`, which only `--all-features` enables, so every documented command for this demo carries it. A build without the feature refuses the offline route, naming the feature and the command, before any watchdog is armed and before any tool is resolved; it fails closed rather than provisioning from the real upstream. The support crate's own seam refusal cannot cover that build, because it gates on *its* `test-seams`, which `mediapm`'s dev-dependency always enables for an example build.
- **Online (opt-in).** `1|true|yes|on` select the real providers, the real tool binaries and the real `MusicBrainz`. No gate reaches it: only an explicit `MEDIAPM_RUN_ONLINE_SYNC=1 cargo run --all-features -p mediapm --example mediapm_demo_online` does. Such a run downloads from YouTube and reuses the real user-level tool download cache (`<os-cache-dir>/mediapm/cache`), so provisioned tools persist across runs.

The assertions follow the path. Only a real `ffmpeg`, `rsgain` and `sd` can satisfy the real-tool facts — the ffprobe stream properties, the untagged and tagged tag sets, the ReplayGain tags and the canonical media-root name — so they run online alone. The offline path reads the materialized file in process and asserts the committed fixture's codec, size and duration; `demo-hierarchy-golden.instructions.md` names that split.

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

## The three-level skip model still applies to the YouTube regression

One consumer is left. `src/mediapm/tests/int/online_sync_post_sync_dump.rs` still reaches the real network and has no mock to answer it, so it keeps the skip model; the two-path model above is what a new upstream-dependent test should use. The online demo no longer uses either — its subject is an upstream too, but it has a stub chain, so it defaults offline and takes its online path by explicit opt-in.

Any non-deterministic test doing real network downloads must apply the same gating. Canonical case: `src/mediapm/tests/int/online_sync_post_sync_dump.rs` (YouTube-download regression):

- **Level 1 (CI skip:)** `ci_mode_detected()` returns early with a documented message.
- **Level 2 (harness skip:)** absent opt-in, returns early — normal `cargo test`/`test-all`/pre-push never downloads.
- **Level 3 (explicit opt-in:)** `MEDIAPM_RUN_ONLINE_SYNC=1` (tokens `1|true|yes|on`) runs the full download and assertions.

This gate is **orthogonal** to the `large-tests` feature: `--all-features` does not run the YouTube test, and the test needs no feature. The single shared env var `MEDIAPM_RUN_ONLINE_SYNC` (`example_isolation::RUN_ONLINE_SYNC_ENV`) has one reader per consumer and the two defaults are opposite: the regression test reads `run_online_sync_enabled()` as an enable toggle (unset/disabled/unknown = skip), while the online demo's `main` reads `online_sync_enabled()` as a path selector (unset/disabled = offline, `1|true|yes|on` = online). So "no opt-in" skips the regression and runs the demo offline. The offline demo (`mediapm_demo`) reads no switch and always runs a full sync.

## Examples-as-tests must be isolated

Examples compile into test binaries (nextest `--all-targets`), so any `main()` persisting state runs concurrently with sibling tests. It must treat itself as a test: never share canonical on-disk locations, never touch the real OS user cache. See `example-temp-isolation.instructions.md` for the full temp model. Summary:

Two env-var overrides, set by tests through the shared [`IsolatedExampleRoots`](../../src/mediapm/src/example_isolation.rs) guard (`mediapm::example_isolation`):

- `MEDIAPM_EXAMPLE_ARTIFACT_ROOT` — artifact root the example mutates. Tests set it to a unique tempdir. Never share the canonical `examples/artifacts/<name>` dir between tests (CAS `store/lock` flock races).
- `MEDIAPM_EXAMPLE_CACHE_ROOT` — user-level tool download cache root. Tests set it to a unique tempdir mapped to `MediaRuntimeStorage.cache_root_override`, so `sync_tools()`/provisioning never touches the real OS cache (`default_mediapm_user_download_cache_root()`). When unset (explicit `cargo run --example`), the example resolves the real cache via `example_isolation::user_level_cache_root()` (`<os-cache-dir>/mediapm/cache`), sharing it with regular syncs; tests always set it via `IsolatedExampleRoots::with_cache`. `example_isolation::uses_isolated_cache_root()` reports whether the hermetic cache is active.

When canonical artifact roots are locked (Windows share violations), call `example_isolation::isolated_artifact_dir()` and keep the returned `TempDir` alive for the rest of the test or `main()` scope.

`main()` must honor these env vars when set and fall back to canonical paths when unset, so `cargo run --example` behavior is unchanged.

**An example whose demo takes no arguments refuses any argument it receives.** `mediapm_demo_online`'s `reject_unexpected_arguments` returns an error naming the first argument, so an argument list is answered with what was passed rather than by running a second demo from inside whatever called it. The guard outlived the managed-tool launchers it originally answered: a dispatch into the demo once turned a tagging step into a 300s timeout, and its message still tells a reader to check what named this binary as its executable. An example whose demo takes flags by design parses argv as usual (`mediapm_progress_tool_sync`, `mediapm_progress_workflow` and `mediapm_progress_materialize` accept `--scenario`, `--width` and `--height`). The argv check in `main` is gated on `!cfg!(test)`, because under `cargo test` the harness owns argv and the embedded test calls `main` itself; the rejection helper is unit-tested directly, by `rejects_a_dispatch_into_the_demo`, `accepts_no_arguments` and `rejects_any_stray_argument`.
