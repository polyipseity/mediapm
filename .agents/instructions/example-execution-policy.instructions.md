---
description: "Use when authoring or editing example files or example-execution tests. Covers the policy that every example main must be exercised by an embedded test, and that a nondeterministic example selects its route through argv parsed once in main: --online names the real upstream, and the test that calls main needs no environment to steer it."
name: "Example Execution Policy"
applyTo: "src/**/examples/**/*.rs"
---

# Example execution policy

Each example under `src/*/examples/` must contain a `#[cfg(test)]` test that runs `main()` via `super::main()`. Tests calling a `#[tokio::main]`-wrapped `main()` must be plain `#[test]` (a `#[tokio::test]` wrapper would nest runtimes and panic). Nextest compiles examples and runs their embedded `#[cfg(test)]` modules but never executes `main()` itself.

## Deterministic vs nondeterministic examples

Nondeterministic examples (network, external services, managed-tool downloads) select their route through argv parsed once in `main` — `--online` names the real upstream — so `main()` decides the route once and the test that calls `main()` needs no environment to steer it. Neither one decides a route the other cannot see, and no example detects CI inside `main()`: a CI test belongs in the test that calls it. Of the examples only `mediapm_demo_online` can reach an upstream at all, and it no longer detects CI: its default route is offline and hermetic (see its two routes below). `mediapm_cli_add_tools` and `mediapm_cli_add_hierarchy` reach no upstream and carry **no** gate — the tools example syncs before it writes any tool requirement and then fabricates its own dummy payloads, and the hierarchy example declares no managed tool. Deterministic examples run their full path in tests with no CI detection.

- **Deterministic** (offline demo): always full path in tests (no reduced mode, no CI detection). Its two `import` steps provision with no network — one captures the media fixture the hierarchy links, the other a scratch payload nothing links, so the document shows both halves of the save policy — so the full path is genuinely offline. The demo therefore covers no real managed binary: `ffmpeg`, `sd`, and `rsgain` fetch real payloads, and `media-tagger` is classified impure because it performs network lookups at run time, so none of them can appear in a demo whose defining property is that it needs no network. Real-binary coverage of the transform and tagging stages lives on the online demo's opt-in route (see "The demo's two routes" below), which an explicit `--online` run reaches by hand.
- **Two routes** (online demo): offline by default, online with `--online`. Neither route is skipped and no environment variable selects anything; argv is the only channel. See "The demo's two routes" below.

`main()` must be deterministic given its inputs: no CI detection, no network probing; the documented env vars select only the temp roots, and the demo's route comes from argv.

## The demo's two routes: offline by default, online with `--online`

`mediapm_demo_online` is one `main` with two routes, and `--online` does nothing but select between them.

- **Offline (default).** A bare invocation selects the stub tool chain: each managed tool provisions from a mocked upstream whose payload is a chain executable that copies its input, and the tagging step names a mocked `MusicBrainz` endpoint. The run is the full pipeline — tool sync, materialization, the idempotency re-run — and needs no network, which is why the embedded `main_is_exercised` runs it in every gate. The provider seams are read in every debug build, so no feature is involved; a release run refuses this route before any watchdog is armed and before any tool is resolved, naming `--release`.
- **Online (opt-in).** `cargo run --package mediapm --example mediapm_demo_online -- --online` selects the real providers, the real tool binaries and the real `MusicBrainz`. No gate reaches it: run it by hand or dispatch the `online` workflow (`workflow_dispatch`). Such a run downloads from YouTube and reuses the real user-level tool download cache (`<os-cache-dir>/mediapm/cache`), so provisioned tools persist across runs.

The assertions follow the route. Only a real `ffmpeg`, `rsgain` and `sd` can satisfy the real-tool facts — the ffprobe stream properties, the untagged and tagged tag sets, the ReplayGain tags and the canonical media-root name — so they run online alone. The offline route reads the materialized file in process and asserts the committed fixture's codec, size and duration; `demo-hierarchy-golden.instructions.md` names that split.

## A network-bound test gets two paths, not a skip ladder

Where the subject is an upstream, prefer a mocked upstream over a skip. The shape is:

- **One shared assertion function** holds the contract, so a hermetic run and a hand-run validation cannot end up asserting different things.
- **A hermetic test** runs it against a mocked upstream and always executes — in CI, in `test-all`, and in pre-push — so the gate reports a real pass instead of a skip.
- **A real-host subject** has no test twin to ignore: when a run against the real hosts matters, it is the online demo run (`cargo run --package mediapm --example mediapm_demo_online -- --online`), which no gate reaches and a reader runs by hand or dispatches through the `online` workflow (`workflow_dispatch`, no inputs).

`src/mediapm/tests/int/all_platform.rs` is the worked example: `every_managed_tool_provisions_a_payload_for_every_os_slot` asserts `assert_every_managed_tool_covers_every_os` against the mock, and that hermetic test is what every gate runs; the real-host half of the shape is the demo run above, not a second copy of the assertion. The `tool_sync` modules answer their upstream the same way, through the stub in `tests/common/stub_upstream.rs` — a mock copied per module is a mock that drifts, and a drifted mock is the silent-coverage failure this shape exists to remove.

`MANAGED_TOOLS` in `all_platform.rs` names the managed-tool set a full sync must provision. It is hand-maintained, and the repository keeps equivalent lists at three further sites — `resolve_tool_fetch_routes_all_tools` and `resolve_tool_fetch_each_fetched_tool_covers_three_oses` (unit, `tools/provider/mod.rs`) and `every_latest_payload_url_is_built_from_the_download_seam` — so nothing derives the set from `resolve_tool_fetch`'s dispatch; a shared production constant is a separate change.

## Seams: four upstream base URLs, read in debug builds

A mock is only reachable when production code reads its host from somewhere a test can set:

| Env var                        | Default                          |
| ------------------------------ | -------------------------------- |
| `MEDIAPM_GITHUB_API_BASE`      | `https://api.github.com`         |
| `MEDIAPM_GITHUB_DOWNLOAD_BASE` | `https://github.com`             |
| `MEDIAPM_MARTIN_RIEDL_BASE`    | `https://ffmpeg.martin-riedl.de` |
| `MEDIAPM_JELLYFIN_BASE`        | `https://repo.jellyfin.org`      |

Each is read per call and never cached. Each reader honors its variable only when `cfg!(debug_assertions)` holds, gated on the reader's **own body** rather than at its call sites, so no call site carries a conditional, a debug build reads its environment, and a release build ignores the variables — which is what keeps a shipped binary unredirectable. That release half is documented here rather than tested: the owner accepted it, so no gate exercises a release build's refusal to redirect.

`mediapm-test-support` (`src/mediapm-test-support/`) owns the mock: `MockUpstream` serves a mounted tool's release assets, and `SeamGuard` installs the seams for its lifetime, taking its lock as a required argument so no call site can pick the wrong mutex by omission. It is a **dev-dependency** of `mediapm` and `mediapm-conductor` and must never depend back on either, which would close a dev-dependency cycle; its `wiremock` dependency is a normal one because the mount API is part of its public surface.

A bare `cargo test -p mediapm` builds and runs: the seams read their environment in every debug build, so there is no feature to pass and no `compile_error!` guard, and no test can be quietly compiled out of the suite. Every gate still passes `--all-features` for the features that remain (see `ci-workflow.instructions.md`, "Explicit forms").

The env-lock rule a test that sets a seam must follow is in `example-temp-isolation.instructions.md` ("Parallelism constraints").

## Examples-as-tests must be isolated

Examples compile into test binaries (nextest `--all-targets`), so any `main()` persisting state runs concurrently with sibling tests. It must treat itself as a test: never share canonical on-disk locations, never touch the real OS user cache. See `example-temp-isolation.instructions.md` for the full temp model. Summary:

Two env-var overrides, set by tests through the shared [`IsolatedExampleRoots`](../../src/mediapm/src/example_isolation.rs) guard (`mediapm::example_isolation`):

- `MEDIAPM_EXAMPLE_ARTIFACT_ROOT` — artifact root the example mutates. Tests set it to a unique tempdir. Never share the canonical `examples/artifacts/<name>` dir between tests (CAS `store/lock` flock races).
- `MEDIAPM_EXAMPLE_CACHE_ROOT` — user-level tool download cache root. Tests set it to a unique tempdir mapped to `MediaRuntimeStorage.cache_root_override`, so `sync_tools()`/provisioning never touches the real OS cache (`default_mediapm_user_download_cache_root()`). When unset (explicit `cargo run --example`), the example resolves the real cache via `example_isolation::user_level_cache_root()` (`<os-cache-dir>/mediapm/cache`), sharing it with regular syncs; tests always set it via `IsolatedExampleRoots::with_cache`. `example_isolation::uses_isolated_cache_root()` reports whether the hermetic cache is active.

When canonical artifact roots are locked (Windows share violations), call `example_isolation::isolated_artifact_dir()` and keep the returned `TempDir` alive for the rest of the test or `main()` scope.

`main()` must honor these env vars when set and fall back to canonical paths when unset, so `cargo run --example` behavior is unchanged.

**An example whose demo takes flags by design parses argv as usual:** `mediapm_demo_online` accepts `--online` and `--timeout-secs <seconds>` and refuses every other token with a message listing both, so a dispatch into the demo is answered with what was passed rather than by running a second demo from inside whatever called it. (`mediapm_progress_tool_sync`, `mediapm_progress_workflow` and `mediapm_progress_materialize` accept `--scenario`, `--width` and `--height` the same way.) The argv parse in `main` is gated on `!cfg!(test)`, because under `cargo test` the harness owns argv and the embedded test calls `main` itself; `parse_demo_args` is unit-tested directly, by `parse_demo_args_accepts_no_arguments`, `parse_demo_args_refuses_a_dispatch_and_lists_the_flags` and `rejects_any_stray_argument`.
