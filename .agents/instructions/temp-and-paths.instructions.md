---
description: "Use when editing temp dirs, example/test isolation, path layout, or janitor behavior. Covers mediapm temp naming/lifecycle, env overrides, MediaPmPaths, and cache layout."
name: "Temp, paths, and isolation"
applyTo: "src/**/*.rs, scripts/**"
---

# Temp, paths, and isolation

Canonical spec for mediapm-owned temp directories, example/test isolation, and `MediaPmPaths` layout. Role helpers and the prefix constant live in `src/mediapm-utils/src/temp.rs`; example env wiring lives in `src/mediapm/src/example_isolation.rs`; path resolution lives in `src/mediapm/src/paths.rs`.

## Temp contract

All mediapm-owned temp roots use one prefix: `mediapm-{role}-{unique}`. `MEDIAPM_TEMP_PREFIX = "mediapm-"` is defined only in `src/mediapm-utils/src/temp.rs`.

| Role | Helper | Typical use |
| --- | --- | --- |
| output | `output_dir()` | Example/test workspace roots, test harness roots |
| cache | `cache_dir()` | Hermetic user-level download cache |
| runtime | `runtime_dir_for_workspace(root)` | Conductor sandbox tmp root, stable 16-hex hash of workspace path |

Output and cache roots are RAII `ManagedTempDir`; bind to a local for the full scope.

### Runtime tmp lifecycle

Runtime roots are lazily created and removed by the coordinator on every `run_workflow` exit (normal completion, step failure, early error). SIGKILL/crash leaves the runtime tree; the janitor reclaims it.

Rules:

- Use only the role helpers. No bare `tempfile::tempdir()` or `TempDir::new()` anywhere in workspace code, including tests. The only prefix-capable constructors are the role helpers and `tempfile::Builder::new().prefix(...)` inside `temp.rs`.
- Never `OnceLock<ManagedTempDir>` for workspace dirs.
- Bind every `ManagedTempDir` to a local for its full scope; never leak a path without a cleanup owner.
- Helpers that create a store under a lazily-created root must return the owning `ManagedTempDir`.

## Why a CAS temp root is safe to remove

A `mediapm-output-*` root holding a `mediapm-cas` store may be removed as soon as the last `FileSystemCas` handle drops. `FileSystemCas::drop` cancels the background WAL consumer and closes the mutation gate (`mediapm_cas::io_gate`): close blocks until every already-dispatched file-system mutation has finished and refuses every later one, so no `create_dir_all` can resurrect a removed tree.

- Lease every operation that can create a directory entry (directory creation, file creation, write-plus-rename pairs) through `CasIoGate::run`. Writes through an already-open file handle and reads need no lease. Never hold a lease across `.await`; take and release it inside the blocking closure, or `close` can deadlock on a `current_thread` runtime.
- The gate closes on the last handle only (an `Arc` token counted in `Drop`), so a dropped clone never closes the store out from under a sibling.
- Test harnesses must not rely on tuple-binding order. `mediapm-cas` `common::open_file_cas` returns a `FileCasFixture` whose field order drops the store before its `ManagedTempDir`.

## Janitor contract

`scripts/clean-mediapm-temp.sh` and `scripts/clean-mediapm-temp.ps1` are behavior-identical twins. CLI: `[--dry-run] [--root <dir>]`. Default root is `$TMPDIR` (`/tmp` when unset) for sh and `[System.IO.Path]::GetTempPath()` for ps1. Sweep only depth-1 `mediapm-*` under the root; never touch the real OS user cache (`<os-cache>/mediapm/cache/`).

- `--dry-run`: prints `would remove:` lines and a count, exits 0, removes nothing.
- Real run: creates and removes a probe directory through the same removal path before sweeping. If the probe survives, print `cannot remove directories under <root>: the probe <path> survived` to stderr and exit 1 with no `removed:` lines. Clear readonly bits, retry transient failures (6 attempts, 40 ms backoff, mirroring `remove_dir_all_with_retry`), print `removed:` lines and a count. A path that survives its own removal is reported as `failed to remove: <path>` on stderr, the sweep continues, and the run exits 1 with `<N> director(ies) could not be removed`.
- No matches: `no mediapm temp directories found`, exit 0. Unknown arg: stderr, exit 1. Missing or unreadable/unsearchable root: exit 1 naming the root.
- A POSIX-shell invocation passes the root in that shell's own spelling (`C:/...`, not `C:\...`).
- Janitor sandbox self-match gotcha: Rust janitor tests seed fake dirs in a nested `scope` subdir of a `mediapm-`-prefixed sandbox and pass `scope` via `--root` (or child-scoped `TMPDIR`), because `find -maxdepth 1 -name 'mediapm-*'` would match the sandbox root itself.
- Janitor invocation names root with `--root`, never env. Keep both scripts behaviorally identical; changing one requires the matching change in the other. Self-tests live in `tests/scripts/` and are driven by `cargo --locked test-pkg mediapm-tests`.

## Gates and authoring

The test-runner `test` subcommand runs gates last: nextest, then the unprefixed-tempdir gate (`scripts/test-runner/src/gates/tempdir.rs`), then the janitor dry-run gate (`scripts/test-runner/src/gates/janitor.rs`). A non-zero nextest exit returns before both gates.

- Unprefixed-tempdir invariant: `tempfile::tempdir(` and `.prefix(` may appear only in `src/mediapm-utils/src/temp.rs`.
- Readonly-marked trees must be removed with `mediapm_utils::temp::remove_dir_all_with_retry`; plain `remove_dir_all` silently fails on readonly subtrees. `ManagedTempDir` drop uses the retry path.
- A janitor invocation names its root with `--root`, never through environment.

## Env overrides and isolation

`MEDIAPM_EXAMPLE_OUTPUT_ROOT` and `MEDIAPM_EXAMPLE_CACHE_ROOT` are example-layer only. The real OS user cache is not managed temp and is never cleaned by the janitor.

### Parallelism constraints

- Tests use `mediapm::example_isolation::IsolatedExampleRoots`. `with_cache()` or `output_only()` hold the process-wide env lock for the guard lifetime; never nest lock/guards (`parking_lot::Mutex` is non-reentrant).
- Direct env mutation without a guard must hold `example_isolation::lock_process_env()` for the mutation scope. Tests that depend on the default environment must hold the lock too, whether reading a seam or assuming one is not installed.
- Integration tests use `output_dir() + cache_dir() + cache_root_override`; never the real OS cache or canonical example output roots. Bind `ManagedTempDir` locals for the full scope and pass `cache_root_override` when sync/provisioning/version resolution runs.
- A test that spawns the binary isolates it the same way: pass `--tool-cache-dir <cache_dir>`, which is the CLI form of `cache_root_override`, and remove ambient `MEDIAPM_*` variables from the child. Process-environment isolation cannot reach the OS user cache: `dirs::cache_dir()` reads the platform known folder (`FOLDERID_LocalAppData` on Windows) rather than home-directory or XDG-style environment variables.
- Share-violation fallbacks use `isolated_output_dir()` and keep the returned `ManagedTempDir` alive until cleanup.
- Explicit `cargo run --example` resolves the real OS user-level cache via `example_isolation::user_level_cache_root()` when `MEDIAPM_EXAMPLE_CACHE_ROOT` is unset; embedded tests always isolate it.

## Paths layout

`MediaPmPaths` (`src/mediapm/src/paths.rs`) declares exactly 14 fields:

| Field | Default | Purpose |
| --- | --- | --- |
| `root_dir` | workspace root | Top-level workspace directory |
| `runtime_root` | `{root_dir}/.mediapm/` | Runtime-owned state root |
| `mediapm_ncl` | `{root_dir}/mediapm.ncl` | User-edited policy config |
| `conductor_user_ncl` | `{root_dir}/mediapm.conductor.ncl` | Conductor user document |
| `conductor_generated_ncl` | `{root_dir}/mediapm.conductor.generated.ncl` | Conductor generated document |
| `conductor_state_config` | `{runtime_root}/state.conductor.json` | Conductor volatile state |
| `conductor_tmp_dir` | `$TMPDIR/mediapm-runtime-{16hex}/` | Conductor sandbox tmp root |
| `conductor_schema_dir` | `{runtime_root}/config/conductor/` | Conductor Nickel contract sources |
| `mediapm_state_json` | `{runtime_root}/state.json` | MediaPM machine state |
| `env_file` | `{runtime_root}/.env` | User-authored dotenv |
| `env_generated_file` | `{runtime_root}/.env.generated` | Machine-generated dotenv |
| `schema_export_dir` | `Some({runtime_root}/config/mediapm/)` | MediaPM Nickel contract sources; `None` skips export |
| `hierarchy_root_dir` | `{root_dir}` | Materialized media library root |
| `tools_dir` | `{runtime_root}/tools/` | Tool-content unpack directory |

Cache directories are method outputs, not fields: `workspace_cache_dir()`, `workspace_cache_store_dir()`, `workspace_yt_dlp_cache_dir()`, `workspace_media_tagger_cache_dir()`, `workspace_mediapm_cache_dir()`. They derive from `runtime_root` and cannot be overridden.

`MediaPmPathOverrides` resolve relative to the `mediapm.ncl` parent. `schema_export_dir`: `None` means computed default, `Some(None)` disables export, `Some(Some(path))` uses the resolved override.

Invariants:

- `conductor_tmp_dir` is under OS temp as `mediapm-runtime-{16hex}`, not under `runtime_root`. Per-step sandboxes live under `{conductor_tmp_dir}/sandbox/` and are removed after each `run_workflow`.
- `tools_dir` always lives under `runtime_root`.
- Contract export dirs hold the Nickel sources the runtime evaluates, byte for byte; they are not JSON schema. `schema_export_dir: None` skips only the mediapm half.
- `.env.example` is machine-managed beside `env_file`, regenerated on construction/refresh; it is not a `MediaPmPaths` field.
