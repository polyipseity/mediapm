---
description: "Use for temp dirs, isolation, paths and janitor behavior."
name: "Temp, paths, and isolation"
applyTo: "src/**/*.rs, scripts/**"
---

# Temp, paths, and isolation

Spec for mediapm temp roots, isolation, and `MediaPmPaths`. Helpers/prefix: `src/mediapm-utils/src/temp.rs`; example env: `src/mediapm/src/example_isolation.rs`; paths: `src/mediapm/src/paths.rs`.

## Temp contract

All mediapm temp roots use `mediapm-{role}-{unique}`. `MEDIAPM_TEMP_PREFIX = "mediapm-"` only in `src/mediapm-utils/src/temp.rs`.

| Role | Helper | Use |
| --- | --- | --- |
| output | `output_dir()` | Example/test roots |
| cache | `cache_dir()` | Hermetic user cache |
| runtime | `runtime_dir_for_workspace(root)` | Conductor tmp root, stable 16-hex workspace hash |

Roots are RAII `ManagedTempDir`, bound locally for full scope. Runtime roots are created lazily and removed on every `run_workflow` exit (completion/failure/early error); SIGKILL/crash leaves the tree for the janitor.

- Role helpers only: no bare `tempfile::tempdir()`/`TempDir::new()`, including tests; prefix-capable constructors are role helpers and `tempfile::Builder::new().prefix(...)` in `temp.rs`. No `OnceLock<ManagedTempDir>` for workspace dirs.
- Bind every `ManagedTempDir` locally; helpers creating a store under a lazy root return the owning `ManagedTempDir`.

## CAS temp root removal

A `mediapm-output-*` root holding a `mediapm-cas` store may be removed once the last `FileSystemCas` drops: `FileSystemCas::drop` cancels the WAL consumer and closes the mutation gate (`mediapm_cas::io_gate`), waiting for dispatched mutations and refusing later ones, so no `create_dir_all` resurrects the tree. Lease directory-entry operations (creation, write-plus-rename) through `CasIoGate::run`; open-handle writes/reads need no lease; never hold a lease across `.await` (`close` can deadlock on `current_thread`). Gate closes on last handle (`Arc` token in `Drop`). Tests must not rely on tuple order: `common::open_file_cas` returns `FileCasFixture` whose fields drop the store before its `ManagedTempDir`.

## Janitor contract

`scripts/clean-mediapm-temp.sh`/`scripts/clean-mediapm-temp.ps1` twins: `[--dry-run] [--root <dir>]`. Default root `$TMPDIR` (`/tmp` unset) for sh, `[System.IO.Path]::GetTempPath()` for ps1. Sweep only depth-1 `mediapm-*`; never touch `<os-cache>/mediapm/cache/`.

- `--dry-run`: `would remove:` lines + count, exit 0, no removal. Real run: probe first; if it survives, stderr `cannot remove directories under <root>: the probe <path> survived`, exit 1, no `removed:` lines. Clear readonly, retry 6x/40 ms (`remove_dir_all_with_retry`), print `removed:` lines + count. Survivor: stderr `failed to remove: <path>`, sweep continues, exit 1 `<N> director(ies) could not be removed`.
- No matches: `no mediapm temp directories found`, exit 0. Unknown arg: stderr, exit 1. Bad root: exit 1 naming it. POSIX spelling `C:/...`, not `C:\...`.
- Self-match gotcha: tests seed dirs in nested `scope` under a `mediapm-` sandbox, pass `scope` via `--root` (or child `TMPDIR`); `find -maxdepth 1 -name 'mediapm-*'` matches the sandbox root. Root via `--root`, never env; twins stay identical. Self-tests `tests/scripts/`, `cargo --locked test-pkg mediapm-tests`.

## Gates and authoring

Test-runner `test` gates: nextest, then unprefixed-tempdir (`scripts/test-runner/src/gates/tempdir.rs`), then janitor dry-run (`scripts/test-runner/src/gates/janitor.rs`); non-zero nextest returns first.

- `tempfile::tempdir(`/`.prefix(` only in `src/mediapm-utils/src/temp.rs`; readonly trees need `mediapm_utils::temp::remove_dir_all_with_retry` (plain `remove_dir_all` fails), also used by `ManagedTempDir` drop; janitor root via `--root`, never env.

## Env overrides and isolation

`MEDIAPM_EXAMPLE_OUTPUT_ROOT`/`MEDIAPM_EXAMPLE_CACHE_ROOT` are example-layer only; real OS user cache is not managed temp and never cleaned.

- `mediapm::example_isolation::IsolatedExampleRoots`: `with_cache()`/`output_only()` hold process env lock for guard lifetime; never nest (`parking_lot::Mutex` non-reentrant). Direct env mutation needs `example_isolation::lock_process_env()`; so do tests assuming default env (reading a seam or assuming one absent).
- Integration: `output_dir() + cache_dir() + cache_root_override`; no real OS cache or canonical example output roots; bind locals, pass override for sync/provisioning/version resolution. Spawned binary: `--tool-cache-dir <cache_dir>` (CLI `cache_root_override`), strip ambient `MEDIAPM_*`. Env isolation cannot reach OS cache: `dirs::cache_dir()` reads `FOLDERID_LocalAppData` on Windows, not home/XDG vars.
- Share-violation fallbacks use `isolated_output_dir()` kept alive; explicit `cargo run --example` resolves real cache via `example_isolation::user_level_cache_root()` when `MEDIAPM_EXAMPLE_CACHE_ROOT` unset; embedded tests always isolate.

## Paths layout

`MediaPmPaths` (`src/mediapm/src/paths.rs`) has exactly 15 fields:

| Field | Default | Purpose |
| --- | --- | --- |
| `root_dir` | workspace root | Workspace root |
| `runtime_root` | `{root_dir}/.mediapm/` | State root |
| `mediapm_ncl` | `{root_dir}/mediapm.ncl` | User policy config |
| `conductor_user_ncl` | `{root_dir}/mediapm.conductor.ncl` | User intent |
| `conductor_generated_ncl` | `{root_dir}/mediapm.conductor.generated.ncl` | Generated runtime |
| `conductor_state_config` | `{runtime_root}/state.conductor.json` | Volatile state |
| `conductor_tmp_dir` | `$TMPDIR/mediapm-runtime-{16hex}/` | Sandbox tmp root |
| `conductor_schema_dir` | `{runtime_root}/config/conductor/` | Conductor contracts |
| `mediapm_state_json` | `{runtime_root}/state.json` | Machine state |
| `env_file` | `{runtime_root}/.env` | User dotenv |
| `env_generated_file` | `{runtime_root}/.env.generated` | Generated dotenv |
| `auth_file` | `{runtime_root}/auth.json` | Credentials; full override |
| `schema_export_dir` | `Some({runtime_root}/config/mediapm/)` | Contracts; `None` skips |
| `hierarchy_root_dir` | `{root_dir}` | Library root |
| `tools_dir` | `{runtime_root}/tools/` | Tool unpack dir |

Cache dirs are method outputs, not fields: `workspace_cache_dir()`, `workspace_cache_store_dir()`, `workspace_yt_dlp_cache_dir()`, `workspace_media_tagger_cache_dir()`, `workspace_mediapm_cache_dir()`; derive from `runtime_root`, no overrides.

`MediaPmPathOverrides` resolve relative to `mediapm.ncl` parent. `schema_export_dir`: `None` = computed default, `Some(None)` disables export, `Some(Some(path))` uses override. `auth_file` names the whole file, not a directory: `runtime.paths.auth_file`, `--auth-file`, or `MEDIAPM_AUTH_FILE`; relative value resolves against that parent.

- `conductor_tmp_dir` under OS temp as `mediapm-runtime-{16hex}`, not `runtime_root`; per-step sandboxes under `{conductor_tmp_dir}/sandbox/`, removed after each `run_workflow`.
- `tools_dir` always under `runtime_root`.
- Export dirs hold Nickel sources evaluated byte for byte, not JSON schema; `schema_export_dir: None` skips only mediapm half.
- `.env.example` machine-managed beside `env_file`, regenerated on construction/refresh; not a `MediaPmPaths` field.
