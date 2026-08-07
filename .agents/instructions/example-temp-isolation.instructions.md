---
description: "Use when authoring mediapm examples, examples-as-tests, or integration tests that create filesystem state. Documents the temp-directory model, env overrides, cleanup lifecycle, and parallelism constraints."
name: "Example and Test Temp Isolation"
applyTo: "src/mediapm/examples/**/*.rs,src/mediapm/tests/**/*.rs,src/mediapm/src/example_isolation.rs"
---

# Test and example temp file model

## Purpose

mediapm tests and examples can create multi-gigabyte trees (CAS stores, tool downloads, conductor sandboxes, materialized hierarchy). This document is the canonical model for where those directories live, how they are isolated from production paths, and how they are torn down. Role prefixes and helpers live in `mediapm_utils::temp` (`src/mediapm-utils/src/temp.rs`); example env wiring lives in `mediapm::example_isolation` (`src/mediapm/src/example_isolation.rs`).

## Role prefixes (all under `$TMPDIR`)

```text
$TMPDIR/mediapm-{role}-{unique}
```

| Role | Prefix | API | Typical use |
| ---- | ------ | --- | ----------- |
| **artifact** | `mediapm-artifact-` | `mediapm_utils::temp::artifact_dir()` | Example/test workspace roots, integration-test `MediaPmService` roots |
| **cache** | `mediapm-cache-` | `mediapm_utils::temp::cache_dir()` | Hermetic user-level tool download cache (`cache_root_override`) |
| **runtime** | `mediapm-runtime-{16hex}` | `mediapm_utils::temp::runtime_dir_for_workspace(root)` | Conductor sandbox tmp root per workspace (stable hash of workspace path) |

Do not call `tempfile::tempdir()` or `TempDir::new()` directly in workspace code — use the role helpers above so orphans are identifiable and `scripts/clean-mediapm-temp.sh` can remove them.

## Directory classes

| Class | Typical path | Created by | Hermetic in tests? |
| ----- | ------------ | ---------- | ------------------ |
| **Canonical example artifact root** | `src/mediapm/examples/artifacts/<name>/` | `cargo run --example` when env overrides unset | No — manual runs only; never share between parallel tests |
| **Isolated artifact root** | `$TMPDIR/mediapm-artifact-*` | `IsolatedExampleRoots` sets `MEDIAPM_EXAMPLE_ARTIFACT_ROOT` | Yes — one unique root per guard |
| **Fallback artifact root** | `$TMPDIR/mediapm-artifact-*` | `isolated_artifact_dir()` when canonical reset fails (share violation) | Yes — caller holds `TempDir` until drop |
| **Workspace runtime tree** | `{artifact_root}/.mediapm/` | `MediaPmService::new_fs_at*` | Lives under isolated or canonical artifact root |
| **Workspace CAS store** | `{artifact_root}/.mediapm/store/` | FileSystem CAS open | Holds `store/lock` flock — parallel opens on same path fail |
| **Workspace tool cache** | `{artifact_root}/.mediapm/cache/` | sync / materialization | Distinct from user-level download cache |
| **User-level download cache** | `<os-cache>/mediapm/cache/` by default | `sync_tools`, provisioning | Tests must override via `MEDIAPM_EXAMPLE_CACHE_ROOT` → `MediaRuntimeStorage.cache_root_override` |
| **Isolated download cache** | `$TMPDIR/mediapm-cache-*` | `IsolatedExampleRoots::with_cache` | Yes |
| **Conductor sandbox** | `{conductor_tmp_dir}/sandbox/{instance_key}/` | step worker per workflow step | Removed when coordinator calls `remove_runtime_tmp_dir` after `run_workflow` |
| **Conductor tmp root** | `$TMPDIR/mediapm-runtime-{16hex}/` | `runtime_dir_for_workspace` from workspace root | See `paths-layout.instructions.md` |
| **Integration-test workspace** | `$TMPDIR/mediapm-artifact-*` | `tests/int/*`, `tests/e2e/*` via `artifact_dir()` | Yes — RAII drop removes tree when test process exits normally |
| **Manual debug trees** | ad-hoc fixed paths | local debugging | No — not auto-cleaned; use `scripts/clean-mediapm-temp.sh` |

Canonical artifact roots are documentation and manual-demo targets. Examples-as-tests must never write there concurrently.

## Env overrides

| Env var | Constant | Read by | Purpose |
| ------- | -------- | ------- | ------- |
| `MEDIAPM_EXAMPLE_ARTIFACT_ROOT` | `example_isolation::ARTIFACT_ROOT_ENV` | Example `artifact_root()` helpers | Workspace / artifact root the example mutates |
| `MEDIAPM_EXAMPLE_CACHE_ROOT` | `example_isolation::CACHE_ROOT_ENV` | `example_runtime_storage()` → `cache_root_override` | User-level tool download cache for sync/provisioning |

`main()` must honor these when set and fall back to canonical paths when unset so `cargo run --example` behavior is unchanged.

## Lifecycle: examples-as-tests

1. **Guard** — `let _guard = IsolatedExampleRoots::with_cache()` or `artifact_only()` creates prefixed tempdirs and sets env vars for the guard's lifetime.
2. **Run** — `super::main()` or helper runs against isolated paths; example code reads env via `artifact_root()` / `example_runtime_storage()`.
3. **Fallback** — If reset of canonical artifact root hits Windows share violation (or permission denied / OS error 32), call `isolated_artifact_dir()` and keep the returned `TempDir` alive for the rest of the test or `main()` scope.
4. **Guard drop** — Restores prior env values, then `remove_dir_all_with_retry` on guard tempdirs (clears readonly bits, retries share violations).

`remove_dir_all_with_retry` is also used when examples reset artifact roots at the start of a run.

## Lifecycle: integration tests (`tests/int`, `tests/e2e`)

Integration tests do not use `IsolatedExampleRoots` unless they invoke example code. Standard pattern:

```rust
let root = mediapm_utils::temp::artifact_dir().expect("artifact dir");
let cache_root = mediapm_utils::temp::cache_dir().expect("cache dir");
let runtime_storage = MediaRuntimeStorage {
    cache_root_override: Some(cache_root.path().to_path_buf()),
    ..MediaRuntimeStorage::default()
};
let service = MediaPmService::new_fs_at_with_runtime_storage_overrides(root.path(), runtime_storage).await?;
// ... test body ...
// root and cache_root drop at end of test → prefixed tempdirs removed
```

Rules:

- Bind `TempDir` to a local variable for the full test scope — do not leak paths without a cleanup owner.
- Pass `cache_root_override` whenever the test calls `sync_tools`, provisioning, or full `sync_library`.
- Drop exclusive CAS handles before reopening the same store path (see `StoreLocked` pattern in `rust-conventions.instructions.md`).
- Process-global env vars: restore immediately after the consuming setup step, not at function exit, when tests in the same binary could run in parallel.

Prefixed tempdirs clean on normal process exit. Killed nextest workers (timeout, SIGKILL) may leave `$TMPDIR/mediapm-*` trees — use `scripts/clean-mediapm-temp.sh` periodically.

## Lifecycle: conductor sandboxes

During `run_workflow`, each executed step gets a sandbox under `{conductor_tmp_dir}/sandbox/`. After the workflow finishes (success or partial failure), the coordinator calls `remove_runtime_tmp_dir(conductor_tmp_dir)` to remove the entire runtime tmp tree. Failures are logged as warnings and do not fail the workflow result.

mediapm-driven runs wire `conductor_tmp_dir` from `MediaPmPaths` (`mediapm-runtime-{16hex}` under `$TMPDIR`), not a hardcoded global path.

## Parallelism constraints

| Test kind | Parallelism | Constraint |
| --------- | ----------- | ---------- |
| Integration tests in `tests/mod.rs` harness | cargo/nextest parallel across binaries | Each test uses its own `artifact_dir()` — safe |
| Embedded `#[cfg(test)]` in one example binary | **unsafe in parallel within the same binary** | `MEDIAPM_EXAMPLE_*` are process-global; two tests in one example crate racing on env corrupt paths and CAS locks |
| Example `main_is_exercised` + sibling tests | Run example test binaries with `--test-threads=1` when debugging flakes | CI nextest often runs one process per test — still avoid overlapping env mutation in one binary |

When adding multiple tests to one example file, either use a single isolation guard per test and avoid parallel execution in that binary, or run `cargo test -p mediapm --example <name> -- --test-threads=1`.

## Managed temp path detection

`mediapm_utils::temp::is_managed_path` returns true when the final path component starts with `mediapm-artifact-`, `mediapm-cache-`, or `mediapm-runtime-`. Use this when deciding whether aggressive cleanup is safe.

## Manual janitor

`scripts/clean-mediapm-temp.sh` removes stale trees under `$TMPDIR` matching only:

- `mediapm-artifact-*`
- `mediapm-cache-*`
- `mediapm-runtime-*`

Supports `--dry-run`. It does not delete canonical `examples/artifacts/` or the real OS user cache.

## Authoring checklist

- [ ] Example `main()` reads `ARTIFACT_ROOT_ENV` / `CACHE_ROOT_ENV` (via `example_isolation` constants), not hardcoded canonical paths only.
- [ ] Example tests use `IsolatedExampleRoots` — not hand-rolled `tempfile` + `set_var` guards.
- [ ] Share-violation fallbacks use `isolated_artifact_dir()` and keep the `TempDir` alive until cleanup.
- [ ] Full-sync demos that run tool-update precheck require `with_cache()` so `CACHE_ROOT_ENV` is set.
- [ ] Integration tests use `artifact_dir()` + `cache_dir()` + `cache_root_override` — never the real OS cache or canonical example artifacts.
- [ ] No test fixtures written into `src/` or committed artifact trees.
- [ ] After local debug with fixed paths, run the janitor script or delete manually.

## Related instructions

- `example-execution-policy.instructions.md` — `main_is_exercised`, CI skip policy for nondeterministic examples
- `rust-conventions.instructions.md` — general test isolation, `StoreLocked`, async tests
- `paths-layout.instructions.md` — `conductor_tmp_dir`, `mediapm_tmp_dir`, workspace cache layout
- `tool-sync-coordinator-and-identity.instructions.md` — hermetic cache override for provisioning
