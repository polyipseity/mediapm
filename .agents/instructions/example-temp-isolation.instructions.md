---
description: "Use when authoring mediapm examples, examples-as-tests, or integration tests that create filesystem state. Documents the temp-directory model, env overrides, cleanup lifecycle, and parallelism constraints."
name: "Example and Test Temp Isolation"
applyTo: "src/mediapm/examples/**/*.rs,src/mediapm/tests/**/*.rs,src/mediapm/src/example_isolation.rs"
---

# Test and example temp file model

## Purpose

mediapm tests and examples can create multi-gigabyte trees (CAS stores, tool downloads, conductor sandboxes, materialized hierarchy). This document is the canonical model for where those directories live, how they are isolated from production paths, and how they are torn down. Role prefixes and helpers live in `mediapm_utils::temp` (`src/mediapm-utils/src/temp.rs`); example env wiring lives in `mediapm::example_isolation` (`src/mediapm/src/example_isolation.rs`). Explicit (Level 3) `cargo run --example` runs resolve the real OS user-level cache and are not hermetic — that is intentional (persistence across runs); only embedded tests isolate via `MEDIAPM_EXAMPLE_CACHE_ROOT`.

## Role prefixes (all under `$TMPDIR`)

```text
$TMPDIR/mediapm-{role}-{unique}
```

| Role | Prefix | API | Typical use |
| ---- | ------ | --- | ----------- |
| **artifact** | `mediapm-artifact-` | `mediapm_utils::temp::artifact_dir()` | Example/test workspace roots, integration-test `MediaPmService` roots |
| **cache** | `mediapm-cache-` | `mediapm_utils::temp::cache_dir()` | Hermetic user-level tool download cache (`cache_root_override`) |
| **runtime** | `mediapm-runtime-{16hex}` | `mediapm_utils::temp::runtime_dir_for_workspace(root)` | Conductor sandbox tmp root per workspace (stable hash of workspace path) |

Do not call `tempfile::tempdir()` or `TempDir::new()` directly in workspace code — use the role helpers above so orphans are identifiable and the janitor can remove them. The naming contract, janitor contract, regression gates, and authoring rules are the canonical spec: **`temp-directory-spec.instructions.md`**.

## Directory classes

| Class | Typical path | Created by | Hermetic in tests? |
| ----- | ------------ | ---------- | ------------------ |
| **Canonical example artifact root** | `src/mediapm/examples/artifacts/<name>/` | `cargo run --example` when env overrides unset | No — manual runs only; never share between parallel tests |
| **Isolated artifact root** | `$TMPDIR/mediapm-artifact-*` | `IsolatedExampleRoots` sets `MEDIAPM_EXAMPLE_ARTIFACT_ROOT` | Yes — one unique root per guard |
| **Fallback artifact root** | `$TMPDIR/mediapm-artifact-*` | `isolated_artifact_dir()` when canonical reset fails (share violation) | Yes — caller holds `TempDir` until drop |
| **Workspace runtime tree** | `{artifact_root}/.mediapm/` | `MediaPmService::new_fs_at*` | Lives under isolated or canonical artifact root |
| **Workspace CAS store** | `{artifact_root}/.mediapm/store/` | FileSystem CAS open | Holds `store/lock` flock — parallel opens on same path fail |
| **Workspace tool cache** | `{artifact_root}/.mediapm/cache/` | sync / materialization | Distinct from user-level download cache |
| **User-level download cache** | `<os-cache>/mediapm/cache/` by default | `sync_tools`, provisioning | Tests must override via `MEDIAPM_EXAMPLE_CACHE_ROOT` → `MediaRuntimeStorage.cache_root_override`; explicit `cargo run --example` uses it directly via `user_level_cache_root()` |
| **Isolated download cache** | `$TMPDIR/mediapm-cache-*` | `IsolatedExampleRoots::with_cache` | Yes |
| **Level-3 user-level cache** | `<os-cache>/mediapm/cache` | `example_isolation::user_level_cache_root()` when `MEDIAPM_EXAMPLE_CACHE_ROOT` unset (explicit `cargo run --example`) | No — the real persistent user-level cache, shared with regular mediapm syncs; tests never touch it |
| **Conductor sandbox** | `{conductor_tmp_dir}/sandbox/{instance_key}/` | step worker per workflow step | Removed when coordinator calls `remove_runtime_tmp_dir` after `run_workflow` |
| **Conductor tmp root** | `$TMPDIR/mediapm-runtime-{16hex}/` | `runtime_dir_for_workspace` from workspace root | See `paths-layout.instructions.md` |
| **Integration-test workspace** | `$TMPDIR/mediapm-artifact-*` | `tests/int/*`, `tests/e2e/*` via `artifact_dir()` | Yes — RAII drop removes tree when test process exits normally |
| **Manual debug trees** | ad-hoc fixed paths | local debugging | No — not auto-cleaned; use `scripts/clean-mediapm-temp.sh` / `scripts/clean-mediapm-temp.ps1` |

Canonical artifact roots are documentation and manual-demo targets. Examples-as-tests must never write there concurrently.

## Env overrides

| Env var | Constant | Read by | Purpose |
| ------- | -------- | ------- | ------- |
| `MEDIAPM_EXAMPLE_ARTIFACT_ROOT` | `example_isolation::ARTIFACT_ROOT_ENV` | Example `artifact_root()` helpers | Workspace / artifact root the example mutates |
| `MEDIAPM_EXAMPLE_CACHE_ROOT` | `example_isolation::CACHE_ROOT_ENV` | `example_runtime_storage()` → `cache_root_override` | User-level tool download cache for sync/provisioning; defaults to the real user-level cache `<os-cache>/mediapm/cache` (`example_isolation::user_level_cache_root()`) when unset; `example_isolation::uses_isolated_cache_root()` reports whether the isolated override is active |

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
- Process-global env vars: hold the example env lock (`example_isolation::lock_process_env()`) for the mutation scope, or keep an `IsolatedExampleRoots` guard alive; restore env before the lock/guard is released. Restore-early remains a good practice.

Prefixed tempdirs clean on normal process exit. Killed nextest workers (timeout, SIGKILL) may leave `$TMPDIR/mediapm-*` trees — run the janitor (`scripts/clean-mediapm-temp.sh` on POSIX, `scripts/clean-mediapm-temp.ps1` on Windows) periodically.

## Lifecycle: conductor sandboxes

During `run_workflow`, each executed step gets a sandbox under `{conductor_tmp_dir}/sandbox/`. After the workflow finishes (success or partial failure), the coordinator calls `remove_runtime_tmp_dir(conductor_tmp_dir)` to remove the entire runtime tmp tree. Failures are logged as warnings and do not fail the workflow result. The full lifecycle contract is in **`temp-directory-spec.instructions.md`** ("Runtime tmp lifecycle").

mediapm-driven runs wire `conductor_tmp_dir` from `MediaPmPaths` (`mediapm-runtime-{16hex}` under `$TMPDIR`), not a hardcoded global path.

## Parallelism constraints

| Test kind | Parallelism | Constraint |
| --------- | ----------- | ---------- |
| Integration tests in `tests/mod.rs` harness | cargo/nextest parallel across binaries | Each test uses its own `artifact_dir()` — safe |
| Embedded `#[cfg(test)]` in one example binary | safe in parallel within the same binary | `IsolatedExampleRoots` holds a process-wide env lock (`example_isolation::lock_process_env()`) for its whole lifetime; direct env mutation without a guard must acquire it explicitly |
| Example `main_is_exercised` + sibling tests | safe in parallel within the same binary | Guard-held or explicitly acquired env lock covers every `MEDIAPM_EXAMPLE_*` mutation; no `--test-threads=1` needed |

Tests that mutate `MEDIAPM_EXAMPLE_*` (or any process env) directly without a guard must hold the lock via `example_isolation::lock_process_env()` for the mutation scope. The lock is a `parking_lot::MutexGuard` (Send), so it can be held across `.await` in `#[tokio::test]` bodies that keep an `IsolatedExampleRoots` guard alive. Never nest `lock_process_env()` inside `with_cache()`/`artifact_only()` (or inside another guard): the `parking_lot::Mutex` is non-reentrant and nesting deadlocks.

## Managed temp path detection

`mediapm_utils::temp::is_managed_path` returns true when the final path component starts with `mediapm-artifact-`, `mediapm-cache-`, or `mediapm-runtime-`. Use this when deciding whether aggressive cleanup is safe.

## Manual janitor

`scripts/clean-mediapm-temp.sh` (POSIX) and its twin `scripts/clean-mediapm-temp.ps1` (Windows) remove stale mediapm temp trees. The full janitor contract — glob set (temp-root three prefixes only), `--dry-run` semantics, readonly/retry handling, and the parity requirement — is in **`temp-directory-spec.instructions.md`** ("Janitor contract").

## Authoring checklist

- [ ] Example `main()` reads `ARTIFACT_ROOT_ENV` / `CACHE_ROOT_ENV` (via `example_isolation` constants), not hardcoded canonical paths only.
- [ ] Example tests use `IsolatedExampleRoots` — not hand-rolled `tempfile` + `set_var` guards.
- [ ] Share-violation fallbacks use `isolated_artifact_dir()` and keep the `TempDir` alive until cleanup.
- [ ] Full-sync demos that run tool-update precheck require `with_cache()` so `CACHE_ROOT_ENV` is set.
- [ ] Integration tests use `artifact_dir()` + `cache_dir()` + `cache_root_override` — never the real OS cache or canonical example artifacts.
- [ ] No test fixtures written into `src/` or committed artifact trees.
- [ ] After local debug with fixed paths, run the janitor script or delete manually.

## Related instructions

- `example-execution-policy.instructions.md` — `main_is_exercised`, three-level run model for nondeterministic examples (CI skip / test-harness reduced config-only mode / explicit-run full sync)
- `rust-conventions.instructions.md` — general test isolation, `StoreLocked`, async tests
- `paths-layout.instructions.md` — `conductor_tmp_dir`, `mediapm_tmp_dir`, workspace cache layout
- `tool-sync-coordinator-and-identity.instructions.md` — hermetic cache override for provisioning
