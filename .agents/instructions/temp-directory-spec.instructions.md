---
description: "Use when authoring or editing mediapm temp-directory behavior: production code, tests, examples, janitor scripts, or the regression gate. Canonical source of truth for the temp-directory naming contract, lifecycle owners, janitor contract, and authoring rules."
name: "Temp Directory Spec"
applyTo: "src/**/*.rs, scripts/**"
---

# Temp directory spec

Canonical source of truth for all mediapm-owned temporary directories under the OS temp dir (`std::env::temp_dir()`): naming contract, lifecycle ownership, janitor scripts, the regression gate, and authoring rules. Example-specific wiring (env overrides, `IsolatedExampleRoots`) lives in `example-temp-isolation.instructions.md`; path layout lives in `paths-layout.instructions.md`.

## Naming contract

All mediapm-owned temp roots share the single `mediapm-` prefix (`mediapm-{role}-{unique}` naming):

| Role    | Prefix    | Constructor                                             | Typical use                                            |
| ------- | --------- | ------------------------------------------------------- | ------------------------------------------------------ |
| artifact | `mediapm-` | `mediapm_utils::temp::artifact_dir()`                   | Example/test workspace roots, test harness roots       |
| cache   | `mediapm-`  | `mediapm_utils::temp::cache_dir()`                      | Hermetic user-level download cache                     |
| runtime | `mediapm-`  | `mediapm_utils::temp::runtime_dir_for_workspace(root)` | Conductor sandbox tmp root (stable per workspace) |

- The single prefix constant (`MEDIAPM_TEMP_PREFIX = "mediapm-"`) lives ONLY in `src/mediapm-utils/src/temp.rs`. No other file defines it.
- `{unique}` for artifact/cache is a `tempfile`-generated random suffix; runtime uses a stable 16-hex hash of the workspace root path.

## Directory classes and lifecycle owners

| Class          | Created by                                       | Removed by                                                                        |
| -------------- | ------------------------------------------------ | --------------------------------------------------------------------------------- |
| artifact root  | `artifact_dir()` (RAII `TempDir`)                | owning scope drop                                                                 |
| cache root     | `cache_dir()` (RAII `TempDir`)                   | owning scope drop                                                                 |
| runtime root   | lazily, first sandbox `create_dir_all`           | coordinator `remove_runtime_tmp_dir` on every `run_workflow` exit; janitor reclaim |
| per-step sandbox | step worker `create_sandbox`                     | whole runtime tree removal at workflow end                                         |

RAII `TempDir` owners must be bound to a local for the full scope that needs the directory. Helpers that create a store under a lazily-created root must return the owning `TempDir` so the caller controls the lifecycle.

## Runtime tmp lifecycle

- `{runtime_root}/sandbox/{instance_key}` is created lazily by the first executed step.
- The coordinator removes the whole runtime tree on EVERY `run_workflow` exit path (normal completion, step failure, early error return).
- SIGKILL/crash leaves the tree; the janitor reclaims it on the next cleanup run (by design).

## Janitor contract (sh + ps1 parity twins)

`scripts/clean-mediapm-temp.sh` (POSIX) and `scripts/clean-mediapm-temp.ps1` (Windows) are behavior-identical twins:

- Glob set: the single `mediapm-*` temp-root glob ONLY at depth 1 under the OS temp dir (bash: `$TMPDIR`/`/tmp`; PowerShell: `[System.IO.Path]::GetTempPath()`). No workspace-relative globs of any kind.
- `--dry-run`: prints `would remove:` lines plus a count, exits 0.
- Real run: clears readonly bits (`chmod -R u+w` / `Clear-ReadOnlyAttributes`), retries transient failures (6 attempts, 40 ms backoff — mirrors `remove_dir_all_with_retry`), prints `removed:` lines plus a count.
- No matches: prints `no mediapm temp directories found`, exits 0.
- Unknown argument: prints to stderr, exits 1.
- The real OS user cache (`<os-cache>/mediapm/cache/`) is never touched.
- Keep both scripts behaviorally identical; changing one requires the matching change in the other.
- Self-test scripts (`tests/scripts/test-clean-mediapm-temp.sh` / `.ps1`, `tests/scripts/test-run-all-tests.sh` / `.ps1`) are driven by the root `tests/` crate (package `mediapm-tests`) via `cargo --locked test-pkg mediapm-tests` (cargo/nextest), not invoked directly from `run-all-tests.*`.
- Janitor sandbox self-match gotcha: the Rust janitor tests (`tests/scripts/mod.rs`) seed fake dirs in a nested `scope` subdir of a `mediapm-`-prefixed sandbox — the sandbox root must carry the managed prefix so a leaked sandbox is reclaimed by the janitor/orphan gate, but `find -maxdepth 1 -name 'mediapm-*'` would match the root itself; the nested `scope` basename dodges the glob, and the janitor is pointed at `scope` via the child-scoped `TMPDIR`.

## Regression gate contract

- `scripts/run-all-tests.sh` ends with a dry-run gate: `clean-mediapm-temp.sh --dry-run | grep -q 'would remove'` -> exit 1. Any leftover managed dir fails the suite. CI-covered (ubuntu-latest).
- `scripts/run-all-tests.ps1` mirrors the gate for parity; the ps1 janitor self-test is CI-covered via the Windows script-tests job (`cargo --locked test-pkg mediapm-tests`).
- Unprefixed-tempdir invariant gate: `tempfile::tempdir(` and `.prefix(` may appear ONLY in `src/mediapm-utils/src/temp.rs`. Naming drift or a reintroduced unprefixed tempdir fails the suite.

## Authoring rules

- Use the role helpers only: `artifact_dir()`, `cache_dir()`, `runtime_dir_for_workspace()`. NO bare `tempfile::tempdir()` anywhere in the tree — tests included. The only prefix-capable constructors are the role helpers and the `tempfile::Builder::new().prefix(...)` calls inside `temp.rs`.
- Never `OnceLock<TempDir>` for workspace dirs (the guard's destructor never runs).
- Readonly-marked trees must be removed with `mediapm_utils::temp::remove_dir_all_with_retry` (clears readonly bits, retries share violations); plain `remove_dir_all` silently fails on readonly subtrees.
- Helpers that create a store for a lazily-created root must return the owning `TempDir`.
- Bind every `TempDir` to a local for its full scope; never leak a path without a cleanup owner.

## Env overrides

`MEDIAPM_EXAMPLE_ARTIFACT_ROOT` / `MEDIAPM_EXAMPLE_CACHE_ROOT` remain example-layer-only (see `example-temp-isolation.instructions.md`). The user-level OS cache is not managed temp and is never cleaned by the janitor.

## Related instructions

- `example-temp-isolation.instructions.md` — example/test wiring: env overrides, `IsolatedExampleRoots`, parallelism constraints.
- `paths-layout.instructions.md` — path resolution rows for `conductor_tmp_dir` / `mediapm_tmp_dir`.
- `scripts-and-permissions.instructions.md` — script placement, line endings, permissions.
