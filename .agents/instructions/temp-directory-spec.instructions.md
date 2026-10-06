---
description: "Use when authoring or editing mediapm temp-directory behavior: production code, tests, examples, janitor scripts, or the regression gate. Canonical source of truth for the temp-directory naming contract, lifecycle owners, janitor contract, and authoring rules."
name: "Temp Directory Spec"
applyTo: "src/**/*.rs, scripts/**"
---

# Temp directory spec

Naming contract, lifecycle ownership, janitor scripts, the regression gate, and authoring rules for all mediapm-owned temporary directories under the OS temp dir (`std::env::temp_dir()`). Example-specific wiring (env overrides, `IsolatedExampleRoots`) lives in `example-temp-isolation.instructions.md`; path layout lives in `paths-layout.instructions.md`.

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
- Temp-root precondition: the root must exist and be readable and searchable (`[[ ! -d || ! -r || ! -x ]]` / `Test-Path -PathType Container`). A root that is missing or cannot be enumerated exits 1 rather than reporting a clean sweep; the ps1 twin reaches the same exit 1 through the terminating `UnauthorizedAccessException` rather than a contract message. Both janitor self-tests cover it with a `chmod 000` root, skipped as root (`access(2)` succeeds) and on Windows (the permission bits are not the gate).
- The real OS user cache (`<os-cache>/mediapm/cache/`) is never touched.
- Keep both scripts behaviorally identical; changing one requires the matching change in the other.
- The gate spawns the Windows twin through `pwsh -File`, so it needs `pwsh` on `PATH` on a Windows host: without it the spawn fails and the gate fails the run rather than skipping. The pwsh janitor self-tests in `tests/scripts/mod.rs` skip instead.
- Self-test scripts (`tests/scripts/test-clean-mediapm-temp.sh` / `.ps1`) are driven by the root `tests/` crate (package `mediapm-tests`) via `cargo --locked test-pkg mediapm-tests` (cargo/nextest). They test the janitor's own contract; the gates that run the janitor live in the `test-runner` crate and are tested by its Rust unit tests.
- Janitor sandbox self-match gotcha: the Rust janitor tests (`tests/scripts/mod.rs`) seed fake dirs in a nested `scope` subdir of a `mediapm-`-prefixed sandbox — the sandbox root must carry the managed prefix so a leaked sandbox is reclaimed by the janitor/orphan gate, but `find -maxdepth 1 -name 'mediapm-*'` would match the root itself; the nested `scope` basename dodges the glob, and the janitor is pointed at `scope` via the child-scoped `TMPDIR`.

## Why a CAS temp root is safe to remove

A `mediapm-artifact-*` root holding a `mediapm-cas` store may be removed the moment the last `FileSystemCas` handle drops, with no sleep, retry, or explicit close at the call site. `FileSystemCas::drop` cancels the background WAL consumer and closes the store's mutation gate (`mediapm_cas::io_gate`): the gate blocks until every already-dispatched file-system mutation has finished and refuses every later one, so no `create_dir_all` can resurrect a removed tree. The write-after-teardown fingerprint this prevents is a leaked root holding exactly one blob object, one `metadata-v1.json`, one `checkpoint`, or an empty `blobs/v1/blake3/ab/cd/` chain.

Two rules keep that guarantee true when editing the CAS:

- Lease every operation that can create a directory entry (directory creation, file creation, write-plus-rename pairs) through `CasIoGate::run`. A write through an already-open file handle and a read cannot resurrect a removed tree, so they need no lease. Never hold a lease across an `.await` — take and release it inside the blocking closure, or `close` can deadlock on a `current_thread` runtime.
- The gate closes on the **last** handle only (an `Arc` token counted in `Drop`), so a dropped clone never closes the store out from under a sibling handle.

Test harnesses must not rely on tuple-binding order for this. `mediapm-cas`'s `common::open_file_cas` returns a `FileCasFixture` whose field order makes the store drop before its `TempDir`; a `(TempDir, FileSystemCas)` tuple makes the same guarantee a convention each call site has to remember.

## Regression gate contract

- The gate is `scripts/test-runner`'s `test` subcommand, and it runs last: nextest, then the unprefixed-tempdir gate (`gates/tempdir.rs`), then the janitor gate (`gates/janitor.rs`). A non-zero nextest exit returns before both, so the gates never explain leftovers from a run that was already rejected.
- The gate reads the janitor's exit status before its output: a sweep that exits non-zero fails the run as `error: mediapm temp-dir sweep failed: <janitor diagnostic>`, and a sweep that exits 0 reporting leftovers fails it as `error: test suite left mediapm temp dirs behind: <offending directories>`. The verdict carries the offending directories as evidence, so the first thing a developer sees names the trees to delete rather than only reporting that some exist. The two messages differ so a reader can tell a failed sweep from a dirty one. On the passing path the gate also forwards any captured line that is not one of the janitor's output-contract lines to stderr, so a diagnostic it emits while still exiting 0 cannot vanish into the captured output. CI-covered (ubuntu-latest).
- Each gate exists once. The janitor gate was previously two implementations that had to be kept in step — a `grep -rn` walk in the bash runner and a `Get-ChildItem | Select-String` walk in the PowerShell twin — and one of them skipped files `read_to_string` refuses. Both live in Rust now, so there is no second copy to drift and no per-platform rewrite of the same rule.
- The gate's own tests are the Rust unit tests in `scripts/test-runner/src/gates/janitor.rs`, which drive `enforce` against a stub janitor in a scratch tree: `a_failed_sweep_names_its_own_diagnostic` (non-zero sweep, gate names the diagnostic), `a_leftover_fails_the_gate` (exit 0 with a `would remove:` path, gate names the directory and not the janitor's count), `a_count_only_sweep_fails_with_the_bare_verdict` (exit 0 with only a `would remove` count, gate still fails on the broad trigger), and `a_clean_sweep_passes` (nothing to remove). `contract_lines_are_recognised`, `a_count_line_needs_digits` and `contract_lines_must_match_wholly` pin which lines count as janitor contract and which are diagnostics. `program_tests` pins the spawn shape rather than the verdict: `the_unix_branch_spawns_the_janitor_by_path` (the janitor is spawned by path, not through an interpreter, because `clean-mediapm-temp.sh` is bash and would fail on `ubuntu-latest`, where `sh` is dash, while passing on macOS, where `sh` is bash 3.2). The unprefixed-tempdir gate is covered the same way in `scripts/test-runner/src/gates/tempdir.rs`.
- The Windows script-tests job (`cargo --locked test-pkg mediapm-tests`) covers the janitor's own behavior on Windows, and its `cargo --locked test-pkg test-runner` step runs the gate's own unit tests, so the gate's tests are CI-covered on `ubuntu-latest` and Windows.
- Unprefixed-tempdir invariant gate: `tempfile::tempdir(` and `.prefix(` may appear ONLY in `src/mediapm-utils/src/temp.rs`. Naming drift or a reintroduced unprefixed tempdir fails the suite.

## Authoring rules

- Use the role helpers only: `artifact_dir()`, `cache_dir()`, `runtime_dir_for_workspace()`. NO bare `tempfile::tempdir()` anywhere in the tree — tests included. The only prefix-capable constructors are the role helpers and the `tempfile::Builder::new().prefix(...)` calls inside `temp.rs`.
- Never `OnceLock<TempDir>` for workspace dirs (the guard's destructor never runs).
- Readonly-marked trees must be removed with `mediapm_utils::temp::remove_dir_all_with_retry` (clears readonly bits, retries share violations); plain `remove_dir_all` silently fails on readonly subtrees.
- Helpers that create a store for a lazily-created root must return the owning `TempDir`.
- Bind every `TempDir` to a local for its full scope; never leak a path without a cleanup owner.

## Env overrides

`MEDIAPM_EXAMPLE_ARTIFACT_ROOT` / `MEDIAPM_EXAMPLE_CACHE_ROOT` remain example-layer-only (see `example-temp-isolation.instructions.md`). The user-level OS cache is not managed temp and is never cleaned by the janitor.
