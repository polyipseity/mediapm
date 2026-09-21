---
description: "Rust conventions, test expectations, docstring depth, and CLI output styling."
name: "Rust Conventions"
applyTo: "src/**/*.rs, tests/**/*.rs"
---

# Rust Conventions

## Testing scope and expectations

Treat tests as executable specifications that assert concrete invariants about behavior under specific conditions, not just that code runs without crashing. The suite is organized around four invariant categories that must hold across the workspace: planner determinism (same inputs → identical plans), sync idempotency (re-running sync → same results), sidecar/object integrity (metadata and CAS objects stay consistent after operations), and GC safety (GC removes only unreachable data).

### Invariant categories

**Planner determinism**: the workflow planner must produce the same plan given identical inputs, regardless of execution order or timing. Test this by invoking the planner twice with the same configuration and asserting the outputs are structurally identical.

**Sync idempotency**: running sync multiple times with the same state must produce the same materialized output and tool state. Test this by running sync, capturing the output state, running sync again, and asserting no unexpected changes occurred on the second pass.

**Sidecar/object integrity**: sidecar metadata files and CAS objects must remain consistent after all operations including materialization, verification, and garbage collection. Test this by comparing sidecar contents against expected values and verifying content-addressed object hashes.

**GC safety**: garbage collection must remove only data that is unreachable from the current configuration and state. Test this by setting up a known data layout, running GC, and asserting that reachable data is preserved and orphaned data is removed.

### Integration test layout

For workspace-crate integration tests under `src/*/tests/`, prefer one CAS-style harness layout: a top-level `tests/mod.rs` file for wiring and re-exports, with scenario modules grouped under `tests/e2e/` (end-to-end workflow validation), `tests/int/` (integration between two or more components), and `tests/prop/` (property-based tests for determinism and idempotency).

### Demo examples

`mediapm_demo` and `mediapm_demo_online` validate the full pipeline end-to-end with real tool invocations. Run `mediapm_demo` before push, never during incremental development. `mediapm_demo` uses stream-copy (`codec_copy = "true"`) for fast local fixture execution. The online demo's full-sync path is human-gated to explicit `cargo run --example` runs (three-level run model in `example-execution-policy.instructions.md`). During development, prefer selective `cargo test -p <crate>` calls.

Demos need external tools: `mediapm_demo` needs ffmpeg, rsgain, and media-tagger; `mediapm_demo_online` needs yt-dlp, ffmpeg, media-tagger, and network access. Use selective unit/integration tests during iteration.

### CI auto-detection in demos

See `example-execution-policy.instructions.md` for the three-level run model, the `MEDIAPM_RUN_ONLINE_SYNC` env var, and the CI detection gate. The offline demo has no CI path and always runs its full-sync path.

### Verification commands reference

Run `cargo test -p <crate>` for selective crate testing during development. Before push, run `cargo test --no-fail-fast` for full workspace validation (the online demo's embedded test runs reduced config-only mode, no network), then `cargo run --package mediapm --example mediapm_demo`. The online demo's full-sync path runs only on explicit `cargo run --package mediapm --example mediapm_demo_online` when network validation is wanted. Use `cargo fmt-check`, `cargo clippy-all`, and `cargo test-all` for the full CI gate suite.

### Full-sync demo verification

When running full-sync demos, verify the following success indicators: all managed tool downloads complete (check output logs), workflow execution shows `sync executed: true` or an equivalent success marker, the artifact root contains the expected materialized output hierarchy, and `manifest.json` is written with populated artifact paths and timing profile.

### Timing expectations

`mediapm_demo` (local transcode): approximately 5-15 seconds. `mediapm_demo_online` (yt-dlp + transcode): approximately 15-45 seconds, network-dependent. Repeated explicit runs reuse the real user-level tool download cache via `example_isolation::user_level_cache_root()`. For manual online demo runs, use `MEDIAPM_DEMO_ONLINE_TIMEOUT_SECS=600` and `env -u TMPDIR` as needed to extend the timeout.

## Required test qualities

### Structure and assertions

Use explicit arrange/act/assert structure. Set up fixtures and inputs, invoke the code under test exactly once, then assert observable outcomes. Prefer `assert_eq!(actual, expected)` with a descriptive failure message over vague boolean assertions like `assert!(result.is_ok())`. For error cases, match the specific error variant and verify the message or context contains the expected information.

### Platform safety

Keep tests platform-safe by normalizing path separators when asserting path strings. Use `std::path::MAIN_SEPARATOR` or utility functions that convert paths to a canonical representation so assertions pass on both Unix (forward slash) and Windows (backslash). When comparing generated paths against expected values, convert both sides to a canonical form before comparing rather than using raw string equality.

### Determinism and ordering

Avoid depending on nondeterministic iteration order from `HashMap`, `HashSet`, or similar unordered collections in test assertions. Use ordered collection types (`BTreeMap`, `BTreeSet`) or sort before comparing when asserting collection contents. When testing concurrent operations, use deterministic scheduling patterns rather than relying on timing-based synchronization.

### Test isolation

Use temporary directories for filesystem tests — create them with `mediapm_utils::temp::artifact_dir()` / `cache_dir()` (or `assert_fs::TempDir` where appropriate) and avoid depending on host machine state or pre-existing data. Tests must be self-contained and leave no filesystem artifacts after completion. Do not write test fixtures into the source tree. When setting process-global environment variables that affect other tests' behavior, remove them immediately after the setup step that consumes them — do not wait until function exit. Delayed cleanup races with parallel test execution (cargo test runs integration tests in the same process concurrently).

**mediapm temp model:** Integration tests (`src/mediapm/tests/`) use per-test `mediapm_utils::temp::artifact_dir()` workspaces plus `MediaRuntimeStorage.cache_root_override` (via `cache_dir()`) for hermetic provisioning. Examples-as-tests use `mediapm::example_isolation::IsolatedExampleRoots` and `MEDIAPM_EXAMPLE_*` env overrides. See `example-temp-isolation.instructions.md` for role prefixes, cleanup lifecycle, conductor sandbox teardown, and example env serialization (process-wide lock via `example_isolation::lock_process_env()`).

### Credential isolation for AcoustID tests

Tests that validate missing-AcoustID-key behavior must explicitly blank credentials by unsetting `ACOUSTID_API_KEY` or passing a CLI override. This prevents ambient host environment variables from masking the expected failure path. Set the env var to an empty string with `std::env::remove_var` or use a test-scoped approach that restores the original value on drop. The same principle applies to any test that depends on the absence of ambient configuration — always explicitly control the environment rather than assuming it is empty.

### Async test patterns

Use `tokio::test` for async tests. Prefer `#[tokio::test(start_paused = true)]` for tests that involve timeouts or intervals to avoid real-time waiting. For tests that spawn tasks, ensure all spawned tasks complete before the test function returns by using `tokio::task::spawn` with a `JoinHandle` that is awaited. Avoid `tokio::spawn` without tracking the handle in tests — unchecked background tasks may outlive the test and cause flaky failures in subsequent tests.

### Module split conventions

Place `#[cfg(test)]` blocks inline in the source file they test. If the inline block exceeds approximately 300 lines, split into a themed sibling file named `foo_<theme>.rs` and declare it with `#[cfg(test)] mod foo_<theme>;` in the module. This keeps test code close to the implementation while avoiding excessively long files.

### StoreLocked pattern for tests opening CAS twice

`FileSystemCas::open()` acquires an exclusive `flock` on `{root}/lock`. When a test opens CAS at `cas_root` and later passes `&cas_root` to `sync_hierarchy()` (which opens the same store internally), the second open hits `CasError::StoreLocked`. The fix is to `drop(cas)` before calling `sync_hierarchy()`, then reopen with `FileSystemCas::open(&cas_root).await` if CAS access is needed after the sync completes. The same pattern applies to `ToolDownloadCache::open()` at the global cache path — defer opening the cache until provisioning is actually needed.

## Terminal output matching

> **Hard rule:** This is a mandatory convention for all terminal-rendering tests.

When writing progress bar tests that use `InMemoryTerm` to capture rendered
output, **MUST** use exact `assert_eq!(term.contents(), concat!(...))`
matching over substring or count-only assertions. The expected output string
must appear **literally** in the test source code via `concat!(...)` — never
read from an external file, environment variable, or runtime-constructed
string.

**Why.** An exact-string assertion is self-documenting: the expected output appears inline, so a reader sees the rendered shape without running the test or opening a snapshot file. It collapses fragile `lines()[i].contains(...)` chains into one `assert_eq!` that catches every class of rendering defect at once — missing blank lines, wrong bar ordering, stale position/total, wrong fill characters, missing brackets, wrong elapsed times, truncated or wrapped output. A substring assertion like `assert!(lines[0].contains("3/5"))` checks one dimension; the bar could be on the wrong line with wrong neighbors and still pass.

**Exception.** Substring or count-only assertions are acceptable when:

- The test validates behavior across non-deterministic timing (e.g. spinner
  animation cycles with real time sources).
- The test validates behavior across terminal resize events where exact
  content is dimension-dependent and the dimension change is what is being
  tested.
- The assertion is a preliminary debug check and an exact assertion follows
  on the same output.

When asserting across resize events with dynamic height, prefer comparing
non-empty content line counts (`lines().filter(|l| !l.is_empty()).count()`)
over total `.lines().count()` — trailing empty lines in the virtual terminal
buffer may differ after grow→shrink cycles even when visible content is
functionally identical.

**Capture strategy.** When the exact expected output is not known in advance:

1. Write the test body with a deliberately wrong expected string (e.g.
   `assert_eq!(term.contents(), "WRONG")`).
2. Run the test — it fails and prints the actual `term.contents()` output.
3. Copy the actual output verbatim into the `concat!(...)` assertion.
4. Re-run to confirm PASS.

This avoids manual arithmetic of `█` fill characters and bar-template width
computation.

**Inline expected output in source code.** Write the expected output literally in the test body via `concat!(...)`. Do not read it from external snapshot files, golden files, environment variables, or runtime-constructed strings — an inline `concat!(...)` is self-documenting and shows mismatches directly in the assertion line.

**Conventions.** Use `H=24, W=40` from `common::mk()` as the default terminal
size unless the test specifically targets narrow/wide/short/tall behavior.
For tests with `ProgressGroup`, use `mk_with_size(h, w)` with the minimum
height that fits the expected number of bars (children + overall + blank
reserve). Name tests with a suffix that signals exact matching
(e.g. `consumer_exact_parallel_worker_output`), making the assertion style
self-documenting. Prefix slots right-pad to their template width and the
rendered width includes ANSI escapes — a 30-col `{prefix:>30.30}` slot holding
`\x1b[0m` plus 4 visible chars renders 26 leading spaces. When a space-count
expectation breaks, re-derive the width from the template slot plus ANSI
overhead before suspecting a rendering refactor.

## Behavior change expectations

### Atomic test updates

Update or add tests in the same commit as a behavior change. Never change behavior without test updates — silent behavior changes are a quality regression. Update CLI and reporting docs if command output contracts change. This covers all behavior changes, however trivial: renaming a CLI flag, changing a default, altering error wording, or reordering output fields.

### Demo update policy

When the full pipeline behavior changes (tool provisioning, sync orchestration, materialization defaults), verify that both `mediapm_demo` and `mediapm_demo_online` still produce correct output. Update the demo examples if the expected output or timing profile has changed meaningfully. The demos serve as the authoritative end-to-end contract for the mediapm application.

### Content map coverage

For conductor executable `content_map` changes, cover both file and directory-ZIP semantics in tests. This includes explicit invalid ZIP failure paths (e.g., truncated or corrupt archive data), root-directory key (`./` on Unix, `.\\` on Windows) handling, and non-overwrite collision rejection when separate content map entries target the same output file path.

### yt-dlp output-variant coverage

For yt-dlp output-variant behavior changes, cover the object semantics of `kind` (default capture behavior), optional `capture_kind` override (`"file"` or `"folder"`), optional `langs` capture filtering (subtitle-family artifacts only), and the ownership boundary where downloader language selection remains the responsibility of step `options.sub_langs` — output-variant `langs` is a capture-filter hint, not a downloader setting.

### Regex capture coverage

For conductor regex capture behavior changes, assert `file_regex` exact-one matching (the pattern must match exactly one file) and `folder_regex` zero-to-many behavior (the pattern may match zero, one, or many folders, and zero-match is a valid success path).

### Sidecar synchronization

Keep `verify` and `gc` in sync with sidecar model updates. When the sidecar data model changes, update both verification and GC logic in the same commit. The sidecar model is the authoritative description of reachable data: verification confirms reachable data matches it, and GC removes data it does not reference. The two must agree on reachability.

### State document and migration coverage

When changing state document schemas or adding migration paths, add tests that verify round-trip serialization (write then read) for both the old and new schema versions. Test that migration from previous versions produces the expected current-version output. Verify that unknown fields in older persisted state are handled gracefully (either preserved or rejected with a clear error) per the crate's versioning policy.

## Docstring depth requirement

### Module-level docs

Add `//!` module docs at the top of every Rust module file: why the module exists, its role in the crate, the abstractions it provides, the modules it collaborates with, and any architectural invariants spanning its contents.

### Item-level docs

Add `///` docs for **every Rust item touched**, including private items. This covers: `struct`, `enum`, `trait`, `type`, `const`, `static`, `fn`/methods/associated functions, helper structs/enums in test modules, and internal state-machine and actor message enums. Documentation must be present on all items in a touched file, not only the exact lines changed.

### Documentation content expectations

Each docstring must include, where applicable: the purpose of the item and where it fits in the module, key invariants and assumptions the caller must uphold, side effects of using the item (mutation, locking, persistence, I/O behavior), error conditions and failure modes for fallible functions, and performance notes when behavior is intentionally optimized (e.g., allocation patterns, algorithmic complexity).

### Field documentation

Document important field semantics on both public and private structs — especially fields whose meaning is not obvious from the type alone. A field of type `bool` needs a doc comment explaining what `true` and `false` each mean in context. A field of type `PathBuf` should describe what path it represents and under what conditions it is set.

### Test documentation

In tests, add concise doc comments or inline comments stating the user-level guarantee the test protects, the invariant being validated, and why failure matters. A test of garbage collection should say what kind of orphan data it expects to be cleaned up and what data is intentionally preserved.

### Strictness policy

Treat missing docs on touched private helpers as quality regressions. Do not accept placeholder docs that restate the item name — e.g. `/// Runs optimize` with no explanation of what optimization runs or when. For touched files, document every obvious top-level helper, constant, and type rather than only the changed lines.

### Documentation review checklist

When reviewing documentation changes, check: does every public item have a semantic explanation of its purpose? Do private helpers and constants in touched files have at least a brief doc comment? Do struct fields explain their meaning rather than relying on type alone? Do test functions state what user-level guarantee they protect? Do module-level docs explain the module's role in the crate architecture? If any of these are missing, treat it as a review finding that must be resolved before the change is complete.

### Anti-patterns to avoid

Avoid these known anti-patterns: bare one-line docs that repeat the function name verbatim, public APIs without any semantic explanation, private helper functions or constants without docs in touched files, tests that only check "it runs" without asserting durable observable behavior, and silent behavior changes without corresponding test updates.

## CLI output style

All CLI output primitives, progress bar architecture, per-screen summary formats, formatting helpers, duration formatting, the dependency boundary rule, and the output stream policy live in `progress-output.instructions.md`. Read that file before editing any CLI output code.

The `--quiet` / `MEDIAPM_QUIET` flags suppress hints and progress. The `--no-progress` flag or `ProgressGroup::disabled()` suppresses progress bars entirely.

## Behavior change expectations

### Atomic test updates

Update or add tests in the same commit as a behavior change. Never change behavior without test updates: silent behavior changes are a quality regression. Update CLI and reporting docs if command output contracts change. This covers all behavior changes, however trivial: renaming a CLI flag, changing a default, altering error wording, or reordering output fields.

### Demo update policy

When the full pipeline behavior changes (tool provisioning, sync orchestration, materialization defaults), verify that both `mediapm_demo` and `mediapm_demo_online` still produce correct output. Update the demo examples if the expected output or timing profile has changed meaningfully. The demos serve as the authoritative end-to-end contract for the mediapm application.

### Content map coverage

For conductor executable `content_map` changes, cover both file and directory-ZIP semantics in tests. This includes explicit invalid ZIP failure paths (e.g., truncated or corrupt archive data), root-directory key (`./` on Unix, `.\` on Windows) handling, and non-overwrite collision rejection when separate content map entries target the same output file path.

### yt-dlp output-variant coverage

For yt-dlp output-variant behavior changes, cover the object semantics of `kind` (default capture behavior), optional `capture_kind` override (`"file"` or `"folder"`), optional `langs` capture filtering (subtitle-family artifacts only), and the ownership boundary where downloader language selection remains the responsibility of step `options.sub_langs` (output-variant `langs` is a capture-filter hint, not a downloader setting).

### Regex capture coverage

For conductor regex capture behavior changes, assert `file_regex` exact-one matching (the pattern must match exactly one file) and `folder_regex` zero-to-many behavior (the pattern may match zero, one, or many folders, and zero-match is a valid success path).

### Sidecar synchronization

Keep `verify` and `gc` in sync with sidecar model updates. When the sidecar data model changes, update both verification and GC logic in the same commit. The sidecar model is the authoritative description of reachable data: verification confirms reachable data matches it, and GC removes data it does not reference. The two must agree on reachability.

### State document and migration coverage

When changing state document schemas or adding migration paths, add tests that verify round-trip serialization (write then read) for both the old and new schema versions. Test that migration from previous versions produces the expected current-version output. Verify that unknown fields in older persisted state are handled gracefully (either preserved or rejected with a clear error) per the crate's versioning policy.
