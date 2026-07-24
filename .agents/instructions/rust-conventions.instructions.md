---
description: "Use when editing Rust source, tests, or introducing new APIs. Covers Rust conventions, test expectations, docstring depth requirements, and CLI output styling rules."
name: "Rust Conventions"
applyTo: "src/**/*.rs, tests/**/*.rs"
---

# Rust Conventions

## Testing scope and expectations

Treat tests as executable specification that documents and enforces the system's behavior contracts. Every test asserts a concrete invariant about how the code behaves under specific conditions, not just that it runs without crashing. The test suite is organized around four core categories of invariants that must hold across the workspace: planner determinism (re-running the same inputs produces identical plans), sync idempotency (re-running sync produces the same results), sidecar/object integrity (metadata and content-addressed objects remain consistent after operations), and GC safety (garbage collection does not remove reachable data and correctly removes unreachable data).

### Invariant categories

**Planner determinism**: the workflow planner must produce the same plan given identical inputs, regardless of execution order or timing. Test this by invoking the planner twice with the same configuration and asserting the outputs are structurally identical.

**Sync idempotency**: running sync multiple times with the same state must produce the same materialized output and tool state. Test this by running sync, capturing the output state, running sync again, and asserting no unexpected changes occurred on the second pass.

**Sidecar/object integrity**: sidecar metadata files and CAS objects must remain consistent after all operations including materialization, verification, and garbage collection. Test this by comparing sidecar contents against expected values and verifying content-addressed object hashes.

**GC safety**: garbage collection must remove only data that is unreachable from the current configuration and state. Test this by setting up a known data layout, running GC, and asserting that reachable data is preserved and orphaned data is removed.

### Integration test layout

For workspace-crate integration tests under `src/*/tests/`, prefer one CAS-style harness layout: a top-level `tests/mod.rs` file for wiring and re-exports, with scenario modules grouped under `tests/e2e/` (end-to-end workflow validation), `tests/int/` (integration between two or more components), and `tests/prop/` (property-based tests for determinism and idempotency). This layout keeps test organization predictable across all crates in the workspace.

### Demo examples

`mediapm_demo` and `mediapm_demo_online` validate the full pipeline end-to-end with real tool invocations. Run both in sequence before push — never during incremental development. `mediapm_demo` uses stream-copy (`codec_copy = "true"`) for fast local fixture execution, avoiding re-encode-heavy transforms. `mediapm_demo_online` prefers `MEDIAPM_DEMO_ONLINE_RUN_SYNC=true` when that environment variable is set. During development, prefer selective `cargo test -p <crate>` calls for fast iteration.

Do not run demos during routine development. Demos are intentionally time-consuming and require external tools: `mediapm_demo` needs ffmpeg, rsgain, and media-tagger with full media transcoding; `mediapm_demo_online` needs yt-dlp, ffmpeg, media-tagger, and active network access. Use selective unit and integration tests during iteration to keep cycles fast while validating behavior changes.

### Per-crate coverage scope

Each crate has defined coverage responsibilities. `mediapm-cas` tests cover store/get/constraint/optimize operations. `mediapm-conductor` tests cover tool import/run/cache/re-exec workflows. `mediapm` tests cover tool lifecycle (add, sync, remove) + media add/add-local + sync/materialize + state document operations (prune, verify). Unit tests stay close to module-level invariants in `#[cfg(test)]` blocks; behavior-focused integration tests in `tests/` cover workflow guarantees.

### Advanced correctness coverage

Add property tests via `proptest` for determinism and idempotency-sensitive logic such as planning, keying, and merge functions — these catch edge cases that example-based tests miss. Add concurrency-permutation tests via `loom` for lock/atomic-sensitive components when race safety is a core invariant. Add deterministic golden or snapshot assertions where rendered planning output or state projections must remain stable across refactors.

### Performance validation

Performance claims must be backed by an evidence-first loop: profile to identify the hotspot, hypothesize an optimization, implement it, benchmark to measure the effect, and revert optimizations that do not produce measured wins. Benchmark hot paths such as hashing throughput, reconstruction depth impact, orchestration overhead, and materialization throughput when making performance claims about those areas.

### CI auto-detection in demos

Both demos auto-detect CI mode using standard CI environment variables (`CI=true`, `GITHUB_ACTIONS`, etc.) and skip external tool execution when detected. In config-only mode, artifacts and workflow validation complete without spawning external processes or requiring network access. This lets CI validate configuration parsing and workflow structure without tool dependencies. When CI is detected, neither ffmpeg, yt-dlp, rsgain, nor media-tagger are required — only the Rust toolchain and workspace dependencies are needed.

### Verification commands reference

Run `cargo test -p <crate>` for selective crate testing during development. Before push, run `cargo test --no-fail-fast` for full workspace validation, then `cargo run --package mediapm --example mediapm_demo` followed by `cargo run --package mediapm --example mediapm_demo_online` in sequence. Use `cargo fmt-check`, `cargo clippy-all`, and `cargo test-all` for the full CI gate suite.

### Full-sync demo verification

When running full-sync demos, verify the following success indicators: all managed tool downloads complete (check output logs), workflow execution shows `sync executed: true` or an equivalent success marker, the artifact root contains the expected materialized output hierarchy, and `manifest.json` is written with populated artifact paths and timing profile.

### Timing expectations

`mediapm_demo` (local transcode): approximately 5–15 seconds. `mediapm_demo_online` (yt-dlp + transcode): approximately 15–45 seconds, network-dependent. The online demo resets only its artifact root between runs — it preserves `.mediapm/cache` so repeated runs reuse tool downloads and media-tagger caches. For manual online demo runs, use `MEDIAPM_DEMO_ONLINE_TIMEOUT_SECS=600` and `env -u TMPDIR` as needed to extend the timeout and avoid temp-directory interference. Metadata-only ffmpeg extraction can be tuned with `-probesize 32k -analyzeduration 0` for dramatic speedups — for example, ffmetadata export drops from approximately 5 seconds to approximately 0.02 seconds.

## Required test qualities

### Structure and assertions

Use explicit arrange/act/assert structure throughout each test body. The three sections should be visibly separated: first set up the test fixtures and inputs, then invoke the code under test exactly once, then assert on the observable outcomes. Make assertions specific and diagnostic — prefer `assert_eq!(actual, expected)` with a descriptive failure message over vague boolean assertions like `assert!(result.is_ok())`. When asserting error cases, match on the specific error variant and verify the error message or context contains the expected information.

### Platform safety

Keep tests platform-safe by normalizing path separators when asserting path strings. Use `std::path::MAIN_SEPARATOR` or utility functions that convert paths to a canonical representation so assertions pass on both Unix (forward slash) and Windows (backslash). Avoid hardcoding path separators in test assertions. When comparing generated paths against expected values, convert both sides to a canonical form before comparing rather than using raw string equality.

### Determinism and ordering

Avoid depending on nondeterministic iteration order from `HashMap`, `HashSet`, or similar unordered collections in test assertions. Use ordered collection types (`BTreeMap`, `BTreeSet`) or sort before comparing when asserting collection contents. When testing concurrent operations, use deterministic scheduling patterns rather than relying on timing-based synchronization.

### Test isolation

Use temporary directories for filesystem tests — create them with `tempfile::TempDir` or `assert_fs::TempDir` and avoid depending on host machine state or pre-existing data. Tests must be self-contained and leave no filesystem artifacts after completion. Do not write test fixtures into the source tree.

### Credential isolation for AcoustID tests

Tests that validate missing-AcoustID-key behavior must explicitly blank credentials by unsetting `ACOUSTID_API_KEY` or passing a CLI override. This prevents ambient host environment variables from masking the expected failure path. Set the env var to an empty string with `std::env::remove_var` or use a test-scoped approach that restores the original value on drop. The same principle applies to any test that depends on the absence of ambient configuration — always explicitly control the environment rather than assuming it is empty.

### Async test patterns

Use `tokio::test` for async tests. Prefer `#[tokio::test(start_paused = true)]` for tests that involve timeouts or intervals to avoid real-time waiting. For tests that spawn tasks, ensure all spawned tasks complete before the test function returns by using `tokio::task::spawn` with a `JoinHandle` that is awaited. Avoid `tokio::spawn` without tracking the handle in tests — unchecked background tasks may outlive the test and cause flaky failures in subsequent tests.

### Module split conventions

Place `#[cfg(test)]` blocks inline in the source file they test. If the inline block exceeds approximately 300 lines, split into a themed sibling file named `foo_<theme>.rs` and declare it with `#[cfg(test)] mod foo_<theme>;` in the module. This keeps test code close to the implementation while avoiding excessively long files.

### StoreLocked pattern for tests opening CAS twice

`FileSystemCas::open()` acquires an exclusive `flock` on `{root}/lock`. When a test opens CAS at `cas_root` and later passes `&cas_root` to `sync_hierarchy()` (which opens the same store internally), the second open hits `CasError::StoreLocked`. The fix is to `drop(cas)` before calling `sync_hierarchy()`, then reopen with `FileSystemCas::open(&cas_root).await` if CAS access is needed after the sync completes. The same pattern applies to `ToolDownloadCache::open()` at the global cache path — defer opening the cache until provisioning is actually needed.

## Behavior change expectations

### Atomic test updates

Update existing tests or add new tests in the same commit as a behavior change. Never change behavior without corresponding test updates — silent behavior changes are a quality regression. Update CLI and reporting documentation if command output contracts change. This applies to all behavior changes regardless of apparent triviality: renaming a CLI flag, changing a default value, altering error message wording, or modifying output field order all deserve test updates.

### Demo update policy

When the full pipeline behavior changes (tool provisioning, sync orchestration, materialization defaults), verify that both `mediapm_demo` and `mediapm_demo_online` still produce correct output. Update the demo examples if the expected output or timing profile has changed meaningfully. The demos serve as the authoritative end-to-end contract for the mediapm application.

### Content map coverage

For conductor executable `content_map` changes, cover both file and directory-ZIP semantics in tests. This includes explicit invalid ZIP failure paths (e.g., truncated or corrupt archive data), root-directory key (`./` on Unix, `.\\` on Windows) handling, and non-overwrite collision rejection when separate content map entries target the same output file path.

### yt-dlp output-variant coverage

For yt-dlp output-variant behavior changes, cover the object semantics of `kind` (default capture behavior), optional `capture_kind` override (`"file"` or `"folder"`), optional `langs` capture filtering (subtitle-family artifacts only), and the ownership boundary where downloader language selection remains the responsibility of step `options.sub_langs` — output-variant `langs` is a capture-filter hint, not a downloader setting.

### Regex capture coverage

For conductor regex capture behavior changes, assert `file_regex` exact-one matching (the pattern must match exactly one file) and `folder_regex` zero-to-many behavior (the pattern may match zero, one, or many folders, and zero-match is a valid success path).

### Sidecar synchronization

Keep `verify` and `gc` expectations synchronized with sidecar model updates. When the sidecar data model changes, update both the verification logic and the garbage collection logic in the same commit. The sidecar model is the authoritative description of what data is reachable — verification confirms that reachable data matches the sidecar description, and GC removes data that the sidecar model does not reference. These two operations must always agree on the definition of reachability.

### State document and migration coverage

When changing state document schemas or adding migration paths, add tests that verify round-trip serialization (write then read) for both the old and new schema versions. Test that migration from previous versions produces the expected current-version output. Verify that unknown fields in older persisted state are handled gracefully (either preserved or rejected with a clear error) per the crate's versioning policy.

## Docstring depth requirement

### Module-level docs

Add `//!` module docs at the top of every Rust module file explaining why the module exists and its role in the crate. Describe what abstractions the module provides, what other modules it collaborates with, and any architectural invariants that span the module's contents.

### Item-level docs

Add `///` docs for **every Rust item touched**, including private items. This covers: `struct`, `enum`, `trait`, `type`, `const`, `static`, `fn`/methods/associated functions, helper structs/enums in test modules, and internal state-machine and actor message enums. Documentation must be present on all items in a touched file, not only the exact lines changed.

### Documentation content expectations

Each docstring must include, where applicable: the purpose of the item and where it fits in the module, key invariants and assumptions the caller must uphold, side effects of using the item (mutation, locking, persistence, I/O behavior), error conditions and failure modes for fallible functions, and performance notes when behavior is intentionally optimized (e.g., allocation patterns, algorithmic complexity).

### Field documentation

Document important field semantics on both public and private structs — especially fields whose meaning is not obvious from the type alone. A field of type `bool` needs a doc comment explaining what `true` and `false` each mean in context. A field of type `PathBuf` should describe what path it represents and under what conditions it is set.

### Test documentation

In tests, add concise doc comments or inline comments stating the user-level guarantee the test protects, the invariant being validated, and why failure matters. A test of garbage collection should say what kind of orphan data it expects to be cleaned up and what data is intentionally preserved.

### Strictness policy

Treat missing docs on touched private helpers as quality regressions. Do not accept placeholder docs that merely restate the item name — for example, `/// Runs optimize` on an `optimize` function without explaining what optimization it performs or under what conditions. If a file has many undocumented internals, continue documenting until no obvious top-level helper, constant, or type remains undocumented. For touched files, prefer "document-everything in that file" completion over only documenting the exact changed lines.

### Documentation review checklist

When reviewing documentation changes, check: does every public item have a semantic explanation of its purpose? Do private helpers and constants in touched files have at least a brief doc comment? Do struct fields explain their meaning rather than relying on type alone? Do test functions state what user-level guarantee they protect? Do module-level docs explain the module's role in the crate architecture? If any of these are missing, treat it as a review finding that must be resolved before the change is complete.

### Anti-patterns to avoid

Avoid these known anti-patterns: bare one-line docs that repeat the function name verbatim, public APIs without any semantic explanation, private helper functions or constants without docs in touched files, tests that only check "it runs" without asserting durable observable behavior, and silent behavior changes without corresponding test updates.

## CLI output style

### StatusIcon

The `StatusIcon` enum has four variants: `Success` (✓ bold green — changes applied), `NoChange` (– dim — already up to date), `Warning` (Δ bold yellow — completed with degradation), and `Error` (✗ bold red — handled failure). Every icon uses a Unicode glyph. No ASCII fallback is implemented currently.

### print_result shape

Every result line follows the exact structure `{icon} {bold_op}    {k}={v}  {k}={v}    in {duration}`, printed to stdout. The duration component is omitted when `None` is passed. The caller passes each field value as `&value as &dyn std::fmt::Display`. Keys are `snake_case` and alphabetic only; well-known abbreviations like `id`, `dir`, `ref` are permitted. The separator is `=` with no surrounding spaces. Numbers appear as bare digits without comma separators. Strings are unquoted unless they contain spaces. Field ordering puts quantity fields first, then identifiers and names, then boolean flags. Zero-valued fields may be omitted when they are not meaningful, but primary metrics must always be shown.

### Warning, hint, and error helpers

All diagnostic helpers write to stderr. `print_warning(msg)` produces `"  Δ {msg}"` with the Δ glyph in yellow. `print_hint(msg)` produces `"  → {msg}"` with the → glyph in bold cyan and is suppressed by `--quiet`. `print_error(msg)` produces `"  ✗ {msg}"` with the ✗ glyph in bold red. `print_heading(heading)` renders the heading text in bold with a dimmed ── underline. Warnings and hints never appear on the same line as a result — they are emitted as separate stderr lines, typically after the stdout result line.

### Progress bar architecture

`mediapm-utils::progress::ProgressGroup` wraps `MultiProgress` from the indicatif crate. `TrackedHandle` wraps an `Arc<SharedState>` with an optional `ProgressBar` instance. Construction uses `ProgressGroup::new()` for a simple group without an overall bar, or `ProgressGroup::with_overall(label, total)` to pin an aggregate bar at the bottom of the group. Finalization calls `finish_success("done")` on each tracked handle followed by `group.join()`, which keeps bars visible until the group drops. Use `join_and_clear()` only when the terminal must be cleared before subsequent output — for example, before printing a result line.

### Global toggle and auto-detection

`set_progress_enabled(false)` suppresses all progress output. Progress bars are also automatically disabled when stderr is not a TTY or when `--quiet` / `MEDIAPM_QUIET` is active. `progress_enabled()` queries the current state.

### Spinner animation

Every progress bar automatically enables a steady tick at 100 ms intervals via `enable_steady_tick(100ms)`, keeping the spinner animating even during long periods without position updates — for example, slow downloads. No manual `tick()` calls are needed. The spinner uses braille dots: `⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏`.

### Library stack

The styling stack uses `indicatif` 0.17 (`ProgressBar`, `MultiProgress`, `ProgressStyle`, `HumanBytes`, `HumanCount`) and `console` 0.15 (`Term::stderr().size()` for terminal width detection, `style()` for ANSI coloring). Do not add `owo-colors`, `colored`, `termion`, or other styling crates — `console::style()` is the single styling entry point.

### Styling rules

The spinner glyph is green. The prefix is bold and right-aligned to 12 characters. The elapsed timer is cyan. For child bars, the bar fill is cyan on blue. For the overall bar, the bar fill is green on dim. The ETA display is dim. Progress characters use `█` for fill and `░` for empty. The active visual template is chosen automatically via `apply_bar_style()` which checks `terminal_width()`.

### Narrow terminal fallback

On terminals narrower than 60 columns, a compact fallback template is used without the bar fill: `{spinner:.green} {prefix} [{elapsed_precise}] {pos}/{len} {msg}`.

### Custom RHS message

Call `set_message()` on any `TrackedHandle` to append custom text after the auto-computed right-hand side (count/total, elapsed, rate, ETA). The message appears separated by two spaces from the auto-computed text. Clear it by setting an empty string. Common uses include `"skipped"` (tool already provisioned at the latest version) and `"cached (N)"` (N sources served from download cache). The message works after `finish()` or `finish_success()` because the daemon ticker continues syncing shared state to the indicatif bar until the bar is removed from `MultiProgress`.

### Formatting helpers

`format_bytes(u64) -> String` wraps `indicatif::HumanBytes` and produces output like `"650.23 MiB"` or `"1.24 GiB"`. `format_count(u64) -> String` wraps `indicatif::HumanCount` and produces output like `"1.2M"` or `"42"`. There is no `format_throughput` function — use `format_bytes(value) + "/s"` inline if throughput formatting is needed.

### Performance note for progress bars

For standalone bars created via `ProgressBar::new()` (outside a `MultiProgress`), only the final `finish_success` or `finish_error` render is guaranteed to appear. Use `MultiProgress` (via `ProgressGroup`) for live-updating progress during long async awaits.

### Duration formatting

The `format_duration(Duration) -> String` function formats durations as follows: values under 1 second show two decimal places (e.g., `0.01s`, `0.05s`); values from 1 to 9 seconds show two decimal places (e.g., `1.00s`, `9.00s`); values from 10 to 59 seconds show whole seconds without decimals (e.g., `10s`, `42s`); values from 1 to 59 minutes show minutes and seconds (e.g., `1m 0s`, `30m 42s`); values of 1 hour or more show hours, minutes, and seconds (e.g., `1h 0m 0s`, `2h 15m 30s`).

## Output crate module structure

### Module: mediapm::output::report

Located in the `mediapm` crate behind the `cli` feature. Exports `StatusIcon`, `print_result`, `print_warning`, `print_hint`, `print_error`, `print_heading`, and `print_status_report`. Used for all user-facing CLI output including result lines, warnings, errors, hints, headings, and aligned status reports.

### Module: mediapm::output::progress

Located in the `mediapm` crate behind the `cli` feature. Exports `ProgressGroup`, `TrackedHandle`, `set_progress_enabled`, and `progress_enabled`. Used for progress bar rendering during long-running operations such as tool provisioning, media sync, and materialization.

### Module: mediapm-utils::progress

Located in the `mediapm-utils` crate. Two availability tiers: `DownloadProgressSnapshot` and `ProgressCallback` are always available without an indicatif dependency. These are the types used at the conductor library boundary — the conductor's `run_workflow` and related APIs take `ProgressCallback` closures and never import indicatif directly. `ProgressGroup`, `TrackedHandle`, `set_progress_enabled`, `format_bytes`, and `format_count` are behind the `progress` feature gate and are used by CLI binaries that render progress bars to the terminal.

### Dependency boundary rule

The conductor *library* (`mediapm-conductor`) must not depend on indicatif directly. It receives progress updates via `Fn` callbacks typed as `ProgressCallback`. Only the conductor *CLI binary* and the `mediapm` crate may use indicatif, accessed through `mediapm-utils/progress` with the `progress` feature enabled. This boundary ensures that downstream consumers of the conductor library (such as alternative frontends or test harnesses) are not forced to pull in indicatif and its transitive dependencies.

### Common usage pattern in handlers

Every CLI command handler follows a consistent shape: perform the operation, print the result line via `print_result` with the appropriate `StatusIcon`, then print any warnings or hints on stderr. The sync command uses the legacy `print_sync_summary(&summary)` wrapper which internally calls `print_result` with a formatted summary.

### Output stream policy

Result lines via `print_result` always go to stdout and are never suppressible. Progress bars go to stderr and are suppressible via `--quiet` or non-TTY detection. Warnings and errors go to stderr and are never suppressible. Hints go to stderr and are suppressible via `--quiet`. This policy ensures that the structured result output on stdout can always be parsed or piped without interference from diagnostic output, which remains on stderr.
