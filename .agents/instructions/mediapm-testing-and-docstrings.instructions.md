---
description: "Use when editing mediapm tests or introducing new Rust APIs. Covers test intent, invariant coverage, and thorough newcomer-oriented Rustdoc/docstring expectations."
name: "mediapm Testing and Docstrings"
applyTo: "tests/**/*.rs, src/**/*.rs"
---

# mediapm Testing and Docstrings

## Testing scope expectations

- Treat tests as executable specification for:
  - planner determinism,
  - sync idempotency,
  - sidecar/object integrity,
  - GC safety semantics.
- For workspace-crate integration tests (`src/*/tests/`), prefer one CAS-style
  harness layout:
  - top-level `tests/tests.rs` for wiring,
  - scenario modules grouped under `tests/e2e/`, `tests/int/`, and
    `tests/prop/`.
- For examples that depend on external tools/network/media providers, prefer
  compile-only coverage (`[[example]] ... test = false`) with explicit rustdoc
  notes explaining why runtime execution is intentionally excluded from
  automated test runs (for example `cargo run -p mediapm --example demo_online`).
- Keep `src/mediapm/examples/demo.rs` executable within automated tests:
  source ingest should stay local (`import` + bundled fixture bytes), and
  tests may force configuration-only execution via
  `MEDIAPM_DEMO_RUN_SYNC=false`.
- Keep `src/mediapm/examples/demo.rs` ffmpeg behavior fast for local fixture
  execution: prefer stream-copy (`codec_copy = "true"`) over re-encode-heavy
  demo transforms.
- Even when CI keeps these examples compile-only, changes under
  `src/mediapm/**` must still run
  `cargo run --package mediapm --example demo_online` as the final local
  validation gate.
  Execute this gate with rate-limit discipline: run once per validation pass,
  avoid rapid consecutive retries, and apply cool-down backoff before retrying
  transient provider (`HTTP 429`) failures.
  If the run appears stuck, verify active process state
  (`cargo`/`mediapm`/`yt-dlp`/`ffmpeg`), inspect artifact timestamp movement,
  and check stderr for fallback-root messages (`demo-online-fallback-*`) before
  deciding to rerun.
  Use `MEDIAPM_DEMO_ONLINE_TIMEOUT_SECS` to bound long runs and treat timeout
  failures as blockers.
  Keep timeout/watchdog notices as plain-text single-shot lines and avoid
  periodic heartbeat stderr output while conductor progress rows are active so
  progress output is not duplicated.
  This gate is strict: do not mark runs as passed via skip manifests,
  placeholder payload acceptance, or fallback success markers.
- Prefer behavior-focused integration tests in `tests/` for workflow guarantees.
- Keep unit tests close to module-level invariants (`#[cfg(test)]` in same file)
  when they validate tight internal helpers.
- Treat test coverage as a crate-scoped contract:
  - `mediapm-cas`: store/get/constraint/optimize coverage,
  - `mediapm-conductor`: tool import/run/cache/re-exec coverage,
  - `mediapm`: tool lifecycle + media add/add-local + sync/materialize
    - lockfile/prune/verify coverage.

## Advanced correctness coverage

- Add property tests (`proptest`) for determinism/idempotency-sensitive logic
  such as planning/keying/merge functions.
- Add concurrency-permutation tests (`loom`) for lock/atomic-sensitive
  components when race safety is a core invariant.
- Add deterministic golden/snapshot assertions where rendered planning output
  or state projections must remain stable.

## Performance validation expectations

- Benchmark hot paths (for example hashing throughput, reconstruction depth
  impact, orchestration overhead, materialization throughput) when making
  performance claims.
- Follow evidence-first loop: profile -> hypothesize -> optimize -> benchmark,
  and revert optimizations that do not produce measured wins.

## Required test qualities

- Use explicit arrange/act/assert structure.
- Make assertions specific and diagnostic (avoid vague boolean assertions when possible).
- Keep tests platform-safe (normalize path separators when asserting path strings).
- Use temporary directories for filesystem tests; avoid depending on host machine state.
- Tests that validate missing-AcoustID-key behavior must explicitly blank
  credentials (`ACOUSTID_API_KEY` and/or CLI override) so ambient host
  environment variables cannot mask the expected failure path.
- When splitting a module into folder form (`foo/mod.rs`), place module-local
  unit tests in `foo/tests.rs` and wire them with `#[cfg(test)] mod tests;`
  from `foo/mod.rs`.

## When behavior changes

If a code change alters user-visible behavior:

- Update existing tests or add new tests in the same commit.
- For conductor executable `content_map` changes, cover both file and
  directory-ZIP semantics (including explicit invalid ZIP failure paths),
  root-directory key (`./` or `.\\`) handling, and non-overwrite collision
  rejection when separate entries target the same file path.
- For yt-dlp output-variant behavior changes, cover object semantics for
  `kind` default capture behavior, optional `capture_kind` override,
  optional `langs` capture filtering, and the ownership boundary where
  downloader language selection remains step `options.sub_langs`.
- For conductor regex capture behavior changes, assert `file_regex` exact-one
  matching and `folder_regex` zero-to-many behavior (including zero-match
  success paths).
- Update CLI/reporting docs if command output contract changes.
- Keep `verify` and `gc` expectations synchronized with sidecar model updates.

## Rustdoc/docstring depth requirement

For newcomers with no codebase context:

- Add `//!` module docs explaining why the module exists.
- Add `///` docs for **every Rust item touched**, including private items:
  - `struct`, `enum`, `trait`, `type`, `const`, `static`,
  - `fn`/methods/associated functions,
  - helper structs/enums in test modules,
  - internal state-machine and actor message enums.
- Use detailed docstrings where possible, not one-liners:
  - purpose and where the item fits in the module,
  - key invariants and assumptions,
  - side effects, mutation, locking, persistence, or I/O behavior,
  - error conditions and failure modes for fallible functions,
  - performance notes when behavior is intentionally optimized.
- Document important field semantics in structs (public and private).
- In tests, add concise item docs/comments that state the user-level guarantee,
  the invariant being protected, and why failure matters.
- For touched files, prefer "document-everything in that file" completion over
  only documenting the exact changed lines.

### Strictness policy

- Treat missing docs on touched private helpers as quality regressions.
- Do not accept placeholder docs that restate names (for example,
  "Runs optimize" without semantics).
- If a file has many undocumented internals, continue documenting until no
  obvious top-level helper/constant/type remains undocumented.

## Anti-patterns to avoid

- Bare one-line docs that repeat the function name.
- Public APIs without any semantic explanation.
- Private helper functions/constants without docs in touched files.
- Tests that only check "it runs" without asserting durable behavior.
- Silent behavior changes without corresponding test updates.

## Validation commands

Before finishing, run targeted validation on affected crates:

**Standard development workflow:**

- `cargo fmt-check` (formatting check on all Rust files)
- `cargo test-pkg <crate>` (affected crate testing; e.g., `cargo test-pkg mediapm`)
- `cargo clippy-pkg <crate>` (affected crate lint; e.g., `cargo clippy-pkg mediapm`)
- For edits under `src/mediapm/**`, run
  `cargo run --package mediapm --example demo_online` last and report
  transient external-provider failures explicitly if encountered.
  Inspect `src/mediapm/examples/.artifacts/demo-online/` after the run and
  validate sidecar-family payload correctness (not only path existence).
  Apply rate-limit-safe retry behavior here too: no rapid retry loops after
  `HTTP 429`; wait for cool-down before a retry.
  If the run appears stalled, confirm process activity and artifact timestamp
  movement first, and check stderr for fallback-root messages
  (`demo-online-fallback-*`) before reissuing the command.
  Use `MEDIAPM_DEMO_ONLINE_TIMEOUT_SECS` to cap runtime and treat timeout
  failures as blockers until resolved or reviewer-accepted.
  Keep timeout/watchdog notices as plain-text single-shot lines and avoid
  periodic heartbeat stderr output while conductor progress rows are active so
  progress output is not duplicated.
  Treat those failures as blockers until the run succeeds or the reviewer
  accepts the transient failure.

**Before submitting (pre-push):**

- `cargo fmt-check` (all files)
- `cargo clippy-all` (full workspace)
- `cargo test-all` (full workspace)

See `.cargo/config.toml` for complete alias definitions.
