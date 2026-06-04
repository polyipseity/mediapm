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
  - top-level `tests/mod.rs` for wiring,
  - scenario modules grouped under `tests/e2e/`, `tests/int/`, and
    `tests/prop/`.
- Keep `src/mediapm/examples/demo.rs` and
  `src/mediapm/examples/demo_online.rs` full-sync for explicit manual runs
  (`cargo run --example ...`). For `demo_online`, prefer
  `MEDIAPM_DEMO_ONLINE_RUN_SYNC=true` whenever this env var is set.
- Keep `src/mediapm/examples/demo.rs` ffmpeg behavior fast for local fixture
  execution: prefer stream-copy (`codec_copy = "true"`) over re-encode-heavy
  demo transforms.
- For development loops under `src/mediapm/**`, prefer selective tests while
  iterating, but before completion always run both demos end-to-end in normal
  sequence (not parallel):
  - `cargo run --package mediapm --example mediapm_demo`
  - `cargo run --package mediapm --example mediapm_demo_online`
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

## Demo Example Execution Context

### When to run demos

Run both example demos **only when validating changes at the END of work** to
confirm the full pipeline works end-to-end:

- `cargo run --package mediapm --example mediapm_demo`
- `cargo run --package mediapm --example mediapm_demo_online`

Run them in **normal sequence (not concurrently)** as part of pre-push
validation.

### When NOT to run demos

**Do not run demos during routine development or incremental changes.** Demos
are intentionally time-consuming and require external tool setup:

- `mediapm_demo`: Requires ffmpeg, rsgain, media-tagger with full media
  transcoding.
- `mediapm_demo_online`: Requires yt-dlp, ffmpeg, media-tagger, and active
  network access for downloading.

During active development, use selective unit and integration tests instead
(`cargo test-pkg <crate>`). This keeps iteration cycles fast while validating
behavior changes.

### CI mode (config-only) is auto-detected

Both demos auto-detect when to run in config-only mode (no external tools
required):

- **`mediapm_demo`**: Runs full sync by default. Detect CI environment using
  standard CI env vars (`CI=true`, `GITHUB_ACTIONS`, etc.) and skip external
  tool execution.
- **`mediapm_demo_online`**: Detects CI environment and runs config-only mode
  (validates config without network/tool access).

In config-only mode, artifacts and workflow validation complete without
spawning external processes or requiring network access.

### What to look for in output

When running full-sync demos, verify:

- **Success indicators**:
  - All managed tool downloads complete (check output logs).
  - Workflow execution shows `sync executed: true` or equivalent success marker.
  - Artifact root contains expected materialized output hierarchy.
  - `manifest.json` is written with populated artifact paths and timing profile.

- **Timing expectations**:
  - `mediapm_demo` (local transcode): ~5–15 seconds.
  - `mediapm_demo_online` (yt-dlp + transcode): ~15–45 seconds (network
    dependent).

## Required test qualities

- Use explicit arrange/act/assert structure.
- Make assertions specific and diagnostic (avoid vague boolean assertions when possible).
- Keep tests platform-safe (normalize path separators when asserting path strings).
- Use temporary directories for filesystem tests; avoid depending on host machine state.
- Tests that validate missing-AcoustID-key behavior must explicitly blank
  credentials (`ACOUSTID_API_KEY` and/or CLI override) so ambient host
  environment variables cannot mask the expected failure path.
- When splitting a module into folder form (`foo/mod.rs`), place unit tests as
  `#[cfg(test)]` blocks inline in the source file they test. If the inline
  block exceeds ~300 lines, split into a themed sibling file
  `foo_<theme>.rs` declared with `#[cfg(test)] mod foo_<theme>;`.
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

- `cargo test-pkg <crate>` (affected crate testing; e.g., `cargo test-pkg mediapm`)
- `cargo build-pkg <crate>` (affected crate build; e.g., `cargo build-pkg mediapm`)
- Run both demo commands in normal sequence (not concurrently):
  - `cargo run --package mediapm --example mediapm_demo`
  - `cargo run --package mediapm --example mediapm_demo_online`
- Do not run manual `cargo fmt`, `cargo check`, or `cargo clippy` in normal
  development loops; `prek.toml` commit hooks already enforce formatting and
  lint/check gates.
- Add or update tests selectively for the touched behavior; avoid full test
  suite runs during development.

**Before submitting (pre-push):**

- `cargo fmt-check` (all files)
- `cargo clippy-all` (full workspace)
- `cargo test-all` (full workspace)

See `.cargo/config.toml` for complete alias definitions.

## Specification references

- Consolidated technical specification:
  `.agents/instructions/crate-specifications.md`.
- Edge-case and ambiguity analysis:
  `.agents/instructions/elaboration-pass-edge-cases.md`.
