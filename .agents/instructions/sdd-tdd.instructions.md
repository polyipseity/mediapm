---
description: "Use when adding features, fixing bugs, or handling errors. Covers SDD/TDD workflow, validation gates, and workspace error-code/taxonomy rules."
name: "SDD, TDD, and errors"
applyTo: "**/*"
---

# SDD, TDD, and errors

## Workflow

- Artifacts live in `.pi/plans/` (gitignored by `~/.config/git/ignore`). Never create a `docs/` directory; durable facts belong in instruction files, `AGENTS.md`, or coverage-matrix rows.
- Feature: write spec (update relevant `AGENTS.md` with invariants/contracts/edge cases), then write tests (unit in `#[cfg(test)]`, integration in `tests/int/` or `tests/e2e/`, property with `#[cfg(feature = "proptest")]` for determinism/idempotency/round-trip, demo examples for mediapm full pipeline), then implement, then update coverage matrix. Every example under `src/*/examples/` must exercise `main()` via an embedded test; a nondeterministic example's test asserts whichever path the switch selects rather than skipping (see `example-execution-policy.instructions.md`).
- Bug fix: write a failing test that reproduces the bug, fix the implementation, run `cargo test --no-fail-fast` for no regressions, add a spec entry if the bug revealed an `AGENTS.md` gap.
- Terminal-rendered tests (progress bar, spinner, terminal output) must use `assert_eq!(actual, expected)` with `concat!(...)`. Substring/count-only assertions are acceptable only in the narrow exceptions documented in `rust-conventions.instructions.md` ("Terminal output matching").

## Regression requirements (schema strictness)

- **R1**: Every closed hole gets a reject test (`regression_*`) proving the previously accepted input now errors with a clear diagnostic.
- **R2**: No valid-doc regression; a round-trip test proves a previously valid minimal document still decodes identically.
- **R3**: Migration round-trips stay green; migration output never violates the target contract.
- **R4**: Parity tests updated, not weakened; string-containment assertions updated in the same commit and extended with strictness assertions.
- **R5**: Strictness guard module (e.g. `tests/int/schema_strictness.rs`) re-asserts strictness via serde behavior (unknown field errors, fractional integral errors, unknown enum name errors).
- **R6**: Naming for selective runs: `strict_*`, `regression_*`, `parity_*`.

## Validation gates

- Pre-commit (`prek`, every commit): `cargo fmt`, linting, basic checks.
- During development: selective `cargo test -p <crate>`.
- Before push: `cargo test --no-fail-fast`; demos `cargo run --example mediapm_demo` (and `_online` as applicable).
- Every example change: embedded tests exercise `main()` (nextest `--all-targets`); explicit `cargo run --example` exercises the full path.
- Per release: compare spec items vs test status.

## Coverage matrix

Coverage matrix lives in `.agents/coverage-matrix.md` (outside the `.agents/instructions/` auto-load glob). Update it when spec items or tests change. No unicode emoji in this file or coverage-matrix entries; use ASCII markers only: `[covered]`, `[partial]`, `[missing]`, `[by-design]`. `[covered]` claims a test asserts the item; `[by-design]` is for items no test could assert and must record why.

## Error codes and taxonomy

- Format: `error[CRATE-TYPE###]: <description>`. Prefixes: `MPM`, `CAS`, `CND`, `UTL`. Type: `E` (error, aborts) or `W` (warning, non-fatal). The catalog lives in `MediaPmError::code` and workspace error enums; do not spell catalog codes at construction sites.
- `MediaPmError` variants carry operation/path/detail context. `Io` carries `operation`, `path`, `source`; `ConductorDocument` carries `operation`, `path`, `detail`. `ConfigValidation` is the only structured-code variant: it carries catalog code + `context`/`detail`/`suggestion`, all required. Every other variant derives its code from `MediaPmError::code`.
- Propagation: map conductor errors via `?`/`From`; wrap in `Workflow` only with extra context. Provisioning failures: non-critical produce warnings in `ToolSyncReport`; critical (document load/save, CAS import) propagate as `Err`. Use `MediaPmError::Io` for filesystem I/O with descriptive operation labels, `MediaPmError::ConductorDocument` for NCL `decode_document`/`encode_document` failures, `MediaPmError::ConfigValidation` for fail-fast config checks and Nickel/serde decoding of `mediapm.ncl`.
- Pointers: `src/mediapm/src/error.rs`, `src/mediapm-conductor/src/error.rs`, `src/mediapm-cas/src/error.rs`.
