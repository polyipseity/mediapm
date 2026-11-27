---
description: "Use for features, bugs, errors, SDD/TDD and coverage."
name: "SDD, TDD, and errors"
applyTo: "**/*"
---

# SDD, TDD, and errors

## Workflow

- Artifacts in `.pi/plans/` (gitignored by `~/.config/git/ignore`); never `docs/`; facts in instructions/`AGENTS.md`/matrix.
- Feature: spec (`AGENTS.md` invariants/contracts/edges) -> tests (unit `#[cfg(test)]`, integration `tests/int/`/`tests/e2e/`, property `#[cfg(feature = "proptest")]`, demos) -> implement -> coverage matrix. Every `src/*/examples/` example exercises `main()` via embedded test; nondeterministic tests assert selected path, never skip (`examples-and-demos.instructions.md`). Bug: failing reproduction, fix, `cargo test --no-fail-fast`, spec entry for gaps. Terminal tests use `assert_eq!(actual, expected)` + `concat!(...)`; substring/count only in `rust-workflow.instructions.md` exceptions.

## Regression requirements (schema strictness)

R1 `regression_*` reject per closed hole with clear diagnostic; R2 round-trip previously valid minimal doc; R3 migration round-trips green and output satisfies target contract; R4 parity tests updated not weakened, containment assertions updated in same commit + strictness; R5 guard module (`tests/int/schema_strictness.rs`) asserts unknown field/fractional integral/unknown enum errors; R6 `strict_*`/`regression_*`/`parity_*` names.

## Validation gates

`prek`: `cargo fmt`, lint, checks; development `cargo test -p <crate>`; pre-push `cargo test --no-fail-fast`, `cargo run --example mediapm_demo`/`_online`. Example changes: embedded `main()` tests + explicit run. Per release: spec items vs tests.

## Coverage matrix

`.agents/coverage-matrix.md` (outside `.agents/instructions/` auto-load glob) changes with specs/tests; ASCII markers `[covered]` (test asserts item), `[partial]`, `[missing]`, `[by-design]` (untestable, records why); no unicode emoji.

## Error codes and taxonomy

- `error[CRATE-TYPE###]: <description>`; prefixes `MPM`, `CAS`, `CND`, `UTL`; `E` aborts, `W` non-fatal. Catalog in `MediaPmError::code` + workspace enums; no catalog codes at construction sites.
- `MediaPmError` carries context: `Io` (`operation`, `path`, `source`), `ConductorDocument` (`operation`, `path`, `detail`); `ConfigValidation` sole structured variant (`context`/`detail`/`suggestion` required). Other variants derive code from `MediaPmError::code`.
- Conductor errors via `?`/`From`; `Workflow` wrap only with extra context. Provisioning: non-critical warnings in `ToolSyncReport`, critical (doc load/save, CAS import) `Err`. `MediaPmError::Io` for filesystem, `MediaPmError::ConductorDocument` for NCL `decode_document`/`encode_document`, `MediaPmError::ConfigValidation` for fail-fast config and Nickel/serde decode of `mediapm.ncl`.
- `src/mediapm/src/error.rs`, `src/mediapm-conductor/src/error.rs`, `src/mediapm-cas/src/error.rs`.
