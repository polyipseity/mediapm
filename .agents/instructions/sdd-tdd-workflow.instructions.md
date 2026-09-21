---
description: "Use when starting new features, fixing bugs, or extending the codebase. Describes the spec-driven and test-driven development workflow adopted across the mediapm workspace."
name: "Spec-Driven and Test-Driven Development Workflow"
applyTo: "**/*"
---

# SDD/TDD Workflow

Spec-first, test-first development workflow for the mediapm workspace.

## When Adding a Feature

1. **Write the spec**: Update the relevant `AGENTS.md` with invariants, contracts, edge cases, and cross-crate integration boundaries if applicable.
2. **Write tests**, in this order:
   - **Unit tests** (`#[cfg(test)]` in the same file) for internal logic
   - **Integration tests** (`tests/int/` or `tests/e2e/`) for public API contracts
   - **Property tests** (`#[cfg(feature = "proptest")]`) for determinism, idempotency, and round-trip behavior
   - **Demo examples** (for mediapm) validate the full pipeline
   - **Example mains**: every example under `src/*/examples/` must exercise its own `main()` via an embedded test; deterministic examples always run their full path, nondeterministic examples skip in CI, run a deterministic reduced mode in the test harness, and run their full path only on explicit `cargo run --example` (see `example-execution-policy.instructions.md`)
3. **Enforce exact-output matching for terminal-rendering tests**: Tests that validate progress bar, spinner, or any terminal-rendered output must use `assert_eq!(actual, expected)` with `concat!(...)` string matching. Substring or count-only assertions are not acceptable except in the narrow exceptions documented in `rust-conventions.instructions.md` ("Terminal output matching").
4. **Implement**: Code against the spec and tests. Verify all tests pass before moving on.
5. **Update the coverage matrix**: Mark spec items as covered (`[covered]`), partial (`[partial]`), or uncovered (`[missing]`).

## When Fixing a Bug

1. **Write a failing test** that reproduces the bug, in the relevant `tests/` directory or `#[cfg(test)]` block.
2. **Fix the implementation**: run the test suite to confirm the fix.
3. **Verify no regressions**: run `cargo test --no-fail-fast` and confirm the suite is fully green.
4. **Add a spec entry** if the bug revealed a gap in `AGENTS.md`.

## When Adding a New Managed Tool

Follow the step-by-step guide in `src/mediapm/AGENTS.md` ("Adding a New Managed Tool"). The TL;DR:

1. Spec first: document the contract
2. Test first: write provider/preset/workflow tests
3. Implement provider → preset → workflow
4. Register in all dispatchers
5. Integration test end-to-end

## Regression requirements

These invariants apply to every schema-strictness change. The normative strictness spec (S1–S13) lives in `nickel.instructions.md`; the test suites below are its proof.

- **R1: Every closed hole gets a reject test.** For each `..` removed, each `Dyn`/`TagOrString` replaced, each silent-ignore path removed, a `regression_*` test proves the previously-accepted input now errors with a clear diagnostic.
- **R2: No valid-doc regression.** For every tightened type a round-trip test proves a previously-valid minimal document still decodes identically (byte-for-byte where feasible).
- **R3: Migration round-trips stay green.** `v1→v2`, `v2→v1`, and (mediapm) `state.ncl→state.json` migration tests pass against the tightened contracts; migration output never violates the target contract.
- **R4: Parity tests updated, not weakened.** `schema_sync.rs` string-containment assertions are updated in the same commit as the schema change (never deleted without a replacement assertion), and extended with strictness assertions.
- **R5: Strictness guard module.** A dedicated test module (e.g. `tests/int/schema_strictness.rs`) re-asserts the strictness properties via serde behavior: unknown field on each guarded struct errors, fractional numbers on integral fields error, unknown enum names error, so a future loosening fails the suite even if it compiles.
- **R6: Naming convention for selective runs.** New reject tests are named `strict_*`; regression tests `regression_*`; parity tests `parity_*`. This allows `cargo test -p <crate> strict_`, `regression_`, `parity_` as validation gates and makes intent visible in test names.

## Coverage Tracking

The "Coverage matrix" section maps each spec item to its test status. Update it when a new spec item is added, a new test covers a spec item, or a spec item becomes stale or is removed.

## Unicode emoji prohibition

Do not use unicode emoji in this file or in any coverage matrix entries. Unicode emoji in agent-edited files can cause agent harness crashes. Use only ASCII markers in the Status column:

- `[covered]`: spec item is fully tested
- `[partial]`: spec item is partially tested (approximation or incomplete)
- `[missing]`: spec item is not yet tested

## Validation Gates

| Gate                | What it validates                                                       | Frequency            |
| ------------------- | ----------------------------------------------------------------------- | -------------------- |
| Pre-commit (`prek`) | `cargo fmt`, linting, basic checks                                      | Every commit         |
| Selective tests     | `cargo test -p <crate>` for iterating                                   | During development   |
| Full workspace      | `cargo test --no-fail-fast`                                             | Before push          |
| Demos               | `cargo run --example mediapm_demo` (and \_online)                       | Before push          |
| Example mains       | Embedded tests exercise each example `main()` (nextest `--all-targets`); explicit `cargo run --example` exercises the full path (the only level for nondeterministic examples) | Every example change |
| Coverage review     | Compare spec items vs test status                                       | Per-release          |

## Coverage matrix

The full coverage matrix is maintained in `.agents/coverage-matrix.md` (outside the `.agents/instructions/` auto-load glob). Update it when spec items or tests change. See `AGENTS.md` ("SDD/TDD compliance") for the tracking workflow.
