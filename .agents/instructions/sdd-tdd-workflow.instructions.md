---
description: "Use when starting new features, fixing bugs, or extending the codebase. Describes the spec-driven and test-driven development workflow adopted across the mediapm workspace."
name: "Spec-Driven and Test-Driven Development Workflow"
applyTo: "**/*"
---

# SDD/TDD Workflow

This file defines the **spec-first, test-first** development workflow adopted
across the mediapm workspace.

## When Adding a Feature

1. **Write the spec** — Update the relevant `AGENTS.md` with:
   - Invariants, contracts, and edge cases for the new functionality
   - Cross-crate integration boundaries if applicable
   - Any changes to the `spec-development-index.instructions.md`

2. **Write tests** — In this order:
   - **Unit tests** (`#[cfg(test)]` in the same file) for internal logic
   - **Integration tests** (`tests/int/` or `tests/e2e/`) for public API contracts
   - **Property tests** (`#[cfg(feature = "proptest")]`) for determinism,
     idempotency, and round-trip behavior
   - **Demo examples** (for mediapm) validate the full pipeline

3. **Implement** — Code against the spec and tests. Verify all tests pass
   before moving to the next step.

4. **Update the coverage matrix** — Mark spec items as covered
   (🟢), partial (🟡), or uncovered (🔴) in
   `/memories/session/spec-test-coverage.md`.

## When Fixing a Bug

1. **Write a failing test** that reproduces the bug — this test goes into the
   relevant `tests/` directory or `#[cfg(test)]` block
2. **Fix the implementation** — run the test suite to confirm the fix
3. **Verify no regressions** — run `cargo test --no-fail-fast` and compare
   against the baseline in `/memories/repo/pre-existing-test-failures.json`
4. **Add a spec entry** if the bug revealed a gap in `AGENTS.md`

## When Adding a New Managed Tool

Follow the step-by-step guide in `src/mediapm/AGENTS.md` (section:
"Adding a New Managed Tool"). The TL;DR is:

1. Spec first — document the contract
2. Test first — write provider/preset/workflow tests
3. Implement provider → preset → workflow
4. Register in all dispatchers
5. Integration test end-to-end

## Coverage Tracking

The spec-to-test coverage matrix (`/memories/session/spec-test-coverage.md`)
maps each spec item to its test status. Update it when:

- A new spec item is added
- A new test is written that covers a spec item
- A spec item becomes stale or is removed

## Validation Gates

| Gate | What it validates | Frequency |
|------|-------------------|-----------|
| Pre-commit (`prek`) | `cargo fmt`, linting, basic checks | Every commit |
| Selective tests | `cargo test -p <crate>` for iterating | During development |
| Full workspace | `cargo test --no-fail-fast` | Before push |
| Demos | `cargo run --example mediapm_demo` (and _online) | Before push |
| Coverage review | Compare spec items vs test status | Per-release |
