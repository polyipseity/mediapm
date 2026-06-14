---
description: "Use when editing Rust source, Cargo config, or Rust CI validation in this repository."
name: "Rust Workflow Guidance"
applyTo: "**/*.rs, Cargo.toml, Cargo.lock, rust-toolchain.toml, rustfmt.toml, clippy.toml, .cargo/**/*.toml, .github/workflows/**/*.yml, .github/workflows/**/*.yaml"
---

# Rust Workflow Guidance

## Scope

- Apply this guidance when working on Rust source or Rust-specific tooling.
- Treat this repository as an implemented mediapm MVP with functional-core-style
  architecture and sidecar-backed state.

## Source-of-truth files

- `Cargo.toml` for package identity and dependency graph.
- `.cargo/config.toml` for local cargo behavior and aliases.
- `rust-toolchain.toml` for toolchain/channel/components.
- `rustfmt.toml` and `clippy.toml` for style and lint policy.
- `.github/workflows/ci.yml` for canonical CI validation behavior.
- `prek.toml` for local git hooks (pre-commit framework configured as TOML).
- `AGENTS.md` + `.agents/instructions/*.instructions.md` for active
  architecture and implementation contract.
- `src/` workspace member crates for current module boundaries:
  - `src/mediapm-cas/` (CAS)
  - `src/mediapm-conductor/` (Conductor)
  - `src/conductor-builtins/*/` (conductor built-ins)
  - `src/mediapm/` (mediapm application)

If planning docs mention `application`, `configuration`, `domain`,
`infrastructure`, and `support`, treat them as conceptual layering terms unless
matching directories are explicitly introduced.

## Validation workflow

When editing Rust source, validate changes with selective checks first:

- **During development** (recommended for speed — these run in seconds):
  - Prefer selective individual tests (`cargo test -p <crate> <test_name>`) for tight edit loops.
  - Use focused package builds (`cargo build-pkg <crate>`) and avoid package-wide test churn unless a specific issue requires it.
  - Rely on `prek.toml` pre-commit hooks for formatting, type checking, and clippy on commit rather than running those commands manually.
  - Examples:
    - `cargo test -p mediapm source_metadata_falls_back_to_uri_when_unavailable`
    - `cargo build-pkg mediapm-conductor` builds only mediapm-conductor
    - `cargo test -p mediapm-cas locator_parser_expands_environment_variables`
  - See `.cargo/config.toml` for alias definitions

- **Before submitting**:
  - `prek.toml` handles full workspace validation on `git push` via pre-push hooks.
  - Local manual runs of `cargo fmt-check`, `cargo clippy-all`, or `cargo test-all` are not required for normal submission because the hooks already enforce those gates.
  - The only required manual runtime verification before completion is:
    - `cargo run --package mediapm --example mediapm_demo`
    - `cargo run --package mediapm --example mediapm_demo_online`

- **CI parity reference** (`.github/workflows/ci.yml`):
  - CI runs: `cargo test-all`, `cargo clippy-all`, `cargo fmt-check`, `cargo build-all`
  - CI also runs: `cargo bin rumdl check` (project-specific check)

- Equivalent explicit forms are acceptable when aliases are unavailable:
  - `cargo test -p mediapm --all-targets --all-features` → same as `cargo test-pkg mediapm`
  - `cargo clippy -p mediapm --all-targets --all-features` → same as `cargo clippy-pkg mediapm` when workspace lints are enabled

- For edits under `src/mediapm/**`, avoid full online demo runs during normal development.
  - Run selective tests only while iterating.
  - Reserve full integration/demo runs for push/pre-push workflows handled by hooks/CI unless a reviewer explicitly asks for local runtime verification.

- If source or configs are incomplete, report gaps explicitly instead of inventing commands.

### After module splits or cross-crate refactors

When refactoring touches multiple crates or splits large modules:

1. Run targeted checks on each affected crate first
2. Then rely on `prek.toml` pre-push hooks for full-workspace validation before pushing, rather than running manual full workspace commands.

## Git hooks and pre-commit

This repository uses the pre-commit framework (configured via `prek.toml`) to manage local git hooks. The hooks run automatically on `git commit` and `git push` to catch issues early and auto-fix formatting:

- **pre-commit stage** (on `git commit`):
  - `check-case-conflict`
  - `check-executables-have-shebangs`
  - `check-illegal-windows-names`
  - `check-merge-conflict`
  - `check-shebang-scripts-are-executable`
  - `check-symlinks`
  - `destroyed-symlinks`
  - `detect-private-key`
  - `end-of-file-fixer`
  - `fix-byte-order-marker`
  - `name-tests-test`
  - `trailing-whitespace`
  - `rumdl-fmt`
  - `fmt` (`cargo fmt` on changed `.rs` files)
- **commit-msg stage**: runs `commitlint`
- **pre-push stage** (on `git push`): runs workspace `cargo-check`, `clippy`, and `test`

Treat these hooks as the canonical lint/format/check gate. During normal coding, prefer selective test/build runs and rely on commit/push hooks for full lint/format/check execution.

To install or update hooks locally, run:

```bash
pre-commit install
```

You can also run hooks manually:

```bash
pre-commit run --all-files          # Run all hooks
pre-commit run cargo-fmt            # Run a specific hook
```

To temporarily skip hooks during a commit, use `SKIP`:

```bash
SKIP=cargo-test git commit -m "message"
```

## Editing conventions

- Keep changes minimal, deterministic, and aligned with the repository's
  functional-core direction documented in active instruction files.
- Keep dependency and feature surfaces explicit:
  - prefer existing workspace dependencies before adding new crates,
  - remove direct dependencies that become unused after refactors,
  - keep optional behavior compile-time gated behind explicit Cargo features,
  - avoid hidden feature fan-out through default features.
- Avoid adding hidden mutable state or introducing databases unless explicitly requested.
- Keep stack-specific detail in this file rather than growing root `AGENTS.md`.
- Keep Rust code fully documented with module-level `//!` and item-level
  `///` docs for public and private items in touched files.
- Prefer detailed docstrings over brief labels; include semantics,
  invariants, side effects, and error behavior.
- Do not assume bootstrap-template structure (`Cargo.toml` +
  `rust-toolchain.toml` + single `src/main.rs`) when changing workspace-wide
  guidance; verify the real workspace members first.

## Rust module split layout convention

- When a module grows and is split into multiple files, use folder-module
  layout by default:
  - move `foo.rs` to `foo/mod.rs`,
  - place submodules as `foo/<submodule>.rs`,
  - place unit tests as `#[cfg(test)]` blocks inline in the source file they
    test. If the inline block exceeds ~300 lines, split into a themed sibling
    file `foo_<theme>.rs` declared with `#[cfg(test)] mod foo_<theme>;`.
- In `foo/mod.rs`, prefer conventional declarations (`mod tests;`) instead of
  `#[path = "..."]` for routine in-folder test/module wiring.
- Do not keep both `foo.rs` and `foo/mod.rs` for the same module.
- Keep module-level docs (`//!`) on `foo/mod.rs` after the move so crate/module
  purpose stays discoverable.
- After a split, run targeted validation:
  - `cargo test -p <crate> <focused_test_name>` (affected behavior)
  - `cargo build-pkg <crate>` (affected crate build)

## Example target naming convention

- All workspace examples must use the crate-name prefix to avoid Cargo filename-collision warnings.
- Naming pattern: `<crate_name>_<example_name>.rs`
  - Examples: `cas_demo.rs`, `conductor_bootstrap_defaults.rs`, `mediapm_demo_online.rs`
- This ensures unique target names across the workspace when running `cargo build --all-targets`.
- When invoking examples, use the full target name:
  - `cargo run --package cas --example cas_demo`
  - `cargo run --package conductor --example conductor_runtime_diagnostics`
  - `cargo run --package mediapm --example mediapm_demo_online`
- All examples must follow this convention; enforce it during code review.

## Docstring completion bar

- When editing `*.rs`, treat documentation as part of definition-of-done.
- For touched files, document:
  - module purpose (`//!`),
  - top-level constants and types,
  - helper functions and internal state structures,
  - tests with explicit guarantee statements.
- Avoid shallow docs that only rename symbols; write newcomer-oriented
  explanations that clarify intent and boundaries.

## Lint suppression policy

- Do not add bare suppression attributes (`#[allow(...)]` or `#![allow(...)]`)
  for rustc/clippy lints.
- Prefer direct code fixes for lint findings first.
- When suppression is truly unavoidable, use item-scoped
  `#[expect(<lint>, reason = "<substantive rationale>")]`.
  - Keep scope as narrow as possible (single item/block, never crate-wide).
  - The `reason` must explain _why the code shape is required now_, not just
    restate the lint name.
  - Good reasons reference concrete constraints such as platform behavior,
    API-shape compatibility, or orchestration-ordering invariants.
- Treat `#[expect(...)]` as temporary technical debt:
  - remove it when refactors make the lint unnecessary,
  - and investigate any `unfulfilled_lint_expectations` warning rather than
    suppressing it.
- For platform edge cases (for example
  `clippy::permissions_set_readonly_false`) and diagnostic-only numeric
  conversions (for example `clippy::cast_precision_loss`), include explicit
  safety/correctness boundaries in the `reason` string.

## Core architectural constraints

- Keep planner behavior pure and deterministic.
- Keep side effects concentrated in executor/infrastructure.
- Preserve sidecar invariants and migration-provenance semantics.
- Keep object-store writes and sidecar writes atomic.

## CLI/API parity contract

- For crates that expose both a CLI binary and a library API, keep behavior
  parity as an explicit invariant:
  - new CLI operations should route through library/API entry points,
  - API validation and failure semantics should match CLI-backed behavior,
  - CLI-only ergonomic sugar is acceptable, capability gaps are not.
- When adding or renaming CLI operations, update tests so parser behavior and
  API-backed execution paths are both covered.

## Specification references

- Consolidated technical specification:
  `.agents/instructions/crate-specifications.md`.
- Edge-case and ambiguity analysis:
  `.agents/instructions/elaboration-pass-edge-cases.md`.
