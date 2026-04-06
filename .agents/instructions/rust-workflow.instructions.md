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
- `PLAN.md` for active architecture and implementation contract.
- `src/` phase crates for current module boundaries:
  - `src/cas/` (Phase 1)
  - `src/conductor/` (Phase 2)
  - `src/conductor-builtins/*/` (Phase 2 built-ins)
  - `src/mediapm/` (Phase 3)

If planning docs mention `application`, `configuration`, `domain`,
`infrastructure`, and `support`, treat them as conceptual layering terms unless
matching directories are explicitly introduced.

## Validation workflow

When editing Rust source, validate changes with targeted checks first:

- **During development** (recommended for speed — these run in seconds):
  - `cargo test-pkg <crate>` — test only the affected crate(s)
  - `cargo clippy-pkg <crate>` — lint only the affected crate(s)
  - `cargo build-pkg <crate>` — build only the affected crate(s)
  - Examples:
    - `cargo test-pkg mediapm` runs only mediapm tests
    - `cargo clippy-pkg mediapm-conductor` lints only mediapm-conductor
    - `cargo test-pkg mediapm-cas` for CAS-specific validation
  - See `.cargo/config.toml` for the alias definitions

- **Before submitting (pre-push validation)** — validate full workspace:
  - `cargo fmt-check` (checks all Rust file formatting)
  - `cargo clippy-all` (full workspace lint with strict warnings)
  - `cargo test-all` (full workspace tests)
  - These are intentionally slow and designed for CI/pre-push gates

- **CI parity reference** (`.github/workflows/ci.yml`):
  - CI runs: `cargo test-all`, `cargo clippy-all`, `cargo fmt-check`, `cargo build-all`
  - CI also runs: `cargo bin rumdl check` (project-specific check)

- Equivalent explicit forms are acceptable when aliases are unavailable:
  - `cargo test -p mediapm --all-targets --all-features` → same as `cargo test-pkg mediapm`
  - `cargo clippy -p mediapm --all-targets --all-features -- -D warnings` → same as `cargo clippy-pkg mediapm`

- If source or configs are incomplete, report gaps explicitly instead of inventing commands.

### After module splits or cross-crate refactors

When refactoring touches multiple crates or splits large modules:

1. Run targeted checks on each affected crate first
2. Then run full-workspace validation before pushing:
   - `cargo fmt-check`
   - `cargo clippy-all`
   - `cargo test-all`

## Editing conventions

- Keep changes minimal, deterministic, and aligned with the functional-core direction in `PLAN.md`.
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
  - place module-local unit tests in `foo/tests.rs`.
- In `foo/mod.rs`, prefer conventional declarations (`mod tests;`) instead of
  `#[path = "..."]` for routine in-folder test/module wiring.
- Do not keep both `foo.rs` and `foo/mod.rs` for the same module.
- Keep module-level docs (`//!`) on `foo/mod.rs` after the move so crate/module
  purpose stays discoverable.
- After a split, run targeted validation:
  - `cargo fmt-check` (all files)
  - `cargo test-pkg <crate>` (affected crate tests)
  - `cargo clippy-pkg <crate>` (affected crate lint)

## Docstring completion bar

- When editing `*.rs`, treat documentation as part of definition-of-done.
- For touched files, document:
  - module purpose (`//!`),
  - top-level constants and types,
  - helper functions and internal state structures,
  - tests with explicit guarantee statements.
- Avoid shallow docs that only rename symbols; write newcomer-oriented
  explanations that clarify intent and boundaries.

## Core architectural constraints

- Keep planner behavior pure and deterministic.
- Keep side effects concentrated in executor/infrastructure.
- Preserve sidecar invariants and migration-provenance semantics.
- Keep object-store writes and sidecar writes atomic.
