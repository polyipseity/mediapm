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

- Preferred local checks:
  - `cargo fmt-check`
  - `cargo clippy-all`
  - `cargo test-all`
- Equivalent explicit forms are acceptable when aliases are unavailable.
- CI parity reference (`.github/workflows/ci.yml`):
  - `cargo test-all`
  - `cargo clippy-all`
  - `cargo fmt-check`
  - `cargo build-all`
  - plus `cargo bin rumdl check` as an additional project check.
- If source or configs are incomplete, report gaps explicitly instead of inventing commands.

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
- After a split, re-run `cargo fmt-check`, `cargo clippy-all`, and
  `cargo test-all` to catch stale paths/imports.

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
