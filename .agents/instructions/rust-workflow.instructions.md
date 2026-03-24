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
- `src/` first-level folders (`application`, `configuration`, `domain`,
  `infrastructure`, `support`) for module boundaries.

## Validation workflow

- Preferred local checks:
  - `cargo fmt-check`
  - `cargo clippy-all`
  - `cargo test-all`
- Equivalent explicit forms are acceptable when aliases are unavailable.
- If source or configs are incomplete, report gaps explicitly instead of inventing commands.

## Editing conventions

- Keep changes minimal, deterministic, and aligned with the functional-core direction in `PLAN.md`.
- Avoid adding hidden mutable state or introducing databases unless explicitly requested.
- Keep stack-specific detail in this file rather than growing root `AGENTS.md`.
- Keep public Rust APIs fully documented with module-level `//!` and item-level
  `///` docs so newcomers can navigate the codebase quickly.

## Core architectural constraints

- Keep planner behavior pure and deterministic.
- Keep side effects concentrated in executor/infrastructure.
- Preserve sidecar invariants and migration-provenance semantics.
- Keep object-store writes and sidecar writes atomic.
