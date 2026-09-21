---
description: "Use when editing Rust source, Cargo config, or Rust CI validation in this repository."
name: "Rust Workflow Guidance"
applyTo: "**/*.rs, Cargo.toml, Cargo.lock, rust-toolchain.toml, .rustfmt.toml, .clippy.toml, .config/**/*.toml, .cargo/**/*.toml, .github/workflows/**/*.yml, .github/workflows/**/*.yaml"
---

# Rust Workflow Guidance

## Source-of-truth files

- `Cargo.toml` (package identity, dependency graph), `.cargo/config.toml` (aliases), `.config/nextest.toml` (runner), `rust-toolchain.toml` (channel/components), `rustfmt.toml`/`clippy.toml` (style/lint), `.github/workflows/ci.yml` (CI), `prek.toml` (pre-commit hooks).
- `AGENTS.md` + `.agents/instructions/*.instructions.md` for active architecture and implementation contract.
- Workspace members: `src/mediapm-cas/` (CAS), `src/mediapm-conductor/` (Conductor), `src/mediapm-conductor-builtins/*/` (built-ins), `src/mediapm/` (application).

## Validation workflow

When editing Rust source, validate with selective checks first:

- Prefer `cargo test -p <crate> <test_name>` for tight edit loops; use `cargo build-pkg <crate>` to avoid package-wide churn.
- Rely on `prek.toml` pre-commit hooks for fmt/type/clippy on commit rather than running them manually.
- libtest args (e.g. `--test-threads=8`) go AFTER `--`: `cargo test -p mediapm-utils --features progress -- --test-threads=8`.
- Stale-artifact gotcha: `passthrough_conductor_tool_run_help_is_routable` in `src/mediapm/src/main.rs` can fail spuriously from stale incremental artifacts affecting clap's `try_parse_from`; clean and rebuild with `cargo clean -p mediapm -p mediapm-conductor`.
- `prek.toml` runs full workspace validation on `git push`; manual `cargo fmt-check`/`clippy-all`/`test-all` are not required for normal submission.
- Required manual runtime verification before completion: `cargo run --package mediapm --example mediapm_demo` and `mediapm_demo_online`.
- For `src/mediapm/**` edits, avoid full online demo runs during normal development; reserve them for push/pre-push unless a reviewer asks for local runtime verification.
- If source or configs are incomplete, report gaps explicitly instead of inventing commands.

## Editing conventions

- Keep changes minimal, deterministic, aligned with the functional-core direction in active instruction files.
- Keep dependency/feature surfaces explicit: prefer existing workspace deps, remove unused direct deps, gate optional behavior behind Cargo features, avoid default-feature fan-out.
- Avoid hidden mutable state or databases unless explicitly requested.
- Keep stack-specific detail here rather than growing root `AGENTS.md`.
- Document touched Rust code with module-level `//!` and item-level `///` docs (semantics, invariants, side effects, error behavior).
- Do not assume bootstrap-template structure when changing workspace-wide guidance; verify real members first.

## Rust module split layout convention

- When a module outgrows one file, move `foo.rs` to `foo/mod.rs`, place submodules as `foo/<submodule>.rs`, and keep unit tests inline as `#[cfg(test)]` blocks (split into a themed `foo_<theme>.rs` sibling only past ~300 lines).
- In `foo/mod.rs` prefer `mod tests;` over `#[path = "..."]`; never keep both `foo.rs` and `foo/mod.rs`; keep `//!` docs on `foo/mod.rs`.
- After a split, run `cargo test -p <crate> <focused_test_name>` and `cargo build-pkg <crate>`.

## Example target naming convention

- All workspace examples must use the crate-name prefix to avoid Cargo filename-collision warnings: `<crate_name>_<example_name>.rs` (e.g. `cas_demo.rs`, `mediapm_demo_online.rs`).
- This ensures unique target names across the workspace when running `cargo build --all-targets`.
- When invoking examples, use the full target name: `cargo run --package cas --example cas_demo`, `cargo run --package conductor --example conductor_runtime_diagnostics`, `cargo run --package mediapm --example mediapm_demo_online`.
- All examples must follow this convention; enforce it during code review.

## Docstring requirements

See `.agents/instructions/rust-conventions.instructions.md` for Rustdoc/docstring depth requirements (module-level `//!`, item-level `///`, invariants, side effects, and error behavior for all touched items).

## Lint suppression policy

- Do not add bare suppression attributes (`#[allow(...)]` or `#![allow(...)]`) for rustc/clippy lints.
- Prefer direct code fixes for lint findings first.
- When suppression is truly unavoidable, use item-scoped `#[expect(<lint>, reason = "<substantive rationale>")]`.
  - Keep scope as narrow as possible (single item/block, never crate-wide).
  - The `reason` must explain _why the code shape is required now_, not just restate the lint name.
  - Good reasons reference concrete constraints such as platform behavior, API-shape compatibility, or orchestration-ordering invariants.
- Treat `#[expect(...)]` as temporary technical debt: remove it when refactors make the lint unnecessary, and investigate any `unfulfilled_lint_expectations` warning rather than suppressing it.
- For platform edge cases (e.g. `clippy::permissions_set_readonly_false`) and diagnostic-only numeric conversions (e.g. `clippy::cast_precision_loss`), include explicit safety/correctness boundaries in the `reason` string.

## Core architectural constraints

- Keep planner behavior pure and deterministic.
- Keep side effects concentrated in executor/infrastructure.
- Preserve sidecar invariants and migration-provenance semantics.
- Keep object-store writes and sidecar writes atomic.

## CLI/API parity contract

See `AGENTS.md` ("CLI and API Parity" section) for the canonical parity contract. New CLI operations should route through library/API entry points, and capability gaps between CLI and API are not acceptable.
