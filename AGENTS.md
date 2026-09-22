# Project Guidelines

## Repository Shape

- Root `AGENTS.md` is the workspace-wide source of truth. Do not add `.github/copilot-instructions.md`.
- This is a Rust workspace organized around mediapm's crate architecture. Keep guidance aligned to concrete files and current implementation state.
- Put file-type and workflow rules in `.agents/instructions/*.instructions.md`, reusable workflows in `.agents/prompts/*.prompt.md`, skill assets in `.agents/skills/<skill>/`.
- Every crate directory ships an `AGENTS.md` with crate-local guidance.
- Workspace member crates under `src/`:
  - `src/mediapm-cas/` (CAS)
  - `src/mediapm-conductor/` (Conductor)
  - `src/mediapm-conductor-builtins/` (parent, has its own `AGENTS.md`) and its `*/` builtins (each with an `AGENTS.md`)
  - `src/mediapm/` (mediapm application)
  - `src/mediapm-utils/` (shared utilities for builtins)
  - `tests/` (repository script integration tests, package `mediapm-tests`)
- Integration tests live with workspace crates (e.g. `src/mediapm/tests/`). Use one shared harness shape: top-level `tests/mod.rs` as the entrypoint, grouped submodules under `tests/e2e/`, `tests/int/`, `tests/prop/`.

## Quick Start

- **CAS**: `InMemoryCas`/`FileSystemCas`, `put(bytes)` → hash, `get(hash)` → bytes.
- **Conductor**: write `conductor.ncl`, create `Conductor`, call `run_workflow("name")`.
- **Builtins**: CLI (`mediapm-conductor-builtin-echo --arg message "hi" --arg output stdout`) or Rust API (`BTreeMap<String, String>` args + optional payload bytes).
- **MediaPM**: write `mediapm.ncl`, create `MediaPmService`, call `sync_library()`.
- **Tests/build**: `cargo test -p <crate>`; `cargo build-pkg <crate>`; full validation via `cargo fmt-check`, `cargo clippy-all`, `cargo test-all`.
- Per-crate `src/*/AGENTS.md` and `.agents/instructions/` carry contracts and edge cases.

## Architecture

- Agent customization is file-driven: `opencode.jsonc` registers `.agents/instructions/**/*.md` and `.agents/skills/`; `.opencode/` mirrors OpenCode config; `.vscode/settings.json` defines terminal auto-approve and editor behavior.
- Repository automation: `.github/workflows/ci.yml`, `.github/dependabot.yml`, `.commitlintrc.mjs`.
- Formatting and newline behavior: `.editorconfig`, `.gitattributes`, `.markdownlint.jsonc`, `.agents/.markdownlint.jsonc`.

## Core Engineering Contract

See `mediapm-architecture.instructions.md` for the full cross-crate engineering principles (simplicity first, performance, functional core, incremental by default, actor concurrency, type-system invariants, documentation as API contract).

The `Option` at configuration boundaries rule is canonically stated here: user-facing config types use plain values with serde defaults, not `Option<T>`. All serde defaults live in `src/mediapm/src/config/defaults.rs`. See `nickel.instructions.md` ("Contract patterns") for the Nickel-side equivalent.

Git safety: NEVER run `git reset` (especially `--hard` or `--keep`). Use `git revert` to undo published changes, or `git restore` to discard working-tree changes.

Technology baseline: `ractor` (actor/orchestration), `blake3` (hashing), `futures` (+ `async-trait`), `tracing` + `tracing-subscriber`, `serde` + deterministic `serde_json`.

## Rust Architecture Snapshot

See `mediapm-architecture.instructions.md` for the full module-layer index, cross-crate invariants, and per-crate responsibilities. Key invariants from this file that are referenced elsewhere:

- `src/mediapm-conductor/`: `conductor.ncl` is user-owned intent; `conductor.generated.ncl` is machine-managed runtime state; unresolvable conflicts fail fast.
- `src/mediapm-conductor-builtins/`: Builtin contract stability rule: all builtins share the same input conventions. CLI uses Rust flag/option conventions with string values; API uses `BTreeMap<String, String>` args plus optional payload bytes.
- `src/mediapm/`: Composes CAS + Conductor. Do not add direct dependencies on individual builtin crates. Cross-crate invariants:

See `src/mediapm/AGENTS.md` for the full runtime path defaults, media schema rules, tool provisioning reference, conductor integration boundary, and example policy.

See `src/mediapm/AGENTS.md` for runtime path defaults, media schema rules, tool provisioning reference, conductor integration boundary, and example policy.

## SDD/TDD compliance

- Spec-to-test coverage is tracked in `.agents/coverage-matrix.md`. Update it when spec items or tests change.
- All new features follow spec-first, test-first implementation per `.agents/instructions/sdd-tdd-workflow.instructions.md`.
- Close high-priority test gaps before feature work in the same area.

## Build and Test

- Detect install, build, test, lint, format, type-check, and release commands from actual repository files instead of assuming a default stack.
- For Rust workflows, treat `Cargo.toml`, `.cargo/config.toml`, `rust-toolchain.toml`, `.github/workflows/ci.yml`, and `.agents/instructions/rust-workflow.instructions.md` as source-of-truth inputs.
- See `.agents/instructions/rust-workflow.instructions.md` for the canonical validation workflow: selective tests during development, demo runs before completion, full-workspace checks via `prek.toml` hooks.
- When a language, framework, task runner, or test system is present, add or refine focused instruction files for it rather than stuffing detailed rules into `AGENTS.md`.

## Dependency and Feature Discipline

- Keep dependency surfaces minimal and explicit: prefer existing workspace dependencies before introducing new crates; remove now-unused direct dependencies when refactors eliminate them; avoid parallel libraries solving the same concern in one crate.
- Keep optional behavior feature-gated at compile time: optional dependencies behind explicit Cargo features; avoid hidden feature fan-out through default features; prefer `default-features = false` for internal cross-crate dependencies unless one default behavior is intentionally required.
- When changing crate features, validate representative feature matrices with targeted `cargo check` invocations so minimal builds stay healthy.
- Prefer small, atomic commits for dependency/feature-surface changes so feature-boundary regressions are easy to audit and bisect.
- No JavaScript package-manager manifests or lockfiles (`package.json`, `package-lock.json`, `yarn.lock`, `pnpm-lock.yaml`, `bun.lock`, `node_modules/`, etc.) belong here; it is a pure Rust workspace. JS tooling (commitlint) runs only through self-provisioned CI actions and pre-commit hooks. Re-adding such files is a policy violation.

## CLI and API Parity

- For each crate exposing both a CLI binary and a reusable library API, keep behavior parity as an explicit contract: new CLI operations route through corresponding library/API entry points instead of duplicating core logic in `main.rs`; programmatic APIs expose the same validation and failure semantics as CLI paths; CLI-only argument ergonomics are acceptable, but capability gaps between CLI and API are not.
- When adding/renaming CLI operations, update tests so parsing + API-backed execution coverage protects the parity contract.

## Conventions

- Distinguish what is present today from what is only part of the intended template contract. Do not describe absent files as if they already exist.
- Treat this file and focused `.agents/instructions/*.instructions.md` files as the active implementation contract. Keep them in sync with code and avoid reviving deleted standalone planning documents.
- This is a multi-member Rust workspace with crate members under `src/`. Do not regress to bootstrap assumptions (single-crate `src/main.rs` with only minimal `Cargo.toml` + `rust-toolchain.toml`).
- When docs mention `application`, `configuration`, `domain`, `infrastructure`, `support`, treat them as conceptual layering terms unless matching directories are explicitly introduced.
- Before writing stack-specific guidance, inspect concrete evidence (manifests, lockfiles, source tree, scripts, CI workflows, editor settings, config files).
- See `.agents/instructions/rust-conventions.instructions.md` for Rustdoc/docstring depth requirements.
- When you detect a real stack, add instructions for it in a narrow, well-named instruction file whose `description` and `applyTo` target the relevant files.
- Prefer linking to canonical config files instead of copying large policy blocks into multiple customization files.
- Keep customization files narrowly scoped: repo-wide defaults in `AGENTS.md`, detailed file-specific guidance in `.agents/instructions/`.
- Commit headers use Conventional Commits with mandatory scope (`type(scope): subject`). Do not use crate/tool-prefixed headers like `mediapm: ...`, `conductor: ...`, `cas: ...`; put crate/tool identity in `scope`.
- Prefer updating `AGENTS.md` and `.agents/instructions/*.instructions.md` directly for durable repository policy.
- Do not deliberately rename examples/tests solely to force workspace-wide unique target names. Shared canonical names (e.g. `demo`) are allowed; use package-qualified invocations to disambiguate.
- Respect the repository newline policy: Markdown and shell scripts use LF; PowerShell and batch scripts use CRLF.
