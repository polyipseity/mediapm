# Project Guidelines

## Repository Shape

- Rust workspace for mediapm. Root `AGENTS.md` is workspace-wide; do not add `.github/copilot-instructions.md`.
- File/workflow rules live in `.agents/instructions/*.instructions.md`; reusable workflows in `.agents/prompts/*.prompt.md` and mirrored `.opencode/commands/`; skills in `.agents/skills/<skill>/`. Every runtime crate has `src/*/AGENTS.md`; `src/mediapm-test-support/` is covered here.
- Crates: `mediapm-cas`, `mediapm-conductor`, `mediapm-conductor-builtins/*`, `mediapm`, `mediapm-{,cas-,conductor-}cli`, `mediapm-utils`, `tests/` (`mediapm-tests`). Integration tests use `tests/mod.rs` + `e2e|int|prop/`. Config: `opencode.jsonc`, `.vscode/settings.json`, `.github/workflows/`, `.commitlintrc.mjs`, `.editorconfig`, `.markdownlint*.jsonc`.

## Core Contract

- Read code/config before documenting behavior. Keep this file short; put detail in focused instruction files with narrow `applyTo`.
- Engineering: pure core/imperative shell, incremental, async I/O, ractor actors, strong types, deterministic serde. Baseline: `ractor`, `blake3`, `futures`/`async-trait`, `tracing`, `serde`.
- Config boundaries: no `Option` in user config; serde defaults in `src/mediapm/src/config/defaults.rs`. NEVER `git reset`; use `git revert` or `git restore`.
- Pointers: `mediapm-architecture.instructions.md`, `rust-workflow.instructions.md`, `sdd-tdd.instructions.md`, `state-and-versioning.instructions.md`, `tool-provisioning.instructions.md`, `temp-and-paths.instructions.md`, `workflow-runtime.instructions.md`.

## Key Invariants

- `mediapm-conductor`: `conductor.ncl` is user intent; `conductor.generated.ncl` is machine-managed; unresolvable conflicts fail fast.
- `mediapm-conductor-builtins`: stable builtin contract. CLI uses flag/option string args; API uses `BTreeMap<String, String>` args plus optional payload bytes.
- `mediapm`: composes CAS + Conductor. No direct dependency on individual builtin crates. See `src/mediapm/AGENTS.md`.
- Instance-key identity is v8 (`INSTANCE_KEY_VERSION` in `src/mediapm-conductor/src/instance_key.rs`); contract in `src/mediapm-conductor/AGENTS.md`.

## Workflow and Policy

- SDD/TDD: spec-first, test-first; coverage in `.agents/coverage-matrix.md`. Plans/specs in gitignored `.pi/plans/`; never create `docs/`.
- Validation: fast inner loop during edits; full/parity gates at checkpoints. Source of truth: `Cargo.toml`, `.cargo/config.toml`, `rust-toolchain.toml`, `.github/workflows/ci.yml`, `prek.toml`, `rust-workflow.instructions.md`.
- Dependencies/features: minimal explicit deps, remove unused direct deps, optional behavior behind Cargo features, `default-features = false` for internal cross-crate deps unless intentionally required. Validate representative feature matrices.
- CLI/API parity: CLI operations route through library/API entry points; no capability gaps. Add tests for parsing + API-backed execution.
- Commits: Conventional Commits with mandatory scope (`type(scope): subject`); crate/tool identity goes in scope, not `mediapm: ...` prefixes.
- Version sequences for persisted schemas/wire/domain-separator/key-version values are integers (`v1->v2->v3`); dotted revisions banned. SemVer package versions and upstream tool tags are out of scope. See `state-and-versioning.instructions.md`.
- Newlines: Markdown/shell LF; PowerShell/batch CRLF. No JS package-manager manifests/lockfiles in this Rust workspace.
