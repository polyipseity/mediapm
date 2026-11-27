# Project Guidelines

## Repository Shape

- Rust workspace for mediapm; root `AGENTS.md` workspace-wide, no `.github/copilot-instructions.md`.
- Rules in `.agents/instructions/*.instructions.md`, workflows in `.agents/prompts/*.prompt.md` + mirrored `.opencode/commands/`, skills in `.agents/skills/<skill>/`. Every runtime crate has `src/*/AGENTS.md`; `src/mediapm-test-support/` covered here.
- Crates: `mediapm-cas`, `mediapm-conductor`, `mediapm-conductor-builtins/*`, `mediapm`, `mediapm-{,cas-,conductor-}cli`, `mediapm-utils`, `tests/` (`mediapm-tests`). Integration tests `tests/mod.rs` + `e2e|int|prop/`; config `opencode.jsonc`, `.vscode/settings.json`, `.github/workflows/`, `.commitlintrc.mjs`, `.editorconfig`, `.markdownlint*.jsonc`.

## Core Contract

- Read code/config before documenting; keep this file short, detail in focused instructions with narrow `applyTo`.
- Engineering: pure core/imperative shell, incremental, async I/O, `ractor`, strong types, deterministic serde. Baseline `ractor`, `blake3`, `futures`/`async-trait`, `tracing`, `serde`.
- No `Option` in user config; serde defaults in `src/mediapm/src/config/defaults.rs`. NEVER `git reset`; use `git revert` or `git restore`.
- Pointers: `mediapm-architecture.instructions.md`, `rust-workflow.instructions.md`, `sdd-tdd.instructions.md`, `state-and-versioning.instructions.md`, `tool-provisioning.instructions.md`, `temp-and-paths.instructions.md`, `workflow-runtime.instructions.md`.

## Key Invariants

- `mediapm-conductor`: `conductor.ncl` user intent, `conductor.generated.ncl` machine-managed, unresolvable conflicts fail fast.
- `mediapm-conductor-builtins`: stable contract; CLI flag/option string args, API `BTreeMap<String, String>` args + optional payload bytes.
- `mediapm` composes CAS + Conductor, no direct dependency on individual builtin crates (`src/mediapm/AGENTS.md`).
- Instance-key identity v8: `INSTANCE_KEY_VERSION` in `src/mediapm-conductor/src/instance_key.rs`, contract in `src/mediapm-conductor/AGENTS.md`.

## Workflow and Policy

- SDD/TDD spec-first/test-first; coverage in `.agents/coverage-matrix.md`; plans in gitignored `.pi/plans/`, never `docs/`.
- Validation: fast inner loop, full/parity gates at checkpoints; source of truth `Cargo.toml`, `.cargo/config.toml`, `rust-toolchain.toml`, `.github/workflows/ci.yml`, `prek.toml`, `rust-workflow.instructions.md`.
- Dependencies: minimal explicit deps, remove unused, optional behavior behind features, `default-features = false` for internal cross-crate deps unless required; validate feature matrices.
- CLI/API parity: CLI through library/API entry points, no capability gaps; test parsing + API execution.
- Commits `type(scope): subject`; crate/tool in scope, not `mediapm: ...`.
- Persisted schema/wire/domain-separator/key versions are integers (`v1->v2->v3`), dotted revisions banned; SemVer/upstream tags out of scope (`state-and-versioning.instructions.md`).
- Newlines Markdown/shell LF, PowerShell/batch CRLF; no JS package-manager manifests/lockfiles.
