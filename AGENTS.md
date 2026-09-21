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

Hard principles for all workspace crates:

- simplicity first;
- performance is a user-visible feature;
- functional core, imperative shell;
- incremental by default with explicit content-addressed cache keys;
- async I/O/orchestration with runtime adapters (Tokio default);
- actor-first concurrency with explicit supervision behavior;
- type-system-enforced invariants where practical; prefer typing-enforcement over repeated runtime validation;
- resolve `Option` at configuration boundaries: user-facing config types use plain values with serde defaults, not `Option<T>`. Optional semantics resolve at the serde boundary so downstream code never handles `Option`. All serde defaults live in `src/mediapm/src/config/defaults.rs` — field-level `#[serde(default = "...")]` must reference a `defaults::` function, not inline literals.
  - Ban sentinel / empty-string-as-`None`. A plain `String` must never encode "unset" via a reserved sentinel such as `""`. If a field can be absent, it stays `Option` on the boundary struct only; the resolved type holds a real value.
  - Defaults are real or context-derived. When no reasonable default exists at the serde boundary, the field is `Option` on the boundary struct and the conversion into the resolved form receives the missing value explicitly (a base path, env value, or another resolved field) as an argument to the `From`/conversion function. The conversion never fabricates a sentinel. If no such information exists, the field is required and the document must supply it (fail fast).
  - Exceptions require a docstring, and only "testing only" is valid. Any deviation from the no-`Option` rule MUST carry a docstring stating it is an exception with that reason (typically `#[serde(skip)]`, doc-hidden). Any other reason is rejected; such a field must follow the boundary/`From` pattern or be a required input.
- pragmatic macro usage (reduce boilerplate, do not hide critical flow);
- documentation is part of the API contract.

Git safety: NEVER run `git reset` (especially `--hard` or `--keep`). Use `git revert` to undo published changes, or `git restore` to discard working-tree changes.

Technology baseline: `ractor` (actor/orchestration), `blake3` (hashing), `futures` (+ `async-trait`), `tracing` + `tracing-subscriber`, `serde` + deterministic `serde_json`.

Performance engineering loop is mandatory: profile, hypothesize, optimize, benchmark, keep-or-revert based on evidence. Hot-path expectations: prefer contiguous data layouts and bounded allocations; avoid hidden clones and tiny-syscall loops; keep async handlers non-blocking and route unavoidable blocking work through bounded worker boundaries.

Definition-of-done across crates: public APIs have integration coverage; major features have end-to-end coverage; determinism/idempotency behavior is tested; migration behavior is documented and auditable; performance claims are benchmark-backed; formatting/lint/tests pass in CI.

## Rust Architecture Snapshot

- `src/mediapm-cas/` provides the CAS identity model and async API contracts (CAS topology visualization also lives here).
- `src/mediapm-conductor/` provides the orchestration state model and persistence-merge logic. Cross-crate invariants:
  - `conductor.ncl` is user-owned intent; `conductor.generated.ncl` is machine-managed runtime state; unresolvable conflicts fail fast.
  - All three config documents carry explicit top-level numeric `version` markers; the runtime state document is volatile-only.
  - Builtin tool definitions in persisted config are strict (`kind`, `name`, `version` only); extra fields are rejected on decode.
  - Instance identity excludes content-map payload details and merged persistence flags; output persistence merged across equivalent calls: `save` uses AND, `force_full` uses OR.
  - `tools.<tool>.runtime.content_map` keys are sandbox-relative; absolute and path-traversal entries are rejected; separate entries must not overwrite the same target path.
  - Pure workflows may auto-recover from CAS integrity failures (warn + drop + retry once); impure workflows also fail without auto-retry unless `retry_impure` is enabled (via `runtime.retry_impure` in `conductor.ncl`/`mediapm.ncl`, `--retry-impure` CLI flag, or `RunWorkflowOptions.retry_impure`). See `src/mediapm-conductor/AGENTS.md` for the full config document model, tool schema invariants, template syntax contract, and versioned schema policy.
- `src/mediapm-conductor-builtins/` provides versioned built-in tool contracts (`echo`, `fs`, `import`, `export`, `archive`). Builtin runtime behavior lives in these crates (not inline in `src/mediapm-conductor`), and each builtin crate stays independently runnable via its own binary target. Builtin contract stability rule: all builtins share the same input conventions. CLI uses normal Rust flag/option conventions with all argument values as strings; API input uses `BTreeMap<String, String>` args plus optional raw payload bytes. A builtin CLI may optionally define one default option key so one value can be provided without spelling the option key, but explicit keyed input must remain supported and map to the same API key. Builtin API and CLI execution must fail fast on undeclared argument/input keys, missing required keys, and invalid key combinations; do not silently ignore unknown input. For builtins whose success is pure (a deterministic function of inputs), the success payload may be deterministic bytes or `BTreeMap<String, String>`. Impure builtins may communicate success through side effects. CLI failures use ordinary Rust error types; do not encode failures as fake success payloads.
- `src/mediapm/` composes CAS + Conductor into the media-facing API and CLI scaffold. It depends directly on `mediapm-cas` and `mediapm-conductor`; do not add direct dependencies on individual `src/mediapm-conductor-builtins/*` crates. Cross-crate invariants:
  - Runtime state root defaults to `.mediapm/`; `mediapm.ncl` `runtime` may override `mediapm_dir`, paths, and `inherited_env_vars`.
  - Media-source entries may include optional `title` and `description`; add flows auto-populate them from source metadata when available.
  - When `mediapm` invokes conductor, pass grouped runtime-storage paths so conductor volatile writes target `<mediapm_dir>/state.conductor.ncl` (not standalone `.conductor/state.ncl`); `mediapm` machine-managed state persists at `<mediapm_dir>/state.json` (JSON always-write) and uses `runtime.media_state_config` for overrides.
  - Keep cache domains strictly separated: managed-tool download reuse is the user-level cache (`<os-cache-dir>/mediapm/cache/` for mediapm-driven runs), while conductor tool-content materialization is workspace-scoped under `<mediapm_dir>/tools/` (or `<conductor_dir>/tools/` standalone). Never treat these as interchangeable.
  - Materialization follows stage → verify → commit semantics (read-only after commit, NFD-only filenames, reserved-char rejection, configured link/write order). See `.agents/instructions/mediapm-architecture.instructions.md`.
  - `yt-dlp`, `media-tagger`, and `rsgain` defaults: see `.agents/instructions/preset-dispatch.instructions.md`. Managed `media-tagger` cache defaults to `<mediapm_dir>/cache` (shared CAS/index layout).
  - Schema exports default to `<mediapm_dir>/config/mediapm` for mediapm; standalone conductor defaults to `<conductor_dir>/config/conductor`; mediapm-driven conductor defaults to `<mediapm_dir>/config/conductor`.
  - Dependency keys use bare tool IDs (e.g., `dependencies.ffmpeg = "inherit"`). Each tool accepts only keys matching its registered `dependency_types()`; see `.agents/instructions/error-codes.instructions.md` for the error code catalog.
  - Managed-tool dependency classes (same-step companion vs cross-step) are strictly separated, but a single dependency may carry both roles. Dependencies are direct-only and non-transitive; same-step companion payloads are inlined under the reserved `deps/<mediapm_tool_id>/` prefix and never env-exposed. See `.agents/instructions/tool-sync-tool-config.instructions.md`.
  - yt-dlp `output_variants` values must not embed `format`; any explicit format selector belongs in step `options.format`.
  - output-variant values are object-driven: `kind` controls default file-vs-folder capture policy, and optional `capture_kind = "file"|"folder"` may override that default per variant.
  - output-variant kind naming is strict (no legacy aliases): `primary` for main transform outputs; yt-dlp folder-family kinds use plural names (`subtitles`, `thumbnails`, `links`, `chapters`) while file-family kinds stay singular (`primary`, `description`, `infojson`, `comment`, `archive`, `annotation`, playlist file sidecars).
  - yt-dlp output-variant `langs` is an optional capture-filter hint for subtitle-family artifacts only; downloader language selection stays step-option owned via `options.sub_langs`.
  - Hierarchy uses an ordered node-array schema (`hierarchy = [ { ... } ]`) with recursive `children` and explicit kinds: `folder` (default), `media`, `media_folder`, `playlist`; legacy flat-map and `"/kind"` forms are unsupported. `media` uses singular `variant`, `media_folder` uses plural `variants` and optional `rename_files`; hierarchy `id` is optional and must be unique when provided. `media_id` is optional, but `media`/`media_folder` require one effective non-empty value (direct or inherited). Playlist `ids` resolve by ordered id entries targeting hierarchy-node ids, accept string shorthand and object entries (`{ id, path }`), and remain file-leaf entries.
  - Media-source entries must not define `media.<id>.id` overrides; playlist membership is owned by hierarchy-node ids only.
  - Hierarchy directory entries may define optional ordered `rename_files = [{ pattern, replacement }, ...]` regex rewrites applying to extracted folder file members; file hierarchy targets keep `rename_files` empty.
  - Managed executable payloads keep all-platform `content_map` keys with `${context.os}` command selectors. See `.agents/instructions/tool-sync-3-phase-provisioning.instructions.md`.

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
