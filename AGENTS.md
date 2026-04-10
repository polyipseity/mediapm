# Project Guidelines

## Repository Shape

- Root `AGENTS.md` is the workspace-wide source of truth. Do not add
  `.github/copilot-instructions.md`.
- Treat this repository as a Rust workspace that is evolving toward the
  mediapm phase architecture. Keep guidance aligned to concrete files and
  current implementation state.
- Keep this file short and durable. Put file-type and workflow-specific rules
  in `.agents/instructions/*.instructions.md`, reusable workflows in
  `.agents/prompts/*.prompt.md`, and skill assets in `.agents/skills/<skill>/`.
- `src/` now contains workspace member crates:
  - `src/cas/` (Phase 1)
  - `src/conductor/` (Phase 2)
  - `src/conductor-builtins/*/` (Phase 2 built-ins)
  - `src/mediapm/` (Phase 3)
- Integration tests currently live with the phase crates (for example,
  `src/mediapm/tests/`).
  Prefer one shared harness shape across phase crates:
  - top-level `tests/tests.rs` as the integration harness,
  - grouped submodules under `tests/e2e/`, `tests/int/`, and `tests/prop/`.

## Architecture

- Agent customization is file-driven:
  - `opencode.json` registers `.agents/instructions/**/*.md` and
    `.agents/skills/`
  - `.opencode/commands/` mirrors prompt workflows for OpenCode consumers
  - `.vscode/settings.json` defines terminal auto-approve patterns and editor
    behavior
- Repository automation currently lives in:
  - `.github/workflows/ci.yml` for CI scaffolding
  - `.github/dependabot.yml` for dependency-update scope
  - `.commitlintrc.mjs` for commit message policy
- Formatting and newline behavior come from `.editorconfig`, `.gitattributes`,
  `.markdownlint.jsonc`, and `.agents/.markdownlint.jsonc`.

## Rust Architecture Snapshot

- `src/cas/` provides the Phase 1 CAS identity model and async API contracts.
  CAS topology visualization implementation also belongs in this crate.
- `src/conductor/` provides the Phase 2 orchestration state model and
  persistence-merge logic.
  Conductor schema/runtime invariants include:
  - builtin tool definitions in persisted config remain strict
    (`kind`, `name`, `version` only),
  - `conductor.ncl`, `conductor.machine.ncl`, and the resolved runtime state
    document path (default `.conductor/state.ncl`) always carry explicit
    top-level numeric `version` markers,
  - `conductor.ncl` and `conductor.machine.ncl` may define grouped runtime
    storage path fields under one `runtime` record
    (`runtime.conductor_dir`, `runtime.state_config`,
    `runtime.cas_store_dir`),
  - the resolved runtime state document path (default
    `.conductor/state.ncl`) is volatile-only and may define only
    `version`, `impure_timestamps`, and `state_pointer`,
  - orchestration-state snapshots carry explicit top-level `version`; each
    instance stores immutable `tool_name`; executable `metadata` remains
    `ToolSpec`-shape while builtin `metadata` persists only
    `kind`/`name`/`version` and decode rejects extra builtin metadata fields;
    executable tool input defaults are runtime-only and must be declared under
    `tool_configs.<tool>.input_defaults` (tool-level
    `tools.<tool>.inputs.<input>.default` is unsupported);
    output persistence stored in orchestration state is the effective merged
    policy across duplicate equivalent tool calls (`save`: AND,
    `force_full`: OR); instance identity is derived only from
    `tool_name`, `metadata`, optional `impure_timestamp`, and `inputs` keyed
    by CAS hash references,
  - workflow specs may include optional informational `name` and
    `description`; workflow identity remains the workflow map key,
    and runtime semantics/cache keys must not depend on those metadata fields,
  - executable `tool_configs.<tool>.content_map` keys are sandbox-relative
    paths where trailing `/` or `\\` means directory-from-ZIP unpack,
    `./` (or `.\\`) unpacks directly at sandbox root, non-trailing keys
    materialize regular files, separate entries must not overwrite the same
    target file path, and every referenced content-map hash must also be
    present in `external_data`.
- `src/conductor-builtins/` provides versioned built-in tool contracts such as
  `echo`, `fs`, `import`, `export`, and `archive`.
  Builtin runtime behavior must live in these crates (not inline in
  `src/conductor`), and each builtin crate should remain independently runnable
  via its own binary target.
  Builtin contract stability rule: all builtins must share the same input
  conventions. CLI must use normal Rust flag/option conventions while keeping
  all argument values as strings, and API input must use
  `BTreeMap<String, String>` args plus optional raw payload bytes for
  content-oriented operations. A builtin CLI may optionally define one default
  option key so one value can be provided without spelling the option key, but
  explicit keyed input must remain supported and map to the same API key.
  Builtin API and CLI execution must fail fast on undeclared argument/input
  keys, missing required keys, and invalid key combinations; do not silently
  ignore unknown input.
  For builtins whose successful non-error result is pure (a deterministic
  function of inputs), the success payload may be deterministic bytes or
  `BTreeMap<String, String>`.
  Impure builtins may primarily communicate success through side effects and do
  not need to force CLI success into a pure string-only payload. CLI failures
  may use ordinary Rust error types; do not encode failures as fake success
  payloads.
- `src/mediapm/` composes CAS + Conductor into the Phase 3 media-facing API
  and CLI scaffold.

## Build and Test

- Verify the relevant manifests, scripts, workflow files, and local configs
  exist before you run or document toolchain commands.
- Detect install, build, test, lint, format, type-check, and release commands
  from actual repository files instead of assuming a default stack.
- For Rust workflows, treat `Cargo.toml`, `.cargo/config.toml`,
  `rust-toolchain.toml`, `.github/workflows/ci.yml`, and
  `.agents/instructions/rust-workflow.instructions.md` as source-of-truth
  inputs for validation commands and expectations.
- Prefer targeted cargo validation by default for faster feedback:
  - `cargo test-pkg <crate>` for specific crate testing
  - `cargo clippy-pkg <crate>` for specific crate linting
  - `cargo build-pkg <crate>` for specific crate building
  - Example: `cargo test-pkg mediapm` runs only mediapm tests
  - See `.cargo/config.toml` for available aliases and convenience shortcuts
- Use full-workspace validation only for pre-push checks and CI:
  - `cargo fmt-check` (checks formatting on all files)
  - `cargo clippy-all` (lints entire workspace)
  - `cargo test-all` (tests entire workspace)
  - Note: these are intentionally slow; use targeted commands during development
- When a language, framework, task runner, or test system is clearly present,
  add or refine focused instruction files for it rather than stuffing detailed
  rules into `AGENTS.md`.
- Keep CI, editor automation, prompt examples, and instruction files aligned
  with the commands and configs that are actually present in the repository.

## Conventions

- Distinguish between what is present today and what is only part of the
  intended template contract. Do not describe absent files as if they already
  exist.
- Treat `PLAN.md` as an active implementation contract. Do not describe it as
  intentionally unimplemented.
- Do not regress to bootstrap assumptions (single-crate `src/main.rs` with only
  minimal `Cargo.toml` + `rust-toolchain.toml`). This repository is a
  multi-member Rust workspace with phase crates under `src/`.
- When docs mention `application`, `configuration`, `domain`,
  `infrastructure`, and `support`, treat them as conceptual layering terms
  unless matching directories are explicitly introduced in the workspace.
- Before writing stack-specific guidance, inspect concrete evidence such as
  manifests, lockfiles, source tree layout, scripts, CI workflows, editor
  settings, and dedicated config files.
- For Rust edits, treat detailed docstrings as mandatory in touched files:
  document public and private items (`//!` + `///`) with semantics,
  invariants, and side-effect notes, not just name restatements.
- When you detect a real stack, add instructions for it carefully and
  thoroughly in a narrow, well-named instruction file whose `description` and
  `applyTo` target the relevant files.
- Prefer linking to canonical config files instead of copying large policy
  blocks into multiple customization files.
- Keep customization files narrowly scoped: repo-wide defaults in `AGENTS.md`,
  detailed file-specific guidance in `.agents/instructions/`.
- Prefer updating `AGENTS.md` and `.agents/instructions/*.instructions.md`
  directly for durable repository policy. Do not keep long-lived policy only in
  `/memories/repo/`; if temporary repo memory notes are used, merge them into
  instruction files and remove them.
- When splitting one Rust module into multiple files, adopt folder-module
  layout consistently: move `foo.rs` to `foo/mod.rs`, place sibling modules in
  `foo/*.rs`, and place local unit tests in `foo/tests.rs` with
  `#[cfg(test)] mod tests;`. Avoid keeping both `foo.rs` and `foo/mod.rs`, and
  avoid `#[path = "..."]` for routine in-crate module/test placement.
- Preserve mirrored prompt content between `.agents/prompts/` and
  `.opencode/commands/` when both copies exist.
- Respect the repository newline policy: Markdown and shell scripts use LF;
  PowerShell and batch scripts use CRLF.

## Key References

- `AGENTS.md` — workspace-wide defaults
- `.agents/instructions/*.instructions.md` — focused authoring rules by file type
- `.agents/prompts/commit-staged.prompt.md` and
  `.opencode/commands/commit-staged.prompt.md` — mirrored commit workflow prompt
- `opencode.json` — instruction and skill discovery
- `.vscode/settings.json` — terminal auto-approve and editor behavior
- `.github/workflows/ci.yml`, `.github/dependabot.yml`, `.commitlintrc.mjs` —
  automation and policy
- `Cargo.toml`, `.cargo/config.toml`, `rust-toolchain.toml`, `rustfmt.toml`,
  `clippy.toml` — Rust package and quality configuration
- `.agents/instructions/rust-workflow.instructions.md` — Rust editing and
  validation guidance
- `.agents/instructions/mediapm-architecture.instructions.md` — phase boundaries
  and cross-crate invariants
- `.agents/instructions/mediapm-testing-and-docstrings.instructions.md` — test
  expectations and Rustdoc/docstring depth requirements
- `.editorconfig`, `.gitattributes`, `.markdownlint.jsonc`,
  `.agents/.markdownlint.jsonc` — formatting and line-ending rules
