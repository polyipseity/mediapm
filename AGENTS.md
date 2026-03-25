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
- `src/conductor/` provides the Phase 2 orchestration state model and
  persistence-merge logic.
- `src/conductor-builtins/` provides versioned built-in tool contracts such as
  `fs-ops`, `import`, and `zip`.
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
- Prefer cargo aliases from `.cargo/config.toml` for local validation:
  - `cargo fmt-check`
  - `cargo clippy-all`
  - `cargo test-all`
- When a language, framework, task runner, or test system is clearly present,
  add or refine focused instruction files for it rather than stuffing detailed
  rules into `AGENTS.md`.
- Keep CI, editor automation, prompt examples, and instruction files aligned
  with the commands and configs that are actually present in the repository.

## Conventions

- Distinguish between what is present today and what is only part of the
  intended template contract. Do not describe absent files as if they already
  exist.
- Before writing stack-specific guidance, inspect concrete evidence such as
  manifests, lockfiles, source tree layout, scripts, CI workflows, editor
  settings, and dedicated config files.
- When you detect a real stack, add instructions for it carefully and
  thoroughly in a narrow, well-named instruction file whose `description` and
  `applyTo` target the relevant files.
- Prefer linking to canonical config files instead of copying large policy
  blocks into multiple customization files.
- Keep customization files narrowly scoped: repo-wide defaults in `AGENTS.md`,
  detailed file-specific guidance in `.agents/instructions/`.
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
