---
description: "Use when editing markdown, agent customization, TOML, scripts, tooling config, or creating commits. Covers repo markdown rules, TOML/script conventions, stack detection, and commit message policy."
name: "Authoring and repo conventions"
applyTo: "AGENTS.md, src/**/AGENTS.md, README.md, .agents/**/*.md, .opencode/**/*.md, .github/**/*.md, **/*.toml, scripts/**, opencode.json, opencode.jsonc, .vscode/settings.json, .github/workflows/**/*.yml, .github/workflows/**/*.yaml, .github/dependabot.yml, .editorconfig, .gitattributes, .commitlintrc.mjs"
---

# Authoring and repo conventions

## Markdown and agent customization

Applies to `AGENTS.md`, `.instructions.md`, `.prompt.md`, skill docs, mirrored OpenCode prompts, and markdown under `.github/`. Keep root `AGENTS.md` durable and repo-wide; move file-type details into focused instruction files. Describe absent downstream files as optional/future-facing.

- Follow `.markdownlint.jsonc` (root) and `.agents/.markdownlint.jsonc` (under `.agents/`). Key rules: `MD013` disabled (no hard-wrap paragraphs); `MD033`/`MD051` disabled (inline HTML/bare anchors allowed when they help); `MD060` enabled and flags mixed-style tables, use compact style `| --- | --- |` with single-space padded cells; under `.agents/**` `MD036` is disabled and nested lists use 3-space bullets and 5-space sub-bullets (`rumdl-fmt` enforces MD007/MD032 and fails the commit on fix).
- "Link, don't embed": point to canonical files (`.github/workflows/ci.yml`, `.commitlintrc.mjs`, `opencode.json`, `.vscode/settings.json`) instead of duplicating policy blocks.
- Every `.instructions.md` and `.prompt.md` keeps valid YAML frontmatter between `---` markers with `description`, `name`, and `applyTo`. Preserve prompt `argument-hint` and `${input:...}` placeholders unless intentionally changing the interface.
- Mirrored prompts: `.agents/prompts/*.prompt.md` and `.opencode/commands/*.prompt.md` stay byte-identical; the `.opencode/` copy is untracked and never hook-checked. Edit both in the same change unless intentionally diverging and documenting why. Keep shell examples platform-accurate (PowerShell-native quoting; Bash/zsh heredocs safe).
- No `.github/copilot-instructions.md`; root `AGENTS.md` is the workspace-wide source of truth. Do not create a `docs/` directory; durable facts belong in instruction files, `AGENTS.md`, or coverage-matrix rows. Migrate normative rules from retired plan docs into active instruction files in the same change, and merge temporary repo-memory notes into instruction files then delete them.

## TOML

- `#` line comments only. Multi-line arrays use bracketed style with trailing commas. Multi-line tables use `[section]` headers with indented key-value pairs.
- `Cargo.toml`: workspace inheritance where possible (`version.workspace = true`, `edition.workspace = true`, `rust-version.workspace = true`, `license-file.workspace = true`, `publish.workspace = true`). Dependency order: `name.workspace = true`; `name = { workspace = true, features = [...] }`; `name = { path = "...", default-features = false }`; `name = { workspace = true, optional = true }`. Use `dep:name` for optional dependency features. Every crate `Cargo.toml` has `[lints] workspace = true`.
- `prek.toml`: `[[repos]]` array of tables; hooks as inline tables `{ id = "...", ... }`; `exclude` as regex strings; `stages` arrays for lifecycle scoping.
- `.cargo/config.toml`: `[alias]` for cargo shortcuts (`build-all`, `clippy-all`, `fmt-check`, `test-all`, etc.); `[build]` with `target-dir`.
- `rust-toolchain.toml`: minimal `[toolchain]` with `channel`, `profile`, `components`.
- `.rumdl.toml`: root has `[global]` with `include` and `disable`; overrides use `extends = "../.rumdl.toml"` and `extend-disable`.

## Scripts and executable permissions

- Repo helper scripts live in `scripts/`; their self-tests live in `tests/scripts/` (root `tests/` crate, package `mediapm-tests`). Do not place test scripts inside `scripts/`.
- Name scripts for the task, keep them narrowly focused and non-interactive by default. If a script becomes application code, move it into the source tree.
- Twin rule: user-facing cross-platform helper scripts ship as `.sh` + `.ps1` twins with identical CLI and output; each platform runs its native script. A gate that exists only to hold a rule may be a Rust crate instead of twin scripts (`scripts/test-runner` is a crate; the janitor stays a twin because it is a user-facing command with its own output contract).
- Line endings and permissions follow `.editorconfig` and `.gitattributes`: `*.sh` LF, `*.ps1` CRLF, `*.bat` CRLF. Track executable bits for directly-executed non-Windows shell scripts. New script extensions get explicit policy before widespread use.
- Portability: explicit error handling, predictable exit codes, idempotent operations. Do not assume Bash-only features in `.sh` unless required and documented. In PowerShell, prefer clear cmdlet names over aliases.
- PowerShell `-File` gotcha: `pwsh -File script.ps1 --token` leaves dash-prefixed tokens unbound in `$args`; fold the first one in when the positional param is empty, with a why-comment.
- POSIX sh `set -e` gotcha: `var="$(cmd)"` aborts when `cmd` exits non-zero; guard with `if var="$(cmd)"; then` or append `|| true`, and annotate any `|| true` with `# check-suppress:suppression_doc:` and its reason.
- Keep script behavior consistent with CI, `AGENTS.md`, and prompt guidance. When script location or behavior changes, re-check `.github/workflows/ci.yml`, `.vscode/settings.json`, and prompt/instruction references.

## Stack and tooling detection

- Detection order: workspace guidance (`AGENTS.md`), dependency manifests/lockfiles, build/test/formatter/linter/compiler configs, CI workflows/repo scripts, source dirs/entrypoints, editor/automation config. Prefer multiple signals; document ambiguity instead of inventing rules.
- Detect active languages, frameworks, build systems, package managers, test runners, linters, formatters, type checkers, doc generators, release tooling, and directory boundaries needing their own instructions. Confirm commands from task-runner configs, scripts, CI steps, and workspace docs; record their source of truth.
- Evidence standards: config files, lockfiles, scripts, workflow steps, or representative source files are strong evidence; empty dirs and comments are weak. This repository has no JavaScript package-manager ecosystem; a `package.json` or JS lockfile in the tree is a mistake to report and remove, not evidence of a stack.
- When a stack is detected, add or refine a focused instruction file with narrow `applyTo`, evidence-backed commands/configs/locations/tests/failure modes, and links to canonical config files. Keep repo-wide discovery in `AGENTS.md` and stack details in dedicated files.

## Commit message policy

- Headers follow Conventional Commits: `type(scope): subject`. Scope is mandatory. Do not use crate-prefix headers (`mediapm:`, `conductor:`, `cas:` or similar `<crate-or-tool>:`); put crate/tool identity in `scope`.
- A commit message is a subject line only unless a reader would misread the change from the subject alone. Write a body only for: a mechanism was deleted, behaviour now differs from a rule stated elsewhere, or a trade-off was accepted that a reviewer would otherwise object to. Body is at most three lines and states the why. Do not restate the diff, list files, quote test counts, or narrate steps.
