---
description: "Use for markdown, TOML, scripts, tool config and commits."
name: "Authoring and repo conventions"
applyTo: "AGENTS.md, src/**/AGENTS.md, README.md, .agents/**/*.md, .opencode/**/*.md, .github/**/*.md, **/*.toml, scripts/**, opencode.json, opencode.jsonc, .vscode/settings.json, .github/workflows/**/*.yml, .github/workflows/**/*.yaml, .github/dependabot.yml, .editorconfig, .gitattributes, .commitlintrc.mjs"
---

# Authoring and repo conventions

## Markdown and agent customization

Applies to `AGENTS.md`, `.instructions.md`, `.prompt.md`, skills, mirrored OpenCode prompts, `.github/` markdown. Root `AGENTS.md` durable/repo-wide; file detail in focused instructions; absent downstream files described as optional/future-facing.

- `.markdownlint.jsonc` (root), `.agents/.markdownlint.jsonc`: `MD013` disabled (no hard-wrap), `MD033`/`MD051` disabled, `MD060` enabled (compact `| --- | --- |`, single-space cells); under `.agents/`/`.agents/**` `MD036` disabled, nested lists 3-space/5-space (`rumdl-fmt` enforces MD007/MD032, fails commit on fix).
- Link, don't embed: `.github/workflows/ci.yml`, `.commitlintrc.mjs`, `opencode.json`, `.vscode/settings.json` instead of duplicated policy.
- `.instructions.md`/`.prompt.md`: YAML frontmatter between `---` with `description`, `name`, `applyTo`; preserve `argument-hint`, `${input:...}`.
- Mirrored `.agents/prompts/*.prompt.md`/`.opencode/commands/*.prompt.md` byte-identical; `.opencode/` untracked, never hook-checked; edit both together unless intentionally diverging with documented why. Shell examples platform-accurate (PowerShell-native quoting, Bash/zsh heredocs).
- No `.github/copilot-instructions.md`; no `docs/`; durable facts in instructions, `AGENTS.md`, coverage-matrix rows. Migrate retired plan-doc rules into active instructions in the same change; merge temporary repo-memory notes then delete them.

## TOML

`#` comments; bracketed multi-line arrays with trailing commas; `[section]` + indented keys. `Cargo.toml`: workspace inheritance (`version.workspace = true`, `edition.workspace = true`, `rust-version.workspace = true`, `license-file.workspace = true`, `publish.workspace = true`), dependency order `name.workspace = true`; `name = { workspace = true, features = [...] }`; `name = { path = "...", default-features = false }`; `name = { workspace = true, optional = true }`; `dep:name` optional features; `[lints] workspace = true` in every crate. `prek.toml`: `[[repos]]`, hooks `{ id = "...", ... }`, `exclude` regex, `stages` arrays. `.cargo/config.toml`: `[alias]` (`build-all`, `clippy-all`, `fmt-check`, `test-all`), `[build]` `target-dir`; `rust-toolchain.toml`: `[toolchain]` `channel`/`profile`/`components`; `.rumdl.toml` root `[global]` `include`/`disable`, overrides `extends = "../.rumdl.toml"` + `extend-disable`.

## Scripts and executable permissions

- Helpers in `scripts/`, self-tests in `tests/scripts/` (crate `mediapm-tests`); no test scripts in `scripts/`. Narrow, non-interactive, task-named scripts; application code moves into source tree.
- Twin rule: `.sh` + `.ps1` user-facing helpers with identical CLI/output; native script per platform. Rule-only gate may be Rust crate (`scripts/test-runner`); janitor stays twin (user-facing output contract).
- `.editorconfig`/`.gitattributes`: `*.sh` LF, `*.ps1`/`*.bat` CRLF; track executable bits; new extensions need explicit policy.
- Portability: explicit errors, predictable exits, idempotence; no Bash-only `sh` features unless documented; PowerShell cmdlet names over aliases. `pwsh -File script.ps1 --token` (`-File`) leaves dash tokens in `$args`: fold first into empty positional param. POSIX `cmd` in `var="$(cmd)"` aborts under `set -e`: use `if var="$(cmd)"; then` or `|| true` annotated `# check-suppress:suppression_doc:` + reason.
- Keep scripts aligned with CI/`AGENTS.md`/prompts; re-check `.github/workflows/ci.yml`, `.vscode/settings.json`, instruction references on change.

## Stack and tooling detection

- Order: `AGENTS.md`, manifests/lockfiles, build/test/lint configs, CI/scripts, source, editor config; prefer multiple signals, document ambiguity.
- Detect languages, frameworks, build systems, package managers, test runners, linters, formatters, type checkers, doc generators, release tooling, boundaries; confirm commands via configs/scripts/CI/docs. Strong evidence: configs, lockfiles, scripts, workflow steps, source; weak: empty dirs/comments. No JS ecosystem: `package.json`/JS lockfile must be reported and removed.
- Detected stack gets focused instruction file (narrow `applyTo`, commands/configs/tests/failure modes, canonical links); repo-wide discovery stays in `AGENTS.md`.

## Commit message policy

`type(scope): subject`, scope mandatory; no crate prefixes (`mediapm:`, `conductor:`, `cas:`, `<crate-or-tool>:`); identity in `scope`. Subject alone unless misleading; body only for deleted mechanism, behaviour differing from a rule, or accepted trade-off; max three lines of why, no diff/file/test narration.
