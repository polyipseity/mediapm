---
description: "Use when editing markdown docs, AGENTS.md, prompt files, or agent customization markdown. Covers repo-specific markdownlint rules, linking strategy, mirrored prompts, and safe YAML frontmatter."
name: "Markdown and Customization Authoring"
applyTo: "AGENTS.md, src/**/AGENTS.md, README.md, .agents/**/*.md, .opencode/**/*.md, .github/**/*.md"
---

# Markdown and Customization Authoring

Covers human-facing docs and agent customization markdown: `AGENTS.md`, `.instructions.md`, `.prompt.md`, skill docs, mirrored OpenCode prompts, and markdown under `.github/`. Keep root `AGENTS.md` durable and repo-wide; move file-type details into focused instruction files. If a downstream file is not present yet, describe it as optional or future-facing, not as an existing fact.

## Repo-specific markdown behavior

- Follow `.markdownlint.jsonc` at the repo root and `.agents/.markdownlint.jsonc` for files under `.agents/`.
- Tables must use a single consistent column style; compact (`| --- | --- |`) is the repo convention, with single-space padded content cells. `MD060` is enabled in the editor extension and flags mixed-style tables.
- Do not hard-wrap paragraphs just to satisfy line-length rules; `MD013` is disabled.
- Inline HTML and bare anchor behavior are permitted where they clearly improve the document; `MD033` and `MD051` are disabled.
- Under `.agents/**`, `MD036` is also disabled, so emphasis-only pseudo-headings are allowed when they genuinely improve readability.
- Under `.agents/**`, use 3-space bullets and 5-space sub-bullets for nested list hierarchy; `rumdl-fmt` auto-flattens other indents (MD007/MD032) and fails the commit when it does.

## Writing style

- Keep content scannable, task-oriented, specific to this repository.
- Prefer short sections, bullets, and explicit file references in backticks.
- "Link, don't embed": point to canonical files (`.github/workflows/ci.yml`, `.commitlintrc.mjs`, `opencode.json`, `.vscode/settings.json`) instead of duplicating policy blocks.

## Detection-first customization updates

See `.agents/instructions/stack-and-tooling-detection.instructions.md` for the canonical guidance on detecting languages, frameworks, build systems, and validation workflows from concrete repository files.

## Customization frontmatter

Every `.instructions.md` and `.prompt.md` file must keep valid YAML frontmatter between `---` markers. Follow the frontmatter conventions in global `workspace-guidance.instructions.md` (`description`, `name`, `applyTo` with appropriate values). For prompt files, preserve `argument-hint` fields and `${input:...}` placeholders exactly unless you are intentionally changing the interface.

## Mirrored prompt files

- `.agents/prompts/commit-staged.prompt.md` and `.opencode/commands/commit-staged.prompt.md` are mirrored copies today — keep them byte-identical; the `.opencode/` copy is untracked and never hook-checked.
- If you edit one mirrored prompt, update the other in the same change unless you are intentionally diverging the two surfaces and documenting why.
- Keep shell examples platform-accurate: PowerShell examples should use PowerShell-native quoting, and Bash/zsh examples should use heredocs safely.

## Safe editing patterns

- Do not add `.github/copilot-instructions.md`; root `AGENTS.md` is the workspace-wide source of truth.
- When retiring plan docs, migrate normative rules into active instruction files in the same change and remove stale references.
- Merge temporary repo-memory notes into instruction files and delete them in the same workflow.
- Prefer relative file references valid after cloning to a different path.
- Distinguish "present now" from "expected after initialization" when mentioning future manifests or configs.
