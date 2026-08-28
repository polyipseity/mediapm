---
description: "Use when creating or updating AGENTS.md, instruction files, or documenting repository tooling. Covers detection of languages, frameworks, runtimes, build commands, test workflows, and CI automation from concrete repository files."
name: "Stack and Tooling Detection"
applyTo: "AGENTS.md, src/**/AGENTS.md, .agents/instructions/**/*.md, opencode.json, .vscode/settings.json, .github/workflows/**/*.yml, .github/workflows/**/*.yaml, .github/dependabot.yml, .editorconfig, .gitattributes"
---

# Stack and Tooling Detection

## Detection order

Start with the highest-signal files and directories: workspace-wide guidance (`AGENTS.md`), dependency manifests and lockfiles, build/test/formatter/linter/compiler configs, CI workflows and repo scripts, source directories and entrypoints, editor settings and automation configs. Prefer multiple signals over a single clue. If evidence conflicts, document the ambiguity and avoid inventing hard rules until the structure clarifies.

## What to detect

- Programming languages in active use, not merely hinted at by empty folders.
- Frameworks, build systems, package managers, test runners, linters, formatters, type checkers, documentation generators, and release tooling.
- Directory boundaries that deserve their own instructions because they follow different conventions.
- Canonical commands and where they are defined.
- Platform-specific constraints such as line endings, executable bits, or shell assumptions.

For this repository, confirm the workspace crate split from real files: `src/mediapm-cas/` (CAS), `src/mediapm-conductor/` (orchestration), `src/mediapm-conductor-builtins/*/` (builtin runtime crates), `src/mediapm/` (media-facing composition).

## Tooling and validation discovery

- Identify canonical commands from task-runner configs, scripts, CI steps, and workspace docs; record where each comes from so instructions can be updated when the source of truth changes. If several entrypoints wrap the same behavior, document the canonical one and note wrappers briefly.
- When changing a tooling instruction, check neighboring configs in the same pass (CI workflows, editor automation, dependency-update automation, formatting/line-ending config, prompt files) so the repo does not describe one workflow while automating another.
- For every detected stack, document how agents validate changes: what to run, where commands are defined, what files are source of truth, what to avoid when the stack is only partially initialized. When retiring standalone plan docs, migrate their mandatory validation expectations (format/lint/tests plus runtime gates) into active instruction files in the same change. When no runnable validation exists yet, say so explicitly and point to the files that would need adding.

## Evidence standards

- A single empty directory is weak evidence; a real config file, lockfile, script, workflow step, or representative source file is strong evidence.
- This repository has no JavaScript package-manager ecosystem. A `package.json` or JS lockfile in the tree is a mistake to report and remove, not evidence of a stack.
- Comments in docs are weaker than executable config unless the docs are clearly the source of truth.
- Prefer on-disk facts over habits carried from similar repositories.

## How to write follow-up instructions

- When a stack is detected, create or refine a focused instruction file whose `name`, `description`, and `applyTo` clearly target that stack. Keep repo-wide discovery rules in `AGENTS.md` and stack details in dedicated files; do not overload the root guidance.
- Make the new instruction thorough and evidence-backed: commands, key config files, source locations, testing expectations, and common failure modes. Link to canonical config files instead of copying long option lists unless a short inline summary is critical to agent behavior.
- Keep `applyTo` globs narrow so the detailed instruction only loads for the files it truly governs.

## What to avoid

- Do not assume a default language or task runner just because a similar repo used one.
- Do not keep stale stack-specific files after the repo has been generalized or reoriented.
- Do not leave broad placeholders such as "follow standard best practices" when concrete repository evidence can support sharper guidance.
