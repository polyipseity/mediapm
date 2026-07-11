---
description: "Use when creating or updating AGENTS.md, instruction files, or documenting repository tooling. Covers detection of languages, frameworks, runtimes, build commands, test workflows, and CI automation from concrete repository files."
name: "Stack and Tooling Detection"
applyTo: "AGENTS.md, src/**/AGENTS.md, .agents/instructions/**/*.md, opencode.json, .vscode/settings.json, .github/workflows/**/*.yml, .github/workflows/**/*.yaml, .github/dependabot.yml, .editorconfig, .gitattributes"
---

# Stack and Tooling Detection

## Goal

- Treat stack and tooling discovery as an evidence-based process.
- Before you write language-specific, framework-specific, or runtime-specific instructions, determine what the repository actually uses from concrete files on disk.
- Once detected, add instructions for the detected stack carefully and thoroughly in focused files with narrow scope.
- When long-form architecture plans are retired, migrate durable technical requirements (runtime model, testing expectations, crate boundaries, invariants) into instruction files instead of leaving them only in deleted planning docs.

## Detection order

- Start with the highest-signal files and directories:
  - workspace-wide guidance such as `AGENTS.md`
  - dependency manifests and lockfiles
  - build, test, formatter, linter, and compiler configs
  - CI workflows and repo scripts
  - source directories, file extensions, and representative entrypoints
  - editor settings and automation configs
- Prefer multiple signals over a single clue when deciding that a stack is truly in use.
- If evidence conflicts, document the ambiguity and avoid inventing hard rules until the repository structure clarifies the intended setup.

## What to detect

- Programming languages in active use, not merely hinted at by empty folders.
- Frameworks, build systems, package managers, test runners, linters, formatters, type checkers, documentation generators, and release tooling.
- Directory boundaries that deserve their own instructions because they follow different conventions.
- Canonical commands, if any, and where they are defined.
- Platform-specific constraints such as line endings, executable bits, or shell assumptions.

For this repository specifically, confirm the workspace crate split and its responsibilities from real files:

- `src/mediapm-cas/` as CAS,
- `src/mediapm-conductor/` as orchestration,
- `src/mediapm-conductor-builtins/*/` as builtin runtime crates,
- `src/mediapm/` as media-facing composition.

## Tooling and validation discovery

### Command discovery

- Identify canonical commands from task-runner configs, scripts, CI steps, and workspace docs.
- Record where each command comes from so instructions can be updated when the source of truth changes.
- If several entrypoints wrap the same behavior, document the canonical one and note the wrappers briefly instead of duplicating the whole command matrix.

### Config coordination

- When you change an instruction about tooling, check the neighboring configs in the same pass:
  - CI workflows
  - editor automation
  - dependency update automation
  - formatting and line-ending config
  - prompt files that tell agents how to run checks
- Keep those files consistent so the repo does not describe one workflow while automating another.

### Validation guidance

- For every detected stack, document how agents should validate changes:
  - what to run
  - where the commands are defined
  - what files act as the source of truth
  - what should be avoided when the stack is only partially initialized
- When retiring standalone plan docs, ensure their mandatory validation expectations (format/lint/tests plus any required runtime gates) are migrated into active instruction files in the same change.
- When no runnable validation exists yet, say that explicitly and point to the files that would need to be added before validation can be automated.

## Evidence standards

- A single empty directory is weak evidence.
- A real config file, lockfile, script, workflow step, or representative source file is strong evidence.
- Comments in docs are weaker than executable config unless the docs are clearly the source of truth.
- Prefer on-disk facts over habits carried from similar repositories.

## How to write follow-up instructions

- When a stack is detected, create or refine a focused instruction file whose `name`, `description`, and `applyTo` clearly target that stack.
- Keep repo-wide discovery rules in `AGENTS.md` and stack details in dedicated files; do not overload the root guidance.
- Make the new instruction thorough enough to cover code structure, tests, commands, config files, pitfalls, and validation workflow for that stack.
- Link to canonical config files instead of copying long option lists unless a short inline summary is critical to agent behavior.
- Make the added instruction thorough and evidence-backed: commands, key config files, source locations, testing expectations, and common failure modes.
- Keep `applyTo` globs narrow so the detailed instruction only loads for the files it truly governs.

## What to avoid

- Do not assume a default language or task runner just because a similar repo used one.
- Do not keep stale stack-specific files after the repo has been generalized or reoriented.
- Do not leave broad placeholders such as "follow standard best practices" when concrete repository evidence can support sharper guidance.
