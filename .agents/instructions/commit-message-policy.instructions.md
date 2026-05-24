---
description: "Use when creating commits or editing commit workflow/policy files in this repository."
name: "Commit Message Policy"
applyTo: "AGENTS.md, .agents/prompts/commit-staged.prompt.md, .commitlintrc.mjs"
---

# Commit Message Policy

## Required format

- Commit headers must follow Conventional Commits shape:
  `type(scope): subject`.
- Scope is mandatory for normal project commits.

## Forbidden crate-prefix headers

- Do not start commit headers with crate-prefix forms such as:
  - `mediapm: ...`
  - `conductor: ...`
  - `cas: ...`
  - or similar `<crate-or-tool>:` headers.
- Crate/tool identity belongs in the Conventional Commit scope, not as the
  header prefix.

## Examples

- ✅ `feat(mediapm): align demo-online sidecar hierarchy`
- ✅ `fix(conductor): reject invalid content_map traversal keys`
- ❌ `mediapm: align demo-online sidecar hierarchy`
- ❌ `conductor: reject invalid content_map traversal keys`
