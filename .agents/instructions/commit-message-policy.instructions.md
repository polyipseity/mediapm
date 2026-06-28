---
description: "Use when creating commits or editing commit workflow/policy files in this repository."
name: "Commit Message Policy"
applyTo: "**"
---

# Commit Message Policy

## Required format

- Commit headers must follow Conventional Commits shape: `type(scope): subject`.
- Scope is mandatory for normal project commits.

## Forbidden crate-prefix headers

- Do not start commit headers with crate-prefix forms such as:
  - `mediapm: ...`
  - `conductor: ...`
  - `cas: ...`
  - or similar `<crate-or-tool>:` headers.
- Crate/tool identity belongs in the Conventional Commit scope, not as the header prefix.

## Examples

- ✅ `feat(mediapm): align demo-online sidecar hierarchy`
- ✅ `fix(conductor): reject invalid content_map traversal keys`
- ❌ `mediapm: align demo-online sidecar hierarchy`
- ❌ `conductor: reject invalid content_map traversal keys`

## Atomic commit workflow (unstaged changes → multiple commits)

See the canonical stash-first procedure in the global maintain instructions (`~/.agents/instructions/maintain.instructions.md`).

This repository's pre-commit formatting command:

```bash
prek run --all-files
```

If that is unavailable, run formatters manually:

```bash
cargo fmt && prek run end-of-file-fixer trailing-whitespace rumdl-fmt --all-files
```

The hard rules from the global maintain instructions (including "NEVER ask subagents to run git commit") also apply here.
