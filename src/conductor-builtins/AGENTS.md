# Conductor Builtins Crate Instructions

This file defines crate-local guidance for `src/conductor-builtins/`.
Follow this together with workspace-wide policy in `AGENTS.md` and focused
instruction files in `.agents/instructions/`.

## Scope

- Applies to all files under `src/conductor-builtins/`.
- Sub-crates under this directory each ship their own `AGENTS.md` for
  builtin-specific details.

## Conventions

- Each builtin provides a library API and an optional CLI binary.
- All builtins share the same input conventions: `BTreeMap<String, String>`
  args plus optional raw payload bytes for content-oriented operations.
- Fail fast on undeclared or missing input keys.
