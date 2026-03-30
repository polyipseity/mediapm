---
description: "Use when editing mediapm tests or introducing new Rust APIs. Covers test intent, invariant coverage, and thorough newcomer-oriented Rustdoc/docstring expectations."
name: "mediapm Testing and Docstrings"
applyTo: "tests/**/*.rs, src/**/*.rs"
---

# mediapm Testing and Docstrings

## Testing scope expectations

- Treat tests as executable specification for:
  - planner determinism,
  - sync idempotency,
  - sidecar/object integrity,
  - GC safety semantics.
- Prefer behavior-focused integration tests in `tests/` for workflow guarantees.
- Keep unit tests close to module-level invariants (`#[cfg(test)]` in same file)
  when they validate tight internal helpers.

## Required test qualities

- Use explicit arrange/act/assert structure.
- Make assertions specific and diagnostic (avoid vague boolean assertions when possible).
- Keep tests platform-safe (normalize path separators when asserting path strings).
- Use temporary directories for filesystem tests; avoid depending on host machine state.

## When behavior changes

If a code change alters user-visible behavior:

- Update existing tests or add new tests in the same commit.
- Update CLI/reporting docs if command output contract changes.
- Keep `verify` and `gc` expectations synchronized with sidecar model updates.

## Rustdoc/docstring depth requirement

For newcomers with no codebase context:

- Add `//!` module docs explaining why the module exists.
- Add `///` docs for **every Rust item touched**, including private items:
  - `struct`, `enum`, `trait`, `type`, `const`, `static`,
  - `fn`/methods/associated functions,
  - helper structs/enums in test modules,
  - internal state-machine and actor message enums.
- Use detailed docstrings where possible, not one-liners:
  - purpose and where the item fits in the module,
  - key invariants and assumptions,
  - side effects, mutation, locking, persistence, or I/O behavior,
  - error conditions and failure modes for fallible functions,
  - performance notes when behavior is intentionally optimized.
- Document important field semantics in structs (public and private).
- In tests, add concise item docs/comments that state the user-level guarantee,
  the invariant being protected, and why failure matters.
- For touched files, prefer "document-everything in that file" completion over
  only documenting the exact changed lines.

### Strictness policy

- Treat missing docs on touched private helpers as quality regressions.
- Do not accept placeholder docs that restate names (for example,
  "Runs optimize" without semantics).
- If a file has many undocumented internals, continue documenting until no
  obvious top-level helper/constant/type remains undocumented.

## Anti-patterns to avoid

- Bare one-line docs that repeat the function name.
- Public APIs without any semantic explanation.
- Private helper functions/constants without docs in touched files.
- Tests that only check "it runs" without asserting durable behavior.
- Silent behavior changes without corresponding test updates.

## Validation commands

Before finishing:

- `cargo fmt-check`
- `cargo clippy-all`
- `cargo test-all`
