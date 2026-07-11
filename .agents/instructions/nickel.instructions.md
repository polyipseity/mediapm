---
description: "Use when authoring or editing Nickel (.ncl) schema, config, or migration files in this repository."
name: "Nickel Conventions"
applyTo: "**/*.ncl"
---

# Nickel conventions

## Doc and section comments

- Use `# |||` for structured doc comments on contracts, types, and public API surfaces.
- Use `#` for section headers, divider lines (`# ||| ---`), and inline notes.

## Indentation and formatting

- **2-space indentation** throughout. Never use tabs.
- Every `.ncl` file ends with an export record: `{ ... }` on the final line.

## Schema version markers

- Every persisted Nickel document must carry an explicit top-level numeric `version` field.
- Version files export `validate_document_vN` and `envelope_contract_vN` — not plain `validate_document` at the version file level; unversioned aliases live in `mod.ncl`.
- `mod.ncl` is the migration registry: exports `current_version`, `supported_versions` (array), `migrate_to` (function), and a `SupportedVersion` predicate contract.

## Contract patterns

- Use `std.contract.from_predicate` for all custom contracts.
- Use `std.contract.any_of` for tagged unions.
- Dictionary/map types: `{ _ : Type }`.
- Open records: `{ .. }` at end of record for extensibility.
- Optional fields: `{ field | Contract | optional }`.
- Default values: `| default = value` syntax.
- Use `Dyn` for nullable/untyped fields since `null` and `String` are unrelated in Nickel's contract system.

## Migration patterns

- Migration logic uses `std.record.has_field`, `std.record.fields`, `std.array.fold_left`, `%{key}` dynamic field access.
- Each version file defines both `validate_document_vN` and `envelope_contract_vN`.
- `mod.ncl` imports all version files and wires the migration graph.

## Common pitfalls

- Nickel numbers are always floats; guard integer-only fields with `std.number.is_integer`.
- Use `let ... in` for all local bindings — no top-level imperative style.
- Use `import "path"` for module references.
- Top-level envelope contract applied as `data | NickelDocumentV2`.

## File organization

- Schema version files: `src/<crate>/src/config/versions/`.
- Per-version files: `v1.ncl`, `v2.ncl`, etc.
- Registry: `mod.ncl` serves as migration registry.
- Top-level config: `conductor.ncl`, `mediapm.ncl` at workspace/user config root.
