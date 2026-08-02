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
- Open records: `{ .. }` at end of record for extensibility — permitted only with a `# |||` why-comment per the strictness policy (S1).
- Optional fields: `{ field | Contract | optional }`.
- Default values: `| default = value` syntax.
- Use `Dyn` for nullable/untyped fields since `null` and `String` are unrelated in Nickel's contract system — only where no closed set exists (S2).

## Schema strictness policy

All Nickel schema work (existing, new, and schema-related code) in this repository follows the normative strictness spec below. The test-side obligations are codified in `sdd-tdd-workflow.instructions.md` under "Regression requirements".

**S1 — Closed record contracts by default.** No `..` in any record contract without a `# |||` why-comment on the preceding line. Unknown fields must fail validation, never be silently dropped.
**S2 — No untyped values.** No `Dyn`, `std.enum.TagOrString`, or bare `String` where a closed set of values exists. Tag-like fields use `std.contract.from_predicate` or `std.contract.any_of` enums mirroring the Rust enum exactly (same variant names, same serde rename).
**S3 — Integer guards on every Number.** Any `Number` field whose domain is integral (counts, slots, timeouts, version markers, indexes) uses a `from_predicate` integer predicate (`value == std.number.floor value` or `std.number.is_integer`).
**S4 — Required version marker.** Every persisted Nickel document contract requires the top-level `version` field (no `| optional`).
**S5 — Per-version exports.** Every `versions/vN.ncl` file exports `validate_document_vN`, `envelope_contract_vN`, and its migration function, using `# |||` doc comments and the `{ ... }` export record convention.
**S6 — Unversioned registry surface.** Every `versions/mod.ncl` exports `current_version`, `supported_versions`, `migrate_to`, `SupportedVersion`, plus unversioned `validate_document` and `envelope_contract` aliases.
**S7 — deny_unknown_fields everywhere.** Every Rust serde type on a config/state/envelope/document boundary carries `#[serde(deny_unknown_fields)]` (the Rust mirror of S1). Applies to new types introduced by any schema work too.
**S8 — No catch-all types.** No `serde_json::Value`, `Dyn`, `any`, or bare dict-of-string where a closed shape exists. Config/state surfaces use typed untagged enums (`#[serde(untagged)]`) with per-variant `deny_unknown_fields`.
**S9 — Fail-fast on unknown names.** Stringly-typed names that map to enums (verify strategies, platform keys, materialization methods, capture kinds) must produce an error on unknown values, never silent ignore.
**S10 — Strict JSON schema exports.** Every exported JSON schema (`mediapm.schema.json`, any future conductor schema export) is strict: `additionalProperties: false` at every object, typed items, enum `const` lists, `"type": "integer"` for integral fields, `required` lists. No `{ "type": "object" }` stubs.
**S11 — Migration compatibility.** Every migration (`migrate_v1_to_v2`, `migrate_v2_to_v1`) must produce output satisfying the tightened target contract, proven by a round-trip test.
**S12 — Schema/Rust parity guarded by tests.** Every persisted Nickel schema has a parity test (the `schema_sync.rs` pattern) asserting the Nickel contract and the Rust serde shape agree — including the strictness properties (closed records, integer guards, enums), not just field presence.
**S13 — New schemas follow the same cycle.** Any new schema file, contract, or schema-related code path added must itself satisfy S1–S12, get spec items + tests in the same commit, and be added to the coverage matrix.

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
