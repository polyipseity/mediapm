---
name: "Struct versioning and migration protocol"
description: "Use when adding versioned structs, wire formats, DeltaState bridges, or version migrations anywhere in src/."
applyTo: "src/**/*.rs"
---

# Struct Versioning and Migration Protocol

## Scope

- Apply this guidance when introducing versioned data structures, wire formats,
  or state migrations anywhere in `src/`.
- Keep wire/transport concerns versioned and isolated from functional core state.

## Required structure

- Keep the version-agnostic source of truth in a central, unversioned struct
  (e.g., `DeltaState`).
- Place each wire-format version in a dedicated subdirectory or file per
  version (e.g., `versions/v1.rs`, `versions/v2.rs`).
- Keep versioned envelope structs `pub(crate)` to avoid exposing wire internals
  in the public API surface.

## Strict `versions/` boundary policy

- Inside `versions/vX.rs`, do **not** import unversioned structs from outside
  `versions/`.
- A `versions/vX.rs` file may reference only the immediately previous version
  module (for example `v3` may reference `v2`), and only for
  version-to-version isomorphism/migration.
- Implement latest-version ↔ unversioned-struct isomorphism in
  `versions/mod.rs`, not in individual `vX.rs` files.
- Files outside `versions/` must interact with versioned symbols through
  `versions/mod.rs` only; do not import `versions::vX` directly.
- In `versions/mod.rs`, do not directly re-export `versions::vX` structs/types
  as public API. Expose unversioned wrapper functions/constants instead.
- Files outside `versions/` should keep their own unversioned runtime data
  structures and call into `versions/mod.rs` only when encoding/decoding
  persisted or wire versioned formats.
- When a feature area has an aggregate module (for example `index/mod.rs`),
  prefer exposing unversioned facade APIs there so sibling modules (for example
  `index/db.rs`, `index/graph.rs`) do not import `versions` paths directly.
- In non-`versions/` modules for that feature area, avoid version-tag fields
  and explicit version checks; keep version parsing/validation confined to
  `versions/` code.
- Keep an explicit module-level `DO NOT REMOVE` policy docstring in each file
  under `versions/` so these invariants remain visible during future edits.

## Required optic bridges

- Every wire-format version should expose a version-local optic bridge in
  `vX.rs` (e.g., `IsoPrime<RcBrand, VersionEnvelope, VersionLocalState>`).
- Bridge latest version-local state to unversioned runtime state in
  `versions/mod.rs`.
- Prefer optic composition over manual `From`/`Into`, `to_state`, or
  `from_state` conversions for version envelopes.
- Implement envelope migration by optic composition through the version-agnostic
  state (`old_iso.view` then `new_iso.review`).

## Hash and checksum safety

- Delegate multihash parsing/serialization to `rust-multihash` through `Hash`
  helpers; do not manually parse varint code/size.
- Keep checksum scope tied to logical envelope fields and document exactly which
  fields are covered.

## Schema versioning

- Preserve original reference semantics (e.g., `original_variant_hash`).
- Keep lineage references valid (`from_variant_hash` and `to_variant_hash`
  exist in `variants`).
- Keep schema version explicit (`schema_version`) and migrations sequential.
- Record migration provenance for each applied schema hop.

## Unicode normalization boundary

- Do not apply Unicode NFD normalization in versioned structs or envelope
  fields.
- NFD normalization is reserved for mediapm internal filepath handling.
