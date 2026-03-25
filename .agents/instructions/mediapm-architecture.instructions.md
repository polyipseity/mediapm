---
description: "Use when editing mediapm Rust source under src/. Covers the module-layer architecture, sidecar invariants, planning/execution boundaries, and storage/link behavior expectations."
name: "mediapm Architecture and Invariants"
applyTo: "src/**/*.rs"
---

# mediapm Architecture and Invariants

## Purpose

- Keep code aligned with mediapm's phase-based architecture and explicit state model.
- Preserve determinism and auditability of media state transitions.
- Keep boundaries between planning logic and side effects clear.

## Module layout (source of truth)

- `src/cas/` (Phase 1)
  - identity/hash model
  - CAS async API contracts
  - storage/index/constraint behavior
- `src/conductor/` (Phase 2)
  - orchestration state model
  - deterministic instance-key and merge logic
  - workflow execution contracts
- `src/conductor-builtins/*/` (Phase 2 built-ins)
  - versioned built-in tool contracts such as `fs-ops`, `import`, `zip`
- `src/mediapm/` (Phase 3)
  - media-facing API
  - CLI shell and phase composition over conductor + CAS

If you introduce a new file, place it in the phase crate that owns that
concern. Avoid re-introducing flat `src/*.rs` module sprawl at workspace root.

## Layering rules

- `cas` should remain runtime-agnostic at public API boundaries.
- `conductor` should keep deterministic planning/keying logic explicit and testable.
- `mediapm` should compose phase 1/2 APIs rather than bypassing them.
- Built-ins should stay narrowly scoped and version-addressable.
- Prefer one-directional dependencies:
  - `cas -> conductor -> mediapm` composition,
  - with built-ins consumed by conductor/mediapm as contracts,
  - and no circular crate dependencies.

## Identity and storage invariants

- Canonical identity key is URI (`canonical_uri`), not path display strings.
- Content identity is BLAKE3 hash and object fan-out path under `.mediapm/objects/blake3/<2-char>/<rest>`.
- Sidecar paths are derived from canonical URI digest under `.mediapm/media/<media-id>/media.json`.
- Object files are immutable once imported.

## Sidecar schema and history expectations

- Preserve `original.original_variant_hash` semantics (initial variant reference).
- Keep `edits` lineage references valid (`from_variant_hash` and `to_variant_hash` exist in `variants`).
- Keep schema version explicit (`schema_version`) and migrations sequential.
- Record migration provenance for each applied schema hop.

## Determinism and safety expectations

- Sort and serialize JSON deterministically for stable diffs.
- Use atomic write flow for sidecars and object writes (temp file + sync + rename).
- Keep `plan` output stable for identical inputs.
- Keep `sync` idempotent for unchanged state.

## Link materialization expectations

- Respect configured method order and deterministic fallback reasons.
- Keep behavior explicit for symlink/hardlink/copy capabilities.
- Preserve no-op behavior when existing link already matches desired target.

## Documentation requirements for Rust code

When you add or change public APIs in `src/`:

- Add module-level `//!` docs describing purpose and boundaries.
- Add `///` docs for public structs/enums/functions and key public fields.
- Explain invariants and side effects, not just what types are called.
- Prefer newcomer-readable docs over shorthand internal jargon.

## Validation checklist after Rust edits

Run and check:

- `cargo fmt-check`
- `cargo clippy-all`
- `cargo test-all`

If you intentionally change behavior, update tests and docs in the same change.
