---
description: "Use when editing mediapm Rust source under src/. Covers the module-layer architecture, sidecar invariants, planning/execution boundaries, and storage/link behavior expectations."
name: "mediapm Architecture and Invariants"
applyTo: "src/**/*.rs"
---

# mediapm Architecture and Invariants

## Purpose

- Keep code aligned with mediapm's functional-core direction and explicit state model.
- Preserve determinism and auditability of media state transitions.
- Keep boundaries between planning logic and side effects clear.

## Module layout (source of truth)

- `src/configuration/`
  - `config.rs`: declarative config schema and file IO.
- `src/domain/`
  - `canonical.rs`: URI/path canonicalization.
  - `model.rs`: sidecar domain model and hash types.
  - `metadata.rs`: raw + normalized metadata probing shape.
  - `migration.rs`: schema-version migration chain and provenance.
- `src/application/`
  - `planner.rs`: deterministic effect planning (no IO).
  - `executor.rs`: effect interpretation (filesystem side effects).
- `src/infrastructure/`
  - `store.rs`: object store + sidecar persistence.
  - `verify.rs`: integrity checks and invariants.
  - `gc.rs`: reachability-based object cleanup.
  - `formatter.rs`: config/sidecar canonical formatting.
- `src/support/`
  - `util.rs`: shared helper functions.

If you introduce a new file, place it under one of these first-level folders.
Avoid re-introducing flat `src/*.rs` module sprawl.

## Layering rules

- `domain` must not depend on CLI or command dispatch concerns.
- `planner` should be pure in behavior: derive effects from inputs; avoid side effects.
- `executor` is the side-effect shell and may call infrastructure IO.
- `infrastructure` should not hide behavior; keep operations explicit and auditable.
- Prefer one-directional dependencies:
  - `configuration/domain/support -> application -> infrastructure` is acceptable for orchestration,
    but avoid circular dependencies and hidden global state.

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
