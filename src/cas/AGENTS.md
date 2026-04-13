# CAS Agent Guide

This guide captures implementation context that is easy to miss from signatures
alone.

## Context map

- `StorageActor` is the write/read gateway for `put/get/delete/set_constraint`.
- `IndexActor` is the persistence coordinator for `redb` snapshot flushes.
- `OptimizerActor` runs rewrite/prune maintenance passes.
- `CasNodeActor` is the wire-command façade that composes the three actors.

### Interaction rules

- `StorageActor` may request index flushes via `IndexActorMessage::FlushSnapshot`.
- `StorageActor` may request optimizer actions when disk pressure increases.
- `CasNodeActor` dispatches commands to child actors; it does not bypass them.

## Invariants

- A non-empty hash should exist in redb only if at least one on-disk object file
  exists (`<hash>` or `<hash>.diff`).
- Full objects are stored as raw payload bytes with no headers.
- Delta objects are stored as `.diff` files using the oxidelta-backed payload
  format.
- Persisted object payload files are read-only by default after successful CAS
  writes (`<hash>` and `<hash>.diff`).
- CAS-owned overwrite/delete paths may temporarily clear read-only bits before
  replacing or removing object files.
- Delete is transitive for delta descendants to avoid orphan reconstruction paths.
- Constraints never persist an explicit empty-only candidate list; empty base is
  implicit at read time.

## Documentation requirement (strict)

- In `src/cas/**/*.rs`, document touched files thoroughly:
  - module docs via `//!`,
  - item docs via `///` for public and private items.
- Prefer rich docs (purpose, invariants, side effects, failure behavior,
  and performance rationale where relevant) over brief labels.
- For tests, include concise guarantee-oriented doc/comments describing what
  user-facing or invariant behavior the test protects.

## Codec versioning protocol

- This crate is currently unreleased; backward compatibility is **not**
  required between in-development wire versions.
- Keep `DeltaState` in `src/cas/src/codec/object.rs` as the stable,
  version-agnostic source of truth.
- Put each on-disk wire format in its own file under
  `src/cas/src/codec/versions/` (for example `v1.rs`, `v2.rs`).
- Inside `versions/vX.rs`, do **not** import unversioned structs from outside
  `versions/`.
- A `versions/vX.rs` file may reference only the immediately previous version
  file, and only to implement version-to-version isomorphism/migration.
- Implement latest-version ↔ unversioned-struct isomorphism in
  `versions/mod.rs`, not in `vX.rs` files.
- Keep an explicit `DO NOT REMOVE` policy docstring at the top of each file in
  `versions/`.
- Files outside `codec/versions/` and `index/versions/` must interact with
  versioned symbols only through each folder's `versions/mod.rs` API surface;
  never import `versions::vX` directly.
- `versions/mod.rs` must not directly re-export `versions::vX` structs/types;
  expose unversioned APIs/wrappers there instead.
- Non-`versions/` modules should keep unversioned runtime data structures and
  use `versions/mod.rs` only for serialization/deserialization boundaries.
- For the `index/` tree specifically, modules outside `index/versions/` should
  import unversioned facade symbols from `index/mod.rs` instead of importing
  `index::versions::*` paths directly.
- In non-`versions/` index modules (for example `index/db.rs`, `index/graph.rs`,
  `index/state.rs`), do not add `version` fields or explicit version-checking
  logic. Keep those concerns in `index/versions/` and expose only
  version-agnostic helpers through the facade.
- Keep versioned envelope structs `pub(crate)`; they are internal to the codec
  boundary and must not leak into the public API.
- Every wire version must provide an `IsoPrime` between its envelope and a
  version-local state type in `versions/`.
- `versions/mod.rs` must provide the `IsoPrime` bridge between the latest
  version-local state and unversioned runtime `DeltaState`.
- Do not add manual `From`/`Into` or ad-hoc `to_state`/`from_state` conversion
  methods for version envelopes; use optics exclusively.
- Implement version-to-version migration through optic composition
  (`old_iso.view` -> `new_iso.review`) so payload bytes are not recompressed.
- Delegate hash byte encoding/decoding to `rust-multihash` via the `Hash`
  helpers; do not manually parse multihash varints in envelope code.
- Do **not** apply Unicode NFD normalization in CAS codec structs/fields;
  NFD normalization is reserved for mediapm filepath handling only.

### Automated enforcement

- The versioning convention is CI-enforced for both version roots:
  - `src/cas/src/codec/versions/mod.rs` test
    `versioned_files_keep_policy_guard_and_boundary_rules`
  - `src/cas/src/index/versions/mod.rs` test
    `versioned_files_keep_policy_guard_and_boundary_rules`
- These tests scan `versions/v*.rs` files and fail if a version file:
  - removes the `DO NOT REMOVE` policy guard docstring,
  - imports unversioned runtime structs directly, or
  - references non-adjacent versions (anything other than `vN` or `vN-1`).
- Additional guard tests also fail when any non-`versions/` Rust file directly
  imports/references `versions::vX`; non-version files must go through
  `versions/mod.rs`.
- When adding `vN.rs`, keep the policy guard text and boundary rules intact.

## Operational notes

- Disk pressure is evaluated before accepting writes.
  - Soft threshold: enable compression-first mode.
  - Hard threshold: reject writes with `CasError::OutOfSpace` and trigger prune.
- In-flight deduplication (`DashMap<Hash, Arc<Notify>>`) ensures a hash is
  persisted once even when many concurrent puts race.
