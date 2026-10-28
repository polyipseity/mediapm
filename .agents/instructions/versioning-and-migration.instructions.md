---
description: "Use when adding versioned structs, wire formats, DeltaState bridges, or version migrations anywhere in src/, or when choosing version sequences (schema, wire-format, domain-separator and key-scheme versions step forward as integers, no dotted revision)."
name: "Struct versioning and migration protocol"
applyTo: "src/**/*.rs"
---

# Struct Versioning and Migration Protocol

## Required structure

- Keep the version-agnostic source of truth in one unversioned struct per surface. `DeltaState` in `src/mediapm-cas/src/delta/object.rs` holds `base_hash`, `content_len` and `payload`, and every delta wire version converts to it.
- Place each version in its own file under a `versions/` directory, whether the version covers a wire format or an on-disk layout. Examples: `src/mediapm-cas/src/delta/versions/v1.rs` through `v3.rs`, `src/mediapm-cas/src/storage/wal/versions/v1.rs` and `v2.rs`, and `src/mediapm/src/state/versions/v1.rs` through `v5.rs`. Eight such directories exist across the workspace; the list above is a sample, not the set.
- Keep versioned envelope and version-local state types `pub(crate)` or narrower; the mediapm state ladder uses `pub(super)`. `V3Envelope` and `DeltaStateV3` stay inside `delta/versions/`, and no module outside that directory names a wire shape. `has_known_versioned_type_leak` in `src/mediapm-cas/src/delta/versions/mod_policy_guard.rs` enforces the second half by matching an enumerated token list, so a type absent from that list is unguarded until someone adds it.
- Make the surface's `versions/mod.rs` the only bridge between versioned and unversioned state. `decode_delta_state` and `encode_delta_state` in `delta/versions/mod.rs` are that bridge for CAS deltas.

## Where the version tag lives

A tag sits in the payload it belongs to or in the filename. Each surface picks one and stays with it.

- **In the envelope.** A delta file starts with six magic bytes and a little-endian `u16`: `CASDLT` for version 3 and later, `MDCASD` for versions 1 and 2. `decode_magic_embedded_version` rejects a `CASDLT` envelope below version 3, an `MDCASD` envelope above version 2, and version 0, which is reserved for a scheme with more than 65535 versions.
- **In the document.** `state.json` and `state.conductor.json` carry an integer `version` field. `extract_state_version_field` in the mediapm ladder and `peek_version_marker` in the conductor ladder raise on a missing or non-numeric marker, and `peek_version_marker` also rejects a value beyond `u32`. A document that names no version, names one this build does not read, or does not parse is an error rather than an empty rebuild; `state-persistence.instructions.md` and `src/mediapm-conductor/AGENTS.md` section D hold the per-document rules.
- **In the filename.** Per-directory metadata snapshots carry no version field inside. The name states it (`metadata-v1.json`), and `src/mediapm-cas/src/storage/metadata_store/versions/mod.rs` owns both the filenames and the codecs, so a `metadata-v2.json` cannot reach a V1 parser.

## Canonical versioning pattern (config documents)

Every versioned config surface follows this shape, on either transport:

1. **Migrations live at the version boundary.** Each `vN.ncl` owns the edges into and out of itself: `v2.ncl` exports `migrate_v1_to_v2` and `migrate_v3_to_v2`, `v1.ncl` exports `migrate_v2_to_v1`. `mod.ncl` selects the edge and composes two hops for the `1 <-> 3` pairs, so no edge implementation lives in the registry. The JSON documents do the same in Rust, in `versions/mod.rs` or `state/versions/`.
2. **Parse into a `*Latest` boundary type.** The conductor's `config/versions/mod.rs` parses into `v_latest::NickelEnvelopeLatest`, and `from_boundary` and `into_boundary` are the two directions. The mediapm side parses into `MediaRuntimeStorageLatest` and resolves it with `resolve_runtime_storage`.
3. **Resolve defaults into plain values** with a separate resolved model: user-facing config types use plain values with serde defaults, not `Option<T>`. `resolve_runtime_storage` turns `MediaRuntimeStorageLatest` into `MediaRuntimeStorage`, and an unset boundary path stays empty so the `MediaPmPaths` defaults win. The resolved model's `Option`s are documented exceptions: `cache_root_override` exists for tests, and the `resolved_*` provenance fields record an absent upstream value.

**Each version envelope keeps its own shape.** `MediaRuntimeStorageV1` in `src/mediapm/src/config/versions/v1.ncl` lists its fields flat, while `MediaRuntimeStorageLatest` groups most of them under `paths`, `materialization`, `verification`, `caching`, `lifecycle` and `environment`, leaving `path_sanitization` and `retry_impure` at the top level. The bridge is a conversion, so the old contract keeps the shape its version actually wrote. Do not reshape an old contract to mirror the newest grouping.

**Version-specific types stay inside the version file.** `versions/v_latest.rs` holds the `*Latest` family (`MediaRuntimeStorageLatest`, `RuntimePathsConfigLatest`, `RuntimeMaterializationConfigLatest` and their siblings), and `versions/document.rs` holds `MediaPmDocument`, the boundary document carrying `MediaRuntimeStorageLatest` on its `runtime` field. `config/mod.rs` holds the resolved model, whose fields carry plain values with serde defaults.

## Strict `versions/` boundary policy

- Inside `versions/vX.rs`, do not import unversioned structs from outside `versions/`.
- A `versions/vX.rs` file may reference only the immediately previous version module (for example `v3` reaching `v2`), and only for version-to-version migration.
- Implement latest-version to unversioned-struct isomorphism in `versions/mod.rs`, not in individual `vX.rs` files.
- Files outside `versions/` reach versioned symbols through `versions/mod.rs` only. `no_file_outside_versions_references_a_version_module_path` in `src/mediapm-cas/src/delta/versions/mod_policy_guard.rs` scans every workspace member and fails on any such path in code, so the rule is enforced by the suite rather than by review. Doc comments and string literals that describe the rule are not violations, because the guard projects each file onto its code first.
- Do not re-export `versions::vX` structs as public API from `versions/mod.rs`; expose unversioned functions and constants.
- Files outside `versions/` keep their own unversioned runtime data structures and call into `versions/mod.rs` when encoding or decoding a persisted format.
- Keep the `DO NOT REMOVE` policy docstring in every Rust file under a `versions/` directory. `every_versions_dir_keeps_policy_guard_docstring` checks every workspace member, and it collects `.rs` files only, so a Nickel version file carries no such marker.

## Migration policy

A migration converts one version's state into the next when a reader meets it, and records nothing about having run. Whether the reader writes the converted document back is per surface: the conductor rewrites a v1 state file at load (`load_state_file_async`), mediapm's JSON state ladder writes nothing on a version hop (only the legacy `.ncl` migration writes, and it deletes the `.ncl` it replaced), and a delta decode rewrites nothing because `decode_delta_state` converts in memory only.

- **Hops are sequential and live with a version.** `decode_delta_state` dispatches on the embedded version and walks `V1 -> V2 -> V3` through `From` impls: `From<DeltaStateV1> for DeltaStateV2` copies `base_hash`, `content_len` and `payload` field by field, and `From<DeltaStateV2> for DeltaStateV3` keeps those three and computes `diff_hash`. `encode_delta_state` always writes the latest version.
- **A hop preserves content, not provenance.** No field records which versions a document passed through, and no code writes one. Preserved state is the delta's `base_hash`, `content_len` and `payload`; anything a later version adds is derived at migration time or defaulted.
- **An unreadable version is an error.** An unknown embedded version returns `CasError::corrupt_object("delta envelope: unsupported version N")`, and a state document naming an unsupported version fails its load. The read path of a version that can exist on disk stays in place.
- **The workspace uses no optics library.** There is no `IsoPrime`, no `RcBrand` and no optic composition anywhere under `src/`; `src/mediapm-conductor/src/config/mod.rs` records the choice. A migration is a `From` impl or a `migrate_*` function, and the version module holding it is where the hop belongs.

Delta wire versions:

| Version | Magic | Fields beside the payload | What the hop into it does |
| --- | --- | --- | --- |
| V1 | `MDCASD` | `base_hash`, `content_len`, `payload_len`, `checksum` | no hop; the decoder reads `checksum` and never verifies it |
| V2 | `MDCASD` | `base_hash`, `content_len`, `payload_len` | copies the three state fields from V1 and drops `checksum` |
| V3 | `CASDLT` | `base_hash`, `content_len`, `diff_hash` | copies the three state fields from V2 and computes `diff_hash = blake3(payload)` |

## Hash and checksum safety

- Delegate multihash parsing and serialization to `multihash` through the `Hash` helpers. `Hash::from_storage_bytes_with_len` reads a `Multihash<32>` and rejects any code except `BLAKE3_MULTICODEC`, so a hand-rolled varint parser is never correct.
- Tie an integrity field to the bytes it covers, and verify it where the envelope is validated. V3's `diff_hash` covers the payload and `V3Envelope::validate` recomputes it. V1's `checksum` is parsed and never verified, which is why its encode path is test-only. An older version that carries an unverified field is read as-is; do not add another one.

## Version sequences are integers

- A version sequence is an integer counter and only moves forward: `v1 -> v2 -> v3 -> v4`. That covers persisted schema versions, wire formats, domain separators, key-scheme versions and migration ladders. Never append a dotted or in-place revision (`v2.1`, `v3.2`), and never describe a scheme change as a patch of the previous version. SemVer package versions (`0.1.0`) and upstream tool tags (`v2.2.12`) are dotted by design and out of scope.
- A changed scheme is a new integer. Persisted identities, domain separators, wire formats and migration ladders all follow the rule, whether or not the surrounding schema version moves.
- Because a bump is visible and cheap, compatibility branches for revisions that never shipped in a release are deleted, not carried: the old keys miss once and re-execute.
- When a scheme version and a schema version move independently, keep them separate: a runtime scheme constant belongs to the module that derives the value, never to a `versions/` wire module.

## Unicode normalization boundary

- Do not apply Unicode NFD normalization in versioned structs or envelope fields; NFD is reserved for mediapm internal filepath handling.
