---
description: "Use for persisted state, migrations, Nickel schemas and versioning."
name: "State, versioning, and Nickel"
applyTo: "src/**/*.rs, **/*.ncl"
---

# State, versioning, and Nickel

## Integer versioning

- Schema, wire, domain-separator, key-scheme, migration versions are integers: `v1 -> v2 -> v3`; never `v2.1`. SemVer packages/upstream tags out of scope. Scheme change = new integer; delete compatibility branches for unreleased revisions.
- Keep scheme/schema versions separate when independent; runtime scheme constant belongs to deriving module, not `versions/` wire module.

## Conductor state v7 and instance keys

- Conductor state is V7: `instance_key_secret_verifier` holds a keyed verifier or explicit `null`, and older states migrate to V7 on load. The verifier is not the secret.
- The active instance-key version is integer `9`. Versions `7` and `8` stay as migration inputs only; new keys are derived at `9`.
- Keyed digests use the `blake3-keyed:` prefix and can occupy derived key fields. CAS hashes keep `blake3:` and are never keyed.

## State format reality

- `state.json`: machine-managed pretty JSON, always written (audit, no byte-change detection), not Nickel.
- `mediapm.ncl` user intent; `conductor.generated.ncl` machine runtime; legacy `state.ncl` auto-migrated on load then deleted; never write new `.ncl` state.
- Missing version, unknown version, or unparseable body aborts load; never rebuild empty. Only missing file rebuilds.
- `state.conductor.json` has independent integer ladder loaded separately.

## Credentials outside state

Versioned `auth.json` (default `<runtime_root>/auth.json`, full override `runtime.paths.auth_file`/`--auth-file`/`MEDIAPM_AUTH_FILE`), integer v1 closed records `services.musicbrainz`/`services.acoustid`. Missing file = all logged out, missing record = one service logged out; unknown fields/services, malformed JSON, non-integer/unknown version are errors. Outside runtime root user-managed (gitignore covers default). Writes via `*.json.tmp` rename, `0600` Unix, `Debug` redaction; Windows ACL out of scope. Not workflow state: no state/config/conductor-state/instance-key bump, migration, or Nickel contract change.

## mediapm state shape

- `MEDIAPM_STATE_VERSION = 5` independent of `MEDIAPM_DOCUMENT_VERSION = 5`; V1-V4 state read-only migration, V5 written; config V1-V4 -> V5; `recheck_seconds` `"always"`/`"never"`/positive seconds. V5: `version`, `managed_files: BTreeMap<String, ManagedFileRecord>`, `managed_tools: Vec<ToolRegistryEntry>`; no per-media progress map.
- `ToolRegistryEntry`: `tool_id, version, canonical_version, content_map_hash, deployed_at, resolved_tag, resolved_version, resolved_vcs_hash`; `resolved_*` `Option<String>` (`None` null, missing -> `None`, empty rejected), in-place change. Active = same id, highest `deployed_at` (Unix ns, `0` undeployed); multiple entries expected. `canonical_version` exact equality, composite tool + SameStep (`tool-provisioning.instructions.md`).
- No `MigrateState`: functions `state/versions/vN.rs`, dispatch `state/versions/mod.rs`, API `state/ser.rs`. Writes via `mediapm_utils::fs::write_bytes_atomically`; content changes fine, metadata-only churn with same hash expected.

## Versioned struct protocol

One unversioned source of truth per surface; each version in own `versions/` file with `pub(crate)` envelopes; `versions/mod.rs` sole bridge and one version-tag location (payload/envelope, document, or filename). Sequential `From`/`migrate_*` hops, no optics; migration keeps content, not provenance; unknown/unreadable version errors, old read paths stay.

## Strict `versions/` boundary

- `versions/vX.rs` (`vX.rs`): no external unversioned imports; only immediately previous module, only for migration. Isomorphism in `versions/mod.rs`. Outside access via `versions/mod.rs`; no public `versions::vX` re-export.
- Every Rust file under `versions/` keeps `DO NOT REMOVE` docstring. Guards: `has_known_versioned_type_leak`, `no_file_outside_versions_references_a_version_module_path`, `every_versions_dir_keeps_policy_guard_docstring`.

## Config document pattern

- Parse `*Latest` at boundary, resolve defaults; user config plain serde-default values, not `Option<T>` (exceptions `cache_root_override` host fact, `resolved_*` provenance). Version types/`From` in version file, not `config/mod.rs`; boundaries (`from_boundary`, `into_boundary`, merge) in `versions/mod.rs`, no re-export.

## Nickel conventions

- `# |||` docs, `#` headers/notes, 2-space indent, no tabs, export record `{ ... }`; persisted docs have numeric top-level `version`.
- `vN.ncl` exports `validate_document_vN`, `envelope_contract_vN`, migration; `mod.ncl` registry only (`current_version`, `supported_versions`, `migrate_to`, `SupportedVersion`, unversioned aliases). Each `vN.ncl` self-contained with its own `*VN` contracts; `mod.ncl` sole multi-version importer.
- Migration into a version lives there (`v2.ncl` -> `migrate_v1_to_v2`, `v1.ncl` -> `migrate_v2_to_v1`); fresh records via `std.record.remove "version"` + `& { version = N }`.
- Contracts: `std.contract.from_predicate`, `std.contract.any_of`, `{ _ : Type }`, `{ .. }` only with `# |||` why-comment, `{ field | Contract | optional }`, `| default = value`, `Dyn` only where no closed set exists.

## Nickel strictness S1-S13

S1 closed records (no `..` without why-comment); S2 tag-like fields mirror Rust enums via `from_predicate`/`any_of`; S3 integer guards on `Number` (`std.number.is_integer`/floor equality); S4 top-level `version` on persisted contracts; S5 per-version exports + docs + record; S6 `mod.ncl` registry aliases; S7 `#[serde(deny_unknown_fields)]` on Rust serde boundaries; S8 no `Value`/`Dyn`/bare dict for closed shapes, typed untagged enums with per-variant `deny_unknown_fields`; S9 fail fast on unknown names; S10 contracts evaluated (`export_config_contracts` writes, runtime never reads); S11 migration satisfies target contract (round-trip); S12 parity tests assert strictness; S13 new schemas follow the cycle + coverage matrix.

## Nickel pitfalls

Numbers are floats: guard integers; `let ... in`, `import "path"`, `data | NickelDocumentV2`; `std.string.is_string` absent in Nickel 0.18 (use `std.is_string`, guard before `std.string.is_match`). Plain functions broken as contracts: `std.contract.from_predicate`; `std.contract.from_validator` expects `'Ok value`/`'Error {...}`, not bool. Shorthand self-reference fails (`{ X, ... }`, `{ x = x }`); `let` export `let _x = X in { X = _x, ... }`. Contracts are not JSON-serializable: export wrappers/functions/aliases.

## Hash, checksum, and normalization

- Multihash via `Hash` helpers + `multihash`; no hand-rolled varint. Tie integrity field to bytes covered, verify where envelope validated; older unverified field read as-is, no new field.
- No NFD in versioned structs/envelopes; NFD only for mediapm internal filepath handling.
