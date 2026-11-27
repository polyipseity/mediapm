---
description: "Use when editing persisted state, config/versioned structs, migrations, or Nickel schemas. Covers state.json format, version ladders, migration rules, and Nickel contract conventions."
name: "State, versioning, and Nickel"
applyTo: "src/**/*.rs, **/*.ncl"
---

# State, versioning, and Nickel

## Integer versioning

- Schema, wire, domain-separator, key-scheme, and migration versions are integers only: `v1 -> v2 -> v3`. Never dotted revisions like `v2.1`.
- SemVer package versions and upstream tool tags are out of scope.
- A scheme change is a new integer. Compatibility branches for unreleased revisions are deleted, not carried.
- When a scheme version and a schema version move independently, keep them separate; a runtime scheme constant belongs to the module that derives the value, never to a `versions/` wire module.

## State format reality

- `state.json` is machine-managed JSON, pretty-printed, always written (audit trail, no byte-change detection). It is not Nickel.
- `mediapm.ncl` is user-owned Nickel intent; `conductor.generated.ncl` is machine-managed Nickel runtime. Legacy `state.ncl` is auto-migrated on load then deleted; never write new `.ncl` state files.
- A state document that names no version, an unknown version, or an unparseable body aborts load; never rebuild as empty. Only a missing file rebuilds.
- Conductor `state.conductor.json` keeps its own independent integer ladder, loaded independently from mediapm state.

## Credentials outside state

- Every service credential lives in one versioned `auth.json`, default `<runtime_root>/auth.json` and overridable in full by `runtime.paths.auth_file`, `--auth-file`, or `MEDIAPM_AUTH_FILE`. The current schema is integer v1: closed typed records under `services.musicbrainz` and `services.acoustid`. A missing file means all services are logged out; a missing record logs out that one service. Unknown fields or services, malformed JSON, a non-integer version, and an unknown version are errors, never a silent logged-out state. An auth file outside the runtime root is user-managed: the generated runtime gitignore protects only the default path under `runtime_root`.
- Writes go through a `*.json.tmp` sibling renamed onto the file, with mode `0600` on Unix and secrets redacted from `Debug`. Windows ACL hardening is out of scope. Credentials are not workflow state: changing `auth.json` needs no state, config, conductor-state, or instance-key-version bump, and no migration or Nickel contract change.

## mediapm state shape

- `MEDIAPM_STATE_VERSION = 5` is current and independent from `MEDIAPM_DOCUMENT_VERSION = 5`. V1-V4 are read-only migration paths for state; V5 is always written. V1-V4 config documents migrate to V5; `recheck_seconds` accepts `"always"`, `"never"`, or a positive integer of seconds.
- V5 fields: `version`, `managed_files: BTreeMap<String, ManagedFileRecord>`, `managed_tools: Vec<ToolRegistryEntry>`. No per-media workflow progress map.
- `ToolRegistryEntry`: `tool_id, version, canonical_version, content_map_hash, deployed_at, resolved_tag, resolved_version, resolved_vcs_hash`. `resolved_*` are `Option<String>` (`None` serializes null, missing deserializes `None`, empty string is invalid/rejected). The `resolved_*` schema change was applied in place with no extra version bump.
- Active tool: same `tool_id`, highest `deployed_at` (Unix epoch ns, `0` means not deployed). Multiple entries per tool id are expected.
- `canonical_version` is compared by exact string equality. The persisted value is composite (tool plus SameStep dependency segments); see `tool-provisioning.instructions.md`.
- No `MigrateState` trait. Hops are plain functions in `state/versions/vN.rs`; dispatch lives in `state/versions/mod.rs`; public API in `state/ser.rs`.
- State writes go through `mediapm_utils::fs::write_bytes_atomically`. `state.json` content changes are not errors; metadata-only churn with unchanged `content_map_hash` is expected.

## Versioned struct protocol

- Keep one unversioned source of truth per surface. Each version lives in its own file under `versions/`; versioned envelopes/types are `pub(crate)` or narrower.
- `versions/mod.rs` is the only bridge between versioned and unversioned state. It owns decode/encode or load/save dispatch for the surface.
- Pick one version-tag location per surface and stay with it: in payload/envelope, in document, or in filename.
- Hops are sequential and live with a version. Use `From` impls or `migrate_*` functions; no optics library. Migration preserves content, not provenance.
- An unreadable or unknown version is an error. Keep read paths for versions that can exist on disk.

## Strict `versions/` boundary

- Inside `versions/vX.rs`, do not import unversioned structs outside `versions/`. A `vX.rs` file may reference only the immediately previous version module, and only for migration.
- Implement latest-to-unversioned isomorphism in `versions/mod.rs`, not in individual `vX.rs` files.
- Files outside `versions/` reach versioned symbols through `versions/mod.rs` only. Do not re-export `versions::vX` structs as public API.
- Every Rust file under a `versions/` directory keeps the `DO NOT REMOVE` policy docstring.
- Guards: `has_known_versioned_type_leak`, `no_file_outside_versions_references_a_version_module_path`, `every_versions_dir_keeps_policy_guard_docstring`.

## Config document pattern

- Migrations live at the version boundary. Parse into a `*Latest` boundary type, then resolve defaults into plain values. User-facing config types use plain values with serde defaults, not `Option<T>`; documented exceptions are `cache_root_override`, the one field a caller sets after the document is resolved because a cache root is a host fact rather than document policy, and the `resolved_*` provenance fields.
- Each version envelope keeps its own shape. Version-specific types and `From` bridges live inside the version file, never in shared `config/mod.rs`. `config/mod.rs` holds the resolved model only.
- Boundary entry points (`from_boundary`, `into_boundary`, multi-document merge) live in `versions/mod.rs` and must not re-export versioned symbols outside `versions/`.

## Nickel conventions

- `# |||` for structured doc comments; `#` for section headers/dividers/inline notes. Use 2-space indentation, never tabs. Every `.ncl` file ends with an export record `{ ... }`.
- Every persisted Nickel document carries an explicit top-level numeric `version`.
- `vN.ncl` exports `validate_document_vN`, `envelope_contract_vN`, and its migration function. `mod.ncl` is registry/dispatcher only: `current_version`, `supported_versions`, `migrate_to`, `SupportedVersion`, and unversioned aliases. No contract definitions in `mod.ncl`.
- Strict version separation: each `vN.ncl` is self-contained, defines only its own `*VN` contract names, and never references other version contracts. `mod.ncl` is the only multi-version importer.
- Each version keeps its own envelope shape. Migration placement: each version owns migration into that version (`v2.ncl` exports `migrate_v1_to_v2`, `v1.ncl` exports `migrate_v2_to_v1`); `mod.ncl` dispatches only.
- Migration edges build fresh records: strip input `version` with `std.record.remove "version"` before merging `& { version = N }`.
- Contract patterns: `std.contract.from_predicate` for custom contracts; `std.contract.any_of` for tagged unions; `{ _ : Type }` for maps; `{ .. }` only with a `# |||` why-comment; `{ field | Contract | optional }` and `| default = value`; `Dyn` only where no closed set exists.

## Nickel strictness S1-S13

- **S1**: Closed records by default. No `..` without a `# |||` why-comment.
- **S2**: No untyped values where a closed set exists. Tag-like fields use `from_predicate`/`any_of` mirroring Rust enums exactly.
- **S3**: Integer guards on every integral `Number` (`std.number.is_integer` or floor equality).
- **S4**: Required top-level `version` marker on every persisted document contract.
- **S5**: Per-version exports: `validate_document_vN`, `envelope_contract_vN`, migration function, `# |||` docs, export record.
- **S6**: Unversioned registry surface in `mod.ncl`: `current_version`, `supported_versions`, `migrate_to`, `SupportedVersion`, unversioned aliases.
- **S7**: `#[serde(deny_unknown_fields)]` on every Rust serde config/state/envelope/document boundary type.
- **S8**: No catch-all `Value`/`Dyn`/bare dict where a closed shape exists; use typed untagged enums with per-variant `deny_unknown_fields`.
- **S9**: Fail-fast on unknown names; never silent ignore.
- **S10**: Exported contracts are evaluated sources. `export_config_contracts` writes them; runtime never reads them back.
- **S11**: Migration output satisfies the target contract, proven by round-trip tests.
- **S12**: Schema/Rust parity tests assert strictness (closed records, integer guards, enums), not just field presence.
- **S13**: New schemas follow the same cycle and coverage-matrix updates.

## Nickel pitfalls

- Nickel numbers are floats; guard integers with `std.number.is_integer`.
- Use `let ... in` and `import "path"`. Apply top-level envelope contracts as `data | NickelDocumentV2`.
- `std.string.is_string` does not exist in Nickel 0.18; use `std.is_string`. Guard before `std.string.is_match` because it errors on non-strings.
- Plain functions as contracts are broken; use `std.contract.from_predicate`. `std.contract.from_validator` expects `'Ok value` / `'Error {...}`, not bool.
- Record shorthand self-reference fails (`{ X, ... }` missing definition, `{ x = x }` infinite recursion). Export with `let _x = X in { X = _x, ... }`.
- Contracts are not JSON-serializable; export wrappers/functions or `let` aliases.

## Hash, checksum, and normalization

- Delegate multihash parsing/serialization to `multihash` through `Hash` helpers. No hand-rolled varint parser.
- Tie an integrity field to the bytes it covers and verify it where the envelope is validated. An unverified older field is read as-is; do not add another one.
- Do not apply Unicode NFD normalization in versioned structs or envelope fields; NFD is reserved for mediapm internal filepath handling.
