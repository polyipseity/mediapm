---
description: "Use when editing state persistence in src/mediapm/src/config/mod.rs, config/versions/, and src/mediapm/src/state/. Covers MediaPmState, ToolRegistryEntry, ManagedFileRecord, version dispatch, JSON I/O, migration rules."
name: "State Persistence"
applyTo: "src/mediapm/src/config/mod.rs, src/mediapm/src/config/versions/**/*.rs, src/mediapm/src/state/**/*.rs"
---

# State persistence

## Format reality

- `state.json` is machine-managed **JSON with pretty-printing and always-write semantics**, not a Nickel document, though legacy versions were `.ncl`.
- `mediapm.ncl` is the user-owned Nickel intent document; `conductor.generated.ncl` is the machine-managed Nickel runtime document. Only `state.json` is JSON.
- Legacy `state.ncl` files are auto-migrated on load and then deleted (`load_mediapm_state_document` in `nickel_io.rs` is JSON-first, falls back to the legacy Nickel file, and removes it after a successful load). Never write new `.ncl` state files.
- A `state.json` that names a schema version is decoded through that version's reader, and any failure is reported: a document whose body does not parse, or that names a version this build does not know, aborts the load instead of being read as empty. Reading it as empty would discard recorded files and let the next scan treat every one of them as stale. A document that names no version is refused for the same reason: every writer emits the field first, so such a file is foreign or damaged, and no rung of the ladder has a reader that could be trusted with it. The only state a load rebuilds is the one that does not exist.

State persisted in `state.json` under `<runtime_root>/` tracks managed file records and tool fetch/deploy metadata, with version dispatch for migration from both legacy `.ncl` formats and older JSON versions. It records no per-media workflow progress: the conductor records what each tool-call instance produced, and `managed_files` records what reached the library. The conductor's own state file (`state.conductor.json`) keeps its own integer ladder, documented in `src/mediapm-conductor/AGENTS.md` section D; the two documents are loaded independently and neither is checked against the other.

- **No `MigrateState` trait**: migration helpers are plain functions in each `state/versions/vN.rs` (no trait dispatch), because a hop is a function and a trait would add a name per hop without removing one.
- **File organization**: public API in `state/ser.rs` (thin delegation), wire types and the migration into each version in `state/versions/vN.rs`, version dispatch utilities in `state/versions/mod.rs`. `v1.rs` reads both legacy Nickel shapes, `v2.rs` through `v4.rs` are read-only historical formats, and `v5.rs` holds the current shape plus the runtime bridge.

## `MediaPmState` fields (v5)

| Field             | Type                                  | Purpose                                         |
| ----------------- | ------------------------------------- | ----------------------------------------------- |
| `version`         | `u32`                                 | Schema version marker (for migration dispatch)  |
| `managed_files`   | `BTreeMap<String, ManagedFileRecord>` | Materialized files keyed by output path         |
| `managed_tools`   | `Vec<ToolRegistryEntry>`              | Flat list of tool deployment metadata entries   |

## `ToolRegistryEntry` (v3)

| Field               | Type             | Purpose                                                        |
| ------------------- | ---------------- | -------------------------------------------------------------- |
| `tool_id`           | `String`         | Bare logical tool id matching the key in `desired_tools`       |
| `version`           | `String`         | Human-readable version as fetched (informational only)         |
| `canonical_version` | `String`         | Canonical version identifier used for skip-if-up-to-date logic |
| `content_map_hash`  | `String`         | blake3 hash of the content_map JSON (content-addressed identity); empty for no-payload tools |
| `deployed_at`       | `u64`            | Unix-epoch seconds when deployed (0 = not yet)                 |
| `resolved_tag`      | `Option<String>` | Provenance: resolved upstream git tag, or `None`               |
| `resolved_version`  | `Option<String>` | Provenance: resolved upstream version, or `None`               |
| `resolved_vcs_hash` | `Option<String>` | Provenance: resolved upstream VCS commit hash, or `None`       |

### `resolved_*` provenance fields

The three `resolved_*` fields are `Option<String>`: `None` serializes as JSON
`null`, and a missing field deserializes to `None` via `#[serde(default)]`.
Empty is `None`; the empty string `""` is invalid for these fields and is
rejected by the parser (no normalization, no migration). Stale state files
containing `""` fail to load, and the error propagates rather than being
rebuilt: the document names a version, so its contents were written to hold
data.

The `resolved_*` schema change is applied **in place** within the version that
introduced it: it keeps that version's marker, with no extra wire format and no
migration code, and old and new files are distinguished by content rather than
by version number. Every later shape change is a version bump.

**Why-empty invariant:** any field left `None` must be documented via an inline
`// WHY:` comment on the provider dispatch arm, the per-tool provider module
doc comment, and the per-tool row in `provider-dispatch.instructions.md`.
Current `None` fields: ffmpeg `resolved_version`/`resolved_vcs_hash` (mixed
sources; the BtbN build-repo hash is not the upstream ffmpeg source commit)
and media-tagger `resolved_tag` (builtin launcher, no upstream tag).

## `ToolRegistryEntry` vs legacy `ActiveToolInstance`

`ToolRegistryEntry.deployed_at` supersedes the removed `ActiveToolInstance` struct. The "active" tool is resolved by querying `managed_tools` and sorting entries for the same logical tool id by `deployed_at` descending; the latest-deployed entry is the current active version. Multiple entries per tool id are expected (each fetch+deploy cycle creates a new entry).

## `ManagedFileRecord`

```rust
/// Records which media source and variant produced a managed file.
pub struct ManagedFileRecord { /// Media id that produced this managed file. pub media_id: String, /// Output variant selected for this materialized file. pub variant: String, /// Canonical CAS hash string for this file's payload. pub hash: String,
}
```

## V1 format (pre-rewrite wrapper — migration-only, never written)

The pre-rewrite format wrapped state in a `state` key:

```text
{ "version": 1, "state": { "managed_files": { "<path>": { "media_id": "...", "variant": "...", "hash": "..." } }, "tool_registry": { "<key>": { "name": "...", "version": "...", "source": "...", "registry_multihash": "...", "last_transition_unix_seconds": 0 } }, "active_tools": { "<id>": "<key>" }, "workflow_states": { "<media_id>": [ { "variant_hashes": {...}, "steps_completed": 0, "last_impure_sync_at": null } ] }, "last_materialized_state_hash": null }
}
```

V1→v2 mapping:

- `managed_files`: record→record with same shape (kept as-is).
- `tool_registry`: dropped — replaced by `managed_tools` with renamed fields.
- `active_tools`: dropped — superseded by `deployed_at` ordering.
- `workflow_states`: `Vec<T>` → `T` (take last entry or first). V5 drops the map; the field remains part of the V1 wire shape so a legacy document parses.
- `last_materialized_state_hash`: dropped (dead field).

## V1 flat format (post-rewrite — migration-only, never written)

The post-rewrite flat format:

```text
{ "version": 1, "media": { "<id>": { "variant_hashes": {...}, "steps_completed": 0, "last_impure_sync_at": null } }, "tools": { ... }, "tool_registry": { ... }, "active_tools": { ... }, "last_materialized_state_hash": "", "managed_files": [ "<path>" ]
}
```

Flat→v2 mapping:

- `media` → rename to `workflow_states` (type unchanged).
- `tools`: dropped (redundant snapshot with document).
- `tool_registry`: renamed to `managed_tools`.
- `active_tools`: dropped.
- `last_materialized_state_hash`: dropped.
- `managed_files`: `BTreeSet<String>` → `BTreeMap<String, ManagedFileRecord>` via migration helper (`migrate_flat_managed_files` assigns empty `{ media_id: "", variant: "", hash: "" }` records as placeholders).

## State-specific versioning scheme

- `MEDIAPM_STATE_VERSION = 5` in `src/mediapm/src/config/defaults.rs` (independent constant from `MEDIAPM_DOCUMENT_VERSION`).
- `MediaPmState.version` uses `state_version` default, not `document_version`.
- V1 = legacy Nickel formats (both wrapper and flat).
- V2 = intermediate JSON format, read-only: decoded as V3, then migrated forward on load (never written).
- V3 = JSON format before instance invalidation: read-only, migrated forward on load (never written).
- V4 = JSON format with the per-media workflow map: read-only, migrated to V5 on load (never written).
- V5 = current JSON format: no per-media workflow map (always written, never reverted).
- A schema change is a version bump. Versions are integers; a field that appears or disappears is a new `vN` module plus a `from_v(N-1)_into_vN` migration, never an in-place edit of the current version's shape. The `resolved_*` fields are the one recorded exception, and it narrows rather than widens: a missing field reads as `None`, nothing changes what is written, and no file that loaded before stops loading.

## Version dispatch

- On load (`state/ser.rs::from_json_value` → `state/versions/mod.rs::from_json_value`): `versions::extract_state_version_field`, then `v1::from_v1_json_value` (1), `v5::resolve_v5(v5::from_v4_into_v5(v4::from_v3_into_v4(v3::decode_v2_as_v3(..))))` (2), the same composition over `v3::decode_v3(..)` (3), `v5::resolve_v5(v5::from_v4_into_v5(v4::decode_v4(..)))` (4), or `v5::from_v5_json_value` (5). Anything else is an `unsupported mediapm state schema version` error.
- On save (`state/ser.rs::to_json_value` → `state/versions/mod.rs::to_json_value`): `v5::to_v5_json_value` (always V5).
- Migration from `.ncl` (`state/ser.rs::migrate_from_old_nickel`): delegates to `versions::v1::from_v1_json_value` which handles both wrapper and flat V1 shapes → writes `state.json` → deletes `state.ncl`.

## `deployed_at` ordering semantics

`managed_tools` may contain multiple entries for the same logical tool id (e.g. `yt-dlp@hash1`, `yt-dlp@hash2`). Resolve "active tool" by:

1. Filter entries where `tool_id` matches the desired logical tool.
2. Sort by `deployed_at` descending.
3. Return the first entry (latest deploy).

`deployed_at` is a Unix-epoch timestamp in seconds. `0` means "not yet deployed".

## Normalization / retain rules

- `managed_files`: remove entries with empty/whitespace-only keys.
- `managed_tools`: retain only entries with a non-empty `canonical_version` or at least one `Some` `resolved_*` field. No trim/empty-string guards are applied to the `resolved_*` `Option` fields; empty is `None`, never `""`.
- Normalization runs in `MediaPmState::normalize()`.

## Canonical version resolution

`canonical_version` is populated by the provisioning pipeline at fetch time.
The resolve phase determines it from available data (GitHub tag, VCS hash,
etc.) and stores it in the resulting `ToolRegistryEntry`. The semantic kind
(VCS hash vs version vs tag) is fixed per tool at code-writing time; each
tool's provider always returns the same kind of identifier. No runtime
fallback chain exists.

When comparing canonical versions for skip-if-up-to-date logic, use exact
string equality. GitHub-release-based tools (yt-dlp, deno, rsgain, sd) use the
resolved commit hash as canonical version; ffmpeg uses the composite
`"{autobuild_tag}+{macos_leg_version}"` (the macOS leg is the martin-riedl build
id or jellyfin version actually fetched); media-tagger uses the
mediapm build-time git hash. The `resolved_*` provenance fields are separate
and informational; they never participate in skip/update decisions.

What is described above is the provider-resolved segment only. The value
persisted in `ToolRegistryEntry.canonical_version` is a composite that also
carries the tool's SameStep dependency segments; see
`tool-sync-coordinator-and-identity.instructions.md` ("Composite
canonical_version") for the format, the non-transitive rule, and
`compute_composite_canonical_version()`.

## State write policy

`state.json` is written unconditionally via `std::fs::write` after every sync
pass. No byte-level change detection is applied: it is the runtime audit trail
recording each sync's observed metadata (canonical version, deploy timestamp,
fetch hash), and a rotating autobuild tag that yields identical payloads is
still a meaningful state change. `conductor.generated.ncl` absorbs
artifact-stability via its own change-detected write policy
(`write_bytes_if_changed`).

**Invariant:** `state.json` content changes are not errors. A diff showing only
`canonical_version`, `deployed_at`, or `resolved_*` changes with unchanged
`content_map_hash` indicates metadata churn without payload change, expected
behavior.
