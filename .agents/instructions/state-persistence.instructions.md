---
description: "Use when editing state persistence in src/mediapm/src/config/mod.rs, config/versions/, and src/mediapm/src/state/. Covers MediaPmState fields, ToolRegistryEntry, ManagedFileRecord, schema version dispatch, JSON I/O, and migration rules."
name: "State Persistence"
applyTo: "src/mediapm/src/config/mod.rs, src/mediapm/src/config/versions/**/*.rs, src/mediapm/src/state/**/*.rs"
---

# State persistence

## Purpose

- Model machine-managed state persisted in `state.json` under `<runtime_root>/`.
- Track managed file records, tool fetch/deploy metadata, and media workflow step state.
- Support schema version dispatch for forward/backward migration from legacy `.ncl` formats.
- **No `MigrateState` trait** — migration helpers are plain functions in `state/versions/v1.rs` (no trait dispatch). This avoids trait overhead for a single-migration-path design.
- **File organization**: public API lives in `state/ser.rs` (thin delegation), V1 wire types and migration in `state/versions/v1.rs`, V2 wire types in `state/versions/v2.rs`, version dispatch utilities in `state/versions/mod.rs`.

## `MediaPmState` fields (v2)

| Field             | Type                                         | Purpose                                         |
| ----------------- | -------------------------------------------- | ----------------------------------------------- |
| `version`         | `u32`                                        | Schema version marker (for migration dispatch)  |
| `managed_files`   | `BTreeMap<String, ManagedFileRecord>`        | Materialized files keyed by output path         |
| `managed_tools`   | `BTreeMap<String, ToolRegistryEntry>`        | Fetched/deployed tool registry keyed by tool id |
| `workflow_states` | `BTreeMap<String, ManagedWorkflowStepState>` | Per-media-source workflow step state            |

## `ToolRegistryEntry` (v2)

| Field               | Type             | Purpose                                                        |
| ------------------- | ---------------- | -------------------------------------------------------------- |
| `version`           | `Option<String>` | Tool version as fetched                                        |
| `tag`               | `Option<String>` | Tag as fetched                                                 |
| `fetch_hash`        | `Option<String>` | CAS content hash of the fetched payload                        |
| `canonical_version` | `String`         | Canonical version identifier used for skip-if-up-to-date logic |
| `deployed_at`       | `u64`            | Unix-epoch seconds when deployed (0 = not yet)                 |

## `ToolRegistryEntry` vs legacy `ActiveToolInstance`

`ToolRegistryEntry.deployed_at` supersedes the removed `ActiveToolInstance` struct. The "active" tool is resolved by querying `managed_tools` and sorting entries for the same logical tool id by `deployed_at` descending — the latest-deployed entry is the current active version. Multiple entries per tool id are expected (each fetch+deploy cycle creates a new entry).

## `ManagedFileRecord`

```rust
/// Records which media source and variant produced a managed file.
pub struct ManagedFileRecord {
    /// Media id that produced this managed file.
    pub media_id: String,
    /// Output variant selected for this materialized file.
    pub variant: String,
    /// Canonical CAS hash string for this file's payload.
    pub hash: String,
}
```

## V1 format (pre-rewrite wrapper — migration-only, never written)

The pre-rewrite format wrapped state in a `state` key:

```text
{
  "version": 1,
  "state": {
    "managed_files": { "<path>": { "media_id": "...", "variant": "...", "hash": "..." } },
    "tool_registry": { "<key>": { "name": "...", "version": "...", "source": "...", "registry_multihash": "...", "last_transition_unix_seconds": 0 } },
    "active_tools": { "<id>": "<key>" },
    "workflow_states": { "<media_id>": [ { "variant_hashes": {...}, "steps_completed": 0, "last_impure_sync_at": null } ] },
    "last_materialized_state_hash": null
  }
}
```

V1→v2 mapping:

- `managed_files`: record→record with same shape (kept as-is).
- `tool_registry`: dropped — replaced by `managed_tools` with renamed fields.
- `active_tools`: dropped — superseded by `deployed_at` ordering.
- `workflow_states`: `Vec<T>` → `T` (take last entry or first).
- `last_materialized_state_hash`: dropped (dead field).

## V1 flat format (post-rewrite — migration-only, never written)

The post-rewrite flat format (current `state.ncl`):

```text
{
  "version": 1,
  "media": { "<id>": { "variant_hashes": {...}, "steps_completed": 0, "last_impure_sync_at": null } },
  "tools": { ... },
  "tool_registry": { ... },
  "active_tools": { ... },
  "last_materialized_state_hash": "",
  "managed_files": [ "<path>" ]
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

- `MEDIAPM_STATE_VERSION = 2` (independent constant from `MEDIAPM_DOCUMENT_VERSION`).
- `MediaPmState.version` uses `state_version` default, not `document_version`.
- V1 = legacy Nickel formats (both wrapper and flat).
- V2 = current JSON format (always written, never reverted).

## Version dispatch

- On load (`state/ser.rs::from_json_value`): delegates to `versions::extract_state_version_field`, then `versions::v1::from_v1_json_value` or `versions::v2::from_v2_json_value`.
- On save (`state/ser.rs::to_json_value`): delegates to `versions::v2::to_v2_json_value` (always V2).
- Migration from `.ncl` (`state/ser.rs::migrate_from_old_nickel`): delegates to `versions::v1::from_v1_json_value` which handles both wrapper and flat V1 shapes → writes `state.json` → deletes `state.ncl`.

## `deployed_at` ordering semantics

`managed_tools` may contain multiple entries for the same logical tool id (e.g. `yt-dlp@hash1`, `yt-dlp@hash2`). Resolve "active tool" by:

1. Filter entries where `tool_id` matches the desired logical tool.
2. Sort by `deployed_at` descending.
3. Return the first entry (latest deploy).

`deployed_at` is a Unix-epoch timestamp in seconds. `0` means "not yet deployed".

## Normalization / retain rules

- `managed_files`: remove entries with empty/whitespace-only keys.
- `managed_tools`: retain only entries where at least one of `version`, `tag`, or `canonical_version` is non-empty.
- `workflow_states`: no special normalization.
- Normalization runs in `MediaPmState::normalize()`.

## Canonical version resolution

`canonical_version` is populated by the provisioning pipeline at fetch time.
The resolve phase determines it from available data (GitHub tag, VCS hash,
etc.) and stores it in the resulting `ToolRegistryEntry`. The semantic kind
(VCS hash vs version vs tag) is fixed per tool at code-writing time — each
tool's provider always returns the same kind of identifier. No runtime
fallback chain exists.

When comparing canonical versions for skip-if-up-to-date logic, use exact
string equality. All providers use the resolved tag verbatim — no prefix
transformation is applied.
