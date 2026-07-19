---
description: "Use when editing state persistence in src/mediapm/src/config/mod.rs and config/versions/. Covers MediaPmState fields, ToolRegistryEntry, ActiveToolInstance, schema version dispatch, and normalization rules."
name: "State Persistence"
applyTo: "src/mediapm/src/config/mod.rs, src/mediapm/src/config/versions/**/*.rs"
---

# State persistence

## Purpose

- Model machine-managed state persisted in `state.ncl` under `<runtime_root>/`.
- Track tool fetch metadata, active deployments, media workflow state, and managed file sets.
- Support schema version dispatch for forward/backward migration.

## `MediaPmState` fields

| Field | Type | Purpose |
|-------|------|---------|
| `version` | `u32` | Schema version marker (for migration dispatch) |
| `media` | `BTreeMap<String, ManagedWorkflowStepState>` | Per-media-source workflow state |
| `tools` | `BTreeMap<String, ToolRequirement>` | Stale tool requirement snapshot (for diff detection) |
| `tool_registry` | `BTreeMap<String, ToolRegistryEntry>` | Fetched-tool registry keyed by tool id |
| `active_tools` | `BTreeMap<String, ActiveToolInstance>` | Active tool deployments keyed by tool id |
| `last_materialized_state_hash` | `String` | Hash of state snapshot at last materialization |
| `managed_files` | `BTreeSet<String>` | Set of files currently managed (for cleanup) |

## `ToolRegistryEntry`

| Field | Type | Purpose |
|-------|------|---------|
| `version` | `Option<String>` | Tool version as fetched |
| `tag` | `Option<String>` | Tag as fetched |
| `fetch_hash` | `Option<String>` | CAS content hash of the fetched payload |
| `deployed_at` | `u64` | Unix-epoch seconds when deployed (0 = not yet) |

## `ActiveToolInstance`

| Field | Type | Purpose |
|-------|------|---------|
| `tool_id` | `String` | Tool identifier for registry lookups |
| `content_hash` | `String` | CAS content hash of the deployed payload |
| `deployed_path` | `String` | Filesystem path to the deployed executable/bundle |

## Schema version dispatch

- `MediaPmState.version` determines which deserialization envelope to use.
- Current version constants are in `defaults::` (e.g. `MEDIAPM_DOCUMENT_VERSION`).
- Wire envelopes live per-version under `config/versions/v{n}.rs` with `Migrate` trait implementations.
- On load: detect version → deserialize matching envelope → migrate to current model.
- On save: always serialize as current version.

## Normalization / retain rules

- `tools`: retain only entries with non-empty `version` or `tag` (after trim).
- `tool_registry`: retain only entries where at least one of `version`/`tag` is non-empty.
- `managed_files`: remove empty/whitespace-only entries.
- Normalization runs in `MediaPmState::normalize()`.
