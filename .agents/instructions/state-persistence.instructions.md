---
description: "Use when editing state persistence in src/mediapm/src/config/mod.rs, config/versions/, and src/mediapm/src/state/. Covers MediaPmState fields, ToolRegistryEntry, ManagedFileRecord, schema version dispatch, JSON I/O, and migration rules."
name: "State Persistence"
applyTo: "src/mediapm/src/config/mod.rs, src/mediapm/src/config/versions/**/*.rs, src/mediapm/src/state/**/*.rs"
---

# State persistence

## Format reality

- `state.json` is machine-managed and stored as **JSON with pretty-printing and always-write semantics** — it is not a Nickel document, even though legacy versions of this file were `.ncl`.
- `mediapm.ncl` is the user-owned Nickel intent document; `conductor.generated.ncl` is the machine-managed Nickel runtime document. Only `state.json` is JSON.
- Legacy `state.ncl` files are auto-migrated on load and then deleted (`load_mediapm_state_document` in `nickel_io.rs` is JSON-first, falls back to the legacy Nickel file, and removes it after a successful load). Never write new `.ncl` state files.

## Purpose

- Model machine-managed state persisted in `state.json` under `<runtime_root>/`.
- Track managed file records, tool fetch/deploy metadata, and media workflow step state.
- Support schema version dispatch for forward/backward migration from legacy `.ncl` formats.
- **No `MigrateState` trait** — migration helpers are plain functions in `state/versions/v1.rs` (no trait dispatch). This avoids trait overhead for a single-migration-path design.
- **File organization**: public API lives in `state/ser.rs` (thin delegation), V1 wire types and migration in `state/versions/v1.rs`, V2 wire types in `state/versions/v2.rs`, version dispatch utilities in `state/versions/mod.rs`.

## `MediaPmState` fields (v3)

| Field             | Type                                         | Purpose                                         |
| ----------------- | -------------------------------------------- | ----------------------------------------------- |
| `version`         | `u32`                                        | Schema version marker (for migration dispatch)  |
| `managed_files`   | `BTreeMap<String, ManagedFileRecord>`        | Materialized files keyed by output path         |
| `managed_tools`   | `Vec<ToolRegistryEntry>`                     | Flat list of tool deployment metadata entries   |
| `workflow_states` | `BTreeMap<String, ManagedWorkflowStepState>` | Per-media-source workflow step state            |

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
Empty is `None` — the empty string `""` is invalid for these fields and is
rejected by the parser (no normalization, no migration). Stale state files
containing `""` fail to load and are discarded/regenerated on the next run.

The schema change is applied **in place**: the existing V3 schema keeps its
version marker (3) — there is no version bump, no V4 wire format, and no
migration code. Old and new files are distinguished by content, not by version
number.

**Why-empty invariant:** any field left `None` must be documented — an inline
`// WHY:` comment on the provider dispatch arm, the per-tool provider module
doc comment, and the per-tool row in `provider-dispatch.instructions.md`.
Current `None` fields: ffmpeg `resolved_version`/`resolved_vcs_hash` (mixed
sources; the BtbN build-repo hash is not the upstream ffmpeg source commit)
and media-tagger `resolved_tag` (builtin launcher, no upstream tag).

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

The post-rewrite flat format (current `state.json`):

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

- `MEDIAPM_STATE_VERSION = 3` (independent constant from `MEDIAPM_DOCUMENT_VERSION`).
- `MediaPmState.version` uses `state_version` default, not `document_version`.
- V1 = legacy Nickel formats (both wrapper and flat).
- V2 = intermediate JSON format, migrated to V3 on load (never written).
- V3 = current JSON format (always written, never reverted). Schema changes to V3 are applied in place — no version bump for field-level changes.

## Version dispatch

- On load (`state/ser.rs::from_json_value`): delegates to `versions::extract_state_version_field`, then `versions::v1::from_v1_json_value`, `versions::v3::from_v2_into_v3` (version 2), or `versions::v3::from_v3_json_value` (version 3).
- On save (`state/ser.rs::to_json_value`): delegates to `versions::v3::to_v3_json_value` (always V3).
- Migration from `.ncl` (`state/ser.rs::migrate_from_old_nickel`): delegates to `versions::v1::from_v1_json_value` which handles both wrapper and flat V1 shapes → writes `state.json` → deletes `state.ncl`.

## `deployed_at` ordering semantics

`managed_tools` may contain multiple entries for the same logical tool id (e.g. `yt-dlp@hash1`, `yt-dlp@hash2`). Resolve "active tool" by:

1. Filter entries where `tool_id` matches the desired logical tool.
2. Sort by `deployed_at` descending.
3. Return the first entry (latest deploy).

`deployed_at` is a Unix-epoch timestamp in seconds. `0` means "not yet deployed".

## Normalization / retain rules

- `managed_files`: remove entries with empty/whitespace-only keys.
- `managed_tools`: retain only entries with a non-empty `canonical_version` or at least one `Some` `resolved_*` field. No trim/empty-string guards are applied to the `resolved_*` `Option` fields — empty is `None`, never `""`.
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
string equality. GitHub-release-based tools (yt-dlp, deno, rsgain, sd) use the
resolved commit hash as canonical version; ffmpeg uses the composite
`"{autobuild_tag}+evermeet-{evermeet_version}"`; media-tagger uses the
mediapm build-time git hash. The `resolved_*` provenance fields are separate
and informational — they never participate in skip/update decisions.

## State write policy

`state.json` is written unconditionally via `std::fs::write` after every sync
pass. No byte-level change detection is applied. This is intentional:

- `state.json` serves as the runtime audit trail, recording every sync
  invocation's observed metadata (canonical version, deploy timestamp, fetch
  hash).
- A change in `canonical_version` (e.g., a rotating autobuild tag) that
  produces identical binary payloads is still a meaningful state change — the
  tool's upstream label advanced, and `state.json` records that.
- The companion document `conductor.generated.ncl` absorbs the
  artifact-stability concern via its own change-detected write policy
  (`write_bytes_if_changed`).

**Invariant:** `state.json` content changes are not errors. A diff showing only
`canonical_version`, `deployed_at`, or `resolved_*` changes with unchanged
`content_map_hash` indicates metadata churn without payload change — expected
behavior.
