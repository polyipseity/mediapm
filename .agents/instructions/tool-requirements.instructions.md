---
description: "Use when editing tool requirement configuration in src/mediapm/src/config/mod.rs. Covers ToolRequirement fields, flattened dependency BTreeMap, and normalization rules."
name: "Tool Requirements"
applyTo: "src/mediapm/src/config/mod.rs"
---

# Tool requirements

## `ToolRequirement` fields

| Field              | Type                                  | Default                     | Purpose                                                                            |
| ------------------ | ------------------------------------- | --------------------------- | ---------------------------------------------------------------------------------- |
| `version_spec`     | `ConfigVersionSpec`                   | `Latest`                    | Version specification: `"latest"`, `"inherit"`, or `{ vcs_hash?, version?, tag? }` |
| `dependencies`     | `BTreeMap<String, ConfigVersionSpec>` | `{}`                        | Map of dependency tool id → version spec                                           |
| `recheck_seconds`  | `u64`                                 | `0` (use default heuristic) | Recheck interval for metadata freshness                                            |
| `max_input_slots`  | `u32`                                 | from `defaults`             | Max ffmpeg input slot count                                                        |
| `max_output_slots` | `u32`                                 | from `defaults`             | Max ffmpeg output slot count                                                       |

`version_spec` replaces the old `version`/`tag`/`desired_git_hash`/`desired_tag`/`desired_version`
fields. See `ConfigVersionSpec` in the conductor provider module for the full serde format.

The `dependencies` field is now a flat map: `dependencies = { ffmpeg = "inherit", deno = "latest" }`.
No nested `.deps` wrapper, no `dep_type` — companion binding role flags are
determined by per-preset `known_dependency_type()` lookup, not by user config.

## `ConfigVersionSpec` enum

`ConfigVersionSpec` is the config-facing serde type; `Inherit` is resolved away
at the config boundary into the internal `VersionSpec` (`Latest`/`Exact` only,
never serialized).

| Variant                    | Meaning                                                                        |
| -------------------------- | ------------------------------------------------------------------------------ |
| `Latest`                   | Fetch the latest available version (`"latest"`).                               |
| `Inherit`                  | Use the global tool version spec from `tools.<id>.version_spec` (`"inherit"`). |
| `Exact(VersionSpecFields)` | Exact fields: `{ vcs_hash?, version?, tag? }`                                  |

## `VersionSpecFields` struct

| Field      | Type             | Purpose                                   |
| ---------- | ---------------- | ----------------------------------------- |
| `vcs_hash` | `Option<String>` | VCS hash (git commit, etc.). Exact match. |
| `version`  | `Option<String>` | Version string. Exact match.              |
| `tag`      | `Option<String>` | VCS tag. Exact match.                     |

At least one field must be non-`None` (enforced at deserialization).
Multiple fields may be present; when they are, all must match at
provision time or provisioning errors.

## Normalization rules

- `ToolRequirement` entries are kept during normalization if `version_spec` is set (any variant). Old `version`/`tag` fields no longer exist — the single `version_spec` field is authoritative.
- `dependencies` entries with `ConfigVersionSpec::Inherit` are treated as "use global default" — they are not removed but resolved at provision time.
- Normalization runs in `MediaPmDocument::normalize()` and `MediaPmState::normalize()`.

## Dependency types (`DependencyTypes`)

Role flags on a `bool`-field struct defined in `src/mediapm/src/tools/dependency.rs`. A dependency may carry one or both roles. Not user-configurable; no serde derives.

| Role flag | Meaning |
|---|---|
| `SAME_STEP` | Folded into the same step as a companion |
| `CROSS_STEP` | Invoked as a separate workflow step |

`SAME_STEP` and `CROSS_STEP` are the single-role constants; `combine()` unions them. The companion relationship type is determined by per-preset `known_dependency_type()` lookup, not by user config.

## Spec matching (`spec_matches_entry`)

`spec_matches_entry(spec, resolved_tag: Option<&str>, resolved_version: Option<&str>, resolved_vcs_hash: Option<&str>) -> bool`.

- `VersionSpec::Latest` always returns `false` (caller re-resolves); `Inherit` is resolved away before this point.
- `VersionSpec::Exact(fields)`: each specified field matches only when stored is `Some` AND equals the spec; unspecified fields are unchecked; stored `None` never matches (missing field means always re-provision).
- All comparisons are exact string match — no trim, no semver normalization.
