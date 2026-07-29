---
description: "Use when editing tool requirement configuration in src/mediapm/src/config/mod.rs. Covers ToolRequirement fields, ToolRequirementDependencies selectors, and normalization rules."
name: "Tool Requirements"
applyTo: "src/mediapm/src/config/mod.rs"
---

# Tool requirements

## Purpose

- Model how users declare managed tool version requirements in `mediapm.ncl` under `tools.<id>`.
- Provide typed dependency declarations via `DependencySpec` for cross-tool
  companion resolution.

## `ToolRequirement` fields

| Field              | Type                          | Default                     | Purpose                                    |
| ------------------ | ----------------------------- | --------------------------- | ------------------------------------------ |
| `version_spec`     | `VersionSpec`                 | `Latest`                    | Version specification: `"latest"`, `"inherit"`, or `{ vcs_hash?, version?, tag? }` |
| `dependencies`     | `ToolRequirementDependencies` | default                     | Cross-tool dependency specs                |
| `recheck_seconds`  | `u64`                         | `0` (use default heuristic) | Recheck interval for metadata freshness    |
| `max_input_slots`  | `u32`                         | from `defaults`             | Max ffmpeg input slot count                |
| `max_output_slots` | `u32`                         | from `defaults`             | Max ffmpeg output slot count               |

`version_spec` replaces the old `version`/`tag`/`desired_git_hash`/`desired_tag`/`desired_version`
fields. See `VersionSpec` in the conductor provider module for the full serde format.

## `ToolRequirementDependencies` fields

| Field  | Type                           | Purpose                                             |
| ------ | ------------------------------ | --------------------------------------------------- |
| `deps` | `BTreeMap<String, DependencySpec>` | Map of dependency tool id → dependency spec          |

Replaces the old flat `ffmpeg_version`/`deno_version`/`sd_version` fields.

## `DependencySpec` struct

| Field          | Type             | Default      | Purpose                                                    |
| -------------- | ---------------- | ------------ | ---------------------------------------------------------- |
| `dep_type`     | `DependencyType` | `Inter`      | Companion relationship type (same-step vs cross-step)      |
| `version_spec` | `VersionSpec`    | `Inherit`    | Version spec: `"latest"`, `"inherit"`, or exact fields    |

## `DependencyType` enum

| Variant   | Meaning                                                                 |
| --------- | ----------------------------------------------------------------------- |
| `Cross`   | Invoked as a separate workflow step (cross-step dependency).            |
| `Inter`   | Folded into the same step as a companion (interstep / same-step).       |
| `Both`    | Functions as both interstep AND cross-step.                             |

Default is `Inter`.

## `VersionSpec` enum

| Variant   | Meaning                                                                 |
| --------- | ----------------------------------------------------------------------- |
| `Latest`  | Fetch the latest available version (`"latest"`).                        |
| `Inherit` | Use the global tool version spec from `tools.<id>.version_spec` (`"inherit"`). |
| `Exact(VersionSpecFields)` | Exact fields: `{ vcs_hash?, version?, tag? }`                    |

## `VersionSpecFields` struct

| Field      | Type     | Purpose                                    |
| ---------- | -------- | ------------------------------------------ |
| `vcs_hash` | `Option<String>` | VCS hash (git commit, etc.). Exact match. |
| `version`  | `Option<String>` | Version string. Exact match.               |
| `tag`      | `Option<String>` | VCS tag. Exact match.                      |

At least one field must be non-`None` (enforced at deserialization).
Multiple fields may be present; when they are, all must match at
provision time or provisioning errors.

## Normalization rules

- `ToolRequirement` entries are kept during normalization if `version_spec`
  is set (any variant). Old `version`/`tag` fields no longer exist — the
  single `version_spec` field is authoritative.
- `ToolRequirementDependencies` with `VersionSpec::Inherit` are treated as
  "use global default" — they are not removed but resolved at provision time.
- Normalization runs in `MediaPmDocument::normalize()` and
  `MediaPmState::normalize()`.
