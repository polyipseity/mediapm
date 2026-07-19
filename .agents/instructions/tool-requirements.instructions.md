---
description: "Use when editing tool requirement configuration in src/mediapm/src/config/mod.rs and source_types.rs. Covers ToolRequirement fields, ToolRequirementDependencies selectors, MediaMetadataValue enum, and normalization rules."
name: "Tool Requirements"
applyTo: "src/mediapm/src/config/mod.rs, src/mediapm/src/config/source_types.rs"
---

# Tool requirements

## Purpose

- Model how users declare managed tool version/tag requirements in `mediapm.ncl` under `tools.<id>`.
- Provide selector-based dependency version requirements for cross-tool companion resolution.

## `ToolRequirement` fields

| Field              | Type                          | Default                     | Purpose                                    |
| ------------------ | ----------------------------- | --------------------------- | ------------------------------------------ |
| `version`          | `MediaMetadataValue`          | `Literal("")`               | Version metadata value or selector binding |
| `tag`              | `String`                      | `""`                        | Tag metadata value or selector binding     |
| `dependencies`     | `ToolRequirementDependencies` | default                     | Cross-tool dependency version selectors    |
| `recheck_seconds`  | `u64`                         | `0` (use default heuristic) | Recheck interval for metadata freshness    |
| `max_input_slots`  | `u32`                         | from `defaults`             | Max ffmpeg input slot count                |
| `max_output_slots` | `u32`                         | from `defaults`             | Max ffmpeg output slot count               |

Both `version` and `tag` serve as version selectors; a tool entry must have at least one non-empty value to be retained during normalization.

## `ToolRequirementDependencies` fields

| Field            | Type                 | Purpose                                             |
| ---------------- | -------------------- | --------------------------------------------------- |
| `ffmpeg_version` | `MediaMetadataValue` | Selector or literal for ffmpeg companion dependency |
| `deno_version`   | `MediaMetadataValue` | Selector or literal for deno companion dependency   |
| `sd_version`     | `MediaMetadataValue` | Selector or literal for sd dependency               |

Each dependency follows the same `MediaMetadataValue` enum. The string `"inherit"` signals "use global default" and is treated as empty by companion resolution.

## `MediaMetadataValue` enum

- `Literal(String)` — a concrete text value (e.g. `"2025.01.15"`).
- `Variant(MediaMetadataVariantBinding)` — extract a metadata key from a produced file variant.
- `Fallback(Vec<MediaMetadataValueCandidate>)` — ordered fallback list; first non-empty match wins.

## Normalization rules

- `ToolRequirement` entries without a meaningful `version` or `tag` (both empty/whitespace after trim) are **removed** during normalization.
- `ToolRequirementDependencies` selectors with value `"inherit"` or empty are treated as "use global default" by companion resolution — they are **not** removed from the struct but are skipped during selection.
- Normalization runs in `MediaPmDocument::normalize()` and `MediaPmState::normalize()`.
