---
description: "Use when editing companion dependency resolution in src/mediapm/src/conductor_bridge/sync/tool_config.rs. Covers ffmpeg/deno companion selector resolution, same-step vs cross-step strategy, and content-map prefixing."
name: "Tool Sync Companion Dependencies"
applyTo: "src/mediapm/src/conductor_bridge/sync/tool_config.rs"
---

# Tool sync companion dependencies

## Purpose

- Resolve companion tool version selectors (ffmpeg, deno) from `ToolRequirementDependencies` so the coordinator can bind companion payloads into the requester's content map or as separate tools.

## Companion selector resolution

### `resolve_companion_ffmpeg_selection(requirements)`

- Iterates all tool requirements in insertion order.
- Returns the first `ffmpeg_version` literal value that is non-empty and not `"inherit"`.
- Returns `None` if no override is specified (global default applies).

### `resolve_companion_deno_selection(requirements)`

- Same logic as ffmpeg, but for `deno_version`.

## Search rules

- Only `MediaMetadataValue::Literal(v)` values are considered — `Variant` and `Fallback` selectors are skipped.
- The string `"inherit"` (case-insensitive) signals "use global default" and is skipped.
- First non-default literal wins — there is no merging or aggregation.

## Same-step vs cross-step strategy

- **Same-step** (companion dependency of the same step, e.g. yt-dlp requiring ffmpeg or deno): companion payload bytes are inlined into the requester's content_map with a prefix (e.g. `companions/`). The companion selector identity is folded into the requester tool id.
- **Cross-step** (workflow-expanded steps that invoke other logical tools): payload bytes and ids remain separate. Companion selector identity does NOT fold into the requester tool id.

## Content-map prefix helpers

- `prefix_same_step_companion_content_map(prefix, map)`: prepends `prefix` to each content-map key.
- `prefix_same_step_companion_content_entries(prefix, entries)`: same as above, for `BTreeMap<String, String>` entries.

Currently these helpers are defined but companion binding is not yet wired in the coordinator (stubs in `reconcile_desired_tools`).
