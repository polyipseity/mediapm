---
description: "Use when editing tool-sync path resolution in src/mediapm/src/paths.rs. Covers MediaPmPaths fields, MediaPmPathOverrides, default layout under .mediapm/, and override resolution rules."
name: "Tool Sync Paths Layout"
applyTo: "src/mediapm/src/paths.rs"
---

# Tool sync paths layout

## Purpose

- Centralize filesystem path layout for all mediapm state, config, and cache directories.
- Ensure consistent path defaults and override resolution across CLI and library entry points.

## `MediaPmPaths` fields

| Field | Default | Purpose |
|-------|---------|---------|
| `root_dir` | workspace root | Top-level workspace directory |
| `runtime_root` | `{root_dir}/.mediapm/` | Runtime-owned state root |
| `mediapm_ncl` | `{root_dir}/mediapm.ncl` | User-edited policy config |
| `conductor_user_ncl` | `{root_dir}/mediapm.conductor.ncl` | Conductor user document |
| `conductor_generated_ncl` | `{root_dir}/mediapm.conductor.generated.ncl` | Conductor generated document |
| `conductor_state_config` | `{runtime_root}/state.conductor.ncl` | Conductor volatile state |
| `conductor_tmp_dir` | `$TMPDIR/mediapm-{hash}/` | Conductor sandbox tmp |
| `conductor_schema_dir` | `{runtime_root}/config/conductor/` | Conductor schema exports |
| `mediapm_state_ncl` | `{runtime_root}/state.ncl` | MediaPM machine state |
| `env_file` | `{runtime_root}/.env` | User-authored dotenv |
| `env_generated_file` | `{runtime_root}/.env.generated` | Machine-generated dotenv |
| `schema_export_dir` | `Some({runtime_root}/config/mediapm/)` | MediaPM schema exports (`None` = disabled) |
| `mediapm_tmp_dir` | `$TMPDIR/mediapm-{hash}/` | MediaPM staging tmp |
| `hierarchy_root_dir` | `{root_dir}` | Materialized media library root |
| `tools_dir` | `{runtime_root}/tools/` | Tool-content unpack directory |
| `cache/` | `{runtime_root}/cache/` | Cache root |
| `cache/store/` | `{runtime_root}/cache/store/` | Shared cache store (CAS) |
| `cache/yt-dlp/` | `{runtime_root}/cache/yt-dlp/` | yt-dlp cache |
| `cache/mediapm/` | `{runtime_root}/cache/mediapm/` | MediaPM metadata cache |

## `MediaPmPathOverrides` resolution rules

Override fields come from `MediaRuntimeStorage` in `mediapm.ncl`:

- `mediapm_dir`: relative paths resolve against the `mediapm.ncl` parent directory; absolute paths used as-is.
- `hierarchy_root_dir`: resolves relative to `mediapm.ncl` parent.
- Conductor config paths (`conductor_config`, `conductor_generated_config`, `conductor_state_config`, `conductor_schema_dir`): resolve relative to `mediapm.ncl` parent.
- `mediapm_schema_dir`: `None` → use computed default; `Some(None)` → disable export; `Some(Some(path))` → resolve relative to `mediapm.ncl` parent.

## Cache subdirectory layout

```text
{runtime_root}/cache/
  store/            # Shared CAS store (tool payloads)
  yt-dlp/           # yt-dlp download cache
  mediapm/           # MediaPM metadata cache
```

## Key invariants

- `tools_dir` always lives under `runtime_root`, never under workspace root directly.
- `conductor_tmp_dir` and `mediapm_tmp_dir` use OS temp dir with a workspace-hashded name, not `runtime_root`.
- Schema export is optional — `schema_export_dir: None` disables it.
