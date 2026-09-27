---
description: "Use when editing path resolution in src/mediapm/src/paths.rs. Covers MediaPmPaths fields, MediaPmPathOverrides, default layout under .mediapm/, and override resolution rules."
name: "Paths Layout"
applyTo: "src/mediapm/src/paths.rs"
---

# Paths layout

Centralizes filesystem path layout for all mediapm state, config, and cache directories, with consistent defaults and override resolution across CLI and library entry points.

## `MediaPmPaths` fields

The struct declares exactly 15 fields (`paths.rs:41-73`); the cache directories below are **method outputs**, not fields, and are listed separately.

| Field                     | Default                                      | Purpose                                                                                                                                   |
| ------------------------- | -------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| `root_dir`                | workspace root                               | Top-level workspace directory                                                                                                             |
| `runtime_root`            | `{root_dir}/.mediapm/`                       | Runtime-owned state root (`paths.rs:80`)                                                                                                 |
| `mediapm_ncl`             | `{root_dir}/mediapm.ncl`                     | User-edited policy config                                                                                                                 |
| `conductor_user_ncl`      | `{root_dir}/mediapm.conductor.ncl`           | Conductor user document                                                                                                                   |
| `conductor_generated_ncl` | `{root_dir}/mediapm.conductor.generated.ncl` | Conductor generated document                                                                                                              |
| `conductor_state_config`  | `{runtime_root}/state.conductor.json`        | Conductor volatile state (`paths.rs:87`, `paths.rs:175`; not `.ncl`)                                                                     |
| `conductor_tmp_dir`       | `$TMPDIR/mediapm-runtime-{16hex}/`           | Conductor sandbox tmp                                                                                                                     |
| `conductor_schema_dir`    | `{runtime_root}/config/conductor/`           | Conductor schema exports                                                                                                                  |
| `mediapm_state_json`       | `{runtime_root}/state.json`                   | MediaPM machine state (JSON always-write)                                                                                                |
| `env_file`                | `{runtime_root}/.env`                        | User-authored dotenv                                                                                                                      |
| `env_generated_file`      | `{runtime_root}/.env.generated`              | Machine-generated dotenv                                                                                                                  |
| `schema_export_dir`       | `Some({runtime_root}/config/mediapm/)`       | MediaPM schema exports (`None` = disabled)                                                                                                |
| `mediapm_tmp_dir`         | `$TMPDIR/mediapm-runtime-{16hex}/`           | MediaPM staging tmp                                                                                                                       |
| `hierarchy_root_dir`      | `{root_dir}`                                 | Materialized media library root                                                                                                           |
| `tools_dir`               | `{runtime_root}/tools/`                      | Tool-content unpack directory; subdirs are `<sanitize_tool_id(conductor_tool_id)>/payload/` and `.env.generated` paths mirror that layout |

## Cache directory methods

These are `MediaPmPaths` methods, not struct fields — each derives from `runtime_root` and none can be overridden.

| Method                                | Output                                 | Purpose                     | Source        |
| ------------------------------------- | -------------------------------------- | --------------------------- | ------------- |
| `workspace_cache_dir()`               | `{runtime_root}/cache/`                | Cache root                  | `paths.rs:110` |
| `workspace_cache_store_dir()`         | `{runtime_root}/cache/store/`          | Shared cache store (CAS)    | `paths.rs:116` |
| `workspace_yt_dlp_cache_dir()`        | `{runtime_root}/cache/yt-dlp/`         | yt-dlp cache                | `paths.rs:122` |
| `workspace_media_tagger_cache_dir()`  | `{runtime_root}/cache/media_tagger/`   | media-tagger cache          | `paths.rs:128` |
| `workspace_mediapm_cache_dir()`       | `{runtime_root}/cache/mediapm/`        | MediaPM metadata cache      | `paths.rs:134` |

## `MediaPmPathOverrides` resolution rules

Override fields come from `MediaRuntimeStorage` in `mediapm.ncl`:

- `mediapm_dir`: relative paths resolve against the `mediapm.ncl` parent; absolute used as-is.
- `hierarchy_root_dir`: resolves relative to `mediapm.ncl` parent.
- Conductor config paths (`conductor_config`, `conductor_generated_config`, `conductor_state_config`, `conductor_schema_dir`): resolve relative to `mediapm.ncl` parent.
- `mediapm_schema_dir`: `None` → computed default; `Some(None)` → disable export; `Some(Some(path))` → resolve relative to `mediapm.ncl` parent.

## Cache subdirectory layout

```text
{runtime_root}/cache/
  store/            # Shared CAS store (tool payloads)
  yt-dlp/           # yt-dlp download cache
  media_tagger/     # media-tagger cache
  mediapm/          # MediaPM metadata cache
```

## Key invariants

- `tools_dir` always lives under `runtime_root`, never under workspace root directly.
- The cache subdirectories are method outputs, so no `mediapm.ncl` override can relocate them.
- `conductor_tmp_dir` and `mediapm_tmp_dir` use OS temp dir with a workspace-hashed `mediapm-runtime-{16hex}` name, not `runtime_root` (see `temp-directory-spec.instructions.md`).
- Per-step conductor sandboxes live under `{conductor_tmp_dir}/sandbox/` and are removed after each `run_workflow` (see `temp-directory-spec.instructions.md`).
- Schema export is optional — `schema_export_dir: None` disables it.
