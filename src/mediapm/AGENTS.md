# `mediapm` crate

Facade over `mediapm-cas` + `mediapm-conductor`: sync, provisioning, materialization. Root `AGENTS.md` + `.agents/instructions/*.instructions.md` baseline.

## Module architecture

| Module | Role |
| --- | --- |
| `config/` | `mediapm.ncl`, defaults, Nickel I/O, versions, validation |
| `state/` | `state.json` version dispatch |
| `auth/` | `auth.json`, `mediapm auth` (`state-and-versioning.instructions.md`, `tool-provisioning.instructions.md`) |
| `service.rs` | `MediaPmService` orchestration |
| `conductor_bridge/` | Integration, reconciliation, generated writes |
| `tools/` | Preset/provider dispatch, workflows |
| `builtins/media_tagger/` | AcoustID, cover art, ffmetadata, MusicBrainz |
| `materializer/` | CAS->filesystem, playlists, ZIP, progress |
| `paths.rs`, `global.rs` | Paths, user cache |
| `sync_report.rs`, `output/` | Reports, CLI formatting/progress |
| `metadata_cache.rs`, `source_metadata.rs` | Metadata cache, ffprobe |
| `error.rs` | `MediaPmError` taxonomy |

Pointers `src/mediapm/src/`, `.agents/instructions/mediapm-architecture.instructions.md`.

## Key types

`MediaPmService<Cas>` (`service.rs`) sync/add-remove/tool lifecycle; `MediaPmPaths` (`paths.rs`) canonical paths; `SyncRequest`/`SyncReport` (`sync_report.rs`) options/outcome; `MaterializeReport` (`materializer/mod.rs`) counts/notices/warnings.

## Config documents

| Document | Default path | Owner | Purpose |
| --- | --- | --- | --- |
| `mediapm.ncl` | `<root>/mediapm.ncl` | User | Media/hierarchy/tools/runtime |
| `mediapm.conductor.ncl` | `<root>/mediapm.conductor.ncl` | User | Intent; no reconcile save |
| `mediapm.conductor.generated.ncl` | `<root>/mediapm.conductor.generated.ncl` | Machine | Registry/hashes |
| `state.json` | `<runtime_root>/state.json` | Machine | Files/tool deployments |

Top-level `version` on all; active schema v4, v1-v3 via Nickel migrations (`config/versions/`); `save` default `"auto"` (`config/output_types.rs`). See `state-and-versioning.instructions.md`.

## CLI overview

`mediapm [global flags] <subcommand>`: `sync`, `tool`, `media`, `hierarchy`, `cas`, `conductor`, `global`, `auth`, `completions`. Flags: CLI arg > `MEDIAPM_*` env > config > default; defs `mediapm-cli`, dispatch `main.rs`.

## Conductor integration

- Composes CAS+Conductor; no direct dep on `mediapm-conductor-builtins/*`; volatile writes under `<mediapm_dir>`; exports Nickel, not JSON schema.
- Bridge owns generated doc/state writes; `mediapm.conductor.ncl` user intent, generated machine-managed, conflicts fail fast.
- `media invalidate <media> <step-index>` marks recorded instances, `--undo` clears, `sync --force-rerun` current run only; markers select workflow+step id, shared instances affect same-work media (`conductor_bridge/`, `mediapm-conductor/AGENTS.md`).

## Output retention and rerun

- `save` `"auto"` (default) -> `"full"` hierarchy-materialized, `true` final/impure step, else `false`; materialized wins. Materialized `false` refused at plan build as `MPM-E010` (media, step, variant, hierarchy entry).
- `"full"` records empty object as only permitted base; missing bytes fatal exit-1; retention does not change production. Conductor `true` default only for docs mediapm did not synthesize. Tool upgrades do not change step identity; use explicit invalidation.

## Managed tool provisioning

- Tools `yt-dlp`, `ffmpeg`, `deno`, `rsgain`, `sd` (presets), `media-tagger` (host builtin, no payload), `import` (builtin provider, no preset).
- Content-addressed all-platform provisioning; sync fail-fast, writes nothing on failure. Conductor id distinct; cache keys use it; `canonical_version` includes direct SameStep segments.
- User cache `<os-cache-dir>/mediapm/cache/`, workspace `<mediapm_dir>/tools/`; tests isolate. Companion deps `deps/{mediapm_tool_id}/{os}/{filename}` use `build_raw_os_conditional_selector`, not `build_os_conditional_selector` (`tool-provisioning.instructions.md`).

## Host builtin: media-tagger

Registered each run via `RunWorkflowOptions.host_builtins`, in-process dispatch (`MediaTaggerBuiltin`). Bare `media-tagger`, empty content map, builtin kind; prune retains via `HOST_BUILTIN_IDS`. Retired `option_args`, `leading_args`, `trailing_args`, `enable_acoustid` refused. Removing command parts changed tagging instance keys once: recorded instances re-execute on first sync. Network/cache bounded, injectable (`builtins/media_tagger/{mod,musicbrainz,cover_art}.rs`).

## Cache, materialization, CAS integrity

- `<runtime>/cache/`: ffprobe `mediapm/metadata.json` (1-day TTL), tagger `media_tagger/`; global `mediapm global tool-cache status|prune`.
- Materialization CAS -> target directly, no staging commit; skip compares recorded/resolved bytes, `zip_member` hash/length; read-only targets removed first; fallback order `hardlink, symlink, reflink, copy` is a notice.
- NFD: config rejects non-NFD components, resolved/interpolated normalized before workers; reserved chars/traversal rejected. CAS verify `VerifyTriggerStrategy` (`Always`, `Modified`, `Sample`, `Stale`); hash mismatch no fallback.

## Testing and validation

Fast loop `cargo test-pkg mediapm`/`cargo build-pkg mediapm`; full gates `rust-workflow.instructions.md`. Hermetic `IsolatedExampleRoots`, `mediapm_utils::temp::output_dir()/cache_dir()`, overrides; no real OS cache. Demos `mediapm_demo`/`mediapm_demo_online` (offline default, `--online` manual), update with behavior. Features default `cli,media-tagger`; `cli` clap/completions, `media-tagger` native deps, `proptest` property tests.

## References

`Cargo.toml`, `paths.rs`, `service.rs`, `config/mod.rs`, `error.rs`, `conductor_bridge/sync/mod.rs`, `tools/provider/mod.rs`, `materializer/mod.rs`; `mediapm-architecture.instructions.md`, `.agents/instructions/tool-provisioning.instructions.md`, `.agents/instructions/state-and-versioning.instructions.md`, `temp-and-paths.instructions.md`, `examples-and-demos.instructions.md`.
