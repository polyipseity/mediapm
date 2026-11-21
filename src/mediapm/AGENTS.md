# `mediapm` crate

Media orchestration facade over `mediapm-cas` and `mediapm-conductor`: library sync, managed-tool provisioning, and hierarchy materialization. Root `AGENTS.md` and `.agents/instructions/*.instructions.md` are the workspace baseline.

## Module architecture

| Module | Role |
| --- | --- |
| `config/` | `mediapm.ncl` model, defaults, Nickel I/O, versioned schema, validation. |
| `state/` | `state.json` serialization/version dispatch. |
| `service.rs` | `MediaPmService` orchestration and standalone helpers. |
| `conductor_bridge/` | Conductor integration, tool reconciliation, generated doc/state writes. |
| `tools/` | Managed tool preset/provider dispatch and workflow synthesis. |
| `builtins/media_tagger/` | Host builtin: AcoustID, cover art, ffmetadata, MusicBrainz. |
| `materializer/` | CAS to filesystem writes, playlists, ZIP extraction, progress labels. |
| `paths.rs`, `global.rs` | Workspace path bundle and user-level cache paths. |
| `sync_report.rs`, `output/` | Sync observer/report types, CLI formatting/progress. |
| `metadata_cache.rs`, `source_metadata.rs` | Workspace metadata cache and local ffprobe probes. |
| `error.rs` | `MediaPmError` taxonomy. |

Pointers: code under `src/mediapm/src/`, `.agents/instructions/mediapm-architecture.instructions.md`.

## Key types

| Type | Purpose |
| --- | --- |
| `MediaPmService<Cas>` (`service.rs`) | Orchestration API: sync, add/remove media, tool lifecycle. |
| `MediaPmPaths` (`paths.rs`) | Resolved canonical path bundle for one workspace root. |
| `SyncRequest`/`SyncReport` (`sync_report.rs`) | Sync inputs/options and per-phase outcome. |
| `MaterializeReport` (`materializer/mod.rs`) | Materialization counts, notices, warnings. |

## Config document model

| Document | Default path | Owner | Purpose |
| --- | --- | --- | --- |
| `mediapm.ncl` | `<root>/mediapm.ncl` | User | Media, hierarchy, tools, runtime. |
| `mediapm.conductor.ncl` | `<root>/mediapm.conductor.ncl` | User | Conductor intent/workflows; never a reconcile save target. |
| `mediapm.conductor.generated.ncl` | `<root>/mediapm.conductor.generated.ncl` | Machine | Generated tool registry/resolved hashes. |
| `state.json` | `<runtime_root>/state.json` | Machine | Managed files and tool deployment metadata. |

All carry explicit top-level `version`. Active config schema is v4; v1-v3 readable through Nickel migrations in `config/versions/`. Output variant `save` defaults to `"auto"` (`config/output_types.rs`). See `.agents/instructions/state-and-versioning.instructions.md`.

## CLI overview

`mediapm [global flags] <subcommand>` with `sync`, `tool`, `media`, `hierarchy`, `cas`, `conductor`, `global`, `completions`. Flag resolution: CLI arg > `MEDIAPM_*` env > config > default. Definitions in `mediapm-cli`; dispatch in `main.rs`.

## Conductor integration

- MediaPM composes CAS+Conductor; no direct deps from `mediapm` to `mediapm-conductor-builtins/*`.
- Runtime storage paths keep volatile writes under `<mediapm_dir>`; mediapm exports Nickel contract sources (not JSON schema).
- Conductor bridge owns generated doc/state writes. `mediapm.conductor.ncl` is user intent; `mediapm.conductor.generated.ncl` is machine-managed; conflicts fail fast.
- Explicit invalidation: `media invalidate <media> <step-index>` marks recorded instances; `--undo` clears markers; `sync --force-rerun` forces current-run rerun only. Markers select workflow+step id; shared instances can affect same-work media. See `conductor_bridge/`, `mediapm-conductor/AGENTS.md`.

## Output retention and rerun policy

- Variant `save` default `"auto"`: resolves to full for hierarchy-materialized, final-step, or impure-step outputs; otherwise false. Materialized wins.
- Materialized variant declared `false` is refused at plan build as `MPM-E010` (media, step, variant, hierarchy entry).
- `full` marks CAS keep-full; missing resolved bytes are fatal. Retention mode does not change capture/production.
- Tool upgrades do not change step identity; use explicit invalidation. See `.agents/instructions/mediapm-architecture.instructions.md`.

## Managed tool provisioning

- Managed tools: `yt-dlp`, `ffmpeg`, `deno`, `rsgain`, `sd` (preset/workflow), `media-tagger` (host builtin, no payload), `import` (builtin provider, no preset).
- Provisioning is content-addressed and all-platform; sync is fail-fast and writes nothing on provision failure.
- Conductor tool id and mediapm tool id are distinct; cache/provision keys use conductor tool ids. `canonical_version` includes direct SameStep dependency segments.
- User-level download cache: `<os-cache-dir>/mediapm/cache/`; workspace tool cache: `<mediapm_dir>/tools/`. Tests/examples isolate caches.
- Companion dep keys `deps/{mediapm_tool_id}/{os}/{filename}` use `build_raw_os_conditional_selector`, not `build_os_conditional_selector`. See `.agents/instructions/tool-provisioning.instructions.md`.

## Host builtin: media-tagger

Registered every run via `RunWorkflowOptions.host_builtins`; tagging dispatches in-process (`MediaTaggerBuiltin`), not as a child command. Tool entry is bare `media-tagger`, empty content map, builtin kind; prune retains it via `HOST_BUILTIN_IDS`. Retired command options (`option_args`, `leading_args`, `trailing_args`, `enable_acoustid`) are refused explicitly. Removing command parts changed tagging instance keys once, so recorded tagging instances re-execute on first sync after that change. Network/cache behavior is bounded and injectable; see `builtins/media_tagger/{mod,musicbrainz,cover_art}.rs`.

## Cache, metadata, materialization, CAS integrity

- Workspace caches under `<runtime>/cache/`: ffprobe metadata (`mediapm/metadata.json`, 1-day TTL) and media-tagger HTTP cache (`media_tagger/`). Global tool cache: `mediapm global tool-cache status|prune`.
- Materialization writes directly from CAS to target, no staging commit. Skip compares recorded/resolved content; `zip_member` compares member hash/length. Read-only targets removed before replacement. Link fallback order configurable (`hardlink, symlink, reflink, copy`); fallback is a notice.
- NFD enforced: config rejects non-NFD user components; resolved/interpolated components normalized before workers start. Reserved chars/traversal rejected.
- CAS verify is configurable by `VerifyTriggerStrategy` (`Always`, `Modified`, `Sample`, `Stale`) and runtime verify settings. Hash mismatch has no fallback.

## Testing and validation

- Fast loop: `cargo test-pkg mediapm`, `cargo build-pkg mediapm`, or selective tests. Full/parity gates: `rust-workflow.instructions.md`.
- Hermetic tests/examples: `IsolatedExampleRoots`, `mediapm_utils::temp::artifact_dir()/cache_dir()`, cache overrides; never real OS user cache in tests.
- Demo checks: `mediapm_demo` and `mediapm_demo_online` (default offline/hermetic, `--online` real/manual). Demo behavior changes require updating demos and embedded tests.
- Features: default `cli,media-tagger`; `cli` adds clap/completions; `media-tagger` enables native tagger deps; `proptest` enables property tests.

## References

- Code: `Cargo.toml`, `paths.rs`, `service.rs`, `config/mod.rs`, `error.rs`, `conductor_bridge/sync/mod.rs`, `tools/provider/mod.rs`, `materializer/mod.rs`
- Instructions: `mediapm-architecture.instructions.md`, `tool-provisioning.instructions.md`, `state-and-versioning.instructions.md`, `temp-and-paths.instructions.md`, `examples-and-demos.instructions.md`
