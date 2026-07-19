# `mediapm` Crate Guidance

Media orchestration facade composing `mediapm-cas` and `mediapm-conductor` into a unified library and CLI for media library sync, managed-tool provisioning, and hierarchy materialization.

## Module Architecture

```text
lib.rs                     — Re-exports, SyncSummary, ToolsSyncSummary, global cache ops
main.rs                    — CLI dispatch (clap): sync, tool, media, hierarchy, cas, conductor
error.rs                   — MediaPmError: 6 variants (see error-taxonomy.instructions.md)
output.rs                  — CLI formatting (print_sync_summary)
util.rs                    — first_non_empty_json_string helper
http_client.rs             — Shared reqwest::Client (OnceLock)
global.rs                  — MediaPmGlobalPaths, MEDIAPM_USER_AGENT
paths.rs                   — MediaPmPaths (17 fields), MediaPmPathOverrides
source_metadata.rs         — Online/local metadata probes
hierarchy.rs               — Hierarchy node mutation
metadata_cache.rs          — JSON metadata cache, TTL-based expiry
service.rs                 — MediaPmService<Cas> orchestration
service_standalone.rs      — Standalone helpers (document loading, path resolution)
test_util.rs               — Shared test Tokio runtime

config/                    — NCL document model, serde types, version dispatch
  mod.rs                   —   MediaPmDocument, MediaPmState, MediaRuntimeStorage, re-exports
  defaults.rs              —   Constants (version=1, slot limits, cache TTLs, materialization order)
  custom_deserializers.rs  —   Serde helpers (f64→u64, option strings)
  source_types.rs          —   MediaSourceSpec, MediaStep, MediaStepTool
  hierarchy_types.rs       —   HierarchyNode (ordered array), flattening, playlist, SanitizeNamesConfig
  output_types.rs          —   OutputVariantConfig, OutputCaptureKind, OutputSaveConfig
  nickel_io.rs             —   .ncl eval, load/save/merge documents
  versions/                —   Schema version dispatch (mod.rs + v1.rs + .ncl)
  validation/              —   Cross-field validation (mod.rs, hierarchy.rs, sources.rs)

conductor_bridge/          — Conductor integration
  mod.rs                   —   ToolSyncReport, reconcile_desired_tools
  constants.rs             —   Input/output key constants, tool IDs, slot limits
  documents.rs             —   Load/save conductor NCL documents
  runtime_storage.rs       —   RuntimeStoragePaths resolution
  util.rs                  —   Shared helpers
  sync/                    —   Tool reconciliation (provision, tool_config, content_import, lifecycle)
  tool_runtime/            —   ToolSpec/ToolRuntime builders, option tokens, template, launcher

materializer/              — CAS→filesystem materialization
  mod.rs                   —   sync_hierarchy(), MaterializeReport
  commit.rs                —   Read-only flag, remove stale paths
  file_ops.rs              —   Hardlink/symlink/reflink/copy
  metadata.rs              —   Template/metadata resolution
  resolve.rs               —   Source/variant hash resolution, existence checks
  playlist.rs              —   Playlist generation (M3U8, PLS, XSPF, WPL, ASX)
  zip.rs                   —   ZIP folder extraction

tools/                     — Managed tool preset/provider + workflow synthesis
  mod.rs                   —   Module router, is_known_tool_id()
  downloader.rs            —   ToolDownloadCache type alias (re-exports from conductor)
  preset/                  —   ToolSpec/ToolRuntime builders: deno, ffmpeg, media_tagger, rsgain, sd, yt_dlp
  provider/                —   Source descriptors (URLs per OS): deno, ffmpeg, media_tagger, rsgain, sd, yt_dlp
  workflows/               —   Step synthesis: ffmpeg, media_tagger, rsgain, yt_dlp, yt_dlp_inputs

builtins/                  — Native tool implementations
  mod.rs                   —   Builtin discovery
  media_tagger/            —   Native tagger: acoustid, cover_art, ffmetadata, musicbrainz, util
```

## Key Types

| Type | File | Purpose |
|------|------|---------|
| `MediaPmService<Cas>` | `service.rs` | Orchestration API: sync, add/remove media, tool lifecycle |
| `MediaPmDocument` | `config/mod.rs` | Deserialized `mediapm.ncl` |
| `MediaPmState` | `config/mod.rs` | Machine-managed `state.ncl` |
| `MediaRuntimeStorage` | `config/mod.rs` | Runtime path overrides (config, CLI, env) |
| `MediaPmPaths` | `paths.rs` | Resolved canonical path bundle for one workspace root |
| `MediaPmGlobalPaths` | `global.rs` | User-level cache (`<os-cache>/mediapm/cache/`) |
| `MediaPmError` | `error.rs` | Error taxonomy (5 variants) |
| `HierarchyNode` | `config/hierarchy_types.rs` | Ordered node-array hierarchy with recursive children |

## Config Document Model

Four-document system, all with explicit top-level `version`:

| Document | Default path | Owner | Purpose |
|----------|-------------|-------|---------|
| `mediapm.ncl` | `<root>/mediapm.ncl` | User | Declares media, hierarchy, tools, runtime |
| `<mediapm_dir>/conductor.ncl` | Generated | Conductor user intent + workflow defs |  |
| `<mediapm_dir>/conductor.generated.ncl` | Machine | Tool registry, resolved hashes |  |
| `<mediapm_dir>/state.ncl` | Machine | Per-media workflow state, managed files, hashes |  |

## CLI Overview

```text
mediapm [--root PATH] [--mediapm-dir PATH] [--conductor-config PATH]
        [--conductor-generated-config PATH] [--conductor-state-config PATH]
        [--media-state-config PATH] [--env-file PATH] [--retry-impure]
        <subcommand>

Subcommands:
  sync                           Reconcile library state
  tool (add|remove|list|sync|run|prune|refresh-runtime)
  media (add|add-local|remove|list|invalidate)
  hierarchy (add|remove)
  cas [args...]                  Passthrough to mediapm-cas CLI
  conductor [args...]            Passthrough to mediapm-conductor CLI
  global (path|init|tool-cache)
  export-schemas
  completions <shell>
```

Flag resolution: CLI arg > env var (`MEDIAPM_*`) > config > default.

## Conductor Integration

MediaPM creates `SimpleConductor`, passes grouped runtime-storage paths so volatile writes go to `<mediapm_dir>/`:

| Conductor path | MediaPM default |
|---------------|-----------------|
| `conductor_dir` | `<mediapm_dir>` |
| `conductor_state_config` | `<mediapm_dir>/state.conductor.ncl` |
| `cas_store_dir` | `<mediapm_dir>/store` |
| `conductor_tmp_dir` | `<mediapm_dir>/tmp` |
| `conductor_schema_dir` | `<mediapm_dir>/config/conductor` |
| `conductor_tools_dir` | `<mediapm_dir>/tools` |

Do not add direct deps from `mediapm` to `mediapm-conductor-builtins/*` crates.

## Managed Tool Provisioning

6 managed tools: `yt-dlp`, `ffmpeg`, `deno`, `rsgain`, `media-tagger`, `sd`. See `.agents/instructions/provider-dispatch.instructions.md` for per-OS source descriptors and URL resolution. See `.agents/instructions/preset-dispatch.instructions.md` for preset spec builders and tool defaults. See `.agents/instructions/tool-sync-3-phase-provisioning.instructions.md` for the provisioning pipeline and paths.

**Provisioning paths**: see `.agents/instructions/tool-sync-3-phase-provisioning.instructions.md` for the provisioning pipeline, paths, and `FetchedToolPayload` shape (`content_map`, `os_exec_paths`).

**URL resolution**: See `.agents/instructions/provider-dispatch.instructions.md` for URL resolution (`resolve_latest_github_tag`), per-tool URL templating, and metadata cache usage.

**Tool defaults**: see `.agents/instructions/preset-dispatch.instructions.md` for per-tool spec builders and default settings.

**User-level cache**: `<os-cache-dir>/mediapm/cache/` (30-day eviction) — shared download cache distinct from workspace tool cache.

### Adding a New Managed Tool

Follow this spec-first, test-first workflow:

1. **Spec first** — Document the tool's contract in `src/mediapm/AGENTS.md`:
   - Source URL scheme and supported OSes
   - Companion dependencies (if any)
   - Input/output contracts for the workflow step
   - Any special runtime requirements (sandbox, env vars)

2. **Test first** — Write tests before implementation:
   - Unit tests for the tool's source descriptors (URLs per OS) in `tools/provider/<tool>.rs`
   - Unit tests for the preset builder in `tools/preset/<tool>.rs`
   - Unit tests for the workflow synthesis in `tools/workflows/<tool>.rs`
   - Integration test case in `tests/int/all_platform.rs`

3. **Implement provider** — Create `tools/provider/<tool>.rs`:
   - Define per-OS `SourceProducer::Fetch` entries with download URLs
   - Register in `tools/provider/mod.rs` dispatch table

4. **Implement preset** — Create `tools/preset/<tool>.rs`:
   - Define `pub(crate) fn apply(...) -> (ToolSpec, ToolRuntime)`
   - Set correct `impure`, `content_map`, `command_selector`, and `slot_limits`
   - Register in `tools/preset/mod.rs` dispatch table

5. **Implement workflow** — Create (or extend) `tools/workflows/<tool>.rs`:
   - Define `build_<tool>_command()`, `build_<tool>_inputs()`, `build_<tool>_outputs()`, `build_<tool>_defaults()`
   - Define `build_<tool>_spec()` composing the above into a full `ToolSpec`
   - Add `step_<tool>()` synthesizer and register in the step dispatch

6. **Register everywhere**:
   - Add to `is_known_tool_id()` in `tools/mod.rs`
   - Add to `tools/mod.rs` module declarations (`pub(crate) mod <tool>;`)
   - Add config defaults in `config/defaults.rs` if needed
   - Add CLI test cases in `main.rs` tests (route parsing)

7. **Integration test** — Verify end-to-end:
   - Provider resolves the correct URLs per OS
   - Preset produces valid `ToolSpec` with non-empty command/inputs/outputs
   - Workflow step synthesizes correct command-line tokens

## Cache Architecture (Three-Tier)

See `.agents/instructions/cache-and-http.instructions.md` for the three-tier cache specification (content, metadata, provision), TTL policies, and hard boundary rules.

## Materialization

Direct CAS→output-path writes; no staging commit. Materialized paths marked read-only after sync. Link fallback order configurable in `runtime.materialization_preference_order` (default: hardlink → symlink → reflink → copy). NFD filenames enforced; reserved path chars rejected. ZIP extraction under `<mediapm_dir>/tmp/`.

## Metadata Cache

`metadata_cache.rs` — single JSON file at `<runtime_root>/cache/mediapm/`. BLAKE3-hex keys, 86400s TTL, timer-based batch flush (~300s cooldown). Graceful degradation on I/O/serialization errors.

## CAS Integrity Verification

Configurable per `VerifyTriggerStrategy`: `Always`, `Modified` (default), `Sample { denominator: 100 }` (default), `Stale { timeout: 604800s }` (default). Gated by `MediaRuntimeStorage.verify_on_read_*` fields.

## Cross-Crate Invariants

- **Content identity**: BLAKE3-256 multihash; `Hash::composite(&[Hash])` for deterministic composite hashing.
- **MediaPM → Conductor**: MediaPM owns media defs, hierarchy, tool provisioning. Conductor owns step execution, state persistence.
- **MediaPM → CAS**: Materialization reads from CAS; all outputs read-only after commit. Hash mismatch → no fallback.
- **NCL→Rust sync**: Typed envelope pattern — `deny_unknown_fields` on envelope, `#[serde(flatten)]` inner. Custom deserializers for Nickel f64→u64. All user-facing config fields must be non-Option in domain types; absent config keys are resolved to explicit defaults at the serde boundary, so downstream code never handles `Option`.
- **Lock→CAS referential integrity**: Prune must not remove hashes referenced by lock records.

## Testing & Validation

Development: `cargo test-pkg mediapm` / `cargo build-pkg mediapm` / selective tests.

Post-change: both demo examples mandatory:

- `cargo run --package mediapm --example mediapm_demo`
- `cargo run --package mediapm --example mediapm_demo_online`

Full workspace: `cargo fmt-check && cargo clippy-all && cargo test-all`.

## Feature Flags

| Feature | Deps | Purpose |
|---------|------|---------|
| `default` = `cli` + `media-tagger` | — | Production set |
| `cli` | clap, clap_complete | CLI binary |
| `media-tagger` | chromaprint-next, musicbrainz_rs, ffmetadata | Native tagger builtin |

## Reference Files

- `Cargo.toml` — Dependencies and features
- `config/mod.rs` — Config types and defaults
- `paths.rs` — Path resolution
- `service.rs` — MediaPmService orchestration
- `error.rs` — Error taxonomy
- `.agents/instructions/*.instructions.md` — Focused guidance by concern (error taxonomy, paths, cache, provider dispatch, preset dispatch, state persistence, document lifecycle, tool sync coordinator, 3-phase provisioning, content-addressed identity, companion dependencies, generated env output)
