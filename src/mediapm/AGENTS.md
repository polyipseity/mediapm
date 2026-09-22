# `mediapm` Crate Guidance

Media orchestration facade over `mediapm-cas` and `mediapm-conductor`: media library sync, managed-tool provisioning, and hierarchy materialization.

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
example_isolation.rs       — Example/test temp dirs, MEDIAPM_EXAMPLE_* env, cleanup (see example-temp-isolation.instructions.md)
  mod.rs                   —   MediaPmDocument, MediaPmState, MediaRuntimeStorage, re-exports
  defaults.rs              —   Constants (version=2, slot limits, cache TTLs, materialization order)
  custom_deserializers.rs  —   Serde helpers (f64→u64, option strings)
  source_types.rs          —   MediaSourceSpec, MediaStep, MediaStepTool
  hierarchy_types.rs       —   HierarchyNode (ordered array), flattening, playlist, SanitizeNamesConfig
  output_types.rs          —   OutputVariantValue (YtDlp | Generic), OutputCaptureKind, OutputSaveConfig
  nickel_io.rs             —   .ncl eval, load/save/merge documents
  versions/                —   Schema version dispatch (mod.rs + v1.rs + v2.rs + .ncl)
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
| --- | --- | --- |
| `MediaPmService<Cas>` | `service.rs` | Orchestration API: sync, add/remove media, tool lifecycle |
| `MediaPmDocument` | `config/mod.rs` | Deserialized `mediapm.ncl` |
| `MediaPmState` | `config/mod.rs` | Machine-managed `state.json` |
| `MediaRuntimeStorage` | `config/mod.rs` | Runtime path overrides (config, CLI, env) |
| `MediaPmPaths` | `paths.rs` | Resolved canonical path bundle for one workspace root |
| `MediaPmGlobalPaths` | `global.rs` | User-level cache (`<os-cache>/mediapm/cache/`) |
| `MediaPmError` | `error.rs` | Error taxonomy (6 variants) |
| `HierarchyNode` | `config/hierarchy_types.rs` | Ordered node-array hierarchy with recursive children |

## Config Document Model

Four-document system, all with explicit top-level `version`:

| Document | Default path | Owner | Purpose |
| --- | --- | --- | --- |
| `mediapm.ncl` | `<root>/mediapm.ncl` | User | Declares media, hierarchy, tools, runtime |
| `<mediapm_dir>/conductor.ncl` | Generated | Conductor user intent + workflow defs | |
| `<mediapm_dir>/conductor.generated.ncl` | Machine | Tool registry, resolved hashes | |
| `<mediapm_dir>/state.json` | Machine | Per-media workflow state, managed files, hashes | |

Config schema versioning (see `.agents/instructions/nickel.instructions.md` for the migration placement and strict version separation policies):

- v2 is the active config version (`CURRENT_VERSION = 2`). Its envelope drops `MediaPmState` entirely; state is managed separately via `state.json` and is not part of the v2 config surface.
- v1 remains readable as legacy: its envelope keeps the optional `state` field, so stateful v1 documents still load and migrate to v2 (which strips `state`).
- Each version file owns the migration INTO that version (`v2.ncl` exports `migrate_v1_to_v2`, `v1.ncl` exports `migrate_v2_to_v1`); `mod.ncl` only dispatches via `migrate_to`. Version files are self-contained and never mix `*V1`/`*V2` contract names.

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

MediaPM creates `Conductor`, passes grouped runtime-storage paths so volatile writes go to `<mediapm_dir>/`:

| Conductor path | MediaPM default |
| --- | --- |
| `conductor_dir` | `<mediapm_dir>` |
| `conductor_state_config` | `<mediapm_dir>/state.conductor.ncl` |
| `cas_store_dir` | `<mediapm_dir>/store` |
| `conductor_tmp_dir` | `<mediapm_dir>/tmp` |
| `conductor_schema_dir` | `<mediapm_dir>/config/conductor` |
| `conductor_tools_dir` | `<mediapm_dir>/tools` |

Do not add direct deps from `mediapm` to `mediapm-conductor-builtins/*` crates.

## Managed Tool Provisioning

6 managed tools: `yt-dlp`, `ffmpeg`, `deno`, `rsgain`, `media-tagger`, `sd`. See `.agents/instructions/provider-dispatch.instructions.md` (per-OS source descriptors, URL resolution, `resolve_latest_github_tag`), `.agents/instructions/preset-dispatch.instructions.md` (preset spec builders, defaults), and `.agents/instructions/tool-sync-3-phase-provisioning.instructions.md` (provisioning pipeline, paths, `FetchedToolPayload` shape) for details.

**User-level cache**: `<os-cache-dir>/mediapm/cache/` (7-day eviction) — shared download cache distinct from workspace tool cache.

**Companion dep path contract**: Inlined companion dep content map keys follow `deps/{mediapm_tool_id}/{os}/{filename}` (e.g. `deps/ffmpeg/macos/ffmpeg`). The OS directory component is already embedded in the key structure. OS-conditional selectors for these paths MUST NOT use `build_os_conditional_selector` (which prepends `{os}/`) — use `build_raw_os_conditional_selector` instead. Using the wrong helper produces double-prefixed paths like `macos/deps/ffmpeg/macos/ffmpeg` that break runtime path resolution. See `mediapm-conductor::tools::helpers` module docs for the two selector flavors.

### Media metadata caching

Two caches are enabled by default, both workspace-scoped under `<runtime>/cache/` in separate subdirectories:

- **ffprobe cache** (`metadata_cache.rs`): `MediaPmService` owns a `MetadataCache` at `paths.workspace_mediapm_cache_dir()` (`<runtime>/cache/mediapm/metadata.cache.json`). Each `add_local_source_*` call consults it keyed by `ffprobe:{path}` (TTL = 1 day, `METADATA_CACHE_ENTRY_TTL_SECONDS`); the probe runs only on a miss. Expired entries are pruned at open and flushed atomically on drop.
- **media-tagger HTTP cache**: the media-tagger step synthesizes a `cache_dir` input defaulting to `paths.workspace_media_tagger_cache_dir()` (`<runtime>/cache/media_tagger`), threaded through `synthesize_media_tagger_step` → `build_media_tagger_metadata_inputs` as the `cache_dir` step option (overridable via `options.cache_dir`). `MediaTaggerHttpCache` honors a non-empty `cache_dir`; empty disables caching. `cache_expiry_seconds` defaults to `86400`.

### Global tool cache CLI

`mediapm global tool-cache` inspects and prunes the user-level tool cache (`<os-cache-dir>/mediapm/cache/`), distinct from the workspace metadata caches. Implemented in `src/mediapm/src/global.rs`, it reuses the `mediapm-conductor` `Cache` engine directly (no background loop).

- **`status`** — Opens the cache without the 24-hour prune loop (`Cache::open_without_background`) and reports the summed `entry_count` across the `tools` (7-day TTL) and `tool_metadata` (1-day TTL) domains, plus `tool_cache_dir`, `store_dir`, and `index` paths.
- **`prune`** — Opens without the loop, then runs `Cache::prune_expired_immediate`, bypassing the `PRUNE_INTERVAL_SECONDS` cooldown across both domains, reporting `removed_entries` and `removed_payloads`. Unreferenced CAS payload blobs are physically deleted; still-referenced entries are retained.

Both accept `--cache-root <dir>` (resolved via `MediaPmGlobalPaths::from_tool_cache_dir`); omitted uses the default user-level root.

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

### Canonical version tracking

Every `ToolRegistryEntry` has a non-optional `canonical_version` (`String`). The semantic kind (VCS hash vs version) is fixed per tool at code-writing time:

- **Builtin tools** (media-tagger): `MEDIAPM_GIT_HASH` (compile-time constant from `build.rs`).
- **GitHub-release tools** (yt-dlp, ffmpeg, deno, rsgain, sd): the resolved tag name verbatim.

Skip logic: when `reconcile_desired_tools` finds the same `canonical_version` in `state.managed_tools` with a non-empty `content_map_hash`, provisioning is skipped for that tool. The `Ok(None)` branch still populates `canonical_version` from the resolved value. `canonical_version` defaults to `""` via `#[serde(default)]` for backward-compat with old state files.

## Cache Architecture (Three-Tier)

See `.agents/instructions/cache-and-http.instructions.md` for the three-tier cache specification (content, metadata, provision), TTL policies, and hard boundary rules.

## Materialization

Direct CAS→output-path writes; no staging commit. Materialized paths marked read-only after sync. Link fallback order configurable in `runtime.materialization_preference_order` (default: hardlink → symlink → reflink → copy). NFD filenames enforced; reserved path chars rejected. ZIP extraction under `<mediapm_dir>/tmp/`.

The rendered materialization progress screen (`[mat]`/`[stg]`/`[vrf]`/`[cmt]` phases) is documented in `.agents/instructions/progress-output.instructions.md` (Screen C).

## CAS Integrity Verification

Configurable per `VerifyTriggerStrategy`: `Always`, `Modified` (default), `Sample { denominator: 100 }` (default), `Stale { timeout: 604800s }` (default). Gated by `MediaRuntimeStorage.verify_on_read` (typed `Vec<VerifyStrategy>`, snake_case wire names `always`/`modified`/`sample`/`stale`; unknown names rejected at the serde boundary) plus `verify_on_read_sample_denominator` and `verify_on_read_stale_timeout_secs` fields.

## Cross-Crate Invariants

Cross-crate data flow, shared invariants, and the full module-layer index are in `mediapm-architecture.instructions.md`. Key mediapm-specific rules:

- **MediaPM → Conductor**: MediaPM owns media defs, hierarchy, tool provisioning. Conductor owns step execution, state persistence, and runtime env files. MediaPM delegates gitignore creation to `extend_runtime_gitignore()` at construction and dotenv writing to `write_generated_dotenv()` during tool sync.
- **MediaPM → CAS**: Materialization reads from CAS; all outputs read-only after commit. Hash mismatch means no fallback.
- **Lock→CAS referential integrity**: Prune must not remove hashes referenced by lock records.

See `versioning-and-migration.instructions.md` for the config versioning pattern and `nickel.instructions.md` for the NCL-to-Rust sync contract (S1-S13).

## Testing & Validation

Development: `cargo test-pkg mediapm` / `cargo build-pkg mediapm` / selective tests.

**Temp isolation:** Examples-as-tests use `mediapm::example_isolation::IsolatedExampleRoots` and `MEDIAPM_EXAMPLE_*` env overrides; integration tests use `mediapm_utils::temp::artifact_dir()` plus `cache_dir()` for `cache_root_override`. See `.agents/instructions/example-temp-isolation.instructions.md` for role prefixes, cleanup lifecycle, and example env serialization (process-wide lock held by `IsolatedExampleRoots` / `lock_process_env()`).

**Demo hierarchy golden:** Online link files use two exact naming formats — yt-dlp shape under `sidecars/links/` vs mediapm root projections — encoded in golden helpers and live asserts. Content verification covers exact bytes in the e2e test and resilient structural checks (magic bytes, link format, ffprobe metadata) in the live demo. See `.agents/instructions/demo-hierarchy-golden.instructions.md`.

Post-change: demo examples:

- `cargo run --package mediapm --example mediapm_demo`
- `cargo run --package mediapm --example mediapm_demo_online` — full-sync online path (network + external tools), human-gated
- Cache-using examples (`mediapm_demo`, `mediapm_demo_online`, `mediapm_cli_add_tools`, `mediapm_cli_add_hierarchy`) reuse the real user-level tool download cache (`<os-cache-dir>/mediapm/cache`, via `example_isolation::user_level_cache_root()`) at explicit `cargo run --example`, so downloaded tools persist across runs; embedded tests stay isolated via `MEDIAPM_EXAMPLE_CACHE_ROOT`.

The online demo's embedded test (`main_is_exercised`) runs reduced (config-only) mode deterministically in the test harness (skips in CI), so the pre-push gate exercises only reduced mode — no network. Its full-sync path runs only on the explicit `cargo run --package mediapm --example mediapm_demo_online` above.

Full workspace: `cargo fmt-check && cargo clippy-all && cargo test-all`.

## Feature Flags

| Feature | Deps | Purpose |
| --- | --- | --- |
| `default` = `cli` + `media-tagger` | — | Production set |
| `cli` | clap, clap_complete | CLI binary |
| `media-tagger` | chromaprint-next, musicbrainz_rs, ffmetadata | Native tagger builtin |

## Reference Files

- `Cargo.toml` — Dependencies and features
- `config/mod.rs` — Config types and defaults
- `paths.rs` — Path resolution
- `service.rs` — MediaPmService orchestration
- `error.rs` — Error taxonomy
- `.agents/instructions/*.instructions.md` — Focused guidance by concern (error taxonomy, paths, cache, provider dispatch, preset dispatch, state persistence, document lifecycle, tool sync coordinator, 3-phase provisioning, content-addressed identity, companion dependencies)
