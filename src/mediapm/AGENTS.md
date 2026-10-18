# `mediapm` Crate Guidance

Media orchestration facade over `mediapm-cas` and `mediapm-conductor`: media library sync, managed-tool provisioning, and hierarchy materialization.

## Module Architecture

```text
lib.rs                     — Re-exports, SyncSummary, ToolsSyncSummary, global cache ops
main.rs                    — CLI dispatch (clap): sync, tool, media, hierarchy, cas, conductor
error.rs                   — MediaPmError: 7 variants (see error-taxonomy.instructions.md)
output/                    — CLI formatting + progress: mod.rs (print_sync_summary), observer.rs (CliSyncObserver), progress.rs (ProgressScreen re-exports)
util.rs                    — first_non_empty_json_string helper
global.rs                  — MediaPmGlobalPaths, MEDIAPM_GIT_HASH
paths.rs                   — MediaPmPaths (15 fields), MediaPmPathOverrides
source_metadata.rs         — Local-source ffprobe probes + conductor CAS root
hierarchy.rs               — Hierarchy node mutation
metadata_cache.rs          — JSON metadata cache, TTL-based expiry
service.rs                 — MediaPmService<Cas> orchestration
service_standalone.rs      — Standalone helpers (document loading, path resolution)
example_isolation.rs       — Example/test temp dirs, MEDIAPM_EXAMPLE_* env, cleanup (see example-temp-isolation.instructions.md)
config/                    — Document/config model: types, serde defaults, Nickel I/O, schema versions
  mod.rs                   —   MediaPmDocument, MediaPmState, MediaRuntimeStorage, re-exports
  defaults.rs              —   Constants (version=2, slot limits, cache TTLs, materialization order)
  custom_deserializers.rs  —   Serde helpers (f64→u64, option strings)
  source_types.rs          —   MediaSourceSpec, MediaStep, MediaStepTool
  hierarchy_types.rs       —   HierarchyNode (ordered array), flattening, playlist, SanitizeNamesConfig
  output_types.rs          —   OutputVariantValue (YtDlp | Generic), OutputCaptureKind, OutputSaveConfig
  nickel_io.rs             —   .ncl eval, load/save/merge documents
  versions/                —   Nickel schema boundary (mod.rs, mod.ncl, v_latest.rs, document.rs, v1.ncl, v2.ncl)
  validation/              —   Cross-field validation (mod.rs, hierarchy.rs, sources.rs)
state/                    — MediaPmState serialization + version dispatch
  mod.rs                   —   Public state persistence surface (ser, versions)
  ser.rs                   —   to_json_value/from_json_value, legacy .ncl migration
  versions/                —   State wire formats and dispatch (mod.rs + v1.rs + v2.rs + v3.rs)
sync_report.rs            — SyncObserver trait, per-phase reports, sync options
demo_hierarchy_spec.rs    — Demo hierarchy golden layout contract (see demo-hierarchy-golden.instructions.md)

conductor_bridge/          — Conductor integration
  mod.rs                   —   ToolSyncReport, reconcile_desired_tools
  constants.rs             —   Input/output key constants, tool IDs, slot limits
  documents.rs             —   Load/save conductor NCL documents
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
  progress_labels.rs       —   MaterializationBarLabel truncation order (see progress-output.instructions.md)
  zip_reader.rs            —   ZIP reading: member/folder extraction, binding parse

tools/                     — Managed tool preset/provider + workflow synthesis
  mod.rs                   —   Module router, is_known_tool_id()
  dependency.rs            —   DependencyTypes role flags, known_dependency_type() (see preset-dispatch.instructions.md)
  downloader.rs            —   ToolDownloadCache type alias (re-exports from conductor)
  preset/                  —   ToolSpec/ToolRuntime builders: deno, ffmpeg, media_tagger, rsgain, sd, yt_dlp
  provider/                —   Source descriptors (URLs per OS): deno, ffmpeg, import, media_tagger, rsgain, sd, yt_dlp
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
| `MediaPmError` | `error.rs` | Error taxonomy (7 variants) |
| `HierarchyNode` | `config/hierarchy_types.rs` | Ordered node-array hierarchy with recursive children |

## Config Document Model

Four-document system, all with explicit top-level `version`:

| Document | Default path | Owner | Purpose |
| --- | --- | --- | --- |
| `mediapm.ncl` | `<root>/mediapm.ncl` (`paths.mediapm_ncl`) | User | Declares media, hierarchy, tools, runtime |
| `mediapm.conductor.ncl` | `<root>/mediapm.conductor.ncl` (`paths.conductor_user_ncl`) | User | Conductor user intent + workflow defs; never a reconcile save target |
| `mediapm.conductor.generated.ncl` | `<root>/mediapm.conductor.generated.ncl` (`paths.conductor_generated_ncl`) | Machine | Tool registry, resolved hashes |
| `state.json` | `<runtime_root>/state.json` (`paths.mediapm_state_json`; `<runtime_root>` defaults to `<root>/.mediapm`) | Machine | Per-media workflow state, managed files, hashes |

The two conductor documents sit at the workspace root, not under `<mediapm_dir>`: `MediaPmPathOverrides.mediapm_dir` only moves `runtime_root` (`paths.rs`). Only the conductor-path overrides (`conductor_config`, `conductor_generated_config`, `conductor_state_config`, `conductor_schema_dir`) relocate those documents, each resolved relative to the `mediapm.ncl` parent directory.

Config schema versioning (see `.agents/instructions/nickel.instructions.md` for the migration placement and strict version separation policies):

- v2 is the active config version (`mod.ncl` `current_version = 2`). Its contract drops `MediaPmState` entirely; state is managed separately via `state.json` and is not part of the v2 config surface.
- v1 remains readable as legacy: its contract keeps the optional `state` field, so stateful v1 documents still load and migrate to v2 (which strips `state`).
- Each version file owns the migration INTO that version (`v2.ncl` exports `migrate_v1_to_v2`, `v1.ncl` exports `migrate_v2_to_v1`); `mod.ncl` only dispatches via `migrate_to`. Version files are self-contained and never mix `*V1`/`*V2` contract names.
- The ladder is Nickel only. `src/config/versions/` carries the embedded `.ncl` sources, the `*Latest` boundary types in `v_latest.rs`, the wire-shaped `MediaPmDocument` in `document.rs`, and the contract-evaluation helpers; it deliberately holds no Rust `decode`/`migrate_to` mirror of the ladder, because every `mediapm.ncl` on disk is read through `mod.ncl`.
- The boundary is crossed only by two unversioned entry points in `config/versions/mod.rs`: `resolve_runtime_storage` (read direction) and `runtime_boundary` (write direction, with `RuntimeBoundaryDeviations` for the knobs a document moves away from the spelled-out defaults). `MediaPmDocument` is re-exported from `config/mod.rs` — an unversioned name, so naming it is not a leak — and `config/mod.rs` names no `versions::v` path. The `*Latest` family is not publicly nameable, so bumping the active version is not a breaking API change.
- `no_file_outside_versions_references_a_version_module_path` in `src/mediapm-cas/src/delta/versions/mod_policy_guard.rs` enforces that rule across every workspace member, and `every_versions_dir_keeps_policy_guard_docstring` enforces the `DO NOT REMOVE` marker on every `versions/` file. Both are graded on the tree as it stands, so run them after touching a `versions/` boundary.

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
| `conductor_dir` | `<mediapm_dir>` (= `runtime_root`, default `{root_dir}/.mediapm`) |
| `conductor_state_config` | `<mediapm_dir>/state.conductor.json` (overrides the conductor default `state.json`; `paths.rs:87`, `paths.rs:175`) |
| `cas_store_dir` | `<mediapm_dir>/store` |
| `conductor_tmp_dir` | `<mediapm_dir>/tmp` |
| `conductor_schema_dir` | `<mediapm_dir>/schemas` (conductor default, `mediapm-conductor/src/defaults.rs:16`, joined in `mediapm-conductor/src/api.rs:55`) |
| `conductor_tools_dir` | `<mediapm_dir>/tools` |

Every row but `conductor_state_config` is the conductor's own default from `mediapm-conductor/src/defaults.rs`, because mediapm builds `RuntimeStoragePaths::new(&runtime_root).with_config_paths(...)` (`src/mediapm/src/service.rs:1059`, and `:1414` for the in-memory service) and never applies the conductor's `PathOverrides` — conductor code reads only `conductor_tmp_dir` off that struct (`mediapm-conductor/src/conductor_impl.rs:61`), and the conductor schema export mediapm performs writes to `MediaPmPaths.conductor_schema_dir` = `<mediapm_dir>/config/conductor/` (`src/mediapm/src/paths.rs:89`, read at `service.rs:783`, written by `export_mediapm_nickel_config_schemas` at `src/mediapm/src/lib.rs:573`), not to `<mediapm_dir>/schemas`.

Do not add direct deps from `mediapm` to `mediapm-conductor-builtins/*` crates.

## Managed Tool Provisioning

6 managed tools carry a preset and a workflow module: `yt-dlp`, `ffmpeg`, `deno`, `rsgain`, `media-tagger`, `sd`. A seventh, `import`, reaches `resolve_tool_fetch` (`tools/provider/import.rs`, a launcher-only builtin source-ingest tool with no preset and no workflow module, so it is absent from `is_known_tool_id` and from `apply_preset`). See `.agents/instructions/provider-dispatch.instructions.md` (per-OS source descriptors, URL resolution, `resolve_latest_github_tag`), `.agents/instructions/preset-dispatch.instructions.md` (preset spec builders, defaults), and `.agents/instructions/tool-sync-3-phase-provisioning.instructions.md` (provisioning pipeline, paths, `FetchedToolPayload` shape) for details.

**User-level cache**: `<os-cache-dir>/mediapm/cache/` (7-day eviction) — shared download cache distinct from workspace tool cache.

**Companion dep path contract**: Inlined companion dep content map keys follow `deps/{mediapm_tool_id}/{os}/{filename}` (e.g. `deps/ffmpeg/macos/ffmpeg`). The OS directory component is already embedded in the key structure. OS-conditional selectors for these paths MUST NOT use `build_os_conditional_selector` (which prepends `{os}/`) — use `build_raw_os_conditional_selector` instead. Using the wrong helper produces double-prefixed paths like `macos/deps/ffmpeg/macos/ffmpeg` that break runtime path resolution. See `mediapm-conductor::tools::helpers` module docs for the two selector flavors.

### Media metadata caching

Two caches are enabled by default, both workspace-scoped under `<runtime>/cache/` in separate subdirectories:

- **ffprobe cache** (`metadata_cache.rs`): `MediaPmService` owns a `MetadataCache` at `paths.workspace_mediapm_cache_dir()` (`<runtime>/cache/mediapm/metadata.cache.json`). Each `add_local_source_*` call consults it keyed by `ffprobe:{path}` (TTL = 1 day, `METADATA_CACHE_ENTRY_TTL_SECONDS`); the probe runs only on a miss. Expired entries are pruned at open and flushed atomically on drop.
- **media-tagger HTTP cache**: the media-tagger step synthesizes a `cache_dir` input defaulting to `paths.workspace_media_tagger_cache_dir()` (`<runtime>/cache/media_tagger`), threaded through `synthesize_media_tagger_step` → `build_media_tagger_metadata_inputs` as the `cache_dir` step option (overridable via `options.cache_dir`). `MediaTaggerHttpCache` honors a non-empty `cache_dir`; empty disables caching. `cache_expiry_seconds` defaults to `86400`.

`ffprobe` is the only source-metadata probe. Online `${media.metadata.*}` values are resolved during materialization from the yt-dlp `info.json` sidecar held in CAS (`materializer/metadata.rs`, `resolve_variant_metadata_key`); `media add` never contacts a remote source, and `add_media_source_with_position` takes its title, description, and media id from the caller.

### Media-tagger network contract

The media-tagger's MusicBrainz and Cover Art Archive traffic is bounded, injectable, and fails loudly. The module docs in `builtins/media_tagger/{mod,musicbrainz,cover_art}.rs` carry the deep spec; `tests/int/media_tagger_hermetic.rs` drives the whole metadata stage against `wiremock` hosts with both endpoints injected.

The 10 boolean options (`strict_identification`, `write_all_tags`, `write_all_images`, `save_images_to_tags`, `embed_only_one_front_image`, `caa_approved_only`, `preserve_images`, `clear_existing_tags`, `enable_tag_saving`, `release_ars`) are value arguments (`--write-all-images false` is accepted; a bare `--flag` is a parse error) and their workflow token specs are `TokenSpec::Pair`, so a configured `false` reaches the process. An equality-gated `Bool` token spec combined with clap's `true` default used to flip every configured `false` to `true`.

`musicbrainz_endpoint` and `caa_endpoint` are honored for real: the former's host[:port] selects the `musicbrainz_rs` fetch domain (the crate appends `/ws/2` and speaks plain HTTP, so scheme and path are ignored), the latter (`DEFAULT_CAA_ENDPOINT`, `https://coverartarchive.org`) replaces the once-hardcoded archive URLs. One `MUSICBRAINZ_FETCH_DEADLINE` (90s) wraps the recording+release fetch, because `api_bindium`'s 503 handling sleeps on the server's `Retry-After` with no cap; every cover-art GET carries a 30s per-request timeout and the whole cover-art phase runs under one `COVER_ART_PHASE_BUDGET` (60s) deadline.

Fetch failures are loud: a MusicBrainz or cover-art fetch error fails the step — the stale-cache-on-error fallbacks are gone, though a fresh cache hit still short-circuits — and HTTP 404 from the archive is the only legitimate "no art" answer. Cover-art failures are unconditional, not gated by `strict_identification`, because a failed image fetch is a missing asset rather than missing metadata.

### Media-tagger cache contract

`MediaTaggerHttpCache` (implementation home: `builtins/media_tagger/cover_art.rs`) is a JSON index (`media-tagger.json`) plus a CAS `store/` under the step's `cache_dir`. Each namespace is keyed by every input that changes the server's answer, so an option change re-derives locally instead of serving another run's shapes:

| Namespace | Key inputs | TTL |
| --- | --- | --- |
| `musicbrainz-payloads` | recording/release MBIDs, `write_all_tags`, `release_ars`, MusicBrainz host\[:port\] | `cache_expiry_seconds` (24h default) |
| `caa-entries` — the raw archive response; the selection filters run on every read | CAA endpoint URL | `cache_expiry_seconds` |
| `caa-images` — downloaded image bytes | image URL (size already encoded there) | 30 days base (`MEDIA_TAGGER_LONG_LIVED_MAX_AGE_SECS`) |
| `acoustid-lookups` — the decoded answer, zero-match included | fingerprint, full AcoustID endpoint URL | 30 days base |

The two long-lived namespaces follow `max(cache_expiry_seconds, 30 days)`; a negative `cache_expiry_seconds` is never-expire for every namespace. The freshness ladder: a fresh row is used directly, a stale row is refetched (fetch failure stays a hard error, never stale-serving), an undecodable row drops from the index and reads as a miss. API keys never enter keys or index entries. Old-format rows (pre-shape MusicBrainz keys, post-filter CAA entries) miss and self-heal; regression tests pin both. Cache construction runs one `evict_expired` pass: stale rows dropped, dangling rows dropped, unreferenced blobs reclaimed through the CAS maintenance listing.

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

- **Builtin launchers** (media-tagger, import): `MEDIAPM_GIT_HASH`, the compile-time constant emitted by `build.rs`.
- **GitHub-release tools** (yt-dlp, deno, rsgain, sd): the resolved upstream commit hash, not the tag.
- **ffmpeg**: the composite `"{autobuild_tag}+{macos_leg_version}"`, where the macOS leg is the martin-riedl build id (e.g. `1759500000_N-127260-g7bc3576910`) when the permalink chain served or the jellyfin-ffmpeg version (e.g. `7.1.3-1`) when the fallback did; the leg is corrected after fetch from the URL that actually served the bytes (`canonical_version_after_fetch`), and the artifact set has no single commit hash, so neither a version nor a VCS hash identifies it.

What the provider resolves is only the tool's own segment. The value persisted in `ToolRegistryEntry.canonical_version` is a **composite**: `compute_composite_canonical_version()` appends one `;{dep_id}:{dep_own_version}` segment per SameStep dependency, sorted by dep id, so a dep's version change re-provisions the requester. Dep segments are non-transitive — each carries the dep's own first segment, never the dep's own composite — and a tool with no SameStep deps stores the bare provider value unchanged. Skip detection compares the composite, not the bare value.

Skip logic: when `reconcile_desired_tools` finds an entry in `state.managed_tools` whose composite `canonical_version` equals the freshly computed one and whose `content_map_hash` is non-empty, provisioning is skipped for that tool. The `Ok(None)` branch still populates `canonical_version` from the resolved value. `canonical_version` defaults to `""` via `#[serde(default)]` for backward-compat with old state files.

## Cache Architecture (Three-Tier)

See `.agents/instructions/cache-and-http.instructions.md` for the three-tier cache specification (content, metadata, provision), TTL policies, and hard boundary rules.

## Materialization

Direct CAS→output-path writes; no staging commit. Materialized paths marked read-only after sync. Link fallback order configurable in `runtime.materialization_preference_order` (default: hardlink → symlink → reflink → copy). NFD filenames enforced at two stages: config-declared components are rejected when not NFD-normalized, resolved components (which carry interpolated metadata) are normalized instead. Reserved path chars rejected. ZIP extraction under `<mediapm_dir>/tmp/`.

The rendered materialization progress screen (per-(entry,variant) file-count bars; rows carry media id / variant / basename with no phase tag) is documented in `.agents/instructions/progress-output.instructions.md` (Screen C).

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

Full workspace: `cargo fmt-check && cargo clippy-all && cargo test-all && cargo test-doc-all`. `cargo test-all` runs nextest plus the tempdir and janitor gates; `cargo test-doc-all` runs the workspace doctests plus the rustdoc link gate, separately because nextest cannot execute doctests. Both delegate to the `test-runner` crate (`scripts/test-runner`).

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
