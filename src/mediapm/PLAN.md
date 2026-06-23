# `mediapm` Crate — Requirements, Features, and Simplification Plan

## Overview

`mediapm` is the media orchestration facade crate. It composes `mediapm-cas`
(content-addressed storage), `mediapm-conductor` (declarative workflow engine),
and `mediapm`-specific policy logic (media library sync, tool provisioning,
hierarchy materialization, builtin media tagger) into a unified CLI and library
API.

The crate ships as:

- a library (`lib.rs`) — reusable `MediaPmService<Cas>` generic API,
- a CLI binary (`main.rs`) — `clap`-based interface.

---

## Requirements

### 1. Declarative Media Library Sync

Users define desired media state in `mediapm.ncl` (Nickel format). The top-level
config comprises five sections:

| Section | Purpose |
|---------|---------|
| `hierarchy` | Ordered node-array defining output layout (folders, media entries, playlists) with recursive `children` |
| `media` | Per-media config map keyed by media ID (`youtube.<video-id>`), containing metadata overrides (artist, title, description), step pipelines (tool chain with options and output variants), and per-platform metadata |
| `tools` | Managed tool requirements with version tags and optional dependency inheritance |
| `runtime` | Operational settings (hierarchy root dir, materialization order, instance TTL, retry policy) |
| `version` | Schema version marker |

`mediapm sync` reconciles that state against the filesystem:

1. Read desired media, hierarchy, tools from `mediapm.ncl`.
2. Load conductor machine state and lock state (`state.ncl`).
3. Determine which media ids need workflow execution (new/changed config).
4. Execute conductor workflows for pending media.
5. Materialize CAS outputs to final library paths under the hierarchy root.
6. Remove stale hierarchy paths, empty parent dirs.
7. Mark managed paths read-only.

**Media ID naming**: IDs follow `<source>.<native-id>` convention (e.g.,
`youtube.<video-id>`). This convention is used for hierarchy presets (node IDs
like `youtube.<video-id>.video`) and media presets (media map keys like
`youtube.<video-id>`). The source prefix enables cross-source disambiguation
without a separate `source_type` field. Preset generators (`--preset yt-dlp`)
auto-generate these IDs; custom configs may use any convention.

**Media entry model**: Each entry in the `media` map contains:

- **Top-level fields** (`artist`, `title`, `description`): Human-readable
  display identifiers, auto-populated from lightweight source metadata when
  available, independently overridable by the user. These are NOT directly used
  for path template resolution — that is handled by `metadata.<key>`.
- **`metadata`** object: Structured fallback-chain definitions for hierarchy
  path template resolution (Section 5). Each `<key>` may be a literal string, a
  single variant binding `{ metadata_key, variant }`, or an ordered fallback
  array.
- **`steps`** array: Ordered list of processing steps forming the media
  pipeline. Typical pattern: ingest → (optional ffmpeg) → media-tagger → rsgain.

**Step model**: Each step has these fields:

| Field | Required | Description |
|-------|----------|-------------|
| `tool` | Yes | Tool name: `yt-dlp`, `import`, `ffmpeg`, `media-tagger`, `rsgain` |
| `input_variants` | No | Previous step output variant names this step consumes. Source-ingest steps (yt-dlp, import) keep this empty — they originate content. Transform steps list which prior outputs to consume. Supports exact string (`"video"`) or regex object (`{ regex = "^subtitles/.+$" }`). |
| `output_variants` | Yes | Map of variant names to output configs (`kind`, optional `save` (boolean or `"full"`), `capture_kind`, `idx`, `extension`, `zip_member`) |
| `options` | No | Tool-specific key-value pairs (per-tool option key whitelist enforced by validation) |

The `input_variants` / `output_variants` naming is significant: `output_variants` specifies what a step produces, and `input_variants` selects from those produced outputs. A step's `output_variants` may produce outputs with the same name as its `input_variants` (e.g., ffmpeg consumes `input_variants = ["video"]` and produces `output_variants = { video = { kind = "primary", extension = "mkv" } }`). This is the update-a-variant pattern — the transform replaces/rewraps the variant content while preserving its name for downstream steps.

**Use cases** — Typical deployments fall into two patterns:

- **Homogeneous pipelines** (preset-driven): All media entries share identical
  step sequences with the same tool set and option keys. The current production
  deployment (93 entries) follows this pattern. The system should optimize for
  this common case: config loading, workflow synthesis, and caching all benefit
  from detecting and exploiting pipeline uniformity.

**Scalability**: The system targets 2000+ media entries with sub-second config
loading, incremental workflow execution (only new/changed media), and efficient
hierarchy flattening. Hierarchy preset generators produce constant-size output
per entry, independent of total count.

**Scalability testing requirements**:

- Config loading benchmarks at 2000+ entries with hierarchy generation.
- Hierarchy flattening benchmarks at 2000 entries × 8 child nodes (16,000 leaf
  nodes) to validate sub-second performance.
- Incremental workflow execution: verify that only changed entries trigger
  re-execution, with O(1) constant overhead independent of total entry count.

### 2. Multiple Source Types

| Source type | Preset | Ingest step | Default pipeline |
|---|---|---|---|
| Online | `--preset yt-dlp` | `yt-dlp` | yt-dlp → media-tagger → rsgain |
| Local | `--preset local` | `import` | import → media-tagger → rsgain |

- Online sources: URL-based, downloaded via yt-dlp.
- Local sources: file-path based, imported via CAS hash.
- Both pipelines optionally pass through ffmpeg (transcode) when enabled.

**Test requirements** — Both source types must have test coverage:

- **Online (yt-dlp)**: Integration tests with mocked yt-dlp output verifying
  the full ingest → metadata extraction → CAS storage pipeline.
- **Local (import)**: Integration tests exercising file-based import with
  ffprobe probe for metadata extraction, CAS hash import, and subsequent
  media-tagger/rsgain pipeline steps. The import pipeline is exercised by 0%
  of current deployments and therefore needs explicit mock-based coverage.

### 3. Managed Tool Provisioning

6 managed tools with built-in catalog entries:

| Tool | Source | Type |
|---|---|---|
| `yt-dlp` | GitHub Releases | Media downloader |
| `ffmpeg` | GitHub Releases (BtbN + evermeet-ffmpeg) | Transcode/analysis |
| `deno` | GitHub Releases | JS runtime (yt-dlp companion) |
| `rsgain` | GitHub Releases ZIP | Loudness normalization |
| `media-tagger` | Internal launcher | Metadata tagging |
| `sd` | GitHub Releases | String replacement (rsgain companion) |

Tool lifecycle commands:

- `tool add <name>` — register requirement in `mediapm.ncl`.
- `tool sync` — download, validate, register in conductor machine state.
- `tool list` — show registered tools and binary status.
- `tool remove <name>` — remove requirement from config.
- `tool prune <id>` — remove downloaded binaries and/or metadata.
- `tool run <id> [args...]` — execute managed binary directly.
- `tool refresh-runtime` — regenerate `.env.generated` and path scaffolding.

**Runtime environment files**:

- Conductor loads zero or more dotenv files in specified order before workflow
  execution. The standard convention uses two colocated files:
  - `.mediapm/.env` — user-authored config with commented-out defaults for
    `ACOUSTID_API_KEY`, `HTTP_PROXY`, `MEDIAPM_DOWNLOAD_TIMEOUT_SECONDS`, etc.
  - `.mediapm/.env.generated` — auto-generated by `tool refresh-runtime` with
    concrete absolute paths for tool binaries. Generated variables per tool:
    - **All tools**: `MEDIAPM_CONDUCTOR_EXECUTABLE_TIMEOUT_SECS`,
      `MEDIAPM_CONDUCTOR_RPC_TIMEOUT_SECONDS`
    - **ffmpeg**: `MEDIAPM_MEDIA_TAGGER_FFMPEG_BIN` (overridable),
      `MEDIAPM_YT_DLP_FFMPEG_LOCATION`
    - **deno** (yt-dlp JS runtime): `MEDIAPM_YT_DLP_JS_RUNTIMES`
    - **media-tagger**: `MEDIAPM_MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_MACOS`
      (or per-platform launcher variant)
    - **yt-dlp**: `MEDIAPM_DOWNLOAD_TIMEOUT_SECONDS`
- Conductor loads them in order: `.env` first, then `.env.generated`. Later
  files override earlier values. User overrides go in `.env`.
- The env file list is explicitly configurable. Each file name is resolved
  relative to the conductor runtime root. An empty list loads nothing.

**Version identity (two-level model)**:

- User config uses ergonomic forms: `{ tag = "latest" }` (auto-resolve to newest
  compatible release) or `{ version = "7.0.0" }` (pin to specific version).
- Machine state (`state.ncl`) resolves these to deterministic pinned
  hashes: `mediapm.tools.<name>+<source>@<deploy-hash>`.
- This gives reproducible deployments while keeping the user config concise.

**Dependency inheritance**: Tool requirements may declare companion dependencies
that inherit the workspace version:

```nickel
media-tagger = {
  dependencies = { ffmpeg_version = "inherit" },
  tag = "latest",
}
rsgain = {
  dependencies = { ffmpeg_version = "inherit", sd_version = "inherit" },
  tag = "latest",
}
yt-dlp = {
  dependencies = { deno_version = "inherit", ffmpeg_version = "inherit" },
  tag = "latest",
}
```

- `"inherit"` means "use the same version as the workspace's active tool entry
  for that dependency".
- Dependency key names follow `<tool>_version` convention where hyphens in
  tool names are replaced with underscores (e.g., `media_tagger_version` for
  `media-tagger`, `ffmpeg_version` for `ffmpeg`, `deno_version` for `deno`,
  `sd_version` for `sd`).
- This keeps tool versions synchronized without per-tool duplication.

### 4. Hierarchy & Materialization

- Ordered node-array hierarchy schema (`hierarchy = [{ ... }]`) with recursive
  `children`.
- Node kinds: `folder` (default when omitted on parent nodes with `children`),
  `media` (singular `variant`), `media_folder` (plural
  `variants` + optional `rename_files`), `playlist`.
- **`folder`+`media_id` grouping**: `folder` nodes carry an optional
  `media_id` field to act as implicit grouping parents for per-media child
  subtrees. When a `folder` sets `media_id`, descendant nodes inherit it unless
  they set their own. This convention is in active production use: one grouping
  folder per media entry with ~7–8 child nodes for video, archive, description,
  infojson, subtitles, thumbnails, links. The inherited `media_id` enables
  media-aware path template resolution (`${media.id}`, `${media.metadata.<key>}`)
  for descendant node filenames and `rename_files` patterns. The flattening
  pipeline propagates `media_id` through the tree, so the convention is fully
  supported.
- Materialization: direct CAS→output-path writes (no staging commit).
- Materialization methods in fallback order: hardlink → symlink → reflink → copy.
  Configurable via `runtime.materialization_preference_order`.
- Materialized paths marked read-only after sync.
- Path components support `${media.id}`, `${media.metadata.<key>}` placeholders.
- NFD-only filenames enforced; reserved path characters rejected.
- ZIP folder variant extraction under `.mediapm/tmp/`.

**rename_files regex patterns** (`media_folder` only):

- `media_folder` nodes may specify `rename_files = [{ pattern, replacement }]`
  to rewrite extracted filenames.
- Patterns are applied to each extracted file member.
- Replacement supports `${media.id}`, `${media.metadata.<key>}` placeholders.
- Examples from the yt-dlp hierarchy preset:
  - Subtitles: `pattern = "^(?:.*/)?(?:.*\\.)?([^.\\/]+)\\.([^.\\/]+)$"`,
    `replacement = "${media.metadata.artist} - ${media.metadata.title} [${media.id}].$1.$2"`
  - Thumbnails (individual): `pattern = "^.*\\.([^.]*)$"`,
    `replacement = "${media.metadata.artist} - ${media.metadata.title} [${media.id}].thumbnail.$1"`
  - Thumbnails (folder icon): `pattern = "^.*\\.([^.]*)$"`, `replacement = "folder.$1"`
  - Links: `pattern = "^.*\\.([^.]*)$"`,
    `replacement = "${media.metadata.artist} - ${media.metadata.title} [${media.id}].link.$1"`

**Dual thumbnail capture** (produced by yt-dlp hierarchy preset):
Each video produces two thumbnail `media_folder` entries:

1. One that renames the thumbnail to `[artist] - [title] [id].thumbnail.<ext>`
   (for individual display).
2. One that renames to `folder.<ext>` (for folder-icon use in file managers).
Both reference the `thumbnails` variant with different `rename_files` rules.

**Playlist nodes**:

- `playlist` kind nodes define ordered playlist files from hierarchy node IDs.
- `ids` field references hierarchy node IDs (e.g., `"youtube.<video-id>.video"`),
  not media IDs. Each entry targets a specific variant output within a media.
- IDs must be explicitly enumerated — no auto-discovery, glob patterns, or
  wildcard expansion.
- `path` sets the output filename (e.g., `"all.m3u8"`).
- Playlist generation: M3U8, PLS, XSPF, WPL, ASX.
- Playlists are placed under their hierarchy parent folder.

**Playlist path conventions**:

- Entry paths in generated playlists are **relative** by default (e.g.,
  `../music videos/Artist - Title [id]/file.mkv`), computed as relative offsets
  from the playlist file's directory to the referenced materialized output.
- Path type can be configured via an object form instead of a string ID:

  ```nickel
  { id = "youtube.<video-id>.video", path = "absolute" }
  ```

  Supported `path` values: `"relative"` (default), `"absolute"`.
- The bare string `"youtube.<video-id>.video"` is shorthand for
  `{ id = "youtube.<video-id>.video", path = "relative" }`.

**ffmpeg source note**: Multiple ffmpeg sources exist for different platforms.
Payloads from ALL sources are always downloaded regardless of the current
platform — on macOS you download both evermeet (macOS) and BtbN (Linux/Windows)
payloads, on Linux you download both BtbN and evermeet, etc. This ensures
cross-platform reproducibility (e.g., a CI runner on Linux can validate tool
content for macOS targets).

- BtbN: Linux, Windows
- evermeet: macOS

**Download code sharing**: The tool download/resolution logic for ffmpeg (and
other managed tools) should be shared between `mediapm` and `mediapm-conductor`,
since both need to download tool presets from the same catalog sources. Avoid
duplicating the catalog, download, and platform-resolution logic.

### 5. Metadata Resolution

Metadata serves two distinct roles:

- **[Top-level media fields]** (`artist`, `title`, `description`): Human-readable
  display identifiers, independently overridable (Section 1).
- **[`metadata.<key>` fields]**: Structured fallback-chain definitions consumed
  by hierarchy path template resolution (`${media.metadata.<key>}` placeholders
  in hierarchy `path` and `rename_files.replacement`).

**Metadata value types** — Each `metadata.<key>` value is one of three forms,
resolved in order by `#[serde(untagged)]` deserialization:

| Form | Nickel syntax | Description |
|------|---------------|-------------|
| Literal string | `video_ext = ".mkv"` | Used as-is. |
| Single variant binding | `video_id = { variant = "infojson", metadata_key = "id" }` | Extract a named key from a specific step variant's output metadata. |
| Fallback chain (array) | `artist = [{ variant = "video", metadata_key = "artist" }, { variant = "infojson", metadata_key = "uploader" }, "Fallback Name"]` | Ordered list of candidates; first non-empty match wins; literal strings act as terminal fallback. |

**Variant binding** (`MediaMetadataVariantBinding`): References a step's output
by `variant` name, then extracts a named `metadata_key` from that output's
metadata (JSON key lookup in yt-dlp/infojson output, or ffprobe tag extraction
for media content). Supports an optional regex transform:

```nickel
{
  variant = "infojson",
  metadata_key = "uploader",
  transform = { pattern = "^(.*) - Topic$", replacement = "$1" },
}
```

The transform is a full-match regex with capture-group replacement; it applies
only when the resolved value matches the pattern.

**Resolution pipeline** (in `materializer/metadata.rs`):

1. **Template extraction**: `${media.metadata.<key>}` patterns extracted from
   path templates.
2. **Per-key resolution**: Dispatches on value type — `Literal` returns the
   string directly; `Variant` calls through to variant-payload resolution;
   `Fallback` iterates candidates returning the first non-empty result.
3. **Variant payload resolution**: Fetches actual bytes for the specified step
   variant (via CAS or materialized output), then extracts the requested key.
4. **Metadata extraction from payload**: Checks persistent metadata cache first
   (BLAKE3-keyed). On cache miss: tries JSON key lookup with case-insensitive
   matching through top-level keys, `format.tags.<key>`, and `streams[].tags.<key>`;
   falls back to running managed ffprobe for media content. Stores result in
   cache.
5. **Optional regex transform**: Applied if the binding includes `transform`.

**Persistent metadata cache** (`metadata_cache.rs`): On-disk JSONC file keyed
by `blake3::hash(media_id)`. TTL-based expiry (86400s default), timer-based
batch persistence (dirty flag with periodic flush, not write-through). Avoids
repeated ffprobe/network calls for the same media entry.

**Source metadata probes** — Used to auto-populate top-level fields and test
source reachability:

- **Online** (`source_metadata.rs`): Calls yt-dlp `--skip-download --dump-json`
  for a URL, returns title/artist/description.
- **Local**: Runs ffprobe on a file path for title/artist.

**Produced variant sidecars** — Certain step variants carry metadata that feeds
resolution:

- yt-dlp `infojson` variant: Full yt-dlp JSON (title, uploader, id, etc.) —
  the most common metadata source for online media.
- yt-dlp `description` variant: Separate description text.
- ffprobe analysis: Available for any media-content variant, exposing
  format/stream tags.

**`video_ext` convention**: The file extension for materialized primary output
comes from the ffmpeg step's `output_variants.<name>.extension` (e.g.,
`extension = "mkv"`). It is referenced in path templates via
`${media.metadata.video_ext}`, set by the media entry's `metadata.video_ext`.

**Test requirements** — Regex transform path:

- Unit tests for `MediaMetadataVariantBinding` that verify the regex transform
  is applied when the resolved value matches the pattern, and skipped when it
  does not.
- Integration tests verifying the full resolution pipeline (template extraction
  → variant payload resolution → regex transform → path substitution) with
  mocked variant payloads. The regex transform path is exercised by 0% of
  current deployments, making it a high-risk untested code path.

### 6. Native Media Tagger Builtin

Replaces external Picard dependency with internal pipeline that branches on
whether MBIDs are explicitly provided:

- **With explicit MBID overrides** (e.g., `recording_mbid`, `release_mbid` in
  step `options`): Use the MBID directly — skip fingerprinting and AcoustID
  lookup entirely. Proceed straight to MusicBrainz API fetch.
- **Without explicit MBIDs**: Full auto-resolution path:
  1. Decode audio via ffmpeg → Chromaprint fingerprint.
  2. Resolve MBIDs via AcoustID lookup.
  3. Fetch recording/release metadata via MusicBrainz API.

After MBID resolution (by either path):
4. Fetch cover art from Cover Art Archive (deterministic selection).
5. Map metadata to FFmetadata key/value pairs.
6. Persist FFmetadata document for downstream apply.

Configurable: `strict_identification`, MBID overrides, AcoustID API key,
cover-art providers/types/sizes, tag-writing controls.

**Versioning**: `media-tagger` is part of mediapm itself, so its version always
follows the mediapm crate version (via `env!("CARGO_PKG_VERSION")` or
equivalent). No separate `media-tagger` version pinning is needed. The catalog
entry MUST report the crate version, not a separately maintained string — any
discrepancy (e.g., a deployment showing `version = "0.0.0"`) is a bug.

**Test requirements**:

- Both MBID paths must have test coverage:
  - **Explicit MBID path**: Unit tests with mocked MusicBrainz API responses
    verifying that explicit `recording_mbid`/`release_mbid` options skip
    fingerprinting and AcoustID entirely.
  - **Auto-resolution path (AcoustID)**: Integration tests (with mocked network
    endpoints for AcoustID and MusicBrainz APIs) verifying the full
    Chromaprint → AcoustID lookup → MusicBrainz fetch pipeline. The AcoustID
    path is exercised by 0% of current deployments, making it a high-risk
    untested code path.
- AcoustID API calls must be mockable via trait injection or HTTP mock server
  to enable deterministic testing without external service dependencies.

### 7. Versioned Config Schemas

- `mediapm.ncl` — user-authored config, versioned (`v1` currently).
- `state.ncl` — machine-managed state, versioned.
- Both carry explicit top-level `version` marker.
- Migration dispatcher in `config/versions/`.
- Nickel contract validation via `nickel-lang-core`.

### 8. Global User Cache

- `<os-cache-dir>/mediapm/cache/` — shared download cache across workspaces.
- Layout: `store/` (CAS payloads) + `tools.jsonc` (metadata index).
- 30-day eviction for inactive entries.
- Commands: `global path`, `global init`, `global tool-cache status/prune/clear`.

### 9. Passthrough CLIs

- `mediapm cas [args...]` — forward to `mediapm-cas` binary.
- `mediapm conductor [args...]` — forward to `mediapm-conductor` binary.
- `mediapm completions <shell>` — shell completion script generation.

### 10. Error Taxonomy

Centralized `MediaPmError` enum:

- `InvalidSource` — scheme validation failures.
- `Workflow(String)` — consistency violations.
- `Serialization(String)` — schema/Nickel errors.
- `Io { operation, path, source }` — filesystem errors with context.
- `Conductor(ConductorError)` — propagated from conductor crate.

### 12. ffmpeg Transform Step

When an ffmpeg step is present in a media pipeline, it sits between the ingest
step (yt-dlp/import) and the tagging/normalization steps:

**Stream selection**: By default ffmpeg selects ALL streams (video, audio,
subtitle, attachments, and data streams) from the input — not only the first
video stream. This preserves multi-track content through the transform.

**Output format (extension)**: When `extension` is specified in the ffmpeg
output variant (e.g., `extension = "mkv"`), the step re-wraps or transcodes to
that container. This is a separate operation from yt-dlp's
`merge_output_format` — yt-dlp produces an intermediate format, then ffmpeg
produces the final output.

**Stream indexing**: `idx` in ffmpeg output variants selects the N-th stream
(of the matching type) as the primary output. When omitted, all streams are
passed through. Multiple output variants can extract different streams for
separate materialization.

### 11. Content Identity & Integrity

- BLAKE3 hashing for all content identities.
- CAS verification on read (configurable sampling/stale-timeout).
- Referential integrity: lock records → CAS hashes, pre-prune validation.

---

## Architecture

### Module Map

```text
lib.rs                          — Re-exports, top-level types, global cache fns
main.rs                         — CLI entry, clap dispatch, async main
error.rs                        — MediaPmError enum
paths.rs                        — MediaPmPaths (15 path fields)
global.rs                       — MediaPmGlobalPaths (user-level cache)
http_client.rs                  — Shared reqwest::Client (OnceLock)
metadata_cache.rs               — On-disk JSONC metadata cache with TTL
source_metadata.rs              — Online/local metadata probe helpers
hierarchy.rs                    — Hierarchy preset builders, node mutation
service.rs                      — MediaPmService<Cas> impl (~1000+ lines)
service_standalone.rs           — Free functions extracted from service.rs
test_util.rs                    — Shared test Tokio runtime builder

config/                         — NCL document model, serde types, I/O
  mod.rs                        —   MediaPmDocument, MediaRuntimeStorage, MediaPmState...
  source_types.rs               —   MediaSourceSpec, MediaStep, MediaStepTool...
  hierarchy_types.rs            —   HierarchyNode, HierarchyEntry, flattening...
  output_types.rs               —   Output variant configs, capture kinds...
  custom_deserializers.rs       —   Custom serde deserializers
  nickel_io.rs                  —   Nickel evaluate/encode helpers
  versions/mod.rs               —   Schema version dispatch
  versions/v1.rs                —   V1 envelope/state types
  versions/mod.ncl, v1.ncl      —   Embedded Nickel contracts
  validation/mod.rs             —   Cross-field document validation
  validation/hierarchy.rs       —   Hierarchy policy validation
  validation/sources.rs         —   Source/step validation

conductor_bridge/               — Conductor integration layer
  mod.rs                        —   ToolSyncReport, reconcile_desired_tools
  constants.rs                  —   Input/output key constants
  documents.rs                  —   Load/save conductor NCL documents
  runtime_storage.rs            —   Runtime storage path resolution
  util.rs                       —   Shared helpers
  sync/mod.rs                   —   Tool reconciliation coordinator
  sync/provision.rs             —   Concurrent tool provisioning
  sync/tool_config.rs           —   Tool config generation
  sync/content_import.rs        —   Content map CAS import
  sync/lifecycle.rs             —   Tool lifecycle transitions
  tool_runtime/mod.rs           —   Sandbox paths, slot limits, tool spec builders
  tool_runtime/option_constants.rs — Tool option default definitions
  tool_runtime/option_tokens.rs — Template token definitions
  tool_runtime/template.rs      — Command template syntax constants
  tool_runtime/launcher.rs      — media-tagger launcher path resolution

materializer/                   — CAS→filesystem materialization
  mod.rs                        —   sync_hierarchy() entry
  commit.rs                     —   Remove/re-only path operations
  file_ops.rs                   —   Hardlink/symlink/reflink/copy helpers
  metadata.rs                   —   Template/metadata value resolution
  resolve.rs                    —   Source/variant hash resolution
  playlist.rs                   —   Playlist file generation (5 formats)
  zip.rs                        —   ZIP archive member/folder extraction

tools/                          — Managed tool provisioning and workflow synthesis
  mod.rs                        —   Module router
  catalog/mod.rs                —   TOOL_CATALOG, ToolOs, PlatformValue
  catalog/ffmpeg.rs             —   ffmpeg catalog entry
  catalog/yt_dlp.rs             —   yt-dlp catalog entry
  catalog/deno.rs               —   deno catalog entry
  catalog/rsgain.rs             —   rsgain catalog entry
  catalog/media_tagger.rs       —   media-tagger catalog entry
  catalog/sd.rs                 —   sd catalog entry
  downloader/mod.rs             —   ToolDownloadCache, download/resolution
  downloader/cache.rs           —   Download cache helpers
  downloader/github.rs          —   GitHub release API
  downloader/http.rs            —   HTTP download
  downloader/materialize.rs     —   Payload→CAS materialization
  downloader/models.rs          —   Download plan types
  downloader/resolve.rs         —   Download plan resolution
  workflows/mod.rs              —   Managed workflow synthesis
  workflows/ffmpeg.rs           —   ffmpeg step synthesis
  workflows/media_tagger.rs     —   media-tagger step synthesis
  workflows/rsgain.rs           —   rsgain step synthesis
  workflows/yt_dlp.rs           —   yt-dlp step synthesis
  workflows/yt_dlp_inputs.rs    —   yt-dlp input defaults synthesis

builtins/
  mod.rs                        —   Builtin command module router
  media_tagger/mod.rs           —   Native tagging pipeline
  media_tagger/acoustid.rs      —   AcoustID lookup
  media_tagger/cover_art.rs     —   CAA image fetch/selection
  media_tagger/ffmetadata.rs    —   FFmetadata parse/write
  media_tagger/musicbrainz.rs   —   MusicBrainz API client
  media_tagger/util.rs          —   ffmpeg resolution, subprocess helpers
```

### Key Designs

- **Generic service**: `MediaPmService<Cas: CasApi>` — pluggable CAS backend.
- **Three-document pattern**: `mediapm.ncl` (user intent) → `state.ncl` (machine
  state) → conductor NCL docs (runtime).
- **Direct materialization**: No staging commit; CAS→output-path writes with
  automatic cleanup on partial failure.
- **Deterministic tool identity**: `mediapm.tools.<name>+<source>@<hash|version|tag>`.
- **Cross-platform payload layout**: Platform-prefixed `content_map` keys
  (`windows/`, `linux/`, `macos/`) with `${context.os}`-guarded command selectors.

---

## Simplification Opportunities

### High Impact

| # | Area | Observation | Suggestion |
|---|------|-------------|------------|
| 1 | **service.rs** | Single file >1000 lines. All logic is in `impl MediaPmService<C>` block. | Split into submodules by concern: `sync/`, `media_crud/`, `tools_crud/`, `hierarchy_crud/`. Each submodule re-exports from `service/mod.rs`. |
| 2 | **service_standalone.rs** | 15+ free functions extracted from service.rs but still tightly coupled to it. | Fold back into `service/` submodules or distribute to more specific modules (e.g. path/validation helpers → `config/`, conductor state helpers → `conductor_bridge/`). |
| 3 | **Materializer file count** | 6 files for one primary entry point (`sync_hierarchy`). | Merge `commit.rs` + `file_ops.rs` → `file_ops.rs`; evaluate merging `resolve.rs` into `mod.rs` (it's small). |
| 4 | **Conductor bridge sync/ tool_config.rs** | Largest sync file. | Extract `env_generated` and `companion_*` functions into separate files under `sync/`. |
| 5 | **Tools catalog per-file** | 7 files (mod.rs + 6 per-tool) for small catalog entries. | Merge all catalog entries into one file or keep but only if they grow. Current per-file entries are ~50-100 lines each — marginal value. Consider merging into `catalog/entries.rs`. |
| 6 | **Tools workflows per-file** | 6 files for step synthesis. | Merge `yt_dlp_inputs.rs` into `yt_dlp.rs` (it's a supporting module). Keep others separate since they're larger. |

### Medium Impact

| # | Area | Observation | Suggestion |
|---|------|-------------|------------|
| 7 | **Config validation split** | `validation/hierarchy.rs` + `validation/sources.rs` | Consider merging into one `validation/rules.rs` since both are cross-field checks on `MediaPmDocument`. |
| 8 | **custom_deserializers.rs** | Very small file (~2 functions). | Inline into `config/mod.rs` or a simple `deserializers.rs` inline in the module. |
| 9 | **conductor_bridge/constants.rs** | Standalone constants file. | Could merge into `conductor_bridge/legacy.rs` or `tool_runtime/mod.rs` (where they're mostly consumed). |
| 10 | **metadata_cache.rs** | Timer-based persistence adds complexity (interval checks, dirty flag, Drop handler). | Consider simplifying to write-through (every `set()` flushes) since cache size is small. Remove `Arc<Mutex<>>` by making cache non-shared. |
| 11 | **http_client.rs** | `OnceLock<Result<...>>` pattern. Two custom error paths. | Simplify: make `build_shared_http_client` infallible (reasonable defaults always work), remove the `Err` variant in `OnceLock`. |
| 12 | **MediaPmPaths field count** | 15 fields, complex resolution. | Group related paths into sub-structs: `ConductorPaths`, `CachePaths`, `ConfigPaths`. |

### Low Impact / Cosmetic

| # | Area | Observation | Suggestion |
|---|------|-------------|------------|
| 13 | **conductor_bridge/tool_runtime/ option_constants.rs, option_tokens.rs, template.rs** | Three small constants files. | Merge into one `tool_runtime/constants.rs`. |
| 14 | **global.rs** | `MediaPmGlobalPaths` methods: `from_cache_base_dir`, `from_data_base_dir`, `from_tool_cache_dir`. | Could consolidate constructors (some are unused except in tests). |
| 15 | **config/validation/ unused code** | Check for dead validation functions. | Remove unused validators. |

### Defer (Intentionally Kept)

| # | Area | Rationale |
|---|------|-----------|
| — | **`impl MediaPmService<C>` in one block** | Documented trade-off: spliting impl blocks across files needs `include!()`. Keep whole until trait breakup or derive-based impl splitting. |
| — | **Large AGENTS.md** | Documented policy content; split into `.agents/instructions/` is already done. |
| — | **`builtins/media_tagger` file count** | Each file covers one distinct API domain (AcoustID, CoverArt, MB, FFmetadata). Keep separate. |
| — | **`downloader/` file count** | Each file covers one concern (cache, github, http, materialize, models, resolve). Keep separate. |

---

## Execution Order (if implementing)

1. **Merge `custom_deserializers.rs`** into `config/mod.rs` (low risk, pure code
   move).
2. **Simplify `http_client.rs`** — make builder infallible, flatten to
   `OnceLock<Client>`.
3. **Simplify `metadata_cache.rs`** — write-through instead of timer-based
   persistence.
4. **Merge `option_constants.rs` + `option_tokens.rs` + `template.rs`** into
   one `tool_runtime/constants.rs`.
5. **Merge `config/validation/` into one `rules.rs`** (or at minimum reduce
   file count).
6. **Merge `yt_dlp_inputs.rs` into `yt_dlp.rs`** under `tools/workflows/`.
7. **Combine `materializer/commit.rs` + `file_ops.rs`**.
8. **Consolidate `catalog/` entries** — evaluate merging all into one file.
9. **Restructure `service.rs`** into `service/` module with submodules
   (highest impact, last because it's invasive).

Each step preserves public API surface and CLI behavior.
