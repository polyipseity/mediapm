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

**Config object policy**: All configuration(-like) structs use concrete value
types (no `Option`). Defaults are pushed to the boundary — a dedicated
`defaults.rs` module supplies `pub const` defaults. Boundary callers (CLI
parsing, library entry points, Nickel deserialization) apply these before
passing values downward. Internal code never consults defaults inline. (See
`src/mediapm-cas/src/defaults.rs` and `src/mediapm-conductor/src/defaults.rs`
for existing implementations.)

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
| `runtime` | Operational settings — see **Runtime configuration** below |
| `version` | Schema version marker |

**Runtime configuration** — The `runtime` section in `mediapm.ncl` configures operational behavior. Config fields use concrete value types (no `Option`); defaults are defined in `defaults.rs` and applied by boundary callers.

| Field | Default | Description |
|-------|---------|-------------|
| `mediapm_dir` | `.mediapm` | Runtime state directory under workspace root |
| `hierarchy_root_dir` | (required) | Output root for materialized library |
| `materialization_preference_order` | `["hardlink", "symlink", "reflink", "copy"]` | Method fallback order |
| `conductor_config` | `<mediapm_dir>/conductor.ncl` | Conductor config doc path |
| `conductor_machine_config` | `<mediapm_dir>/conductor.machine.ncl` | Conductor machine config doc path |
| `conductor_state_config` | `<mediapm_dir>/state.conductor.ncl` | Conductor state config doc path |
| `conductor_schema_dir` | `<mediapm_dir>/config/conductor/` | Conductor schema export path |
| `inherited_env_vars` | `{}` | Per-platform inherited env var name lists. Type: `BTreeMap<String, Vec<String>>` — each key is a platform name (`"macos"`, `"linux"`, `"windows"`, or `"all"`) mapping to a list of env var names to inherit into the managed-tool execution environment. When resolving, the current platform's key is looked up; if absent, the `"all"` entry is used as fallback. |
| `media_state_config` | `<mediapm_dir>/state.ncl` | Media state config path |
| `env_file` | `<mediapm_dir>/.env` | User-authored dotenv file path |
| `env_generated_file` | `<mediapm_dir>/.env.generated` | Auto-generated dotenv file path |
| `mediapm_schema_dir` | `<mediapm_dir>/config/mediapm` | MediaPM schema export path; `null` disables export |
| `profiler_enabled` | `false` | Enable runtime profiler |
| `verify_materialization` | `false` | Verify CAS→filesystem hash after materialization |
| `retry_impure` | `false` | Allow auto-retry for impure workflows |
| `path_sanitization` | `Inherit` | Hierarchy filename sanitization mode. Type: `SanitizeNamesConfig` — `Disabled` (pass through), `Inherit` (default, root→`Enabled`), `Enabled` (built-in reserved→`_`), or `Custom(BTreeMap<char, char>)` (explicit per-char mapping, overrides reserved-char set) |
| `instance_ttl_seconds` | `604800` (7 days) | Conductor GC instance pruning TTL. Matches the conductor config field name. |
| `verify_on_read` | `["modified", "sample"]` | CAS integrity verification triggers (`always`, `modified`, `sample`, `stale`) |
| `verify_on_read_sample_denominator` | `100` | 1-in-N sample probability when `sample` in `verify_on_read` |
| `verify_on_read_stale_timeout_secs` | `604800` (7 days) | Age threshold for stale verification when `stale` in `verify_on_read` |
| `reconstructed_cache_ttl_seconds` | `3600` (1 hour) | CAS reconstructed-bytes cache TTL. Renamed from `reconstructed_bytes_cache_ttl_secs` to match CAS crate naming (`reconstructed_cache_ttl` in `BackgroundEngine`). |

> **Conductor config ownership**: `verify_on_read` and related fields,
> `reconstructed_cache_ttl_seconds`, and `instance_ttl_seconds` belong
> semantically to the conductor crate's `ConductorRuntimeConfig`. The conductor
> crate must expose them; mediapm passes them through.

`mediapm sync` reconciles that state against the filesystem:

1. Read desired media, hierarchy, tools from `mediapm.ncl`.
2. Load conductor machine state and lock state (`state.ncl`).
3. Determine which media ids need workflow execution (new/changed config).
4. Execute conductor workflows for pending media.
5. Materialize CAS outputs to final library paths under the hierarchy root.
6. Remove stale hierarchy paths, empty parent dirs.
7. Mark managed paths read-only.

**Invalidation**: `mediapm media invalidate <media-id> <step-index>` selectively
marks completed tool calls for one media step as needing re-execution, without
re-adding or removing media. The result type `MediaStepInvalidationSummary`
reports which steps were invalidated with these fields:

| Field | Type | Description |
|-------|------|-------------|
| `workflow_id` | `String` | The workflow whose step was targeted |
| `targeted_step_ids` | `Vec<String>` | IDs of the steps that were invalidated |
| `removed_generated_timestamps` | `Vec<String>` | Previously recorded step-generation timestamps that were cleared |
| `removed_instances` | `Vec<String>` | Completed tool-call instances that were removed |
| `regenerated_step` | `bool` | Whether the step was immediately regenerated after invalidation |

Two boolean flag pairs control behavior:

| Flag pair | Default | Effect |
|-----------|---------|--------|
| `--invalidate-calls` / `--no-invalidate-calls` | `--invalidate-calls` | Mark completed tool calls as needing re-execution |
| `--regenerate` / `--no-regenerate` | `--no-regenerate` | Immediately regenerate the targeted step after invalidation |

The two flags in each pair are mutually exclusive. `--no-invalidate-calls` is
a no-op (the command does nothing).

**Sync CLI flags** — Flattened via `#[command(flatten)]` across `mediapm sync` (verify-materialization only applies to mediapm sync, not tool sync):

| Flag pair | Default | Effect |
|-----------|---------|--------|
| `--verify-materialization` / `--no-verify-materialization` | enabled | Verify CAS→filesystem integrity after materialization (mediapm sync only) |
| `--check-tag-updates` / `--no-check-tag-updates` | enabled | Re-check metadata tags on already-materialized files for upstream changes |

The two flags in each pair are mutually exclusive (`conflicts_with`). The `--no-*`
form explicitly overrides the per-command default. Per-command defaults are
defined in `sync_library_with_tag_update_checks(tag-updates-default, verify-materialization-default)`.

**Global CLI flags** — These apply to every `mediapm` invocation (parsed before the subcommand):

| Flag | Default | Env var | Description |
|------|---------|---------|-------------|
| `--root <path>` | `.` | `MEDIAPM_ROOT` | Workspace root hosting `mediapm.ncl` and `.mediapm/` |
| `--mediapm-dir <path>` | — | `MEDIAPM_DIR` | Override `runtime.mediapm_dir` |
| `--conductor-config <path>` | — | `MEDIAPM_CONDUCTOR_CONFIG` | Override `runtime.conductor_config` |
| `--conductor-machine-config <path>` | — | `MEDIAPM_CONDUCTOR_MACHINE_CONFIG` | Override `runtime.conductor_machine_config` |
| `--conductor-state-config <path>` | — | `MEDIAPM_CONDUCTOR_STATE_CONFIG` | Override `runtime.conductor_state_config` |
| `--media-state-config <path>` | — | `MEDIAPM_MEDIA_STATE_CONFIG` | Override `runtime.media_state_config` |
| `--env-file <path>` | — | `MEDIAPM_ENV_FILE` | Override `runtime.env_file` |
| `--env-generated-file <path>` | — | `MEDIAPM_ENV_GENERATED_FILE` | Override `runtime.env_generated_file` |
| `--retry-impure` | `false` | `MEDIAPM_RETRY_IMPURE` | Enable corrupt-object retry for impure workflow steps |

When both a CLI flag and its env var are present, argv takes precedence. When
neither is set, the value from `mediapm.ncl` (or the built-in default) is used.

**`retry_impure` explained**: Conductor workflows may encounter
`CorruptObject` errors (CAS integrity failures). By default, only **pure**
workflows — those whose outputs are a deterministic function of their inputs —
auto-recover by dropping the corrupt object, clearing impure timestamps, and
re-executing once. **Impure** workflows (non-deterministic, e.g., those with
side effects) do NOT auto-retry unless `retry_impure = true`, which gives them
the same single retry recovery path. This prevents non-deterministic side
effects (like duplicate downloads or API calls) from accidental re-execution.

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

**Media add CLI** — `mediapm media add` flags (`MediaAddArgs`):

| Flag | Values | Description |
|------|--------|-------------|
| `--preset <preset>` | `yt-dlp`, `local` | Source type preset |
| `<source>` | (positional) | Source value: `http(s)://` URL for `yt-dlp`, filesystem path for `local` |
| `--title <text>` | string | Title override; prepended as `Literal` in metadata fallback chain (§5) |
| `--artist <text>` | string | Artist override; prepended as `Literal` in metadata fallback chain (§5) |
| `--description <text>` | string | Description override; takes full precedence over source-derived metadata (not fallback) |
| `--album <text>` | string | Album metadata; populates `metadata["album"]` as `Literal` when set |
| `--recording-mbid <uuid>` | string | MusicBrainz recording MBID passed to media-tagger step options |
| `--release-mbid <uuid>` | string | MusicBrainz release MBID passed to media-tagger step options |
| `--insert-position <pos>` | `sorted`, `beginning`, `end` | Insertion position in media map |
| `--overwrite` | flag | Replace existing media entry with same id |

The `--title`, `--artist`, and `--description` flags interact with the metadata
fallback chain (§5). `--title` and `--artist` are prepended as `Literal`
candidates at the front of their key's fallback chain — source-derived values
(from `--dump-json` for yt-dlp, ffprobe for local) occupy the next position,
and any existing `metadata.<key>` definition in `mediapm.ncl` serves as the
final fallback. `--description` differs: it takes full precedence over source
metadata rather than participating in the fallback chain.

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

- `tool add <name>` — register requirement in `mediapm.ncl`. If the name is not
  in the built-in catalog, a warning is printed but the requirement is still
  registered (allowing custom/unknown tools).
- `tool sync` — download, validate, register in conductor machine state.
- `tool list` — show registered tools and binary status. Output is a simple,
  minimal table with no unnecessary decoration. Current format:

  ```text
  tool_id              binary_present
  yt-dlp               true
  ffmpeg               true
  deno                 false
  rsgain               false
  media-tagger         true
  sd                   false
  ```

  The output is plain text, tab-separated for machine parsing, with a header
  line. `binary_present` is `true`/`false` indicating whether the binary was
  found on disk.
- `tool remove <name>` — remove requirement from config.
- `tool prune <id> [--metadata]` — remove downloaded binaries. With `--metadata`,
  also erase the tool's machine-document metadata and registry entry, forcing a
  full re-fetch on re-provisioning.
- `tool run <id> [args...]` — execute managed binary. Trailing `args` are passed
  verbatim (`trailing_var_arg = true`, `allow_hyphen_values = true`) so flags
  like `--flag value` reach the managed tool without `mediapm` interpreting them.
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
- Materialized paths marked read-only after sync. On macOS/BSD, the immutable
  flag (`UF_IMMUTABLE`/`SF_IMMUTABLE`) is cleared before managed replacement or
  removal operations to allow overwrites, then re-set after materialization.
  This is handled by `clear_bsd_immutable_flags()` in `materializer/commit.rs`.
- Path components support `${media.id}`, `${media.metadata.<key>}` placeholders.
- NFD-only filenames enforced; reserved path characters rejected.
- ZIP folder variant extraction under `.mediapm/tmp/`.

**Progress indication** — Materialization shows a progress bar per worker slot
with an overall progress bar summarizing total progress. The design is:

- One bar per active worker (e.g., `[worker 1/4] [====>    ] media-id`).
- One overall bar at the bottom (`[total] [========>   ] 45/120 items`).
- Bars collapse to a single summary line on completion.
- Minimal, non-flashing output (overwrite-in-place via carriage return).

This applies to materialization, tool download, and any batch operation with
predictable item counts.

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
- Playlist generation: M3U8, PLS, XSPF, WPL, ASX (controlled by `PlaylistFormat` enum passed to the playlist generator).
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

**Hierarchy CLI** — `mediapm hierarchy add` and `mediapm hierarchy remove`:

`mediapm hierarchy add`:

| Flag | Values | Description |
|------|--------|-------------|
| `--preset <preset>` | `local`, `yt-dlp` | Hierarchy-add preset (`MediaHierarchyPreset`) |
| `--root-folder <path>` | string | Root folder name; defaults to `media/` |
| `--insert-position <pos>` | `sorted`, `beginning`, `end` | Insertion position in root-folder group |
| `--overwrite` | flag | Replace existing node with same id |
| `<media-id>` | (positional) | Existing media id in `mediapm.ncl` |

`mediapm hierarchy remove`:

| Flag | Values | Description |
|------|--------|-------------|
| `--preset <preset>` | `local`, `yt-dlp` | Hierarchy-remove preset |
| `--root-folder <path>` | string | Root folder name; defaults to `media/` |
| `<media-id>` | (positional) | Existing media id in `mediapm.ncl` |

**Insertion position** (`AddInsertPosition`) controls where entries are placed:

| Value | Effect |
|-------|--------|
| `sorted` | Lexicographic sorted position (default) |
| `beginning` | Beginning of the map or group |
| `end` | End of the map or group |

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

**Builtin CLI entry point**: `mediapm builtin media-tagger` exposes the native
tagger as a standalone CLI for testing and scripting:

| Flag | Description |
|------|-------------|
| `--input <path>` | Optional input media payload path (omit for MBID-only fetch) |
| `--output <path>` | Output media payload path (required) |
| `--acoustid-api-key <key>` | AcoustID API key override |
| `--acoustid-endpoint <url>` | AcoustID lookup endpoint |
| `--musicbrainz-endpoint <url>` | MusicBrainz endpoint label |
| `--cache-dir <path>` | Persistent metadata/cover-art cache directory |
| `--cache-expiry-seconds <n>` | Cache expiry budget in seconds (negative = no expiry) |
| `--strict-identification` | Fail hard when identity cannot be resolved |
| `--write-all-tags` | Project all MusicBrainz tags |
| `--write-all-images` | Attach all cover-art images |
| `--save-images-to-tags` | Embed selected images into output tags |
| `--embed-only-one-front-image` | Keep only one front cover image |
| `--ca-providers <list>` | Ordered cover-art provider selector |
| `--caa-image-types <expr>` | CAA image-type selector |
| `--caa-image-size <size>` | Requested CAA image size |
| `--caa-approved-only` | Restrict to approved CAA entries |
| `--preserve-images` | Preserve existing embedded images |
| `--clear-existing-tags` | Clear existing tags before applying new ones |
| `--enable-tag-saving` | Enable output-tag writing |
| `--release-ars` | Enable release relationship processing |
| `--cover-art-slot-count <n>` | Deterministic cover-art attachment slots |
| `--recording-mbid <id>` | Recording MBID override (`auto`, `none`, or MBID) |
| `--release-mbid <id>` | Release MBID override (`auto`, `none`, or MBID) |

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

Four-document model:

- `mediapm.ncl` — **User intent for mediapm**: `media`, `hierarchy`, `tools`,
  `runtime`. Versioned (`v1` currently). Authoritative user configuration.
- `mediapm.conductor.ncl` — **User intent for conductor**: Generated from
  `mediapm.ncl` via schema mapping at sync time. Contains workflow definitions,
  tool config overrides, and conductor-specific settings. Carries a top-level
  `version` marker.
- `mediapm.conductor.generated.ncl` — **Machine-managed conductor state**:
  Tool registry (resolved hashes, provisioned versions), workflow runtime state,
  and conductor orchestration cache. Fully machine-controlled; users should not
  edit it. Carries explicit top-level `version` marker.
- `state.ncl` — **Machine-managed mediapm state**: Additional fully
  machine-controlled file tracked alongside conductor state. Contains:
  - `version`: Schema version marker.
  - `workflow_states`: Per-media per-step tracking, including:
    - `generated_timestamp` (distinct from conductor's `ImpureTimestamp`):
      Records when a step was synthesized/generated by mediapm, not when it
      was executed. Renamed from `impure_timestamp` in earlier designs to avoid
      confusion with the conductor-level `ImpureTimestamp`.
  - `last_materialized_state_hash: Option<Hash>`: CAS hash of the last fully
    materialized library state. When the user's config hash matches this,
    materialization is skipped entirely (fast no-op sync).
  - `managed_files`: Set of files managed by mediapm (for cleanup tracking).
  - `tool_registry`: Resolved tool identities and deployment state.
  - `active_tools`: Currently provisioned tool instances.

All machine-managed documents have explicit top-level `version` markers.
Migration dispatcher in `config/versions/`. Nickel contract validation via
`nickel-lang-core`.

**Version dispatch**: The `version` field at the top of each config document
selects the correct schema and migration path. This enables backward-compatible
config evolution: a config written for `v1` still loads after the codebase has
migrated to `v2`, with automatic upgrade during the first write-back.

**Validation rules** — Cross-field document validation lives in
`config/validation/`. Key rules enforced:

- **`content_map` key safety**: All `tool_configs.<tool>.content_map` keys
  must be sandbox-relative; absolute paths and path-traversal (`../`) entries
  are rejected.
- **No duplicate target paths**: Separate `content_map` entries must not
  overwrite the same target path.
- **Managed tool schema strictness**: Builtin tool definitions in persisted
  config are strictly `kind` + `name` + `version` only; extra fields are
  rejected on decode.
- **Hierarchy ID uniqueness**: Node `id` values must be unique across the
  hierarchy when provided.
- **`media_id` requirement**: `media` and `media_folder` nodes require one
  effective non-empty `media_id` (direct or inherited from a parent folder).
- **`rename_files` placement**: `rename_files` regex rewrites are valid only
  on `media_folder`/`folder` nodes (not on `media` file-leaf targets).
- **Output-variant kind strictness**: Kind naming uses canonical names only
  — no legacy aliases (e.g., `subtitles` not `subtitle`).
- **Hierarchy schema strictness**: Only ordered node-array format
  (`hierarchy = [{ ... }]`) is accepted; legacy flat-map and `"/kind"` forms
  are unsupported.

### 8. Global User Cache

- `<os-cache-dir>/mediapm/cache/` — shared download cache across workspaces.
- Layout: `store/` (CAS payloads) + `tools.jsonc` (metadata index).
- 30-day eviction for inactive entries.
- Commands: `global path`, `global init`, `global tool-cache status/prune/clear`.

**Output structs**:

- `GlobalToolCacheStatus` reports cache directory layout:
  - `tool_cache_dir`: Root directory of the tool cache.
  - `store_dir`: CAS payload subdirectory.
  - `index_jsonc`: Metadata index file path.
  - `entry_count`: Number of entries in the index.
- `GlobalToolCachePruneSummary` reports cleanup results:
  - `removed_entries`: Count of stale index entries removed.
  - `removed_payloads`: Count of orphaned payload files removed.

These are intentionally minimal — no nested report types, no per-entry
breakdowns. If richer output is needed later, add a `--verbose` flag instead
of expanding the default output.

### 9. Passthrough CLIs

- `mediapm cas [args...]` — forward to `mediapm-cas` binary (or run
  in-process if the crate is available as a library).
- `mediapm conductor [args...]` — forward to `mediapm-conductor` binary (or run
  in-process).
- `mediapm completions <shell>` — shell completion script generation (always
  in-process, no binary needed).

**Auto-injection strategy**: Defaults are injected via **environment variables**
(rather than argv injection) so passthrough CLI args always take precedence:
CLI > env var > config > built-in default. Each CLI flag maps to a `MEDIAPM_`-
prefixed env var (see the Global CLI flags table in §1 for the full mapping).

Additionally, env vars are supported for configuration values that have no CLI
flag equivalent — across all three crates (`mediapm-cas`, `mediapm-conductor`,
`mediapm`). These env vars bypass the config layer entirely and are consumed
directly at the binary/library boundary. Examples of settings currently lacking
CLI equivalents:

| Crate | Setting without CLI equivalent | Notes |
|-------|-------------------------------|-------|
| `mediapm-cas` | `verify_on_read` strategies, sample denominator, stale timeout | `CasIntegrityConfig` / `CasConfig` — only settable via Rust API |
| `mediapm-conductor` | `retry_impure` (already has `--retry-impure` in `mediapm` only) | `ConductorRuntimeConfig` — conductor binary itself lacks a dedicated flag |
| `mediapm-conductor` | `platform_inherited_env_var_names` (map of platform→env-var-name lists) | Nested map — cannot be a simple CLI flag |
| `mediapm` | `path_sanitization` (character mapping), `instance_ttl_seconds`, `reconstructed_cache_ttl_seconds` | Runtime config — currently Nickel-only, no CLI flags |

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
- CAS verification on read (configurable via `runtime.verify_on_read` and related
  fields — see §1 Runtime configuration). These fields belong semantically to
  the conductor crate (which opens the CAS store) and are passed through from
  mediapm. The CAS crate exposes `CasIntegrityConfig` with strategies:
  `Always`, `Modified`, `Sample`, `Stale`.
- `reconstructed_cache_ttl_seconds` (renamed from `reconstructed_bytes_cache_ttl_secs`): TTL for CAS reconstructed-bytes cache.
  Owned by conductor config, passed through from mediapm.
- Referential integrity: lock records → CAS hashes, pre-prune validation.

---

## Architecture

### Module Map

```text
lib.rs                          — Re-exports, top-level types (`MediaStepInvalidationSummary`,
                                `GlobalToolCacheStatus`, `GlobalToolCachePruneSummary`,
                                `SyncSummary`, `ToolsSyncSummary`), public free functions:
                                global cache ops (`resolve_default_global_paths`,
                                `ensure_global_directory_layout`, `global_tool_cache_status`,
                                `global_tool_cache_prune_expired`, `global_tool_cache_clear`),
                                runtime helpers (`load_runtime_dotenv_for_root`,
                                `resolve_effective_paths_for_root`, `load_runtime_dotenv`),
                                schema export (`export_mediapm_nickel_config_schemas`),
                                builtin discovery (`registered_builtin_ids`).
main.rs                         — CLI entry, clap dispatch, async main
error.rs                        — MediaPmError enum
defaults.rs                     — Centralized default values for all config fields
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

conductor_bridge/               — Conductor integration layer (tool provisioning, doc I/O, runtime storage — NOT workflow step synthesis; see tools/workflows/)
  mod.rs                        —   ToolSyncReport, reconcile_desired_tools
  constants.rs                  —   Input/output key constants
  documents.rs                  —   Load/save conductor NCL documents
  runtime_storage.rs            —   Runtime storage path resolution
  util.rs                       —   Shared helpers
  sync/mod.rs                   —   Tool reconciliation coordinator
  sync/provision.rs             —   Concurrent tool provisioning
  sync/tool_config.rs           —   Tool config generation (companion binding, content-map policy)
  sync/content_import.rs        —   Content map CAS import
  sync/lifecycle.rs             —   Tool lifecycle transitions
  tool_runtime/mod.rs           —   Sandbox paths, slot limits, tool spec builders (NOT step synthesis)
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

tools/                          — Managed tool catalog, provisioning, and workflow synthesis
  mod.rs                        —   Module router, re-exports
  catalog.rs                    —   TOOL_CATALOG, ToolOs, PlatformValue; all entries inline (flat)
  downloader.rs                 —   ToolDownloadCache, download, resolution, GitHub API, HTTP, cache, materialization, models (flat)
  models.rs                     —   Download plan types, tool identity models
  workflows/                    —   Managed workflow step synthesis (owns code formerly in conductor_bridge)
    mod.rs                      —     Module router
    ffmpeg.rs                   —     ffmpeg step synthesis
    media_tagger.rs             —     media-tagger step synthesis
    rsgain.rs                   —     rsgain step synthesis
    yt_dlp.rs                   —     yt-dlp step synthesis
    yt_dlp_inputs.rs            —     yt-dlp input defaults synthesis

builtins/                       — Builtin command implementations
  mod.rs                        —   Module router, builtin discovery
  media_tagger/                 —   Native media-tagger builtin
    mod.rs                      —     CLI argument routing (InternalMediaTaggerArgs)
    acoustid.rs                 —     AcoustID lookup
    cover_art.rs                —     Cover Art Archive fetch
    ffmetadata.rs               —     FFmetadata key/value mapping
    musicbrainz.rs              —     MusicBrainz API
    util.rs                     —     Shared helpers
```

### Key Designs

- **No Option in config objects**: All configuration structs use concrete value
  types. A `defaults.rs` module supplies `pub const` defaults; boundary callers
  apply them before passing values downward. Internal code never unwraps
  optional config values.
- **Simplified library API**: `MediaPmApi` trait is replaced by concrete
  `MediaPmService` methods. The service exposes explicit CRUD operations
  (add/remove/list/invalidate media, add/remove hierarchy, tool lifecycle)
  rather than a generic trait. Contrived trait abstractions are removed.
- **Generic service**: `MediaPmService<Cas: CasApi>` — pluggable CAS backend.
- **Four-document model**: (1) `mediapm.ncl` — mediapm user intent (media,
  hierarchy, tools, runtime). (2) `mediapm.conductor.ncl` — mediapm→conductor
  user intent, generated from `mediapm.ncl` via schema mapping. (3)
  `mediapm.conductor.generated.ncl` — machine-managed conductor runtime state.
  (4) `state.ncl` — additional machine-only mediapm state (materialization
  hashes, generated timestamps, managed files).
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
| 13 | **MediaRuntimeStorage Option fields** | ~20 fields, most `Option<T>` with defaults in getters. | Strip `Option` wrappers, push defaults to `defaults.rs`, apply at boundary. Align with CAS/conductor pattern. |

### Low Impact / Cosmetic

| # | Area | Observation | Suggestion |
|---|------|-------------|------------|
| 14 | **conductor_bridge/tool_runtime/ option_constants.rs, option_tokens.rs, template.rs** | Three small constants files. | Merge into one `tool_runtime/constants.rs`. |
| 15 | **global.rs** | `MediaPmGlobalPaths` methods: `from_cache_base_dir`, `from_data_base_dir`, `from_tool_cache_dir`. | Could consolidate constructors (some are unused except in tests). |
| 16 | **config/validation/ unused code** | Check for dead validation functions. | Remove unused validators. |

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
