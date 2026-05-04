# Mediapm Crate Instructions

This file defines crate-local guidance for `src/mediapm/`.
Follow this together with workspace-wide policy in `AGENTS.md` and focused
instruction files in `.agents/instructions/`.

## Scope

- Applies to all files under `src/mediapm/`.
- Use this file for mediapm behavior and integration policy.
- If rules conflict, prefer root `AGENTS.md` for global policy and this file
  for mediapm-specific implementation details.

## Orchestration contract

- Treat `mediapm` as specialized media orchestration over conductor + CAS:
  deterministic planning/state reconciliation first, side effects second.
- Keep sync behavior atomic through staging under effective `.mediapm/tmp`
  before commit into materialized output roots.
- Preserve strict cross-platform path safety and deterministic link fallback
  behavior.

## Source of truth (mediapm)

Use concrete files as canonical references:

- Crate manifest: `src/mediapm/Cargo.toml`
- Library entry: `src/mediapm/src/lib.rs`
- CLI entry: `src/mediapm/src/main.rs`
- Error taxonomy: `src/mediapm/src/error.rs`
- Builtin media tagger implementation: `src/mediapm/src/builtins/media_tagger.rs`
- Config module root: `src/mediapm/src/config/mod.rs`
- Lockfile module root: `src/mediapm/src/lockfile/mod.rs`
- Config wire versions/migrations: `src/mediapm/src/config/versions/`
- Lockfile wire versions/migrations: `src/mediapm/src/lockfile/versions/`
- Integration tests: `src/mediapm/tests/`

Core dependency boundary:

- Compose CAS and Conductor via `mediapm-cas` and `mediapm-conductor`.
- Do not add direct dependencies from `src/mediapm/` to
  `src/conductor-builtins/*` crates.

## Runtime paths and resolution invariants

Keep these mediapm defaults and path rules intact:

- Runtime root defaults to `.mediapm/`.
- `mediapm.ncl` may optionally override runtime fields:
  `mediapm_dir`, `conductor_config`, `conductor_machine_config`,
  `conductor_state_config`, `inherited_env_vars`, `media_state_config`,
  `env_file`, `hierarchy_root_dir`, `mediapm_tmp_dir`,
  `conductor_tmp_dir`, `conductor_schema_dir`, `mediapm_schema_dir`, and
  `use_user_tool_cache`.
- `runtime.inherited_env_vars` is platform-keyed (`windows`, `linux`,
  `macos`, ...) where each value is an ordered list of environment-variable
  names. Runtime reads only the active host platform entry.
- Default runtime values:
  - `mediapm_dir = .mediapm`
  - `conductor_config = mediapm.conductor.ncl`
  - `conductor_machine_config = mediapm.conductor.machine.ncl`
  - `conductor_state_config = <mediapm_dir>/state.conductor.ncl`
  - `conductor_tmp_dir = <mediapm_dir>/tmp`
  - `conductor_schema_dir = <mediapm_dir>/config/conductor`
  - `media_state_config = <mediapm_dir>/state.ncl`
  - `env_file = <mediapm_dir>/.env`
  - `mediapm_schema_dir = <mediapm_dir>/config/mediapm`
- Materialized output root defaults to the directory containing the topmost
  `mediapm.ncl` (no implicit `library/` directory).
- Relative `runtime.hierarchy_root_dir` resolves relative to the topmost
  `mediapm.ncl` directory.
- Relative `runtime.mediapm_tmp_dir` resolves relative to effective
  `runtime.mediapm_dir`.
- Relative `runtime.conductor_tmp_dir` resolves relative to effective
  `runtime.mediapm_dir`.
- Relative `runtime.conductor_schema_dir` resolves relative to effective
  `runtime.mediapm_dir`.
- Relative `runtime.conductor_config`, `runtime.conductor_machine_config`,
  `runtime.conductor_state_config`, and `runtime.media_state_config` resolve relative to the
  topmost `mediapm.ncl` directory.
- Runtime dotenv loading uses effective `runtime.env_file` (default
  `<mediapm_dir>/.env`) and keeps a colocated `.gitignore` containing only
  `/.env`.
- Runtime inherited env-name defaults follow conductor host defaults
  (`SYSTEMROOT`, `WINDIR`, `TEMP`, `TMP` on Windows; empty list elsewhere)
  and merge with configured names from the active host platform entry under
  `runtime.inherited_env_vars`.
- Generated managed-tool configs should not redundantly copy those inherited
  names into `tool_configs.<tool>.env_vars`; keep tool-config env vars for
  tool-specific overrides only.
- Generated default dotenv environment-variable lines stay commented (`# ...`)
  so user/shell environment values are picked up unless operators explicitly
  opt into file-based overrides by uncommenting entries.
- `runtime.use_user_tool_cache` defaults to enabled when omitted. When
  enabled, both `mediapm` and the conductor it invokes share one cache root:
  `<os-cache-dir>/mediapm/cache/`. The layout is `cache/store/` for CAS
  payloads plus `cache/tools.jsonc`; additional `*.jsonc` indexes are allowed
  and participate in shared payload-retention decisions. Eviction stays fixed
  at 30 days of inactivity. Conductor standalone uses a separate base
  directory (`<os-cache-dir>/mediapm-conductor/cache/`) with the same flat
  layout.
- `tools.ffmpeg.max_input_slots` and
  `tools.ffmpeg.max_output_slots` default to `64` when omitted and
  bound generated ffmpeg indexed input/output slot fan-out.

## Media workflow pipeline expectations

Online-source pipeline contract:

1. downloader ingest,
2. optional transcode,
3. metadata application (default enabled).

Local-source pipeline contract:

1. import ingest,
2. optional transcode,
3. metadata application (default enabled).

Permanent-transcode policy:

- online defaults to enabled; local defaults to disabled,
- when enabled, pre-transcode source payload is not retained as primary cached
  product,
- transcode result becomes the cache product and is tracked as safety external
  data in lock state for controlled pruning.

## Versioning and migration policy

- Persisted `mediapm.ncl` documents must carry an explicit top-level numeric
  `version` marker.
- Persisted machine-managed `state.ncl` documents must carry an explicit
  top-level numeric `version` marker and use the top-level `state` payload
  field.
- Persisted machine-managed `state.ncl` tracks per-media workflow-step refresh
  state under `state.workflow_step_state.<media_id>.step-<index>` with
  `explicit_config` and optional `impure_timestamp`.
- Keep config wire-version dispatch and migration logic in
  `src/mediapm/src/config/versions/` (`mod.rs` + `vN.rs`).
- Keep unversioned/latest Nickel contract aliases (`validate_document` and
  `envelope_contract`) in `src/mediapm/src/config/versions/mod.ncl`; versioned
  files such as `vN.ncl` should expose only version-suffixed contracts
  (`validate_document_vN`, `envelope_contract_vN`).
- Keep machine-managed state wire-version dispatch and migration logic in
  `src/mediapm/src/lockfile/versions/` (`mod.rs` + `vN.rs`).
- Preserve sequential, explicit migration behavior across schema versions.

## Media schema and managed workflow reconciliation

For `media.<id>` semantics and runtime reconciliation:

- Media entries may define optional `title`, optional `description`,
  optional `workflow_id`,
  optional strict `metadata`, ordered `steps`, and optional `variant_hashes`
  CAS pointers by variant key.
- `mediapm media add` and `mediapm media add-local` should auto-populate
  `title`/`description` from lightweight online/local metadata probes when
  available, with deterministic fallback text when probes fail.
- `mediapm media add` should synthesize minimal default managed steps as
  `yt-dlp -> rsgain -> media-tagger`; `mediapm media add-local` should
  synthesize `import -> rsgain -> media-tagger`.
  Generated step options should stay minimal (only required/non-default
  values), and final output variants should rely on default `save = true`
  unless explicit `save = "full"` is needed.
- Tool requirements may set `ffmpeg_version` on `yt-dlp`, `rsgain`, and
  `media-tagger` (defaulting to inherit/global behavior when omitted).
- Metadata entries must be strict per key:
  - literal form: `<key> = "value"`
  - variant-binding form:
    `<key> = { variant = "<file-variant>", metadata_key = "<json-key>", transform = { pattern = "<regex>", replacement = "<replacement>" }? }`
  - variant-binding metadata must target file variants (not folder captures),
    and runtime extraction expects JSON-object payloads with string values.
  - when `transform` is provided, `pattern` is evaluated with full-match
    semantics against the extracted value and `replacement` supports regex
    capture-group substitution.
- Hierarchy paths may include `${media.id}` and
  `${media.metadata.<key>}` placeholders; config validation and runtime
  resolution must fail fast when referenced metadata keys are missing or
  unresolved.
- Hierarchy uses an ordered node-array schema (`hierarchy = [ { ... } ]`)
  with recursive `children`; legacy flat-map and `"/kind"` forms are
  intentionally unsupported (no backward compatibility).
- Hierarchy node kinds are explicit: `folder` (default), `media`,
  `media_folder`, and `playlist`.
  `media` nodes use required singular `variant`; `media_folder` nodes use
  required plural `variants` and may define `rename_files`.
  hierarchy `id` is optional on all node kinds and must be unique when
  provided. `media_id` is optional on all node kinds; `media` and
  `media_folder` require a non-empty effective `media_id` (direct or
  inherited).
- Hierarchy entries with `kind = "playlist"` emit playlist files and resolve
  members by ordered `ids` entries with optional per-item path-mode
  overrides; playlist item refs accept string shorthand (`"<id>"`) and
  object form (`{ id = "...", path = "relative"|"absolute" }`), where `id`
  always targets hierarchy-node `id` (not `media` map keys); playlist nodes
  must stay file leaves.
- Media-source entries must not define `media.<id>.id` overrides; playlist
  membership is owned by hierarchy-node ids only.
- Example/demo hierarchy should remain Jellyfin-compatible for media files:
  `music videos/<artist> - <title> [<media.id>]/<artist> - <title> [<media.id>](<ext>)`,
  with non-media sidecars grouped under `sidecars/`.
- Local ingest from `mediapm media add-local` is represented as an
  `import` step with `options.kind = "cas_hash"` and
  `options.hash = "blake3:<hex>"`.
- Each step declares `tool`, `input_variants` for non-source-ingest
  transforms (source-ingest tools `yt-dlp` and `import`
  must keep `input_variants` empty), `output_variants` as a
  map (`variant_name -> { save?, save_full? }`) with defaults
  `input_variants` and hierarchy `variants` selectors may use either exact
  strings (`"variant"`) or regex object syntax (`{ regex = "^variant$" }`);
  regex selectors are matched against available variant names and may resolve
  multiple variants for directory targets,
  `save = true`, `save_full = false`; hierarchy file-path variants must be
  file outputs whose latest producer keeps persisted-save semantics
  (`save = true` or `save = "full"`), while hierarchy directory-path variants may remain
  folder outputs with default `save_full = false`; `ffmpeg`, `rsgain`, and
  `media-tagger` output variants may also define optional `extension` to drive
  generated `output_path_<idx>` values; strict
  operation-specific `options`.
- Machine-managed state `managed_files` entries must persist canonical CAS hash
  strings for each materialized file, and workflow reconciliation must ensure
  every managed-file hash is rooted in conductor `external_data` alongside
  managed local-variant and tool-content hashes.
- Low-level list bindings (`option_args`, `leading_args`, `trailing_args`) live
  under the same step `options` map as other operation options.
- Managed media-tool step `options` are value-centric: users should provide
  option values (not raw option-key tokens). Runtime command templates
  translate those values to concrete CLI flags/arguments via conductor
  conditional + unpack syntax; when an option value is empty, runtime
  rendering must omit both the option key and the option value together.
- Managed `sd` rewrite commands must always include an explicit file operand
  (`inputs/input.ffmeta`) so executions stay file-backed and never block on
  stdin reads.
- Option values are scalar strings by default; ordered string-list values are
  only valid for `option_args`, `leading_args`, and `trailing_args`.
- For generated boolean-style option inputs, runtime templates only treat the
  exact string `"true"` as enabled. Any other value (including `"false"`,
  `"1"`, `"yes"`, and `"on"`) is treated as disabled.
- Managed `media-tagger` defaults should keep `strict_identification = "true"`
  unless callers explicitly override that input, and should default
  `write_all_tags = "true"` plus `write_all_images = "true"`.
  Managed tool defaults should also set `cover_art_slot_count` to
  `tools.ffmpeg.max_input_slots - 1` so media-tagger and ffmpeg apply stages
  agree on deterministic attachment-slot fanout.
- `media-tagger` metadata-fetch mode should allow explicit MBID-driven runs
  without `input_content`; fingerprint/AcoustID autodetection still requires
  input media.
- When `media-tagger` needs AcoustID lookup (no explicit recording MBID
  override), missing/empty AcoustID credentials must fail immediately; valid
  key sources are CLI `--acoustid-api-key` or `ACOUSTID_API_KEY`, and
  provided-credential lookup/auth failures are surfaced as runtime errors. For
  `mediapm sync` workflow execution, include `ACOUSTID_API_KEY` in
  `runtime.inherited_env_vars` when relying on environment-based key lookup.
- `yt-dlp` output artifact families (for example subtitles/thumbnails/infojson
  and playlist sidecars) should be exposed via `output_variants`; description
  and infojson bind to file captures while folder families map to
  artifact-capture outputs in generated conductor workflows. When multiple
  output variants are declared on one yt-dlp step, synthesize one shared
  workflow call and merge required sidecar toggles instead of emitting one
  downloader process per output variant.
- output-variant values are object-driven across managed tools: `kind`
  determines default file-vs-folder capture behavior, and optional
  `capture_kind = "file"|"folder"` may override that default per
  variant.
- output-variant kind naming is strict (no legacy aliases): use `primary`
  for main transform outputs; yt-dlp folder-family kinds use plural labels
  (`subtitles`, `thumbnails`, `links`, `chapters`) while
  file-family kinds remain singular (`primary`, `description`, `infojson`,
  `comment`, `archive`, `annotation`, playlist file sidecars).
- yt-dlp output-variant `langs` is an optional capture-filter hint for
  subtitle-family artifacts; download language selection remains owned by step
  `options.sub_langs`.
- hierarchy directory entries may define ordered
  `rename_files = [{ pattern, replacement }, ...]` regex rewrites applied to
  extracted folder file members; file hierarchy targets must keep
  `rename_files` empty.
- Do not document or reintroduce a separate dedicated per-variant
  output-folder configuration model; folder/file behavior is defined by
  `kind` plus optional `capture_kind`.
- Generated yt-dlp variant synthesis must set explicit sidecar toggles per
  variant kind so primary/sandbox variants do not accidentally capture
  unrelated sidecar families.
- Playlist-only output variants must not capture single-item artifacts when
  playlist mode is disabled (`no_playlist = true`); keep playlist/non-playlist
  sidecar capture behavior explicitly gated.
- `yt-dlp` output-variant config objects must not define `format`; format
  selection belongs only in step `options.format`.
- Generated yt-dlp commands should use a deterministic post-edit filename
  marker (`__mediapm__`) before extension so one shared downloader run can
  safely isolate sidecar families without mixing outputs. Managed output
  captures must use regex selectors under `downloads/`; folder-regex captures
  should use capture groups to strip the marker before final ZIP member names
  so user-visible materialized sidecars do not expose the internal marker.
- Managed tool defaults should favor rich metadata capture and preservation:
  - `yt-dlp`: prefer `format = "bestvideo*+bestaudio/best"`, metadata/embed
    toggles enabled, `sub_langs = "all"`, unified subtitle capture enabled
    by default (`write_subs = "true"`, mapped to manual + automatic subtitle
    toggles) while broad translated subtitle pressure should still be reduced
    with precise `options.sub_langs` selectors and optional
    `options.sleep_subtitles`.
    Keep this mitigation anchored to documented upstream incidents in
    `https://github.com/yt-dlp/yt-dlp/issues/13831#issuecomment-3875360390`
    and
    `https://github.com/yt-dlp/yt-dlp/issues/13831#issuecomment-3712613129`:
    broad translated subtitle requests are the highest-risk path for
    `HTTP 429`, focused subtitle requests are usually lower risk, and
    extractor-args
    translation-skip knobs are not a reliable substitute for precise language
    selectors, highest-quality
    single-thumbnail capture by default
    (`write_thumbnail = "true"`, `write_all_thumbnails = "false"`),
    `merge_output_format = "mkv"`, chapter embedding enabled by default
    (`embed_chapters = "true"`, `split_chapters = "false"`), comments
    capture enabled by default (`write_comments = "true"`),
    `clean_info_json = "true"`, and all link sidecar formats enabled by
    default (`write_url_link = "true"`, `write_webloc_link = "true"`,
    `write_desktop_link = "true"`);
    Default managed cache path is `<mediapm_dir>/cache/yt-dlp`;
  - `ffmpeg`: default toward metadata-preserving copy behavior
    (`codec_copy = "true"`, `map_metadata = "0"`,
    `map_chapters = "0"`, `movflags = "+faststart"`)
    while allowing explicit per-step overrides for transcode flows;
  - `rsgain`: keep true-peak normalization defaults enabled with
    tool-level single-track defaults (`album = "false"`,
    `album_mode = "false"`), execute in `custom` mode directly on the
    managed media output, and keep default behavior
    container/stream-preserving (not audio-only). Managed ReplayGain merge
    synthesis should preserve single-track mode by default and only include
    album-family tags when callers opt in explicitly through step options;
  - `media-tagger`: keep strict identification enabled by default while
    populating broad MusicBrainz/Picard-compatible tag aliases and preserving
    existing source metadata unless explicitly overridden. Cover-art behavior
    should select one highest-quality payload per distinct artwork entry
    (prefer CAA original image URL, fallback to best thumbnail), emit
    deterministic slot artifacts for ffmpeg `attached_pic` mapping, and keep
    all compatible kind metadata in `coverart_*` tags. The emitted
    `coverart_*` metadata key family must stay synchronized with Picard
    cover-art metadata usage in
    `https://github.com/metabrainz/picard/blob/master/picard/coverart/image.py`.
    Default managed cache path is `<mediapm_dir>/cache` with shared layout
    `<mediapm_dir>/cache/store/` (CAS payloads) +
    `<mediapm_dir>/cache/media-tagger.jsonc` (metadata index); do not create
    dedicated media-tagger subfolders under `store/`.
- Online sources must be declared in downloader step `options.uri` (not
  top-level media fields).
- Each media entry reconciles to exactly one managed workflow:
  - default workflow id: `mediapm.media.<id>`
  - `workflow_id` may override the default.
- Managed workflow metadata behavior:
  - `name` defaults to `<id>` (without `mediapm.media.` prefix)
  - `description` may mirror `media.<id>.description`
  - identity remains the workflow map key; runtime/cache semantics must not
    depend on optional metadata.
- Variant-flow dependencies must be explicit with `${step_output...}` input
  bindings and matching `depends_on` edges.
- Managed workflow refresh behavior is strict and step-local:
  - refresh when explicit user-facing step config changes, or
  - refresh when mediapm-managed step `impure_timestamp` is missing.
  Unchanged explicit config with a present timestamp must preserve prior
  immutable step tool ids so existing outputs stay reusable across newer tool
  activations until users explicitly change config or clear timestamp state.

## Tool provisioning and catalog expectations

- `mediapm sync` and `mediapm tools sync` provision workspace-local tools under
  `.mediapm/tools/`.
- Tag-update default behavior differs intentionally:
  - `mediapm sync` defaults to skipping remote checks for tag-only selectors.
  - `mediapm tools sync` defaults to checking for updates.
- `mediapm.ncl` `tools.<name>` entries must define `version` or `tag` (or
  both matching); `recheck_seconds` is optional and controls how long release
  metadata cache entries can be reused before remote refresh. When omitted,
  release metadata defaults to one-day cache reuse.
- Immutable tool-id precedence is:
  - `mediapm.tools.<name>+source@git-hash`
  - `mediapm.tools.<name>+source@version`
  - `mediapm.tools.<name>+source@tag`
- Internal `media-tagger` launcher resolution always pins identity/version to
  the currently running `mediapm` package version, even when callers request
  moving selectors like `latest`.
- `conductor::registered_builtin_ids()` returns namespaced immutable ids (for
  example `mediapm.builtin.import@1.0.0`); when constructing
  `ToolKindSpec::Builtin`, map `name` to the process-name suffix
  (`import`/`export`/etc.) rather than copying the full namespaced id.
- Default catalog tracks:
  - `ffmpeg`: GitHub Releases, BtbN preferred on Windows with fallbacks
  - `yt-dlp`: GitHub Releases `latest`
  - `rsgain`: GitHub Releases `latest` ZIP assets
  - `media-tagger`: internal `mediapm` launcher shim that invokes
    `mediapm builtins media-tagger` (Chromaprint + AcoustID + MusicBrainz +
    FFmetadata + FFmpeg)
- Managed tool downloader planning should remain cross-platform (`windows`,
  `linux`, `macos`) even if later import/materialization may be host-filtered.
- Managed executable materialization should emit platform-prefixed
  `content_map` keys (`windows/`, `linux/`, `macos/`) or one shared `./` root
  for platform-identical payloads, and generated command selectors should use
  `${context.os == "<target>" ? ... | ...}` so every selector branch maps to
  one materialized target.
- For GitHub release assets (especially ffmpeg), resolve concrete asset URLs
  from release metadata instead of assuming static
  `releases/latest/download/...` links.
- Keep default `yt-dlp` reconciliation concurrency constrained to one active
  call (`tool_configs.<yt-dlp-tool-id>.max_concurrent_calls = 1`) unless user
  config overrides it.
- Keep default `yt-dlp` conductor retry budget at one outer retry
  (`tool_configs.<yt-dlp-tool-id>.max_retries = 1`) because yt-dlp already
  has internal network retry controls.

Toolsmith reconciliation flow (`mediapm sync` / `mediapm tools sync`):

1. read desired tools from `mediapm.ncl`,
2. query registered tool state,
3. register/promote immutable tool identities for missing/mismatched versions,
4. persist active selection in lock state.

Before finalizing tool registration, keep validation deterministic:

- resolved tool identity must serialize to deterministic CAS-hashable metadata,
- executable validation should include a successful version probe (for example
  `--version`) where applicable.

## Conductor integration boundary

When mediapm invokes conductor, always pass grouped runtime-storage paths
resolved from effective mediapm paths so volatile writes do not fall back to
standalone conductor defaults.

Effective grouped defaults:

- `conductor_dir = <mediapm_dir>`
- `conductor_state_config = <mediapm_dir>/state.conductor.ncl`
- `cas_store_dir = <mediapm_dir>/store`
- `conductor_tmp_dir = <mediapm_dir>/tmp`
- `conductor_schema_dir = <mediapm_dir>/config/conductor`

## Identity, sidecar, and storage invariants

- Canonical identity key is URI (`canonical_uri`), not display path strings.
- Content identity is BLAKE3 with object fan-out under
  `.mediapm/objects/blake3/<2-char>/<rest>`.
- Sidecars are derived from canonical URI digest under
  `.mediapm/media/<media-id>/media.json`.
- Object files are immutable once imported.
- Managed hierarchy outputs committed under resolved `runtime.hierarchy_root_dir`
  must be marked read-only after sync (including copied, linked, or symlinked
  managed paths when applicable).
- Runtime may temporarily clear read-only bits only for managed
  replacement/removal operations.
- Preserve `original.original_variant_hash` semantics.
- Keep `edits` lineage references valid (`from_variant_hash` and
  `to_variant_hash` must exist in `variants`).
- Keep schema version explicit and migrations sequential.
- Machine-managed state `managed_files` provenance stores per-file `media_id` (not source
  URI strings) together with `variant` and `last_synced_unix_millis`.
- Materializer verification enforces NFD-only filenames and rejects reserved
  path characters (`<`, `>`, `:`, `"`, `/`, `\\`, `|`, `?`, `*`).
- Link/write materialization order follows
  `runtime.materialization_preference_order` (must be non-empty and
  duplicate-free); default order is hardlink -> symlink -> reflink -> copy.

## Testing, validation, and docs bar

During development, prefer targeted cargo aliases from `.cargo/config.toml`:

- `cargo test-pkg mediapm`
- `cargo clippy-pkg mediapm`
- `cargo build-pkg mediapm`

Pre-push/full-workspace validation:

- `cargo fmt-check`
- `cargo clippy-all`
- `cargo test-all`

Hard runtime gate for `src/mediapm/**` edits:

- Run `cargo run --package mediapm --example demo_online` after targeted
  tests/lints.
- Inspect generated artifacts under
  `src/mediapm/examples/.artifacts/demo-online/` after the run and verify
  sidecar-family payload correctness (not only file/path presence).
- Confirm the interpolated media root under `music videos/` contains the
  metadata-templated demo filenames
  `${media.metadata.artist} - ${media.metadata.title} [${media.id}].untagged${media.metadata.video_ext}`
  and
  `${media.metadata.artist} - ${media.metadata.title} [${media.id}]${media.metadata.video_ext}`;
  both should keep video+audio streams, while sidecar hierarchy stays under
  `sidecars/` and selected sidecar families are additionally mirrored at
  media root (including regex-selected subtitle folders).
- Keep demo tool dependency examples explicit: `yt-dlp` and `media-tagger`
  inherit `ffmpeg`, `rsgain` inherits both `ffmpeg` and `sd`, while `ffmpeg`
  and `sd` declare no dependencies.
- To reduce provider rate-limit risk (`HTTP 429`), run this gate once per
  validation pass, avoid rapid consecutive reruns, and wait with backoff
  before retrying transient provider failures.
- If the run appears stuck, triage before rerun: confirm active process state
  (`cargo`/`mediapm`/`yt-dlp`/`ffmpeg`), inspect artifact timestamp movement,
  and check stderr for fallback-root messages (`demo-online-fallback-*`) when
  canonical cleanup is locked.
- First-run demo bootstrap can spend several minutes downloading/extracting
  managed tools; be patient and avoid interrupting while progress is still
  moving.
- Use `MEDIAPM_DEMO_ONLINE_TIMEOUT_SECS` to cap long demo runs and treat
  timeout failures as blockers unless reviewer explicitly accepts them.
- Keep `demo_online` timeout/watchdog notices as single-shot plain-text lines
  and avoid periodic heartbeat stderr logging while conductor progress bars
  are active, so progress rows are not duplicated or visually corrupted.
- Treat provider/network failures as blockers unless reviewer explicitly
  accepts the transient failure.
- Keep `demo_online` comment-sidecar validation realistic: do not
  intentionally force zero comments (for example via
  `youtube:max_comments=0`) when validating comments capture flows.
- When a `demo_online` pass does not validate comment sidecars, prefer
  disabling comment extraction explicitly (for example
  `write_comments = "false"`) to reduce provider-throttling timeout risk.

Example policy:

- Examples that depend on external tooling/network must detect test-target
  execution (`cfg!(test)`) and default to config-only mode so automated test
  runs avoid provider/network/tool side effects.
- Keep full-sync behavior for explicit manual runs (`cargo run --example ...`),
  with environment overrides (`MEDIAPM_DEMO_RUN_SYNC`,
  `MEDIAPM_DEMO_ONLINE_RUN_SYNC`) available for forced-mode diagnostics.
- Keep the demo fixture transcode fast: prefer ffmpeg stream-copy
  (`codec_copy = "true"`) into an audio-focused container/extension
  (`.m4a`) rather than demo-time audio re-encoding.

Rust docs quality bar for touched files:

- Add/refresh `//!` module docs and `///` item docs for public and private
  items.
- Document invariants, failure modes, and side effects (not just symbol names).

Internal module-boundary policy for this crate:

- Keep mediapm crate errors centralized in `src/mediapm/src/error.rs` and
  re-export them from `lib.rs`.
- Keep the media tagger implementation only under `builtins/` (do not create
  a second root-level `src/mediapm/src/media_tagger.rs`).
- Keep `config` and `lockfile` as folder modules rooted at
  `config/mod.rs` and `lockfile/mod.rs`.

## Reference instruction files

- `.agents/instructions/mediapm-architecture.instructions.md`
- `.agents/instructions/mediapm-testing-and-docstrings.instructions.md`
- `.agents/instructions/rust-workflow.instructions.md`
