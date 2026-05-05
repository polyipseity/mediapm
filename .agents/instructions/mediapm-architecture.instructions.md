---
description: "Use when editing mediapm Rust source under src/. Covers the module-layer architecture, sidecar invariants, planning/execution boundaries, and storage/link behavior expectations."
name: "mediapm Architecture and Invariants"
applyTo: "src/**/*.rs"
---

# mediapm Architecture and Invariants

## Purpose

- Keep code aligned with mediapm's crate-oriented architecture and explicit state model.
- Preserve determinism and auditability of media state transitions.
- Keep boundaries between planning logic and side effects clear.

## Cross-crate engineering principles

- Keep planning/diffing/key-derivation logic pure and deterministic.
- Keep side effects (filesystem/process/network) in explicit boundary modules.
- Prefer incremental updates over full rebuilds; cache keys remain explicit and
  content-addressed.
- Keep async boundaries runtime-agnostic in domain/application layers (Tokio is
  default runtime adapter, not a domain-level dependency).
- Use actors for concurrency orchestration with explicit supervision and typed
  messages.
- Use type-level modeling (newtypes/strong enums/constrained constructors) so
  invalid states are hard to represent.

## Module layout (source of truth)

- `src/cas/` (CAS)
  - identity/hash model
  - CAS async API contracts
  - storage/index/constraint behavior
  - topology visualization rendering/execution helpers
- `src/conductor/` (Conductor)
  - orchestration state model
  - deterministic instance-key and merge logic
  - workflow execution contracts
- `src/conductor-builtins/*/` (conductor built-ins)
  - versioned built-in tool contracts and runtime implementations such as
    `echo`, `fs`, `import`, `export`, `archive`
- `src/mediapm/` (mediapm application crate)
  - media-facing API
  - CLI shell and composition over conductor + CAS

If you introduce a new file, place it in the crate that owns that
concern. Avoid re-introducing flat `src/*.rs` module sprawl at workspace root.

When splitting one Rust module into multiple files under `src/`, prefer
folder-module layout:

- move `foo.rs` to `foo/mod.rs`,
- place sibling module files in `foo/*.rs`,
- place module-local unit tests in `foo/tests.rs` with `#[cfg(test)] mod tests;`.

Avoid `#[path = "..."]` for routine in-crate module/test wiring unless there
is a narrow, documented reason.

## Conceptual layering terms

- Architecture guidance may reference `application`, `configuration`,
  `domain`, `infrastructure`, and `support` as architecture-layer concepts.
- Treat those names as conceptual boundaries unless matching directories are
  explicitly added to this workspace.
- When implementing crate-level work, keep concrete file placement aligned to
  the existing crates listed above.

## Layering rules

- `cas` should remain runtime-agnostic at public API boundaries.
  CAS-specific invariants to preserve in `src/cas/**`:
  - storage follows an "everything is a diff" logical model where full blobs
    are treated as diff-from-empty identity in planning/index semantics,
  - diff graph/index relationships stay acyclic and reconstructable,
  - optimizer candidate selection balances delta size against chain depth cost
    (avoid regressions that optimize bytes while making reconstruction
    pathologically deep),
  - storage fan-out and hash identity behavior stay deterministic.
- `conductor` should keep deterministic planning/keying logic explicit and testable.
  Conductor-specific invariants to preserve in `src/conductor/**`:
  - `conductor.ncl` (user intent) and `conductor.machine.ncl`
    (machine-managed state) remain separate ownership surfaces,
  - persisted builtin tool entries stay strict (`kind`, `name`, `version` only),
  - `conductor.ncl`, `conductor.machine.ncl`, and the resolved runtime state
    document path (default `.conductor/state.ncl`) must carry explicit
    top-level numeric `version` markers,
  - `conductor.ncl` and `conductor.machine.ncl` may define grouped runtime
    storage path fields only under one `runtime` record
    (`runtime.conductor_dir`, `runtime.conductor_state_config`,
    `runtime.cas_store_dir`) plus optional platform-keyed inherited host
    env-name map (`runtime.inherited_env_vars`),
  - runtime inherited env-name defaults are host-specific (`SYSTEMROOT`,
    `WINDIR`, `TEMP`, `TMP` on Windows; empty list elsewhere) and merge user,
    machine, and invocation-option values with case-insensitive de-duplication,
  - the resolved runtime state document path (default
    `.conductor/state.ncl`) is volatile-only and may define only
    `version`, `impure_timestamps`, and `state_pointer`,
  - orchestration-state snapshots must keep builtin metadata strict
    (`kind`/`name`/`version` only) and decoding must reject extra builtin
    metadata fields,
  - human-facing orchestration-state JSON rendering (for example CLI `state`
    output or demo snapshot artifacts) should use the persisted wire-envelope
    projection so builtin metadata remains strict and runtime-only optional
    fields are not emitted for builtins,
  - orchestration-state output persistence values stored per output must be the
    effective merged policy across duplicate equivalent tool calls
    (`save`: AND, `force_full`: OR),
  - instance identity excludes tool content-map payload materialization details
    and excludes merged output-persistence flags,
  - reverse-diff optimization hints should continue to prefer frequently-read
    outputs as fast retrieval roots when safe to do so,
  - executable `tool_configs.<tool>.content_map` is sandbox-relative and uses
    trailing `/` or `\\` keys as directory-from-ZIP unpack targets,
  - `./` (or `.\\`) unpacks ZIP content directly at sandbox root,
  - non-trailing `content_map` keys materialize direct file bytes,
  - separate content-map entries must not overwrite the same file path,
  - every hash referenced by `tool_configs.<tool>.content_map` must also be
    present in top-level `external_data`,
  - absolute/escaping paths are rejected,
  - when cached `${step_output...}` CAS payloads fail integrity checks,
    conductor may auto-recover only for pure workflows by warning, dropping
    affected cached instances, deleting corrupt hashes, and retrying once;
    impure workflows fail without auto-retry.
- `mediapm` should compose CAS and conductor APIs rather than bypassing them.
  For `mediapm` runtime paths, preserve these invariants:
  - crate-level error taxonomy remains centralized in
    `src/mediapm/src/error.rs` and is re-exported via `lib.rs`,
  - media tagger implementation remains under
    `src/mediapm/src/builtins/media_tagger.rs` (do not reintroduce a second
    root-level `src/mediapm/src/media_tagger.rs`),
  - `config` and `lockfile` stay folder modules rooted at
    `src/mediapm/src/config/mod.rs` and `src/mediapm/src/lockfile/mod.rs`,
  - default runtime root is `.mediapm/`,
  - `mediapm.ncl` `runtime` may optionally override
    `mediapm_dir`, `conductor_config`, `conductor_machine_config`,
    `conductor_state_config`, `inherited_env_vars`, `media_state_config`,
    `hierarchy_root_dir`, `mediapm_tmp_dir`, `conductor_tmp_dir`,
    `conductor_schema_dir`, `env_file`, `mediapm_schema_dir`, and
    `use_user_tool_cache`,
  - `runtime.inherited_env_vars` is platform-keyed (`windows`, `linux`,
    `macos`, ...) and each platform key maps to an ordered list of
    inherited environment-variable names,
  - defaults for those paths are:
    `mediapm_dir = .mediapm`,
    `conductor_config = mediapm.conductor.ncl`,
    `conductor_machine_config = mediapm.conductor.machine.ncl`,
    `conductor_state_config = <mediapm_dir>/state.conductor.ncl`,
    `media_state_config = <mediapm_dir>/state.ncl`,
  - persisted `mediapm.ncl` schema keeps explicit top-level numeric
    `version` markers,
  - persisted machine-managed `state.ncl` schema keeps explicit top-level
    numeric `version` markers and one top-level `state` payload field,
  - `mediapm.ncl` wire-version dispatch and migrations live under
    `src/mediapm/src/config/versions/` with version-specific wire envelopes in
    `vN.rs`,
  - machine-managed state wire-version dispatch and migrations live under
    `src/mediapm/src/lockfile/versions/` with version-specific wire envelopes
    in `vN.rs`,
  - default materialized output root is the topmost `mediapm.ncl` directory
    itself (no implicit `library/` folder),
  - mediapm-managed materialized outputs are marked read-only after sync
    commit; runtime may clear read-only only for managed
    replacement/removal operations,
  - when `mediapm` invokes conductor, grouped conductor runtime-storage
    defaults also target effective `mediapm_dir`
    (`conductor_dir = <mediapm_dir>`,
    `conductor_state_config = <mediapm_dir>/state.conductor.ncl`,
    `cas_store_dir = <mediapm_dir>/store`) with inherited env-name defaults
    matching conductor host defaults; managed tool-config env-vars should not
    redundantly restate those inherited names,
  - `mediapm` workflow execution must pass grouped runtime-storage paths
    derived from effective runtime path resolution so volatile state writes do
    not regress to standalone `.conductor/state.ncl` defaults,
  - relative `runtime.hierarchy_root_dir` resolves relative to the topmost
    `mediapm.ncl` directory,
  - relative `runtime.mediapm_tmp_dir` resolves relative to effective
    `runtime.mediapm_dir`,
  - `runtime.use_user_tool_cache` defaults to enabled when omitted
    and controls a shared user-level managed-tool download cache; when invoked
    through `mediapm`, the cache root is `<os-cache-dir>/mediapm/cache/` with
    fixed layout `cache/store/` (CAS payloads), default metadata index
    `cache/tools.jsonc`, optional additional indexes `cache/*.jsonc`, and
    fixed 30-day eviction; conductor standalone uses
    `<os-cache-dir>/mediapm-conductor/cache/` with the same layout,
  - `tools.ffmpeg.max_input_slots` and
    `tools.ffmpeg.max_output_slots` default to `64` when omitted and
    bound generated ffmpeg indexed input/output slot fan-out,
  - runtime dotenv loading uses effective `runtime.env_file`
    (default `<mediapm_dir>/.env`), keeps a colocated `.gitignore` containing
    only `/.env`, and generated default dotenv environment-variable lines stay
    commented (`# ...`) so ambient shell/user environment variables are not
    shadowed by placeholder file values,
  - relative `runtime.conductor_config`,
    `runtime.conductor_machine_config`,
    `runtime.conductor_state_config`, and `runtime.media_state_config` resolve
    relative to the topmost `mediapm.ncl` directory,
  - local-source ingest created by `mediapm media add-local` is represented as
    an `import` step (`options.kind = "cas_hash"`, `options.hash =
"blake3:<hex>"`),
  - media source schema may additionally keep manual payload pointers in
    `variant_hashes` (variant name -> CAS hash),
  - media source entries may include optional human-readable `title` and
    `description`,
  - each media source may optionally define explicit `workflow_id` override,
  - media source `metadata` is strict when present:
    each key maps to either a literal string value or to one
    `{ variant = "<file-variant>", metadata_key = "<json-key>", transform = { pattern = "<regex>", replacement = "<replacement>" }? }` binding;
    metadata bindings must target file variants (not folder captures), and
    hierarchy placeholders `${media.id}` and `${media.metadata.<key>}` must fail fast when
    referenced keys are undefined or unresolved,
  - hierarchy uses an ordered node-array schema (`hierarchy = [ { ... } ]`)
    with recursive `children`; legacy flat-map and `"/kind"` forms are
    unsupported (no backward compatibility),
  - hierarchy kinds are explicit: `folder` (default), `media`,
    `media_folder`, and `playlist`; `media` uses singular `variant`,
    `media_folder` uses plural `variants` and may define `rename_files`,
    and playlist `ids` resolve by ordered id entries, accept string shorthand
    and object refs (`{ id, path }`),
  - hierarchy entries with `kind = "playlist"` emit playlist files, may define
    ordered `ids` with optional per-item path-mode overrides, and must remain
    file-leaf nodes,
  - demo/example hierarchy layouts should remain Jellyfin-compatible for media
    files:
    `music videos/<artist> - <title> [<media.id>]/<artist> - <title> [<media.id>](<ext>)`,
    with non-media sidecars grouped under `sidecars/`,
  - media processing uses one ordered `steps` list where each step defines
    `tool` (`yt-dlp`, `import`, `ffmpeg`, `rsgain`,
    `media-tagger`), `input_variants` for non-source-ingest transforms
    (source-ingest tools `yt-dlp` and `import` keep
    `input_variants` empty), `output_variants` as a map keyed
    by output variant name with optional
    per-variant policy overrides (`save`, `save_full`) where defaults are
    `save = true` and `save_full = false`; hierarchy file-path variants must
    resolve to file outputs with latest-producer persisted-save semantics
    (`save = true` or `save = "full"`), while hierarchy directory-path variants may use folder
    outputs with default `save_full = false`; strict tool-specific `options`,
  - machine-managed state `managed_files` entries must carry canonical CAS
    hash strings for each materialized file; workflow reconciliation must root
    all managed-file hashes in top-level conductor `external_data`,
  - managed media-tool step `options` must stay value-centric:
    values represent option payloads (not raw CLI option-name tokens);
    runtime command templates expand values into CLI arguments via conductor
    conditional + unpack syntax and must omit both option key and option
    value when the configured value is empty,
  - option values are scalar strings by default; ordered string-list values
    are only valid for `option_args`, `leading_args`, and `trailing_args`,
  - for generated boolean-style media option inputs, runtime templates must
    treat only the exact value `"true"` as enabled and treat every other value
    as disabled,
  - managed `media-tagger` defaults should keep
    `strict_identification = "true"` unless callers explicitly override it,
    and should keep `cover_art_slot_count = tools.ffmpeg.max_input_slots - 1`
    so metadata and apply stages stay slot-compatible,
  - when `media-tagger` runs on the AcoustID lookup path (no explicit
    recording MBID override), missing/empty AcoustID credentials must fail
    immediately; key sources remain CLI `--acoustid-api-key` or
    `ACOUSTID_API_KEY`, and provided-credential lookup/auth failures surface as
    runtime errors; for `mediapm sync` workflow execution, include
    `ACOUSTID_API_KEY` in `runtime.inherited_env_vars` when relying on
    environment-key lookup,
  - `yt-dlp` non-primary artifact families (for example subtitles,
    thumbnails, descriptions, infojson, comments, links, chapter splits, and
    playlist sidecars) are exposed via `output_variants`; description/infojson
    should bind to file captures while folder families map to artifact-capture
    outputs in generated tool/workflow specs; aggregated downloader synthesis
    must keep one shared yt-dlp call for multiple requested output families
    while isolating artifact bundles through regex folder captures, and any
    internal filename post-edit marker used for disambiguation must be removed
    via regex-capture rename semantics before user-visible outputs,
  - yt-dlp output-variant synthesis must apply explicit sidecar toggles per
    variant kind so primary/sandbox variants do not capture unrelated sidecar
    families,
  - playlist-only output variants must keep explicit gating so single-item
    runs with `no_playlist = true` cannot capture playlist sidecar artifacts,
  - hierarchy directory entries may define ordered
    `rename_files = [{ pattern, replacement }, ...]` regex rewrites that
    apply to extracted folder file members; file hierarchy targets must keep
    `rename_files` empty,
  - managed media tool defaults should stay quality- and metadata-preserving:
    `yt-dlp` defaults to `bestvideo*+bestaudio/best` plus enabled metadata,
    `sub_langs = "all"`, unified subtitle writes enabled by default
    (`write_subs = "true"`, mapped to manual + automatic subtitle toggles)
    while broad translated subtitle pressure should still be reduced using
    precise `options.sub_langs` selectors and optional
    `options.sleep_subtitles`.
    Keep this mitigation anchored to documented upstream incidents in
    `https://github.com/yt-dlp/yt-dlp/issues/13831#issuecomment-3875360390`
    and
    `https://github.com/yt-dlp/yt-dlp/issues/13831#issuecomment-3712613129`:
    broad translated subtitle requests are the highest-risk path for
    `HTTP 429`, focused subtitle requests are usually lower risk, and
    extractor-args
    translation-skip knobs are not a reliable substitute for precise language
    selectors, and
    highest-quality single-thumbnail capture (`write_thumbnail = "true"`,
    `write_all_thumbnails = "false"`), `ffmpeg` defaults to
    metadata-preserving copy behavior, `rsgain` defaults to single-track
    true-peak normalization with direct `custom`-mode execution while keeping
    container/stream layout by default (not audio-only output), and
    `media-tagger` defaults should maximize broad MusicBrainz/Picard-compatible
    metadata population while preserving existing source metadata unless
    overridden by media-tagger values; cover-art selection should keep one
    highest-quality payload per distinct CAA artwork entry (original image
    preferred, thumbnail fallback allowed), emit deterministic attachment slot
    artifacts for ffmpeg `attached_pic` mapping, and keep emitted
    `coverart_*` metadata keys synchronized with Picard cover-art behavior
    documented
    in `https://github.com/metabrainz/picard/blob/master/picard/coverart/image.py`,
  - output-variant values are object-driven across managed tools: `kind`
    defines default file-vs-folder capture behavior and optional
    `capture_kind` (`file`/`folder`) may override that default,
  - yt-dlp output-variant `langs` is only a capture-filter hint for
    subtitle-family artifacts; downloader language selection remains
    step-option owned via `options.sub_langs`,
  - do not document or rely on a separate dedicated per-variant
    output-folder configuration model,
  - online source URLs are declared by downloader steps via `options.uri`
    (not by top-level media fields),
  - step low-level list input bindings use `options.option_args`,
    `options.leading_args`, and `options.trailing_args`,
  - default tool reconciliation sets `yt-dlp` to one active concurrent call
    (`tool_configs.<yt-dlp-tool-id>.max_concurrent_calls = 1`) unless users
    explicitly choose a different value,
  - each media source must reconcile to exactly one managed workflow id
    (`mediapm.media.<media-id>` by default, overrideable with `workflow_id`),
  - managed workflow metadata may include optional informational `name` and
    `description`;
    `name` defaults to `<media-id>` (without the `mediapm.media.` prefix)
    and `description` may mirror `media.<id>.description`,
  - workflow identity remains the workflow map key and runtime semantics/cache
    keys must not depend on workflow `name`/`description`,
  - transform variant dependencies must be expressed with explicit
    `${step_output...}` input bindings and matching `depends_on` edges,
  - managed executable media tool specs must expose comprehensive IO contracts,
    including list inputs (`leading_args`, `trailing_args`, and option
    `option_args`), scalar option/source inputs (`source_url` or
    `input_content`),
    and outputs (`output_content`, `stdout`, `stderr`, `process_code`),
  - `mediapm sync` and `mediapm tools sync` must keep tool provisioning
    workspace-local under `.mediapm/tools/`,
  - default tag-update policy differs by command (`sync` skips remote checks
    for tag-only selectors unless overridden; `tools sync` checks by default),
  - tool source/release-track defaults are catalog-driven (source is not
    required in `mediapm.ncl`),
  - each `mediapm.ncl` tool entry must declare `version` or `tag` (or both),
  - when both `version` and `tag` are provided they must resolve to the same
    release selector,
  - immutable tool ids include source identifiers and resolve with precedence
    `mediapm.tools.<name>+source@git-hash` ->
    `mediapm.tools.<name>+source@version` ->
    `mediapm.tools.<name>+source@tag`; resolution must fail if none are
    available,
  - `conductor::registered_builtin_ids()` exposes namespaced immutable ids;
    when building `ToolKindSpec::Builtin`, map `name` to the process-name
    suffix (`import`, `export`, ...) instead of copying full namespaced ids,
  - catalog defaults are: `ffmpeg` preferring GitHub Releases (BtbN on
    Windows) with platform fallbacks, `yt-dlp` from GitHub Releases on
    `latest`, `rsgain` from GitHub Releases on `latest` ZIP assets, and
    `media-tagger` from the built-in internal launcher
    (`mediapm builtins media-tagger`) using Chromaprint + AcoustID +
    MusicBrainz + FFmetadata + FFmpeg,
  - downloader-plan resolution should remain cross-platform (`windows`,
    `linux`, `macos`) even if later import/materialization is host-filtered,
  - for GitHub release assets (notably ffmpeg), resolve concrete asset URLs
    from release metadata instead of assuming static
    `releases/latest/download/...` paths,
  - archive-backed managed tool payloads should prefer compact
    directory-form `content_map` entries (trailing `/` keys with ZIP bytes)
    over one-entry-per-file maps when possible,
  - managed executable payload materialization should keep all-platform
    `content_map` coverage (`windows/`, `linux/`, `macos/`, or shared `./`
    root when payloads are platform-identical), and generated command
    selectors should use `${context.os == "<target>" ? ... | ...}` branches
    that all map to one materialized target,
  - step execution order is the declared `steps` list order,
  - step `options` are tool-specific and unknown keys are rejected,
  - materialization uses stage -> verify -> commit semantics with staging under
    effective `.mediapm/tmp` and atomic commit into library roots,
  - materializer path validation enforces NFD-only filenames and rejects
    reserved characters (`<`, `>`, `:`, `"`, `/`, `\\`, `|`, `?`, `*`),
  - materializer link/write order follows
    `runtime.materialization_preference_order` (must be non-empty and
    duplicate-free); default order remains hardlink -> symlink -> reflink ->
    copy,
  - online/local source pipelines keep explicit ingest -> optional transcode ->
    metadata-application sequencing, and permanent-transcode safety external
    data remains machine-state-tracked/pruneable.
- Built-ins should stay narrowly scoped and version-addressable.
- Builtin runtime behavior must remain inside `src/conductor-builtins/*`
  crates (not inline in `src/conductor`).
- Each builtin crate should expose both a library API and an independently
  runnable binary target.
- Each builtin crate must use a uniform input contract:
  - CLI arguments use standard Rust flags/options and all values remain strings,
  - API arguments are `BTreeMap<String, String>` with optional raw payload
    bytes for content-oriented operations.
    Builtins may optionally define one default CLI option key so one value can be
    passed without spelling the key, but explicit keyed input must remain
    supported and must map to the same API key.
    Builtin execution must fail fast on undeclared keys, missing required keys,
    and invalid argument combinations; do not silently drop unknown values.
    For builtins whose successful non-error result is pure, successful payloads
    may be deterministic bytes or `BTreeMap<String, String>`. Impure builtins
    may instead primarily communicate success through side effects. The only
    allowed CLI/API difference is input ergonomics (string flag transport vs map
    transport).
    CLI failures may use ordinary Rust error types; do not wrap failures inside
    string-only success objects.
- Builtin crate `version` values should be explicit per crate in each builtin
  crate `Cargo.toml` (do not inherit workspace package version).
- Prefer one-directional dependencies:
  - `cas -> conductor -> mediapm` composition,
  - with built-ins consumed by conductor runtime contracts,
  - and no circular crate dependencies.
- `src/mediapm` should not depend directly on individual
  `src/conductor-builtins/*` crates; use conductor exports/APIs for builtin
  identity or behavior.

## Identity and storage invariants

- Canonical identity key is URI (`canonical_uri`), not path display strings.
- Content identity is BLAKE3 hash and object fan-out path under `.mediapm/objects/blake3/<2-char>/<rest>`.
- Sidecar paths are derived from canonical URI digest under `.mediapm/media/<media-id>/media.json`.
- CAS object files are immutable once imported and persisted as read-only by default.

## Sidecar schema and history expectations

- Preserve `original.original_variant_hash` semantics (initial variant reference).
- Keep `edits` lineage references valid (`from_variant_hash` and `to_variant_hash` exist in `variants`).
- Keep schema version explicit (`schema_version`) and migrations sequential.
- Record migration provenance for each applied schema hop.

## Determinism and safety expectations

- Sort and serialize JSON deterministically for stable diffs.
- Use atomic write flow for sidecars and object writes (temp file + sync + rename).
- Keep `plan` output stable for identical inputs.
- Keep `sync` idempotent for unchanged state.

## Link materialization expectations

- Respect configured method order and deterministic fallback reasons.
- Keep behavior explicit for symlink/hardlink/copy capabilities.
- Preserve no-op behavior when existing link already matches desired target.

## Documentation requirements for Rust code

When you add or change public APIs in `src/`:

- Add module-level `//!` docs describing purpose and boundaries.
- Add `///` docs for public structs/enums/functions and key public fields.
- Explain invariants and side effects, not just what types are called.
- Prefer newcomer-readable docs over shorthand internal jargon.

## pulsebar rendering contract

`pulsebar` has two rendering modes that are not interchangeable:

- `ProgressBar::new()` — **standalone mode** (`managed = false`). Calls to
  `set_message`, `set_position`, and `advance` internally call `try_render()`,
  which writes a carriage-return update directly. `try_render()` is throttled
  to one paint every 50 ms — so if three mutations fire in rapid succession
  (within the same millisecond) only the first one triggers a terminal write.
  The remaining state updates are silently skipped. Consequently, a standalone
  bar set up before a long async `await` may show stale state for the entire
  await duration (e.g., stuck at `0/13` for 5 minutes while yt-dlp runs),
  because no mutation occurs during the await and the throttle is never reset.
  `finish_success`/`finish_error` call `render_final()` which bypasses the
  throttle and always writes — so the final state is always correct.

- `MultiProgress::new()` + `multi.add_bar()` — **managed mode**. A background
  OS thread fires every 50 ms and renders all managed bars unconditionally,
  regardless of whether any mutations have occurred since the last cycle. This
  is the only mode that produces live-updating progress during long async awaits
  (e.g., subprocess execution, network downloads).

**Rule:** Whenever progress bars must remain visually live during long-running
async operations (tool execution, downloads, actor message waiting), always use
`MultiProgress`. Never use standalone `ProgressBar::new()` for workflow-level
or download-level progress that spans blocking awaits.

**Specific invariants for conductor workflow progress:**

- `execute_workflows` in `src/conductor/src/orchestration/coordinator.rs` must
  create a single `MultiProgress` at the top of the function and all workflow
  bars must be allocated via `multi.add_bar(...)`.
- A settle delay (`tokio::time::sleep`) of at least one render interval (75 ms)
  must be awaited before returning from `execute_workflows` so the background
  thread can flush final `finish_success`/`finish_error` states before the
  `MultiProgress` drops and the thread stops.
- The same settle delay must precede each early-return error path that calls
  `workflow_progress.finish_error(...)`.
- The same settle delay must also precede retry-loop continuation after
  `workflow_progress.finish_error("retrying")` so the previous retry status is
  rendered before the replacement retry bar is allocated.

**Progress message format:**

- Conductor workflow bars use the format: `<name> · <N/total> · <step-id>` for
  single-step levels, and `<name> · <N-M/total> · <step-id>, <step-id2>[, +K more]`
  for multi-step levels.
- Use `N/total` (not `N-N/total`) when first == last (single-step level).
- Do not add prefixes like "running " or wrap step ids in `step '...'` quotes —
  they are redundant noise.

## Validation checklist after Rust edits

**During development:**

Run targeted validation on affected crates:

- `cargo fmt-check` (formatting check on all files)
- `cargo clippy-pkg <crate>` (e.g., `cargo clippy-pkg mediapm`)
- `cargo test-pkg <crate>` (e.g., `cargo test-pkg mediapm`)
- If changes touch `src/mediapm/**`, run
  `cargo run --package mediapm --example mediapm_demo_online` as the final runtime
  gate after targeted test/lint checks.
  After the run, inspect generated artifacts under
  `src/mediapm/examples/.artifacts/demo-online/` and verify sidecar-family
  payload correctness (not only path existence).
  Confirm the interpolated media root under `music videos/` contains the
  metadata-templated demo filenames
  `${media.metadata.artist} - ${media.metadata.title} [${media.id}].untagged${media.metadata.video_ext}`
  and
  `${media.metadata.artist} - ${media.metadata.title} [${media.id}]${media.metadata.video_ext}`,
  both preserving video+audio streams while sidecar hierarchy stays under
  `sidecars/` and selected sidecar families are additionally mirrored at
  media root.
  Run this gate with rate-limit discipline: at most one run per validation
  pass, no rapid retry loops, and cool-down backoff before retrying transient
  provider (`HTTP 429`) failures.
  Treat this as a hard gate: do not replace failures with placeholder or
  skip-success behavior; report transient external-provider failures as
  blockers until run success or explicit reviewer acceptance.

**Before submitting (pre-push):**

Run full workspace validation:

- `cargo fmt-check`
- `cargo clippy-all`
- `cargo test-all`

See `.cargo/config.toml` for all targeted aliases.

If you intentionally change behavior, update tests and docs in the same change.
