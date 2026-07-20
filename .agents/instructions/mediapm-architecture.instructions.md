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
- Prefer incremental updates over full rebuilds; cache keys remain explicit and content-addressed.
- Keep async boundaries runtime-agnostic in domain/application layers (Tokio is default runtime adapter, not a domain-level dependency).
- Use actors for concurrency orchestration with explicit supervision and typed messages.
- Use type-level modeling (newtypes/strong enums/constrained constructors) so invalid states are hard to represent.
- Resolve `Option` at the configuration boundary (serde deserialization): domain types use plain values with serde defaults, not `Option<T>`. Optional semantics are resolved at the very boundary — absent config keys produce a default; downstream code never handles `Option`. This avoids propagating `Option` handling through most of the codebase. All serde defaults are centralized in `src/mediapm/src/config/defaults.rs` — field-level `#[serde(default = "...")]` must reference a `defaults::` function, not inline literals.

See `src/mediapm-conductor/AGENTS.md`, `src/mediapm-conductor-builtins/*/AGENTS.md`, `src/mediapm/AGENTS.md`, and `src/mediapm-cas/AGENTS.md` for per-crate guidance and the root `AGENTS.md`'s "Repository Shape" section for the crate directory listing.

Detailed per-concern specifications (error taxonomy, paths, cache, provider dispatch, preset dispatch, state persistence, document lifecycle, tool-sync coordinator, 3-phase provisioning, content-addressed identity, companion dependencies, generated env output) live in `.agents/instructions/*.instructions.md`. This file covers cross-crate boundaries and invariants only.

## All-platform download principle (tool payloads)

Managed tool payloads are downloaded and CAS-imported for all supported OSes regardless of host platform. Never filter by host OS in the provisioner. See `.agents/instructions/tool-sync-3-phase-provisioning.instructions.md` for the full provisioning pipeline and content-map conventions.

## MediaPM tool mapping to Conductor tools

One logical mediapm tool maps to potentially many conductor tool entries in
`generated_doc.tools`, keyed `{name}@{hash}`.

`state.managed_tools` stores the authoritative provisioning record keyed by
bare `{tool_id}` (without hash suffix). Multiple conductor tool entries can
share the same logical tool name (different hashes), but only one
`managed_tools` entry exists per tool id — always reflecting the latest
provisioned version.

Active tool resolution: the entry in `state.managed_tools[tool_id]` where
`fetch_hash` is non-empty (i.e., has a content map) is the active version.
Inactive entries are conductor tool entries with stale hashes that were
superseded by a newer provision cycle.

`canonical_version` on `ToolRegistryEntry` enables skip-if-up-to-date
provisioning: if the stored `canonical_version` matches the resolved canonical
version from the provider, and `fetch_hash` is non-empty, the 3-phase
provision pipeline is skipped.

See `.agents/instructions/rust-workflow.instructions.md` for module split conventions.

## Conceptual layering terms

- Architecture guidance may reference `application`, `configuration`, `domain`, `infrastructure`, and `support` as architecture-layer concepts.
- Treat those names as conceptual boundaries unless matching directories are explicitly added to this workspace.
- When implementing crate-level work, keep concrete file placement aligned to the existing crates listed above.

## Layering rules

- `cas` should remain runtime-agnostic at public API boundaries. CAS-specific invariants to preserve in `src/mediapm-cas/**`:
  - storage follows an "everything is a diff" logical model where full blobs are treated as diff-from-empty identity in planning/index semantics,
  - diff graph/index relationships stay acyclic and reconstructable,
  - optimizer candidate selection balances delta size against chain depth cost (avoid regressions that optimize bytes while making reconstruction pathologically deep),
  - storage fan-out and hash identity behavior stay deterministic,
  - orchestration runtime constants (RPC timeouts, disk-pressure thresholds) are centralized in `src/mediapm-cas/src/orchestration/config.rs` so clients and actor implementations remain aligned,
  - concurrent batch operations over hash collections use the `batch_concurrent_map!` macro (defined in `src/mediapm-cas/src/api.rs`) for consistent `FuturesUnordered` + index-ordering behavior across `exists_many`, `get_many`, `info_many`, and `get_constraint_many`.
  - `set_constraint_batch` Patch arm uses `continue` (not `return Err`) when `target_hash` is absent from the index — `push_reverse_diff_hints` creates Patch ops for literal-derived hashes never stored in CAS,
  - WAL Delete re-materializes dependent deltas before physical removal: scan ObjectStore for `Delta { base_hash == hash }`, decode VCDIFF, store as Full, then delete; synchronous within Delete processing,
  - FileJournal checkpoint replay uses `>` not `>=`; lock order is tokio mutex first then std mutex; V3+ prefix `CASDLT`, legacy V1/V2 prefix `MDCASD`; active segment → sealed at 64 MiB → cleanup after checkpoint passes,
  - CAS index/filesystem desync risk: `exists()`/`exists_many()` are index-only and cannot see orphaned files; startup orphan scan, filesystem fallback in `exists()`, and bidirectional consistency check are the fix strategy,
  - Rust 1.95+ native `File::lock()`/`File::try_lock()` for CAS filesystem locking (use `std::fs::TryLockError::WouldBlock` for contention; no `fs4::FileExt` import needed for locks; `fs4::available_space()` still needed for disk space queries),
- `conductor` should keep deterministic planning/keying logic explicit and testable. Conductor-specific invariants to preserve in `src/mediapm-conductor/**`:
  - `conductor.ncl` (user intent) and `conductor.generated.ncl` (machine-managed state) remain separate ownership surfaces,
  - persisted builtin tool entries stay strict (`kind`, `name`, `version` only),
  - `conductor.ncl`, `conductor.generated.ncl`, and the resolved runtime state document path (default `.conductor/state.ncl`) must carry explicit top-level numeric `version` markers,
  - `conductor.ncl` and `conductor.generated.ncl` may define grouped runtime storage path fields only under one `runtime` record (`runtime.conductor_dir`, `runtime.conductor_state_config`, `runtime.cas_store_dir`) plus optional platform-keyed inherited host env-name map (`runtime.inherited_env_vars`),
  - runtime inherited env-name defaults are host-specific (`SYSTEMROOT`, `WINDIR`, `TEMP`, `TMP` on Windows; empty list elsewhere) and merge user, machine, and invocation-option values with case-insensitive de-duplication,
  - the resolved runtime state document path (default `.conductor/state.ncl`) is volatile-only and may define only `version`, `impure_timestamps`, and `state_pointer`,
  - orchestration-state snapshots must keep builtin metadata strict (`kind`/`name`/`version` only) and decoding must reject extra builtin metadata fields,
  - human-facing orchestration-state JSON rendering (for example CLI `state` output or demo snapshot artifacts) should use the persisted wire-envelope projection so builtin metadata remains strict and runtime-only optional fields are not emitted for builtins,
  - orchestration-state output persistence values stored per output must be the effective merged policy across duplicate equivalent tool calls (`save`: AND, `force_full`: OR),
  - instance identity excludes tool content-map payload materialization details and excludes merged output-persistence flags,
  - reverse-direction constraints (input as target, output as base) narrow each input's base-candidate set to include the output hash, enabling the optimizer to delta-encode inputs against their outputs,
  - executable `tool_configs.<tool>.content_map` is sandbox-relative and uses trailing `/` or `\\` keys as directory-from-ZIP unpack targets,
  - `./` (or `.\\`) unpacks ZIP content directly at sandbox root,
  - non-trailing `content_map` keys materialize direct file bytes,
  - separate content-map entries must not overwrite the same file path,
  - every hash referenced by `tool_configs.<tool>.content_map` must also be present in top-level `external_data`,
  - absolute/escaping paths are rejected,
  - when cached `${step_output...}` CAS payloads fail integrity checks, conductor may auto-recover only for pure workflows by warning, dropping affected cached instances, deleting corrupt hashes, and retrying once; impure workflows fail without auto-retry.
  - tool-content cache pruning is explicit-call-only (`prune_expired_entries()`); no automatic prune on access — eviction policy is the caller's responsibility,
  - `${env.VAR_NAME}` input-binding segments are expanded at execution time in the step worker; persisted orchestration state only keeps hashes and does not serialize resolved plain content,
  - directory-form tool content-map entries are cached under runtime root's `tools/` directory (derived from `runtime_tmp_dir.parent().join("tools")`), with 24h stale-entry eviction and cached payload reuse,
- `mediapm` should compose CAS and conductor APIs rather than bypassing them. For `mediapm` runtime paths, preserve these invariants:
  - crate-level error taxonomy is centralized in `src/mediapm/src/error.rs` (see `.agents/instructions/error-taxonomy.instructions.md` for variant details).
  - media tagger implementation remains under `src/mediapm/src/builtins/media_tagger.rs` (do not reintroduce a second root-level `src/mediapm/src/media_tagger.rs`),
  - `config` is a folder module rooted at `src/mediapm/src/config/mod.rs`,
  - default runtime root is `.mediapm/`,
  - `mediapm.ncl` `runtime` overrideable fields: see `.agents/instructions/paths-layout.instructions.md` for the full list and resolution rules.
  - `runtime.inherited_env_vars` is platform-keyed (`windows`, `linux`, `macos`, ...) and each platform key maps to an ordered list of inherited environment-variable names,
  - path defaults (`mediapm_dir`, `conductor_config`, etc.): see `.agents/instructions/paths-layout.instructions.md`.
  - persisted `mediapm.ncl` schema keeps explicit top-level numeric `version` markers,
  - persisted machine-managed `state.ncl` schema keeps explicit top-level numeric `version` markers and one top-level `state` payload field,
  - `mediapm.ncl` wire-version dispatch and migrations: see `.agents/instructions/state-persistence.instructions.md`.
  - machine-managed `state.ncl` wire-version dispatch and migrations: see `.agents/instructions/state-persistence.instructions.md`.
  - `mediapm tool add` should load `mediapm.ncl` without cross-field validation so existing inherit dependency selectors do not block bootstrap of missing managed tools,
  - default materialized output root is the topmost `mediapm.ncl` directory itself (no implicit `library/` folder),
  - mediapm-managed materialized outputs are marked read-only after sync commit; runtime may clear read-only only for managed replacement/removal operations; on macOS (and other BSDs), stale managed-path removal must also clear user/system immutable flags (`UF_IMMUTABLE`/`SF_IMMUTABLE`) via `libc::chflags` before chmod-based readonly clearing, or `remove_file()` fails with `EACCES`,
  - when `mediapm` invokes conductor, grouped conductor runtime-storage defaults also target effective `mediapm_dir` (`conductor_dir = <mediapm_dir>`, `conductor_state_config = <mediapm_dir>/state.conductor.ncl`, `cas_store_dir = <mediapm_dir>/store`) with inherited env-name defaults matching conductor host defaults; managed tool-config env-vars should not redundantly restate those inherited names,
  - `mediapm` workflow execution must pass grouped runtime-storage paths derived from effective runtime path resolution so volatile state writes do not regress to standalone `.conductor/state.ncl` defaults,
  - relative `runtime.hierarchy_root_dir` resolves relative to the topmost `mediapm.ncl` directory,
  - managed-tool downloads use a shared user-level cache (default `<os-cache-dir>/mediapm/cache/` with 30-day eviction). This cache domain is distinct from workspace tool-content materialization. See `.agents/instructions/cache-and-http.instructions.md` for the full three-tier cache spec.
  - ffmpeg slot limits default to 16 input / 4 output; see `.agents/instructions/preset-dispatch.instructions.md` for ffmpeg spec builder defaults.
  - runtime dotenv loading follows generated-env-output conventions. See `.agents/instructions/tool-sync-generated-env-output.instructions.md` for the `.env.generated` format and quoting rules.
  - relative `runtime.conductor_config`, `runtime.conductor_generated_config`, `runtime.conductor_state_config`, and `runtime.media_state_config` resolve relative to the topmost `mediapm.ncl` directory (see `.agents/instructions/paths-layout.instructions.md`).
  - local-source ingest created by `mediapm media add-local` is represented as an `import` step (`options.kind = "cas_hash"`, `options.hash = "blake3:<hex>"`),
  - media source schema may additionally keep manual payload pointers in `variant_hashes` (variant name -> CAS hash),
  - media source entries may include optional human-readable `title` and `description`,
  - each media source may optionally define explicit `workflow_id` override,
  - media source `metadata` is strict when present: each key maps to either a literal string value or to one `{ variant = "<file-variant>", metadata_key = "<json-key>", transform = { pattern = "<regex>", replacement = "<replacement>" }? }` binding; metadata bindings must target file variants (not folder captures), and hierarchy placeholders `${media.id}` and `${media.metadata.<key>}` must fail fast when referenced keys are undefined or unresolved,
  - hierarchy uses an ordered node-array schema (`hierarchy = [ { ... } ]`) with recursive `children`; legacy flat-map and `"/kind"` forms are unsupported (no backward compatibility),
  - hierarchy kinds are explicit: `folder` (default), `media`, `media_folder`, and `playlist`; `media` uses singular `variant`, `media_folder` uses plural `variants` and may define `rename_files`, and playlist `ids` resolve by ordered id entries, accept string shorthand and object refs (`{ id, path }`),
  - hierarchy entries with `kind = "playlist"` emit playlist files, may define ordered `ids` with optional per-item path-mode overrides, and must remain file-leaf nodes,
  - hierarchy node `id` suffix convention: tagged media nodes omit a dedicated suffix (bare media id), while untagged media variants carry the `.untagged` suffix to distinguish them from tagged peers. This ensures tagged nodes sort before untagged variants in natural ordering and makes the variant role explicit in the id string. Sidecar and container nodes use their own descriptive suffixes (`.media_folder`, etc.).
  - demo/example hierarchy layouts should remain Jellyfin-compatible for media files: `music videos/<artist> - <title> [<media.id>]/<artist> - <title> [<media.id>](<ext>)`, with non-media sidecars grouped under `sidecars/`,
  - media processing uses one ordered `steps` list where each step defines `tool` (`yt-dlp`, `import`, `ffmpeg`, `rsgain`, `media-tagger`), `input_variants` for non-source-ingest transforms (source-ingest tools `yt-dlp` and `import` keep `input_variants` empty), `output_variants` as a map keyed by output variant name with optional per-variant policy overrides (`save`, `save_full`) where defaults are `save = true` and `save_full = false`; hierarchy file-path variants must resolve to file outputs with latest-producer persisted-save semantics (`save = true` or `save = "full"`), while hierarchy directory-path variants may use folder outputs with default `save_full = false`; strict tool-specific `options`,
  - machine-managed state `managed_files` entries must carry canonical CAS hash strings for each materialized file; workflow reconciliation must root all managed-file hashes in top-level conductor `external_data`,
  - managed media-tool step `options` must stay value-centric: values represent option payloads (not raw CLI option-name tokens); runtime command templates expand values into CLI arguments via conductor conditional + unpack syntax and must omit both option key and option value when the configured value is empty,
  - option values are scalar strings by default; ordered string-list values are only valid for `option_args`, `leading_args`, and `trailing_args`,
  - for generated boolean-style media option inputs, runtime templates must treat only the exact value `"true"` as enabled and treat every other value as disabled,
  - when `media-tagger` runs on the AcoustID lookup path (no explicit recording MBID override), missing/empty AcoustID credentials must fail immediately; key sources remain CLI `--acoustid-api-key` or `ACOUSTID_API_KEY`, and provided-credential lookup/auth failures surface as runtime errors; for `mediapm sync` workflow execution, include `ACOUSTID_API_KEY` in `runtime.inherited_env_vars` when relying on environment-key lookup,
  - `yt-dlp` non-primary artifact families (for example subtitles, thumbnails, descriptions, infojson, comments, links, chapter splits, and playlist sidecars) are exposed via `output_variants`; description/infojson should bind to file captures while folder families map to artifact-capture outputs in generated tool/workflow specs; aggregated downloader synthesis must keep one shared yt-dlp call for multiple requested output families while isolating artifact bundles through regex folder captures, and any internal filename post-edit marker used for disambiguation must be removed via regex-capture rename semantics before user-visible outputs,
  - yt-dlp output-variant synthesis must apply explicit sidecar toggles per variant kind so primary/sandbox variants do not capture unrelated sidecar families,
  - playlist-only output variants must keep explicit gating so single-item runs with `no_playlist = true` cannot capture playlist sidecar artifacts,
  - hierarchy directory entries may define ordered `rename_files = [{ pattern, replacement }, ...]` regex rewrites that apply to extracted folder file members; file hierarchy targets must keep `rename_files` empty,
  - hierarchy flattening validation allows same-path entries when they have different `rename_files` rules, since `rename_files` produce distinct final output filenames at materialization time; the materializer uses isolated staging directories per entry for multi-entry deduplication,
  - See `.agents/instructions/preset-dispatch.instructions.md` for managed-tool default settings.
  - output-variant values are object-driven across managed tools: `kind` defines default file-vs-folder capture behavior and optional `capture_kind` (`file`/`folder`) may override that default,
  - yt-dlp output-variant `langs` is only a capture-filter hint for subtitle-family artifacts; downloader language selection remains step-option owned via `options.sub_langs`,
  - do not document or rely on a separate dedicated per-variant output-folder configuration model,
  - online source URLs are declared by downloader steps via `options.uri` (not by top-level media fields),
  - step low-level list input bindings use `options.option_args`, `options.leading_args`, and `options.trailing_args`,
  - default tool reconciliation sets `yt-dlp` to one active concurrent call (`tool_configs.<yt-dlp-tool-id>.max_concurrent_calls = 1`) unless users explicitly choose a different value,
  - each media source must reconcile to exactly one managed workflow id (`mediapm.media.<media-id>` by default, overrideable with `workflow_id`),
  - managed workflow metadata may include optional informational `name` and `description`; `name` defaults to `<media-id>` (without the `mediapm.media.` prefix) and `description` may mirror `media.<id>.description`,
  - workflow identity remains the workflow map key and runtime semantics/cache keys must not depend on workflow `name`/`description`,
  - transform variant dependencies must be expressed with explicit `${step_output...}` input bindings and matching `depends_on` edges,
  - managed executable media tool specs must expose comprehensive IO contracts, including list inputs (`leading_args`, `trailing_args`, and option `option_args`), scalar option/source inputs (`source_url` or `input_content`), and outputs (`output_content`, `stdout`, `stderr`, `process_code`),
  - `mediapm sync` and `mediapm tools sync` must keep tool provisioning workspace-local under `.mediapm/tools/`,
  - default tag-update policy differs by command (`sync` skips remote checks for tag-only selectors unless overridden; `tools sync` checks by default),
  - tool source/release-track defaults are catalog-driven (source is not required in `mediapm.ncl`),
  - each `mediapm.ncl` tool entry must declare `version` or `tag` (or both),
  - when both `version` and `tag` are provided they must resolve to the same release selector,
  - immutable tool ids include source identifiers and resolve with precedence `mediapm.tools.<name>+source@git-hash` -> `mediapm.tools.<name>+source@version` -> `mediapm.tools.<name>+source@tag`; resolution must fail if none are available,
  - `conductor::registered_builtin_ids()` exposes namespaced immutable ids; when building `ToolKindSpec::Builtin`, map `name` to the process-name suffix (`import`, `export`, ...) instead of copying full namespaced ids,
  - catalog defaults (per-tool source preferences): see `.agents/instructions/provider-dispatch.instructions.md` for per-tool source descriptors.
  - downloader-plan resolution must remain cross-platform; see `.agents/instructions/tool-sync-3-phase-provisioning.instructions.md`.
  - tool preset downloads are never host-platform-only. See `.agents/instructions/tool-sync-3-phase-provisioning.instructions.md` for the all-platform provisioning invariant.
  - for GitHub release assets, resolve concrete URLs from release metadata; see `.agents/instructions/provider-dispatch.instructions.md` for URL resolution patterns.
  - archive-backed managed tool payloads should prefer compact directory-form `content_map` entries (trailing `/` keys with ZIP bytes) over one-entry-per-file maps when possible,
  - managed executable payloads keep all-platform `content_map` coverage; see `.agents/instructions/tool-sync-3-phase-provisioning.instructions.md` for content-map conventions and command selector patterns.
  - managed runtime defaults and executable paths resolve via `<tools_dir>/<tool-id>/payload/<os>/...`; see `.agents/instructions/tool-sync-3-phase-provisioning.instructions.md`.
  - managed-tool dependencies are split into same-step companion (inline payload + fold selector) and cross-step (separate payload + ids). See `.agents/instructions/tool-sync-companion-dependencies.instructions.md` for the full contract.
  - step execution order is the declared `steps` list order,
  - step `options` are tool-specific and unknown keys are rejected,
  - materialization uses stage -> verify -> commit semantics with staging under effective `.mediapm/tmp` and atomic commit into library roots,
  - materializer path validation always enforces NFD-only filenames,
  - hierarchy nodes may additionally specify `sanitize_names` configured via boolean, `"inherit"`, or per-character object mapping:
    - `false`: skip reserved-character replacement entirely (reserved chars are still rejected by subsequent validation),
    - `"inherit"` (default): inherit from parent hierarchy node; the root seed is `true`,
    - `true`: replace reserved characters (`<`, `>`, `:`, `"`, `|`, `?`, `*`) with runtime-default mapping (underscore `_`),
    - object (`{ "<": "_", "|": "-", ... }`): apply per-character custom mapping merged over runtime defaults,
    - NFD normalization is always enforced regardless of this setting,
    - resolved `rename_files` replacement strings are sanitized with the same effective replacement map,
  - materializer link/write order follows `runtime.materialization_preference_order` (must be non-empty and duplicate-free); default order remains hardlink -> symlink -> reflink -> copy,
  - online/local source pipelines keep explicit ingest -> optional transcode -> metadata-application sequencing, and permanent-transcode safety external data remains machine-state-tracked/pruneable.
- Built-ins should stay narrowly scoped and version-addressable.
- Builtin runtime behavior must remain inside `src/mediapm-conductor-builtins/*` crates (not inline in `src/mediapm-conductor`).
- Each builtin crate should expose both a library API and an independently runnable binary target.
- Each builtin crate must use a uniform input contract:
  - CLI arguments use standard Rust flags/options and all values remain strings,
  - API arguments are `BTreeMap<String, String>` with optional raw payload bytes for content-oriented operations. Builtins may optionally define one default CLI option key so one value can be passed without spelling the key, but explicit keyed input must remain supported and must map to the same API key. Builtin execution must fail fast on undeclared keys, missing required keys, and invalid argument combinations; do not silently drop unknown values. For builtins whose successful non-error result is pure, successful payloads may be deterministic bytes or `BTreeMap<String, String>`. Impure builtins may instead primarily communicate success through side effects. The only allowed CLI/API difference is input ergonomics (string flag transport vs map transport). CLI failures may use ordinary Rust error types; do not wrap failures inside string-only success objects.
- Builtin crate `version` values should be explicit per crate in each builtin crate `Cargo.toml` (do not inherit workspace package version).
- Prefer one-directional dependencies:
  - `cas -> conductor -> mediapm` composition,
  - with built-ins consumed by conductor runtime contracts,
  - and no circular crate dependencies.
- `src/mediapm` should not depend directly on individual `src/mediapm-conductor-builtins/*` crates; use conductor exports/APIs for builtin identity or behavior.

## Identity and storage invariants

- Canonical identity key is URI (`canonical_uri`), not path display strings.
- Content identity is BLAKE3 hash and object fan-out path under `.mediapm/objects/blake3/<0..2>/<2..4>/<4..>`.
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

## Cache policy

- All caches in CAS and downstream should be TTL-based, not bounded by entry count. This avoids memory-pressure tuning loops and keeps behavior predictable under varying DB sizes.
- CAS-specific cache details (integrity cache TTL, verified-content in-memory cache) are documented in `src/mediapm-cas/AGENTS.md`.

See `.agents/instructions/mediapm-testing-and-docstrings.instructions.md` for Rustdoc/docstring depth requirements.

## pulsebar rendering contract

`pulsebar` has two rendering modes that are not interchangeable:

- `ProgressBar::new()` — **standalone mode** (`managed = false`). Calls to `set_message`, `set_position`, and `advance` internally call `try_render()`, which writes a carriage-return update directly. `try_render()` is throttled to one paint every 50 ms — so if three mutations fire in rapid succession (within the same millisecond) only the first one triggers a terminal write. The remaining state updates are silently skipped. Consequently, a standalone bar set up before a long async `await` may show stale state for the entire await duration (e.g., stuck at `0/13` for 5 minutes while yt-dlp runs), because no mutation occurs during the await and the throttle is never reset. `finish_success`/`finish_error` call `render_final()` which bypasses the throttle and always writes — so the final state is always correct.

- `MultiProgress::new()` + `multi.add_bar()` — **managed mode**. A background OS thread fires every 50 ms and renders all managed bars unconditionally, regardless of whether any mutations have occurred since the last cycle. This is the only mode that produces live-updating progress during long async awaits (e.g., subprocess execution, network downloads).

**Rule:** Whenever progress bars must remain visually live during long-running async operations (tool execution, downloads, actor message waiting), always use `MultiProgress`. Never use standalone `ProgressBar::new()` for workflow-level or download-level progress that spans blocking awaits.

**Specific invariants for conductor workflow progress:**

- `execute_workflows` in `src/mediapm-conductor/src/orchestration/coordinator.rs` must create a single `MultiProgress` at the top of the function and all workflow bars must be allocated via `multi.add_bar(...)`.
- A settle delay (`tokio::time::sleep`) of at least one render interval (75 ms) must be awaited before returning from `execute_workflows` so the background thread can flush final `finish_success`/`finish_error` states before the `MultiProgress` drops and the thread stops.
- The same settle delay must precede each early-return error path that calls `workflow_progress.finish_error(...)`.
- The same settle delay must also precede retry-loop continuation after `workflow_progress.finish_error("retrying")` so the previous retry status is rendered before the replacement retry bar is allocated.

**Terminal-width contract:**

- All progress-bar messages must always fit within the terminal width.
- `workflow_level_progress_message()` detects terminal width via `terminal_size()` (default 80 cols when unavailable) and allocates space for the workflow display name before computing available room for the step preview.
- The step preview string degrades gracefully when it cannot fit:
  - single step: truncate with `...` suffix,
  - two steps: fit both or truncate the second,
  - 3+ steps: try `"first, second, +N more"` with decreasing N, then degrade to two-step display, then finally truncate the single first step.
- All truncation is character-count-based (not byte-based) for correct Unicode handling.

## CAS integrity verification

CAS integrity verification ensures stored objects have not been corrupted on read. See `src/mediapm-cas/AGENTS.md` for the full specification (trust model, verify-on-read strategies, TTL cache, `verify_time` field, configuration, and schema migration).

**Progress message format:**

- Conductor workflow bars use the format: `<name> · <N/total> · <step-id>` for single-step levels, and `<name> · <N-M/total> · <step-id>, <step-id2>[, +K more]` for multi-step levels.
- Use `N/total` (not `N-N/total`) when first == last (single-step level).
- Do not add prefixes like "running " or wrap step ids in `step '...'` quotes — they are redundant noise.

## Tool sync flow

The `mediapm tool sync` pipeline is documented across focused instruction files, one per concern:

| Concern                           | Instruction file                                       | Target files                                    |
| --------------------------------- | ------------------------------------------------------ | ----------------------------------------------- |
| CLI entry & service orchestration | `src/mediapm/AGENTS.md`                                | `main.rs`, `service.rs`                         |
| Tool requirements                 | `tool-requirements.instructions.md`                    | `config/mod.rs`, `config/source_types.rs`       |
| State persistence                 | `state-persistence.instructions.md`                    | `config/mod.rs`, `config/versions/`             |
| Reconciliation coordinator        | `tool-sync-coordinator.instructions.md`                | `conductor_bridge/sync/mod.rs`                  |
| 3-phase provisioning pipeline     | `tool-sync-3-phase-provisioning.instructions.md`       | `conductor_bridge/sync/provision.rs`            |
| Content-addressed tool identity   | `tool-sync-content-addressed-identity.instructions.md` | `conductor_bridge/sync/mod.rs`, `documents.rs`  |
| Companion dependency binding      | `tool-sync-companion-dependencies.instructions.md`     | `conductor_bridge/sync/tool_config.rs`          |
| Generated env output              | `tool-sync-generated-env-output.instructions.md`       | `conductor_bridge/sync/tool_config.rs`          |
| Document I/O and lifecycle        | `document-io-lifecycle.instructions.md`                | `conductor_bridge/documents.rs`, `lifecycle.rs` |
| Paths layout                      | `paths-layout.instructions.md`                         | `paths.rs`                                      |
| Cache architecture & HTTP client  | `cache-and-http.instructions.md`                       | `tools/downloader.rs`, `http_client.rs`         |
| Error taxonomy                    | `error-taxonomy.instructions.md`                       | `error.rs`                                      |
| Provider dispatch                 | `provider-dispatch.instructions.md`                    | `tools/provider/`                               |
| Preset dispatch                   | `preset-dispatch.instructions.md`                      | `tools/preset/`, `tools/workflows/`             |
