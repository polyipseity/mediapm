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
  `conductor_state`, `inherited_env_vars`, `lockfile`, `env_file`,
  `library_dir`, `tmp_dir`, and `use_user_download_cache`.
- `runtime.inherited_env_vars` is platform-keyed (`windows`, `linux`,
  `macos`, ...) where each value is an ordered list of environment-variable
  names. Runtime reads only the active host platform entry.
- Default runtime values:
  - `mediapm_dir = .mediapm`
  - `conductor_config = mediapm.conductor.ncl`
  - `conductor_machine_config = mediapm.conductor.machine.ncl`
  - `conductor_state = <mediapm_dir>/state.ncl`
  - `lockfile = <mediapm_dir>/lock.jsonc`
  - `env_file = <mediapm_dir>/.env`
- Materialized output root defaults to the directory containing the topmost
  `mediapm.ncl` (no implicit `library/` directory).
- Relative `runtime.library_dir` resolves relative to the topmost
  `mediapm.ncl` directory.
- Relative `runtime.tmp_dir` resolves relative to effective
  `runtime.mediapm_dir`.
- Relative `runtime.conductor_config`, `runtime.conductor_machine_config`,
  `runtime.conductor_state`, and `runtime.lockfile` resolve relative to the
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
- `runtime.use_user_download_cache` defaults to enabled when omitted and uses
  shared user cache layout `tool-cache/store/` + `tool-cache/index.jsonc`
  with fixed 30-day eviction.
- `tools.ffmpeg.max_input_slots` and
  `tools.ffmpeg.max_output_slots` default to `64` when omitted and
  bound generated ffmpeg indexed input/output slot fan-out.

## Media workflow pipeline expectations

Online-source pipeline contract:

1. downloader ingest,
2. optional transcode,
3. metadata application (default enabled).

Local-source pipeline contract:

1. import/import-once ingest,
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
- Persisted lockfile documents must carry an explicit top-level numeric
  `version` marker.
- Keep config wire-version dispatch and migration logic in
  `src/mediapm/src/config/versions/` (`mod.rs` + `vN.rs`).
- Keep unversioned/latest Nickel contract aliases (`validate_document` and
  `envelope_contract`) in `src/mediapm/src/config/versions/mod.ncl`; versioned
  files such as `vN.ncl` should expose only version-suffixed contracts
  (`validate_document_vN`, `envelope_contract_vN`).
- Keep lockfile wire-version dispatch and migration logic in
  `src/mediapm/src/lockfile/versions/` (`mod.rs` + `vN.rs`).
- Preserve sequential, explicit migration behavior across schema versions.

## Media schema and managed workflow reconciliation

For `media.<id>` semantics and runtime reconciliation:

- Media entries may define optional `description`, optional `workflow_id`,
  optional strict `metadata`, ordered `steps`, and optional `variant_hashes`
  CAS pointers by variant key.
- Metadata entries must be strict per key:
  - literal form: `<key> = "value"`
  - variant-binding form:
    `<key> = { variant = "<file-variant>", metadata_key = "<json-key>" }`
  - variant-binding metadata must target file variants (not folder captures),
    and runtime extraction expects JSON-object payloads with string values.
- Hierarchy paths may include `${media.metadata.<key>}` placeholders; config
  validation and runtime resolution must fail fast when referenced metadata
  keys are missing or unresolved.
- Local ingest from `mediapm media add-local` is represented as an
  `import-once` step with `options.kind = "cas_hash"` and
  `options.hash = "blake3:<hex>"`.
- Each step declares `tool`, `input_variants` for non-source-ingest
  transforms (source-ingest tools `yt-dlp`, `import`, and `import-once`
  must keep `input_variants` empty), `output_variants` as a
  map (`variant_name -> { save?, save_full? }`) with defaults
  `save = true`, `save_full = false`; hierarchy file-path variants must be
  file outputs whose latest producer keeps `save = true` and
  `save_full = true`, while hierarchy directory-path variants may remain
  folder outputs with default `save_full = false`; strict
  operation-specific `options`.
- Low-level list bindings (`option_args`, `leading_args`, `trailing_args`) live
  under the same step `options` map as other operation options.
- Managed media-tool step `options` are value-centric: users should provide
  option values (not raw option-key tokens). Runtime command templates
  translate those values to concrete CLI flags/arguments via conductor
  conditional + unpack syntax; when an option value is empty, runtime
  rendering must omit both the option key and the option value together.
- Option values are scalar strings by default; ordered string-list values are
  only valid for `option_args`, `leading_args`, and `trailing_args`.
- For generated boolean-style option inputs, runtime templates only treat the
  exact string `"true"` as enabled. Any other value (including `"false"`,
  `"1"`, `"yes"`, and `"on"`) is treated as disabled.
- Managed `media-tagger` defaults should keep `strict_identification = "true"`
  unless callers explicitly override that input.
- When `media-tagger` needs AcoustID lookup (no explicit recording MBID
  override), missing/empty AcoustID credentials must fail immediately; valid
  key sources are CLI `--acoustid-api-key` or `ACOUSTID_API_KEY`, and
  provided-credential lookup/auth failures are surfaced as runtime errors. For
  `mediapm sync` workflow execution, include `ACOUSTID_API_KEY` in
  `runtime.inherited_env_vars` when relying on environment-based key lookup.
- `yt-dlp` output artifact families (for example subtitles/thumbnails/infojson
  and playlist sidecars) should be exposed via `output_variants`; description
  and infojson bind to file captures while folder families map to
  artifact-capture outputs in generated conductor workflows.
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

## Tool provisioning and catalog expectations

- `mediapm sync` and `mediapm tools sync` provision workspace-local tools under
  `.mediapm/tools/`.
- Tag-update default behavior differs intentionally:
  - `mediapm sync` defaults to skipping remote checks for tag-only selectors.
  - `mediapm tools sync` defaults to checking for updates.
- `mediapm.ncl` `tools.<name>` entries must define `version` or `tag` (or
  both matching); `recheck_seconds` is optional and controls how long release
  metadata cache entries can be reused before remote refresh.
- Immutable tool-id precedence is:
  - `mediapm.tools.<name>+source@git-hash`
  - `mediapm.tools.<name>+source@version`
  - `mediapm.tools.<name>+source@tag`
- Internal `media-tagger` launcher resolution always pins identity/version to
  the currently running `mediapm` package version, even when callers request
  moving selectors like `latest`.
- Default catalog tracks:
  - `ffmpeg`: GitHub Releases, BtbN preferred on Windows with fallbacks
  - `yt-dlp`: GitHub Releases `latest`
  - `rsgain`: GitHub Releases `latest` ZIP assets
  - `media-tagger`: internal `mediapm` launcher shim that invokes
    `mediapm builtins media-tagger` (Chromaprint + AcoustID + MusicBrainz +
    FFmetadata + FFmpeg)
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
- `state_config = <mediapm_dir>/state.ncl`
- `cas_store_dir = <mediapm_dir>/store`

## Identity, sidecar, and storage invariants

- Canonical identity key is URI (`canonical_uri`), not display path strings.
- Content identity is BLAKE3 with object fan-out under
  `.mediapm/objects/blake3/<2-char>/<rest>`.
- Sidecars are derived from canonical URI digest under
  `.mediapm/media/<media-id>/media.json`.
- Object files are immutable once imported.
- Managed hierarchy outputs committed under resolved `runtime.library_dir`
  must be marked read-only after sync (including copied, linked, or symlinked
  managed paths when applicable).
- Runtime may temporarily clear read-only bits only for managed
  replacement/removal operations.
- Preserve `original.original_variant_hash` semantics.
- Keep `edits` lineage references valid (`from_variant_hash` and
  `to_variant_hash` must exist in `variants`).
- Keep schema version explicit and migrations sequential.
- Lockfile `managed_files` provenance stores per-file `media_id` (not source
  URI strings) together with `variant` and `last_synced_unix_millis`.
- Materializer verification enforces NFD-only filenames and rejects reserved
  path characters (`<`, `>`, `:`, `"`, `/`, `\\`, `|`, `?`, `*`).
- Link/write fallback ordering remains deterministic: hardlink -> symlink ->
  reflink -> copy.

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
- Treat provider/network failures as blockers unless reviewer explicitly
  accepts the transient failure.

Example policy:

- Examples requiring external tooling/network should stay compile-only in cargo
  tests (`[[example]] ... test = false`) with explicit rustdoc notes.
- Keep `examples/demo.rs` locally runnable during automated tests by using
  local ingest (`import-once` + bundled fixture asset) and allowing tests to
  force configuration-only mode via `MEDIAPM_DEMO_RUN_SYNC=false`.

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
