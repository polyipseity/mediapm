---
description: "Use when editing mediapm Rust source under src/. Covers the module-layer architecture, sidecar invariants, planning/execution boundaries, and storage/link behavior expectations."
name: "mediapm Architecture and Invariants"
applyTo: "src/**/*.rs"
---

# mediapm Architecture and Invariants

## Purpose

- Keep code aligned with mediapm's phase-based architecture and explicit state model.
- Preserve determinism and auditability of media state transitions.
- Keep boundaries between planning logic and side effects clear.

## Module layout (source of truth)

- `src/cas/` (Phase 1)
  - identity/hash model
  - CAS async API contracts
  - storage/index/constraint behavior
  - topology visualization rendering/execution helpers
- `src/conductor/` (Phase 2)
  - orchestration state model
  - deterministic instance-key and merge logic
  - workflow execution contracts
- `src/conductor-builtins/*/` (Phase 2 built-ins)
  - versioned built-in tool contracts and runtime implementations such as
    `echo`, `fs`, `import`, `export`, `archive`
- `src/mediapm/` (Phase 3)
  - media-facing API
  - CLI shell and phase composition over conductor + CAS

If you introduce a new file, place it in the phase crate that owns that
concern. Avoid re-introducing flat `src/*.rs` module sprawl at workspace root.

When splitting one Rust module into multiple files under `src/`, prefer
folder-module layout:

- move `foo.rs` to `foo/mod.rs`,
- place sibling module files in `foo/*.rs`,
- place module-local unit tests in `foo/tests.rs` with `#[cfg(test)] mod tests;`.

Avoid `#[path = "..."]` for routine in-crate module/test wiring unless there
is a narrow, documented reason.

## Conceptual layering terms in planning docs

- `PLAN.md` may reference `application`, `configuration`, `domain`,
  `infrastructure`, and `support` as architecture-layer concepts.
- Treat those names as conceptual boundaries unless matching directories are
  explicitly added to this workspace.
- When implementing phase work, keep concrete file placement aligned to the
  existing phase crates listed above.

## Layering rules

- `cas` should remain runtime-agnostic at public API boundaries.
- `conductor` should keep deterministic planning/keying logic explicit and testable.
  Conductor-specific invariants to preserve in `src/conductor/**`:
  - persisted builtin tool entries stay strict (`kind`, `name`, `version` only),
  - `conductor.ncl`, `conductor.machine.ncl`, and the resolved runtime state
    document path (default `.conductor/state.ncl`) must carry explicit
    top-level numeric `version` markers,
  - `conductor.ncl` and `conductor.machine.ncl` may define grouped runtime
    storage path fields only under one `runtime` record
    (`runtime.conductor_dir`, `runtime.state_config`,
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
- `mediapm` should compose phase 1/2 APIs rather than bypassing them.
  For Phase 3 runtime paths, preserve these `mediapm` invariants:
  - default runtime root is `.mediapm/`,
  - `mediapm.ncl` `runtime` may optionally override
    `mediapm_dir`, `conductor_config`, `conductor_machine_config`,
    `conductor_state`, `inherited_env_vars`, `lockfile`, `library_dir`,
    `tmp_dir`, and `use_user_download_cache`,
  - `runtime.inherited_env_vars` is platform-keyed (`windows`, `linux`,
    `macos`, ...) and each platform key maps to an ordered list of
    inherited environment-variable names,
  - defaults for those paths are:
    `mediapm_dir = .mediapm`,
    `conductor_config = mediapm.conductor.ncl`,
    `conductor_machine_config = mediapm.conductor.machine.ncl`,
    `conductor_state = <mediapm_dir>/state.ncl`,
    `lockfile = <mediapm_dir>/lock.jsonc`,
  - persisted `mediapm.ncl` schema keeps explicit top-level numeric
    `version` markers,
  - persisted lockfile schema keeps explicit top-level numeric `version`
    markers,
  - `mediapm.ncl` wire-version dispatch and migrations live under
    `src/mediapm/src/config/versions/` with version-specific wire envelopes in
    `vN.rs`,
  - lockfile wire-version dispatch and migrations live under
    `src/mediapm/src/lockfile/versions/` with version-specific wire envelopes
    in `vN.rs`,
  - default materialized output root is the topmost `mediapm.ncl` directory
    itself (no implicit `library/` folder),
  - when `mediapm` invokes conductor, grouped conductor runtime-storage
    defaults also target effective `mediapm_dir`
    (`conductor_dir = <mediapm_dir>`, `state_config = <mediapm_dir>/state.ncl`,
    `cas_store_dir = <mediapm_dir>/store`),
  - `mediapm` workflow execution must pass grouped runtime-storage paths
    derived from effective phase-3 path resolution so volatile state writes do
    not regress to standalone `.conductor/state.ncl` defaults,
  - relative `runtime.library_dir` resolves relative to the topmost
    `mediapm.ncl` directory,
  - relative `runtime.tmp_dir` resolves relative to effective
    `runtime.mediapm_dir`,
  - `runtime.use_user_download_cache` defaults to enabled when omitted
    and controls one shared user-level global managed-tool download cache with
    fixed layout `tool-cache/store/` + `tool-cache/index.jsonc` and fixed
    30-day eviction,
  - relative `runtime.conductor_config`,
    `runtime.conductor_machine_config`,
    `runtime.conductor_state`, and `runtime.lockfile` resolve
    relative to the topmost `mediapm.ncl` directory,
  - local-source ingest created by `mediapm media add-local` is represented as
    an `import` step (`options.kind = "cas_hash"`, `options.hash =
    "blake3:<hex>"`),
  - media source schema may additionally keep manual payload pointers in
    `variant_hashes` (variant name -> CAS hash),
  - each media source may optionally define explicit `workflow_id` override,
  - media processing uses one ordered `steps` list where each step defines
    `tool` (`yt-dlp`, `import`, `ffmpeg`, `rsgain`, `picard`),
    `input_variants`, `output_variants`, strict tool-specific
    `options`, and optional low-level `input_options` bindings,
  - online source URLs are declared by downloader steps via `options.uri`
    (not by top-level media fields),
  - step low-level list input bindings can be provided through
    `input_options` for `leading_args` and `trailing_args`,
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
    including `leading_args` and `trailing_args` `string_list` inputs,
    source/operation payload input (`source_url` or `input_content`), and
    outputs (`output_content`, `stdout`, `stderr`, `process_code`),
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
  - catalog defaults are: `ffmpeg` preferring GitHub Releases (BtbN on
    Windows) with platform fallbacks, `yt-dlp` from GitHub Releases on
    `latest`, `rsgain` from GitHub Releases on `latest` ZIP assets, and
     `picard` from GitHub Releases on `latest` with headless CLI (`-e "load … ; quit"`),
    `QT_QPA_PLATFORM=offscreen`, and custom `PICARD_INI`,
  - archive-backed managed tool payloads should prefer compact
    directory-form `content_map` entries (trailing `/` keys with ZIP bytes)
    over one-entry-per-file maps when possible,
  - step execution order is the declared `steps` list order,
  - step `options` are tool-specific and unknown keys are rejected.
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

## Validation checklist after Rust edits

**During development:**

Run targeted validation on affected crates:

- `cargo fmt-check` (formatting check on all files)
- `cargo clippy-pkg <crate>` (e.g., `cargo clippy-pkg mediapm`)
- `cargo test-pkg <crate>` (e.g., `cargo test-pkg mediapm`)
- If changes touch `src/mediapm/**`, run
  `cargo run --package mediapm --example demo_online` as the final runtime
  gate after targeted test/lint checks.
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
