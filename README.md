# mediapm

`mediapm` is now organized as a **Rust workspace of phase-focused crates**.

The current implementation establishes compile-ready contracts and scaffolding
for the three major phases defined in `PLAN.md`:

- Phase 1 CAS in `src/cas/`
- Phase 2 Conductor in `src/conductor/`
- Phase 2 built-ins in `src/conductor-builtins/*/`
- Phase 3 mediapm facade/CLI in `src/mediapm/`

## Workspace layout

- `src/cas/` — content identity types, constraints, and async CAS API contract
- `src/conductor/` — orchestration state model, persistence merge semantics,
  and conductor API contract
- `src/conductor-builtins/fs/` — `fs` builtin runtime (filesystem staging)
- `src/conductor-builtins/echo/` — builtin echo runtime + standalone runner
- `src/conductor-builtins/import/` — impure source-ingest builtin (`file`/`folder`/`fetch` kinds)
- `src/conductor-builtins/export/` — impure filesystem materialization builtin (`file`/`folder` kinds)
- `src/conductor-builtins/archive/` — pure archive transform builtin (ZIP-only content transforms)
- `src/mediapm/` — phase-3 media API + CLI scaffold composed over phase 1/2
  (`mediapm-cas` + `mediapm-conductor`; builtins are reached via conductor)
- `scripts/cargo-bin/` — helper binary used by repo tooling

## Status

- Workspace split and inter-crate wiring are in place.
- Public APIs are documented and covered by baseline tests.
- Runtime behavior is intentionally minimal scaffolding for incremental phase
  implementation.

## Commands

### Validation

**For development (recommended for speed):**

Use targeted aliases from `.cargo/config.toml` to validate only affected crates:

- `cargo test-pkg <crate>` — run tests for a single crate (e.g., `cargo test-pkg mediapm`)
- `cargo clippy-pkg <crate>` — lint a single crate (e.g., `cargo clippy-pkg mediapm-cas`)
- `cargo build-pkg <crate>` — build a single crate

**Before submitting (pre-push):**

Run full workspace validation:

- `cargo fmt-check` — check formatting on all Rust files
- `cargo clippy-all` — lint entire workspace
- `cargo test-all` — test entire workspace

These workspace-wide commands are intentionally slow and best suited for pre-push gates.

Integration tests across phase crates use one shared harness shape:

- top-level `tests/tests.rs` entrypoint,
- grouped modules under `tests/e2e/`, `tests/int/`, and `tests/prop/`.

CAS topology-visualization integration tests live in
`src/cas/tests/int/cas_visualize.rs`.

Run the phase-3 CLI:

- `cargo run -p mediapm -- sync`
- `cargo run -p mediapm -- sync --check-tag-updates`
- `cargo run -p mediapm -- tools sync`
- `cargo run -p mediapm -- tools sync --no-check-tag-updates`
- `cargo run -p mediapm -- tools list`
- `cargo run -p mediapm -- global path`
- `cargo run -p mediapm -- global tool-cache status`
- `cargo run -p mediapm -- global tool-cache prune`
- `cargo run -p mediapm -- media add https://example.com/video.mkv`
- `cargo run -p mediapm -- media add-local ./path/to/local/file.mkv`

Tag-update default policy:

- `mediapm sync` defaults to **not** checking remote updates for tag-only tool
  selectors (for example `tag = "latest"`)
- `mediapm tools sync` defaults to checking remote updates for tag-only
  selectors
- both commands expose `--check-tag-updates` / `--no-check-tag-updates`
  overrides

Optional phase-3 path overrides can be supplied per command:

- `--mediapm-dir <path>`
- `--conductor-config <path>`
- `--conductor-machine-config <path>`
- `--conductor-state <path>`
- `--lockfile <path>`

CLI overrides take precedence over `mediapm.ncl` `runtime` values.

Run phase-3 examples:

- `cargo run -p mediapm --example bootstrap_defaults`
- `cargo run -p mediapm --example demo`
- `cargo run -p mediapm --example demo_online`

Progress rendering notes:

- workflow/tool progress uses `pulsebar` defaults,
- progress labels are intentionally compact (task name only),
- step counters and percentage are displayed by pulsebar itself.

`demo_online` is intentionally **compile-only** in automated test/CI flows
(`test = false` in `src/mediapm/Cargo.toml`) because it depends on external
tool distribution endpoints and third-party media/network availability.

`demo_online` declares one managed media workflow over
`https://www.youtube.com/watch?v=dQw4w9WgXcQ` with a downloader/transcode
sequence (`yt-dlp -> ffmpeg -> ffmpeg -> rsgain -> media-tagger`), runs full `mediapm sync`
(`MediaPmService::sync_library_with_tag_update_checks`) to provision tools and
execute the pipeline, then validates managed-tool registration,
managed-workflow shape, and materialized outputs under one metadata-resolved
hierarchy root `demo/Rickroll Demo/dQw4w9WgXcQ/`: transcoded
`rickroll-144p.mp4`, normalized/tagged `rickroll-144p-tagged.mp4`, plus
full downloader sidecar families (subtitles, auto subtitles, thumbnails,
description, infojson, comments, links, chapter files, and playlist sidecars).
The example also verifies that the transcoded output starts with an MP4
container signature (`ftyp`) so the emitted video variant is real MP4 content.

The online demo writes artifacts under
`src/mediapm/examples/.artifacts/demo-online/` and uses that directory directly
as the example workspace root (no extra nested `workspace/` folder).
On Windows, if that canonical directory is temporarily locked (for example by
an external process holding a transient sharing lock), the demo creates a
unique sibling fallback workspace directory named
`demo-online-fallback-<pid>-<timestamp>` and continues execution.

The persistent phase-3 demo writes artifacts under
`src/mediapm/examples/.artifacts/demo/` and uses that `demo/` directory
directly as the example workspace root.

The persistent phase-3 demo ingests the bundled binary fixture
`src/mediapm/examples/assets/sample-av.mp4` by importing it into CAS and then
configuring the source step as `import-once` (`kind = "cas_hash"`,
`hash = "blake3:..."`). This keeps demo source ingest fully local and removes
the old local-HTTP-fixture dependency for source setup.

`cargo run -p mediapm --example demo` still defaults to full sync execution.
Automated tests run the real demo entrypoint in configuration-only mode by
setting `MEDIAPM_DEMO_RUN_SYNC=false`, so the example itself is executed during
`mediapm` test runs without depending on network/tool-download availability.

`mediapm` runtime defaults:

- runtime root (`mediapm_dir`): `.mediapm`
- conductor user config (`conductor_config`): `mediapm.conductor.ncl`
- conductor machine config (`conductor_machine_config`):
  `mediapm.conductor.machine.ncl`
- conductor volatile state (`conductor_state`): `<mediapm_dir>/state.ncl`
- inherited host env names (`inherited_env_vars`):
  platform-keyed object (`windows`, `linux`, `macos`, ...) where each value is
  an ordered list of inherited environment-variable names for that platform;
  runtime merges only the active host platform entry with host defaults
  (`SYSTEMROOT`, `WINDIR`, `TEMP`, and `TMP` on Windows; empty on other
  platforms)
- lockfile (`lockfile`): `<mediapm_dir>/lock.jsonc`
- materialized output root (`library_dir`): top-level `mediapm.ncl` directory
- staging directory (`tmp_dir`): `.mediapm/tmp`
- shared user-level tool download cache toggle (`use_user_download_cache`):
  enabled by default (`true` when omitted)
- ffmpeg generated input-slot limit (`tools.ffmpeg.max_input_slots`):
  `64`
- ffmpeg generated output-slot limit (`tools.ffmpeg.max_output_slots`):
  `64`

Runtime dotenv bootstrap behavior:

- `mediapm` creates `<mediapm_dir>/.env` when missing and loads it on sync/tool
  operations,
- generated default environment-variable lines are intentionally commented
  (`# ...`) so shell/user-level environment values remain visible by default,
- users can opt in to file-based values by uncommenting the specific lines.

When `mediapm` composes conductor, it writes conductor runtime storage defaults
into `mediapm.conductor.machine.ncl` as:

- `conductor_dir = <mediapm_dir>`
- `state_config = <mediapm_dir>/state.ncl`
- `cas_store_dir = <mediapm_dir>/store`
- `inherited_env_vars = { <host-platform> = <host default list> }`

`mediapm sync` also passes these grouped runtime paths directly to conductor
workflow execution, so volatile state persists at the resolved
`conductor_state` path (default `<mediapm_dir>/state.ncl`) instead of the
standalone conductor fallback `.conductor/state.ncl`. `runtime.inherited_env_vars`
values from `mediapm.ncl` are merged from the active host platform entry and
forwarded to conductor run options. Those inherited names are intentionally not
duplicated into generated `tool_configs.<tool>.env_vars`.

Relative `runtime.library_dir` values in `mediapm.ncl` resolve
relative to the outermost `mediapm.ncl` directory. Relative
`runtime.tmp_dir` values resolve relative to
`runtime.mediapm_dir` (or default `.mediapm/`). Relative
`runtime.conductor_config`,
`runtime.conductor_machine_config`,
`runtime.conductor_state`, and `runtime.lockfile` values
resolve relative to the outermost `mediapm.ncl` directory.
`runtime.use_user_download_cache` controls whether managed-tool payload
downloads and release-metadata responses use a shared global user cache across
all local `mediapm` workspaces.
When enabled (default), cache files are stored in one user cache root
(`%APPDATA%/mediapm/tool-cache` on Windows,
`$XDG_DATA_HOME/mediapm/tool-cache` or `$HOME/.local/share/mediapm/tool-cache`
on Linux, and `$HOME/Library/Application Support/mediapm/tool-cache` on macOS).
The cache layout is fixed as:

- `tool-cache/store/` (CAS payload storage)
- `tool-cache/index.jsonc` (logical-key metadata index)

Cache rows are evicted automatically after 30 days of inactivity.

Media source schema highlights in `mediapm.ncl`:

- each `media.<id>` can include optional `description` and optional strict
  `metadata` object
- `metadata` keys support exactly two forms:
  - literal: `<key> = "value"`
  - variant binding:
    `<key> = { variant = "<file-variant>", metadata_key = "<json-key>" }`
  Metadata bindings must target file variants (not folder captures), and
  runtime expects JSON-object payloads with string values at `metadata_key`.
- hierarchy paths may interpolate metadata through
  `${media.metadata.<key>}` placeholders; unknown/missing keys fail fast.
- each `media.<id>` may optionally override managed workflow id via
  `workflow_id`; when omitted, default is `mediapm.media.<id>`
- default runtime policy limits `yt-dlp` to one active call by setting
  `tool_configs.<yt-dlp-tool-id>.max_concurrent_calls = 1`
- local sources added via `media add-local` are modeled as one managed
  `import-once` step with `options.kind = "cas_hash"` and `options.hash =
  "blake3:..."`
- manually seeded variant pointers may still be declared via `variant_hashes`
  (map of variant name -> CAS hash pointer)
- all media processing is declared in one ordered `steps` list where each step
  declares:
  - `tool` (`yt-dlp`, `import`, `import-once`, `ffmpeg`, `rsgain`,
    `media-tagger`)
  - `input_variants` for non-source-ingest transforms; source-ingest tools
    (`yt-dlp`, `import`, `import-once`) keep `input_variants` empty
  - `output_variants` as a map keyed by output variant name where values
    optionally override output-policy flags (`save`, `save_full`), with
    defaults `save = true` and `save_full = false`; hierarchy file paths must
    select file variants whose latest producer keeps `save = true` and
    `save_full = true`, while hierarchy directory paths may keep folder
    variants at default `save_full = false`
    (`yt-dlp` also uses this map to expose non-primary artifact families like
    subtitles, thumbnails, descriptions, infojson, comments, link files,
    chapter splits, and playlist sidecars)
  - `options` (tool-specific; unknown keys are rejected at load time), where
    values are scalar strings by default and list values are reserved for
    low-level bindings `option_args`, `leading_args` (inserted immediately
    after executable), and `trailing_args` (appended at end of args)
- online media URIs now live in downloader step options (`options.uri`) rather
  than one top-level source URI field
- supported option families include downloader controls
  (`uri`, `format`, `write_description`, `write_info_json`, `ffmpeg_location`),
  plus transform controls for `ffmpeg`, `rsgain`, and `media-tagger`
- managed `media-tagger` tool defaults `strict_identification` to `"true"`
  unless explicitly overridden in step options
- when `media-tagger` needs AcoustID lookup (no explicit
  `recording_mbid` override), missing/empty credentials now fail immediately;
  when credentials are provided via CLI `--acoustid-api-key` or
  `ACOUSTID_API_KEY`, lookup/authentication failures are surfaced as errors.
  For `mediapm sync` workflow execution, include `ACOUSTID_API_KEY` in
  `runtime.inherited_env_vars` when relying on environment-based key lookup

Managed conductor workflow/runtime invariants for `mediapm`:

- each `media.<id>` reconciles to exactly one managed workflow
  (`workflow id = mediapm.media.<id>` unless `media.<id>.workflow_id` overrides)
- step variant-flow dependencies are rendered as explicit `${step_output...}`
  bindings plus matching `depends_on` edges so independent branches can run
  ASAP
- generated executable tool contracts for downloaded media tools declare
  comprehensive IO, including list inputs (`leading_args`, `trailing_args`,
  and option `option_args`) plus scalar option/source inputs:
  - operation/source inputs (`input_content` or `source_url`),
  - outputs (`output_content`, `stdout`, `stderr`, `process_code`)

CAS visualization ownership:

- topology visualization rendering/execution helpers live in `src/cas/`
- `mediapm` CLI `cas ...` commands passthrough to the standalone
  `mediapm-cas` CLI
- Phase 1 CAS commands can also be run directly via
  `cargo run -p mediapm-cas -- <cas-args>`

Built-in tool download catalog used by `mediapm` reconciliation:

| Tool | Download source | Catalog release track | Notes |
| --- | --- | --- | --- |
| `ffmpeg` | GitHub Releases (BtbN preferred; Evermeet used for macOS fallback) | `latest` | Uses platform-native archives for workspace-local installs (`.zip` on Windows/macOS, `.tar.xz` on Linux). |
| `yt-dlp` | GitHub Releases | `latest` track available | Supports pinned release selectors or moving `latest` selector. |
| `rsgain` | GitHub Releases | `latest` | Prefers portable ZIP assets for workspace-local installs. |
| `media-tagger` | Built-in `mediapm` launcher (`mediapm internal media-tagger`) | `latest` | Native tagging flow using Chromaprint + AcoustID + MusicBrainz + FFmetadata + FFmpeg. |

For `media-tagger`, moving `latest` selectors are resolved to the currently
running `mediapm` package version so immutable tool ids always match the
actual builtin implementation being executed.

`mediapm.ncl` tool declarations require at least one release selector per
logical tool: `tools.<name>.version` or `tools.<name>.tag` (or both).
When both selectors are provided, they must match the same resolved release.
`tools.<name>.recheck_seconds` is optional and controls how long cached release
metadata can be reused before `mediapm` refreshes from upstream release APIs.
When omitted, release metadata is refreshed on each reconciliation attempt.
If refresh fails and cached metadata exists, `mediapm` emits a warning and
continues with cached metadata; if no cache exists, reconciliation fails.

Resolved immutable tool ids are derived in this precedence order:

- `mediapm.tools.<name>+source@git-hash` when release metadata exposes a commit hash,
- otherwise `mediapm.tools.<name>+source@version`,
- otherwise `mediapm.tools.<name>+source@tag`.

`source` is a stable identifier from the downloader catalog (for example
`github-releases` or `github-btbn`) to avoid collisions across upstreams.

Tool payloads are materialized into conductor `tool_configs.<tool>.content_map`
by importing downloaded files into CAS while preserving archive directory
structure. Archive payloads prefer compact directory-form entries (for example
`./` or `windows/`) that store uncompressed ZIP bytes to be unpacked by
conductor, instead of writing one content-map entry per extracted file. When
separate OS-specific packages are provisioned, content-map keys are rooted
under `windows/`, `linux/`, and `macos/`, and executable commands use
`${context.platform == "<target>" ? ... | ...}` selectors to pick the
correct binary at runtime.
Managed tool downloads also use a shared user-level CAS cache by default to
avoid re-downloading identical release assets across workspaces for the current
user.

Standalone conductor runtime storage defaults (CLI/API):

- runtime root (`conductor_dir`): `.conductor`
- volatile state document (`state_config`): `.conductor/state.ncl`
- filesystem CAS store (`cas_store_dir`): `.conductor/store/`

These grouped runtime paths are also part of the persisted user/machine
configuration schema (`conductor.ncl` and `conductor.machine.ncl`) via
one grouped optional `runtime` field containing
`conductor_dir`, `state_config`, `cas_store_dir`, and optional
platform-keyed `inherited_env_vars`.

The conductor CLI exposes grouped path flags (`--conductor-dir`,
`--config-state`, `--cas-store-dir`). `--cas-store-dir` accepts any CAS
locator string (plain filesystem path or URL); defaults to the resolved
`<conductor_dir>/store` path when omitted.

The persistent conductor demo (`src/conductor/examples/demo.rs`) writes
orchestration state to
`src/conductor/examples/.artifacts/demo/orchestration-state.pretty.json` and
prints that file path instead of streaming the full JSON state payload to
stdout. Both this demo artifact and the `conductor state` command render the
persisted orchestration-state wire-envelope shape.

Current orchestration-state snapshots include explicit top-level `version` and
store per-instance `tool_name` plus normalized metadata: executable metadata
keeps `ToolSpec` shape, while builtin metadata persists only
`kind`/`name`/`version`. Each instance records optional `impure_timestamp` at
instance scope and stores input references by CAS hash identity. For
deduplicated equivalent tool calls, persisted output persistence flags are the
effective merged policy (`save`: logical AND, `force_full`: logical OR).
Builtin orchestration-state metadata decoding is strict and rejects extra
non-identity fields.

`mediapm` persistence versioning now follows the same boundary pattern used in
`cas` and `conductor`:

- `mediapm.ncl` decoding/encoding is delegated through
  `src/mediapm/src/config/versions/`
- lockfile decoding/encoding is delegated through
  `src/mediapm/src/lockfile/versions/`
- both persisted documents carry explicit top-level numeric `version` markers
- version-specific wire envelopes live in `vN.rs` modules and runtime structs
  stay outside the versioned wire layer
- lockfile managed-file sync timestamps persist as Unix-epoch milliseconds via
  `managed_files.<path>.last_synced_unix_millis`; per-file provenance stores
  `managed_files.<path>.media_id`

## Notes

This repository now matches the requested multi-crate phase topology, but it is
still an implementation scaffold rather than the full feature-complete system
described in `PLAN.md`.

Builtin runtime policy (mandatory):

- Builtin runtime behavior lives in dedicated crates under
  `src/conductor-builtins/*` (including `echo`).
- `src/conductor` only dispatches to builtin crate APIs and does not keep
  builtin runtime logic inline.
- Each builtin crate remains independently runnable via its own binary target
  while also exposing a library API.
- Builtin crates use explicit crate versions in each builtin `Cargo.toml`
  (`version = "..."`) instead of inheriting workspace package version.
- Builtin crates must share one stable input contract:
  CLI uses normal Rust flags/options while keeping argument values as strings,
  and API accepts `BTreeMap<String, String>` args plus optional raw payload
  bytes for content-oriented operations (for example archive/export). A builtin
  CLI may optionally expose one default option key so one value can be passed
  without spelling the option key, while explicit keyed input remains supported
  and maps to the same API key. Builtin execution must fail on unrecognized
  args/inputs, missing required keys, and invalid argument combinations instead
  of silently ignoring mismatches. If a builtin's successful non-error result is pure,
  its success payload may be deterministic bytes or `BTreeMap<String, String>`.
  Impure builtins may
  instead primarily communicate success through side effects. CLI failures may
  use ordinary Rust error types instead of being encoded into the success
  payload.
