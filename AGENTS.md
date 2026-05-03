# Project Guidelines

## Repository Shape

- Root `AGENTS.md` is the workspace-wide source of truth. Do not add
  `.github/copilot-instructions.md`.
- Treat this repository as a Rust workspace organized around mediapm's
  crate-oriented architecture. Keep guidance aligned to concrete files and
  current implementation state.
- Keep this file short and durable. Put file-type and workflow-specific rules
  in `.agents/instructions/*.instructions.md`, reusable workflows in
  `.agents/prompts/*.prompt.md`, and skill assets in `.agents/skills/<skill>/`.
- `src/` now contains workspace member crates:
  - `src/cas/` (CAS)
  - `src/conductor/` (Conductor)
  - `src/conductor-builtins/*/` (conductor built-ins)
  - `src/mediapm/` (mediapm application)
- Integration tests currently live with workspace crates (for example,
  `src/mediapm/tests/`).
  Prefer one shared harness shape across crates:
  - top-level `tests/tests.rs` as the integration harness,
  - grouped submodules under `tests/e2e/`, `tests/int/`, and `tests/prop/`.

## Architecture

- Agent customization is file-driven:
  - `opencode.jsonc` registers `.agents/instructions/**/*.md` and
    `.agents/skills/`
  - `.opencode/commands/` mirrors prompt workflows for OpenCode consumers
  - `.vscode/settings.json` defines terminal auto-approve patterns and editor
    behavior
- Repository automation currently lives in:
  - `.github/workflows/ci.yml` for CI scaffolding
  - `.github/dependabot.yml` for dependency-update scope
  - `.commitlintrc.mjs` for commit message policy
- Formatting and newline behavior come from `.editorconfig`, `.gitattributes`,
  `.markdownlint.jsonc`, and `.agents/.markdownlint.jsonc`.

## Core Engineering Contract

- This repository now treats `AGENTS.md` + `.agents/instructions/*.instructions.md`
  as the durable implementation contract (the legacy planning markdown files
  are intentionally retired after policy migration).
- Hard principles for all workspace crates:
  - simplicity first;
  - performance is a user-visible feature;
  - functional core, imperative shell;
  - incremental by default with explicit content-addressed cache keys;
  - async I/O/orchestration with runtime adapters (Tokio default);
  - actor-first concurrency with explicit supervision behavior;
  - type-system-enforced invariants where practical;
  - pragmatic macro usage (reduce boilerplate, do not hide critical flow);
  - documentation is part of the API contract.
- Technology baseline:
  - actor/orchestration: `ractor`,
  - hashing: `blake3`,
  - async contracts: `futures` (+ `async-trait` where useful),
  - tracing/diagnostics: `tracing` + `tracing-subscriber`,
  - serialization: `serde` + deterministic `serde_json` policy.
- Performance engineering loop is mandatory:
  1. profile,
  2. hypothesize,
  3. optimize,
  4. benchmark,
  5. keep-or-revert based on evidence.
- Hot-path expectations:
  - prefer contiguous data layouts and bounded allocations,
  - avoid hidden clones and tiny-syscall loops,
  - keep async handlers non-blocking and route unavoidable blocking work
    through bounded worker boundaries.
- Definition-of-done expectations across crates:
  - public APIs have integration coverage,
  - major features have end-to-end coverage,
  - determinism/idempotency behavior is tested,
  - migration behavior is documented and auditable,
  - performance claims are benchmark-backed,
  - formatting/lint/tests pass in CI.

## Rust Architecture Snapshot

- `src/cas/` provides the CAS identity model and async API contracts.
  CAS topology visualization implementation also belongs in this crate.
- `src/conductor/` provides the orchestration state model and
  persistence-merge logic.
  Key cross-crate invariants:
  - `conductor.ncl` is user-owned intent; `conductor.machine.ncl` is
    machine-managed runtime state; unresolvable conflicts fail fast.
  - All three config documents must carry explicit top-level numeric `version`
    markers; the runtime state document is volatile-only.
  - Builtin tool definitions in persisted config are strict
    (`kind`, `name`, `version` only); extra fields are rejected on decode.
  - Instance identity excludes content-map payload details and merged
    persistence flags; output persistence merged across equivalent calls:
    `save` uses AND, `force_full` uses OR.
  - `tool_configs.<tool>.content_map` keys are sandbox-relative; absolute and
    path-traversal entries are rejected; separate entries must not overwrite
    the same target path.
  - Pure workflows may auto-recover from CAS integrity failures (warn + drop +
    retry once); impure workflows fail without auto-retry.
  See `src/conductor/AGENTS.md` for the full configuration document model,
  tool schema invariants, template syntax contract, and versioned schema policy.
- `src/conductor-builtins/` provides versioned built-in tool contracts such as
  `echo`, `fs`, `import`, `export`, and `archive`.
  Builtin runtime behavior must live in these crates (not inline in
  `src/conductor`), and each builtin crate should remain independently runnable
  via its own binary target.
  Builtin contract stability rule: all builtins must share the same input
  conventions. CLI must use normal Rust flag/option conventions while keeping
  all argument values as strings, and API input must use
  `BTreeMap<String, String>` args plus optional raw payload bytes for
  content-oriented operations. A builtin CLI may optionally define one default
  option key so one value can be provided without spelling the option key, but
  explicit keyed input must remain supported and map to the same API key.
  Builtin API and CLI execution must fail fast on undeclared argument/input
  keys, missing required keys, and invalid key combinations; do not silently
  ignore unknown input.
  For builtins whose successful non-error result is pure (a deterministic
  function of inputs), the success payload may be deterministic bytes or
  `BTreeMap<String, String>`.
  Impure builtins may primarily communicate success through side effects and do
  not need to force CLI success into a pure string-only payload. CLI failures
  may use ordinary Rust error types; do not encode failures as fake success
  payloads.
- `src/mediapm/` composes CAS + Conductor into the media-facing API
  and CLI scaffold.
  `src/mediapm/` should depend directly on `mediapm-cas` and
  `mediapm-conductor`; do not add direct dependencies on individual
  `src/conductor-builtins/*` crates.
  Key cross-crate invariants:
  - Runtime state root defaults to `.mediapm/`; `mediapm.ncl` `runtime` may
    optionally override `mediapm_dir`, paths, and `inherited_env_vars`.
  - Media-source entries may include optional human-readable `title` and
    `description`; add flows should auto-populate them from lightweight source
    metadata when available.
  - When `mediapm` invokes conductor, pass grouped runtime-storage paths so
    conductor volatile writes target
    `<mediapm_dir>/state.conductor.ncl` (not standalone
    `.conductor/state.ncl` defaults); `mediapm` machine-managed state persists
    at `<mediapm_dir>/state.ncl` and uses `runtime.media_state_config` for overrides.
  - Materialized outputs are marked read-only after sync commit; runtime may
    clear read-only bits only for managed replacement/removal operations.
  - Materializer enforces NFD-only filenames and rejects reserved characters
    (`<`, `>`, `:`, `"`, `/`, `\\`, `|`, `?`, `*`).
  - Link/write materialization order follows
    `runtime.materialization_preference_order` (must be non-empty and
    duplicate-free); default order is hardlink → symlink → reflink → copy.
  - `yt-dlp` reconciliation defaults to one active concurrent call and one
    outer conductor retry, `sub_langs = "all"`, and unified subtitle capture
    enabled (`write_subs = "true"`, mapped to manual + automatic subtitle
    downloader toggles). Keep translated subtitle pressure low with precise
    `options.sub_langs` selectors and optional `options.sleep_subtitles` when
    provider throttling appears. Keep this mitigation anchored to documented
    upstream incidents in
    `https://github.com/yt-dlp/yt-dlp/issues/13831#issuecomment-3875360390`
    and
    `https://github.com/yt-dlp/yt-dlp/issues/13831#issuecomment-3712613129`:
    broad translated subtitle requests are the highest-risk path for
    `HTTP 429`, focused subtitle requests are usually lower risk, and
    extractor-args
    translation-skip knobs are not a reliable substitute for precise language
    selectors,
    `merge_output_format = "mkv"`, chapter embedding enabled
    by default (`embed_chapters = "true"`, `split_chapters = "false"`),
    `clean_info_json = "true"`, comments capture enabled by default
    (`write_comments = "true"`), all internet-shortcut link outputs enabled
    by default (`write_url_link`, `write_webloc_link`,
    `write_desktop_link`), and highest-quality single-thumbnail capture by
    default; `media-tagger` defaults to `strict_identification = "true"`,
    `write_all_tags = "true"`, `write_all_images = "true"`, and
    `cover_art_slot_count = tools.ffmpeg.max_input_slots - 1`.
  - Managed `rsgain` defaults stay in single-track mode
    (`album = "false"`, `album_mode = "false"`).
  - Managed `media-tagger` cache defaults to `<mediapm_dir>/cache` and uses
    the shared CAS/index layout (`cache/store/` + `cache/media-tagger.jsonc`)
    without dedicated media-tagger subfolders under `store/`.
  - Schema exports default to `<mediapm_dir>/config/mediapm` for mediapm;
    standalone conductor defaults to `<conductor_dir>/config/conductor`; and
    mediapm-driven conductor defaults to `<mediapm_dir>/config/conductor`.
  - Tool requirements may set `ffmpeg_version` for `yt-dlp`, `rsgain`, and
    `media-tagger` (inherit/global semantics when omitted).
  - yt-dlp `output_variants` values must not embed `format`; any explicit
    format selector belongs in step `options.format`.
  - output-variant values are object-driven across managed tools: `kind`
    controls default file-vs-folder capture policy, and optional
    `capture_kind = "file"|"folder"` may override that default per
    variant.
  - output-variant kind naming is strict (no legacy aliases): use
    `primary` for main transform outputs; yt-dlp folder-family kinds must use
    plural names (`subtitles`, `thumbnails`, `links`,
    `chapters`) while file-family kinds remain singular (`primary`,
    `description`, `infojson`, `comment`, `archive`, `annotation`, playlist file sidecars).
  - yt-dlp output-variant `langs` is an optional capture-filter hint for
    subtitle-family artifacts only; downloader language selection remains
    step-option owned via `options.sub_langs`.
  - Hierarchy uses an ordered node-array schema (`hierarchy = [ { ... } ]`)
    with recursive `children` and explicit kinds: `folder` (default),
    `media`, `media_folder`, and `playlist`; legacy flat-map and `"/kind"`
    forms are unsupported (no backward compatibility).
    `media` uses singular `variant`, `media_folder` uses plural `variants`
    and optional `rename_files`; hierarchy `id` is optional on all kinds and
    must be unique when provided. `media_id` is optional on all kinds,
    but `media`/`media_folder` require one effective non-empty value (direct
    or inherited). Playlist `ids` resolve by ordered id entries that target
    hierarchy-node ids, accept string shorthand and object entries
    (`{ id, path }`), and remain file-leaf entries.
  - Media-source entries must not define `media.<id>.id` overrides; playlist
    membership is owned by hierarchy-node ids only.
  - Hierarchy directory entries may define optional ordered
    `rename_files = [{ pattern, replacement }, ...]` regex rewrites that
    apply to extracted folder file members; file hierarchy targets must keep
    `rename_files` empty.
  - Managed executable materialization keeps all-platform `content_map` keys
    (`windows/`, `linux/`, `macos/`, or shared `./` for platform-identical
    payloads) and uses `${context.os == "<target>" ? ... | ...}` command
    selectors.
  See `src/mediapm/AGENTS.md` for runtime path defaults, media schema rules,
  tool provisioning catalog, conductor integration boundary, and example policy.

## Build and Test

- Verify the relevant manifests, scripts, workflow files, and local configs
  exist before you run or document toolchain commands.
- Detect install, build, test, lint, format, type-check, and release commands
  from actual repository files instead of assuming a default stack.
- For Rust workflows, treat `Cargo.toml`, `.cargo/config.toml`,
  `rust-toolchain.toml`, `.github/workflows/ci.yml`, and
  `.agents/instructions/rust-workflow.instructions.md` as source-of-truth
  inputs for validation commands and expectations.
- Prefer targeted cargo validation by default for faster feedback:
  - `cargo test-pkg <crate>` for specific crate testing
  - `cargo clippy-pkg <crate>` for specific crate linting
  - `cargo build-pkg <crate>` for specific crate building
  - Example: `cargo test-pkg mediapm` runs only mediapm tests
  - See `.cargo/config.toml` for available aliases and convenience shortcuts
- For any change that touches `src/mediapm/**`, run
  `cargo run --package mediapm --example demo_online` as a final runtime gate
  after targeted tests/lints so the managed online workflow remains healthy.
  After the run, inspect generated artifacts under
  `src/mediapm/examples/.artifacts/demo-online/` (including sidecar-family
  content correctness, not only path existence).
  To reduce third-party provider rate-limit risk (`HTTP 429`), run this gate
  at most once per validation pass, avoid immediate repeat retries, and apply
  backoff cool-down before re-running after transient provider failures.
  If the run appears stuck, do triage before retrying: check whether
  `cargo`/`mediapm`/`yt-dlp`/`ffmpeg` processes are still active, inspect
  artifact-root timestamps, and check stderr for fallback-root messages
  (`demo-online-fallback-*`) when canonical cleanup is locked.
  First-run bootstrap often needs several minutes for managed tool download
  and extraction; be patient and avoid interrupting while progress is still
  advancing.
  Use `MEDIAPM_DEMO_ONLINE_TIMEOUT_SECS` to keep runs bounded and treat
  timeout failures as blockers (same as other provider/network failures).
  `demo_online` enforces this as a hard timeout and exits with code `124`
  when exceeded.
  Keep timeout/watchdog notices user-facing and progress-safe: avoid periodic
  heartbeat stderr lines during conductor progress rendering, and keep timeout
  notice output plain text (no row-clear ANSI control sequences) so terminal
  progress rows are not duplicated.
  Treat this as mandatory: do not replace failures with placeholder/skip
  success paths. If external providers block completion, report the failure
  explicitly and keep the task blocked until the run succeeds or the reviewer
  accepts the transient failure.
- Use full-workspace validation only for pre-push checks and CI:
  - `cargo fmt-check` (checks formatting on all files)
  - `cargo clippy-all` (lints entire workspace)
  - `cargo test-all` (tests entire workspace)
  - Note: these are intentionally slow; use targeted commands during development
- When a language, framework, task runner, or test system is clearly present,
  add or refine focused instruction files for it rather than stuffing detailed
  rules into `AGENTS.md`.
- Keep CI, editor automation, prompt examples, and instruction files aligned
  with the commands and configs that are actually present in the repository.

## Conventions

- Distinguish between what is present today and what is only part of the
  intended template contract. Do not describe absent files as if they already
  exist.
- Treat this file and focused `.agents/instructions/*.instructions.md` files
  as the active implementation contract. Keep these files in sync with code
  and avoid reviving deleted standalone planning documents.
- Do not regress to bootstrap assumptions (single-crate `src/main.rs` with only
  minimal `Cargo.toml` + `rust-toolchain.toml`). This repository is a
  multi-member Rust workspace with crate members under `src/`.
- When docs mention `application`, `configuration`, `domain`,
  `infrastructure`, and `support`, treat them as conceptual layering terms
  unless matching directories are explicitly introduced in the workspace.
- Before writing stack-specific guidance, inspect concrete evidence such as
  manifests, lockfiles, source tree layout, scripts, CI workflows, editor
  settings, and dedicated config files.
- For Rust edits, treat detailed docstrings as mandatory in touched files:
  document public and private items (`//!` + `///`) with semantics,
  invariants, and side-effect notes, not just name restatements.
- When you detect a real stack, add instructions for it carefully and
  thoroughly in a narrow, well-named instruction file whose `description` and
  `applyTo` target the relevant files.
- Prefer linking to canonical config files instead of copying large policy
  blocks into multiple customization files.
- Keep customization files narrowly scoped: repo-wide defaults in `AGENTS.md`,
  detailed file-specific guidance in `.agents/instructions/`.
- Prefer updating `AGENTS.md` and `.agents/instructions/*.instructions.md`
  directly for durable repository policy. Do not keep long-lived policy only in
  `/memories/repo/`; if temporary repo memory notes are used, merge them into
  instruction files and remove them.
- When splitting one Rust module into multiple files, adopt folder-module
  layout consistently: move `foo.rs` to `foo/mod.rs`, place sibling modules in
  `foo/*.rs`, and place local unit tests in `foo/tests.rs` with
  `#[cfg(test)] mod tests;`. Avoid keeping both `foo.rs` and `foo/mod.rs`, and
  avoid `#[path = "..."]` for routine in-crate module/test placement.
- Do not deliberately rename examples/tests solely to force workspace-wide
  unique target names. Shared canonical names (for example `demo`) are allowed;
  when running examples, use package-qualified invocations to disambiguate.
- Preserve mirrored prompt content between `.agents/prompts/` and
  `.opencode/commands/` when both copies exist.
- Respect the repository newline policy: Markdown and shell scripts use LF;
  PowerShell and batch scripts use CRLF.

## Key References

- `AGENTS.md` — workspace-wide defaults
- `.agents/instructions/*.instructions.md` — focused authoring rules by file type
- `.agents/prompts/commit-staged.prompt.md` and
  `.opencode/commands/commit-staged.prompt.md` — mirrored commit workflow prompt
- `opencode.jsonc` — instruction and skill discovery
- `.vscode/settings.json` — terminal auto-approve and editor behavior
- `.github/workflows/ci.yml`, `.github/dependabot.yml`, `.commitlintrc.mjs` —
  automation and policy
- `Cargo.toml`, `.cargo/config.toml`, `rust-toolchain.toml`, `rustfmt.toml`,
  `clippy.toml` — Rust package and quality configuration
- `.agents/instructions/rust-workflow.instructions.md` — Rust editing and
  validation guidance
- `.agents/instructions/mediapm-architecture.instructions.md` — crate boundaries
  and cross-crate invariants
- `.agents/instructions/mediapm-testing-and-docstrings.instructions.md` — test
  expectations and Rustdoc/docstring depth requirements
- `.editorconfig`, `.gitattributes`, `.markdownlint.jsonc`,
  `.agents/.markdownlint.jsonc` — formatting and line-ending rules
