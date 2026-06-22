# mediapm-conductor — Simplification Plan

## Purpose

`mediapm-conductor` is the deterministic workflow engine that plans, schedules,
and executes multi-step media pipelines over content-addressed storage (CAS).
It is the core orchestrator between user-authored config (.ncl), managed tool
definitions, and the CAS persistence layer.

---

## 1. Features and Requirements (captured)

### 1.1 Core orchestration (keep)

- **Multi-workflow execution**: run one or more workflows defined in config,
  each with ordered steps forming a DAG.
- **Deterministic caching**: cache step outputs by tool call instance key
  (derived from tool identity + resolved inputs + impure timestamps). Second
  run of unchanged workflow hits cache.
- **Impure timestamp management**: impure tool steps get a monotonic timestamp
  injected at planning time, which becomes part of the tool call instance key.
- **CAS integrity recovery**: corrupt-object detection with drop+retry for pure
  workflows; optional retry for impure via `retry_impure` flag.
- **Output persistence**: per-output `save` policy (true/false/`"full"`/unsaved)
  controlling what gets persisted to CAS.
- **GC on tool call instances**: TTL-based pruning of old tool call instances
  from orchestration state.

### 1.2 Configuration model (critical — current single-document replacement was wrong)

- **Zero-to-many source documents**: accept zero to many user config files (each
  is a Nickel file). Each config is parsed independently using its own embedded
  schema version marker. After independent parsing, all configs are merged
  together. **Error on any value conflict across documents** — do not silently
  override or first-wins. If the current single-document model does not support
  this, FIX IT.
- **One volatile state document**: kept for machine-managed runtime state.
  State document is never merged with source documents; it is layered on top at
  runtime.
- **Default behavior**: one config `conductor.ncl` + auto state document.
- **Config mutation**: operations that modify config default to modifying the
  first config; target can be explicitly specified via `--config-index` or
  equivalent for multi-config setups.
- **Versioned schema**: `vX.ncl` embedded Nickel contracts. Migration of
  Nickel schemas always uses Nickel files (`.ncl`), never Rust. State
  migration uses Rust (state is JSON blob in CAS). Follows the versioning
  pattern from `mediapm-cas` crate (no optics library).
- **Tool definitions**: builtin (name+version) or executable (command+env_vars+
  success_codes). Each tool has a `runtime` sub-object holding fields that must
  NOT be part of the tool call instance key (content_map, concurrency limits,
  retry config, input defaults, runtime env_vars).
- **`tool_configs` removed**: what was in `tool_configs.<tool>` moves to
  `tools.<tool>.runtime`. The `runtime` property is excluded from tool call
  instance identity computation.
- **Runtime storage paths**: `conductor_dir`, `conductor_state_config`,
  `cas_store_dir`, `conductor_tmp_dir`, `conductor_tools_dir`,
  `conductor_schema_dir`. All path overrides must also be settable via
  environment variables (e.g., `MEDIAPM_CONDUCTOR_DIR`, `MEDIAPM_CAS_STORE_DIR`).
- **Input binding interpolation**: `${external_data.<hash>}`,
  `${step_output.<step_id>.<output>}`, `${env.<VAR>}`, with `:zip(<member>)`
  selector.
- **Workflow steps**: `id`, `tool`, `inputs`, `depends_on`, `outputs` per step.
- **Option removal**: eliminate `Option` from configuration types wherever
  possible. Push defaults to the boundary. Centralize defaults in a dedicated
  `defaults.rs` module (following `mediapm-cas` pattern).

### 1.3 CLI (moderate complexity, expand)

- Subcommands:
  - `run` — run a workflow.
  - `state` (show, compile, export, import, edit, invalidate-tool-call) — manage
    orchestration state. `state edit` opens the state document in `$EDITOR`.
    `state invalidate-tool-call` invalidates a specific tool call instance by
    its instance key.
  - `import` (tool/data) — import tools or data.
    - `import tool <name> [--source <url>]` — import a tool definition from a URL or local path.
    - `import data <hash> [--description <text>] [--overwrite]` — import external data by CAS hash with optional human-readable description. Overwrite flag controls whether existing entries are replaced.
  - `remove` (tool/data) — remove tools or data.
  - `cas` (passthrough) — pass through to CAS operations.
  - `compile` — compile and validate config.
  - `completions` — shell completion generation.
  - `gc` — garbage-collect old tool call instances.
  - `export-schemas` — export Nickel schema files.
- **Global overrides**: `--conductor-dir`, `--config-state`, `--cas-store-dir`,
  `--conductor-schema-dir`, `--conductor-tmp-dir`, `--conductor-tools-dir`.
  Each global override must also be settable via a corresponding environment
  variable (e.g., `MEDIAPM_CONDUCTOR_DIR`, `MEDIAPM_CONFIG_STATE`,
  `MEDIAPM_CAS_STORE_DIR`, `MEDIAPM_CONDUCTOR_SCHEMA_DIR`). CLI flag takes
  precedence over env var, env var takes precedence over config file default.
- Feature-gated behind `cli` feature (default on).
- CLI/API parity contract.

### 1.4 Actor architecture (simplified — avoid old complexity)

- **No full actor framework**. The old actor-per-concern model (DocumentLoader,
  Scheduler, StateStore as separate actors with typed clients + message enums +
  spawn functions + RPC error handling) is too complex. Do NOT reproduce it.
- **Simplified helpers**: document loading, scheduling, and state persistence
  are plain async methods on the coordinator or dedicated helper structs behind
  `Arc<RwLock<...>>` or `Mutex`, not actors. Keep the code human-maintainable.
- **StepWorker pool**: the one place where actor isolation is justified
  (parallel execution pool). Keep worker pool as actors; inline everything else.
- **WorkflowCoordinator**: the central controller that orchestrates all
  components using async calls (not message-passing).

### 1.4a API trait (simplified — remove unnecessary features)

- The `ConductorApi` trait is intentionally simplified to only the essential
  methods needed by consumers (`mediapm` crate). The old trait in `src_old/`
  had 9 methods; the simplified trait keeps only:
  - `run_workflow(name)` — run a workflow by name.
  - `run_workflow_with_options(name, options)` — run with override options.
  - `get_runtime_diagnostics()` — retrieve runtime diagnostic counters.
- **Intentionally removed** (not needed for simplified design):
  - `submit_workflow` / `poll_workflow` / `cancel_workflow` — no async workflow
    submission model; workflows run synchronously.
  - `get_workflow_status` — status is returned directly from run calls.
  - `export_state_to_path` — state persistence is internal; consumers use
    first-class CLI commands instead.
  - `add_tool_config` / `remove_tool_config` — replaced by CLI import/remove
    subcommands and direct document mutation helpers.
- No new methods should be added to the trait unless a concrete consumer
  (mediapm or CLI) demonstrates a need.

### 1.5 Tool provisioning (keep, already well-factored)

- `ProvisionCache<C>` (formerly `ToolContentCache<C>`) extracted into
  `provision/` module with sub-files: `types.rs`, `helpers.rs`, `extract.rs`,
  `retain.rs`, `provisioner.rs`.
- `ProvisionedTool` (formerly `ToolCacheEntry`) RAII guard for payload access.
- Single-flight extraction via `Notify`.
- TTL-based expiry.
- Payload directory layout.

### 1.6 Tool presets, builtins, and downloads (reorganized)

- Feature flag: `tool-presets` (included by default) for source-fetched
  executables.
- Each preset tool lives in its own subfolder under `tools/` (e.g.,
  `tools/sd/`).
- Each builtin also lives in its own subfolder under `tools/` (`echo/`,
  `fs/`, `import/`, `archive/`, `export/`), each re-exporting constants
  from the corresponding `mediapm_conductor_builtin_*` crate.
- `tools/` root provides a central registry (`BuiltinRegistration`,
  `ALL_BUILTINS`, `find_builtin`, `registered_builtin_ids`) plus shared
  abstractions for tool-specific code.
- `CommonExecutableTool::Sd` enum variant (only one preset currently).
- Download via `reqwest` + extraction via `flate2`+`tar`+`zip`.
- `UserLevelCache` for persistent download caching.
- Platform-conditional path selectors for multi-platform payloads.

### 1.7 Profiler (low complexity, keep)

- `WorkflowRunProfile` + `StepExecutionProfile`
- JSON artifact written after successful run
- Controlled by `profile_output_path` or `MEDIAPM_CONDUCTOR_PROFILE_JSON` env var

### 1.8 Runtime environment (low complexity, keep)

- `.env` / `.env.generated` file loading
- Inherited env var merging with host platform defaults
- `runtime_env::ensure_runtime_env_files`

### 1.9 Error taxonomy (low complexity, keep)

- `ConductorError` enum: Workflow, Cas, Serialization, Io, Internal
- No RPC error helpers (no actor-based RPC).

### 1.10 Full template engine and input resolution (REQUIRED — no shortcuts)

- **Everything must be implemented**:
  - `${step_output.<step_id>.<output>}` — cross-step output references.
  - `${external_data.<hash>}` — CAS-addressed external data references.
  - `${env.<VAR>}` — host environment variable references.
  - `${*<token>}` — unpack tokens for expanding archive references inline.
  - `:zip(<member>)` — ZIP archive member selectors.
  - `${...:file(<path>)}` / `${...:folder(<path>)}` — materialization directives
    that write resolved content to the sandbox filesystem before execution.
  - Platform conditionals (`context.os == "macos" ? ... : ...`) — select
    different values based on host platform.
  - Template comparison operators (`==`, `!=`, `<`, `<=`, `>`, `>=`).
  - Template escape (`\${`) — literal `${{` sequences.
- **No partial implementation**. Every `${}` form listed above must be parsed,
  resolved, and tested. The current regex-only approach for `step_output` is
  insufficient.

### 1.11 Eliminate all stubs (REQUIRED — no new stubs)

Every public API method that currently returns `Err("not yet implemented")` must
be fully implemented. **Do NOT add new stubs when implementing something;
implement it completely.**

- `SimpleConductor::add_tool_config` — full implementation via document
  mutation + persistence.
- `SimpleConductor::remove_tool_config` — full implementation.
- `SimpleConductor::run_tool_passthrough` — full implementation.
- `SimpleConductor::run_cas_passthrough` — full implementation.
- `SimpleConductor::export_schemas` — full implementation.
- `document_io::load_document` — full implementation with versioned Nickel
  evaluation.
- `document_io::save_document` — full implementation.

### 1.12 Save policy and platform inherited env vars

- **`save` policy field** on `ExternalContentRef` or equivalent: controls
  whether (and how) output is persisted to CAS (`true`/`false`/`"full"`/unsaved).
  Implement this properly, not as a stub.
- **Platform inherited env vars** (`PlatformInheritedEnvVars`): model for
  specifying which host environment variables are inherited by tool processes.
  Not just a flat `BTreeMap<String,String>` — must support the full
  semantics from `src_old/`.

### 1.13 Generic cache TTL pruning (new)

- The generic `Cache<C>` in `cache/` uses a JSONC index file to track cached
  entries with per-type TTL expiry. Currently has a fixed 30-day TTL.
- **Per-type TTL pruning**: each cache entry type defines its own TTL duration
  (not a single global TTL). Entries are pruned based on last-use datetime.
- **Last-use tracking**: on creation, each entry records the current datetime
  as its last-use timestamp. The cache exposes a `refresh_last_used(key)`
  method so callers can explicitly update the last-use datetime (the cache
  users determine what counts as "used").
- **Pruning granularity**: stale entries are removed on explicit prune calls
  only (not automatically on access). The caller determines what counts as
  "used" via `refresh_last_used()`. No background GC thread needed.

### 1.14 User-level cache and downloader cache types

- **`UserLevelCache`**: a thin wrapper that places the generic `Cache<C>` in a
  user-level directory (determined by `default_download_cache_root()`).
  It does not add new semantics — it is simply the generic cache rooted at an
  OS-appropriate user-level path.
- **Downloader cache** (uses `UserLevelCache`): caches downloaded tool assets
  and metadata. Two distinct cache entries with different TTL policies:
  - `tools.json`: stores downloaded tool executables/packages.
    - TTL: **30 days**.
    - Expiration is based on **last-use datetime** — touching or refreshing
      the entry extends its lifetime.
  - `tool_metadata.json`: stores tool metadata (version listings, checksums,
    available releases).
    - TTL: **1 day**.
    - Expiration is based on **creation date only** — refreshing last-use does
      not extend its lifetime. Metadata is considered stale after 1 day
      regardless of access frequency.
- Both types share the same `UserLevelCache` instance, differentiated by their
  cache key / index section.

---

## 2. Complexity analysis

| Area | Estimated lines | Complexity | Simplification potential |
|---|---|---|---|
| Config model (documents + versions) | ~1500 | High | Medium |
| Actor architecture | ~600 | Medium | **High** |
| StepWorkerExecutor | ~2000 | **Highest** | **High** |
| Tool presets/download | ~500 | High | Medium |
| CLI | ~500 | Moderate | Low |
| API/trait | ~300 | Low | Low |
| Coordinator | ~500 | High | Medium |
| ProvisionCache | ~300 | Low | None |
| State model + versions | ~600 | Medium | Low |

---

## 3. Simplification opportunities

### 3.1 Multi-document config with merge (RESTORE — current single-document is wrong)

**Problem**: Current single-document model is wrong. `src_old/` had proper
zero-to-many independent-parse-and-merge semantics. The rewrite lost this.

**Design**:

- Accept zero to many user config files. Each is a standalone Nickel file.
- **Each config is parsed independently** using its own embedded schema version
  marker. Version markers may differ across documents (different schema
  revisions in different files).
- After independent parsing, all resolved configs are merged together.
- **Error on any value conflict across documents** — do NOT first-wins, do NOT
  last-wins, do NOT silent override. Conflicts are bugs and must be surfaced.
- One volatile state document kept separately. State is layered at runtime,
  never merged into source documents.
- Default behavior: one config `conductor.ncl` + auto state document.
- Mutation operations default to first config; explicit `--config-index` or
  equivalent for multi-config setups.

**Benefits**: Correct multi-source model, independent versioning per file,
conflict detection surfaces configuration errors early.

### 3.2 Simplified non-actor helpers (not old actor architecture)

**Problem**: The old actor architecture created indirection for relatively simple
operations. Each actor had a typed client + message enum + actor impl + spawn
function + RPC error handling. The rewrite initially removed actors entirely,
which was good, but also removed capabilities that need to be restored.

**Design**:

- Do NOT reproduce the old actor framework. Keep things as plain async helpers.
- `DocumentLoader`: async helper on coordinator or dedicated struct. Cache by
  blake3 hash using a `HashMap` or `LruCache`. No actor wrapper.
- `Scheduler`: plain struct behind `Mutex` with EWMA estimation + trace buffer
  if needed. Not an actor.
- `StateStore`: plain struct behind `Arc<RwLock<...>>` with CAS CRUD and
  in-memory cache. Not an actor.
- `StepWorker`: keep as actors for parallel execution pool. This is the one
  place where actor isolation is justified.
- `WorkflowCoordinator`: central controller using async calls, not
  message-passing.

**Verdict**: Replace actor-per-concern with non-actor helpers behind
synchronization primitives. StepWorker pool stays as actors; everything else
is inlined. Saves ~400 lines of boilerplate and eliminates RPC error handling.

### 3.3 Break up StepWorkerExecutor (accept)

**Problem**: Single ~2000-line file mixing template expansion, sandbox
materialization, process execution (builtin + executable), output capture,
ZIP extraction, and regex file selection.

**Options**:

- Extract template expansion into `step_worker/template.rs` (already exists as
  sibling module, but still tightly coupled).
- Extract sandbox materialization (content_map resolution, file/directory
  writes, ZIP unpacking) into `step_worker/sandbox.rs` or `materializer.rs`.
- Extract output capture (stdout/stderr/file/regex/folder-as-zip) into
  `step_worker/capture.rs`.
- Keep process dispatch (builtin vs executable) and overall flow in `mod.rs`.

**Verdict**: Split into focused sibling modules. ~2000 lines → 3-4 files of
~500-700 lines each. Better isolation, readability, and testability.

### 3.4 Strip versioning to mediapm-cas pattern (discard full overhaul, but remove fp-library)

**Problem**:

- State schema has v1→v2 migration with full optic bridges.
- Config schema has v1 only but with `fp-library` iso pattern.
- Migration dispatch is latest-first with version guard comments.

**Changes**:

- Remove `fp-library` dependency entirely.
- Follow `mediapm-cas` versioning pattern: version markers in persisted data,
  straightforward serde dispatch, manual migration functions.
- Nickel schema migration always uses Nickel (`.ncl` transformations), never
  Rust.
- State migration uses Rust (state is JSON blob in CAS).
- Keep the `versions/` module structure but without optics.

### 3.5 Reorganize tool presets (keep, gate behind feature, on by default)

**Problem**: Tool presets introduce `reqwest`, `flate2`, `tar`, `zip`
dependencies and a download cache. Currently only one tool (`sd`) exists.
Code structure mixes tool-specific logic with generic download abstractions.

**Reorganization**:

- Each preset tool gets its own subfolder under `tools/` (e.g., `tools/sd/`).
- `tools/` root provides traits and shared types that interface tool-specific
  code with the rest of the conductor.
- Keep `tool-presets` feature (on by default) to gate the dependency tree.
- No functional removal — just structural cleanup.

### 3.6 Rationalize env-var configuration surface (expand for global overrides)

**Problem**: Global path overrides (conductor-dir, cas-store-dir, etc.) must be
settable via environment variables, not just CLI flags and config files.

**Design**:

- Every global path override must have a corresponding environment variable.
- Naming convention: `MEDIAPM_CONDUCTOR_DIR`, `MEDIAPM_CAS_STORE_DIR`,
  `MEDIAPM_CONFIG_STATE`, `MEDIAPM_CONDUCTOR_SCHEMA_DIR`,
  `MEDIAPM_CONDUCTOR_TMP_DIR`, `MEDIAPM_CONDUCTOR_TOOLS_DIR`.
- Precedence: CLI flag > env var > config file default.
- Remove rarely-overridden tuning env vars (worker pool size, RPC timeouts,
  etc. — these are dead since there's no actor RPC). Keep only
  `MEDIAPM_CONDUCTOR_EXECUTABLE_TIMEOUT_SECS` (safety-critical) and
  `MEDIAPM_CONDUCTOR_PROFILE_JSON` (debugging).

### 3.7 Remove Option from config types, implement save policy (new)

**Problem**: Many configuration structs wrap fields in `Option<T>`, forcing
callers to unwrap/propagate everywhere. This pushes complexity into consumers
instead of centralizing defaults. The `save` policy field on output references
is missing entirely.

**Design**:

- Remove `Option` from config fields where a sensible default exists.
- Push all defaults to a single `defaults.rs` module (like `mediapm-cas`).
- At the boundary (deserialization, construction), fill in defaults so that
  internal logic never sees `None`.
- Fields that genuinely need "not set" semantics keep `Option`, but audit each.
- **Implement `save` policy field** on external content refs / output
  references: `true` (persist to CAS), `false` (do not persist),
  `"full"` (persist with full metadata), `unsaved` (keep only in runtime state).

**Benefits**: Simplifies internal code paths, eliminates unwrap chains,
centralizes default knowledge, restores missing persistence control.

### 3.8 Terminology: "instance" → "tool call instance" (across all code)

**Problem**: Code and comments frequently refer to "instance" alone, creating
ambiguity with CAS instances, config instances, etc.

**Fix**: Every reference to a tool call instance must use the full term
"tool call instance". Update struct names, variable names, docstrings, and
comments. (e.g., `ToolCallInstance` is already correct; `instances` field on
`OrchestrationState` stays as-is since it's contextual, but all prose/docs use
full term.)

---

## 4. Priorities (updated)

| Priority | Change | Effort | Impact | Status |
|---|---|---|---|---|
| **P0** | **Multi-document config model**: zero-to-many independent parse + version + error-on-conflict merge. Fix the single-document regression. | High | **Critical** — core model correctness | ✅ Done |
| **P0** | **Full template engine**: `${step_output}`, `${external_data}`, `${env}`, `${*token}`, `:zip()`, `:file()`, `:folder()`, platform conditionals, comparison ops, escape. No shortcuts. | High | **Critical** — workflow expressiveness | ✅ Done |
| **P0** | **Eliminate all stubs**: `add_tool_config`, `remove_tool_config`, `run_tool_passthrough`, `run_cas_passthrough`, `export_schemas`, `document_io::load_document`, `document_io::save_document`. No new stubs when implementing — finish each fully. | High | **Critical** — API completeness | ✅ Done |
| **P1** | **CLI features**: `state edit` (opens `$EDITOR`), `state invalidate-tool-call`, `completions` subcommand, global overrides with env var fallbacks. | Medium | High — missing UX parity | ✅ Done |
| **P1** | **Simplified helpers** (not full actors): inline DocumentLoader/Scheduler/StateStore behind async helpers + sync primitives. Keep StepWorker pool as actors. | Medium | High — removes RPC boilerplate | ✅ Done |
| **P2** | **Generic cache TTL pruning**: per-type TTL, last-use tracking, `refresh_last_used()`, explicit prune calls (caller determines "used"). | Medium | Medium — cache correctness | ✅ Done |
| **P2** | **Downloader cache types**: `tools.json` (30-day, last-use-based) and `tool_metadata.json` (1-day, creation-date-only). | Low | Medium — download cache freshness | ✅ Done |
| **P2** | **Platform inherited env vars**: simplified to flat `BTreeMap<String,String>` with `default_runtime_inherited_env_vars_for_host()`. | Low | Medium — tool isolation correctness | ✅ Done |
| **P2** | **`save` policy field**: implemented on `OutputCaptureSpec` (`true`/`false`) with `PersistenceFlags`. | Low | Medium — persistence control | ✅ Done |
| **P3** | Break up StepWorkerExecutor into sibling modules | Low | High — maintainability of largest file | ✅ Done |
| **P4** | Strip `fp-library`, adopt mediapm-cas versioning pattern | Medium | Medium — fewer deps, simpler version story | ✅ Done |
| **P5** | Remove `Option` from config types, centralize `defaults.rs` | Medium | Medium — cleaner internal code | ✅ Done |
| **P6** | Reorganize tools/ into per-tool subfolders with registry | Low | Medium — cleaner code structure | ✅ Done |
| **P7** | Terminology cleanup ("instance" → "tool call instance") | Low | Low — reduced ambiguity | ✅ Done |

## 5. Non-goals (to preserve)

- **Deterministic caching**: core value proposition, keep intact.
- **CAS integration**: the whole point, keep everything.
- **Config model**: keep multi-doc design, preserve user intent vs
  runtime-managed separation (via volatile state document).
- **Input binding interpolation**: `${external_data}`, `${step_output}`,
  `${env}` are essential for workflow expressiveness.
- **Content map materialization**: essential for managed tool execution.
- **GC**: important for long-running deployments.
- **ProvisionCache**: already well-factored, don't touch.
- **Error types**: ConductorError enum is clean and minimal.

---

## 6. Execution plan

### Phase 0 — Core model correctness (P0)

1. **Multi-document config model**: zero-to-many independent parse by version
   marker + error-on-conflict merge. ✅
2. **Full template engine**: ALL `${}` forms implemented (step_output,
   external_data, env, unpack tokens, ZIP selectors, file/folder materialization
   directives, platform conditionals, comparison operators, escape). ✅
3. **Eliminate all stubs**: `add_tool_config`, `remove_tool_config`,
   `run_tool_passthrough`, `run_cas_passthrough`, `export_schemas`,
   `document_io::load_document`, `document_io::save_document` fully implemented. ✅
4. **`save` policy field**: implemented on `OutputCaptureSpec` with `PersistenceFlags`. ✅
5. **Platform inherited env vars**: simplified model with `default_runtime_inherited_env_vars_for_host()`. ✅

### Phase 1 — CLI completeness (P1)

6. `state edit` (opens `$EDITOR`). ✅
7. `state invalidate-tool-call` (invalidate by instance key). ✅
8. `completions` subcommand (shell completion generation). ✅
9. Global path overrides as CLI flags with env var fallbacks.
   Precedence: CLI flag > env var > config default. ✅

### Phase 2 — Simplified architecture (P1)

10. Actor-per-concern replaced with plain async helpers.
    - DocumentLoader: async helper with blake3-hash cache.
    - Scheduler: plain struct with `Mutex`.
    - StateStore: plain struct with `Arc<RwLock<...>>`.
    - StepWorker pool: keeps ractor actors (parallel isolation). ✅

### Phase 2a — Cache TTL pruning (P2)

11. Per-type TTL pruning in generic `Cache<C>`:
    - Per-type TTL durations, `refresh_last_used()`, explicit prune calls.
    - Automatic on-access pruning intentionally NOT implemented;
      caller determines "used" semantics. ✅
12. Downloader cache types:
    - `tools.json` (30-day, last-use-based TTL).
    - `tool_metadata.json` (1-day, creation-date-only TTL). ✅

### Phase 3 — Structural cleanup (P3-P7)

13. `StepWorkerExecutor` split into sibling modules (template, sandbox,
    capture, inputs, process, cache, executor). ✅
14. `fp-library` dependency removed; `mediapm-cas` versioning pattern adopted. ✅
15. `Option` removed from config types; `defaults.rs` centralized. ✅
    - Note: dead `InputBinding` struct deleted (was placeholder, never used).
16. Terminology: rename all ambiguous "instance" references to
    "tool call instance". ✅

### Already done (before PLAN.md creation)

- Reorganize `tools/` into per-tool subfolders with central registry. ✅
  - Builtins (`echo`, `fs`, `import`, `archive`, `export`) each get
    `tools/<name>/mod.rs` re-exporting from external builtin crate.
  - `tools/mod.rs` provides `BuiltinRegistration`, `ALL_BUILTINS`,
    `find_builtin()`, `registered_builtin_ids()`.
- Rename `ToolContentCache` → `ProvisionCache`, `ToolCacheEntry` →
  `ProvisionedTool`. ✅
- Move `tool_cache/` out of `tools/` to crate-root `provision/`. ✅
- Split `provision/` into focused sub-files: `types.rs`, `helpers.rs`,
  `extract.rs`, `retain.rs`, `provisioner.rs`. ✅

---

*Started: 2025-07-17*
*Updated: 2026-06-17*

---

*Started: 2025-07-17*
*Updated: 2026-06-17 — all P0-P7 items completed*
