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

### 1.2 Configuration model (significant complexity, candidate for simplification)

- **Multiple-document Nickel config**: zero to many user configs + one volatile
  state document. All configs parsed independently by their schema version, then
  merged together. Error on value conflict across documents.
- **Default config**: one file `conductor.ncl`.
- **Config mutation**: operations that modify config default to modifying the
  first config; target can be explicitly specified.
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
  `conductor_schema_dir`.
- **Input binding interpolation**: `${external_data.<hash>}`,
  `${step_output.<step_id>.<output>}`, `${env.<VAR>}`, with `:zip(<member>)`
  selector.
- **Workflow steps**: `id`, `tool`, `inputs`, `depends_on`, `outputs` per step.
- **Option removal**: eliminate `Option` from configuration types wherever
  possible. Push defaults to the boundary. Centralize defaults in a dedicated
  `defaults.rs` module (following `mediapm-cas` pattern).

### 1.3 CLI (moderate complexity, keep)

- Subcommands: `run`, `state` (get/set/delete/pointer), `import` (tool/data),
  `remove` (tool/data), `cas` (passthrough), `compile`, `completions`, `gc`.
- Feature-gated behind `cli` feature (default on).
- CLI/API parity contract.

### 1.4 Actor architecture (high complexity, simplification candidate)

- **DocumentLoaderActor**: loads, caches, validates, and merges Nickel
  documents. Cache keyed by blake3 hash of source texts.
- **SchedulerActor**: EWMA-based runtime estimation, worker queue metrics,
  trace ring buffer.
- **StateStoreActor**: CAS-backed state CRUD, persist-and-publish, unsaved-hash
  cleanup, GC orchestration.
- **StepWorker pool**: N workers receiving `StepExecutionRequest` messages,
  resolving inputs, rendering templates, materializing sandbox, running
  processes, capturing outputs.
- **WorkflowCoordinator**: not an actor itself, but the central controller that
  spawns, configures, and orchestrates all actors.

### 1.5 Tool content cache (keep, already well-factored)

- `ToolContentCache<C>` extracted into standalone module
- RAII guards for shared/exclusive access
- Single-flight extraction via `Notify`
- TTL-based expiry
- Payload directory layout

### 1.6 Tool presets / downloads (reorganize)

- Feature flag: `tool-presets` (included by default)
- Each preset tool lives in its own subfolder under `tools/` (e.g.,
  `tools/sd/`).
- `tools/` root provides abstractions (traits, shared types) that interface
  tool-specific code with the rest of the conductor.
- `CommonExecutableTool::Sd` enum variant (only one preset currently).
- Download via `reqwest` + extraction via `flate2`+`tar`+`zip`.
- `UserDownloadCache` for persistent download caching.
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
- RPC error helpers for actor communication

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
| ToolContentCache | ~300 | Low | None |
| State model + versions | ~600 | Medium | Low |

---

## 3. Simplification opportunities

### 3.1 Multi-document config with merge (replaces old 3.1)

**Problem**: Current three-document (user/machine/state) model is rigid. Adding a
new config source requires API changes. The machine/state distinction is
conceptually useful but over-engineered in implementation.

**Design**:

- Accept zero to many user configs (each is a Nickel file).
- One volatile state document (kept, as today).
- All configs parsed independently by their embedded version marker.
- Merged in order (first defined wins? actually: error on conflict).
- Default behavior: one config `conductor.ncl` + auto state document.
- Mutation operations default to first config; explicit `--config-index` or
  equivalent for multi-config setups.

**Benefits**: More flexible, simpler mental model (flat list + state), fewer
special document types.

### 3.2 Inline actors that are thin wrappers (accept)

**Problem**: The actor architecture creates indirection for relatively simple
operations. Each actor has a typed client + message enum + actor impl + spawn
function.

**Candidates for inlining**:

- `DocumentLoaderActor`: caching aside, its core logic (load, validate, merge,
  persist) could be async methods on `WorkflowCoordinator` or a simpler helper.
  The cache-by-blake3-hash could be a `HashMap` or `LruCache` in the coordinator.
- `SchedulerActor`: EWMA estimation + trace buffer could be a plain struct
  behind `Mutex` or a dedicated non-actor module.
- `StateStoreActor`: CAS CRUD with in-memory cache — could be a plain struct.
  The "publish current state" pattern is simple enough for `Arc<RwLock<...>>`.

**Verdict**: Replace actor-per-concern with non-actor helpers behind
synchronization primitives. `StepWorker` is the one that benefits most from
actor isolation (parallel execution pool). Keep worker pool as actors; inline
the others. Saves ~400 lines of boilerplate and eliminates RPC error handling.

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

### 3.6 Reduce env-var configuration surface (accept)

**Problem**: 6+ environment variables for tuning parameters
(`MEDIAPM_CONDUCTOR_WORKER_POOL_SIZE`, `RPC_TIMEOUT_SECONDS`,
`EXECUTABLE_TIMEOUT_SECS`, `PROFILE_OUTPUT_PATH`, etc.).

**Observation**: Most of these have sensible defaults. The env-var surface
adds discoverability burden.

**Verdict**: Remove rarely-overridden env vars. Keep only
`MEDIAPM_CONDUCTOR_EXECUTABLE_TIMEOUT_SECS` (safety-critical) and
`MEDIAPM_CONDUCTOR_PROFILE_JSON` (debugging). Inline the rest as constants.

### 3.7 Remove Option from config types (new)

**Problem**: Many configuration structs wrap fields in `Option<T>`, forcing
callers to unwrap/propagate everywhere. This pushes complexity into consumers
instead of centralizing defaults.

**Design**:

- Remove `Option` from config fields where a sensible default exists.
- Push all defaults to a single `defaults.rs` module (like `mediapm-cas`).
- At the boundary (deserialization, construction), fill in defaults so that
  internal logic never sees `None`.
- Fields that genuinely need "not set" semantics keep `Option`, but audit each.

**Benefits**: Simplifies internal code paths, eliminates unwrap chains,
centralizes default knowledge.

### 3.8 Terminology: "instance" → "tool call instance" (across all code)

**Problem**: Code and comments frequently refer to "instance" alone, creating
ambiguity with CAS instances, config instances, etc.

**Fix**: Every reference to a tool call instance must use the full term
"tool call instance". Update struct names, variable names, docstrings, and
comments. (e.g., `ToolCallInstance` is already correct; `instances` field on
`OrchestrationState` stays as-is since it's contextual, but all prose/docs use
full term.)

---

## 4. Recommended simplification priorities

| Priority | Change | Effort | Impact |
|---|---|---|---|
| **P0** | Inline DocumentLoader, Scheduler, StateStore actors | Medium | High — removes RPC boilerplate, simplifies coordinator |
| **P1** | Break up StepWorkerExecutor into sibling modules | Low | High — improves maintainability of largest file |
| **P2** | Strip `fp-library`, adopt mediapm-cas versioning pattern | Medium | Medium — fewer deps, simpler version story |
| **P3** | Reorganize tool presets into subfolder layout | Low | Medium — cleaner code structure |
| **P4** | Reduce env-var config surface | Low | Low — fewer tuning knobs to document |
| **P5** | Remove Option from config types, centralize defaults | Medium | Medium — cleaner internal code |
| **P6** | Terminology cleanup ("instance" → "tool call instance") | Low | Low — reduced ambiguity |

## 5. Non-goals (to preserve)

- **Deterministic caching**: core value proposition, keep intact.
- **CAS integration**: the whole point, keep everything.
- **Config model**: keep multi-doc design, preserve user intent vs
  runtime-managed separation (via volatile state document).
- **Input binding interpolation**: `${external_data}`, `${step_output}`,
  `${env}` are essential for workflow expressiveness.
- **Content map materialization**: essential for managed tool execution.
- **GC**: important for long-running deployments.
- **ToolContentCache**: already well-factored, don't touch.
- **Error types**: ConductorError enum is clean and minimal.

---

## 6. Execution plan

### Phase 1 — Structural cleanup

1. Split `StepWorkerExecutor` into sibling modules (template, sandbox, capture).
2. Reorganize `tools/` into per-tool subfolders (e.g., `tools/sd/`).
3. Terminology: rename all ambiguous "instance" references to "tool call instance".
4. Strip `fp-library` dependency, adopt `mediapm-cas` versioning pattern.

### Phase 2 — Architectural simplification

5. Inline `DocumentLoaderActor` → async helper on coordinator.
6. Inline `SchedulerActor` → `Mutex<PlainScheduler>`.
7. Inline `StateStoreActor` → `Arc<RwLock<StateStore>>`.

### Phase 3 — Config and defaults cleanup

8. Implement multi-document config model (zero to many + volatile state).
9. Add `runtime` property to tool definitions; remove `tool_configs`.
10. Remove `Option` from config types; create `defaults.rs`.
11. Prune env-var configuration surface.

---

*Started: 2025-07-17*
*Updated: 2026-06-16*
