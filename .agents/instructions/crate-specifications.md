---
description: "Use when reviewing or updating repository architecture guidance and cross-crate contracts for mediapm."
name: "Crate Specifications & Architecture Reference"
applyTo: "AGENTS.md, src/**/AGENTS.md, .agents/instructions/**/*.md"
---

<!-- markdownlint-disable-file -->

# Mediapm Crate Specifications & Architecture Reference

> **❖ Maintenance rule**: This specification document and
> `.agents/instructions/elaboration-pass-edge-cases.md` must be updated
> alongside any code change that affects the described contracts, invariants,
> or behavior. Keeping specs in sync with code is a definition-of-done
> requirement.

This document consolidates authoritative technical specifications for all 4 main crates in the mediapm workspace, with cross-crate integration boundaries and shared invariants.

## Quick Reference: Crate Responsibilities

| Crate | Purpose | Type | Key Exports |
|-------|---------|------|-------------|
| **cas** | Content-addressed storage with delta encoding | Library + CLI | `CasApi`, `CasMaintenanceApi`, `Hash`, `FileSystemCas`, `InMemoryCas` |
| **conductor** | Deterministic workflow orchestration with CAS backing | Library + CLI | `ConductorApi`, `SimpleConductor`, `WorkflowSpec`, `OrchestrationState` |
| **conductor-builtins** (5 crates) | Standalone tool implementations (echo, fs, archive, import, export) | Library + CLI per crate | Builtin CLI/API executables |
| **mediapm** | Media library façade composing CAS + Conductor | Library + CLI | `MediaPmService`, `MediaPmApi`, `MediaPmDocument`, `MediaPmPaths` |

---

## Cross-Crate Data Flow

```
User Input (mediapm.ncl)
    ↓
MediaPm Configuration Parsing
    ├─→ CAS: Content-address media
    ├─→ Conductor: Synthesize workflows
    └─→ Builtins: Tool registration
    ↓
Conductor Workflow Execution
    ├─ Step 1: import (builtin) → CAS store
    ├─ Step 2: ffmpeg (managed tool) → CAS store
    ├─ Step 3: media-tagger (managed tool) → CAS store
    └─ Step N: export (builtin) → Materialized files
    ↓
CAS-Backed Materialization
    ├─ Staging: Link/copy CAS objects to temp dir
    ├─ Validation: Verify hashes
    └─ Commit: Atomic rename to final location
    ↓
State Persistence (state.ncl)
    └─ Lock records: path → media_id, variant, hash
```

---

## Shared Invariants Across Crates

### 1. Content Identity Contract

**Principle**: Same bytes → same hash (always)

| Crate | Implementation |
|-------|-----------------|
| **CAS** | Blake3-256 multihash; `from_content()` is deterministic |
| **Conductor** | CAS hash used for state blob identity; external_data keyed by hash |
| **Builtins** | Pure builtins (echo, archive) produce deterministic payloads |
| **MediaPM** | Lock records keyed by `(media_id, variant)` → CAS hash |

**Verification**: If same input produces different hash across runs, it's a bug.

### 2. Constraint Correctness Contract

**Principle**: Base selection respects explicit constraints

| Crate | Enforcement |
|-------|-------------|
| **CAS** | `set_constraint()` validates bases exist; optimizer honors constraints |
| **Conductor** | External data → CAS → constraint metadata preserved across loads |
| **Builtins** | N/A (read-only, no constraints) |
| **MediaPM** | Workflow state persisted in CAS; constraints implicit (content-addressed) |

**Verification**: If optimizer rewrite violates explicit constraints, it's a bug.

### 3. Reconstructability Contract

**Principle**: Stored bytes are retrievable exactly

| Crate | Guarantee |
|-------|----------|
| **CAS** | `get(hash)` returns exact bytes; delta chains reconstructible |
| **Conductor** | State blob persisted to CAS; can round-trip serialize ↔ deserialize |
| **Builtins** | Output bytes persist; pure outputs are deterministic |
| **MediaPM** | Files materialized from CAS are byte-identical to source |

**Verification**: If retrieved bytes differ from stored, it's a bug.

### 4. Atomicity Contract

**Principle**: Operations succeed or fail cleanly (no partial state)

| Crate | Mechanism |
|-------|-----------|
| **CAS** | Temp file + atomic rename; index snapshots on mutation |
| **Conductor** | State persisted atomically; workflow fails fast on conflicts |
| **Builtins** | File operations succeed or rollback (no orphaned state) |
| **MediaPM** | Staging → validation → commit; rollback on failure |

**Verification**: If partial state persists after failure, it's a bug.

### 5. Determinism Contract

**Principle**: Identical inputs → identical outputs (pure paths only)

| Crate | Scope |
|-------|-------|
| **CAS** | `put()` and `get()` are deterministic; `optimize()` may change encoding |
| **Conductor** | Pure workflows deterministic; impure workflows may vary on retries |
| **Builtins** | Pure (echo, archive) deterministic; impure (fs, import, export) side-effect-driven |
| **MediaPM** | Lock state deterministic; sync can skip if hash unchanged |

**Verification**: If pure operation produces different output, it's a bug.

---

## Integration Boundaries & Contracts

### CAS ↔ Conductor

**Entry Point**: Conductor requires `CasApi` trait object at startup
- `SimpleConductor::new(cas: Arc<C: CasApi>)`

**Operations**:
1. External data stored in CAS: `put_from_uri(uri) → Hash`
2. Workflow state serialized to CAS: `put(orchestration_state_bytes) → Hash`
3. Tool content materialized from CAS: `get(hash) → Bytes`
4. Index repair on startup (optional): `repair_index() → IndexRepairReport`

**Ownership**:
- **Conductor owns**: External data refs, state blobs, input bindings
- **CAS owns**: Storage, persistence, optimization, constraint metadata

**Contract**:
- Conductor may call CAS operations concurrently (thread-safe)
- CAS doesn't reference Conductor types (no circular dep)
- Failures are propagated as-is (no translation)

### Conductor ↔ Builtins

**Entry Point**: Conductor discovers builtins at compile time
- `registered_builtin_ids()` returns `["import@1.0.0", "fs@1.0.0", ...]`

**Operations**:
1. CLI invocation: Builtin binary receives `--arg KEY VALUE` pairs
2. API invocation: Builtin library receives `BTreeMap<String, String>` params
3. Result handling: Pure builtins return deterministic payloads; impure signal via side effects

**Ownership**:
- **Conductor owns**: Tool lifecycle, input binding resolution, output capture
- **Builtins own**: Implementation logic, error semantics, validation rules

**Contract**:
- CLI and API inputs/outputs must be identical (parity)
- Fail-fast validation: undeclared keys rejected immediately
- No encoding of failures in success payloads (exit codes or Result errors only)

### MediaPM ↔ Conductor

**Entry Point**: MediaPM creates Conductor at service startup
- `SimpleConductor::new(cas)` → synthesize workflows → execute

**Operations**:
1. Workflow synthesis: MediaPM builds `WorkflowSpec` from media steps
2. Tool registration: MediaPM adds managed-tool `ToolSpec` to machine config
3. State preservation: MediaPM loads/merges user + machine + state documents
4. Sync execution: MediaPM triggers `conductor.run_workflow(...)` per media entry

**Ownership**:
- **MediaPM owns**: Media-source definitions, hierarchy materialization, tool provisioning
- **Conductor owns**: Workflow execution, step scheduling, state persistence

**Contract**:
- Conductor documents isolated per-workspace (no cross-workspace bleed)
- MediaPM respects conductor's state versioning (explicit migration support)
- Sync is all-or-nothing: succeeds and persists, or fails and reverts staged

### MediaPM ↔ CAS (Direct)

**Entry Point**: MediaPM materializes from CAS
- Conductor already uses CAS for state; MediaPM uses for file materialization

**Operations**:
1. Content verification: Check file hash against lock record
2. Cache hit detection: If hash unchanged, skip re-materialization
3. Link materialization: Call `cas.get()` and write to staging area
4. Atomic commit: Rename staged files to final location

**Ownership**:
- **MediaPM owns**: Hierarchy logic, staging/commit orchestration, lock records
- **CAS owns**: Storage, persistence, object retrieval

**Contract**:
- All materialized files are read-only after commit
- Hashes must match; mismatch → failed materialization (no fallback)
- Platform-independent path resolution (normalized, slash-separated)

---

## Detailed Section References

For comprehensive details, refer to the following specifications collected from parallel subagent analysis:

### CAS Specification (src/cas/)

**14 Detailed Sections**:
1. Module Architecture Overview
2. Public API Surface (traits, types, aliases)
3. Core Type Invariants & Constraints
4. Actor-Based Orchestration
5. Storage Backends (FileSystemCas, InMemoryCas)
6. Index & Persistence
7. Codec & Delta Encoding
8. Error Handling
9. Performance Characteristics
10. Testing Approach
11. Documentation Requirements
12. Integration Boundaries (Conductor, mediapm)
13. Configuration & Deployment
14. Future Extension Points

**Key Takeaways**:
- **Module Structure**: 8 submodules (api, cli, error, hash, codec, index, orchestration, storage)
- **Public Traits**: `CasApi` (read/write/maintain), `CasMaintenanceApi` (optimize/prune/repair)
- **Type Model**: `Hash` (Blake3 multihash), `Constraint` (base selection), `ObjectInfo` (metadata)
- **Storage**: `FileSystemCas` (persistent), `InMemoryCas` (ephemeral)
- **Versioning**: Adjacent-only migrations; optics-based bridging
- **Performance**: O(1) full objects, O(depth) delta objects; mmap for ≥64KB

### Conductor Specification (src/conductor/)

**15 Detailed Sections**:
1. Module Structure & Responsibilities
2. Public API Surface (ConductorApi trait, SimpleConductor)
3. Configuration Model (3-document pattern)
4. State Management
5. Orchestration Runtime Architecture
6. Builtin Tool Integration
7. Configuration Document Versioning
8. CLI Structure
9. Error Handling
10. Testing Approach
11. Documentation & Contracts
12. Cross-Crate Integration
13. Performance Expectations
14. Known Limitations & Future Directions
15. Key Files Reference Table

**Key Takeaways**:
- **3-Document Pattern**: user (intent), machine (setup), state (volatile)
- **Public Trait**: `ConductorApi` with workflow execution, state inspection, diagnostics
- **Orchestration**: Actor-based (ractor); step-stream batch dispatch (`StreamBatch`/`StreamStep`/`StepOutcome`); adaptive cost model
- **State Model**: `OrchestrationState` with tool call instances; persisted in CAS
- **Versioning**: Explicit version markers; optics-based migration
- **Performance**: < 10ms planning; adaptive scheduler for load balancing

**Step dispatch and cache probe**:
- Coordinator collects ready steps across active workflows into a `StreamBatch`
  (`Vec<StreamStep>`), where each `StreamStep` carries `workflow_name`, `step_id`,
  `step`, and `outputs`.
- The execution hub dispatches these steps concurrently via `execute_batch`,
  bounded by a semaphore, enabling cross-workflow parallelism within a batch.
- `StepOutcome { step_id, result }` outcomes are routed back to individual
  workflow states.
- The step-worker cache probe uses `cas.exists_many(check_hashes)` →
  `CasExistenceBitmap` (backed by `BitVec`) instead of sequential per-output
  `cas.exists()` calls, reducing CAS round-trips from O(output_count) to O(1).

**Step-stream dedup and trace semantics**:
- Steps from multiple workflows started simultaneously do not see each other's
  in-flight cache entries, so naturally-identical steps across workflows may
  both execute (`executed_instances=N`) instead of one caching off the other
  (`executed_instances=1`, `cached_instances=N-1`). This is inherent to
  parallel dispatch, not a bug.
- Step-stream dispatch bypasses `plan_level()` and `begin_level_metrics()`. The
  scheduler's `runtime_diagnostics()` must therefore fall back to
  `max(self.worker_pool_size, worker_metrics.len())` when
  `begin_level_metrics()` was never called (worker_pool_size defaults to 0).
- `assigned_steps_total` is incremented in the step-stream path via
  `record_completion()` using `saturating_add(1)`, since the stream dispatch
  does not go through the sequential assignment tracking.
- Trace events differ by dispatch path: `LevelPlanned` and `StepAssigned` are
  only emitted by the legacy `plan_level` / `execute_level` sequential path.
  The step-stream path emits only `StepCompleted` for completed step outcomes.

### §16 Workflow Progress Display

Conductor uses `pulsebar` (via `MultiProgress`) to display workflow execution
progress. Each workflow step gets a progress bar. On completion, bars are not
marked as finished through `finish_success`/`finish_error`; instead, the pattern
from `provision.rs` is used: `set_message("ready")` or `set_message("failed")`
(to display the final state) without an explicit finish call. This avoids
pulsebar's behavior of appending a render-time-clock-based elapsed duration to
finished rows.

- Format strings intentionally omit `{elapsed}` so finished bars show no
  ticking duration.
- `bar.finish_error("failed")` → `bar.set_message("failed")` (bars stay in
  Running state; no finished-line render).
- `bar.finish_success("ready")` → `bar.set_message("ready")` (similarly avoids
  the finished-line render).
- The constant `WORKFLOW_PROGRESS_SETTLE_MS` (75 ms) ensures the background
  render thread has time to flush final bar states before `MultiProgress` is
  dropped.
- Zero-step workflows still create bars (no format string set, handled by
  pulsebar defaults).


### Conductor-Builtins Specification (src/conductor-builtins/)

**9 Detailed Sections**:
1. Shared Builtin Framework
2. CLI Convention Contract
3. API Input/Output Contract
4. Validation & Error Semantics
5. Success Payload Format Rules
6. Builtin Specifications (5 detailed specs)
7. Testing Patterns
8. Integration Boundaries
9. Documentation Requirements

**Key Takeaways**:
- **5 Builtins**: echo (pure), archive (pure), fs (impure), import (impure), export (impure)
- **CLI Contract**: `--arg KEY VALUE` keyed pairs; fail-fast validation
- **API Contract**: `BTreeMap<String, String>` params + optional binary inputs
- **Purity Rules**: Pure = deterministic payload; impure = side-effect primary
- **Determinism**: `echo` and `archive` produce identical output for same input
- **Path Safety**: Relative/absolute modes; rejects traversal (`..`), absolute in relative mode

**Builtin Specs**:
1. **echo**: String pass-through to stdout/stderr; pure
2. **fs**: Directory/file creation; impure
3. **archive**: ZIP pack/unpack/repack; pure
4. **import**: File/folder/URL/CAS ingestion; impure
5. **export**: Payload materialization to disk; impure

### MediaPM Specification (src/mediapm/)

**17 Detailed Sections**:
1. Overview & Purpose
2. Application Module Hierarchy
3. Public API Surface
4. Configuration Schema (mediapm.ncl)
5. Runtime Path Resolution
6. Media-Source & Hierarchy Patterns
7. Runtime State Management & Persistence
8. Tool Provisioning & Managed-Tool Integration
9. CLI Command Routing & Implementation
10. Cross-Crate Integration Boundaries
11. Cache Organization & Link Order
12. Materialization Rules & Output Variants
13. Performance Expectations & Hot Paths
14. Testing Approach & Coverage
15. Documentation & API Contract Requirements
16. Key Invariants & Contracts
17. Error Taxonomy

**Key Takeaways**:
- **Three-Layer Composition**: CAS (identity) + Conductor (orchestration) + MediaPM (policy)
- **Configuration**: `mediapm.ncl` (user), `state.ncl` (machine), with versioning
- **Module Hierarchy**: 13 submodules (config, lockfile, paths, conductor_bridge, materializer, tools, etc.)
- **Public Trait**: `MediaPmApi` with `process_source()` and `sync_library()`
- **State Management**: Staged → commit atomicity; lock records for cache hits
- **Tool Provisioning**: User-level cache (downloads) vs. workspace cache (extracted binaries)
- **Materialization**: Link order preference (hardlink → symlink → reflink → copy)
- **Hierarchy path sanitization**:
  - `hierarchy[*].sanitize_names` controls reserved-character replacement in
    materialized hierarchy paths,
  - `SanitizeNamesConfig` has four variants: `Disabled`, `Inherit`, `Enabled`, and
    `Custom(…)` for per-character mapping overrides,
  - Serialization: `Disabled` → `false`, `Inherit` → `"inherit"`, `Enabled` → `true`,
    `Custom(…)` → `{ "<": "_", ... }` (single-character key-value map),
  - `Inherit` (default): inherit from parent hierarchy node; the root seed is
    `Enabled`,
  - `Enabled`: replace reserved characters using the effective mapping (runtime
    defaults merged with per-entry `Custom` overrides),
  - `Disabled`: skip reserved-character replacement entirely (reserved chars are
    still rejected by the subsequent validation step),
  - `Custom(…)`: merge per-character custom mapping over `runtime.path_sanitization`
    defaults,
  - the default changed from `Disabled` to `Inherit` during the Inherit variant
    introduction,
  - NFD normalization is always enforced regardless of sanitize_names setting,
  - the replacement occurs after NFD normalization but before reserved-char
    validation so replaced paths always pass strict validation,
  - resolved `rename_files` replacement strings are also sanitized using the
    same effective replacement map before materialization,
- **Hierarchy flattening dedup**:
  - the dedup key during flattening is `(template_path, media_id)`, not
    `template_path` alone,
  - hierarchy entries at the same template path with different `media_id`
    values are NOT duplicates — `${media.id}` placeholders resolve to
    different paths during materialization,
  - hierarchy nodes at the same path may declare the same output variants as
    long as their `rename_files` rules differ (different `rename_files` produce
    distinct final filenames at materialization time),
  - overlapping variants with identical `rename_files` at the same path are
    rejected as duplicates,
  - `rename_files` coexistence is supported by materializer isolation: each
    `media_folder` entry gets an independent staging directory so that
    per-entry `rename_files` rules can produce files without cross-entry
    overwrite conflicts.
- **Hierarchy node ID suffix convention**: tagged media nodes carry no
  dedicated suffix (bare media id), while untagged media variants carry the
  `.untagged` suffix. This ensures tagged nodes sort before untagged variants
  and makes the variant role explicit. Sidecar and container nodes use their
  own descriptive suffixes (`.media_folder`, etc.).
- **Hierarchy preset do-not-overwrite by id**: `insert_hierarchy_preset_node()`
  skips insertion when the incoming node (or any of its children) has an `id`
  that already exists anywhere in the hierarchy tree. This prevents accidental
  overwrite of user-defined nodes by preset entries.
- **Hierarchy preset nameless-folder merge**: when the incoming preset node
  has both `id: None` and `media_id: None` (a pure container) and exactly one
  existing matching folder at the same path also has both fields `None`, the
  preset children are merged into the existing folder instead of being inserted
  as a duplicate sibling. This prevents parent-folder duplication when a user
  manually created a container folder at the same path that a preset targets.
- **Hierarchy preset overwrite flag**: `insert_hierarchy_preset_node()`
  accepts an `overwrite: bool` parameter. When `true`, existing hierarchy
  nodes with matching ids (top-level or child) are removed before insertion,
  bypassing the do-not-overwrite guard. The CLI exposes this as
  `mediapm hierarchy add --overwrite`.
- **Media source registration do-not-overwrite by default**:
  `add_media_source()` and `add_local_source()` use `overwrite: false` by
  default — they check whether the target `media_id` already exists in
  `document.media` and return successfully without modifying the entry.
  The service-layer methods `add_media_source_with_position()` and
  `add_local_source_with_position()` accept an `overwrite: bool` parameter;
  when `true`, the existing entry is replaced unconditionally. The CLI exposes
  this as `mediapm media add --overwrite`.
- **Local media ID from CAS hash**: `media_id_from_local_path()` no longer
  uses nanoid-based random suffixes. Local media IDs now derive from the CAS
  content hash of the source file:
  `media_id_from_local_path(hash: &mediapm_cas::Hash)` produces
  `local.<first-12-hex-chars-of-hash>`. This makes local media IDs
  deterministic (same file → same media ID) and removes the `rand` dependency
  from mediapm's public API surface.
- **Empty directory cleanup after stale hierarchy removal**: after removing
  stale materialized paths, the materializer walks up from each removed path's
  parent directory, removing directories that contain no files (recursively),
  stopping at `hierarchy_root_dir`. The count of removed empty directories is
  reported via `MaterializeReport.removed_empty_dirs` →
  `SyncSummary.removed_empty_dirs`, which is logged at CLI level.
- **yt-dlp companion path env template refs**: managed yt-dlp companion paths
  (ffmpeg, deno) are injected as `${env.MEDIAPM_YT_DLP_FFMPEG_LOCATION}` and
  `deno:${env.MEDIAPM_YT_DLP_JS_RUNTIMES}` in `input_defaults`. The resolved absolute
  paths are stored in `generated_runtime_env_vars` and written to
  `<conductor_dir>/.env.generated` (never to persisted config documents). The
  machine document's `runtime.inherited_env_vars` for the active platform is
  augmented with the generated variable names so conductor inherits them at
  execution time. Absolute paths may only leak via generated env files; they
  must never appear in any other persisted configuration or cached state.
- **Tool identity preservation during workflow re-synthesis**: when sync runs
  against a previously-synthesized workflow, `preserve_existing_generated_step_tools()`
  rewrites each generated step's tool id from the existing workflow snapshot.
  The function implements a 2-way decision per step:
  - If `previous.tool == generated.tool`: the tool id is kept as-is; validity
    is checked via `preserved_step_tool_is_valid()` (ensures the tool still
    exists in `machine.tools` and `Executable` kinds have non-empty `content_map`
    in `machine.tool_configs`).
  - If the tool identity differs from the previously-synthesized one, mismatch
    is flagged (`all_matched = false`) to trigger a refresh cascade that
    installs the newly-generated identity. This applies uniformly to all tools
    regardless of name — any tool's identity can encode volatile fields such as
    same-step companion selectors, dependency versions, or provision timestamps.
  - Returns `true` when every generated step id was found in `existing` and
    the tool id is unchanged and still valid.
- **Dependency selector inheritance validation**: `ensure_inherit_dependency_target_is_configured()`
  enforces that `inherit`/`global` selectors on tool dependencies
  (e.g. `tools.yt-dlp.dependencies.ffmpeg_version = "inherit"`) require the
  target tool to be defined in `tools.<dependency_tool_name>`. If the target
  tool is missing, validation fails with an explicit error pointing to the
  missing configuration. Only `rsgain`, `yt-dlp`, and `media-tagger` may define
  dependency selectors; other tools that attempt selector definitions are
  rejected. The validation runs for all configured tool dependencies during
  document load.

---

### §18 Hierarchy Sync Progress Display

MediaPM uses `pulsebar` (via `MultiProgress`) during hierarchy sync to display
per-worker materialization progress and overall hierarchy completion. The same
pattern from `provision.rs` is used: format strings omit `{elapsed}`, and
completion is signaled via `set_message` + `set_position` rather than
`finish_success`/`finish_error`.

- Hierarchy progress bar format: `"{msg}  {bar}  {pos}/{total}"` (no elapsed).
- Worker progress bar format: `"{msg}  [{bar:18}]  {pct}"` (no elapsed).
- `worker_bar.finish_success(...)` → `worker_bar.set_position(100)` +
  `worker_bar.set_message(...)` (fills the progress bar and updates the
  message without triggering a finished-line render).
- `hierarchy_progress.finish_success("done")` →
  `hierarchy_progress.set_message("done")` (the bar position is already at
  its total through `hierarchy_progress.advance(1)` per entry).
- A 75 ms settle delay (matching the conductor pattern) allows the render
  thread to flush final state before `MultiProgress` is dropped.

---

## Versioning Policy Across Crates

### Schema Versioning (All Crates)

**Principle**: Explicit version markers in persisted files; sequential migrations only

| Crate | Document Type | Version Field | Migration Path |
|-------|---|---|---|
| **CAS** | Object envelope, index schema | Embedded in wire format | `codec/versions/`, `index/versions/` |
| **Conductor** | User/machine/state documents | Top-level `version: u32` | `model/config/versions/`, `model/state/versions/` |
| **MediaPM** | `mediapm.ncl`, `state.ncl` | Top-level `version: u32` | `config/versions/`, `lockfile/versions/` |

**Rules** (all crates):
1. Each persisted schema carries explicit version marker
2. Migrations happen via optics (fp-library) + version-specific ISO modules
3. No speculative forward-compatibility; only N → N+1 migrations
4. Failed migrations fail fast (no silent degradation)
5. Every version file includes `DO NOT REMOVE` guard (CI enforces)

---

## Error Handling Across Crates

### Error Propagation Model

```
CAS Errors
├─ NotFound(hash)
├─ InvalidConstraint(msg)
├─ OutOfSpace (triggers prune)
└─ Codec, InvariantViolation, ...

Conductor Errors
├─ Workflow(msg) — intent/execution failure
├─ Cas(CasError) — delegated
├─ Serialization(msg) — document encoding
└─ Io { operation, path, source }

Builtin Errors
├─ CLI: exit code + stderr
└─ API: Result<T, String> (fail-fast before side effects)

MediaPM Errors
├─ Workflow(msg) — policy/state failure
├─ Conductor(ConductorError) — delegated
├─ Io { operation, path, source }
└─ Serialization(msg)
```

**Contract**:
- **Fail-fast**: Validation before execution; no partial state on error
- **Atomicity**: CAS failures in pure workflows trigger one-shot retry; impure workflows fail immediately
- **Recovery**: MediaPM staged directory rollback on failure; state.ncl unchanged
- **Diagnostics**: Error messages include actionable context (path, hash, expected vs. actual)

**Error detection patterns**:
- `CorruptWorkflowOutput` uses `#[error(transparent)]` which delegates its
  `Display` implementation entirely to the inner `CorruptWorkflowOutputContext`.
  The inner context format is `"workflow '{name}' step '{id}' failed to read
  output ... due to CAS corruption: {detail}"` and does **not** contain the
  word "impure". To detect impure-workflow corruption errors, use
  `matches!(error, ConductorError::CorruptWorkflowOutput(_))` rather than
  string containment checks.

---

## Testing Strategy Across Crates

### Unified Test Organization

All crates follow this pattern:
```
tests/
├── tests.rs              # Entry module
├── e2e/                  # End-to-end workflows
├── int/                  # API-level integration
└── prop/                 # Property-based (reserved)
```

### Coverage Expectations

| Category | CAS | Conductor | Builtins | MediaPM |
|----------|-----|-----------|----------|---------|
| **Happy Path** | put → get | user → machine → execute | valid args → correct output | sync roundtrip |
| **Validation** | Constraint logic | Document merging | Fail-fast keys | Lock reconciliation |
| **Error Paths** | NotFound, Codec | Corrupt output | Invalid input | Staged rollback |
| **Determinism** | Hash stability | State stability | Pure output consistency | Idempotent sync |
| **Concurrency** | Concurrent puts | Actor coordination | N/A | Parallel materialization |

---

## Common Patterns & Idioms

### 1. Type-Erased API Traits

**Pattern**: Core functionality exposed via traits; implementations plugged in

**Examples**:
- `CasApi` implemented by `FileSystemCas`, `InMemoryCas`
- `ConductorApi` implemented by `SimpleConductor`
- `MediaPmApi` implemented by `MediaPmService`

**Benefit**: Backend-agnostic; testable with in-memory implementations

### 2. Async Actor Orchestration

**Pattern**: Actor-based coordination for long-running, concurrent work

**Used By**: CAS, Conductor (not mediapm, which is pure async/sync)

**Benefit**: Message-driven, bounded concurrency, natural failure handling

### 3. Three-Document Configuration

**Pattern**: User intent + machine setup + volatile state in separate documents

**Used By**: Conductor, MediaPM

**Benefit**: Clear ownership; enables tooling (user edits, CI-friendly diffs)

### 4. Content-Addressed State Persistence

**Pattern**: Serialize state to deterministic bytes; store in CAS; pointer in volatile doc

**Used By**: Conductor, MediaPM (implicitly)

**Benefit**: Deduplication, integrity verification, fast cache hits

### 5. Optics-Based Versioning

**Pattern**: fp-library optics for isomorphic wire ↔ runtime type conversion

**Used By**: CAS codecs, Conductor schemas, MediaPM configs

**Benefit**: Type-safe; explicit migrations; no ad-hoc serialization

### 6. Staged-and-Commit Materialization

**Pattern**: Write to temp directory, validate, then atomic rename to final location

**Used By**: CAS, MediaPM (via conductor)

**Benefit**: Atomicity; clean rollback on failure; no orphaned partial state

---

## Performance Considerations

### Hot Paths

| Path | Target | Technique |
|------|--------|-----------|
| **CAS read** (full object) | O(file_size) | mmap for ≥64KB; buffer pool for small |
| **CAS delta read** | O(depth × patch_size) | Concurrent candidate scoring (8 tasks) |
| **Conductor planning** | < 10ms | Level-based topological sort (no DAG simulation) |
| **Conductor scheduling** | EWMA cost model + O(1) batch cache probe | Step-stream batch dispatch; `exists_many` via `CasExistenceBitmap` |
| **MediaPM sync** | Parallel workflows + step-stream dispatch | Bounded worker pool; cross-workflow step-stream dispatch in execution hub |

### Resource Bounds

| Resource | Default | Config |
|----------|---------|--------|
| Delta chain depth | 32 | `MAX_DELTA_DEPTH` |
| Buffer pool size | 128 | `FILESYSTEM_STREAM_BUFFER_POOL_MAX_BUFFERS` |
| Actor RPC timeout | 8 sec | `FILESYSTEM_OBJECT_ACTOR_RPC_TIMEOUT_MS` |
| Optimizer concurrency | 8 | `FILESYSTEM_CANDIDATE_EVAL_CONCURRENCY` |
| Materialization workers | CPU cores | Derived from hardware |

**pulsebar rendering:**
- terminal-width contract: all progress messages must fit within the terminal
  width; detected via `terminal_size` crate; defaults to 80 cols,
- step preview degrades gracefully (truncation with `...` suffix,
  `+N more` counter, ...) to respect the available width.

---

## Future Extension Points

### 1. New CAS Hash Algorithms
- Add variant to `HashAlgorithm` enum
- Implement multihash trait
- Update multicodec code table

### 2. New Builtin Tools
- Create `src/conductor-builtins/<name>/`
- Implement API contract (`BTreeMap` in/out)
- Register in `registered_builtin_ids()`

### 3. New Managed Tools
- Add to downloader catalog
- Define tool spec in `mediapm.ncl`
- Tool sync handles provisioning

### 4. New Workflow Execution Backends
- Implement `ConductorApi` trait
- Pass to `MediaPmService::new(...)`
- Swap without changing caller code

### 5. New Output Variant Kinds
- Add to `OutputVariantKind` enum
- Update hierarchy materialization
- Extend CLI/API output handling

### 6. Index-Backed Existence Checks

**Status**: Design proposal — not yet implemented.

**Motivation**: Current `exists()` and `exists_many()` delegate to the storage
backend, which for `FileSystemCas` means a stat(2) syscall per hash. For batch
probes (for example, conductor cache probe with many outputs), this creates an
O(output_count) syscall storm. An index-backed design would serve existence
checks entirely from memory.

**Proposed `IndexState` API**:

```rust
impl IndexState {
    /// Returns true when `hash` is known to exist in storage.
    ///
    /// Guarantees:
    /// - `true` means the object is retrievable (no false positives),
    /// - `false` means the object may still exist (conservative — caller
    ///   must fall through to storage for a definitive answer).
    pub fn contains(&self, hash: &Hash) -> bool { ... }

    /// Batch variant — checks up to `hashes.len()` entries in one call.
    pub fn contains_many(&self, hashes: &[Hash]) -> CasExistenceBitmap { ... }
}
```

**Index invalidation strategy**:
- The index is populated lazily on first existence check, then incrementally
  updated as new objects are stored.
- Object removal (prune, GC) removes entries from the index synchronously.
- Index rebuild is triggered on startup if the stored index version differs
  from the code version.

**Accepted guarantee trade-off**: False negatives are acceptable (index misses
fall back to storage). False positives are NOT acceptable — `contains(hash) == true`
must always be correct. This is enforced by:
- Index entries are only added after successful `put()` or confirmed
  storage-layer `exists()`,
- Index entries are removed synchronously during delete operations,
- On-disk index persistence uses the same atomic-commit pattern as the
  object store.

**Integration with Conductor**: The `exists_many` method on `CasApi` would
first query the index, then batch-check any remaining unknowns against storage.
This split ensures the index remains a pure optimization: correctness does not
depend on it.

**Performance target**:
- Hot index (fits in RAM): O(1) per check, zero syscalls,
- Cold index (first run, partial load): O(misses) stat(2) calls plus batch fill,
- Expected throughput: 10,000+ checks per millisecond on modern hardware.

---

## Key References & Documentation

### CAS Reference
- **Public Traits**: `CasApi`, `CasMaintenanceApi`
- **Types**: `Hash`, `Constraint`, `ObjectInfo`, `OptimizeReport`
- **Backends**: `FileSystemCas`, `InMemoryCas`
- **Performance**: O(1) full, O(depth) delta; mmap + buffer pool

### Conductor Reference
- **Public Trait**: `ConductorApi`
- **Implementation**: `SimpleConductor`
- **Schemas**: 3-document (user, machine, state)
- **Execution**: Actor-based, step-stream batch dispatch, adaptive scheduling, `CasExistenceBitmap` cache probe

### Builtins Reference
- **Framework**: CLI contract (`--arg`), API contract (`BTreeMap`)
- **5 Tools**: echo, fs, archive, import, export
- **Purity**: Pure (echo, archive) vs. impure (fs, import, export)
- **Validation**: Fail-fast; undeclared keys rejected immediately

### MediaPM Reference
- **Public Trait**: `MediaPmApi`
- **Implementation**: `MediaPmService`
- **Schemas**: mediapm.ncl (user), state.ncl (machine)
- **Materialization**: Staging → validation → commit; atomic

---

This document serves as the integration reference. Refer to crate-specific AGENTS.md files for detailed implementation guidance per crate.

---

# PART 2: HUMAN-READABLE GUIDE & QUICK START

## Executive Summaries

### CAS: Content-Addressed Storage

**In 30 seconds**: CAS is mediapm's durable, content-identified object store. Every piece of data (file, media, config, state) hashes to a unique Blake3-256 hash. Clients call `put()` to store bytes and `get()` to retrieve them. CAS automatically deduplicates identical content, optimizes storage via delta encoding, and maintains index integrity. It's the foundation that makes Conductor workflows deterministic and MediaPM sync idempotent.

**Who uses it**: Conductor (stores workflow state), MediaPM (materializes media files), and internally within CAS for self-referential metadata.

**Core contract**: Bytes in → hash; hash in → bytes out. Same input always produces same hash. Retrieved bytes are byte-identical to stored.

**Key insight**: CAS is the source of truth for identity and deduplication. If two media sources have identical content, CAS stores it once and references it twice.

### Conductor: Deterministic Workflow Orchestration

**In 30 seconds**: Conductor is mediapm's deterministic workflow engine. You define steps (shell commands, tool invocations, transformations) in a Nickel config file. Conductor executes them in order, captures output to CAS, and makes execution deterministic so identical input always produces identical output. It persists its state to CAS so you can pause, inspect, and resume workflows. Built-in tools (echo, fs, archive, import, export) handle common operations; managed tools (ffmpeg, yt-dlp, media-tagger) are registered and provisioned at runtime.

**Who uses it**: MediaPM (synthesizes workflows from media definitions), developers (build custom workflows).

**Core contract**: Pure workflows are deterministic. Impure workflows (file I/O, network) may vary on retries. Execution fails fast on conflicts or data errors.

**Key insight**: Nickel configs are executable blueprints. The same workflow definition, given the same input, always produces the same output. This enables caching, resume, and reliable batch processing.

### Conductor-Builtins: Standard Tools

**In 30 seconds**: Five small, focused tools that Conductor uses as building blocks: **echo** (string passthrough), **fs** (directory/file operations), **archive** (ZIP pack/unpack), **import** (file/URL ingest to CAS), **export** (CAS materialization to disk). Each follows the same CLI and API contract: keyed arguments, fail-fast validation, deterministic output (for pure tools) or clean side effects (for impure). They're the primitives that enable Conductor workflows to compose larger operations.

**Who uses it**: Conductor (builtin registry), MediaPM (workflow synthesis), advanced users (custom workflows).

**Core contract**: Identical inputs produce identical CLI behavior. Undeclared keys are rejected immediately. Pure tools produce deterministic payloads; impure tools succeed or fail cleanly (no partial state).

**Key insight**: Builtins are modular and composable. Combine import → archive → export for file transformation workflows.

### MediaPM: Media Library Façade

**In 30 seconds**: MediaPM is the user-facing application that ties CAS + Conductor + policy together into a media management system. You define media sources, processing workflows, and organization hierarchy in `mediapm.ncl`. MediaPM downloads/ingests media, runs it through Conductor workflows, and materializes the output to organized folders. It tracks what's been processed via a lock file, so re-running sync skips unchanged media. It provisions managed tools (ffmpeg, yt-dlp, media-tagger) on demand.

**Who uses it**: End users (media library management), integrations (batch processing).

**Core contract**: Sync is all-or-nothing for each media entry. If processing succeeds, files are materialized atomically. If it fails, nothing is materialized. Lock records track what's been done.

**Key insight**: MediaPM is policy on top of orchestration. Same Conductor engine; different Nickel policy → different behavior. Swap policy by editing `mediapm.ncl`.

---

## Quick Start Guide (5 Minutes Per Crate)

### CAS Quick Start: Store & Retrieve

```rust
// 1. Create an in-memory CAS for testing
use mediapm_cas::api::CasApi;
use mediapm_cas::inmemory::InMemoryCas;

let cas = Arc::new(InMemoryCas::new());

// 2. Store bytes
let data = b"Hello, CAS!";
let hash = cas.put(Bytes::from_static(data)).await?;
println!("Stored hash: {}", hash); // blake3 multihash

// 3. Retrieve bytes
let retrieved = cas.get(&hash).await?;
assert_eq!(&retrieved[..], data);

// 4. Attempt to retrieve non-existent hash → error (NotFound)
let fake_hash = Hash::from_hex("cafe...")?;
match cas.get(&fake_hash).await {
    Err(CasError::NotFound(_)) => println!("Not found, as expected"),
    _ => panic!("Expected NotFound"),
}
```

**5-minute checklist**:
- [ ] Create CAS instance (in-memory or filesystem)
- [ ] Call `put()` with bytes
- [ ] Retrieve the hash from the result
- [ ] Call `get(hash)` to retrieve bytes
- [ ] Verify bytes are identical to input
- [ ] Try `get()` with a fake hash; expect NotFound error

### Conductor Quick Start: Define & Execute Workflow

```nickel
# conductor.ncl (user-provided workflow)
{
  version = 1,
  tools = {
    echo = { version = "1.0.0" },
  },
  workflows = {
    greet = {
      steps = [
        {
          id = "step_1",
          tool = "echo",
          args = {
            message = "Hello from Conductor!",
            output = "stdout",
          },
        },
      ],
    },
  },
}
```

```rust
// 1. Create Conductor with CAS
let conductor = SimpleConductor::new(Arc::new(cas)).await?;

// 2. Load workflow config from above
let workflow_spec = /* parse conductor.ncl */;

// 3. Execute workflow
let result = conductor.run_workflow("greet").await?;
println!("Output: {}", result.final_output);

// 4. Inspect state (internal step outputs)
let state = conductor.get_state("greet").await?;
for step in state.steps {
    println!("Step {}: {:?}", step.id, step.status);
}
```

**5-minute checklist**:
- [ ] Write Nickel workflow (tools + steps)
- [ ] Create SimpleConductor with CAS
- [ ] Load workflow and execute
- [ ] Check final output or state
- [ ] Inspect step-level details in OrchestrationState

### Builtins Quick Start: CLI & API

```bash
# Echo builtin (CLI)
$ mediapm-conductor-builtin-echo \
    --arg message "Hello World" \
    --arg output stdout
Hello World

# Archive builtin (CLI) - pack directory to ZIP
$ mediapm-conductor-builtin-archive \
    --arg action pack \
    --arg path /tmp/mydir \
    --input /path/to/output.zip
# Creates /path/to/output.zip with contents of /tmp/mydir

# Import builtin (CLI) - ingest file to CAS
$ mediapm-conductor-builtin-import \
    --arg source /tmp/video.mp4 \
    --arg dest cas://
# Outputs JSON: { "cas_hash": "blake3:..." }

# Export builtin (CLI) - retrieve from CAS to disk
$ mediapm-conductor-builtin-export \
    --arg cas_hash "blake3:..." \
    --arg dest /tmp/output.mp4
# Materializes CAS object to /tmp/output.mp4
```

**API usage** (Rust):
```rust
use mediapm_conductor_builtin_archive::api::archive;
use std::collections::BTreeMap;

let mut args = BTreeMap::new();
args.insert("action".to_string(), "pack".to_string());
args.insert("path".to_string(), "/tmp/mydir".to_string());

let payload = std::fs::read("/path/to/output.zip")?;
let result = archive(&args, Some(&payload)).await?;
// result: String (success msg) or error
```

**5-minute checklist**:
- [ ] Choose a builtin (echo, fs, archive, import, or export)
- [ ] Call CLI with `--arg KEY VALUE` pairs
- [ ] Verify output or file creation
- [ ] If impure builtin, check side effects (files created, network requests)
- [ ] Verify fail-fast: undeclared `--arg` rejected immediately

### MediaPM Quick Start: Define Media & Sync

```nickel
# mediapm.ncl (user media configuration)
{
  version = 1,
  media_sources = {
    music = [
      {
        id = "song_1",
        title = "Example Song",
        source = "https://example.com/song.mp3",
      },
    ],
  },
  hierarchy = [
    {
      kind = "folder",
      name = "Music",
      children = [
        {
          kind = "media",
          id = "song_1",
          variant = "primary",
        },
      ],
    },
  ],
}
```

```rust
// 1. Create MediaPM service
let mediapm = MediaPmService::new(
    Arc::new(cas),
    Arc::new(conductor),
    "/path/to/.mediapm"
).await?;

// 2. Load configuration
mediapm.load_config_from_file("mediapm.ncl").await?;

// 3. Sync library (download, process, materialize)
let result = mediapm.sync_library().await?;
println!("Synced {} media entries", result.processed_count);

// 4. Check lock file (what was processed)
let locks = mediapm.get_lock_records().await?;
for lock in locks {
    println!("{}: {} (hash: {})", lock.media_id, lock.variant, lock.cas_hash);
}
```

**5-minute checklist**:
- [ ] Write `mediapm.ncl` with media sources and hierarchy
- [ ] Create MediaPmService with CAS and Conductor
- [ ] Call `sync_library()`
- [ ] Verify files materialized to output directory
- [ ] Check lock records (cache state)
- [ ] Re-run sync; verify it skips unchanged media

---

## Decision Rationale: Why These Design Choices?

### Why Content-Addressed Storage (CAS) Instead of Named Files?

**The Problem**: Media workflows involve many intermediate outputs (downloaded video, converted format, tagged metadata, thumbnail). Storing each by name makes deduplication hard, wastes space, and makes verification tedious.

**CAS Solution**: Every piece of data hashes to a Blake3-256 hash. Store once, reference many. Identical content (e.g., same video from different sources) is deduplicated automatically. Verification is trivial: recompute hash, compare to stored hash.

**Example**: Two media sources link to the same file. Without CAS, you'd store two copies. With CAS:
- Source A downloads file → hash abc123
- Source B downloads same file → hash abc123
- One stored object; both sources reference it

**Trade-off**: Names become hashes (less human-readable in storage). Solved by lock files that map (media_id, variant) → hash.

### Why Nickel for Configuration Instead of YAML/TOML/JSON?

**The Problem**: Workflow orchestration needs parameterization, conditionals, and reusable templates. YAML/TOML/JSON are static; they can't express "if OS is Linux, use ffmpeg; else use mediainfo."

**Nickel Solution**: Nickel is a configuration language with a type system and computation. You can define functions, conditionals, and data-driven config. Nickel files evaluate to JSON-compatible output that Conductor reads.

**Example**:
```nickel
let ffmpeg_path =
  if context.os == "windows"
  then "C:\\ffmpeg\\bin\\ffmpeg.exe"
  else "/usr/bin/ffmpeg"
;
{ tool_config.ffmpeg.path = ffmpeg_path }
```

**Trade-off**: Developers need to learn Nickel syntax. Benefit: expressive, type-checked, reusable templates.

### Why Staged-and-Commit Materialization Instead of Direct Writes?

**The Problem**: Media workflows are long-running. If a workflow crashes halfway through materializing 100 files, you're left with 47 partially-written files, corrupted lock state, and unclear recovery.

**Staged-and-Commit Solution**:
1. Write to temporary directory (staging area)
2. Validate all files (hashes, permissions)
3. Atomic rename entire directory to final location
4. Update lock file only after successful commit

If anything fails mid-way, staging directory is left untouched, final location is unchanged, lock file is unchanged.

**Example**:
```
Step 1: CAS → /tmp/mediapm_stage_XYZ/
        ├─ song.mp3 (from CAS)
        ├─ song.jpg (from CAS)
        └─ song.txt (from CAS)

Step 2: Validate hashes against lock records

Step 3: Crash? → No problem, /tmp/mediapm_stage_XYZ/ is orphaned.
        Original files untouched. Re-run sync → starts fresh.

Step 4: Success? → atomic mv /tmp/mediapm_stage_XYZ/ → /media/library/song/
        Update lock file

Step 5: Done. All files at final location, lock file consistent.
```

**Trade-off**: Requires temporary space; slightly slower (rename + lock write). Benefit: atomic, recoverable, no orphaned partial state.

### Why Three-Document Pattern (User, Machine, State) Instead of Single File?

**The Problem**: MediaPM needs to store user intent (what they want), machine setup (what tools are available), and runtime state (what's been processed). Mixing them in one file makes it hard to distinguish ownership and difficult to version migrate.

**Three-Document Solution**:
- **`mediapm.ncl`** (user): Media sources, hierarchy, preferences. User edits this.
- **`state.ncl`** (machine): Tool provisioning, cache paths, runtime setup. System generates this.
- **Lock file**: What's been processed (media_id, variant → CAS hash). System maintains this.

Each document can be versioned, migrated, and edited independently. Clear ownership prevents merge conflicts.

**Example Workflow**:
```
1. User edits mediapm.ncl (add new media source)
2. User runs `mediapm sync`
3. MediaPM reads mediapm.ncl (user intent)
4. MediaPM reads/creates state.ncl (machine setup from intent)
5. MediaPM reads lock file (what's been processed)
6. MediaPM syncs (only new media)
7. MediaPM updates lock file (not user-facing)
```

**Trade-off**: More files to manage. Benefit: clear separation of concerns, easier versioning, less lock contention.

### Why Actor-Based Orchestration for CAS?

**The Problem**: CAS may have multiple concurrent requests (from Conductor, from MediaPM, from tests). Direct synchronous calls would serialize all access. Concurrent access with locks would be error-prone.

**Actor Solution**: CAS internally uses actors (ractor) to serialize access to mutable state. Clients send messages; actors process them sequentially. This guarantees no race conditions on index mutations.

**Trade-off**: Slight latency (message round-trip). Benefit: thread-safe, no lock deadlocks, bounded concurrency (one actor per type).

### Why Fail-Fast Validation in Builtins?

**The Problem**: Builtins run inside Conductor workflows. If a builtin accepts invalid input but fails mid-execution, the entire workflow is compromised, and recovery is unclear.

**Fail-Fast Solution**: Builtins validate inputs before any side effects. Undeclared `--arg` keys are rejected immediately. Invalid values (e.g., out-of-bounds) are rejected before I/O.

**Example**:
```bash
# GOOD: Rejected immediately
$ mediapm-conductor-builtin-fs --arg operation mkdir --arg path /tmp/test --arg invalid_key val
Error: Unknown argument: invalid_key

# BAD (not allowed): Partial side effects
$ builtin-that-accepts-anything --arg operation mkdir --arg invalid_key val
# Creates /tmp/test, then fails on invalid_key → orphaned directory
```

**Trade-off**: Strict validation; no lenient fallbacks. Benefit: predictable, auditable behavior.

---

## Worked Examples: Realistic Scenarios

### Scenario 1: Download Video, Extract Audio, Tag Metadata

**Use Case**: User wants to download a YouTube video, extract audio to MP3, and tag it with music metadata.

**Config** (`mediapm.ncl`):
```nickel
{
  version = 1,
  media_sources = {
    music = [
      {
        id = "video_1",
        title = "Example Video",
        source = "https://www.youtube.com/watch?v=...",
      },
    ],
  },
  workflows = {
    audio_extraction = [
      { step = "download", tool = "import", options = { format = "bestvideo+bestaudio" } },
      { step = "merge_audio", tool = "ffmpeg", options = { output_format = "mp3" } },
      { step = "tag", tool = "media-tagger", options = { write_tags = "true" } },
      { step = "export", tool = "export", options = { destination = "audio" } },
    ],
  },
  hierarchy = [
    {
      kind = "media",
      id = "video_1",
      variant = "audio",
    },
  ],
}
```

**Execution Flow**:
1. `mediapm sync` loads config
2. Discovers `video_1` not in lock file
3. Creates workflow: import → ffmpeg → media-tagger → export
4. Conductor executes:
   - **import**: yt-dlp downloads video.mp4 → CAS hash `h1`
   - **ffmpeg**: Reads `h1` from CAS, extracts audio → MP3 → CAS hash `h2`
   - **media-tagger**: Reads `h2`, tags metadata → MP3 → CAS hash `h3`
   - **export**: Reads `h3`, materializes to `/media/audio/video_1_audio.mp3`
5. Updates lock: `{ media_id: "video_1", variant: "audio", cas_hash: "h3" }`
6. Done. Files materialized, lock updated.

**Re-run Sync**:
- User runs `mediapm sync` again
- Locks show `video_1` already processed with hash `h3`
- If `mediapm.ncl` unchanged, sync skips `video_1` (cache hit)
- If user updates `mediapm.ncl` (e.g., new tag values), conductor re-executes workflow and updates lock

**Key Insight**: CAS deduplicates across video_1 and other videos with same audio format. Lock file tracks what's been processed.

---

### Scenario 2: Parallel Media Processing with Failure Recovery

**Use Case**: User wants to sync 100 music videos in parallel, but one fails due to network error. Re-running sync should skip completed ones.

**Setup**:
- `mediapm.ncl` defines 100 media sources
- `state.ncl` sets `parallel_workers = 8`

**Execution**:
1. MediaPM launches 8 worker tasks
2. Workers process media in parallel; each task updates lock after success
3. Worker 3 fails on media_id `video_50` (network timeout)
4. MediaPM logs error, continues other workers
5. At end: 99 synced, 1 failed; lock shows 99 with hashes, video_50 unlisted
6. User investigates video_50 (network still down?)
7. User re-runs `mediapm sync`
8. MediaPM skips 99 (already in lock), retries video_50
9. If video_50 succeeds, lock updated; if still fails, user can skip it in config

**Key Insight**: Lock file enables crash recovery. Each successful sync atomically commits. Failed entries remain out of lock, so re-runs retry them.

---

### Scenario 3: Adding a New Builtin Tool

**Use Case**: Conductor has 5 builtins. Team wants to add a `convert` builtin for generic audio/video format conversion.

**Steps**:

1. **Create crate** (`src/conductor-builtins/convert/Cargo.toml`):
```toml
[package]
name = "mediapm-conductor-builtin-convert"
version = "1.0.0"

[[bin]]
name = "mediapm-conductor-builtin-convert"
path = "src/main.rs"

[dependencies]
mediapm-conductor-builtin-support = { path = "../support" }
```

2. **Implement API** (`src/conductor-builtins/convert/src/lib.rs`):
```rust
pub async fn convert(
    args: &BTreeMap<String, String>,
    _payload: Option<&[u8]>,
) -> Result<String, String> {
    // Validate inputs
    let input_format = args.get("input_format")
        .ok_or("missing input_format")?;
    let output_format = args.get("output_format")
        .ok_or("missing output_format")?;
    let cas_hash = args.get("input_hash")
        .ok_or("missing input_hash")?;

    // Call external tool (ffmpeg)
    let output = run_ffmpeg(input_format, output_format, cas_hash).await?;

    Ok(format!("{{ \"output_hash\": \"{}\" }}", output))
}
```

3. **Implement CLI** (`src/conductor-builtins/convert/src/main.rs`):
```rust
#[tokio::main]
async fn main() {
    let args = parse_cli_args();
    match convert(&args, None).await {
        Ok(msg) => println!("{}", msg),
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    }
}
```

4. **Register in Conductor**:
```rust
// In conductor/src/lib.rs
pub fn registered_builtin_ids() -> Vec<&'static str> {
    vec![
        "echo@1.0.0",
        "fs@1.0.0",
        "archive@1.0.0",
        "import@1.0.0",
        "export@1.0.0",
        "convert@1.0.0",  // NEW
    ]
}
```

5. **Test API parity**:
```rust
#[tokio::test]
async fn test_convert_api() {
    let mut args = BTreeMap::new();
    args.insert("input_format".to_string(), "mp3".to_string());
    args.insert("output_format".to_string(), "wav".to_string());
    args.insert("input_hash".to_string(), "blake3:...".to_string());

    let result = convert(&args, None).await.unwrap();
    assert!(result.contains("output_hash"));
}
```

6. **Test CLI invocation**:
```bash
$ mediapm-conductor-builtin-convert \
    --arg input_format mp3 \
    --arg output_format wav \
    --arg input_hash blake3:...
{"output_hash": "blake3:..."}
```

**Key Insight**: New builtins follow the same CLI/API contract. Fail-fast validation, deterministic outputs (if pure), and clean error handling.

---

### Scenario 4: Workspace Migration (Move `.mediapm` Directory)

**Use Case**: User has processed 500 media items at `~/.mediapm`. They want to move it to `/data/mediapm` on a new disk.

**Current State**:
- `~/.mediapm/mediapm.ncl` (user config)
- `~/.mediapm/state.ncl` (machine setup)
- `~/.mediapm/lock.ncl` (processing records)
- `~/.mediapm/store/` (CAS store with all objects)
- `~/.mediapm/output/` (materialized files)

**Migration Steps**:

1. **Copy files**:
```bash
$ cp -r ~/.mediapm /data/mediapm
```

2. **Update runtime path** (if config has hardcoded paths):
```bash
# Edit /data/mediapm/mediapm.ncl
# Change any hardcoded ~/.mediapm references to /data/mediapm
```

3. **Verify CAS integrity**:
```rust
let cas = FileSystemCas::new("/data/mediapm/store")?;
let report = cas.repair_index().await?;
println!("Repaired: {}", report.fixed_entries);
```

4. **Test sync**:
```bash
$ mediapm sync --runtime-dir /data/mediapm
# Should read lock file, see all 500 processed, skip them (hash unchanged)
```

**Key Insight**: CAS store and lock file are portable. Paths are relative. Repaired index on migration detects any orphaned objects. Sync uses lock file for cache hits, so migration is fast (no re-processing).

---

### Scenario 5: Debugging a Failed Workflow Step

**Use Case**: Workflow crashes during media-tagger step. How to debug?

**Error Output**:
```
Error: Step media_tagger failed: exit code 1
Stderr: [media-tagger error details...]
```

**Debug Steps**:

1. **Check Conductor state**:
```rust
let state = conductor.get_state("sync_workflow").await?;
for step in state.steps {
    println!("{}: {:?}", step.id, step.status);
    if let Some(output) = &step.output {
        println!("  Output: {}", output);
    }
    if let Some(error) = &step.error {
        println!("  Error: {}", error);
    }
}
```

2. **Retrieve intermediate CAS objects**:
```bash
# Get hash of previous step (ffmpeg)
$ grep "previous_hash" conductor.state.ncl
# previous_hash = "blake3:abc123"

# Retrieve object from CAS
$ mediapm-conductor-builtin-export \
    --arg cas_hash "blake3:abc123" \
    --arg dest /tmp/debug_input.mp3

# Inspect it
$ file /tmp/debug_input.mp3
$ ffprobe /tmp/debug_input.mp3
```

3. **Run media-tagger directly**:
```bash
$ mediapm-conductor-builtin-media-tagger \
    --arg input_file /tmp/debug_input.mp3 \
    --arg tag_artist "Test" \
    --arg debug "true"
# See detailed error output
```

4. **Check tool version**:
```bash
$ mediapm-conductor-builtin-media-tagger --version
# Check against requirement in state.ncl
```

5. **Update config and retry**:
```nickel
# In mediapm.ncl, increase timeout for media-tagger
{ workflows.sync.steps.media_tagger.timeout_ms = 120_000 }
```

6. **Re-run sync**:
```bash
$ mediapm sync
# Conductor retries from failed step (or re-runs entire workflow if not supported)
```

**Key Insight**: Conductor state is inspectable. Intermediate objects are retrievable from CAS. Failed steps can be debugged in isolation by running the tool directly.

---

## Troubleshooting Guide

### Problem: CAS returns NotFound for a Hash I Just Stored

**Symptoms**:
```
Error: CAS NotFound(hash)
Expected: hash lookup after put() should work
```

**Causes**:
1. **Bug in hash computation**: Your bytes hashed differently than expected
2. **Wrong hash value**: Copy-paste error when passing hash to `get()`
3. **CAS instance mismatch**: Stored to `InMemoryCas`, retrieving from different instance
4. **Index corruption**: Rare; `put()` succeeded but index update failed

**Debug**:
```rust
// Verify hash is correct
let data = b"test data";
let hash1 = Hash::from_bytes(data)?;
let hash2 = cas.put(Bytes::from_static(data)).await?;
assert_eq!(hash1, hash2); // Should match

// Verify same CAS instance
let cas1 = Arc::new(InMemoryCas::new());
let hash = cas1.put(data).await?;
let _ = cas1.get(&hash).await?; // OK

let cas2 = Arc::new(InMemoryCas::new());
let _ = cas2.get(&hash).await?; // NotFound (different instance)
```

**Solution**:
- Use same CAS instance for put + get
- Verify hash computation with known value
- If FileSystemCas, check permissions (readable from index path)

---

### Problem: Conductor Workflow Hangs

**Symptoms**:
```
Workflow running for > timeout (e.g., 60 sec)
Steps not advancing
```

**Causes**:
1. **Builtin crash**: Tool exits without output; Conductor waits for response
2. **Network timeout**: Tool waiting for network; no timeout configured
3. **Deadlock**: Two steps depend on each other (DAG validation bug)
4. **Resource exhaustion**: Too many concurrent workers; blocked on I/O

**Debug**:
```rust
// Check step status
let state = conductor.get_state("workflow").await?;
for step in state.steps {
    let elapsed = step.started_at.elapsed();
    if elapsed > Duration::from_secs(10) {
        println!("Step {} running for {:?}", step.id, elapsed);
        println!("  Status: {:?}", step.status);
    }
}

// Run step directly to see if it's the tool or conductor
// (via builtin CLI)
```

**Solution**:
- Add timeout to step in config: `timeout_ms = 30_000`
- Check builtin logs (stderr)
- Verify DAG is acyclic (use `conductor.validate_workflow()`)
- Reduce concurrent workers if system is resource-constrained

---

### Problem: MediaPM Sync Partially Succeeds; Lock File Inconsistent

**Symptoms**:
```
Some files materialized; some not
Lock file has partial records
Re-running sync has unpredictable behavior
```

**Causes**:
1. **Crash during commit**: Staged directory committed, but lock file write failed
2. **Disk full**: Materialization succeeded, but lock write failed
3. **Permission error**: Earlier stages succeeded, later permission check failed

**Debug**:
```bash
# Check lock file for partial entries
$ cat ~/.mediapm/lock.ncl | grep -E "media_id|cas_hash"

# Check staged directory (orphaned partial writes)
$ ls ~/.mediapm/.staged/ 2>/dev/null

# Check permissions
$ ls -la ~/.mediapm/lock.ncl
```

**Solution**:
- Manually delete orphaned lock entries or re-run with `--force-resync`
- Clean up staged directory: `rm -rf ~/.mediapm/.staged/*`
- Verify disk space: `df -h ~/.mediapm`
- Verify write permissions: `touch ~/.mediapm/test_write`

---

### Problem: Builtin Reports "Unknown Argument"

**Symptoms**:
```
Error: Unknown argument: typo_in_key
```

**Causes**:
1. **Typo in --arg name**: `--arg output_format mp3` but builtin expects `--arg output format`
2. **Wrong builtin**: Used echo for operation that requires fs
3. **Outdated builtin version**: Argument was removed in newer version

**Debug**:
```bash
# Check builtin documentation
$ mediapm-conductor-builtin-echo --help
# Lists all valid --arg names

# Check builtin version
$ mediapm-conductor-builtin-echo --version
```

**Solution**:
- Check builtin CLI help for valid argument names
- Use correct builtin for operation (fs for file ops, archive for ZIP, etc.)
- Upgrade builtin if needed

---

### Problem: Deterministic Builtin Produces Different Output

**Symptoms**:
```
Same input, different output
Expected: Pure builtins (echo, archive) should be deterministic
```

**Causes**:
1. **Hidden environment dependency**: Tool reads system time or random seed
2. **Non-deterministic compression**: ZIP archive includes timestamps
3. **Floating-point precision**: Numeric operations vary by platform
4. **Bug in builtin**: Uninitialized variable or non-deterministic hash

**Debug**:
```bash
# Run same command twice
$ echo -n "test" | mediapm-conductor-builtin-archive \
    --arg action pack --input archive1.zip
$ echo -n "test" | mediapm-conductor-builtin-archive \
    --arg action pack --input archive2.zip

# Compare files
$ diff <(unzip -l archive1.zip) <(unzip -l archive2.zip)
$ diff <(blake3 archive1.zip) <(blake3 archive2.zip)
```

**Solution**:
- Archive builtin should use `--reproducible` flag if available (zip -r with no timestamps)
- Check builtin source for environment dependencies (time, random)
- File issue if determinism is violated

---

## Implementation Checklists

### Adding a New Builtin Tool

- [ ] Create `src/conductor-builtins/<name>/` directory
- [ ] Set up `Cargo.toml` with package and binary name
- [ ] Implement `lib.rs` with public API function (signature: `async fn(BTreeMap<String, String>, Option<&[u8]>) -> Result<String, String>`)
- [ ] Implement `main.rs` with CLI argument parsing (`--arg KEY VALUE` pairs)
- [ ] Add validation: Reject undeclared `--arg` keys immediately
- [ ] If impure: Verify side effects are idempotent (safe to retry)
- [ ] If pure: Test determinism (`cargo test --release` multiple times, compare output hashes)
- [ ] Register in `conductor/src/lib.rs` (`registered_builtin_ids()`)
- [ ] Write integration tests (both CLI and API)
- [ ] Verify CLI and API produce identical behavior for same inputs
- [ ] Document argument names and types
- [ ] Add example usage to crate README
- [ ] Update `AGENTS.md` to list new builtin

### Adding a New CAS Backend

- [ ] Implement `CasApi` trait (put, get, contains, etc.)
- [ ] Implement `CasMaintenanceApi` trait (optimize, prune, repair)
- [ ] Add error types (must include `NotFound`, `OutOfSpace`)
- [ ] Implement index persistence (if applicable)
- [ ] Write property tests (determinism: `put(x) → get() ≡ x`)
- [ ] Write stress tests (concurrent puts, hash collisions)
- [ ] Document performance characteristics (O(1) get? O(log n)?)
- [ ] Benchmark against FileSystemCas
- [ ] Add configuration for resource limits
- [ ] Update `AGENTS.md` with backend comparison

### Adding a New Workflow Execution Backend

- [ ] Implement `ConductorApi` trait
- [ ] Implement state serialization (to CAS or other store)
- [ ] Implement builtin registration and invocation
- [ ] Add error mapping to Conductor error types
- [ ] Write integration test (simple workflow: define, execute, inspect state)
- [ ] Write concurrency test (parallel workflow execution)
- [ ] Document failure modes (what happens if step fails? can you retry?)
- [ ] Verify determinism (pure workflows produce same output)
- [ ] Benchmark planning time (< 10ms for typical workflows)
- [ ] Update `AGENTS.md` with backend description

### Adding a New MediaPM Media Source Type

- [ ] Define source kind (URL, local file, CAS hash, etc.)
- [ ] Add to `mediapm.ncl` schema (version increment if incompatible)
- [ ] Implement source reader (retrieve bytes)
- [ ] Add to `mediapm/src/config.rs` (parse from config)
- [ ] Add to sync logic (mediapm/src/service.rs)
- [ ] Write test with example config
- [ ] Add example to README
- [ ] Update `AGENTS.md` with new source type

### Adding a New Managed Tool

- [ ] Add tool spec to `mediapm.ncl` schema (tool name, version, selectors)
- [ ] Add tool provisioner (download, extract, verify hash)
- [ ] Add to tool registry (mediapm/src/tools/)
- [ ] Implement CLI/API wrapper (if needed for execution)
- [ ] Add test (provisioning, verification)
- [ ] Document tool requirements (OS, dependencies)
- [ ] Add example workflow using new tool
- [ ] Update `AGENTS.md` with tool details

### Adding a New Test Feature

- [ ] Determine category: happy path, edge case, error, concurrency, performance
- [ ] Choose test module: `tests/e2e/`, `tests/int/`, `tests/prop/`
- [ ] Write test with clear name: `test_<component>_<scenario>`
- [ ] Verify test fails without the feature (verify it's not trivially passing)
- [ ] Add inline comments explaining test purpose
- [ ] If determinism-sensitive, use fixed seeds / deterministic inputs
- [ ] If performance-sensitive, add benchmark comment (expected time)
- [ ] Verify test passes in release build
- [ ] Run test 5x to check for flakiness
- [ ] Add test to CI (`.github/workflows/ci.yml`)

---

## Demo Verification Results

### Offline Demo (`mediapm_demo`)

The offline demo runs a complete 10-step pipeline using synthetic/offline data:

| Phase | Tool | Steps | Status |
|-------|------|-------|--------|
| Import | `import` (CAS) | 4 steps | ✅ executed |
| Processing | `ffmpeg` | 2 steps | ✅ executed |
| Metadata | `media-tagger` | 2 steps | ✅ executed |
| Gain | `rsgain` | 1 step | ✅ executed |
| Export | `export` | 1 step | ✅ executed |

- **Steps executed**: 10/10, **cached**: 0/10, **rematerialized**: 0/10
- **Total wall time**: ~27s (single run, cold cache)
- **Profile output**: verified complete with progress and timing for each step
- **Key takeaway**: Full pipeline executes end-to-end without any managed tool
  provisioning — all tools resolved from system PATH or runtime config.

### Online Demo (`mediapm_demo_online`)

Tests the same pipeline with actual yt-dlp managed tool provisioning:

| Phase | Tool | Steps | Status |
|-------|------|-------|--------|
| Download | `yt-dlp + ffmpeg + deno` | 4 steps | ✅ executed (provisioned) |
| Processing | `ffmpeg` | 2 steps | ✅ executed |
| Metadata | `media-tagger` | 2 steps | ✅ executed |
| Gain | `rsgain` | 1 step | ✅ executed |
| Export | `export` | 1 step | ✅ executed |

- **Steps executed**: 10/10, **cached**: 0/10, **rematerialized**: 0/10
- **yt-dlp step**: 38.4s (includes download + ffmpeg + deno companion provisioning)
- **Total wall time**: ~67s (single run, cold cache, network-dependent)
- **Companion inlining**: yt-dlp companion payloads (ffmpeg + deno) are inlined
  into the yt-dlp tool content map per same-step companion dependency rules
- **Key takeaway**: full managed-tool download → workflow execution → materialization
  pipeline works end-to-end with real yt-dlp downloads

### Testing Summary

- **`cargo test -p mediapm-conductor`**: 200 lib tests + 15 integration tests = 215 total, all pass ✅
- Key test categories validated:
  - Step-stream dedup semantics (parallel dispatch cross-workflow)
  - CorruptWorkflowOutput error variant detection (`matches!` macro)
  - Scheduler diagnostics fallback for step-stream path
  - Trace event completeness per dispatch path

---

## Visual Diagrams & State Machines

### Data Flow: End-to-End Sync

```
User Config (mediapm.ncl)
        ↓
┌───────────────────────────────────────────────────────┐
│ MediaPM Service                                       │
│ ┌─────────────────────────────────────────────────┐   │
│ │ 1. Parse config + load schemas                  │   │
│ │ 2. Read lock file (what's been processed)       │   │
│ │ 3. Identify new media entries                   │   │
│ └─────────────────────────────────────────────────┘   │
│                       ↓                                │
│ ┌─────────────────────────────────────────────────┐   │
│ │ For each media entry:                           │   │
│ │   3a. Synthesize Conductor workflow             │   │
│ │   3b. Pass workflow + input to Conductor        │   │
│ └─────────────────────────────────────────────────┘   │
└───────────────────────────────────────────────────────┘
        ↓
┌───────────────────────────────────────────────────────┐
│ Conductor (SimpleConductor)                           │
│ ┌─────────────────────────────────────────────────┐   │
│ │ 1. Parse workflow (steps, tool references)      │   │
│ │ 2. Topological sort (level-based DAG)           │   │
│ │ 3. Bind inputs (resolve parameters)             │   │
│ └─────────────────────────────────────────────────┘   │
│                       ↓                                │
│ ┌─────────────────────────────────────────────────┐   │
│ │ For each step (level-by-level):                 │   │
│ │   4a. Invoke tool (builtin or managed)          │   │
│ │   4b. Capture output → CAS.put() → hash         │   │
│ │   4c. Store step result in OrchestrationState   │   │
│ └─────────────────────────────────────────────────┘   │
│                       ↓                                │
│ ┌─────────────────────────────────────────────────┐   │
│ │ 5. Serialize OrchestrationState → CAS.put()     │   │
│ │ 6. Return final output hash                      │   │
│ └─────────────────────────────────────────────────┘   │
└───────────────────────────────────────────────────────┘
        ↓
┌───────────────────────────────────────────────────────┐
│ CAS (FileSystemCas or InMemoryCas)                    │
│ ┌─────────────────────────────────────────────────┐   │
│ │ For each put(bytes):                            │   │
│ │   1. Compute Blake3-256 hash                    │   │
│ │   2. Check if hash exists (deduplication)       │   │
│ │   3. If new: write to store, update index       │   │
│ │   4. Return hash                                │   │
│ └─────────────────────────────────────────────────┘   │
└───────────────────────────────────────────────────────┘
        ↓
MediaPM (back in materialization phase)
        ↓
┌───────────────────────────────────────────────────────┐
│ Staging & Commit                                      │
│ ┌─────────────────────────────────────────────────┐   │
│ │ 1. Get final output hash from Conductor         │   │
│ │ 2. CAS.get(hash) → bytes                        │   │
│ │ 3. Write to staging directory                   │   │
│ │ 4. Validate permissions, read-only bit          │   │
│ │ 5. Atomic rename → final location               │   │
│ │ 6. Update lock file (media_id → hash)           │   │
│ └─────────────────────────────────────────────────┘   │
└───────────────────────────────────────────────────────┘
        ↓
Output Files + Updated Lock
```

---

### State Machine: Conductor Workflow Execution

```
                    ┌─────────────────┐
                    │   PENDING       │ (awaiting input)
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │  VALIDATING     │ (schema, bindings)
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │  PLANNING       │ (topological sort)
                    └────────┬────────┘
                             │
                    ┌────────▼────────────────┐
                    │ DISPATCHING STEPS       │
                    │ (level-by-level)        │
                    └────────┬─────────────────┘
                             │
         ┌───────────────────┼───────────────────┐
         │                   │                   │
    ┌────▼────┐      ┌──────▼──────┐      ┌─────▼────┐
    │ STEP[0] │      │  STEP[1]    │      │STEP[n]   │ (parallel)
    │(tool)   │      │ (tool)      │      │(tool)    │
    │EXECUTING│      │ EXECUTING   │      │EXECUTING │
    └────┬────┘      └──────┬──────┘      └─────┬────┘
         │                   │                   │
    ┌────▼────────┐  ┌──────▼──────┐  ┌────────▼────┐
    │DONE/ERROR   │  │DONE/ERROR   │  │DONE/ERROR   │
    │Store result │  │Store result │  │Store result │
    │in state     │  │in state     │  │in state     │
    └────┬────────┘  └──────┬──────┘  └────────┬────┘
         │                   │                   │
         └───────────────────┼───────────────────┘
                             │
                    ┌────────▼────────┐
                    │ FINALIZING      │ (serialize state to CAS)
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
              ┌─────┤ COMPLETED or    │
              │     │ FAILED          │
              │     └─────────────────┘
              │
              ├─→ COMPLETED: Return final hash
              └─→ FAILED: Return error + partial state
```

---

### Module Dependency Graph: CAS Crate

```
┌──────────────────────────────┐
│        Public API            │
│   CasApi, CasMaintenanceApi  │
│   Hash, Constraint, ObjectInfo
└──────────────────────────────┘
             ▲
             │ (implements/exports)
             │
┌────────────┴──────────────────────┐
│                                   │
├─ api module ◀─────────────────────┼─ orchestration module
│                                   │ (Actor-based coordination)
├─ cli module ◀─────────────────────┼─ storage module
│                                   │ (FileSystemCas, InMemoryCas)
├─ error module                     │
│                                   │
├─ hash module ◀─────────────────────┼─ codec module
│  (Blake3, multihash)              │ (Versioned encode/decode)
│                                   │
├─ index module ◀─────────────────────┼─ (depends on)
│  (Persistence, repair)            │
│                                   │
└───────────────────────────────────┘
             ▲
             │
       External Dependencies:
       - blake3 (hashing)
       - serde (serialization)
       - tokio (async runtime)
       - ractor (actor framework)
```

---

### Module Dependency Graph: Conductor Crate

```
┌──────────────────────────────┐
│       Public API             │
│   ConductorApi (trait)       │
│   SimpleConductor (impl)     │
└──────────────────────────────┘
             ▲
             │
┌────────────┴──────────────────────┐
│                                   │
├─ api module (trait)               │
│                                   │
├─ cli module ◀─────────────────────┼─ model::config
│  (Command line interface)         │ (Three-document schema)
│                                   │
├─ error module                     │
│                                   │
├─ orchestration module ◀───────────┼─ tools module
│  (Actor-based execution)          │ (Builtin registry,
│                                   │  tool invocation)
├─ model::state                     │
│  (OrchestrationState)             │
│                                   │
└───────────────────────────────────┘
             ▲
             │
       Internal Dependencies:
       - CasApi (from cas crate)
       - Conductor-builtins (echo, fs, etc.)

       External Dependencies:
       - nickel (config language)
       - serde (serialization)
       - tokio (async runtime)
       - ractor (actor framework)
```

---

### Materialization State Machine: MediaPM

```
┌─────────────────────────────────────┐
│     SYNC INITIATED                  │
│  (media entry + final CAS hash)     │
└────────────────┬────────────────────┘
                 │
        ┌────────▼────────┐
        │ CHECK LOCK FILE │
        └────────┬────────┘
                 │
         ┌───────┴───────┐
         │               │
    ┌────▼──────┐   ┌───▼──────────┐
    │ FOUND IN  │   │ NOT IN LOCK  │
    │ LOCK &    │   │ → PROCEED    │
    │ HASH OK   │   └──────┬───────┘
    │ → SKIP    │          │
    └───────────┘   ┌──────▼────────────┐
                    │ STAGING PHASE     │
                    │ 1. CAS.get(hash)  │
                    │ 2. Write to /tmp/ │
                    │ 3. Check perms    │
                    └──────┬────────────┘
                           │
              ┌────────────┴───────────────┐
              │                           │
         ┌────▼──────┐         ┌─────────▼──┐
         │ SUCCESS   │         │ FAILURE    │
         │ → COMMIT  │         │ → ROLLBACK │
         └────┬──────┘         └────────┬───┘
              │                         │
    ┌─────────▼──────────┐   ┌──────────▼──────┐
    │ COMMIT PHASE       │   │ ROLLBACK PHASE  │
    │ 1. Atomic rename   │   │ 1. Delete /tmp/ │
    │ 2. Set read-only   │   │ 2. Leave lock   │
    │ 3. Update lock     │   │    unchanged    │
    │ 4. Materialize ok  │   │ 3. Sync can     │
    └────────┬───────────┘   │    retry        │
             │               └─────────────────┘
    ┌────────▼──────────────┐
    │ LOCKED & MATERIALIZED │
    │ (cache hit on re-sync)│
    └───────────────────────┘
```

---

## Glossary: Key Terms

| Term | Definition | Example |
|------|-----------|---------|
| **CAS** | Content-Addressed Storage. Store objects by hash; retrieve by hash. Identity is content, not name. | `put(bytes) → hash:abc123; get(hash:abc123) → bytes` |
| **Hash** | Blake3-256 multihash. Uniquely identifies bytes. Same bytes → same hash always. | `hash:blake3:abc123def456...` |
| **Constraint** | CAS metadata indicating which objects can be selected as delta bases. Used to control optimization. | `constraint { bases = [hash1, hash2] }` |
| **Delta Encoding** | Compression technique: store only differences from a base object. Saves space for similar files. | `delta { base: hash1, patch: diff_bytes }` |
| **Object** | Unit of data in CAS. May be raw bytes (full object) or delta-encoded (delta object). | Full: 10MB video; Delta: 50KB patch from previous version |
| **Builtin** | Standard tool provided by Conductor. Five builtins: echo, fs, archive, import, export. | `{ tool = "echo", args = { message = "hello" } }` |
| **Managed Tool** | External tool provisioned by MediaPM (ffmpeg, yt-dlp, media-tagger). Downloaded, cached, versioned. | `tool_config.ffmpeg = { version = "7.0.0", ... }` |
| **Workflow** | Directed acyclic graph (DAG) of steps. Each step invokes a tool; output feeds to next step. | `steps: [import, ffmpeg, media-tagger, export]` |
| **Step** | Single operation in a workflow. Invokes one tool with input bindings. | `{ id = "convert", tool = "ffmpeg", args = { format = "mp3" } }` |
| **OrchestrationState** | Complete state of workflow execution: which steps ran, outputs, errors. Persisted to CAS. | Includes per-step: id, status, output hash, error message |
| **Staging Directory** | Temporary location where files are written before final commit. Atomic rename moves files to output location. | `~/.mediapm/.staged_xyz/` → `~/.mediapm/output/` |
| **Lock File** | Records what's been synced: media_id + variant → final CAS hash. Enables cache hits on re-run. | `{ media_id = "song_1", variant = "primary", cas_hash = "..." }` |
| **Media Source** | Origin of media data: URL, local file, CAS hash, etc. Specified in `mediapm.ncl`. | `source = "https://example.com/video.mp4"` |
| **Hierarchy** | Folder/media organization in output directory. Specifies which media go where. | `hierarchy = [{ kind = "folder", name = "Music", children = [...] }]` |
| **Variant** | Output type for media (primary, audio, thumbnail, etc.). One media entry → multiple variants. | Media: song.mp3; Variants: primary, lyrics, cover_art |
| **Materialization** | Process of writing CAS objects to disk (final output files). Atomic: stage → validate → commit. | CAS object → /media/library/song/song.mp3 |
| **Pure** | Operation with deterministic output. Same input always → same output. No side effects. | `echo` and `archive` are pure; `fs` and `import` are impure |
| **Impure** | Operation with non-deterministic or side-effect output. May vary on retry. | `import` (network), `fs` (file system), `export` (disk I/O) |
| **Determinism** | Property: identical inputs produce identical outputs. Required for cache hits and reproducible builds. | If sync ran twice without config change, skip all steps (cache) |
| **Fail-Fast** | Validation before execution. Invalid inputs rejected immediately, before side effects. | Builtin: `--arg unknown_key val` rejected before running tool |
| **Atomicity** | Operation succeeds or fails completely. No partial state. Transactional. | Materialization: All files written or none (no orphaned files) |
| **Index** | CAS metadata: (hash → storage location, compressed status, size). Updated on every put/optimize. | Maps `hash:abc123` to `/store/ab/c123` on disk |
| **Repair** | CAS maintenance operation: scan storage, rebuild index, detect orphaned objects. | `cas.repair_index() → IndexRepairReport` |
| **Optimize** | CAS maintenance: convert full objects to delta-encoded to save space. | `cas.optimize() → OptimizeReport { bytes_saved: 5GB }` |

---

## FAQ: Common Developer Questions

### 1. **How do I start developing a feature that uses CAS?**

Start with an in-memory CAS for fast iteration:
```rust
use mediapm_cas::inmemory::InMemoryCas;
let cas = Arc::new(InMemoryCas::new());
```
Test your feature logic. When ready for production, swap to `FileSystemCas`:
```rust
use mediapm_cas::filesystem::FileSystemCas;
let cas = Arc::new(FileSystemCas::new("/tmp/cas_store")?);
```
Both implement `CasApi`, so code is backend-agnostic.

### 2. **Why is my Conductor workflow not executing?**

Check three things in order:
1. **Config parses**: `conductor.ncl` is valid Nickel
2. **DAG is valid**: No circular dependencies; all tool refs exist
3. **Tools are registered**: Check `registered_builtin_ids()`

Use `conductor.validate_workflow()` before executing.

### 3. **How do I test that my builtin is deterministic?**

Run it multiple times with same input; compute hash of output:
```bash
for i in {1..5}; do
  mediapm-conductor-builtin-archive \
      --arg action pack \
      --arg path /tmp/test \
      --input archive_$i.zip
  blake3 archive_$i.zip
done
# All hashes should be identical
```

### 4. **Can I use MediaPM without Conductor?**

Technically yes (MediaPM's materialization is independent), but pointless. MediaPM is a policy layer on top of Conductor. Without Conductor, you're just using CAS + file I/O, which isn't media management.

### 5. **How do I handle media that changes sources?**

If same `media_id` now points to different source:
1. Update `mediapm.ncl` (change source URL)
2. Run `mediapm sync`
3. MediaPM re-downloads, re-processes, updates lock
4. Old CAS objects remain (deduplication helps if content is similar)

If you want to force re-process:
1. Delete lock entry for that media_id
2. Run sync → re-processes, updates lock

### 6. **What if tool update breaks backward compatibility?**

E.g., ffmpeg 6 → 7 produces different output format:
1. MediaPM config specifies tool version: `tool_config.ffmpeg.version = "7.0.0"`
2. Update config to `"6.4.0"` (older version)
3. Run sync → Conductor uses ffmpeg 6.4
4. Output hashes change; lock updated

Different tool versions → different workflow outputs → different hashes. No magic; explicit version control.

### 7. **How do I move my .mediapm workspace to a new directory?**

```bash
cp -r ~/.mediapm /data/mediapm_backup
rm -rf ~/.mediapm
mkdir -p /data/mediapm
cp -r /data/mediapm_backup/* /data/mediapm/
```

CAS store and lock files are portable (relative paths). To verify integrity:
```rust
let cas = FileSystemCas::new("/data/mediapm/store")?;
let report = cas.repair_index().await?;
println!("Repaired: {} entries", report.fixed_entries);
```

### 8. **Why does Conductor persist state to CAS instead of a plain file?**

Three reasons:
1. **Deduplication**: Identical workflow runs produce identical state hashes; stored once.
2. **Verification**: Hash proves state hasn't been corrupted.
3. **Integration**: Workflow state is just another CAS object; same backup/restore mechanisms.

### 9. **How do I debug a failed builtin invocation?**

Run the builtin directly with same arguments:
```bash
# From workflow
{ tool = "ffmpeg", args = { input_hash = "h1", format = "mp3" } }

# Direct CLI (manual)
$ mediapm-conductor-builtin-import \
    --arg source cas://h1 \
    --arg dest mp3

# Check stderr and exit code
```

### 10. **Can I run two syncs in parallel on the same .mediapm?**

No. Lock file and state documents are not designed for concurrent writers. One sync at a time. If you need parallel processing, use multiple .mediapm directories or contact maintainers for concurrent-safe version.

### 11. **How do I know if CAS.get() is using delta decoding?**

Inspect the object in CAS:
```rust
let info = cas.get_object_info(&hash).await?;
if info.is_delta {
    println!("Delta object; depth: {}", info.delta_chain_depth);
} else {
    println!("Full object; size: {}", info.byte_size);
}
```

Large objects with long delta chains are slower to retrieve. Use `cas.optimize()` to consolidate deltas.

### 12. **What's the difference between mediapm.ncl and conductor.ncl?**

- **`conductor.ncl`** (Conductor-owned): Workflow definitions, tool specs, step DAG. Conductor execution rules.
- **`mediapm.ncl`** (MediaPM-owned): Media sources, hierarchy, materialization policy. MediaPM synthesis rules.

MediaPM reads `mediapm.ncl`, synthesizes Conductor workflows, passes to Conductor.

### 13. **How do I know if my workflow is deterministic?**

1. Run workflow with input A twice; capture output hashes
2. If hashes match → deterministic
3. If hashes differ → Non-deterministic (check for time, randomness, external state)

Or: Check if all steps invoke pure builtins/tools (echo, archive). If any impure step (import, fs, export) with external IO, determinism is not guaranteed.

### 14. **Can I extend CAS with custom hash algorithms?**

Yes. Add to `HashAlgorithm` enum, implement multihash trait, update codec table. Backward compatibility requires migration. See `src/cas/codec/versions/` for pattern.

### 15. **How do I monitor CAS performance?**

Enable tracing:
```rust
let subscriber = tracing_subscriber::registry()
    .with(tracing_subscriber::fmt::layer());
tracing::subscriber::set_global_default(subscriber)?;
```

Then:
- `get()` emits span with duration
- `put()` emits span with encoding (full vs. delta)
- `optimize()` emits metrics (bytes saved)

Parse logs to identify hot paths.

---

## Appendix: File Structure Reference

```
mediapm/
├── AGENTS.md (this file's target reference)
├── src/
│   ├── cas/
│   │   ├── AGENTS.md (crate-specific guidance)
│   │   ├── src/
│   │   │   ├── api.rs (CasApi trait)
│   │   │   ├── lib.rs (exports)
│   │   │   ├── cli.rs (command-line interface)
│   │   │   ├── hash.rs (Hash type, Blake3)
│   │   │   ├── error.rs (CasError)
│   │   │   ├── codec/ (serialization & versioning)
│   │   │   ├── index/ (persistence, repair)
│   │   │   ├── orchestration/ (actor-based coordination)
│   │   │   └── storage/ (FileSystemCas, InMemoryCas)
│   │   └── tests/
│   │       ├── tests.rs (harness)
│   │       ├── e2e/ (end-to-end)
│   │       ├── int/ (integration)
│   │       └── prop/ (property-based)
│   │
│   ├── conductor/
│   │   ├── AGENTS.md (crate-specific guidance)
│   │   ├── src/
│   │   │   ├── api.rs (ConductorApi trait)
│   │   │   ├── lib.rs (SimpleConductor)
│   │   │   ├── cli.rs (command-line interface)
│   │   │   ├── error.rs (ConductorError)
│   │   │   ├── model/
│   │   │   │   ├── config.rs (3-document schema)
│   │   │   │   └── state.rs (OrchestrationState)
│   │   │   ├── orchestration/ (actor-based execution)
│   │   │   └── tools/ (builtin registry)
│   │   └── tests/
│   │       ├── tests.rs (harness)
│   │       ├── e2e/ (end-to-end)
│   │       └── int/ (integration)
│   │
│   ├── conductor-builtins/
│   │   ├── echo/
│   │   │   ├── src/ (CLI + API)
│   │   │   └── tests/
│   │   ├── fs/
│   │   ├── archive/
│   │   ├── import/
│   │   └── export/
│   │
│   └── mediapm/
│       ├── AGENTS.md (crate-specific guidance)
│       ├── src/
│       │   ├── api.rs (MediaPmApi trait)
│       │   ├── lib.rs (MediaPmService)
│       │   ├── cli.rs (command-line interface)
│       │   ├── config/ (mediapm.ncl schema)
│       │   ├── lockfile/ (lock.ncl persistence)
│       │   ├── paths.rs (runtime path resolution)
│       │   ├── conductor_bridge.rs (ConductorApi integration)
│       │   ├── materializer.rs (staging/commit)
│       │   ├── tools.rs (tool provisioning)
│       │   └── error.rs (MediaPmError)
│       └── tests/
│           ├── tests.rs (harness)
│           ├── e2e/ (end-to-end)
│           └── int/ (integration)
│
├── .agents/
│   └── instructions/
│       ├── crate-specifications.md (this file)
│       ├── commit-message-policy.instructions.md
│       ├── rust-workflow.instructions.md
│       └── ... (other instruction files)
│
└── opencode.jsonc (agent customization registry)
```

---

**Document Status**: Finalized for human review. Ready for integration into AGENTS.md or standalone reference.

**Last Updated**: 2026-05-31

**Audience**: Code reviewers, new developers, maintainers, integration engineers.
