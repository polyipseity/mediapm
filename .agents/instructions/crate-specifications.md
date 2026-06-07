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
    └─ Direct materialization to final output paths

Temp extraction directory (`mediapm_tmp_dir`, for zip processing only)
    └─ Extract → materialize → cleanup
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
| **CAS** | `set_constraint_batch()` validates each op's bases exist; optimizer honors constraints |
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
| **Conductor** | State persisted atomically; workflow fails fast on conflicts; OS-backed per-conductor-dir temp dirs (hash of conductor_dir under `std::env::temp_dir()`) replace global temp for sandboxes, ZIP extractions, and regex captures |
| **Builtins** | File operations succeed or rollback (no orphaned state) |
| **MediaPM** | Direct materialization to final output paths; CAS integrity trusted by default; temp extraction dir (hash of workspace root under `std::env::temp_dir()`) used for zip processing only; per-workspace temp dirs are for sandboxes and ZIP extractions |

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

### 6. NCL↔Rust Schema Sync Contract

**Principle**: NCL schema definitions and Rust struct definitions must remain
bidirectionally consistent — field names, types, optionality, and version
markers must match exactly.

| Crate | Enforcement |
|-------|-------------|
| **Conductor** | Typed Rust bridge (`v_latest.rs`) with exhaustive round-trip tests covering all fields and variant branches. NCL `IntegerNumberV1` contract validated against Rust integer sentinels. |
| **MediaPM** | Typed envelope (`MediaPmDocumentEnvelopeV1`) wrapping flattened inner struct at the version layer (conductor pattern). `deny_unknown_fields` on parent envelope. Round-trip decode/encode tests for runtime, state, and platform-specific fields. |
| **Builtins** | N/A (no NCL schema — CLI/API contracts enforced by builtin validation) |

**Architecture**:
- Each versioned NCL document has a corresponding typed Rust struct at the
  version layer (e.g., `v1.rs`), with an envelope struct that carries the
  explicit `version` marker and flattens the inner document.
- `decode()`: deserialize JSON value → typed envelope → extract inner document.
- `encode()`: serialize inner document → wrap in typed envelope → JSON value.
- `deny_unknown_fields` lives on the **parent envelope** because
  `#[serde(flatten)]` on the child prevents it from carrying its own
  `deny_unknown_fields`.
- Child structs with `#[serde(flatten)]` must NOT set `deny_unknown_fields`.

**Test coverage**:
- `decode_rejects_unsupported_mediapm_ncl_version` — future version markers fail.
- `encode_round_trip_preserves_latest_version_marker` — default document keeps
  current version.
- `populated_runtime_storage_round_trips_through_typed_envelope` — all
  `MediaRuntimeStorage` fields survive encode→decode.
- `populated_mediapm_state_round_trips_through_typed_envelope` — managed files,
  tool registry, workflow states with impure timestamps round-trip.
- `typed_envelope_rejects_unknown_top_level_field` — envelope-level
  `deny_unknown_fields` catches extra keys.
- `inherited_env_vars_round_trip_preserves_platform_keys` — all three platform
  entries survive round-trip.

**`PlatformInheritedEnvVars`**: This is a type alias for
`BTreeMap<String, Vec<String>>` (not a struct with named fields). Platform
keys are `"windows"`, `"linux"`, `"macos"` — access via `.get(key)` /

`.map(|v| v.as_slice())`.

**Verification**: If a new field is added to the Rust struct but not to the NCL
schema (or vice versa), the round-trip tests will detect the mismatch. Adding
a field requires: (1) update NCL schema, (2) update Rust struct, (3) verify
round-trip tests pass.

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
- Conductor temp directories isolated per-conductor-dir (hash-based path under `std::env::temp_dir()`)
- MediaPM respects conductor's state versioning (explicit migration support)
- Sync materializes directly to final output paths; no intermediate staging phase

### MediaPM ↔ CAS (Direct)

**Entry Point**: MediaPM materializes from CAS
- Conductor already uses CAS for state; MediaPM uses for file materialization

**Operations**:
1. Content verification: Check file hash against lock record
2. Cache hit detection: If hash unchanged, skip re-materialization
3. Link materialization: Call `cas.get()` and write to final output path

**Ownership**:
- **MediaPM owns**: Hierarchy logic, materialization orchestration, lock records
- **CAS owns**: Storage, persistence, object retrieval

**Contract**:
- All materialized files are read-only after commit
- Hashes must match; mismatch → failed materialization (no fallback)
- Platform-independent path resolution (normalized, slash-separated); enforced
  by `HierarchyPath` which stores path components as a `Vec<String>`, joined
  by `/` at materialization time

### Instance Output Existence Checking

During hierarchy materialization, the materializer checks whether each
candidate orchestration instance's required step outputs still exist in CAS.
For step outputs that do not require ZIP member extraction, the check uses
`cas.info(hash)` — a lightweight existence check that costs one redb index
lookup + one stat call — instead of `cas.get(hash)` which would load the
full content bytes (potentially multi-GB video files). ZIP-member outputs
still use `cas.get(hash)` to extract specific members.

**Implementation**: `instance_has_materializable_required_outputs()` in
`src/mediapm/src/materializer/resolve.rs`.

### Orchestration State Decode Migration

**`decode_state()`** in `src/conductor/src/model/state/versions/mod.rs`
automatically detects V1 envelope format (inline instance objects) and V2
format (CAS-backed instance refs) by parsing the `version` field from raw
JSON. V1 instances are migrated through the V1→V2 ISO bridge
(`tool_call_instance_v1_v2_iso`) then V2→runtime bridge
(`tool_call_instance_v2_iso`). The returned state always carries the latest
version marker, making re-persistence produce a V2 envelope — a self-healing
one-time migration cost per state blob.

### Metadata Cache

**File**: `src/mediapm/src/metadata_cache.rs`

**Purpose**: Persistent on-disk cache for metadata resolution during hierarchy
instantiation and add-path workflows, with 1-day TTL based on non-usage.

**Backend**: Single JSONC file (`metadata.jsonc`) stored at
`<runtime_root>/cache/mediapm/`. NOT CAS-backed — cache is a simple
`BTreeMap<String, MetadataCacheEntry>` serialized with `serde_json`. Each entry
contains a `serde_json::Value` payload and `last_access_unix_seconds` timestamp.

**Key Derivation**:
- Hierarchy metadata: `blake3::hash(media_id.as_bytes()).to_hex().to_string()`
- Add-path metadata: `blake3::hash(canonicalized_path.to_string_lossy().as_bytes()).to_hex().to_string()`

**TTL Policy**: 86400 seconds from `last_access_unix_seconds`. Entries are
evicted on load (not on set). Access via `get()` updates `last_access_unix_seconds`
to current time (in-memory dirty flag only).

**Persistence**:
- `set()` is in-memory dirty flag only; no immediate write.
- Timer-based batch flush: ~300s cooldown after last `set()` or `get()`.
- `flush()` writes to temp file via `AtomicFileOp` then renames.
- `Drop` impl triggers one final synchronous flush.
- Load on `open()`: read file, deserialize, filter stale entries, write back
  atomically if any were removed.

**Integration Points**:
- `MaterializationLookupContext` in `materializer/mod.rs` carries
  `metadata_cache: Option<Arc<MetadataCache>>`.
- `extract_metadata_value_from_variant_payload()` in `materializer/metadata.rs`
  checks cache before ffprobe invocation, stores on success.
- `try_fetch_local_source_metadata_with_ffprobe()` in `source_metadata.rs`
  checks cache before ffprobe invocation, stores on success.
- `service.rs` opens cache for add-local-source flow.

**Contract**:
- Cache miss → probe tool → store in cache → return.
- Cache hit (TTL valid) → return cached value, skip probe.
- TTL expired → treat as miss, re-probe, update cache.
- Serialization failure → treat as miss (log warning, continue).
- File open/read/write failure → graceful degradation (cache unavailable,
  proceed without caching, no crash).
- Clock skew: if `last_access_unix_seconds > now`, treat as just-verified
  (do not spuriously evict).

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
- **Type Model**: `Hash` (Blake3 multihash), `Constraint` (base selection), `ConstraintBatchOp` (batch operation), `ObjectInfo` (metadata)
- **Storage**: `FileSystemCas` (persistent), `InMemoryCas` (ephemeral)
- **Versioning**: Adjacent-only migrations; optics-based bridging
- **Performance**: O(1) full objects, O(depth) delta objects; mmap for ≥64KB

### CAS Integrity Verification

The content-addressed storage layer implements configurable integrity verification
that re-checks BLAKE3 hashes when objects are read. Verification is gated by a
list of trigger strategies (`VerifyTriggerStrategy`):

- `Always` — Re-verify on every `get()`.
- `Modified` — Re-verify when the object's mtime has changed since the last put
  or verify (fieldless variant; no per-entity timestamp is tracked).
- `Sample { denominator }` — Re-verify on a 1-in-N probabilistic basis.
- `Stale { timeout }` — Re-verify when the elapsed time since the last put or
  verify exceeds the timeout.

All strategies are evaluated on every `get()`; verification runs if *any*
matching strategy triggers.

**Configuration** (`CasIntegrityConfig`):

```rust
pub struct CasIntegrityConfig {
    pub verify_on_read: Vec<VerifyTriggerStrategy>,
    pub reconstructed_bytes_cache_ttl: Duration,
}
```

Default `verify_on_read`: `[Modified, Sample { denominator: 100 }, Stale { timeout: 604800s }]`.
Default `reconstructed_bytes_cache_ttl`: `3600s`.

Reconstructed-object bytes are cached with a configurable TTL (`reconstructed_bytes_cache_ttl`)
to reduce redundant decoding work. No separate integrity-result cache is maintained;
verification decisions are made fresh on every `get()` call against object-file
metadata and the strategy list.

**Runtime wiring** (`MediaRuntimeStorage`, `RuntimeStorageConfig`):

The CAS integrity configuration is wired through the runtime storage config stack:

- `MediaRuntimeStorage.verify_on_read_sample_denominator: Option<u64>` — overrides the
  `Sample` strategy denominator (default: 100).
- `MediaRuntimeStorage.verify_on_read_stale_timeout_secs: Option<u64>` — overrides the
  `Stale` strategy timeout in seconds (default: 604800, 7 days).
- `MediaRuntimeStorage.reconstructed_bytes_cache_ttl_secs: Option<u64>` — overrides the
  reconstructed-bytes cache TTL in seconds (default: 3600, 1 hour).

These three fields are mirrored in `RuntimeStorageConfig` (conductor crate) and
converted to `CasIntegrityConfig` via `MediaRuntimeStorage::to_cas_integrity_config()`.
The resulting config is passed through `RunWorkflowOptions.cas_integrity_config` to
the conductor orchestration layer.

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
- **Orchestration**: Actor-based (ractor); dependency-stream dispatch with inline coordination (`WorkflowDepState` + `FuturesUnordered`); round-robin worker assignment; adaptive cost model
- **State Model**: `OrchestrationState` with tool call instances; persisted in CAS
- **Versioning**: Explicit version markers; optics-based migration
- **Performance**: < 10ms planning; adaptive scheduler for load balancing

**Step dispatch (dependency-stream model)**:
- Coordinator builds per-workflow dependency graphs (`WorkflowDepState` with
  `remaining_deps` + `dependents` + `step_outputs`) during Phase 1 — deduplicates
  shared dependent steps, detects cycles, validates all referenced steps exist.
- Phase 2 dispatches via a single `FuturesUnordered` loop across all workflows:
  seeds a `global_ready_queue` with zero-dependency steps, assigns workers
  round-robin, processes completions (updates `remaining_deps`, enqueues newly-
  ready dependents), and handles impure timestamp planning inline.
- The old `execution_hub.rs` actor is eliminated — dispatch and completion
  processing are inline in `coordinator.rs`; the protocol-level
  `StreamBatch`/`StreamStep`/`StepOutcome` types are removed.
- Step-worker cache probe still uses `cas.exists_many(check_hashes)` →
  `CasExistenceBitmap` (backed by `BitVec`) for O(1) batch existence checks.

**Per-tool concurrency enforcement**:
- `UnifiedToolSpec::max_concurrent_calls` (previously `#[expect(dead_code)]`) is now
  enforced at dispatch time. Before the dispatch loop, the coordinator builds a map
  of per-tool `tokio::sync::Semaphore` instances from the config value (values > 0
  create a capacity-limited semaphore; -1 means unlimited, no semaphore created).
- The dispatch inner loop scans the `global_ready_queue` for a step whose tool has
  available capacity instead of unconditionally popping the front element. Steps
  for tools at their concurrent-call limit are re-queued (fairness: re-queued at
  back, so they don't starve behind a continuously-filling queue of other steps).
- The acquired `OwnedSemaphorePermit` is held inside the in_flight `Future` for
  the entire step duration, automatically releasing capacity on completion (or
  worker failure).
- This applies to all tools, not only managed tools; builtin tools with
  `max_concurrent_calls = -1` (the default) are unbounded as before.

**Per-tool retry enforcement**:
- `UnifiedToolSpec.max_retries` is enforced in the coordinator's dispatch loop.
- `dispatch_step_rpc` wraps a single RPC dispatch in a retry loop. On every
  attempt, it calls the worker RPC. If the RPC fails and retries are exhausted,
  the error propagates directly to the caller. There is no local-execution
  fallback — the actor system is the sole execution path.
- The semaphore permit (concurrency slot) is held across all retry attempts so the
  tool's concurrency capacity is fully occupied during retries.
- Between retries there is a fixed 500 ms `sleep`.
- Conductor normalizes `-1` (omitted by config author) to `3` at document-merge
  time. Mediapm may override this per tool (e.g. yt-dlp defaults to 1).

**Dedup and trace semantics**:
- Steps from multiple workflows started simultaneously do not see each other's
  in-flight cache entries, so naturally-identical steps across workflows may
  both execute (`executed_instances=N`) instead of one caching off the other
  (`executed_instances=1`, `cached_instances=N-1`). This is inherent to
  parallel dispatch, not a bug.
- The dependency-stream model has no `plan_level()` or `begin_level_metrics()`
  (both removed). The scheduler's `runtime_diagnostics()` falls back to
  `max(self.worker_pool_size, worker_metrics.len())` (worker_pool_size
  defaults to 0).
- `assigned_steps_total` is incremented via `record_completion()` using
  `saturating_add(1)` at each step completion.
- Trace events: `LevelPlanned` and `StepAssigned` no longer exist (removed
  with `plan_level`/`execute_level`). Only `StepCompleted` is emitted.

### Instance Key Lifecycle and Failure Recovery

**Instance Key Derivation** (`derive_instance_key()` in `src/conductor/src/step_worker/mod.rs`):

- Input: `BLAKE3(tool.name tagged + tool.metadata serialized + optional impure_timestamp + each input hash)`
- Operates on `BTreeMap<String, ResolvedInputKey>` — reads hashes directly from the state-stored type without loading any content bytes
- Deterministic for pure steps (same tool + inputs → same key)
- Impure steps include a timestamp, so each invocation produces a distinct key

**Failure Preservation** (coordinator error checkpoint at `src/conductor/src/orchestration/coordinator.rs:303-320`):

- On **both** success and error, the coordinator calls `commit_run(next_state: state.clone(), ...)` and advances `state_document.state_pointer`.
- On success: `pending_unsaved_hashes` is the accumulated set from all completed steps — all new CAS outputs are protected from GC.
- On error: `pending_unsaved_hashes: BTreeSet::new()` is empty — the failed step contributed no new CAS objects, and any in-flight steps' pending outputs (dropped by `FuturesUnordered`) are also unprotected.
- `state.clone()` preserves ALL current instances — no entries are discarded.

**OrchestrationState Immutability**:

- `OrchestrationState { version: u32, instances: BTreeMap<String, ToolCallInstance> }` is stored as an immutable CAS blob
- `ToolCallInstance.inputs` uses `BTreeMap<String, ResolvedInputKey>` (hash-only) — no content bytes are retained in state, keeping each entry at ~32 bytes per input regardless of content size
- The `instances` map is append-mostly — new entries are inserted on each successful step, but old entries may be removed by instance GC (see below) before the blob is persisted
- Old CAS blobs remain reachable as long as any caller holds their hash

**State Pointer Advancement**:

- The `state_pointer` advances on **every run** (both success and failure) — it always points to a CAS blob containing the latest checkpoint.
- The key difference between success and error is `pending_unsaved_hashes`: on the error branch it is empty, meaning unsaved-output GC protection is weaker.
- Old blobs are only unreferenced when `state_pointer` moves to a new blob that omits old entries.
- CAS garbage collection is explicit-only (`cas.delete()`); there is no active pruning of unreferenced `OrchestrationState` blobs.

**Implication**: A failed workflow step cannot cause previously **completed** steps to lose their I/O. The instance key change only affects the failed step's retry; all prior instances remain in the immutable state blob. However, any steps that were in-flight (dispatched but not yet completed) when the error occurred have their pending outputs dropped — those steps must be re-executed on the next run.

**Instance Garbage Collection**:

GC follows a two-phase reachability-first strategy:

1. **GC root reachability** (`referenced_instance_keys: HashSet<String>`): a
   runtime-only field on `OrchestrationState` (skip-serialized). Populated by
   `merge_step_result_into_state()` — every completed step's instance key is
   added. Referenced instances are NEVER evicted, regardless of age.
2. **Last-unreachable tracking** (`aux.<key>.last_unreachable`): a non-optional
   `ImpureTimestamp` inside `AuxData` (envelope-level aux map). The runtime
   type enforces non-null — `None` is only possible on the wire (in
   `Option<ImpureTimestampV2>` for backward compat) and is resolved to
   `now()` by the ISO bridge during deserialization. This timestamp is set
   when an instance becomes tracked as unreachable — the coordinator does
   NOT refresh `last_unreachable` on step completion.
3. **`gc_instances(cutoff)` method** (`OrchestrationState`):
   - Phase 1 — mark: for every instance key NOT in `referenced_instance_keys`
     that lacks an `aux` entry, inject `AuxData { last_unreachable: now }`.
     This ensures previously-unmarked instances get one GC cycle of protection
     before becoming eligible for eviction.
   - Phase 2 — evict: remove all instances whose key is NOT in
     `referenced_instance_keys` and whose `last_unreachable < cutoff` (epoch-seconds
     comparison with subsec-nanos tiebreaker).
     Because `last_unreachable` is non-optional in the runtime type, no `None`
     safety net is needed — the type system guarantees every entry is
     populated.
- **TTL configuration** (`RuntimeStorageConfig.instance_ttl_seconds`):
   Config option of type `Option<u64>`. `None` means "use the default".
   The coordinator resolves `None` to `DEFAULT_INSTANCE_TTL_SECONDS`
   (604 800 — 7 days) via `set_instance_ttl` before passing the value to
   the state-store actor. The state store never sees `None` after config
   processing; the actor is also spawned with the 7-day default at
   creation time.
   Cutoff is computed as `SystemTime::now() - Duration::from_secs(ttl)`
   before each persistence call.
- **GC trigger points**: `commit_run()` and `persist_and_publish_state()` in `StateStoreService` compute the cutoff and call `gc_instances()` before persisting the state blob to CAS. `SetInstanceTtl` cast message loads the TTL from runtime config into the state-store actor at startup.
- **MediaPM delegation**: `MediaRuntimeStorage.instance_ttl_seconds` is propagated through `apply_runtime_storage_defaults()` → `RuntimeStorageConfig.instance_ttl_seconds`, then into the conductor machine doc's runtime config.

### CAS GC Sweep

The CAS `CasMaintenanceApi` now exposes GC sweep capabilities:

- `list_all_hashes()`: Returns all content hashes tracked in the index. Backend implementations enumerate from their authoritative index (not on-disk directory walk).
- `gc_sweep(&self, roots: &BTreeSet<Hash>)`: Deletes all objects NOT in the root set. Computes `all_hashes - roots` and deletes orphans via `delete_many()`.

**Root set composition**: A shared `compute_gc_roots()` in `gc.rs` computes the root set from:
- `user.external_data` + `machine.external_data` values
- `state_pointer` (the current orchestration-state hash)
- Instance output/input pointers from the `OrchestrationState` pointed to by `state_pointer`

`content_map` entries are not iterated directly — the decode-time invariant (`vet_latest_envelope`) enforces `content_map` ⊆ `external_data`, so all content-map hashes are covered by external_data roots.

The CLI `run_gc()` command (in `src/conductor/src/cli/mod.rs`) is the primary consumer of this shared function.

**Sweep contract**: Deleting a non-root object that is a delta base of a root object is safe — the CAS backend handles rebasing automatically during deletion. Sweep does not consider constraint metadata for root-set computation; constraints are orthogonal to reachability.

**Early progress event**: `execute_workflows()` emits a synthetic `WorkflowStepEvent` with `total_steps: 1, completed_steps: 0` at the top of the method body, before dep-graph construction. This ensures the mediapm progress bar renders immediately even when the first real step event is delayed by cold-start overhead (Nickel eval, actor spawning).

**Background GC loop**: The conductor node actor spawns a background task in `pre_start` that:
1. **Waits** for the `gc_initialized` flag to be set (via `Acquire` load with 1-second polling), which happens after the first successful `LoadResolvedState` or `ReplaceResolvedState` call populates the coordinator's `external_data` roots. This prevents premature GC from sweeping all unprotected objects before state is loaded.
2. **Shared state**: The actor state holds `shared_external_data: Arc<RwLock<BTreeMap<Hash, ExternalContentRef>>>` (updated after each successful LoadResolvedState / ReplaceResolvedState / RunGc handler) and `shared_state_store: Arc<OnceLock<StateStoreClient>>` (populated by the SubmitWorkflow handler after `ensure_runtime_support()`).
3. **Enters a periodic loop**: reads the shared external_data snapshot and calls `run_cas_gc_sweep()` directly via the shared state store client (bypassing the actor mailbox entirely), then sleeps `GC_INTERVAL_SECONDS` (3600) and repeats. The `RunGc` handler is preserved for CLI use.

The `gc_initialized` flag is an `Arc<AtomicBool>` on `ConductorActorState`, shared with the background task. It is also set as a backstop after any successful `RunGc` handler execution.

The index compaction (`compact_index()`) persists the in-memory `IndexState` to a temporary redb, atomically replaces the active index file, then re-persists to catch any concurrent writes during the file-rename window.

> **⚠️ Agent policy — do NOT disable the background GC loop**: The `None` TTL passed to `RunGc` means "use configured/default" — this is correct. Agents must NEVER alter `GC_INTERVAL_SECONDS` to an absurdly large value or make the loop a no-op to avoid implementing GC properly. If the GC loop causes issues, fix the GC implementation — do not disable it.

**Instance TTL**: The config field `instance_ttl_seconds` is `Option<u64>`; `None` means "use the default". The coordinator resolves `None` to `DEFAULT_INSTANCE_TTL_SECONDS` (604 800 — 7 days) before passing to the state store; the actor also starts with the 7-day default at spawn time. Instance GC is never truly disabled — at worst it runs with a very generous TTL. When an explicit value is set, cutoff = `now - ttl`. Configured via `runtime.instance_ttl_seconds`.

> **⚠️ Agent policy — do NOT disable GC**: `None` means "use configured/default" — this is the correct semantic. Automatic coding agents (including GitHub Copilot, Cursor, Claude Code, Aider, and OpenCode) must NEVER set `instance_ttl_seconds` to an absurdly large value or otherwise disable GC out of laziness. Doing so causes unbounded state growth and degrades runtime performance over time. If a task seems easier with GC disabled, the correct fix is to improve the GC design, not to disable it.

**Deserialization guarantee**: After `decode_state()` runs, every instance
key has a corresponding `aux` entry with a non-optional `last_unreachable`.
The decode path injects `ImpureTimestamp::now()` at two points:
1. The V2 ISO bridge maps `Option<ImpureTimestampV2>` (wire) to
   `ImpureTimestamp` (runtime), converting `None` to `now()`.
2. A post-processing loop inserts `AuxData { last_unreachable: now }` for any
   instance key that still lacks an entry (no `aux` record at all).

This ensures the runtime `AuxData.last_unreachable` is always populated,
elliminating all `None`-checking from GC and other runtime code paths.
Type-enforcement replaces defensive validation.

### §16 Channel-Based Workflow Progress Events

Conductor no longer renders progress bars internally. Instead, it emits
workflow step completion events through an optional channel. The consumer
(mediapm service layer) creates the channel, owns the `MultiProgress` and
`ProgressBar` instances, and renders progress based on received events.

- **API types** (`src/conductor/src/api.rs`):
  - `WorkflowStepEvent` struct with fields: `total_steps: usize`,
    `completed_steps: usize`, `workflow_name: String`, `step_id: String`,
    `workflow_display_name: String`, `executed: bool`,
    `worker_index: usize`, `worker_count: usize`.
    Derives `Debug + Clone`.
  - `WorkflowProgressSender` type alias:
    `tokio::sync::mpsc::UnboundedSender<WorkflowStepEvent>`.
  - `RunWorkflowOptions.progress_sender: Option<WorkflowProgressSender>`.

- **Coordinator event emission** (`src/conductor/src/orchestration/coordinator.rs`):
  - `execute_workflows` accepts `progress_sender: Option<WorkflowProgressSender>`.
  - Before the dispatch loop, `total_steps` is computed as the sum of
    `ds.step_outputs.len() + ds.ready_queue.len()` across all `dep_states`.
  - After each step completion, if `progress_sender` is `Some`, a
    `WorkflowStepEvent` is sent via the channel with the step's worker index
    and total worker count.
  - Completed steps are tracked via a local `completed_steps` counter
    (`saturating_add(1)` per event) rather than re-computed from dependency
    state lengths, ensuring every dispatched event is counted exactly once.
  - The coordinator no longer imports or uses `pulsebar` at all.
  - No `MultiProgress` or progress bars are created in the coordinator.

- **Consumer rendering** (`src/mediapm/src/service.rs`):
  - `sync_library_with_tag_update_checks` creates an
    `mpsc::unbounded_channel`, a `MultiProgress`, and spawns a `tokio` receiver
    task.
  - On the first event, one overall bar and `worker_count` text-only worker
    lines are created. The overall bar uses format
    `"{msg}  [{bar:20}]  {pos}/{total}"` and worker lines use
    `mp.add_bar(0).with_format("{msg}")` (no bar, no total — pure text).
  - Per-worker step counts are tracked in a `Vec<usize>` and incremented on
    each event using `event.worker_index`.
  - The receiver task updates the overall bar's position and message on each
    event. The overall bar's per-event message uses the aggregate format
    `"completed {completed_steps}/{total_steps} steps"`. Worker lines show the
    current step and per-worker count:
    `"worker {wi}: {workflow}: {step}  ({count})"`.
  - When the channel closes (sender dropped), the overall bar shows
    `"all workflows complete"` and each worker line shows
    `"worker {wi}: done  ({count})"`. A 75 ms settle delay flushes the render
    thread.
  - For the filesystem CAS branch (no events), `rx` is dropped immediately
    and the handle awaited.
  - For the normal conductor branch, `tx` is dropped after the match
    completes and the handle awaited.


### §17 Tool Content Cache (`src/conductor/src/tool_cache/mod.rs`)

The `ToolContentCache<C>` struct at
`src/conductor/src/tool_cache/mod.rs`
is the sole authority over the `tools_dir/` directory tree. No external code
creates, reads, writes, or deletes anything inside cache directories — all
TTL checking, metadata management, locking, extraction, and pruning is
internal to this module.

**Public API**:
- `PAYLOAD_DIR_NAME` — literal `"payload"`, the subdirectory name inside
  each tool cache entry where extracted content lives.
- `sanitize_tool_id(name) -> String` — replaces reserved filesystem
  characters with `_`. Used by all callers to derive cache directory names.
- `ToolContentCache<C: CasApi + Send + Sync>` — shared mutable cache root.
  - `new(tools_dir, cas)` — construct with a shared CAS backend.
  - `materialize(tool_id, content_map, ...) -> ToolCacheEntry` — core
    API: returns a RAII-guarded path to the cached tool payload.
  - `link_to_sandbox(entry, sandbox_dir)` — associated fn that hard-links
    the cache entry's payload into a per-step sandbox.
  - `prune()` — remove expired TTL entries.
  - `retain_only(active_ids)` — remove cache directories not in the
    provided set. Used by mediapm lifecycle for sync-time cleanup.

**Lock protocol**: Per-entry `flock` advisory locking via `fs4::FileExt`.
- **Fast path (cache hit)**: non-blocking `try_lock_shared()` on
  `tools/<sanitized_id>/.lock`. Returns `ToolCacheEntry` on success.
- **Slow path (cache miss)**: DashMap + `OnceCell` prevents redundant
  extraction: the first caller acquires the entry, subsequent callers wait.
  Extraction acquires an exclusive `flock` via blocking `lock()` inside
  `spawn_blocking`. After extraction the `.lock` file is recreated and a
  shared-lock fd replaces the exclusive fd (downgrade, no unlock gap).
  A semaphore limits concurrent extractions across different tool IDs.
- **Prune**: non-blocking `try_lock()` exclusive. Skip entries that return
  `WouldBlock`.

**Guard lifecycle**: `ToolCacheEntry` (the return type of `materialize()`)
holds a shared-lock fd in an RAII guard. For direct-execution paths, the
entry is held across the entire process spawn so the cache entry cannot be
evicted mid-use. For one-shot callers (`resolve_managed_tool_executable`,
`run_managed_tool`), the entry is dropped immediately after use.

**Safety**: Locks are per-open-file-description (standard `flock` semantics).
Automatically released when the fd is closed — no manual unlock needed, even
if the holding task panics.

**Platform guard**: Locking is gated behind `cfg(unix)`. On non-Unix
platforms, `ToolCacheEntry` holds no fd and locking is a no-op.

**Cache ownership boundary**: `ToolContentCache` owns `tools_dir/*`
exclusively. External callers:
- Only receive `ToolCacheEntry` (path + RAII guard) from `materialize()`.
- Use `retain_only()` for bulk cleanup — never call `remove_dir_all` on
  cache directories.
- Never read/write `metadata.json` or check TTL externally.

**Sync-time stale-entry pruning**: When `mediapm sync` reconciles desired
tools, `prune_unmanaged_tool_artifacts` in `lifecycle.rs` computes the set
of stale tool IDs and passes them to `ToolContentCache::retain_only()`.
The cache module handles all filesystem cleanup. Prior to this design,
`lifecycle.rs` directly called `remove_dir_all` on cache directories;
this is now delegated to the cache module.

**Pruned entry filtering**: `compute_stale_entry_report` in `lifecycle.rs`
filters out entries whose `status == ToolRegistryStatus::Pruned`.


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
- **Module Hierarchy**: 12 submodules (config, paths, conductor_bridge, materializer, tools, etc.)
- **Public Trait**: `MediaPmApi` with `process_source()` and `sync_library()`
- **State Management**: Direct materialization; lock records for cache hits
- **Tool Provisioning**: User-level cache (downloads) vs. workspace cache (extracted binaries)
- **Materialization**: Link order preference (hardlink → symlink → reflink → copy)
- **HierarchyPath type**: `HierarchyNode.path` is a `HierarchyPath(Vec<String>)`
  newtype, not a raw `String`,
  - empty path (`vec![]`) is valid for root pass-through folder nodes,
  - serde serializes zero components as `""`, one component as `"abc"`, multiple
    components as `["a", "b"]`,
  - deserialize splits bare strings by `/` (consistent with `From<&str>`),
    rejecting empty components between delimiters via `trim_matches('/')`,
  - array form deserializes each element as one component (no further splitting),
  - `From<&str>` splits by `/` for ergonomic Rust construction; `Default` yields
    an empty path,
  - path components are validated at flattening time (non-empty, no `.`/`..`,
    NFD normalized),
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
  - the default sanitization mapping replaces `<` `>` `:` `"` `|` `?` `*` `/` `\\`
    with `_`; `/` and `\\` are included because they are path separators on
    Unix/Windows and must not appear within a single path component,
  - sanitization and validation operate per-component on the `Vec<String>`
    component pipeline (preserving legitimate `/` separators that delimit
    hierarchy components) rather than on the raw joined path string,
  - the materialization pipeline uses five stages on a `Vec<String>` component
    list:
    1. `check_nfd_source()` — rejects source components that are not NFD
       normalized,
    2. template resolution — resolves `${...}` placeholders per component
       (via `resolve_hierarchy_relative_path`),
    3. forced NFD normalization — each resolved component is normalized with
       `.nfd().collect::<String>()` to catch non-NFD output from template
       expansion,
    4. per-component sanitization — `sanitize_path_component()` replaces
       reserved characters (`<` `>` `:` `"` `|` `?` `*` `/` `\\`) using the
       effective replacement map,
    5. `validate_components()` — ensures all components are NFD, non-empty,
       and free of `.`/`..`/reserved chars, then joins with `"/"` into the
       final relative path,
  - `sanitize_path_component` is the per-component replacement function: it
    applies the effective replacement map to a single component string,
  - `validate_components` is the post-resolution validation function that checks
    NFD normalization, forbidden characters, and empty/`.`/`..` segments,
- **Hierarchy path flattening-time validation**:
  - `validate_hierarchy_path_component()` runs at flattening time (in
    `hierarchy_types.rs`) over each component of `HierarchyPath::components()`,
  - `validate_hierarchy_path_component` forbids empty components, `.`/`..`
    segments, and non-NFD-normalized content,
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
    `media_folder` entry gets an independent working directory so that
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
- **Media metadata resolution policy**: When adding a media source, metadata
  is resolved through independent fallback chains for 6 persisted slots.
  MBID (recording/release) is restricted to the media-tagger step options — it
  never feeds into `MediaSourceSpec.title`, `MediaSourceSpec.artist`,
  `MediaSourceSpec.description`, `metadata["title"]`, `metadata["artist"]`, or
  `metadata["album"]`. The final fallback for every slot is the literal string
  `"unknown"`. The six slots and their chains are:

  | Slot | CLI | Remote (yt-dlp) | Local (ffprobe) | Fallback |
  |---|---|---|---|---|
  | `MediaSourceSpec.title` | `--title` | metadata.title | format.tags.title/track | `"unknown"` |
  | `MediaSourceSpec.artist` | `--artist` | uploader/channel/artist/creator | format.tags.artist/album_artist | `"unknown"` |
  | `MediaSourceSpec.description` | `--description` | metadata.description | format.tags.description/comment/synopsis | auto-build → `"unknown"` |
  | `metadata["title"]` | `--title` literal (prepended) | `Video:title` → `Video:track` → `Infojson:title` / `media:title` | resolved `MediaSourceSpec.title` → `"unknown"` |
  | `metadata["artist"]` | `--artist` literal (prepended) | `Video:artist` → `Video:album_artist` → `Infojson:uploader` / `media:artist` → `media:album_artist` | resolved `MediaSourceSpec.artist` → `"unknown"` |
  | `metadata["album"]` | `--album` literal | — | — | absent (not inserted) |

  The metadata map uses a `Fallback` chain for `title` and `artist` where
  explicit CLI values are **prepended** as `Literal` candidates before the
  Variant sources, keeping the source-derived literal as the final fallback.
  The `album` entry is a single-entry `Literal` that is only present when the
  `--album` flag is explicitly passed; it has no fallback chain and no
  source-derived behavior.

  The auto-built description template differs between flows:
  - Remote: `"title: {title}\nartist: {artist}"`
  - Local: `"file: {filename}\ntitle: {title}\nartist: {artist}"`
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
- **Companion resolution independent of `should_set_*` guards**: companion
  path resolution (ffmpeg, deno for yt-dlp) is always performed during tool
  reconciliation whenever yt-dlp is provisioned, independent of
  `should_set_yt_dlp_ffmpeg_location` and `should_set_yt_dlp_js_runtimes`
  guards. The guards only control whether `input_defaults` receives the
  template ref string (e.g. `${env.MEDIAPM_YT_DLP_FFMPEG_LOCATION}`). Env var
  generation fires whenever companion paths resolve, ensuring
  `.env.generated` is refreshed on every `media tool sync` run regardless of
  prior runs.
- **`media sync` env dependency**: `sync_library_with_tag_update_checks()`
  (`media sync`) does not call `reconcile_desired_tools()`. Users must run
  `media tool sync` before `media sync` to ensure `.env.generated` is
  current. This is by design — tool sync provisions companion binaries and
  generates runtime env vars, while library sync consumes them.
- **mediapm_dir resolution contract**:
  `MediaRuntimeStorage.mediapm_dir` → `MediaPmPaths::with_runtime_storage()`
  → `runtime_root` → all dependent paths (tools, cache, state, env files,
  schema export, tmp). Default is `<root_dir>/.mediapm`. Relative paths
  resolve against the `mediapm.ncl` parent directory.
- **Tool identity preservation during workflow re-synthesis**: when sync runs
  against a previously-synthesized workflow, `preserve_existing_generated_step_tools()`
  rewrites each generated step's tool id from the existing workflow snapshot.
  The function implements a 2-way decision per step:
  - If `previous.tool == generated.tool`: the tool id is kept as-is; validity
    is checked via `preserved_step_tool_is_valid()` (ensures the tool still
    exists in `machine.tools` and `Executable` kinds have non-empty `content_map`
    in `machine.tool_configs`).
  - If the tool identity differs from the previously-synthesized one but the
    previous tool is still valid (exists in `machine.tools` with required
    `content_map`), `generated.tool` is rewritten to `previous.tool.clone()`.
    This preserves the old tool id and keeps the impure timestamp stable — tool
    version updates alone do NOT trigger a refresh cascade. The rewrite applies
    uniformly across all tools regardless of name.
  - If the previous tool is no longer valid (pruned from `machine.tools` or
    missing `content_map` for `Executable` kinds), mismatch is flagged to
    install the newly-generated identity.
  - Returns `true` when every generated step id was found in `existing` and
    the tool id is unchanged (or was successfully preserved) and still valid.
- **Two-tier impure timestamp system**: MediaPM and Conductor maintain
  separate impure timestamp domains with different triggers:
  - **MediaPM-owned timestamps** (`workflow_states[media_id][index].impure_timestamp`):
    track step config identity transitions. Timestamps are withheld during
    workflow synthesis (`None` when `requires_refresh` is true) and written
    after the conductor workflow completes — the
    `sync_library_with_tag_update_checks()` backfill pass iterates state doc
    step states and stamps `fresh_impure_timestamp()` where `impure_timestamp`
    is `None`. A timestamp refresh requires an explicit configuration change
    in `mediapm.ncl` — tool version updates alone do NOT update mediapm
    timestamps.
  - **Conductor-owned timestamps** (`impure_timestamps[workflow_name][step_id]`
    in conductor state doc): authoritative for instance key derivation in
    `derive_instance_key()`. These are entirely separate from mediapm timestamps.
    Step IDs use `MediaStepTool` enum values, which are stable across tool
    versions.
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
| **MediaPM** | `mediapm.ncl`, `state.ncl` | Top-level `version: u32` | `config/versions/` |

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
- **Atomicity**: CAS errors propagate directly via `?` regardless of workflow purity; no auto-retry on CAS failure
- **Recovery**: MediaPM cleans up on failure; state.ncl unchanged
- **Diagnostics**: Error messages include actionable context (path, hash, expected vs. actual)

---

## Testing Strategy Across Crates

### Unified Test Organization

All crates follow this pattern:
```
tests/
├── mod.rs                # Entry module
├── e2e/                  # End-to-end workflows
├── int/                  # API-level integration
└── prop/                 # Property-based (reserved)
```

### Coverage Expectations

| Category | CAS | Conductor | Builtins | MediaPM |
|----------|-----|-----------|----------|---------|
| **Happy Path** | put → get | user → machine → execute | valid args → correct output | sync roundtrip |
| **Validation** | Constraint logic | Document merging | Fail-fast keys | Lock reconciliation |
| **Error Paths** | NotFound, Codec | CAS errors propagate directly | Invalid input | Materialization rollback |
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

### 6. Direct Materialization

**Pattern**: Materialize directly to final output paths; temp extraction only for zip processing

**Used By**: MediaPM (via conductor)

**Benefit**: Simpler, faster; CAS integrity trusted by default

### 7. Content-Addressed Memory Lifecycle

**Pattern**: Use `bytes::Bytes` for all CAS-resident data to enable zero-copy sharing and cheap clones (ref-count bumps). Avoid `Vec<u8>` for hot-path CAS data in public APIs.

**Rationale**:
- CAS `get()` returns `Bytes` — clone is O(1) ref-count increment, not O(n) memory copy
- `materialize_to_path()` skips the `Bytes` round-trip entirely when the backend can fast-path via `fs::copy`
- `ResolvedInput.plain_content` uses `Bytes` so cloning resolved inputs for step invocation is O(1) instead of O(content_size)
- File content comparisons operate on `&[u8]` slices without allocating new buffers

**Examples**:
- `CasApi::put<D: TryInto<Bytes>>(data: D)` — accepts anything that converts to `Bytes`
- `CasApi::get(hash) -> Result<Bytes>` — returns shared ref-counted buffer
- `CasApi::get_stream(hash) -> CasByteStream` — streams large objects in 256 KiB chunks
- `CasApi::materialize_to_path(hash, dest) -> Result<()>` — writes directly without returning bytes to caller
- `ResolvedInput.plain_content: Bytes` — step inputs are cheap to clone across worker boundaries

**Used By**: CAS, Conductor (step worker input resolution), MediaPM (materialization)

**Benefit**: Reduced memory pressure on large syncs; no unnecessary copies on hot path; pluggable fast path for filesystem backend.

### 8. Two-Phase Input Resolution (Hash-First, Content-On-Demand)

**Pattern**: Split input resolution into two passes so the state-stored `ToolCallInstance` carries only lightweight hash references. Full content (`Bytes`) is loaded only for inputs that templates actually reference at execution time.

**Rationale**:
- `ToolCallInstance.inputs` is persisted to CAS in `OrchestrationState`. Storing full `ResolvedInput` (with `plain_content: Bytes`) in every instance would pin GB-scale data in long-lived CAS blobs, even for steps whose outputs are never re-executed.
- Most step inputs are materialized as files and never need in-memory content — only template-referenced inputs (via `${...}`) require content bytes.
- Separating the hash (state identity) from the content (execution payload) follows the functional-core / imperative-shell principle: state is pure identity, content is ephemeral runtime.

**Design**:

- **`ResolvedInputKey { hash: Hash }`** — a hash-only type used in `ToolCallInstance.inputs: BTreeMap<String, ResolvedInputKey>`. Stores only the content hash; occupies ~32 bytes per entry regardless of content size.
- **`ResolvedInput`** — the full content type, kept for the execution hot path (`step_worker`). Includes `plain_content: Bytes` alongside the hash. Used only during active step execution, then dropped.
- **Two-pass resolution in `StepWorker`**:
  1. **Pass 1 (hash resolution)**: Resolve all bindings to their CAS hashes. Produce `BTreeMap<String, ResolvedInputKey>`. No CAS `get()` is called — only `HashConstraint` evaluation and `content_map` key lookups. This map is stored in `ToolCallInstance.inputs`.
  2. **Pass 2 (content loading)**: Scan step templates for `${input_name}` or `${input_name.path}` references. For each referenced input, call `cas.get(hash)` to load `Bytes` content. Produce `BTreeMap<String, ResolvedInput>` only for the referenced subset. Unreferenced inputs remain hash-only — zero content loaded.
- **ZIP member selectors** (`hash#member_path`): Pass 1 resolves the parent archive hash only. If a template references a ZIP member selector, Pass 2 loads the full archive, extracts the member, hashes the extracted content, and returns it as `ResolvedInput`. The archive `Bytes` is dropped after extraction — only the member's Bytes is retained.

**Invariants**:
- Every binding in the original step spec MUST resolve to a hash in Pass 1. A binding that fails hash resolution (missing `content_map` entry, unresolvable `from` reference) is a hard error.
- Pass 2 content loading is lazy: only inputs whose name appears in a template expression are loaded. Inputs that are materialized only via `content_map` → file write are never loaded into memory.
- `ResolvedInputKey` is comparable and hashable — instance key derivation (`derive_instance_key`) uses input hashes directly without loading content.
- `ToolCallInstance.inputs` stores `ResolvedInputKey` exclusively. `ResolvedInput` exists only transiently during `step_worker` execution.

**Examples**:
- `ResolvedInputKey { hash }` — state-stored identity reference
- `ResolvedInput { hash, plain_content: Bytes }` — ephemeral execution context
- Pass 1: `resolve_input_binding_hash(binding) -> Hash` — no CAS content read
- Pass 2: `scan_template_referenced_inputs(template) -> BTreeSet<String>` then `load_inputs_content(referenced) -> BTreeMap<String, ResolvedInput>`
- Instance key: `derive_instance_key(tool_id, inputs: &BTreeMap<String, ResolvedInputKey>)` — pure hash comparison

**Used By**: Conductor (state model, step worker)

**Benefit**: Eliminates GB-scale content retention in `OrchestrationState` CAS blobs. Post-execution memory per instance drops from O(content_size) to O(32 bytes). Peak execution memory only loads the subset of inputs that templates reference — for file-only pipelines, zero content bytes are loaded into memory.

---

## Performance Considerations

### Hot Paths

| Path | Target | Technique |
|------|--------|-----------|
| **CAS read** (full object) | O(file_size) | mmap for ≥64KB; buffer pool for small |
| **CAS delta read** | O(depth × patch_size) | Concurrent candidate scoring (8 tasks) |
| **Conductor planning** | < 10ms | Level-based topological sort (no DAG simulation) |
| **Conductor scheduling** | EWMA cost model + O(1) batch cache probe | Step-stream batch dispatch; `exists_many` via `CasExistenceBitmap` |
| **CAS stream read** (large object) | O(file_size) | Streaming chunks (256 KiB) via `stream::unfold`; small objects ≤256 KiB read in one chunk |
| **CAS materialize** (full object fast path) | O(file_size) | `fs::copy` for filesystem backend — kernel-level copy, no userspace buffer allocation; delta fallback via `get()` + write |
| **MediaPM sync** | Parallel workflows + step-stream dispatch | Bounded worker pool; cross-workflow step-stream dispatch in execution hub |

### Resource Bounds

| Resource | Default | Config |
|----------|---------|--------|
| Delta chain depth | 32 | `MAX_DELTA_DEPTH` |
| Buffer pool size | 128 | `FILESYSTEM_STREAM_BUFFER_POOL_MAX_BUFFERS` |
| Actor RPC timeout | 8 sec | `FILESYSTEM_OBJECT_ACTOR_RPC_TIMEOUT_MS` |
| Conductor RPC timeout | 300 sec | `MEDIAPM_CONDUCTOR_RPC_TIMEOUT_SECONDS` |
| Optimizer concurrency | 8 | `FILESYSTEM_CANDIDATE_EVAL_CONCURRENCY` |
| Materialization workers | CPU cores | Derived from hardware |

**pulsebar rendering:**
- terminal-width contract: all progress messages must fit within the terminal
  width; detected via `terminal_size` crate; defaults to 80 cols,
- step preview degrades gracefully (truncation with `...` suffix,
  `+N more` counter, ...) to respect the available width.

**Recovery memory**: CAS `repair_index()` uses `O(delta_count × delta_size)`
memory instead of `O(total_store_bytes)` — full objects are streamed and
discarded; only delta-object bytes are held in memory for chain reconstruction.

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

## Index Repair & Recovery Scan

The CAS `repair_index()` operation rebuilds the index from the actual storage
contents. The scan pipeline uses a two-pass approach to minimize memory
pressure:

**Pass 1 — Catalog scan**: Walk the storage backend and classify each object
into a `ScannedObjectCatalog` with two maps:

| Map | Type | Contents |
|-----|------|----------|
| `full_objects` | `BTreeMap<Hash, ObjectMeta>` | Metadata only (hash, size, compression). Stream-verified during scan; bytes discarded after verification. |
| `delta_objects` | `BTreeMap<Hash, StoredObject>` | Full bytes retained in memory. Needed for delta-chain reconstruction. |

**Pass 2 — Index reconstruction**: Walk the delta chain roots reachable from
`delta_objects`, reconstruct full content on demand, and insert entries into
the rebuilt index.

**Memory model**: Recovery memory is `O(delta_count × delta_size)` instead of
`O(total_store_bytes)`. Full-object bytes are streamed and discarded; only
delta-object bytes are held in memory for reconstruction. The validation memo
caches only delta reconstruction results; full objects are re-read on demand
during the final verification pass.

**Error handling**: CAS errors propagate via `?` regardless of workflow purity; no auto-retry on CAS failure.

---

## Key References & Documentation

### CAS Reference
- **Public Traits**: `CasApi`, `CasMaintenanceApi`
- **Types**: `Hash`, `Constraint`, `ConstraintBatchOp`, `ObjectInfo`, `OptimizeReport`
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
- **Materialization**: Direct to final output paths; temp extraction only for zip processing

---

## Filesystem Locking

The `FileSystemCas` backend uses an advisory lock file to coordinate access
across processes:

| Property | Value |
|----------|-------|
| Lock file location | `<store_root>/lock` |
| Lock type | `fs4::fs_std::FileExt::try_lock_exclusive()` (non-blocking) |
| Scope | Per-store-filesystem — all `FileSystemCas` instances sharing the same root |
| Release | On `File` drop (closes file descriptor) |
| Error type | `CasError::StoreLocked { root: PathBuf }` |
| Wait behavior | `FileSystemRecoveryOptions.wait_for_lock: bool` (default `false`). When `true`, retries in a loop with backoff instead of failing immediately. |
| State | `FileSystemState.lock_file: Option<File>` — held for the lifetime of the `FileSystemCas` instance |

**Contract**: The lock is advisory — cooperative processes must respect it.
Non-cooperative processes (e.g., a direct `cp` or `rsync` into the store) are
not prevented but risk corrupting the index or creating inconsistent state.

---

## Known Limitations

- **Advisory lock**: The store lock is advisory only. Cooperative processes
  that attempt `try_lock_exclusive()` will be serialized, but a process that
  bypasses the lock (direct filesystem manipulation, a CAS client built without
  locking) can still cause concurrent-access corruption.
- **Index false negatives**: Index-backed existence checks may return `false`
  for objects that exist in storage (conservative by design). Callers must
  fall back to storage for a definitive answer.
- **Manual filesystem modification**: Direct manipulation of files under the
  CAS store root (adding, removing, or modifying files outside the CAS API) is
  unsupported and may produce silently incorrect index state.
- **Recovery scope**: `repair_index()` only verifies and rebuilds the index
  from existing storage objects. It does not detect or repair corrupted object
  content (bit rot) — that requires an external integrity-verification tool
  such as a periodic `blake3sum` audit.
- **Parallel sync**: MediaPM lock file and state documents are not designed for
  concurrent writers. Only one `mediapm sync` at a time per workspace.

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

### Why Direct Materialization Instead of Staged-and-Commit?

**The Problem**: The old staged-and-commit approach wrote all output to a staging directory, verified hashes, then atomically renamed to the final location. This added complexity, required extra disk space for staging, and slowed materialization.

**Direct Materialization Solution**:
1. Materialize directly to final output paths from CAS
2. CAS integrity is trusted by default — objects validated at CAS put time, not at materialization time
3. Lock file updated after successful materialization

If anything fails mid-way, partial files are cleaned up, and re-run resumes from the lock record (unfinished entries have no lock record, so they are re-materialized).

**Example**:
```
Step 1: CAS → /media/library/song/
        ├─ song.mp3 (from CAS)
        ├─ song.jpg (from CAS)
        └─ song.txt (from CAS)

Step 2: Update lock file

Step 3: Done. All files at final location, lock file consistent.

Temp extraction directory (`mediapm_tmp_dir`) is used only for zip processing
and sandbox isolation — not for staging materialization output.
```

**Trade-off**: No atomic-materialization safety net for CAS integrity failures. Benefit: simpler, faster, less disk overhead. CAS errors propagate directly via `?` regardless of workflow purity.

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
1. **Crash during materialization**: Files already written to output paths, but lock file write failed
2. **Disk full**: Materialization succeeded, but lock write failed
3. **Permission error**: Earlier writes succeeded, later permission check failed

**Debug**:
```bash
# Check lock file for partial entries
$ cat ~/.mediapm/lock.ncl | grep -E "media_id|cas_hash"

# Check for orphaned output files
$ ls -la ~/.mediapm/output/ 2>/dev/null

# Check permissions
$ ls -la ~/.mediapm/lock.ncl
```

**Solution**:
- Manually delete orphaned lock entries or re-run with `--force-resync`
- Clean up orphaned output files: remove files in output paths that have no lock record
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
│ Materialization                                       │
│ ┌─────────────────────────────────────────────────┐   │
│ │ 1. Get final output hash from Conductor         │   │
│ │ 2. CAS.get(hash) → bytes                        │   │
│ │ 3. Write directly to final output path          │   │
│ │ 4. Set permissions, read-only bit               │   │
│ │ 5. Update lock file (media_id → hash)           │   │
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
    └───────────┘   ┌──────▼────────────────┐
                    │ MATERIALIZE PHASE     │
                    │ 1. CAS.get(hash)      │
                    │ 2. Write to output    │
                    │ 3. Set perms          │
                    └──────┬────────────────┘
                           │
              ┌────────────┴───────────────┐
              │                           │
         ┌────▼──────┐         ┌─────────▼──┐
         │ SUCCESS   │         │ FAILURE    │
         │ → PERSIST │         │ → CLEANUP  │
         └────┬──────┘         └────────┬───┘
              │                         │
    ┌─────────▼──────────┐   ┌──────────▼──────────┐
    │ PERSIST PHASE      │   │ CLEANUP PHASE       │
    │ (update lock file) │   │ (remove partial     │
    └────────────────────┘   │  output files)      │
                             └─────────────────────┘
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
| **ConstraintBatchOp** | Batch operation for setting and patching constraints — the preferred API for multi-op constraint mutations. `Set` validates target + bases exist; `Patch` merges new bases into existing constraints. | `ConstraintBatchOp::Set { target_hash, potential_bases }` |
| **Delta Encoding** | Compression technique: store only differences from a base object. Saves space for similar files. | `delta { base: hash1, patch: diff_bytes }` |
| **Object** | Unit of data in CAS. May be raw bytes (full object) or delta-encoded (delta object). | Full: 10MB video; Delta: 50KB patch from previous version |
| **Builtin** | Standard tool provided by Conductor. Five builtins: echo, fs, archive, import, export. | `{ tool = "echo", args = { message = "hello" } }` |
| **Managed Tool** | External tool provisioned by MediaPM (ffmpeg, yt-dlp, media-tagger). Downloaded, cached, versioned. | `tool_config.ffmpeg = { version = "7.0.0", ... }` |
| **Workflow** | Directed acyclic graph (DAG) of steps. Each step invokes a tool; output feeds to next step. | `steps: [import, ffmpeg, media-tagger, export]` |
| **Step** | Single operation in a workflow. Invokes one tool with input bindings. | `{ id = "convert", tool = "ffmpeg", args = { format = "mp3" } }` |
| **OrchestrationState** | Complete state of workflow execution: which steps ran, outputs, errors. Persisted to CAS. | Includes per-step: id, status, output hash, error message |
| **Staging Directory (legacy)** | Previously used for intermediate temp storage before final commit. Direct materialization replaced this. | No longer used. Temp extraction uses per-workspace `mediapm_tmp_dir`. |
| **Lock File** | Records what's been synced: media_id + variant → final CAS hash. Enables cache hits on re-run. | `{ media_id = "song_1", variant = "primary", cas_hash = "..." }` |
| **Media Source** | Origin of media data: URL, local file, CAS hash, etc. Specified in `mediapm.ncl`. | `source = "https://example.com/video.mp4"` |
| **Hierarchy** | Folder/media organization in output directory. Specifies which media go where. | `hierarchy = [{ kind = "folder", name = "Music", children = [...] }]` |
| **Variant** | Output type for media (primary, audio, thumbnail, etc.). One media entry → multiple variants. | Media: song.mp3; Variants: primary, lyrics, cover_art |
| **Materialization** | Process of writing CAS objects to disk at final output paths. CAS integrity trusted by default. | CAS object → /media/library/song/song.mp3 |
| **Pure** | Operation with deterministic output. Same input always → same output. No side effects. | `echo` and `archive` are pure; `fs` and `import` are impure |
| **Impure** | Operation with non-deterministic or side-effect output. May vary on retry. | `import` (network), `fs` (file system), `export` (disk I/O) |
| **Determinism** | Property: identical inputs produce identical outputs. Required for cache hits and reproducible builds. | If sync ran twice without config change, skip all steps (cache) |
| **Fail-Fast** | Validation before execution. Invalid inputs rejected immediately, before side effects. | Builtin: `--arg unknown_key val` rejected before running tool |
| **Atomicity** | Operation succeeds or fails completely. No partial state. Transactional. | Materialization: All files written or none (no orphaned files) |
| **Index** | CAS metadata: (hash → storage location, compressed status, size). Updated on every put/optimize. | Maps `hash:abc123` to `/store/ab/c123` on disk |
| **Repair** | CAS maintenance operation: scan storage, rebuild index, detect orphaned objects. | `cas.repair_index() → IndexRepairReport` |
| **Optimize** | CAS maintenance: convert full objects to delta-encoded to save space. | `cas.optimize() → OptimizeReport { bytes_saved: 5GB }` |
| **Stream** | CAS read returning an async `Stream<Item = Result<Bytes>>` instead of a single buffer. Enables processing large objects without loading them entirely into memory. | `cas.get_stream(hash) → CasByteStream` |
| **Materialize** | CAS read that writes object bytes directly to a file path without returning the full buffer. Fast path uses `fs::copy` in the filesystem backend for full objects. | `cas.materialize_to_path(hash, dest) → Result<()>` |
| **CasByteStream** | Type alias for `Pin<Box<dyn Stream<Item = Result<Bytes, CasError>> + Send + 'static>>`. Produced by `get_stream()`. Streams 256 KiB chunks for large objects; single chunk for ≤256 KiB. | Stream of `Result<Bytes>` |
| **CasByteReader** | Type alias for `Box<dyn AsyncRead + Unpin + Send + 'static>`. Alternative streaming interface for async-read consumers. | AsyncRead adapter |

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

Note the two-tier timestamp architecture: tool version updates change the
conductor timestamp (affecting conductor instance key derivation) but do NOT
change the mediapm impure timestamp. MediaPM timestamps only refresh on
explicit `mediapm.ncl` step config changes. This means a tool version rollback
alone does not force workflow re-execution from mediapm's perspective — only
conductor's instance keys change, which affects runtime state but not the
mediapm-level workflow plan.

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
│   │       ├── mod.rs (harness)
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
│   │       ├── mod.rs (harness)
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
│       │   ├── paths.rs (runtime path resolution)
│       │   ├── conductor_bridge.rs (ConductorApi integration)
│       │   ├── materializer.rs (materialization)
│       │   ├── tools.rs (tool provisioning)
│       │   └── error.rs (MediaPmError)
│       └── tests/
│           ├── mod.rs (harness)
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
