# mediapm — Master Implementation Plan (Rewritten)

This document is the implementation contract for `mediapm`.

It rewrites the previous plan with one non-negotiable goal:
**build a simple, high-performance, fully functional, incremental, async,
runtime-agnostic, actor-based Rust system that remains modular and testable as
it scales**.

It is aligned with:

- `PLAN_PHASE_1.md` (Unified Delta CAS)
- `PLAN_PHASE_2.md` (Conductor functional orchestration)
- `PLAN_PHASE_3.md` (Media orchestration and materialization)

---

## 1) Hard principles (must always hold)

1. **Simplicity first**
   - Choose the smallest design that preserves correctness and extensibility.
   - Prefer explicit data flow over clever abstractions.

2. **Performance as a feature**
   - Optimize for cache locality, branch predictability, allocation minimization,
     bounded copying, and deterministic I/O behavior.
   - Use profiling + benchmarks before and after optimization.

3. **Functional core, imperative shell**
   - Planning, diffing, normalization, and key derivation are pure functions.
   - Mutable state and side effects are pushed to system boundaries.

4. **Incremental by default**
   - Every major operation computes minimal deltas and avoids full rebuilds.
   - Cache keys are explicit and content-addressed.

5. **Async everywhere it matters**
   - All I/O and orchestration paths are async.
   - Blocking operations are isolated behind bounded worker interfaces.

6. **Runtime agnostic architecture**
   - Tokio is the default runtime.
   - Runtime-specific code is isolated behind thin adapters.
   - Core/application layers never depend directly on Tokio types.

7. **Actor pattern for parallelism and extensibility**
   - Parallelism and lifecycle orchestration use `ractor` actors.
   - Supervision trees are explicit.
   - Messages are typed and versionable.

8. **Extreme modularity**
   - Keep stable module boundaries and contracts.
   - Prefer composable traits + generics over monolithic implementations.

9. **Type-system-enforced invariants**
   - Invalid states should be unrepresentable whenever practical.
   - Use newtypes, typestates, constrained constructors, and exhaustive enums.

10. **Macro use is pragmatic**
    - Use macros to remove repetitive boilerplate in messages, errors,
      registrations, and test fixtures.
    - Do not hide critical control flow in opaque macros.

11. **Documentation is part of the API**
    - Public modules/types/functions require clear Rustdoc explaining semantics,
      invariants, side effects, and failure modes.

12. **Quality gates are mandatory**
    - Formatting: `rustfmt`
    - Linting: `clippy` (with strict CI gates)
    - Tests: unit + integration + end-to-end + property + concurrency where
      applicable.

---

## 2) Research-backed technology baseline

This plan uses existing libraries where they provide strong leverage and keeps
targeted custom implementations where project-specific performance or semantics
demand it.

### Core crates

- **Actors / orchestration:** `ractor`
  - Runtime features: `tokio_runtime` (default), `async-std` (optional)
  - Supervision, messaging priority, actor lifecycle hooks.
- **Hashing:** `blake3`
  - SIMD-accelerated, strong performance for CAS identity.
- **Async abstraction:** `futures`, optional `async-trait` (where useful)
- **Tracing:** `tracing`, `tracing-subscriber`
- **Serialization:** `serde`, `serde_json` (deterministic writer policy)

### Performance-oriented crates (adopt with measurement)

- `bytes` (zero-copy buffer ownership/slicing)
- `smallvec` (small inline collections)
- `hashbrown` (SwissTable map/set behavior)
- `ahash` (fast keyed hashing when HashDoS is irrelevant)
- `memmap2` (large file read/write patterns where safe)

### Testing and verification

- **Unit/integration/e2e:** `cargo test` (+ `cargo-nextest` optional runner)
- **Property tests:** `proptest`
- **Concurrency permutation tests:** `loom` (for lock-free/atomic-sensitive code)
- **Benchmarks:** `criterion` (or `divan` for focused microbench suites)

### Migration optics

- Candidate libraries: `panproto-lens`, `karpal-optics`
- Rule: if external optics do not cleanly satisfy bidirectional,
  schema-evolution, and maintenance requirements, implement a lightweight
  internal optics layer (`Lens`/`Prism`/`Traversal`) specialized for sidecar
  migrations.

---

## 3) Target architecture (phase-composed)

`mediapm` is a three-layer composed system:

1. **Phase 1: CAS** — content-addressed, diff-aware storage foundation.
2. **Phase 2: Conductor** — functional orchestration over CAS.
3. **Phase 3: mediapm** — media-domain orchestration, metadata policy,
   hierarchy materialization, tool lifecycle.

### 3.1 Layering and boundaries

Core source boundaries:

- `domain/`: pure value types, invariants, canonicalization, migration specs.
- `application/`: pure planning + actor message contracts.
- `infrastructure/`: persistence, filesystem, external tools, metadata providers.
- `configuration/`: config schemas and load/save adapters.
- `support/`: utilities shared across layers.

**Dependency rule:** lower-level pure modules must not depend on higher-level
I/O modules.

---

## 4) Actor system model (ractor-first)

All parallel and extensible behavior is actor-driven.

### 4.1 Top-level actor groups

- `CasSupervisor`
  - `StorageActor`
  - `IndexActor`
  - `OptimizerActor`
- `ConductorSupervisor`
  - `WorkflowPlannerActor`
  - `WorkflowExecutorActor`
  - `ToolRegistryActor`
  - `StateStoreActor`
- `MediaPmSupervisor`
  - `SourceProcessorActor`
  - `MaterializerActor`
  - `MetadataActor`
  - `ToolsmithActor`
  - `LockfileActor`

### 4.2 Supervision policy

- Explicit restart policies per child actor.
- Distinguish recoverable operational faults from invariant violations.
- Panic handling is explicit and consistent with runtime configuration.

### 4.3 Messaging constraints

- Message enums are versionable and documented.
- Payloads use strongly typed newtypes (hashes, canonical URIs, normalized
  paths, actor ids).
- Prefer bounded message size with content references for large payloads.

---

## 5) Runtime-agnostic async contract

### 5.1 Runtime interface boundary

Define runtime-facing traits for:

- spawn / join tasks
- timers / sleep / timeout
- async fs/process adapters
- cancellation signaling

Tokio adapter is default; alternative adapters (e.g., async-std) implement the
same traits.

### 5.2 Runtime isolation rules

- No Tokio-specific types in domain and planner signatures.
- Runtime-specific code only in adapters and executable shell.
- Actor code uses `ractor` abstractions and internal runtime traits.

---

## 6) Data and invariant model

## 6.1 Identity and canonicalization

- URI is the canonical identity key.
- Content identity is BLAKE3 hash newtype.
- Paths for materialization must satisfy strict portability constraints
  (including NFD-only requirement and rejected characters).

### 6.2 CAS invariants (Phase 1)

- Object storage path fan-out is deterministic.
- Diff graph remains acyclic.
- Every index entry points to existing content.
- Optimizer never violates reconstructability.

### 6.3 Conductor invariants (Phase 2)

- Tool call instance key excludes tool content map and effective persistence
  flags (as specified in phase plan).
- Effective persistence flags are merged deterministically.
- Missing non-saved outputs trigger controlled re-execution.

### 6.4 mediapm invariants (Phase 3)

- Lockfile tracks every managed materialized file and safety external data.
- Staging is always under `.mediapm/tmp/` before atomic commit.
- Link fallback order is deterministic and logged.
- Permanent transcode safety references are retained and pruneable by policy.

---

## 7) Migration strategy using optics (bidirectional-capable)

Migration logic is represented as composable optics transformations.

### 7.1 Requirements

- Sequential version hops (`vN -> vN+1`) with explicit registry.
- Each hop records provenance.
- Forward and backward transforms are preserved where semantically possible.
- Migration chain serves as living historical reference of schema evolution.

### 7.2 Optics model

- `Lens`: deterministic field-level transformations.
- `Prism`: enum/optional branch transforms.
- `Traversal`: repeated/nested collection transforms.

### 7.3 Law checks

- Enforce round-trip properties where transformations are lossless.
- For lossy migrations, enforce documented one-way guarantees and explicit
  complement data handling.

---

## 8) Performance engineering plan

Performance work follows a strict loop:

1. profile
2. hypothesize
3. optimize
4. benchmark
5. keep or revert

### 8.1 Hot-path rules

- Prefer contiguous memory layouts and compact hot structs.
- Minimize allocations in tight loops (`with_capacity`, workhorse buffers,
  `SmallVec` when measured beneficial).
- Keep common branches cheap; isolate cold paths (`#[cold]` where appropriate).
- Avoid unnecessary cloning and intermediate allocations.
- Use streaming and buffered I/O; avoid tiny syscalls in loops.

### 8.2 Build-profile tuning

For release-quality benchmarking and production artifacts, evaluate:

- `codegen-units = 1`
- `lto = "thin"` or `"fat"`
- `panic = "abort"` where operationally acceptable
- optional target tuning (`target-cpu`) based on distribution requirements

All tuning changes require benchmark evidence.

### 8.3 Concurrency and scheduling

- Keep actor work units small and cooperative.
- No blocking inside async actor handlers.
- Use dedicated bounded blocking workers for unavoidable blocking operations.

---

## 9) Testing strategy (mandatory coverage)

### 9.1 Unit tests

- Required for all public and critical internal modules/types.
- Validate invariants, parsing, normalization, merge logic, and key derivation.

### 9.2 Integration tests (all public APIs)

- Every public API path has integration coverage for success + failure modes.
- Validate side effects and persisted state semantics.

### 9.3 End-to-end tests (all major features)

At minimum:

- CAS store/get/constraint/optimize flow
- Conductor tool import/run/cache/re-exec flow
- mediapm tool lifecycle + media add/add-local + sync materialization
- lockfile + pruning + verify workflows

### 9.4 Advanced correctness tests

- Property tests for planner determinism and idempotency.
- Loom tests for concurrency-sensitive components.
- Golden/snapshot tests for deterministic planning output.

### 9.5 Performance tests

- Benchmarks for hashing, reconstruction depth, orchestration overhead,
  materialization throughput, and metadata pipeline costs.
- Track regressions in CI for key benchmarks when feasible.

---

## 10) Documentation contract

Every public module and API must include:

- purpose
- invariants
- complexity/performance notes for hot paths
- error behavior
- examples where non-trivial

Migration changes must include rationale comments and schema-hop notes.

---

## 11) Tooling and quality gates

### 11.1 Local quality commands

Preferred aliases:

- `cargo fmt-check`
- `cargo clippy-all`
- `cargo test-all`

### 11.2 CI quality gates

- Formatting must pass.
- Clippy warnings fail the build (except explicitly justified allowances).
- Full test suite (unit + integration + e2e) must pass.

---

## 12) Implementation roadmap (execution order)

## Stage A — Architecture hardening

- Finalize actor topology and message contracts.
- Finalize runtime abstraction traits and adapters.
- Lock module boundaries and dependency direction.

## Stage B — Phase 1 CAS completion

- Unified diff/full storage model implementation.
- Index + DSU + optimizer actor behavior.
- CAS CLI/API + stress and integrity tests.

## Stage C — Phase 2 Conductor completion

- CUE dual-file model integration.
- Tool/workflow instance dedup + persistence merge logic.
- orchestration state migration optics + verification tests.

## Stage D — Phase 3 mediapm completion

- policy/config schema + lockfile semantics
- media pipelines (online/local)
- toolsmith lifecycle (add/remove/list/update)
- atomic materializer with strict path invariants and link fallback

## Stage E — Hardening and scale

- large-workspace performance profiling
- benchmark-driven optimizations
- docs and operational playbooks

---

## 13) Definition of done

The system is done when all of the following are true:

1. Core principles in Section 1 are demonstrably enforced.
2. All public APIs have integration tests.
3. All major features have end-to-end tests.
4. Determinism/idempotency invariants are tested and passing.
5. Runtime can switch with minimal adapter-level changes.
6. Actor supervision behavior is validated under fault scenarios.
7. Migration chain is documented, testable, and auditable.
8. Performance claims are benchmark-backed.
9. `rustfmt`, `clippy`, and test gates pass in CI.

---

## 14) Practical implementation notes

- Prefer existing crates when they satisfy requirements without forcing
  complexity.
- Build custom components only when necessary for correctness, performance, or
  domain invariants.
- Keep optimization local and measurable; avoid premature global complexity.
- When in doubt, choose the design that is easiest to reason about,
  benchmark, and test.

This plan is intentionally strict: it is designed to keep `mediapm` fast,
predictable, maintainable, and evolvable over a long lifespan.
