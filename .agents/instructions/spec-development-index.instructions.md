---
description: "Use when implementing features that span multiple crates, when you need to find which AGENTS.md covers a specific contract or invariant, or after deleting crate-specifications.md and elaboration-pass-edge-cases.md."
name: "Specification Development Index"
applyTo: "AGENTS.md, src/**/AGENTS.md, .agents/instructions/**/*.md"
---

# Specification Development Index

This file is the replacement for the deleted monolithic
`.agents/instructions/crate-specifications.md` and
`.agents/instructions/elaboration-pass-edge-cases.md`. All content from those
files has been distributed to per-crate `AGENTS.md` files. Use this index to
locate the relevant specification or edge-case content by crate.

## Per-Crate AGENTS.md Files

| Crate                                         | File                               | Coverage                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| --------------------------------------------- | ---------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **CAS** (content-addressed storage)           | `src/cas/AGENTS.md`                | Core model contract, storage/retrieval semantics, codec versioning protocol, CAS integrity verification (`VerifyTriggerStrategy`), index-backed existence checks, index repair & recovery scan, filesystem locking, cross-crate shared invariants, integration boundaries (CAS↔Conductor), performance, known limitations, Part 1 edge cases (1.1–1.26), Part 2 additional specifications (decision rationale, performance constraints, testing requirements, troubleshooting, implementation checklist, extension points, cross-crate versioning, index repair ambiguity resolved)                                                                                                                                                                                              |
| **Conductor** (workflow orchestration)        | `src/conductor/AGENTS.md`          | Orchestration contract, 3-document config model, 27 tool schema invariants, template syntax specification, tool-content cache design, process execution semantics, step dispatch (dependency-stream model), Instance GC lifecycle, CAS GC sweep, background GC loop, channel-based progress events, orchestration state decode migration, instance key lifecycle & failure recovery, integration boundaries (CAS↔Conductor, Conductor↔Builtins, MediaPM↔Conductor), performance, known limitations, N.1–N.19 edge cases (expanded Part 2), O.1–O.9 specification sections (decision rationale, EWMA performance details, testing requirements, troubleshooting, implementation checklist, extension points, cross-crate references, ambiguities resolved, architecture diagrams) |
| **Conductor-Builtins** (tool implementations) | `src/conductor-builtins/AGENTS.md` | Shared validation framework, CLI/API contract, 5 builtin specs (echo, fs, archive, import, export), tool registration & identity, builtin contract stability rule, integration boundary (Conductor↔Builtins), performance, Part 3 edge cases (3.1–3.6), section H (ambiguities resolved: fail-fast scope, deterministic payload)                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| MediaPM (user-facing API/CLI)                 | `src/mediapm/AGENTS.md`            | Runtime path resolution, media workflow pipeline, versioning & migration, media schema (hierarchy node kinds, metadata binding, managed tool reconciliation), tool provisioning & catalog, materialization, metadata cache, lock records, HierarchyPath sanitization pipeline (5 stages), companion paths, integration boundary (MediaPM↔Conductor), identity/sidecar/storage invariants, testing policy, L–N edge case sections (2.1–2.11 Conductor edge cases, 3.1–3.6 Builtins edge cases, 4.1–4.33 MediaPM edge cases, 5.1–5.5 Metadata cache edge cases, 6.1–6.8 Cross-crate conflicts)                                                                                                                                                                                     |

## Per-Builtin AGENTS.md Stubs

| Builtin | File                                       | Content                                                               |
| ------- | ------------------------------------------ | --------------------------------------------------------------------- |
| Echo    | `src/conductor-builtins/echo/AGENTS.md`    | Pure tool, `message` in/out, zero side effects                        |
| FS      | `src/conductor-builtins/fs/AGENTS.md`      | 6 filesystem operations, sandbox enforcement, atomic write            |
| Archive | `src/conductor-builtins/archive/AGENTS.md` | tar+zstd compression, streaming extraction, decompression bomb limits |
| Import  | `src/conductor-builtins/import/AGENTS.md`  | Content-addressed import, CAS dedup, remote URL support               |
| Export  | `src/conductor-builtins/export/AGENTS.md`  | Content-addressed export, atomic write, disk-full handling            |

## Related Instruction Files

| File                                                                    | Purpose                                                                                                  |
| ----------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| `.agents/instructions/mediapm-architecture.instructions.md`             | MediaPM Rust source architecture: module-layer design, sidecar invariants, planning/execution boundaries |
| `.agents/instructions/mediapm-testing-and-docstrings.instructions.md`   | Test intent & coverage expectations, Rustdoc/docstring depth requirements                                |
| `.agents/instructions/rust-workflow.instructions.md`                    | Rust editing conventions: formatting, linting, selective vs full-workspace validation                    |
| `.agents/instructions/commit-message-policy.instructions.md`            | Conventional Commit scope rules, crate-prefix header ban                                                 |
| `.agents/instructions/markdown-and-customizations.instructions.md`      | Markdown formatting rules, YAML frontmatter patterns, repo linking conventions                           |
| `.agents/instructions/scripts-and-permissions.instructions.md`          | Script placement, cross-platform runtime detection, permission expectations                              |
| `.agents/instructions/tooling-and-validation-detection.instructions.md` | Repository tooling detection: build, test, lint, CI workflow commands                                    |
| `.agents/instructions/tool-content-cache-refactoring.instructions.md`   | ToolContentCache extraction from StepWorkerExecutor                                                      |
| `.agents/instructions/versioning-and-migration.instructions.md`         | Versioned structs, wire formats, DeltaState bridges                                                      |
| `.agents/instructions/language-and-stack-detection.instructions.md`     | Language/framework detection from concrete files                                                         |

## Quick Reference: Where to Look

| When you need...                              | Start with...                                                     |
| --------------------------------------------- | ----------------------------------------------------------------- |
| CAS storage semantics or integrity            | `src/cas/AGENTS.md`                                               |
| Conductor orchestration or tool contracts     | `src/conductor/AGENTS.md`                                         |
| Builtin tool parameter or output schema       | `src/conductor-builtins/AGENTS.md`                                |
| Media workflow pipeline or media-schema rules | `src/mediapm/AGENTS.md`                                           |
| Cross-crate integration boundaries            | All 4 per-crate AGENTS.md files (Integration Boundaries sections) |
| Edge cases for a specific crate               | That crate's AGENTS.md (edge case sections)                       |
| Shared invariants (all 6)                     | Any per-crate AGENTS.md (Shared Invariants section)               |
| Rust source editing conventions               | `.agents/instructions/rust-workflow.instructions.md`              |
| Architecture module-layer rules               | `.agents/instructions/mediapm-architecture.instructions.md`       |

## Source Temp File Distribution

The following temp files from the original monolithic-specs deletion were
verified against current per-crate AGENTS.md. All content is already covered
(no new content needed).

| Temp file              | Internal section                                                                                                        | Covered in                                                                                                                                                   |
| ---------------------- | ----------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `part6-cross-crate.md` | Part 7: Ambiguities (fail-fast, deterministic payload, cleanup, tool IDs, index repair, config versioning)              | `src/conductor-builtins/AGENTS.md` H.7 (§7.1–7.2), `src/mediapm/AGENTS.md` (§7.3), `src/conductor/AGENTS.md` O.8 (§7.4, 7.7), `src/cas/AGENTS.md` 2.8 (§7.6) |
| `part7-ambiguities.md` | Part 8: Performance (CAS optimizer, EWMA, parallelization, lock reconciliation, delta cache, builtin overhead)          | `src/cas/AGENTS.md` 2.2 (§8.1, 8.5), `src/conductor/AGENTS.md` O.2 (§8.2), `src/mediapm/AGENTS.md` (§8.3–8.4), `src/conductor-builtins/AGENTS.md` H.2 (§8.6) |
| `part8-performance.md` | Part 9: Testing gaps (CAS deltas, conductor external data, builtins path safety, mediapm sync, cross-crate integration) | `src/cas/AGENTS.md` 2.3 (§9.1), `src/conductor/AGENTS.md` O.3 (§9.2), `src/conductor-builtins/AGENTS.md` H.3 (§9.3), `src/mediapm/AGENTS.md` (§9.4–9.5)      |

> **Note**: The temp files are misnamed relative to their internal headers.
> No "Part 6: Cross-Crate" content exists in any of the three temp files. The
> actual cross-crate invariants are distributed across all four per-crate
> AGENTS.md files (see the Cross-Crate Contracts section below and the
> per-crate Integration Boundaries sections).

## Cross-Crate Contracts

Cross-crate content from the deleted monolithic files has been distributed to
per-crate AGENTS.md files. Key cross-crate reference points:

### Verified Cross-Crate Contracts (all already in AGENTS.md)

| Contract                                              | Location                                                                                    |
| ----------------------------------------------------- | ------------------------------------------------------------------------------------------- |
| CAS versioning vs Conductor versioning                | `src/cas/AGENTS.md` 2.7, `src/mediapm/AGENTS.md` §6.1                                       |
| Builtin failure semantics vs Conductor error recovery | `src/conductor/AGENTS.md` O.7, N.15; `src/mediapm/AGENTS.md` §6.2                           |
| MediaPM lock vs CAS constraint consistency            | `src/mediapm/AGENTS.md` §6.3                                                                |
| Tool ID collision (builtin vs managed)                | `src/conductor/AGENTS.md` O.7, N.16; `src/mediapm/AGENTS.md` §6.4                           |
| State persistence consistency across layers           | `src/conductor/AGENTS.md` N.17; `src/mediapm/AGENTS.md` §6.5                                |
| Cache invalidation across tool versions               | `src/conductor/AGENTS.md` O.7; `src/mediapm/AGENTS.md` §6.6                                 |
| Instance key immutability and failure recovery        | `src/conductor/AGENTS.md` O.7; `src/mediapm/AGENTS.md` §6.7                                 |
| NCL↔Rust schema sync contract                         | `src/conductor/AGENTS.md` N.18, O.7; `src/mediapm/AGENTS.md` §6.8                           |
| Composite hash (`Hash::composite`) across crates      | `src/cas/AGENTS.md` 1.22–1.23                                                               |
| Cross-workflow cache-probe race                       | `src/conductor/AGENTS.md` N.12                                                              |
| Direct materialization cleanup on failure             | `src/mediapm/AGENTS.md` (automatic cleanup section); `src/conductor-builtins/AGENTS.md` 3.5 |
| Index repair semantics                                | `src/cas/AGENTS.md` 2.8 (ambiguity resolved)                                                |
| EWMA scheduler parameters                             | `src/conductor/AGENTS.md` O.2                                                               |
| CAS optimizer and delta reconstruction cache          | `src/cas/AGENTS.md` 2.2                                                                     |
| Builtin deterministic payload / fail-fast             | `src/conductor-builtins/AGENTS.md` H.7                                                      |

### Testing Gaps (Cross-Crate)

The following cross-crate integration test scenarios are identified in
`src/mediapm/AGENTS.md` (missing test coverage sections) and are not yet
fully covered by dedicated e2e tests:

- CAS version + Conductor version mismatch
- Builtin validation error → Conductor doesn't retry
- Transient builtin error → Conductor retries N times
- CAS prune removes hash in MediaPM lock (cross-layer)
- Tool ID collision (builtin vs managed)
- State blob persisted but lock not updated (cross-layer consistency)

---

## Testing Coverage Gaps

| Category    | Untested Scenarios                                               | Est. Count  |
| ----------- | ---------------------------------------------------------------- | ----------- |
| Edge cases  | CAS corruption recovery, Conductor DAG cycles, MediaPM atomicity | ~15         |
| Security    | Symlink escapes, ZIP bombs, path traversal                       | ~6          |
| Performance | Optimization timing, scheduler fairness, sync parallelization    | ~3          |
| Cross-crate | Integration scenarios across crate boundaries                    | ~6          |
| **Total**   |                                                                  | **~80–120** |

## Risk Assessment

### Critical (operational blockers)

- **Delta chain corruption recovery** (CAS) — silent data loss if intermediate base deleted
- **CAS vs Conductor version mismatch** — unmarshaling failure on deployment
- **Partial sync rollback semantics** (MediaPM) — inconsistent materialized files
- **Tool ID collision detection** (Conductor) — builtin overridden silently
- **Missing external_data during execution** (Conductor) — workflow fails mid-execution without validation

### High (correctness impact)

- Symlink loop/escape handling (Builtins) — sandbox escape
- ZIP bomb extraction (Builtins) — disk exhaustion DoS
- Concurrent sync conflicts (MediaPM) — lock corruption race
- Out-of-space prune semantics (CAS) — auto vs manual retry
- Windows reserved names (Builtins) — cross-platform compatibility

### Medium (usability impact)

- Fail-fast validation scope (All) — unclear error semantics
- Deterministic payload definition (Builtins) — timestamps/permissions handling
- Tool provisioning cache invalidation (MediaPM) — version mismatch, stale tools

## Glossary

| Term                | Definition                                                               |
| ------------------- | ------------------------------------------------------------------------ |
| **CAS**             | Content-Addressed Storage — objects addressed by Blake3-256 multihash    |
| **Hash**            | Blake3-256 multihash uniquely identifying bytes                          |
| **Delta Encoding**  | Store only differences from a base object; saves space for similar files |
| **Constraint**      | CAS metadata controlling which objects can be delta bases                |
| **Object**          | Unit of data in CAS (full bytes or delta-encoded)                        |
| **Builtin**         | Standard Conductor-provided tool (echo, fs, archive, import, export)     |
| **Managed Tool**    | External tool provisioned by MediaPM (ffmpeg, yt-dlp, media-tagger)      |
| **Workflow**        | Directed acyclic graph (DAG) of steps, each invoking a tool              |
| **Step**            | Single operation in a workflow; invokes one tool with input bindings     |
| **Materialization** | Writing CAS objects to disk at final output paths                        |
| **Pure/Impure**     | Deterministic function of inputs vs side-effect-having operation         |
| **Fail-Fast**       | Validation raised before ANY processing or side effects                  |
| **Lock File**       | Records processed `media_id` + variant → final CAS hash                  |
| **Media Source**    | Origin of media data: URL, local file, CAS hash                          |
| **Hierarchy**       | Folder/media organization in output directory                            |
| **Variant**         | Output type for media (primary, audio, thumbnail, etc.)                  |
| **Index**           | CAS metadata mapping hash → storage location, size, compressed status    |
| **Repair**          | CAS maintenance: scan storage, rebuild index, detect orphaned objects    |
| **Optimize**        | CAS maintenance: convert full objects to delta-encoded to save space     |
| **CasByteStream**   | `Pin<Box<dyn Stream<Item = Result<Bytes, CasError>> + Send + 'static>>`  |

## FAQ for Developers

| Q                                                                | A                                                                                                                                                                                                   |
| ---------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| How to force re-process a media source that changed?             | Delete the lock entry for that `media_id`, then re-run sync.                                                                                                                                        |
| Does tool version rollback force re-execution?                   | Tool version changes update conductor timestamps (affecting instance keys) but NOT mediapm impure timestamps — mediapm timestamps only refresh on `mediapm.ncl` step config changes.                |
| Why does Conductor persist state to CAS instead of a plain file? | Deduplication (identical runs → same hash), verification (hash proves integrity), and unified backup/restore.                                                                                       |
| Can I run two syncs in parallel on the same `.mediapm`?          | No. Lock file and state documents are single-writer; concurrent writes cause corruption.                                                                                                            |
| What is the boundary between `mediapm.ncl` and `conductor.ncl`?  | Conductor owns workflow definitions, tool specs, step DAG. MediaPM owns media sources, hierarchy, materialization policy. MediaPM synthesizes workflows from `mediapm.ncl` and passes to Conductor. |
| How to extend CAS with custom hash algorithms?                   | Add to `HashAlgorithm` enum, implement multihash trait, update codec table, migrate.                                                                                                                |
| What does "fail-fast" mean exactly?                              | Errors raised before ANY side effects — validation in a separate pass before execution.                                                                                                             |
| Are timestamps/permissions part of deterministic output?         | Yes — "deterministic" means byte-for-byte identical output, including all file metadata.                                                                                                            |
| Is materialization cleanup on failure automatic?                 | Yes — failure during materialization triggers automatic, unconditional cleanup of files written during that sync. No manual `cleanup()` call needed.                                                |
| What does `repair_index()` do?                                   | Updates on-disk index to current schema version, removes orphaned entries. Original object data untouched; no re-hashing.                                                                           |
| When does config version bump?                                   | Required on field removal, rename, type change, or semantics change. NOT required for adding optional fields with defaults.                                                                         |

## File Structure Reference

| Path                               | Role                                    |
| ---------------------------------- | --------------------------------------- |
| `src/cas/src/api.rs`               | `CasApi` trait                          |
| `src/cas/src/hash.rs`              | `Hash` type, Blake3                     |
| `src/cas/src/codec/`               | Serialization & versioning              |
| `src/cas/src/index/`               | Persistence, repair                     |
| `src/cas/src/storage/`             | `FileSystemCas`, `InMemoryCas`          |
| `src/conductor/src/api.rs`         | `ConductorApi` trait                    |
| `src/conductor/src/model/`         | 3-document config + orchestration state |
| `src/conductor/src/orchestration/` | Actor-based execution                   |
| `src/conductor-builtins/*/src/`    | Each builtin: CLI + API library         |
| `src/mediapm/src/api.rs`           | `MediaPmApi` trait                      |
| `src/mediapm/src/config/`          | `mediapm.ncl` schema                    |
| `src/mediapm/src/materializer.rs`  | Materialization logic                   |
| `src/mediapm/src/tools.rs`         | Tool provisioning                       |

## Cross-Crate Ambiguity Registry

Resolved ambiguities that previously caused confusion across crate boundaries:

| Ambiguity                                                                                          | Resolution                                                                                                                                 |
| -------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| **Fail-fast scope**: before any side effects, before committed changes, or on first invalid input? | **(a) Before ANY side effects.** Validation is a separate pass before execution begins; no processing happens before validation completes. |
| **Deterministic payload**: do timestamps/permissions count toward determinism?                     | **Yes.** Deterministic means byte-for-byte identical output for identical input — all metadata must be deterministic or omitted.           |
| **Materialization cleanup**: automatic or manual on failure?                                       | **Automatic.** Failure triggers unconditional cleanup of files written during that sync. API returns error; no manual `cleanup()` needed.  |
| **Tool ID format and dedup**: case-sensitive? semver? free-form?                                   | **Exact string match, case-sensitive.** Arbitrary strings; no normalization or semver enforcement built in.                                |
| **Index repair**: in-place or rebuild? re-hash?                                                    | **Updates on-disk index** to current schema version, removes orphaned entries. No re-hashing; object data untouched.                       |
| **Config versioning**: what requires a version bump?                                               | **Required**: field removal, rename, type change, semantics change. **Not required**: adding optional fields with defaults.                |

---

_Sections derived from the deleted `crate-specifications.md` and `elaboration-pass-edge-cases.md` (commit `d6aa286`). See per-crate `AGENTS.md` files for crate-specific edge cases and invariants._
