---
description: "Use when implementing features that span multiple crates, when you need to find which AGENTS.md covers a specific contract or invariant, or after deleting crate-specifications.md and elaboration-pass-edge-cases.md."
name: "Specification Development Index"
applyTo: "src/mediapm-cas/AGENTS.md, src/mediapm-conductor/AGENTS.md, src/mediapm-conductor-builtins/AGENTS.md, src/mediapm/AGENTS.md"
---

# Specification Development Index

This file is the replacement for the deleted monolithic `.agents/instructions/crate-specifications.md` and `.agents/instructions/elaboration-pass-edge-cases.md`. All content from those files has been distributed to per-crate `AGENTS.md` files. Use this index to locate the relevant specification or edge-case content by crate.

## Per-Crate AGENTS.md Files

| Crate                                         | File                                       | Coverage                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| --------------------------------------------- | ------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **CAS** (content-addressed storage)           | `src/mediapm-cas/AGENTS.md`                | Hash identity (blake3-256, multihash wire format), 4-trait public API (CasApi, CasApiStreaming, ConstraintApi, CasMaintenanceApi), architecture module tree & data flow, internals (Journal, ObjectStore, ReadView, Delta Codec, BackgroundEngine), delete semantics (no dangling deltas), invariants & edge cases (content identity, crash safety, TOCTOU, delta chain integrity, constraints, codec versioning), cross-crate integration (Conductor, MediaPM), build & test commands                                                                                                                                                                                                                                                                                           |
| **Conductor** (workflow orchestration)        | `src/mediapm-conductor/AGENTS.md`          | Orchestration contract, 3-document config model, 27 tool schema invariants, template syntax specification, tool-content cache design, process execution semantics, step dispatch (dependency-stream model), Instance GC lifecycle, CAS GC sweep, background GC loop, channel-based progress events, orchestration state decode migration, instance key lifecycle & failure recovery, integration boundaries (CAS↔Conductor, Conductor↔Builtins, MediaPM↔Conductor), performance, known limitations, N.1–N.19 edge cases (expanded Part 2), O.1–O.9 specification sections (decision rationale, EWMA performance details, testing requirements, troubleshooting, implementation checklist, extension points, cross-crate references, ambiguities resolved, architecture diagrams). Sub-modules: `src/mediapm-conductor/src/tools/preset/AGENTS.md` (preset builders), `src/mediapm-conductor/src/tools/provider/AGENTS.md` (provider pipeline) |
| **Conductor-Builtins** (tool implementations) | `src/mediapm-conductor-builtins/AGENTS.md` | Shared validation framework, CLI/API contract, 5 builtin specs (echo, fs, archive, import, export), tool registration & identity, builtin contract stability rule, integration boundary (Conductor↔Builtins), performance, Part 3 edge cases (3.1–3.6), section H (ambiguities resolved: fail-fast scope, deterministic payload)                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| MediaPM (user-facing API/CLI)                 | `src/mediapm/AGENTS.md`                    | Runtime path resolution, media workflow pipeline, versioning & migration, media schema (hierarchy node kinds, metadata binding, managed tool reconciliation), tool provisioning & catalog, materialization, metadata cache, lock records, HierarchyPath sanitization pipeline (5 stages), companion paths, integration boundary (MediaPM↔Conductor), identity/sidecar/storage invariants, testing policy, L–N edge case sections (2.1–2.11 Conductor edge cases, 3.1–3.6 Builtins edge cases, 4.1–4.33 MediaPM edge cases, 5.1–5.5 Metadata cache edge cases, 6.1–6.8 Cross-crate conflicts), managed tool onboarding guide. Sub-modules: `src/mediapm/src/tools/preset/AGENTS.md` (preset builders), `src/mediapm/src/tools/provider/AGENTS.md` (provider source descriptors)                                                     |

## Per-Builtin AGENTS.md Stubs

| Builtin | File                                               | Content                                                               |
| ------- | -------------------------------------------------- | --------------------------------------------------------------------- |
| Echo    | `src/mediapm-conductor-builtins/echo/AGENTS.md`    | Pure tool, `message` in/out, zero side effects                        |
| FS      | `src/mediapm-conductor-builtins/fs/AGENTS.md`      | 6 filesystem operations, sandbox enforcement, atomic write            |
| Archive | `src/mediapm-conductor-builtins/archive/AGENTS.md` | tar+zstd compression, streaming extraction, decompression bomb limits |
| Import  | `src/mediapm-conductor-builtins/import/AGENTS.md`  | Content-addressed import, CAS dedup, remote URL support               |
| Export  | `src/mediapm-conductor-builtins/export/AGENTS.md`  | Content-addressed export, atomic write, disk-full handling            |

## Related Instruction Files

| File                                                                    | Purpose                                                                                                  |
| ----------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| `.agents/instructions/mediapm-architecture.instructions.md`             | MediaPM Rust source architecture: module-layer design, sidecar invariants, planning/execution boundaries |
| `.agents/instructions/mediapm-testing-and-docstrings.instructions.md`   | Test intent & coverage expectations, Rustdoc/docstring depth requirements                                |
| `.agents/instructions/rust-workflow.instructions.md`                    | Rust editing conventions: formatting, linting, selective vs full-workspace validation                    |
| `.agents/instructions/commit-message-policy.instructions.md`            | Conventional Commit scope rules, crate-prefix header ban                                                 |
| `.agents/instructions/markdown-and-customizations.instructions.md`      | Markdown formatting rules, YAML frontmatter patterns, repo linking conventions                           |
| `.agents/instructions/scripts-and-permissions.instructions.md`          | Script placement, cross-platform runtime detection, permission expectations                              |
| `.agents/instructions/stack-and-tooling-detection.instructions.md`      | Stack and tooling detection: languages, frameworks, build commands, validation workflows                 |
| `.agents/instructions/versioning-and-migration.instructions.md` | Versioned structs, wire formats, DeltaState bridges |

## Quick Reference: Where to Look

| When you need...                              | Start with...                                                     |
| --------------------------------------------- | ----------------------------------------------------------------- |
| CAS storage semantics or integrity            | `src/mediapm-cas/AGENTS.md`                                       |
| Conductor orchestration or tool contracts     | `src/mediapm-conductor/AGENTS.md`                                 |
| Builtin tool parameter or output schema       | `src/mediapm-conductor-builtins/AGENTS.md`                        |
| Media workflow pipeline or media-schema rules | `src/mediapm/AGENTS.md`                                           |
| Cross-crate integration boundaries            | All 4 per-crate AGENTS.md files (Integration Boundaries sections) |
| Edge cases for a specific crate               | That crate's AGENTS.md (edge case sections)                       |
| Shared invariants (all 6)                     | Any per-crate AGENTS.md (Shared Invariants section)               |
| Rust source editing conventions               | `.agents/instructions/rust-workflow.instructions.md`              |
| Architecture module-layer rules               | `.agents/instructions/mediapm-architecture.instructions.md`       |

## Source Temp File Distribution

The following temp files from the original monolithic-specs deletion were verified against current per-crate AGENTS.md. All content is already covered (no new content needed).

| Temp file              | Internal section                                                                                                        | Covered in                                                                                                                                                                           |
| ---------------------- | ----------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `part6-cross-crate.md` | Part 7: Ambiguities (fail-fast, deterministic payload, cleanup, tool IDs, index repair, config versioning)              | `src/mediapm-conductor-builtins/AGENTS.md` H.7 (§7.1–7.2), `src/mediapm/AGENTS.md` (§7.3), `src/mediapm-conductor/AGENTS.md` O.8 (§7.4, 7.7), `src/mediapm-cas/AGENTS.md` 2.8 (§7.6) |
| `part7-ambiguities.md` | Part 8: Performance (CAS optimizer, EWMA, parallelization, lock reconciliation, delta cache, builtin overhead)          | `src/mediapm-cas/AGENTS.md` 2.2 (§8.1, 8.5), `src/mediapm-conductor/AGENTS.md` O.2 (§8.2), `src/mediapm/AGENTS.md` (§8.3–8.4), `src/mediapm-conductor-builtins/AGENTS.md` H.2 (§8.6) |
| `part8-performance.md` | Part 9: Testing gaps (CAS deltas, conductor external data, builtins path safety, mediapm sync, cross-crate integration) | `src/mediapm-cas/AGENTS.md` 2.3 (§9.1), `src/mediapm-conductor/AGENTS.md` O.3 (§9.2), `src/mediapm-conductor-builtins/AGENTS.md` H.3 (§9.3), `src/mediapm/AGENTS.md` (§9.4–9.5)      |

> **Note**: The temp files are misnamed relative to their internal headers.
> No "Part 6: Cross-Crate" content exists in any of the three temp files. The
> actual cross-crate invariants are distributed across all four per-crate
> AGENTS.md files (see the Cross-Crate Contracts section below and the
> per-crate Integration Boundaries sections).

## Cross-Crate Contracts

Cross-crate content from the deleted monolithic files has been distributed to per-crate AGENTS.md files. Key cross-crate reference points:

### Verified Cross-Crate Contracts (all already in AGENTS.md)

| Contract                                              | Location                                                                                            |
| ----------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| CAS versioning vs Conductor versioning                | `src/mediapm-cas/AGENTS.md` 2.7, `src/mediapm/AGENTS.md` §6.1                                       |
| Builtin failure semantics vs Conductor error recovery | `src/mediapm-conductor/AGENTS.md` O.7, N.15; `src/mediapm/AGENTS.md` §6.2                           |
| MediaPM lock vs CAS constraint consistency            | `src/mediapm/AGENTS.md` §6.3                                                                        |
| Tool ID collision (builtin vs managed)                | `src/mediapm-conductor/AGENTS.md` O.7, N.16; `src/mediapm/AGENTS.md` §6.4                           |
| State persistence consistency across layers           | `src/mediapm-conductor/AGENTS.md` N.17; `src/mediapm/AGENTS.md` §6.5                                |
| Cache invalidation across tool versions               | `src/mediapm-conductor/AGENTS.md` O.7; `src/mediapm/AGENTS.md` §6.6                                 |
| Instance key immutability and failure recovery        | `src/mediapm-conductor/AGENTS.md` O.7; `src/mediapm/AGENTS.md` §6.7                                 |
| NCL↔Rust schema sync contract                         | `src/mediapm-conductor/AGENTS.md` N.18, O.7; `src/mediapm/AGENTS.md` §6.8                           |
| Composite hash (`Hash::composite`) across crates      | `src/mediapm-cas/AGENTS.md` §1, §7; `src/mediapm/AGENTS.md` §C                                      |
| Cross-workflow cache-probe race                       | `src/mediapm-conductor/AGENTS.md` N.12                                                              |
| Direct materialization cleanup on failure             | `src/mediapm/AGENTS.md` (automatic cleanup section); `src/mediapm-conductor-builtins/AGENTS.md` 3.5 |
| Index repair semantics                                | `src/mediapm-cas/AGENTS.md` §2.4 (ambiguity resolved)                                               |
| EWMA scheduler parameters                             | `src/mediapm-conductor/AGENTS.md` O.2                                                               |
| CAS optimizer and delta reconstruction cache          | `src/mediapm-cas/AGENTS.md` §4.5, §4.4                                                              |
| Builtin deterministic payload / fail-fast             | `src/mediapm-conductor-builtins/AGENTS.md` H.7                                                      |

### Testing Gaps (Cross-Crate)

The following cross-crate integration test scenarios are identified in `src/mediapm/AGENTS.md` (missing test coverage sections) and are not yet fully covered by dedicated e2e tests:

- CAS version + Conductor version mismatch
- Builtin validation error → Conductor doesn't retry
- Transient builtin error → Conductor retries N times
- CAS prune removes hash in MediaPM lock (cross-layer)
- Tool ID collision (builtin vs managed)
- State blob persisted but lock not updated (cross-layer consistency)

---

## File Structure Reference

| Path                                       | Role                                    |
| ------------------------------------------ | --------------------------------------- |
| `src/mediapm-cas/src/api.rs`               | `CasApi` trait                          |
| `src/mediapm-cas/src/hash.rs`              | `Hash` type, Blake3                     |
| `src/mediapm-cas/src/codec/`               | Serialization & versioning              |
| `src/mediapm-cas/src/index/`               | Persistence, repair                     |
| `src/mediapm-cas/src/storage/`             | `FileSystemCas`, `InMemoryCas`          |
| `src/mediapm-conductor/src/api.rs`         | `ConductorApi` trait                    |
| `src/mediapm-conductor/src/model/`         | 3-document config + orchestration state |
| `src/mediapm-conductor/src/orchestration/` | Actor-based execution                   |
| `src/mediapm-conductor-builtins/*/src/`    | Each builtin: CLI + API library         |
| `src/mediapm/src/api.rs`                   | `MediaPmApi` trait                      |
| `src/mediapm/src/config/`                  | `mediapm.ncl` schema                    |
| `src/mediapm/src/materializer.rs`          | Materialization logic                   |
| `src/mediapm/src/tools.rs`                 | Tool provisioning                       |

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
