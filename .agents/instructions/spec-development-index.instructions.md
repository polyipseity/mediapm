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

| Crate | File | Coverage |
|-------|------|----------|
| **CAS** (content-addressed storage) | `src/cas/AGENTS.md` | Core model contract, storage/retrieval semantics, codec versioning protocol, CAS integrity verification (`VerifyTriggerStrategy`), index-backed existence checks, index repair & recovery scan, filesystem locking, cross-crate shared invariants, integration boundaries (CAS↔Conductor), performance, known limitations, Part 1 edge cases (1.1–1.23) |
| **Conductor** (workflow orchestration) | `src/conductor/AGENTS.md` | Orchestration contract, 3-document config model, 27 tool schema invariants, template syntax specification, tool-content cache design, process execution semantics, step dispatch (dependency-stream model), Instance GC lifecycle, CAS GC sweep, background GC loop, channel-based progress events, orchestration state decode migration, instance key lifecycle & failure recovery, integration boundaries (CAS↔Conductor, Conductor↔Builtins, MediaPM↔Conductor), performance, known limitations, Part 2 edge cases (2.1–2.11), cross-cutting edge cases from Part 4 |
| **Conductor-Builtins** (tool implementations) | `src/conductor-builtins/AGENTS.md` | Shared validation framework, CLI/API contract, 5 builtin specs (echo, fs, archive, import, export), tool registration & identity, builtin contract stability rule, integration boundary (Conductor↔Builtins), performance, Part 3 edge cases (3.1–3.6) |
| MediaPM (user-facing API/CLI) | `src/mediapm/AGENTS.md` | Runtime path resolution, media workflow pipeline, versioning & migration, media schema (hierarchy node kinds, metadata binding, managed tool reconciliation), tool provisioning & catalog, materialization, metadata cache, lock records, HierarchyPath sanitization pipeline (5 stages), companion paths, integration boundary (MediaPM↔Conductor), identity/sidecar/storage invariants, testing policy, Part 4 edge cases (4.1–4.29) |

## Per-Builtin AGENTS.md Stubs

| Builtin | File | Content |
|---------|------|---------|
| Echo | `src/conductor-builtins/echo/AGENTS.md` | Pure tool, `message` in/out, zero side effects |
| FS | `src/conductor-builtins/fs/AGENTS.md` | 6 filesystem operations, sandbox enforcement, atomic write |
| Archive | `src/conductor-builtins/archive/AGENTS.md` | tar+zstd compression, streaming extraction, decompression bomb limits |
| Import | `src/conductor-builtins/import/AGENTS.md` | Content-addressed import, CAS dedup, remote URL support |
| Export | `src/conductor-builtins/export/AGENTS.md` | Content-addressed export, atomic write, disk-full handling |

## Related Instruction Files

| File | Purpose |
|------|---------|
| `.agents/instructions/mediapm-architecture.instructions.md` | MediaPM Rust source architecture: module-layer design, sidecar invariants, planning/execution boundaries |
| `.agents/instructions/mediapm-testing-and-docstrings.instructions.md` | Test intent & coverage expectations, Rustdoc/docstring depth requirements |
| `.agents/instructions/rust-workflow.instructions.md` | Rust editing conventions: formatting, linting, selective vs full-workspace validation |
| `.agents/instructions/commit-message-policy.instructions.md` | Conventional Commit scope rules, crate-prefix header ban |
| `.agents/instructions/markdown-and-customizations.instructions.md` | Markdown formatting rules, YAML frontmatter patterns, repo linking conventions |
| `.agents/instructions/scripts-and-permissions.instructions.md` | Script placement, cross-platform runtime detection, permission expectations |
| `.agents/instructions/tooling-and-validation-detection.instructions.md` | Repository tooling detection: build, test, lint, CI workflow commands |
| `.agents/instructions/tool-content-cache-refactoring.instructions.md` | ToolContentCache extraction from StepWorkerExecutor |
| `.agents/instructions/versioning-and-migration.instructions.md` | Versioned structs, wire formats, DeltaState bridges |
| `.agents/instructions/language-and-stack-detection.instructions.md` | Language/framework detection from concrete files |

## Quick Reference: Where to Look

| When you need... | Start with... |
|-----------------|---------------|
| CAS storage semantics or integrity | `src/cas/AGENTS.md` |
| Conductor orchestration or tool contracts | `src/conductor/AGENTS.md` |
| Builtin tool parameter or output schema | `src/conductor-builtins/AGENTS.md` |
| Media workflow pipeline or media-schema rules | `src/mediapm/AGENTS.md` |
| Cross-crate integration boundaries | All 4 per-crate AGENTS.md files (Integration Boundaries sections) |
| Edge cases for a specific crate | That crate's AGENTS.md (edge case sections) |
| Shared invariants (all 6) | Any per-crate AGENTS.md (Shared Invariants section) |
| Rust source editing conventions | `.agents/instructions/rust-workflow.instructions.md` |
| Architecture module-layer rules | `.agents/instructions/mediapm-architecture.instructions.md` |
