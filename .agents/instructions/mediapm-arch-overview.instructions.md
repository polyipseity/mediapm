---
description: "Use when editing mediapm Rust source under src/. Covers cross-crate architecture principles and module-layer index. Provides a compact overview; see per-crate AGENTS.md for detailed invariants."
name: "mediapm Architecture Overview"
applyTo: "src/**/*.rs"
---

# MediaPM architecture overview

## Cross-crate engineering principles

- **Pure core, imperative shell.** Keep planning, diffing, and key derivation pure and deterministic. Confine side effects (filesystem, process, network) to explicit boundary modules.
- **Incremental by default.** Prefer incremental updates over full rebuilds. Use explicit content-addressed cache keys throughout.
- **Async I/O with runtime adapters.** Default to Tokio. Use `async-trait` where useful.
- **Actor concurrency.** Use `ractor` with typed messages for orchestration and stateful actors.
- **Type-level modeling.** Use newtypes and strong enums to make invalid states unrepresentable. Resolve `Option` at configuration boundaries via serde defaults, not in domain code. All serde defaults are centralized in `src/mediapm/src/config/defaults.rs`.
- **Deterministic serialization.** Use `serde` + deterministic `serde_json` policy for all content-addressed data.

## All-platform download principle

Managed tool payloads are downloaded for all supported OSes regardless of the host platform. The download pipeline fetches every platform-specific variant so the cache is complete for any target.

## Module-layer index

| Crate | Role |
|---|---|
| `mediapm-cas/` | Content-addressed storage, async CAS API |
| `mediapm-conductor/` | Workflow orchestration, Nickel config evaluation |
| `mediapm-conductor-builtins/*/` | Built-in tools (echo, fs, archive, import, export) |
| `mediapm/` | Media-facing API, CLI, managed-tool lifecycle |
| `mediapm-utils/` | Shared utilities for builtins |

## Cross-references

See per-crate `AGENTS.md` for detailed invariants, edge cases, and behavioral contracts. See `.agents/instructions/mediapm-architecture.instructions.md` for the materialization contract and crate-boundary invariants.
