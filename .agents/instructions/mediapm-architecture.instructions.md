---
description: "Use when editing mediapm Rust source under src/. Covers crate-boundary invariants, identity and storage semantics, and materialization contracts."
name: "mediapm Architecture and Invariants"
applyTo: "src/**/*.rs"
---

# mediapm Architecture and Invariants

## Purpose

- Keep code aligned with mediapm's crate-oriented architecture and explicit state model.
- Preserve determinism and auditability of media state transitions.
- Keep boundaries between planning logic and side effects clear.

## Cross-crate engineering principles

- **Pure core, imperative shell.** Keep planning, diffing, and key derivation pure and deterministic; confine side effects (filesystem, process, network) to explicit boundary modules.
- **Incremental by default.** Prefer incremental updates over full rebuilds. Use explicit content-addressed cache keys throughout.
- **Async I/O with runtime adapters.** Default to Tokio. Use `async-trait` where useful.
- **Actor concurrency.** Use `ractor` with typed messages for orchestration and stateful actors.
- **Type-level modeling.** Use newtypes and strong enums to make invalid states unrepresentable. Resolve `Option` at configuration boundaries via serde defaults, not in domain code. All serde defaults are centralized in `src/mediapm/src/config/defaults.rs`.
- **Deterministic serialization.** Use `serde` + deterministic `serde_json` policy for all content-addressed data.

## Module-layer index

| Crate | Role |
|---|---|
| `mediapm-cas/` | Content-addressed storage, async CAS API |
| `mediapm-conductor/` | Workflow orchestration, Nickel config evaluation |
| `mediapm-conductor-builtins/*/` | Built-in tools (echo, fs, archive, import, export) |
| `mediapm/` | Media-facing API, CLI, managed-tool lifecycle |
| `mediapm-utils/` | Shared utilities for builtins |

Per-crate `AGENTS.md` holds detailed invariants and contracts.

## Identity and storage invariants

- Canonical identity key is URI (`canonical_uri`), not path display strings.
- Content identity is BLAKE3 hash; objects stored under `.mediapm/objects/blake3/<0..2>/<2..4>/<4..>`.
- CAS object files are immutable once imported and persisted as read-only.
- Sidecar paths derived from canonical URI digest under `.mediapm/media/<media-id>/media.json`.
- A user-authored `media` map key is one identity with three consumers — `${media.id}` path interpolation, the `state.workflow_states` key, and every `ManagedFileRecord.media_id` — so `config::hierarchy_types::validate_media_id` **rejects** at the config boundary (from `flatten_hierarchy_nodes_inner`, beside `validate_hierarchy_path_component`) any id that is empty, carries a control character, has leading or trailing whitespace, contains a path separator or a reserved filename character, or *is* `.`/`..`. Rejecting rather than sanitizing is the policy for the same reason as the NFD rule: the key is the user's to spell correctly, and a sanitized spelling would put `a_b` on disk while the state kept `a/b`. The message is rendered with `escape_debug` and names the code point of a control character, so a diagnostic never splits the log line carrying it. The reserved-character rule calls `materializer::commit::is_rejected_char` — the same predicate the sanitizer rewrites from — so the rule and the rewrite cannot drift; it is deliberately platform-independent, because a `a:b` key the sanitizer rewrites to `a_b` on every platform would split the library on a platform-dependent rule. The boundary is the hierarchy walk *plus* `config::hierarchy_types::validate_media_ids`, which validates **every** key and is called from `build_media_workflow_plan_with_limits`: the walk alone misses a `media` entry no hierarchy node references, and the workflow synthesizer turns every key into a `media/{id}` workflow name. `config::validation::validate_document` and `MediaPmDocument::normalize()` are **not** that boundary — neither is reachable from production, so neither may be cited as coverage.
- Preserve `original.original_variant_hash` semantics and `edits` lineage references.
- Keep schema version explicit (`schema_version`) and migrations sequential.

## Determinism and safety

- Sort and serialize JSON deterministically for stable diffs.
- Use atomic write flow for sidecars and object writes (temp file + sync + rename).
- Keep `plan` output stable for identical inputs.
- Keep `sync` idempotent for unchanged state.

## Link materialization

- Respect configured method order (hardlink → symlink → reflink → copy by default) with deterministic fallback reasons.
- Use stage → verify → commit semantics with staging under effective `.mediapm/tmp` and atomic commit into library roots.
- NFD-only filenames are enforced at two stages, because the user controls one input and not the other. At the config stage, `config::hierarchy_types::check_nfd_source` (called from `validate_hierarchy_path_component`) **rejects** a non-NFD component the user declared, with a message distinct from the reserved-character one, before any template placeholder is resolved. At the post-resolution stage, `sync_hierarchy` runs `materializer::commit::sanitize_and_validate_components` over every resolved entry after metadata interpolation and before any entry worker starts, which **normalizes** each component — tag metadata, ffprobe output, and upstream values are not the user's to fix — and rejects `.`/`..` components and unrewritten separators rather than committing them.
- `rename_files` replacement strings are sanitized with the configured replacement map.

## Cache policy

- All caches are TTL-based, not bounded by entry count.
- Managed-tool downloads use shared user-level cache (`<os-cache-dir>/mediapm/cache/`, 7-day eviction).
- Workspace-scoped tool-content cache at `<mediapm_dir>/tools/` for conductor-level payloads (24h stale-entry eviction).

## All-platform download

Managed tool payloads are downloaded and CAS-imported for all supported OSes regardless of host platform. Never filter by host OS in the provisioner.

## Tool sync flow reference

| Concern                            | Instruction file                                     |
| ---------------------------------- | ---------------------------------------------------- |
| CLI entry & service orchestration  | `src/mediapm/AGENTS.md`                              |
| Tool requirements                  | `tool-requirements.instructions.md`                  |
| State persistence                  | `state-persistence.instructions.md`                  |
| Coordinator & identity             | `tool-sync-coordinator-and-identity.instructions.md` |
| 3-phase provisioning               | `tool-sync-3-phase-provisioning.instructions.md`     |
| Tool config (companion deps + env) | `tool-sync-coordinator-and-identity.instructions.md` |
| Paths layout                       | `paths-layout.instructions.md`                       |
| Cache & HTTP                       | `cache-and-http.instructions.md`                     |
| Error taxonomy                     | `error-taxonomy.instructions.md`                     |
| Provider dispatch                  | `provider-dispatch.instructions.md`                  |
| Preset dispatch                    | `preset-dispatch.instructions.md`                    |

## mediapm-rust-writer agent

A specialized `mediapm-rust-writer` subagent is available in this workspace for implementing Rust code that follows mediapm architecture invariants and typing conventions. Invoke via `runSubagent(agentName: "mediapm-rust-writer")` with a description of the Rust implementation task. The agent applies Rust typing conventions and mediapm architecture invariants to produce idiomatic Rust code matching the project's conventions.
