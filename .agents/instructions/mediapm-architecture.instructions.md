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
- See `mediapm-arch-overview.instructions.md` for the compact architecture overview.

## Identity and storage invariants

- Canonical identity key is URI (`canonical_uri`), not path display strings.
- Content identity is BLAKE3 hash; objects stored under `.mediapm/objects/blake3/<0..2>/<2..4>/<4..>`.
- CAS object files are immutable once imported and persisted as read-only.
- Sidecar paths derived from canonical URI digest under `.mediapm/media/<media-id>/media.json`.
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
- Materializer path validation enforces NFD-only filenames.
- `rename_files` replacement strings are sanitized with the configured replacement map.

## Cache policy

- All caches are TTL-based, not bounded by entry count.
- Managed-tool downloads use shared user-level cache (`<os-cache-dir>/mediapm/cache/`, 30-day eviction).
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
| Tool config (companion deps + env) | `tool-sync-tool-config.instructions.md`              |
| Document I/O lifecycle             | `document-io-lifecycle.instructions.md`              |
| Paths layout                       | `paths-layout.instructions.md`                       |
| Cache & HTTP                       | `cache-and-http.instructions.md`                     |
| Error taxonomy                     | `error-taxonomy.instructions.md`                     |
| Provider dispatch                  | `provider-dispatch.instructions.md`                  |
| Preset dispatch                    | `preset-dispatch.instructions.md`                    |

## mediapm-rust-writer agent

A specialized `mediapm-rust-writer` subagent is available in this workspace for implementing Rust code that follows mediapm architecture invariants and typing conventions. Invoke via `runSubagent(agentName: "mediapm-rust-writer")` with a description of the Rust implementation task. The agent applies Rust typing conventions and mediapm architecture invariants to produce idiomatic Rust code matching the project's conventions.
