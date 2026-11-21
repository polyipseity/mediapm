---
description: "Use when editing mediapm Rust source under src/. Covers crate-boundary invariants, identity and storage semantics, and materialization contracts."
name: "mediapm Architecture and Invariants"
applyTo: "src/**/*.rs"
---

# mediapm architecture and invariants

## Purpose

- Keep code aligned with mediapm's crate-oriented architecture and explicit state model.
- Preserve deterministic, auditable media state transitions.
- Keep planning logic separate from side effects.

## Cross-crate principles

- **Pure core, imperative shell.** Planning, diffing, and key derivation stay pure and deterministic; filesystem/process/network side effects live in explicit boundary modules.
- **Incremental by default.** Prefer incremental updates and explicit content-addressed cache keys.
- **Async and actors.** Default to Tokio; use `async-trait` where useful. Use `ractor` with typed messages for orchestration/stateful actors.
- **Type-level invariants.** Use newtypes and strong enums to make invalid states unrepresentable. Resolve `Option` at configuration boundaries via serde defaults, not in domain code. Centralize serde defaults in `src/mediapm/src/config/defaults.rs`.
- **Deterministic serialization.** Use `serde` with deterministic `serde_json` for content-addressed data.

## Module layer index

| Crate | Role |
| --- | --- |
| `mediapm-cas/` | Content-addressed storage, async CAS API |
| `mediapm-conductor/` | Workflow orchestration, Nickel config evaluation |
| `mediapm-conductor-builtins/*/` | Built-in tools (echo, fs, archive, import, export) |
| `mediapm/` | Media-facing API, CLI, managed-tool lifecycle |
| `mediapm-cli/`, `mediapm-cas-cli/`, `mediapm-conductor-cli/` | Clap definitions shared by binaries and build scripts |
| `mediapm-utils/` | Shared utilities for builtins |

Per-crate `AGENTS.md` files hold detailed invariants and contracts.

## Identity and storage invariants

- Canonical identity key is URI (`canonical_uri`), not path display strings. Content identity is BLAKE3; objects live under `.mediapm/store/blobs/v1/blake3/<0..2>/<2..4>/<4..>`.
- CAS object files are immutable and read-only once persisted. `FileSystemBlobStore` enforces mode on commit (`atomic_write`, `write_from_reader`, `write_stream`), restores missing mode on skip, and clears read-only before delete on Windows. Hardlinks inherit the mode; on Windows the attribute is shared by names.
- No per-media sidecar. Runtime media state is `ManagedFileRecord`s in `.mediapm/state.json` plus conductor per-instance output records. Library sidecars are only tool-written files (yt-dlp link/info/subtitle/thumbnail outputs).
- `media` map keys are validated at the config boundary by `config::hierarchy_types::validate_media_id` (and `validate_media_ids` from `build_media_workflow_plan_with_limits`): reject empty/control/leading-trailing whitespace/path-separator/reserved chars and `.`/`..`. Reject, do not sanitize. Validation is platform-independent via `path_component::is_rejected_char`. `config::validation::validate_document` and `MediaPmDocument::normalize()` are not this boundary.
- Persisted documents keep explicit integer version markers; migrations are sequential per version. Constants live in `config/defaults.rs` (`MEDIAPM_DOCUMENT_VERSION`, `MEDIAPM_STATE_VERSION`). See `state-and-versioning.instructions.md`.

## Determinism and safety

- Serialize JSON deterministically for stable diffs.
- Use atomic temp-write/sync/rename for state and object writes.
- Keep `plan` output stable for identical inputs.
- Keep `sync` idempotent for unchanged state.

## Link materialization

- Honor `materialization_preference_order` (default: hardlink, symlink, reflink, copy). `materialize_file_entry` reads it from `SyncSharedState`.
- A fallback is a notice, not error and not silence: record entry path and actual method in `MaterializeReport.notices`, surface as sync warnings/CLI `Δ`, and exit warning (`3`) when any notice exists. No method succeeding is still an error.
- Write library entries directly from CAS object to target path: validate object, remove read-only target, then write. No staging tree or rename commit. `MediaPmPaths` has no staging directory.
- Decide skip by target content. Store-object entries compare recorded hash and object length; `zip_member` entries compare member hash/length and replace read-only target through the same remove-then-write path.
- NFD filenames: config stage rejects non-NFD user-declared components (`check_nfd_source`/`validate_hierarchy_path_component`) with a distinct message. Post-resolution stage normalizes components after interpolation (`sanitize_and_validate_components`) before workers start; reject `.`/`..` and unrewritten separators. Sanitize `rename_files` replacement strings with the configured replacement map.

## Output save policy

- Per output variant `save` is `"auto"` (default), boolean, or `"full"`. `tools/workflows/mod.rs::resolve_variant_save` resolves to conductor `SaveMode`; explicit policies return themselves.
- `auto` order: materialized by hierarchy -> `"full"`; final-step or impure-step output -> `true`; otherwise `false`. Materialized wins, so library-linked variants are never delta-stored. `AutoSaveContext` comes from `hierarchy_consumed_variants`.
- Materialized variant declared `false` is refused at plan build as `MPM-E010` (media, step, variant, hierarchy entry).
- Mode decides retention, not production: every mode captures when the step runs. `"full"` marks object keep-full in CAS (optimizer may not delta-store it; it may still be a base). Missing resolved bytes are fatal exit-1; unproduced variants are counted misses. Conductor `true` save default applies only to documents mediapm did not synthesize.

## Cache policy and downloads

- Caches have no entry-count cap. Tool caches expire by clock; materializer records (`<runtime>/cache/mediapm/materialization.json`) expire on change and keep only current-run confirmations.
- Managed-tool downloads use shared user-level cache (`<os-cache-dir>/mediapm/cache/`, 7-day eviction). Workspace tool-content cache is `<mediapm_dir>/tools/` (24h stale-entry eviction).
- All-platform download: download and CAS-import managed tool payloads for all supported OSes regardless of host platform. Never filter by host OS in the provisioner.

## Pointers

- `src/mediapm/AGENTS.md` - CLI entry and service orchestration.
- `tool-provisioning.instructions.md` - tool sync/provisioning/dispatch.
- `state-and-versioning.instructions.md` - state format, versions, migrations.
- `temp-and-paths.instructions.md` - paths, temp isolation, janitor contract.
- `sdd-tdd.instructions.md` - SDD/TDD workflow and error taxonomy.
