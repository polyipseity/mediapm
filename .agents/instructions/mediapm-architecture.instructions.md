---
description: "Use for mediapm Rust architecture and storage contracts."
name: "mediapm Architecture and Invariants"
applyTo: "src/**/*.rs"
---

# mediapm architecture and invariants

## Cross-crate principles

- Deterministic, auditable state transitions; planning separate from side effects.
- **Pure core, imperative shell**: planning/diffing/key derivation pure; filesystem/process/network effects in boundary modules.
- Incremental updates with explicit content-addressed keys. Tokio + `async-trait` where useful; `ractor` typed messages for actors.
- Strong enums/newtypes make invalid states unrepresentable; `Option` resolved at config boundaries via serde defaults, not domain code; defaults in `src/mediapm/src/config/defaults.rs`. Deterministic `serde`/`serde_json` for content-addressed data.

## Module layer index

| Crate | Role |
| --- | --- |
| `mediapm-cas/` | Content-addressed storage, async CAS API |
| `mediapm-conductor/` | Workflow orchestration, Nickel config evaluation |
| `mediapm-conductor-builtins/*/` | Built-ins (echo, fs, archive, import, export) |
| `mediapm/` | Media API, CLI, managed-tool lifecycle |
| `mediapm-cli/`, `mediapm-cas-cli/`, `mediapm-conductor-cli/` | Clap definitions for binaries/build scripts |
| `mediapm-utils/` | Shared utilities for builtins |

Per-crate `AGENTS.md` holds detail.

## Identity and storage

- Identity = URI (`canonical_uri`), not path display; BLAKE3 content under `.mediapm/store/blobs/v1/blake3/<0..2>/<2..4>/<4..>`.
- CAS objects immutable/read-only: `FileSystemBlobStore` mode on commit (`atomic_write`, `write_from_reader`, `write_stream`), restores missing mode on skip, clears read-only before Windows delete; hardlinks inherit mode.
- No per-media sidecar: `ManagedFileRecord`s in `.mediapm/state.json` + conductor instance outputs; library sidecars only tool-written (yt-dlp link/info/subtitle/thumbnail).
- `media` keys validated by `config::hierarchy_types::validate_media_id`/`validate_media_ids` from `build_media_workflow_plan_with_limits`: reject empty/control/whitespace/separator/reserved and `.`/`..`; reject, don't sanitize; platform-independent `path_component::is_rejected_char`. `config::validation::validate_document`/`MediaPmDocument::normalize()` are not this boundary.
- Explicit integer versions, sequential migrations; `MEDIAPM_DOCUMENT_VERSION`/`MEDIAPM_STATE_VERSION` in `config/defaults.rs` (`state-and-versioning.instructions.md`).

## Determinism and safety

Deterministic JSON for stable diffs; atomic temp-write/sync/rename; stable `plan`; idempotent `sync`.

## Link materialization

- `materialization_preference_order` (default hardlink, symlink, reflink, copy) read by `materialize_file_entry` from `SyncSharedState`.
- Fallback = notice (`MaterializeReport.notices` path + method, sync warnings/CLI `Δ`, exit `3`), not error/silence; no method succeeding is error. Direct CAS -> target: validate, remove read-only target, write; no staging/rename (`MediaPmPaths` has none). Skip by content: store hash/length, `zip_member` member hash/length.
- NFD: config rejects non-NFD user components (`check_nfd_source`/`validate_hierarchy_path_component`); resolved components normalized after interpolation (`sanitize_and_validate_components`) before workers; reject `.`/`..`/unrewritten separators; sanitize `rename_files` replacements. Playlist paths use `/` everywhere: XSPF `<location>`, WPL `<media src>`, ASX `<ref href>` are URI references (`\` not separator); `resolve_playlist_target_relative_path` returns rendered string.

## Output save policy

- `save` = `"auto"` (default), boolean, `"full"`; `tools/workflows/mod.rs::resolve_variant_save` -> conductor `SaveMode`.
- `auto` -> `"full"` hierarchy-materialized, `true` final/impure step, else `false`; materialized wins. `AutoSaveContext` from `hierarchy_consumed_variants`; materialized `false` refused as `MPM-E010` (media, step, variant, hierarchy entry).
- Mode = retention, not production. `"full"` records empty object as only permitted CAS base (may still be base); missing bytes fatal exit-1, unproduced variants counted misses; conductor `true` default only for non-synthesized docs.

## Cache policy and downloads

No entry-count cap; tool caches clock-expire, materializer records (`<runtime>/cache/mediapm/materialization.json`) expire on change, current-run confirmations only. User cache `<os-cache-dir>/mediapm/cache/` 7-day eviction, workspace `<mediapm_dir>/tools/` 24h. Download/CAS-import all supported OSes, never host-filter.

## Pointers

`src/mediapm/AGENTS.md` (CLI/service), `tool-provisioning.instructions.md`, `state-and-versioning.instructions.md`, `temp-and-paths.instructions.md`, `sdd-tdd.instructions.md`.
