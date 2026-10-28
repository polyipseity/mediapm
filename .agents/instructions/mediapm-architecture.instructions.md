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
| `mediapm-cli/`, `mediapm-cas-cli/`, `mediapm-conductor-cli/` | Clap command definitions, shared by each binary and its own build script |
| `mediapm-utils/` | Shared utilities for builtins |

Per-crate `AGENTS.md` holds detailed invariants and contracts.

## Identity and storage invariants

- Canonical identity key is URI (`canonical_uri`), not path display strings.
- Content identity is BLAKE3 hash; objects stored under `.mediapm/store/blobs/v1/blake3/<0..2>/<2..4>/<4..>`.
- CAS object files are immutable once imported and persisted as read-only. The store enforces the mode itself, in `FileSystemBlobStore`: every commit path (`atomic_write`, `write_from_reader`, `write_stream`) makes the staged file read-only before its rename, the skip path restores a mode an older build left unset, and every delete path clears the attribute first on Windows, because that platform cannot replace or unlink a read-only file. The mode is therefore a property of the store, not a side effect of the materializer's chmod through a hardlinked output, and a hardlink inherits it; the one exception is Windows, where the attribute belongs to every name of the file and the clear that makes a delete possible on one name is visible on the others. A staged file has the attribute cleared before it is written, so an interrupted commit cannot wedge the next write for that hash.
- There is no per-media sidecar. Runtime media state is the `ManagedFileRecord`s in `.mediapm/state.json` that name each materialized path, plus the conductor's per-instance record of what each step produced; the only sidecars in a library are the ones a tool wrote, such as yt-dlp's `.url`/`.webloc`/`.desktop` link files and its `.info.json`, `.vtt` and thumbnail outputs.
- A user-authored `media` map key is one identity with two consumers — `${media.id}` path interpolation and every `ManagedFileRecord.media_id` — so `config::hierarchy_types::validate_media_id` **rejects** at the config boundary (from `flatten_hierarchy_nodes_inner`, beside `validate_hierarchy_path_component`) any id that is empty, carries a control character, has leading or trailing whitespace, contains a path separator or a reserved filename character, or *is* `.`/`..`. Rejecting rather than sanitizing is the policy for the same reason as the NFD rule: the key is the user's to spell correctly, and a sanitized spelling would put `a_b` on disk while the state kept `a/b`. The message is rendered with `escape_debug` and names the code point of a control character, so a diagnostic never splits the log line carrying it. The reserved-character rule calls `path_component::is_rejected_char` — the same predicate the sanitizer rewrites from — so the rule and the rewrite cannot drift; it is deliberately platform-independent, because a `a:b` key the sanitizer rewrites to `a_b` on every platform would split the library on a platform-dependent rule. The boundary is the hierarchy walk *plus* `config::hierarchy_types::validate_media_ids`, which validates **every** key and is called from `build_media_workflow_plan_with_limits`: the walk alone misses a `media` entry no hierarchy node references, and the workflow synthesizer turns every key into a `media/{id}` workflow name. `config::validation::validate_document` and `MediaPmDocument::normalize()` are **not** that boundary — neither is reachable from production, so neither may be cited as coverage.
- Keep the version marker explicit in every persisted document and every migration sequential per version: the two constants live in `config/defaults.rs` (`MEDIAPM_DOCUMENT_VERSION`, `MEDIAPM_STATE_VERSION`), and the ladders are `state-persistence.instructions.md` for `state.json` and `nickel.instructions.md` for the Nickel documents.

## Determinism and safety

- Sort and serialize JSON deterministically for stable diffs.
- Use atomic write flow for state documents and object writes (temp file + sync + rename).
- Keep `plan` output stable for identical inputs.
- Keep `sync` idempotent for unchanged state.

## Link materialization

- Respect the document's `materialization_preference_order` (hardlink → symlink → reflink → copy when it is unset) rather than a built-in order: the order is a library decision, and `materialize_file_entry` reads it off `SyncSharedState`, which `sync_hierarchy` fills from the document.
- A fallback is a notice, not an error and not silence. A method further down the order wrote the file, so the entry lands, and the notice names the entry's relative path and the method that actually wrote it; `MaterializeReport.notices` carries it into the sync summary's warnings, where the CLI prints it as a `Δ` line, and a run that carried one exits as a warning (`3`), because the library is not the layout the document asked for even though every entry landed. No method succeeding is still an error.
- Write each library entry straight from the CAS object into its target path: validate the object, remove a read-only target, then write, with no staging tree and no rename commit. `MediaPmPaths` declares no staging directory, and the commit step in `materializer/commit.rs` validates and clears the target rather than staging it.
- A skip is decided against what the target must hold, not against what the store holds. An entry written straight from a CAS object compares the recorded hash and the object's length; an entry whose variant carries a `zip_member` binding stores the archive under the variant hash but writes one extracted member, so it compares the member's own hash and length, and replaces the read-only target through the same remove-then-write path a store object uses.
- NFD-only filenames are enforced at two stages, because the user controls one input and not the other. At the config stage, `config::hierarchy_types::check_nfd_source` (called from `validate_hierarchy_path_component`) **rejects** a non-NFD component the user declared, with a message distinct from the reserved-character one, before any template placeholder is resolved. At the post-resolution stage, `sync_hierarchy` runs `materializer::commit::sanitize_and_validate_components` over every resolved entry after metadata interpolation and before any entry worker starts, which **normalizes** each component — tag metadata, ffprobe output, and upstream values are not the user's to fix — and rejects `.`/`..` components and unrewritten separators rather than committing them.
- `rename_files` replacement strings are sanitized with the configured replacement map.

## Output save policy

- One `save` policy per output variant: `"auto"` (the default), a boolean, or `"full"`. `tools/workflows/mod.rs::resolve_variant_save` resolves the declaration into the `SaveMode` the conductor records on the capture spec, and an explicit policy is answered with itself without consulting the context.
- `"auto"` answers in one order, and the order is the rule: a variant the media's hierarchy materializes resolves to `"full"`; otherwise a variant the media's final step produces, or an impure step produces, resolves to `true`; otherwise `false`. A materialized variant wins over both other reasons, so a variant the library links to is never stored as a delta against another blob. The context is `AutoSaveContext`, filled from `config::hierarchy_types::hierarchy_consumed_variants` so the answer matches what the materializer writes.
- A materialized variant declared `false` contradicts the hierarchy and is refused at plan build as `MPM-E010`, naming the media, the step, the variant and the hierarchy entry whose selector consumes it.
- The mode decides retention, never production: every mode writes the capture when the step runs. `"full"` additionally marks the object keep-full in the CAS, so the optimizer never stores it as a delta (it may still serve as a base); the materializer errors when a resolved hash names bytes the store does not hold, which is the fatal exit-1 band, while a variant nothing produced stays a counted miss. The collector's side of retention — an unsaved output stays rooted while its producing instance is reachable — is in `src/mediapm-conductor/AGENTS.md` section G.

## Cache policy

- Caches carry no entry-count cap. The tool caches below expire on a clock; the materializer's own records (`src/mediapm/src/materializer/outputs.rs`, `<runtime>/cache/mediapm/materialization.json`) expire on change and hold only what the current run confirmed.
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
