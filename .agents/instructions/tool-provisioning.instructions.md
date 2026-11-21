---
description: "Use when editing tool sync, provisioning, provider/preset dispatch, tool requirements, or cache/HTTP behavior. Covers coordinator flow, identity, 3-phase provisioning, and dispatch invariants."
name: "Tool provisioning"
applyTo: "src/mediapm/src/conductor_bridge/sync/**/*.rs, src/mediapm/src/conductor_bridge/documents.rs, src/mediapm/src/tools/provider/**/*.rs, src/mediapm/src/tools/preset/**/*.rs, src/mediapm/src/tools/workflows/**/*.rs, src/mediapm/src/config/mod.rs, src/mediapm-conductor/src/http/**/*.rs, src/mediapm-conductor-builtins/import/src/lib.rs"
---

# Tool provisioning

Merged guidance for tool sync coordination, identity, 3-phase provisioning, provider/preset dispatch, tool requirements, and cache/HTTP behavior. Source of truth remains the implementation in the paths named above.

## Two id concepts (never confuse)

- **MediaPM tool id**: plain logical id (`yt-dlp`, `ffmpeg`, `deno`, `rsgain`, `media-tagger`, `sd`, `import`). Used in `mediapm.ncl` tool keys, dependency keys, `ToolRegistryEntry.tool_id`, `step.tool`, and env var name stem (hash-free).
- **MediaPM conductor tool id**: generated-doc `tools` map key `"{name}@{hash}"` when content map is non-empty, else bare name. Used as provision-cache key, `tool_runtimes` key, payload dir `<tools_dir>/<sanitize_tool_id(conductor_tool_id)>/payload/`, and `.env.generated` payload path root. Mediapm must never key provisioning state by the plain tool id.

## Coordinator flow

- `reconcile_desired_tools()` (sync/mod.rs) loads generated doc, registers/applies builtin defaults, opens cache domains, resolves desired tools, provisions via a unified dependency-aware bounded pool with per-edge waits, rewrites generated conductor doc, writes env/state, and prunes stale entries. See `conductor_bridge/sync/mod.rs`, `sync/pool`, `sync/lifecycle.rs`, `documents.rs`.
- Fail fast: first `ProvisionFailure` aborts the whole sync, cancels in-flight jobs, and writes nothing (no apply, no `state.json`, no `conductor.generated.ncl`). Warnings are non-fatal notices only; overall bar finish reflects warnings/failure via `finish_overall_bar()`.
- Skip requires newest deployment record, active spec content map hash match, and workspace content map available. Skip still registers one full `ToolRegistryEntry` in `report.tool_records` (never only backfills). Every provisioned tool contributes exactly one `tool_records` entry (Resolved or Skip). `resolved_field_backfills` is secondary provenance backfill only.
- `find_active_tool_spec(doc, tool_name)` is the only way to resolve the active spec: prefer first name match with non-empty content map, else first name match, else `None`. Do not inline name-match loops.
- Active/pruning set is the live `tool_runtimes` keys (mediapm conductor tool ids). Prune generated-doc stale `"{name}@{old_hash}"` keys and filesystem dirs outside that set. `retain_only_tool_dirs`/prune paths operate on conductor tool ids.
- `logical_tool_requires_sync` must resolve from the same cache root used by sync (`runtime_storage_overrides.cache_root_override`) and pass the same metadata cache handle.

## Identity and composite version

- Content-addressed hash is `content_map_hash(&content_map)` (`serde_json::to_string` -> blake3 hex), the single function for `ToolRegistryEntry.content_map_hash`, generated key, and skip comparison. Tool key format: `"{name}@{hash}"` or bare name.
- `content_map ⊆ external_data` is enforced on encode/decode of conductor NCL documents; every tool content-map hash must be registered in `generated_doc.external_data`.
- `canonical_version` is compared by exact string equality for skip/update decisions. Persisted value is composite: bare tool version plus SameStep dependency segments; non-transitive, direct-only. `compute_composite_canonical_version()` in `sync/mod.rs` is the single source of truth for compute/compare/store paths (skip, resolved/no-payload storage, `logical_tool_requires_sync`). Format and segment rules live there.
- Same-step deps are inlined into requester content map under `deps/<mediapm_tool_id>/...` (mediapm tool id, not conductor id), direct-only/non-transitive, before `content_map_hash`; inlined `deps/` keys are skipped by generated dotenv env vars.
- Dual-write strategy: `state.json` is always written (audit trail). `conductor.generated.ncl` is written only when artifact bytes/content-map hashes change (`write_bytes_if_changed` / change-detected path). State churn without conductor-file churn is expected and correct.

## 3-phase provisioning

- Pre-resolve determines `PreResolveOutcome::Resolved` or `Skip`. `provision_entry` (sync/mod.rs) wraps `fetch_and_import_tool_payload` (sync/provision.rs) with pre-resolve, error handling, and progress bar lifecycle; entries are independent and may run in parallel via `buffer_unordered`.
- Phases: resolve bar (metadata lookup count, cached/skipped suffix) -> ranged-GET prefetch for fetch sources (best effort, silent failure, fills expected size) -> fetch (delegates to conductor provider fetch, concurrent source fetch with deterministic collected ordering and monotone completed-items counter) -> process (extract/repack/import to CAS, build os-conditional selector, per-source bars).
- Progress bars are relayed from conductor callbacks, created on demand, owned by coordinator group, and tracked for error finish. Empty fetch sources return `Ok(None)` with no bars beyond resolve.
- Metadata cache TTL is creation-time anchored; never call `touch()`. Resolve/fetch bars set cached/skipped suffix components before finish.
- Fetched payload carries content map, os exec paths, human/canonical version, and resolved provenance fields. Conductor provision cache must use conductor tool id only.

## Provider dispatch

- `resolve_tool_fetch(tool_id, metadata_cache)` routes to per-tool source modules and returns `(ResolvedToolFetch, ResolvedToolMetadata)`. `MetadataCacheTracker` auto-counts metadata lookups; do not maintain manual counts. `resolve_latest_github_tag` is cache-first and must not touch metadata cache TTL.
- Upstream versions/layouts are resolved, never pinned (except stable upstream permalinks/repo URLs/arch-label maps). Degrade on unreadable index/asset list rather than fail when a shorter chain is possible. A fetch-phase re-derived value must be derived the same way as resolve (notably macOS leg probe/fetch URL). Container format is decided by bytes first (`xz`/`zip`/`gzip` header), URL only when no header matches; declared-but-unrecognizable archive stays on extraction path and fails loudly.
- Canonical version: GitHub-release tools use resolved commit hash (not tag). ffmpeg uses `"{autobuild_tag}+{macos_leg_version}"`, rewritten after fetch from served URL (`canonical_version_after_fetch`). media-tagger uses mediapm build-time git hash. Resolved provenance fields are `Option<String>` (`None` serialized null, never `""`) and informational only; comparison/skip/update use `canonical_version`.
- `None` WHY notes (must also exist as inline `// WHY:` in dispatch arm and per-tool provider module doc): ffmpeg `resolved_version`/`resolved_vcs_hash` are `None` because mixed FFmpeg-Builds + martin-riedl + jellyfin sources give no single meaningful version/hash for the artifact set (build-script tag deref is not upstream source). media-tagger `resolved_tag` is `None` because it is a host builtin shipped inside mediapm with no upstream tagged release/payload.
- Platform notes: ffmpeg Windows/Linux use FFmpeg-Builds autobuild tag assets with alias fallback; macOS uses martin-riedl -> jellyfin chain (dual-arch slot, arch-segmented content keys, GET+Range leg probe, arch labels mapped at URL-build time, jellyfin member discovered from repo index/pointer files). GitHub release assets skip checksum probe on github.com/objects.githubusercontent.com; martin-riedl/jellyfin/hermetic servers are still probed. media-tagger and builtin import resolve no payload sources.

## Preset dispatch and workflows

- `apply_preset(tool_name, content_map, os_exec_paths, slot_limits)` dispatches case-insensitively to per-tool spec builders (`tools/preset/`), producing `ToolSpec` and `ToolRuntime`. Unknown tool panics. Called only after successful provisioning with populated content map/os exec paths. Workflow synthesis lives in `tools/workflows/`.
- Command templates use `${...}` engine only (`${context.os == ...}`, `${inputs.<key>}`, `${env.<VAR>}`); no `{tool_dir}`/`{os}` placeholders. Content-map keys are payload-relative and read consistently by staging strategies. Variant capture shape is resolver-owned: capture kind defaults from variant kind, subtitle language selection scopes capture token for file/folder captures, zip member binding only where declared, invalid combinations refused as `MPM-E011`.
- Dependency types come from per-preset registries (`DependencyTypes` in `tools/dependency.rs`), explicit and non-transitive/direct-only. Key invariants: impure tools marked impure, required concurrency/retry defaults preserved per workflow, step input variant refs are either `${step_output...}` or `${external_data.<hash>}` declared in plan external data.

## Tool requirements

- `ToolRequirement` (config/mod.rs): `version_spec` (`ConfigVersionSpec`: `Latest`/`Inherit`/`Exact{vcs_hash?,version?,tag?}`), flattened `dependencies: BTreeMap<String, ConfigVersionSpec>`, recheck/slot fields, and payload staging as documented in code. `Inherit` resolves at config/provision boundaries into internal `VersionSpec` (never serialized internally). No nested `.deps` wrapper and no user-configured dep type.
- Normalization keeps entries with set `version_spec`; dependency entries may be `Inherit` and are resolved at provision time. Dependency roles are from preset `known_dependency_type()`. `spec_matches_entry` treats `Latest` as re-resolve (`false`), `Exact` as exact string match on specified fields only, and stored `None` as never matching.

## Cache and HTTP

- Three tiers under user-level cache root (`default_mediapm_user_download_cache_root()` / `<os-cache-dir>/mediapm/cache/`): content cache `tools.json` (7d, last-use TTL, raw payload bytes keyed by URI plus resolved identity for moving targets), metadata cache `tool_metadata.json` (1d, creation-time TTL, never touch), provision cache (24h last-use, extracted binaries in workspace tools dir, not in user cache). Workspace `<runtime_root>/tools/` and user-level download cache are never interchangeable.
- Shared HTTP clients are `OnceLock` singletons: conductor async client (30s connect, 30 min request, shared UA) and import blocking client (`fetch`, 60s/60s). Both honor `MEDIAPM_HTTP_TIMEOUT_SECONDS` (min 30s). No-redirect variant exists in conductor client. Every shared-client call site must keep HTTP client policy docstring (central client, no local `reqwest::Client`).
- `mediapm-conductor/src/http/` is fully self-contained: no `use crate::`, no `ConductorError`, extractable by copy + Cargo.toml; errors map to `ConductorError` at call sites. `build.rs` enforces the boundary. MediaPM and tagger transport use the shared client per code policy.

## Testing and hermeticity

- Tests/examples are hermetic: never read/write real OS user cache. Use `cache_root_override` / `MEDIAPM_EXAMPLE_CACHE_ROOT` and temp dirs. Do not rely on `default_mediapm_user_download_cache_root().is_none()` as skip guard. Verify both skip-if-up-to-date and full-provision paths; skip-path tests must seed generated-doc/state as required by skip invariants in sync tests.

## Pointers

- Coordinator/identity: `src/mediapm/src/conductor_bridge/sync/{mod.rs,pool/*,provision.rs,lifecycle.rs}`, `src/mediapm/src/conductor_bridge/documents.rs`, `src/mediapm-conductor/src/provision/*`.
- Provider: `src/mediapm/src/tools/provider/{mod.rs,*.rs}`, `src/mediapm-conductor/src/tools/provider/mod.rs`.
- Preset/workflows: `src/mediapm/src/tools/preset/{mod.rs,*.rs}`, `src/mediapm/src/tools/workflows/{mod.rs,*.rs}`, `src/mediapm/src/tools/dependency.rs`.
- Config/HTTP: `src/mediapm/src/config/mod.rs`, `src/mediapm-conductor/src/http/`, `src/mediapm-conductor-builtins/import/src/lib.rs`.
