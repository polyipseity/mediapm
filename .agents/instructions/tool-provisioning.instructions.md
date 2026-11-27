---
description: "Use for tool sync, provisioning, dispatch, cache/HTTP, tagger credentials."
name: "Tool provisioning"
applyTo: "src/mediapm/src/conductor_bridge/sync/**/*.rs, src/mediapm/src/conductor_bridge/documents.rs, src/mediapm/src/tools/provider/**/*.rs, src/mediapm/src/tools/preset/**/*.rs, src/mediapm/src/tools/workflows/**/*.rs, src/mediapm/src/config/mod.rs, src/mediapm/src/env_registry.rs, src/mediapm/src/auth/**/*.rs, src/mediapm/src/builtins/media_tagger/gate.rs, src/mediapm-conductor/src/http/**/*.rs, src/mediapm-conductor-builtins/import/src/lib.rs"
---

# Tool provisioning

## Tool ids

- MediaPM id: logical `yt-dlp`, `ffmpeg`, `deno`, `rsgain`, `media-tagger`, `sd`, `import` in `mediapm.ncl` keys, dependency keys, `ToolRegistryEntry.tool_id`, `step.tool`, env name stem (hash-free).
- Conductor id: generated `tools` key `"{name}@{hash}"` when content map non-empty, else bare name; provision-cache key and root of `<tools_dir>/<sanitize_tool_id(conductor_tool_id)>/payload/` + `.env.generated` paths. Never key state by plain id.

## Coordinator flow

- `reconcile_desired_tools()` (`sync/mod.rs`): load doc, apply builtin defaults, resolve, provision via dependency-aware bounded pool (per-edge waits), rewrite generated doc, write env/state, prune. See `conductor_bridge/sync/mod.rs`, `sync/pool`, `sync/lifecycle.rs`, `documents.rs`.
- First `ProvisionFailure` aborts, cancels, writes nothing (no apply, `state.json`, `conductor.generated.ncl`); warnings non-fatal; `finish_overall_bar()` reflects them. Skip: newest record + hash match + workspace map, full `ToolRegistryEntry` in `report.tool_records` (`tool_records`), one Resolved/Skip entry per tool, `resolved_field_backfills` = provenance.
- `recheck_seconds`: `always` resolve; `never` reuse matching record without upstream (info notice if none); `N` until N s old; independent of metadata TTL, including payload-free builtins.
- `find_active_tool_spec(doc, deployments, tool_name)` sole resolver; `ToolDeployments` from `state.json`: newest `deployed_at` wins, key from `content_map_hash`, non-empty-map preference fallback only without deployment record. Updates clear superseded siblings' maps: one non-empty map per tool; no inline name-match loops.
- Live set from generated doc at each use (`live_tool_specs`, run's ids); prune stale `"{name}@{old_hash}"` keys/dirs via `retain_only_tool_dirs` on conductor ids. `logical_tool_requires_sync` uses sync's `runtime_storage_overrides.cache_root_override` + metadata handle.

## Identity and composite version

- `content_map_hash(&content_map)` (`serde_json::to_string` -> blake3 hex) sole hash for `ToolRegistryEntry.content_map_hash`, keys, skip; key `"{name}@{hash}"` or bare name.
- `content_map ⊆ external_data` enforced on NCL encode/decode; hashes in `generated_doc.external_data`.
- `canonical_version` exact equality, persisted composite = version + SameStep segments (direct-only/non-transitive). `compute_composite_canonical_version()` (`sync/mod.rs`) sole compute/compare/store source (skip, resolved/no-payload, `logical_tool_requires_sync`).
- Same-step deps inlined under `deps/<mediapm_tool_id>/...` before `content_map_hash`; inlined `deps/` skipped by generated dotenv vars.
- Dual write: `state.json` always (audit), `conductor.generated.ncl` only on byte/hash change (`write_bytes_if_changed`); state-only churn expected.

## 3-phase provisioning

- `PreResolveOutcome::Resolved`/`Skip`; `provision_entry` (`sync/mod.rs`) wraps `fetch_and_import_tool_payload` (`sync/provision.rs`) with pre-resolve, errors, progress; entries parallel via `buffer_unordered`.
- Phases: resolve bar (lookup count, cached/skipped suffix) -> ranged-GET prefetch (best effort, silent, fills size) -> fetch (conductor provider, concurrent, deterministic order, monotone counter) -> process (extract/repack/CAS import, os-conditional selector, per-source bars).
- Bars from conductor callbacks, on demand, coordinator-owned, error-finish tracked; empty sources `Ok(None)`, no bars beyond resolve. Metadata TTL creation-anchored, never `touch()`; resolve/fetch bars set cached/skipped suffix before finish.
- Payload: content map, os exec paths, human/canonical version, provenance; provision cache keyed by conductor id.

## Provider dispatch

- `resolve_tool_fetch(tool_id, metadata_cache)` returns `(ResolvedToolFetch, ResolvedToolMetadata)`; `MetadataCacheTracker` auto-counts lookups; `resolve_latest_github_tag` cache-first, never touches TTL.
- Resolve versions/layouts (no pins except stable permalinks/repo URLs/arch-label maps); degrade on unreadable index/assets if shorter chain exists; fetch derivations match resolve (macOS leg probe/URL). Format from bytes (`xz`/`zip`/`gzip`), URL fallback only; unrecognized declared archive stays on extraction path, fails loudly.
- Canonical version: GitHub releases resolved commit hash; ffmpeg `"{autobuild_tag}+{macos_leg_version}"` rewritten from served URL (`canonical_version_after_fetch`); media-tagger build-time git hash. Provenance `Option<String>` (`None` null, never `""`), informational only.
- `None` WHY (also inline `// WHY:` in dispatch arm and module doc): ffmpeg `resolved_version`/`resolved_vcs_hash` because mixed FFmpeg-Builds + martin-riedl + jellyfin sources have no single meaningful version/hash (build-script tag deref is not source); media-tagger `resolved_tag` because host builtin shipped inside mediapm has no tagged release/payload.
- ffmpeg Windows/Linux FFmpeg-Builds autobuild assets + alias fallback; macOS martin-riedl -> jellyfin (dual arch, arch keys, GET+Range probe, URL-time labels, member from index/pointer files). GitHub assets skip checksum probe on github.com/objects.githubusercontent.com; others probed. media-tagger/import no payload.

## Preset dispatch and workflows

- `apply_preset(tool_name, content_map, os_exec_paths, slot_limits)` dispatches case-insensitively to `tools/preset/` -> `ToolSpec`/`ToolRuntime`. Policy only in `ToolSpec.runtime`; `ToolDefinition` derives runtime from it, so presets cannot hide policy from generated doc. Unknown tool panics; call only after provisioning. Workflows in `tools/workflows/`.
- Templates `${...}` only (`${context.os == ...}`, `${inputs.<key>}`, `${env.<VAR>}`); no `{tool_dir}`/`{os}`. Content-map keys payload-relative. Capture shape resolver-owned: kind from variant, subtitle selection scopes token for file/folder captures, zip binding only where declared, invalid combos `MPM-E011`.
- Deps from per-preset `DependencyTypes` (`tools/dependency.rs`), explicit/direct-only; keep impure flags, concurrency/retry defaults; refs `${step_output...}`/`${external_data.<hash>}` declared in plan external data.

## Tool requirements

- `ToolRequirement` (config/mod.rs): `version_spec` (`ConfigVersionSpec`: `Latest`/`Inherit`/`Exact{vcs_hash?,version?,tag?}`), flattened `dependencies: BTreeMap<String, ConfigVersionSpec>`, recheck/slot fields, staging per code. `Inherit` resolves at boundaries to internal `VersionSpec` (never serialized); no `.deps` wrapper/user dep type.
- Normalization keeps entries with set `version_spec`; dependency `Inherit` resolves at provision time; roles from preset `known_dependency_type()`. `spec_matches_entry`: `Latest` = re-resolve (`false`), `Exact` = exact match on specified fields, stored `None` = never.

## Cache and HTTP

- User cache (`default_mediapm_user_download_cache_root()` / `<os-cache-dir>/mediapm/cache/`) tiers: content `tools.json` (7d last-use, bytes by URI + resolved identity), metadata `tool_metadata.json` (1d creation TTL, never touch), provision (24h last-use, extracted binaries in workspace tools dir). Workspace `<runtime_root>/tools/` and user cache never interchangeable.
- `OnceLock` clients: conductor async (30s connect, 30 min request, shared UA, no-redirect), import blocking (`fetch`, 60s/60s); `MEDIAPM_HTTP_TIMEOUT_SECONDS` min 30s. Call sites keep HTTP policy docstring (central client, no local `reqwest::Client`).
- `mediapm-conductor/src/http/` self-contained: no `use crate::`, no `ConductorError`, copy + Cargo.toml extractable, errors mapped at call sites, `build.rs` enforces; MediaPM/tagger use shared client.

## Credentials and login gates

- `resolve_gate` (`src/mediapm/src/builtins/media_tagger/gate.rs`) at point of need: `Need::None` or disabled `enable_logged_in_features_<service>` never reads auth state; wanted field under disabled flag dropped with info log; required need under disabled flag errors regardless of login; enabled + wanted/required need + no login errors. No startup precheck/unauthenticated fallback.
- Credentials only via hidden prompts on `mediapm auth <musicbrainz|acoustid> login` into `auth.json`; no `ACOUSTID_API_KEY`/MusicBrainz OAuth env fallback; missing hard-errors naming login + MusicBrainz URL, never silent no-op/dotenv read. `--offline` skips validation/revocation; AcoustID logout has no revocation. No-separate-step actions append `(--offline has no separate <step> step)` from `run_auth_action`: `remote` status, `revocation` AcoustID logout; offline MusicBrainz logout states local record removed without revocation. `MEDIAPM_ACOUSTID_BASE` debug-only probe, per-call, release-ignored; canonical endpoint in `auth::acoustid` builds without `media-tagger`.

## Testing and pointers

Hermetic tests: no real user cache; `cache_root_override`/`MEDIAPM_EXAMPLE_CACHE_ROOT` + temp dirs; no `default_mediapm_user_download_cache_root().is_none()` guard; cover skip/full provisioning, skip tests seed generated-doc/state. Pointers: `src/mediapm/src/conductor_bridge/sync/{mod.rs,pool/*,provision.rs,lifecycle.rs}`, `src/mediapm/src/conductor_bridge/documents.rs`, `src/mediapm-conductor/src/provision/*`, `src/mediapm/src/tools/provider/{mod.rs,*.rs}`, `src/mediapm-conductor/src/tools/provider/mod.rs`, `src/mediapm/src/tools/preset/{mod.rs,*.rs}`, `src/mediapm/src/tools/workflows/{mod.rs,*.rs}`, `src/mediapm/src/tools/dependency.rs`, `src/mediapm/src/config/mod.rs`, `src/mediapm-conductor/src/http/`, `src/mediapm-conductor-builtins/import/src/lib.rs`.
