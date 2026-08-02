---
description: "Use when editing tool-sync reconciliation coordination and content-addressed identity keys in src/mediapm/src/conductor_bridge/sync/mod.rs and documents.rs."
name: "Tool Sync Coordinator and Content-Addressed Identity"
applyTo: "src/mediapm/src/conductor_bridge/sync/mod.rs, src/mediapm/src/conductor_bridge/documents.rs"
---

# Tool sync coordinator and content-addressed identity

## Reconciliation coordinator

### Purpose

- Orchestrate the full tool-sync lifecycle: document init → provisioning → spec assembly → env output → save.
- Produce a `ToolSyncReport` summarizing added/updated/removed tools and non-fatal warnings.

### `reconcile_desired_tools()` flow

1. **Load generated document** — `load_conductor_generated_document(paths)`. Returns empty `NickelDocument` if file doesn't exist.
2. **Register builtins** — `register_missing_builtin_tools()`, `apply_builtin_runtime_defaults()`.
3. **Open cache** — `Cache::open()` with two domains: `"tools"` (content, 30d TTL) and `"tool_metadata"` (metadata, 1d TTL) under the user-level cache root. The cache root path is determined by the `cache_root_override` parameter:
   - `None` → use `default_mediapm_user_download_cache_root()` (default OS cache dir)
   - `Some(path)` → use the provided path as the cache root
     A single `Cache` instance owns its own `FileSystemCas` internally; no external CAS injection is needed.
4. **Provision skip** — before fetching each tool, look up `state.managed_tools` by tool*id group (via `index_managed_tools()`) and find an active entry (non-empty `content_map_hash`) whose `canonical_version` matches the resolved canonical version. If found, route through `PreResolveOutcome::Skip` instead of `PreResolveOutcome::Resolved`. The provisioning function shows a resolve bar with `set_message("skipped")` and returns `Ok(None)` immediately. The coordinator increments `tools_skipped` and advances the overall bar. Skipped tools are also candidates for `resolved*\*` backfill — see "Resolved-field population and skip backfill" below. When a skipped tool's runtime is reconstructed under its conductor tool id, the coordinator uses the canonical `find_active_tool_spec()` helper (both skip paths — the version-matched skip and the fetch-level skip — plus any external consumer such as the demo examples). See "Active tool spec resolution" below.
5. **Active-tool tracking (pruning)** — the active set for filesystem pruning is
   the set of **mediapm conductor tool ids** collected in `tool_runtimes` (every
   tool inserted by the provisioning loop, keyed by its generated-doc key —
   `{name}@{hash}` when the content map is non-empty, bare `{name}` when empty).
   Tools NOT in this set get their content_map cleared and filesystem payloads
   removed after the provisioning loop. (`compute_used_tool_ids` was deleted:
   the provisioning loop's `tool_runtimes` keys are the single source of truth
   for what is active.)
   5b. **Per-tool provisioning loop** — for each `(tool_id, requirement_value)` in `desired_tools`:
   - Check if it's a builtin source-ingest tool (`is_builtin_source_ingest_requirement`).
   - Resolve the tool fetch via `provider::resolve_tool_fetch()`. If resolve fails, emit a warning and continue.
   - Determine `PreResolveOutcome`: `Skip` if the tool is already provisioned at the resolved version, else `Resolved`.
   - Call `fetch_and_import_tool_payload()` with the outcome. On skip (`was_skip`), increment `tools_skipped` and continue.
   - On `Ok(Some(payload))`: compute content-addressed hash, build spec+runtime, insert into generated doc.
   - **External data registration**: before inserting the tool spec, register every CAS hash in the tool's `content_map` as an `ExternalDataEntry` in `generated_doc.external_data` with `OutputSaveMode::Saved`. This satisfies the `content_map ⊆ external_data` invariant.
   - On `Ok(None)`: create minimal spec without content map.
   - On `Err`: append warning to report, continue loop.
6. **Dependency version resolution** — call `resolve_dep_version_spec()` for
   each dependency's `version_spec` (`ConfigVersionSpec` from serde).
   `ConfigVersionSpec::Inherit` is resolved against the global tool
   requirements; `Exact`/`Latest` pass through (converted to `VersionSpec`).
   Errors on missing global tool or circular inherit resolution.
7. **Create tools dir** — `std::fs::create_dir_all(&paths.tools_dir)`.
8. **Write env file** — `mediapm_conductor::runtime_env::write_generated_dotenv()`.
   The `tool_runtimes` map is keyed by **mediapm conductor tool id**; env var
   names derive from the stripped plain mediapm tool id (hash-free), while env
   var values point at `<tools_dir>/<sanitize_tool_id(conductor_tool_id)>/payload/<key>`
   mirroring the provision-cache layout.
9. **Save generated document** — `save_conductor_generated_document()`.

### Dual-write strategy

The sync coordinator persists two distinct documents with different write policies:

- **`state.json` (metadata-driven, always-write):** Updated unconditionally after every
  sync pass. Records the latest sync metadata (canonical version, deploy timestamp,
  fetch hash) even when tool payloads are unchanged. This is the runtime audit trail
  for every invocation.

- **`conductor.generated.ncl` (artifact-driven, change-detected):** Updated only when
  tool content-map hashes (the actual binary payloads) differ from the previous write.
  Uses `write_bytes_if_changed()` — reads the existing file and skips the write when
  bytes are identical. This is the artifact manifest — it only changes when deployable
  artifacts change.

**Rationale:** Canonical version tags (e.g., daily autobuild timestamps from BtbN) can
change without producing different binaries. An unconditional conductor-file write
would create git noise for every upstream tag rotation. The dual strategy gives:

- Zero git churn in the conductor file when payloads are stable.
- A complete sync history in state.json for debugging and audit.

### `ToolSyncReport` fields

| Field                      | Type                     | Purpose                                                                                                                        |
| -------------------------- | ------------------------ | ------------------------------------------------------------------------------------------------------------------------------ |
| `tools_added`              | `usize`                  | Tools newly registered (not previously in generated doc)                                                                       |
| `tools_updated`            | `usize`                  | Tools updated to match desired version                                                                                         |
| `tools_removed`            | `usize`                  | Tools removed (no longer in desired set)                                                                                       |
| `tools_skipped`            | `usize`                  | Tools skipped because their canonical version was already provisioned. Shown in the resolve bar with `set_message("skipped")`. |
| `pruned_tools`             | `usize`                  | Number of stale `"{name}@{old_hash}"` keys pruned from the generated doc                                                       |
| `resolved_field_backfills` | `Vec<ToolRegistryEntry>` | Entries whose `resolved_*` provenance fields were backfilled in place during skip (see below)                                  |
| `warnings`                 | `Vec<String>`            | Non-fatal warnings (provision failures)                                                                                        |

### Resolved-field population and skip backfill

- **Population at record construction**: every `ToolRegistryEntry` produced by the provisioning loop (both the `Ok(Some(payload))` and `Ok(None)` paths) carries the three `resolved_*` fields from provider metadata (`ResolvedToolMetadata`). `resolved_tag`, `resolved_version`, and `resolved_vcs_hash` are `Option<String>` — `None` (JSON `null`) when the provider has no value, never `""`.
- **Skip backfill**: when a tool routes through `PreResolveOutcome::Skip` (already provisioned at the resolved canonical version), its stored entry may predate the `resolved_*` fields. The coordinator collects those entries in `report.resolved_field_backfills`, and `service.rs` applies them in place after the sync pass: match on `(tool_id, canonical_version)`, fill only `None` fields from fresh provider metadata, never overwrite `Some` values, and preserve `version`, `content_map_hash`, and `deployed_at`. Entries with no missing fields are not collected. The backfill is a no-op when provider metadata matches the stored values, keeping re-sync byte-identical.

### Active tool spec resolution

`find_active_tool_spec(doc, tool_name)` is the single authoritative way to resolve the active `ToolSpec` for a logical tool name inside a generated document. The generated doc may hold several specs with the same bare `name`: pruned stale versions keep the name with an emptied `runtime.content_map`, while the active version carries the payload map.

- Resolution contract: prefer the first spec (deterministic `BTreeMap` key order) whose `runtime.content_map` is non-empty; fall back to the first name match (any content map) so a no-payload tool still resolves; return `None` when no spec matches.
- The reconcile skip paths (version-matched skip and fetch-level skip) both use this helper to reconstruct the skipped tool's runtime under its conductor tool id; external consumers (e.g. the demo examples) delegate to the same helper so resolution never diverges from the reconcile contract.
- Do not inline a name-match loop elsewhere; name collisions with stale pruned entries are resolved by this single preference rule.

### Invariants

- **Two tool id concepts (never confuse)**:
  - **MediaPM tool id** — the plain logical id (`yt-dlp`, `ffmpeg`, `deno`, `rsgain`, `media-tagger`, `sd`). Used in `mediapm.ncl` tools keys, dependency keys, `ToolRegistryEntry.tool_id`, `step.tool`, and the **env var name stem** (`MEDIAPM_YT_DLP_LINUX[_DIR]` — hash-free).
  - **MediaPM conductor tool id** — the generated-doc `tools` map key: `"{name}@{hash}"` when the content map is non-empty, bare `"{name}"` when empty. This is the **provision-cache key** (`<tools_dir>/<sanitize_tool_id(conductor_tool_id)>/payload/`) and the **`tool_runtimes` map key**; the mediapm layer must never key provisioning state by the plain mediapm tool id.
- Provision failures produce warnings only — they never abort the loop or return `Err`. The failed tool will be retried on next sync.
- Content-addressed hash is computed from `serde_json::to_string(&payload.content_map)` → `blake3::hash()` → hex.
- Tool key format: `"{name}@{hash}"` when content_map non-empty, bare `"{name}"` when empty.
- Builtin source-ingest tools (`import`) skip hash-key generation and use bare name.
- Progress bar shows `desired_tools.len()` total items; bar finishes success (no warnings) or error (warnings present).
- `content_map ⊆ external_data` invariant: every CAS hash referenced in any tool's `runtime.content_map` must have a matching `ExternalDataEntry` in `generated_doc.external_data`. Enforced on both encode (`encode_document()`) and decode (`decode_document()`) of conductor NCL documents.
- State churn without conductor-file churn is expected and correct. A change to
  `state.json` alone means only metadata changed; a change to
  `conductor.generated.ncl` means binary artifacts changed.

### Provisioning pruning (generated doc + filesystem)

- **Active set**: the live `tool_runtimes` keys (mediapm conductor tool ids —
  every tool inserted by the provisioning loop, keyed by its generated-doc
  key). The active set is NOT recomputed separately; `compute_used_tool_ids`
  was deleted because the provisioning loop's `tool_runtimes` keys are the
  single source of truth for what remains provisioned.
- **Generated doc pruning**: after the provisioning loop, old `"{name}@{old_hash}"`
  keys are pruned from the generated document when the content_map_hash changes
  (new hash → new key → old key is stale). The `pruned_tools` field in
  `ToolSyncReport` tracks the count of pruned keys.
- **Filesystem pruning**: `retain_only_tool_dirs(data_dir, active_conductor_ids)`
  removes filesystem tool directories not in the active set; the set is the
  `tool_runtimes` keys (conductor tool ids), so provisioned dirs keyed by
  `sanitize_tool_id(conductor_tool_id)` are retained.
- **Preserves keys for remaining tools**: pruning only removes stale/unused keys;
  newly computed keys for active tools survive the prune.

### Testing invariants

- Tests and examples must be hermetic: never read from or write to the real OS-level user cache dir. Use `cache_root_override` to inject a tempdir. Examples-as-tests drive the override through the `MEDIAPM_EXAMPLE_CACHE_ROOT` env var (see `example-execution-policy.instructions.md`).
- The `default_mediapm_user_download_cache_root().is_none()` skip guard is macOS-ineffective and must not be relied upon. Use `cache_root_override` instead.
- Test assertions should verify the override path was used (e.g., cache index files exist under the override path rather than the default).
- Tests must verify both the skip-if-up-to-date path (state.json-only change) and the
  full-provision path (both files change). A hermetic test should assert that re-running
  sync with identical tool payloads produces identical conductor file bytes.

## Content-addressed identity

### Purpose

- Provide deterministic, content-addressed tool identity keys so identical payloads produce identical keys (idempotency) and version changes produce new keys (orphaning).

### Key scheme

Format: `"{name}@{blake3(content_map_json)}"`

- `name` is the tool identifier (e.g. `"yt-dlp"`).
- `hash` is the lowercase hex blake3 hash of the content_map JSON serialized with `serde_json::to_string`.
- When content_map is empty (no payload fetched, internal launcher), the bare `"{name}"` is used — no `@` suffix.

### Semantics

- **Idempotent**: same payload content_map → same hash → same key. Re-running sync with identical tool version produces the same key, so the generated document entry is overwritten in-place (no orphaned entries).
- **Orphaned on version change**: new payload → new content_map → new hash → new key. The old `"{name}@{old_hash}"` entry remains in the generated document until the next garbage collection pass.
- **Bare keys for no-payload tools**: tools that don't fetch a payload (no provider sources, or internal launchers) use bare `"{name}"`. These entries are always overwritten in-place.

### Key parsing in `list_tools`

In `list_tools()` (`documents.rs`), keys are parsed by splitting on the last `@`:

- `key.rfind('@')` splits `"{name}@{version}"` into `(name, version)`.
- Bare keys (no `@`) use the entire key as `name` with an empty `version` string.
- This parsing is used for `mediapm tool list` output.

### Hash domain

- The hash covers only the content_map JSON, not the tool binary bytes or any other metadata.
- The content_map is a `BTreeMap<String, String>` — its JSON serialization is deterministic due to BTreeMap's sorted key order.
- CAS hash of the tool binary itself is stored separately in `content_map` values.

## Composite canonical_version

### Purpose

`ToolRegistryEntry.canonical_version` stores a **composite** version string that
includes the tool's own version plus the versions of its SameStep dependencies.
This ensures that a tool is re-provisioned when any SameStep dependency version
changes — not just when the tool itself changes.

### Format

```text
<bare_version>;dep_id_1:<dep_ver_1>;dep_id_2:<dep_ver_2>;...
```

- `bare_version` is the tool's own resolved canonical version (e.g.,
  `MEDIAPM_GIT_HASH` for builtin launchers, a tag or VCS hash for fetched
  tools).
- Each `dep_id:dep_ver` pair is the dependency's tool ID and resolved
  canonical version, sorted deterministically by `dep_id`.
- Only **SameStep** dependencies (classified by `known_dependency_type()`)
  are included. CrossStep and Both variants are excluded because they
  resolve in a different sync pass.
- For tools with no SameStep dependencies, `composite == bare`.

### `compute_composite_canonical_version()` helper

A `pub(crate)` function in `sync/mod.rs` that:

1. Accepts the bare canonical version, tool ID, `ToolRequirement` (with
   `dependencies` as `ConfigVersionSpec`), and the live state for dep lookups.
2. Matches each dep's `ConfigVersionSpec`: `Inherit`/`Latest` match any
   active entry; `Exact` verifies against the spec via `spec_matches_entry`.
3. For SameStep deps only, appends `;dep_id:resolved_ver` segments.
4. Returns the composite format string.

This helper is the single source of truth used by all 3 injection points:

- **Provision skip check** (`PreResolveOutcome::Skip`): compares expected
  composite against stored composite.
- **Resolved storage** (`PreResolveOutcome::Resolved`): stores the composite
  in the new `ToolRegistryEntry`.
- **No-payload path** (`Ok(None)`): stores the composite for tools that
  resolve without fetching (e.g., skipped tools).
- **Service comparison** (`logical_tool_requires_sync`): compares stored
  composite against computed composite for the desired tool.

### Indexing: `index_managed_tools()`

A `pub(crate)` function in `sync/mod.rs` that groups `ToolRegistryEntry`s
by `tool_id` into `HashMap<String, Vec<ToolRegistryEntry>>`. Used by both
the skip check (find active entries) and the service comparison (look up
stored entries for comparison).

### Test coverage

| Spec item                                                                                    | Test(s)                                                          | Status    |
| -------------------------------------------------------------------------------------------- | ---------------------------------------------------------------- | --------- |
| `compute_composite_canonical_version` returns bare version when no SameStep deps             | `compute_composite_canonical_version_no_deps` (unit)             | [covered] |
| `compute_composite_canonical_version` appends `;dep:ver` for SameStep deps                   | `compute_composite_canonical_version_with_same_step_deps` (unit) | [covered] |
| Stored `canonical_version` in state.json is composite after sync                             | `sync_stores_composite_canonical_version` (integration)          | [covered] |
| Re-sync skips tool when stored composite matches computed composite                          | `sync_skip_triggers_on_unchanged_composite` (integration)        | [covered] |
| `logical_tool_requires_sync` returns `false` when composite matches                          | `sync_logical_requires_sync_composite_comparison` (integration)  | [covered] |
| `logical_tool_requires_sync` returns `true` when composite mismatches                        | `sync_logical_requires_sync_on_composite_mismatch` (integration) | [covered] |
| Public API: `compute_composite_canonical_version` and `index_managed_tools` are `pub(crate)` | Compilation check (used by integration tests via service.rs)     | [covered] |
