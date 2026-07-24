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
3. **Open caches** — `ToolDownloadCache::open()` for content cache (30d TTL) and metadata cache (1d TTL) under the user-level cache root. The cache root path is determined by the `cache_root_override` parameter:
   - `None` → use `default_mediapm_user_download_cache_root()` (default OS cache dir)
   - `Some(path)` → use the provided path as the cache root for both content and metadata caches
4. **Provision skip** — before fetching each tool, compare `state.managed_tools[tool_id].canonical_version` against the resolved canonical version using direct string equality. If they match AND the stored `fetch_hash` is non-empty, route through `PreResolveOutcome::Skip` instead of `PreResolveOutcome::Resolved`. The provisioning function shows a resolve bar with `set_message("skipped")` and returns `Ok(None)` immediately. The coordinator increments `tools_skipped` and advances the overall bar.
5. **Per-tool provisioning loop** — for each `(tool_id, requirement_value)` in `desired_tools`:
   - Check if it's a builtin source-ingest tool (`is_builtin_source_ingest_requirement`).
   - Resolve the tool fetch via `provider::resolve_tool_fetch()`. If resolve fails, emit a warning and continue.
   - Determine `PreResolveOutcome`: `Skip` if the tool is already provisioned at the resolved version, else `Resolved`.
   - Call `fetch_and_import_tool_payload()` with the outcome. On skip (`was_skip`), increment `tools_skipped` and continue.
   - On `Ok(Some(payload))`: compute content-addressed hash, build spec+runtime, insert into generated doc.
   - **External data registration**: before inserting the tool spec, register every CAS hash in the tool's `content_map` as an `ExternalDataEntry` in `generated_doc.external_data` with `OutputSaveMode::Saved`. This satisfies the `content_map ⊆ external_data` invariant.
   - On `Ok(None)`: create minimal spec without content map.
   - On `Err`: append warning to report, continue loop.
6. **Companion binding resolution** — `resolve_companion_ffmpeg_selection()`, `resolve_companion_deno_selection()` (currently stubs).
7. **Create tools dir** — `std::fs::create_dir_all(&paths.tools_dir)`.
8. **Write env file** — `write_generated_runtime_env_file()`.
9. **Save generated document** — `save_conductor_generated_document()`.

### `ToolSyncReport` fields

| Field           | Type          | Purpose                                                                                                                        |
| --------------- | ------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `tools_added`   | `usize`       | Tools newly registered (not previously in generated doc)                                                                       |
| `tools_updated` | `usize`       | Tools updated to match desired version                                                                                         |
| `tools_removed` | `usize`       | Tools removed (no longer in desired set)                                                                                       |
| `tools_skipped` | `usize`       | Tools skipped because their canonical version was already provisioned. Shown in the resolve bar with `set_message("skipped")`. |
| `warnings`      | `Vec<String>` | Non-fatal warnings (provision failures)                                                                                        |

### Invariants

- Provision failures produce warnings only — they never abort the loop or return `Err`. The failed tool will be retried on next sync.
- Content-addressed hash is computed from `serde_json::to_string(&payload.content_map)` → `blake3::hash()` → hex.
- Tool key format: `"{name}@{hash}"` when content_map non-empty, bare `"{name}"` when empty.
- Builtin source-ingest tools (`import`) skip hash-key generation and use bare name.
- Progress bar shows `desired_tools.len()` total items; bar finishes success (no warnings) or error (warnings present).
- `content_map ⊆ external_data` invariant: every CAS hash referenced in any tool's `runtime.content_map` must have a matching `ExternalDataEntry` in `generated_doc.external_data`. Enforced on both encode (`encode_document()`) and decode (`decode_document()`) of conductor NCL documents.

### Testing invariants

- Tests must be hermetic: never read from or write to the real OS-level user cache dir. Use `cache_root_override` to inject a tempdir.
- The `default_mediapm_user_download_cache_root().is_none()` skip guard is macOS-ineffective and must not be relied upon. Use `cache_root_override` instead.
- Test assertions should verify the override path was used (e.g., cache index files exist under the override path rather than the default).

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
