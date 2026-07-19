---
description: "Use when editing tool-sync reconciliation coordination in src/mediapm/src/conductor_bridge/sync/mod.rs. Covers the reconcile_desired_tools full flow, progress orchestration, and ToolSyncReport contract."
name: "Tool Sync Reconciliation Coordinator"
applyTo: "src/mediapm/src/conductor_bridge/sync/mod.rs"
---

# Tool sync reconciliation coordinator

## Purpose

- Orchestrate the full tool-sync lifecycle: document init → provisioning → spec assembly → env output → save.
- Produce a `ToolSyncReport` summarizing added/updated/removed tools and non-fatal warnings.

## `reconcile_desired_tools()` flow

1. **Load generated document** — `load_conductor_generated_document(paths)`. Returns empty `NickelDocument` if file doesn't exist.
2. **Register builtins** — `register_missing_builtin_tools()`, `apply_builtin_runtime_defaults()`.
3. **Open caches** — `ToolDownloadCache::open()` for content cache (30d TTL) and metadata cache (1d TTL) under the user-level cache root.
4. **Per-tool provisioning loop** — for each `(tool_id, requirement_value)` in `desired_tools`:
   - Check if it's a builtin source-ingest tool (`is_builtin_source_ingest_requirement`).
   - Call `fetch_and_import_tool_payload()` to run the 3-phase pipeline.
   - On `Ok(Some(payload))`: compute content-addressed hash, build spec+runtime, insert into generated doc.
   - **External data registration**: before inserting the tool spec, register every CAS hash in the tool's `content_map` as an `ExternalDataEntry` in `generated_doc.external_data` with `OutputSaveMode::Saved`. This satisfies the `content_map ⊆ external_data` invariant.
   - On `Ok(None)`: create minimal spec without content map.
   - On `Err`: append warning to report, continue loop.
5. **Companion binding resolution** — `resolve_companion_ffmpeg_selection()`, `resolve_companion_deno_selection()` (currently stubs).
6. **Create tools dir** — `std::fs::create_dir_all(&paths.tools_dir)`.
7. **Write env file** — `write_generated_runtime_env_file()`.
8. **Save generated document** — `save_conductor_generated_document()`.

## `ToolSyncReport` fields

| Field           | Type          | Purpose                                                  |
| --------------- | ------------- | -------------------------------------------------------- |
| `tools_added`   | `usize`       | Tools newly registered (not previously in generated doc) |
| `tools_updated` | `usize`       | Tools updated to match desired version                   |
| `tools_removed` | `usize`       | Tools removed (no longer in desired set)                 |
| `warnings`      | `Vec<String>` | Non-fatal warnings (provision failures)                  |

## Invariants

- Provision failures produce warnings only — they never abort the loop or return `Err`. The failed tool will be retried on next sync.
- Content-addressed hash is computed from `serde_json::to_string(&payload.content_map)` → `blake3::hash()` → hex.
- Tool key format: `"{name}@{hash}"` when content_map non-empty, bare `"{name}"` when empty.
- Builtin source-ingest tools (`import`) skip hash-key generation and use bare name.
- Progress bar shows `desired_tools.len()` total items; bar finishes success (no warnings) or error (warnings present).
- `content_map ⊆ external_data` invariant: every CAS hash referenced in any tool's `runtime.content_map` must have a matching `ExternalDataEntry` in `generated_doc.external_data`. Enforced on both encode (`encode_document()`) and decode (`decode_document()`) of conductor NCL documents.
