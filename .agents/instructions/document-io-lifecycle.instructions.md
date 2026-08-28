---
description: "Use when editing conductor document loading/saving and builtin lifecycle in src/mediapm/src/conductor_bridge/documents.rs and lifecycle.rs. Covers NCL load/save, builtin registration, lifecycle helpers."
name: "Document I/O and Lifecycle"
applyTo: "src/mediapm/src/conductor_bridge/documents.rs, src/mediapm/src/conductor_bridge/sync/lifecycle.rs"
---

# Document I/O and lifecycle

Four-document model: `mediapm.ncl` (user intent), `conductor.generated.ncl` (machine-generated tool defs), `state.ncl` (mediapm machine state), `state.conductor.ncl` (conductor runtime state).

## Document load/save

### `load_conductor_document(path, label)`

- File exists: read bytes → `decode_document()`.
- Missing: return default empty `NickelDocument`.
- Errors: `MediaPmError::Io` (read), `MediaPmError::ConductorDocument` (decode, with operation + path + detail).

### `save_conductor_document(path, document, label)`

- `encode_document()` → bytes → `write_bytes_if_changed()` (writes only when content differs, avoiding filesystem churn and nix-daemon rebuilds).
- Errors: `MediaPmError::ConductorDocument` (encode), `MediaPmError::Io` (write).

### Document lifecycle helpers

| Function                                        | Purpose                                  |
| ----------------------------------------------- | ---------------------------------------- |
| `load_conductor_generated_document(paths)`      | Loads `paths.conductor_generated_ncl`    |
| `save_conductor_generated_document(paths, doc)` | Saves to `paths.conductor_generated_ncl` |
| `load_conductor_state_document(paths)`          | Loads `paths.conductor_state_config`     |
| `save_conductor_state_document(paths, doc)`     | Saves to `paths.conductor_state_config`  |

## Builtin registration

### `register_missing_builtin_tools(generated_doc)`

- Ensures all builtin tool defs (`echo`, `fs`, `import`, `export`, `archive`) exist in the generated document.
- Idempotent: skips existing tools. Builtins registered with `kind`, `name`, `version` only (strict schema).

### `apply_builtin_runtime_defaults(generated_doc)`

- Sets default runtime values (impure flag, etc.) only when the tool exists but the field is unset.

### `list_tools(paths)`

- Parses tool keys (`"{name}@{hash}"` or `"{name}"`) into `ConductorToolRow { name, version, managed }`.
- Used by `mediapm tool list`.

## Lifecycle helpers (`lifecycle.rs`)

| Function                                          | Purpose                                                                 |
| ------------------------------------------------- | ----------------------------------------------------------------------- |
| `is_builtin_source_ingest_requirement(tool_name)` | True for builtin `import` (special content-ingestion handling)          |
| `is_hash_in_tool_content_maps(hash, doc)`         | Checks if a hash is still referenced by any tool content map            |
| `lock_registry_version(cas, tool_id, identity)`   | Stores a deterministic CAS marker `registry-locks/{tool_id}/{identity}` |

## Key invariants

- `write_bytes_if_changed` gates all NCL saves — the coordinator writes only when encoded bytes differ, preventing filesystem/git churn when payloads are identical across syncs. State JSON does not use this gate (see state-persistence spec).
- Builtin tools are re-registered every sync (idempotent `insert`).
- `list_tools` key parsing uses `rfind('@')` to handle tool names containing `@`.
- Conductor NCL files are artifact manifests: content changes only when binary payload hashes change. Metadata-only updates (version tag rotations without payload change) go to `state.json`, not NCL.
