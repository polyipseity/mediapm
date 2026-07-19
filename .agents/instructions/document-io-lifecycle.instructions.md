---
description: "Use when editing conductor document loading/saving and builtin lifecycle in src/mediapm/src/conductor_bridge/documents.rs and lifecycle.rs. Covers NCL load/save, builtin registration, lifecycle helpers."
name: "Document I/O and Lifecycle"
applyTo: "src/mediapm/src/conductor_bridge/documents.rs, src/mediapm/src/conductor_bridge/sync/lifecycle.rs"
---

# Document I/O and lifecycle

## Purpose

- Manage the four-document model: `mediapm.ncl` (user intent), `conductor.generated.ncl` (machine-generated tool defs), `state.ncl` (mediapm machine state), `state.conductor.ncl` (conductor runtime state).
- Register builtin tool definitions and apply lifecycle transitions.

## Document load/save

### `load_conductor_document(path, label)`

- If file exists: read bytes → `decode_document()`.
- If file doesn't exist: return default empty `NickelDocument`.
- Errors: `MediaPmError::Io` for filesystem read failure, `MediaPmError::ConductorDocument` for decode failure with operation + path + detail context.

### `save_conductor_document(path, document, label)`

- `encode_document()` → bytes → `write_bytes_if_changed()`.
- `write_bytes_if_changed` only writes to disk when content differs (avoids filesystem churn and unnecessary nix-daemon rebuilds).
- Errors: `MediaPmError::ConductorDocument` for encode failure, `MediaPmError::Io` for write failure.

### Document lifecycle helpers

| Function                                        | Purpose                                  |
| ----------------------------------------------- | ---------------------------------------- |
| `load_conductor_generated_document(paths)`      | Loads `paths.conductor_generated_ncl`    |
| `save_conductor_generated_document(paths, doc)` | Saves to `paths.conductor_generated_ncl` |
| `load_conductor_state_document(paths)`          | Loads `paths.conductor_state_config`     |
| `save_conductor_state_document(paths, doc)`     | Saves to `paths.conductor_state_config`  |

## Builtin registration

### `register_missing_builtin_tools(generated_doc)`

- Ensures all builtin tool definitions (`echo`, `fs`, `import`, `export`, `archive`) exist in the generated document.
- Idempotent: skips tools that already exist.
- Builtins are registered with `kind`, `name`, `version` only (strict schema).

### `apply_builtin_runtime_defaults(generated_doc)`

- Sets default runtime values for builtin tools (impure flag, etc.).
- Only applies when the tool exists but the field is unset.

### `list_tools(paths)`

- Parses tool keys (`"{name}@{hash}"` or `"{name}"`) into `ConductorToolRow { name, version, managed }`.
- Used by `mediapm tool list` CLI command.

## Lifecycle helpers (`lifecycle.rs`)

| Function                                          | Purpose                                                                 |
| ------------------------------------------------- | ----------------------------------------------------------------------- |
| `is_builtin_source_ingest_requirement(tool_name)` | Returns true for builtin `import` (special content-ingestion handling)  |
| `is_hash_in_tool_content_maps(hash, doc)`         | Checks if a hash is still referenced by any tool content map            |
| `lock_registry_version(cas, tool_id, identity)`   | Stores a deterministic CAS marker `registry-locks/{tool_id}/{identity}` |

## Key invariants

- `write_bytes_if_changed` is the gate for all NCL writes — the coordinator only saves when something actually changed.
- Builtin tools are always re-registered on every sync (idempotent `insert` semantics).
- `list_tools` key parsing uses `rfind('@')` to handle tool names containing `@`.
