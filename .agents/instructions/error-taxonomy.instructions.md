---
description: "Use when editing error handling in src/mediapm/src/error.rs. Covers MediaPmError variants, context preservation conventions, and ConductorError mapping."
name: "Error Taxonomy"
applyTo: "src/mediapm/src/error.rs"
---

# Error taxonomy

## Purpose

- Centralize crate-level error variants so all submodules share one consistent error contract.
- Preserve operation + path context for I/O and document errors.

## `MediaPmError` variants

| Variant | When used | Context |
|---------|-----------|---------|
| `InvalidSource(String)` | Source URI fails scheme requirements | error string |
| `Workflow(String)` | Workflow/state consistency violation, provisioning failure, invalid configuration | error string |
| `Serialization(String)` | Serialization or schema conversion failure | error string |
| `Io { operation, path, source }` | Filesystem I/O failure | operation label + target path + `std::io::Error` |
| `Conductor(ConductorError)` | Error propagated from conductor | via `#[from]` |
| `ConductorDocument { operation, path, detail }` | Conductor NCL document I/O failure | operation label + target path + detail string |

## Context preservation rules

- `Io` errors always carry a human-readable `operation` label and the `path` involved.
- `ConductorDocument` errors similarly carry `operation`, `path`, and a `detail` string.
- When mapping from conductor errors, prefer `Conductor` variant via `?` or `map_err`; if additional context is needed, wrap in `Workflow`.

## Error propagation

- Provisioning failures: non-critical failures produce warnings in `ToolSyncReport`; critical failures (document load/save, CAS import) propagate as `Err`.
- Use `MediaPmError::Io` for `std::fs::create_dir_all`, `std::fs::read`, `std::fs::write` calls with descriptive operation labels.
- Use `MediaPmError::ConductorDocument` for NCL `decode_document`/`encode_document` failures.
