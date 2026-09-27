---
description: "Use when editing error handling in src/mediapm/src/error.rs. Covers MediaPmError variants, context preservation conventions, and ConductorError mapping."
name: "Error Taxonomy"
applyTo: "src/mediapm/src/error.rs"
---

# Error taxonomy

Centralizes crate-level error variants so submodules share one contract, preserving operation + path context for I/O and document errors.

## `MediaPmError` variants

| Variant | When used | Context |
| --- | --- | --- |
| `InvalidSource(String)` | Source URI fails scheme requirements | error string |
| `Workflow(String)` | Workflow/state consistency violation, provisioning failure, invalid configuration | error string |
| `Serialization(String)` | Serialization or schema conversion failure | error string |
| `Io { operation, path, source }` | Filesystem I/O failure | operation label + target path + `std::io::Error` |
| `Conductor(ConductorError)` | Error propagated from conductor | via `#[from]` |
| `ConductorDocument { operation, path, detail }` | Conductor NCL document I/O failure | operation label + target path + detail string |
| `ConfigValidation { code, context, detail, suggestion }` | Config/version validation failure with a catalog code | `&'static str` code (e.g. `MPM-E001`) + what was validated + what went wrong + suggested fix |

## Context preservation rules

- `Io` errors always carry a human-readable `operation` label and the `path`.
- `ConductorDocument` errors carry `operation`, `path`, and a `detail` string.
- `ConfigValidation` is the only variant that carries a structured code: `code` is the catalog code from `error-codes.instructions.md` (one of `MPM-E001`–`MPM-E004`, `MPM-E009`; see `MediaPmError::code` at `error.rs:81`), and `context`/`detail`/`suggestion` are all required so the rendered message names what was validated, what went wrong, and the fix. Every other variant derives its code from `MediaPmError::code`; do not hardcode a code string at a construction site.
- Map conductor errors via the `Conductor` variant (`?` or `map_err`); wrap in `Workflow` only when extra context is needed.

## Error propagation

- Provisioning failures: non-critical produce warnings in `ToolSyncReport`; critical (document load/save, CAS import) propagate as `Err`.
- Use `MediaPmError::Io` for `std::fs::create_dir_all`/`read`/`write` with descriptive operation labels.
- Use `MediaPmError::ConductorDocument` for NCL `decode_document`/`encode_document` failures.
- Use `MediaPmError::ConfigValidation` for fail-fast config checks and Nickel/serde decoding of `mediapm.ncl`, picking the catalog code that matches the check.
