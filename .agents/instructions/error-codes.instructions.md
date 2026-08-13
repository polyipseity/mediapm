---
description: "Use when debugging or handling error codes across mediapm workspace crates. Covers MPM, CAS, CND, and UTL error code catalogs with severity, description, and suggested fix."
name: "Error Code Catalog"
applyTo: "**/*.rs"
---

# Error code catalog

Central reference for all crate-prefixed error codes in the mediapm workspace.

## Code format

`<CRATE-PREFIX>-<TYPE><NNN>`

- Prefix: `MPM` (mediapm), `CAS` (mediapm-cas), `CND` (mediapm-conductor), `UTL` (mediapm-utils)
- Type: `E` = error (aborts the operation), `W` = warning (non-fatal)
- Number: Sequential 3-digit within each prefix+type group

Display format: `error[MPM-E001]: <description>` or `warning[MPM-W001]: <description>`

## MPM — mediapm

| Code | Title | Description | Suggested fix |
| --- | --- | --- | --- |
| MPM-E001 | Unknown dependency key | A dependency key does not match any known dep type or configured tool | Use a bare tool ID as the key, or check valid keys in the error message |
| MPM-E002 | Inherit with unconfigured tool | A dependency uses `"inherit"` but the target tool is not in the tools section | Add the tool to the tools section, or use `"latest"` / explicit version |
| MPM-E003 | Circular inherit | A dependency and its target both use `"inherit"` | Set an explicit version for the target tool to break the cycle |
| MPM-E004 | Config parse failure | A config value failed to deserialize (via `serde_json`) | Check the expected shape and fields in the config document |
| MPM-E005 | Invalid source | Media source specification is invalid | Review source parameters and structure |
| MPM-E006 | Workflow error | General workflow orchestration failure | Check the conductor document and tool configuration |
| MPM-E007 | I/O error | Filesystem operation failed | Check filesystem permissions, disk space, and paths |
| MPM-E008 | Conductor document error | Conductor document loading or validation failed | Check the conductor-generated NCL document |
| MPM-E009 | Managed namespace conflict | The user conductor document declares a tool key or workflow name inside a namespace reserved for mediapm-managed entries | Remove or rename the conflicting entry; mediapm owns the managed namespace |
| MPM-W001 | Silenced serde error | A `ToolRequirement` deserialization failed and was logged instead of blocking the operation | Inspect logs for the deserialization details; the affected tool entry may be incomplete |

## CAS — mediapm-cas

| Code | Title | Description | Suggested fix |
| --- | --- | --- | --- |
| CAS-E001 | Object not found | The requested CAS object is missing from the store | Verify the content hash and that the object was stored |
| CAS-E002 | Invalid argument | An argument violates CAS invariants | Check argument constraints in `CasApi` documentation |
| CAS-E003 | Internal error | Unexpected internal state in the CAS layer | File a bug report with the reproduction steps |
| CAS-E004 | I/O error | Filesystem operation failed in the CAS layer | Check filesystem permissions, disk space, and paths |
| CAS-E005 | Corrupt object | Data integrity check failed (hash mismatch) | Re-fetch or re-store the object; data may be corrupted |
| CAS-E006 | Object too large | Object exceeds the operation size limits | Split the object into smaller chunks or increase limits |
| CAS-E007 | Lock contention | Another process holds the CAS directory lock | Wait for the other process to finish, or check for stale locks |

## CND — mediapm-conductor

| Code | Title | Description | Suggested fix |
| --- | --- | --- | --- |
| CND-E001 | Workflow error | Invalid config, missing tools, or orchestration failure | Check the conductor document and tool state |
| CND-E002 | CAS error | Error forwarded from the CAS layer | Check the wrapped CAS error for details |
| CND-E003 | Serialization error | JSON or Nickel encode/decode failure | Check document format and structure |
| CND-E004 | I/O error | Filesystem operation failed in the conductor layer | Check filesystem permissions, disk space, and paths |
| CND-E005 | Internal error | Unexpected conductor state | File a bug report with the reproduction steps |

## UTL — mediapm-utils

(The `mediapm-utils` crate currently has no error codes defined. Future additions should use the `UTL-` prefix.)
