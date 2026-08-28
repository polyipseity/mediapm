---
description: "Use when debugging or handling error codes across mediapm workspace crates. Covers MPM, CAS, CND, and UTL error code catalogs with severity, description, and suggested fix."
name: "Error Code Catalog"
applyTo: "**/*.rs"
---

# Error code catalog

Crate-prefixed error codes across the workspace. Format: `<CRATE-PREFIX>-<TYPE><NNN>` where prefix is `MPM`/`CAS`/`CND`/`UTL`, type is `E` (error, aborts) or `W` (warning, non-fatal), and the number is sequential per group. Displayed as `error[MPM-E001]: <description>`.

## MPM — mediapm

| Code | Title | Description | Suggested fix |
| --- | --- | --- | --- |
| MPM-E001 | Unknown dependency key | Dependency key matches no known dep type or configured tool | Use a bare tool ID as the key, or check valid keys in the error message |
| MPM-E002 | Inherit with unconfigured tool | Dependency uses `"inherit"` but the target tool is absent | Add the tool, or use `"latest"` / explicit version |
| MPM-E003 | Circular inherit | Dependency and target both use `"inherit"` | Set an explicit version for the target to break the cycle |
| MPM-E004 | Config parse failure | Config value failed to deserialize (`serde_json`) | Check the expected shape and fields in the config document |
| MPM-E005 | Invalid source | Media source specification is invalid | Review source parameters and structure |
| MPM-E006 | Workflow error | General workflow orchestration failure | Check the conductor document and tool configuration |
| MPM-E007 | I/O error | Filesystem operation failed | Check filesystem permissions, disk space, and paths |
| MPM-E008 | Conductor document error | Conductor document loading or validation failed | Check the conductor-generated NCL document |
| MPM-E009 | Managed namespace conflict | User conductor document declares a tool key or workflow name in the mediapm-managed namespace | Remove or rename the conflicting entry; mediapm owns the managed namespace |
| MPM-W001 | Silenced serde error | `ToolRequirement` deserialization failed and was logged instead of blocking | Inspect logs; the affected tool entry may be incomplete |

## CAS — mediapm-cas

| Code | Title | Description | Suggested fix |
| --- | --- | --- | --- |
| CAS-E001 | Object not found | Requested CAS object missing from the store | Verify the content hash and that the object was stored |
| CAS-E002 | Invalid argument | Argument violates CAS invariants | Check argument constraints in `CasApi` |
| CAS-E003 | Internal error | Unexpected internal CAS state | File a bug report with reproduction steps |
| CAS-E004 | I/O error | Filesystem operation failed in the CAS layer | Check filesystem permissions, disk space, and paths |
| CAS-E005 | Corrupt object | Data integrity check failed (hash mismatch) | Re-fetch or re-store the object |
| CAS-E006 | Object too large | Object exceeds operation size limits | Split into smaller chunks or increase limits |
| CAS-E007 | Lock contention | Another process holds the CAS directory lock | Wait for it to finish, or check for stale locks |

## CND — mediapm-conductor

| Code | Title | Description | Suggested fix |
| --- | --- | --- | --- |
| CND-E001 | Workflow error | Invalid config, missing tools, or orchestration failure | Check the conductor document and tool state |
| CND-E002 | CAS error | Error forwarded from the CAS layer | Check the wrapped CAS error |
| CND-E003 | Serialization error | JSON or Nickel encode/decode failure | Check document format and structure |
| CND-E004 | I/O error | Filesystem operation failed in the conductor layer | Check filesystem permissions, disk space, and paths |
| CND-E005 | Internal error | Unexpected conductor state | File a bug report with reproduction steps |

## UTL — mediapm-utils

No error codes defined yet. Future additions use the `UTL-` prefix.
