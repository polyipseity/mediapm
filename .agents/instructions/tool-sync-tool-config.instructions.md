---
description: "Use when editing companion dependency resolution via ToolRequirementDependencies in src/mediapm/src/config/mod.rs and src/mediapm/src/conductor_bridge/sync/mod.rs."
name: "Tool Sync Dependency Resolution"
applyTo: "src/mediapm/src/config/mod.rs, src/mediapm/src/conductor_bridge/sync/mod.rs"
---

# Tool sync dependency resolution

## Companion dependencies

### Purpose

- Model tool-to-tool dependency declarations (`deps: BTreeMap<String, DependencySpec>`)
  directly on `ToolRequirementDependencies`, replacing the old flat
  `ffmpeg_version`/`deno_version`/`sd_version` field approach.
- Consumed by `compute_used_tool_ids` to determine which tools are active
  (by traversing transitive `Inter`/`Both` deps from step tool IDs).
- Companion binding (inlining same-step deps into the requester's content map)
  uses `dep_type: Inter` to identify same-step relationships.

### Dependency data model

#### `DependencySpec` (in `src/mediapm/src/config/mod.rs`)

| Field          | Type             | Default                          | Purpose                                                    |
| -------------- | ---------------- | -------------------------------- | ---------------------------------------------------------- |
| `dep_type`     | `DependencyType` | `Inter`                          | Companion relationship type (same-step vs cross-step)      |
| `version_spec` | `VersionSpec`    | `Inherit`                        | Version spec: `"latest"`, `"inherit"`, or exact fields    |

#### `DependencyType`

| Variant   | Meaning                                                                 |
| --------- | ----------------------------------------------------------------------- |
| `Cross`   | Invoked as a separate workflow step (cross-step dependency).            |
| `Inter`   | Folded into the same step as a companion (interstep / same-step).       |
| `Both`    | Functions as both interstep AND cross-step.                             |

Default is `Inter`.

#### `VersionSpec::Inherit`

- Signals "use the dependency tool's global version spec from `tools.<id>.version_spec`".
- Resolved at provisioning time by `resolve_dep_version_spec()`.
- Errors if the referenced tool is not configured in the workspace, or if
  the global tool itself has `version_spec: inher` (circular resolution).

#### Spec matching (`spec_matches_entry`)

- `spec_matches_entry(spec, resolved_tag, resolved_version, resolved_vcs_hash) -> bool`
- For `VersionSpec::Latest` and `VersionSpec::Inherit`, always returns `false`
  (caller must re-resolve).
- For `VersionSpec::Exact(fields)`, all specified fields must match.
  Unspecified fields are not checked.
- All comparisons are exact string match, trimmed whitespace. No semver
  normalization.

### Active-tool computation (`compute_used_tool_ids`)

- Input: `desired_tools` (all tool requirements from config) and `step_tool_ids`
  (tools directly referenced by workflow steps).
- Traverses transitive dependencies via `deps.keys()` using a stack-based DFS
  with a `HashSet` visited set.
- Returns the set of tool IDs that should be provisioned.
- Tools NOT in this set get their content_map cleared, filesystem payloads
  removed, and provisioning skipped entirely.
- Handles circular dependencies correctly (visited-set terminates the DFS).

### Companion binding strategy (future)

- **Same-step** (`dep_type: Inter`): companion payload bytes will be inlined
  into the requester's content_map with a prefix (e.g. `companions/`).
- **Cross-step** (`dep_type: Cross`): payload bytes and ids remain separate.
- Not yet wired in the coordinator; `dep_type` annotations are available for
  future binding implementation.

_Note: the generated env output function previously documented here has moved to
`mediapm-conductor::runtime_env::write_generated_dotenv`. See
`src/mediapm-conductor/AGENTS.md` for the current documentation._
