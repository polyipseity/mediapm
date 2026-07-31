---
description: "Use when editing companion dependency resolution via flattened BTreeMap dependencies in src/mediapm/src/config/mod.rs and src/mediapm/src/conductor_bridge/sync/mod.rs."
name: "Tool Sync Dependency Resolution"
applyTo: "src/mediapm/src/config/mod.rs, src/mediapm/src/conductor_bridge/sync/mod.rs"
---

# Tool sync dependency resolution

## Companion dependencies

### Purpose

- Model tool-to-tool dependency declarations as a flat
  `BTreeMap<String, VersionSpec>` on `ToolRequirement`, replacing the old
  `ToolRequirementDependencies` wrapper and `DependencySpec` struct.
- Dependency relationship type (`SameStep` vs `CrossStep`) is determined by
  per-preset `known_dependency_type()` lookup, not by user config.
- Consumed by `compute_used_tool_ids` to determine which tools are active
  (by traversing transitive `SameStep`/`Both` deps from step tool IDs).
- Companion binding (inlining same-step deps into the requester's content map)
  uses `DependencyType::SameStep` to identify same-step relationships.

### Dependency data model

`ToolRequirement.dependencies` is a flat `BTreeMap<String, VersionSpec>`:

```nickel
dependencies = { ffmpeg = "inherit", deno = "latest" }
```

No `DependencySpec`, no `ToolRequirementDependencies`, no `dep_type` in user
config. The companion relationship type is defined internally per preset.

#### `DependencyType` (in `crate::tools::dependency::DependencyType`)

| Variant     | Meaning                                                          |
| ----------- | ---------------------------------------------------------------- |
| `SameStep`  | Folded into the same step as a companion (same-step dependency). |
| `CrossStep` | Invoked as a separate workflow step (cross-step dependency).     |
| `Both`      | Functions as both same-step AND cross-step.                      |

Not user-configurable. No serde derives. Defined in `src/mediapm/src/tools/dependency.rs`.

#### `VersionSpec::Inherit`

- Signals "use the dependency tool's global version spec from `tools.<id>.version_spec`".
- Resolved at provisioning time by `resolve_dep_version_spec()`.
- Errors if the referenced tool is not configured in the workspace, or if
  the global tool itself has `version_spec: inherit` (circular resolution).

#### Spec matching (`spec_matches_entry`)

- `spec_matches_entry(spec, resolved_tag: Option<&str>, resolved_version: Option<&str>, resolved_vcs_hash: Option<&str>) -> bool`
- For `VersionSpec::Latest` and `VersionSpec::Inherit`, always returns `false`
  (caller must re-resolve).
- For `VersionSpec::Exact(fields)`, each specified field matches only when the
  stored value is `Some` AND equals the spec value. Unspecified fields are not
  checked; stored `None` never matches — an entry missing a resolved field is
  always re-provisioned.
- All comparisons are exact string match. No trim, no semver normalization.

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

- **Same-step** (`DependencyType::SameStep`): companion payload bytes will be inlined
  into the requester's content_map with a prefix (e.g. `companions/`).
- **Cross-step** (`DependencyType::CrossStep`): payload bytes and ids remain separate.
- Not yet wired in the coordinator; `DependencyType` annotations from per-preset
  `dependency_types()` are available for future binding implementation.

_Note: the generated env output function previously documented here has moved to
`mediapm-conductor::runtime_env::write_generated_dotenv`. See
`src/mediapm-conductor/AGENTS.md` for the current documentation._
