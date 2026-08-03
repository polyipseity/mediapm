---
description: "Use when editing companion dependency resolution via flattened BTreeMap dependencies in src/mediapm/src/config/mod.rs and src/mediapm/src/conductor_bridge/sync/mod.rs."
name: "Tool Sync Dependency Resolution"
applyTo: "src/mediapm/src/config/mod.rs, src/mediapm/src/conductor_bridge/sync/mod.rs"
---

# Tool sync dependency resolution

## Companion dependencies

### Purpose

- Model tool-to-tool dependency declarations as a flat
  `BTreeMap<String, ConfigVersionSpec>` on `ToolRequirement`, replacing the old
  `ToolRequirementDependencies` wrapper and `DependencySpec` struct.
- Dependency role flags (same-step vs cross-step) are determined by per-preset
  `known_dependency_type()` lookup, not by user config; a dependency may carry
  both roles.
- Companion binding (inlining same-step deps into the requester's content map)
  uses the same-step role flag (`DependencyTypes::SAME_STEP`) to identify
  same-step relationships.

### Dependency data model

`ToolRequirement.dependencies` is a flat `BTreeMap<String, ConfigVersionSpec>`:

```nickel
dependencies = { ffmpeg = "inherit", deno = "latest" }
```

No `DependencySpec`, no `ToolRequirementDependencies`, no `dep_type` in user
config. The companion relationship type is defined internally per preset.

#### `DependencyTypes` (in `crate::tools::dependency::DependencyTypes`)

Role flags on a `bool`-field struct; a dependency may carry one or both roles.
`SAME_STEP` and `CROSS_STEP` are the single-role constants; `combine()` unions
them (the removed `Both` variant's semantics = both flags set).

| Role flag    | Meaning                                                          |
| ------------ | ---------------------------------------------------------------- |
| `SAME_STEP`  | Folded into the same step as a companion (same-step dependency). |
| `CROSS_STEP` | Invoked as a separate workflow step (cross-step dependency).     |

Not user-configurable. No serde derives. Defined in `src/mediapm/src/tools/dependency.rs`.

#### `ConfigVersionSpec::Inherit`

- Signals "use the dependency tool's global version spec from `tools.<id>.version_spec`".
- Resolved at provisioning time by `resolve_dep_version_spec()`.
- Errors if the referenced tool is not configured in the workspace, or if
  the global tool itself has `version_spec: inherit` (circular resolution).

#### Spec matching (`spec_matches_entry`)

- `spec_matches_entry(spec, resolved_tag: Option<&str>, resolved_version: Option<&str>, resolved_vcs_hash: Option<&str>) -> bool`
- For `VersionSpec::Latest`, always returns `false` (caller must re-resolve).
  `Inherit` has been resolved away before this point.
- For `VersionSpec::Exact(fields)`, each specified field matches only when the
  stored value is `Some` AND equals the spec value. Unspecified fields are not
  checked; stored `None` never matches — an entry missing a resolved field is
  always re-provisioned.
- All comparisons are exact string match. No trim, no semver normalization.

### Active-tool tracking (via `tool_runtimes` keys)

- The active set for provisioning retention is the live `tool_runtimes` keys —
  every tool inserted by the provisioning loop, keyed by its **mediapm
  conductor tool id** (`{name}@{hash}` when the content map is non-empty, bare
  `{name}` when empty).
- `compute_used_tool_ids` was deleted: the provisioning loop's `tool_runtimes`
  keys are the single source of truth for what remains provisioned. Tools NOT
  in this set get their content_map cleared and filesystem payloads removed
  after the provisioning loop (`retain_only_tool_dirs` with the conductor-id
  set).
- Env output distinguishes the two ids: env var **names** derive from the plain
  mediapm tool id (hash-free, e.g. `MEDIAPM_YT_DLP_LINUX`), env var **values**
  point at `<tools_dir>/<sanitize_tool_id(conductor_tool_id)>/payload/<key>`
  mirroring the provision-cache layout.

### Companion binding (inlined same-step deps)

- **Same-step** (`DependencyTypes::SAME_STEP` role): the requester's content map
  gains `deps/{dep_mediapm_tool_id}/{dep_own_key}` → payload hash entries for
  every DIRECT same-step dep, copying the dep's OWN (pre-inline) payload map.
  Wired in `sync/mod.rs`: `inline_same_step_deps` runs at the provision merge
  point; per-outcome own maps are tracked in `provisioned_own_maps`. `deps/` is
  a **reserved prefix**.
- **Direct-only, non-transitive**: inlining copies only the dep's own payload
  keys; a dep's own `deps/` entries are never re-inlined into the requester
  (`inline_same_step_deps` skips keys already under `deps/`).
- **Cross-step** (`DependencyTypes::CROSS_STEP` role): payload bytes and ids remain
  separate.
- A dependency carrying both roles is inlined for its same-step role AND kept
  separate for its cross-step role.
- **No companion env vars**: `write_generated_dotenv` skips `deps/`-prefixed
  keys; inlined companions are referenced via the predictable `deps/<tool_id>/`
  path, never env vars.
- **Consumption wiring deferred to Stream A**: inlining produces the payload;
  wiring `ffmpeg_location` to `deps/ffmpeg/{os}/ffmpeg` needs OS-conditional
  step inputs (not supported today) or a future mechanism. yt-dlp's
  `ffmpeg_location` default remains bare `"ffmpeg"`; the predictable
  `deps/<id>/` path is the future hook.

_Note: the generated env output function previously documented here has moved to
`mediapm-conductor::runtime_env::write_generated_dotenv`. See
`src/mediapm-conductor/AGENTS.md` for the current documentation._
