---
description: "Use when editing companion dependency resolution via flattened BTreeMap dependencies in src/mediapm/src/config/mod.rs and src/mediapm/src/conductor_bridge/sync/mod.rs."
name: "Tool Sync Dependency Resolution"
applyTo: "src/mediapm/src/config/mod.rs, src/mediapm/src/conductor_bridge/sync/mod.rs"
---

# Tool sync dependency resolution

## Companion dependencies

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
- Resolved at provisioning time by `resolve_dep_version_spec()`; errors if the referenced tool is unconfigured or itself `inherit` (circular).

#### Spec matching (`spec_matches_entry`)

- `spec_matches_entry(spec, resolved_tag: Option<&str>, resolved_version: Option<&str>, resolved_vcs_hash: Option<&str>) -> bool`.
- `VersionSpec::Latest` always returns `false` (caller re-resolves); `Inherit` is resolved away before this point.
- `VersionSpec::Exact(fields)`: each specified field matches only when stored is `Some` AND equals the spec; unspecified fields are unchecked; stored `None` never matches (missing field ⇒ always re-provision).
- All comparisons are exact string match — no trim, no semver normalization.

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
- Env output distinguishes the two ids: env var **names** derive from the plain mediapm tool id (hash-free, e.g. `MEDIAPM_YT_DLP_LINUX`); env var **values** point at `<tools_dir>/<sanitize_tool_id(conductor_tool_id)>/payload/<key>`, mirroring the provision-cache layout.

### Companion binding (inlined same-step deps)

- **Same-step** (`DependencyTypes::SAME_STEP`): the requester's content map gains `deps/{dep_mediapm_tool_id}/{dep_own_key}` → payload hash for every DIRECT same-step dep, copying the dep's OWN (pre-inline) payload map. Wired in `sync/mod.rs`: `inline_same_step_deps` runs at the provision merge point; per-outcome own maps tracked in `provisioned_own_maps`. `deps/` is a **reserved prefix**.
- **Direct-only, non-transitive**: inlining copies only the dep's own payload keys; a dep's own `deps/` entries are never re-inlined (`inline_same_step_deps` skips keys already under `deps/`).
- **Cross-step** (`DependencyTypes::CROSS_STEP`): payload bytes and ids stay separate.
- A dependency carrying both roles is inlined for its same-step role AND kept separate for its cross-step role.
- **No companion env vars**: `write_generated_dotenv` skips `deps/`-prefixed keys; inlined companions are referenced via the predictable `deps/<tool_id>/` path, never env vars.
- **Consumption wiring deferred to Stream A**: inlining produces the payload; wiring `ffmpeg_location` to `deps/ffmpeg/{os}/ffmpeg` needs OS-conditional step inputs (unsupported today) or a future mechanism. yt-dlp's `ffmpeg_location` default stays bare `"ffmpeg"`; the predictable `deps/<id>/` path is the future hook.
