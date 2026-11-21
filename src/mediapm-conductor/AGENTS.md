# Conductor crate instructions

Crate-local guidance for `src/mediapm-conductor/`. Conductor orchestrates workflows from `conductor.ncl`; `conductor.generated.ncl` is machine-managed; content and outputs are CAS-backed. Root `AGENTS.md` and `.agents/instructions/*.instructions.md` are the workspace baseline; this file keeps only conductor-specific invariants and pointers.

## Scope and entry points

- Applies to all files under `src/mediapm-conductor/`.
- Source of truth: `Cargo.toml`, `src/lib.rs`, `src/main.rs`, `src/cli.rs`, `src/api.rs`, `src/conductor_impl.rs`, `src/config/`, `src/state/`, `src/orchestration/`, `src/provision/`, `src/tools/`, `src/gc.rs`, `src/defaults.rs`.
- CLI/API parity: CLI handlers call `ConductorApi`/`Conductor`; validation and mutation semantics match API paths. CLI-only ergonomics (args, formatting) may differ.
- Progress boundary: the library does not depend on indicatif; progress flows through `RunWorkflowOptions.progress_group`/`progress_sender` and `WorkflowStepEvent`. Rendering/truncation/budget rules: `workflow-runtime.instructions.md`.

## Configuration and state model

- Two config documents plus runtime state: user intent `conductor.ncl`, machine-managed `conductor.generated.ncl`, runtime orchestration state at `RuntimeStoragePaths.state_file_path` (default `.conductor/state.json`, pretty JSON with its own `version`; not Nickel).
- Load/save/conflict: effective config merges user and generated documents; runtime state loads/saves separately. Unresolvable/non-mergeable conflicts fail fast and require manual resolution. Machine/setup automation mutates only `conductor.generated.ncl`; user intent stays machine-unmodified.
- Storage paths come from `RunWorkflowOptions.runtime_storage_paths` (not document fields except runtime behavior like `retry_impure` and platform inherited env config). Contract export writes Nickel sources the runtime evaluates (`mod.ncl` and imported version files) before execution; `export-schemas` keeps its name but exports Nickel, not JSON schema.
- State/generated docs are versioned with explicit integer markers and sequential migrations. See `state-and-versioning.instructions.md`, `config/versions/`, `state/versions/`.
- Document invariant: every CAS hash named by a document must be declared in `external_data` (tool `runtime.content_map` values, `${external_data.<hash>}` in step inputs/tool defaults/tool command parts, and hash-valued inputs). Enforced at decode/encode/merge via `validate_external_data_invariant`; undeclared hashes fail fast. See `config/versions/mod.rs`, `config/versions/merge.rs`, `orchestration::step_worker::template::external_data_hashes`.

## Core invariants

- Pure planner/imperative shell: deterministic planning/keying in pure logic; process/filesystem/network effects at explicit execution boundaries.
- Determinism and identity: deterministic ids, serialization, and content-addressed work; atomic writes for state/object mutations where applicable. CAS objects/instances are addressed by content-derived hashes.
- Async orchestration: Tokio runtime, `ractor` actor-oriented coordination with explicit message contracts and supervision; step workers run as actors.
- Step/tool identity: workflow steps reference tools by `ToolSpec.name`, not map key; `find_tool_by_name` resolves by spec name. Payload staging is resolved at document read (`NickelDocument::effective_payload_staging`) so unified tools carry concrete strategy.
- GC/maintenance: root set is surviving instance outputs plus document `external_data` keys and tool content hashes; reachable/retention rules and background maintenance live in `gc.rs`/`conductor_impl.rs`. Background GC is not disabled as a workaround; fix implementation issues instead.

## Environment model

- No inherited ambient env by default; scrub ambient env and pass only declared names: document platform defaults (`runtime.environment.platform_inherited_env_vars`, host platform entry), tool `inherited_env_vars`, and tool literal `env_vars`.
- `env_vars` is a whitelist/literal map: a literal outranks an inherited/platform name of the same key; empty value means take host value for that name. Deterministic non-empty literals are key material and inlined where bindings read them; inherited names and empty-value host reads are recorded provenance, not host-value key material. `conductor tool run` applies the same whitelist/resolver.
- Shared child-env policy: `mediapm_utils::child_env`. Config/default helpers: `config/mod.rs` (`default_platform_inherited_env_var_names`, `default_runtime_inherited_env_vars`). Runtime resolution/application: `orchestration/step_worker/`, `env_registry.rs`, CLI runtime env helpers in `cli.rs`.

## Instance key v8

- `INSTANCE_KEY_VERSION` and derivation live in `src/mediapm-conductor/src/instance_key.rs` (domain `mediapm/conductor/tool call instances/v8`).
- Key covers tool-call id, command key-material parts, declared builtin inputs, env_vars map, v8 output spec, and staged `materialized_inputs` (ordered as implemented). Output hashes, timestamps, persistence/save facts, and inherited host env values are recorded results/provenance, not key material.
- Provenance split: deterministic document-fixed literals key/inline; inherited env, captured hashes, timestamps, and persistence facts are recorded but not keyed. Key-version moves are forward-only; records carry `key_version`/`attempted_key_version_nk` for migration/reuse handling.
- Pointers: `instance_key.rs`, `orchestration/step_worker/cache.rs`, `state/` instance record model.

## Locking and workspace safety

- Single-writer lock discipline for workspace state: no stale lock takeover.
- State mutations use the facade state gate for read-modify-write consistency; coordinator/GC/run paths must not race state writes. CAS store locking is advisory/cooperative as implemented by the backend.
- Pointers: `conductor_impl.rs`, `orchestration/coordinator.rs`, `gc.rs`, CAS store lock behavior in `mediapm-cas` integration.

## Work materialization and CAS IO

- Payload staging strategies: `runtime.payload_staging` default and per-tool `runtime.payload_staging` (`inherit` or concrete strategy). Resolved strategies include `in_place`, `hard_link`, and `copy`; validation and sandbox preparation follow the resolved strategy.
- `copy` materializes `tool_content_maps` into the step sandbox; `in_place` executes from provisioned payload root without staging tool files into cwd; `hard_link` links payload into sandbox. Cwd remains sandbox for outputs/capture; path resolution and payload-argument qualification follow strategy rules in step worker/provision code.
- Capture/materialization boundaries: outputs are explicit capture specs; settlement/cache probe/materialization honor recorded capture identity, `allow_empty`, persistence facts, and required-output checks. Missing required blobs on work-skipping paths downgrade to re-execution/invalidation as implemented.
- Pointers: `provision/`, `provision/helpers.rs`, `orchestration/step_worker/{executor,process,cache,template}.rs`, `orchestration/coordinator.rs`.

## Provider, tool, and builtin contract

- Tool provisioning follows the 3-phase provider contract (resolve/fetch/process) with `ToolProvider`/`ToolRequirements` and preset/runtime builders; content maps and os exec paths are provisioned content-addressed. See `tool-provisioning.instructions.md`, `tools/provider/`, `tools/preset/`, `provision/`.
- Provision cache is conductor-owned under `<conductor_tools_dir>` (default `<conductor_dir>/tools`), keyed by conductor tool id, with metadata/lock/payload entry invariants and RAII guards; do not bypass it.
- Host builtin boundary: `HostBuiltin`/`HostBuiltins`/`HostBuiltinRequest` in `tools/host_builtin.rs`; host registries passed via `RunWorkflowOptions::host_builtins`. Static builtins dispatch first; host ids cannot shadow static ids and document builtin ids are checked at run start.
- Builtin runtime logic lives in `src/mediapm-conductor-builtins/*`; conductor runtime does not reimplement builtin behavior inline. Registry and production/test-only gating: `tools/mod.rs`.

## Error handling and diagnostics

- Workflow/config/runtime failures use workspace/conductor error types and codes; follow `sdd-tdd.instructions.md` for MPM error taxonomy and regression discipline.
- Fail fast on invalid documents, merge conflicts, undeclared external-data hashes, missing required inputs/captures, and un-dispatchable builtins at the appropriate boundary (load/merge/run). Preserve useful context for process and CAS failures.

## Build, test, and change discipline

- Focused checks: `cargo test-pkg mediapm-conductor`, `cargo test -p mediapm-conductor <test_name>`, `cargo clippy-pkg mediapm-conductor`, `cargo build-pkg mediapm-conductor`.
- Workspace/full gates and hook/CI parity: `rust-workflow.instructions.md`. Test expectations and isolation: `rust-workflow.instructions.md`, `temp-and-paths.instructions.md`, `examples-and-demos.instructions.md`.
- Keep edits minimal and deterministic. Prefer updating focused instruction files, module docs, and tests with behavior changes instead of expanding this crate guide.

## Key references

- API/runtime: `api.rs`, `conductor_impl.rs`, `cli.rs`, `defaults.rs`
- Config/state/migrations: `config/mod.rs`, `config/versions/`, `state/mod.rs`, `state/versions/`
- Orchestration: `orchestration/coordinator.rs`, `orchestration/node.rs`, `orchestration/protocol.rs`, `orchestration/step_worker/`
- Identity/provisioning/tools: `instance_key.rs`, `provision/`, `tools/{mod,host_builtin}.rs`, `tools/{provider,preset}/`, `gc.rs`
- Workspace instructions: `mediapm-architecture.instructions.md`, `tool-provisioning.instructions.md`, `state-and-versioning.instructions.md`, `workflow-runtime.instructions.md`, `temp-and-paths.instructions.md`, `sdd-tdd.instructions.md`, `rust-workflow.instructions.md`
