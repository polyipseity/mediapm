# Conductor crate instructions

`src/mediapm-conductor/`: orchestration from `conductor.ncl`; `conductor.generated.ncl` machine-managed; outputs CAS-backed. Root `AGENTS.md` + `.agents/instructions/*.instructions.md` are baseline.

## Scope and entry points

- Source: `Cargo.toml`, `src/lib.rs`, `src/main.rs`, `src/cli.rs`, `src/api.rs`, `src/conductor_impl.rs`, `src/config/`, `src/state/`, `src/orchestration/`, `src/provision/`, `src/tools/`, `src/gc.rs`, `src/defaults.rs`.
- CLI/API parity: handlers call `ConductorApi`/`Conductor`, same validation/mutation semantics; only args/formatting differ.
- No indicatif in library: progress via `RunWorkflowOptions.progress_group`/`progress_sender`, `WorkflowStepEvent`; rules in `workflow-runtime.instructions.md`.

## Configuration and state

- User `conductor.ncl`, machine `conductor.generated.ncl`, runtime state `RuntimeStoragePaths.state_file_path` (default `.conductor/state.json`, pretty JSON with own `version`, not Nickel).
- Merge user + generated; state loads/saves separately. Unresolvable conflicts fail fast, manual resolution; automation mutates only generated doc.
- Paths from `RunWorkflowOptions.runtime_storage_paths` (not doc fields except runtime behavior/platform inherited env config). Contract export writes evaluated Nickel sources (`mod.ncl` + imports) before execution; `export-schemas` exports Nickel, not JSON schema.
- `max_concurrent_calls`: `null` (default) or >=1; `max_retries` 3 default, `0` = one attempt; step override only `step.runtime.max_retries`; no impurity gate (state pointer moves only on success, D4a).
- Integer versions/sequential migrations (`state-and-versioning.instructions.md`, `config/versions/`, `state/versions/`).
- External-data invariant: every CAS hash named by a doc declared in `external_data` (tool `runtime.content_map` values, `${external_data.<hash>}` in step inputs/tool defaults/tool command parts, hash-valued inputs); `validate_external_data_invariant` at decode/encode/merge fails fast. See `config/versions/mod.rs`, `config/versions/merge.rs`, `orchestration::step_worker::template::external_data_hashes`.

## Core invariants

- Pure planner/imperative shell, deterministic ids/serialization/content-addressed work, atomic writes; Tokio + `ractor` typed-message actors, step workers as actors. Steps reference `ToolSpec.name` (`find_tool_by_name`) by `tool_id`, not map keys; staging resolved at read (`NickelDocument::effective_payload_staging`).
- GC roots = surviving outputs + `external_data` keys + tool hashes (`gc.rs`/`conductor_impl.rs`); never disable GC as workaround. 3-phase provider (`ToolProvider`/`ToolRequirements`), conductor cache `<conductor_tools_dir>` (default `<conductor_dir>/tools`) keyed by conductor id with metadata/lock/payload guards (`tool-provisioning.instructions.md`, `tools/provider/`, `tools/preset/`, `provision/`).
- Host builtins `HostBuiltin`/`HostBuiltins`/`HostBuiltinRequest` (`tools/host_builtin.rs`) via `RunWorkflowOptions::host_builtins`; static dispatch first, no shadowing, doc ids checked at run start; logic in `src/mediapm-conductor-builtins/*`, gating in `tools/mod.rs`. Fail fast on invalid docs, merge conflicts, undeclared hashes, missing inputs/captures, un-dispatchable builtins (`sdd-tdd.instructions.md`).

## Environment model

Scrub ambient env; pass only declared names from platform defaults (`runtime.environment.platform_inherited_env_vars`, host entry), `inherited_env_vars`, literal `env_vars`. Literal outranks inherited/platform key; empty value = host value. Non-empty literals inlined as key material; inherited/empty reads provenance only; `conductor tool run` uses same resolver. `mediapm_utils::child_env`; helpers `config/mod.rs` (`default_platform_inherited_env_var_names`, `default_runtime_inherited_env_vars`); resolution `orchestration/step_worker/`, `env_registry.rs`, `cli.rs`.

## Instance key v9 and the instance key secret

- `INSTANCE_KEY_VERSION = 9` with derivation in `src/mediapm-conductor/src/instance_key.rs` (domain `mediapm/conductor/tool call instances/v9`). Versions `7` and `8` remain migration inputs only. Records carry `key_version`/`attempted_key_version_nk`.
- Keyed mode reads the sibling `instance-key-secret.hex` beside the conductor state file. The file holds 64 lowercase hex characters plus one newline, is published through a sibling temporary and rename with Unix mode `0600`, and passes a byte-frequency sanity check under the strict generated format; relaxed loads accept uppercase hex without the newline. State stores only a `KeyedHash` verifier with the `blake3-keyed:` prefix. CAS hashes keep `blake3:`.
- Force-generate replaces the file and verifier when state records no verifier. Force-disable permits unkeyed mode only with the file gone and overrides a stored verifier. A verifier mismatch is an error.
- Keyed v9 cannot migrate older unkeyed material: those records miss and re-execute rather than being retroactively privatized. Output hashes, timestamps, persistence facts, and inherited host env values stay results, not key material.
- Pointers: `instance_key.rs`, `instance_key_secret.rs`, `keyed_hash.rs`, `derived_hash.rs`, `orchestration/step_worker/cache.rs`, `state/`.

## Locking and workspace safety

Single-writer discipline, no stale lock takeover; facade state gate for read-modify-write; coordinator/GC/run must not race. CAS store locking advisory/cooperative. Pointers `conductor_impl.rs`, `orchestration/coordinator.rs`, `gc.rs`, CAS lock behavior in `mediapm-cas`.

## Work materialization and CAS IO

- Payload staging `runtime.payload_staging` default + per-tool (`inherit` or `in_place`, `hard_link`, `copy`); `copy` materializes `tool_content_maps` into sandbox, `in_place` from payload root, `hard_link` links; cwd stays sandbox for outputs/capture; path/payload-argument qualification per step worker/provision code.
- Outputs are explicit capture specs; settlement/cache probe/materialization honor recorded identity, `allow_empty`, persistence facts, required outputs; missing required blobs on work-skipping paths downgrade to re-execution/invalidation.
- Pointers `provision/`, `provision/helpers.rs`, `orchestration/step_worker/{executor,process,cache,template}.rs`, `orchestration/coordinator.rs`.

## Build, test, and change discipline

`cargo test-pkg mediapm-conductor`, `cargo test -p mediapm-conductor <test_name>`, `cargo clippy-pkg mediapm-conductor`, `cargo build-pkg mediapm-conductor`; gates `rust-workflow.instructions.md`, isolation `temp-and-paths.instructions.md`/`examples-and-demos.instructions.md`. Minimal edits; update focused docs/tests.

## Key references

`api.rs`, `conductor_impl.rs`, `cli.rs`, `defaults.rs`; `config/mod.rs`, `config/versions/`, `state/mod.rs`, `state/versions/`; `orchestration/coordinator.rs`, `orchestration/node.rs`, `orchestration/protocol.rs`, `orchestration/step_worker/`; `instance_key.rs`, `provision/`, `tools/{mod,host_builtin}.rs`, `tools/{provider,preset}/`, `gc.rs`; `mediapm-architecture.instructions.md`, `tool-provisioning.instructions.md`, `state-and-versioning.instructions.md`, `workflow-runtime.instructions.md`, `temp-and-paths.instructions.md`, `sdd-tdd.instructions.md`, `rust-workflow.instructions.md`.
