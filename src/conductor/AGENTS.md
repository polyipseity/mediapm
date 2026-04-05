# Conductor Crate Instructions

This file defines crate-local guidance for `src/conductor/`.
Follow this together with the workspace root `AGENTS.md` and relevant
`.agents/instructions/*.instructions.md` files.

## Scope

- Applies to all files under `src/conductor/`.
- Treat this file as the primary implementation policy for conductor behavior,
  with root `AGENTS.md` as the workspace-wide baseline.
- If rules conflict, prefer root `AGENTS.md` for global policy and this file for
  conductor-specific design/behavior details.

## Current Stack and Entry Points

Use concrete files as source of truth:

- Crate manifest: `src/conductor/Cargo.toml`
- Library entry: `src/conductor/src/lib.rs`
- CLI entry: `src/conductor/src/main.rs`
- CLI implementation: `src/conductor/src/cli.rs`
- Runtime orchestration: `src/conductor/src/orchestration/coordinator.rs`,
  `src/conductor/src/orchestration/actors/`,
  `src/conductor/src/orchestration/protocol.rs`
- Runtime model: `src/conductor/src/model/config/mod.rs`,
  `src/conductor/src/model/state/mod.rs`
- Versioned config schema + migration bridge:
  `src/conductor/src/model/config/versions/`

Key ecosystem (from `Cargo.toml`):

- Async runtime: `tokio`
- Actor framework: `ractor`
- Serialization: `serde`, `serde_json`
- CAS integration: `mediapm-cas`
- Hashing: `blake3`
- CLI: `clap`

## Configuration Document Model

Conductor uses two config documents plus one runtime state document:

- User-edited config: `conductor.ncl`
- Program-edited config: `conductor.machine.ncl`
- Volatile runtime state: resolved from grouped runtime path config
  (`RunWorkflowOptions.runtime_storage_paths`), default
  `.conductor/state.ncl`

Grouped runtime path defaults:

- runtime root (`conductor_dir`): `.conductor`
- volatile state path (`config_state`): `<conductor_dir>/state.ncl`
- filesystem CAS store (`cas_store_dir`): `<conductor_dir>/store`

Document contract:

- `conductor.ncl` is treated as user-edited input and is not machine-mutated.
- `conductor.machine.ncl` stores machine-managed setup/config declarations.
- `conductor.ncl` and `conductor.machine.ncl` may define grouped runtime
  storage fields under one `runtime_storage` record:
  `runtime_storage.conductor_dir`, `runtime_storage.state_ncl`,
  `runtime_storage.cas_store_dir`. The `cas_store_dir` field accepts any CAS
  locator string (filesystem path or URL).
- resolved state path (default `.conductor/state.ncl`) stores volatile runtime state only and may define
  only `version`, `impure_timestamps`, and `state_pointer`.
- All three files must define explicit top-level numeric `version` markers.
- `conductor.ncl` and `conductor.machine.ncl` share the full schema surface;
  resolved runtime state path (default `.conductor/state.ncl`) is a strict
  volatile subset.
- Effective configuration is resolved by merging all three documents.
- Conflicts must fail fast with explicit workflow errors.
- End-user automation for setup operations (for example add-tool/import-tool and
  add-external-data/import-data flows) must mutate only
  `conductor.machine.ncl`.
- Once setup is recorded through machine-document automation, do not require
  duplicate tool declarations in `conductor.ncl` just to make workflows runnable.

## Conductor Builtin Tool Strategy

This repository follows the design principle that conductor builtins are the
"connective tissue" for bootstrapping and cross-platform consistency. The
official baseline set is:

- `echo` (reference pure pass-through runtime contract)
- `fs` (rooted filesystem staging operations: ensure_dir, write_text, copy)
- `import` (`kind=file|folder|fetch` ingestion to pure bytes)
- `export` (`kind=file|folder` filesystem materialization)
- `archive` (pure ZIP-only pack/unpack/repack transforms)

All other domain logic remains external tooling or Phase 3 workflow behavior.

## Tool Schema and Runtime Invariants

When editing tool/config schema behavior, preserve these invariants:

1. Tool name is immutable identity and must include version in name
  (example: `compose@1.0.0`).

2. Tool-level `version` field is not used.

3. Builtin tool definitions in persisted config are strict: only
   `kind`, `name`, and `version` are allowed.

4. Executable tool definitions may declare `inputs`, `command`, `env_vars`,
   `success_codes`, and explicit `outputs`.

5. Workflow step `inputs` are always tool-call input data (for both
   executable and builtin tools).

6. For executable tools, workflow step inputs must reference declared
   executable tool inputs; missing required inputs (without defaults) are
   errors.

7. For builtin tools, step inputs are pass-through bindings and builtin crates
   enforce their own strict argument/input contracts.

8. Workflow-step input bindings are always strings and support `${...}`
   interpolation with these expression forms:
   `${external_data.<name>}` and `${step_output.<step_id>.<output_name>}`.
   Input-binding interpolation is text-oriented and does not support
   materialization directives such as `:file(...)` or `:folder(...)`;
   unsupported `${...}` expressions are invalid.

9. `${step_output.<step_id>.<output_name>}` references define the workflow DAG
   implicitly; there is no explicit `depends_on` field.

10. Tool execution is kind-tagged directly on each tool (`kind = "executable"`
    or `kind = "builtin"`) rather than nested under `tool.process`.

11. Step-level `process` overrides are not part of the workflow-step contract.

12. Outputs are explicit and capture-based
    (`stdout`/`stderr`/`process_code`/`file`/`folder`) at the tool definition.

13. Step `outputs` configure per-output persistence policy (`save`,
    `force_full`) only and can only target declared tool outputs.

14. `tool_configs.<tool>.content_map` is executable-only and
    sandbox-relative: keys ending with `/` or `\\` mean destination directories
    whose mapped CAS bytes must be ZIP payloads to unpack there; keys without a
    trailing slash/backslash mean destination files whose mapped bytes are
    written directly; `./` (or `.\\`) is valid and means sandbox-root unpack;
    separate content-map entries must not overwrite the same target file path;
    absolute and escaping paths are invalid.

15. Cache rematerialization checks are scoped to outputs actually referenced by
    `${step_output...}` workflow-step inputs; missing unreferenced outputs do
    not force rerun for otherwise cache-hit instances.

16. Keep step-output references minimal so independent steps can remain in
    parallelizable topological levels.

17. Builtin `import`/`export` path semantics for `kind=file|folder` are:
    `path_mode` defaults to `relative` and resolves `path` against the
    outermost config directory, `relative` paths must not escape that root,
    and `path_mode=absolute` requires an explicit absolute `path`.

18. Orchestration-state snapshots must include explicit top-level `version`.

19. `ToolCallInstance.metadata` is persistence-normalized: executable metadata
    remains `ToolSpec`-shape, while builtin metadata persists only
    `kind`/`name`/`version`; `impure_timestamp` belongs at instance top-level,
    not inside metadata. Decode must reject extra builtin metadata fields.

20. `ToolCallInstance.inputs` persist CAS hash references (no inline
    `plain_content` payload and no separate `source_hash` provenance field).

21. For duplicate equivalent tool-call instances merged under one instance key,
    persisted output `persistence` must be the effective merged policy across all
    callers (`save`: logical AND, `force_full`: logical OR).

22. Any human-facing orchestration-state JSON output (for example CLI `state`
    output or demo artifacts) must render the persisted wire-envelope shape so
    builtin metadata stays strict (`kind`/`name`/`version`) and does not leak
    runtime-only optional fields.

If adding validation, apply it both where practical:

- schema bridge validation (`model/config/versions/mod.rs`)
- runtime unification/execution checks (`orchestration/coordinator.rs` and
  `orchestration/actors/step_worker/mod.rs`)

## `${...}` Template Syntax Contract

Template expansion behavior is implemented in
`src/conductor/src/orchestration/actors/step_worker/template.rs`.

Supported token forms:

- `${<name>}`
  - JavaScript-like bare identifier interpolation for input keys.
- `${inputs.<name>}`
  - Decodes input bytes with lossy UTF-8 conversion and injects text.
- `${inputs["<name>"]}` / `${inputs['<name>']}`
  - JavaScript-like bracket notation for input keys.
- `${<selector>:file(<relative_path>)}`
  - Uses one selector form above, queues bytes for `<relative_path>`, then
    injects that path string.
- `${inputs.<name>:file(<relative_path>)}`
  - Queues input bytes for `<relative_path>` under an ad hoc temporary
    execution directory, then injects that relative path string.
- `${os.<target>?<value>}`
  - Includes `<value>` only on matching host OS (`windows`, `linux`, or
    `macos`), otherwise renders empty content.
  - `<value>` resolves recursively and supports selector/materialization
    special forms (for example `inputs.payload:file(payload.txt)`).
- `\${...}`
  - Escapes interpolation start and renders literal `${...}`.
- JavaScript-like string escapes in literal spans are supported.
  - Examples: `\\`, `\n`, `\t`, `\xNN`, `\u{NNNN}`.

Rendering scope:

- All `${...}` spans are parsed.
- Unsupported expression forms fail explicitly (no silent literal fallback).
- `${context.config_dir}` is unsupported in template rendering and
  unsupported in workflow-step input bindings.

Rules:

- Absolute file paths in `file(...)` are rejected.
- Unknown inputs fail workflow resolution.
- `${...` without a closing `}` fails workflow resolution.
- Unsupported/trailing escape sequences fail workflow resolution.
- Malformed `:file(...)` tokens (for example missing closing `)`) fail workflow
  resolution.

When changing parser/templating logic, update Rust docstrings in:

- `render_templates`
- `render_template_value`
- `resolve_template_token`

and any schema-field docs that reference template usage.

## Rust module split layout convention

When splitting one conductor Rust module into multiple files, use folder-module
layout consistently:

- move `foo.rs` to `foo/mod.rs`,
- place sibling module files in `foo/*.rs`,
- place local unit tests in `foo/tests.rs` with `#[cfg(test)] mod tests;`.

Avoid keeping both `foo.rs` and `foo/mod.rs` for one module and avoid
`#[path = "..."]` for ordinary in-crate module/test placement.

## Process and Builtin Execution Semantics

Execution dispatch is decided per tool:

- Process tool: run `process.command[0]` with resolved arguments from remaining
  command entries.
- Builtin tool: dispatch by builtin name/version.

Guidance:

- Keep builtin dispatch deterministic and explicitly version-gated.
- Builtin runtime logic must live in `src/conductor-builtins/*` crates,
  including `echo`; do not re-implement builtin behavior inline inside
  `src/conductor` runtime code.
- Each builtin crate must expose both:
  - a library API for conductor dispatch, and
  - a standalone binary (`src/main.rs`) so builtin behavior can run
    independently during debugging and validation.
- Builtin crates must share one identical input contract shape:
  - CLI uses standard Rust flags/options and all CLI values are strings,
  - API input uses `BTreeMap<String, String>` args plus optional raw payload
    bytes for content-oriented operations.
  Builtins may optionally define one default CLI option key so one value can be
  provided without spelling the key, but explicit keyed input must remain
  supported and map to the same API key.
  Builtin execution must fail fast on undeclared keys, missing required keys,
  and invalid argument combinations; do not silently ignore mismatches.
  When a builtin's successful non-error result is pure, the success payload
  may be deterministic bytes or `BTreeMap<String, String>`. Impure builtins
  may instead primarily communicate success through side effects. The only
  allowed API-vs-CLI
  difference is argument encoding ergonomics (flag transport on CLI vs key/value
  map in API).
  CLI failures may use ordinary Rust error types; do not coerce failures into
  fake string-only success payloads.
- Builtin crates must use explicit crate versions in their own `Cargo.toml`
  (`version = "..."`) instead of inheriting workspace package version.
- Ensure process execution errors preserve useful stderr context.
- Create an isolated temporary cwd only when a step actually needs to execute.
- The temporary cwd is ad hoc execution scratch space, not a directory tied to
  tool identity or cached instances.
- Materialize merged `tool_content_maps[tool_name]` into that cwd using
  relative paths only, with directory-form keys (`/` or `\\` suffix)
  unpacked as ZIP payloads, including `./` for sandbox-root unpack.
- Reject `content_map` collisions where separate entries would materialize the
  same file path; allow merges when paths are distinct.
- Treat all tool-relative paths (`process.command[0]`, template `:file(...)`, and
  output `capture.file.path`) as relative to that cwd.
- Reject absolute/traversal paths (`..`, rooted/prefixed paths); do not allow
  sandbox escape.
- Keep output capture behavior explicit and per-output.

## Versioned Schema Editing Policy

For config schema files under
`src/conductor/src/model/config/versions/`:

- This repository may intentionally evolve `v1` directly when requested.
- Do not add compatibility shims unless explicitly requested.
- Keep Rust bridge structs synchronized with `.ncl` contracts.
- Keep test fixtures aligned with current schema semantics.

If schema shape changes, update together:

- `v1.ncl`
- `v_latest.rs`
- bridge mappings in `versions/mod.rs`
- runtime model in `config/mod.rs` (if runtime semantics changed)
- affected examples/tests

## Example Policy

Examples live under `src/conductor/examples/`.

- `demo.rs` may generate persistent inspectable artifacts under
  `.artifacts/demo/`.
- `demo.rs` should clear `.artifacts/demo/` before each run so generated
  examples remain deterministic and easy to inspect.
- `demo.rs` should exercise all official builtins (`echo`, `fs`, `import`,
  `export`, `archive`) at least once.
- `demo.rs` should keep generated `conductor.ncl` newcomer-friendly by
  including explicit default grouped runtime storage values as schema fields
  (not comments):
  `conductor_dir = .conductor`, `state_ncl = .conductor/state.ncl`,
  `cas_store_dir = .conductor/store/`.
- When demonstrating filesystem flows in `demo.rs`, prefer compact pipelines
  that keep builtin `import` at the beginning and builtin `export` at the end,
  while minimizing intermediate filesystem-oriented steps.
- `demo.rs` should persist orchestration state snapshots to a file under
  `examples/.artifacts/demo/` and print only the file path (not full state
  JSON payloads) to stdout.
- Non-demo examples should prefer ephemeral behavior unless persistence is
  essential to the teaching goal.
- Keep example tool definitions consistent with current schema invariants.

## Validation Workflow

Prefer workspace cargo aliases from `.cargo/config.toml` where applicable:

- `cargo fmt-check`
- `cargo clippy-all`
- `cargo test-all`

Conductor-focused loop after meaningful edits:

1. `cargo fmt --all`
2. `cargo fmt-check`
3. `cargo test -p mediapm-conductor`
4. `cargo build -p mediapm-conductor --examples`
5. If examples changed, run representative examples (especially `demo`).

## Rust Docstring Expectations

For touched Rust code in this crate:

- Add/refresh `///` or `//!` docs for behavior changes.
- Document invariants, edge cases, and side effects (not just names).
- When behavior depends on configuration merging or schema rules, state that
  explicitly.
- For templating, include supported token forms and failure conditions.

## Change Discipline

- Keep edits scoped and coherent; avoid unrelated refactors.
- Preserve actor/runtime boundaries (`orchestration/` vs `model/`).
- Prefer explicit errors over silent coercion.
- When conflicts are possible, fail with actionable messages including field or
  tool names.
