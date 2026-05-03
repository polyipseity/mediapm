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

## Orchestration contract

- Keep conductor as a functional orchestration engine over CAS:
  deterministic planning/keying in pure logic, with process/filesystem effects
  isolated to execution boundaries.
- Keep user intent and machine-managed runtime state separated across
  `conductor.ncl` and `conductor.machine.ncl`; unresolved non-mergeable
  conflicts fail fast and require manual resolution.
- Runtime orchestration remains async + actor-oriented (`ractor`) with explicit
  message contracts and predictable supervision behavior.

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
- volatile state path (`conductor_state_config`): `<conductor_dir>/state.ncl`
- filesystem CAS store (`cas_store_dir`): `<conductor_dir>/store`
- schema export directory: `<conductor_dir>/config/conductor`

Schema export behavior contract:

- both CLI workflow entrypoints and API workflow execution must export
  conductor schemas to `<conductor_dir>/config/conductor` before runtime
  execution continues.

Document contract:

- `conductor.ncl` is treated as user-edited input and is not machine-mutated.
- `conductor.machine.ncl` stores machine-managed setup/config declarations.
- `conductor.ncl` and `conductor.machine.ncl` may define grouped runtime
  storage fields under one `runtime` record:
  `runtime.conductor_dir`, `runtime.conductor_state_config`,
  `runtime.cas_store_dir`, and optional platform-keyed inherited host
  env-name map `runtime.inherited_env_vars`. The `cas_store_dir` field accepts any CAS
  locator string (filesystem path or URL).
- Runtime inherited env-name defaults are host-specific (`SYSTEMROOT`,
  `WINDIR`, `TEMP`, `TMP` on Windows; empty list elsewhere) and merge user,
  machine, and invocation-option values with case-insensitive de-duplication.
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

Dual-file ownership model summary:

- `conductor.ncl` is human-owned intent and workflow/tool declarations.
- `conductor.machine.ncl` is machine-owned operational state such as content
  maps and machine-derived runtime metadata.
- Conductor embeds Nickel evaluation in-process (`nickel-lang-core`) and does
  not delegate schema evaluation to an out-of-process secondary interpreter.

## Conductor Builtin Tool Strategy

This repository follows the design principle that conductor builtins are the
"connective tissue" for bootstrapping and cross-platform consistency. The
official baseline set is:

- `echo` (reference pure pass-through runtime contract)
- `fs` (rooted filesystem staging operations: ensure_dir, write_text, copy)
- `import` (`kind=file|folder|fetch` ingestion to pure bytes)
- `export` (`kind=file|folder` filesystem materialization)
- `archive` (pure ZIP-only pack/unpack/repack transforms)

All other domain logic remains external tooling or mediapm workflow behavior.

For portable string-manipulation tasks in workflows, prefer provisioning
`sd` via conductor tool preset import (`import tool --preset sd`) instead of
platform-specific shell tools (`sed`, PowerShell regex one-offs, etc.).
Use `sd` for deterministic text rewrites where possible so workflow behavior
stays consistent across Windows/Linux/macOS runners.

Common executable tool presets must use one module file per preset under
`src/conductor/src/tools/` (for example `tools/sd.rs`) with registry/dispatch
kept in `tools/mod.rs`; avoid re-centralizing preset implementation logic in
`api.rs`.

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
   executable tool inputs; missing required inputs are errors unless
   `tool_configs.<tool>.input_defaults` provides the input. Tool-level
   defaults under `tools.<tool>.inputs.<input>.default` are unsupported.

7. For builtin tools, step inputs are pass-through bindings and builtin crates
   enforce their own strict argument/input contracts.

8. Workflow-step input bindings are typed call-site values:
   scalar `string` or `string_list` (list-of-strings). Both forms support
   `${...}` interpolation with expression forms

   `${external_data.<hash>}` and `${step_output.<step_id>.<output_name>}`;
   list bindings apply interpolation per item. Input-binding interpolation is
   text-oriented and does not support materialization directives such as
   `:file(...)` or `:folder(...)`; unsupported `${...}` expressions are
   invalid.

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
    every referenced hash must be rooted in top-level `external_data`; absolute
    and escaping paths are invalid.

15. `tool_configs.<tool>.description` is optional human-facing metadata only;

    it must not affect instance identity, scheduler behavior, or cache keys.

16. Workflow `name` and `description` are optional human-facing metadata only;

    workflow identity remains the workflow map key and runtime behavior/cache
    keys must not depend on those fields.

17. Cache rematerialization checks are scoped to outputs actually referenced by
    `${step_output...}` workflow-step inputs; missing unreferenced outputs do
    not force rerun for otherwise cache-hit instances.

18. Keep step-output references minimal so independent steps can remain in
    parallelizable topological levels.

19. Builtin `import`/`export` path semantics for `kind=file|folder` are:
    `path_mode` defaults to `relative` and resolves `path` against the
    outermost config directory, `relative` paths must not escape that root,
    and `path_mode=absolute` requires an explicit absolute `path`.

20. Orchestration-state snapshots must include explicit top-level `version`.

21. `ToolCallInstance.metadata` is persistence-normalized: executable metadata
    remains `ToolSpec`-shape, while builtin metadata persists only
    `kind`/`name`/`version`; `impure_timestamp` belongs at instance top-level,
    not inside metadata. Decode must reject extra builtin metadata fields.

22. `ToolCallInstance.inputs` persist CAS hash references (no inline
    `plain_content` payload and no separate `source_hash` provenance field).

23. For duplicate equivalent tool-call instances merged under one instance key,
    persisted output `persistence` must be the effective merged policy across all
    callers (`save`: logical AND, `force_full`: logical OR).

    Merge rationale:
    - `save` remains enabled unless every equivalent caller opts out,
    - `force_full` remains enabled if any equivalent caller requires full-data
      persistence.

24. Any human-facing orchestration-state JSON output (for example CLI `state`
    output or demo artifacts) must render the persisted wire-envelope shape so
    builtin metadata stays strict (`kind`/`name`/`version`) and does not leak
    runtime-only optional fields.

25. If a cached referenced `${step_output...}` payload fails CAS integrity

    checks, conductor may auto-recover only for pure workflows by warning,
    dropping affected cached instances, deleting the corrupt hash, and retrying
    the workflow once. Impure workflows must fail without auto-retry.

26. `tool_configs.<tool>.max_retries` controls per-tool outer retry budget

    after the initial failed call. Valid values are `-1` (use runtime default)
    or non-negative integers. Runtime unified execution normalizes `-1` to the
    current default retry policy.

27. Newly captured output references must initialize persistence from the
    resolved output specification policy before equivalent-call merge logic is
    applied; do not seed new output entries with unconditional saved defaults.

Instance-key rationale to preserve:

- Equivalent-call dedup identity excludes tool content-map payload details and
  excludes merged persistence flags so metadata/content-map churn does not
  invalidate logically equivalent historical executions.

## Reverse-diff optimization intent

- Preserve conductor-to-CAS optimization hints that bias storage so frequently
  consumed outputs remain fast-access roots while related inputs may be stored
  as diffs when safe.
- Constraint patch planning must skip the CAS empty-content root identity so
  optimization does not emit invalid reverse-diff constraint updates.

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
- `${*inputs.<name>}`
  - Standalone executable command-argument unpack token.
  - The token must occupy the full command argument entry.
  - Runtime expands list inputs into one argv entry per list item.
  - Scalar inputs expand into one argv entry when non-empty.
- `${*<condition> ? <true> | <false>}`
  - Standalone executable command-argument conditional unpack token.
  - The token must occupy the full command argument entry.
  - Runtime evaluates one conditional expression and emits one argv entry when
    the selected branch renders non-empty.
- `${<selector>:file(<relative_path>)}`
  - Uses one selector form above, queues bytes for `<relative_path>`, then
    injects that path string.
- `${inputs.<name>:file(<relative_path>)}`
  - Queues input bytes for `<relative_path>` under an ad hoc temporary
    execution directory, then injects that relative path string.
- `${context.os}`
  - Injects host platform text (`windows`, `linux`, or `macos`).
- `${context.working_directory}`
  - Injects the current process working directory as a text path string.
- `${<left> <op> <right> ? <true> | <false>}`
  - Comparison conditional with operators `==`, `!=`, `<`, `<=`, `>`, `>=`.
  - Branch values resolve recursively and support
    selector/materialization special forms (for example
    `inputs.payload:file(payload.txt)`).
- `${<operand> ? <true> | <false>}` / `${!<operand> ? <true> | <false>}`
  - Truthiness conditional where non-empty scalar values and non-empty list
    values are truthy.
- `${<expr1> && <expr2> ? <true> | <false>}` / `${<expr1> || <expr2> ? <true> | <false>}`
  - Logical-and (`&&`) and logical-or (`||`) combine sub-conditions.
  - `&&` binds tighter than `||` (standard precedence).
  - Parentheses group sub-conditions: `${(<expr1> || <expr2>) && <expr3> ? <true> | <false>}`.
  - A leading `!` negates a primary: `${!(a == "x") ? <true> | <false>}`.
  - The branch separator `|` is always distinct from `||`: `||` inside the
    condition is consumed by the recursive-descent parser; a lone `|` outside
    any depth-tracked delimiter ends the condition and begins the false branch.
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
- List-typed inputs are invalid in normal `${...}` interpolation and are only
  valid in standalone unpack tokens (`${*...}`) inside executable command
  argument arrays.
- `${...` without a closing `}` fails workflow resolution.
- Unsupported/trailing escape sequences fail workflow resolution.
- Malformed `:file(...)` tokens (for example missing closing `)`) fail workflow
  resolution.
- `path_regex` template literals should avoid raw bracket escapes (`\[`/`\]`);
  prefer regex-safe literals such as `\x5B` and `\x5D` when matching
  bracketed markers.
- Conditional branches that include literal `?` or `|` content must quote that
  content as a string (for example `"2:v:0?"`) so parser control tokens are
  not misinterpreted.

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
- Guard external executable subprocesses with a bounded timeout (default
  `900` seconds) so stuck child processes cannot stall worker actors forever;
  allow explicit operator override via
  `MEDIAPM_CONDUCTOR_EXECUTABLE_TIMEOUT_SECS`.
- Execute external tools with stdin disconnected (`Stdio::null`) so accidental
  interactive prompts cannot block worker actors indefinitely.
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
- `capture.kind = "file"|"folder"` stays path-template based;
  `capture.kind = "file_regex"|"folder_regex"` evaluates regex against
  normalized sandbox-relative paths (`/` separators on all hosts).
- Regex file capture must resolve to exactly one file; zero or multiple
  matches are workflow errors.
- Regex folder capture (`folder_regex`) may resolve zero to many paths;
  zero matches are valid.
- `folder_regex` capture rename expansions (capture-group based) must remain
  deterministic and fail fast on post-rename path collisions.

## Versioned Schema Editing Policy

For config schema files under
`src/conductor/src/model/config/versions/`:

- This repository may intentionally evolve `v1` directly when requested.
- Do not add compatibility shims unless explicitly requested.
- Keep Rust bridge structs synchronized with `.ncl` contracts.
- Keep unversioned/latest Nickel contract aliases (`validate_document` and
  `envelope_contract`) in `mod.ncl`; versioned files (`vN.ncl`) should expose
  only version-suffixed contracts (`validate_document_vN`,
  `envelope_contract_vN`).
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
  `conductor_dir = .conductor`, `conductor_state_config = .conductor/state.ncl`,
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

**For development:** Use targeted cargo aliases from `.cargo/config.toml`:

- `cargo test-pkg mediapm-conductor` — test only conductor crate
- `cargo clippy-pkg mediapm-conductor` — lint only conductor crate
- `cargo fmt-check` — check formatting on all files

Conductor-focused development loop after meaningful edits:

1. `cargo fmt --all`
2. `cargo fmt-check`
3. `cargo test-pkg mediapm-conductor`
4. `cargo clippy-pkg mediapm-conductor`
5. `cargo build-pkg mediapm-conductor --all-targets --all-features`
6. If examples changed, run representative examples (especially `demo`).

**Before submitting (pre-push):** Run full workspace validation:

- `cargo fmt-check`
- `cargo clippy-all`
- `cargo test-all`

See `.cargo/config.toml` for all available validation aliases and shortcuts.

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
