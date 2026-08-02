# ||| Schema version 2 — Unified conductor config document (v2).
# |||
# ||| This contract validates the Nickel envelope shape that Rust deserializes
# ||| into `v_latest::NickelEnvelopeLatest`. Keep fields and their types in
# ||| sync with the Rust side.
# |||
# ||| Schema version 2 uses a single unified document model (replacing the old
# ||| three-document user/machine/state split from v1).  Tools are tagged by kind
# ||| (builtin/executable), workflows are ordered-step DAGs, and runtime-only
# ||| configuration lives in each tool's `runtime` sub-object.

# ||| Non-empty string contract.
let NonEmptyStringV2 =
  std.contract.from_predicate (fun value =>
    std.string.is_match "^.+$" value
  )
in

# ||| Version marker for this schema.
let VersionTwoV2 = std.contract.from_predicate (fun value => value == 2) in

# ||| Integer-valued number (not float); defined as a predicate because Nickel
# ||| `std.number` contracts coerce — we want to reject non‑integer values.
let IntegerNumberV2 =
  std.contract.from_predicate (fun value =>
    std.is_number value && value == std.number.floor value
  )
in

# ||| Tool input kind contract.
let ToolInputKindV2 =
  std.contract.any_of [
    std.contract.from_predicate (fun value => value == "string"),
    std.contract.from_predicate (fun value => value == "content"),
    std.contract.from_predicate (fun value => value == "env"),
    std.contract.from_predicate (fun value => value == "string_list"),
  ]
in

# ||| One declared tool input spec.
let ToolInputSpecV2 = {
  kind | ToolInputKindV2 | optional,
  required | Bool | optional,
}
in

# ||| Save mode: "false" (do not persist), "true" (persist normally), or "full" (force full persistence).
let SaveModeV2 =
  std.contract.any_of [
    std.contract.from_predicate (fun value => value == "false"),
    std.contract.from_predicate (fun value => value == "true"),
    std.contract.from_predicate (fun value => value == "full"),
  ]
in

# ||| Output capture specification for workflow steps.
let OutputCaptureSpecV2 = {
  name | NonEmptyStringV2,
  capture | NonEmptyStringV2,
  save | SaveModeV2 | optional,
  allow_empty | Bool | optional,
  include_topmost_folder | Bool | optional,
}
in

# ||| Tool content map entry: relative path → CAS hash or inline content.
# ||| Keys ending with `/` unpack ZIP payloads into that directory.
let ToolContentMapV2 = { _ : NonEmptyStringV2 } in

# ||| Runtime-only tool configuration (not part of tool-call identity).
let ToolRuntimeV2 = {
  content_map | ToolContentMapV2 | optional,
  impure | Bool | optional,
  inherited_env_vars | Array NonEmptyStringV2 | optional,
  max_concurrent_calls | IntegerNumberV2 | optional,
  max_retries | IntegerNumberV2 | optional,
}
in

# ||| Platform-specific inherited environment variable names.
# ||| Keys are closed to the three supported platforms (S-A5).
let PlatformInheritedEnvVarsV2 = {
  windows | Array NonEmptyStringV2 | optional,
  linux | Array NonEmptyStringV2 | optional,
  macos | Array NonEmptyStringV2 | optional,
}
in

# ||| Runtime configuration for the conductor itself (not per-tool).
let ConductorRuntimeConfigV2 = {
  retry_impure | Bool | optional,
  platform_inherited_env_vars | PlatformInheritedEnvVarsV2 | optional,
}
in

# ||| Input binding: a single string or an array of strings.
# ||| Used in default_inputs to support both scalar and list defaults.
let InputBindingV2 =
  std.contract.any_of [
    String,
    Array String,
  ]
in

# ||| Builtin tool kind: identity is builtin_id (e.g. "echo@v1").
let BuiltinToolSpecV2 = {
  kind | std.contract.from_predicate (fun value => value == "builtin"),
  name | NonEmptyStringV2,
  builtin_id | NonEmptyStringV2,
  inputs | { _ : ToolInputSpecV2 } | optional,
  default_inputs | { _ : InputBindingV2 } | optional,
  outputs | { _ : OutputCaptureSpecV2 } | optional,
  runtime | ToolRuntimeV2 | optional,
}
in

# ||| Executable tool kind: external command with environment and exit-code policy.
let ExecutableToolSpecV2 = {
  kind | std.contract.from_predicate (fun value => value == "executable"),
  name | NonEmptyStringV2,
  command | Array String,
  env_vars | { _ : String } | optional,
  success_codes | Array IntegerNumberV2 | optional,
  inputs | { _ : ToolInputSpecV2 } | optional,
  default_inputs | { _ : InputBindingV2 } | optional,
  outputs | { _ : OutputCaptureSpecV2 } | optional,
  runtime | ToolRuntimeV2 | optional,
}
in

# ||| One tool definition (tagged union by kind).
let ToolSpecV2 = std.contract.any_of [BuiltinToolSpecV2, ExecutableToolSpecV2] in

# ||| One workflow step (references one tool).
let WorkflowStepSpecV2 = {
  id | NonEmptyStringV2,
  tool | NonEmptyStringV2,
  inputs | { _ : String } | optional,
  outputs | { _ : OutputCaptureSpecV2 } | optional,
  max_retries | IntegerNumberV2 | optional,
  depends_on | Array NonEmptyStringV2 | optional,
}
in

# ||| One workflow definition (ordered steps DAG).
let WorkflowSpecV2 = {
  name | NonEmptyStringV2,
  display_name | NonEmptyStringV2 | optional,
  description | NonEmptyStringV2 | optional,
  impure | Bool | optional,
  steps | Array WorkflowStepSpecV2 | optional,
}
in

# ||| External data content reference: a CAS-addressed blob with
# ||| metadata describing how to persist it.
let ExternalContentRefV2 = {
  hash | NonEmptyStringV2 | optional,
  description | String | optional,
  save
    | std.contract.any_of [
      std.contract.from_predicate (fun value => value == "full"),
      Bool,
    ]
    | optional,
}
in

# ||| Top-level persisted Nickel document envelope for schema version 2.
# ||| Closed record: unknown top-level fields are rejected (S-A1).
let NickelDocumentV2 = {
  version | VersionTwoV2,
  tools | { _ : ToolSpecV2 } | optional,
  workflows | Array WorkflowSpecV2 | optional,
  external_data | { _ : ExternalContentRefV2 } | optional,
  runtime | ConductorRuntimeConfigV2 | optional,
}
in

# ||| v1 → v2:  Convert map-indexed tools/workflows to arrays,
# ||| merge tool_configs into each tool's inline runtime,
# ||| drop v1-only top-level fields (runtime, external_data,
# ||| impure_timestamps, state_pointer).
let migrate_v1_to_v2_fn = fun data =>
  let config_runtime_map =
    if std.record.has_field "runtime" data
      && std.is_record data.runtime
      && std.record.has_field "tool_configs" data.runtime then
      # ||| v1 nests `tool_configs` under `runtime` (not top-level).
      let tc_root = data.runtime.tool_configs in
      std.record.fields tc_root
      |> std.array.fold_left
        (fun acc =>
          fun key =>
            let tc = tc_root."%{key}" in
            acc
            & {
              "%{key}" = {
                default_inputs =
                  if std.record.has_field "input_defaults" tc then
                    tc.input_defaults
                  else
                    {},
                # ||| Partial ToolRuntimeV2. Absent fields are omitted, not
                # ||| set to `null`: explicit `null` is rejected by
                # ||| `| optional` contracts, so omission keeps the migrated
                # ||| document valid against the tightened envelope.
                runtime =
                  (if std.record.has_field "content_map" tc then { content_map = tc.content_map } else {})
                  & (if std.record.has_field "env_vars" tc then { inherited_env_vars = std.record.fields tc.env_vars } else {})
                  & (if std.record.has_field "max_concurrent_calls" tc then { max_concurrent_calls = tc.max_concurrent_calls } else {})
                  & (if std.record.has_field "max_retries" tc then { max_retries = tc.max_retries } else {})
              }
            }
        )
        {}
    else
      {}
  in
  let tools_v2 =
    if std.record.has_field "tools" data then
      std.record.fields data.tools
      |> std.array.fold_left
        (fun acc =>
          fun key =>
            let t = data.tools."%{key}" in
            let kind = if std.record.has_field "kind" t then t.kind else "executable" in
            let config_rt = std.record.get_or "%{key}" null config_runtime_map in
            let spec =
              if kind == "builtin" then
                # BuiltinToolSpecV1 — already has name + version
                {
                  kind = "builtin",
                  name = key,
                  builtin_id = t.builtin_id,
                  inputs =
                    if std.record.has_field "inputs" t then
                      t.inputs
                      |> std.record.map (fun ik =>
                        fun iv =>
                          (if std.record.has_field "kind" iv then { kind = iv.kind } else { kind = "string" })
                          & (if std.record.has_field "required" iv then { required = iv.required } else {})
                      )
                    else
                      {},
                  default_inputs = if config_rt == null then {} else (config_rt.default_inputs | { _ : InputBindingV2 }),
                  runtime = if config_rt == null then {} else config_rt.runtime,
                }
              else
                # ExecutableToolSpecV1 — use map key as name
                let cmd_arr = if std.record.has_field "command" t then t.command else [] in
                let base_env_vars = if std.record.has_field "env_vars" t then t.env_vars else {} in
                let base_success_codes = if std.record.has_field "success_codes" t then t.success_codes else [] in
                {
                  kind = "executable",
                  name = key,
                  command = cmd_arr,
                  env_vars = base_env_vars,
                  success_codes = base_success_codes,
                  inputs =
                    if std.record.has_field "inputs" t then
                      t.inputs
                      |> std.record.map (fun ik =>
                        fun iv =>
                          (if std.record.has_field "kind" iv then { kind = iv.kind } else { kind = "string" })
                          & (if std.record.has_field "required" iv then { required = iv.required } else {})
                      )
                    else
                      {},
                  default_inputs = if config_rt == null then {} else (config_rt.default_inputs | { _ : InputBindingV2 }),
                  runtime =
                    { impure = if std.record.has_field "is_impure" t then (t.is_impure | Bool) else false }
                    & (if config_rt == null then {} else config_rt.runtime),
                }
            in
            acc & { "%{key}" = spec }
        )
        {}
    else
      {}
  in
  let workflows_v2 =
    if std.record.has_field "workflows" data then
      std.record.fields data.workflows
      |> std.array.map (fun key =>
        let w = data.workflows."%{key}" in
        let w_name = if std.record.has_field "name" w then w.name else key in
        { name = w_name }
        & (if std.record.has_field "description" w then { description = w.description } else {})
        & {
            steps =
              if std.record.has_field "steps" w then
                w.steps
                |> std.array.map (fun step =>
                  let mapped_outputs =
                    if std.record.has_field "outputs" step then
                      std.record.fields step.outputs
                      |> std.array.fold_left
                        (fun acc =>
                          fun oname =>
                            acc & { "%{oname}" = { name = oname, capture = oname } }
                        )
                        {}
                    else
                      {}
                  in
                  {
                    id = step.id,
                    tool = step.tool,
                    inputs =
                      if std.record.has_field "inputs" step then
                        step.inputs
                        |> std.record.map (fun ik =>
                          fun iv =>
                            if std.is_string iv then iv else ""
                        )
                      else
                        {},
                    outputs = mapped_outputs,
                    depends_on = if std.record.has_field "depends_on" step then step.depends_on else [],
                  }
                )
              else
                [],
          }
      )
    else
      []
  in
  let runtime_v2 =
    if std.record.has_field "runtime" data then
      let rt = data.runtime in
      (if std.record.has_field "retry_impure" rt then { retry_impure = rt.retry_impure | Bool } else {})
      & (if std.record.has_field "inherited_env_vars" rt && std.is_record rt.inherited_env_vars then
          # ||| Platform keys are closed (S-A5): carry known platforms through
          # ||| and drop unknown keys the closed v2 contract would reject.
          {
            platform_inherited_env_vars =
              (if std.record.has_field "windows" rt.inherited_env_vars then { windows = rt.inherited_env_vars.windows } else {})
              & (if std.record.has_field "linux" rt.inherited_env_vars then { linux = rt.inherited_env_vars.linux } else {})
              & (if std.record.has_field "macos" rt.inherited_env_vars then { macos = rt.inherited_env_vars.macos } else {})
          }
        else
          {})
    else
      {}
  in
  let external_data_v2 =
    if std.record.has_field "external_data" data then
      data.external_data
    else
      {}
  in
  {
    version = 2,
    tools = tools_v2,
    workflows = workflows_v2,
    external_data = external_data_v2,
    runtime = runtime_v2,
  }
in

{
  # ||| Validates one Nickel value as schema v2.
  validate_document_v2 = fun data => data | NickelDocumentV2,

  # ||| Exposes the v2 envelope contract directly for Rust bridge inspection.
  envelope_contract_v2 = NickelDocumentV2,

  # ||| Migrates a v1 document to schema v2.
  migrate_v1_to_v2 = migrate_v1_to_v2_fn,
}
