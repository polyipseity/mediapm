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
  description | String | optional,
  required | Bool | optional,
}
in

# ||| Output capture specification for workflow steps.
let OutputCaptureSpecV2 = {
  name | NonEmptyStringV2,
  capture | NonEmptyStringV2,
}
in

# ||| Tool content map entry: relative path → CAS hash or inline content.
# ||| Keys ending with `/` unpack ZIP payloads into that directory.
let ToolContentMapV2 = { _ : NonEmptyStringV2 } in

# ||| Runtime-only tool configuration (not part of tool-call identity).
let ToolRuntimeV2 = {
  content_map | ToolContentMapV2 | optional,
  impure | Bool | optional,
  inherited_env_vars | { _ : String } | optional,
  max_concurrent_calls | IntegerNumberV2 | optional,
  max_retries | IntegerNumberV2 | optional,
}
in

# ||| Runtime configuration for the conductor itself (not per-tool).
let ConductorRuntimeConfigV2 = {
  retry_impure | Bool | optional,
  platform_inherited_env_var_names | { _ : Array String } | optional,
}
in

# ||| Builtin tool kind: identity is (name, version).
let BuiltinToolSpecV2 = {
  kind | std.contract.from_predicate (fun value => value == "builtin"),
  name | NonEmptyStringV2,
  version | NonEmptyStringV2,
  inputs | { _ : ToolInputSpecV2 } | optional,
  default_inputs | { _ : String } | optional,
  outputs | { _ : OutputCaptureSpecV2 } | optional,
  runtime | ToolRuntimeV2 | optional,
}
in

# ||| Executable tool kind: external command with environment and exit-code policy.
let ExecutableToolSpecV2 = {
  kind | std.contract.from_predicate (fun value => value == "executable"),
  name | NonEmptyStringV2,
  version | NonEmptyStringV2,
  command | Array String,
  env_vars | { _ : String } | optional,
  success_codes | Array IntegerNumberV2 | optional,
  inputs | { _ : ToolInputSpecV2 } | optional,
  default_inputs | { _ : String } | optional,
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
  display_name | String | optional,
  description | String | optional,
  impure | Bool | optional,
  steps | Array WorkflowStepSpecV2 | optional,
}
in

# ||| External data content reference: a CAS-addressed blob with
# ||| metadata describing how to persist it.
let ExternalContentRefV2 = {
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
let NickelDocumentV2 = {
  version | VersionTwoV2,
  tools | { _ : ToolSpecV2 } | optional,
  workflows | Array WorkflowSpecV2 | optional,
  external_data | { _ : ExternalContentRefV2 } | optional,
  runtime | ConductorRuntimeConfigV2,
}
in

# ||| v1 → v2:  Convert map-indexed tools/workflows to arrays,
# ||| merge tool_configs into each tool's inline runtime,
# ||| drop v1-only top-level fields (runtime, external_data,
# ||| impure_timestamps, state_pointer).
let migrate_v1_to_v2 = fun data =>
  let config_runtime_map =
    if std.record.has_field "tool_configs" data then
      std.record.fields data.tool_configs
      |> std.array.fold_left
        (fun acc =>
          fun key =>
            let tc = data.tool_configs."%{key}" in
            acc
            & {
              "%{key}" = {
                content_map =
                  if std.record.has_field "content_map" tc then tc.content_map else null,
                default_inputs =
                  if std.record.has_field "input_defaults" tc then tc.input_defaults else null,
                inherited_env_vars =
                  if std.record.has_field "env_vars" tc then tc.env_vars else null,
                max_concurrent_calls =
                  if std.record.has_field "max_concurrent_calls" tc then tc.max_concurrent_calls else null,
                max_retries =
                  if std.record.has_field "max_retries" tc then tc.max_retries else null,
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
                  name = t.name,
                  version = t.version,
                  inputs =
                    if std.record.has_field "inputs" t then
                      t.inputs
                      |> std.record.map (fun ik =>
                        fun iv =>
                          if std.record.has_field "kind" iv then
                            { kind = iv.kind, description = iv.description, required = iv.required }
                          else
                            { kind = "string", description = iv.description, required = iv.required }
                      )
                    else
                      {},
                  default_inputs = if config_rt == null then {} else (config_rt.default_inputs | { _ : String }),
                  runtime =
                    if config_rt == null then
                      null
                    else
                      {
                        content_map = config_rt.content_map,
                        impure = null,
                        inherited_env_vars = config_rt.inherited_env_vars,
                        max_concurrent_calls = config_rt.max_concurrent_calls,
                        max_retries = config_rt.max_retries,
                      },
                }
              else
                # ExecutableToolSpecV1 — use map key as name
                let cmd_arr = if std.record.has_field "command" t then t.command else [] in
                let base_env_vars = if std.record.has_field "env_vars" t then t.env_vars else {} in
                let base_success_codes = if std.record.has_field "success_codes" t then t.success_codes else [] in
                {
                  kind = "executable",
                  name = key,
                  version = "",
                  command = cmd_arr,
                  env_vars = base_env_vars,
                  success_codes = base_success_codes,
                  inputs =
                    if std.record.has_field "inputs" t then
                      t.inputs
                      |> std.record.map (fun ik =>
                        fun iv =>
                          if std.record.has_field "kind" iv then
                            { kind = iv.kind, description = iv.description, required = iv.required }
                          else
                            { kind = "string", description = iv.description, required = iv.required }
                      )
                    else
                      {},
                  default_inputs = if config_rt == null then {} else (config_rt.default_inputs | { _ : String }),
                  runtime = {
                    impure = if std.record.has_field "is_impure" t then (t.is_impure | Bool) else false,
                    content_map = if config_rt == null then null else config_rt.content_map,
                    inherited_env_vars = if config_rt == null then null else config_rt.inherited_env_vars,
                    max_concurrent_calls = if config_rt == null then null else config_rt.max_concurrent_calls,
                    max_retries = if config_rt == null then null else config_rt.max_retries,
                  },
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
        {
          name = w_name,
          display_name = null,
          description = if std.record.has_field "description" w then w.description else null,
          impure = null,
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
                  max_retries = null,
                  depends_on = if std.record.has_field "depends_on" step then step.depends_on else null,
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
      {
        retry_impure = if std.record.has_field "retry_impure" rt then (rt.retry_impure | Bool) else null,
        platform_inherited_env_var_names =
          if std.record.has_field "inherited_env_vars" rt then rt.inherited_env_vars else null,
      }
    else
      {}
  in
  let external_data_v2 =
    if std.record.has_field "external_data" data then
      data.external_data
    else
      null
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
  migrate_v1_to_v2,
}
