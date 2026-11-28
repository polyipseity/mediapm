# ||| Schema version 6 — Unified conductor config document (v6).
# |||
# ||| This contract validates the Nickel envelope shape that Rust deserializes
# ||| into `v_latest::NickelEnvelopeLatest`. Keep fields and their types in
# ||| sync with the Rust side.
# |||
# ||| Schema version 6 makes runtime policy explicit: tool concurrency is
# ||| `null` (unlimited) or a positive integer, tool and step retries are
# ||| non-negative integers with `null` meaning "inherit the tool", and the
# ||| step `runtime` object is the only place a step may override retries.
# ||| The v5 `runtime.retry_impure` flag is removed; retries are bounded by
# ||| `max_retries` and a failed attempt moves no state pointer.

# ||| Non-empty string contract.
# ||| A non-empty string, with a diagnostic that survives migration.
# |||
# ||| The migration wrapper rebuilds the document, so a rejected value no longer
# ||| carries the span it was written at; a `from_predicate` failure would print
# ||| the value alone with no field name. The custom contract states the reason
# ||| itself, and the record contract around it names the field.
let NonEmptyStringV6 =
  std.contract.custom (fun label value =>
    if !std.is_string value then
      'Error { message = "must be a string, not %{std.to_string value}" }
    else if value == "" then
      'Error { message = "must not be empty" }
    else
      'Ok value
  )
in

# ||| Removes a field when the record has one, and leaves it alone otherwise.
# |||
# ||| `std.record.remove` requires the field to exist, and a document is free to
# ||| omit `tools` entirely.
let drop_field = fun field record =>
  if std.record.has_field field record then std.record.remove field record else record
in

# ||| Version marker for this schema.
let VersionSixV6 = std.contract.from_predicate (fun value => value == 6) in

# ||| Integer-valued number (not float); defined as a predicate because Nickel
# ||| `std.number` contracts coerce — we want to reject non‑integer values.
let IntegerNumberV6 =
  std.contract.from_predicate (fun value =>
    std.is_number value && value == std.number.floor value
  )
in

# ||| Nullable count contract: `null` means "unlimited" or "inherit".
let NullableCountV6 = std.contract.any_of [
  std.contract.from_predicate (fun value => value == null),
  std.contract.from_predicate (fun value =>
    std.is_number value && value == std.number.floor value && value >= 1
  ),
]
in

# ||| Retry count contract: a non-negative integer, `null` meaning "inherit".
let NullableRetriesV6 = std.contract.any_of [
  std.contract.from_predicate (fun value => value == null),
  std.contract.from_predicate (fun value =>
    std.is_number value && value == std.number.floor value && value >= 0
  ),
]
in

# ||| Tool input kind contract.
let ToolInputKindV6 =
  std.contract.any_of [
    std.contract.from_predicate (fun value => value == "string"),
    std.contract.from_predicate (fun value => value == "content"),
    std.contract.from_predicate (fun value => value == "env"),
    std.contract.from_predicate (fun value => value == "string_list"),
  ]
in

# ||| Payload staging strategy: how a step obtains the payload it executes from.
# ||| `in_place` runs the command under the provisioned payload directory,
# ||| `hard_link` links that tree into the step sandbox, and `copy` writes a
# ||| copy of the content map into the sandbox.
# |||
# ||| This is the set the document-level default is drawn from: a default has
# ||| nothing to inherit from, so the contract keeps `inherit` out of it.
let PayloadStagingV6 =
  std.contract.any_of [
    std.contract.from_predicate (fun value => value == "in_place"),
    std.contract.from_predicate (fun value => value == "hard_link"),
    std.contract.from_predicate (fun value => value == "copy"),
  ]
in

# ||| A tool's staging choice: a strategy, or `inherit` for the document's
# ||| default.
# |||
# ||| Absent means `inherit`, and the runtime resolves the choice against
# ||| `ConductorRuntimeConfigV6.payload_staging` before any step runs, so
# ||| `inherit` never reaches an executor.
let ConfigPayloadStagingV6 =
  std.contract.any_of [
    std.contract.from_predicate (fun value => value == "inherit"),
    PayloadStagingV6,
  ]
in

# ||| One declared tool input spec.
let ToolInputSpecV6 = {
  kind | ToolInputKindV6 | optional,
  required | Bool | optional,
}
in

# ||| Save mode: "false" (do not persist), "true" (persist normally), or "full" (force full persistence).
let SaveModeV6 =
  std.contract.any_of [
    std.contract.from_predicate (fun value => value == "false"),
    std.contract.from_predicate (fun value => value == "true"),
    std.contract.from_predicate (fun value => value == "full"),
  ]
in

# ||| Output capture specification for workflow steps.
let OutputCaptureSpecV6 = {
  name | NonEmptyStringV6,
  capture | NonEmptyStringV6,
  save | SaveModeV6 | optional,
  allow_empty | Bool | optional,
}
in

# ||| Tool content map entry: relative path → CAS hash or inline content.
# ||| Keys ending with `/` unpack ZIP payloads into that directory.
let ToolContentMapV6 = { _ : NonEmptyStringV6 } in

# ||| Runtime-only tool configuration (not part of tool-call identity).
let ToolRuntimeV6 = {
  content_map | ToolContentMapV6 | optional,
  impure | Bool | optional,
  inherited_env_vars | Array NonEmptyStringV6 | optional,
  max_concurrent_calls | NullableCountV6 | optional,
  max_retries | NullableRetriesV6 | optional,
  # ||| Absolute path of the directory holding this tool's provisioned payload
  # ||| tree. Absent means the document names none.
  payload_root | String | optional,
  # ||| How this tool asks for its payload to be staged. Absent means
  # ||| `inherit`, which takes the document's default; `in_place` and
  # ||| `hard_link` need `payload_root`.
  payload_staging | ConfigPayloadStagingV6 | optional,
}
in

# ||| Platform-specific inherited environment variable names.
# ||| Keys are closed to the three supported platforms (S-A5).
let PlatformInheritedEnvVarsV6 = {
  windows | Array NonEmptyStringV6 | optional,
  linux | Array NonEmptyStringV6 | optional,
  macos | Array NonEmptyStringV6 | optional,
}
in

# ||| Conductor platform environment configuration (nested under `environment`).
let RuntimePlatformEnvConfigV6 = {
  platform_inherited_env_vars | PlatformInheritedEnvVarsV6 | optional,
}
in

# ||| Conductor-owned secret policy: booleans only, never secret material.
# ||| Both fields default to true; a partial record leaves the unstated one true.
let InstanceKeySecretConfigV6 = {
  use_secret | std.contract.from_predicate (fun value => std.is_bool value) | optional,
  require_generated_format | std.contract.from_predicate (fun value => std.is_bool value) | optional,
}
in

# ||| Runtime configuration for the conductor itself (not per-tool).
let ConductorRuntimeConfigV6 = {
  environment | RuntimePlatformEnvConfigV6 | optional,
  platform_inherited_env_vars | PlatformInheritedEnvVarsV6 | optional,
  # ||| How a step obtains the payload it executes from when the tool names no
  # ||| strategy of its own. Absent means `in_place`.
  payload_staging | PayloadStagingV6 | optional,
  instance_key_secret | InstanceKeySecretConfigV6 | optional,
}
in

# ||| Input binding: a single string or an array of strings.
# ||| Used in default_inputs to support both scalar and list defaults.
let InputBindingV6 =
  std.contract.any_of [
    String,
    Array String,
  ]
in

# ||| Builtin tool kind: identity is builtin_id (e.g. "echo@v1").
let BuiltinToolSpecV6 = {
  kind | std.contract.from_predicate (fun value => value == "builtin"),
  name | NonEmptyStringV6,
  # ||| Human-readable tool version, shown on progress rows. Absent means the
  # ||| document makes no version claim, so nothing is rendered.
  version | NonEmptyStringV6 | optional,
  builtin_id | NonEmptyStringV6,
  inputs | { _ : ToolInputSpecV6 } | optional,
  default_inputs | { _ : InputBindingV6 } | optional,
  outputs | { _ : OutputCaptureSpecV6 } | optional,
  runtime | ToolRuntimeV6 | optional,
}
in

# ||| Executable tool kind: external command with environment and exit-code policy.
let ExecutableToolSpecV6 = {
  kind | std.contract.from_predicate (fun value => value == "executable"),
  name | NonEmptyStringV6,
  # ||| Human-readable tool version, shown on progress rows. Absent means the
  # ||| document makes no version claim, so nothing is rendered.
  version | NonEmptyStringV6 | optional,
  command | Array String,
  env_vars | { _ : String } | optional,
  success_codes | Array IntegerNumberV6 | optional,
  inputs | { _ : ToolInputSpecV6 } | optional,
  default_inputs | { _ : InputBindingV6 } | optional,
  outputs | { _ : OutputCaptureSpecV6 } | optional,
  runtime | ToolRuntimeV6 | optional,
}
in

# ||| One tool definition (tagged union by kind).
let ToolSpecV6 = std.contract.any_of [BuiltinToolSpecV6, ExecutableToolSpecV6] in

# ||| What a step may override about its tool's runtime. Impurity and the
# ||| concurrency cap are absent on purpose: the closed record is what makes
# ||| stating them a load error.
let StepRuntimeV6 = {
  max_retries | NullableRetriesV6 | optional,
}
in

# ||| One workflow step (references one tool).
let WorkflowStepSpecV6 = {
  id | NonEmptyStringV6,
  tool | NonEmptyStringV6,
  inputs | { _ : String } | optional,
  outputs | { _ : OutputCaptureSpecV6 } | optional,
  runtime | StepRuntimeV6 | optional,
  depends_on | Array NonEmptyStringV6 | optional,
}
in

# ||| One workflow definition (ordered steps DAG).
let WorkflowSpecV6 = {
  name | NonEmptyStringV6,
  display_name | NonEmptyStringV6 | optional,
  description | NonEmptyStringV6 | optional,
  impure | Bool | optional,
  steps | Array WorkflowStepSpecV6 | optional,
}
in

# ||| External data content reference: a CAS-addressed blob with
# ||| metadata describing how to persist it.
let ExternalContentRefV6 = {
  hash | NonEmptyStringV6 | optional,
  description | String | optional,
  save
    | std.contract.any_of [
      std.contract.from_predicate (fun value => value == "full"),
      Bool,
    ]
    | optional,
}
in

# ||| Rejects a builtin tool spec that declares `env_vars`, with a message
# ||| naming the tool's dictionary key and the field.
# |||
# ||| A builtin has no literal environment surface, so a non-empty
# ||| `tool_configs.<tool>.env_vars` entry or a tools-side
# ||| `tools.<tool>.env_vars` declaration cannot take effect: the v1-to-v2
# ||| migration carries both surfaces onto the tool spec (v2.ncl) and this
# ||| validator fails the load here instead of letting either be dropped in
# ||| silence.
# ||| It runs over the `tools` dictionary before the closed record contract,
# ||| so the message can quote the tool's key where the record contract's
# ||| `any_of` failure cannot (a migrated record has no source span to name
# ||| it from). `std.contract.from_validator` rather than `from_predicate`
# ||| because only a validator customises the error text (nickel-lang-core
# ||| 0.18.0, `stdlib/std.ncl`).
let RejectBuiltinEnvVarsV6 =
  std.contract.from_validator (fun tools =>
    if std.typeof tools != 'Record then
      # Not a dictionary: the contract below reports the shape failure.
      'Ok
    else
      std.array.fold_left
        (fun acc =>
          fun key =>
            if acc != 'Ok then
              acc
            else
              let spec = tools."%{key}" in
              if std.typeof spec == 'Record
                 && std.record.has_field "kind" spec
                 && spec.kind == "builtin"
                 && std.record.has_field "env_vars" spec
              then
                'Error {
                  message = "tool `%{key}` is a builtin and declares `env_vars`",
                  notes = [
                    "A builtin has no literal environment surface: remove `env_vars` from this tool's configuration, or make the tool executable.",
                  ],
                }
              else
                'Ok)
        'Ok
        (std.record.fields tools))
in

# ||| Top-level persisted Nickel document envelope for schema version 6.
# ||| Closed record: unknown top-level fields are rejected (S-A1).
let NickelDocumentV6 = {
  version | VersionSixV6,
  tools | std.contract.Sequence [RejectBuiltinEnvVarsV6, { _ : ToolSpecV6 }] | optional,
  workflows | Array WorkflowSpecV6 | optional,
  external_data | { _ : ExternalContentRefV6 } | optional,
  runtime | ConductorRuntimeConfigV6 | optional,
}
in

# ||| Splits a capture token on the first `:` and returns { prefix, remainder }.
# ||| If there is no `:`, remainder is "".
let split_capture_token = fun token =>
  let parts = std.string.split ":" token in
  if std.array.length parts <= 1 then
    { prefix = token, remainder = "" }
  else
    {
      prefix = std.array.first parts,
      remainder = std.string.join ":" (std.array.drop_first parts),
    }
in

# ||| Migrates one v4 output capture spec to v5.
# |||
# ||| `folder`/`folder_regex` become `folder_flat`/`folder_flat_regex` when
# ||| `include_topmost_folder` is explicitly `false`; `true` or absent keeps the
# ||| original kind. The boolean is always dropped.
let migrate_output_capture_spec_v4_to_v5 = fun spec =>
  let include_topmost =
    if std.record.has_field "include_topmost_folder" spec then
      spec.include_topmost_folder
    else
      true
  in
  let capture = spec.capture in
  let parts = split_capture_token capture in
  let capture_v5 =
    if parts.prefix == "folder" && include_topmost == false then
      "folder_flat:%{parts.remainder}"
    else if parts.prefix == "folder_regex" && include_topmost == false then
      "folder_flat_regex:%{parts.remainder}"
    else
      capture
  in
  (drop_field "include_topmost_folder" (drop_field "capture" spec))
  & { capture = capture_v5 }
in

# ||| Migrates a map of output capture specs from v4 to v5.
let migrate_output_capture_specs_v4_to_v5 = fun outputs =>
  outputs |> std.record.map (fun _name => fun spec => migrate_output_capture_spec_v4_to_v5 spec)
in

# ||| Migrates one v4 tool spec's output capture specs to v5.
let migrate_tool_spec_v4_to_v5 = fun spec =>
  if std.record.has_field "outputs" spec then
    (std.record.remove "outputs" spec)
    & { outputs = migrate_output_capture_specs_v4_to_v5 spec.outputs }
  else
    spec
in

# ||| Migrates one v4 workflow step's output capture specs to v5.
let migrate_workflow_step_v4_to_v5 = fun step =>
  if std.record.has_field "outputs" step then
    (std.record.remove "outputs" step)
    & { outputs = migrate_output_capture_specs_v4_to_v5 step.outputs }
  else
    step
in

# ||| Migrates one v4 workflow spec's step outputs to v5.
let migrate_workflow_v4_to_v5 = fun workflow =>
  if std.record.has_field "steps" workflow then
    (std.record.remove "steps" workflow)
    & { steps = std.array.map migrate_workflow_step_v4_to_v5 workflow.steps }
  else
    workflow
in

# ||| v4 → v5: capture layout is folded into the capture token and the
# ||| `include_topmost_folder` boolean is removed.
let migrate_v4_to_v5_fn = fun data =>
  let tools_v5 =
    if std.record.has_field "tools" data then
      data.tools |> std.record.map (fun _key => fun spec => migrate_tool_spec_v4_to_v5 spec)
    else
      {}
  in
  let workflows_v5 =
    if std.record.has_field "workflows" data then
      std.array.map migrate_workflow_v4_to_v5 data.workflows
    else
      []
  in
  let base = drop_field "tools" (drop_field "workflows" (drop_field "version" data)) in
  let with_tools =
    if std.record.has_field "tools" data then base & { tools = tools_v5 } else base
  in
  let with_workflows =
    if std.record.has_field "workflows" data then
      with_tools & { workflows = workflows_v5 }
    else
      with_tools
  in
  with_workflows & { version = 5 }
in

# ||| Migrates one v5 tool runtime to v6.
# |||
# ||| v5 represents unlimited concurrency as `max_concurrent_calls = 0`; v6
# ||| represents it as `null`. Absent stays absent so the v6 default applies.
let migrate_tool_runtime_v5_to_v6 = fun rt =>
  if std.record.has_field "max_concurrent_calls" rt then
    if rt.max_concurrent_calls == 0 then
      (drop_field "max_concurrent_calls" rt) & { max_concurrent_calls = null }
    else
      rt
  else
    rt
in

# ||| Migrates one v5 tool spec to v6 by normalizing its runtime.
let migrate_tool_spec_v5_to_v6 = fun spec =>
  if std.record.has_field "runtime" spec then
    (drop_field "runtime" spec)
    & { runtime = migrate_tool_runtime_v5_to_v6 spec.runtime }
  else
    spec
in

# ||| v5 → v6: runtime policy becomes explicit and `retry_impure` is dropped.
# |||
# ||| A step's bare `max_retries` folds into `runtime.max_retries`. The
# ||| document-level `runtime.retry_impure` is removed because retries are now
# ||| bounded by `max_retries` and a failed attempt moves no state pointer.
# ||| Tool `runtime.max_concurrent_calls == 0` becomes `null` (unlimited).
let migrate_v5_to_v6_fn = fun data =>
  let without_version = drop_field "version" data in
  let runtime_v6 =
    if std.record.has_field "runtime" without_version then
      let rt = without_version.runtime in
      let base = drop_field "retry_impure" rt in
      base
    else
      null
  in
  let with_runtime =
    if std.record.has_field "runtime" without_version then
      (drop_field "runtime" without_version) & { runtime = runtime_v6 }
    else
      without_version
  in
  let with_tools =
    if std.record.has_field "tools" with_runtime then
      (drop_field "tools" with_runtime)
      & {
        tools =
          with_runtime.tools
          |> std.record.map (fun _key => fun spec => migrate_tool_spec_v5_to_v6 spec),
      }
    else
      with_runtime
  in
  let with_workflows =
    if std.record.has_field "workflows" with_tools then
      (drop_field "workflows" with_tools)
      & {
        workflows = std.array.map (fun wf =>
          let without_steps = drop_field "steps" wf in
          let steps_v6 =
            if std.record.has_field "steps" wf then
              std.array.map (fun step =>
                let legacy =
                  if std.record.has_field "max_retries" step then step.max_retries else null
                in
                let without = drop_field "max_retries" step in
                if legacy == null then
                  without
                else
                  without & { runtime = { max_retries = legacy } }
              ) wf.steps
            else
              []
          in
          if std.record.has_field "steps" wf then
            without_steps & { steps = steps_v6 }
          else
            wf
        ) with_tools.workflows
      }
    else
      with_tools
  in
  with_workflows & { version = 6 }
in

{
  # ||| Validates one Nickel value as schema v6.
  validate_document_v6 = fun data => data | NickelDocumentV6,

  # ||| Exposes the v6 envelope contract directly for Rust bridge inspection.
  envelope_contract_v6 = NickelDocumentV6,

  # ||| Migrates a v5 document to schema v6.
  migrate_v5_to_v6 = migrate_v5_to_v6_fn,
}
