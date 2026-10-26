# ||| Schema version 3 — Unified conductor config document (v3).
# |||
# ||| This contract validates the Nickel envelope shape that Rust deserializes
# ||| into `v_latest::NickelEnvelopeLatest`. Keep fields and their types in
# ||| sync with the Rust side.
# |||
# ||| Schema version 3 uses a single unified document model (replacing the old
# ||| three-document user/machine/state split from v1).  Tools are tagged by kind
# ||| (builtin/executable), workflows are ordered-step DAGs, and runtime-only
# ||| configuration lives in each tool's `runtime` sub-object.
# |||
# ||| Version 3 adds the payload staging decision: a tool's `runtime` names the
# ||| directory its provisioned payload lives in and how a step obtains the
# ||| payload it executes from.

# ||| Non-empty string contract.
# ||| A non-empty string, with a diagnostic that survives migration.
# |||
# ||| The migration wrapper rebuilds the document, so a rejected value no longer
# ||| carries the span it was written at; a `from_predicate` failure would print
# ||| the value alone with no field name. The custom contract states the reason
# ||| itself, and the record contract around it names the field.
let NonEmptyStringV3 =
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
let VersionThreeV3 = std.contract.from_predicate (fun value => value == 3) in

# ||| Integer-valued number (not float); defined as a predicate because Nickel
# ||| `std.number` contracts coerce — we want to reject non‑integer values.
let IntegerNumberV3 =
  std.contract.from_predicate (fun value =>
    std.is_number value && value == std.number.floor value
  )
in

# ||| Tool input kind contract.
let ToolInputKindV3 =
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
let PayloadStagingV3 =
  std.contract.any_of [
    std.contract.from_predicate (fun value => value == "in_place"),
    std.contract.from_predicate (fun value => value == "hard_link"),
    std.contract.from_predicate (fun value => value == "copy"),
  ]
in

# ||| One declared tool input spec.
let ToolInputSpecV3 = {
  kind | ToolInputKindV3 | optional,
  required | Bool | optional,
}
in

# ||| Save mode: "false" (do not persist), "true" (persist normally), or "full" (force full persistence).
let SaveModeV3 =
  std.contract.any_of [
    std.contract.from_predicate (fun value => value == "false"),
    std.contract.from_predicate (fun value => value == "true"),
    std.contract.from_predicate (fun value => value == "full"),
  ]
in

# ||| Output capture specification for workflow steps.
let OutputCaptureSpecV3 = {
  name | NonEmptyStringV3,
  capture | NonEmptyStringV3,
  save | SaveModeV3 | optional,
  allow_empty | Bool | optional,
  include_topmost_folder | Bool | optional,
}
in

# ||| Tool content map entry: relative path → CAS hash or inline content.
# ||| Keys ending with `/` unpack ZIP payloads into that directory.
let ToolContentMapV3 = { _ : NonEmptyStringV3 } in

# ||| Runtime-only tool configuration (not part of tool-call identity).
let ToolRuntimeV3 = {
  content_map | ToolContentMapV3 | optional,
  impure | Bool | optional,
  inherited_env_vars | Array NonEmptyStringV3 | optional,
  max_concurrent_calls | IntegerNumberV3 | optional,
  max_retries | IntegerNumberV3 | optional,
  # ||| Absolute path of the directory holding this tool's provisioned payload
  # ||| tree. Absent means the document names none.
  payload_root | String | optional,
  # ||| How a step obtains the payload it executes from. Absent means
  # ||| `in_place`, which needs `payload_root`.
  payload_staging | PayloadStagingV3 | optional,
}
in

# ||| Platform-specific inherited environment variable names.
# ||| Keys are closed to the three supported platforms (S-A5).
let PlatformInheritedEnvVarsV3 = {
  windows | Array NonEmptyStringV3 | optional,
  linux | Array NonEmptyStringV3 | optional,
  macos | Array NonEmptyStringV3 | optional,
}
in

# ||| Conductor platform environment configuration (nested under `environment`).
let RuntimePlatformEnvConfigV3 = {
  platform_inherited_env_vars | PlatformInheritedEnvVarsV3 | optional,
}
in

# ||| Runtime configuration for the conductor itself (not per-tool).
let ConductorRuntimeConfigV3 = {
  retry_impure | Bool | optional,
  environment | RuntimePlatformEnvConfigV3 | optional,
  platform_inherited_env_vars | PlatformInheritedEnvVarsV3 | optional,
}
in

# ||| Input binding: a single string or an array of strings.
# ||| Used in default_inputs to support both scalar and list defaults.
let InputBindingV3 =
  std.contract.any_of [
    String,
    Array String,
  ]
in

# ||| Builtin tool kind: identity is builtin_id (e.g. "echo@v1").
let BuiltinToolSpecV3 = {
  kind | std.contract.from_predicate (fun value => value == "builtin"),
  name | NonEmptyStringV3,
  # ||| Human-readable tool version, shown on progress rows. Absent means the
  # ||| document makes no version claim, so nothing is rendered.
  version | NonEmptyStringV3 | optional,
  builtin_id | NonEmptyStringV3,
  inputs | { _ : ToolInputSpecV3 } | optional,
  default_inputs | { _ : InputBindingV3 } | optional,
  outputs | { _ : OutputCaptureSpecV3 } | optional,
  runtime | ToolRuntimeV3 | optional,
}
in

# ||| Executable tool kind: external command with environment and exit-code policy.
let ExecutableToolSpecV3 = {
  kind | std.contract.from_predicate (fun value => value == "executable"),
  name | NonEmptyStringV3,
  # ||| Human-readable tool version, shown on progress rows. Absent means the
  # ||| document makes no version claim, so nothing is rendered.
  version | NonEmptyStringV3 | optional,
  command | Array String,
  env_vars | { _ : String } | optional,
  success_codes | Array IntegerNumberV3 | optional,
  inputs | { _ : ToolInputSpecV3 } | optional,
  default_inputs | { _ : InputBindingV3 } | optional,
  outputs | { _ : OutputCaptureSpecV3 } | optional,
  runtime | ToolRuntimeV3 | optional,
}
in

# ||| One tool definition (tagged union by kind).
let ToolSpecV3 = std.contract.any_of [BuiltinToolSpecV3, ExecutableToolSpecV3] in

# ||| One workflow step (references one tool).
let WorkflowStepSpecV3 = {
  id | NonEmptyStringV3,
  tool | NonEmptyStringV3,
  inputs | { _ : String } | optional,
  outputs | { _ : OutputCaptureSpecV3 } | optional,
  max_retries | IntegerNumberV3 | optional,
  depends_on | Array NonEmptyStringV3 | optional,
}
in

# ||| One workflow definition (ordered steps DAG).
let WorkflowSpecV3 = {
  name | NonEmptyStringV3,
  display_name | NonEmptyStringV3 | optional,
  description | NonEmptyStringV3 | optional,
  impure | Bool | optional,
  steps | Array WorkflowStepSpecV3 | optional,
}
in

# ||| External data content reference: a CAS-addressed blob with
# ||| metadata describing how to persist it.
let ExternalContentRefV3 = {
  hash | NonEmptyStringV3 | optional,
  description | String | optional,
  save
    | std.contract.any_of [
      std.contract.from_predicate (fun value => value == "full"),
      Bool,
    ]
    | optional,
}
in

# ||| Top-level persisted Nickel document envelope for schema version 3.
# ||| Closed record: unknown top-level fields are rejected (S-A1).
let NickelDocumentV3 = {
  version | VersionThreeV3,
  tools | { _ : ToolSpecV3 } | optional,
  workflows | Array WorkflowSpecV3 | optional,
  external_data | { _ : ExternalContentRefV3 } | optional,
  runtime | ConductorRuntimeConfigV3 | optional,
}
in

# ||| v2 → v3:  every tool runtime gains the staging decision.
# |||
# ||| A v2 document names no payload root, and `copy` is exactly the behaviour
# ||| it had, so the migration records `copy` rather than breaking a standalone
# ||| document. A document written from now on names its root and the strategy
# ||| that uses it.
let migrate_v2_to_v3_fn = fun data =>
  let migrated_runtime = fun runtime =>
    runtime & { payload_root = "", payload_staging = "copy" }
  in
  let tools_v3 =
    if std.record.has_field "tools" data then
      data.tools
      |> std.record.map (fun key =>
        fun spec =>
          if std.record.has_field "runtime" spec then
            # ||| The spec is rebuilt rather than merged: `&` merges records
            # ||| recursively, and the values on the merged side lose their
            # ||| source span, so a diagnostic would point at `<unknown>`
            # ||| instead of at the field a document wrote.
            (std.record.remove "runtime" spec)
            & { runtime = migrated_runtime spec.runtime }
          else
            spec & { runtime = migrated_runtime {} }
      )
    else
      {}
  in
  (drop_field "tools" (drop_field "version" data))
  & { version = 3, tools = tools_v3 }
in

# ||| v4 → v3:  the document-level staging default is materialized onto every tool.
# |||
# ||| v3 keeps the staging decision on each tool and has no place for a
# ||| document-wide one, so the hop writes the default onto a tool that inherited
# ||| it. A tool that already states a strategy keeps it, and `in_place` is left
# ||| absent because that is what an absent field already means in v3: the
# ||| downgrade changes the shape and never the behaviour.
# |||
# ||| The document runtime loses exactly the one field v4 added. v4 and v3 declare
# ||| the same other runtime fields, so removing the record instead of the field
# ||| would drop `retry_impure` and the inherited environment names that v3 (and
# ||| every earlier version) still carry.
let migrate_v4_to_v3_fn = fun data =>
  let document_default =
    if
      std.record.has_field "runtime" data
      && std.record.has_field "payload_staging" data.runtime
    then
      data.runtime.payload_staging
    else
      "in_place"
  in
  let runtime_v3 = fun runtime =>
    let stated =
      if std.record.has_field "payload_staging" runtime then
        runtime.payload_staging
      else
        "inherit"
    in
    let effective = if stated == "inherit" then document_default else stated in
    # `drop_field` rather than `std.record.remove`: a runtime record that never
    # named a strategy has no field to remove, and removing an absent field
    # fails evaluation.
    let without_strategy = drop_field "payload_staging" runtime in
    if effective == "in_place" then
      without_strategy
    else
      without_strategy & { payload_staging = effective }
  in
  let tools_v3 =
    if std.record.has_field "tools" data then
      data.tools
      |> std.record.map (fun key =>
        fun spec =>
          if std.record.has_field "runtime" spec then
            # ||| The spec is rebuilt rather than merged, for the reason the hop
            # ||| into v3 records: `&` merges records recursively, so a field
            # ||| dropped on one side would come back from the other.
            (std.record.remove "runtime" spec) & { runtime = runtime_v3 spec.runtime }
          else if document_default == "in_place" then
            spec
          else
            spec & { runtime = { payload_staging = document_default } }
      )
    else
      {}
  in
  # ||| The document runtime keeps every field v3 declares; only the default
  # ||| leaves. It is attached after the base record has had `runtime` removed,
  # ||| because `&` merges recursively and a runtime left on both sides would
  # ||| carry the dropped field back in.
  let document_scope =
    let base = drop_field "tools" (drop_field "runtime" (drop_field "version" data)) in
    let runtime_v3_document =
      if std.record.has_field "runtime" data then
        { runtime = drop_field "payload_staging" data.runtime }
      else
        {}
    in
    base & runtime_v3_document & { version = 3 }
  in
  if std.record.has_field "tools" data then
    document_scope & { tools = tools_v3 }
  else
    document_scope
in

{
  # ||| Validates one Nickel value as schema v3.
  validate_document_v3 = fun data => data | NickelDocumentV3,

  # ||| Exposes the v3 envelope contract directly for Rust bridge inspection.
  envelope_contract_v3 = NickelDocumentV3,

  # ||| Migrates a v2 document to schema v3.
  migrate_v2_to_v3 = migrate_v2_to_v3_fn,

  # ||| Migrates a v4 document down to schema v3, recording on every tool the
  # ||| strategy it staged by under the document-level default.
  migrate_v4_to_v3 = migrate_v4_to_v3_fn,
}
