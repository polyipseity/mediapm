# ||| Schema version 4 — Unified conductor config document (v4).
# |||
# ||| This contract validates the Nickel envelope shape that Rust deserializes
# ||| into `v_latest::NickelEnvelopeLatest`. Keep fields and their types in
# ||| sync with the Rust side.
# |||
# ||| Schema version 4 uses a single unified document model (replacing the old
# ||| three-document user/machine/state split from v1).  Tools are tagged by kind
# ||| (builtin/executable), workflows are ordered-step DAGs, and runtime-only
# ||| configuration lives in each tool's `runtime` sub-object.
# |||
# ||| Version 4 adds a document-level staging default: the conductor runtime
# ||| names the strategy a tool uses when it names none, so a library chooses
# ||| once instead of on every tool. Version 3 keeps the decision on each tool's
# ||| runtime, and a tool that still names one keeps it.

# ||| Non-empty string contract.
# ||| A non-empty string, with a diagnostic that survives migration.
# |||
# ||| The migration wrapper rebuilds the document, so a rejected value no longer
# ||| carries the span it was written at; a `from_predicate` failure would print
# ||| the value alone with no field name. The custom contract states the reason
# ||| itself, and the record contract around it names the field.
let NonEmptyStringV4 =
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
let VersionFourV4 = std.contract.from_predicate (fun value => value == 4) in

# ||| Integer-valued number (not float); defined as a predicate because Nickel
# ||| `std.number` contracts coerce — we want to reject non‑integer values.
let IntegerNumberV4 =
  std.contract.from_predicate (fun value =>
    std.is_number value && value == std.number.floor value
  )
in

# ||| Tool input kind contract.
let ToolInputKindV4 =
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
let PayloadStagingV4 =
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
# ||| `ConductorRuntimeConfigV4.payload_staging` before any step runs, so
# ||| `inherit` never reaches an executor.
let ConfigPayloadStagingV4 =
  std.contract.any_of [
    std.contract.from_predicate (fun value => value == "inherit"),
    PayloadStagingV4,
  ]
in

# ||| One declared tool input spec.
let ToolInputSpecV4 = {
  kind | ToolInputKindV4 | optional,
  required | Bool | optional,
}
in

# ||| Save mode: "false" (do not persist), "true" (persist normally), or "full" (force full persistence).
let SaveModeV4 =
  std.contract.any_of [
    std.contract.from_predicate (fun value => value == "false"),
    std.contract.from_predicate (fun value => value == "true"),
    std.contract.from_predicate (fun value => value == "full"),
  ]
in

# ||| Output capture specification for workflow steps.
let OutputCaptureSpecV4 = {
  name | NonEmptyStringV4,
  capture | NonEmptyStringV4,
  save | SaveModeV4 | optional,
  allow_empty | Bool | optional,
  include_topmost_folder | Bool | optional,
}
in

# ||| Tool content map entry: relative path → CAS hash or inline content.
# ||| Keys ending with `/` unpack ZIP payloads into that directory.
let ToolContentMapV4 = { _ : NonEmptyStringV4 } in

# ||| Runtime-only tool configuration (not part of tool-call identity).
let ToolRuntimeV4 = {
  content_map | ToolContentMapV4 | optional,
  impure | Bool | optional,
  inherited_env_vars | Array NonEmptyStringV4 | optional,
  max_concurrent_calls | IntegerNumberV4 | optional,
  max_retries | IntegerNumberV4 | optional,
  # ||| Absolute path of the directory holding this tool's provisioned payload
  # ||| tree. Absent means the document names none.
  payload_root | String | optional,
  # ||| How this tool asks for its payload to be staged. Absent means
  # ||| `inherit`, which takes the document's default; `in_place` and
  # ||| `hard_link` need `payload_root`.
  payload_staging | ConfigPayloadStagingV4 | optional,
}
in

# ||| Platform-specific inherited environment variable names.
# ||| Keys are closed to the three supported platforms (S-A5).
let PlatformInheritedEnvVarsV4 = {
  windows | Array NonEmptyStringV4 | optional,
  linux | Array NonEmptyStringV4 | optional,
  macos | Array NonEmptyStringV4 | optional,
}
in

# ||| Conductor platform environment configuration (nested under `environment`).
let RuntimePlatformEnvConfigV4 = {
  platform_inherited_env_vars | PlatformInheritedEnvVarsV4 | optional,
}
in

# ||| Runtime configuration for the conductor itself (not per-tool).
let ConductorRuntimeConfigV4 = {
  retry_impure | Bool | optional,
  environment | RuntimePlatformEnvConfigV4 | optional,
  platform_inherited_env_vars | PlatformInheritedEnvVarsV4 | optional,
  # ||| How a step obtains the payload it executes from when the tool names no
  # ||| strategy of its own. Absent means `in_place`.
  payload_staging | PayloadStagingV4 | optional,
}
in

# ||| Input binding: a single string or an array of strings.
# ||| Used in default_inputs to support both scalar and list defaults.
let InputBindingV4 =
  std.contract.any_of [
    String,
    Array String,
  ]
in

# ||| Builtin tool kind: identity is builtin_id (e.g. "echo@v1").
let BuiltinToolSpecV4 = {
  kind | std.contract.from_predicate (fun value => value == "builtin"),
  name | NonEmptyStringV4,
  # ||| Human-readable tool version, shown on progress rows. Absent means the
  # ||| document makes no version claim, so nothing is rendered.
  version | NonEmptyStringV4 | optional,
  builtin_id | NonEmptyStringV4,
  inputs | { _ : ToolInputSpecV4 } | optional,
  default_inputs | { _ : InputBindingV4 } | optional,
  outputs | { _ : OutputCaptureSpecV4 } | optional,
  runtime | ToolRuntimeV4 | optional,
}
in

# ||| Executable tool kind: external command with environment and exit-code policy.
let ExecutableToolSpecV4 = {
  kind | std.contract.from_predicate (fun value => value == "executable"),
  name | NonEmptyStringV4,
  # ||| Human-readable tool version, shown on progress rows. Absent means the
  # ||| document makes no version claim, so nothing is rendered.
  version | NonEmptyStringV4 | optional,
  command | Array String,
  env_vars | { _ : String } | optional,
  success_codes | Array IntegerNumberV4 | optional,
  inputs | { _ : ToolInputSpecV4 } | optional,
  default_inputs | { _ : InputBindingV4 } | optional,
  outputs | { _ : OutputCaptureSpecV4 } | optional,
  runtime | ToolRuntimeV4 | optional,
}
in

# ||| One tool definition (tagged union by kind).
let ToolSpecV4 = std.contract.any_of [BuiltinToolSpecV4, ExecutableToolSpecV4] in

# ||| One workflow step (references one tool).
let WorkflowStepSpecV4 = {
  id | NonEmptyStringV4,
  tool | NonEmptyStringV4,
  inputs | { _ : String } | optional,
  outputs | { _ : OutputCaptureSpecV4 } | optional,
  max_retries | IntegerNumberV4 | optional,
  depends_on | Array NonEmptyStringV4 | optional,
}
in

# ||| One workflow definition (ordered steps DAG).
let WorkflowSpecV4 = {
  name | NonEmptyStringV4,
  display_name | NonEmptyStringV4 | optional,
  description | NonEmptyStringV4 | optional,
  impure | Bool | optional,
  steps | Array WorkflowStepSpecV4 | optional,
}
in

# ||| External data content reference: a CAS-addressed blob with
# ||| metadata describing how to persist it.
let ExternalContentRefV4 = {
  hash | NonEmptyStringV4 | optional,
  description | String | optional,
  save
    | std.contract.any_of [
      std.contract.from_predicate (fun value => value == "full"),
      Bool,
    ]
    | optional,
}
in

# ||| Top-level persisted Nickel document envelope for schema version 4.
# ||| Closed record: unknown top-level fields are rejected (S-A1).
let NickelDocumentV4 = {
  version | VersionFourV4,
  tools | { _ : ToolSpecV4 } | optional,
  workflows | Array WorkflowSpecV4 | optional,
  external_data | { _ : ExternalContentRefV4 } | optional,
  runtime | ConductorRuntimeConfigV4 | optional,
}
in

# ||| v3 → v4:  the document gains a staging default it does not use yet.
# |||
# ||| Absent means `in_place`, which is what a v3 document already meant for a
# ||| tool naming no strategy, so the hop records nothing and only bumps the
# ||| marker. A tool that names its own strategy keeps it.
let migrate_v3_to_v4_fn = fun data => (drop_field "version" data) & { version = 4 } in

{
  # ||| Validates one Nickel value as schema v4.
  validate_document_v4 = fun data => data | NickelDocumentV4,

  # ||| Exposes the v4 envelope contract directly for Rust bridge inspection.
  envelope_contract_v4 = NickelDocumentV4,

  # ||| Migrates a v3 document to schema v4.
  migrate_v3_to_v4 = migrate_v3_to_v4_fn,
}
