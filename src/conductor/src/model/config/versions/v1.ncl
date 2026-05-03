# ||| Version 1 persisted schema contract for Conductor configuration/state.
# |||
# ||| This file defines the Nickel-side wire contract used by Rust bridge code
# ||| under `model/config/versions/`. Keep field meanings and nullability stable
# ||| unless a new schema version is introduced.
# |||
# ||| Maintenance guidance:
# ||| - Prefer additive changes for forward evolution.
# ||| - If semantics must change incompatibly, create a new version file and
# |||   add explicit migration edges in `mod.ncl`.
# ||| - Keep comments parser-safe (`# |||`) so Nickel evaluation remains stable.

# ||| Non-empty string contract used for IDs, names, and required text fields.
# ||| Centralizing this avoids tiny drift across repeated string predicates.
let NonEmptyStringV1 =
  std.contract.from_predicate (fun value => std.is_string value && std.string.length value > 0)
in
# ||| Hashes are stored as normalized hash strings and decoded by Rust.
# ||| Keep this broad (`String`) here; algorithm/encoding validation is done in
# ||| Rust so one source of truth governs hash parsing behavior.
let HashStringV1 = String
in
# ||| Schema marker contract for the v1 envelope.
let VersionOneV1 =
  std.contract.from_predicate (fun value => value == 1)
in
# ||| Integer contract used for numeric fields that must be whole numbers.
# ||| Nickel numbers are floating-point; this predicate guards integer-only
# ||| fields such as process success exit codes.
let IntegerNumberV1 =
  std.contract.from_predicate (fun value => std.is_number value && value == std.number.floor value)
in

# ||| Timezone-independent impure execution timestamp.
# ||| Stored as UTC Unix epoch split into integral fields so Rust can recover
# ||| full nanosecond precision deterministically.
let ImpureTimestampV1 = {
  epoch_seconds | IntegerNumberV1,
  subsec_nanos | IntegerNumberV1,
}
in

# ||| Optional per-step output persistence overrides.
# ||| - `save`: tri-state output persistence mode.
# |||   - `false`: do not persist output bytes.
# |||   - `true`: persist output with regular incremental behavior.
# |||   - `"full"`: persist output and prefer full-data storage behavior.
# ||| Keep this tri-state ordering aligned with Rust merge semantics:
# ||| `false < true < "full"`.
let OutputSaveModeV1 =
  std.contract.any_of [
    Bool,
    std.contract.from_predicate (fun value => value == "full"),
  ]
in
let OutputPolicyV1 = {
  save | OutputSaveModeV1 | optional,
}
in

# ||| Per-input declaration for reusable tool input contracts.
# ||| `kind` defaults to `"string"` when omitted.
let ToolInputKindV1 =
  std.contract.any_of [
    std.contract.from_predicate (fun value => value == "string"),
    std.contract.from_predicate (fun value => value == "string_list"),
  ]
in

# ||| Per-input declaration for reusable tool input contracts.
let ToolInputSpecV1 = {
  kind | ToolInputKindV1 | optional,
}
in

# ||| Executable tool definition.
# ||| First command entry is executable path relative to execution sandbox.
# ||| Remaining command entries are template-rendered process arguments.
# ||| Kind-variant strictness (`kind = executable` must not include builtin-only
# ||| properties, and vice versa) is enforced by the Rust bridge enum with
# ||| `deny_unknown_fields`.
let ExecutableToolSpecV1 = {
  kind
    | std.contract.from_predicate (fun value => value == "executable"),
  is_impure | Bool | optional,
  inputs | { _ : ToolInputSpecV1 } | optional,
  command | Array String | optional,
  env_vars | { _ : String } | optional,
  success_codes | Array IntegerNumberV1 | optional,
  outputs | { _ : Dyn } | optional,
}
in

# ||| Builtin tool definition.
# ||| Builtin identity is (`name`, `version`) and must match registered runtime
# ||| builtin implementations in conductor-builtins crates.
let BuiltinToolSpecV1 = {
  kind
    | std.contract.from_predicate (fun value => value == "builtin"),
  name | NonEmptyStringV1,
  version | NonEmptyStringV1,
}
in

# ||| Allowed capture kinds for declared tool outputs.
# ||| `process_code` captures the exit status as a string-typed payload value.
let OutputCaptureKindV1 =
  std.contract.any_of [
    std.contract.from_predicate (fun value => value == "stdout"),
    std.contract.from_predicate (fun value => value == "stderr"),
    std.contract.from_predicate (fun value => value == "file"),
    std.contract.from_predicate (fun value => value == "file_regex"),
    std.contract.from_predicate (fun value => value == "folder"),
    std.contract.from_predicate (fun value => value == "folder_regex"),
    std.contract.from_predicate (fun value => value == "process_code"),
  ]
in

# ||| Output capture variant for `kind = "stdout"`.
let OutputCaptureStdoutV1 = {
  kind | std.contract.from_predicate (fun value => value == "stdout"),
}
in

# ||| Output capture variant for `kind = "stderr"`.
let OutputCaptureStderrV1 = {
  kind | std.contract.from_predicate (fun value => value == "stderr"),
}
in

# ||| Output capture variant for `kind = "process_code"`.
let OutputCaptureProcessCodeV1 = {
  kind | std.contract.from_predicate (fun value => value == "process_code"),
}
in

# ||| Output capture variant for `kind = "file"`.
let OutputCaptureFileV1 = {
  kind | std.contract.from_predicate (fun value => value == "file"),
  path | NonEmptyStringV1,
}
in

# ||| Output capture variant for `kind = "file_regex"`.
let OutputCaptureFileRegexV1 = {
  kind | std.contract.from_predicate (fun value => value == "file_regex"),
  path_regex | NonEmptyStringV1,
}
in

# ||| Output capture variant for `kind = "folder"`.
let OutputCaptureFolderV1 = {
  kind | std.contract.from_predicate (fun value => value == "folder"),
  path | NonEmptyStringV1,
  include_topmost_folder | Bool | optional,
}
in

# ||| Output capture variant for `kind = "folder_regex"`.
# ||| When regex capture groups match selected file paths, captured strings are
# ||| joined to form ZIP member paths; otherwise original sandbox-relative paths
# ||| are preserved.
let OutputCaptureFolderRegexV1 = {
  kind | std.contract.from_predicate (fun value => value == "folder_regex"),
  path_regex | NonEmptyStringV1,
}
in

# ||| Capture source contract for one output declaration.
# ||| Variant records are strict by kind: stream captures cannot declare
# ||| path-only fields and file/folder captures must satisfy required fields.
let OutputCaptureV1 =
  std.contract.any_of [
    OutputCaptureStdoutV1,
    OutputCaptureStderrV1,
    OutputCaptureProcessCodeV1,
    OutputCaptureFileV1,
    OutputCaptureFileRegexV1,
    OutputCaptureFolderV1,
    OutputCaptureFolderRegexV1,
  ]
in

# ||| Tool output declaration (capture-only; persistence is call-level policy).
# ||| When `allow_empty` is `true`, a capture that produces no output
# ||| (missing file, no regex match, missing folder) is treated as a
# ||| successful empty output rather than a workflow error.  Downstream
# ||| steps that reference an empty-capture output as a step input receive
# ||| a workflow error at resolution time.
let ToolOutputSpecV1 = {
  capture | OutputCaptureV1,
  allow_empty | Bool | optional,
}
in

# ||| External content metadata resolved through CAS.
# ||| Hash identity is represented by the map key in `external_data`.
# ||| Save policy accepts only:
# ||| - `true`: regular saved behavior,
# ||| - `"full"`: saved behavior with full-data preference.
# ||| `false` is rejected for external-data references.
let ExternalDataSaveModeV1 =
  std.contract.any_of [
    std.contract.from_predicate (fun value => value == true),
    std.contract.from_predicate (fun value => value == "full"),
  ]
in
let ExternalContentRefV1 = {
  description | String | optional,
  save | ExternalDataSaveModeV1 | optional,
}
in

# ||| Workflow-step input binding value.
# ||| Input bindings are scalar strings or list-of-strings.
# ||| Supported interpolation expressions inside `${...}` (validated in Rust):
# ||| - `external_data.<hash>`
# ||| - `step_output.<step_id>.<output_name>`
# ||| Plain text outside `${...}` is preserved as literal input content.
# ||| For list bindings, interpolation rules apply per list item.
# ||| Input-binding interpolation does not support materialization directives
# ||| such as `:file(...)` or `:folder(...)`.
let InputBindingV1 =
  std.contract.any_of [
    String,
    Array String,
  ]
in

# ||| One workflow step record.
# ||| Execution order is derived from `${step_output.<step_id>.<output_name>}`
# ||| references in input bindings plus optional explicit `depends_on` edges.
let WorkflowStepSpecV1 = {
  id | NonEmptyStringV1,
  tool | NonEmptyStringV1,
  inputs | { _ : InputBindingV1 } | optional,
  depends_on | Array NonEmptyStringV1 | optional,
  # `outputs` configures persistence policy only; it does not declare
  # available outputs, which always come from the referenced tool definition.
  outputs | { _ : OutputPolicyV1 } | optional,
}
in

# ||| One workflow definition.
# ||| `name` and `description` are informational only and do not affect
# ||| workflow identity, topological ordering, or cache keys.
# ||| Empty or omitted `steps` means no work for that workflow key.
let WorkflowSpecV1 = {
  name | String | optional,
  description | String | optional,
  steps | Array WorkflowStepSpecV1 | optional,
}
in

# ||| One reusable tool definition.
# ||| Tools are tagged by executable/builtin kind.
let ToolSpecV1 = std.contract.any_of [ExecutableToolSpecV1, BuiltinToolSpecV1]
in

# ||| Runtime-only execution configuration for one declared tool.
# ||| `max_concurrent_calls` controls execution scheduling only; it does not
# ||| participate in tool identity metadata/dedup keys.
let ToolConfigV1 = {
  # Keep this dynamic in Nickel and enforce `-1` or positive integer in Rust
  # validation so sentinel semantics stay stable across evaluators.
  max_concurrent_calls | Dyn | optional,
  # Keep this dynamic in Nickel and enforce `-1` or non-negative integer in
  # Rust validation so retry semantics stay stable across evaluators.
  max_retries | Dyn | optional,
  # Optional human-facing note for runtime config context.
  # This field is informational only and does not affect runtime identity.
  description | String | optional,
  # Optional per-tool default input bindings used when one workflow step omits
  # matching input keys.
  input_defaults | { _ : InputBindingV1 } | optional,
  # Optional runtime environment additions for executable process execution.
  #
  # Runtime merges inherited host names first, then merges
  # `tools.<tool>.env_vars` with `tool_configs.<tool>.env_vars` and rejects
  # duplicate keys across those two explicit maps.
  #
  # This field is invalid for builtin tools.
  env_vars | { _ : String } | optional,
  # Executable sandbox materialization map (`relative_path -> hash`).
  #
  # Key semantics:
  # - key ending with `/` or `\\` means destination directory; the hash must
  #   resolve to ZIP payload bytes that are unpacked into that directory.
  # - key without trailing slash/backslash means destination file; bytes are
  #   written directly to that file.
  #
  # Runtime rejects absolute paths and escaping paths (`..`).
  content_map | { _ : HashStringV1 } | optional,
}
in

# ||| Platform-keyed inherited host environment-variable names.
# ||| Keys are platform names (`windows`, `linux`, `macos`, ...), and values
# ||| are ordered environment-variable-name lists for that platform.
let PlatformInheritedEnvVarsV1 = {
  windows | Array NonEmptyStringV1 | optional,
  linux | Array NonEmptyStringV1 | optional,
  macos | Array NonEmptyStringV1 | optional,
}
in

# ||| Grouped runtime storage-path configuration.
let RuntimeStorageV1 = {
  conductor_dir | NonEmptyStringV1 | optional,
  conductor_state_config | NonEmptyStringV1 | optional,
  cas_store_dir | NonEmptyStringV1 | optional,
  conductor_tmp_dir | NonEmptyStringV1 | optional,
  conductor_schema_dir | NonEmptyStringV1 | optional,
  # Optional additional inherited host environment-variable names keyed by
  # platform.
  #
  # Runtime always includes host defaults (`SYSTEMROOT`, `WINDIR`, `TEMP`,
  # `TMP` on Windows; empty default elsewhere). Runtime reads only the active
  # host platform entry from this map and merges those names after defaults
  # with case-insensitive de-duplication.
  inherited_env_vars | PlatformInheritedEnvVarsV1 | optional,
  # Optional toggle for shared global user-level managed-tool download cache.
  # Omitted values default to enabled behavior.
  use_user_tool_cache | Bool | optional,
}
in

# ||| Top-level persisted Nickel document envelope for schema version 1.
# ||| Merge semantics (user vs machine docs) are implemented in Rust; this
# ||| contract only specifies syntactic shape and local field constraints.
let NickelDocumentV1 = {
  version | VersionOneV1,
  # Grouped runtime storage path configuration.
  # Defaults are resolved by runtime when fields are omitted.
  runtime | RuntimeStorageV1 | optional,
  # External-data map keys are CAS hash strings parsed by Rust.
  external_data | { _ : ExternalContentRefV1 } | optional,
  tools | { _ : ToolSpecV1 } | optional,
  workflows | { _ : WorkflowSpecV1 } | optional,
  tool_configs | { _ : ToolConfigV1 } | optional,
  impure_timestamps | { _ : { _ : ImpureTimestampV1 } } | optional,
  state_pointer | Dyn | optional,
}
in

{
  # ||| Validates one Nickel value as schema v1.
  # ||| Use this for explicit version-locked validation in tests.
  validate_document_v1 = fun data => data | NickelDocumentV1,

  # ||| Exposes the v1 envelope contract directly.
  # ||| Rust bridge may inspect this contract shape for diagnostics.
  envelope_contract_v1 = NickelDocumentV1,
}
