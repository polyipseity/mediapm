# V1 document schema contracts for mediapm Nickel config.
#
# STRICT closed-contract surface (S-C1..S-C7): every record is closed
# (unknown fields are rejected at evaluation time instead of being silently
# dropped or passed through to the Rust deserializer), numeric fields that
# map to Rust integer types carry an integer guard, and string-enum fields
# map to the exact Rust enum wire values.
#
# This file mirrors the serde wire format of the Rust types in
# `mediapm/src/config` (`MediaPmDocument`, `MediaSourceSpec`, `MediaStep`,
# `HierarchyNode`, `MediaRuntimeStorage`, `ToolRequirement`,
# `ConfigVersionSpec`/`VersionSpec`, `MediaPmState`).  The integration tests
# in `tests/int/schema_sync.rs` and `tests/int/schema_strictness.rs` keep the
# two surfaces in sync.

# ||| String enum contract: restricts a string value to one of the `allowed`
# values.
let EnumV1 = fun allowed =>
  std.contract.from_predicate (
    fun value => std.is_string value && std.array.elem value allowed
  )
in

# ||| Non-negative integer contract (guards fractional floats like 2.5).
let IntegerNumberV1 = std.contract.from_predicate (
  fun value => std.is_number value && std.number.is_integer value
)
in

# ||| Non-empty string contract.
let NonEmptyStringV1 = std.contract.from_predicate (
  fun value => std.is_string value && std.string.length value > 0
)
in

# ||| Path contract: a single path component string or an array of path
# component strings (mirrors `HierarchyPath`'s serde shape).
let PathV1 = std.contract.any_of [String, Array String]
in

# ||| Filename sanitization mode (mirrors `SanitizeNamesConfig`'s untagged
# wire shape: "inherit", a bool, or a char-to-char map).
let SanitizeNamesConfigV1 = std.contract.any_of [
  std.contract.from_predicate (fun value => value == "inherit"),
  Bool,
  { _ : String },
]
in

# ||| Output save config (mirrors `OutputSaveConfig`: bool or the literal
# "full").
let OutputSaveConfigV1 = std.contract.any_of [
  Bool,
  std.contract.from_predicate (fun value => value == "full"),
]
in

# ||| Exact version spec (mirrors `VersionSpecFields`: closed record).
let ExactVersionSpecV1 = {
  vcs_hash | String | optional,
  version | String | optional,
  tag | String | optional,
}
in

# ||| Config version spec (mirrors `ConfigVersionSpec`: "latest", "inherit",
# or an exact record).
let ConfigVersionSpecV1 = std.contract.any_of [
  std.contract.from_predicate (fun value => value == "latest"),
  std.contract.from_predicate (fun value => value == "inherit"),
  ExactVersionSpecV1,
]
in

# ||| Resolved version spec (mirrors `VersionSpec`: "latest" or an exact
# record — no "inherit").
let VersionSpecV1 = std.contract.any_of [
  std.contract.from_predicate (fun value => value == "latest"),
  ExactVersionSpecV1,
]
in

# ||| Tool requirement (mirrors `ToolRequirement`).  Closed: no open `..`
# tail.
let ToolRequirementV1 = {
  version_spec
    | ConfigVersionSpecV1
    | optional,
  dependencies | { _ : ConfigVersionSpecV1 } | default = {},
  recheck_seconds | IntegerNumberV1 | optional,
  max_input_slots | IntegerNumberV1 | optional,
  max_output_slots | IntegerNumberV1 | optional,
}
in

# ||| Media metadata regex transform (mirrors `MediaMetadataRegexTransform`).
let MediaMetadataRegexTransformV1 = {
  pattern | String,
  replacement | String,
}
in

# ||| Media metadata variant binding (mirrors `MediaMetadataVariantBinding`).
let MediaMetadataVariantBindingV1 = {
  variant | String,
  metadata_key | String,
  transform | MediaMetadataRegexTransformV1 | optional,
}
in

# ||| Media metadata value (mirrors `MediaMetadataValue`: string, variant
# binding, or an array of either).
let MediaMetadataValueV1 = std.contract.any_of [
  String,
  MediaMetadataVariantBindingV1,
  Array (std.contract.any_of [String, MediaMetadataVariantBindingV1]),
]
in

# ||| Output capture kind (mirrors `OutputCaptureKind`: "file" or "folder").
let OutputCaptureKindV1 = EnumV1 ["file", "folder"]
in

# ||| Generic output variant (mirrors `GenericOutputVariantConfig`).
let GenericOutputVariantConfigV1 = {
  kind | String,
  save | OutputSaveConfigV1 | default = true,
  capture_kind | OutputCaptureKindV1 | optional,
  zip_member | String | optional,
  idx | IntegerNumberV1 | optional,
  extension | String | optional,
}
in

# ||| yt-dlp output kind (mirrors `YtDlpOutputKind`).
let YtDlpOutputKindV1 = EnumV1 [
  "primary",
  "chapters",
  "subtitles",
  "thumbnails",
  "description",
  "infojson",
  "comment",
  "archive",
  "annotation",
  "links",
]
in

# ||| yt-dlp output variant (mirrors `YtDlpOutputVariantConfig`).
let YtDlpOutputVariantConfigV1 = {
  kind | YtDlpOutputKindV1,
  save | OutputSaveConfigV1 | default = true,
  capture_kind | OutputCaptureKindV1 | optional,
  langs | String | optional,
  thumbnail_ids | String | optional,
  sub_format | String | optional,
  convert | String | optional,
  zip_member | String | optional,
}
in

# ||| Output variant value (mirrors the untagged `OutputVariantValue` enum:
# generic or yt-dlp variant).
let OutputVariantValueV1 = std.contract.any_of [
  GenericOutputVariantConfigV1,
  YtDlpOutputVariantConfigV1,
]
in

# ||| Media step (mirrors `MediaStep`).
let MediaStepSpecV1 = {
  tool | EnumV1 ["yt-dlp", "import", "ffmpeg", "rsgain", "media-tagger"],
  input_variants | Array String | default = [],
  output_variants | { _ : OutputVariantValueV1 } | default = {},
  options | { _ : String } | default = {},
}
in

# ||| Media source spec (mirrors `MediaSourceSpec`).  Closed: no open `..`
# tail.
let MediaSourceSpecV1 = {
  description | NonEmptyStringV1 | optional,
  title | NonEmptyStringV1 | optional,
  artist | String | optional,
  metadata | { _ : MediaMetadataValueV1 } | default = {},
  variant_hashes | { _ : String } | default = {},
  steps | Array MediaStepSpecV1 | default = [],
}
in

# ||| Hierarchy node (mirrors `HierarchyNode`).  Recursive via `let rec`.
let rec HierarchyNodeV1 = {
  path | PathV1 | default = "",
  kind | EnumV1 ["folder", "media", "media_folder", "playlist"],
  id | String | optional,
  media_id | String | optional,
  variant | String | optional,
  variants | Array String | default = [],
  rename_files | Array {
    pattern | String,
    replacement | String,
  } | default = [],
  format | EnumV1 ["m3u8", "pls", "xspf", "wpl", "asx"] | optional,
  ids | Array (std.contract.any_of [
    String,
    { id | String, path | String | optional },
  ]) | default = [],
  sanitize_names | SanitizeNamesConfigV1 | optional,
  children | Array HierarchyNodeV1 | default = [],
}
in

# ||| Runtime storage (mirrors `MediaRuntimeStorage`).  Closed: no open `..`
# tail.  `tools` and `cache_root_override` are `#[serde(skip)]` in Rust and
# are intentionally absent here.
let MediaRuntimeStorageV1 = {
  mediapm_dir | String | optional,
  hierarchy_root_dir | String | optional,
  materialization_preference_order
    | Array (EnumV1 ["hardlink", "symlink", "reflink", "copy"])
    | optional,
  verify_on_read
    | Array (EnumV1 ["always", "modified", "sample", "stale"])
    | optional,
  verify_on_read_sample_denominator | IntegerNumberV1 | optional,
  verify_on_read_stale_timeout_secs | IntegerNumberV1 | optional,
  reconstructed_cache_ttl_seconds | IntegerNumberV1 | optional,
  instance_ttl_seconds | IntegerNumberV1 | optional,
  inherited_env_vars | {
    windows | Array NonEmptyStringV1 | optional,
    linux | Array NonEmptyStringV1 | optional,
    macos | Array NonEmptyStringV1 | optional,
  } | optional,
  media_state_config | String | optional,
  conductor_config | String | optional,
  conductor_generated_config | String | optional,
  conductor_state_config | String | optional,
  conductor_schema_dir | String | optional,
  env_file | String | optional,
  env_generated_file | String | optional,
  mediapm_schema_dir | String | optional,
  profiler_enabled | Bool | default = false,
  verify_materialization | Bool | default = false,
  retry_impure | Bool | default = false,
  path_sanitization | SanitizeNamesConfigV1 | default = "inherit",
}
in

# ||| Managed file record (mirrors `ManagedFileRecord`).
let ManagedFileRecordV1 = {
  media_id | String,
  variant | String,
  hash | String,
}
in

# ||| Managed workflow step state (mirrors `ManagedWorkflowStepState`).
let ManagedWorkflowStepStateV1 = {
  variant_hashes | { _ : String } | default = {},
  steps_completed | IntegerNumberV1 | default = 0,
  last_impure_sync_at | { utc_epoch_seconds | IntegerNumberV1 } | optional,
}
in

# ||| Tool registry entry (mirrors `ToolRegistryEntry`).
let ToolRegistryEntryV1 = {
  tool_id | String,
  version | String,
  canonical_version | String | default = "",
  content_map_hash | String | default = "",
  deployed_at | IntegerNumberV1 | default = 0,
  resolved_tag | String | optional,
  resolved_version | String | optional,
  resolved_vcs_hash | String | optional,
}
in

# ||| mediapm state document (mirrors `MediaPmState`).  Closed: no open `..`
# tail.
let MediaPmStateV1 = {
  version | IntegerNumberV1 | default = 3,
  managed_files | { _ : ManagedFileRecordV1 } | default = {},
  managed_tools | Array ToolRegistryEntryV1 | default = [],
  workflow_states | { _ : ManagedWorkflowStepStateV1 } | default = {},
}
in

# ||| mediapm V1 document (mirrors `MediaPmDocument`).  Closed: no open `..`
# tail.  `version` must be a non-negative integer.
let MediaPmDocumentV1 = {
  version | IntegerNumberV1,
  media | { _ : MediaSourceSpecV1 } | default = {},
  hierarchy | Array HierarchyNodeV1 | default = [],
  tools | { _ : ToolRequirementV1 } | default = {},
  runtime | MediaRuntimeStorageV1 | default = {},
}
in

# Alias each exported contract through a `let` binding first: record
# shorthand fields that reference sibling fields (e.g. `{ X = X }`) recurse
# infinitely in Nickel, so the canonical pattern is
# `let _x = x in { X = _x }`.
let _media_pm_document_v1 = MediaPmDocumentV1 in
let _media_pm_state_v1 = MediaPmStateV1 in
let _config_version_spec_v1 = ConfigVersionSpecV1 in
let _version_spec_v1 = VersionSpecV1 in
let _tool_requirement_v1 = ToolRequirementV1 in
let _media_runtime_storage_v1 = MediaRuntimeStorageV1 in
let _hierarchy_node_v1 = HierarchyNodeV1 in
let _media_source_spec_v1 = MediaSourceSpecV1 in
let _media_step_spec_v1 = MediaStepSpecV1 in
let _output_variant_value_v1 = OutputVariantValueV1 in
let _sanitize_names_config_v1 = SanitizeNamesConfigV1 in

{
  # ||| Validates one V1 mediapm document, filling in defaults.
  validate_document_v1 = fun document => document | _media_pm_document_v1,
  # ||| The V1 document envelope contract (unwrapped).
  envelope_contract_v1 = _media_pm_document_v1,
  MediaPmDocumentV1 = _media_pm_document_v1,
  MediaPmStateV1 = _media_pm_state_v1,
  ConfigVersionSpecV1 = _config_version_spec_v1,
  VersionSpecV1 = _version_spec_v1,
  ToolRequirementV1 = _tool_requirement_v1,
  MediaRuntimeStorageV1 = _media_runtime_storage_v1,
  HierarchyNodeV1 = _hierarchy_node_v1,
  MediaSourceSpecV1 = _media_source_spec_v1,
  MediaStepSpecV1 = _media_step_spec_v1,
  OutputVariantValueV1 = _output_variant_value_v1,
  SanitizeNamesConfigV1 = _sanitize_names_config_v1,
}
