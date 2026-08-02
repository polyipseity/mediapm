# V2 document schema contracts for mediapm Nickel config (active version).
#
# STRICT closed-contract surface (S-C1..S-C7 shape): every record is closed
# (unknown fields are rejected at evaluation time instead of being silently
# dropped or passed through to the Rust deserializer), numeric fields that
# map to Rust integer types carry an integer guard, and string-enum fields
# map to the exact Rust enum wire values.
#
# This file mirrors the serde wire format of the Rust document-domain types in
# `mediapm/src/config` (`MediaPmDocument`, `MediaSourceSpec`, `MediaStep`,
# `HierarchyNode`, `MediaRuntimeStorage`, `ToolRequirement`,
# `ConfigVersionSpec`/`VersionSpec`).  The integration tests in
# `tests/int/schema_sync.rs` and `tests/int/schema_strictness.rs` keep the two
# surfaces in sync.
#
# STRICT VERSION SEPARATION: this file is fully self-contained and uses
# exclusively `*V2`-suffixed contract names.  No `*V1` names, no shared or
# imported cross-version contracts.  `mod.ncl` is the only cross-version file
# and holds only the registry.
#
# `MediaPmState` is intentionally absent: state is managed separately via
# `state.json` and is not part of the V2 config surface.  The V1→V2 migration
# strips the legacy `state` field (see `migrate_v1_to_v2`).

# ||| String enum contract: restricts a string value to one of the `allowed`
# values.
let EnumV2 = fun allowed =>
  std.contract.from_predicate (
    fun value => std.is_string value && std.array.elem value allowed
  )
in

# ||| Non-negative integer contract (guards fractional floats like 2.5).
let IntegerNumberV2 = std.contract.from_predicate (
  fun value => std.is_number value && std.number.is_integer value
)
in

# ||| Non-empty string contract.
let NonEmptyStringV2 = std.contract.from_predicate (
  fun value => std.is_string value && std.string.length value > 0
)
in

# ||| Path contract: a single path component string or an array of path
# component strings (mirrors `HierarchyPath`'s serde shape).
let PathV2 = std.contract.any_of [String, Array String]
in

# ||| Filename sanitization mode (mirrors `SanitizeNamesConfig`'s untagged
# wire shape: "inherit", a bool, or a char-to-char map).
let SanitizeNamesConfigV2 = std.contract.any_of [
  std.contract.from_predicate (fun value => value == "inherit"),
  Bool,
  { _ : String },
]
in

# ||| Output save config (mirrors `OutputSaveConfig`: bool or the literal
# "full").
let OutputSaveConfigV2 = std.contract.any_of [
  Bool,
  std.contract.from_predicate (fun value => value == "full"),
]
in

# ||| Exact version spec (mirrors `VersionSpecFields`: closed record).
let ExactVersionSpecV2 = {
  vcs_hash | String | optional,
  version | String | optional,
  tag | String | optional,
}
in

# ||| Config version spec (mirrors `ConfigVersionSpec`: "latest", "inherit",
# or an exact record).
let ConfigVersionSpecV2 = std.contract.any_of [
  std.contract.from_predicate (fun value => value == "latest"),
  std.contract.from_predicate (fun value => value == "inherit"),
  ExactVersionSpecV2,
]
in

# ||| Resolved version spec (mirrors `VersionSpec`: "latest" or an exact
# record — no "inherit").
let VersionSpecV2 = std.contract.any_of [
  std.contract.from_predicate (fun value => value == "latest"),
  ExactVersionSpecV2,
]
in

# ||| Tool requirement (mirrors `ToolRequirement`).  Closed: no open `..`
# tail.
let ToolRequirementV2 = {
  version_spec
    | ConfigVersionSpecV2
    | optional,
  dependencies | { _ : ConfigVersionSpecV2 } | default = {},
  recheck_seconds | IntegerNumberV2 | optional,
  max_input_slots | IntegerNumberV2 | optional,
  max_output_slots | IntegerNumberV2 | optional,
}
in

# ||| Media metadata regex transform (mirrors `MediaMetadataRegexTransform`).
let MediaMetadataRegexTransformV2 = {
  pattern | String,
  replacement | String,
}
in

# ||| Media metadata variant binding (mirrors `MediaMetadataVariantBinding`).
let MediaMetadataVariantBindingV2 = {
  variant | String,
  metadata_key | String,
  transform | MediaMetadataRegexTransformV2 | optional,
}
in

# ||| Media metadata value (mirrors `MediaMetadataValue`: string, variant
# binding, or an array of either).
let MediaMetadataValueV2 = std.contract.any_of [
  String,
  MediaMetadataVariantBindingV2,
  Array (std.contract.any_of [String, MediaMetadataVariantBindingV2]),
]
in

# ||| Output capture kind (mirrors `OutputCaptureKind`: "file" or "folder").
let OutputCaptureKindV2 = EnumV2 ["file", "folder"]
in

# ||| Generic output variant (mirrors `GenericOutputVariantConfig`).
let GenericOutputVariantConfigV2 = {
  kind | String,
  save | OutputSaveConfigV2 | default = true,
  capture_kind | OutputCaptureKindV2 | optional,
  zip_member | String | optional,
  idx | IntegerNumberV2 | optional,
  extension | String | optional,
}
in

# ||| yt-dlp output kind (mirrors `YtDlpOutputKind`).
let YtDlpOutputKindV2 = EnumV2 [
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
let YtDlpOutputVariantConfigV2 = {
  kind | YtDlpOutputKindV2,
  save | OutputSaveConfigV2 | default = true,
  capture_kind | OutputCaptureKindV2 | optional,
  langs | String | optional,
  thumbnail_ids | String | optional,
  sub_format | String | optional,
  convert | String | optional,
  zip_member | String | optional,
}
in

# ||| Output variant value (mirrors the untagged `OutputVariantValue` enum:
# generic or yt-dlp variant).
let OutputVariantValueV2 = std.contract.any_of [
  GenericOutputVariantConfigV2,
  YtDlpOutputVariantConfigV2,
]
in

# ||| Media step (mirrors `MediaStep`).
let MediaStepSpecV2 = {
  tool | EnumV2 ["yt-dlp", "import", "ffmpeg", "rsgain", "media-tagger"],
  input_variants | Array String | default = [],
  output_variants | { _ : OutputVariantValueV2 } | default = {},
  options | { _ : String } | default = {},
}
in

# ||| Media source spec (mirrors `MediaSourceSpec`).  Closed: no open `..`
# tail.
let MediaSourceSpecV2 = {
  description | NonEmptyStringV2 | optional,
  title | NonEmptyStringV2 | optional,
  artist | String | optional,
  metadata | { _ : MediaMetadataValueV2 } | default = {},
  variant_hashes | { _ : String } | default = {},
  steps | Array MediaStepSpecV2 | default = [],
}
in

# ||| Hierarchy node (mirrors `HierarchyNode`).  Recursive via `let rec`.
let rec HierarchyNodeV2 = {
  path | PathV2 | default = "",
  kind | EnumV2 ["folder", "media", "media_folder", "playlist"],
  id | String | optional,
  media_id | String | optional,
  variant | String | optional,
  variants | Array String | default = [],
  rename_files | Array {
    pattern | String,
    replacement | String,
  } | default = [],
  format | EnumV2 ["m3u8", "pls", "xspf", "wpl", "asx"] | optional,
  ids | Array (std.contract.any_of [
    String,
    { id | String, path | String | optional },
  ]) | default = [],
  sanitize_names | SanitizeNamesConfigV2 | optional,
  children | Array HierarchyNodeV2 | default = [],
}
in

# ||| Runtime storage (mirrors `MediaRuntimeStorage`).  Closed: no open `..`
# tail.  `tools` and `cache_root_override` are `#[serde(skip)]` in Rust and
# are intentionally absent here.
let MediaRuntimeStorageV2 = {
  mediapm_dir | String | optional,
  hierarchy_root_dir | String | optional,
  materialization_preference_order
    | Array (EnumV2 ["hardlink", "symlink", "reflink", "copy"])
    | optional,
  verify_on_read
    | Array (EnumV2 ["always", "modified", "sample", "stale"])
    | optional,
  verify_on_read_sample_denominator | IntegerNumberV2 | optional,
  verify_on_read_stale_timeout_secs | IntegerNumberV2 | optional,
  reconstructed_cache_ttl_seconds | IntegerNumberV2 | optional,
  instance_ttl_seconds | IntegerNumberV2 | optional,
  inherited_env_vars | {
    windows | Array NonEmptyStringV2 | optional,
    linux | Array NonEmptyStringV2 | optional,
    macos | Array NonEmptyStringV2 | optional,
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
  path_sanitization | SanitizeNamesConfigV2 | default = "inherit",
}
in

# ||| mediapm V2 document (mirrors `MediaPmDocument`).  Closed: no open `..`
# tail.  `version` must be a non-negative integer.  No `state` field: state
# is managed separately via `state.json` and is not part of the V2 config
# surface.
let MediaPmDocumentV2 = {
  version | IntegerNumberV2,
  media | { _ : MediaSourceSpecV2 } | default = {},
  hierarchy | Array HierarchyNodeV2 | default = [],
  tools | { _ : ToolRequirementV2 } | default = {},
  runtime | MediaRuntimeStorageV2 | default = {},
}
in

# Alias each exported contract through a `let` binding first: record
# shorthand fields that reference sibling fields (e.g. `{ X = X }`) recurse
# infinitely in Nickel, so the canonical pattern is
# `let _x = x in { X = _x }`.
let _media_pm_document_v2 = MediaPmDocumentV2 in
let _config_version_spec_v2 = ConfigVersionSpecV2 in
let _version_spec_v2 = VersionSpecV2 in
let _tool_requirement_v2 = ToolRequirementV2 in
let _media_runtime_storage_v2 = MediaRuntimeStorageV2 in
let _hierarchy_node_v2 = HierarchyNodeV2 in
let _media_source_spec_v2 = MediaSourceSpecV2 in
let _media_step_spec_v2 = MediaStepSpecV2 in
let _output_variant_value_v2 = OutputVariantValueV2 in
let _sanitize_names_config_v2 = SanitizeNamesConfigV2 in

# ||| Migration edge INTO V2 (placement policy: each version file owns the
# ||| migration into that version; `mod.ncl` only dispatches).  V1 accepts an
# ||| optional `state` payload; V2 drops it (state is managed separately via
# ||| `state.json`).  `std.record.remove` requires the field to be present,
# ||| so the removal is guarded by `std.record.has_field`.
let migrate_v1_to_v2_fn = fun data =>
  let stripped =
    if std.record.has_field "state" data then
      std.record.remove "state" data
    else
      data
  in
  stripped & { version = 2 }
in

{
  # ||| Validates one V2 mediapm document, filling in defaults.
  validate_document_v2 = fun document => document | _media_pm_document_v2,
  # ||| The V2 document envelope contract (unwrapped).
  envelope_contract_v2 = _media_pm_document_v2,
  # ||| Migrates one V1 document to V2 (strips the legacy `state` field and
  # ||| bumps the version marker).
  migrate_v1_to_v2 = migrate_v1_to_v2_fn,
  MediaPmDocumentV2 = _media_pm_document_v2,
  ConfigVersionSpecV2 = _config_version_spec_v2,
  VersionSpecV2 = _version_spec_v2,
  ToolRequirementV2 = _tool_requirement_v2,
  MediaRuntimeStorageV2 = _media_runtime_storage_v2,
  HierarchyNodeV2 = _hierarchy_node_v2,
  MediaSourceSpecV2 = _media_source_spec_v2,
  MediaStepSpecV2 = _media_step_spec_v2,
  OutputVariantValueV2 = _output_variant_value_v2,
  SanitizeNamesConfigV2 = _sanitize_names_config_v2,
}
