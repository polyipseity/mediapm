# V3 document schema contracts for mediapm Nickel config (active version).
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
# exclusively `*V3`-suffixed contract names.  No `*V1`/`*V2` names, no shared
# or imported cross-version contracts.  `mod.ncl` is the only cross-version
# file and holds only the registry.
#
# `MediaPmState` is intentionally absent: state is managed separately via
# `state.json` and is not part of the V3 config surface.  The V1→V2 hop strips
# the legacy `state` field, and the V2→V3 hop only bumps the version marker,
# because both staging fields are optional and admit themselves; see
# `migrate_v2_to_v3` and `v2.ncl`'s `migrate_v1_to_v2`.
#
# V3 adds two fields, both optional and both staging decisions: a tool names
# the strategy its payload is staged by, and the library names the strategy a
# tool takes when it names none.

# ||| String enum contract: restricts a string value to one of the `allowed`
# values.
let EnumV3 = fun allowed =>
  std.contract.from_predicate (
    fun value => std.is_string value && std.array.elem value allowed
  )
in

# ||| Non-negative integer contract (guards fractional floats like 2.5).
let IntegerNumberV3 = std.contract.from_predicate (
  fun value => std.is_number value && std.number.is_integer value
)
in

# ||| Non-empty string contract.
let NonEmptyStringV3 = std.contract.from_predicate (
  fun value => std.is_string value && std.string.length value > 0
)
in

# ||| Path contract: a single path component string or an array of path
# component strings (mirrors `HierarchyPath`'s serde shape).
let PathV3 = std.contract.any_of [String, Array String]
in

# ||| Filename sanitization mode (mirrors `SanitizeNamesConfig`'s untagged
# wire shape: "inherit", a bool, or a char-to-char map).
let SanitizeNamesConfigV3 = std.contract.any_of [
  std.contract.from_predicate (fun value => value == "inherit"),
  Bool,
  { _ : String },
]
in

# ||| Output save config (mirrors `OutputSaveConfig`: bool or the literal
# "full").
let OutputSaveConfigV3 = std.contract.any_of [
  Bool,
  std.contract.from_predicate (fun value => value == "full"),
]
in

# ||| Exact version spec (mirrors `VersionSpecFields`: closed record).
let ExactVersionSpecV3 = {
  vcs_hash | String | optional,
  version | String | optional,
  tag | String | optional,
}
in

# ||| Config version spec (mirrors `ConfigVersionSpec`: "latest", "inherit",
# or an exact record).
let ConfigVersionSpecV3 = std.contract.any_of [
  std.contract.from_predicate (fun value => value == "latest"),
  std.contract.from_predicate (fun value => value == "inherit"),
  ExactVersionSpecV3,
]
in

# ||| Resolved version spec (mirrors `VersionSpec`: "latest" or an exact
# record — no "inherit").
let VersionSpecV3 = std.contract.any_of [
  std.contract.from_predicate (fun value => value == "latest"),
  ExactVersionSpecV3,
]
in

# ||| Payload staging strategy (mirrors `PayloadStaging`): the strategy a step
# ||| runs with once a choice has been resolved.
let PayloadStagingV3 = EnumV3 ["in_place", "hard_link", "copy"]
in

# ||| Payload staging choice (mirrors `ConfigPayloadStaging`): a tool either
# ||| names a strategy outright or takes the library-wide default, which is what
# ||| `"inherit"` asks for.
let ConfigPayloadStagingV3 = std.contract.any_of [
  std.contract.from_predicate (fun value => value == "inherit"),
  PayloadStagingV3,
]
in

# ||| Tool requirement (mirrors `ToolRequirement`).  Closed: no open `..`
# tail.
let ToolRequirementV3 = {
  version_spec
    | ConfigVersionSpecV3
    | optional,
  dependencies | { _ : ConfigVersionSpecV3 } | default = {},
  recheck_seconds | IntegerNumberV3 | optional,
  # ||| ffmpeg-only: setting this on a non-ffmpeg tool is rejected (MPM-E001).
  max_input_slots | IntegerNumberV3 | optional,
  # ||| ffmpeg-only: setting this on a non-ffmpeg tool is rejected (MPM-E001).
  max_output_slots | IntegerNumberV3 | optional,
  # ||| How this tool's payload reaches the step.  Absent means "inherit", the
  # ||| library-wide default in `runtime.materialization.payload_staging`.
  payload_staging | ConfigPayloadStagingV3 | optional,
}
in

# ||| Media metadata regex transform (mirrors `MediaMetadataRegexTransform`).
let MediaMetadataRegexTransformV3 = {
  pattern | String,
  replacement | String,
}
in

# ||| Media metadata variant binding (mirrors `MediaMetadataVariantBinding`).
let MediaMetadataVariantBindingV3 = {
  variant | String,
  metadata_key | String,
  transform | MediaMetadataRegexTransformV3 | optional,
}
in

# ||| Media metadata value (mirrors `MediaMetadataValue`: string, variant
# binding, or an array of either).
let MediaMetadataValueV3 = std.contract.any_of [
  String,
  MediaMetadataVariantBindingV3,
  Array (std.contract.any_of [String, MediaMetadataVariantBindingV3]),
]
in

# ||| Output capture kind (mirrors `OutputCaptureKind`: "file" or "folder").
let OutputCaptureKindV3 = EnumV3 ["file", "folder"]
in

# ||| Generic output variant (mirrors `GenericOutputVariantConfig`).
let GenericOutputVariantConfigV3 = {
  kind | String,
  save | OutputSaveConfigV3 | default = true,
  capture_kind | OutputCaptureKindV3 | optional,
  zip_member | String | optional,
  idx | IntegerNumberV3 | optional,
  extension | String | optional,
}
in

# ||| yt-dlp output kind (mirrors `YtDlpOutputKind`).
let YtDlpOutputKindV3 = EnumV3 [
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
let YtDlpOutputVariantConfigV3 = {
  kind | YtDlpOutputKindV3,
  save | OutputSaveConfigV3 | default = true,
  capture_kind | OutputCaptureKindV3 | optional,
  langs | String | optional,
  thumbnail_ids | String | optional,
  sub_format | String | optional,
  convert | String | optional,
  zip_member | String | optional,
}
in

# ||| Output variant value (mirrors the untagged `OutputVariantValue` enum:
# generic or yt-dlp variant).
let OutputVariantValueV3 = std.contract.any_of [
  GenericOutputVariantConfigV3,
  YtDlpOutputVariantConfigV3,
]
in

# ||| Media step (mirrors `MediaStep`).
let MediaStepSpecV3 = {
  tool | EnumV3 ["yt-dlp", "import", "ffmpeg", "rsgain", "media-tagger"],
  input_variants | Array String | default = [],
  output_variants | { _ : OutputVariantValueV3 } | default = {},
  options | { _ : String } | default = {},
}
in

# ||| Media source spec (mirrors `MediaSourceSpec`).  Closed: no open `..`
# tail.
let MediaSourceSpecV3 = {
  description | NonEmptyStringV3 | optional,
  title | NonEmptyStringV3 | optional,
  artist | String | optional,
  metadata | { _ : MediaMetadataValueV3 } | default = {},
  variant_hashes | { _ : String } | default = {},
  steps | Array MediaStepSpecV3 | default = [],
}
in

# ||| Hierarchy node (mirrors `HierarchyNode`).  Recursive via `let rec`.
let rec HierarchyNodeV3 = {
  path | PathV3 | default = "",
  kind | EnumV3 ["folder", "media", "media_folder", "playlist"],
  id | String | optional,
  media_id | String | optional,
  variant | String | optional,
  variants | Array String | default = [],
  rename_files | Array {
    pattern | String,
    replacement | String,
  } | default = [],
  format | EnumV3 ["m3u8", "pls", "xspf", "wpl", "asx"] | optional,
  ids | Array (std.contract.any_of [
    String,
    { id | String, path | String | optional },
  ]) | default = [],
  sanitize_names | SanitizeNamesConfigV3 | optional,
  children | Array HierarchyNodeV3 | default = [],
}
in

# ||| Runtime path configuration (mirrors
# ||| `MediaRuntimeStorageLatest::paths`).  Closed: no open `..` tail.
let RuntimePathsConfigV3 = {
  mediapm_dir | String | optional,
  hierarchy_root_dir | String | optional,
  mediapm_state_config | String | optional,
  conductor_config | String | optional,
  conductor_generated_config | String | optional,
  conductor_state_config | String | optional,
  conductor_schema_dir | String | optional,
  mediapm_schema_dir | String | optional,
  env_file | String | optional,
  env_generated_file | String | optional,
}
in

# ||| Runtime materialization configuration (mirrors
# ||| `MediaRuntimeStorageLatest::materialization`).  Closed: no open `..` tail.
let RuntimeMaterializationConfigV3 = {
  materialization_preference_order
    | Array (EnumV3 ["hardlink", "symlink", "reflink", "copy"])
    | optional,
  verify_materialization | Bool | optional,
  # ||| Strategy a tool takes when it names none.  Absent means "in_place",
  # ||| which is also the only strategy that predates this field.
  payload_staging | PayloadStagingV3 | optional,
}
in

# ||| Runtime verification configuration (mirrors
# ||| `MediaRuntimeStorageLatest::verification`).  Closed: no open `..` tail.
let RuntimeVerificationConfigV3 = {
  verify_on_read
    | Array (EnumV3 ["always", "modified", "sample", "stale"])
    | optional,
  verify_on_read_sample_denominator | IntegerNumberV3 | optional,
  verify_on_read_stale_timeout_secs | IntegerNumberV3 | optional,
}
in

# ||| Runtime caching configuration (mirrors
# ||| `MediaRuntimeStorageLatest::caching`).  Closed: no open `..` tail.
let RuntimeCachingConfigV3 = {
  reconstructed_cache_ttl_seconds | IntegerNumberV3 | optional,
}
in

# ||| Runtime lifecycle configuration (mirrors
# ||| `MediaRuntimeStorageLatest::lifecycle`).  Closed: no open `..` tail.
let RuntimeLifecycleConfigV3 = {
  instance_ttl_seconds | IntegerNumberV3 | optional,
}
in

# ||| Runtime environment configuration (mirrors
# ||| `MediaRuntimeStorageLatest::environment`).  Closed: no open `..` tail.
let RuntimeEnvironmentConfigV3 = {
  inherited_env_vars | {
    windows | Array NonEmptyStringV3 | optional,
    linux | Array NonEmptyStringV3 | optional,
    macos | Array NonEmptyStringV3 | optional,
  } | optional,
  profiler_enabled | Bool | optional,
}
in

# ||| Runtime storage (mirrors `MediaRuntimeStorage`).  Closed: no open `..`
# tail.  `tools` is read but never serialized by the boundary type (it holds
# requirements the document states at the top level), which is why the contract
# admits it; `cache_root_override` is not part of the document at all and is
# absent here.
let MediaRuntimeStorageV3 = {
  paths | RuntimePathsConfigV3 | default = {},
  materialization | RuntimeMaterializationConfigV3 | default = {},
  verification | RuntimeVerificationConfigV3 | default = {},
  caching | RuntimeCachingConfigV3 | default = {},
  lifecycle | RuntimeLifecycleConfigV3 | default = {},
  environment | RuntimeEnvironmentConfigV3 | default = {},
  path_sanitization | SanitizeNamesConfigV3 | optional,
  retry_impure | Bool | optional,
  tools | { _ : ToolRequirementV3 } | default = {},
}
in

# ||| mediapm V3 document (mirrors `MediaPmDocument`).  Closed: no open `..`
# tail.  `version` must be a non-negative integer.  No `state` field: state
# is managed separately via `state.json` and is not part of the V3 config
# surface.
let MediaPmDocumentV3 = {
  version | IntegerNumberV3,
  media | { _ : MediaSourceSpecV3 } | default = {},
  hierarchy | Array HierarchyNodeV3 | default = [],
  tools | { _ : ToolRequirementV3 } | default = {},
  runtime | MediaRuntimeStorageV3 | default = {},
}
in

# Alias each exported contract through a `let` binding first: record
# shorthand fields that reference sibling fields (e.g. `{ X = X }`) recurse
# infinitely in Nickel, so the canonical pattern is
# `let _x = x in { X = _x }`.
let _media_pm_document_v3 = MediaPmDocumentV3 in
let _config_version_spec_v3 = ConfigVersionSpecV3 in
let _version_spec_v3 = VersionSpecV3 in
let _tool_requirement_v3 = ToolRequirementV3 in
let _media_runtime_storage_v3 = MediaRuntimeStorageV3 in
let _hierarchy_node_v3 = HierarchyNodeV3 in
let _media_source_spec_v3 = MediaSourceSpecV3 in
let _media_step_spec_v3 = MediaStepSpecV3 in
let _output_variant_value_v3 = OutputVariantValueV3 in
let _sanitize_names_config_v3 = SanitizeNamesConfigV3 in
let _payload_staging_v3 = PayloadStagingV3 in
let _config_payload_staging_v3 = ConfigPayloadStagingV3 in

# ||| Removes `name` when it is there.
# |||
# ||| `std.record.remove` fails on an absent field, and every field a hop here
# ||| removes is optional, so the presence guard is the rule rather than the
# ||| exception.
let drop_field = fun name record =>
  if std.record.has_field name record then std.record.remove name record else record
in

# ||| Migration edge INTO V3 (placement policy: each version file owns the
# ||| migration into that version; `mod.ncl` only dispatches).
# |||
# ||| V3 adds two optional fields and reinterprets nothing, so a V2 document is
# ||| already a valid V3 document: the hop states the new version marker and
# ||| leaves every other field as the document wrote it.
let migrate_v2_to_v3_fn = fun data =>
  (drop_field "version" data) & { version = 3 }
in

{
  # ||| Validates one V3 mediapm document, filling in defaults.
  validate_document_v3 = fun document => document | _media_pm_document_v3,
  # ||| The V3 document envelope contract (unwrapped).
  envelope_contract_v3 = _media_pm_document_v3,
  # ||| Migrates one V2 document to V3 (states the new version marker; V3 adds
  # ||| only optional fields, so nothing else moves).
  migrate_v2_to_v3 = migrate_v2_to_v3_fn,
  PayloadStagingV3 = _payload_staging_v3,
  ConfigPayloadStagingV3 = _config_payload_staging_v3,
  MediaPmDocumentV3 = _media_pm_document_v3,
  ConfigVersionSpecV3 = _config_version_spec_v3,
  VersionSpecV3 = _version_spec_v3,
  ToolRequirementV3 = _tool_requirement_v3,
  MediaRuntimeStorageV3 = _media_runtime_storage_v3,
  HierarchyNodeV3 = _hierarchy_node_v3,
  MediaSourceSpecV3 = _media_source_spec_v3,
  MediaStepSpecV3 = _media_step_spec_v3,
  OutputVariantValueV3 = _output_variant_value_v3,
  SanitizeNamesConfigV3 = _sanitize_names_config_v3,
}
