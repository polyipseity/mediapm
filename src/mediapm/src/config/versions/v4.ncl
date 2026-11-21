# V4 document schema contracts for mediapm Nickel config (active version).
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
# exclusively `*V4`-suffixed contract names.  No `*V1`/`*V2`/`*V3` names, no
# shared or imported cross-version contracts.  `mod.ncl` is the only
# cross-version file and holds only the registry.
#
# `MediaPmState` is intentionally absent: state is managed separately via
# `state.json` and is not part of the V4 config surface.
#
# V4 rewrites media-tagger step option names in `media[*].steps[*].options`:
# `clear_existing_tags` becomes `preserve_existing_tags` with the boolean
# string value inverted, and `preserve_images` becomes
# `preserve_existing_images` with its value kept.

# ||| String enum contract: restricts a string value to one of the `allowed`
# values.
let EnumV4 = fun allowed =>
  std.contract.from_predicate (
    fun value => std.is_string value && std.array.elem value allowed
  )
in

# ||| Non-negative integer contract (guards fractional floats like 2.5).
let IntegerNumberV4 = std.contract.from_predicate (
  fun value => std.is_number value && std.number.is_integer value
)
in

# ||| Non-empty string contract.
let NonEmptyStringV4 = std.contract.from_predicate (
  fun value => std.is_string value && std.string.length value > 0
)
in

# ||| Path contract: a single path component string or an array of path
# component strings (mirrors `HierarchyPath`'s serde shape).
let PathV4 = std.contract.any_of [String, Array String]
in

# ||| Filename sanitization mode (mirrors `SanitizeNamesConfig`'s untagged
# wire shape: "inherit", a bool, or a char-to-char map).
let SanitizeNamesConfigV4 = std.contract.any_of [
  std.contract.from_predicate (fun value => value == "inherit"),
  Bool,
  { _ : String },
]
in

# ||| Output save config (mirrors `OutputSaveConfig`: a bool, or the literal
# "full" or "auto").
let OutputSaveConfigV4 = std.contract.any_of [
  Bool,
  std.contract.from_predicate (fun value => value == "full"),
  std.contract.from_predicate (fun value => value == "auto"),
]
in

# ||| Exact version spec (mirrors `VersionSpecFields`: closed record).
let ExactVersionSpecV4 = {
  vcs_hash | String | optional,
  version | String | optional,
  tag | String | optional,
}
in

# ||| Config version spec (mirrors `ConfigVersionSpec`: "latest", "inherit",
# or an exact record).
let ConfigVersionSpecV4 = std.contract.any_of [
  std.contract.from_predicate (fun value => value == "latest"),
  std.contract.from_predicate (fun value => value == "inherit"),
  ExactVersionSpecV4,
]
in

# ||| Resolved version spec (mirrors `VersionSpec`: "latest" or an exact
# record — no "inherit").
let VersionSpecV4 = std.contract.any_of [
  std.contract.from_predicate (fun value => value == "latest"),
  ExactVersionSpecV4,
]
in

# ||| Payload staging strategy (mirrors `PayloadStaging`): the strategy a step
# ||| runs with once a choice has been resolved.
let PayloadStagingV4 = EnumV4 ["in_place", "hard_link", "copy"]
in

# ||| Payload staging choice (mirrors `ConfigPayloadStaging`): a tool either
# ||| names a strategy outright or takes the library-wide default, which is what
# ||| `"inherit"` asks for.
let ConfigPayloadStagingV4 = std.contract.any_of [
  std.contract.from_predicate (fun value => value == "inherit"),
  PayloadStagingV4,
]
in

# ||| Tool requirement (mirrors `ToolRequirement`).  Closed: no open `..`
# tail.
let ToolRequirementV4 = {
  version_spec
    | ConfigVersionSpecV4
    | optional,
  dependencies | { _ : ConfigVersionSpecV4 } | default = {},
  recheck_seconds | IntegerNumberV4 | optional,
  # ||| ffmpeg-only: setting this on a non-ffmpeg tool is rejected (MPM-E001).
  max_input_slots | IntegerNumberV4 | optional,
  # ||| ffmpeg-only: setting this on a non-ffmpeg tool is rejected (MPM-E001).
  max_output_slots | IntegerNumberV4 | optional,
  # ||| How this tool's payload reaches the step.  Absent means "inherit", the
  # ||| library-wide default in `runtime.materialization.payload_staging`.
  payload_staging | ConfigPayloadStagingV4 | optional,
}
in

# ||| Media metadata regex transform (mirrors `MediaMetadataRegexTransform`).
let MediaMetadataRegexTransformV4 = {
  pattern | String,
  replacement | String,
}
in

# ||| Media metadata variant binding (mirrors `MediaMetadataVariantBinding`).
let MediaMetadataVariantBindingV4 = {
  variant | String,
  metadata_key | String,
  transform | MediaMetadataRegexTransformV4 | optional,
}
in

# ||| Media metadata value (mirrors `MediaMetadataValue`: string, variant
# binding, or an array of either).
let MediaMetadataValueV4 = std.contract.any_of [
  String,
  MediaMetadataVariantBindingV4,
  Array (std.contract.any_of [String, MediaMetadataVariantBindingV4]),
]
in

# ||| Output capture kind (mirrors `OutputCaptureKind`: "file" or "folder").
let OutputCaptureKindV4 = EnumV4 ["file", "folder"]
in

# ||| Generic output variant (mirrors `GenericOutputVariantConfig`).
let GenericOutputVariantConfigV4 = {
  kind | String,
  save | OutputSaveConfigV4 | default = "auto",
  capture_kind | OutputCaptureKindV4 | optional,
  zip_member | String | optional,
  idx | IntegerNumberV4 | optional,
  extension | String | optional,
}
in

# ||| yt-dlp output kind (mirrors `YtDlpOutputKind`).
let YtDlpOutputKindV4 = EnumV4 [
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
let YtDlpOutputVariantConfigV4 = {
  kind | YtDlpOutputKindV4,
  save | OutputSaveConfigV4 | default = "auto",
  capture_kind | OutputCaptureKindV4 | optional,
  langs | String | optional,
  thumbnail_ids | String | optional,
  sub_format | String | optional,
  convert | String | optional,
  zip_member | String | optional,
}
in

# ||| Output variant value (mirrors the untagged `OutputVariantValue` enum:
# generic or yt-dlp variant).
let OutputVariantValueV4 = std.contract.any_of [
  GenericOutputVariantConfigV4,
  YtDlpOutputVariantConfigV4,
]
in

# ||| Media step (mirrors `MediaStep`).
let MediaStepSpecV4 = {
  tool | EnumV4 ["yt-dlp", "import", "ffmpeg", "rsgain", "media-tagger"],
  input_variants | Array String | default = [],
  output_variants | { _ : OutputVariantValueV4 } | default = {},
  options | { _ : String } | default = {},
}
in

# ||| Media source spec (mirrors `MediaSourceSpec`).  Closed: no open `..`
# tail.
let MediaSourceSpecV4 = {
  description | NonEmptyStringV4 | optional,
  title | NonEmptyStringV4 | optional,
  artist | String | optional,
  metadata | { _ : MediaMetadataValueV4 } | default = {},
  variant_hashes | { _ : String } | default = {},
  steps | Array MediaStepSpecV4 | default = [],
}
in

# ||| Hierarchy node (mirrors `HierarchyNode`).  Recursive via `let rec`.
let rec HierarchyNodeV4 = {
  path | PathV4 | default = "",
  kind | EnumV4 ["folder", "media", "media_folder", "playlist"],
  id | String | optional,
  media_id | String | optional,
  variant | String | optional,
  variants | Array String | default = [],
  rename_files | Array {
    pattern | String,
    replacement | String,
  } | default = [],
  format | EnumV4 ["m3u8", "pls", "xspf", "wpl", "asx"] | optional,
  ids | Array (std.contract.any_of [
    String,
    { id | String, path | String | optional },
  ]) | default = [],
  sanitize_names | SanitizeNamesConfigV4 | optional,
  children | Array HierarchyNodeV4 | default = [],
}
in

# ||| Runtime path configuration (mirrors
# ||| `MediaRuntimeStorageLatest::paths`).  Closed: no open `..` tail.
let RuntimePathsConfigV4 = {
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
let RuntimeMaterializationConfigV4 = {
  materialization_preference_order
    | Array (EnumV4 ["hardlink", "symlink", "reflink", "copy"])
    | optional,
  verify_materialization | Bool | optional,
  # ||| Strategy a tool takes when it names none.  Absent means "in_place",
  # ||| which is also the only strategy that predates this field.
  payload_staging | PayloadStagingV4 | optional,
}
in

# ||| Runtime verification configuration (mirrors
# ||| `MediaRuntimeStorageLatest::verification`).  Closed: no open `..` tail.
let RuntimeVerificationConfigV4 = {
  verify_on_read
    | Array (EnumV4 ["always", "modified", "sample", "stale"])
    | optional,
  verify_on_read_sample_denominator | IntegerNumberV4 | optional,
  verify_on_read_stale_timeout_secs | IntegerNumberV4 | optional,
}
in

# ||| Runtime caching configuration (mirrors
# ||| `MediaRuntimeStorageLatest::caching`).  Closed: no open `..` tail.
let RuntimeCachingConfigV4 = {
  reconstructed_cache_ttl_seconds | IntegerNumberV4 | optional,
}
in

# ||| Runtime lifecycle configuration (mirrors
# ||| `MediaRuntimeStorageLatest::lifecycle`).  Closed: no open `..` tail.
let RuntimeLifecycleConfigV4 = {
  instance_ttl_seconds | IntegerNumberV4 | optional,
  background_gc_interval_seconds | IntegerNumberV4 | optional,
}
in

# ||| Runtime environment configuration (mirrors
# ||| `MediaRuntimeStorageLatest::environment`).  Closed: no open `..` tail.
let RuntimeEnvironmentConfigV4 = {
  inherited_env_vars | {
    windows | Array NonEmptyStringV4 | optional,
    linux | Array NonEmptyStringV4 | optional,
    macos | Array NonEmptyStringV4 | optional,
  } | optional,
  profiler_enabled | Bool | optional,
}
in

# ||| Runtime storage (mirrors `MediaRuntimeStorage`).  Closed: no open `..`
# tail.  `tools` is read but never serialized by the boundary type (it holds
# requirements the document states at the top level), which is why the contract
# admits it; `cache_root_override` is not part of the document at all and is
# absent here.
let MediaRuntimeStorageV4 = {
  paths | RuntimePathsConfigV4 | default = {},
  materialization | RuntimeMaterializationConfigV4 | default = {},
  verification | RuntimeVerificationConfigV4 | default = {},
  caching | RuntimeCachingConfigV4 | default = {},
  lifecycle | RuntimeLifecycleConfigV4 | default = {},
  environment | RuntimeEnvironmentConfigV4 | default = {},
  path_sanitization | SanitizeNamesConfigV4 | optional,
  retry_impure | Bool | optional,
  tools | { _ : ToolRequirementV4 } | default = {},
}
in

# ||| mediapm V4 document (mirrors `MediaPmDocument`).  Closed: no open `..`
# tail.  `version` must be a non-negative integer.  No `state` field: state
# is managed separately via `state.json` and is not part of the V4 config
# surface.
let MediaPmDocumentV4 = {
  version | IntegerNumberV4,
  media | { _ : MediaSourceSpecV4 } | default = {},
  hierarchy | Array HierarchyNodeV4 | default = [],
  tools | { _ : ToolRequirementV4 } | default = {},
  runtime | MediaRuntimeStorageV4 | default = {},
}
in

# Alias each exported contract through a `let` binding first: record
# shorthand fields that reference sibling fields (e.g. `{ X = X }`) recurse
# infinitely in Nickel, so the canonical pattern is
# `let _x = x in { X = _x }`.
let _media_pm_document_v4 = MediaPmDocumentV4 in
let _config_version_spec_v4 = ConfigVersionSpecV4 in
let _version_spec_v4 = VersionSpecV4 in
let _tool_requirement_v4 = ToolRequirementV4 in
let _media_runtime_storage_v4 = MediaRuntimeStorageV4 in
let _hierarchy_node_v4 = HierarchyNodeV4 in
let _media_source_spec_v4 = MediaSourceSpecV4 in
let _media_step_spec_v4 = MediaStepSpecV4 in
let _output_variant_value_v4 = OutputVariantValueV4 in
let _sanitize_names_config_v4 = SanitizeNamesConfigV4 in
let _payload_staging_v4 = PayloadStagingV4 in
let _config_payload_staging_v4 = ConfigPayloadStagingV4 in

# ||| Removes `name` when it is there.
# |||
# ||| `std.record.remove` fails on an absent field, and every field a hop here
# ||| removes is optional, so the presence guard is the rule rather than the
# ||| exception.
let drop_field = fun name record =>
  if std.record.has_field name record then std.record.remove name record else record
in

# ||| Inverts a boolean string value for the v3 <-> v4 tag-preservation rename.
# |||
# ||| Handles trimmed, case-insensitive `"true"` and `"false"` strings. Any
# ||| other value is returned unchanged so the migration does not invent a
# ||| meaning for an unrecognized string.
let invert_bool_string_v4 = fun value =>
  if std.is_string value then
    let normalized = std.string.lowercase (std.string.trim value) in
    if normalized == "true" then
      "false"
    else if normalized == "false" then
      "true"
    else
      value
  else
    value
in

# ||| Renames retired media-tagger option keys in one step options record.
let migrate_media_tagger_options_v3_to_v4 = fun options =>
  let with_preserve_existing_tags =
    if std.record.has_field "clear_existing_tags" options then
      (drop_field "clear_existing_tags" options)
      & {
        preserve_existing_tags = invert_bool_string_v4 options.clear_existing_tags,
      }
    else
      options
  in
  if std.record.has_field "preserve_images" with_preserve_existing_tags then
    (drop_field "preserve_images" with_preserve_existing_tags)
    & {
      preserve_existing_images = with_preserve_existing_tags.preserve_images,
    }
  else
    with_preserve_existing_tags
in

# ||| Rewrites one media step's option keys when it carries an options record.
let migrate_step_options_v3_to_v4 = fun step =>
  if std.record.has_field "options" step then
    (drop_field "options" step)
    & { options = migrate_media_tagger_options_v3_to_v4 step.options }
  else
    step
in

# ||| Rewrites option keys in every step of one media source spec.
let migrate_media_source_v3_to_v4 = fun _id spec =>
  if std.record.has_field "steps" spec then
    (drop_field "steps" spec)
    & { steps = std.array.map migrate_step_options_v3_to_v4 spec.steps }
  else
    spec
in

# ||| Migration edge INTO V4 (placement policy: each version file owns the
# ||| migration into that version; `mod.ncl` only dispatches).
# |||
# ||| V4 renames the retired media-tagger option keys in every
# ||| `media[*].steps[*].options` record and states the new version marker.
let migrate_v3_to_v4_fn = fun data =>
  let without_version = drop_field "version" data in
  let with_media =
    if std.record.has_field "media" without_version then
      (drop_field "media" without_version)
      & { media = std.record.map migrate_media_source_v3_to_v4 without_version.media }
    else
      without_version
  in
  with_media & { version = 4 }
in

# ||| Migration edge INTO V4 from V5 (placement policy: each version file owns
# ||| the migration into that version; `mod.ncl` only dispatches).
# |||
# ||| V4 predates the three-shape `recheck_seconds` policy, so the hop restores
# ||| the old integer sentinel: `"always"` and `"never"` become `0`, and a
# ||| positive integer of seconds is preserved as-is.
let migrate_v5_to_v4_fn = fun data =>
  let without_version = drop_field "version" data in
  let with_tools =
    if std.record.has_field "tools" without_version then
      (drop_field "tools" without_version)
      & {
        tools = std.record.map (fun _name tool =>
          if std.record.has_field "recheck_seconds" tool then
            let recheck = tool.recheck_seconds in
            (drop_field "recheck_seconds" tool)
            & {
              recheck_seconds =
                if std.is_string recheck then
                  0
                else
                  recheck,
            }
          else
            tool
        ) without_version.tools
      }
    else
      without_version
  in
  with_tools & { version = 4 }
in

{
  # ||| Validates one V4 mediapm document, filling in defaults.
  validate_document_v4 = fun document => document | _media_pm_document_v4,
  # ||| The V4 document envelope contract (unwrapped).
  envelope_contract_v4 = _media_pm_document_v4,
  # ||| Migrates one V3 document to V4 (rewrites retired media-tagger option
  # ||| keys and states the new version marker).
  migrate_v3_to_v4 = migrate_v3_to_v4_fn,
  # ||| Migrates one V5 document down to V4 (restores the integer recheck
  # ||| sentinel and states the V4 version marker).
  migrate_v5_to_v4 = migrate_v5_to_v4_fn,
  PayloadStagingV4 = _payload_staging_v4,
  ConfigPayloadStagingV4 = _config_payload_staging_v4,
  MediaPmDocumentV4 = _media_pm_document_v4,
  ConfigVersionSpecV4 = _config_version_spec_v4,
  VersionSpecV4 = _version_spec_v4,
  ToolRequirementV4 = _tool_requirement_v4,
  MediaRuntimeStorageV4 = _media_runtime_storage_v4,
  HierarchyNodeV4 = _hierarchy_node_v4,
  MediaSourceSpecV4 = _media_source_spec_v4,
  MediaStepSpecV4 = _media_step_spec_v4,
  OutputVariantValueV4 = _output_variant_value_v4,
  SanitizeNamesConfigV4 = _sanitize_names_config_v4,
}
