# V5 document schema contracts for mediapm Nickel config (active version).
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
# exclusively `*V5`-suffixed contract names.  No `*V1`/`*V2`/`*V3` names, no
# shared or imported cross-version contracts.  `mod.ncl` is the only
# cross-version file and holds only the registry.
#
# `MediaPmState` is intentionally absent: state is managed separately via
# `state.json` and is not part of the V5 config surface.
#
# V5 defines `recheck_seconds` as a three-shape policy: the string `"always"`,
# the string `"never"`, or a positive integer number of seconds. Version marker
# is 5. The v4->v5 migration maps legacy `recheck_seconds = 0` to `"always"`.

# ||| String enum contract: restricts a string value to one of the `allowed`
# values.
let EnumV5 = fun allowed =>
  std.contract.from_predicate (
    fun value => std.is_string value && std.array.elem value allowed
  )
in

# ||| Non-negative integer contract (guards fractional floats like 2.5).
let IntegerNumberV5 = std.contract.from_predicate (
  fun value => std.is_number value && std.number.is_integer value
)
in

# ||| Non-empty string contract.
let NonEmptyStringV5 = std.contract.from_predicate (
  fun value => std.is_string value && std.string.length value > 0
)
in

# ||| Path contract: a single path component string or an array of path
# component strings (mirrors `HierarchyPath`'s serde shape).
let PathV5 = std.contract.any_of [String, Array String]
in

# ||| Filename sanitization mode (mirrors `SanitizeNamesConfig`'s untagged
# wire shape: "inherit", a bool, or a char-to-char map).
let SanitizeNamesConfigV5 = std.contract.any_of [
  std.contract.from_predicate (fun value => value == "inherit"),
  Bool,
  { _ : String },
]
in

# ||| Output save config (mirrors `OutputSaveConfig`: a bool, or the literal
# "full" or "auto").
let OutputSaveConfigV5 = std.contract.any_of [
  Bool,
  std.contract.from_predicate (fun value => value == "full"),
  std.contract.from_predicate (fun value => value == "auto"),
]
in

# ||| Exact version spec (mirrors `VersionSpecFields`: closed record).
let ExactVersionSpecV5 = {
  vcs_hash | String | optional,
  version | String | optional,
  tag | String | optional,
}
in

# ||| Config version spec (mirrors `ConfigVersionSpec`: "latest", "inherit",
# or an exact record).
let ConfigVersionSpecV5 = std.contract.any_of [
  std.contract.from_predicate (fun value => value == "latest"),
  std.contract.from_predicate (fun value => value == "inherit"),
  ExactVersionSpecV5,
]
in

# ||| Resolved version spec (mirrors `VersionSpec`: "latest" or an exact
# record — no "inherit").
let VersionSpecV5 = std.contract.any_of [
  std.contract.from_predicate (fun value => value == "latest"),
  ExactVersionSpecV5,
]
in

# ||| Payload staging strategy (mirrors `PayloadStaging`): the strategy a step
# ||| runs with once a choice has been resolved.
let PayloadStagingV5 = EnumV5 ["in_place", "hard_link", "copy"]
in

# ||| Payload staging choice (mirrors `ConfigPayloadStaging`): a tool either
# ||| names a strategy outright or takes the library-wide default, which is what
# ||| `"inherit"` asks for.
let ConfigPayloadStagingV5 = std.contract.any_of [
  std.contract.from_predicate (fun value => value == "inherit"),
  PayloadStagingV5,
]
in

# ||| When a sync may reuse a tool deployment instead of resolving again.
# ||| Mirrors `ToolRecheck`: two named forms and a positive integer of seconds.
let ToolRecheckSpellingV5 = std.contract.from_predicate (
  fun value => value == "always" || value == "never"
)
in

# ||| When a sync may reuse a tool deployment instead of resolving again.
# ||| Mirrors `ToolRecheck`: two named forms and a positive integer of seconds.
let ToolRecheckV5 = std.contract.any_of [
  ToolRecheckSpellingV5,
  std.contract.from_predicate (fun value =>
    std.is_number value && std.number.floor value == value && value >= 1
  ),
]
in

# ||| Tool requirement (mirrors `ToolRequirement`).  Closed: no open `..`
# tail.
let ToolRequirementV5 = {
  version_spec
    | ConfigVersionSpecV5
    | optional,
  dependencies | { _ : ConfigVersionSpecV5 } | default = {},
  recheck_seconds | ToolRecheckV5 | optional,
  # ||| ffmpeg-only: setting this on a non-ffmpeg tool is rejected (MPM-E001).
  max_input_slots | IntegerNumberV5 | optional,
  # ||| ffmpeg-only: setting this on a non-ffmpeg tool is rejected (MPM-E001).
  max_output_slots | IntegerNumberV5 | optional,
  # ||| How this tool's payload reaches the step.  Absent means "inherit", the
  # ||| library-wide default in `runtime.materialization.payload_staging`.
  payload_staging | ConfigPayloadStagingV5 | optional,
}
in

# ||| Media metadata regex transform (mirrors `MediaMetadataRegexTransform`).
let MediaMetadataRegexTransformV5 = {
  pattern | String,
  replacement | String,
}
in

# ||| Media metadata variant binding (mirrors `MediaMetadataVariantBinding`).
let MediaMetadataVariantBindingV5 = {
  variant | String,
  metadata_key | String,
  transform | MediaMetadataRegexTransformV5 | optional,
}
in

# ||| Media metadata value (mirrors `MediaMetadataValue`: string, variant
# binding, or an array of either).
let MediaMetadataValueV5 = std.contract.any_of [
  String,
  MediaMetadataVariantBindingV5,
  Array (std.contract.any_of [String, MediaMetadataVariantBindingV5]),
]
in

# ||| Output capture kind (mirrors `OutputCaptureKind`: "file" or "folder").
let OutputCaptureKindV5 = EnumV5 ["file", "folder"]
in

# ||| Generic output variant (mirrors `GenericOutputVariantConfig`).
let GenericOutputVariantConfigV5 = {
  kind | String,
  save | OutputSaveConfigV5 | default = "auto",
  capture_kind | OutputCaptureKindV5 | optional,
  zip_member | String | optional,
  idx | IntegerNumberV5 | optional,
  extension | String | optional,
}
in

# ||| yt-dlp output kind (mirrors `YtDlpOutputKind`).
let YtDlpOutputKindV5 = EnumV5 [
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
let YtDlpOutputVariantConfigV5 = {
  kind | YtDlpOutputKindV5,
  save | OutputSaveConfigV5 | default = "auto",
  capture_kind | OutputCaptureKindV5 | optional,
  langs | String | optional,
  thumbnail_ids | String | optional,
  sub_format | String | optional,
  convert | String | optional,
  zip_member | String | optional,
}
in

# ||| Output variant value (mirrors the untagged `OutputVariantValue` enum:
# generic or yt-dlp variant).
let OutputVariantValueV5 = std.contract.any_of [
  GenericOutputVariantConfigV5,
  YtDlpOutputVariantConfigV5,
]
in

# ||| Media step (mirrors `MediaStep`).
let MediaStepSpecV5 = {
  tool | EnumV5 ["yt-dlp", "import", "ffmpeg", "rsgain", "media-tagger"],
  input_variants | Array String | default = [],
  output_variants | { _ : OutputVariantValueV5 } | default = {},
  options | { _ : String } | default = {},
}
in

# ||| Media source spec (mirrors `MediaSourceSpec`).  Closed: no open `..`
# tail.
let MediaSourceSpecV5 = {
  description | NonEmptyStringV5 | optional,
  title | NonEmptyStringV5 | optional,
  artist | String | optional,
  metadata | { _ : MediaMetadataValueV5 } | default = {},
  variant_hashes | { _ : String } | default = {},
  steps | Array MediaStepSpecV5 | default = [],
}
in

# ||| How a playlist renders an entry's path: relative to the playlist, or from
# ||| the library root. A word outside this set fails at load rather than
# ||| silently meaning relative.
let PlaylistOutputModeV5 = std.contract.from_predicate (
  fun v => v == "relative" || v == "absolute"
)
in

# ||| Hierarchy node (mirrors `HierarchyNode`).  Recursive via `let rec`.
let rec HierarchyNodeV5 = {
  path | PathV5 | default = "",
  kind | EnumV5 ["folder", "media", "media_folder", "playlist"],
  id | String | optional,
  media_id | String | optional,
  variant | String | optional,
  variants | Array String | default = [],
  rename_files | Array {
    pattern | String,
    replacement | String,
  } | default = [],
  format | EnumV5 ["m3u8", "pls", "xspf", "wpl", "asx"] | optional,
  ids | Array (std.contract.any_of [
    String,
    {
      id | String,
      # ||| How the entry's path is rendered: relative to the playlist, or from the
      # ||| library root. A word outside this set fails at load rather than silently
      # ||| meaning relative.
      path | PlaylistOutputModeV5 | optional,
    },
  ]) | default = [],
  sanitize_names | SanitizeNamesConfigV5 | optional,
  children | Array HierarchyNodeV5 | default = [],
}
in

# ||| Runtime path configuration (mirrors
# ||| `MediaRuntimeStorageLatest::paths`).  Closed: no open `..` tail.
let RuntimePathsConfigV5 = {
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
let RuntimeMaterializationConfigV5 = {
  materialization_preference_order
    | Array (EnumV5 ["hardlink", "symlink", "reflink", "copy"])
    | optional,
  verify_materialization | Bool | optional,
  # ||| Strategy a tool takes when it names none.  Absent means "in_place",
  # ||| which is also the only strategy that predates this field.
  payload_staging | PayloadStagingV5 | optional,
}
in

# ||| Runtime verification configuration (mirrors
# ||| `MediaRuntimeStorageLatest::verification`).  Closed: no open `..` tail.
let RuntimeVerificationConfigV5 = {
  verify_on_read
    | Array (EnumV5 ["always", "modified", "sample", "stale"])
    | optional,
  verify_on_read_sample_denominator | IntegerNumberV5 | optional,
  verify_on_read_stale_timeout_secs | IntegerNumberV5 | optional,
}
in

# ||| Runtime caching configuration (mirrors
# ||| `MediaRuntimeStorageLatest::caching`).  Closed: no open `..` tail.
let RuntimeCachingConfigV5 = {
  reconstructed_cache_ttl_seconds | IntegerNumberV5 | optional,
}
in

# ||| Runtime lifecycle configuration (mirrors
# ||| `MediaRuntimeStorageLatest::lifecycle`).  Closed: no open `..` tail.
let RuntimeLifecycleConfigV5 = {
  instance_ttl_seconds | IntegerNumberV5 | optional,
  background_gc_interval_seconds | IntegerNumberV5 | optional,
}
in

# ||| Runtime environment configuration (mirrors
# ||| `MediaRuntimeStorageLatest::environment`).  Closed: no open `..` tail.
let RuntimeEnvironmentConfigV5 = {
  inherited_env_vars | {
    windows | Array NonEmptyStringV5 | optional,
    linux | Array NonEmptyStringV5 | optional,
    macos | Array NonEmptyStringV5 | optional,
  } | optional,
  profiler_enabled | Bool | optional,
}
in

# ||| Runtime storage (mirrors `MediaRuntimeStorage`).  Closed: no open `..`
# tail.  `tools` is read but never serialized by the boundary type (it holds
# requirements the document states at the top level), which is why the contract
# admits it; `cache_root_override` is not part of the document at all and is
# absent here.
let MediaRuntimeStorageV5 = {
  paths | RuntimePathsConfigV5 | default = {},
  materialization | RuntimeMaterializationConfigV5 | default = {},
  verification | RuntimeVerificationConfigV5 | default = {},
  caching | RuntimeCachingConfigV5 | default = {},
  lifecycle | RuntimeLifecycleConfigV5 | default = {},
  environment | RuntimeEnvironmentConfigV5 | default = {},
  path_sanitization | SanitizeNamesConfigV5 | optional,
  retry_impure | Bool | optional,
  tools | { _ : ToolRequirementV5 } | default = {},
}
in

# ||| mediapm V5 document (mirrors `MediaPmDocument`).  Closed: no open `..`
# tail.  `version` must be a non-negative integer.  No `state` field: state
# is managed separately via `state.json` and is not part of the V5 config
# surface.
let MediaPmDocumentV5 = {
  version | IntegerNumberV5,
  media | { _ : MediaSourceSpecV5 } | default = {},
  hierarchy | Array HierarchyNodeV5 | default = [],
  tools | { _ : ToolRequirementV5 } | default = {},
  runtime | MediaRuntimeStorageV5 | default = {},
}
in

# Alias each exported contract through a `let` binding first: record
# shorthand fields that reference sibling fields (e.g. `{ X = X }`) recurse
# infinitely in Nickel, so the canonical pattern is
# `let _x = x in { X = _x }`.
let _media_pm_document_v5 = MediaPmDocumentV5 in
let _config_version_spec_v5 = ConfigVersionSpecV5 in
let _version_spec_v5 = VersionSpecV5 in
let _tool_requirement_v5 = ToolRequirementV5 in
let _media_runtime_storage_v5 = MediaRuntimeStorageV5 in
let _hierarchy_node_v5 = HierarchyNodeV5 in
let _media_source_spec_v5 = MediaSourceSpecV5 in
let _media_step_spec_v5 = MediaStepSpecV5 in
let _output_variant_value_v5 = OutputVariantValueV5 in
let _sanitize_names_config_v5 = SanitizeNamesConfigV5 in
let _payload_staging_v5 = PayloadStagingV5 in
let _config_payload_staging_v5 = ConfigPayloadStagingV5 in

# ||| Removes `name` when it is there.
# |||
# ||| `std.record.remove` fails on an absent field, and every field a hop here
# ||| removes is optional, so the presence guard is the rule rather than the
# ||| exception.
let drop_field = fun name record =>
  if std.record.has_field name record then std.record.remove name record else record
in

# ||| Migration edge INTO V5 (placement policy: each version file owns the
# ||| migration into that version; `mod.ncl` only dispatches).
# |||
# ||| V5 widens `recheck_seconds` from an integer to three shapes and states the
# ||| new version marker. The field keeps its name. The old `0` sentinel meant
# ||| "no explicit interval", which is what `always` now says.
let migrate_v4_to_v5_fn = fun data =>
  let without_version = drop_field "version" data in
  let with_tools =
    if std.record.has_field "tools" without_version then
      (drop_field "tools" without_version)
      & {
        tools = std.record.map (fun _name tool =>
          if std.record.has_field "recheck_seconds" tool then
            let legacy = tool.recheck_seconds in
            (drop_field "recheck_seconds" tool)
            & {
              recheck_seconds =
                if legacy == 0 then
                  "always"
                else
                  legacy,
            }
          else
            tool
        ) without_version.tools
      }
    else
      without_version
  in
  with_tools & { version = 5 }
in

{
  # ||| Validates one V5 mediapm document, filling in defaults.
  validate_document_v5 = fun document => document | _media_pm_document_v5,
  # ||| The V5 document envelope contract (unwrapped).
  envelope_contract_v5 = _media_pm_document_v5,
  # ||| Migrates one V4 document to V5 (widens `recheck_seconds` and states the
  # ||| new version marker).
  migrate_v4_to_v5 = migrate_v4_to_v5_fn,
  PayloadStagingV5 = _payload_staging_v5,
  ConfigPayloadStagingV5 = _config_payload_staging_v5,
  MediaPmDocumentV5 = _media_pm_document_v5,
  ConfigVersionSpecV5 = _config_version_spec_v5,
  VersionSpecV5 = _version_spec_v5,
  ToolRequirementV5 = _tool_requirement_v5,
  MediaRuntimeStorageV5 = _media_runtime_storage_v5,
  HierarchyNodeV5 = _hierarchy_node_v5,
  MediaSourceSpecV5 = _media_source_spec_v5,
  MediaStepSpecV5 = _media_step_spec_v5,
  OutputVariantValueV5 = _output_variant_value_v5,
  SanitizeNamesConfigV5 = _sanitize_names_config_v5,
}
