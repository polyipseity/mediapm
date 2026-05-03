# ||| Persisted schema v1 contract for `mediapm.ncl`.
# |||
# ||| This contract intentionally focuses on stable top-level layout and
# ||| version-marker safety while allowing runtime-side Rust validation to own
# ||| detailed semantic invariants.

let VersionOneV1 =
  std.contract.from_predicate
    (fun value => value == 1)
in

# ||| Non-empty string contract used for required hierarchy ids and selectors.
let NonEmptyStringV1 =
  std.contract.from_predicate
    (fun value => std.is_string value && std.string.length value > 0)
in

# ||| Grouped runtime-storage shape for `mediapm.ncl` v1.
let PlatformInheritedEnvVarsV1 = {
  windows | Array Dyn | optional,
  linux | Array Dyn | optional,
  macos | Array Dyn | optional,
}
in

# ||| Grouped runtime-storage shape for `mediapm.ncl` v1.
let RuntimeStorageV1 = {
  mediapm_dir | Dyn | optional,
  hierarchy_root_dir | Dyn | optional,
  mediapm_tmp_dir | Dyn | optional,
  materialization_preference_order | Dyn | optional,
  conductor_config | Dyn | optional,
  conductor_machine_config | Dyn | optional,
  conductor_state_config | Dyn | optional,
  conductor_tmp_dir | Dyn | optional,
  conductor_schema_dir | Dyn | optional,
  inherited_env_vars | PlatformInheritedEnvVarsV1 | optional,
  media_state_config | Dyn | optional,
  env_file | Dyn | optional,
  mediapm_schema_dir | Dyn | optional,
  use_user_tool_cache | Dyn | optional,
}
in

# ||| Machine-managed state payload for `state.ncl` persistence.
let MediaPmStateV1 = {
  managed_files | Dyn | optional,
  tool_registry | Dyn | optional,
  active_tools | Dyn | optional,
}
in

# ||| Hierarchy node record shape for ordered node-list schema.
# ||| Tagged hierarchy-node kinds used by ordered `hierarchy = [ { ... } ]`.
let HierarchyKindFolderV1 =
  std.contract.from_predicate (fun value => value == "folder")
in
let HierarchyKindMediaV1 =
  std.contract.from_predicate (fun value => value == "media")
in
let HierarchyKindMediaFolderV1 =
  std.contract.from_predicate (fun value => value == "media_folder")
in
let HierarchyKindPlaylistV1 =
  std.contract.from_predicate (fun value => value == "playlist")
in

# ||| Playlist entry path mode selector for object-style refs.
let PlaylistEntryPathModeV1 =
  std.contract.any_of [
    std.contract.from_predicate (fun value => value == "relative"),
    std.contract.from_predicate (fun value => value == "absolute"),
  ]
in

# ||| Playlist ids support string shorthand (`"id"`) and
# ||| object form (`{ id = "...", path = "relative"|"absolute" }`).
let PlaylistItemRefV1 =
  std.contract.any_of [
    NonEmptyStringV1,
    {
      id | NonEmptyStringV1,
      path | PlaylistEntryPathModeV1 | optional,
    },
  ]
in

# ||| Hierarchy node tagged union for v1.
# ||| - `folder` nodes may omit `kind` (default folder behavior).
# ||| - `media` and `media_folder` nodes require explicit hierarchy `id` and
# |||   `media_id`.
# ||| - `playlist` nodes resolve ordered members by hierarchy-node ids.
let rec HierarchyNodeV1 =
  std.contract.any_of [
    {
      path | String,
      kind | HierarchyKindFolderV1 | optional,
      children | Array HierarchyNodeV1 | optional,
    },
    {
      path | String,
      kind | HierarchyKindMediaV1,
      id | NonEmptyStringV1,
      media_id | NonEmptyStringV1,
      variant | NonEmptyStringV1,
      children | Array HierarchyNodeV1 | optional,
    },
    {
      path | String,
      kind | HierarchyKindMediaFolderV1,
      id | NonEmptyStringV1,
      media_id | NonEmptyStringV1,
      variants | Array NonEmptyStringV1,
      rename_files | Array Dyn | optional,
      children | Array HierarchyNodeV1 | optional,
    },
    {
      path | String,
      kind | HierarchyKindPlaylistV1,
      format | Dyn | optional,
      ids | Array PlaylistItemRefV1,
      children | Array HierarchyNodeV1 | optional,
    },
  ]
in

# ||| Top-level persisted envelope for `mediapm.ncl` schema version 1.
let MediaPmDocumentV1 = {
  version | VersionOneV1,
  runtime | RuntimeStorageV1 | optional,
  tools | Dyn | optional,
  media | Dyn | optional,
  # ||| Hierarchy payload is one ordered array of recursive node objects.
  # ||| Runtime intentionally rejects legacy map-based hierarchy forms.
  hierarchy | Array HierarchyNodeV1 | optional,
  # ||| Machine-managed realized state document payload.
  state | MediaPmStateV1 | optional,
}
in

{
  # ||| Validates one Nickel value as schema v1.
  validate_document_v1 = fun data => data | MediaPmDocumentV1,

  # ||| Exposes v1 envelope contract directly for diagnostics.
  envelope_contract_v1 = MediaPmDocumentV1,
}
