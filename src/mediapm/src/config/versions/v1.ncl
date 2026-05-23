# ||| Persisted schema v1 contract for `mediapm.ncl`.
# |||
# ||| This contract intentionally focuses on stable top-level layout and
# ||| version-marker safety while allowing runtime-side Rust validation to own
# ||| detailed semantic invariants not captured by schema-level contracts.

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

# ||| Tool option values are scalar strings only.
let ToolOptionValueV1 = String
in

# ||| Tool selector contracts used by media-step tagged unions.
let MediaStepToolYtDlpV1 =
  std.contract.from_predicate (fun value => value == "yt-dlp")
in
let MediaStepToolImportV1 =
  std.contract.from_predicate (fun value => value == "import")
in
let MediaStepToolFfmpegV1 =
  std.contract.from_predicate (fun value => value == "ffmpeg")
in
let MediaStepToolRsgainV1 =
  std.contract.from_predicate (fun value => value == "rsgain")
in
let MediaStepToolMediaTaggerV1 =
  std.contract.from_predicate (fun value => value == "media-tagger")
in

# ||| Tool-specific option contracts for `media.<id>.steps[].options`.
let MediaStepOptionsYtDlpV1 = {
  uri | ToolOptionValueV1 | optional,
  leading_args | ToolOptionValueV1 | optional,
  trailing_args | ToolOptionValueV1 | optional,
  option_args | ToolOptionValueV1 | optional,
  format | ToolOptionValueV1 | optional,
  format_sort | ToolOptionValueV1 | optional,
  extract_audio | ToolOptionValueV1 | optional,
  audio_format | ToolOptionValueV1 | optional,
  audio_quality | ToolOptionValueV1 | optional,
  remux_video | ToolOptionValueV1 | optional,
  recode_video | ToolOptionValueV1 | optional,
  convert_subs | ToolOptionValueV1 | optional,
  convert_thumbnails | ToolOptionValueV1 | optional,
  merge_output_format | ToolOptionValueV1 | optional,
  embed_thumbnail | ToolOptionValueV1 | optional,
  embed_metadata | ToolOptionValueV1 | optional,
  embed_subs | ToolOptionValueV1 | optional,
  embed_chapters | ToolOptionValueV1 | optional,
  embed_info_json | ToolOptionValueV1 | optional,
  write_subs | ToolOptionValueV1 | optional,
  sub_langs | ToolOptionValueV1 | optional,
  sub_format | ToolOptionValueV1 | optional,
  write_thumbnail | ToolOptionValueV1 | optional,
  write_all_thumbnails | ToolOptionValueV1 | optional,
  write_info_json | ToolOptionValueV1 | optional,
  clean_info_json | ToolOptionValueV1 | optional,
  write_comments | ToolOptionValueV1 | optional,
  write_description | ToolOptionValueV1 | optional,
  write_annotations | ToolOptionValueV1 | optional,
  write_link | ToolOptionValueV1 | optional,
  write_url_link | ToolOptionValueV1 | optional,
  write_webloc_link | ToolOptionValueV1 | optional,
  write_desktop_link | ToolOptionValueV1 | optional,
  write_chapters | ToolOptionValueV1 | optional,
  split_chapters | ToolOptionValueV1 | optional,
  playlist_items | ToolOptionValueV1 | optional,
  no_playlist | ToolOptionValueV1 | optional,
  skip_download | ToolOptionValueV1 | optional,
  retries | ToolOptionValueV1 | optional,
  limit_rate | ToolOptionValueV1 | optional,
  concurrent_fragments | ToolOptionValueV1 | optional,
  proxy | ToolOptionValueV1 | optional,
  socket_timeout | ToolOptionValueV1 | optional,
  sleep_subtitles | ToolOptionValueV1 | optional,
  user_agent | ToolOptionValueV1 | optional,
  referer | ToolOptionValueV1 | optional,
  add_header | ToolOptionValueV1 | optional,
  cookies | ToolOptionValueV1 | optional,
  cookies_from_browser | ToolOptionValueV1 | optional,
  cache_dir | ToolOptionValueV1 | optional,
  ffmpeg_location | ToolOptionValueV1 | optional,
  paths | ToolOptionValueV1 | optional,
  output | ToolOptionValueV1 | optional,
  parse_metadata | ToolOptionValueV1 | optional,
  replace_in_metadata | ToolOptionValueV1 | optional,
  download_sections | ToolOptionValueV1 | optional,
  postprocessor_args | ToolOptionValueV1 | optional,
  extractor_args | ToolOptionValueV1 | optional,
  http_chunk_size | ToolOptionValueV1 | optional,
  download_archive | ToolOptionValueV1 | optional,
  sponsorblock_mark | ToolOptionValueV1 | optional,
  sponsorblock_remove | ToolOptionValueV1 | optional,
}
in

let MediaStepOptionsImportV1 = {
  kind | ToolOptionValueV1 | optional,
  hash | ToolOptionValueV1 | optional,
}
in

let MediaStepOptionsFfmpegV1 = {
  leading_args | ToolOptionValueV1 | optional,
  trailing_args | ToolOptionValueV1 | optional,
  option_args | ToolOptionValueV1 | optional,
  audio_codec | ToolOptionValueV1 | optional,
  video_codec | ToolOptionValueV1 | optional,
  container | ToolOptionValueV1 | optional,
  audio_bitrate | ToolOptionValueV1 | optional,
  video_bitrate | ToolOptionValueV1 | optional,
  audio_quality | ToolOptionValueV1 | optional,
  video_quality | ToolOptionValueV1 | optional,
  crf | ToolOptionValueV1 | optional,
  preset | ToolOptionValueV1 | optional,
  threads | ToolOptionValueV1 | optional,
  log_level | ToolOptionValueV1 | optional,
  progress | ToolOptionValueV1 | optional,
  tune | ToolOptionValueV1 | optional,
  profile | ToolOptionValueV1 | optional,
  level | ToolOptionValueV1 | optional,
  pixel_format | ToolOptionValueV1 | optional,
  frame_rate | ToolOptionValueV1 | optional,
  sample_rate | ToolOptionValueV1 | optional,
  channels | ToolOptionValueV1 | optional,
  audio_filters | ToolOptionValueV1 | optional,
  video_filters | ToolOptionValueV1 | optional,
  filter_complex | ToolOptionValueV1 | optional,
  start_time | ToolOptionValueV1 | optional,
  duration | ToolOptionValueV1 | optional,
  to | ToolOptionValueV1 | optional,
  movflags | ToolOptionValueV1 | optional,
  map_metadata | ToolOptionValueV1 | optional,
  map_chapters | ToolOptionValueV1 | optional,
  map | ToolOptionValueV1 | optional,
  map_channel | ToolOptionValueV1 | optional,
  copy_ts | ToolOptionValueV1 | optional,
  start_at_zero | ToolOptionValueV1 | optional,
  stats | ToolOptionValueV1 | optional,
  no_overwrite | ToolOptionValueV1 | optional,
  codec_copy | ToolOptionValueV1 | optional,
  faststart | ToolOptionValueV1 | optional,
  hwaccel | ToolOptionValueV1 | optional,
  sample_format | ToolOptionValueV1 | optional,
  channel_layout | ToolOptionValueV1 | optional,
  metadata | ToolOptionValueV1 | optional,
  timestamp | ToolOptionValueV1 | optional,
  disposition | ToolOptionValueV1 | optional,
  fps_mode | ToolOptionValueV1 | optional,
  force_key_frames | ToolOptionValueV1 | optional,
  aspect | ToolOptionValueV1 | optional,
  stream_loop | ToolOptionValueV1 | optional,
  max_muxing_queue_size | ToolOptionValueV1 | optional,
  strict | ToolOptionValueV1 | optional,
  maxrate | ToolOptionValueV1 | optional,
  bufsize | ToolOptionValueV1 | optional,
  bitstream_filter | ToolOptionValueV1 | optional,
  shortest | ToolOptionValueV1 | optional,
  vn | ToolOptionValueV1 | optional,
  an | ToolOptionValueV1 | optional,
  sn | ToolOptionValueV1 | optional,
  dn | ToolOptionValueV1 | optional,
  id3v2_version | ToolOptionValueV1 | optional,
}
in

let MediaStepOptionsRsgainV1 = {
  leading_args | ToolOptionValueV1 | optional,
  trailing_args | ToolOptionValueV1 | optional,
  option_args | ToolOptionValueV1 | optional,
  mode | ToolOptionValueV1 | optional,
  album | ToolOptionValueV1 | optional,
  album_aes77 | ToolOptionValueV1 | optional,
  skip_existing | ToolOptionValueV1 | optional,
  tagmode | ToolOptionValueV1 | optional,
  loudness | ToolOptionValueV1 | optional,
  target_lufs | ToolOptionValueV1 | optional,
  clip_mode | ToolOptionValueV1 | optional,
  true_peak | ToolOptionValueV1 | optional,
  dual_mono | ToolOptionValueV1 | optional,
  album_mode | ToolOptionValueV1 | optional,
  max_peak | ToolOptionValueV1 | optional,
  lowercase | ToolOptionValueV1 | optional,
  id3v2_version | ToolOptionValueV1 | optional,
  opus_mode | ToolOptionValueV1 | optional,
  jobs | ToolOptionValueV1 | optional,
  multithread | ToolOptionValueV1 | optional,
  preset | ToolOptionValueV1 | optional,
  dry_run | ToolOptionValueV1 | optional,
  output | ToolOptionValueV1 | optional,
  quiet | ToolOptionValueV1 | optional,
  skip_tags | ToolOptionValueV1 | optional,
  preserve_mtime | ToolOptionValueV1 | optional,
  preserve_mtimes | ToolOptionValueV1 | optional,
  input_extension | ToolOptionValueV1 | optional,
}
in

let MediaStepOptionsMediaTaggerV1 = {
  leading_args | ToolOptionValueV1 | optional,
  trailing_args | ToolOptionValueV1 | optional,
  option_args | ToolOptionValueV1 | optional,
  acoustid_endpoint | ToolOptionValueV1 | optional,
  musicbrainz_endpoint | ToolOptionValueV1 | optional,
  cache_dir | ToolOptionValueV1 | optional,
  cache_expiry_seconds | ToolOptionValueV1 | optional,
  strict_identification | ToolOptionValueV1 | optional,
  write_all_tags | ToolOptionValueV1 | optional,
  write_all_images | ToolOptionValueV1 | optional,
  recording_mbid | ToolOptionValueV1 | optional,
  release_mbid | ToolOptionValueV1 | optional,
  output_container | ToolOptionValueV1 | optional,
}
in

# ||| Tool-specific step contracts.
let MediaStepYtDlpV1 = {
  tool | MediaStepToolYtDlpV1,
  input_variants | Array String | optional,
  output_variants | Dyn,
  options | MediaStepOptionsYtDlpV1 | optional,
}
in

let MediaStepImportV1 = {
  tool | MediaStepToolImportV1,
  input_variants | Array String | optional,
  output_variants | Dyn,
  options | MediaStepOptionsImportV1 | optional,
}
in

let MediaStepFfmpegV1 = {
  tool | MediaStepToolFfmpegV1,
  input_variants | Array String | optional,
  output_variants | Dyn,
  options | MediaStepOptionsFfmpegV1 | optional,
}
in

let MediaStepRsgainV1 = {
  tool | MediaStepToolRsgainV1,
  input_variants | Array String | optional,
  output_variants | Dyn,
  options | MediaStepOptionsRsgainV1 | optional,
}
in

let MediaStepMediaTaggerV1 = {
  tool | MediaStepToolMediaTaggerV1,
  input_variants | Array String | optional,
  output_variants | Dyn,
  options | MediaStepOptionsMediaTaggerV1 | optional,
}
in

let MediaStepV1 =
  std.contract.any_of [
    MediaStepYtDlpV1,
    MediaStepImportV1,
    MediaStepFfmpegV1,
    MediaStepRsgainV1,
    MediaStepMediaTaggerV1,
  ]
in

let MediaSourceV1 = {
  id | NonEmptyStringV1 | optional,
  title | String | optional,
  description | String | optional,
  workflow_id | String | optional,
  metadata | Dyn | optional,
  variant_hashes | { _ : String } | optional,
  steps | Array MediaStepV1 | optional,
}
in

# ||| Machine-managed impure timestamp shape for workflow refresh state.
let MediaPmImpureTimestampV1 = {
  epoch_seconds | Dyn,
  subsec_nanos | Dyn,
}
in

# ||| One machine-managed workflow step refresh snapshot.
let ManagedWorkflowStepStateV1 = {
  explicit_config | Dyn,
  impure_timestamp | MediaPmImpureTimestampV1 | optional,
}
in

# ||| Machine-managed state payload for `state.ncl` persistence.
let MediaPmStateV1 = {
  managed_files | Dyn | optional,
  tool_registry | Dyn | optional,
  active_tools | Dyn | optional,
  workflow_states | { _ : Array ManagedWorkflowStepStateV1 } | optional,
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
  media | { _ : MediaSourceV1 } | optional,
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

  # ||| Exposes tool-specific option contracts for schema-aware UX and docs.
  tool_option_contracts_v1 = {
    yt_dlp = MediaStepOptionsYtDlpV1,
    "import" = MediaStepOptionsImportV1,
    ffmpeg = MediaStepOptionsFfmpegV1,
    rsgain = MediaStepOptionsRsgainV1,
    media_tagger = MediaStepOptionsMediaTaggerV1,
  },
}
