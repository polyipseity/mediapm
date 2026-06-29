// V1 document schema contracts for mediapm Nickel config.
//
// These contracts describe the user-facing and runtime-persisted shapes for
// V1 documents.

let shared = import "mod.ncl" in

let ToolRequirementV1 = {
  version | String | { .. } | optional,
  tag | String | optional,
  dependencies | {
    ffmpeg_version | String | { .. } | optional,
    deno_version | String | { .. } | optional,
    sd_version | String | { .. } | optional,
    ..
  } | default = {},
  recheck_seconds | Number | optional,
  max_input_slots | Number | optional,
  max_output_slots | Number | optional,
  ..
}
in

{
  // MediaPmDocument V1 envelope for `mediapm.ncl` user config.
  MediaPmDocumentV1 = {
    version | shared.VersionContract,
    media | { .. },
    hierarchy
      | Array
        {
          path
            | String
            | Array String
            | default
            = "",
          kind
            | std.enum.TagOrString
            | default
            = `folder,
          id | String | optional,
          media_id | String | optional,
          variant | String | optional,
          variants | Array String | default = [],
          rename_files
            | Array
              {
                pattern | String,
                replacement | String,
              }
            | default
            = [],
          format
            | std.enum.TagOrString
            | default
            = `m3u8,
          ids
            | Array
              (
                String
                | {
                  id | String,
                  path | String | optional,
                }
              )
            | default
            = [],
          sanitize_names
            | String
            | { : Str }
            | default
            = "inherit",
          children
            | Array Dyn
            | default
            = [],
        }
      | default
    = [],
    tools | { : ToolRequirementV1 } | default = {},
    runtime | {
      mediapm_dir | String | optional,
      materialization_preference_order
        | Array (
            String
            | {
              method | String,
              ..
            }
          )
        | optional,
      media_state_config | { .. } | optional,
      ..
    }
      | default
    = {},
  },

  // MediaPmState V1 envelope for `<mediapm_dir>/state.ncl`.
  MediaPmStateV1 = {
    version | shared.VersionContract,
    media_state
      | {
        ..
          | {
            variant_hashes
              | {
                ..
                  | String,
              }
              | optional,
            steps_completed | Number | optional,
            last_impure_sync_at | Number | optional,
          }
      }
      | default
    = {},
    tools
      | {
        ..
          | {
            version | String | optional,
            tag | String | optional,
          }
      }
      | default
    = {},
    ..
  },
}
