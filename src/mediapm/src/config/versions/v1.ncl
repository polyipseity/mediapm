# ||| Persisted schema v1 contract for `mediapm.ncl`.
# |||
# ||| This contract intentionally focuses on stable top-level layout and
# ||| version-marker safety while allowing runtime-side Rust validation to own
# ||| detailed semantic invariants.

let VersionOneV1 =
  std.contract.from_predicate
    (fun value => value == 1)
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
  library_dir | Dyn | optional,
  tmp_dir | Dyn | optional,
  conductor_config | Dyn | optional,
  conductor_machine_config | Dyn | optional,
  conductor_state | Dyn | optional,
  inherited_env_vars | PlatformInheritedEnvVarsV1 | optional,
  lockfile | Dyn | optional,
  env_file | Dyn | optional,
  schema_config_dir | Dyn | optional,
  use_user_download_cache | Dyn | optional,
}
in

# ||| Top-level persisted envelope for `mediapm.ncl` schema version 1.
let MediaPmDocumentV1 = {
  version | VersionOneV1,
  runtime | RuntimeStorageV1 | optional,
  tools | Dyn | optional,
  media | Dyn | optional,
  hierarchy | Dyn | optional,
}
in

{
  # ||| Validates one Nickel value as schema v1.
  validate_document_v1 = fun data => data | MediaPmDocumentV1,

  # ||| Exposes v1 envelope contract directly for diagnostics.
  envelope_contract_v1 = MediaPmDocumentV1,
}
