# Migration registry for persisted Conductor Nickel documents.
#
# Design constraints:
# - The version graph is explicit and finite.
# - Unknown versions must fail contract validation rather than coerce.
# - All version files must be present in the workspace alongside mod.ncl.
let SupportedVersion_pred =
  std.contract.from_predicate (fun value => value == 1 || value == 2)
in

let v2_migration = import "v2.ncl" in
let v1_migration = import "v1.ncl" in

let migrate_to_fn = fun requested_version =>
  fun data =>
    let version = data.version | SupportedVersion_pred in
    let target = requested_version | SupportedVersion_pred in
    if version == target then
      data
    else if version == 1 && target == 2 then
      v2_migration.migrate_v1_to_v2 data
    else
      v1_migration.migrate_v2_to_v1 data
in

let current_version_val = (2 | SupportedVersion_pred) in
let supported_versions_val = [1, 2] in

{
  current_version = current_version_val,
  supported_versions = supported_versions_val,
  migrate_to = migrate_to_fn,
  SupportedVersion = SupportedVersion_pred,
}
