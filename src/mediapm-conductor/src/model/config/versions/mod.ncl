# Migration registry for persisted Conductor Nickel documents.
#
# Design constraints:
# - The version graph is explicit and finite.
# - Unknown versions must fail contract validation rather than coerce.
# - All version files must be present in the workspace alongside mod.ncl.

let SupportedVersion =
  std.contract.from_predicate
    (fun value => value == 1 || value == 2)
in

let v2_migration = import "v2.ncl" in
let v1_migration = import "v1.ncl" in

let migrate_to =
  fun requested_version =>
    fun data =>
      let version = data.version | SupportedVersion in
      let target = requested_version | SupportedVersion in
      if version == target then
        data
      else if version == 1 && target == 2 then
        v2_migration.migrate_v1_to_v2 data
      else
        v1_migration.migrate_v2_to_v1 data
in

let current_version = (2 | SupportedVersion) in
let supported_versions = [1, 2] in

{
  current_version,
  supported_versions,
  migrate_to,
  SupportedVersion,
}
