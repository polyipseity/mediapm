# Migration registry for persisted Conductor Nickel documents.
#
# Design constraints:
# - The version graph is explicit and finite.
# - Unknown versions must fail contract validation rather than coerce.
# - All version files must be present in the workspace alongside mod.ncl.
let SupportedVersion_pred =
  std.contract.from_predicate (fun value =>
    value == 1 || value == 2 || value == 3 || value == 4
  )
in

let v4_migration = import "v4.ncl" in
let v3_migration = import "v3.ncl" in
let v2_migration = import "v2.ncl" in
let v1_migration = import "v1.ncl" in

# Each hop is written out rather than chained through an index, so a version
# pair with no defined migration fails loudly instead of silently landing on
# some other hop's output. Every pair of supported versions is handled above;
# the final branch exists because `if` is an expression, and it is a contract
# that no value satisfies so a newly supported version cannot reach it without
# being noticed.
let UnreachableMigrationPair =
  std.contract.from_predicate (fun value => false)
in

let migrate_to_fn = fun requested_version =>
  fun data =>
    let version = data.version | SupportedVersion_pred in
    let target = requested_version | SupportedVersion_pred in
    if version == target then
      data
    else if version == 1 && target == 2 then
      v2_migration.migrate_v1_to_v2 data
    else if version == 2 && target == 1 then
      v1_migration.migrate_v2_to_v1 data
    else if version == 2 && target == 3 then
      v3_migration.migrate_v2_to_v3 data
    else if version == 3 && target == 2 then
      v2_migration.migrate_v3_to_v2 data
    else if version == 1 && target == 3 then
      v3_migration.migrate_v2_to_v3 (v2_migration.migrate_v1_to_v2 data)
    else if version == 3 && target == 1 then
      v1_migration.migrate_v2_to_v1 (v2_migration.migrate_v3_to_v2 data)
    else if version == 3 && target == 4 then
      v4_migration.migrate_v3_to_v4 data
    else if version == 4 && target == 3 then
      v3_migration.migrate_v4_to_v3 data
    else if version == 2 && target == 4 then
      v4_migration.migrate_v3_to_v4 (v3_migration.migrate_v2_to_v3 data)
    else if version == 4 && target == 2 then
      v2_migration.migrate_v3_to_v2 (v3_migration.migrate_v4_to_v3 data)
    else if version == 1 && target == 4 then
      v4_migration.migrate_v3_to_v4
        (v3_migration.migrate_v2_to_v3 (v2_migration.migrate_v1_to_v2 data))
    else if version == 4 && target == 1 then
      v1_migration.migrate_v2_to_v1
        (v2_migration.migrate_v3_to_v2 (v3_migration.migrate_v4_to_v3 data))
    else
      data | UnreachableMigrationPair
in

let current_version_val = (4 | SupportedVersion_pred) in
let supported_versions_val = [1, 2, 3, 4] in

{
  current_version = current_version_val,
  supported_versions = supported_versions_val,
  migrate_to = migrate_to_fn,
  SupportedVersion = SupportedVersion_pred,
  # ||| Unversioned alias for the latest-schema document validator.
  validate_document = v4_migration.validate_document_v4,
  # ||| Unversioned alias for the latest-schema envelope contract.
  envelope_contract = v4_migration.envelope_contract_v4,
}
