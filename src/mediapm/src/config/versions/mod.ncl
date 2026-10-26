# Shared Nickel schema contracts for mediapm config documents (S-C8).
#
# This module provides the versioned registry surface: the supported-version
# set, the current version, the `migrate_to` edge, and unversioned aliases
# into the current version's module.  Version-local contracts and migration
# edges are defined in `v1.ncl`, `v2.ncl` and `v3.ncl` (each version file owns
# the migration into that version; this module only dispatches).
#
# STRICT VERSION SEPARATION: this is the only cross-version file.  It holds
# only the registry (version guards, `current_version`, `supported_versions`,
# `migrate_to` dispatch, unversioned aliases `validate_document` /
# `envelope_contract`); it defines no document contracts itself.

let v1_migration = import "v1.ncl" in
let v2_migration = import "v2.ncl" in
let v3_migration = import "v3.ncl" in

# ||| Version marker contract: any non-negative integer.  Guards fractional
# floats like 1.5 which the old `%typeof%`-based contract accepted.
let VersionContract = std.contract.from_predicate (
  fun value => std.is_number value && value >= 0 && std.number.is_integer value
)
in

# ||| Generic document envelope contract.
#
# All persisted mediapm documents must carry a top-level `version` field
# matching the supported set.
let DocumentEnvelopeContract = {
  version | VersionContract,
  ..
}
in

# ||| Numeric predicate accepting exactly the currently supported document
# versions (1, 2 and 3).  Built with `std.contract.from_predicate` so it can
# be applied with `|` (a plain function used as a contract would receive the
# contract label instead of the value).
let SupportedVersion_pred =
  std.contract.from_predicate (fun version => version == 1 || version == 2 || version == 3)
in

# ||| Contract the dispatch falls through to when no arm matched.
#
# Every pair of supported versions has an arm below, so reaching it means this
# file lost one; failing loudly is what keeps a missing edge from silently
# migrating a document to the wrong shape.
let UnreachableMigrationPair =
  std.contract.from_predicate (fun _value => false)
in

# ||| Migration edge: identity when the requested version equals the
# document's version; otherwise dispatches to the target-version module that
# owns the edge.  This module never implements an edge itself.
let migrate_to_fn = fun target_version data =>
  let version = data.version | SupportedVersion_pred in
  let target = target_version | SupportedVersion_pred in
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
  else
    data | UnreachableMigrationPair
in

# Record-field shorthand (`{ VersionContract, }`) self-references the field
# and fails with "missing definition", so contract values are aliased under
# distinct names and bound explicitly.  Literal exports (current_version,
# supported_versions) are inlined directly.
let _version_contract = VersionContract in
let _document_envelope_contract = DocumentEnvelopeContract in

{
  VersionContract = _version_contract,
  DocumentEnvelopeContract = _document_envelope_contract,

  SupportedVersion = SupportedVersion_pred,
  current_version = (3 | SupportedVersion_pred),
  supported_versions = [1, 2, 3],

  migrate_to = migrate_to_fn,

  # ||| Unversioned aliases resolve to the active (V3) module.
  validate_document = v3_migration.validate_document_v3,
  envelope_contract = v3_migration.envelope_contract_v3,
}
