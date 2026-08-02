# Shared Nickel schema contracts for mediapm config documents (S-C8).
#
# This module provides the versioned registry surface: the supported-version
# set, the current version, the `migrate_to` edge, and unversioned aliases
# into the current version's module.  Version-local contracts are defined in
# `v1.ncl`.

let v1_migration = import "v1.ncl" in

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
# version (1).  Built with `std.contract.from_predicate` so it can be applied
# with `|` (a plain function used as a contract would receive the contract
# label instead of the value).
let SupportedVersion_pred =
  std.contract.from_predicate (fun version => version == 1)
in

# ||| Migration edge: identity when the requested version equals the
# document's version, and a loud contract failure for any other request.
# mediapm currently supports a single version, so this is a stub edge.
let migrate_to_fn = fun target_version data =>
  if data.version == target_version then
    data
  else
    data | std.contract.from_predicate (fun _ => false)
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
  current_version = (1 | SupportedVersion_pred),
  supported_versions = [1],

  migrate_to = migrate_to_fn,

  validate_document = v1_migration.validate_document_v1,
  envelope_contract = v1_migration.envelope_contract_v1,
}
