// Shared Nickel schema contracts for mediapm config documents.
//
// This module provides the schema contracts shared across all serialized
// document versions. Version-local overrides are defined in `v1.ncl`.

{
  // Non-negative integer string-encoded (Nickel serializes integers as Number,
  // but this contract ensures the schema document treats it as a version
  // marker).
  VersionContract = std.contract.from_predicate (
    fun value => %typeof% value == 'Number && value >= 0
  ),

  // Generic document envelope contract.
  //
  // All persisted mediapm documents must carry a top-level `version` field
  // matching the supported set.
  DocumentEnvelopeContract = {
    version | VersionContract,
    ..
  },
}
