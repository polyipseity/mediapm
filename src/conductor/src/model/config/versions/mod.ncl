# ||| Migration registry for persisted Conductor Nickel documents.
# |||
# ||| This module defines the version graph and migration entrypoints used by
# ||| Rust when loading user/machine configuration documents. Keep behavior
# ||| deterministic: for the same input document + target version, migration
# ||| output must be byte-for-byte equivalent after Nickel normalization.
# |||
# ||| Design constraints:
# ||| - The version graph is explicit and finite (`supported_versions`).
# ||| - Atomic migrations represent exactly one hop between adjacent versions.
# ||| - `migrate_to` composes atomic hops until the requested version is reached.
# ||| - Unknown versions must fail contract validation rather than coerce.
# |||
# ||| Current repository state:
# ||| - Only schema version `1` is supported.
# ||| - Atomic migration table is intentionally empty.
# ||| - Entry points remain stable so Rust callers need no branching.

# ||| Supported persisted schema markers.
# |||
# ||| Keep this predicate narrow and explicit. When adding a new version, update
# ||| this contract and the exported `supported_versions` list in the same commit
# ||| so Rust-side invariants stay synchronized.
let SupportedVersion =
  std.contract.from_predicate
    (fun value => value == 1)
in

# ||| One declared atomic migration hop.
# |||
# ||| `from` and `to` describe a single direct edge in the migration graph.
# ||| Multi-hop upgrades are represented by multiple edges and composed by
# ||| `migrate_to_version`.
let AtomicMigrationPair = {
  from | SupportedVersion,
  to | SupportedVersion,
}
in

# ||| Declared one-hop migration edges.
# ||| Keep this table exhaustive and bidirectional when adding new schema
# ||| versions so Rust invariant tests can validate migration completeness.
# |||
# ||| Example shape when v2 exists:
# ||| [
# |||   { from = 1, to = 2 },
# |||   { from = 2, to = 1 },
# ||| ]
# |||
# ||| Keep ordering deterministic (for example sorted by `from`, then `to`).
let AtomicMigrations =
  ([] : Array AtomicMigrationPair)
in

# ||| Contract requiring one atomic hop implementation to be explicitly
# ||| declared in `migrate_atomic_version`.
# |||
# ||| This prevents silent fallback behavior when a new schema marker is added
# ||| to `SupportedVersion` but an adjacent atomic branch is forgotten.
let ImplementedAtomicMigrationStep =
  std.contract.from_predicate
    (fun value => value == true)
in

# ||| Computes the immediate adjacent target marker for one atomic migration
# ||| hop while moving from `current_version` toward `requested_version`.
# |||
# ||| This helper guarantees hop-by-hop traversal so `migrate_to_version`
# ||| always routes through `migrate_atomic_version`.
let next_atomic_target =
  fun current_version =>
    fun requested_version =>
      if current_version < requested_version then
        current_version + 1
      else if current_version > requested_version then
        current_version - 1
      else
        current_version
in

# ||| Executes exactly one atomic migration hop.
# ||| When new versions are introduced, keep branches explicit and adjacent.
# ||| Example:
# ||| branch one maps from marker 1 to marker 2 via v1_to_v2.
# ||| branch two maps from marker 2 to marker 1 via v2_to_v1.
# |||
# ||| This function should never perform multi-hop traversal; it is an internal
# ||| building block used by `migrate_to_version`.
let migrate_atomic_version =
  fun from_version =>
    let from_marker = from_version | SupportedVersion in
    fun to_version =>
      let to_marker = to_version | SupportedVersion in
      fun data =>
        if from_marker == to_marker then
          data
        else
          # ||| Add explicit per-hop migration branches here when introducing
          # ||| additional schema versions.
          # |||
          # ||| Example (when v2 exists):
          # ||| else if from_marker == 1 && to_marker == 2 then
          # |||   data & { version = 2 }
          # ||| else if from_marker == 2 && to_marker == 1 then
          # |||   data & { version = 1 }
          # ||| else
          # |||   <unimplemented atomic migration>
          let _ = false | ImplementedAtomicMigrationStep in
          data
in

# ||| Migrates any supported document to one requested target version.
# |||
# ||| Expected behavior when multiple versions exist:
# ||| 1. Validate `requested_version` is supported.
# ||| 2. Detect source `data.version`.
# ||| 3. If source == target, return data unchanged.
# ||| 4. Otherwise, apply atomic hops until target is reached.
# ||| 5. Return a value that validates against the target envelope contract.
# |||
# ||| With only v1 supported today, this function behaves as an identity for
# ||| v1 documents and fails on unsupported version markers via contracts.
let migrate_to_version =
  fun requested_version =>
    let target_version = requested_version | SupportedVersion in
    fun data =>
      let source_version = data.version | SupportedVersion in
      let rec apply_atomic_chain =
        fun current_version =>
          fun current_data =>
            if current_version == target_version then
              current_data
            else
              let hop_target = next_atomic_target current_version target_version in
              let migrated_data =
                migrate_atomic_version current_version hop_target current_data
              in
              apply_atomic_chain hop_target migrated_data
      in
      apply_atomic_chain source_version data
in

{
  # ## DO NOT REMOVE: atomic migration table guard
  #
  # - `atomic_migration_pairs` must list every one-hop migration edge.
  # - Each edge must be mirrored with a reverse edge for bidirectional checks.
  # - Rust tests consume this table to verify atomic migration behavior.
  # ||| One-hop migration edges consumed by Rust compatibility tests.
  atomic_migration_pairs = AtomicMigrations,

  # ||| Latest schema marker currently supported by this migration module.
  # ||| Rust defaults to this value when no target override is provided.
  current_version = 1,

  # ||| All schema markers recognized by this migration module.
  # ||| Keep values sorted in ascending order for predictable diffs.
  supported_versions = [1],

  # ||| Atomic migration entrypoint from source marker to target marker.
  # ||| Intended for tests and explicit one-hop execution only.
  migrate_atomic = migrate_atomic_version,

  # ||| General migration entrypoint to one requested target version.
  # ||| Preferred entrypoint for non-latest historical replays.
  migrate_to = migrate_to_version,

  # ||| Canonical migration entrypoint to the latest schema.
  # ||| Preferred default entrypoint used during normal runtime loading.
  migrate = migrate_to_version 1,

  # ||| Latest validation entrypoint (currently aliases v1).
  # ||| Keep this alias stable so callers avoid version branching.
  validate_document = (import "v1.ncl").validate_document_v1,

  # ||| Latest envelope contract entrypoint (currently aliases v1).
  # ||| Keep synchronized with `validate_document` alias target.
  envelope_contract = (import "v1.ncl").envelope_contract_v1,
}
