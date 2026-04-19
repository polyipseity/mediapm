# ||| Migration registry for persisted `mediapm.ncl` Nickel documents.
# |||
# ||| Repository state: only schema version `1` is currently supported.
# ||| Migration entrypoints are stable so Rust callers do not need branching.

let SupportedVersion =
  std.contract.from_predicate
    (fun value => value == 1)
in

let AtomicMigrationPair = {
  from | SupportedVersion,
  to | SupportedVersion,
}
in

let AtomicMigrations =
  ([] : Array AtomicMigrationPair)
in

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

let migrate_atomic_version =
  fun from_version =>
    let from_marker = from_version | SupportedVersion in
    fun to_version =>
      let to_marker = to_version | SupportedVersion in
      fun data =>
        if from_marker == to_marker then
          data
        else
          data
in

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
  atomic_migration_pairs = AtomicMigrations,
  current_version = 1,
  supported_versions = [1],
  migrate_atomic = migrate_atomic_version,
  migrate_to = migrate_to_version,
  migrate = migrate_to_version 1,

  # ||| Latest validation entrypoint (currently aliases v1).
  validate_document = (import "v1.ncl").validate_document_v1,

  # ||| Latest envelope contract entrypoint (currently aliases v1).
  envelope_contract = (import "v1.ncl").envelope_contract_v1,
}
