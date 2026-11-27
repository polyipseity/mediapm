# Export Builtin Crate

Crate: `mediapm-conductor-builtin-export`

## Parameters

- `cas_hash: String` (required): CAS hash to export.
- `dest: String` (required): output file/directory/glob path.

## Contract

- Impure: filesystem writes. Stage to temp then rename (no partial output).
- Overwrite behavior policy-driven. Preflight free space (payload + buffer).
- Remove partial files on failure or full disk.
