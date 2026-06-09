# Export Builtin Crate

Crate: `mediapm-conductor-builtin-export`

## Parameters

- `cas_hash: String` (required): CAS hash of content to export.
- `dest: String` (required): Output file/directory/glob path.

## Contract

- **Purity**: Impure — filesystem write side effects.
- **Atomic write**: Stage to temp file, then rename (prevents partial output).
- **Overwrite control**: Policy-driven overwrite behavior.
- **Disk-full protection**: Pre-flight free-space check (payload + buffer).
- **Cleanup**: Partial files removed on failure.
