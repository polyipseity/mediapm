# Fs Builtin Crate

Crate: `mediapm-conductor-builtin-fs`

## Parameters

- `operation: String` (required): One of `read`, `write`, `remove`, `copy`, `move`, `list`.
- `path: String` (required): Target path (sandbox-relative or absolute).
- `source`/`destination: String`: For copy/move operations.

## Contract

- **Purity**: Impure — filesystem side effects.
- **Sandbox enforcement**: Path traversal (`..`) rejected; symlink loops detected.
- **Atomicity**: Write uses tempfile + atomic rename.
- **Cross-platform**: Windows reserved names (CON, PRN, etc.) rejected on all platforms.
