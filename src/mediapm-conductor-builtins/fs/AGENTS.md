# Fs Builtin Crate

Crate: `mediapm-conductor-builtin-fs`

## Parameters

- `operation: String` (required): `read`, `write`, `remove`, `copy`, `move`, `list`.
- `path: String` (required): target path (sandbox-relative or absolute).
- `source`/`destination: String`: for copy/move.

## Contract

- Impure: filesystem side effects. Write uses tempfile + atomic rename.
- Sandbox: traversal (`..`) rejected, symlink loops detected.
- Cross-platform: Windows reserved names (CON, PRN, etc.) rejected on all platforms.
