# Echo Builtin Crate

Crate: `mediapm-conductor-builtin-echo`

## Parameters

- `message: String` (required): The string to echo.
- `output: String`: Where to write (stdout/stderr/file). Defaults to stdout.

## Contract

- **Purity**: Pure — deterministic; same input always produces same output.
- **Side effects**: Zero (unless output is a file).
- **Failure**: Never fails under normal conditions (no I/O errors).
