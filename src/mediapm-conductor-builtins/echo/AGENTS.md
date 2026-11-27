# Echo Builtin Crate

Crate: `mediapm-conductor-builtin-echo`

## Parameters

- `message: String` (required): string to echo.
- `output: String`: stdout/stderr/file; defaults to stdout.

## Contract

- Pure: deterministic, no side effects unless output is a file.
- Never fails under normal conditions (no I/O errors).
