# Archive Builtin Crate

Crate: `mediapm-conductor-builtin-archive`

## Parameters

- `action: String` (required): One of `pack`, `unpack`, `repack`, `transform`.
- `path: String`: Source/target path.
- `filter: String`: Glob filter for transform entries.
- `mode: String`: `text` or `binary` for transform.
- `find_N`/`replace_N`: Numbered key pairs for regex transform.

## Contract

- **Purity**: Pure — deterministic output for same input.
- **Zip bomb protection**: `max_decompressed_size` (default 1 GB) enforced.
- **Transform order**: Sequential in numbered key order; each operates on previous output.
- **Streaming**: Extraction is streaming (not fully buffered).
