# Archive Builtin Crate

Crate: `mediapm-conductor-builtin-archive`

## Parameters

- `action: String` (required): `pack`, `unpack`, `repack`, `transform`.
- `path: String`: source/target path. `filter: String`: glob for transform entries. `mode: String`: `text` or `binary` transform.
- `find_N`/`replace_N`: numbered key pairs for regex transform.

## Contract

- Pure: deterministic output for same input. Streaming extraction (not fully buffered).
- `max_decompressed_size` default 1 GB enforced (zip bomb protection).
- Transform sequential in numbered key order, each on previous output.
