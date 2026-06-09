# Import Builtin Crate

Crate: `mediapm-conductor-builtin-import`

## Parameters

- `source: String` (required): File path, directory, glob, or URL.
- `dest: String`: Destination (CAS URI or path).
- `timeout_secs: String`: Timeout for URL imports (default 300).

## Contract

- **Purity**: Impure — network/filesystem read side effects.
- **CAS integration**: Content-addressed; output is CAS hash.
- **Deduplication**: Same content → same hash; cached entries not re-imported.
- **Timeouts**: URL imports enforce configurable timeout; slow sources fail gracefully.
