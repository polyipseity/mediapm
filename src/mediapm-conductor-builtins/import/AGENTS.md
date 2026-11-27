# Import Builtin Crate

Crate: `mediapm-conductor-builtin-import`

## Parameters

- `source: String` (required): file path, directory, glob, or URL.
- `dest: String`: CAS URI or path. `timeout_secs: String`: URL import timeout (default 300).

## Contract

- Impure: network/filesystem reads; output is CAS hash.
- Content-addressed: same content -> same hash; cached entries not re-imported.
- URL imports enforce configurable timeout; slow sources fail gracefully.
