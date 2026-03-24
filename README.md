# mediapm

`mediapm` is a Rust, declarative, workspace-local media reconciler.

The project is designed around deterministic reconciliation: users declare
desired media state, `mediapm` plans explicit effects, and then executes those
effects while preserving provenance in sidecars.

## Status

- Core implementation from `PLAN.md` is now in place as an MVP:
  - URI-canonicalized media identity
  - BLAKE3 content-addressed object store in `.mediapm/objects/blake3/...`
  - JSON sidecar state in `.mediapm/media/<media-id>/media.json`
  - Deterministic planning and effect execution
  - Link materialization with fallback (`symlink -> hardlink -> copy`)
  - Integrity verification and garbage collection

## Rust baseline configuration

- `Cargo.toml` — crate manifest and package metadata
- `rust-toolchain.toml` — stable toolchain + required components
- `rustfmt.toml` — formatter policy
- `clippy.toml` — lint policy
- `.cargo/config.toml` — cargo aliases and target settings
- `.github/workflows/ci.yml` — CI checks (`fmt`, `clippy`, `test`)

## Commands

- `mediapm plan` — dry-run effects from declarative config
- `mediapm sync` — apply import + link reconciliation
- `mediapm verify` — verify hash/object/sidecar integrity
- `mediapm gc` — collect unreferenced objects (dry-run by default)
- `mediapm fmt` — canonicalize config and sidecar JSON formatting
- `mediapm edit` — edit metadata/history (revertable or non-revertable)

All commands support `--workspace <path>`. Commands that read config also
support `--config <path>` (default: `mediapm.json`).

## Architecture overview

- The source tree is intentionally split into first-level layers under `src/`:
  - `configuration/`: desired-state schema and file IO
  - `domain/`: identity, sidecar schema, metadata shape, migrations
  - `application/`: planning and execution orchestration
  - `infrastructure/`: persistence, verify, GC, formatting
  - `support/`: shared deterministic utilities
- URI identity and byte-content identity are intentionally separate:
  - canonical URI identifies logical media item intent
  - BLAKE3 hash identifies exact object bytes
- Sidecars preserve history/lineage rather than replacing state in place.

For deeper rationale and end-to-end flow, see crate rustdoc in
`src/lib.rs` (for example via `cargo doc --open`).

## Config (JSON)

`mediapm` currently accepts JSON configuration (`mediapm.json` by default):

- `sources`: source URIs (path-like values are canonicalized to `file://` URIs)
- `links`: explicit desired link targets
- `metadata_overrides`: per-URI metadata overlay values
- `policies`: link method order and sync policy toggles

Example:

```json
{
 "sources": [
  { "uri": "inbox/song.flac" }
 ],
 "links": [
  {
   "path": "library/song.flac",
   "from_uri": "inbox/song.flac",
   "select": { "prefer": "latest_non_lossy" }
  }
 ],
 "metadata_overrides": {
  "file:///ABSOLUTE/PATH/TO/inbox/song.flac": {
   "tags": {
    "artist": "Artist"
   }
  }
 },
 "policies": {
  "link_methods": ["symlink", "hardlink", "copy"],
  "strict_rehash": false,
  "musicbrainz_enabled": false
 }
}
```

## Local validation

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo test --workspace --all-targets --all-features`
