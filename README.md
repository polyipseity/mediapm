# mediapm

`mediapm` manages media libraries declaratively: resolve tools, download sources, and materialize a content-addressed hierarchy on disk.

## Workspace

| Crate | Role |
| --- | --- |
| `src/mediapm-cas/` (`mediapm-cas`) | Content-addressed storage — identity types, hash codec, async API |
| `src/mediapm-conductor/` (`mediapm-conductor`) | Declarative workflow orchestration — state model, persistence merge |
| `src/mediapm-conductor-builtins/echo/` | Echo builtin |
| `src/mediapm-conductor-builtins/fs/` | Filesystem staging builtin |
| `src/mediapm-conductor-builtins/import/` | Source-ingest builtin (`file`/`folder`/`fetch`) |
| `src/mediapm-conductor-builtins/export/` | Filesystem materialization builtin (`file`/`folder`) |
| `src/mediapm-conductor-builtins/archive/` | Archive transform builtin (ZIP) |
| `src/mediapm/` (`mediapm`) | Media API + CLI — composes CAS and Conductor |
| `scripts/cargo-bin/` | Repository tooling helper binary |

## Usage

Run with `cargo run -p mediapm --`:

```sh
# Sync media library and managed tools
cargo run -p mediapm -- sync
cargo run -p mediapm -- sync --check-tag-updates

# Tool management
cargo run -p mediapm -- tools sync
cargo run -p mediapm -- tools sync --no-check-tag-updates
cargo run -p mediapm -- tools list

# Global state
cargo run -p mediapm -- global path
cargo run -p mediapm -- global tool-cache status
cargo run -p mediapm -- global tool-cache prune

# Media sources
cargo run -p mediapm -- media add https://example.com/video.mkv
cargo run -p mediapm -- media add-local ./path/to/local/file.mkv

# Pass-through to sub-CLIs
cargo run -p mediapm -- cas ...
cargo run -p mediapm -- conductor ...
```

Tag-update policy:

- `mediapm sync` does **not** check remote updates for tag-only selectors by default.
- `mediapm tools sync` **does** check remote updates for tag-only selectors by default.

## Configuration

`mediapm` is configured via `mediapm.ncl` (Nickel):

- `mediapm.ncl` — desired state: media sources, hierarchy, tool requirements.
- `.mediapm/state.ncl` — machine-managed realized state.
- `mediapm.conductor.ncl` / `mediapm.conductor.machine.ncl` — conductor runtime docs.

See `src/mediapm/examples/` for annotated bootstrapping and tool-addition examples.

## Development

**Targeted (recommended during development):**

```sh
cargo test-pkg <crate>     # test one crate (e.g. mediapm, mediapm-cas)
cargo clippy-pkg <crate>   # lint one crate
cargo build-pkg <crate>    # build one crate
```

**Full workspace (pre-push):**

```sh
cargo fmt-check   # check formatting
cargo clippy-all  # lint entire workspace
cargo test-all    # test entire workspace
```

**Online integration gate (requires network and external providers):**

```sh
MEDIAPM_DEMO_ONLINE_TIMEOUT_SECS=300 cargo run -p mediapm --example demo_online
```

Inspect generated artifacts under `src/mediapm/examples/.artifacts/demo-online/`.

`demo_online` is a full-sync integration example for normal validation runs.
When setting `MEDIAPM_DEMO_ONLINE_RUN_SYNC` explicitly, use `true`.

Integration tests across workspace crates share one harness shape:

- top-level `tests/mod.rs` entrypoint,
- grouped modules under `tests/e2e/`, `tests/int/`, and `tests/prop/`.
