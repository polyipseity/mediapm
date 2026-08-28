# mediapm

`mediapm` manages media libraries declaratively: resolve tools, download sources, and materialize a content-addressed hierarchy on disk.

## Workspace

| Crate | Role |
| --- | --- |
| `src/mediapm-cas/` (`mediapm-cas`) | Content-addressed storage — identity types, hash codec, async API |
| `src/mediapm-conductor/` (`mediapm-conductor`) | Declarative workflow orchestration — state model, persistence merge |
| `src/mediapm-conductor-builtins/{echo,fs,import,export,archive}/` | Builtin tools (echo, filesystem staging, source ingest, materialization, ZIP transform) |
| `src/mediapm/` (`mediapm`) | Media API + CLI — composes CAS and Conductor |
| `scripts/cargo-bin/` | Repository tooling helper binary |

## Usage

Run with `cargo run -p mediapm --`:

```sh
cargo run -p mediapm -- sync
cargo run -p mediapm -- sync --check-tag-updates

cargo run -p mediapm -- tools sync
cargo run -p mediapm -- tools sync --no-check-tag-updates
cargo run -p mediapm -- tools list

cargo run -p mediapm -- global path
cargo run -p mediapm -- global tool-cache status
cargo run -p mediapm -- global tool-cache prune

cargo run -p mediapm -- media add https://example.com/video.mkv
cargo run -p mediapm -- media add-local ./path/to/local/file.mkv

cargo run -p mediapm -- cas ...
cargo run -p mediapm -- conductor ...
```

Tag-update policy: `mediapm sync` does **not** check remote updates for tag-only selectors by default; `mediapm tools sync` **does**.

## Configuration

`mediapm` is configured via `mediapm.ncl` (Nickel):

- `mediapm.ncl` — desired state: media sources, hierarchy, tool requirements.
- `.mediapm/state.ncl` — machine-managed realized state.
- `mediapm.conductor.ncl` / `mediapm.conductor.generated.ncl` — conductor runtime docs.

See `src/mediapm/examples/` for annotated bootstrapping and tool-addition examples.

## Development

```sh
cargo test-pkg <crate>     # test one crate (e.g. mediapm, mediapm-cas)
cargo clippy-pkg <crate>   # lint one crate
cargo build-pkg <crate>    # build one crate
cargo fmt-check   # check formatting
cargo clippy-all  # lint entire workspace
cargo test-all    # test entire workspace
```

Online integration gate (network + external providers):

```sh
MEDIAPM_DEMO_ONLINE_TIMEOUT_SECS=300 cargo run -p mediapm --example mediapm_demo_online
```

Artifacts land under `src/mediapm/examples/artifacts/demo-online/`. The full-sync path runs only on that explicit `cargo run`; the embedded `main_is_exercised` test runs reduced config-only mode (deterministic, no network, skipped in CI). The explicit run persists downloaded tools in the real user-level cache (`<os-cache-dir>/mediapm/cache`); embedded tests stay isolated via `MEDIAPM_EXAMPLE_CACHE_ROOT` tempdirs.

Integration tests across workspace crates share one harness: top-level `tests/mod.rs` entrypoint, grouped modules under `tests/e2e/`, `tests/int/`, `tests/prop/`.
