---
description: "Quick-reference test command guide for the mediapm workspace. Covers selective crate tests, integration tests, demo runs, and full workspace validation."
name: "mediapm-test"
---

# mediapm test reference

Default to targeted iteration. Treat full workspace/parity gates as explicit checkpoints (pre-push/CI parity), not routine per-edit commands.

## Selective crate tests (default)

Use the narrowest command that covers the change:

| What changed              | Command                                          |
| ------------------------- | ------------------------------------------------ |
| mediapm crate only        | `cargo test-pkg mediapm`                         |
| mediapm lib tests         | `cargo test-pkg mediapm -- lib`                  |
| mediapm CLI tests         | `cargo test-pkg mediapm -- main`                 |
| mediapm integration tests | `cargo test-pkg mediapm -- integration`          |
| A specific test           | `cargo test-pkg mediapm -- <test_name>`          |
| Conductor crate           | `cargo test -p mediapm-conductor`                |
| CAS crate                 | `cargo test -p mediapm-cas`                      |
| A conductor builtin       | `cargo test -p mediapm-conductor-builtin-<name>` |
| Utilities                 | `cargo test -p mediapm-utils`                    |

For targeted iteration, also use crate-scoped `cargo clippy-pkg <crate>`, `cargo build-pkg <crate>`, `cargo check -p <crate>`, or `cargo test -p <crate> <test_name>` as appropriate.

## Explicit checkpoints (pre-push/CI parity)

Do not run these as routine per-edit commands. Use them for pre-push/CI parity or when an explicit parity context requires them.

- **Formatting/linting parity:** `cargo fmt-check`, `cargo bin rumdl check`, `cargo clippy-all`
- **Workspace test gates:** `cargo test-all` and `cargo test-doc-all` (`test-doc-all` is intentionally split into separate doctest and rustdoc link-gate phases)
- **Build/feature parity (as required):** `cargo build-all`/feature-matrix in CI or explicit parity contexts

`prek.toml` runs the full workspace pre-push gate (`cargo-check`, `clippy`, `test-runner test`, `test-runner doc`); CI mirrors and extends it.

## Demo runs (end-to-end checkpoint)

Run both **sequentially** (not parallel) as an end-to-end checkpoint, not during dev iteration:

```sh
cargo run --package mediapm --example mediapm_demo
cargo run --package mediapm --example mediapm_demo_online
```

Both commands run the demos' offline paths, so neither needs network access and neither provisions a real tool binary. No feature flag is involved: the demo is offline by default, and a release build refuses the offline route, naming `--release` to drop or `--online` for the real upstream.

The online path is opt-in and is the only one that needs the external tools (ffmpeg, yt-dlp, rsgain, media-tagger) and network: `cargo run --package mediapm --example mediapm_demo_online -- --online`.

## When to run what

- **Default (during iteration):** run the narrowest targeted test/build/check for the change (often `cargo test-pkg <crate> -- <test_name>` or another selective command above).
- **Affected-crate checkpoint (optional):** run `cargo test-pkg <crate>` or other targeted crate checks when you want broader confidence before staging.
- **Pre-push/CI parity checkpoint:** rely on pre-push hooks for the workspace parity gate; run the explicit parity commands above only when you need to reproduce or inspect them locally.
- **End-to-end checkpoint:** run both demo examples sequentially before completion/push when required.
