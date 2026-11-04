---
description: "Quick-reference test command guide for the mediapm workspace. Covers selective crate tests, integration tests, demo runs, and full workspace validation."
name: "mediapm-test"
---

# mediapm test reference

## Selective crate tests

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

## Demo runs (end-to-end validation)

Run both **sequentially** (not parallel) at the end of work:

```sh
cargo run --package mediapm --example mediapm_demo
cargo run --all-features --package mediapm --example mediapm_demo_online
```

Both commands run the demos' offline paths, so neither needs network access and neither provisions a real tool binary. No feature flag is involved: the demo is offline by default, and a release build refuses the offline route, naming `--release` to drop or `--online` for the real upstream.

Do not run demos during dev iteration. The online path is opt-in and is the only one that needs the external tools (ffmpeg, yt-dlp, rsgain, media-tagger) and network: `cargo run --package mediapm --example mediapm_demo_online -- --online`.

## Full workspace validation

```sh
cargo test-all
```

## Formatting and linting

```sh
cargo fmt-check
cargo clippy-all
```

## When to run what

- **During dev iteration**: `cargo test-pkg mediapm -- <specific_test>` for tight feedback.
- **Before staging**: `cargo test-pkg mediapm` for the affected crate.
- **Before push**: `cargo test --no-fail-fast` + `cargo fmt-check` + `cargo clippy-all`.
- **End-to-end**: both demo examples (sequential).
