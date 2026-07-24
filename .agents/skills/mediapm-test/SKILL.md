---
description: "Quick-reference test command guide for the mediapm workspace. Covers selective crate tests, integration tests, demo runs, and full workspace validation."
name: "mediapm-test"
---

# mediapm test reference

## Selective crate tests

| What changed              | Command                                          |
| ------------------------- | ------------------------------------------------ |
| mediapm crate only        | `cargo test -p mediapm`                          |
| mediapm lib tests         | `cargo test -p mediapm -- lib`                   |
| mediapm CLI tests         | `cargo test -p mediapm -- main`                  |
| mediapm integration tests | `cargo test -p mediapm -- integration`           |
| A specific test           | `cargo test -p mediapm -- <test_name>`           |
| Conductor crate           | `cargo test -p mediapm-conductor`                |
| CAS crate                 | `cargo test -p mediapm-cas`                      |
| A conductor builtin       | `cargo test -p mediapm-conductor-builtin-<name>` |
| Utilities                 | `cargo test -p mediapm-utils`                    |

## Demo runs (end-to-end validation)

Run both **sequentially** (not parallel) at the end of work:

```sh
cargo run --package mediapm --example mediapm_demo
cargo run --package mediapm --example mediapm_demo_online
```

Do not run demos during dev iteration — they require external tools (ffmpeg,
yt-dlp, rsgain, media-tagger) and network access.

## Full workspace validation

```sh
cargo test --no-fail-fast
```

## Formatting and linting

```sh
cargo fmt-check
cargo clippy-all
```

## When to run what

1. **During dev iteration**: `cargo test -p mediapm -- <specific_test>` for
   tight feedback loop
2. **Before staging**: `cargo test -p mediapm` for the affected crate
3. **Before push**: `cargo test --no-fail-fast` + `cargo fmt-check` +
   `cargo clippy-all`
4. **End-to-end**: both demo examples (sequential)
