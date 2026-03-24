# mediapm

`mediapm` is currently bootstrapped as a Rust workspace with baseline tooling.

## Status

- Rust project scaffolding and quality gates are configured.
- `PLAN.md` is intentionally **not implemented yet**.

## Rust baseline configuration

- `Cargo.toml` — crate manifest and package metadata
- `rust-toolchain.toml` — stable toolchain + required components
- `rustfmt.toml` — formatter policy
- `clippy.toml` — lint policy
- `.cargo/config.toml` — cargo aliases and target settings
- `.github/workflows/ci.yml` — CI checks (`fmt`, `clippy`, `test`)

## Local validation

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo test --workspace --all-targets --all-features`
