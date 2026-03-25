# mediapm

`mediapm` is now organized as a **Rust workspace of phase-focused crates**.

The current implementation establishes compile-ready contracts and scaffolding
for the three major phases defined in `PLAN.md`:

- Phase 1 CAS in `src/cas/`
- Phase 2 Conductor in `src/conductor/`
- Phase 2 built-ins in `src/conductor-builtins/*/`
- Phase 3 mediapm facade/CLI in `src/mediapm/`

## Workspace layout

- `src/cas/` — content identity types, constraints, and async CAS API contract
- `src/conductor/` — orchestration state model, persistence merge semantics,
  and conductor API contract
- `src/conductor-builtins/fs-ops/` — impure filesystem builtin contract
- `src/conductor-builtins/import/` — impure one-shot import builtin contract
- `src/conductor-builtins/zip/` — archive builtin contract
- `src/mediapm/` — phase-3 media API + CLI scaffold composed over phase 1/2
- `scripts/cargo-bin/` — helper binary used by repo tooling

## Status

- Workspace split and inter-crate wiring are in place.
- Public APIs are documented and covered by baseline tests.
- Runtime behavior is intentionally minimal scaffolding for incremental phase
  implementation.

## Commands

Use workspace aliases from `.cargo/config.toml`:

- `cargo fmt-check`
- `cargo clippy-all`
- `cargo test-all`

Run the phase-3 CLI scaffold:

- `cargo run -p mediapm -- plan`
- `cargo run -p mediapm -- sync`

## Notes

This repository now matches the requested multi-crate phase topology, but it is
still an implementation scaffold rather than the full feature-complete system
described in `PLAN.md`.
