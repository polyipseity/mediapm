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
- `src/conductor-builtins/fs/` — `fs` builtin runtime (filesystem staging)
- `src/conductor-builtins/echo/` — builtin echo runtime + standalone runner
- `src/conductor-builtins/import/` — impure source-ingest builtin (`file`/`folder`/`fetch` kinds)
- `src/conductor-builtins/export/` — impure filesystem materialization builtin (`file`/`folder` kinds)
- `src/conductor-builtins/archive/` — pure archive transform builtin (ZIP-only content transforms)
- `src/mediapm/` — phase-3 media API + CLI scaffold composed over phase 1/2
  (`mediapm-cas` + `mediapm-conductor`; builtins are reached via conductor)
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

Conductor runtime storage defaults (CLI/API):

- runtime root (`conductor_dir`): `.conductor`
- volatile state document (`state_ncl`): `.conductor/state.ncl`
- filesystem CAS store (`cas_store_dir`): `.conductor/store/`

These grouped runtime paths are also part of the persisted user/machine
configuration schema (`conductor.ncl` and `conductor.machine.ncl`) via
one grouped optional `runtime` field containing
`conductor_dir`, `state_ncl`, and `cas_store_dir`.

The conductor CLI exposes grouped path flags (`--conductor-dir`,
`--config-state`, `--cas-store-dir`). `--cas-store-dir` accepts any CAS
locator string (plain filesystem path or URL); defaults to the resolved
`<conductor_dir>/store` path when omitted.

The persistent conductor demo (`src/conductor/examples/demo.rs`) writes
orchestration state to
`src/conductor/examples/.artifacts/demo/orchestration-state.pretty.json` and
prints that file path instead of streaming the full JSON state payload to
stdout. Both this demo artifact and the `conductor state` command render the
persisted orchestration-state wire-envelope shape.

Current orchestration-state snapshots include explicit top-level `version` and
store per-instance `tool_name` plus normalized metadata: executable metadata
keeps `ToolSpec` shape, while builtin metadata persists only
`kind`/`name`/`version`. Each instance records optional `impure_timestamp` at
instance scope and stores input references by CAS hash identity. For
deduplicated equivalent tool calls, persisted output persistence flags are the
effective merged policy (`save`: logical AND, `force_full`: logical OR).
Builtin orchestration-state metadata decoding is strict and rejects extra
non-identity fields.

## Notes

This repository now matches the requested multi-crate phase topology, but it is
still an implementation scaffold rather than the full feature-complete system
described in `PLAN.md`.

Builtin runtime policy (mandatory):

- Builtin runtime behavior lives in dedicated crates under
  `src/conductor-builtins/*` (including `echo`).
- `src/conductor` only dispatches to builtin crate APIs and does not keep
  builtin runtime logic inline.
- Each builtin crate remains independently runnable via its own binary target
  while also exposing a library API.
- Builtin crates use explicit crate versions in each builtin `Cargo.toml`
  (`version = "..."`) instead of inheriting workspace package version.
- Builtin crates must share one stable input contract:
  CLI uses normal Rust flags/options while keeping argument values as strings,
  and API accepts `BTreeMap<String, String>` args plus optional raw payload
  bytes for content-oriented operations (for example archive/export). A builtin
  CLI may optionally expose one default option key so one value can be passed
  without spelling the option key, while explicit keyed input remains supported
  and maps to the same API key. Builtin execution must fail on unrecognized
  args/inputs, missing required keys, and invalid argument combinations instead
  of silently ignoring mismatches. If a builtin's successful non-error result is pure,
  its success payload may be deterministic bytes or `BTreeMap<String, String>`.
  Impure builtins may
  instead primarily communicate success through side effects. CLI failures may
  use ordinary Rust error types instead of being encoded into the success
  payload.
