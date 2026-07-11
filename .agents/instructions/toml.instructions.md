---
description: "Use when authoring or editing TOML config files (Cargo.toml, prek.toml, .cargo/config.toml, rust-toolchain.toml, rumdl.toml) in this repository."
name: "TOML Conventions"
applyTo: "**/*.toml"
---

# TOML conventions

## General formatting

- Use `#` for line comments only. Prefer section-header comments over inline.
- Multi-line arrays use standard bracketed style with trailing commas.
- Multi-line tables use `[section]` headers with indented key-value pairs.

## Cargo.toml (workspace and crate level)

- Use workspace inheritance wherever possible:
  - `version.workspace = true`, `edition.workspace = true`, `rust-version.workspace = true`, `license-file.workspace = true`, `publish.workspace = true`.
- Dependency patterns (in order of preference):
  - `name.workspace = true` — simple pass-through.
  - `name = { workspace = true, features = [...] }` — workspace dep with feature overrides.
  - `name = { path = "...", default-features = false }` — path deps, always opt-out of defaults.
  - `name = { workspace = true, optional = true }` — optional deps.
- Feature syntax: use `dep:name` for optional dependency features (e.g. `"dep:clap"`).
- Every crate `Cargo.toml` must have `[lints] workspace = true`.

## prek.toml

- Uses `[[repos]]` array of tables for hook repositories.
- Hooks defined as inline tables: `{ id = "...", ... }`.
- Use `exclude` patterns as regex strings.
- `stages` arrays for lifecycle scoping.
- Comment each hook selection with rationale.

## .cargo/config.toml

- `[alias]` section defines cargo shortcuts (`build-all`, `clippy-all`, `fmt-check`, `test-all`, etc.).
- `[build]` section with `target-dir`.

## rust-toolchain.toml

- Minimal three-field `[toolchain]` section: `channel`, `profile`, `components`.

## rumdl.toml

- Root file has `[global]` section with `include`, `disable`, `extend-disable` arrays.
- Override files (e.g. `.agents/.rumdl.toml`) use `extends = "../.rumdl.toml"` and override specific fields.
