# `preset/` — Tool preset builders

Per-tool `(ToolSpec, ToolRuntime)` builders; each file exposes `apply()` consumed by `preset::mod::apply_preset()`. Dispatched ids: `sd`, `echo`, `archive`, `export`, `fs`, `import`.

See `crate::tools::provider::AGENTS.md` for source descriptors, `crate::tools::preset::mod.rs` for dispatch.
