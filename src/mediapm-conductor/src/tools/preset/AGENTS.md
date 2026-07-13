# `preset/` — Tool preset builders

Per-tool `(ToolSpec, ToolRuntime)` configuration builders. Each file exposes a
public `apply()` function consumed by `preset::mod::apply_preset()`.

Dispatched tool IDs: `sd`, `echo`, `archive`, `export`, `fs`, `import`.

See `crate::tools::provider::AGENTS.md` for the corresponding source descriptors.
See `crate::tools::preset::mod.rs` for the dispatch table.
