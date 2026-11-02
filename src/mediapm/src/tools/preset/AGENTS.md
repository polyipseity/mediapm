# `preset/` — Managed tool preset builders

Per-tool `(ToolSpec, ToolRuntime)` configuration builders for the 5 managed tools with presets (`ffmpeg`, `yt-dlp`, `deno`, `rsgain`, `sd`). Each file exposes a builder consumed by `preset::mod::apply_preset()` and delegates to `workflows::*::build_*_spec()` for workflow-specific default inputs, outputs, and command templates. `media_tagger.rs` is the exception: the tagger is a builtin mediapm's host runs, so its spec comes from `crate::builtins::host_builtin_tool_spec()` and this module holds only the dependency declaration the sync gates on.

See `crate::tools::provider::AGENTS.md` for source descriptors. See `crate::tools::preset::mod.rs` for the dispatch table.
