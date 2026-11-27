# `preset/` — Managed tool preset builders

Per-tool `(ToolSpec, ToolRuntime)` builders for `ffmpeg`, `yt-dlp`, `deno`, `rsgain`, `sd`, consumed by `preset::mod::apply_preset()`, delegating to `workflows::*::build_*_spec()` for inputs/outputs/templates. `media_tagger.rs` exception: spec from `crate::builtins::host_builtin_tool_spec()`, this module holds only the gated dependency declaration.

See `crate::tools::provider::AGENTS.md` for sources, `crate::tools::preset::mod.rs` for dispatch.
