# `preset/` — Managed tool preset builders

Per-tool `(ToolSpec, ToolRuntime)` configuration builders for the 6 managed
tools. Each file exposes a builder consumed by `preset::mod::apply_preset()`.

Managed tools: `ffmpeg`, `yt-dlp`, `deno`, `rsgain`, `media-tagger`, `sd`.

Delegates to `workflows::*::build_*_spec()` for workflow-specific default
inputs, outputs, and command templates.

See `crate::tools::provider::AGENTS.md` for source descriptors.
See `crate::tools::preset::mod.rs` for the dispatch table.
