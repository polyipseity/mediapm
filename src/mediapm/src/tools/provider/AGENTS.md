# `provider/` — Managed tool provider source descriptors

Per-OS source URL definitions for the 6 managed tools.
Each file defines platform-specific download URLs consumed by
`provider::mod::resolve_tool_fetch()`.

Managed tools: `ffmpeg`, `yt-dlp`, `deno`, `rsgain`, `media-tagger`, `sd`.

See `crate::tools::preset::AGENTS.md` for the corresponding preset builders.
See `crate::tools::provider::mod.rs` for the resolve dispatch table.
