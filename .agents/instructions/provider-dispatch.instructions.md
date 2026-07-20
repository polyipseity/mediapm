---
description: "Use when editing tool provider definitions in src/mediapm/src/tools/provider/. Covers resolve_tool_fetch dispatch, per-tool source modules, resolve_latest_github_tag, and metadata cache usage."
name: "Provider Dispatch"
applyTo: "src/mediapm/src/tools/provider/**/*.rs"
---

# Provider dispatch

## Purpose

- Define per-OS source descriptors for each managed tool (where to download binaries from).
- Route tool names to the appropriate per-tool source module via `resolve_tool_fetch()`.

## `resolve_tool_fetch(tool_id, metadata_cache)` dispatch

Routes tool names (case-insensitive) to per-tool `sources()` functions:

| Tool           | Module            | Source strategy                      |
| -------------- | ----------------- | ------------------------------------ |
| `ffmpeg`       | `ffmpeg.rs`       | GitHub releases + Evermeet (macOS)   |
| `yt-dlp`       | `yt_dlp.rs`       | GitHub releases                      |
| `deno`         | `deno.rs`         | GitHub releases                      |
| `rsgain`       | `rsgain.rs`       | GitHub releases                      |
| `media-tagger` | `media_tagger.rs` | GitHub releases + `GenerateLauncher` |
| `sd`           | `sd.rs`           | GitHub releases                      |

Returns a `ResolvedToolFetch` containing:

- `sources`: per-OS `Vec<ResolvedSource>` with URL, expected size (optional), and producer type.
- Optional `GenerateLauncher` entries for script-based tool bootstrap.

## `resolve_latest_github_tag(owner, repo, metadata_cache)`

- Cache-first: looks up `https://api.github.com/repos/{owner}/{repo}/releases/latest` in metadata cache.
- On miss: sends GET request via `shared_http_client()`, parses `tag_name` from JSON response.
- On hit: stores result in metadata cache with key = API URL.
- **Metadata cache rules**: caller must NOT call `touch()` — TTL (1 day) is anchored to creation time, not last use.

## URL templating rules per tool

Each tool module defines URL patterns that interpolate:

- `{version}` — resolved semver or tag name.
- `{os}` — target OS label (`linux`, `macos`, `windows`).
- `{arch}` — target architecture (`x86_64`, `aarch64`).

## Canonical version policy

The canonical version is the resolved tag verbatim for all GitHub-release-based tools. No prefix stripping or transformation is applied. For tools without external sources (e.g., media-tagger), there is no canonical version.

## Canonical version per tool

| Tool           | Canonical version source         | Example         |
| -------------- | -------------------------------- | --------------- |
| `yt-dlp`       | Resolved GitHub tag, verbatim    | `2025.07.15`    |
| `ffmpeg`       | Resolved GitHub tag, verbatim    | `L2025-07-15`   |
| `deno`         | Resolved GitHub tag, verbatim    | `v2.2.12`       |
| `rsgain`       | Resolved GitHub tag, verbatim    | `v3.7`          |
| `sd`           | Resolved GitHub tag, verbatim    | `v1.1.0`        |
| `media-tagger` | None (no external sources)       | —               |

## Platform-specific considerations

- macOS ffmpeg: Evermeet.cc and getrelease URLs use dynamic endpoints — HEAD is skipped during prefetch.
- media-tagger: uses `GenerateLauncher` for cross-platform launcher script generation instead of binary download on some platforms.
