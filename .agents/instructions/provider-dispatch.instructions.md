---
description: "Use when editing tool provider definitions in src/mediapm/src/tools/provider/. Covers resolve_tool_fetch dispatch, per-tool source modules, resolve_latest_github_tag, and metadata cache usage."
name: "Provider Dispatch"
applyTo: "src/mediapm/src/tools/provider/**/*.rs"
---

# Provider dispatch

## `resolve_tool_fetch(tool_id, metadata_cache)` dispatch

Routes tool names (case-insensitive) to per-tool `sources()` functions:

| Tool           | Module            | Source strategy                      |
| -------------- | ----------------- | ------------------------------------ |
| `ffmpeg`       | `ffmpeg.rs`       | BtbN (win/linux) + martin-riedl/jellyfin chains (macOS) |
| `yt-dlp`       | `yt_dlp.rs`       | GitHub releases                      |
| `deno`         | `deno.rs`         | GitHub releases                      |
| `rsgain`       | `rsgain.rs`       | GitHub releases                      |
| `media-tagger` | `media_tagger.rs` | GitHub releases + `GenerateLauncher` |
| `sd`           | `sd.rs`           | GitHub releases                      |

Returns `(ResolvedToolFetch, ResolvedToolMetadata)`:

- `ResolvedToolFetch`: per-OS `sources` (`Vec<ResolvedSource>` with URL, expected size, and producer type) plus optional `GenerateLauncher` entries for script-based tool bootstrap.
- `ResolvedToolMetadata`: `human_readable_version`, `canonical_version`, `metadata_cached`, `metadata_fetch_count`, and the three provenance fields `resolved_tag`, `resolved_version`, `resolved_vcs_hash` (all `Option<String>`; see "Resolved provenance fields" below).

## `MetadataCacheTracker`

The `MetadataCacheTracker` type wraps `(&Cache, domain: &str)` and automatically
counts every `lookup_bytes` call. `resolve_tool_fetch` uses this to auto-derive
`metadata_fetch_count` (a field on `ResolvedToolMetadata`) instead of
maintaining a manually-updated per-tool constant. When a resolver is added or
removed, the count adjusts automatically.

## `resolve_latest_github_tag(owner, repo, metadata_cache)`

- Cache-first: looks up `https://api.github.com/repos/{owner}/{repo}/releases/latest` in metadata cache.
- On miss: sends GET request via `mediapm_conductor::http::client::shared_http_client()`, parses `tag_name` from JSON response.
- On hit: stores result in metadata cache with key = API URL.
- **Metadata cache rules**: caller must NOT call `touch()` (TTL (1 day) is anchored to creation time, not last use.

## URL templating rules per tool

Each tool module defines URL patterns that interpolate:

- `{version}` — resolved semver or tag name.
- `{os}` — target OS label (`linux`, `macos`, `windows`).
- `{arch}` — target architecture (`x86_64`, `aarch64`).

## Canonical version policy

The canonical version is the resolved commit hash for GitHub-release-based tools (yt-dlp, deno, rsgain, sd); it is not the tag. ffmpeg uses a composite of the BtbN autobuild tag and the macOS-leg version actually fetched (`"{autobuild_tag}+{macos_leg_version}"`, where the leg is the martin-riedl build id or the jellyfin version — see "Canonical version per tool"). For builtin tools that ship with mediapm, the canonical version is the mediapm git hash embedded at build time (VCS hash kind). The semantic kind (VCS hash vs version vs tag) is fixed per tool at code-writing time; it is never determined at runtime.

## Canonical version per tool

| Tool           | Canonical version source                                | Example                              |
| -------------- | ------------------------------------------------------- | ------------------------------------ |
| `yt-dlp`       | Resolved GitHub commit hash                             | `a1b2c3d4e5f6...`                    |
| `ffmpeg`       | Composite: BtbN autobuild tag + macOS-leg version      | `autobuild-2025-07-15-12-00+1760000000_N-999999-gdeadbeef` |
| `deno`         | Resolved GitHub commit hash                             | `b2c3d4e5f6a7...`                    |
| `rsgain`       | Resolved GitHub commit hash                             | `c3d4e5f6a7b8...`                    |
| `sd`           | Resolved GitHub commit hash                             | `d4e5f6a7b8c9...`                    |
| `media-tagger` | mediapm build-time git hash                             | (git hash)                           |

## Resolved provenance fields (`resolved_tag` / `resolved_version` / `resolved_vcs_hash`)

Every `ResolvedToolMetadata` carries three provenance fields of type `Option<String>`. Empty is `None` (serialized as JSON `null`) (never `""`. Providers that have no value return `None`; consumers branch on `Some`/`None`. These fields are informational provenance; version comparison, skip-if-up-to-date, and update decisions all use `canonical_version`.

| Tool           | resolved_tag           | resolved_version         | resolved_vcs_hash       | Why empty? |
| -------------- | ---------------------- | ------------------------ | ----------------------- | ---------- |
| `yt-dlp`       | `Some(tag)`            | `Some(tag)`              | `Some(commit_hash)`     | —          |
| `deno`         | `Some(tag)`            | `Some(tag)`              | `Some(commit_hash)`     | —          |
| `rsgain`       | `Some(tag)`            | `Some(tag)`              | `Some(commit_hash)`     | —          |
| `sd`           | `Some(tag)`            | `Some(tag)`              | `Some(commit_hash)`     | —          |
| `ffmpeg`       | `Some(autobuild_tag)`  | `None`                   | `None`                  | Mixed sources (BtbN autobuilds + martin-riedl/jellyfin chains): neither a single version nor a single VCS hash identifies the artifact set. A `git/refs/tags/{tag}` deref on BtbN yields the build-script repo commit, not the upstream ffmpeg source commit; martin-riedl and jellyfin publish versioned archives with no git provenance. Resolvable ≠ meaningful. |
| `media-tagger` | `None`                 | `Some(CARGO_PKG_VERSION)` | `Some(MEDIAPM_GIT_HASH)` | Builtin launcher shipped inside mediapm: no upstream tagged release; identity is the mediapm build. |

**Why-empty invariant (hard rule):** any of the three fields left as `None` MUST carry a documented reason in three places: (a) an inline `// WHY:` comment on the provider dispatch arm, (b) the per-tool provider module doc comment, (c) the per-tool row in this file. A `None` field without documentation is a code-review failure.

## Platform-specific considerations

- macOS ffmpeg: the chain is martin-riedl.de → jellyfin-ffmpeg (never GitHub). Each macOS arch (`aarch64`, `x86_64`) resolves its own source, so the ffmpeg macOS OS slot is a **dual-arch slot** and its content keys are arch-segmented (`macos/{arch}/...`); `os_exec_paths["macos"]` records the host arch's payload. The martin-riedl permalink 302-redirects to a versioned download; the build id parsed from that redirect target becomes the macOS leg of the composite version.
- GitHub release assets (yt-dlp, deno, rsgain, sd, BtbN ffmpeg) never serve a sibling `.sha256`, so the checksum probe is skipped for github.com / objects.githubusercontent.com hosts; martin-riedl and jellyfin (and hermetic test servers) are still probed so "verify when served" keeps working wherever a checksum might appear.
- media-tagger: uses `GenerateLauncher` for cross-platform launcher script generation instead of binary download on some platforms.
