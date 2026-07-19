---
description: "Use when editing cache and HTTP client in src/mediapm/src/tools/downloader.rs and http_client.rs. Covers three-tier cache hierarchy, TTL policies, and shared HTTP client configuration."
name: "Cache and HTTP Client"
applyTo: "src/mediapm/src/tools/downloader.rs, src/mediapm/src/http_client.rs"
---

# Cache and HTTP client

## Purpose

- Provide efficient caching for downloaded tool payloads and metadata (GitHub tags) to avoid redundant network transfers.
- Share one `reqwest::Client` process-wide for connection pooling and TLS reuse.

## Three-tier cache hierarchy

| Cache | TTL | Basis | Content | Key |
|-------|-----|-------|---------|-----|
| **Content cache** (`tools.json`) | 30d | Last-use | Raw downloaded tool payload bytes | URL or resource identifier |
| **Metadata cache** (`tool_metadata.json`) | 1d | Creation-time | GitHub API responses (tag names, versions) | API endpoint URL |
| **Provision cache** (RAII) | 24h | Creation-time | Extracted tool binaries (per-platform unpack results) | Tool identity hash |

### Important: TTL basis differences

- Content cache TTL is **last-use based** — touching a cached entry resets its TTL clock. Used via `lookup_bytes()` / `store_bytes()`.
- Metadata cache TTL is **creation-time based** — entries expire based on when they were stored, not when last accessed. Caller must NOT call `touch()` — doing so would extend the TTL, defeating the 1-day freshness guarantee.
- Provision cache is **RAII** — the extracted temp directory lives for the duration of the `ProvisionCache` handle (24h default).

## Cache location

All caches live under `default_mediapm_user_download_cache_root()` (OS-specific user-level cache directory, typically `~/.cache/mediapm/` on Linux or `~/Library/Caches/mediapm/` on macOS).

```text
<os-cache-dir>/mediapm/
  tools.json            # Content cache (30d, last-use)
  tool_metadata.json    # Metadata cache (1d, creation-time)
  provision/            # RAII provision cache (24h)
```

## Shared HTTP client

Configured once via `OnceLock`:

| Setting | Default | Override |
|---------|---------|----------|
| Connect timeout | 30s | — |
| Request timeout | 30 min | `MEDIAPM_DOWNLOAD_TIMEOUT_SECONDS` env var (min 30s) |
| User-Agent | `mediapm/0.0.0 (+https://github.com/mediapm/mediapm)` | — |

## Hard boundary rules

- Workspace-scoped conductor tool-content storage (`<runtime_root>/tools/`) and user-level download cache (`<os-cache-dir>/mediapm/`) are **never interchangeable**.
- The content cache holds raw downloaded bytes for cross-workspace reuse.
- The tools directory holds materialized (extracted) binaries for one specific workspace.
