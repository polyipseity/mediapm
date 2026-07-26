---
description: "Use when editing cache and HTTP client configuration. Covers three-tier cache hierarchy, TTL policies, shared HTTP client configuration, and decoupling invariants for HTTP modules."
name: "Cache and HTTP Client Invariants"
applyTo: "src/mediapm/src/tools/downloader.rs, src/mediapm/src/http_client.rs, src/mediapm-conductor/src/http/**/*.rs, src/mediapm-conductor-builtins/import/src/lib.rs"
---

# Cache and HTTP client

## Purpose

- Provide efficient caching for downloaded tool payloads and metadata (GitHub tags) to avoid redundant network transfers.
- Share one `reqwest::Client` process-wide for connection pooling and TLS reuse.

## Three-tier cache hierarchy

| Cache                                     | TTL | Basis         | Content                                               | Key                        |
| ----------------------------------------- | --- | ------------- | ----------------------------------------------------- | -------------------------- |
| **Content cache** (`tools.json`)          | 30d | Last-use      | Raw downloaded tool payload bytes                     | URL or resource identifier |
| **Metadata cache** (`tool_metadata.json`) | 1d  | Creation-time | GitHub API responses (tag names, versions)            | API endpoint URL           |
| **Provision cache** (RAII)                | 24h | Creation-time | Extracted tool binaries (per-platform unpack results) | Tool identity hash         |

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

| Setting         | Default                                               | Override                                             |
| --------------- | ----------------------------------------------------- | ---------------------------------------------------- |
| Connect timeout | 30s                                                   | —                                                    |
| Request timeout | 30 min                                                | `MEDIAPM_DOWNLOAD_TIMEOUT_SECONDS` env var (min 30s) |
| User-Agent      | `mediapm/0.0.0 (+https://github.com/mediapm/mediapm)` | —                                                    |

## Hard boundary rules

- Workspace-scoped conductor tool-content storage (`<runtime_root>/tools/`) and user-level download cache (`<os-cache-dir>/mediapm/`) are **never interchangeable**.
- The content cache holds raw downloaded bytes for cross-workspace reuse.
- The tools directory holds materialized (extracted) binaries for one specific workspace.

## HTTP client invariants

The codebase has **two** shared HTTP clients (async + blocking), each gated behind its own Cargo feature. Both use the `OnceLock` pattern.

| Client | Crate | Feature | Runtime |
| ------ | ----- | ------- | ------- |
| `shared_http_client()` / `shared_no_redirect_http_client()` | `mediapm-conductor` | `tool-presets` | Tokio (reqwest async) |
| `shared_http_client()` | `mediapm-conductor-builtins/import` | `fetch` | Sync (reqwest blocking) |

### Decoupling invariant (critical)

The HTTP client module in `mediapm-conductor` (`src/http/`) must be **fully self-contained**:

- **Zero `use crate::` imports** — the module must import nothing from its own crate.
- **Zero `ConductorError` references** — define and use `HttpClientError` instead.
- The module must be designed so it can be extracted into a standalone crate by copying the directory and adjusting `Cargo.toml` dependencies — no code changes to the module body.

Error mapping from `HttpClientError` to `ConductorError` happens **at the call site** (`src/tools/provider/mod.rs`), never inside the `http/` module.

### Decoupling enforcement (three layers)

1. **Build-time regression** — `build.rs` in `mediapm-conductor` scans `src/http/` for `use crate::` and `ConductorError` and panics on violation.
2. **Module-scoped types** — `HttpClientError` is the only error type used inside `http/`.
3. **Review checklist** — when editing `src/http/`, verify no new dependencies on `crate::` or `ConductorError` were introduced.

### Client configuration policy

| Setting         | Default                                               | Override (env var)                       |
| --------------- | ----------------------------------------------------- | ---------------------------------------- |
| Connect timeout | 30s (conductor) / 60s (import)                        | —                                        |
| Request timeout | 30 min (conductor) / 60s (import)                     | `MEDIAPM_HTTP_TIMEOUT_SECONDS` (min 30s) |
| User-Agent      | `mediapm/<version> (+https://github.com/mediapm/mediapm)` | —                                        |

- The import builtin uses a **blocking** client because it runs in a synchronous context (not a tokio runtime). It must not depend on `mediapm-conductor` (the dependency direction is the opposite).
- Both clients use the same User-Agent format with their respective `CARGO_PKG_VERSION`.
