# Design: versioned source chains for managed-tool downloads

Date: 2026-10-12. Status: approved in brainstorm, pre-implementation. Supersedes nothing; extends the provider layer only.

## Problem

Four facts established this session with evidence:

1. **evermeet.cx is a single point of failure for macOS ffmpeg and it is down** — the live demo died on `tool sync failed: ffmpeg (resolve): evermeet HEAD request failed: error sending request for url (https://evermeet.cx/ffmpeg/getrelease/zip)`. evermeet is 100% of the macOS ffmpeg source, single URL, no candidate, no checksum, unknown retention, one-person server.
2. **No real pinning exists.** `ConfigVersionSpec::Exact` is a validation *gate* (resolve-latest then compare, mismatch fails), not a fetch directive; every provider URL is hardcoded `releases/latest/download/...`. A pin breaks the day upstream ships a newer release.
3. **Per-platform URL lists conflate fallback with architecture coverage.** The conductor treats a source's `urls` as ordered candidates (`fetch_bytes_from_candidates`), so deno's `[aarch64, x86_64]` macOS list would silently fall back to the *wrong architecture* if the first 404'd. ffmpeg's `[gpl-shared, gpl]` is a true fallback; deno's/sd's lists are arch variants wearing the same clothes.
4. **The macOS ffmpeg payload is x86_64-only** (evermeet builds Intel only; its own site says so). Apple Silicon Macs run it under Rosetta.

Research (2026-10-12, web + API): martin-riedl.de publishes per-build permalinks `/download/{os}/{arch}/{unixtime}_{buildid}/ffmpeg.zip` plus `.sha256` files, signed macOS binaries, snapshot builds every ~3 days and stable releases (8.0.1), history pages still linking 2025 builds; latest resolution works via `/redirect/latest/macos/{arch}/{snapshot|release}/...` whose redirect target carries the build id (autopkg recipe confirms the pattern). jellyfin-ffmpeg ships versioned macOS portable archives (7.x/6.x/5.x) from repo.jellyfin.org, org-maintained, well retained, but is a fork of ffmpeg. eugeneware/ffmpeg-static retains every tagged release since 2020 on GitHub; its darwin-x64 binaries are evermeet builds (README provenance), newest is 6.1.1 (stale). ffbinaries.com offers API version permalinks up to 6.1, x64 only, evermeet repacks. BtbN ships exactly one rolling `latest` release (`immutable: false`, assets replaced every build) — historical pins are impossible there.

## User rulings

- One combined design: a source-resolution layer carrying **version targeting and fallback chains** together.
- Chains are **built-in in Rust**; no user-facing chain configuration for now.
- **Strict all-platform fail**: any platform's chain exhausted aborts the whole sync (consistent with the fail-fast pool; no host-platform degradation).
- **macOS ffmpeg chain = martin-riedl (primary) → jellyfin-ffmpeg (fallback). evermeet is removed entirely**, not demoted.
- Same-artifact endpoints are preferred where they exist, with the explicit understanding that the A→D chain crosses builders (identity consequences below are accepted).

## Design

### Chain model

A provider resolves, per (tool, os), an ordered `SourceChain`: endpoints serving one artifact slot. Fetch walks the chain in order, trying each endpoint; only chain exhaustion produces the failure, reported as `all sources exhausted: <endpoint>: <cause>; ...` inside the existing phase-tagged `ProvisionFailure` (fail fast, nothing written, unchanged from the pool contract). The content cache key stays the *actual URL used*, so a fallback endpoint caches under its own key.

Architecture selection becomes explicit: an endpoint names its arch; a chain is same-(os, arch). The `[aarch64, x86_64]` candidate conflation is retired (see "Arch decision" below).

### Version targeting

`resolve_tool_fetch` gains the tool's version target (`Latest` or `Exact`), replacing the resolve-then-compare gate for sources that can target directly:

- **GitHub tools (yt-dlp, deno, rsgain, sd)**: `Latest` keeps the current releases-latest path; `Exact(tag)` resolves through `/repos/{o}/{r}/releases/tags/{tag}` and fetches `releases/download/{tag}/{asset}`. Historical GitHub releases persist, so pins are real. GitHub's per-asset `digest: sha256` is verified when present.
- **BtbN (ffmpeg win/linux)**: pins are refused at resolve with `BtbN does not retain historical builds; pinning ffmpeg's BtbN legs is impossible` — a clear refusal over a fake pin. `Latest` unchanged.
- **ffmpeg (macOS, new)**: `Latest` resolves through martin-riedl's redirect permalink, parsing the build id from the redirect target (the mechanics evermeet's probe used, ported). `Exact` fetches the permalink `/download/macos/{arch}/{id}/ffmpeg.zip` directly. jellyfin-ffmpeg fallback carries its own versioned archive URLs.
- **media-tagger**: unchanged (no payload).

### Identity semantics

Canonical version is a property of *what was fetched*, not of the chain:

- martin-riedl serves: canonical = build id (`N-127260-g7bc3576910`) or release version (`8.0.1`).
- jellyfin-ffmpeg serves (fallback): canonical = jellyfin-ffmpeg version (`7.1.2`).
- The ffmpeg composite canonical becomes `"{btbn_tag}+{macos_leg_version}"`, replacing the `+evermeet-{v}` segment.
- Consequence, accepted: failing over to the fallback flips canonical_version, so the next primary-successful sync flips it back and re-provisions once. CAS makes this cheap (content-addressed, no re-download when a hash repeats) and state.json records the audit trail. There is no checksum-identical alternate for the macOS slot anywhere, so this churn is the price of a live chain.

### ffmpeg macOS chain, as shipped

1. **Primary**: martin-riedl.de, per-arch endpoint pair (see arch decision), snapshot channel for `Latest`, `.sha256` verified when served.
2. **Fallback**: jellyfin-ffmpeg macOS portable archive (versioned, org-hosted). Different builder (fork); CLI-compatible for the preset surface, which implementation-time tests must confirm against the provisioned binary.
3. **evermeet deleted**: source entry, HEAD probe, redirect version parsing for its URL, the "evermeet skips HEAD prefetch" special case, and every doc/matrix row describing it.

### Architecture model (user ruling: fetch both arches for ffmpeg's macOS slot)

- **ffmpeg macOS provisions both arm64 and x86_64 payloads** (two endpoints per channel, both imported to CAS). The content-map key gains an arch segment (e.g. `./macos/aarch64/...`, `./macos/x86_64/...`), `os_exec_paths` gains arch awareness (nested os → arch → path or flat `(os, arch)` keys — pinned by the implementation plan), and workflow execution resolves the host arch at run time: arm64 hosts prefer the arm64 binary, Intel hosts take x86_64, and an arm64 host falls back to the x86_64 payload under Rosetta exactly as today when only one arch exists. State stays deterministic: both payloads are recorded, host arch is not a state property.
- **Other tools stay single-payload-per-os for now**, but their arch handling becomes *explicit*: the current `[aarch64, x86_64]` candidate conflation is replaced by one selected arch per os (host-following), ending the silent wrong-arch fallback without doubling every tool's payload. Dual-arch generalization across all tools is future work, listed as a non-goal here.
- Consequence accepted: the content_map key shape change rotates every affected tool's content-addressed key once on the first sync after implementation (old payloads orphan in CAS until GC) — the same one-time churn a version bump produces, documented in the migration notes.

### Non-goals

User-configurable chains; user-facing pin/mirror config; BtbN historical archives (do not exist); building ffmpeg from source; dual-arch provisioning for non-ffmpeg tools (future work, see architecture model); macOS ffprobe as a separate tool (jellyfin/martin-riedl both ship ffprobe; mediapm provisions ffmpeg only today — out of scope).

### Test plan (hermetic)

Unit: chain iteration order and exhaustion message; version-target URL construction per provider class; BtbN pin refusal error; martin-riedl redirect-parse; digest verification; per-arch endpoint selection and host-arch resolution; content-map arch key generation. Integration: seeded metadata-cache failover (first endpoint errors, fallback serves) with no network; pin + skip interaction through `spec_matches_entry`; dual-arch ffmpeg content map decodes with both arch keys. Level-3 (online gate): provisioned ffmpeg binary runs `-version` on macOS in the online demo path.

### Docs to update in the implementation commit

`provider-dispatch.instructions.md` (per-tool rows, canonical-version table, why-empty invariants), `cache-and-http.instructions.md` (HEAD prefetch special cases), `tool-sync-coordinator-and-identity.instructions.md` (resolve flow), coverage matrix rows, and the `+evermeet` composite format references.
