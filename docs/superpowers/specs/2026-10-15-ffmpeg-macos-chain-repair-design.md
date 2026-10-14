# Design: ffmpeg macOS source-chain repair and fetch fallback warnings

Date: 2026-10-15. Status: approved in brainstorm, pre-implementation. Extends the provider layer and the conductor fetch loop; supersedes nothing.

## Problem

Three facts, each established with live evidence this session (network egress was enabled after the hostname whitelist went live).

1. **The martin-riedl primary source 404s because we send the wrong architecture label.** The site's documented scripting format is `https://ffmpeg.martin-riedl.de/redirect/latest/{macos,linux}/{amd64,arm64}/{snapshot,release}/{file}`. `martin_riedl_permalink` (`src/mediapm/src/tools/provider/ffmpeg.rs:88`) emits our canonical `aarch64`/`x86_64`, so every request 404s. Probed directly: `redirect/latest/macos/arm64/snapshot/ffmpeg.zip` → **200**, redirecting to `/download/macos/arm64/{build_id}/ffmpeg.zip`; `redirect/latest/macos/amd64/snapshot/ffmpeg.zip` → **200**. With `aarch64` the same URL → **404**. The wrong arch token is the sole cause of the primary failure; the permalink shape itself is correct.
2. **The jellyfin fallback is broken three ways and pins a version that no longer exists.** `https://repo.jellyfin.org/releases/ffmpeg/macos/` now answers "This path is no longer valid on our repository server" (the live demo saw **410 Gone**). The current layout is `/files/ffmpeg/macos/latest-{5,6,7,8}.x/{arm64,x86_64}/jellyfin-ffmpeg_{version}_portable_{macarm64|mac64}-gpl.tar.xz`: the path moved, the archive format changed `.zip` → `.tar.xz`, the arch token is `macarm64`/`mac64`, and the current versions (`7.1.4-3`, `8.1.3-1`) differ from the compile-time constant `JELLYFIN_MACOS_VERSION = "7.1.3-1"` (`ffmpeg.rs:61`). A frozen fallback version always rots; the user ruled the hardcoded version a defect.
3. **A fallback that succeeds silently hides the failure it papered over.** `fetch_bytes_from_candidates` (`src/mediapm-conductor/src/tools/provider/mod.rs:644`) accumulates `failures: Vec<(String, String)>` (`:662`), then returns `Ok((buffer, url.clone(), Some(final_url)))` at `:721` on the first success and **discards the failures**. Two of the four failure arms do not even `tracing::warn!`. A primary 404 followed by a fallback 200 produces no user-visible signal at all.

Supporting facts established by reading the code: `.tar.xz` is already a supported archive format (`infer_archive_format` → `ARCHIVE_TAR_XZ`), and `find_os_executable` searches recursively for a file named after the tool, so neither provider's internal archive layout needs to be hardcoded (martin-riedl's zip is a single `ffmpeg` entry; mediapm references no `ffprobe`). Our canonical arch (`aarch64`/`x86_64`) drives content-map keys and `os_exec_paths`, so translating the arch label at URL-build time leaves content-addressed keys untouched.

A newly discovered hosting fact: jellyfin's file URLs **302-redirect to `*.mirror.jellyfin.org`** (observed target `https://sgp1.mirror.jellyfin.org/files/ffmpeg/macos/7.x/7.1.4-3/arm64/...`). That host is not in the current whitelist, so the fallback download is blocked at the proxy even once its URL is correct.

## User rulings

- **One combined fix**: repair the primary, make the fallback dynamic, and surface successful-fallback failures.
- **The hardcoded jellyfin version is a defect.** Resolution must be dynamic; a build-time constant that must match a moving upstream is not a version spec, it is rot.
- **Chain order stays martin-riedl primary → jellyfin fallback**, with a martin-riedl `release` permalink added as a middle candidate (chosen this session): `[snapshot permalink, release permalink, jellyfin archive]`.
- **Warnings surface through `ToolSyncReport.warnings`** (the `Δ` lines and exit status 3), not through the progress channel. `mediapm-conductor` must not depend on `indicatif`.
- **Jellyfin's line is discovered, not pinned**: parse the `/ffmpeg/macos` listing, take the highest `latest-{N}.x`, resolve the current archive within it. No hardcoded major, no hardcoded version.
- **Strict all-platform fail is unchanged**: chain exhaustion aborts the whole sync with the real cause; a fallback success is a warning, not a failure.

## Design

### A. martin-riedl arch-label mapping

Translate our canonical architecture to the site's label only when building martin-riedl URLs.

| Our canonical arch | martin-riedl label |
| ------------------ | ------------------ |
| `aarch64`          | `arm64`            |
| `x86_64`           | `amd64`            |

`martin_riedl_permalink(arch)` (`ffmpeg.rs:88`) and `martin_riedl_download_url(arch, build_id)` (`ffmpeg.rs:95`) apply the mapping internally. The `arch` carried on `ResolvedSource`, in the arch-segmented content keys (`macos/{arch}/...`), and in `os_exec_paths` stays our canonical label, so no content-addressed key churns and no stored state invalidates. `martin_riedl_build_id_from_url` (`:120`) parses the `/download/macos/{arch}/{build_id}/` target and reads only the build-id segment, so it is unaffected by the arch token.

This single change makes the primary succeed for both macOS arches and is what unbreaks the observed demo failure.

### B. jellyfin fallback: dynamic, listing-resolved

Remove the `JELLYFIN_MACOS_VERSION` constant. Add a resolve-time descriptor resolution:

1. **Discover the major line.** GET the plain directory index `https://repo.jellyfin.org/files/ffmpeg/macos/` (an 848-byte index) and select the highest `latest-{N}.x/` from its `href` list. The repo-browser `?path=` HTML is not needed — the `files/` index family is the stable source and returns the same names.
2. **Resolve the archive.** GET `https://repo.jellyfin.org/files/ffmpeg/macos/latest-{N}.x/{dir_label}/` (a 488-byte index) and extract the `jellyfin-ffmpeg_{version}_portable_{macarch}-gpl.tar.xz` filename; the version parses out of that name.
3. **Cache it.** Store the resolved descriptor in the **metadata cache** (1-day, creation-time TTL) keyed by the listing URL, exactly like every other resolve. No `touch()` — the TTL is anchored to creation.

Arch mapping for jellyfin:

| Our canonical arch | directory label | filename token |
| ------------------ | --------------- | -------------- |
| `aarch64`          | `arm64`         | `macarm64`     |
| `x86_64`           | `x86_64`        | `mac64`        |

The download URL is `https://repo.jellyfin.org/files/ffmpeg/macos/latest-{N}.x/{dir_label}/{filename}`, which follows its own 302 to a `*.mirror.jellyfin.org` mirror.

The resolved descriptor is a small record — `JellyfinMacosArchive { major_line, version, dir_label, filename }`. It is resolved once in the ffmpeg dispatch arm of `resolve_tool_fetch` (`src/mediapm/src/tools/provider/mod.rs:691`) and threaded into both chain construction and the resolve-time fallback leg; `resolve_macos_leg` (`ffmpeg.rs:187`) takes it so its degradation path records a real version instead of the constant. `sources(target)` becomes `sources(target, &jellyfin)`; the dispatch arm and the hermetic helper at `src/mediapm/src/conductor_bridge/sync/provision.rs:818` are the only call sites to update, and tests construct a descriptor directly rather than reaching the network.

The composite canonical version needs no new machinery: `jellyfin_version_from_archive_name` (`ffmpeg.rs:133`) parses `jellyfin-ffmpeg_{version}_portable_...` from the served basename, so the post-fetch rewrite records the *actual* version that served and a jellyfin build bump re-provisions automatically.

### C. fetch fallback warnings → `ToolSyncReport.warnings`

Thread the discarded `failures` out to the user-visible report.

- `fetch_bytes_from_candidates` (`conductor provider/mod.rs:644`) changes its `Ok` type from `(Vec<u8>, String, Option<String>)` to a small `FetchedBytes { bytes, served_url, final_url, failures }` so the `failures` accumulated at `:662` survive the success return at `:721` instead of being dropped, and the two silent arms gain the missing `tracing::warn!`.
- `fetch_tool_sources` (`conductor provider/mod.rs:438`) collects them into a new `DownloadedSources.warnings: Vec<String>` field (`:328`).
- `fetch_and_import_tool_payload` (`provision.rs:178`) carries them on `FetchedToolPayload` as `warnings: Vec<String>`; `EntryOutcome::Fetched` (`sync/mod.rs:138`) already transports the payload.
- `apply_entry_outcome` (`sync/mod.rs:522`) pushes each warning into `report.warnings` with the tool id prefixed.

Message shape, one line per failed candidate: `tool ffmpeg os macos (aarch64): candidate failed (<cause> at <url>); served by <fallback url>`. The existing `ToolSyncReport.warnings` → `print_warning` (`Δ`) → `ToolSyncOutcome::Warning` (exit 3) path (`output/observer.rs:83`, `output/mod.rs:203`) carries it to the CLI unchanged.

### Chain construction

Per macOS arch, `sources` emits three ordered candidates under the arch-segmented slot:

1. martin-riedl snapshot permalink (mapped arch; carries the build-id redirect target)
2. martin-riedl release permalink (mapped arch; stable, no version — a cheap extra candidate for when snapshot is down)
3. jellyfin archive (discovered major + resolved version + mapped arch)

Both arches get the full chain. Content keys remain `macos/{arch}/...` with our canonical arch.

### Hosts and caching

The whitelist must gain **`*.mirror.jellyfin.org`** (jellyfin's file 302 target). All other hosts are already listed. Cache semantics are unchanged: content cache 7-day last-use keyed by the actual candidate URL (so a permalink served from cache records the requested URL, not the final mirror target — the parked redirect-skew limitation), metadata cache 1-day creation-time.

## Testing

- **Arch-label mapping (RED first).** The existing wiring test mocks `/redirect/latest/macos/aarch64/...`, i.e. it encodes our bug; update the mock to `arm64` and add a table test pinning `aarch64 → arm64` and `x86_64 → amd64` for both URL builders.
- **Dynamic jellyfin resolution.** A wiremock test serves the `/ffmpeg/macos` listing and the `latest-{N}.x/{arch}/` index, and asserts the resolved descriptor (major line, version, dir label, filename token) and the built URL. A second test asserts the metadata cache is consulted before the network on the second resolve.
- **Fallback warnings.** A wiremock test where candidate 1 returns 404 and candidate 2 succeeds asserts the warning appears in `report.warnings` (and thus in `ToolSyncOutcome::Warning`).
- **Lib-test env-seam discipline.** Every test touching `MEDIAPM_MARTIN_RIEDL_BASE` / `MEDIAPM_JELLYFIN_BASE` / `MEDIAPM_GITHUB_API_BASE` holds `crate::example_isolation::lock_process_env()` for its whole body; async ones carry `#[expect(clippy::await_holding_lock, reason = ...)]`.
- Docs and coverage: update `provider-dispatch.instructions.md` (arch-label mapping, new jellyfin URL family), `cache-and-http.instructions.md` (host list), and the coverage matrix.

## Out of scope

- The permalink-cache-skew limitation (content cache keyed by requested URL, not final redirect target) stays parked.
- The pre-existing broken `--no-default-features` build of `mediapm-conductor` stays out of scope.
- The resolve-time probe's silent degradation (`resolve_macos_leg`, `ffmpeg.rs:187`) becomes moot for the primary once Fix A lands; surfacing its own warning is a possible follow-up, not part of this design.
