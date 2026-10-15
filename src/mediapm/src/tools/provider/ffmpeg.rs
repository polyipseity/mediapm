//! Provider source definitions for `ffmpeg`.
//!
//! Windows and Linux come from BtbN/FFmpeg-Builds (two URL candidates each,
//! tried in order). macOS is a per-architecture chain: the martin-riedl.de
//! `snapshot` permalink (primary), that site's `release` permalink, then the
//! newest jellyfin-ffmpeg archive. `aarch64` and `x86_64` are two sources on
//! one OS label, so the sync layer keys their payloads `macos/{arch}/…`. The
//! permalinks carry the site's arch vocabulary (`arm64`/`amd64`), which is
//! not ours, and the jellyfin archive is resolved on every metadata-cache
//! miss rather than pinned, because that repository retires a build line by
//! deleting its artifacts.
//!
//! Both moving-target families declare the build they resolved as their
//! content-cache identity. A martin-riedl permalink redirects to whichever
//! build the site serves when it is fetched, so those sources carry the
//! resolved macOS leg — but only while the URL is a permalink, since a pinned
//! `Exact` build addresses its own versioned download. A `BtbN`
//! `releases/latest/download` alias serves a fixed path whose asset is
//! replaced when upstream rotates, and it is what both arms fetch, so the
//! Windows and Linux sources carry the resolved `BtbN` autobuild tag
//! unconditionally. A rotated build is then a cache miss instead of the
//! previous build's payload being re-served — and its last-use clock
//! refreshed — on every sync under the new build's name.
//!
//! The conductor fetch layer verifies each downloaded candidate against the
//! serving endpoint's `.sha256` sibling when one is published, so a corrupt
//! or stale chain member fails over to the next automatically.
//!
//! Canonical version: composite of `autobuild-{tag}` from `BtbN` and the
//! macOS leg actually fetched — the martin-riedl build id
//! (`{unixtime}_{buildid}`, e.g. `1759500000_N-127260-g7bc3576910`) when
//! the primary served, the jellyfin-ffmpeg version (e.g. `8.1.3-1`) when
//! the fallback did — e.g.
//! "autobuild-2026-07-22-13-36+1759500000_N-127260-g7bc3576910". The
//! resolve-time leg is corrected after fetch by
//! [`super::canonical_version_after_fetch`], which re-parses the leg from
//! the URL that actually served the bytes.
//!
//! Resolved provenance fields: `resolved_tag` is the `BtbN` `autobuild-*` tag;
//! `resolved_version` and `resolved_vcs_hash` are always `None` — WHY: the
//! artifact set mixes `BtbN` autobuilds, martin-riedl snapshots, and
//! jellyfin-ffmpeg builds, so no single version or VCS hash identifies it;
//! deref'ing a `BtbN` build tag yields the build-script repo commit (not the
//! upstream ffmpeg source commit), a martin-riedl build id embeds an
//! ffmpeg-source git-describe but does not identify the whole set, and the
//! jellyfin fork's version carries no upstream git provenance at all.
//! Resolvable != meaningful.

use std::collections::BTreeMap;

use mediapm_conductor::tools::provider::{
    ResolvedSource, ResolvedToolFetch, SourceProducer, VersionSpec, VersionSpecFields,
};

use super::MetadataCacheTracker;

/// Environment variable overriding the martin-riedl.de base URL.
///
/// Hermetic-test seam only: tests point it at a local `wiremock` server.
/// Unset (the production default) resolves against the real endpoint.
const MARTIN_RIEDL_BASE_ENV: &str = "MEDIAPM_MARTIN_RIEDL_BASE";

/// Production martin-riedl.de base URL.
const MARTIN_RIEDL_BASE_DEFAULT: &str = "https://ffmpeg.martin-riedl.de";

/// Environment variable overriding the repo.jellyfin.org base URL.
///
/// Hermetic-test seam only, same rule as [`MARTIN_RIEDL_BASE_ENV`].
const JELLYFIN_BASE_ENV: &str = "MEDIAPM_JELLYFIN_BASE";

/// Production repo.jellyfin.org base URL.
const JELLYFIN_BASE_DEFAULT: &str = "https://repo.jellyfin.org";

/// macOS architectures the martin-riedl primary and jellyfin fallback both
/// publish. Both land in the content map (`macos/{arch}/…` keys) and the
/// host-arch one drives the os-keyed executable path.
const MACOS_ARCHES: [&str; 2] = ["aarch64", "x86_64"];

/// One canonical architecture and the two labels jellyfin's repository uses
/// for it: the directory archives live under and the pointer-file token that
/// names the archive.
struct JellyfinArchLabels {
    /// Our canonical label — the content-map and `os_exec_paths` vocabulary.
    canonical: &'static str,
    /// The repository directory holding this architecture's archives.
    dir: &'static str,
    /// The `*-gpl.txt` pointer-file token naming this architecture's archive.
    file: &'static str,
}

/// The jellyfin repository's labels for the two macOS architectures.
const JELLYFIN_MACOS_ARCH_LABELS: [JellyfinArchLabels; 2] = [
    JellyfinArchLabels { canonical: "aarch64", dir: "arm64", file: "macarm64" },
    JellyfinArchLabels { canonical: "x86_64", dir: "x86_64", file: "mac64" },
];

/// Returns the martin-riedl base URL — the production endpoint, or the
/// `MEDIAPM_MARTIN_RIEDL_BASE` test override when set.
///
/// Read per call (not cached in a `OnceLock`) so per-test wiremock servers
/// can each take a turn under the process env lock, mirroring
/// `MEDIAPM_GITHUB_API_BASE` in the provider module.
fn martin_riedl_base() -> String {
    std::env::var(MARTIN_RIEDL_BASE_ENV).unwrap_or_else(|_| MARTIN_RIEDL_BASE_DEFAULT.to_string())
}
/// Returns the jellyfin base URL — the production endpoint, or the
/// `MEDIAPM_JELLYFIN_BASE` test override when set. Per-call read, same rule
/// as [`martin_riedl_base`].
fn jellyfin_base() -> String {
    std::env::var(JELLYFIN_BASE_ENV).unwrap_or_else(|_| JELLYFIN_BASE_DEFAULT.to_string())
}

/// martin-riedl's release channel for a macOS permalink.
///
/// `Snapshot` is the rolling master build the site republishes every few
/// days; `Release` is the current tagged stable build. The macOS chain tries
/// `Snapshot` before `Release` before the jellyfin fallback, so a snapshot
/// outage does not exhaust the primary source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MartinRiedlChannel {
    /// Rolling master snapshot build.
    Snapshot,
    /// Current tagged stable release.
    Release,
}

impl MartinRiedlChannel {
    /// The path segment the site uses for this channel.
    const fn path_segment(self) -> &'static str {
        match self {
            Self::Snapshot => "snapshot",
            Self::Release => "release",
        }
    }
}

/// Maps our canonical architecture label to the one martin-riedl.de uses.
///
/// Our canonical vocabulary (`aarch64`/`x86_64`, from `std::env::consts::ARCH`)
/// drives content-map keys and `os_exec_paths`; the site labels the same two
/// targets `arm64`/`amd64`. Translating here keeps the site's vocabulary out
/// of everything downstream, so no content-addressed key depends on it.
///
/// # Panics
///
/// Panics on an arch outside [`MACOS_ARCHES`]: the value comes from our own
/// fixed set, so anything else is a programming error, not user input.
fn martin_riedl_arch_label(arch: &str) -> &'static str {
    match arch {
        "aarch64" => "arm64",
        "x86_64" => "amd64",
        other => panic!("unsupported canonical arch for a martin-riedl URL: {other}"),
    }
}

/// Builds the martin-riedl "latest" permalink for one macOS architecture and
/// release channel.
///
/// The permalink always serves the current build for its channel: the
/// resolve-phase probe reads its redirect target, and the fetch phase
/// downloads through it. The architecture is translated by
/// [`martin_riedl_arch_label`].
fn martin_riedl_permalink(arch: &str, channel: MartinRiedlChannel) -> String {
    format!(
        "{}/redirect/latest/macos/{}/{}/ffmpeg.zip",
        martin_riedl_base(),
        martin_riedl_arch_label(arch),
        channel.path_segment(),
    )
}

/// Builds the martin-riedl versioned download URL for one architecture and
/// build id. The build id is the full `{unixtime}_{buildid}` path segment
/// parsed from a permalink redirect target by [`martin_riedl_build_id_from_url`].
/// The architecture is translated by [`martin_riedl_arch_label`].
fn martin_riedl_download_url(arch: &str, build_id: &str) -> String {
    format!(
        "{}/download/macos/{}/{build_id}/ffmpeg.zip",
        martin_riedl_base(),
        martin_riedl_arch_label(arch),
    )
}

/// The jellyfin-ffmpeg macOS fallback archives the repository currently
/// publishes.
///
/// Resolved rather than pinned: the repository retires a build line by
/// deleting its artifacts — the reason a version constant rots into a `410
/// Gone` — so the version comes from the repository and the canonical leg is
/// derived from whatever the fetch actually served.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct JellyfinMacosFallback {
    /// The version published in the newest `latest-{N}.x` line (e.g. `8.1.3-1`).
    pub(crate) version: String,
    /// Canonical architecture label -> the archive URL the pointer file names.
    pub(crate) urls: BTreeMap<String, String>,
}

/// The jellyfin macOS directory index URL.
fn jellyfin_macos_index_url() -> String {
    format!("{}/files/ffmpeg/macos/", jellyfin_base())
}

/// The `*-gpl.txt` pointer-file URL for one architecture in one build line.
fn jellyfin_macos_pointer_url(labels: &JellyfinArchLabels, line: &str) -> String {
    format!("{}/files/ffmpeg/macos/{line}/{}/{}-gpl.txt", jellyfin_base(), labels.dir, labels.file)
}

/// The archive URL for one architecture in one build line.
fn jellyfin_macos_archive_url(labels: &JellyfinArchLabels, line: &str, filename: &str) -> String {
    format!("{}/files/ffmpeg/macos/{line}/{}/{filename}", jellyfin_base(), labels.dir)
}

/// Picks the highest `latest-{N}.x` line named in the jellyfin macOS index.
///
/// The index is an nginx `<pre>` listing. The comparison is numeric, not
/// textual, because `latest-10.x` sorts before `latest-9.x` as text. Returns
/// `None` when the listing names no such line.
fn newest_jellyfin_major_line(index_html: &str) -> Option<String> {
    index_html
        .split("href=\"latest-")
        .skip(1)
        .filter_map(|rest| rest.split(".x/\"").next())
        .filter_map(|digits| digits.parse::<u32>().ok())
        .max()
        .map(|major| format!("latest-{major}.x"))
}

/// Reads the archive filename out of a `*-gpl.txt` pointer file.
///
/// The repository writes the exact archive name on its own line. The shape is
/// re-checked here so a reorganized pointer file degrades to "no fallback"
/// rather than producing a URL for something that is not a jellyfin archive.
fn jellyfin_pointer_filename(body: &str, file_label: &str) -> Option<String> {
    let name = body.trim();
    let suffix = format!("_portable_{file_label}-gpl.tar.xz");
    (name.starts_with("jellyfin-ffmpeg_") && name.ends_with(&suffix)).then(|| name.to_string())
}

/// Fetches a small text resource, preferring the metadata cache.
///
/// Returns the body and whether it came from the cache. A transport failure,
/// non-success status, or unreadable body yields `None` with a
/// `tracing::warn!` naming the URL: the caller degrades to a shorter macOS
/// chain rather than failing the resolve.
///
/// # HTTP client policy
///
/// Uses the process-wide shared client from
/// [`shared_http_client`](mediapm_conductor::http::client::shared_http_client).
/// Connection pooling, TLS reuse, and DNS caching are managed centrally.
/// Do NOT create a [`reqwest::Client`] locally — always use the shared instance.
async fn fetch_text(
    metadata_cache: Option<&MetadataCacheTracker<'_>>,
    url: &str,
) -> (Option<String>, bool) {
    if let Some(cache) = metadata_cache
        && let Some(bytes) = cache.lookup_bytes(url).await
        && let Ok(text) = String::from_utf8(bytes)
    {
        return (Some(text), true);
    }
    let Ok(client) = mediapm_conductor::http::client::shared_http_client() else {
        tracing::warn!("HTTP client unavailable while reading {url}");
        return (None, false);
    };
    match client.get(url).send().await {
        Ok(response) if response.status().is_success() => match response.text().await {
            Ok(text) => {
                if let Some(cache) = metadata_cache {
                    cache.store_bytes(url, text.as_bytes()).await;
                }
                (Some(text), false)
            }
            Err(e) => {
                tracing::warn!("could not read the body of {url}: {e}");
                (None, false)
            }
        },
        Ok(response) => {
            tracing::warn!("{url} answered GET with {}", response.status());
            (None, false)
        }
        Err(e) => {
            tracing::warn!("could not reach {url}: {e}");
            (None, false)
        }
    }
}

/// Resolves the newest jellyfin-ffmpeg macOS release from the repository.
///
/// Two reads, both through the metadata cache (1-day, creation-time — never
/// `touch`ed, which would extend the freshness window): the macOS directory
/// index, which names the highest `latest-{N}.x` line, and that line's
/// per-architecture `*-gpl.txt` pointer file, which names the archive exactly.
///
/// Returns the fallback plus whether every read was served from the metadata
/// cache, or `None` when the repository is unreadable or a pointer file does
/// not name a well-formed archive. `None` is not an error: the martin-riedl
/// permalinks remain the macOS chain, so a sync the primary can serve still
/// succeeds without a fallback.
///
/// # HTTP client policy
///
/// Uses the process-wide shared client from
/// [`shared_http_client`](mediapm_conductor::http::client::shared_http_client).
/// Connection pooling, TLS reuse, and DNS caching are managed centrally.
/// Do NOT create a [`reqwest::Client`] locally — always use the shared instance.
pub(crate) async fn resolve_jellyfin_macos_fallback(
    metadata_cache: Option<&MetadataCacheTracker<'_>>,
) -> (Option<JellyfinMacosFallback>, bool) {
    let (index, index_cached) = fetch_text(metadata_cache, &jellyfin_macos_index_url()).await;
    let Some(line) = index.as_deref().and_then(newest_jellyfin_major_line) else {
        tracing::warn!(
            "the jellyfin macOS index named no build line; the macOS chain keeps only the \
             martin-riedl permalinks"
        );
        return (None, false);
    };

    let mut version: Option<String> = None;
    let mut urls = BTreeMap::new();
    let mut all_cached = index_cached;
    for labels in JELLYFIN_MACOS_ARCH_LABELS {
        let pointer_url = jellyfin_macos_pointer_url(&labels, &line);
        let (body, cached) = fetch_text(metadata_cache, &pointer_url).await;
        all_cached &= cached;
        let Some(filename) =
            body.as_deref().and_then(|body| jellyfin_pointer_filename(body, labels.file))
        else {
            tracing::warn!(
                "the jellyfin pointer file {pointer_url} named no macOS archive; the macOS \
                 chain keeps only the martin-riedl permalinks"
            );
            return (None, false);
        };
        if version.is_none() {
            version = jellyfin_version_from_archive_name(&filename);
        }
        urls.insert(
            labels.canonical.to_string(),
            jellyfin_macos_archive_url(&labels, &line, &filename),
        );
    }

    let Some(version) = version else {
        tracing::warn!(
            "the jellyfin macOS archive name carried no version; the macOS chain keeps only \
             the martin-riedl permalinks"
        );
        return (None, false);
    };
    (Some(JellyfinMacosFallback { version, urls }), all_cached)
}

/// Parses the build id from a martin-riedl versioned download URL.
///
/// The id is the full `{unixtime}_{buildid}` path segment between the
/// architecture and the archive name in `/download/macos/{arch}/{id}/…` —
/// the same segment the `Exact` path embeds when it constructs these URLs,
/// so one parse rule serves both directions. Returns `None` for permalinks
/// (no `/download/macos/` segment), foreign URLs, and shapes without an id
/// segment.
fn martin_riedl_build_id_from_url(url: &str) -> Option<String> {
    let (_, tail) = url.split_once("/download/macos/")?;
    let mut segments = tail.split('/');
    let _arch = segments.next()?;
    let build_id = segments.next()?;
    (!build_id.is_empty()).then(|| build_id.to_string())
}

/// Parses the jellyfin-ffmpeg version from an archive URL's basename.
///
/// Expects the repository's
/// `jellyfin-ffmpeg_{version}_portable_{arch}-gpl.tar.xz` shape; returns
/// `None` for any other shape (including martin-riedl URLs, whose `ffmpeg.zip`
/// basename carries no version).
fn jellyfin_version_from_archive_name(url: &str) -> Option<String> {
    let basename = url.rsplit('/').next()?;
    let tail = basename.strip_prefix("jellyfin-ffmpeg_")?;
    let (version, _) = tail.split_once("_portable_")?;
    (!version.is_empty()).then(|| version.to_string())
}

/// Derives the macOS canonical-version leg from the URL that actually served
/// the fetch: the martin-riedl build id for a `/download/macos/…` URL, the
/// jellyfin-ffmpeg version for a portable archive URL.
///
/// Returns `None` when neither shape parses (e.g. a permalink cache hit whose
/// redirect target was never observed), leaving the canonical version alone.
pub(crate) fn macos_leg_from_url(url: &str) -> Option<String> {
    martin_riedl_build_id_from_url(url).or_else(|| jellyfin_version_from_archive_name(url))
}

/// Resolves the latest `autobuild-*` tag for BtbN/FFmpeg-Builds.
///
/// `BtbN`'s `/releases/latest` endpoint returns `"tag_name":"latest"`, which
/// is useless. This function lists recent releases and picks the first
/// `autobuild-*` tag.
///
/// Returns `(tag, metadata_cached)` where `metadata_cached` is `true` when
/// the result was served from the metadata cache.
pub(crate) async fn resolve_btbn_tag(
    metadata_cache: Option<&MetadataCacheTracker<'_>>,
) -> Result<(String, bool), mediapm_conductor::ConductorError> {
    super::resolve_latest_autobuild_tag("BtbN", "FFmpeg-Builds", metadata_cache).await
}

/// Advisory macOS leg recorded when neither the martin-riedl probe nor the
/// jellyfin index identified a version.
///
/// The value never survives a successful fetch:
/// [`super::canonical_version_after_fetch`] replaces the leg with the build id
/// or version parsed from the URL that actually served the macOS bytes, so it
/// only labels a resolve that could not identify anything.
const UNRESOLVED_MACOS_LEG: &str = "unresolved";

/// Resolves the macOS canonical leg: the martin-riedl build id behind the
/// `aarch64` snapshot permalink, or `jellyfin_fallback_version` when the probe
/// fails.
///
/// The probe is a `HEAD` request through the redirect-following shared
/// client, so the final URL after all hops is what gets parsed and no zip
/// body is downloaded. The parsed id is cached (keyed by the permalink URL,
/// the same URL the fetch phase caches its bytes under). A failed probe is
/// not an error: the resolve-time leg is advisory — the chain still holds
/// both candidates, and [`super::canonical_version_after_fetch`] rewrites
/// the composite from the URL that actually served the fetch.
///
/// Only the `aarch64` permalink is probed: martin-riedl publishes one build
/// id for both architectures, so a second probe would double the metadata
/// traffic for no information.
///
/// `jellyfin_fallback_version` is whatever
/// [`resolve_jellyfin_macos_fallback`] found, so an unidentified probe still
/// records the version the fallback chain would serve.
///
/// Returns `(leg, metadata_cached)`.
///
/// # HTTP client policy
///
/// Uses the process-wide shared client from
/// [`shared_http_client`](mediapm_conductor::http::client::shared_http_client).
/// Connection pooling, TLS reuse, and DNS caching are managed centrally.
/// Do NOT create a [`reqwest::Client`] locally — always use the shared instance.
pub(crate) async fn resolve_macos_leg(
    metadata_cache: Option<&MetadataCacheTracker<'_>>,
    jellyfin_fallback_version: Option<&str>,
) -> (String, bool) {
    let permalink = martin_riedl_permalink("aarch64", MartinRiedlChannel::Snapshot);

    // Cache lookup, keyed by the permalink the fetch phase caches under.
    if let Some(cache) = metadata_cache
        && let Some(bytes) = cache.lookup_bytes(&permalink).await
        && let Ok(build_id) = String::from_utf8(bytes)
        && !build_id.is_empty()
    {
        return (build_id, true);
    }

    let probe = probe_macos_build_id(&permalink).await;
    if let Some(build_id) = probe {
        if let Some(cache) = metadata_cache {
            cache.store_bytes(&permalink, build_id.as_bytes()).await;
        }
        (build_id, false)
    } else {
        // Advisory-only degradation: a failed probe records the jellyfin
        // fallback version as the macOS leg rather than failing the resolve.
        // The fetch-phase rewrite (`canonical_version_after_fetch`) is what
        // corrects this to the real build id once the payload is served.
        //
        // That correction is one-way, so a degraded leg has to stay rare: the
        // stored composite carries the fetch-observed leg while the next
        // resolve derives this one again, so for as long as the two disagree
        // the tool re-provisions — and drags its same-step dependents with it —
        // instead of skipping. This is exactly why the probe below walks the
        // same method the fetch walks: against martin-riedl a `HEAD` probe
        // failed every single time, so the two legs never agreed.
        tracing::warn!(
            "martin-riedl permalink probe failed; the macOS canonical leg records the \
             resolved jellyfin version, and this tool will not skip until the probe succeeds"
        );
        (jellyfin_fallback_version.unwrap_or(UNRESOLVED_MACOS_LEG).to_string(), false)
    }
}

/// Probes the martin-riedl permalink and parses the build id out of the
/// final redirect target.
///
/// The probe walks the same method the fetch walks — `GET` — because
/// martin-riedl answers `HEAD` with 404 and serves only `GET`. A `HEAD` probe
/// therefore never saw the redirect target, and the leg it recorded (the
/// jellyfin fallback version) disagreed with the fetch-observed leg on every
/// run, so the tool re-provisioned instead of skipping. The `Range` header asks
/// for one byte: the payload is not what the probe is after, and the body is
/// never read.
///
/// Returns `None` on any transport failure, non-success status, or
/// unparsable target — the caller degrades to the fallback leg.
async fn probe_macos_build_id(permalink: &str) -> Option<String> {
    let client = match mediapm_conductor::http::client::shared_http_client() {
        Ok(client) => client,
        Err(e) => {
            tracing::warn!("HTTP client unavailable for martin-riedl probe: {e}");
            return None;
        }
    };
    let request = client.get(permalink).header(reqwest::header::RANGE, "bytes=0-0");
    match request.send().await {
        Ok(response) if response.status().is_success() => {
            let final_url = response.url().to_string();
            let build_id = martin_riedl_build_id_from_url(&final_url);
            if build_id.is_none() {
                tracing::warn!(
                    "could not parse a build id from the martin-riedl redirect target \
                     {final_url}"
                );
            }
            build_id
        }
        Ok(response) => {
            tracing::warn!(
                "martin-riedl permalink {permalink} answered the probe with {}",
                response.status()
            );
            None
        }
        Err(e) => {
            tracing::warn!("martin-riedl permalink probe failed for {permalink}: {e}");
            None
        }
    }
}

/// Returns the resolved sources for `ffmpeg`.
///
/// `target` selects the martin-riedl URLs: `Latest` (and any non-tag spec)
/// chains the snapshot permalink before the release permalink per
/// architecture; `Exact { tag }` substitutes the tag as the martin-riedl
/// build id into a versioned download URL. The tool-level dispatcher refuses
/// ffmpeg pins before ever building an `Exact` fetch (`BtbN` retains no
/// historical builds), so the `Exact` shape exists for direct callers and its
/// URL construction is pinned by unit tests.
///
/// `jellyfin` is the resolved macOS fallback. When it is `None` — the
/// repository was unreadable — the macOS chains keep only their martin-riedl
/// candidates rather than inventing an archive URL.
///
/// `macos_leg` is the resolved macOS build the permalink candidates address
/// (see [`resolve_macos_leg`]). The permalink URL is not an identity — it
/// redirects to whichever build the site serves when it is fetched — so each
/// macOS source records the leg as its content-cache identity, and bytes
/// cached from a permalink are only re-served while the permalink still
/// resolves to that build. [`UNRESOLVED_MACOS_LEG`] names no build and is
/// therefore not an identity. `Exact` addresses one pinned macOS build
/// directly: its versioned download URL is its own identity, so the macOS
/// sources declare no identity in that arm.
///
/// `autobuild_tag` is the resolved `BtbN` autobuild the download aliases
/// address (see [`resolve_btbn_tag`]). Those URLs are
/// `releases/latest/download` aliases — a fixed path whose asset is replaced
/// when upstream rotates — so the Windows and Linux sources record the tag as
/// their content-cache identity, and an entry is only re-served for the tag it
/// was fetched under. The tag itself comes from the metadata cache, whose
/// 1-day creation-time TTL bounds how long an upstream rotation can go
/// unnoticed, so a cold content cache can still fetch newer bytes and store them
/// under the previous day's tag; that mismatch self-heals when the tag
/// re-resolves. `Exact` rewrites only the
/// macOS URLs, so those two sources keep fetching an alias and record the tag
/// in either arm.
#[must_use]
pub(crate) fn sources(
    target: &VersionSpec,
    jellyfin: Option<&JellyfinMacosFallback>,
    macos_leg: &str,
    autobuild_tag: &str,
) -> ResolvedToolFetch {
    let pinned_build_id = match target {
        VersionSpec::Exact(VersionSpecFields { tag, .. }) => tag.as_deref(),
        VersionSpec::Latest => None,
    };
    // The permalink URL is not an identity — it redirects to whichever build
    // the site serves at fetch time — so the resolved leg is keyed alongside
    // it. `Exact` names one versioned download itself, and the unresolved
    // sentinel names no build at all; neither is an identity.
    let permalink_identity: Option<String> = match pinned_build_id {
        Some(_) => None,
        None => (macos_leg != UNRESOLVED_MACOS_LEG).then(|| macos_leg.to_string()),
    };
    // The `BtbN` alias path never changes, and it is the path both arms fetch
    // — the pinned shape rewrites only the macOS URLs — so the tag the resolve
    // picked is what tells one build's bytes from another's either way.
    let autobuild_identity: Option<String> = Some(autobuild_tag.to_string());

    let mut sources = vec![
        ResolvedSource {
            os: "windows".to_string(),
            // WHY: BtbN win64 builds are x86_64-only.
            arch: "x86_64".to_string(),
            producer: SourceProducer::Fetch {
                urls: vec![
                    "https://github.com/BtbN/FFmpeg-Builds/releases/latest/download/ffmpeg-master-latest-win64-gpl-shared.zip".to_string(),
                    "https://github.com/BtbN/FFmpeg-Builds/releases/latest/download/ffmpeg-master-latest-win64-gpl.zip".to_string(),
                ],
            },
            expected_size: None,
            size_hint_bytes: Some(120_000_000),
            identity: autobuild_identity.clone(),
        },
    ];

    for arch in MACOS_ARCHES {
        // `Exact` addresses one pinned build directly; `Latest` walks the
        // site's two rolling channels first, so a snapshot outage still
        // leaves the release permalink as a primary. The jellyfin archive
        // trails either shape when the repository was readable.
        let mut urls = match pinned_build_id {
            Some(build_id) => vec![martin_riedl_download_url(arch, build_id)],
            None => vec![
                martin_riedl_permalink(arch, MartinRiedlChannel::Snapshot),
                martin_riedl_permalink(arch, MartinRiedlChannel::Release),
            ],
        };
        if let Some(archive) = jellyfin.and_then(|fallback| fallback.urls.get(arch)) {
            urls.push(archive.clone());
        }
        sources.push(ResolvedSource {
            os: "macos".to_string(),
            arch: arch.to_string(),
            producer: SourceProducer::Fetch { urls },
            expected_size: None,
            size_hint_bytes: Some(45_000_000),
            identity: permalink_identity.clone(),
        });
    }

    sources.push(ResolvedSource {
        os: "linux".to_string(),
        // WHY: BtbN linux64 builds are x86_64-only.
        arch: "x86_64".to_string(),
        producer: SourceProducer::Fetch {
            urls: vec![
                "https://github.com/BtbN/FFmpeg-Builds/releases/latest/download/ffmpeg-master-latest-linux64-gpl-shared.tar.xz".to_string(),
                "https://github.com/BtbN/FFmpeg-Builds/releases/latest/download/ffmpeg-master-latest-linux64-gpl.tar.xz".to_string(),
            ],
        },
        expected_size: None,
        size_hint_bytes: Some(80_000_000),
        identity: autobuild_identity,
    });

    ResolvedToolFetch { tool_id: "ffmpeg".to_string(), sources }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Our canonical arch labels (`aarch64`/`x86_64`) are translated to the
    /// site's vocabulary (`arm64`/`amd64`) when building martin-riedl URLs:
    /// sending the canonical label is what 404'd every macOS request.
    #[test]
    fn martin_riedl_urls_map_canonical_arch_labels() {
        let _env = crate::example_isolation::lock_process_env();
        assert_eq!(martin_riedl_arch_label("aarch64"), "arm64");
        assert_eq!(martin_riedl_arch_label("x86_64"), "amd64");
        assert_eq!(
            martin_riedl_permalink("aarch64", MartinRiedlChannel::Snapshot),
            format!("{}/redirect/latest/macos/arm64/snapshot/ffmpeg.zip", martin_riedl_base())
        );
        assert_eq!(
            martin_riedl_permalink("x86_64", MartinRiedlChannel::Release),
            format!("{}/redirect/latest/macos/amd64/release/ffmpeg.zip", martin_riedl_base())
        );
        assert_eq!(
            martin_riedl_download_url("x86_64", "1759500000_N-127260-g7bc3576910"),
            format!(
                "{}/download/macos/amd64/1759500000_N-127260-g7bc3576910/ffmpeg.zip",
                martin_riedl_base()
            )
        );
    }

    /// A martin-riedl versioned download URL parses to its full
    /// `{unixtime}_{buildid}` segment — the value the `Exact` path embeds
    /// and [`macos_leg_from_url`] re-derives from a fetch's actual URL.
    #[test]
    fn martin_riedl_build_id_parses_the_versioned_download_path() {
        let url = "https://ffmpeg.martin-riedl.de/download/macos/aarch64/1759500000_N-127260-g7bc3576910/ffmpeg.zip";
        assert_eq!(
            martin_riedl_build_id_from_url(url).as_deref(),
            Some("1759500000_N-127260-g7bc3576910"),
            "the full segment after the arch component is the build id"
        );
    }

    /// Permalinks, foreign hosts, and truncated shapes carry no build id:
    /// parsing refuses instead of inventing one.
    #[test]
    fn martin_riedl_build_id_refuses_non_download_urls() {
        assert_eq!(
            martin_riedl_build_id_from_url(
                "https://ffmpeg.martin-riedl.de/redirect/latest/macos/arm64/snapshot/ffmpeg.zip"
            ),
            None,
            "a permalink has no /download/macos/ segment"
        );
        assert_eq!(
            martin_riedl_build_id_from_url("https://example.com/download/macos/ffmpeg.zip"),
            None,
            "a URL without arch and id segments has no build id"
        );
    }

    /// The jellyfin archive basename carries the version the canonical leg
    /// records, and the repository's pointer file names exactly that archive,
    /// so the resolved URL and the leg cannot drift.
    #[test]
    fn jellyfin_version_parses_from_the_portable_archive_name() {
        assert_eq!(
            jellyfin_version_from_archive_name(
                "https://repo.jellyfin.org/files/ffmpeg/macos/latest-8.x/arm64/\
                 jellyfin-ffmpeg_8.1.3-1_portable_macarm64-gpl.tar.xz"
            )
            .as_deref(),
            Some("8.1.3-1"),
        );
        assert_eq!(
            jellyfin_version_from_archive_name("https://example.com/ffmpeg.zip"),
            None,
            "a martin-riedl-shaped basename carries no jellyfin version"
        );
    }

    /// The macOS leg derives from whichever chain member actually served a
    /// fetch — martin-riedl download URLs parse to the build id, jellyfin
    /// portable archives to their version, and neither-permalink nor foreign
    /// URLs leave the canonical version alone.
    #[test]
    fn macos_leg_parses_from_actual_serving_urls() {
        assert_eq!(
            macos_leg_from_url("https://ffmpeg.martin-riedl.de/download/macos/x86_64/1759500000_N-127260-g7bc3576910/ffmpeg.zip").as_deref(),
            Some("1759500000_N-127260-g7bc3576910"),
        );
        assert_eq!(
            macos_leg_from_url(
                "https://repo.jellyfin.org/files/ffmpeg/macos/latest-8.x/x86_64/\
                 jellyfin-ffmpeg_8.1.3-1_portable_mac64-gpl.tar.xz"
            )
            .as_deref(),
            Some("8.1.3-1"),
        );
        assert_eq!(
            macos_leg_from_url(
                "https://ffmpeg.martin-riedl.de/redirect/latest/macos/x86_64/snapshot/ffmpeg.zip"
            ),
            None,
            "an unredirected permalink reveals no build id",
        );
    }

    /// A macOS leg standing in for a resolved martin-riedl build id, so the
    /// chain tests can name one without probing the site.
    const TEST_MACOS_LEG: &str = "1759500000_N-127260-g7bc3576910";

    /// A `BtbN` autobuild tag standing in for a resolved rolling tag, so the
    /// identity tests can name one without reaching the GitHub API.
    const TEST_AUTOBUILD_TAG: &str = "autobuild-2025-07-15-12-00";

    /// A hand-built jellyfin fallback standing in for a repository read, so
    /// the chain tests do not depend on the network.
    fn test_jellyfin_fallback() -> JellyfinMacosFallback {
        let base = JELLYFIN_BASE_DEFAULT;
        let mut urls = BTreeMap::new();
        urls.insert(
            "aarch64".to_string(),
            format!(
                "{base}/files/ffmpeg/macos/latest-8.x/arm64/\
                 jellyfin-ffmpeg_8.1.3-1_portable_macarm64-gpl.tar.xz"
            ),
        );
        urls.insert(
            "x86_64".to_string(),
            format!(
                "{base}/files/ffmpeg/macos/latest-8.x/x86_64/\
                 jellyfin-ffmpeg_8.1.3-1_portable_mac64-gpl.tar.xz"
            ),
        );
        JellyfinMacosFallback { version: "8.1.3-1".to_string(), urls }
    }

    /// `Latest` builds one chain per macOS architecture — the snapshot
    /// permalink, the release permalink, then the resolved jellyfin archive —
    /// alongside the unchanged `BtbN` Windows and Linux sources. Two macOS
    /// sources on one OS label is what makes the sync layer's arch-segmented
    /// content keys kick in.
    #[test]
    fn sources_latest_chains_snapshot_release_then_jellyfin_for_both_arches() {
        let jellyfin = test_jellyfin_fallback();
        let fetch =
            sources(&VersionSpec::Latest, Some(&jellyfin), TEST_MACOS_LEG, TEST_AUTOBUILD_TAG);

        let macos: Vec<&ResolvedSource> =
            fetch.sources.iter().filter(|s| s.os == "macos").collect();
        assert_eq!(macos.len(), 2, "one macOS source per architecture");
        assert_eq!(macos[0].arch, "aarch64");
        assert_eq!(macos[1].arch, "x86_64");
        for source in &macos {
            let SourceProducer::Fetch { urls } = &source.producer else {
                panic!("macOS sources must be fetch producers");
            };
            let archive = jellyfin.urls.get(&source.arch).expect("every arch has an archive");
            assert_eq!(
                urls,
                &[
                    martin_riedl_permalink(&source.arch, MartinRiedlChannel::Snapshot),
                    martin_riedl_permalink(&source.arch, MartinRiedlChannel::Release),
                    archive.clone(),
                ],
                "snapshot first, release second, jellyfin last"
            );
        }

        let oses: Vec<&str> = fetch.sources.iter().map(|s| s.os.as_str()).collect();
        assert_eq!(oses, vec!["windows", "macos", "macos", "linux"]);
    }

    /// Without a resolved fallback the macOS chains keep their two
    /// martin-riedl candidates and invent nothing: an unreadable repository
    /// shortens the chain instead of producing a URL for a nameless archive.
    #[test]
    fn sources_latest_omits_jellyfin_when_the_repository_was_unreadable() {
        let fetch = sources(&VersionSpec::Latest, None, UNRESOLVED_MACOS_LEG, TEST_AUTOBUILD_TAG);

        for source in fetch.sources.iter().filter(|s| s.os == "macos") {
            let SourceProducer::Fetch { urls } = &source.producer else {
                panic!("macOS sources must be fetch producers");
            };
            assert_eq!(
                urls,
                &[
                    martin_riedl_permalink(&source.arch, MartinRiedlChannel::Snapshot),
                    martin_riedl_permalink(&source.arch, MartinRiedlChannel::Release),
                ],
                "a missing fallback leaves the two permalinks and nothing else"
            );
        }
    }

    /// `Exact { tag }` embeds the tag as the martin-riedl build id in the
    /// versioned download URLs (per arch), with the jellyfin fallback still
    /// chained behind it.
    #[test]
    fn sources_exact_builds_versioned_download_urls() {
        let build_id = "1759500000_N-127260-g7bc3576910";
        let target = VersionSpec::Exact(VersionSpecFields {
            vcs_hash: None,
            version: None,
            tag: Some(build_id.to_string()),
        });
        let jellyfin = test_jellyfin_fallback();
        let fetch = sources(&target, Some(&jellyfin), build_id, TEST_AUTOBUILD_TAG);

        for source in fetch.sources.iter().filter(|s| s.os == "macos") {
            let SourceProducer::Fetch { urls } = &source.producer else {
                panic!("macOS sources must be fetch producers");
            };
            let archive = jellyfin.urls.get(&source.arch).expect("every arch has an archive");
            assert_eq!(
                urls,
                &[martin_riedl_download_url(&source.arch, build_id), archive.clone()],
                "a pinned build id addresses the versioned download directly"
            );
        }
    }

    /// The `BtbN` Windows and Linux sources fetch a `releases/latest/download`
    /// alias: the path never changes and the asset is replaced when upstream
    /// rotates, so those sources carry the resolved autobuild tag as their
    /// content-cache identity and a rotated tag is a cache miss rather than
    /// the previous build's bytes served under the new tag's name. `Exact`
    /// rewrites only the macOS URLs, so those two sources fetch an alias in
    /// either arm and key the tag in either arm.
    #[test]
    fn sources_declare_the_autobuild_tag_as_the_alias_sources_identity() {
        let fetch = sources(&VersionSpec::Latest, None, TEST_MACOS_LEG, TEST_AUTOBUILD_TAG);
        for source in fetch.sources.iter().filter(|s| s.os != "macos") {
            assert_eq!(
                source.identity.as_deref(),
                Some(TEST_AUTOBUILD_TAG),
                "a `latest/download` alias source is keyed by the tag it resolved to"
            );
        }

        let target = VersionSpec::Exact(VersionSpecFields {
            vcs_hash: None,
            version: None,
            tag: Some(TEST_MACOS_LEG.to_string()),
        });
        let exact = sources(&target, None, TEST_MACOS_LEG, TEST_AUTOBUILD_TAG);
        for source in exact.sources.iter().filter(|s| s.os != "macos") {
            assert_eq!(
                source.identity.as_deref(),
                Some(TEST_AUTOBUILD_TAG),
                "`Exact` rewrites only the macOS URLs, so the alias sources still key the tag"
            );
        }
    }

    /// Both martin-riedl permalink candidates carry the resolved macOS leg as
    /// their content-cache identity, so a rotated build is a cache miss rather
    /// than a cached payload served under the new build's name. The unresolved
    /// sentinel is not an identity and must not be keyed; a pinned build id
    /// addresses one versioned download whose URL is its own identity.
    #[test]
    fn sources_declare_the_macos_leg_as_the_permalink_identity() {
        let jellyfin = test_jellyfin_fallback();
        let fetch =
            sources(&VersionSpec::Latest, Some(&jellyfin), TEST_MACOS_LEG, TEST_AUTOBUILD_TAG);

        for source in fetch.sources.iter().filter(|s| s.os == "macos") {
            assert_eq!(
                source.identity.as_deref(),
                Some(TEST_MACOS_LEG),
                "a permalink source is keyed by the build its permalinks resolve to"
            );
        }

        let unresolved =
            sources(&VersionSpec::Latest, None, UNRESOLVED_MACOS_LEG, TEST_AUTOBUILD_TAG);
        for source in unresolved.sources.iter().filter(|s| s.os == "macos") {
            assert_eq!(
                source.identity, None,
                "the unresolved sentinel names no build, so no permalink source is keyed by it"
            );
        }

        let target = VersionSpec::Exact(VersionSpecFields {
            vcs_hash: None,
            version: None,
            tag: Some(TEST_MACOS_LEG.to_string()),
        });
        let exact = sources(&target, Some(&jellyfin), TEST_MACOS_LEG, TEST_AUTOBUILD_TAG);
        for source in exact.sources.iter().filter(|s| s.os == "macos") {
            assert_eq!(
                source.identity, None,
                "a pinned build id addresses one versioned download, which is its own identity"
            );
        }
    }

    /// The permalink probe reads the build id out of the final redirect
    /// target and caches it under the permalink URL (the same key the fetch
    /// phase caches its bytes under); the second resolve is a cache hit.
    #[tokio::test]
    #[expect(
        clippy::await_holding_lock,
        reason = "the env lock must serialize the whole set-seam -> await -> unset-seam resolve; the guard is Send and this is the documented MEDIAPM_* test-seam mutation-scope rule"
    )]
    async fn resolve_macos_leg_serves_the_probed_build_id_then_the_cache() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let build_id = "1759500000_N-127260-g7bc3576910";
        let permalink_path = "/redirect/latest/macos/arm64/snapshot/ffmpeg.zip";
        Mock::given(method("GET"))
            .and(path(permalink_path))
            .respond_with(ResponseTemplate::new(302).insert_header(
                "location",
                format!("/download/macos/arm64/{build_id}/ffmpeg.zip").as_str(),
            ))
            .mount(&server)
            .await;
        // The redirected request lands here; any body would do, the probe only
        // reads the final URL.
        Mock::given(method("GET"))
            .and(path(format!("/download/macos/arm64/{build_id}/ffmpeg.zip")))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let dir = mediapm_utils::temp::cache_dir().expect("cache dir");
        let cache = mediapm_conductor::cache::Cache::open(
            dir.path(),
            &[mediapm_conductor::cache::CacheDomainConfig {
                domain: "tool_metadata".to_string(),
                index_file_name: "tool_metadata.json".to_string(),
                entry_ttl_seconds: mediapm_conductor::cache::ENTRY_TTL_SECONDS,
            }],
        )
        .await
        .expect("cache open");
        let tracker = MetadataCacheTracker::new(&cache, "tool_metadata");

        let _env = crate::example_isolation::lock_process_env();
        // SAFETY: the env lock the guard holds scopes this process-global seam
        // to this test, per the documented MEDIAPM_* mutation-scope rule.
        unsafe {
            std::env::set_var(MARTIN_RIEDL_BASE_ENV, server.uri());
        }

        let (leg, cached) = resolve_macos_leg(Some(&tracker), None).await;
        assert_eq!(leg, build_id, "the probe parses the build id from the redirect target");
        assert!(!cached, "the first resolve goes to the network");

        let (leg_again, cached_again) = resolve_macos_leg(Some(&tracker), None).await;
        assert_eq!(leg_again, build_id, "the cached id survives across resolves");
        assert!(cached_again, "the second resolve is served from the metadata cache");

        // SAFETY: same env-lock scope as the set above.
        unsafe {
            std::env::remove_var(MARTIN_RIEDL_BASE_ENV);
        }
    }

    /// martin-riedl answers `HEAD` with 404 and serves only `GET`, so a probe
    /// that relied on `HEAD` could never read the build id out of the redirect
    /// target. It degraded to the jellyfin version instead, while the fetch
    /// observed the real martin-riedl build id and stored it — so the resolve
    /// leg and the stored composite disagreed on every later sync, and ffmpeg
    /// plus its same-step dependents re-provisioned instead of skipping.
    ///
    /// The probe therefore walks the same method the fetch walks, and carries a
    /// one-byte `Range` so the redirect target is known without the payload.
    #[tokio::test]
    #[expect(
        clippy::await_holding_lock,
        reason = "the env lock must serialize the whole set-seam -> await -> unset-seam resolve; the guard is Send and this is the documented MEDIAPM_* test-seam mutation-scope rule"
    )]
    async fn resolve_macos_leg_reads_the_redirect_target_from_a_get_only_source() {
        use wiremock::matchers::{header, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let build_id = "1791650033_N-127260-g7bc3576910";
        let permalink_path = "/redirect/latest/macos/arm64/snapshot/ffmpeg.zip";
        // The live martin-riedl shape: a `HEAD` is refused outright.
        Mock::given(method("HEAD")).respond_with(ResponseTemplate::new(404)).mount(&server).await;
        Mock::given(method("GET"))
            .and(path(permalink_path))
            .and(header("range", "bytes=0-0"))
            .respond_with(ResponseTemplate::new(302).insert_header(
                "location",
                format!("/download/macos/arm64/{build_id}/ffmpeg.zip").as_str(),
            ))
            .mount(&server)
            .await;
        // The redirect target; the probe reads its URL and never its body.
        Mock::given(method("GET"))
            .and(path(format!("/download/macos/arm64/{build_id}/ffmpeg.zip")))
            .respond_with(ResponseTemplate::new(206).set_body_bytes(b"x".as_slice()))
            .mount(&server)
            .await;

        let _env = crate::example_isolation::lock_process_env();
        // SAFETY: the env lock the guard holds scopes this process-global seam
        // to this test, per the documented MEDIAPM_* mutation-scope rule.
        unsafe {
            std::env::set_var(MARTIN_RIEDL_BASE_ENV, server.uri());
        }

        // A jellyfin fallback is offered, so a degraded probe is visible as its
        // version rather than as the unresolved sentinel.
        let (leg, cached) = resolve_macos_leg(None, Some("8.1.3-1")).await;
        assert_eq!(
            leg, build_id,
            "the probe must follow the GET redirect; a HEAD-only probe degrades to 8.1.3-1 \
             while the fetch stores this build id, and the two never agree"
        );
        assert!(!cached, "the first resolve goes to the network");

        // SAFETY: same env-lock scope as the set above.
        unsafe {
            std::env::remove_var(MARTIN_RIEDL_BASE_ENV);
        }
    }

    /// A permalink that answers the probe with an error degrades to the
    /// resolved jellyfin version as the leg instead of failing the resolve:
    /// the chain still holds its candidates, and fetch feedback corrects the
    /// canonical version if the primary serves later. With no resolved
    /// fallback the leg is the explicit unresolved sentinel.
    #[tokio::test]
    #[expect(
        clippy::await_holding_lock,
        reason = "the env lock must serialize the whole set-seam -> await -> unset-seam resolve; the guard is Send and this is the documented MEDIAPM_* test-seam mutation-scope rule"
    )]
    async fn resolve_macos_leg_falls_back_to_the_jellyfin_version_when_the_probe_fails() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        // The permalink's default answer is 404, but mount it explicitly so
        // the refusal is this test's fixture rather than wiremock's silence.
        Mock::given(method("GET")).respond_with(ResponseTemplate::new(404)).mount(&server).await;

        let _env = crate::example_isolation::lock_process_env();
        // SAFETY: the env lock the guard holds scopes this process-global seam
        // to this test, per the documented MEDIAPM_* mutation-scope rule.
        unsafe {
            std::env::set_var(MARTIN_RIEDL_BASE_ENV, server.uri());
        }

        let (leg, cached) = resolve_macos_leg(None, Some("8.1.3-1")).await;
        assert_eq!(
            leg, "8.1.3-1",
            "a failed probe records the resolved jellyfin version as the leg"
        );
        assert!(!cached, "nothing usable was cached");

        let (unresolved, _) = resolve_macos_leg(None, None).await;
        assert_eq!(
            unresolved, UNRESOLVED_MACOS_LEG,
            "with no resolved fallback the leg says so instead of inventing a version"
        );

        // SAFETY: same env-lock scope as the set above.
        unsafe {
            std::env::remove_var(MARTIN_RIEDL_BASE_ENV);
        }
    }

    /// The jellyfin index names several build lines; the highest number wins,
    /// compared numerically so `latest-10.x` beats `latest-9.x`.
    #[test]
    fn newest_jellyfin_major_line_picks_numerically_and_ignores_other_entries() {
        let listing = "<a href=\"../\">../</a>\n\
                       <a href=\"latest-6.x/\">latest-6.x/</a>\n\
                       <a href=\"9.x/\">9.x/</a>\n\
                       <a href=\"latest-9.x/\">latest-9.x/</a>\n\
                       <a href=\"latest-10.x/\">latest-10.x/</a>\n\
                       <a href=\"latest-old.x/\">latest-old.x/</a>";
        assert_eq!(newest_jellyfin_major_line(listing).as_deref(), Some("latest-10.x"));
        assert_eq!(newest_jellyfin_major_line("<a href=\"../\">../</a>"), None);
    }

    /// A pointer file names the archive to fetch; anything that is not a
    /// jellyfin archive name is refused rather than turned into a URL.
    #[test]
    fn jellyfin_pointer_filename_accepts_only_archive_names() {
        assert_eq!(
            jellyfin_pointer_filename(
                "jellyfin-ffmpeg_8.1.3-1_portable_macarm64-gpl.tar.xz\n",
                "macarm64"
            )
            .as_deref(),
            Some("jellyfin-ffmpeg_8.1.3-1_portable_macarm64-gpl.tar.xz"),
        );
        assert_eq!(
            jellyfin_pointer_filename(
                "jellyfin-ffmpeg_8.1.3-1_portable_mac64-gpl.tar.xz",
                "macarm64"
            ),
            None,
            "the other architecture's archive is not this architecture's",
        );
        assert_eq!(jellyfin_pointer_filename("<html>nope</html>", "mac64"), None);
    }

    /// The repository is read for its newest build line and each
    /// architecture's pointer file; the resolved URLs name the archives the
    /// repository currently serves, and the second resolve is a cache hit.
    #[tokio::test]
    #[expect(
        clippy::await_holding_lock,
        reason = "the env lock must serialize the whole set-seam -> await -> unset-seam resolve; the guard is Send and this is the documented MEDIAPM_* test-seam mutation-scope rule"
    )]
    async fn resolve_jellyfin_macos_fallback_reads_the_newest_line_then_the_cache() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/files/ffmpeg/macos/"))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                "<a href=\"../\">../</a><a href=\"latest-7.x/\">latest-7.x/</a><a href=\"latest-8.x/\">latest-8.x/</a>",
            ))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/files/ffmpeg/macos/latest-8.x/arm64/macarm64-gpl.txt"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string("jellyfin-ffmpeg_8.1.3-1_portable_macarm64-gpl.tar.xz\n"),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/files/ffmpeg/macos/latest-8.x/x86_64/mac64-gpl.txt"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string("jellyfin-ffmpeg_8.1.3-1_portable_mac64-gpl.tar.xz\n"),
            )
            .mount(&server)
            .await;

        let dir = mediapm_utils::temp::cache_dir().expect("cache dir");
        let cache = mediapm_conductor::cache::Cache::open(
            dir.path(),
            &[mediapm_conductor::cache::CacheDomainConfig {
                domain: "tool_metadata".to_string(),
                index_file_name: "tool_metadata.json".to_string(),
                entry_ttl_seconds: mediapm_conductor::cache::ENTRY_TTL_SECONDS,
            }],
        )
        .await
        .expect("cache open");
        let tracker = MetadataCacheTracker::new(&cache, "tool_metadata");

        let _env = crate::example_isolation::lock_process_env();
        // SAFETY: the env lock the guard holds scopes this process-global seam
        // to this test, per the documented MEDIAPM_* mutation-scope rule.
        unsafe {
            std::env::set_var(JELLYFIN_BASE_ENV, server.uri());
        }

        let (fallback, cached) = resolve_jellyfin_macos_fallback(Some(&tracker)).await;
        let fallback = fallback.expect("the repository answers with a usable fallback");
        assert_eq!(fallback.version, "8.1.3-1", "the version comes from the pointer file");
        assert_eq!(
            fallback.urls.get("aarch64").map(String::as_str),
            Some(
                format!(
                    "{}/files/ffmpeg/macos/latest-8.x/arm64/\
                     jellyfin-ffmpeg_8.1.3-1_portable_macarm64-gpl.tar.xz",
                    server.uri()
                )
                .as_str()
            ),
            "the aarch64 archive URL names the pointer file's archive",
        );
        assert_eq!(
            fallback.urls.get("x86_64").map(String::as_str),
            Some(
                format!(
                    "{}/files/ffmpeg/macos/latest-8.x/x86_64/\
                     jellyfin-ffmpeg_8.1.3-1_portable_mac64-gpl.tar.xz",
                    server.uri()
                )
                .as_str()
            ),
            "the x86_64 archive URL names the pointer file's archive",
        );
        assert!(!cached, "the first resolve goes to the network");

        let (fallback_again, cached_again) = resolve_jellyfin_macos_fallback(Some(&tracker)).await;
        assert_eq!(fallback_again, Some(fallback), "the cached fallback survives across resolves");
        assert!(cached_again, "the second resolve is served from the metadata cache");

        // SAFETY: same env-lock scope as the set above.
        unsafe {
            std::env::remove_var(JELLYFIN_BASE_ENV);
        }
    }

    /// An unreadable index is not a failure: the macOS chain drops to its
    /// martin-riedl candidates rather than aborting the resolve.
    #[tokio::test]
    #[expect(
        clippy::await_holding_lock,
        reason = "the env lock must serialize the whole set-seam -> await -> unset-seam resolve; the guard is Send and this is the documented MEDIAPM_* test-seam mutation-scope rule"
    )]
    async fn resolve_jellyfin_macos_fallback_degrades_when_the_index_is_unreadable() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET")).respond_with(ResponseTemplate::new(404)).mount(&server).await;

        let _env = crate::example_isolation::lock_process_env();
        // SAFETY: the env lock the guard holds scopes this process-global seam
        // to this test, per the documented MEDIAPM_* mutation-scope rule.
        unsafe {
            std::env::set_var(JELLYFIN_BASE_ENV, server.uri());
        }

        let (fallback, cached) = resolve_jellyfin_macos_fallback(None).await;
        assert_eq!(fallback, None, "an unreadable repository yields no fallback");
        assert!(!cached, "nothing was cached");

        // SAFETY: same env-lock scope as the set above.
        unsafe {
            std::env::remove_var(JELLYFIN_BASE_ENV);
        }
    }
}
