//! Provider source definitions for `ffmpeg`.
//!
//! Windows and Linux come from BtbN/FFmpeg-Builds (two URL candidates each,
//! tried in order). macOS is a two-endpoint chain per architecture: a
//! martin-riedl.de permalink (primary; `aarch64` and `x86_64` are two
//! sources on one OS label, so the sync layer keys their payloads
//! `macos/{arch}/…`) falling back to a versioned jellyfin-ffmpeg archive.
//! The conductor fetch layer verifies each downloaded candidate against the
//! serving endpoint's `.sha256` sibling when one is published, so a corrupt
//! or stale chain member fails over to the next automatically.
//!
//! Canonical version: composite of `autobuild-{tag}` from `BtbN` and the
//! macOS leg actually fetched — the martin-riedl build id
//! (`{unixtime}_{buildid}`, e.g. `1759500000_N-127260-g7bc3576910`) when
//! the primary served, the jellyfin-ffmpeg version (e.g. `7.1.3-1`) when
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

/// jellyfin-ffmpeg macOS version embedded in the fallback archive names.
///
/// Single source of truth for the fallback version: the archive URL is built
/// from it, the canonical fallback leg is it, and
/// `jellyfin_version_parses_from_the_portable_archive_name` pins that the
/// URL's basename parses back to it, so the URL and the leg cannot drift.
const JELLYFIN_MACOS_VERSION: &str = "7.1.3-1";

/// macOS architectures the martin-riedl primary and jellyfin fallback both
/// publish. Both land in the content map (`macos/{arch}/…` keys) and the
/// host-arch one drives the os-keyed executable path.
const MACOS_ARCHES: [&str; 2] = ["aarch64", "x86_64"];

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

/// Builds the jellyfin-ffmpeg macOS archive URL for one architecture.
///
/// The archive name embeds [`JELLYFIN_MACOS_VERSION`]; the canonical
/// fallback leg is parsed back out of this name by
/// [`jellyfin_version_from_archive_name`].
fn jellyfin_macos_archive(arch: &str) -> String {
    format!(
        "{}/releases/ffmpeg/macos/jellyfin-ffmpeg_{}_portable_macos-{arch}.zip",
        jellyfin_base(),
        JELLYFIN_MACOS_VERSION,
    )
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
/// Expects `jellyfin-ffmpeg_{version}_portable_macos-{arch}.zip`; returns
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

/// Resolves the macOS canonical leg: the martin-riedl build id behind the
/// `aarch64` permalink, or the jellyfin-ffmpeg version when the probe fails.
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
        // corrects this to the real build id once the payload is served. One
        // second-order effect: if the stored composite already carries this
        // same jellyfin version from a prior real failover, the skip check
        // matches and martin-riedl is not re-probed until the BtbN tag
        // rotates (<= 1 day) — self-healing, not a permanent wedge.
        tracing::warn!(
            "martin-riedl permalink probe failed; the macOS canonical leg records the \
             jellyfin fallback version until fetch feedback corrects it"
        );
        (JELLYFIN_MACOS_VERSION.to_string(), false)
    }
}

/// Probes the martin-riedl permalink and parses the build id out of the
/// final redirect target.
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
    match client.head(permalink).send().await {
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
                "martin-riedl permalink {permalink} answered HEAD with {}",
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
/// `target` selects the macOS primary URLs: `Latest` (and any non-tag spec)
/// chains the martin-riedl permalink before the jellyfin archive per
/// architecture; `Exact { tag }` substitutes the tag as the martin-riedl
/// build id into versioned download URLs, keeping the jellyfin fallback
/// behind it. The tool-level dispatcher refuses ffmpeg pins before ever
/// building an `Exact` fetch (`BtbN` retains no historical builds), so the
/// `Exact` shape exists for direct callers and its URL construction is
/// pinned by unit tests.
#[must_use]
pub(crate) fn sources(target: &VersionSpec) -> ResolvedToolFetch {
    let pinned_build_id = match target {
        VersionSpec::Exact(VersionSpecFields { tag, .. }) => tag.as_deref(),
        VersionSpec::Latest => None,
    };

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
        },
    ];

    for arch in MACOS_ARCHES {
        // `Exact` addresses one pinned build directly; `Latest` walks the
        // site's two rolling channels before the jellyfin fallback, so a
        // snapshot outage still leaves the release permalink as a primary.
        let urls = match pinned_build_id {
            Some(build_id) => {
                vec![martin_riedl_download_url(arch, build_id), jellyfin_macos_archive(arch)]
            }
            None => vec![
                martin_riedl_permalink(arch, MartinRiedlChannel::Snapshot),
                martin_riedl_permalink(arch, MartinRiedlChannel::Release),
                jellyfin_macos_archive(arch),
            ],
        };
        sources.push(ResolvedSource {
            os: "macos".to_string(),
            arch: arch.to_string(),
            producer: SourceProducer::Fetch { urls },
            expected_size: None,
            size_hint_bytes: Some(45_000_000),
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

    /// The jellyfin fallback archive name embeds the version the canonical
    /// fallback leg records, so the URL and the leg cannot drift.
    #[test]
    fn jellyfin_version_parses_from_the_portable_archive_name() {
        assert_eq!(
            jellyfin_version_from_archive_name(&jellyfin_macos_archive("aarch64")).as_deref(),
            Some(JELLYFIN_MACOS_VERSION),
            "the archive URL built from the version const parses back to it"
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
            macos_leg_from_url(&jellyfin_macos_archive("x86_64")).as_deref(),
            Some(JELLYFIN_MACOS_VERSION),
        );
        assert_eq!(
            macos_leg_from_url(
                "https://ffmpeg.martin-riedl.de/redirect/latest/macos/x86_64/snapshot/ffmpeg.zip"
            ),
            None,
            "an unredirected permalink reveals no build id",
        );
    }

    /// `Latest` builds one chain per macOS architecture — the snapshot
    /// permalink, the release permalink, then the jellyfin archive —
    /// alongside the unchanged `BtbN` Windows and Linux sources. Two macOS
    /// sources on one OS label is what makes the sync layer's arch-segmented
    /// content keys kick in.
    #[test]
    fn sources_latest_chains_snapshot_release_then_jellyfin_for_both_arches() {
        let fetch = sources(&VersionSpec::Latest);

        let macos: Vec<&ResolvedSource> =
            fetch.sources.iter().filter(|s| s.os == "macos").collect();
        assert_eq!(macos.len(), 2, "one macOS source per architecture");
        assert_eq!(macos[0].arch, "aarch64");
        assert_eq!(macos[1].arch, "x86_64");
        for source in &macos {
            let SourceProducer::Fetch { urls } = &source.producer else {
                panic!("macOS sources must be fetch producers");
            };
            assert_eq!(
                urls,
                &[
                    martin_riedl_permalink(&source.arch, MartinRiedlChannel::Snapshot),
                    martin_riedl_permalink(&source.arch, MartinRiedlChannel::Release),
                    jellyfin_macos_archive(&source.arch),
                ],
                "snapshot first, release second, jellyfin last"
            );
        }

        let oses: Vec<&str> = fetch.sources.iter().map(|s| s.os.as_str()).collect();
        assert_eq!(oses, vec!["windows", "macos", "macos", "linux"]);
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
        let fetch = sources(&target);

        for source in fetch.sources.iter().filter(|s| s.os == "macos") {
            let SourceProducer::Fetch { urls } = &source.producer else {
                panic!("macOS sources must be fetch producers");
            };
            assert_eq!(
                urls,
                &[
                    martin_riedl_download_url(&source.arch, build_id),
                    jellyfin_macos_archive(&source.arch),
                ],
                "a pinned build id addresses the versioned download directly"
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
        Mock::given(method("HEAD"))
            .and(path(permalink_path))
            .respond_with(ResponseTemplate::new(302).insert_header(
                "location",
                format!("/download/macos/arm64/{build_id}/ffmpeg.zip").as_str(),
            ))
            .mount(&server)
            .await;
        // The redirected HEAD lands here; any body would do, the probe only
        // reads the final URL.
        Mock::given(method("HEAD"))
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

        let (leg, cached) = resolve_macos_leg(Some(&tracker)).await;
        assert_eq!(leg, build_id, "the probe parses the build id from the redirect target");
        assert!(!cached, "the first resolve goes to the network");

        let (leg_again, cached_again) = resolve_macos_leg(Some(&tracker)).await;
        assert_eq!(leg_again, build_id, "the cached id survives across resolves");
        assert!(cached_again, "the second resolve is served from the metadata cache");

        // SAFETY: same env-lock scope as the set above.
        unsafe {
            std::env::remove_var(MARTIN_RIEDL_BASE_ENV);
        }
    }

    /// A permalink that answers the probe with an error degrades to the
    /// jellyfin fallback version as the leg instead of failing the resolve:
    /// the chain still holds both candidates, and fetch feedback corrects
    /// the canonical version if the primary serves later.
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
        Mock::given(method("HEAD")).respond_with(ResponseTemplate::new(404)).mount(&server).await;

        let _env = crate::example_isolation::lock_process_env();
        // SAFETY: the env lock the guard holds scopes this process-global seam
        // to this test, per the documented MEDIAPM_* mutation-scope rule.
        unsafe {
            std::env::set_var(MARTIN_RIEDL_BASE_ENV, server.uri());
        }

        let (leg, cached) = resolve_macos_leg(None).await;
        assert_eq!(
            leg, JELLYFIN_MACOS_VERSION,
            "a failed probe records the jellyfin fallback version as the leg"
        );
        assert!(!cached, "nothing usable was cached");

        // SAFETY: same env-lock scope as the set above.
        unsafe {
            std::env::remove_var(MARTIN_RIEDL_BASE_ENV);
        }
    }
}
