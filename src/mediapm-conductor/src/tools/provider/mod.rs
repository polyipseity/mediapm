//! Tool provider: three-phase pipeline for resolving, fetching, and processing
//! downloadable tool payloads (and generating launcher scripts for builtins).
//!
//! # Architecture
//!
//! Every tool (external download + launcher-based builtin) goes through three
//! sequential phases:
//!
//! 1. **Resolve** — gather metadata, resolve versions, determine sources.
//! 2. **Fetch** — download or generate bytes for each source.
//! 3. **Process** — extract archives, repack, import to CAS, build content map.
//!
//! The entire module is gated behind the `tool-presets` feature since all types
//! reference progress types from `mediapm-utils` that are behind that gate.

#![cfg(feature = "tool-presets")]

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

use futures_util::stream::{self, StreamExt};
use mediapm_utils::progress::{
    MultiItemBudget, ProviderPhase, ProviderProgressCallback, ProviderProgressSnapshot,
};

// ---------------------------------------------------------------------------
// Archive format constants (private — inferred internally in phase 3)
// ---------------------------------------------------------------------------

const ARCHIVE_ZIP: &str = "zip";
const ARCHIVE_TAR_GZ: &str = "tar.gz";
const ARCHIVE_TAR_XZ: &str = "tar.xz";

/// Environment override for per-tool source fetch concurrency.
const ENV_SOURCE_FETCH_CONCURRENCY: &str = "MEDIAPM_SOURCE_FETCH_CONCURRENCY";

/// Returns the maximum concurrent HTTP downloads within a single tool's fetch
/// phase.
///
/// Reads `MEDIAPM_SOURCE_FETCH_CONCURRENCY` from env. Falls back to the
/// host's available parallelism, capped at 1 minimum.
fn default_source_fetch_concurrency() -> usize {
    std::env::var(ENV_SOURCE_FETCH_CONCURRENCY)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|&v| v > 0)
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(3, usize::from).max(1))
}

/// Maximum number of sources to probe concurrently for expected sizes.
/// Covers all current tools (≤3 sources) with headroom.
pub const MAX_LOOKAHEAD: usize = 16;

/// Byte threshold between sub-entry progress callbacks during
/// extraction/compression (64 KB).
const SUB_ENTRY_CHUNK: u64 = 65536;

/// Separates a source URL from its resolved identity inside a content-cache
/// key.
const CONTENT_CACHE_IDENTITY_SEPARATOR: &str = "#identity=";

/// Content-cache key for one source URL.
///
/// A URL that is itself an identity — a `{version}`-templated release asset,
/// a pinned build's download — is keyed by the URL alone. A moving target — a
/// permalink that 302-redirects to a versioned download, or any `latest` alias
/// whose asset changes when upstream rotates — also carries its resolved
/// identity, so the bytes cached for one build can never answer for another:
/// without it the entry is found again on every sync, its last-use clock is
/// refreshed, and the payload freezes while the state records the newer build.
fn content_cache_key(url: &str, identity: Option<&str>) -> String {
    match identity {
        Some(identity) => format!("{url}{CONTENT_CACHE_IDENTITY_SEPARATOR}{identity}"),
        None => url.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Config version specification — used at the serde boundary.
///
/// Deserialized from:
/// - `"latest"` → fetch the latest available version
/// - `"inherit"` → use the dependency tool's global version spec (deps only)
/// - `{ vcs_hash?, version?, tag? }` → exact specification with optional fields
///
/// `Inherit` is resolved away before reaching internal code (see
/// [`VersionSpec`] for the resolved variant).
///
/// Custom serde is used instead of `#[serde(untagged)]` because unit variants
/// in an untagged enum serialize to JSON `null`, but we need string values
/// `"latest"` and `"inherit"` (matching the Nickel schema and JSON output).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigVersionSpec {
    /// `"latest"` — fetch the latest available version.
    Latest,
    /// `"inherit"` — use the dependency tool's global version spec.
    Inherit,
    /// `{ vcs_hash?, version?, tag? }` — exact fields.
    Exact(VersionSpecFields),
}

impl Serialize for ConfigVersionSpec {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            ConfigVersionSpec::Latest => serializer.serialize_str("latest"),
            ConfigVersionSpec::Inherit => serializer.serialize_str("inherit"),
            ConfigVersionSpec::Exact(fields) => fields.serialize(serializer),
        }
    }
}

impl<'de> serde::Deserialize<'de> for ConfigVersionSpec {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de;

        struct ConfigVersionSpecVisitor;

        impl<'de> de::Visitor<'de> for ConfigVersionSpecVisitor {
            type Value = ConfigVersionSpec;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str(
                    "\"latest\", \"inherit\", or an object with vcs_hash/version/tag fields",
                )
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<ConfigVersionSpec, E> {
                match v {
                    "latest" => Ok(ConfigVersionSpec::Latest),
                    "inherit" => Ok(ConfigVersionSpec::Inherit),
                    other => Err(de::Error::invalid_value(de::Unexpected::Str(other), &self)),
                }
            }

            fn visit_map<A: de::MapAccess<'de>>(
                self,
                map: A,
            ) -> Result<ConfigVersionSpec, A::Error> {
                let fields =
                    VersionSpecFields::deserialize(de::value::MapAccessDeserializer::new(map))?;
                Ok(ConfigVersionSpec::Exact(fields))
            }
        }

        deserializer.deserialize_any(ConfigVersionSpecVisitor)
    }
}

/// Resolved version spec — Inherit has been resolved at the config boundary.
///
/// No `Inherit` variant: that is resolved to the global tool's concrete spec
/// before reaching internal code. Only [`Latest`](VersionSpec::Latest)
/// (re-resolve on every sync) and [`Exact(VersionSpecFields)`](VersionSpec::Exact)
/// (specific version constraints).
///
/// This type is never serialized — only [`ConfigVersionSpec`] has serde.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VersionSpec {
    /// `"latest"` — fetch the latest available version on every sync.
    Latest,
    /// `{ vcs_hash?, version?, tag? }` — exact fields (at least one required).
    Exact(VersionSpecFields),
}

impl From<&ConfigVersionSpec> for VersionSpec {
    /// Converts the serde-facing spec into the resolved form handed to
    /// provider resolution.
    ///
    /// [`Inherit`](ConfigVersionSpec::Inherit) maps to
    /// [`Latest`](VersionSpec::Latest): `Inherit` is resolved to the global
    /// requirement's concrete spec before dependency resolution, and a
    /// global tool requirement still carrying `Inherit` at this point is
    /// rejected by the caller's post-resolve validation — provider
    /// resolution itself never sees the invalid state.
    fn from(spec: &ConfigVersionSpec) -> Self {
        match spec {
            ConfigVersionSpec::Latest | ConfigVersionSpec::Inherit => VersionSpec::Latest,
            ConfigVersionSpec::Exact(fields) => VersionSpec::Exact(fields.clone()),
        }
    }
}

/// Fields for exact version specification.
///
/// At least one field MUST be non-None (enforced at deserialization via
/// custom validator). Multiple fields may be present; when they are, they
/// must resolve to the same canonical version or provisioning will error.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VersionSpecFields {
    /// Version-control-system hash (git, mercurial, etc.). Exact string match.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vcs_hash: Option<String>,
    /// Version string. Exact string match.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// VCS tag. Exact string match.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
}

impl<'de> serde::Deserialize<'de> for VersionSpecFields {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Raw {
            #[serde(default)]
            vcs_hash: Option<String>,
            #[serde(default)]
            version: Option<String>,
            #[serde(default)]
            tag: Option<String>,
        }
        let raw = Raw::deserialize(deserializer)?;
        let has_any = raw.vcs_hash.is_some() || raw.version.is_some() || raw.tag.is_some();
        if !has_any {
            return Err(serde::de::Error::custom(
                "version_spec object must have at least one of: vcs_hash, version, tag",
            ));
        }
        Ok(Self { vcs_hash: raw.vcs_hash, version: raw.version, tag: raw.tag })
    }
}

/// How phase 2 produces bytes for one source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceProducer {
    /// Fetch bytes from URL candidates (tried in order).
    Fetch { urls: Vec<String> },
    /// Generate launcher script bytes that dispatch a builtin tool.
    GenerateLauncher {
        builtin_id: String,
        /// argv tokens inserted before `builtin_id` (for example `builtin` on mediapm CLI).
        argv_prefix: Vec<String>,
    },
}

impl SourceProducer {
    /// Builds one launcher producer for conductor-style builtin dispatch.
    #[must_use]
    pub fn launcher(builtin_id: impl Into<String>) -> Self {
        Self::GenerateLauncher { builtin_id: builtin_id.into(), argv_prefix: Vec::new() }
    }

    /// Builds one launcher producer for mediapm `builtin <subcommand>` dispatch.
    #[must_use]
    pub fn mediapm_builtin(subcommand: impl Into<String>) -> Self {
        Self::GenerateLauncher {
            builtin_id: subcommand.into(),
            argv_prefix: vec!["builtin".to_string()],
        }
    }
}

/// One resolved source after phase 1.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedSource {
    /// OS target label (e.g. `"linux"`, `"macos"`, `"windows"`).
    pub os: String,
    /// CPU architecture label the source's artifact is built for —
    /// `"x86_64"` or `"aarch64"`.
    ///
    /// Carried but not yet used in content-map keys (arch key segments land
    /// with the ffmpeg macOS chain). Sources publishing one universal
    /// artifact per OS (builtin launcher scripts, yt-dlp's universal macOS
    /// binary) label the host architecture via [`host_arch_label`].
    pub arch: String,
    /// How phase 2 produces bytes for this source.
    pub producer: SourceProducer,
    /// Expected byte size, if known from the prefetch probe.
    pub expected_size: Option<u64>,
    /// Optional soft upper bound for expected byte size, set from provider
    /// definitions. Used as a fallback estimate when the prefetch probe fails.
    pub size_hint_bytes: Option<u64>,
    /// Resolved identity of the bytes this source serves, when its URL is not
    /// itself one — a permalink that redirects to a versioned download whose
    /// build changes upstream, or a `latest` alias whose fixed path has its
    /// asset replaced upstream. The content cache keys on it (the URL plus this
    /// value, joined by `content_cache_key`), so cached bytes are only reused
    /// for the identity they were fetched under.
    ///
    /// `None` for a version-specific URL, whose URL is its identity. The
    /// identity is per source, not per candidate: it covers every URL that
    /// source names, while the cache key stays per URL, so a source's two
    /// permalink candidates keep distinct keys. A source that chains a
    /// versioned candidate behind its permalinks therefore also keys that
    /// candidate by the identity, which is harmless — a versioned URL is only
    /// re-fetched, never served a payload belonging to another build.
    pub identity: Option<String>,
}

/// Returns the architecture label for sources that publish one universal
/// artifact per OS.
///
/// Returns `"aarch64"` on ARM hosts and `"x86_64"` on `x86_64` hosts. Hosts
/// outside the two architectures the managed tools publish builds for
/// resolve `"x86_64"` — the widest-coverage build — rather than inventing a
/// label no release carries; such a host fails at fetch with a clear
/// download error instead of a fabricated arch at resolve.
#[must_use]
pub fn host_arch_label() -> String {
    match std::env::consts::ARCH {
        "aarch64" => "aarch64".to_string(),
        _ => "x86_64".to_string(),
    }
}

/// Phase 1 output: everything needed for phase 2.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedToolFetch {
    /// Logical tool identifier.
    pub tool_id: String,
    /// Per-platform source descriptors.
    pub sources: Vec<ResolvedSource>,
}

/// Phase 2 output: bytes of one resolved source.
#[derive(Debug, Clone)]
pub struct DownloadedSource {
    /// OS target label.
    pub os: String,
    /// CPU architecture label the source's artifact is built for —
    /// `"x86_64"` or `"aarch64"`.
    ///
    /// Carried through fetch so phase 3 can build arch-segmented content
    /// keys for dual-arch OS slots (see [`process_tool_sources`]) and pick
    /// the host-arch payload for the OS-keyed executable path.
    pub arch: String,
    /// The producer that created these bytes (preserved from phase 1).
    pub producer: SourceProducer,
    /// Raw bytes of the payload.
    pub bytes: Vec<u8>,
    /// Expected byte size, if known.
    pub expected_size: Option<u64>,
    /// The URL the bytes actually came from, when this source was fetched
    /// over HTTP: the final URL after redirects, so a permalink fetch
    /// reports the versioned download URL it resolved to. Cache-served
    /// entries carry the requested candidate URL — the cached bytes' redirect
    /// provenance is not retained, and the identity segment a content-cache
    /// key may carry is not part of a URL. `None` for launcher-generated
    /// sources.
    pub actual_url: Option<String>,
}

/// Phase 2 output: all downloaded or generated bytes for one tool.
#[derive(Debug, Clone)]
pub struct DownloadedSources {
    /// Logical tool identifier.
    pub tool_id: String,
    /// Per-platform source bytes.
    pub entries: Vec<DownloadedSource>,
    /// Number of sources that were served from the download cache
    /// (not fetched from the network).
    pub cached_count: usize,
    /// Per-candidate failures that a later chain member covered, in fetch
    /// order: the source provisioned, but a candidate it declares does not
    /// serve. Each entry is a user-facing sentence naming the tool, its OS
    /// and arch, the failed candidate with its cause, and the candidate that
    /// served instead. An empty list means every source was served by its
    /// first candidate, or from cache.
    ///
    /// Carried rather than merely logged so the mediapm layer can surface it
    /// in its `ToolSyncReport.warnings`: this crate must not depend on a
    /// terminal progress layer, and a silently-covered fallback is exactly
    /// the case an operator needs to see.
    pub warnings: Vec<String>,
}

/// Result of processing a single source: content map and executable path.
#[derive(Debug, Clone)]
pub struct ProcessedSource {
    /// Content map entries (os/path → hash).
    pub content_map: BTreeMap<String, String>,
    /// Executable path relative to the extraction root.
    pub exec_path: String,
}

/// Phase 3 output: final content map and per-OS executable paths for a tool.
#[derive(Debug, Clone)]
pub struct ProvisionResult {
    /// Content map: sandbox-relative path → CAS hash hex string.
    /// For archive formats the key is a trailing-slash directory entry
    /// (`{os}/`, or `{os}/{arch}/` for a dual-arch OS slot); for binary
    /// format it is a file-level entry (`{os}/{filename}`, arch-segmented
    /// the same way).
    pub content_map: BTreeMap<String, String>,
    /// Per-OS executable path (without OS prefix, e.g. `"sd-x86_64-linux"`).
    /// A dual-arch OS slot records the host-arch payload's path, prefixed
    /// with the arch segment (e.g. `"aarch64/ffmpeg"`), so the
    /// [`{os}/`-prefixed selector](super::helpers::build_os_conditional_selector)
    /// resolves to the payload's real location. Used by the preset layer to
    /// build the command template via
    /// [`build_os_conditional_selector`](super::helpers::build_os_conditional_selector).
    pub os_exec_paths: BTreeMap<String, String>,
}

// ██████████████████████████████████████████████████████████████████████████████
// Per-tool dispatchers
// ██████████████████████████████████████████████████████████████████████████████

pub(crate) mod archive;
pub(crate) mod echo;
pub(crate) mod export;
pub(crate) mod fs;
pub(crate) mod import;
/// Dispatches to the correct per-tool source resolver.
pub(crate) mod sd;

// ██████████████████████████████████████████████████████████████████████████████
// Phase 1 — Resolve
// ██████████████████████████████████████████████████████████████████████████████

/// Resolves metadata and source descriptors for the given tool.
///
/// # Errors
///
/// Returns [`ConductorError`](crate::error::ConductorError) when the tool is
/// unknown.
#[cfg(feature = "tool-presets")]
#[expect(clippy::unused_async, reason = "public async entrypoint kept for pipeline callers")]
pub async fn resolve_tool_fetch(
    tool_id: &str,
) -> Result<ResolvedToolFetch, crate::error::ConductorError> {
    match tool_id {
        n if n.eq_ignore_ascii_case("sd") => Ok(sd::sources()),
        n if n.eq_ignore_ascii_case("echo") => Ok(echo::sources()),
        n if n.eq_ignore_ascii_case("archive") => Ok(archive::sources()),
        n if n.eq_ignore_ascii_case("export") => Ok(export::sources()),
        n if n.eq_ignore_ascii_case("fs") => Ok(fs::sources()),
        n if n.eq_ignore_ascii_case("import") => Ok(import::sources()),
        _ => Err(crate::error::ConductorError::Workflow(format!(
            "tool {tool_id}: no provider registered for resolution"
        ))),
    }
}

// ██████████████████████████████████████████████████████████████████████████████
// Phase 2 — Fetch
// ██████████████████████████████████████████████████████████████████████████████

/// Fetches (or generates) bytes for every source in `fetch`.
///
/// URL sources are tried in order; the first HTTP 200 wins. Launcher sources
/// generate shell/batch script bytes in memory. Uses `cache` for deduplication
/// of downloaded payloads.
///
/// Progress reporting: the per-chunk total combines already-completed sizes
/// (`agg_completed_bytes`) + the best lower bound for the current source
/// (Content-Length if available, otherwise `downloaded_so_far`) + expected
/// sizes for unstarted sources (`remaining_expected`).  After download,
/// [`DownloadedSource::expected_size`] is set to the actual byte count
/// (or `max(expected, actual)` if a HEAD-based estimate existed).
///
/// # Panics
///
/// Panics if the parallel fetch phase yields fewer results than there are
/// `SourceProducer::Fetch` entries, which indicates internal index accounting
/// rather than a caller error.
///
/// # Errors
///
/// Returns [`ConductorError`](crate::error::ConductorError) when all URL
/// candidates fail or I/O fails.
#[cfg(feature = "tool-presets")]
#[expect(
    clippy::too_many_lines,
    reason = "the parallel futures' MultiItemBudget accounting must stay with the progress snapshots that read it"
)]
pub async fn fetch_tool_sources(
    fetch: &ResolvedToolFetch,
    cache: &crate::cache_user_level::UserLevelCache,
    domain: &str,
    progress_cb: Option<ProviderProgressCallback>,
) -> Result<DownloadedSources, crate::error::ConductorError> {
    type FetchResult = Result<FetchedBytes, crate::error::ConductorError>;

    let total = fetch.sources.len() as u64;

    // Create per-item budget: each source gets its own item.
    // Wrapped in Arc so concurrent fetch futures can share it —
    // MultiItemBudget uses AtomicU64 internally, so advance/set_total
    // are safe under concurrent access.
    let budget = Arc::new({
        let mut b = MultiItemBudget::with_capacity(fetch.sources.len());
        for src in &fetch.sources {
            let est = src.expected_size.or(src.size_hint_bytes).unwrap_or(0);
            b.add_item(est);
        }
        b
    });
    // Shared monotone count of sources that have finished downloading. Each
    // parallel future increments this exactly once, so the reported
    // `{completed}/{total}` prefix never moves backwards.
    let completed_sources = Arc::new(AtomicU64::new(0));

    // ── Parallel fetch phase ──────────────────────────────────────────
    //
    // Spawn one concurrent task per Fetch source. Each task performs its
    // own cache lookup, downloads on miss, and returns the source index
    // together with the result. GenerateLauncher sources are fast and
    // handled inline after the parallel fetch.
    //
    // Fire an initial progress snapshot so the bar shows the byte aggregate
    // immediately (matching the sequential contract where the first snapshot
    // fires before any source completes).
    if let Some(cb) = progress_cb.as_ref() {
        fire_progress(cb, ProviderPhase::Fetch, (0, total), &budget);
    }

    // Build futures for every Fetch source, each carrying its index and
    // owned URLs so the async block is self-contained.
    let fetch_futures: Vec<_> = fetch
        .sources
        .iter()
        .enumerate()
        .filter_map(|(idx, source)| match &source.producer {
            SourceProducer::Fetch { urls } => {
                let urls = urls.clone();
                let tool_id = fetch.tool_id.clone();
                let os_label = source.os.clone();
                let identity = source.identity.clone();
                let cb = progress_cb.clone();
                let budget = budget.clone();
                let completed_sources = Arc::clone(&completed_sources);
                Some(async move {
                    // Try cache lookup for each fallback URL, under the key
                    // that URL is stored with: the URL itself, plus the
                    // source's resolved identity when its URL is a moving
                    // permalink.
                    let mut cache_hit = None;
                    for url in &urls {
                        let key = content_cache_key(url, identity.as_deref());
                        if let Some(cached) = cache.lookup_bytes(domain, &key).await {
                            cache.touch(domain, &key);
                            cache_hit = Some((key, url.clone(), cached));
                            break;
                        }
                    }

                    if let Some((cache_key, requested_url, cached)) = cache_hit {
                        let size = cached.len() as u64;
                        budget.set_total(idx, size);
                        budget.advance(idx, size);
                        completed_sources.fetch_add(1, Ordering::AcqRel);
                        if let Some(ref cb) = cb {
                            fire_fetch_chunk_progress(cb, &completed_sources, total, &budget);
                        }
                        // A cache hit reports the candidate URL it was
                        // looked up under; the bytes' original redirect
                        // provenance is not retained, so the candidate is
                        // the best-known actual URL. The key's identity
                        // suffix is not part of a URL, and the bytes' final
                        // URL is derived from it, so the requested URL is
                        // reported unkeyed. A cache hit never reports
                        // failures — the chain was not walked.
                        let fetched = FetchedBytes {
                            bytes: cached,
                            cache_key: cache_key.clone(),
                            final_url: Some(requested_url),
                            failures: Vec::new(),
                        };
                        (idx, Ok(fetched), true)
                    } else {
                        let estimate = source.expected_size.or(source.size_hint_bytes).unwrap_or(0);
                        budget.set_total(idx, estimate);
                        let total_sources = total;
                        let result = fetch_bytes_from_candidates(
                            &urls,
                            &tool_id,
                            &os_label,
                            identity.as_deref(),
                            &budget,
                            idx,
                            &completed_sources,
                            total_sources,
                            cb.as_ref(),
                        )
                        .await;
                        match result {
                            Ok(fetched) => {
                                cache.store_bytes(domain, &fetched.cache_key, &fetched.bytes).await;
                                completed_sources.fetch_add(1, Ordering::AcqRel);
                                (idx, Ok(fetched), false)
                            }
                            Err(e) => (idx, Err(e), false),
                        }
                    }
                })
            }
            SourceProducer::GenerateLauncher { .. } => None,
        })
        .collect();

    // Run all fetch futures concurrently, bounded by default_source_fetch_concurrency().
    let fetch_concurrency = default_source_fetch_concurrency().min(fetch_futures.len()).max(1);
    let fetch_results: Vec<(usize, FetchResult, bool)> =
        stream::iter(fetch_futures).buffer_unordered(fetch_concurrency).collect().await;

    // Sort by source index to preserve original order (determinism).
    let mut fetch_results = fetch_results;
    fetch_results.sort_by_key(|&(idx, _, _)| idx);

    // ── Collect results in source order ───────────────────────────────
    let mut entries = Vec::with_capacity(fetch.sources.len());
    let mut fetch_iter = fetch_results.into_iter().peekable();
    let mut total_cached: usize = 0;
    // Chain members that a later member covered, as user-facing sentences.
    let mut warnings: Vec<String> = Vec::new();

    for (idx, source) in fetch.sources.iter().enumerate() {
        match &source.producer {
            SourceProducer::Fetch { urls } => {
                // The parallel fetch produced exactly one result per Fetch source,
                // in sorted order — advance the iterator.
                let (_, result, was_cached) =
                    fetch_iter.next().expect("parallel fetch result missing for Fetch source");
                let FetchedBytes { bytes, final_url: actual_url, failures, .. } = result?;
                if was_cached {
                    total_cached += 1;
                }
                // A chain served by a later member is a warning, not a
                // failure: the source provisioned, but a candidate it
                // declares does not serve. Naming the tool, OS and arch is
                // what lets an operator act on it.
                let tool_id = &fetch.tool_id;
                let os = &source.os;
                let arch = &source.arch;
                let served_url = actual_url.as_deref().unwrap_or("an unknown URL");
                for (url, cause) in failures {
                    warnings.push(format!(
                        "tool {tool_id} os {os} (arch {arch}): candidate failed ({cause} at \
                         {url}); served by {served_url}"
                    ));
                }
                entries.push(DownloadedSource {
                    os: source.os.clone(),
                    arch: source.arch.clone(),
                    producer: SourceProducer::Fetch { urls: urls.clone() },
                    expected_size: {
                        let actual_size = bytes.len() as u64;
                        source.expected_size.map(|s| s.max(actual_size)).or(Some(actual_size))
                    },
                    bytes,
                    actual_url,
                });
            }
            SourceProducer::GenerateLauncher { builtin_id, argv_prefix } => {
                // Launcher generation is in-memory, no I/O — run inline.
                let bytes = generate_launcher_script(source.os.as_str(), builtin_id, argv_prefix);
                let launcher_size = bytes.len() as u64;
                budget.set_total(idx, launcher_size);
                budget.advance(idx, launcher_size);
                completed_sources.fetch_add(1, Ordering::AcqRel);
                entries.push(DownloadedSource {
                    os: source.os.clone(),
                    arch: source.arch.clone(),
                    producer: SourceProducer::GenerateLauncher {
                        builtin_id: builtin_id.clone(),
                        argv_prefix: argv_prefix.clone(),
                    },
                    bytes,
                    expected_size: None,
                    actual_url: None,
                });
            }
        }
        if let Some(cb) = progress_cb.as_ref() {
            fire_fetch_chunk_progress(cb, &completed_sources, total, &budget);
        }
    }

    Ok(DownloadedSources {
        tool_id: fetch.tool_id.clone(),
        entries,
        cached_count: total_cached,
        warnings,
    })
}

/// Bytes served by one chain member, plus the members that failed first.
///
/// A chain that succeeds on a later member is not an error, but it is not
/// silent either: `failures` travels with the bytes so the caller can report
/// each covered candidate.
#[cfg(feature = "tool-presets")]
#[derive(Debug, Clone)]
struct FetchedBytes {
    /// The payload bytes the serving member produced.
    bytes: Vec<u8>,
    /// The key the download cache stores these bytes under — the serving
    /// candidate URL, plus the source's resolved identity when that URL is a
    /// moving permalink (see [`content_cache_key`]), since a later run
    /// replays the same candidate list under the identity it resolves.
    cache_key: String,
    /// The URL the bytes are attributed to. The final URL after redirects
    /// when the bytes came over HTTP — a permalink fetch reports the
    /// versioned download it resolved to. `Some` of the requested candidate
    /// URL when the caller served them from cache instead, whose redirect
    /// provenance is not retained. Never a content-cache key: the requested
    /// URL is reported unkeyed, so downstream URL parsing sees a URL.
    final_url: Option<String>,
    /// Candidates that failed before `cache_key` served, in chain order, as
    /// `(candidate url, cause)`. Empty when the first candidate served.
    failures: Vec<(String, String)>,
}

/// Downloads bytes from URL candidates (tried in order — one chain).
///
/// On success, returns a [`FetchedBytes`] naming the bytes, the candidate URL
/// that served them (keyed for the download cache by
/// [`content_cache_key`]: the URL, plus `identity` when the URL is a moving
/// permalink), the final URL after redirects when the bytes came over HTTP,
/// and every candidate that failed first. A cache-served source never reaches
/// this function: the caller reports the candidate as its actual URL, because
/// the cached bytes' redirect provenance is not retained.
///
/// `identity` is the source's resolved identity — what the bytes will be, when
/// a permalink's URL alone does not determine it. It is the same value the
/// caller's cache lookup keys with, so a stored payload is only ever found
/// again under the identity it was fetched for.
///
/// Each successfully-downloaded candidate is verified against its sibling
/// `{url}.sha256` when the endpoint serves one (see
/// [`verify_sha256_sibling`]); a mismatch fails that candidate and the next
/// chain member is tried. Every failure is logged at `warn`, and the returned
/// `failures` list is what lets the sync layer report a chain a later member
/// covered. Only chain exhaustion produces an error, and the error names
/// every endpoint with its per-candidate cause.
///
/// Advances the budget item per HTTP chunk and fires the progress callback
/// after each chunk, so the progress bar updates smoothly during large
/// downloads instead of freezing until the payload is fully received.
///
/// The item counter reported through the callback is the shared
/// completed-source count (monotone), not this source's own index — see
/// [`fire_fetch_chunk_progress`].
///
/// # HTTP client policy
///
/// Uses the process-wide shared client from [`crate::http::client`].
/// Connection pooling, TLS reuse, and DNS caching are managed centrally.
/// Do NOT create a [`reqwest::Client`] locally — always use the shared instance.
#[cfg(feature = "tool-presets")]
#[expect(
    clippy::too_many_arguments,
    reason = "pipeline plumbing passes per-source context explicitly"
)]
async fn fetch_bytes_from_candidates(
    urls: &[String],
    tool_id: &str,
    os_label: &str,
    identity: Option<&str>,
    budget: &MultiItemBudget,
    item_idx: usize,
    completed_sources: &AtomicU64,
    total_sources: u64,
    progress_cb: Option<&ProviderProgressCallback>,
) -> Result<FetchedBytes, crate::error::ConductorError> {
    use crate::error::ConductorError;
    use futures_util::StreamExt;

    let client = crate::http::client::shared_http_client()
        .map_err(|e| ConductorError::Workflow(format!("HTTP client unavailable: {e}")))?;

    // Per-candidate failure causes, kept in chain order so the exhaustion
    // error can name every endpoint the chain tried.
    let mut failures: Vec<(String, String)> = Vec::with_capacity(urls.len());
    // Bytes already advanced for candidates that downloaded but then failed
    // (checksum mismatch, mid-stream error). The budget is per source and
    // monotone, so a later candidate's totals include the bytes spent on
    // earlier ones — the work was really done, the bar just keeps counting.
    let mut spent_bytes: u64 = 0;

    for url in urls {
        let request = client.get(url);
        match request.send().await {
            Ok(response) if response.status().is_success() => {
                let final_url = response.url().to_string();
                let total_bytes = response.content_length();
                let mut buffer = Vec::new();
                let mut stream = response.bytes_stream();
                let mut download_error: Option<String> = None;
                while let Some(chunk_result) = stream.next().await {
                    match chunk_result {
                        Ok(chunk) => {
                            buffer.extend_from_slice(&chunk);
                            // Advance budget per-chunk so progress bar updates
                            // smoothly. Failed candidates' bytes stay counted
                            // (`spent_bytes`), keeping pos ≤ total monotone
                            // across chain members.
                            let current_estimate =
                                spent_bytes + total_bytes.unwrap_or(buffer.len() as u64);
                            budget.set_total(item_idx, current_estimate);
                            budget.advance(item_idx, chunk.len() as u64);
                            if let Some(cb) = progress_cb {
                                fire_fetch_chunk_progress(
                                    cb,
                                    completed_sources,
                                    total_sources,
                                    budget,
                                );
                            }
                        }
                        Err(e) => {
                            tracing::warn!(
                                "mid-stream download error for {url}: {e}; trying the next candidate"
                            );
                            download_error = Some(format!("download error: {e}"));
                            break;
                        }
                    }
                }
                // A mid-stream download error fails this candidate; try the
                // next chain member.
                if let Some(cause) = download_error {
                    spent_bytes += buffer.len() as u64;
                    failures.push((url.clone(), cause));
                    continue;
                }
                // Verify the serving endpoint's sibling .sha256 — the final
                // URL's, so a redirected permalink checks the versioned
                // download's checksum — when one is served; a mismatch fails
                // this candidate and falls through to the next chain member.
                if let Err(cause) = verify_sha256_sibling(client, &final_url, &buffer).await {
                    tracing::warn!("{url} failed verification: {cause}; trying the next candidate");
                    spent_bytes += buffer.len() as u64;
                    failures.push((url.clone(), cause));
                    continue;
                }
                return Ok(FetchedBytes {
                    bytes: buffer,
                    cache_key: content_cache_key(url, identity),
                    final_url: Some(final_url),
                    failures,
                });
            }
            Ok(response) => {
                tracing::warn!("HTTP {} for {}, skipping", response.status(), url);
                failures.push((url.clone(), format!("HTTP {}", response.status())));
            }
            Err(e) => {
                tracing::warn!("HTTP error for {url}: {e}, skipping");
                failures.push((url.clone(), format!("request failed: {e}")));
            }
        }
    }

    let endpoints = failures
        .iter()
        .map(|(url, cause)| format!("{url}: {cause}"))
        .collect::<Vec<_>>()
        .join("; ");
    Err(ConductorError::Workflow(format!(
        "tool {tool_id} os {os_label}: all sources exhausted: {endpoints}"
    )))
}

/// Verifies downloaded bytes against the serving endpoint's sibling
/// `{url}.sha256`.
///
/// `url` is the final URL the bytes came from (after redirects), so a
/// permalink-style candidate is checked against the versioned download's
/// checksum. An endpoint that does not serve a checksum (404/transport
/// error) or serves an unparseable one passes verification with a warning —
/// the checksum rule is "verify when served", never "require one". A served,
/// parseable checksum that does not match the bytes fails the candidate
/// that produced them.
///
/// # HTTP client policy
///
/// Uses the process-wide shared client from [`crate::http::client`] — the
/// caller passes the already-created shared instance.
#[cfg(feature = "tool-presets")]
async fn verify_sha256_sibling(
    client: &reqwest::Client,
    url: &str,
    bytes: &[u8],
) -> Result<(), String> {
    use sha2::Digest;
    // GitHub release assets (and their CDN redirects) never serve a sibling
    // `.sha256`, so probing them spends one wasted round-trip per download.
    // Gate on the serving host before issuing the checksum request; any
    // other host (martin-riedl, jellyfin, and hermetic test servers) is
    // still probed, since "verify when served" must keep working wherever a
    // checksum might appear.
    if !url_serves_checksum(url) {
        return Ok(());
    }
    let checksum_url = format!("{url}.sha256");
    // A short timeout: the checksum file is a few dozen bytes; an endpoint
    // that stalls on it is treated as not serving one.
    let response =
        match client.get(&checksum_url).timeout(std::time::Duration::from_secs(10)).send().await {
            Ok(response) if response.status().is_success() => response,
            _ => return Ok(()),
        };
    let body = response.text().await.unwrap_or_default();
    let Some(expected) = parse_sha256_checksum(&body) else {
        tracing::warn!("unparseable sha256 checksum at {checksum_url}; accepting {url} unverified");
        return Ok(());
    };
    let actual = format!("{:x}", sha2::Sha256::digest(bytes));
    if actual == expected { Ok(()) } else { Err(format!("sha256 mismatch with {checksum_url}")) }
}

/// Parses the first token of a `.sha256` body when it is a 64-hex-digit
/// SHA-256 digest (the `sha256sum` format's leading column, or a bare hex
/// digest). Returns `None` for anything else.
#[cfg(feature = "tool-presets")]
fn parse_sha256_checksum(body: &str) -> Option<String> {
    let token = body.split_whitespace().next()?;
    (token.len() == 64 && token.bytes().all(|b| b.is_ascii_hexdigit()))
        .then(|| token.to_ascii_lowercase())
}

/// Whether the URL's host is one known to serve a sibling `.sha256`.
///
/// GitHub release assets (web downloads and their
/// `objects.githubusercontent.com` redirects) never publish a `.sha256`, so
/// returning `false` for them skips the doomed checksum probe. Every other
/// host — martin-riedl, jellyfin, and hermetic test servers — is still
/// probed, because the checksum rule is "verify when served" and a test or
/// mirror may serve one.
#[cfg(feature = "tool-presets")]
fn url_serves_checksum(url: &str) -> bool {
    let host = url.split("//").nth(1).and_then(|rest| rest.split('/').next()).unwrap_or("");
    let host = host.strip_prefix("www.").unwrap_or(host).to_ascii_lowercase();
    !(host == "github.com"
        || host.ends_with(".github.com")
        || host == "objects.githubusercontent.com"
        || host.ends_with(".githubusercontent.com"))
}

/// Generates a launcher script that dispatches `$MEDIAPM_EXECUTABLE <argv...> {args...}`.
///
/// The launcher is the only place that knows `MEDIAPM_EXECUTABLE` is
/// mandatory, so every emitted script tests the variable before it dispatches:
/// a missing value otherwise surfaces as a step failure nowhere near its cause,
/// either as an empty command or as whatever binary happened to hold the name.
/// The message the guard prints names the build remedy, so the failure states
/// its own fix. The variable must hold the `mediapm` CLI binary; any other
/// value turns the step into a different program.
///
/// POSIX hosts (`os` other than `"windows"`) get a `#!/bin/sh` script whose
/// second line is a `:?` guard and whose third line `exec`s the CLI with the
/// dispatch arguments. Windows gets a CRLF batch script whose guard runs
/// `if not defined` / `exit /b 1`. The dispatch arguments themselves are the
/// same on both shapes and follow the guard unchanged.
///
/// The returned bytes are the script text verbatim; nothing here touches the
/// filesystem or fails.
#[cfg(feature = "tool-presets")]
fn generate_launcher_script(os: &str, builtin_id: &str, argv_prefix: &[String]) -> Vec<u8> {
    let mut dispatch_argv = argv_prefix.to_vec();
    dispatch_argv.push(builtin_id.to_string());
    let dispatch = dispatch_argv.join(" ");
    match os {
        "windows" => format!(concat!(
            "@echo off\r\n",
            "if not defined MEDIAPM_EXECUTABLE (\r\n",
            "echo mediapm CLI executable is not set; build it with \"cargo build --package mediapm\" or set MEDIAPM_EXECUTABLE 1>&2\r\n",
            "exit /b 1\r\n",
            ")\r\n",
            "\"%MEDIAPM_EXECUTABLE%\" {} %*\r\n",
        ), dispatch)
        .into_bytes(),
        _ => format!(concat!(
            "#!/bin/sh\n",
            ": \"${{MEDIAPM_EXECUTABLE:?mediapm CLI executable is not set; build it with 'cargo build --package mediapm' or export MEDIAPM_EXECUTABLE}}\"\n",
            "exec \"${{MEDIAPM_EXECUTABLE}}\" {} \"$@\"\n",
        ), dispatch)
        .into_bytes(),
    }
}

// ██████████████████████████████████████████████████████████████████████████████
// Phase 3 — Process
// ██████████████████████████████████████████████████████████████████████████████

/// Processes downloaded sources: extract archives, import to CAS, build
/// content map and per-OS executable paths.
///
/// For archive formats (ZIP, tar.gz, tar.xz) the extracted directory is
/// repacked into a single uncompressed ZIP and imported to CAS, producing
/// a single trailing-slash content-map key (`{os}/`, or `{os}/{arch}/` for
/// an OS label shared by two sources — a dual-arch slot). For
/// binary/launcher format the data is directly CAS-imported as a
/// file-level entry (`{os}/{filename}`, arch-segmented the same way).
///
/// Archive format is inferred from the URL extension.
///
/// # Progress behavior
///
/// ## Initial bar total is item count (intentional)
///
/// The progress bar is created with `total = total_process_items` (e.g., 6 for
/// 3 archive sources × 2 items each). Before the first source starts
/// processing, the byte-level aggregate is just the sum of initial totals.
/// The bar briefly shows the item count as the byte total — this is
/// intentional and unavoidable: the byte total can only be computed from the
/// actual payload sizes, which are known only when processing begins.
///
/// ## Total refining across sources (expected)
///
/// The aggregate byte total grows as each source is processed because each
/// source's budget items are added to the total when the source begins
/// processing. This is expected: the total is the sum of all processed bytes
/// across all sources, and it increases as more sources are accounted for.
/// The per-source estimate refines further after extraction (from compressed
/// estimate to actual directory size), which is also expected behavior
/// documented in `estimate_uncompressed_size`.
///
/// ## Progress bar smoothness
///
/// Progress callbacks are threaded through `process_single_source` and
/// the extraction helpers so that per-chunk callbacks fire during archive
/// extraction (and during repacking to CAS). This gives the progress bar
/// smooth ~20Hz updates instead of freezing for seconds at a time during
/// the decompression of large archives (yt-dlp, ffmpeg, deno, rsgain).
///
/// The `fire_progress` helper function is the single push point for all
/// progress snapshots. It aggregates the budget state and dispatches it
/// through the provider's progress callback. Both the fetch and process
/// phases use this shared helper, so snapshot semantics stay consistent.
///
/// - **ZIP extraction**: each entry's `compressed_size()` from the ZIP
///   central directory is used as the position weight, so progress tracks
///   compressed bytes processed (input size). A callback fires after each
///   entry with cumulative compressed bytes as the position.
///
/// - **tar.gz / tar.xz extraction**: the compressed payload size is used
///   as the total, and a `CountingReader` tracks how many compressed
///   bytes have been consumed by the decoder. A callback fires after each
///   tar entry, and sub-entry callbacks fire every `SUB_ENTRY_CHUNK` (64
///   KiB) bytes consumed.
///
/// - **Binary / launcher sources**: a single callback fires after the
///   source is fully processed (CAS import is an instant in-memory
///   operation).
///
/// - **Repacking (compress item)**: the `pack_directory_to_uncompressed_zip_bytes`
///   function fires sub-entry callbacks every `SUB_ENTRY_CHUNK` bytes
///   written, keeping the bar smooth during the repack phase.
///
/// The per-source item callback in the main loop below advances the item
/// counter — the prefix shows `{tool} [process] 1/3`, `2/3`, `3/3` — while
/// the per-chunk extraction/repack callbacks above smoothly fill the bytes
/// bar.
///
/// Progress reporting uses [`DownloadedSource::expected_size`] for the
/// aggregated byte total (falling back to `bytes.len()` when unset).  The
/// total accounts for both compressed input bytes during extraction and
/// decompressed bytes during repacking, keeping the progress bar smooth
/// through the full process pipeline.
///
/// [`MAX_LOOKAHEAD`] (16) bounds the number of concurrent prefetch probes
/// during phase 1 (resolve). `SUB_ENTRY_CHUNK` (64 KiB) controls the
/// minimum byte interval between sub-entry progress callbacks during
/// extraction/compression, preventing excessive callback overhead.
///
/// Extraction directories are per source (`{temp_root}/{os_label}/{source_index}`)
/// rather than per OS label, so two sources that happened to share an OS label
/// could never repack each other's files. Duplicate `content_map` keys are
/// rejected before they can overwrite one another.
///
/// # Errors
///
/// Returns [`ConductorError`](crate::error::ConductorError) when extraction,
/// packing, or CAS import fails.
#[cfg(feature = "tool-presets")]
pub async fn process_tool_sources(
    downloaded: &DownloadedSources,
    cas: &impl mediapm_cas::CasApi,
    progress_cb: Option<ProviderProgressCallback>,
) -> Result<ProvisionResult, crate::error::ConductorError> {
    let temp_root = mediapm_utils::temp::artifact_dir().map_err(|source| {
        crate::error::ConductorError::io(
            "creating temp directory for tool extraction",
            std::path::PathBuf::new(),
            source,
        )
    })?;

    let mut content_map: BTreeMap<String, String> = BTreeMap::new();
    let mut os_exec_paths: BTreeMap<String, String> = BTreeMap::new();

    // OS labels carrying sources for MORE THAN ONE distinct architecture are
    // dual-arch slots (the ffmpeg macOS chain publishes an `aarch64` and an
    // `x86_64` payload): their content keys gain an arch segment —
    // `{os}/{arch}/` or `{os}/{arch}/{filename}` — so the two payloads never
    // collide. OS labels with a single source — or with several sources that
    // all share one architecture — keep the flat `{os}/...` shape, so no
    // existing tool's content-addressed keys churn. Several sources under one
    // OS label that all carry the SAME arch would resolve to identical flat
    // keys and are rejected outright rather than silently overwriting.
    let (os_entry_counts, os_arches) =
        collect_os_slot_arches(&downloaded.tool_id, &downloaded.entries)?;

    // Compute total items: archive sources get 2 items (decompress + compress),
    // binary/launcher sources get 1 item.
    let total_items: usize =
        downloaded.entries.iter().map(|e| if is_archive_source(e) { 2usize } else { 1usize }).sum();
    let mut budget = MultiItemBudget::with_capacity(total_items);
    for entry in &downloaded.entries {
        let is_archive = is_archive_source(entry);
        let initial_total = entry.expected_size.unwrap_or(entry.bytes.len() as u64);
        budget.add_item(initial_total); // item 0: decompress (or single binary item)
        if is_archive {
            let (archive_format, _) = resolve_format_and_filename(entry, &downloaded.tool_id);
            let compress_estimate = estimate_uncompressed_size(&entry.bytes, archive_format);
            budget.add_item(compress_estimate); // item 1: compress — starts at accurate estimate
        }
    }
    let total_items_u64 = total_items as u64;

    // Fire initial progress snapshot so the bar shows the byte aggregate
    // immediately, not just the item count from bar construction.
    if let Some(cb) = progress_cb.as_ref() {
        fire_progress(cb, ProviderPhase::Process, (0, total_items_u64), &budget);
    }

    let mut next_item_idx: usize = 0;
    for (source_idx, source) in downloaded.entries.iter().enumerate() {
        let os_label = &source.os;
        let slot_size = os_entry_counts.get(os_label.as_str()).copied().unwrap_or(1);
        let distinct_arches = os_arches.get(os_label.as_str()).map_or(0, BTreeSet::len);
        let is_dual_arch_slot = slot_size > 1 && distinct_arches >= 2;
        // Content-key prefix: arch-segmented for dual-arch slots, flat
        // otherwise (see the `os_entry_counts` rule above).
        let key_prefix = if is_dual_arch_slot {
            format!("{os_label}/{}", source.arch)
        } else {
            os_label.clone()
        };
        // Extraction is scoped per source, not per OS label: if two sources
        // ever shared an OS label, a shared directory would let the second
        // extraction repack the first source's files. The duplicate-key guard
        // above rejects that case outright; this keeps the two concerns
        // independent.
        let os_dir = temp_root.path().join(os_label).join(source_idx.to_string());
        let is_archive = is_archive_source(source);
        let item_count = if is_archive { 2usize } else { 1usize };

        let (archive_format, filename) = resolve_format_and_filename(source, &downloaded.tool_id);

        let processed = process_single_source(
            &source.bytes,
            archive_format,
            os_label,
            &key_prefix,
            &downloaded.tool_id,
            &os_dir,
            &filename,
            cas,
            &budget,
            next_item_idx,
            item_count,
            progress_cb.as_ref(),
            next_item_idx as u64,
            total_items_u64,
        )
        .await?;

        for (key, hash) in processed.content_map {
            if content_map.insert(key.clone(), hash).is_some() {
                return Err(crate::error::ConductorError::Workflow(format!(
                    "tool {}: duplicate content_map key '{key}' — two sources resolve to the \
                     same target (archive sources key on '{{os}}/' or '{{os}}/{{arch}}/', \
                     binary sources on '{{os}}/{{filename}}'); refusing to silently overwrite",
                    downloaded.tool_id
                )));
            }
        }
        // The OS-keyed executable path stays os-keyed: a dual-arch slot
        // records only the host-arch payload (its exec path prefixed with
        // the arch segment, so the `{os}/`-prefixed selector resolves to
        // `{os}/{host_arch}/...`), and the non-host entry records nothing.
        // A single-source OS slot records the exec path unchanged.
        if !is_dual_arch_slot || source.arch == host_arch_label() {
            let exec_value = if is_dual_arch_slot {
                format!("{}/{}", source.arch, processed.exec_path)
            } else {
                processed.exec_path
            };
            os_exec_paths.insert(os_label.clone(), exec_value);
        }

        if let Some(cb) = progress_cb.as_ref() {
            fire_progress(
                cb,
                ProviderPhase::Process,
                ((next_item_idx + item_count) as u64, total_items_u64),
                &budget,
            );
        }
        next_item_idx += item_count;
    }

    Ok(ProvisionResult { content_map, os_exec_paths })
}

/// The per-OS-label source counts and the per-label set of architecture
/// labels, as computed by [`collect_os_slot_arches`].
#[cfg(feature = "tool-presets")]
type OsSlotArches<'a> = (BTreeMap<&'a str, usize>, BTreeMap<&'a str, BTreeSet<&'a str>>);

/// Counts sources and collects the distinct architectures per OS label,
/// rejecting any OS label that carries more than one source but fewer than
/// two distinct architectures.
///
/// An OS label with several sources all sharing one architecture would
/// resolve to identical flat content-map keys, silently overwriting one
/// another; refusing here names the tool and the offending label so the
/// configuration error is caught before any extraction. Returns the per-label
/// source count and the per-label set of architecture labels on success.
#[cfg(feature = "tool-presets")]
fn collect_os_slot_arches<'a>(
    tool_id: &str,
    entries: &'a [DownloadedSource],
) -> Result<OsSlotArches<'a>, crate::error::ConductorError> {
    let mut os_entry_counts: BTreeMap<&str, usize> = BTreeMap::new();
    let mut os_arches: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    for entry in entries {
        *os_entry_counts.entry(entry.os.as_str()).or_default() += 1;
        os_arches.entry(entry.os.as_str()).or_default().insert(entry.arch.as_str());
    }
    for (os_label, count) in &os_entry_counts {
        let distinct_arches = os_arches.get(os_label).map_or(0, BTreeSet::len);
        if *count > 1 && distinct_arches < 2 {
            return Err(crate::error::ConductorError::Workflow(format!(
                "tool {tool_id}: OS label '{os_label}' has {count} sources but only \
                 {distinct_arches} distinct architecture(s); same-OS same-arch sources would \
                 collide on identical content-map keys. Either give each source a distinct \
                 architecture or a distinct OS label."
            )));
        }
    }
    Ok((os_entry_counts, os_arches))
}

/// Infers archive format from a URL's file extension.
///
/// Returns `Some(format)` for recognized archive extensions, or `None` for
/// binary/launcher payloads.
#[cfg(feature = "tool-presets")]
fn infer_archive_format(url: &str) -> Option<&'static str> {
    let url_path = url.split('?').next().unwrap_or(url);
    let filename = url_path.trim_end_matches('/').split('/').next_back().unwrap_or(url_path);
    if filename.ends_with(".tar.xz") {
        Some(ARCHIVE_TAR_XZ)
    } else if filename.ends_with(".tar.gz")
        || std::path::Path::new(filename)
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("tgz"))
    {
        Some(ARCHIVE_TAR_GZ)
    } else if std::path::Path::new(filename)
        .extension()
        .is_some_and(|ext| ext.eq_ignore_ascii_case("zip"))
        || filename == "zip"
    {
        Some(ARCHIVE_ZIP)
    } else {
        // No recognized extension → binary (not an archive)
        None
    }
}

/// Detects an archive container from a payload's leading magic bytes.
///
/// Magic outranks the URL extension because a source may declare its
/// candidates in more than one container format: the ffmpeg macOS chain
/// advertises a martin-riedl `.zip` permalink first and a jellyfin `.tar.xz`
/// last, so the bytes that actually arrived are the only authoritative
/// statement of which container they are. A payload matching no magic is not
/// classified here; [`archive_format_of_entry`] then falls back to the served
/// URL, which keeps a declared-but-unrecognizable archive on the extraction
/// path (where it fails loudly) instead of importing it as an opaque binary.
#[cfg(feature = "tool-presets")]
fn infer_archive_format_from_bytes(bytes: &[u8]) -> Option<&'static str> {
    if bytes.starts_with(&[0xfd, 0x37, 0x7a, 0x58, 0x5a, 0x00]) {
        Some(ARCHIVE_TAR_XZ)
    } else if bytes.starts_with(&[0x50, 0x4b, 0x03, 0x04])
        || bytes.starts_with(&[0x50, 0x4b, 0x05, 0x06])
        || bytes.starts_with(&[0x50, 0x4b, 0x07, 0x08])
    {
        Some(ARCHIVE_ZIP)
    } else if bytes.starts_with(&[0x1f, 0x8b]) {
        Some(ARCHIVE_TAR_GZ)
    } else {
        None
    }
}

/// Returns the URL that describes a downloaded source: the one its bytes came
/// from when the fetch recorded one, and the first declared candidate
/// otherwise.
///
/// `actual_url` is the post-redirect URL for a network fetch and the requested
/// candidate URL for a cache hit, so it names the chain member that served this
/// source rather than the chain's nominal head. Every fetch records it, so the
/// fallback arm is unreachable for entries this module produces; it exists so a
/// constructed entry without a served URL still resolves to something finite.
fn served_source_url(entry: &DownloadedSource) -> &str {
    if let Some(url) = entry.actual_url.as_deref() {
        return url;
    }
    match &entry.producer {
        SourceProducer::Fetch { urls } => urls.first().map_or("", String::as_str),
        SourceProducer::GenerateLauncher { .. } => "",
    }
}

/// Resolves one downloaded source's archive format.
///
/// The served bytes decide when they match a known container, and the served
/// URL's extension decides otherwise; see [`infer_archive_format_from_bytes`]
/// for why the bytes outrank the URL.
#[cfg(feature = "tool-presets")]
fn archive_format_of_entry(entry: &DownloadedSource) -> Option<&'static str> {
    infer_archive_format_from_bytes(&entry.bytes)
        .or_else(|| infer_archive_format(served_source_url(entry)))
}

/// Returns `true` if the source is an archive download.
///
/// Archive sources produce compressed payloads that require decompression
/// (e.g. `.zip`, `.tar.gz`, `.tar.xz`). Binary and launcher sources are used
/// as-is. This is [`archive_format_of_entry`] read as a boolean so the item
/// budget and the extraction decision cannot disagree about one source.
fn is_archive_source(entry: &DownloadedSource) -> bool {
    archive_format_of_entry(entry).is_some()
}

/// Resolves one downloaded source's archive format and binary filename.
///
/// Extracted from inline code in the per-source loop of `process_tool_sources`
/// to be reusable in budget setup (before the loop) and during processing. An
/// archive's filename is empty because `process_single_source` never uses it;
/// a binary payload's filename is the basename of the URL the payload came
/// from, so a chain member that serves under a name different from the
/// chain's first candidate still keys under the name that arrived. Query and
/// fragment are dropped before the basename is taken, matching how
/// [`infer_archive_format`] reads an extension out of the same URL.
fn resolve_format_and_filename(
    entry: &DownloadedSource,
    tool_id: &str,
) -> (Option<&'static str>, String) {
    match &entry.producer {
        SourceProducer::Fetch { .. } => {
            let fmt = archive_format_of_entry(entry);
            let fname = if fmt.is_some() {
                String::new()
            } else {
                let url = served_source_url(entry);
                let path = url.split(['?', '#']).next().unwrap_or(url);
                path.split('/').rfind(|segment| !segment.is_empty()).unwrap_or(tool_id).to_string()
            };
            (fmt, fname)
        }
        SourceProducer::GenerateLauncher { .. } => (None, tool_id.to_string()),
    }
}

/// Reads an XZ variable-length integer from `data` at position `pos`.
///
/// Returns `None` on EOF or overflow. The XZ varint format uses little-endian
/// 7-bit groups with a continuation bit (bit 7 = 1 means more bytes follow).
fn read_xz_varint(data: &[u8], pos: &mut usize) -> Option<u64> {
    if *pos >= data.len() {
        return None;
    }
    let mut result = 0u64;
    let mut shift = 0;
    loop {
        let byte = *data.get(*pos)?;
        *pos += 1;
        result |= u64::from(byte & 0x7F) << shift;
        if byte & 0x80 == 0 {
            return Some(result);
        }
        shift += 7;
        if shift > 63 {
            return None;
        }
    }
}

/// Parses the XZ Stream Index to determine the total uncompressed size.
///
/// Returns `None` on any parse failure (falling back to compressed size).
fn parse_xz_index(bytes: &[u8]) -> Option<u64> {
    // Minimum: Stream Footer (12 bytes) + Index Indicator (1) + No Records
    // (1 varint for count = 0) + CRC32 (4) = 18 bytes
    if bytes.len() < 18 {
        return None;
    }

    // Parse Stream Footer (last 12 bytes)
    let footer = &bytes[bytes.len() - 12..];

    // Backward Size: bytes 4-7 (after CRC32 at 0-3), stored directly as
    // (index_size / 4 - 1) in the lower 24 bits (liblzma does NOT complement
    // despite the spec mentioning complementing).
    let backward_size_raw = u32::from_le_bytes([footer[4], footer[5], footer[6], footer[7]]);
    let backward_size = u64::from(backward_size_raw & 0x00FF_FFFF);

    // Index size in bytes = (backward_size + 1) * 4
    let index_size = (backward_size + 1) * 4;

    let index_start =
        bytes.len() - 12 - usize::try_from(index_size).expect("xz index size fits usize");

    if index_start >= bytes.len() {
        return None;
    }

    let index_data = &bytes[index_start..bytes.len() - 12];

    // Index Indicator: must be 0x00
    if index_data.is_empty() || index_data[0] != 0x00 {
        return None;
    }

    let mut pos: usize = 1;

    // Number of Records (varint)
    let num_records = read_xz_varint(index_data, &mut pos)?;

    let mut total_uncompressed = 0u64;
    for _ in 0..num_records {
        let _unpadded_size = read_xz_varint(index_data, &mut pos)?;
        let uncompressed_size = read_xz_varint(index_data, &mut pos)?;
        total_uncompressed = total_uncompressed.saturating_add(uncompressed_size);
    }

    Some(total_uncompressed)
}

/// Estimates the uncompressed size of archive bytes before extraction.
///
/// For ZIP archives: reads central directory metadata and sums per-entry
/// uncompressed sizes for an accurate estimate.
/// For tar.gz archives: parses the gzip trailer ISIZE field (last 4 bytes,
/// little-endian u32) for the exact uncompressed size of single-member
/// gzip streams.
/// For tar.xz archives: parses the XZ Stream Index for the exact total
/// uncompressed size.
/// Returns 0 for non-archive (binary/launcher) formats.
fn estimate_uncompressed_size(bytes: &[u8], format: Option<&str>) -> u64 {
    match format {
        Some(ARCHIVE_ZIP) => {
            // Sum uncompressed sizes from ZIP central directory metadata.
            // This is fast — no decompression, just parsing the directory.
            let Ok(mut archive) = zip::ZipArchive::new(std::io::Cursor::new(bytes)) else {
                // Fall back to compressed size if ZIP metadata is corrupt.
                return bytes.len() as u64;
            };
            let mut total: u64 = 0;
            for i in 0..archive.len() {
                if let Ok(file) = archive.by_index(i) {
                    total = total.saturating_add(file.size());
                }
            }
            total
        }
        Some(ARCHIVE_TAR_GZ) => {
            // Parse gzip trailer ISIZE (last 4 bytes, little-endian u32)
            // for exact uncompressed size of single-member gzip streams.
            // RFC 1952: ISIZE is the size of original (uncompressed) data
            // modulo 2^32. For tool archives (<4 GiB decompressed) this is
            // exact. Multi-member gzip is rare for tool distributions;
            // fall back to compressed size if parsing fails or looks wrong.
            if bytes.len() >= 18 {
                // 10 header + 8 minimum data/trailer
                let isize_bytes = &bytes[bytes.len() - 4..];
                let isize = u64::from(u32::from_le_bytes([
                    isize_bytes[0],
                    isize_bytes[1],
                    isize_bytes[2],
                    isize_bytes[3],
                ]));
                // ISIZE wraps at 4 GiB. Zero means the original was a
                // multiple of 4 GiB — vanishingly unlikely for tool archives.
                // Use ISIZE when non-zero; it's always more accurate than
                // compressed size even for small payloads where ISIZE < len.
                if isize > 0 {
                    return isize;
                }
            }
            bytes.len() as u64
        }
        Some(ARCHIVE_TAR_XZ) => {
            // Parse XZ Stream Index for exact total uncompressed size.
            // This is metadata-only — no decompression required.
            parse_xz_index(bytes).unwrap_or({
                // Fallback: use compressed size if Index parsing fails.
                bytes.len() as u64
            })
        }
        Some(other) => {
            // Unknown archive format — no estimate available.
            tracing::warn!("unknown archive format {other}, cannot estimate uncompressed size");
            bytes.len() as u64
        }
        None => 0, // Binary/launcher — no compress phase.
    }
}

/// Processes one downloaded source: extract archives or import binaries to CAS.
///
/// For archive formats (ZIP, tar.gz, tar.xz): extract → find executable →
/// repack to uncompressed ZIP → CAS import → single trailing-slash content
/// key (`{os}/`).
/// Reports one fetch-phase progress update during a parallel download.
///
/// `items.0` is the count of sources **completed so far**, read from a shared
/// monotone counter — never the reporting source's own index. Under
/// `buffer_unordered(default_source_fetch_concurrency().min(n))` several sources download
/// concurrently and finish out of order, so a per-future index would make the
/// rendered `{completed}/{total}` prefix oscillate (e.g. `3/3` reported by the
/// fastest source, then `1/3` by a slower one still streaming).
///
/// The byte aggregate still comes from the shared [`MultiItemBudget`], so the
/// bar's fill keeps advancing smoothly while the item counter stays monotone.
///
/// # Arguments
///
/// * `cb` — progress sink; the provider's `[fch]` bar callback.
/// * `completed` — shared counter of finished sources (`Acquire` load).
/// * `total_sources` — total source count for this tool (constant per fetch).
/// * `budget` — shared byte budget for the current tool's sources.
///
/// # Side effects
///
/// Invokes `cb` once, synchronously, on the calling thread.
#[cfg(feature = "tool-presets")]
fn fire_fetch_chunk_progress(
    cb: &ProviderProgressCallback,
    completed: &AtomicU64,
    total_sources: u64,
    budget: &MultiItemBudget,
) {
    fire_progress(
        cb,
        ProviderPhase::Fetch,
        (completed.load(Ordering::Acquire), total_sources),
        budget,
    );
}

///
/// Fires a progress callback with the current budget aggregate.
///
/// Extracted as a shared helper to prevent forgetting to push progress
/// updates to the bar (historical regression cause).
#[cfg(feature = "tool-presets")]
fn fire_progress(
    cb: &ProviderProgressCallback,
    phase: ProviderPhase,
    items: (u64, u64),
    budget: &MultiItemBudget,
) {
    let bytes = budget.aggregate();
    cb(ProviderProgressSnapshot { phase, items, bytes });
}

/// Processes one downloaded source: extract archives or import binaries to CAS.
///
/// For archive formats (ZIP, tar.gz, tar.xz): extract → find executable →
/// repack to uncompressed ZIP → CAS import → single trailing-slash content
/// key (`{key_prefix}/`).
///
/// For binary/launcher format: CAS-import bytes directly using the given
/// `filename` (URL basename for Fetch sources, `tool_id` for launchers).
/// Returns file-level content key (`{key_prefix}/{filename}`).
///
/// `key_prefix` is the content-key prefix the caller computed for this
/// source: the OS label for a single-source OS slot, `{os}/{arch}` for a
/// dual-arch slot (see [`process_tool_sources`]). `os_label` is still used
/// for the extraction directory and error context.
///
/// # Budget item layout
///
/// Archive sources use 2 consecutive budget items (`item_idx` = decompress,
/// `item_idx + 1` = compress). Binary/launcher sources use 1 item (`item_idx`).
/// The caller must set `item_count` to 2 for archive sources, 1 for others.
///
/// # Progress callbacks
///
/// When `progress_cb` is `Some`, fires per-chunk callbacks during extraction
/// and compression so the progress bar updates smoothly instead of freezing
/// for seconds at a time. The `items_completed` and `total_items` parameters
/// let the caller report how many items have been completed before this
/// source (for the prefix counter).
#[cfg(feature = "tool-presets")]
#[expect(
    clippy::too_many_arguments,
    reason = "pipeline plumbing passes per-source context explicitly"
)]
async fn process_single_source(
    bytes: &[u8],
    archive_format: Option<&str>,
    os_label: &str,
    key_prefix: &str,
    tool_id: &str,
    os_dir: &std::path::Path,
    filename: &str,
    cas: &impl mediapm_cas::CasApi,
    budget: &MultiItemBudget,
    item_idx: usize,
    item_count: usize,
    progress_cb: Option<&ProviderProgressCallback>,
    items_completed: u64,
    total_items: u64,
) -> Result<ProcessedSource, crate::error::ConductorError> {
    use crate::error::ConductorError;
    use bytes::Bytes;

    let decompress_wrapper = {
        let progress_cb = progress_cb.cloned();
        move |pos: u64| {
            budget.set_pos(item_idx, pos);
            if let Some(ref cb) = progress_cb {
                // Decompress is item 0 of this source (not yet completed)
                fire_progress(cb, ProviderPhase::Process, (items_completed, total_items), budget);
            }
        }
    };
    let local_cb: Option<&dyn Fn(u64)> = Some(&decompress_wrapper);

    if archive_format.is_some() {
        // ── Archive format ────────────────────────────────────────────
        debug_assert_eq!(item_count, 2, "archive sources must use 2 budget items");
        std::fs::create_dir_all(os_dir).map_err(|source| {
            ConductorError::io(
                format!("creating temp directory for {os_label} tool extraction"),
                os_dir,
                source,
            )
        })?;
        let total_compressed = bytes.len() as u64;

        // Item 0: decompress — total = compressed size (input), pos updated via local_cb
        budget.set_total(item_idx, total_compressed);
        // Item 1: compress — total already set by process_tool_sources via
        // estimate_uncompressed_size; refined to actual dir size after extraction.
        let compress_idx = item_idx + 1;
        extract_archive(bytes, archive_format, os_dir, local_cb)?;
        // Ensure final pos = total (callbacks may already have set it)
        budget.set_pos(item_idx, total_compressed);

        let exec_path = find_os_executable(os_dir, tool_id).unwrap_or_else(|| tool_id.to_string());

        // yt-dlp spawns deno via its bundled JS-challenge provider, which
        // hardcodes `cmd = [deno, 'run', *options, '-']` with
        // `--no-config` and no `--allow-*` flags. deno 2.x enforces a sandbox,
        // so the `ws` npm dependency's `WS_NO_BUFFER_UTIL` env access is denied
        // (`NotCapable`), breaking the challenge and yielding HTTP 403. The
        // `--js-runtimes` CLI only accepts `RUNTIME[:PATH]` (no args), so the
        // only fix is to wrap the deno binary: rename the real executable and
        // place a shim that injects `--allow-all` after the first arg (`run`).
        if tool_id == "deno" {
            wrap_deno_binary(os_dir, &exec_path)?;
        }

        // Item 1: compress — refine total to actual directory content size,
        // then repack to uncompressed ZIP and import to CAS.
        let dir_total = total_dir_size(os_dir);
        budget.set_total(compress_idx, dir_total);
        let compress_wrapper = {
            let progress_cb = progress_cb.cloned();
            move |pos: u64| {
                budget.set_pos(compress_idx, pos);
                if let Some(ref cb) = progress_cb {
                    // Compress: decompress item (item_idx) of this source is done
                    let completed = items_completed + 1;
                    fire_progress(cb, ProviderPhase::Process, (completed, total_items), budget);
                }
            }
        };
        let compress_cb: Option<&dyn Fn(u64)> = Some(&compress_wrapper);

        let zip_bytes = pack_directory_to_uncompressed_zip_bytes(os_dir, 0, compress_cb)?;
        // Ensure final pos = total (callbacks may already have set it)
        budget.set_pos(compress_idx, dir_total);
        let hash = cas.put(Bytes::from(zip_bytes)).await.map_err(ConductorError::Cas)?;
        let key = format!("{key_prefix}/");
        let mut cm = BTreeMap::new();
        cm.insert(key, hash.to_hex());
        Ok(ProcessedSource { content_map: cm, exec_path })
    } else {
        // ── Binary/launcher format ───────────────────────────────────
        debug_assert_eq!(item_count, 1, "binary/launcher sources must use 1 budget item");
        // CAS-import bytes directly; filename is the URL basename
        // (for Fetch sources) or tool_id (for launcher scripts).
        let cost = bytes.len() as u64;
        budget.set_total(item_idx, cost);
        budget.advance(item_idx, cost);
        if let Some(cb) = progress_cb.as_ref() {
            // Binary/launcher: this single item is now complete
            let completed = items_completed + 1;
            fire_progress(cb, ProviderPhase::Process, (completed, total_items), budget);
        }
        let hash = cas.put(Bytes::from(bytes.to_vec())).await.map_err(ConductorError::Cas)?;
        let key = format!("{key_prefix}/{filename}");
        let mut cm = BTreeMap::new();
        cm.insert(key, hash.to_hex());
        Ok(ProcessedSource { content_map: cm, exec_path: filename.to_string() })
    }
}

// ---------------------------------------------------------------------------
// deno permission wrapper
// ---------------------------------------------------------------------------

/// Wraps the deno executable so yt-dlp's bundled JS-challenge provider can run
/// it without sandbox denials.
///
/// yt-dlp invokes deno as `[deno, 'run', *options, '-']` where `options`
/// hardcodes `--no-config` and omits any `--allow-*` flag. deno 2.x enforces a
/// permission sandbox, so the `ws` npm package's `WS_NO_BUFFER_UTIL` env access
/// is denied (`NotCapable`), breaking `YouTube` challenge solving (HTTP 403).
/// Because yt-dlp's `--js-runtimes` accepts only `RUNTIME[:PATH]` (no args), we
/// rename the real binary to `deno.real` (or `deno.real.exe`) and write a shim
/// at the original `deno` path that injects `--allow-all` after the first arg
/// (typically `run`). deno 2.x requires `--allow-all` after the subcommand, not
/// as a top-level flag.
///
/// `exec_rel` is the executable path discovered by [`find_os_executable`],
/// which already handles nested per-OS layouts (e.g. `windows/deno.exe`,
/// `darwin/deno`, `linux/deno`) as well as the flat `deno` case. The real
/// binary is renamed **within the same directory** as the original so the shim
/// can re-exec it via a sibling-relative path; the shim is written back at the
/// original path, leaving `exec_rel` unchanged for downstream consumers.
fn wrap_deno_binary(
    os_dir: &std::path::Path,
    exec_rel: &str,
) -> Result<(), crate::error::ConductorError> {
    use crate::error::ConductorError;

    let real_name = if cfg!(windows) { "deno.real.exe" } else { "deno.real" };

    let exec_abs = os_dir.join(exec_rel);
    let real_abs = exec_abs
        .parent()
        .ok_or_else(|| {
            ConductorError::io(
                "deno executable path has no parent directory".to_string(),
                exec_abs.clone(),
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "no parent"),
            )
        })?
        .join(real_name);

    std::fs::rename(&exec_abs, &real_abs).map_err(|source| {
        ConductorError::io(
            format!("renaming deno binary to '{real_name}' for permission wrapper"),
            &real_abs,
            source,
        )
    })?;

    // deno 2.x only accepts `--allow-all` *after* the subcommand (e.g. `run`),
    // not as a top-level flag. yt-dlp always invokes `deno run ...`, so the shim
    // peels the first arg, injects `--allow-all`, and forwards the rest:
    //   unix:   deno "$1" --allow-all "$2" "$3" ...
    //   windows: deno %1 --allow-all %2 %3 ...
    let shim_contents = if cfg!(windows) {
        format!("@set \"first=%1\"\r\n@shift\r\n@\"%~dp0{real_name}\" %first% --allow-all %*\r\n")
    } else {
        format!(
            "#!/bin/sh\nfirst=\"$1\"; shift; exec \"$(dirname \"$0\")/{real_name}\" \"$first\" --allow-all \"$@\"\n"
        )
    };

    std::fs::write(&exec_abs, shim_contents).map_err(|source| {
        ConductorError::io(
            format!("writing deno permission wrapper shim '{exec_rel}'"),
            &exec_abs,
            source,
        )
    })?;

    // Ensure the shim is executable on unix so it can be invoked directly.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = std::fs::metadata(&exec_abs)
            .map_err(|source| {
                ConductorError::io(
                    "reading deno wrapper shim permissions".to_string(),
                    exec_abs.clone(),
                    source,
                )
            })?
            .permissions();
        permissions.set_mode(permissions.mode() | 0o111);
        std::fs::set_permissions(&exec_abs, permissions).map_err(|source| {
            ConductorError::io(
                "marking deno wrapper shim executable".to_string(),
                exec_abs.clone(),
                source,
            )
        })?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Archive extraction helpers
// ---------------------------------------------------------------------------

/// Tracks how many bytes have been consumed from a byte slice.
///
/// Used to report per-entry progress during tar.gz/xz extraction, where the
/// compressed-bytes-consumed count is the best available estimate of
/// extraction progress. Uses a plain `u64` (not `AtomicU64`) since extraction
/// is single-threaded and sequential.
///
/// `bytes_read` uses [`Cell`] so the consuming code can read the current value
/// without owning the reader (which is deeply nested inside decoder + tar
/// archive wrappers).  `progress_cb` fires sub-entry callbacks every
/// [`SUB_ENTRY_CHUNK`] bytes consumed.
struct CountingReader<'a> {
    cursor: std::io::Cursor<&'a [u8]>,
    bytes_read: &'a Cell<u64>,
    last_cb_pos: Cell<u64>,
    progress_cb: Option<&'a dyn Fn(u64)>,
}

impl Read for CountingReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.cursor.read(buf)?;
        let new = self.bytes_read.get() + n as u64;
        self.bytes_read.set(new);
        // Fire sub-entry callback at SUB_ENTRY_CHUNK boundaries.
        if let Some(cb) = self.progress_cb
            && new - self.last_cb_pos.get() >= SUB_ENTRY_CHUNK
        {
            self.last_cb_pos.set(new);
            cb(new);
        }
        Ok(n)
    }
}

/// Generic tar-entry iteration with per-entry progress reporting.
///
/// Iterates over entries in a tar archive, calls `entry.unpack_in(target_dir)`
/// for each, and fires the progress callback after every entry using
/// `consumed` (compressed bytes consumed so far from [`CountingReader`])
/// as the progress metric.  Sub-entry progress callbacks are handled by
/// the [`CountingReader`] itself (fires at [`SUB_ENTRY_CHUNK`] boundaries
/// during decompression).
fn extract_tar_entries_with_progress<R: Read>(
    mut archive: tar::Archive<R>,
    consumed: &Cell<u64>,
    total_compressed: u64,
    target_dir: &std::path::Path,
    local_cb: Option<&dyn Fn(u64)>,
) -> Result<(), crate::error::ConductorError> {
    use crate::error::ConductorError;
    for result in
        archive.entries().map_err(|e| ConductorError::io("read tar entries", target_dir, e))?
    {
        let mut entry = result.map_err(|e| ConductorError::io("read tar entry", target_dir, e))?;
        entry
            .unpack_in(target_dir)
            .map_err(|e| ConductorError::io("extract tar entry", target_dir, e))?;

        let current = consumed.get().min(total_compressed);
        if let Some(cb) = local_cb.as_ref() {
            cb(current);
        }
    }
    Ok(())
}

/// Extracts archive bytes to the given directory based on archive format.
///
/// `format` must be `Some(ARCHIVE_ZIP)`, `Some(ARCHIVE_TAR_GZ)`, or
/// `Some(ARCHIVE_TAR_XZ)`. Binary payloads are not extracted through this
/// function — they bypass to direct CAS import.
///
/// When `progress_cb` is `Some`, fires per-entry progress callbacks so the
/// progress bar updates smoothly during extraction.
#[cfg(feature = "tool-presets")]
fn extract_archive(
    bytes: &[u8],
    format: Option<&str>,
    target_dir: &std::path::Path,
    local_cb: Option<&dyn Fn(u64)>,
) -> Result<(), crate::error::ConductorError> {
    use crate::error::ConductorError;
    match format {
        Some(ARCHIVE_ZIP) => extract_zip(bytes, target_dir, local_cb),
        Some(ARCHIVE_TAR_GZ) => extract_tar_gz(bytes, target_dir, local_cb),
        Some(ARCHIVE_TAR_XZ) => extract_tar_xz(bytes, target_dir, local_cb),
        Some(other) => {
            Err(ConductorError::Workflow(format!("unsupported archive format: {other}")))
        }
        None => Err(ConductorError::Workflow(
            "extract_archive called with None format (binary payload)".to_string(),
        )),
    }
}

#[cfg(feature = "tool-presets")]
fn extract_zip(
    bytes: &[u8],
    target_dir: &std::path::Path,
    local_cb: Option<&dyn Fn(u64)>,
) -> Result<(), crate::error::ConductorError> {
    use crate::error::ConductorError;
    use std::io::Write;
    let mut archive = zip::ZipArchive::new(std::io::Cursor::new(bytes))
        .map_err(|e| ConductorError::Workflow(format!("ZIP open error: {e}")))?;

    // Track per-entry compressed sizes (input size) as position weight.
    // The sum of compressed_size() is slightly less than source.bytes.len()
    // (ZIP local-file-header overhead), so position stays strictly within
    // the compressed-size budget.
    let mut bytes_done: u64 = 0;

    for i in 0..archive.len() {
        let mut file = archive
            .by_index(i)
            .map_err(|e| ConductorError::Workflow(format!("ZIP entry error: {e}")))?;
        let out_path = target_dir.join(file.name());
        if file.name().ends_with('/') {
            std::fs::create_dir_all(&out_path)
                .map_err(|e| ConductorError::io("create directory", &out_path, e))?;
        } else {
            if let Some(parent) = out_path.parent() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| ConductorError::io("create directory", parent, e))?;
            }
            let mut out = std::fs::File::create(&out_path)
                .map_err(|e| ConductorError::io("create file", &out_path, e))?;
            let entry_compressed = file.compressed_size();
            let entry_decompressed = file.size();
            let mut buf = vec![0u8; 65536];
            let mut written: u64 = 0;
            loop {
                let len = file
                    .read(&mut buf)
                    .map_err(|e| ConductorError::io("read zip entry", &out_path, e))?;
                if len == 0 {
                    break;
                }
                out.write_all(&buf[..len])
                    .map_err(|e| ConductorError::io("write zip entry", &out_path, e))?;
                written += len as u64;
                // Estimate compressed progress proportional to decompressed bytes written
                let est_compressed = (written * entry_compressed)
                    .checked_div(entry_decompressed)
                    .unwrap_or(entry_compressed);
                if let Some(cb) = local_cb.as_ref() {
                    cb(bytes_done + est_compressed);
                }
            }
            bytes_done += entry_compressed;
        }
        if let Some(cb) = local_cb.as_ref() {
            cb(bytes_done);
        }
    }
    Ok(())
}

#[cfg(feature = "tool-presets")]
fn extract_tar_gz(
    bytes: &[u8],
    target_dir: &std::path::Path,
    local_cb: Option<&dyn Fn(u64)>,
) -> Result<(), crate::error::ConductorError> {
    let total_compressed = bytes.len() as u64;
    let consumed = Cell::new(0u64);
    let reader = CountingReader {
        cursor: std::io::Cursor::new(bytes),
        bytes_read: &consumed,
        last_cb_pos: Cell::new(0),
        progress_cb: local_cb,
    };
    let decoder = flate2::read::GzDecoder::new(reader);
    let archive = tar::Archive::new(decoder);
    extract_tar_entries_with_progress(archive, &consumed, total_compressed, target_dir, local_cb)
}

#[cfg(feature = "tool-presets")]
fn extract_tar_xz(
    bytes: &[u8],
    target_dir: &std::path::Path,
    local_cb: Option<&dyn Fn(u64)>,
) -> Result<(), crate::error::ConductorError> {
    let total_compressed = bytes.len() as u64;
    let consumed = Cell::new(0u64);
    let reader = CountingReader {
        cursor: std::io::Cursor::new(bytes),
        bytes_read: &consumed,
        last_cb_pos: Cell::new(0),
        progress_cb: local_cb,
    };
    let decoder = xz2::read::XzDecoder::new(reader);
    let archive = tar::Archive::new(decoder);
    extract_tar_entries_with_progress(archive, &consumed, total_compressed, target_dir, local_cb)
}

// ---------------------------------------------------------------------------
// Pack helpers
// ---------------------------------------------------------------------------

/// Packs one directory tree into uncompressed ZIP bytes.
#[cfg(feature = "tool-presets")]
fn pack_directory_to_uncompressed_zip_bytes(
    dir: &std::path::Path,
    offset: u64,
    local_cb: Option<&dyn Fn(u64)>,
) -> Result<Vec<u8>, crate::error::ConductorError> {
    use crate::error::ConductorError;
    use zip::write::SimpleFileOptions;

    let mut buf = Vec::new();
    {
        let mut writer = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
        let options =
            SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);

        let mut decompressed_accumulator = offset;
        pack_directory_entries(
            &mut writer,
            dir,
            dir,
            &options,
            local_cb,
            &mut decompressed_accumulator,
        )?;

        writer.finish().map_err(|e| {
            ConductorError::Workflow(format!("failed to finalize zip archive: {e}"))
        })?;
    }
    Ok(buf)
}

/// Recursively adds directory entries to the zip writer.
#[cfg(feature = "tool-presets")]
fn pack_directory_entries(
    writer: &mut zip::ZipWriter<std::io::Cursor<&mut Vec<u8>>>,
    root: &std::path::Path,
    dir: &std::path::Path,
    options: &zip::write::SimpleFileOptions,
    local_cb: Option<&dyn Fn(u64)>,
    decompressed_accumulator: &mut u64,
) -> Result<(), crate::error::ConductorError> {
    use crate::error::ConductorError;
    use std::io::Read;
    use std::io::Write;

    for entry in std::fs::read_dir(dir).map_err(|source| {
        ConductorError::io(format!("reading directory '{}'", dir.display()), dir, source)
    })? {
        let entry = entry.map_err(|source| {
            ConductorError::io(
                format!("reading directory entry in '{}'", dir.display()),
                dir,
                source,
            )
        })?;
        let path = entry.path();
        if path.is_dir() {
            pack_directory_entries(
                writer,
                root,
                &path,
                options,
                local_cb,
                decompressed_accumulator,
            )?;
        } else {
            let relative = path.strip_prefix(root).unwrap_or(&path).to_string_lossy().to_string();
            let mut file = std::fs::File::open(&path).map_err(|source| {
                ConductorError::io(
                    format!("opening file '{}' for zip", path.display()),
                    &path,
                    source,
                )
            })?;
            writer.start_file(relative.clone(), *options).map_err(|e| {
                ConductorError::Workflow(format!("failed to start zip entry '{relative}': {e}"))
            })?;
            // Read and write in SUB_ENTRY_CHUNK chunks to fire sub-entry
            // progress callbacks for large files.
            let mut sub_buf =
                vec![0u8; usize::try_from(SUB_ENTRY_CHUNK).expect("SUB_ENTRY_CHUNK fits usize")];
            loop {
                let n = file.read(&mut sub_buf).map_err(|source| {
                    ConductorError::io(
                        format!("reading file '{}' for zip", path.display()),
                        &path,
                        source,
                    )
                })?;
                if n == 0 {
                    break;
                }
                writer.write_all(&sub_buf[..n]).map_err(|e| {
                    ConductorError::Workflow(format!("failed to write zip entry '{relative}': {e}"))
                })?;
                if let Some(cb) = local_cb.as_ref() {
                    *decompressed_accumulator += n as u64;
                    cb(*decompressed_accumulator);
                }
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// File finding helpers
// ---------------------------------------------------------------------------

/// Searches for an executable named `{tool_id}` or `{tool_id}.exe` inside
/// `os_dir` and returns its path relative to `os_dir`.
#[cfg(feature = "tool-presets")]
fn find_os_executable(os_dir: &std::path::Path, tool_id: &str) -> Option<String> {
    let candidates = [tool_id.to_string(), format!("{tool_id}.exe")];
    for name in &candidates {
        if let Some(rel) = find_file_relative(os_dir, os_dir, name) {
            return Some(rel.to_string_lossy().to_string());
        }
    }
    None
}

/// Computes the total size of all regular files under `dir`, recursively.
#[cfg(feature = "tool-presets")]
fn total_dir_size(dir: &std::path::Path) -> u64 {
    let mut total = 0u64;
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                total += total_dir_size(&path);
            } else if let Ok(meta) = path.metadata() {
                total += meta.len();
            }
        }
    }
    total
}

/// Recursively searches for a file with the given name, returning its path
/// relative to `root`.
#[cfg(feature = "tool-presets")]
fn find_file_relative(
    root: &std::path::Path,
    dir: &std::path::Path,
    target: &str,
) -> Option<std::path::PathBuf> {
    for entry in std::fs::read_dir(dir).ok()? {
        let entry = entry.ok()?;
        let path = entry.path();
        if path.is_dir() {
            if let found @ Some(_) = find_file_relative(root, &path, target) {
                return found;
            }
        } else if path.file_name().and_then(|n| n.to_str()) == Some(target) {
            return path.strip_prefix(root).ok().map(std::path::Path::to_path_buf);
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[cfg(feature = "tool-presets")]
mod tests {
    use std::io::Write;

    use mediapm_cas::{CasApi, Hash, InMemoryCas};

    use super::*;

    // ── Synthetic archive helpers ─────────────────────────────────────

    fn synthetic_zip(entries: &[(&str, &[u8])]) -> Vec<u8> {
        use zip::write::SimpleFileOptions;
        let cursor = std::io::Cursor::new(Vec::new());
        let mut writer = zip::ZipWriter::new(cursor);
        let options = SimpleFileOptions::default();
        for (name, content) in entries {
            writer.start_file(*name, options).unwrap();
            writer.write_all(content).unwrap();
        }
        let cursor = writer.finish().unwrap();
        cursor.into_inner()
    }

    fn synthetic_tar_gz(entries: &[(&str, &[u8])]) -> Vec<u8> {
        use flate2::Compression;
        use flate2::write::GzEncoder;
        let buf = Vec::new();
        let encoder = GzEncoder::new(buf, Compression::fast());
        let mut tar = tar::Builder::new(encoder);
        for (name, content) in entries {
            let mut header = tar::Header::new_gnu();
            header.set_path(name).unwrap();
            header.set_size(content.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            tar.append(&header, *content).unwrap();
        }
        let encoder = tar.into_inner().unwrap();
        encoder.finish().unwrap()
    }

    /// Simple deterministic pseudo-random buffer for creating hard-to-compress
    /// data, so compressed size stays close to uncompressed size during
    /// sub-entry progress tests.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "xorshift PRNG deliberately emits the low byte of state"
    )]
    fn pseudo_random_buffer(size: usize) -> Vec<u8> {
        let mut data = vec![0u8; size];
        let mut state: u64 = 123_456_789;
        for byte in &mut data {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            *byte = state as u8;
        }
        data
    }

    fn synthetic_tar_xz(entries: &[(&str, &[u8])]) -> Vec<u8> {
        use xz2::write::XzEncoder;
        let buf = Vec::new();
        let encoder = XzEncoder::new(buf, 6);
        let mut tar = tar::Builder::new(encoder);
        for (name, content) in entries {
            let mut header = tar::Header::new_gnu();
            header.set_path(name).unwrap();
            header.set_size(content.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            tar.append(&header, *content).unwrap();
        }
        let encoder = tar.into_inner().unwrap();
        encoder.finish().unwrap()
    }

    const EXEC_BYTES: &[u8] = b"#!/bin/sh\necho mocked\n";

    // ── infer_archive_format ──────────────────────────────────────────

    #[test]
    fn infer_format_from_url_extension() {
        assert_eq!(infer_archive_format("https://example.com/tool.tar.xz"), Some(ARCHIVE_TAR_XZ));
        assert_eq!(infer_archive_format("https://example.com/tool.tar.gz"), Some(ARCHIVE_TAR_GZ));
        assert_eq!(infer_archive_format("https://example.com/tool.tgz"), Some(ARCHIVE_TAR_GZ));
        assert_eq!(infer_archive_format("https://example.com/tool.zip"), Some(ARCHIVE_ZIP));
        assert_eq!(infer_archive_format("https://example.com/tool"), None);
        assert_eq!(infer_archive_format("https://example.com/tool.exe"), None);
    }

    // ── find_file_relative ────────────────────────────────────────────

    #[test]
    fn find_file_at_root() {
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let file_path = dir.path().join("sd");
        std::fs::write(&file_path, "").unwrap();
        assert_eq!(
            find_file_relative(dir.path(), dir.path(), "sd"),
            Some(std::path::PathBuf::from("sd"))
        );
    }

    #[test]
    fn find_file_in_nested_dir() {
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let sub = dir.path().join("bin");
        std::fs::create_dir(&sub).unwrap();
        std::fs::write(sub.join("sd"), "").unwrap();
        assert_eq!(
            find_file_relative(dir.path(), dir.path(), "sd"),
            Some(std::path::PathBuf::from("bin/sd"))
        );
    }

    #[test]
    fn find_file_absent_returns_none() {
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        assert!(find_file_relative(dir.path(), dir.path(), "nonexistent").is_none());
    }

    // ── find_os_executable ────────────────────────────────────────────

    #[test]
    fn find_os_exec_direct_match() {
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        std::fs::write(dir.path().join("sd"), "").unwrap();
        assert_eq!(find_os_executable(dir.path(), "sd"), Some("sd".into()));
    }

    #[test]
    fn find_os_exec_finds_exe_variant() {
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        std::fs::write(dir.path().join("sd.exe"), "").unwrap();
        assert_eq!(find_os_executable(dir.path(), "sd"), Some("sd.exe".into()));
    }

    #[test]
    fn find_os_exec_not_found_returns_none() {
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        assert!(find_os_executable(dir.path(), "nonexistent").is_none());
    }

    // ── process_single_source (with InMemoryCas) ─────────────────────

    #[tokio::test]
    async fn process_zip_archive_linux_label() {
        let zip = synthetic_zip(&[("sd", EXEC_BYTES)]);
        let cas = InMemoryCas::default();
        let os_dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let mut budget = MultiItemBudget::new();
        budget.add_item(0);
        budget.add_item(0);
        let result = process_single_source(
            &zip,
            Some(ARCHIVE_ZIP),
            "linux",
            "linux",
            "sd",
            os_dir.path(),
            "",
            &cas,
            &budget,
            0usize,
            2usize,
            None,
            0u64,
            2u64,
        )
        .await
        .unwrap();
        assert_eq!(result.content_map.len(), 1);
        assert!(result.content_map.contains_key("linux/"));
        assert_eq!(result.exec_path, "sd");
    }

    #[tokio::test]
    async fn process_tar_gz_archive_macos_label() {
        let tgz = synthetic_tar_gz(&[("sd", EXEC_BYTES)]);
        let cas = InMemoryCas::default();
        let os_dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let mut budget = MultiItemBudget::new();
        budget.add_item(0);
        budget.add_item(0);
        let result = process_single_source(
            &tgz,
            Some(ARCHIVE_TAR_GZ),
            "macos",
            "macos",
            "sd",
            os_dir.path(),
            "",
            &cas,
            &budget,
            0usize,
            2usize,
            None,
            0u64,
            2u64,
        )
        .await
        .unwrap();
        assert_eq!(result.content_map.len(), 1);
        assert!(result.content_map.contains_key("macos/"));
        assert_eq!(result.exec_path, "sd");
    }

    #[tokio::test]
    async fn process_tar_xz_archive_windows_label() {
        let txz = synthetic_tar_xz(&[("sd.exe", EXEC_BYTES)]);
        let cas = InMemoryCas::default();
        let os_dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let mut budget = MultiItemBudget::new();
        budget.add_item(0);
        budget.add_item(0);
        let result = process_single_source(
            &txz,
            Some(ARCHIVE_TAR_XZ),
            "windows",
            "windows",
            "sd",
            os_dir.path(),
            "",
            &cas,
            &budget,
            0usize,
            2usize,
            None,
            0u64,
            2u64,
        )
        .await
        .unwrap();
        assert_eq!(result.content_map.len(), 1);
        assert!(result.content_map.contains_key("windows/"));
        assert_eq!(result.exec_path, "sd.exe");
    }

    /// Verifies that processing the `deno` archive wraps the real binary so
    /// yt-dlp's bundled JS-challenge provider can run it without sandbox
    /// denials: the original `deno` path becomes a shim and the real binary is
    /// renamed to `deno.real` (or `deno.real.exe` on Windows).
    #[tokio::test]
    async fn deno_wrap_flat_layout() {
        let zip = synthetic_zip(&[("deno", EXEC_BYTES), ("deno.real", EXEC_BYTES)]);
        let cas = InMemoryCas::default();
        let os_dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let mut budget = MultiItemBudget::new();
        budget.add_item(0);
        budget.add_item(0);
        let result = process_single_source(
            &zip,
            Some(ARCHIVE_ZIP),
            "linux",
            "linux",
            "deno",
            os_dir.path(),
            "",
            &cas,
            &budget,
            0usize,
            2usize,
            None,
            0u64,
            2u64,
        )
        .await
        .unwrap();
        assert!(result.content_map.contains_key("linux/"));
        assert_eq!(result.exec_path, "deno");
        let shim = std::fs::read_to_string(os_dir.path().join("deno")).unwrap();
        assert!(shim.contains("deno.real"), "shim must re-exec deno.real");
        assert!(shim.contains("--allow-all"), "shim must grant deno permissions");
        // deno 2.x: --allow-all must come AFTER the first arg (typically `run`)
        let first_pos = shim.find("$first").expect("shim must reference $first");
        let allow_pos = shim.find("--allow-all").expect("shim must contain --allow-all");
        assert!(first_pos < allow_pos, "--allow-all must come after the first arg in unix shim");
        assert!(std::fs::metadata(os_dir.path().join("deno.real")).is_ok());
    }

    #[tokio::test]
    async fn deno_wrap_nested_windows_layout() {
        let real_name = if cfg!(windows) { "deno.real.exe" } else { "deno.real" };
        let zip = synthetic_zip(&[("windows/deno.exe", EXEC_BYTES)]);
        let cas = InMemoryCas::default();
        let os_dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let mut budget = MultiItemBudget::new();
        budget.add_item(0);
        budget.add_item(0);
        let result = process_single_source(
            &zip,
            Some(ARCHIVE_ZIP),
            "windows",
            "windows",
            "deno",
            os_dir.path(),
            "",
            &cas,
            &budget,
            0usize,
            2usize,
            None,
            0u64,
            2u64,
        )
        .await
        .unwrap();
        assert!(result.content_map.contains_key("windows/"));
        assert_eq!(result.exec_path, "windows/deno.exe");
        let shim = std::fs::read_to_string(os_dir.path().join("windows/deno.exe")).unwrap();
        assert!(
            shim.contains(real_name),
            "shim must re-exec the renamed deno binary '{real_name}'"
        );
        assert!(shim.contains("--allow-all"), "shim must grant deno permissions");
        // deno 2.x: --allow-all must come AFTER the first arg (typically `run`)
        // Note: cfg!(windows) is false at compile time on macOS/Linux, so even
        // the Windows-layout test produces a unix shim.
        let first_marker = if cfg!(windows) { "%first%" } else { "$first" };
        let first_pos = shim.find(first_marker).unwrap_or_else(|| {
            panic!("shim must reference first-arg placeholder '{first_marker}': {shim}")
        });
        let allow_pos = shim.find("--allow-all").expect("shim must contain --allow-all");
        assert!(first_pos < allow_pos, "--allow-all must come after the first arg in shim");
        assert!(std::fs::metadata(os_dir.path().join("windows").join(real_name)).is_ok());
    }

    #[tokio::test]
    async fn deno_wrap_nested_darwin_layout() {
        let real_name = if cfg!(windows) { "deno.real.exe" } else { "deno.real" };
        let zip = synthetic_zip(&[("darwin/deno", EXEC_BYTES)]);
        let cas = InMemoryCas::default();
        let os_dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let mut budget = MultiItemBudget::new();
        budget.add_item(0);
        budget.add_item(0);
        let result = process_single_source(
            &zip,
            Some(ARCHIVE_ZIP),
            "darwin",
            "darwin",
            "deno",
            os_dir.path(),
            "",
            &cas,
            &budget,
            0usize,
            2usize,
            None,
            0u64,
            2u64,
        )
        .await
        .unwrap();
        assert!(result.content_map.contains_key("darwin/"));
        assert_eq!(result.exec_path, "darwin/deno");
        let shim = std::fs::read_to_string(os_dir.path().join("darwin/deno")).unwrap();
        assert!(
            shim.contains(real_name),
            "shim must re-exec the renamed deno binary '{real_name}'"
        );
        assert!(shim.contains("--allow-all"), "shim must grant deno permissions");
        // deno 2.x: --allow-all must come AFTER the first arg (typically `run`)
        let first_pos = shim.find("$first").expect("shim must reference $first");
        let allow_pos = shim.find("--allow-all").expect("shim must contain --allow-all");
        assert!(first_pos < allow_pos, "--allow-all must come after the first arg in unix shim");
        assert!(std::fs::metadata(os_dir.path().join("darwin").join(real_name)).is_ok());
    }

    #[tokio::test]
    async fn deno_wrap_missing_binary_surfaces_error() {
        // No deno executable in the archive — the wrap must surface a clear
        // error rather than silently succeeding (regression guard for the
        // flat-path assumption that previously masked the nested layout).
        let zip = synthetic_zip(&[("readme.txt", b"not a binary")]);
        let cas = InMemoryCas::default();
        let os_dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let mut budget = MultiItemBudget::new();
        budget.add_item(0);
        budget.add_item(0);
        let err = process_single_source(
            &zip,
            Some(ARCHIVE_ZIP),
            "linux",
            "linux",
            "deno",
            os_dir.path(),
            "",
            &cas,
            &budget,
            0usize,
            2usize,
            None,
            0u64,
            2u64,
        )
        .await
        .expect_err("missing deno binary must surface an error");
        let msg = err.to_string();
        assert!(
            msg.contains("deno.real") || msg.contains("No such file") || msg.contains("ENOENT"),
            "error must name the missing deno binary: {msg}"
        );
    }

    #[tokio::test]
    async fn regression_deno_spec_present_after_process() {
        // Regression guard for the original bug: the real deno GitHub release
        // zip extracts to a nested per-OS subdirectory (e.g. windows/deno.exe),
        // so the flat-path assumption in wrap_deno_binary made the process
        // phase fail with [W] and deno was never inserted into the generated
        // doc's content_map. This asserts the process phase now succeeds and
        // yields a non-empty content_map keyed by the OS label for the nested
        // layout (S-DENO-6 at the provider-pipeline level).
        let zip = synthetic_zip(&[("windows/deno.exe", EXEC_BYTES)]);
        let cas = InMemoryCas::default();
        let os_dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let mut budget = MultiItemBudget::new();
        budget.add_item(0);
        budget.add_item(0);
        let result = process_single_source(
            &zip,
            Some(ARCHIVE_ZIP),
            "windows",
            "windows",
            "deno",
            os_dir.path(),
            "",
            &cas,
            &budget,
            0usize,
            2usize,
            None,
            0u64,
            2u64,
        )
        .await
        .expect("deno process phase must succeed for nested release layout");
        let entry = result
            .content_map
            .get("windows/")
            .expect("deno content_map must contain the windows/ key");
        assert!(!entry.is_empty(), "deno content_map entry must be non-empty");
        assert_eq!(result.exec_path, "windows/deno.exe");
    }

    #[tokio::test]
    async fn process_binary_format_produces_file_entry() {
        let cas = InMemoryCas::default();
        let os_dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let launcher_bytes = generate_launcher_script("linux", "echo@v1", &[]);
        let mut budget = MultiItemBudget::new();
        budget.add_item(0);
        let result = process_single_source(
            &launcher_bytes,
            None,
            "linux",
            "linux",
            "echo",
            os_dir.path(),
            "echo",
            &cas,
            &budget,
            0usize,
            1usize,
            None,
            0u64,
            1u64,
        )
        .await
        .unwrap();
        assert_eq!(result.content_map.len(), 1);
        assert!(result.content_map.contains_key("linux/echo"));
        assert_eq!(result.exec_path, "echo");
    }

    /// Tests that a binary download (non-archive) with a URL-derived filename
    /// different from the tool id is CAS-imported correctly and that the
    /// content-map hash points to retrievable original bytes.
    #[tokio::test]
    async fn process_binary_with_url_derived_filename_cas_roundtrip() {
        let cas = InMemoryCas::default();
        let os_dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let binary_content = b"mock-binary-content-for-cas-test";
        let tool_id = "my-tool";
        let filename = "my-tool-v1.2.3"; // URL-derived, differs from tool_id
        let mut budget = MultiItemBudget::new();
        budget.add_item(0);
        let result = process_single_source(
            binary_content,
            None, // no archive format → binary path
            "linux",
            "linux",
            tool_id,
            os_dir.path(),
            filename,
            &cas,
            &budget,
            0usize,
            1usize,
            None,
            0u64,
            1u64,
        )
        .await
        .expect("process_single_source should succeed for binary");
        assert_eq!(
            result.content_map.len(),
            1,
            "binary import must produce exactly one content_map entry"
        );
        let cmap_key = format!("linux/{filename}");
        let hash_hex = result.content_map.get(&cmap_key).unwrap_or_else(|| {
            panic!(
                "content_map should contain key '{cmap_key}', got keys: {:?}",
                result.content_map.keys().collect::<Vec<_>>()
            )
        });
        // Verify the hash in content_map resolves back to the original bytes.
        let hash = hash_hex.parse::<Hash>().expect("content_map hash must be valid hash string");
        let retrieved = cas.get(hash).await.expect("CAS must contain the hash from content_map");
        assert_eq!(
            retrieved.to_vec(),
            binary_content,
            "CAS round-trip: stored bytes must match original binary content"
        );
        assert_eq!(
            result.exec_path, filename,
            "executable should be URL-derived filename for binary format"
        );
    }

    // ── size_hint_bytes ───────────────────────────────────────────────

    #[test]
    fn sd_provider_sources_have_size_hint_bytes() {
        let fetch = sd::sources();
        for source in &fetch.sources {
            assert!(
                source.size_hint_bytes.is_some(),
                "sd source for {} should have size_hint_bytes",
                source.os
            );
        }
    }

    #[test]
    fn builtin_providers_size_hint_bytes_are_none() {
        for (name, fetch) in [
            ("echo", echo::sources()),
            ("archive", archive::sources()),
            ("export", export::sources()),
            ("fs", fs::sources()),
            ("import", import::sources()),
        ] {
            for source in &fetch.sources {
                assert_eq!(
                    source.size_hint_bytes, None,
                    "{name} source for {} should have None",
                    source.os
                );
            }
        }
    }

    // ── fetch suffix_expected ─────────────────────────────────────

    #[tokio::test]
    async fn fetch_progress_uses_size_hint_bytes_when_expected_size_none() {
        use crate::cache::CacheDomainConfig;
        let cache_root = mediapm_utils::temp::cache_dir().expect("cache dir");
        let cache = crate::cache::Cache::open(
            cache_root.path(),
            &[CacheDomainConfig {
                domain: "download".to_string(),
                index_file_name: "tools.json".to_string(),
                entry_ttl_seconds: crate::cache::ENTRY_TTL_SECONDS,
            }],
        )
        .await
        .expect("open Cache with download domain");

        let cache = crate::cache_user_level::UserLevelCache::from_cache(cache);

        // Pre-seed cache with small bytes under fake URLs.
        cache.store_bytes("download", "mock://a", &[0u8; 50]).await;
        cache.store_bytes("download", "mock://b", &[1u8; 30]).await;

        let fetch = ResolvedToolFetch {
            tool_id: "test".to_string(),
            sources: vec![
                ResolvedSource {
                    os: "linux".to_string(),
                    arch: host_arch_label(),
                    producer: SourceProducer::Fetch { urls: vec!["mock://a".to_string()] },
                    expected_size: None,
                    size_hint_bytes: None,
                    identity: None,
                },
                ResolvedSource {
                    os: "macos".to_string(),
                    arch: host_arch_label(),
                    producer: SourceProducer::Fetch { urls: vec!["mock://b".to_string()] },
                    expected_size: None,
                    size_hint_bytes: Some(500),
                    identity: None,
                },
            ],
        };

        let snapshots: Arc<std::sync::Mutex<Vec<ProviderProgressSnapshot>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let snap_clone = Arc::clone(&snapshots);
        let cb: ProviderProgressCallback = Arc::new(move |snap| {
            snap_clone.lock().unwrap().push(snap);
        });

        let _downloaded = fetch_tool_sources(&fetch, &cache, "download", Some(cb))
            .await
            .expect("fetch should succeed with cached data");

        let all = snapshots.lock().unwrap().clone();
        assert!(!all.is_empty(), "should have recorded at least one snapshot");

        // The first snapshot total should include size_hint_bytes for the
        // remaining (unprocessed) source, not just completed bytes.
        // Source 0 (processed): 50 actual bytes, expected_size=None.
        // Source 1 (remaining): size_hint_bytes=Some(500).
        // Total = 50 + 500 = 550.
        let first_total = all[0].bytes.1;
        assert!(
            first_total > 80,
            "first snapshot total {first_total} should include size_hint_bytes fallback "
        );

        let mut prev_pos = 0u64;
        let mut prev_items = 0u64;
        for (i, snap) in all.iter().enumerate() {
            let pos = snap.bytes.0;
            let tot = snap.bytes.1;
            assert!(pos >= prev_pos, "position decreased at snapshot {i}: {pos} < {prev_pos}");
            assert!(pos <= tot, "position {pos} exceeds total {tot} at snapshot {i}");
            assert!(
                snap.items.0 >= prev_items,
                "items counter decreased at snapshot {i}: {} < {prev_items}",
                snap.items.0
            );
            prev_pos = pos;
            prev_items = snap.items.0;
        }
        assert_eq!(prev_items, 2, "two cached sources must both be counted as completed");
    }

    // ── Regression: cache key using actual URL ───────────────────────

    /// Regression test: content cache key must be the actual URL used, not
    /// blindly `urls[0]`.  When the first URL in the list is NOT cached but
    /// a later URL IS cached, `fetch_tool_sources` must find the cached data
    /// via the later URL.
    ///
    /// Before the fix, the cache key was hardcoded to `urls[0]`, so a source
    /// with `urls = ["a", "b"]` where only "b" is cached would not hit the
    /// cache — it would fall through to network download (and fail in tests).
    #[tokio::test]
    async fn fetch_cache_key_uses_actual_url_not_first_url() {
        use crate::cache::CacheDomainConfig;
        let cache_root = mediapm_utils::temp::cache_dir().expect("cache dir");
        let cache = crate::cache::Cache::open(
            cache_root.path(),
            &[CacheDomainConfig {
                domain: "download".to_string(),
                index_file_name: "tools.json".to_string(),
                entry_ttl_seconds: crate::cache::ENTRY_TTL_SECONDS,
            }],
        )
        .await
        .expect("open Cache with download domain");
        let cache = crate::cache_user_level::UserLevelCache::from_cache(cache);

        // Pre-seed cache with data under URL "b" only (not URL "a").
        let cached_data = b"cached-content-from-url-b";
        cache.store_bytes("download", "mock://b", cached_data).await;

        let fetch = ResolvedToolFetch {
            tool_id: "regression-test".to_string(),
            sources: vec![ResolvedSource {
                os: "linux".to_string(),
                arch: host_arch_label(),
                producer: SourceProducer::Fetch {
                    // "a" is not cached, "b" is cached — old code would miss
                    // on "a" and fail.
                    urls: vec!["mock://a".to_string(), "mock://b".to_string()],
                },
                expected_size: Some(cached_data.len() as u64),
                size_hint_bytes: None,
                identity: None,
            }],
        };

        let result = fetch_tool_sources(&fetch, &cache, "download", None)
            .await
            .expect("fetch should succeed via cached URL 'b'");

        assert_eq!(result.cached_count, 1, "exactly one source should be served from cache");
        assert_eq!(result.entries.len(), 1, "should have exactly one entry");
        assert_eq!(
            result.entries[0].bytes, cached_data,
            "returned bytes should match cached content from URL 'b'"
        );
    }

    // ── Identity-keyed content cache ─────────────────────────────────

    /// Path of the versioned download the permalink of
    /// [`a_permalink_cached_under_one_identity_is_not_served_under_another`]
    /// redirects to. Requests to it are counted to tell a fresh download from
    /// a cache-served fetch.
    const PERMALINK_DOWNLOAD_PATH: &str = "/download/macos/arm64/1759500000_N-1/ffmpeg.zip";

    /// Fetches one permalink source through the real fetch phase with
    /// `identity` as the source's resolved identity, and returns the payload
    /// bytes together with the number of requests the server received for the
    /// versioned download during the call: `0` when the content cache answered,
    /// `1` when the identity was cold and the payload was downloaded again.
    ///
    /// The count is taken over the versioned download path alone: a cold fetch
    /// also spends requests on the permalink's 302 and on the `.sha256` sibling
    /// probe, so a whole-server delta cannot distinguish "downloaded" from
    /// "served from cache".
    async fn fetch_one_source(
        server: &wiremock::MockServer,
        cache: &crate::cache_user_level::UserLevelCache,
        permalink: &str,
        identity: &str,
    ) -> (Vec<u8>, usize, Option<String>) {
        let before = download_request_count(server).await;
        let fetch = ResolvedToolFetch {
            tool_id: "ffmpeg".to_string(),
            sources: vec![ResolvedSource {
                os: "macos".to_string(),
                arch: "aarch64".to_string(),
                producer: SourceProducer::Fetch { urls: vec![permalink.to_string()] },
                expected_size: None,
                size_hint_bytes: None,
                identity: Some(identity.to_string()),
            }],
        };
        let downloaded = fetch_tool_sources(&fetch, cache, "tools", None)
            .await
            .expect("the permalink chain serves");
        let entry = downloaded.entries.first().expect("one source, one entry");
        let bytes = entry.bytes.clone();
        let actual_url = entry.actual_url.clone();
        (bytes, download_request_count(server).await - before, actual_url)
    }

    /// Number of requests the mock server has received for the versioned
    /// download [`PERMALINK_DOWNLOAD_PATH`] names.
    async fn download_request_count(server: &wiremock::MockServer) -> usize {
        server
            .received_requests()
            .await
            .expect("the request recorder is enabled")
            .iter()
            .filter(|request| request.url.path() == PERMALINK_DOWNLOAD_PATH)
            .count()
    }

    /// A permalink's cached payload is stored under its resolved identity, so a
    /// later resolve that names a different build cannot be served the older
    /// build's bytes. Without the identity in the key the entry is found again
    /// and refreshed on every sync, which freezes the payload upstream never
    /// rotates out of the workspace.
    #[tokio::test]
    async fn a_permalink_cached_under_one_identity_is_not_served_under_another() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redirect/latest/macos/arm64/snapshot/ffmpeg.zip"))
            .respond_with(ResponseTemplate::new(302).insert_header(
                "location",
                format!("{}{PERMALINK_DOWNLOAD_PATH}", server.uri()).as_str(),
            ))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(PERMALINK_DOWNLOAD_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"build-one".as_slice()))
            .mount(&server)
            .await;

        let (cache, _dir) = open_tools_cache().await;
        let permalink = format!("{}/redirect/latest/macos/arm64/snapshot/ffmpeg.zip", server.uri());

        let (first, first_downloads, _) =
            fetch_one_source(&server, &cache, &permalink, "build-one").await;
        assert_eq!(first, b"build-one");
        assert_eq!(first_downloads, 1, "a cold identity downloads the payload");

        let (same, reuses, served_url) =
            fetch_one_source(&server, &cache, &permalink, "build-one").await;
        assert_eq!(same, b"build-one", "the same identity is served from the cache");
        assert_eq!(reuses, 0, "a warm identity never reaches the server");
        assert_eq!(
            served_url.as_deref(),
            Some(permalink.as_str()),
            "a cache hit reports the requested URL, never the identity-suffixed cache key"
        );

        // The identity moved: the cached entry must not answer for it.
        let (moved_bytes, moved_downloads, _) =
            fetch_one_source(&server, &cache, &permalink, "build-two").await;
        assert_eq!(moved_downloads, 1, "a different identity is a cache miss");
        assert_eq!(moved_bytes, b"build-one", "the miss re-fetches whatever the permalink serves");
    }

    /// A `latest/download` alias is one stable path whose asset is replaced
    /// when upstream rotates, so the resolved build — not the URL — is what
    /// keeps one build's cached bytes from answering for another's. Two tags
    /// on the same URL must therefore be two cache entries, while a source
    /// whose URL is its own identity still keys by the URL alone.
    #[test]
    fn content_cache_key_separates_a_rotated_alias_from_the_url() {
        let url = "https://github.com/BtbN/FFmpeg-Builds/releases/latest/download/\
                   ffmpeg-master-latest-linux64-gpl.tar.xz";
        let build = content_cache_key(url, Some("autobuild-2025-07-15-12-00"));
        let rotated = content_cache_key(url, Some("autobuild-2025-08-01-09-00"));

        assert_ne!(
            build, rotated,
            "a rotated autobuild tag must not be served the previous build's cached bytes"
        );
        assert!(
            build.starts_with(url) && rotated.starts_with(url),
            "the URL stays the key's base, so a cache hit still reports the requested URL"
        );
        assert_eq!(
            content_cache_key(url, None),
            url,
            "a source with no moving target is keyed by its URL alone"
        );
    }

    // ── process_single_source budget tracking ────────────────────────

    #[tokio::test]
    async fn process_single_source_binary_budget_advances_correct_item() {
        let cas = InMemoryCas::default();
        let os_dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let content = b"binary-content-for-cost-test";
        let mut budget = MultiItemBudget::new();
        budget.add_item(0);
        let _result = process_single_source(
            content,
            None,
            "linux",
            "linux",
            "tool",
            os_dir.path(),
            "tool",
            &cas,
            &budget,
            0usize,
            1usize,
            None,
            0u64,
            1u64,
        )
        .await
        .unwrap();
        let (pos, total) = budget.snap(0);
        assert_eq!(
            pos, total,
            "binary source should advance budget item to completed (pos={pos}, total={total})"
        );
        assert_eq!(total, content.len() as u64);
    }

    #[tokio::test]
    async fn process_single_source_archive_two_items_completed() {
        let content = b"some-content-that-will-be-in-the-archive";
        let zip = synthetic_zip(&[("file.bin", content)]);
        let cas = InMemoryCas::default();
        let os_dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let mut budget = MultiItemBudget::new();
        budget.add_item(0);
        budget.add_item(0);
        let _result = process_single_source(
            &zip,
            Some(ARCHIVE_ZIP),
            "linux",
            "linux",
            "tool",
            os_dir.path(),
            "",
            &cas,
            &budget,
            0usize,
            2usize,
            None,
            0u64,
            2u64,
        )
        .await
        .unwrap();
        // Item 0 (decompress): pos should == total_compressed
        let (pos0, total0) = budget.snap(0);
        assert_eq!(
            pos0, total0,
            "decompress item should be fully advanced (pos={pos0}, total={total0})"
        );
        assert_eq!(total0, zip.len() as u64);
        // Item 1 (compress): pos should == total (decompressed size)
        let (pos1, total1) = budget.snap(1);
        assert_eq!(
            pos1, total1,
            "compress item should be fully advanced (pos={pos1}, total={total1})"
        );
        assert!(total1 >= content.len() as u64);
    }

    // ── process_single_source progress_cb ─────────────────────

    #[tokio::test]
    async fn process_progress_cb_fires_during_extraction() {
        // Create a tar.gz with multiple large pseudo-random entries so
        // sub-entry callbacks fire per SUB_ENTRY_CHUNK (64 KB) during
        // both decompress and compress.  3 × 200 KB = 600 KB total,
        // which at 64 KB/chunk gives ~9-10 callbacks per item, ~20 total.
        let entries: [(&str, &[u8]); 3] = [
            ("file1.bin", &pseudo_random_buffer(200_000)),
            ("file2.bin", &pseudo_random_buffer(200_000)),
            ("file3.bin", &pseudo_random_buffer(200_000)),
        ];
        let tgz = synthetic_tar_gz(&entries);
        let cas = InMemoryCas::default();
        let os_dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let mut budget = MultiItemBudget::new();
        budget.add_item(tgz.len() as u64);
        budget.add_item(0); // compress estimate — will be refined

        let snapshots: Arc<std::sync::Mutex<Vec<ProviderProgressSnapshot>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let snap_clone = Arc::clone(&snapshots);
        let cb: ProviderProgressCallback = Arc::new(move |snap| {
            snap_clone.lock().unwrap().push(snap);
        });

        let _result = process_single_source(
            &tgz,
            Some(ARCHIVE_TAR_GZ),
            "linux",
            "linux",
            "tool",
            os_dir.path(),
            "",
            &cas,
            &budget,
            0usize,
            2usize,
            Some(&cb),
            0u64,
            2u64,
        )
        .await
        .unwrap();

        let all = snapshots.lock().unwrap().clone();
        assert!(!all.is_empty(), "should have recorded at least one snapshot");
        assert!(
            all.len() > 2,
            "should have more snapshots than budget items (got {}), per-chunk callbacks must fire",
            all.len()
        );

        // Verify monotonicity and pos ≤ total for all snapshots.
        let mut prev_pos = 0u64;
        for (i, snap) in all.iter().enumerate() {
            let pos = snap.bytes.0;
            let tot = snap.bytes.1;
            assert!(pos >= prev_pos, "position decreased at snapshot {i}: {pos} < {prev_pos}");
            assert!(pos <= tot, "position {pos} exceeds total {tot} at snapshot {i}");
            prev_pos = pos;
        }
    }

    // ── process position ≤ total ───────────────────────────────

    #[tokio::test]
    async fn process_position_never_exceeds_total_with_archive_entries() {
        // Use a large enough pseudo-random buffer so the zip container overhead
        // is negligible compared to the decompressed size.
        let decompressed = pseudo_random_buffer(200_000);
        let zip = synthetic_zip(&[("tool.bin", &decompressed)]);

        let binary_bytes = vec![0u8; 100];

        let downloaded = DownloadedSources {
            tool_id: "test".to_string(),
            entries: vec![
                DownloadedSource {
                    os: "linux".to_string(),
                    arch: "x86_64".to_string(),
                    producer: SourceProducer::Fetch {
                        urls: vec!["https://example.com/tool.zip".to_string()],
                    },
                    bytes: zip.clone(),
                    expected_size: None,
                    actual_url: None,
                },
                DownloadedSource {
                    os: "macos".to_string(),
                    arch: "x86_64".to_string(),
                    producer: SourceProducer::Fetch {
                        urls: vec!["https://example.com/tool-bin".to_string()],
                    },
                    bytes: binary_bytes.clone(),
                    expected_size: Some(100),
                    actual_url: None,
                },
            ],
            cached_count: 0,
            warnings: Vec::new(),
        };

        let snapshots: Arc<std::sync::Mutex<Vec<ProviderProgressSnapshot>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let snap_clone = Arc::clone(&snapshots);
        let cas = InMemoryCas::default();
        let cb: ProviderProgressCallback = Arc::new(move |snap| {
            snap_clone.lock().unwrap().push(snap);
        });

        let _result = process_tool_sources(&downloaded, &cas, Some(cb))
            .await
            .expect("process should succeed with synthetic entries");

        let all = snapshots.lock().unwrap().clone();
        assert!(!all.is_empty(), "should have recorded at least one snapshot");

        // With per-chunk callbacks, we should have more process snapshots
        // than sources (the archive source fires sub-entry callbacks during
        // decompress and compress, not just once at completion).
        let process_snapshots: Vec<_> =
            all.iter().filter(|s| s.phase == ProviderPhase::Process).collect();
        assert!(
            process_snapshots.len() > downloaded.entries.len(),
            "expected more process snapshots ({}) than sources ({}) — sub-entry callbacks should fire during extraction",
            process_snapshots.len(),
            downloaded.entries.len()
        );

        // Verify monotonicity and pos ≤ total across ALL process snapshots.
        let mut prev_pos = 0u64;
        for (i, snap) in all.iter().enumerate() {
            if snap.phase != ProviderPhase::Process {
                continue;
            }
            let pos = snap.bytes.0;
            let tot = snap.bytes.1;
            assert!(
                pos >= prev_pos,
                "Process position decreased at snapshot {i}: {pos} < {prev_pos}",
            );
            assert!(pos <= tot, "Process position {pos} exceeds total {tot} at snapshot {i}");
            prev_pos = pos;
        }

        // Verify the final snapshot total reflects decompressed cost.
        let final_snap = all.iter().rev().find(|s| s.phase == ProviderPhase::Process).unwrap();
        let final_total = final_snap.bytes.1;
        let compressed_total = (zip.len() + binary_bytes.len()) as u64;
        assert!(
            final_total > compressed_total,
            "final total {final_total} should exceed compressed total {compressed_total} (decompressed cost was not added)"
        );
    }

    /// Two sources resolving to the same `content_map` key must fail loudly
    /// instead of silently overwriting. In a dual-arch slot the key carries an
    /// arch segment, so two archives sharing one OS *and* architecture collide
    /// on `{os}/{arch}/`; the distinct-arch gate lets this mixed set through
    /// (two arches are present), so the per-key duplicate guard is what rejects
    /// the colliding pair. Before this guard the second entry replaced the
    /// first and the tool provisioned with one payload.
    #[tokio::test]
    async fn process_rejects_duplicate_content_map_keys() {
        let first = synthetic_zip(&[("tool.bin", b"first-payload")]);
        let second = synthetic_zip(&[("tool.bin", b"second-payload")]);
        let third = synthetic_zip(&[("tool.bin", b"third-payload")]);

        let downloaded = DownloadedSources {
            tool_id: "test".to_string(),
            entries: vec![
                DownloadedSource {
                    os: "linux".to_string(),
                    arch: "x86_64".to_string(),
                    producer: SourceProducer::Fetch {
                        urls: vec!["https://example.com/tool-a.zip".to_string()],
                    },
                    bytes: first,
                    expected_size: None,
                    actual_url: None,
                },
                DownloadedSource {
                    os: "linux".to_string(),
                    arch: "x86_64".to_string(),
                    producer: SourceProducer::Fetch {
                        urls: vec!["https://example.com/tool-b.zip".to_string()],
                    },
                    bytes: second,
                    expected_size: None,
                    actual_url: None,
                },
                DownloadedSource {
                    os: "linux".to_string(),
                    arch: "aarch64".to_string(),
                    producer: SourceProducer::Fetch {
                        urls: vec!["https://example.com/tool-c.zip".to_string()],
                    },
                    bytes: third,
                    expected_size: None,
                    actual_url: None,
                },
            ],
            cached_count: 0,
            warnings: Vec::new(),
        };

        let cas = InMemoryCas::default();
        let err = process_tool_sources(&downloaded, &cas, None)
            .await
            .expect_err("duplicate content_map key must be rejected");

        let message = err.to_string();
        assert!(
            message.contains("duplicate content_map key"),
            "error must name the duplicate key problem, got: {message}"
        );
        assert!(
            message.contains("linux/x86_64/"),
            "error must name the colliding arch-segmented key, got: {message}"
        );
    }

    // ── generate_launcher_script ──────────────────────────────────────

    #[test]
    fn generate_windows_launcher() {
        let script = generate_launcher_script("windows", "echo@v1", &[]);
        let text = String::from_utf8_lossy(&script);
        assert!(text.contains("MEDIAPM_EXECUTABLE"));
        assert!(text.contains("echo@v1"));
        assert!(text.contains("%*"));
    }

    #[test]
    fn generate_unix_launcher() {
        let script = generate_launcher_script("linux", "echo@v1", &[]);
        let text = String::from_utf8_lossy(&script);
        assert!(text.contains("exec"));
        assert!(text.contains("MEDIAPM_EXECUTABLE"));
        assert!(text.contains("echo@v1"));
        assert!(text.contains("$@"));
    }

    #[test]
    fn generate_unix_launcher_with_mediapm_builtin_prefix() {
        let script = generate_launcher_script("linux", "media-tagger", &["builtin".to_string()]);
        let text = String::from_utf8_lossy(&script);
        assert!(text.contains("builtin media-tagger"));
    }

    #[test]
    fn generate_macos_launcher() {
        let script = generate_launcher_script("macos", "echo@v1", &[]);
        let text = String::from_utf8_lossy(&script);
        assert!(text.contains("exec"));
        assert!(text.contains("MEDIAPM_EXECUTABLE"));
        assert!(text.contains("echo@v1"));
    }

    /// Protects the guard every generated launcher carries: both OS shapes
    /// must refuse an unset `MEDIAPM_EXECUTABLE` and name the build remedy, so
    /// a missing CLI fails on the launcher's second line instead of dispatching
    /// an empty command or an unrelated program.
    #[test]
    fn launcher_script_requires_the_executable_variable() {
        let unix = generate_launcher_script("linux", "media-tagger", &["builtin".to_string()]);
        let unix_text = String::from_utf8_lossy(&unix);
        let guard_line = unix_text.lines().nth(1).expect("launcher script has a second line");
        assert!(
            guard_line.starts_with(": \"${MEDIAPM_EXECUTABLE:?"),
            "second line must be the `:?` guard, got: {guard_line}"
        );
        assert!(
            guard_line.contains("cargo build --package mediapm"),
            "guard message must name the build remedy, got: {guard_line}"
        );

        let windows = generate_launcher_script("windows", "media-tagger", &["builtin".to_string()]);
        let windows_text = String::from_utf8_lossy(&windows);
        assert!(
            windows_text.contains("if not defined MEDIAPM_EXECUTABLE"),
            "batch script must guard on a defined variable, got: {windows_text}"
        );
        assert!(
            windows_text.contains("exit /b 1"),
            "batch guard must leave a non-zero exit status, got: {windows_text}"
        );
        assert!(
            windows_text.contains("cargo build --package mediapm"),
            "guard message must name the build remedy, got: {windows_text}"
        );
    }

    /// Protects the ordering inside the POSIX script: the guard runs first, and
    /// the dispatch arguments stay on the `exec` line, where the exec'd CLI
    /// receives them.
    #[test]
    fn launcher_script_keeps_the_dispatch_behind_the_guard() {
        let script = generate_launcher_script("linux", "media-tagger", &["builtin".to_string()]);
        let text = String::from_utf8_lossy(&script);
        let guard_index = text
            .lines()
            .position(|line| line.starts_with(": \"${MEDIAPM_EXECUTABLE:?"))
            .expect("launcher script carries the guard line");
        let exec_index = text
            .lines()
            .position(|line| line.starts_with("exec "))
            .expect("launcher script carries the exec line");
        assert!(
            guard_index < exec_index,
            "guard line {guard_index} must precede exec line {exec_index}"
        );
        let exec_line = text.lines().nth(exec_index).expect("exec line is indexed");
        assert!(
            exec_line.contains("builtin media-tagger"),
            "dispatch must stay on the exec line, got: {exec_line}"
        );
    }

    /// Protects the failure mode the guard exists for: running the generated
    /// POSIX script with no `MEDIAPM_EXECUTABLE` must exit non-zero, write
    /// nothing to stdout, and name the variable on stderr.
    #[cfg(unix)]
    #[test]
    fn launcher_script_exits_without_the_variable() {
        use std::os::unix::fs::PermissionsExt;

        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let path = dir.path().join("media-tagger-launcher");
        let script = generate_launcher_script("linux", "media-tagger", &["builtin".to_string()]);
        std::fs::write(&path, script).expect("write launcher script");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
            .expect("mark launcher script executable");

        let output = std::process::Command::new(&path)
            .env_remove("MEDIAPM_EXECUTABLE")
            .output()
            .expect("run launcher script");

        assert!(!output.status.success(), "launcher must fail without MEDIAPM_EXECUTABLE");
        assert!(output.stdout.is_empty(), "launcher must not write to stdout");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("MEDIAPM_EXECUTABLE"),
            "stderr must name the variable, got: {stderr}"
        );
    }

    /// Windows twin of the execution test: the batch guard must leave status 1
    /// with an empty stdout and the variable named on stderr.
    ///
    /// The generated batch text is asserted on every platform, so its shape is
    /// compiled and checked wherever the suite runs rather than only on a
    /// Windows host. The execution half is gated on runtime detection because
    /// a `.cmd` batch file needs `cmd /C` to interpret it: on a POSIX host
    /// there is no interpreter to run it with, so only the shape can be
    /// asserted there.
    #[test]
    fn launcher_script_exits_without_the_variable_windows() {
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let path = dir.path().join("media-tagger-launcher.cmd");
        let script = generate_launcher_script("windows", "media-tagger", &["builtin".to_string()]);
        let text = String::from_utf8_lossy(&script).into_owned();

        assert!(
            text.contains("if not defined MEDIAPM_EXECUTABLE"),
            "batch script must guard on a defined variable, got: {text}"
        );
        assert!(
            text.contains("exit /b 1"),
            "batch guard must leave a non-zero exit status, got: {text}"
        );
        assert!(
            text.contains("\"%MEDIAPM_EXECUTABLE%\" builtin media-tagger %*"),
            "dispatch must stay on the batch dispatch line with the passthrough, got: {text}"
        );

        std::fs::write(&path, script).expect("write launcher script");

        if !cfg!(windows) {
            return;
        }

        let output = std::process::Command::new("cmd")
            .arg("/C")
            .arg(&path)
            .env_remove("MEDIAPM_EXECUTABLE")
            .output()
            .expect("run launcher script through cmd");

        assert!(!output.status.success(), "launcher must fail without MEDIAPM_EXECUTABLE");
        assert!(output.stdout.is_empty(), "launcher must not write to stdout");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("MEDIAPM_EXECUTABLE"),
            "stderr must name the variable, got: {stderr}"
        );
    }

    /// Protects the seam the launcher guard exists for: with
    /// `MEDIAPM_EXECUTABLE` set, the generated POSIX launcher must reach that
    /// target with exactly the dispatch arguments followed by the caller's
    /// own. The absence test covers only the guard's failure direction, so a
    /// guard written with `:-` instead of `:?`, or an `exec` moved inside the
    /// guard, would leave it green while the CLI never received its dispatch.
    #[cfg(unix)]
    #[test]
    fn launcher_script_dispatches_to_the_executable_with_the_dispatch_arguments() {
        use std::os::unix::fs::PermissionsExt;

        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let launcher = dir.path().join("media-tagger-launcher");
        let stub = dir.path().join("mediapm-stub");
        let record = dir.path().join("recorded-args.txt");

        std::fs::write(&stub, "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$MEDIAPM_ARG_RECORD\"\n")
            .expect("write stub executable");
        std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o755))
            .expect("mark stub executable");

        let script = generate_launcher_script("linux", "media-tagger", &["builtin".to_string()]);
        std::fs::write(&launcher, script).expect("write launcher script");
        std::fs::set_permissions(&launcher, std::fs::Permissions::from_mode(0o755))
            .expect("mark launcher script executable");

        let output = std::process::Command::new(&launcher)
            .env("MEDIAPM_EXECUTABLE", &stub)
            .env("MEDIAPM_ARG_RECORD", &record)
            .arg("--input")
            .output()
            .expect("run launcher script");

        assert!(
            output.status.success(),
            "launcher must succeed with MEDIAPM_EXECUTABLE set, stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let recorded = std::fs::read_to_string(&record).expect("read the recorded arguments");
        let arguments: Vec<&str> = recorded.lines().collect();
        assert_eq!(arguments, ["builtin", "media-tagger", "--input"]);
    }

    // ── extract helpers ───────────────────────────────────────────────

    #[test]
    fn extract_zip_rejects_tar_xz_bytes() {
        let txz = synthetic_tar_xz(&[("x", &[0u8; 4])]);
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        assert!(extract_archive(&txz, Some(ARCHIVE_ZIP), dir.path(), None).is_err());
    }

    #[test]
    fn extract_tar_gz_rejects_zip_bytes() {
        let zip = synthetic_zip(&[("x", &[0u8; 4])]);
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        assert!(extract_archive(&zip, Some(ARCHIVE_TAR_GZ), dir.path(), None).is_err());
    }

    #[test]
    fn extract_tar_xz_rejects_zip_bytes() {
        let zip = synthetic_zip(&[("x", &[0u8; 4])]);
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        assert!(extract_archive(&zip, Some(ARCHIVE_TAR_XZ), dir.path(), None).is_err());
    }

    // ── pack_directory_to_uncompressed_zip_bytes ──────────────────────

    #[test]
    fn pack_directory_creates_zip_with_all_files() {
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        std::fs::write(dir.path().join("a.txt"), b"aaa").unwrap();
        std::fs::create_dir(dir.path().join("sub")).unwrap();
        std::fs::write(dir.path().join("sub").join("b.txt"), b"bbb").unwrap();

        let zip_bytes = pack_directory_to_uncompressed_zip_bytes(dir.path(), 0, None).unwrap();
        let mut archive = zip::ZipArchive::new(std::io::Cursor::new(&zip_bytes)).unwrap();
        assert_eq!(archive.len(), 2);

        let mut names: Vec<String> =
            (0..archive.len()).map(|i| archive.by_index(i).unwrap().name().to_string()).collect();
        names.sort();
        assert_eq!(names, vec!["a.txt", "sub/b.txt"]);
    }

    // ── Progress callback granularity ─────────────────────────────────
    //
    // These tests verify that archive extraction functions fire progress
    // callbacks at entry-level granularity (not just once per source).
    // Before the fix, the [process] progress bar only updated once per
    // downloaded source, freezing for seconds during extraction of large
    // archives. Per-entry callbacks give the bar smooth 20Hz updates.

    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Creates a counting local progress callback that ignores position values.
    fn counting_local_cb() -> (Box<dyn Fn(u64)>, Arc<AtomicUsize>) {
        let count = Arc::new(AtomicUsize::new(0));
        let cb = {
            let count = count.clone();
            move |_: u64| {
                count.fetch_add(1, Ordering::Relaxed);
            }
        };
        (Box::new(cb), count)
    }

    #[test]
    fn extract_zip_fires_per_entry_progress() {
        let entries: [(&str, &[u8]); 3] =
            [("small.bin", &[0u8; 50]), ("medium.bin", &[1u8; 200]), ("large.bin", &[2u8; 800])];
        let zip = synthetic_zip(&entries);
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let (cb, count) = counting_local_cb();
        extract_zip(&zip, dir.path(), Some(&cb)).unwrap();
        let call_count = count.load(Ordering::Relaxed);
        assert!(
            call_count >= entries.len(),
            "expected >= {} callbacks (one per zip entry), got {call_count}",
            entries.len(),
        );
    }

    #[test]
    fn extract_tar_gz_fires_per_entry_progress() {
        let entries: [(&str, &[u8]); 4] = [
            ("a.bin", &[0u8; 100]),
            ("b.bin", &[1u8; 200]),
            ("c.bin", &[2u8; 300]),
            ("d.bin", &[3u8; 400]),
        ];
        let tgz = synthetic_tar_gz(&entries);
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let (cb, count) = counting_local_cb();
        extract_tar_gz(&tgz, dir.path(), Some(&cb)).unwrap();
        let call_count = count.load(Ordering::Relaxed);
        assert!(
            call_count >= entries.len(),
            "expected >= {} callbacks (one per tar entry), got {call_count}",
            entries.len(),
        );
    }

    #[test]
    fn extract_tar_xz_fires_per_entry_progress() {
        let entries: [(&str, &[u8]); 3] =
            [("x.bin", &[0u8; 64]), ("y.bin", &[1u8; 128]), ("z.bin", &[2u8; 256])];
        let txz = synthetic_tar_xz(&entries);
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let (cb, count) = counting_local_cb();
        extract_tar_xz(&txz, dir.path(), Some(&cb)).unwrap();
        let call_count = count.load(Ordering::Relaxed);
        assert!(
            call_count >= entries.len(),
            "expected >= {} callbacks (one per tar entry), got {call_count}",
            entries.len(),
        );
    }

    // ── Sub-entry progress for large archives ─────────────────────
    //
    // These tests verify that extraction of a single large archive entry
    // fires multiple progress callbacks (sub-entry granularity), not just
    // one callback per entry.  A 200 KB hard-to-compress entry in a ZIP
    // with 64 KB read chunks produces ~4 chunk callbacks.  A 300 KB
    // hard-to-compress tar.gz entry triggers ~2 CountingReader callbacks
    // at SUB_ENTRY_CHUNK (64 KB) boundaries.

    #[test]
    fn extract_zip_large_entry_fires_multiple_sub_entry_callbacks() {
        let large = pseudo_random_buffer(200_000);
        let entries = [("large.bin", large.as_slice())];
        let zip = synthetic_zip(&entries);
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let (cb, count) = counting_local_cb();
        extract_zip(&zip, dir.path(), Some(&cb)).unwrap();
        let call_count = count.load(Ordering::Relaxed);
        assert!(
            call_count >= 5,
            "expected >=5 callbacks for large zip entry (entry-level + ~4 chunk), got {call_count}",
        );
    }

    #[test]
    fn extract_tar_gz_large_entry_fires_sub_entry_progress() {
        let large = pseudo_random_buffer(300_000);
        let entries = [("large.bin", large.as_slice())];
        let tgz = synthetic_tar_gz(&entries);
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let (cb, count) = counting_local_cb();
        extract_tar_gz(&tgz, dir.path(), Some(&cb)).unwrap();
        let call_count = count.load(Ordering::Relaxed);
        assert!(
            call_count >= 3,
            "expected >=3 callbacks for large tar.gz (entry-level + ~2 sub-entry), got {call_count}",
        );
    }

    // ── Progress monotonicity ────────────────────────────────────────
    //
    // These tests verify that extraction progress callbacks produce
    // monotonically non-decreasing position and constant total.  Before
    // the fix, ZIP extraction used decompressed sizes that could cause
    // backward jumps and total changes.

    use std::sync::Mutex;

    /// Records all progress positions for later analysis.
    struct PositionRecorder {
        positions: Arc<Mutex<Vec<u64>>>,
    }

    impl PositionRecorder {
        fn new() -> (Self, Box<dyn Fn(u64)>) {
            let positions: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
            let clone = Arc::clone(&positions);
            let cb = move |pos: u64| {
                clone.lock().unwrap().push(pos);
            };
            (Self { positions }, Box::new(cb))
        }

        fn positions(&self) -> Vec<u64> {
            self.positions.lock().unwrap().clone()
        }
    }

    #[test]
    fn extract_zip_progress_position_non_decreasing_and_total_constant() {
        let entries: [(&str, &[u8]); 4] = [
            ("a.bin", &[0u8; 100]),
            ("b.bin", &[1u8; 500]),
            ("c.bin", &[2u8; 200]),
            ("d.bin", &[3u8; 800]),
        ];
        let zip_bytes = synthetic_zip(&entries);
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let (recorder, cb) = PositionRecorder::new();

        extract_zip(&zip_bytes, dir.path(), Some(&cb)).expect("extract_zip should succeed");

        let positions = recorder.positions();
        assert!(!positions.is_empty(), "should have recorded at least one position");

        let mut prev_position = 0u64;

        for (i, pos) in positions.iter().enumerate() {
            assert!(
                *pos >= prev_position,
                "position decreased at position {i}: {pos} < {prev_position}"
            );
            prev_position = *pos;
        }
    }

    #[test]
    fn extract_tar_gz_progress_position_non_decreasing() {
        let entries: [(&str, &[u8]); 3] =
            [("x.bin", &[0u8; 256]), ("y.bin", &[1u8; 512]), ("z.bin", &[2u8; 128])];
        let tgz = synthetic_tar_gz(&entries);
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let (recorder, cb) = PositionRecorder::new();

        extract_tar_gz(&tgz, dir.path(), Some(&cb)).expect("extract_tar_gz should succeed");

        let positions = recorder.positions();
        assert!(!positions.is_empty());

        let mut prev_pos = 0u64;
        for (i, pos) in positions.iter().enumerate() {
            assert!(*pos >= prev_pos, "position decreased at position {i}: {pos} < {prev_pos}");
            prev_pos = *pos;
        }
    }

    #[test]
    fn extract_tar_xz_progress_position_non_decreasing() {
        let entries: [(&str, &[u8]); 2] = [("a.bin", &[0u8; 200]), ("b.bin", &[1u8; 300])];
        let txz = synthetic_tar_xz(&entries);
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let (recorder, cb) = PositionRecorder::new();

        extract_tar_xz(&txz, dir.path(), Some(&cb)).expect("extract_tar_xz should succeed");

        let positions = recorder.positions();
        assert!(!positions.is_empty());

        let mut prev_pos = 0u64;
        for (i, pos) in positions.iter().enumerate() {
            assert!(*pos >= prev_pos, "position decreased at position {i}: {pos} < {prev_pos}");
            prev_pos = *pos;
        }
    }

    /// Fetch-phase progress with known `expected_size` values: position should
    /// never decrease, and position should never exceed total (the suffix-
    /// expected estimate).  Total may decrease as remaining-expected narrows
    /// (the intentional "ASAP lower-bound" design).
    #[tokio::test]
    async fn fetch_progress_monotonic_with_known_sizes() {
        use crate::cache_user_level::UserLevelCache;

        let fetch = ResolvedToolFetch {
            tool_id: "test-tool".to_string(),
            sources: vec![
                ResolvedSource {
                    os: "linux".to_string(),
                    arch: host_arch_label(),
                    producer: SourceProducer::launcher("test"),
                    expected_size: Some(100),
                    size_hint_bytes: None,
                    identity: None,
                },
                ResolvedSource {
                    os: "macos".to_string(),
                    arch: host_arch_label(),
                    producer: SourceProducer::launcher("test"),
                    expected_size: Some(200),
                    size_hint_bytes: None,
                    identity: None,
                },
                ResolvedSource {
                    os: "windows".to_string(),
                    arch: host_arch_label(),
                    producer: SourceProducer::launcher("test"),
                    expected_size: Some(150),
                    size_hint_bytes: None,
                    identity: None,
                },
            ],
        };

        let cache_root = mediapm_utils::temp::cache_dir().expect("cache dir");
        let cache =
            UserLevelCache::open(cache_root.path(), "tools.json", crate::cache::ENTRY_TTL_SECONDS)
                .await
                .expect("open UserLevelCache");

        let snapshots: Arc<std::sync::Mutex<Vec<ProviderProgressSnapshot>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let snap_clone = Arc::clone(&snapshots);
        let cb: Option<ProviderProgressCallback> = Some(Arc::new(move |snap| {
            snap_clone.lock().unwrap().push(snap);
        }));

        let _result =
            fetch_tool_sources(&fetch, &cache, "download", cb).await.expect("fetch should succeed");

        let all = snapshots.lock().unwrap().clone();
        assert!(!all.is_empty(), "should have recorded at least one fetch snapshot");

        let mut prev_pos = 0u64;
        let mut prev_items = 0u64;
        for (i, snap) in all.iter().enumerate() {
            let pos = snap.bytes.0;
            let tot = snap.bytes.1;
            assert!(pos >= prev_pos, "position decreased at snapshot {i}: {pos} < {prev_pos}");
            assert!(pos <= tot, "position {pos} exceeds total {tot} at snapshot {i}");
            assert!(
                snap.items.0 >= prev_items,
                "items counter decreased at snapshot {i}: {} < {prev_items}",
                snap.items.0
            );
            prev_pos = pos;
            prev_items = snap.items.0;
        }
        assert_eq!(prev_items, 3, "three launcher sources must all be counted as completed");
    }

    /// The fetch progress `items` counter counts *completed sources*, never a
    /// single future's index. Under `buffer_unordered(N)` several futures are
    /// in flight, so a per-future index would make the rendered `{n}/{total}`
    /// prefix oscillate backwards (e.g. `3/3` from a fast source, then `1/3`
    /// from a slower one still streaming).
    ///
    /// # Why the endpoint assertion is on the maximum, not the last element
    ///
    /// [`fire_fetch_chunk_progress`] loads `completed` and *then* invokes the
    /// callback, so the load and the resulting push into the collected vector
    /// are not one atomic step. A thread that loads `2` and is descheduled
    /// before its callback runs can therefore push `(2, total)` *after* a
    /// thread that already pushed `(3, total)`, making the vector's last
    /// element an arbitrary stale sample. Asserting on that last element made
    /// this test fail on its own scheduling noise (measured 2/200 runs) with
    /// no defect present.
    ///
    /// The endpoint property that actually holds — and that the counter bug
    /// would break — is that some report observes every source as completed:
    /// whichever thread performs the final `fetch_add` to `total` loads
    /// `total` in the fire that follows it, so the maximum is pinned at
    /// `total`. That is deterministic, and
    /// [`fetch_items_counter_reports_the_completed_count_exactly`] pins the
    /// load-to-snapshot mapping without any threads at all.
    #[test]
    fn fetch_items_counter_never_decreases_under_concurrent_reporting() {
        use std::sync::atomic::{AtomicU64, Ordering};

        let completed = Arc::new(AtomicU64::new(0));
        let total_sources = 3u64;
        let mut budget_inner = MultiItemBudget::new();
        for _ in 0..total_sources {
            budget_inner.add_item(0);
        }
        let budget = Arc::new(budget_inner);

        let seen: Arc<std::sync::Mutex<Vec<(u64, u64)>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen_cb = Arc::clone(&seen);
        let cb: ProviderProgressCallback = Arc::new(move |snap| {
            seen_cb.lock().unwrap().push(snap.items);
        });

        let mut handles = Vec::new();
        for _ in 0..total_sources {
            let completed = Arc::clone(&completed);
            let budget = Arc::clone(&budget);
            let cb = Arc::clone(&cb);
            handles.push(std::thread::spawn(move || {
                for _ in 0..200 {
                    fire_fetch_chunk_progress(&cb, &completed, total_sources, &budget);
                }
                completed.fetch_add(1, Ordering::AcqRel);
                fire_fetch_chunk_progress(&cb, &completed, total_sources, &budget);
            }));
        }
        for h in handles {
            h.join().expect("reporter thread panicked");
        }

        let all = seen.lock().unwrap().clone();
        assert!(!all.is_empty(), "no snapshots were emitted");
        // The per-thread contiguous blocks are monotone, but interleaving across
        // threads can place earlier snapshots after later ones in the collected
        // vec. Verify the weaker but correct invariant: every snapshot is within
        // [0, total_sources], and the highest value observed reaches
        // total_sources (the final `fetch_add` is always followed by a fire
        // that reads it, so the maximum is deterministic where the last
        // collected element is not).
        let mut max_items = 0u64;
        for (i, (items, total)) in all.iter().enumerate() {
            assert_eq!(*total, total_sources, "snapshot {i}: wrong total");
            assert!(
                *items <= total_sources,
                "snapshot {i}: items {items} exceeds total {total_sources}"
            );
            max_items = max_items.max(*items);
        }
        assert_eq!(max_items, total_sources, "some report must observe every source completed");
    }

    /// Deterministic counterpart to
    /// [`fetch_items_counter_never_decreases_under_concurrent_reporting`]:
    /// pins the exact `completed` count to `snapshot.items` mapping with no
    /// threads involved, so a regression that reports a per-future index (or
    /// any value other than the live completed count) fails on the first
    /// mismatched step instead of depending on scheduler luck.
    #[test]
    fn fetch_items_counter_reports_the_completed_count_exactly() {
        use std::sync::atomic::{AtomicU64, Ordering};

        let total_sources = 4u64;
        let completed = AtomicU64::new(0);
        let mut budget_inner = MultiItemBudget::new();
        for _ in 0..total_sources {
            budget_inner.add_item(0);
        }
        let budget = budget_inner;

        let seen: Arc<std::sync::Mutex<Vec<(u64, u64)>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let cb: ProviderProgressCallback = Arc::new({
            let seen = Arc::clone(&seen);
            move |snap: ProviderProgressSnapshot| {
                seen.lock().unwrap().push(snap.items);
            }
        });

        for expected in 0..=total_sources {
            fire_fetch_chunk_progress(&cb, &completed, total_sources, &budget);
            assert_eq!(
                *seen.lock().unwrap().last().expect("snapshot emitted"),
                (expected, total_sources),
                "snapshot must report the live completed count, never a per-future index"
            );
            if expected < total_sources {
                completed.fetch_add(1, Ordering::AcqRel);
            }
        }
    }

    // ── buffer_unordered(0) regression test ──────────────────────────
    //
    // Verifies that fetch_tool_sources completes when all sources are
    // GenerateLauncher (no Fetch sources). Without the `.max(1)` capacity
    // floor, `buffer_unordered(0)` on an empty stream hangs forever.

    /// Regression test for `buffer_unordered(0)` hanging on empty streams.
    /// When all sources are `GenerateLauncher`, `fetch_futures` is empty and
    /// `fetch_concurrency` was 0 — `buffer_unordered(0)` never polls the
    /// inner stream, so the `.collect().await` never completes.
    #[tokio::test]
    async fn fetch_tool_sources_completes_with_generate_launcher_only() {
        use crate::cache::{Cache, CacheDomainConfig};
        use crate::cache_user_level::UserLevelCache;

        let cache_dir = mediapm_utils::temp::cache_dir().expect("temp cache dir");
        let cache = Cache::open(
            cache_dir.path(),
            &[CacheDomainConfig {
                domain: "tools".to_string(),
                index_file_name: "tools.json".to_string(),
                entry_ttl_seconds: 86400,
            }],
        )
        .await
        .expect("cache open");
        let user_cache = UserLevelCache::from_cache(cache);

        let fetch = ResolvedToolFetch {
            tool_id: "media-tagger".to_string(),
            sources: vec![
                ResolvedSource {
                    os: "linux".to_string(),
                    arch: host_arch_label(),
                    producer: SourceProducer::mediapm_builtin("media-tagger"),
                    expected_size: None,
                    size_hint_bytes: None,
                    identity: None,
                },
                ResolvedSource {
                    os: "macos".to_string(),
                    arch: host_arch_label(),
                    producer: SourceProducer::mediapm_builtin("media-tagger"),
                    expected_size: None,
                    size_hint_bytes: None,
                    identity: None,
                },
                ResolvedSource {
                    os: "windows".to_string(),
                    arch: host_arch_label(),
                    producer: SourceProducer::mediapm_builtin("media-tagger"),
                    expected_size: None,
                    size_hint_bytes: None,
                    identity: None,
                },
            ],
        };

        // Must complete within a reasonable time — the old code hung here.
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            fetch_tool_sources(&fetch, &user_cache, "tools", None),
        )
        .await
        .expect("fetch_tool_sources must not hang (buffer_unordered(0) regression)");

        let downloaded = result.expect("fetch should succeed");
        assert_eq!(downloaded.entries.len(), 3, "all 3 launcher sources processed");
        assert_eq!(downloaded.cached_count, 0, "no cache hits for launchers");
    }

    // ── Counting mechanism regression tests ─────────────────────────
    //
    // These tests verify the byte-counting behavior of extraction and
    // packing helpers. All tests pass with the CURRENT code (pre-Phase 3
    // chunk-policy changes).

    #[test]
    fn counting_reader_tracks_exact_compressed_bytes() {
        // Feed 1 MB of data through CountingReader and verify consumed == 1 MB.
        let data = vec![0xABu8; 1_000_000];
        let consumed = Cell::new(0u64);
        let positions: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
        let positions_clone = Arc::clone(&positions);
        let cb = move |pos: u64| {
            positions_clone.lock().unwrap().push(pos);
        };
        {
            let reader = CountingReader {
                cursor: std::io::Cursor::new(&data[..]),
                bytes_read: &consumed,
                last_cb_pos: Cell::new(0),
                progress_cb: Some(&cb),
            };
            // Read all bytes through CountingReader.
            let mut buf = Vec::new();
            std::io::Read::by_ref(&mut std::io::Read::take(reader, u64::MAX))
                .read_to_end(&mut buf)
                .unwrap();
        }
        assert_eq!(
            consumed.get(),
            1_000_000,
            "CountingReader should track exactly all bytes consumed"
        );
        let recorded = positions.lock().unwrap();
        assert!(!recorded.is_empty(), "CountingReader should have fired at least one callback");
        for (i, pos) in recorded.iter().enumerate() {
            assert!(*pos <= 1_000_000, "position {pos} should never exceed total at callback {i}");
        }
    }

    #[test]
    fn gzdecoder_with_counting_reader_tracks_consumption() {
        // Compress 500 KB via GzEncoder, decompress through GzDecoder +
        // CountingReader, verify consumed >= original size (gzip framing
        // adds ~20-40 bytes overhead, so consumed > original).
        use flate2::Compression;
        use flate2::write::GzEncoder;
        use std::io::Write;

        let original = vec![0x42u8; 500_000];
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&original).unwrap();
        let compressed = encoder.finish().unwrap();
        assert!(compressed.len() < original.len(), "gzip should compress 500 KB of uniform bytes");

        let consumed = Cell::new(0u64);
        let reader = CountingReader {
            cursor: std::io::Cursor::new(&compressed[..]),
            bytes_read: &consumed,
            last_cb_pos: Cell::new(0),
            progress_cb: None,
        };
        let decoder = flate2::read::GzDecoder::new(reader);
        let mut decompressed = Vec::new();
        std::io::Read::by_ref(&mut std::io::Read::take(decoder, u64::MAX))
            .read_to_end(&mut decompressed)
            .unwrap();

        assert_eq!(decompressed.len(), original.len(), "decompressed data should match original");
        assert!(
            consumed.get() <= compressed.len() as u64,
            "CountingReader consumed {} should not exceed compressed input size {}",
            consumed.get(),
            compressed.len()
        );
        // GzDecoder consumes all compressed data to fully decompress,
        // so consumed should be the compressed size (or very close).
        assert!(
            usize::try_from(consumed.get()).expect("consumed byte count fits usize")
                >= compressed.len() - 100,
            "CountingReader consumed {} should be close to compressed size {}",
            consumed.get(),
            compressed.len()
        );
    }

    #[test]
    fn zip_extraction_end_position_equals_entry_compressed() {
        // Extract a synthetic ZIP, verify the final callback position equals
        // the sum of entry compressed sizes.
        use std::io::Write;
        use zip::ZipWriter;
        use zip::write::SimpleFileOptions;

        // Build ZIP with entries of known sizes.
        let mut zip_buf = Vec::new();
        let entry_data: [(&str, &[u8]); 3] =
            [("a.bin", &[0xAAu8; 1000]), ("b.bin", &[0xBBu8; 2000]), ("c.bin", &[0xCCu8; 3000])];
        {
            let mut writer = ZipWriter::new(std::io::Cursor::new(&mut zip_buf));
            for (name, data) in &entry_data {
                writer.start_file(*name, SimpleFileOptions::default()).expect("start zip entry");
                writer.write_all(data).expect("write zip entry");
            }
            writer.finish().expect("finish zip");
        }
        // Compute expected compressed total from central directory.
        let mut archive = zip::ZipArchive::new(std::io::Cursor::new(&zip_buf)).unwrap();
        let expected_total: u64 =
            (0..archive.len()).map(|i| archive.by_index(i).unwrap().compressed_size()).sum();

        let positions: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
        let positions_clone = Arc::clone(&positions);
        let cb = move |pos: u64| {
            positions_clone.lock().unwrap().push(pos);
        };
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        extract_zip(&zip_buf, dir.path(), Some(&cb)).expect("extract_zip");

        let recorded = positions.lock().unwrap();
        assert!(!recorded.is_empty(), "should have recorded positions");
        let last_pos = *recorded.last().unwrap();
        assert_eq!(
            last_pos, expected_total,
            "final callback position {last_pos} should equal total compressed size {expected_total}"
        );
    }

    #[test]
    fn zip_position_never_exceeds_entry_total() {
        // Like zip_extraction_end_position_equals_entry_compressed but
        // verify EVERY snapshot: position ≤ total and position is
        // non-decreasing.
        let entry_data: [(&str, &[u8]); 4] = [
            ("a.bin", &[0xAAu8; 5000]),
            ("b.bin", &[0xBBu8; 1000]),
            ("c.bin", &[0xCCu8; 2000]),
            ("d.bin", &[0xDDu8; 8000]),
        ];
        let zip_buf = synthetic_zip(&entry_data);

        let mut archive = zip::ZipArchive::new(std::io::Cursor::new(&zip_buf)).unwrap();
        let expected_total: u64 =
            (0..archive.len()).map(|i| archive.by_index(i).unwrap().compressed_size()).sum();
        drop(archive);

        let positions: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
        let positions_clone = Arc::clone(&positions);
        let cb = move |pos: u64| {
            positions_clone.lock().unwrap().push(pos);
        };
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        extract_zip(&zip_buf, dir.path(), Some(&cb)).expect("extract_zip");

        let recorded = positions.lock().unwrap();
        assert!(!recorded.is_empty(), "should have recorded positions");
        let mut prev = 0u64;
        for (i, &pos) in recorded.iter().enumerate() {
            assert!(pos >= prev, "position decreased at callback {i}: {pos} < {prev}");
            assert!(
                pos <= expected_total,
                "position {pos} exceeds total {expected_total} at callback {i}"
            );
            prev = pos;
        }
        let last_pos = *recorded.last().unwrap();
        assert_eq!(
            last_pos, expected_total,
            "final position {last_pos} should equal total compressed size {expected_total}"
        );
    }

    /// Reads the central-directory sizes of a single-entry ZIP as
    /// `(compressed, decompressed)`. The extraction estimate is defined in
    /// terms of these two numbers, so a test reads them from the archive it
    /// built rather than assuming a compression ratio.
    fn single_entry_zip_sizes(zip_bytes: &[u8]) -> (u64, u64) {
        let mut archive = zip::ZipArchive::new(std::io::Cursor::new(zip_bytes)).unwrap();
        assert_eq!(archive.len(), 1, "fixture should hold exactly one entry");
        let entry = archive.by_index(0).unwrap();
        (entry.compressed_size(), entry.size())
    }

    /// Extracts `zip_bytes` into a throwaway artifact dir and returns every
    /// position the extraction callback reported, in order.
    fn extract_zip_positions(zip_bytes: &[u8]) -> Vec<u64> {
        let positions: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
        let recorder = Arc::clone(&positions);
        let cb = move |pos: u64| {
            recorder.lock().unwrap().push(pos);
        };
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        extract_zip(zip_bytes, dir.path(), Some(&cb)).expect("extract_zip");
        positions.lock().unwrap().clone()
    }

    /// Upper bound on what a single read of a ZIP entry can hand back.
    /// `extract_zip` reads each entry through a buffer of this size, so one
    /// read advances the decompressed byte count by no more than this. A
    /// change to that read buffer widens the bound derived from it.
    const ZIP_READ_BUFFER_MAX: u64 = 65_536;

    /// The endpoint of the ZIP proportional estimate is exact, not rounded.
    /// Once an entry has been read to its end the estimate reports that
    /// entry's compressed size to the byte. The closing callback fires the
    /// cumulative total rather than the estimate, so a truncated estimate
    /// would leave the final reported value correct while the estimate itself
    /// fell short. Pinning the second-to-last position is what separates the
    /// two.
    #[test]
    fn zip_proportional_estimate_is_exact_at_the_entry_endpoint() {
        let payload = pseudo_random_buffer(200_000);
        let zip_bytes = synthetic_zip(&[("large.bin", payload.as_slice())]);
        let (entry_compressed, _entry_decompressed) = single_entry_zip_sizes(&zip_bytes);

        let positions = extract_zip_positions(&zip_bytes);

        // 200 KB through a 64 KiB read buffer takes at least four reads, so
        // the estimate reports at least four in-entry positions before the
        // single closing callback.
        assert!(
            positions.len() >= 5,
            "expected in-entry estimates ahead of the closing callback, got {} positions",
            positions.len()
        );

        let last_estimate = positions[positions.len() - 2];
        assert_eq!(
            last_estimate, entry_compressed,
            "the last in-entry estimate should land exactly on the entry's compressed size"
        );
        assert_eq!(
            positions[positions.len() - 1],
            entry_compressed,
            "the closing callback should report the same entry total"
        );
    }

    /// Mid-entry, the ZIP estimate is a projection of the bytes written so far
    /// through the entry's own compression ratio, and that bounds it two ways:
    /// it stays under the entry's compressed cost until the entry finishes,
    /// and between two consecutive reports it advances by no more than one
    /// read buffer's worth of that ratio. Both bounds hold whatever chunk
    /// sizes the reader returns, so the test pins the ratio the estimate walks
    /// at rather than the reads that produced it.
    ///
    /// How far the estimate sits from the compressed bytes the deflate
    /// decoder really consumed cannot be asserted at all: the zip crate
    /// exposes no count of consumed compressed bytes, so there is no ground
    /// truth to compare against.
    #[test]
    fn zip_proportional_estimate_advances_at_the_uniform_ratio_mid_entry() {
        // Compressible content keeps the entry ratio far below one, so an
        // estimate that tracked decompressed bytes instead of the ratio would
        // overshoot both bounds here.
        let payload = vec![0xAAu8; 200_000];
        let zip_bytes = synthetic_zip(&[("large.bin", payload.as_slice())]);
        let (entry_compressed, entry_decompressed) = single_entry_zip_sizes(&zip_bytes);

        let positions = extract_zip_positions(&zip_bytes);
        assert!(
            positions.len() >= 3,
            "expected the closing callback and an exact estimate ahead of it, got {} positions: \
             {positions:?}",
            positions.len()
        );
        // The last two positions are the exact endpoint estimate and the
        // closing callback. Everything before them was reported while the
        // entry was still being read.
        let mid_entry = &positions[..positions.len() - 2];

        assert!(
            !mid_entry.is_empty(),
            "the fixture needs a multi-read entry to report a mid-entry position, got {positions:?}"
        );

        // Truncating division can carry one unit between the two terms of a
        // difference, hence the +1.
        let max_step = ZIP_READ_BUFFER_MAX * entry_compressed / entry_decompressed + 1;

        for (i, &value) in mid_entry.iter().enumerate() {
            assert!(
                value < entry_compressed,
                "mid-entry position {value} (index {i}) already reached the entry total \
                 {entry_compressed} before the entry finished"
            );
            if i > 0 {
                let previous = mid_entry[i - 1];
                assert!(
                    value >= previous,
                    "mid-entry position went backwards (index {i}): {value} < {previous}"
                );
                let step = value - previous;
                assert!(
                    step <= max_step,
                    "mid-entry position advanced by {step} (index {i}), more than one read of \
                     {ZIP_READ_BUFFER_MAX} bytes projected at ratio \
                     {entry_compressed}/{entry_decompressed} allows ({max_step})"
                );
            }
        }
    }

    #[test]
    fn compress_budget_total_matches_output_size() {
        // Pack a directory with known files. The callback fires at content
        // byte position (sum of uncompressed file payloads), not at the
        // final ZIP output size (central directory + EOCD overhead is
        // appended after all entries). Verify that the final position equals
        // the total uncompressed content size.
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let content_sizes: [(&str, usize); 3] =
            [("large.bin", 100_000), ("small.bin", 500), ("nested/data.bin", 2000)];
        for (path, size) in &content_sizes {
            let full_path = dir.path().join(path);
            if let Some(parent) = full_path.parent() {
                std::fs::create_dir_all(parent).unwrap();
            }
            std::fs::write(full_path, vec![0x11u8; *size]).unwrap();
        }
        let expected_uncompressed_total: u64 = content_sizes.iter().map(|(_, s)| *s as u64).sum();

        let positions: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
        let positions_clone = Arc::clone(&positions);
        let cb = move |pos: u64| {
            positions_clone.lock().unwrap().push(pos);
        };

        let _zip_bytes =
            pack_directory_to_uncompressed_zip_bytes(dir.path(), 0, Some(&cb)).unwrap();

        let recorded = positions.lock().unwrap();
        assert!(!recorded.is_empty(), "should have recorded at least one position");
        let last_pos = *recorded.last().unwrap();
        assert_eq!(
            last_pos, expected_uncompressed_total,
            "final callback position {last_pos} should equal uncompressed total {expected_uncompressed_total} (ZIP central directory overhead is not tracked)"
        );
    }

    #[test]
    fn compress_monotonic_non_decreasing() {
        // Verify position never decreases during packing.
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        std::fs::write(dir.path().join("a.bin"), [0xAAu8; 10_000]).unwrap();
        std::fs::write(dir.path().join("b.bin"), vec![0xBBu8; 20_000]).unwrap();
        std::fs::write(dir.path().join("c.bin"), vec![0xCCu8; 30_000]).unwrap();

        let positions: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
        let positions_clone = Arc::clone(&positions);
        let cb = move |pos: u64| {
            positions_clone.lock().unwrap().push(pos);
        };

        let _zip_bytes =
            pack_directory_to_uncompressed_zip_bytes(dir.path(), 0, Some(&cb)).unwrap();

        let recorded = positions.lock().unwrap();
        assert!(!recorded.is_empty(), "should have recorded at least one position");
        let mut prev = 0u64;
        for (i, &pos) in recorded.iter().enumerate() {
            assert!(pos >= prev, "position decreased at callback {i}: {pos} < {prev}");
            prev = pos;
        }
    }

    // ── estimate_uncompressed_size ─────────────────────────────────

    #[test]
    fn estimate_uncompressed_size_zip_sums_decompressed_sizes() {
        let entries: [(&str, &[u8]); 3] =
            [("a.bin", &[0u8; 100]), ("b.bin", &[1u8; 200]), ("c.bin", &[2u8; 300])];
        let zip = synthetic_zip(&entries);
        // Total uncompressed: 100 + 200 + 300 = 600
        let estimate = estimate_uncompressed_size(&zip, Some("zip"));
        assert_eq!(estimate, 600, "ZIP estimate should sum uncompressed sizes");
    }

    #[test]
    fn estimate_uncompressed_size_tar_gz_uses_isize() {
        let entries: [(&str, &[u8]); 1] = [("x.bin", &[0u8; 1000])];
        let tgz = synthetic_tar_gz(&entries);
        let estimate = estimate_uncompressed_size(&tgz, Some("tar.gz"));
        // ISIZE = total tar stream size (header + padded data + end-of-archive)
        let expected_uncompressed: u64 =
            entries.iter().map(|(_, c)| 512 + (c.len() as u64).div_ceil(512) * 512).sum::<u64>()
                + 1024; // end-of-archive markers
        assert!(
            estimate > tgz.len() as u64,
            "ISIZE estimate ({estimate}) should exceed compressed size ({})",
            tgz.len()
        );
        assert_eq!(
            estimate, expected_uncompressed,
            "ISIZE should match expected tar stream size ({expected_uncompressed})"
        );
    }

    #[test]
    fn estimate_uncompressed_size_tar_xz_uses_index() {
        let entries: [(&str, &[u8]); 1] = [("y.bin", &[0u8; 500])];
        let txz = synthetic_tar_xz(&entries);
        let estimate = estimate_uncompressed_size(&txz, Some("tar.xz"));
        // XZ Index should contain the total uncompressed tar stream size
        let expected_uncompressed: u64 =
            entries.iter().map(|(_, c)| 512 + (c.len() as u64).div_ceil(512) * 512).sum::<u64>()
                + 1024; // end-of-archive markers
        assert!(
            estimate > txz.len() as u64,
            "Index estimate ({estimate}) should exceed compressed size ({})",
            txz.len()
        );
        assert_eq!(
            estimate, expected_uncompressed,
            "Index should match expected tar stream size ({expected_uncompressed})"
        );
    }

    #[test]
    fn estimate_uncompressed_size_none_format_returns_zero() {
        let bytes = [0u8; 100];
        let estimate = estimate_uncompressed_size(&bytes, None);
        assert_eq!(estimate, 0, "None format should return 0");
    }

    #[test]
    fn estimate_uncompressed_size_unknown_format_returns_compressed_size() {
        let bytes = [0u8; 100];
        let estimate = estimate_uncompressed_size(&bytes, Some("exe"));
        assert_eq!(estimate, 100, "unknown format should return compressed len");
    }

    // ── Property-based tests (proptest) ───────────────────────────────

    use super::super::helpers::build_os_conditional_selector;
    use proptest::prelude::*;

    proptest! {
        /// `infer_archive_format` is deterministic: same input always yields
        /// the same output.
        #[test]
        fn infer_archive_format_deterministic(url: String) {
            let result1 = infer_archive_format(&url);
            let result2 = infer_archive_format(&url);
            assert_eq!(result1, result2);
        }

        /// URLs with recognised extensions are classified correctly.
        #[test]
        fn infer_archive_format_known_patterns(
            stem in "[a-z]+",
            ext in ".tar.xz|.tar.gz|.tgz|.zip",
        ) {
            let url = format!("https://example.com/{stem}{ext}");
            let result = infer_archive_format(&url);
            if ext == ".tar.xz" {
                assert_eq!(result, Some(ARCHIVE_TAR_XZ));
            } else if ext == ".tar.gz" || ext == ".tgz" {
                assert_eq!(result, Some(ARCHIVE_TAR_GZ));
            } else if ext == ".zip" {
                assert_eq!(result, Some(ARCHIVE_ZIP));
            }
        }

        /// `build_os_conditional_selector` includes every OS/path entry.
        #[test]
        fn build_os_conditional_selector_roundtrip(
            entries in prop::collection::btree_map(
                "(linux|macos|windows)".prop_filter("OS must be non-empty", |s| !s.is_empty()),
                "[a-zA-Z0-9._/-]+",
                0..4,
            )
        ) {
            let selector = build_os_conditional_selector(&entries);
            if entries.is_empty() {
                assert_eq!(selector, "", "empty map should produce empty string");
                return Ok(());
            }
            for (os, path) in &entries {
                let expected_fragment = format!("{os}/{path}");
                assert!(
                    selector.contains(&expected_fragment),
                    "selector {selector:?} should contain {expected_fragment:?}"
                );
            }
            if entries.len() == 1 {
                // Single entry collapses to a plain path (no template wrapper).
                let (os, path) = entries.iter().next().unwrap();
                assert_eq!(selector, format!("{os}/{path}"));
            } else {
                assert!(selector.starts_with("${context.os == \""),
                    "multi-OS selector should start with template syntax: {selector:?}");
                assert!(selector.ends_with('}'),
                    "multi-OS selector should end with '}}': {selector:?}");
            }
        }

        /// `find_file_relative` symmetry: if a file exists at `{dir}/{path}`,
        /// searching for its name finds it, and the returned relative path
        /// resolves back to the absolute path when joined with root.
        #[test]
        fn find_file_relative_symmetry(
            _dir_name in "[a-z]{1,8}",
            file_name in "[a-z]{1,8}\\.txt",
            depth in 0..3usize,
        ) {
            let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
            let root = dir.path();

            // Build nested directory structure up to `depth`.
            let mut sub_path = std::path::PathBuf::new();
            for i in 0..depth {
                sub_path.push(format!("sub{i}"));
            }
            let target_dir = root.join(&sub_path);
            std::fs::create_dir_all(&target_dir).unwrap();

            // Create the target file.
            let abs_file_path = target_dir.join(&file_name);
            std::fs::write(&abs_file_path, b"content").unwrap();

            // Search for it.
            let found = find_file_relative(root, root, &file_name);
            prop_assert!(found.is_some(),
                "file {:?} at {:?} should be found under {:?}", file_name, abs_file_path, root);

            let relative = found.unwrap();
            // The relative path + root should equal the absolute file path.
            let reconstructed = root.join(&relative);
            prop_assert_eq!(reconstructed.clone(), abs_file_path.clone(),
                "reconstructed path {:?} should match {:?}", reconstructed, abs_file_path);
        }
    }

    // ── ConfigVersionSpec serde round-trip ─────────────────────────────
    //
    // ConfigVersionSpec uses custom Serialize/Deserialize so unit variants
    // serialize to/from JSON strings "latest" and "inherit" (matching the
    // Nickel schema).

    #[test]
    fn config_version_spec_serde_latest() {
        use super::ConfigVersionSpec;
        let json = serde_json::json!("latest");
        let spec: ConfigVersionSpec = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(spec, ConfigVersionSpec::Latest);
        let back = serde_json::to_value(&spec).unwrap();
        assert_eq!(back, json);
    }

    #[test]
    fn config_version_spec_serde_inherit() {
        use super::ConfigVersionSpec;
        let json = serde_json::json!("inherit");
        let spec: ConfigVersionSpec = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(spec, ConfigVersionSpec::Inherit);
        let back = serde_json::to_value(&spec).unwrap();
        assert_eq!(back, json);
    }

    #[test]
    fn config_version_spec_serde_exact_vcs_hash() {
        use super::{ConfigVersionSpec, VersionSpecFields};
        let json = serde_json::json!({"vcs_hash": "abc123"});
        let spec: ConfigVersionSpec = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(
            spec,
            ConfigVersionSpec::Exact(VersionSpecFields {
                vcs_hash: Some("abc123".into()),
                version: None,
                tag: None,
            })
        );
        let back = serde_json::to_value(&spec).unwrap();
        assert_eq!(back, json);
    }

    #[test]
    fn config_version_spec_serde_exact_version() {
        use super::{ConfigVersionSpec, VersionSpecFields};
        let json = serde_json::json!({"version": "1.0"});
        let spec: ConfigVersionSpec = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(
            spec,
            ConfigVersionSpec::Exact(VersionSpecFields {
                vcs_hash: None,
                version: Some("1.0".into()),
                tag: None,
            })
        );
        let back = serde_json::to_value(&spec).unwrap();
        assert_eq!(back, json);
    }

    #[test]
    fn config_version_spec_serde_exact_tag() {
        use super::{ConfigVersionSpec, VersionSpecFields};
        let json = serde_json::json!({"tag": "v1.2.3"});
        let spec: ConfigVersionSpec = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(
            spec,
            ConfigVersionSpec::Exact(VersionSpecFields {
                vcs_hash: None,
                version: None,
                tag: Some("v1.2.3".into()),
            })
        );
        let back = serde_json::to_value(&spec).unwrap();
        assert_eq!(back, json);
    }

    #[test]
    fn config_version_spec_serde_exact_multi_field() {
        use super::{ConfigVersionSpec, VersionSpecFields};
        let json = serde_json::json!({"vcs_hash": "abc", "version": "1.0", "tag": "v1.0"});
        let spec: ConfigVersionSpec = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(
            spec,
            ConfigVersionSpec::Exact(VersionSpecFields {
                vcs_hash: Some("abc".into()),
                version: Some("1.0".into()),
                tag: Some("v1.0".into()),
            })
        );
        let back = serde_json::to_value(&spec).unwrap();
        assert_eq!(back, json);
    }

    #[test]
    fn config_version_spec_serde_empty_object_error() {
        use super::ConfigVersionSpec;
        let json = serde_json::json!({});
        let result: Result<ConfigVersionSpec, _> = serde_json::from_value(json);
        assert!(result.is_err(), "empty object should fail deserialization");
        let err = result.unwrap_err().to_string();
        assert!(err.contains("at least one"), "error should mention at least one field: {err}");
    }

    #[test]
    fn config_version_spec_serde_deny_unknown_fields() {
        use super::ConfigVersionSpec;
        // Unknown field in object causes VersionSpecFields to reject it.
        let json = serde_json::json!({"vcs_hash": "abc", "unknown": "x"});
        let result: Result<ConfigVersionSpec, _> = serde_json::from_value(json);
        assert!(result.is_err(), "unknown fields should be rejected");
    }

    #[test]
    fn source_fetch_concurrency_default_returns_positive() {
        // Clear any env override so we get the default path.
        unsafe {
            std::env::remove_var(ENV_SOURCE_FETCH_CONCURRENCY);
        }
        let concurrency = default_source_fetch_concurrency();
        assert!(concurrency >= 1, "concurrency must be >= 1, got {concurrency}");
    }

    #[test]
    fn source_fetch_concurrency_env_override() {
        unsafe {
            std::env::set_var(ENV_SOURCE_FETCH_CONCURRENCY, "1");
        }
        let concurrency = default_source_fetch_concurrency();
        assert_eq!(concurrency, 1);
        unsafe {
            std::env::remove_var(ENV_SOURCE_FETCH_CONCURRENCY);
        }
    }

    #[test]
    fn source_fetch_concurrency_env_invalid_falls_back() {
        unsafe {
            std::env::set_var(ENV_SOURCE_FETCH_CONCURRENCY, "xyz");
        }
        let concurrency = default_source_fetch_concurrency();
        assert!(concurrency >= 1, "invalid env must fall back to default >= 1, got {concurrency}");
        unsafe {
            std::env::remove_var(ENV_SOURCE_FETCH_CONCURRENCY);
        }
    }

    // ── dual-arch slots, checksum chains, exhaustion ─────────────────

    /// Computes the lowercase hex SHA-256 of `bytes`, the digest format the
    /// sibling `.sha256` endpoints serve.
    fn sha256_hex(bytes: &[u8]) -> String {
        use sha2::Digest;
        format!("{:x}", sha2::Sha256::digest(bytes))
    }

    /// Opens a hermetic user-level cache with a `tools` domain, for the
    /// fetch-phase tests below.
    async fn open_tools_cache() -> (crate::cache_user_level::UserLevelCache, tempfile::TempDir) {
        let dir = mediapm_utils::temp::cache_dir().expect("cache dir");
        let cache = crate::cache::Cache::open(
            dir.path(),
            &[crate::cache::CacheDomainConfig {
                domain: "tools".to_string(),
                index_file_name: "tools.json".to_string(),
                entry_ttl_seconds: crate::cache::ENTRY_TTL_SECONDS,
            }],
        )
        .await
        .expect("cache open");
        (crate::cache_user_level::UserLevelCache::from_cache(cache), dir)
    }

    /// An OS label shared by two sources (a dual-arch slot) keys its content
    /// map as `{os}/{arch}/…` for both payloads, while a single-source OS
    /// slot keeps the flat `{os}/…` shape. The os-keyed executable path
    /// stays os-keyed: the dual-arch slot records the host-arch payload's
    /// path prefixed with the arch segment (so the `{os}/`-prefixed selector
    /// resolves to the payload's real location), and the non-host entry
    /// records nothing.
    #[tokio::test]
    async fn dual_arch_slot_keys_carry_arch_and_exec_path_resolves_host_arch() {
        let arm_zip = synthetic_zip(&[("ffmpeg", b"arm-payload")]);
        let intel_zip = synthetic_zip(&[("ffmpeg", b"intel-payload")]);
        let win_zip = synthetic_zip(&[("ffmpeg.exe", b"win-payload")]);
        let downloaded = DownloadedSources {
            tool_id: "ffmpeg".to_string(),
            entries: vec![
                DownloadedSource {
                    os: "macos".to_string(),
                    arch: "aarch64".to_string(),
                    producer: SourceProducer::Fetch {
                        urls: vec!["https://example.com/macos-aarch64.zip".to_string()],
                    },
                    bytes: arm_zip,
                    expected_size: None,
                    actual_url: None,
                },
                DownloadedSource {
                    os: "macos".to_string(),
                    arch: "x86_64".to_string(),
                    producer: SourceProducer::Fetch {
                        urls: vec!["https://example.com/macos-x86_64.zip".to_string()],
                    },
                    bytes: intel_zip,
                    expected_size: None,
                    actual_url: None,
                },
                DownloadedSource {
                    os: "windows".to_string(),
                    arch: "x86_64".to_string(),
                    producer: SourceProducer::Fetch {
                        urls: vec!["https://example.com/windows.zip".to_string()],
                    },
                    bytes: win_zip,
                    expected_size: None,
                    actual_url: None,
                },
            ],
            cached_count: 0,
            warnings: Vec::new(),
        };

        let cas = InMemoryCas::default();
        let result =
            process_tool_sources(&downloaded, &cas, None).await.expect("process dual-arch slot");

        // Both macOS payloads land under arch-segmented keys; the
        // single-source windows slot keeps its flat key.
        let keys: Vec<&str> = result.content_map.keys().map(String::as_str).collect();
        assert_eq!(
            keys,
            vec!["macos/aarch64/", "macos/x86_64/", "windows/"],
            "dual-arch slot keys carry the arch segment; single-source slots stay flat"
        );

        // os_exec_paths stays os-keyed; the macOS value names the host-arch
        // payload below the arch segment, windows is unchanged.
        assert_eq!(result.os_exec_paths.len(), 2, "one exec path per OS label");
        assert_eq!(
            result.os_exec_paths.get("windows").map(String::as_str),
            Some("ffmpeg.exe"),
            "single-source os slot keeps the unprefixed exec path"
        );
        assert_eq!(
            result.os_exec_paths.get("macos").map(String::as_str),
            Some(format!("{}/ffmpeg", host_arch_label()).as_str()),
            "dual-arch slot resolves the host-arch payload path"
        );
    }

    /// Every container this module can extract is recognized from its header
    /// alone, and a payload carrying no archive magic stays unclassified. The
    /// empty-ZIP end-of-central-directory record is included because a
    /// legitimately empty archive must not read as a binary payload.
    #[test]
    fn infer_archive_format_from_bytes_recognizes_each_container_by_magic() {
        assert_eq!(
            infer_archive_format_from_bytes(&synthetic_zip(&[("a.bin", b"1")])),
            Some(ARCHIVE_ZIP),
            "a ZIP local-file header starts `PK\\x03\\x04`"
        );
        assert_eq!(
            infer_archive_format_from_bytes(&[0x50, 0x4b, 0x05, 0x06]),
            Some(ARCHIVE_ZIP),
            "an empty ZIP is only its end-of-central-directory record"
        );
        assert_eq!(
            infer_archive_format_from_bytes(&synthetic_tar_xz(&[("a.bin", b"1")])),
            Some(ARCHIVE_TAR_XZ),
            "xz payloads start `fd 37 7a 58 5a 00`"
        );
        assert_eq!(
            infer_archive_format_from_bytes(&synthetic_tar_gz(&[("a.bin", b"1")])),
            Some(ARCHIVE_TAR_GZ),
            "gzip payloads start `1f 8b`"
        );
        assert_eq!(
            infer_archive_format_from_bytes(&[0x7f, b'E', b'L', b'F']),
            None,
            "an ELF payload is a binary, not an archive"
        );
        assert_eq!(infer_archive_format_from_bytes(b"<html>error</html>"), None);
        assert_eq!(infer_archive_format_from_bytes(&[]), None);
    }

    /// A source's format and binary filename follow the URL that served it,
    /// not the head of the candidate chain it was resolved from, and bytes
    /// matching a container outrank any URL.
    #[test]
    fn resolve_format_and_filename_prefers_the_served_url() {
        let jellyfin = "https://repo.jellyfin.org/files/ffmpeg/macos/latest-8.x/arm64/jellyfin-ffmpeg_8.1.3-1_portable_macarm64-gpl.tar.xz";
        let fetch = |urls: Vec<&str>, actual_url: Option<&str>| DownloadedSource {
            os: "macos".to_string(),
            arch: "aarch64".to_string(),
            producer: SourceProducer::Fetch {
                urls: urls.into_iter().map(str::to_string).collect(),
            },
            bytes: Vec::new(),
            expected_size: None,
            actual_url: actual_url.map(str::to_string),
        };

        let served_fallback = fetch(
            vec![
                "https://ffmpeg.martin-riedl.de/redirect/latest/macos/arm64/snapshot/ffmpeg.zip",
                jellyfin,
            ],
            Some(jellyfin),
        );
        assert_eq!(
            resolve_format_and_filename(&served_fallback, "ffmpeg").0,
            Some(ARCHIVE_TAR_XZ),
            "the served URL names the container that arrived"
        );

        let declared_only = fetch(
            vec!["https://ffmpeg.martin-riedl.de/redirect/latest/macos/arm64/snapshot/ffmpeg.zip"],
            None,
        );
        assert_eq!(
            resolve_format_and_filename(&declared_only, "ffmpeg").0,
            Some(ARCHIVE_ZIP),
            "with no served URL recorded, the first candidate is all there is"
        );

        let mut bytes_decide = fetch(vec!["https://example.com/tool.zip"], None);
        bytes_decide.bytes = synthetic_tar_xz(&[("tool", b"payload")]);
        assert_eq!(
            resolve_format_and_filename(&bytes_decide, "tool").0,
            Some(ARCHIVE_TAR_XZ),
            "bytes matching a container outrank a contradicting URL extension"
        );

        let binary = fetch(vec!["https://example.com/tools/tool-bin"], None);
        assert_eq!(
            resolve_format_and_filename(&binary, "tool"),
            (None, "tool-bin".to_string()),
            "a binary payload keys under the basename of the URL that served it"
        );

        let with_query = fetch(vec!["https://example.com/download?file=tool"], None);
        assert_eq!(
            resolve_format_and_filename(&with_query, "tool"),
            (None, "download".to_string()),
            "a query string is not part of the filename, matching how the extension is read"
        );
    }

    /// A source whose candidate chain mixes container formats must be
    /// processed as the format its bytes actually are, not as the format its
    /// first declared candidate advertises. The ffmpeg macOS chain declares a
    /// martin-riedl `.zip` permalink first and a jellyfin `.tar.xz` last, so a
    /// martin-riedl outage legitimately lands xz bytes on a source described by
    /// a `.zip` URL; reading the format off the declared URL opened those bytes
    /// as a ZIP and failed with `ZIP open error: invalid Zip archive: Could not
    /// find EOCD` before any payload was imported. This test fails that way on
    /// the pre-fix code and passes only once the served bytes decide the
    /// format.
    #[tokio::test]
    async fn mixed_format_chain_processes_the_format_its_bytes_are() {
        let arm = synthetic_tar_xz(&[("ffmpeg", b"arm-payload")]);
        let intel = synthetic_tar_xz(&[("ffmpeg", b"intel-payload")]);
        let downloaded = DownloadedSources {
            tool_id: "ffmpeg".to_string(),
            entries: vec![
                DownloadedSource {
                    os: "macos".to_string(),
                    arch: "aarch64".to_string(),
                    producer: SourceProducer::Fetch {
                        urls: vec![
                            "https://ffmpeg.martin-riedl.de/redirect/latest/macos/arm64/snapshot/ffmpeg.zip"
                                .to_string(),
                            "https://repo.jellyfin.org/files/ffmpeg/macos/latest-8.x/arm64/jellyfin-ffmpeg_8.1.3-1_portable_macarm64-gpl.tar.xz".to_string(),
                        ],
                    },
                    bytes: arm,
                    expected_size: None,
                    actual_url: Some(
                        "https://repo.jellyfin.org/files/ffmpeg/macos/latest-8.x/arm64/jellyfin-ffmpeg_8.1.3-1_portable_macarm64-gpl.tar.xz"
                            .to_string(),
                    ),
                },
                DownloadedSource {
                    os: "macos".to_string(),
                    arch: "x86_64".to_string(),
                    producer: SourceProducer::Fetch {
                        urls: vec![
                            "https://ffmpeg.martin-riedl.de/redirect/latest/macos/amd64/snapshot/ffmpeg.zip"
                                .to_string(),
                            "https://repo.jellyfin.org/files/ffmpeg/macos/latest-8.x/x86_64/jellyfin-ffmpeg_8.1.3-1_portable_mac64-gpl.tar.xz".to_string(),
                        ],
                    },
                    bytes: intel,
                    expected_size: None,
                    actual_url: Some(
                        "https://repo.jellyfin.org/files/ffmpeg/macos/latest-8.x/x86_64/jellyfin-ffmpeg_8.1.3-1_portable_mac64-gpl.tar.xz"
                            .to_string(),
                    ),
                },
            ],
            cached_count: 0,
            warnings: Vec::new(),
        };

        let cas = InMemoryCas::default();
        let result = process_tool_sources(&downloaded, &cas, None)
            .await
            .expect("a mixed-format chain must be processed as the format its bytes are");

        let keys: Vec<&str> = result.content_map.keys().map(String::as_str).collect();
        assert_eq!(
            keys,
            vec!["macos/aarch64/", "macos/x86_64/"],
            "both macOS payloads land under arch-segmented keys"
        );
        for (key, expected) in
            [("macos/aarch64/", b"arm-payload".as_slice()), ("macos/x86_64/", b"intel-payload")]
        {
            let hash = result
                .content_map
                .get(key)
                .unwrap_or_else(|| panic!("content_map must key '{key}'"));
            let hash = hash.parse::<Hash>().expect("content_map hash must be a valid hash string");
            let stored = cas.get(hash).await.expect("the imported payload must be in the CAS");
            // Each arch's content is stored as the repacked archive, so the
            // payload has to be read back out of the entry it was packed into.
            let mut archive = zip::ZipArchive::new(std::io::Cursor::new(stored.as_ref()))
                .expect("each processed source must be stored as a ZIP payload");
            let mut entry = archive.by_name("ffmpeg").expect("the archive must carry the binary");
            let mut payload = Vec::new();
            std::io::Read::read_to_end(&mut entry, &mut payload)
                .expect("the packed binary must be readable");
            assert_eq!(
                payload, expected,
                "'{key}' must hold its own arch's payload, not the other arch's"
            );
        }
        assert_eq!(
            result.os_exec_paths.get("macos").map(String::as_str),
            Some(format!("{}/ffmpeg", host_arch_label()).as_str()),
            "the extracted payload's exec path resolves below the host-arch segment"
        );
    }

    /// An OS label carrying more than one source that ALL share one
    /// architecture is rejected outright: those sources would resolve to
    /// identical flat content-map keys and silently overwrite one another.
    /// The distinct-arch gate refuses them with tool + OS context instead.
    #[tokio::test]
    async fn process_rejects_same_os_same_arch_sources() {
        let zip_a = synthetic_zip(&[("ffmpeg", b"payload-a")]);
        let zip_b = synthetic_zip(&[("ffmpeg", b"payload-b")]);
        let downloaded = DownloadedSources {
            tool_id: "ffmpeg".to_string(),
            entries: vec![
                DownloadedSource {
                    os: "macos".to_string(),
                    arch: "aarch64".to_string(),
                    producer: SourceProducer::Fetch {
                        urls: vec!["https://example.com/macos-a.zip".to_string()],
                    },
                    bytes: zip_a,
                    expected_size: None,
                    actual_url: None,
                },
                DownloadedSource {
                    os: "macos".to_string(),
                    arch: "aarch64".to_string(),
                    producer: SourceProducer::Fetch {
                        urls: vec!["https://example.com/macos-b.zip".to_string()],
                    },
                    bytes: zip_b,
                    expected_size: None,
                    actual_url: None,
                },
            ],
            cached_count: 0,
            warnings: Vec::new(),
        };

        let cas = InMemoryCas::default();
        let error = process_tool_sources(&downloaded, &cas, None)
            .await
            .expect_err("same-arch dupes must fail");
        let message = error.to_string();
        assert!(message.contains("ffmpeg"), "error names the tool: {message}");
        assert!(message.contains("macos"), "error names the OS label: {message}");
        assert!(
            message.contains("architecture"),
            "error explains the distinct-arch requirement: {message}"
        );
    }

    /// A candidate whose downloaded bytes mismatch the endpoint's served
    /// `.sha256` fails and the chain falls through to the next member, whose
    /// bytes win — including when the failing candidate was reached through a
    /// redirect (the cache stays keyed by the candidate that succeeded).
    #[tokio::test]
    async fn sha256_mismatch_fails_over_to_the_next_chain_member() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        // Primary: a permalink that redirects to a versioned download whose
        // bytes mismatch the served checksum.
        Mock::given(method("GET"))
            .and(path("/redirect-a.zip"))
            .respond_with(
                ResponseTemplate::new(302)
                    .insert_header("location", "/dl/1700000000_abc/ffmpeg.zip"),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/dl/1700000000_abc/ffmpeg.zip"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"primary-payload"))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/dl/1700000000_abc/ffmpeg.zip.sha256"))
            .respond_with(ResponseTemplate::new(200).set_body_string(sha256_hex(b"tampered")))
            .mount(&server)
            .await;
        // Fallback: serves bytes with the matching checksum.
        let fallback_body = b"fallback-payload";
        Mock::given(method("GET"))
            .and(path("/fallback.zip"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(fallback_body.to_vec()))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/fallback.zip.sha256"))
            .respond_with(ResponseTemplate::new(200).set_body_string(sha256_hex(fallback_body)))
            .mount(&server)
            .await;

        let primary_url = format!("{}/redirect-a.zip", server.uri());
        let fallback_url = format!("{}/fallback.zip", server.uri());
        let fetch = ResolvedToolFetch {
            tool_id: "ffmpeg".to_string(),
            sources: vec![ResolvedSource {
                os: "macos".to_string(),
                arch: "aarch64".to_string(),
                producer: SourceProducer::Fetch {
                    urls: vec![primary_url.clone(), fallback_url.clone()],
                },
                expected_size: None,
                size_hint_bytes: None,
                identity: None,
            }],
        };

        let (cache, _cache_dir) = open_tools_cache().await;
        let downloaded =
            fetch_tool_sources(&fetch, &cache, "tools", None).await.expect("chain succeeds");

        assert_eq!(
            downloaded.entries[0].bytes, fallback_body,
            "the checksum-verified fallback member's bytes must win"
        );
        assert_eq!(
            downloaded.entries[0].actual_url.as_deref(),
            Some(fallback_url.as_str()),
            "the fallback candidate is the actual URL"
        );
        assert!(
            cache.lookup_bytes("tools", &fallback_url).await.is_some(),
            "the fallback caches under its own candidate URL"
        );
        assert!(
            cache.lookup_bytes("tools", &primary_url).await.is_none(),
            "a checksum-failed candidate must not be cached"
        );
    }

    /// When every chain member fails — one 404, one checksum mismatch — the
    /// error names every endpoint with its per-candidate cause, so a
    /// multi-endpoint outage is diagnosable from the message alone.
    #[tokio::test]
    async fn chain_exhaustion_names_every_endpoint() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        // First endpoint: plain 404 (no mock — wiremock's default).
        // Second endpoint: serves bytes with a mismatching checksum.
        Mock::given(method("GET"))
            .and(path("/bad-sum.zip"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"payload"))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/bad-sum.zip.sha256"))
            .respond_with(ResponseTemplate::new(200).set_body_string(sha256_hex(b"other")))
            .mount(&server)
            .await;

        let missing_url = format!("{}/missing.zip", server.uri());
        let bad_sum_url = format!("{}/bad-sum.zip", server.uri());
        let fetch = ResolvedToolFetch {
            tool_id: "ffmpeg".to_string(),
            sources: vec![ResolvedSource {
                os: "macos".to_string(),
                arch: "aarch64".to_string(),
                producer: SourceProducer::Fetch {
                    urls: vec![missing_url.clone(), bad_sum_url.clone()],
                },
                expected_size: None,
                size_hint_bytes: None,
                identity: None,
            }],
        };

        let (cache, _cache_dir) = open_tools_cache().await;
        let error =
            fetch_tool_sources(&fetch, &cache, "tools", None).await.expect_err("chain exhausted");

        let message = error.to_string();
        assert!(message.contains("all sources exhausted"), "exhaustion phrasing: {message}");
        assert!(message.contains(&missing_url), "names the 404 endpoint: {message}");
        assert!(message.contains("HTTP 404"), "names the 404 cause: {message}");
        assert!(message.contains(&bad_sum_url), "names the checksum endpoint: {message}");
        assert!(message.contains("sha256 mismatch"), "names the mismatch cause: {message}");
    }

    /// A chain served by a later member is not silent: the failure that
    /// preceded it travels with the bytes — named with the tool, its OS and
    /// arch, the failed candidate with its cause, and the candidate that
    /// served — so the sync layer can report it.
    ///
    /// The counterpart of `chain_exhaustion_names_every_endpoint`: that test
    /// pins the error a total outage produces, this one pins the warning a
    /// *covered* outage produces, which used to be indistinguishable from a
    /// clean first-candidate fetch.
    #[tokio::test]
    async fn a_covered_chain_failure_is_reported_with_the_serving_candidate() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        // First endpoint: plain 404 (no mock — wiremock's default).
        // Second endpoint: the payload; no `.sha256` sibling is mounted, so
        // the "verify when served" probe finds none and accepts the bytes.
        Mock::given(method("GET"))
            .and(path("/fallback.zip"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"payload"))
            .mount(&server)
            .await;

        let missing_url = format!("{}/missing.zip", server.uri());
        let fallback_url = format!("{}/fallback.zip", server.uri());
        let fetch = ResolvedToolFetch {
            tool_id: "ffmpeg".to_string(),
            sources: vec![ResolvedSource {
                os: "macos".to_string(),
                arch: "aarch64".to_string(),
                producer: SourceProducer::Fetch {
                    urls: vec![missing_url.clone(), fallback_url.clone()],
                },
                expected_size: None,
                size_hint_bytes: None,
                identity: None,
            }],
        };

        let (cache, _cache_dir) = open_tools_cache().await;
        let downloaded = fetch_tool_sources(&fetch, &cache, "tools", None)
            .await
            .expect("the second candidate serves, so the fetch succeeds");

        assert_eq!(downloaded.entries.len(), 1, "one source, one entry");
        assert_eq!(downloaded.entries[0].bytes.as_slice(), b"payload");
        assert_eq!(
            downloaded.warnings.len(),
            1,
            "exactly the covered failure is reported: {:?}",
            downloaded.warnings
        );
        let warning = downloaded.warnings.first().expect("one warning");
        assert!(
            warning.contains("tool ffmpeg os macos (arch aarch64)"),
            "the warning says which tool, OS and arch: {warning}"
        );
        assert!(warning.contains(&missing_url), "names the failed candidate: {warning}");
        assert!(warning.contains("HTTP 404"), "names that candidate's cause: {warning}");
        assert!(
            warning.contains(&fallback_url),
            "names the candidate that served instead: {warning}"
        );
    }
}
