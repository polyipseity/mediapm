//! Cover art fetching, caching, and slot artifact helpers.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, bail};
use mediapm_cas::{CasApi, CasMaintenanceApi, FileSystemCas, Hash};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::time::{Instant, sleep};

use mediapm_conductor::http::client::shared_http_client;

use super::acoustid::AcoustIdLookupResponse;
use super::musicbrainz::MusicBrainzPayload;
use super::util::{join_unique, normalize_optional_text};
use super::{
    MEDIA_TAGGER_CACHE_INDEX_FILE_NAME, MEDIA_TAGGER_CACHE_INDEX_VERSION,
    MEDIA_TAGGER_HTTP_RETRY_ATTEMPTS, MEDIA_TAGGER_HTTP_RETRY_BASE_DELAY_MILLIS,
    cover_art_slot_flag_member_name, cover_art_slot_image_member_name,
};

/// One selected cover-art payload retained for metadata and attach stages.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct SelectedCoverArt {
    /// Highest-quality URL selected for this artwork entry.
    pub(super) url: String,
    /// Primary normalized kind used for compatibility metadata.
    pub(super) maintype: String,
    /// Ordered normalized kind list for compatibility metadata.
    pub(super) types: Vec<String>,
    /// Optional human comment associated with this artwork entry.
    pub(super) comment: String,
}

/// Cover Art Archive response payload.
///
/// This is the raw archive answer, cached as received: the cache stores
/// what the server said, never our filtered view of it, so filter options
/// can change without a refetch.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct CoverArtArchiveResponse {
    /// Artwork entries published for one release/release-group entity.
    #[serde(default)]
    images: Vec<CoverArtArchiveImage>,
}

/// One Cover Art Archive image entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct CoverArtArchiveImage {
    /// Canonical original-quality artwork URL.
    #[serde(default)]
    pub(super) image: Option<String>,
    /// Optional thumbnail URL map keyed by quality labels.
    #[serde(default)]
    pub(super) thumbnails: BTreeMap<String, String>,
    /// Explicit artwork kind labels.
    #[serde(default)]
    pub(super) types: Vec<String>,
    /// Optional artwork comment text.
    #[serde(default)]
    pub(super) comment: Option<String>,
    /// Optional CAA moderation approval marker.
    #[serde(default)]
    pub(super) approved: Option<bool>,
}

/// Canonical Cover Art Archive type labels recognized by managed filtering.
const CAA_SUPPORTED_TYPES: [&str; 18] = [
    "front",
    "back",
    "booklet",
    "medium",
    "tray",
    "obi",
    "spine",
    "track",
    "liner",
    "sticker",
    "poster",
    "watermark",
    "raw/unedited",
    "matrix/runout",
    "top",
    "bottom",
    "panel",
    "other",
];

/// Parsed CAA image-size preference used for URL selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CaaImageSizePreference {
    /// Prefer the canonical original payload URL when available.
    Full,
    /// Prefer the 1200px thumbnail class.
    Large,
    /// Prefer the 500px thumbnail class.
    Medium,
    /// Prefer the 250px thumbnail class.
    Small,
}

/// Parsed cover-art provider selector used by metadata fetch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CoverArtProvider {
    /// Cover Art Archive release endpoint.
    CaaRelease,
    /// URL-relationship provider (accepted for Picard compatibility).
    UrlRelationships,
    /// Cover Art Archive release-group endpoint.
    CaaReleaseGroup,
    /// Local-files provider (accepted for Picard compatibility).
    LocalFiles,
}

/// Long-lived cache budget for rows that answer near-immutable lookups.
///
/// Downloaded image bytes and AcoustID fingerprint answers rarely change,
/// so their rows outlive the metadata namespaces; see
/// [`MediaTaggerHttpCache::expiry_for_namespace`].
pub(super) const MEDIA_TAGGER_LONG_LIVED_MAX_AGE_SECS: i64 = 30 * 24 * 60 * 60;

/// Cache namespace for MusicBrainz recording/release payloads.
const MUSICBRAINZ_PAYLOADS_NAMESPACE: &str = "musicbrainz-payloads";
/// Cache namespace for raw Cover Art Archive index responses.
const CAA_ENTRIES_NAMESPACE: &str = "caa-entries";
/// Cache namespace for downloaded cover-art image bytes.
const CAA_IMAGES_NAMESPACE: &str = "caa-images";
/// Cache namespace for AcoustID fingerprint lookup answers.
const ACOUSTID_LOOKUPS_NAMESPACE: &str = "acoustid-lookups";

/// Cache-expiry policy used by media-tagger HTTP response caches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CacheExpiryPolicy {
    /// Cache rows never expire by age.
    Never,
    /// Cache rows expire after this many seconds.
    Seconds(u64),
}

impl CacheExpiryPolicy {
    /// Resolves one cache-expiry policy from runtime option seconds.
    #[must_use]
    pub(super) fn from_seconds(seconds: i64) -> Self {
        if seconds < 0 { Self::Never } else { Self::Seconds(seconds.unsigned_abs()) }
    }

    /// Returns true when one cached row is still fresh at the current time.
    #[must_use]
    pub(super) fn is_fresh(self, fetched_unix_seconds: u64, now_unix_seconds: u64) -> bool {
        match self {
            Self::Never => true,
            Self::Seconds(max_age_seconds) => {
                now_unix_seconds.saturating_sub(fetched_unix_seconds) <= max_age_seconds
            }
        }
    }

    /// Returns the longer-lived of two policies; [`Self::Never`] wins outright.
    #[must_use]
    pub(super) fn longer(self, other: Self) -> Self {
        match (self, other) {
            (Self::Never, _) | (_, Self::Never) => Self::Never,
            (Self::Seconds(left), Self::Seconds(right)) => Self::Seconds(left.max(right)),
        }
    }
}

/// One cached row plus freshness metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CachedValue<T> {
    /// Cached payload value.
    pub(super) payload: T,
    /// Whether this row is currently within configured expiry budget.
    pub(super) is_fresh: bool,
}

/// JSON cache index for media-tagger HTTP response rows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct MediaTaggerCacheIndex {
    /// Envelope version marker.
    version: u32,
    /// Cache metadata rows keyed by logical cache namespace + lookup key.
    #[serde(default)]
    entries: BTreeMap<String, MediaTaggerCacheIndexEntry>,
}

impl Default for MediaTaggerCacheIndex {
    fn default() -> Self {
        Self { version: MEDIA_TAGGER_CACHE_INDEX_VERSION, entries: BTreeMap::new() }
    }
}

/// One media-tagger cache metadata row persisted inside `media-tagger.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct MediaTaggerCacheIndexEntry {
    /// CAS hash text pointing at cached payload bytes under `store/`.
    hash: String,
    /// Time when payload was fetched/written (Unix seconds).
    fetched_unix_seconds: u64,
}

/// Persistent on-disk HTTP cache used by media-tagger metadata helpers.
#[derive(Debug, Clone)]
pub(super) struct MediaTaggerHttpCache {
    /// Optional CAS storage directory under the cache root.
    store_dir: Option<PathBuf>,
    /// Optional JSON index path under the cache root.
    index_path: Option<PathBuf>,
    /// In-memory cache index rows guarded for concurrent access.
    index: Arc<Mutex<MediaTaggerCacheIndex>>,
    /// Expiry policy applied to cached rows.
    expiry: CacheExpiryPolicy,
}

impl MediaTaggerHttpCache {
    /// Creates one cache policy object from optional directory + expiry config.
    #[must_use]
    pub(super) fn new(root_dir: Option<PathBuf>, expiry: CacheExpiryPolicy) -> Self {
        let normalized_root =
            root_dir.and_then(|path| if path.as_os_str().is_empty() { None } else { Some(path) });

        let store_dir = normalized_root.as_ref().map(|path| path.join("store"));
        let index_path =
            normalized_root.as_ref().map(|path| path.join(MEDIA_TAGGER_CACHE_INDEX_FILE_NAME));
        let index = index_path.as_deref().map(load_media_tagger_cache_index).unwrap_or_default();

        Self { store_dir, index_path, index: Arc::new(Mutex::new(index)), expiry }
    }

    /// Expiry policy governing one cache namespace.
    ///
    /// This is the single source of truth for how long rows of each
    /// namespace live: metadata namespaces follow the configured
    /// `cache_expiry_seconds`; image bytes and AcoustID answers live at
    /// least [`MEDIA_TAGGER_LONG_LIVED_MAX_AGE_SECS`], or the configured
    /// base when that is longer. A never-expire base wins everywhere.
    fn expiry_for_namespace(&self, namespace: &str) -> CacheExpiryPolicy {
        match namespace {
            CAA_IMAGES_NAMESPACE | ACOUSTID_LOOKUPS_NAMESPACE => self
                .expiry
                .longer(CacheExpiryPolicy::from_seconds(MEDIA_TAGGER_LONG_LIVED_MAX_AGE_SECS)),
            _ => self.expiry,
        }
    }

    /// Reads the cached raw archive response for one CAA endpoint URL.
    #[must_use]
    pub(super) async fn read_cover_art_entries(
        &self,
        endpoint: &str,
    ) -> Option<CachedValue<CoverArtArchiveResponse>> {
        self.read_json_payload(CAA_ENTRIES_NAMESPACE, endpoint).await
    }

    /// Persists the raw archive response for one CAA endpoint URL.
    pub(super) async fn write_cover_art_entries(
        &self,
        endpoint: &str,
        payload: &CoverArtArchiveResponse,
    ) -> anyhow::Result<()> {
        self.write_json_payload(CAA_ENTRIES_NAMESPACE, endpoint, payload).await
    }

    /// Reads cached cover-art bytes for one artwork URL.
    #[must_use]
    pub(super) async fn read_cover_art_bytes(&self, url: &str) -> Option<CachedValue<Vec<u8>>> {
        self.read_bytes_payload(CAA_IMAGES_NAMESPACE, url).await
    }

    /// Persists cached cover-art bytes for one artwork URL.
    pub(super) async fn write_cover_art_bytes(
        &self,
        url: &str,
        payload: &[u8],
    ) -> anyhow::Result<()> {
        self.write_bytes_payload(CAA_IMAGES_NAMESPACE, url, payload).await
    }

    /// Reads the cached AcoustID lookup answer for one fingerprint/endpoint key.
    ///
    /// The key comes from [`super::acoustid::acoustid_lookup_cache_key`];
    /// this method only pins the namespace.
    #[must_use]
    pub(super) async fn read_acoustid_lookup(
        &self,
        cache_key: &str,
    ) -> Option<CachedValue<AcoustIdLookupResponse>> {
        self.read_json_payload(ACOUSTID_LOOKUPS_NAMESPACE, cache_key).await
    }

    /// Persists one AcoustID lookup answer under its fingerprint/endpoint key.
    pub(super) async fn write_acoustid_lookup(
        &self,
        cache_key: &str,
        payload: &AcoustIdLookupResponse,
    ) -> anyhow::Result<()> {
        self.write_json_payload(ACOUSTID_LOOKUPS_NAMESPACE, cache_key, payload).await
    }

    /// Reads one MusicBrainz payload row by its shape-built cache key.
    ///
    /// The key comes from [`super::musicbrainz::MusicBrainzFetchShape::cache_key`];
    /// this method only pins the namespace.
    #[must_use]
    pub(super) async fn read_musicbrainz_payload(
        &self,
        cache_key: &str,
    ) -> Option<CachedValue<MusicBrainzPayload>> {
        self.read_json_payload(MUSICBRAINZ_PAYLOADS_NAMESPACE, cache_key).await
    }

    /// Persists one MusicBrainz payload row by its shape-built cache key.
    pub(super) async fn write_musicbrainz_payload(
        &self,
        cache_key: &str,
        payload: &MusicBrainzPayload,
    ) -> anyhow::Result<()> {
        self.write_json_payload(MUSICBRAINZ_PAYLOADS_NAMESPACE, cache_key, payload).await
    }

    /// Builds deterministic logical index key for one namespace/key pair.
    #[must_use]
    pub(super) fn cache_key(namespace: &str, key: &str) -> String {
        format!("{namespace}|{key}")
    }

    /// Reads one JSON payload row from CAS and computes freshness metadata.
    #[must_use]
    pub(super) async fn read_json_payload<T: DeserializeOwned>(
        &self,
        namespace: &str,
        key: &str,
    ) -> Option<CachedValue<T>> {
        let cache_key = Self::cache_key(namespace, key);
        let entry = {
            let index = self.index.lock().ok()?;
            index.entries.get(&cache_key).cloned()
        }?;

        let hash = entry.hash.parse::<Hash>().ok()?;
        let cas = self.open_cache_cas().await.ok().flatten()?;
        let Ok(bytes) = cas.get(hash).await else {
            self.remove_index_entry(&cache_key);
            return None;
        };

        let Ok(payload) = serde_json::from_slice::<T>(&bytes) else {
            self.remove_index_entry(&cache_key);
            return None;
        };

        let now = now_unix_seconds();
        Some(CachedValue {
            is_fresh: self
                .expiry_for_namespace(namespace)
                .is_fresh(entry.fetched_unix_seconds, now),
            payload,
        })
    }

    /// Reads one raw-bytes payload row from CAS and computes freshness metadata.
    #[must_use]
    pub(super) async fn read_bytes_payload(
        &self,
        namespace: &str,
        key: &str,
    ) -> Option<CachedValue<Vec<u8>>> {
        let cache_key = Self::cache_key(namespace, key);
        let entry = {
            let index = self.index.lock().ok()?;
            index.entries.get(&cache_key).cloned()
        }?;

        let hash = entry.hash.parse::<Hash>().ok()?;
        let cas = self.open_cache_cas().await.ok().flatten()?;
        let Ok(bytes) = cas.get(hash).await else {
            self.remove_index_entry(&cache_key);
            return None;
        };

        let now = now_unix_seconds();
        Some(CachedValue {
            is_fresh: self
                .expiry_for_namespace(namespace)
                .is_fresh(entry.fetched_unix_seconds, now),
            payload: bytes.to_vec(),
        })
    }

    /// Writes one JSON payload row to CAS and updates the JSON index.
    pub(super) async fn write_json_payload<T: Serialize>(
        &self,
        namespace: &str,
        key: &str,
        payload: &T,
    ) -> anyhow::Result<()> {
        let Some(cas) = self.open_cache_cas().await? else {
            return Ok(());
        };

        let encoded =
            serde_json::to_vec(payload).context("encoding media-tagger cached payload")?;
        let hash = cas
            .put(encoded.into())
            .await
            .context("writing media-tagger cached payload bytes to CAS")?;
        self.upsert_index_entry(Self::cache_key(namespace, key), hash)
    }

    /// Writes one raw-bytes payload row to CAS and updates the JSON index.
    pub(super) async fn write_bytes_payload(
        &self,
        namespace: &str,
        key: &str,
        payload: &[u8],
    ) -> anyhow::Result<()> {
        let Some(cas) = self.open_cache_cas().await? else {
            return Ok(());
        };

        let hash = cas
            .put(payload.to_vec().into())
            .await
            .context("writing media-tagger cached payload bytes to CAS")?;
        self.upsert_index_entry(Self::cache_key(namespace, key), hash)
    }

    /// Opens the media-tagger cache CAS store from `<cache_root>/store`.
    pub(super) async fn open_cache_cas(&self) -> anyhow::Result<Option<FileSystemCas>> {
        let Some(store_dir) = self.store_dir.as_ref() else {
            return Ok(None);
        };

        let cas = FileSystemCas::open(store_dir).await.with_context(|| {
            format!("opening media-tagger cache CAS store '{}'", store_dir.display())
        })?;

        Ok(Some(cas))
    }

    /// Upserts one cache metadata row and persists updated index JSON.
    pub(super) fn upsert_index_entry(&self, cache_key: String, hash: Hash) -> anyhow::Result<()> {
        let mut index = self
            .index
            .lock()
            .map_err(|_| anyhow::anyhow!("locking media-tagger cache index mutex failed"))?;
        index.entries.insert(
            cache_key,
            MediaTaggerCacheIndexEntry {
                hash: hash.to_string(),
                fetched_unix_seconds: now_unix_seconds(),
            },
        );

        self.write_index_file(&index)
    }

    /// Removes one corrupted cache metadata row and persists updated index JSON.
    pub(super) fn remove_index_entry(&self, cache_key: &str) {
        let Ok(mut index) = self.index.lock() else {
            return;
        };

        if index.entries.remove(cache_key).is_some() {
            let _ = self.write_index_file(&index);
        }
    }

    /// Persists current media-tagger cache index JSON with replace-on-rename semantics.
    pub(super) fn write_index_file(&self, index: &MediaTaggerCacheIndex) -> anyhow::Result<()> {
        let Some(path) = self.index_path.as_ref() else {
            return Ok(());
        };

        let parent = path.parent().ok_or_else(|| {
            anyhow::anyhow!(
                "resolving media-tagger cache index parent directory for '{}' failed",
                path.display()
            )
        })?;
        fs::create_dir_all(parent).with_context(|| {
            format!("creating media-tagger cache directory '{}'", parent.display())
        })?;

        let rendered =
            serde_json::to_string_pretty(index).context("encoding media-tagger cache index")?;
        let mut temp_file = tempfile::NamedTempFile::new_in(parent).with_context(|| {
            format!("creating temporary media-tagger cache index file in '{}'", parent.display())
        })?;
        temp_file.write_all(format!("{rendered}\n").as_bytes()).with_context(|| {
            format!("writing temporary media-tagger cache index '{}'", temp_file.path().display())
        })?;

        // Replace the destination with one rename, with no `remove_file` first.
        //
        // `NamedTempFile::persist` is the atomic replace-the-destination
        // operation on every supported platform: it resolves to `rename(2)` on
        // Unix, and on Windows to `MoveFileExW` with `MOVEFILE_REPLACE_EXISTING`
        // — the same flag `std::fs::rename` passes there, so overwriting an
        // existing index needs no pre-removal on Windows either. The temp file
        // is created in the destination's own directory, so the rename never
        // crosses a filesystem boundary and cannot fail with `EXDEV`. See the
        // `2ed8dfb5` commit message for the full derivation.
        //
        // Removing the destination beforehand opens a window in which
        // `media-tagger.json` does not exist, and the file's only reader,
        // `load_media_tagger_cache_index` (reached solely from
        // `MediaTaggerHttpCache::new`), maps a missing file to an empty
        // index. A cache constructed inside that window starts with no rows,
        // re-downloads what it wants, and its first `upsert_index_entry`
        // persists an index built from that empty map — erasing every row the
        // file held and orphaning their CAS payloads under `store/`. Guarded
        // by `index_rewrite_never_hides_a_live_row_from_a_fresh_cache_reader`.
        temp_file
            .persist(path)
            .map_err(|error| error.error)
            .with_context(|| format!("replacing media-tagger cache index '{}'", path.display()))?;

        Ok(())
    }

    /// Retires every row the read path would already treat as stale or
    /// unreadable, reclaims the blobs only those rows referenced, and
    /// persists each prune once.
    ///
    /// Age is judged by [`Self::expiry_for_namespace`], the same policy the
    /// reads consult: a stale row costs its next use a refetch either way,
    /// so dropping it now loses nothing. Rows whose store blob is missing
    /// go too — nothing can ever read them again. Blob reclamation walks
    /// [`CasMaintenanceApi::list_hashes`], the store's own content listing,
    /// so it depends on no filesystem layout of `store/`.
    ///
    /// Runs once per invocation before any fetch, so no concurrent writer
    /// exists to coordinate with. A disabled cache is a no-op. A poisoned
    /// lock, a failed listing, an unremovable blob, or an index that cannot
    /// be persisted is a hard error: a cache dir broken enough to fail
    /// cleanup will fail the writes meant to use it.
    pub(super) async fn evict_expired(&self) -> anyhow::Result<usize> {
        if self.index_path.is_none() || self.store_dir.is_none() {
            return Ok(0);
        }

        let now = now_unix_seconds();
        let dropped = {
            let mut index = self
                .index
                .lock()
                .map_err(|_| anyhow::anyhow!("locking media-tagger cache index mutex failed"))?;
            let mut dropped = 0usize;
            index.entries.retain(|logical_key, entry| {
                let namespace = logical_key.split_once('|').map_or("", |(namespace, _)| namespace);
                if self.expiry_for_namespace(namespace).is_fresh(entry.fetched_unix_seconds, now) {
                    true
                } else {
                    dropped += 1;
                    false
                }
            });
            if dropped > 0 {
                self.write_index_file(&index)
                    .context("persisting media-tagger cache index after the age prune")?;
            }
            dropped
        };

        let Some(cas) = self.open_cache_cas().await? else {
            return Ok(dropped);
        };
        let present: BTreeSet<String> = cas
            .list_hashes()
            .await
            .context("listing media-tagger cache store blobs for eviction")?
            .into_iter()
            .map(|hash| hash.to_string())
            .collect();

        let (surviving, dangling_dropped) = {
            let mut index = self
                .index
                .lock()
                .map_err(|_| anyhow::anyhow!("locking media-tagger cache index mutex failed"))?;
            let mut surviving = BTreeSet::new();
            let mut dangling_dropped = 0usize;
            index.entries.retain(|_, entry| {
                if present.contains(&entry.hash) {
                    surviving.insert(entry.hash.clone());
                    true
                } else {
                    dangling_dropped += 1;
                    false
                }
            });
            if dangling_dropped > 0 {
                self.write_index_file(&index)
                    .context("persisting media-tagger cache index after the dangling prune")?;
            }
            (surviving, dangling_dropped)
        };

        for orphan in present.difference(&surviving) {
            let hash = orphan
                .parse::<Hash>()
                .with_context(|| format!("parsing media-tagger cache blob hash '{orphan}'"))?;
            cas.delete(hash)
                .await
                .with_context(|| format!("reclaiming media-tagger cache blob '{orphan}'"))?;
        }

        Ok(dropped + dangling_dropped)
    }
}

/// Loads one media-tagger cache index file from disk.
///
/// Missing or malformed index files fall back to an empty index so metadata
/// fetching remains best-effort and non-blocking.
#[must_use]
pub(super) fn load_media_tagger_cache_index(index_path: &Path) -> MediaTaggerCacheIndex {
    let Ok(raw) = fs::read_to_string(index_path) else {
        return MediaTaggerCacheIndex::default();
    };

    let Ok(index) = serde_json::from_str::<MediaTaggerCacheIndex>(&raw) else {
        return MediaTaggerCacheIndex::default();
    };

    if index.version == MEDIA_TAGGER_CACHE_INDEX_VERSION {
        index
    } else {
        MediaTaggerCacheIndex::default()
    }
}

/// Returns current Unix timestamp in seconds.
#[must_use]
pub(super) fn now_unix_seconds() -> u64 {
    mediapm_utils::Timestamp::now().as_unix_secs()
}

/// Per-request ceiling for one Cover Art Archive HTTP call.
///
/// The shared client's own request timeout is the conductor's 30-minute
/// default; a single stalled image download must not hold the tagging step
/// that long on its own.
const MEDIA_TAGGER_COVER_ART_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Wall-clock ceiling for the whole cover-art phase.
///
/// The phase is the sequential image-index lookups plus the image downloads,
/// each of those requests individually capped at
/// [`MEDIA_TAGGER_COVER_ART_REQUEST_TIMEOUT`]. The total is capped here so a
/// slow-but-alive server cannot stall the step across many requests.
pub(super) const COVER_ART_PHASE_BUDGET: Duration = Duration::from_mins(1);

/// Executes one HTTP GET with retry/backoff for transient failures.
///
/// Each attempt carries the cover-art per-request timeout so one request
/// cannot outlive [`MEDIA_TAGGER_COVER_ART_REQUEST_TIMEOUT`] regardless of
/// the shared client's own default.
pub(super) async fn http_get_with_retry(
    http_client: &reqwest::Client,
    url: &str,
) -> Result<reqwest::Response, reqwest::Error> {
    let mut backoff_millis = MEDIA_TAGGER_HTTP_RETRY_BASE_DELAY_MILLIS;

    for attempt in 0..MEDIA_TAGGER_HTTP_RETRY_ATTEMPTS {
        match http_client.get(url).timeout(MEDIA_TAGGER_COVER_ART_REQUEST_TIMEOUT).send().await {
            Ok(response)
                if response.status().is_server_error()
                    && attempt + 1 < MEDIA_TAGGER_HTTP_RETRY_ATTEMPTS =>
            {
                sleep(std::time::Duration::from_millis(backoff_millis)).await;
                backoff_millis = backoff_millis.saturating_mul(2);
            }
            Ok(response) => return Ok(response),
            Err(error) if attempt + 1 < MEDIA_TAGGER_HTTP_RETRY_ATTEMPTS => {
                sleep(std::time::Duration::from_millis(backoff_millis)).await;
                backoff_millis = backoff_millis.saturating_mul(2);
                let _ = error;
            }
            Err(error) => return Err(error),
        }
    }

    unreachable!("retry loop always returns on final attempt")
}

/// Fetches all discoverable cover-art entries for release/release-group entities.
///
/// `caa_endpoint` selects the Cover Art Archive base URL (scheme and host are
/// both honored), and `deadline` caps the whole phase's wall clock.
#[expect(
    clippy::too_many_arguments,
    reason = "each cover-art option mirrors one CAA query parameter; bundling them would obscure the Picard-compatible surface"
)]
pub(super) async fn collect_musicbrainz_cover_art(
    payload: &MusicBrainzPayload,
    cache: &MediaTaggerHttpCache,
    caa_endpoint: &str,
    providers_expression: &str,
    caa_image_types_expression: &str,
    caa_image_size: &str,
    caa_approved_only: bool,
    release_ars: bool,
    deadline: Instant,
) -> anyhow::Result<Vec<SelectedCoverArt>> {
    let providers = parse_cover_art_providers_expression(providers_expression)?;
    let allowed_types = parse_caa_image_types_expression(caa_image_types_expression)?;
    let image_size = parse_caa_image_size_preference(caa_image_size)?;
    let caa_base = caa_endpoint.trim_end_matches('/');

    let mut unique = BTreeMap::new();

    if let Some(release) = payload.release.as_ref() {
        for provider in providers {
            match provider {
                CoverArtProvider::CaaRelease => {
                    let endpoint = format!("{caa_base}/release/{}", release.id);
                    for entry in fetch_cover_art_entries(
                        &endpoint,
                        cache,
                        &allowed_types,
                        image_size,
                        caa_approved_only,
                        deadline,
                    )
                    .await?
                    {
                        unique.entry(entry.url.clone()).or_insert(entry);
                    }
                }
                CoverArtProvider::CaaReleaseGroup => {
                    if let Some(group) = release.release_group.as_ref() {
                        let endpoint = format!("{caa_base}/release-group/{}", group.id);
                        for entry in fetch_cover_art_entries(
                            &endpoint,
                            cache,
                            &allowed_types,
                            image_size,
                            caa_approved_only,
                            deadline,
                        )
                        .await?
                        {
                            unique.entry(entry.url.clone()).or_insert(entry);
                        }
                    }
                }
                CoverArtProvider::UrlRelationships => {
                    let _ = release_ars;
                }
                CoverArtProvider::LocalFiles => {}
            }
        }
    }

    let mut selected = unique.into_values().collect::<Vec<_>>();
    selected.sort_by(|left, right| {
        cover_art_type_priority(&left.maintype)
            .cmp(&cover_art_type_priority(&right.maintype))
            .then_with(|| left.comment.cmp(&right.comment))
            .then_with(|| left.url.cmp(&right.url))
    });

    Ok(selected)
}

/// Filters one raw archive response down to the entries the current options ask for.
///
/// This runs on every read — cached or fresh — so `caa_image_types`,
/// `caa_approved_only`, and the size preference shape the selection without
/// ever entering the cache key. The cache row stays what the server said.
fn select_cover_art_images(
    response: &CoverArtArchiveResponse,
    allowed_types: &BTreeSet<String>,
    image_size: CaaImageSizePreference,
    caa_approved_only: bool,
) -> Vec<SelectedCoverArt> {
    let mut entries = Vec::new();
    for image in &response.images {
        if caa_approved_only && image.approved != Some(true) {
            continue;
        }

        let types = normalized_cover_art_types(image);
        if !types.iter().any(|kind| allowed_types.contains(kind)) {
            continue;
        }

        let Some(url) = select_highest_quality_cover_url_with_preference(image, image_size) else {
            continue;
        };

        let maintype = types
            .iter()
            .min_by_key(|value| cover_art_type_priority(value))
            .cloned()
            .unwrap_or_else(|| "other".to_string());

        entries.push(SelectedCoverArt {
            url,
            maintype,
            types,
            comment: normalize_optional_text(image.comment.as_deref()).unwrap_or_default(),
        });
    }
    entries
}

/// Queries one Cover Art Archive endpoint and returns one selected URL per entry.
///
/// The raw archive response is cached under the endpoint URL and the caller's
/// filters are applied to it on every read, so changing a filter re-derives
/// instead of refetching. The whole request (including retries and body read)
/// runs under `deadline`; exhausting it is an error naming the cover-art
/// phase, never a silent stale-cache substitution.
///
/// # HTTP client policy
///
/// Uses the process-wide shared client from
/// [`shared_http_client`].
/// Connection pooling, TLS reuse, and DNS caching are managed centrally.
/// Do NOT create a [`reqwest::Client`] locally — always use the shared instance.
async fn fetch_cover_art_entries(
    endpoint: &str,
    cache: &MediaTaggerHttpCache,
    allowed_types: &BTreeSet<String>,
    image_size: CaaImageSizePreference,
    caa_approved_only: bool,
    deadline: Instant,
) -> anyhow::Result<Vec<SelectedCoverArt>> {
    let http_client = shared_http_client()
        .map_err(|error| anyhow::anyhow!("initializing shared HTTP client failed: {error}"))?;

    let cached_entries = cache.read_cover_art_entries(endpoint).await;
    if let Some(cached) = cached_entries.as_ref()
        && cached.is_fresh
    {
        return Ok(select_cover_art_images(
            &cached.payload,
            allowed_types,
            image_size,
            caa_approved_only,
        ));
    }

    let (status, body) = tokio::time::timeout_at(deadline, async {
        let response = http_get_with_retry(http_client, endpoint).await?;
        let status = response.status();
        let body = response.text().await.context("reading cover-art response body")?;
        anyhow::Ok((status, body))
    })
    .await
    .map_err(|_| {
        anyhow::anyhow!(
            "cover-art phase budget of {}s exhausted while fetching '{endpoint}'",
            COVER_ART_PHASE_BUDGET.as_secs()
        )
    })
    .with_context(|| format!("fetching cover-art endpoint '{endpoint}'"))??;

    if status == reqwest::StatusCode::NOT_FOUND {
        let empty = CoverArtArchiveResponse::default();
        let _ = cache.write_cover_art_entries(endpoint, &empty).await;
        return Ok(Vec::new());
    }

    if !status.is_success() {
        bail!("cover-art endpoint '{endpoint}' answered HTTP {status}");
    }

    let payload = serde_json::from_str::<CoverArtArchiveResponse>(&body)
        .context("decoding cover-art JSON")?;
    let _ = cache.write_cover_art_entries(endpoint, &payload).await;

    Ok(select_cover_art_images(&payload, allowed_types, image_size, caa_approved_only))
}

/// Returns one normalized highest-quality URL for one CAA image entry.
#[cfg(test)]
pub(super) fn select_highest_quality_cover_url(image: &CoverArtArchiveImage) -> Option<String> {
    select_highest_quality_cover_url_with_preference(image, CaaImageSizePreference::Full)
}

/// Returns one normalized highest-quality URL for one CAA image entry,
/// honoring one parsed size preference.
#[must_use]
fn select_highest_quality_cover_url_with_preference(
    image: &CoverArtArchiveImage,
    preference: CaaImageSizePreference,
) -> Option<String> {
    let mut candidates = Vec::new();

    if let Some(original) = normalize_optional_text(image.image.as_deref()) {
        candidates.push((i32::MAX, original));
    }

    for (key, value) in &image.thumbnails {
        let Some(normalized_url) = normalize_optional_text(Some(value)) else {
            continue;
        };

        let quality_score = key.trim().parse::<i32>().unwrap_or_else(|_| {
            if key.eq_ignore_ascii_case("large") {
                1_200
            } else if key.eq_ignore_ascii_case("small") {
                250
            } else {
                0
            }
        });

        candidates.push((quality_score, normalized_url));
    }

    if candidates.is_empty() {
        return None;
    }

    let pick_exact_key = |keys: &[&str]| {
        keys.iter().find_map(|key| {
            image.thumbnails.get(*key).and_then(|value| normalize_optional_text(Some(value)))
        })
    };

    match preference {
        CaaImageSizePreference::Full => candidates
            .into_iter()
            .max_by(|(left_score, left_url), (right_score, right_url)| {
                left_score.cmp(right_score).then_with(|| left_url.cmp(right_url))
            })
            .map(|(_, url)| url),
        CaaImageSizePreference::Large => pick_exact_key(&["1200", "large"]).or_else(|| {
            candidates
                .into_iter()
                .max_by(|(left_score, left_url), (right_score, right_url)| {
                    left_score.cmp(right_score).then_with(|| left_url.cmp(right_url))
                })
                .map(|(_, url)| url)
        }),
        CaaImageSizePreference::Medium => pick_exact_key(&["500"])
            .or_else(|| pick_exact_key(&["1200", "large", "250", "small"]))
            .or_else(|| {
                candidates
                    .into_iter()
                    .max_by(|(left_score, left_url), (right_score, right_url)| {
                        left_score.cmp(right_score).then_with(|| left_url.cmp(right_url))
                    })
                    .map(|(_, url)| url)
            }),
        CaaImageSizePreference::Small => pick_exact_key(&["250", "small"])
            .or_else(|| pick_exact_key(&["500", "1200", "large"]))
            .or_else(|| {
                candidates
                    .into_iter()
                    .max_by(|(left_score, left_url), (right_score, right_url)| {
                        left_score.cmp(right_score).then_with(|| left_url.cmp(right_url))
                    })
                    .map(|(_, url)| url)
            }),
    }
}

/// Parses one CAA image-size selector string.
fn parse_caa_image_size_preference(raw: &str) -> anyhow::Result<CaaImageSizePreference> {
    let normalized = raw.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return Ok(CaaImageSizePreference::Full);
    }

    match normalized.as_str() {
        "full" | "original" => Ok(CaaImageSizePreference::Full),
        "large" | "1200" => Ok(CaaImageSizePreference::Large),
        "500" | "medium" => Ok(CaaImageSizePreference::Medium),
        "small" | "250" => Ok(CaaImageSizePreference::Small),
        _ => bail!(
            "unsupported caa_image_size '{raw}'; supported values: full, large/1200, medium/500, small/250"
        ),
    }
}

/// Parses one CAA image-type selector expression.
fn parse_caa_image_types_expression(raw: &str) -> anyhow::Result<BTreeSet<String>> {
    let mut include_all = false;
    let mut includes = BTreeSet::new();
    let mut excludes = BTreeSet::new();

    for token in raw.split(',').map(str::trim).filter(|token| !token.is_empty()) {
        let is_exclusion = token.starts_with('-') || token.starts_with('!');
        let normalized =
            token.trim_start_matches('-').trim_start_matches('!').trim().to_ascii_lowercase();

        if normalized.is_empty() {
            continue;
        }

        if matches!(normalized.as_str(), "all" | "*") {
            include_all = true;
            continue;
        }

        if !CAA_SUPPORTED_TYPES.contains(&normalized.as_str()) {
            bail!(
                "unsupported caa_image_types token '{normalized}'; supported types: {}",
                CAA_SUPPORTED_TYPES.join(", ")
            );
        }

        if is_exclusion {
            excludes.insert(normalized);
        } else {
            includes.insert(normalized);
        }
    }

    let mut resolved = if include_all || includes.is_empty() {
        CAA_SUPPORTED_TYPES.iter().map(|value| (*value).to_string()).collect::<BTreeSet<_>>()
    } else {
        includes
    };

    for excluded in excludes {
        resolved.remove(&excluded);
    }

    if resolved.is_empty() {
        bail!("caa_image_types resolved to an empty set; include at least one CAA image type");
    }

    Ok(resolved)
}

/// Parses one cover-art provider selector expression.
fn parse_cover_art_providers_expression(raw: &str) -> anyhow::Result<Vec<CoverArtProvider>> {
    let mut providers = Vec::new();

    for token in raw.split(',').map(str::trim).filter(|token| !token.is_empty()) {
        let normalized = token.to_ascii_lowercase();
        let provider = match normalized.as_str() {
            "caa_release" | "cover_art_archive" | "cover art archive" => {
                CoverArtProvider::CaaRelease
            }
            "url_relationships"
            | "allowed_cover_art_urls"
            | "allowed cover art urls"
            | "urlrels" => CoverArtProvider::UrlRelationships,
            "caa_release_group"
            | "cover_art_archive_release_group"
            | "cover art archive: release group" => CoverArtProvider::CaaReleaseGroup,
            "local_files" | "local files" => CoverArtProvider::LocalFiles,
            _ => {
                bail!(
                    "unsupported ca_providers token '{token}'; supported providers: caa_release, url_relationships, caa_release_group, local_files"
                )
            }
        };

        if !providers.contains(&provider) {
            providers.push(provider);
        }
    }

    if providers.is_empty() {
        Ok(vec![
            CoverArtProvider::CaaRelease,
            CoverArtProvider::UrlRelationships,
            CoverArtProvider::CaaReleaseGroup,
        ])
    } else {
        Ok(providers)
    }
}

/// Normalizes one CAA image-entry kind list into deterministic lowercase tags.
pub(super) fn normalized_cover_art_types(image: &CoverArtArchiveImage) -> Vec<String> {
    let mut ordered = Vec::new();
    let mut seen = BTreeSet::new();

    let mut push_type = |candidate: &str| {
        let normalized = candidate.trim().to_ascii_lowercase();
        if normalized.is_empty() {
            return;
        }
        if seen.insert(normalized.clone()) {
            ordered.push(normalized);
        }
    };

    for kind in &image.types {
        push_type(kind);
    }

    if ordered.is_empty() {
        ordered.push("other".to_string());
    }

    ordered.sort_by_key(|kind| cover_art_type_priority(kind));
    ordered
}

/// Returns stable sort priority for normalized cover-art kind tags.
///
/// Follows the MusicBrainz Cover Art Archive canonical type ordering,
/// supporting all CAA image types: `front`, `back`, `booklet`, `medium`,
/// `tray`, `obi`, `spine`, `track`, `liner`, `sticker`, `poster`,
/// `watermark`, `raw/unedited`, `matrix/runout`, `top`, `bottom`, `panel`,
/// `other`. Unknown types default to priority 100.
#[must_use]
pub(super) fn cover_art_type_priority(kind: &str) -> usize {
    match kind {
        "front" => 0,
        "back" => 1,
        "booklet" => 2,
        "medium" => 3,
        "tray" => 4,
        "obi" => 5,
        "spine" => 6,
        "track" => 7,
        "liner" => 8,
        "sticker" => 9,
        "poster" => 10,
        "watermark" => 11,
        "raw/unedited" => 12,
        "matrix/runout" => 13,
        "top" => 14,
        "bottom" => 15,
        "panel" => 16,
        "other" => 17,
        _ => 100,
    }
}

/// Emits deterministic image-link metadata tags.
///
/// Keep the emitted `coverart_*` key family synchronized with Picard's
/// cover-art metadata usage in
/// `https://github.com/metabrainz/picard/blob/master/picard/coverart/image.py`.
///
/// The image subset passed to this function should already be filtered using
/// [`select_cover_art_for_tag_embedding`] so default embedding behavior stays
/// aligned with Picard's `embed_only_one_front_image` policy.
pub(super) fn insert_musicbrainz_image_tags(
    tags: &mut BTreeMap<String, String>,
    images: &[SelectedCoverArt],
) {
    if images.is_empty() {
        return;
    }

    let primary = images.iter().find(|image| image.maintype == "front").unwrap_or(&images[0]);

    tags.insert("coverart_url".to_string(), primary.url.clone());
    for (index, image) in images.iter().enumerate() {
        tags.insert(format!("coverart_url_{index}"), image.url.clone());
    }
    tags.insert("coverart_maintype".to_string(), primary.maintype.clone());

    let all_types =
        join_unique(images.iter().flat_map(|image| image.types.iter().map(String::as_str)));
    if !all_types.trim().is_empty() {
        tags.insert("coverart_types".to_string(), all_types);
    }

    tags.insert("coverart_comment".to_string(), primary.comment.clone());
}

/// Selects cover-art entries for tag embedding using Picard-compatible policy.
///
/// When `embed_only_one_front_image` is enabled, only the first image with
/// normalized type `front` is kept. If no front image exists, no image is
/// embedded. When disabled, all discovered images are retained, including all
/// supported CAA image types: `front`, `back`, `booklet`, `medium`, `tray`,
/// `obi`, `spine`, `track`, `liner`, `sticker`, `poster`, `watermark`,
/// `raw/unedited`, `matrix/runout`, `top`, `bottom`, `panel`, `other`.
///
/// This mirrors Picard's `ImageList.to_be_saved_to_tags` semantics from
/// `picard/util/imagelist.py` without copying implementation text.
#[must_use]
pub(super) fn select_cover_art_for_tag_embedding(
    images: &[SelectedCoverArt],
    embed_only_one_front_image: bool,
) -> Vec<SelectedCoverArt> {
    if !embed_only_one_front_image {
        return images.to_vec();
    }

    images
        .iter()
        .find(|image| image.types.iter().any(|kind| kind == "front"))
        .cloned()
        .into_iter()
        .collect()
}

/// Writes deterministic cover-art slot members consumed by apply-stage ffmpeg.
///
/// Image downloads run under `deadline`; exhausting it fails the step rather
/// than silently leaving slots empty.
///
/// # HTTP client policy
///
/// Uses the process-wide shared client from
/// [`shared_http_client`].
/// Connection pooling, TLS reuse, and DNS caching are managed centrally.
/// Do NOT create a [`reqwest::Client`] locally — always use the shared instance.
pub(super) async fn persist_cover_art_slot_artifacts(
    output_path: &Path,
    selected_cover_art: &[SelectedCoverArt],
    slot_count: usize,
    cache: &MediaTaggerHttpCache,
    deadline: Instant,
) -> anyhow::Result<()> {
    let artifact_directory = resolve_cover_art_artifact_directory(output_path);
    fs::create_dir_all(&artifact_directory).with_context(|| {
        format!(
            "creating media-tagger cover-art artifact directory '{}'",
            artifact_directory.display()
        )
    })?;

    if selected_cover_art.len() > slot_count {
        bail!(
            "media-tagger resolved {} cover-art entries but only {slot_count} slot(s) are available; increase tools.ffmpeg.max_input_slots to at least {}",
            selected_cover_art.len(),
            selected_cover_art.len() + 1,
        );
    }

    let http_client = shared_http_client()
        .map_err(|error| anyhow::anyhow!("initializing shared HTTP client failed: {error}"))?;

    let mut downloaded = Vec::new();
    for image in selected_cover_art {
        let Some(bytes) = download_cover_art_bytes(http_client, &image.url, cache, deadline)
            .await
            .with_context(|| format!("downloading cover-art payload '{}'", image.url))?
        else {
            continue;
        };

        downloaded.push(bytes);
    }

    if downloaded.len() > slot_count {
        bail!(
            "downloaded {} cover-art entries but only {slot_count} slot(s) are available",
            downloaded.len()
        );
    }

    for slot_index in 1..=slot_count {
        let image_member = cover_art_slot_image_member_name(slot_index);
        let flag_member = cover_art_slot_flag_member_name(slot_index);
        let image_path = artifact_directory.join(&image_member);
        let flag_path = artifact_directory.join(&flag_member);

        if let Some(bytes) = downloaded.get(slot_index - 1) {
            fs::write(&image_path, bytes).with_context(|| {
                format!("writing cover-art slot payload '{}'", image_path.display())
            })?;
            fs::write(&flag_path, b"true").with_context(|| {
                format!("writing cover-art slot flag '{}'", flag_path.display())
            })?;
        } else {
            fs::write(&image_path, &[] as &[u8]).with_context(|| {
                format!("writing empty cover-art slot payload '{}'", image_path.display())
            })?;
            fs::write(&flag_path, &[] as &[u8]).with_context(|| {
                format!("writing empty cover-art slot flag '{}'", flag_path.display())
            })?;
        }
    }

    Ok(())
}

/// Resolves sandbox folder where managed runs expose materialized input files.
pub(super) fn resolve_cover_art_artifact_directory(output_path: &Path) -> PathBuf {
    let metadata_dir = output_path.parent().unwrap_or_else(|| Path::new("."));
    // Cover art files are placed under a dedicated `coverart/` directory in the
    // sandbox root rather than alongside the input media in `inputs/`. This keeps
    // cover art separated from the large input media file and ensures the
    // `sandbox_artifacts` capture for media-tagger can target `coverart/` only,
    // avoiding redundant capture of `inputs/input.media`.
    metadata_dir
        .parent()
        .map_or_else(|| metadata_dir.join("coverart"), |sandbox_root| sandbox_root.join("coverart"))
}

/// Downloads one cover-art payload under the shared phase deadline.
///
/// A `404` is the archive's "no art at this URL" answer and yields `None`;
/// transport failures, server errors, and an empty `200` body are hard
/// errors, never silent stale-cache substitutions. A cached row that is
/// fresh but holds no bytes is a corrupt leftover from an older write path
/// and is refetched rather than serving a silently empty slot.
pub(super) async fn download_cover_art_bytes(
    http_client: &reqwest::Client,
    url: &str,
    cache: &MediaTaggerHttpCache,
    deadline: Instant,
) -> anyhow::Result<Option<Vec<u8>>> {
    let cached_bytes = cache.read_cover_art_bytes(url).await;
    if let Some(cached) = cached_bytes.as_ref()
        && cached.is_fresh
        && !cached.payload.is_empty()
    {
        return Ok(Some(cached.payload.clone()));
    }

    let (status, bytes) = tokio::time::timeout_at(deadline, async {
        let response = http_get_with_retry(http_client, url).await?;
        let status = response.status();
        let bytes = response.bytes().await.context("reading cover-art payload bytes")?;
        anyhow::Ok((status, bytes))
    })
    .await
    .map_err(|_| {
        anyhow::anyhow!(
            "cover-art phase budget of {}s exhausted while downloading '{url}'",
            COVER_ART_PHASE_BUDGET.as_secs()
        )
    })
    .with_context(|| format!("fetching cover-art payload '{url}'"))??;

    if status == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }

    if !status.is_success() {
        bail!("cover-art endpoint '{url}' answered HTTP {status}");
    }

    if bytes.is_empty() {
        bail!("cover-art endpoint '{url}' answered an empty payload");
    }

    let payload = bytes.to_vec();
    let _ = cache.write_cover_art_bytes(url, &payload).await;
    Ok(Some(payload))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use mediapm_cas::CasError;

    use super::*;

    /// Pins the invariant that rewriting the media-tagger cache index never
    /// leaves its destination file absent, because the only reader of that
    /// file treats a missing file as an empty index.
    ///
    /// # Who reads the file
    ///
    /// [`MediaTaggerHttpCache`] loads `media-tagger.json` exactly once, in
    /// [`MediaTaggerHttpCache::new`], through
    /// [`load_media_tagger_cache_index`], which maps both a missing and a
    /// malformed file to [`MediaTaggerCacheIndex::default`] — an index with
    /// no rows. Every later lookup in `read_json_payload` /
    /// `read_bytes_payload` is served from the mutex-guarded in-memory index
    /// that `new` populated, so no *other* file read exists. The reader that
    /// this test drives is therefore precisely a cache instance constructed
    /// while another instance is rewriting the file.
    ///
    /// # Why a lost row is data loss, not a miss
    ///
    /// A cache constructed on an empty index re-downloads every payload it
    /// wants and then calls `upsert_index_entry`, which persists an index
    /// built from that empty in-memory map. The rows the other instance had
    /// already published are erased from the file, and their CAS payloads
    /// under `store/` are left unreferenced. So one construction that lands
    /// inside the rewrite window does not merely cost one cache miss: it
    /// discards every row the cache held.
    ///
    /// # Reachability
    ///
    /// The default `cache_dir` (`<runtime>/cache/media_tagger`) is covered by
    /// the workspace conductor CAS directory lock that `MediaPmService` holds
    /// for its whole lifetime, so a second `mediapm` process on the same
    /// workspace cannot reach a write. The step option `cache_dir` is
    /// user-overridable, though, and two workspaces pointed at one shared
    /// media-tagger cache root hold different CAS locks. The writer below is
    /// that second instance, and it is the same `write_index_file` a real
    /// instance calls from `upsert_index_entry` and `remove_index_entry`.
    ///
    /// # How the overlap is forced
    ///
    /// A rewriting thread performs the index write back to back while a
    /// constructing thread builds a fresh cache for the whole rewrite window,
    /// which is the same "drive the racing operation explicitly instead of
    /// waiting for it" shape as
    /// `index_rewrite_never_hides_a_live_reference_from_the_prune_scan` in
    /// `mediapm-conductor`'s `cache.rs`: no sleeps, and the reader is required
    /// to have sampled while the writer was still running, so the race cannot
    /// pass vacuously.
    #[test]
    fn index_rewrite_never_hides_a_live_row_from_a_fresh_cache_reader() {
        /// Safety valve for the rewriting thread. It stops as soon as the
        /// constructing thread is done, so a reader that fails early cannot
        /// leave the rewriting thread running.
        const MAX_REWRITES: u64 = 4096;
        /// Constructions still taken after the writer finished, so a writer
        /// that outran the reader cannot leave the race unobserved.
        const MIN_READS: usize = 8;

        let root = mediapm_utils::temp::cache_dir().expect("cache dir");
        let writer_cache = MediaTaggerHttpCache::new(
            Some(root.path().to_path_buf()),
            CacheExpiryPolicy::from_seconds(60),
        );

        // The live row lives only in this file, so a reader can miss it only
        // while the file is missing, never because of anything else.
        let live_key =
            MediaTaggerHttpCache::cache_key("caa-images", "https://example.test/live.jpg");
        let live_index = MediaTaggerCacheIndex {
            version: MEDIA_TAGGER_CACHE_INDEX_VERSION,
            entries: BTreeMap::from([(
                live_key.clone(),
                MediaTaggerCacheIndexEntry {
                    hash: Hash::from_bytes([9u8; 32]).to_string(),
                    fetched_unix_seconds: 1,
                },
            )]),
        };
        writer_cache.write_index_file(&live_index).expect("seed live index");

        let stop = AtomicBool::new(false);
        let (missed, reads, reads_during_rewrites) = std::thread::scope(|scope| {
            let stop_flag = &stop;
            let writer_key = live_key.clone();
            let writer = scope.spawn(move || {
                let mut round: u64 = 1;
                while round <= MAX_REWRITES && !stop_flag.load(Ordering::Relaxed) {
                    let mut index = live_index.clone();
                    // Vary the bytes so every round really replaces the
                    // destination instead of rewriting identical content.
                    if let Some(row) = index.entries.get_mut(&writer_key) {
                        row.fetched_unix_seconds = round;
                    }
                    writer_cache.write_index_file(&index).expect("rewrite the cache index");
                    round += 1;
                }
            });

            let mut missed = 0usize;
            let mut reads = 0usize;
            let mut reads_during_rewrites = 0usize;
            while !writer.is_finished() || reads < MIN_READS {
                let reader = MediaTaggerHttpCache::new(
                    Some(root.path().to_path_buf()),
                    CacheExpiryPolicy::from_seconds(60),
                );
                reads += 1;
                if !writer.is_finished() {
                    reads_during_rewrites += 1;
                }
                let loaded = reader.index.lock().expect("reader index lock");
                if !loaded.entries.contains_key(&live_key) {
                    missed += 1;
                }
            }
            stop.store(true, Ordering::Relaxed);
            writer.join().expect("index rewriting thread");
            (missed, reads, reads_during_rewrites)
        });

        assert!(
            reads_during_rewrites > 0,
            "the reader never sampled while an index rewrite was in flight, so the race window was \
             not exercised ({reads} reads, {reads_during_rewrites} of them during a rewrite)"
        );
        assert_eq!(
            missed, 0,
            "an index rewrite must replace its destination atomically: {missed} of {reads} fresh \
             cache constructions could not see the live row, and the first persist from such a \
             cache erases every row the file held"
        );
        let final_reader = MediaTaggerHttpCache::new(
            Some(root.path().to_path_buf()),
            CacheExpiryPolicy::from_seconds(60),
        );
        assert!(
            final_reader
                .index
                .lock()
                .expect("final reader index lock")
                .entries
                .contains_key(&live_key),
            "the live row must be readable once every rewrite has landed"
        );
    }

    /// One Cover Art Archive index response in the wire format the archive
    /// actually serves, with one front image at full quality.
    fn caa_index_json(image_url: &str) -> String {
        format!(
            r#"{{"images":[{{"image":"{image_url}","thumbnails":{{"250":"{image_url}?thumb","1200":"{image_url}?large"}},"types":["Front"],"comment":"","approved":true}}]}}"#
        )
    }

    /// A minimal recording+release payload whose release-group lets the
    /// release-group provider fire; the ids are valid MBID UUIDs because the
    /// entity types parse them.
    fn payload_with_release() -> MusicBrainzPayload {
        serde_json::from_str(
            r#"{
                "recording":{"id":"8f3471b5-7e6a-48da-86a9-c1c07a0f47ae","title":"Song"},
                "release":{
                    "id":"11111111-1111-1111-1111-111111111111",
                    "title":"Album",
                    "release-group":{"id":"22222222-2222-2222-2222-222222222222","title":"Album"}
                }
            }"#,
        )
        .expect("minimal recording+release payload")
    }

    /// The whole cover-art phase runs under one deadline, so a server that
    /// answers slower than the budget has left is cut at the budget instead
    /// of holding the tagging step hostage. This is the stall class a
    /// 30-minute client default used to allow: the request is individually
    /// healthy, but the step cannot wait for it.
    #[tokio::test]
    async fn the_cover_art_phase_budget_cuts_a_slow_endpoint() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .respond_with(
                wiremock::ResponseTemplate::new(200)
                    .set_delay(Duration::from_secs(1))
                    .set_body_raw(
                        caa_index_json("https://example.test/front.jpg"),
                        "application/json",
                    ),
            )
            .mount(&server)
            .await;

        let cache = MediaTaggerHttpCache::new(None, CacheExpiryPolicy::from_seconds(0));
        let allowed = parse_caa_image_types_expression("all").expect("all types");
        let endpoint = format!("{}/release/11111111-1111-1111-1111-111111111111", server.uri());

        let started = Instant::now();
        let result = fetch_cover_art_entries(
            &endpoint,
            &cache,
            &allowed,
            CaaImageSizePreference::Full,
            false,
            started + Duration::from_millis(200),
        )
        .await;
        let elapsed = started.elapsed();

        let error =
            result.expect_err("a response slower than the phase budget must fail the phase");
        let rendered = format!("{error:#}");
        assert!(
            rendered.contains("cover-art phase budget") && rendered.contains(&endpoint),
            "the budget error must name the cover-art phase and the endpoint, got: {rendered}"
        );
        assert!(
            elapsed < Duration::from_millis(900),
            "the deadline must fire at 200 ms, not after the server's 1 s delay: {elapsed:?}"
        );
    }

    /// The same deadline covers image downloads, not just index lookups —
    /// the download is the request that actually fetches megabytes from
    /// archive.org in production.
    #[tokio::test]
    async fn the_phase_budget_cuts_a_slow_image_download() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .respond_with(wiremock::ResponseTemplate::new(200).set_delay(Duration::from_secs(1)))
            .mount(&server)
            .await;

        let cache = MediaTaggerHttpCache::new(None, CacheExpiryPolicy::from_seconds(0));
        let http_client = shared_http_client().expect("shared HTTP client");
        let url = format!("{}/front.jpg", server.uri());

        let started = Instant::now();
        let result = download_cover_art_bytes(
            http_client,
            &url,
            &cache,
            started + Duration::from_millis(200),
        )
        .await;
        let elapsed = started.elapsed();

        let error = result.expect_err("a download slower than the phase budget must fail");
        let rendered = format!("{error:#}");
        assert!(
            rendered.contains("cover-art phase budget") && rendered.contains(&url),
            "the budget error must name the cover-art phase and the image URL, got: {rendered}"
        );
        assert!(
            elapsed < Duration::from_millis(900),
            "the deadline must fire at 200 ms, not after the server's 1 s delay: {elapsed:?}"
        );
    }

    /// A server error is a hard failure, not a silent empty result: the old
    /// code answered `500` with the stale cache (or an empty list), so a
    /// tagging step could quietly ship without cover art while the archive
    /// was down. The retries still burn their cap first, then the status
    /// fails the phase.
    #[tokio::test]
    async fn a_cover_art_server_error_is_an_error_not_a_stale_fallback() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .respond_with(wiremock::ResponseTemplate::new(500))
            .mount(&server)
            .await;

        let cache = MediaTaggerHttpCache::new(None, CacheExpiryPolicy::from_seconds(0));
        let allowed = parse_caa_image_types_expression("all").expect("all types");
        let endpoint = format!("{}/release/11111111-1111-1111-1111-111111111111", server.uri());

        let result = fetch_cover_art_entries(
            &endpoint,
            &cache,
            &allowed,
            CaaImageSizePreference::Full,
            false,
            Instant::now() + Duration::from_secs(30),
        )
        .await;

        let error = result.expect_err("a 500 must fail the fetch, not answer with stale data");
        assert!(
            format!("{error:#}").contains(&endpoint),
            "the error must name the endpoint that failed, got: {error:#}"
        );
    }

    /// A transport failure is a hard failure for the same reason: a closed
    /// port used to be answered with stale cache bytes, hiding an outage
    /// behind outdated cover art. The port is closed, so the failure is
    /// instant and hermetic.
    #[tokio::test]
    async fn a_cover_art_transport_failure_is_an_error_not_a_stale_fallback() {
        let cache = MediaTaggerHttpCache::new(None, CacheExpiryPolicy::from_seconds(0));
        let allowed = parse_caa_image_types_expression("all").expect("all types");
        let endpoint = "http://127.0.0.1:9/release/11111111-1111-1111-1111-111111111111";

        let result = fetch_cover_art_entries(
            endpoint,
            &cache,
            &allowed,
            CaaImageSizePreference::Full,
            false,
            Instant::now() + Duration::from_secs(30),
        )
        .await;

        let error =
            result.expect_err("a connection failure must fail the fetch, not serve stale data");
        assert!(
            format!("{error:#}").contains(endpoint),
            "the error must name the endpoint that failed, got: {error:#}"
        );
    }

    /// A stale row is data, not a fallback: even when the cache holds a
    /// previously fetched payload, a failed fetch must surface the failure
    /// so the step cannot quietly write outdated cover-art links while the
    /// archive is down. The row ages past its one-second budget in real
    /// time (whole unix seconds, hence the 2.5 s sleep), then the endpoint
    /// — a local wiremock — answers 500.
    #[tokio::test]
    async fn a_stale_row_is_not_served_when_the_fetch_fails() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .respond_with(wiremock::ResponseTemplate::new(500))
            .mount(&server)
            .await;

        let root = mediapm_utils::temp::cache_dir().expect("cache directory");
        let cache = MediaTaggerHttpCache::new(
            Some(root.path().to_path_buf()),
            CacheExpiryPolicy::from_seconds(1),
        );
        let endpoint = format!("{}/release/11111111-1111-1111-1111-111111111111", server.uri());
        let stale_response: CoverArtArchiveResponse = serde_json::from_str(
            r#"{"images":[{"image":"https://example.test/stale.jpg","thumbnails":{},"types":["front"],"comment":"","approved":true}]}"#,
        )
        .expect("stale raw response");
        cache.write_cover_art_entries(&endpoint, &stale_response).await.expect("seed stale row");
        tokio::time::sleep(Duration::from_millis(2500)).await;

        let allowed = parse_caa_image_types_expression("all").expect("all types");
        let result = fetch_cover_art_entries(
            &endpoint,
            &cache,
            &allowed,
            CaaImageSizePreference::Full,
            false,
            Instant::now() + Duration::from_secs(30),
        )
        .await;

        let error = result.expect_err("a stale row must not stand in for a failed fetch");
        assert!(
            !format!("{error:#}").contains("stale.jpg"),
            "the error must be the fetch failure itself, not the stale payload, got: {error:#}"
        );
    }

    /// An image download that comes back empty on a `200` is a broken
    /// payload, not a reason to fall back to stale bytes or skip the slot
    /// silently.
    #[tokio::test]
    async fn an_empty_200_image_body_is_an_error() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .respond_with(wiremock::ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let cache = MediaTaggerHttpCache::new(None, CacheExpiryPolicy::from_seconds(0));
        let http_client = shared_http_client().expect("shared HTTP client");
        let url = format!("{}/front.jpg", server.uri());

        let result = download_cover_art_bytes(
            http_client,
            &url,
            &cache,
            Instant::now() + Duration::from_secs(30),
        )
        .await;

        let error = result.expect_err("an empty 200 body must fail the download");
        assert!(
            format!("{error:#}").contains(&url),
            "the error must name the image URL, got: {error:#}"
        );
    }

    /// A `404` is the archive's "no art exists" answer and stays an empty
    /// result — that is the API speaking, not a skip. Both call sites keep
    /// it: the index lookup resolves to zero entries, the image download to
    /// `None`.
    #[tokio::test]
    async fn a_cover_art_404_is_the_no_art_answer() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .respond_with(wiremock::ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let cache = MediaTaggerHttpCache::new(None, CacheExpiryPolicy::from_seconds(0));
        let allowed = parse_caa_image_types_expression("all").expect("all types");
        let endpoint = format!("{}/release/11111111-1111-1111-1111-111111111111", server.uri());

        let entries = fetch_cover_art_entries(
            &endpoint,
            &cache,
            &allowed,
            CaaImageSizePreference::Full,
            false,
            Instant::now() + Duration::from_secs(30),
        )
        .await
        .expect("404 is the no-art answer for an index lookup");
        assert!(entries.is_empty(), "a 404 index must resolve to zero entries");

        let http_client = shared_http_client().expect("shared HTTP client");
        let url = format!("{}/front.jpg", server.uri());
        let bytes = download_cover_art_bytes(
            http_client,
            &url,
            &cache,
            Instant::now() + Duration::from_secs(30),
        )
        .await
        .expect("404 is the no-art answer for an image download");
        assert!(bytes.is_none(), "a 404 image must resolve to no payload");
    }

    /// The `caa-endpoint` option really redirects traffic: one local server
    /// stands in for the archive, both provider paths hit it with the right
    /// MBIDs, and the front image comes back selected. Without this the
    /// option would be cosmetic and hermetic cover-art tests impossible.
    #[tokio::test]
    async fn the_caa_endpoint_selects_where_cover_art_requests_go() {
        let server = wiremock::MockServer::start().await;
        let release_id = "11111111-1111-1111-1111-111111111111";
        let group_id = "22222222-2222-2222-2222-222222222222";
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path(format!("/release/{release_id}")))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_raw(
                caa_index_json("https://example.test/release-front.jpg"),
                "application/json",
            ))
            .mount(&server)
            .await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path(format!("/release-group/{group_id}")))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_raw(
                caa_index_json("https://example.test/group-front.jpg"),
                "application/json",
            ))
            .mount(&server)
            .await;

        let cache = MediaTaggerHttpCache::new(None, CacheExpiryPolicy::from_seconds(0));
        let selected = collect_musicbrainz_cover_art(
            &payload_with_release(),
            &cache,
            &server.uri(),
            "caa_release,caa_release_group",
            "all",
            "full",
            false,
            false,
            Instant::now() + Duration::from_secs(30),
        )
        .await
        .expect("cover-art collection against the mock archive");

        let urls = selected.iter().map(|entry| entry.url.as_str()).collect::<Vec<_>>();
        assert_eq!(
            urls,
            vec!["https://example.test/group-front.jpg", "https://example.test/release-front.jpg"],
            "both provider paths must resolve their front image through the configured endpoint"
        );

        let paths = server
            .received_requests()
            .await
            .expect("request recorder is enabled")
            .iter()
            .map(|request| request.url.path().to_string())
            .collect::<Vec<_>>();
        assert_eq!(
            paths,
            vec![format!("/release/{release_id}"), format!("/release-group/{group_id}")],
            "the configured endpoint must receive both provider requests at their MBID paths"
        );
    }

    /// A `caa-entries` row written by the old post-filter cache reads as a
    /// miss and drops out of the index.
    ///
    /// Upgrade builds ship rows whose cached value was the filtered
    /// selection, not the raw archive response. The new reader must not
    /// serve them — the filters that shaped them are unknown — and the
    /// self-heal path must clear them so they stop paying decode cost on
    /// every lookup.
    #[tokio::test]
    async fn regression_a_post_filter_caa_row_reads_as_a_miss() {
        let root = mediapm_utils::temp::cache_dir().expect("cache directory");
        let cache = MediaTaggerHttpCache::new(
            Some(root.path().to_path_buf()),
            CacheExpiryPolicy::from_seconds(60),
        );
        let endpoint = "https://coverartarchive.org/release/11111111-1111-1111-1111-111111111111";
        let old_row = serde_json::json!([{
            "url": "https://example.test/legacy.jpg",
            "maintype": "front",
            "types": ["front"],
            "comment": ""
        }]);
        cache
            .write_json_payload("caa-entries", endpoint, &old_row)
            .await
            .expect("seed post-filter row");

        let read = cache.read_cover_art_entries(endpoint).await;
        assert!(read.is_none(), "a post-filter row must not satisfy a raw-response read");

        let key = MediaTaggerHttpCache::cache_key("caa-entries", endpoint);
        assert!(
            !cache.index.lock().expect("index lock").entries.contains_key(&key),
            "the undecodable row must drop out of the index so it stops costing a decode per lookup"
        );
    }

    /// One cached raw archive response serves every filter combination.
    ///
    /// The cache stores the archive's answer, not our selection, so changing
    /// `caa_image_types`, `caa_approved_only`, or the size preference
    /// re-derives from the cached response instead of pinning the first
    /// run's choices — and without a second request. The server's request
    /// count is the cache-hit proof; the two differing selections are the
    /// filter-independence proof.
    #[tokio::test]
    async fn one_raw_archive_response_serves_every_filter_combination() {
        let server = wiremock::MockServer::start().await;
        let body = r#"{"images":[
            {"image":"https://example.test/front.jpg","thumbnails":{"250":"https://example.test/front-250.jpg","1200":"https://example.test/front-1200.jpg"},"types":["Front"],"comment":"","approved":true},
            {"image":"https://example.test/back.jpg","thumbnails":{"250":"https://example.test/back-250.jpg"},"types":["Back"],"comment":"","approved":false}
        ]}"#;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .respond_with(
                wiremock::ResponseTemplate::new(200).set_body_raw(body, "application/json"),
            )
            .mount(&server)
            .await;

        let root = mediapm_utils::temp::cache_dir().expect("cache directory");
        let cache = MediaTaggerHttpCache::new(
            Some(root.path().to_path_buf()),
            CacheExpiryPolicy::from_seconds(60),
        );
        let endpoint = format!("{}/release/11111111-1111-1111-1111-111111111111", server.uri());

        let front_only = parse_caa_image_types_expression("front").expect("front type");
        let strict = fetch_cover_art_entries(
            &endpoint,
            &cache,
            &front_only,
            CaaImageSizePreference::Medium,
            true,
            Instant::now() + Duration::from_secs(30),
        )
        .await
        .expect("approved front under strict filters");
        assert_eq!(
            strict.iter().map(|entry| entry.url.as_str()).collect::<Vec<_>>(),
            vec!["https://example.test/front-1200.jpg"],
            "the strict filter keeps only the approved front at its preferred size"
        );

        let everything = parse_caa_image_types_expression("all").expect("all types");
        let loose = fetch_cover_art_entries(
            &endpoint,
            &cache,
            &everything,
            CaaImageSizePreference::Full,
            false,
            Instant::now() + Duration::from_secs(30),
        )
        .await
        .expect("loose filters over the same release");
        assert_eq!(
            loose.iter().map(|entry| entry.url.as_str()).collect::<Vec<_>>(),
            vec!["https://example.test/front.jpg", "https://example.test/back.jpg"],
            "loose filters re-derive both images at full quality from the same cached response"
        );

        let received = server.received_requests().await.expect("request recorder is enabled");
        assert_eq!(
            received.len(),
            1,
            "the second read must hit the cache: filter changes may not cost a refetch"
        );
    }

    /// Back-dates one cache row, standing in for elapsed wall time.
    fn age_index_row(cache: &MediaTaggerHttpCache, namespace: &str, key: &str, age_seconds: u64) {
        let index_key = MediaTaggerHttpCache::cache_key(namespace, key);
        let mut index = cache.index.lock().expect("index lock");
        let entry = index.entries.get_mut(&index_key).expect("row to back-date");
        entry.fetched_unix_seconds = now_unix_seconds().saturating_sub(age_seconds);
    }

    /// Metadata rows age on the configured clock; image bytes outlive them.
    #[tokio::test]
    async fn image_rows_outlive_metadata_rows_on_a_short_base_policy() {
        let root = mediapm_utils::temp::cache_dir().expect("cache directory");
        let cache = MediaTaggerHttpCache::new(
            Some(root.path().to_path_buf()),
            CacheExpiryPolicy::from_seconds(1),
        );
        let one_hour = 60 * 60;

        cache
            .write_bytes_payload("caa-images", "https://example.test/art.jpg", b"jpeg")
            .await
            .expect("seed image row");
        cache
            .write_json_payload(
                "caa-entries",
                "https://example.test/release",
                &serde_json::json!({"images": []}),
            )
            .await
            .expect("seed entries row");
        age_index_row(&cache, "caa-images", "https://example.test/art.jpg", one_hour);
        age_index_row(&cache, "caa-entries", "https://example.test/release", one_hour);

        let images = cache
            .read_bytes_payload("caa-images", "https://example.test/art.jpg")
            .await
            .expect("image row");
        let entries = cache
            .read_json_payload::<serde_json::Value>("caa-entries", "https://example.test/release")
            .await
            .expect("entries row");

        assert!(images.is_fresh, "a one-hour-old image row stays fresh under the long-lived tier");
        assert!(!entries.is_fresh, "a one-hour-old metadata row is stale under a one-second base");
    }

    /// On a day-long base, AcoustID answers still outlive the MusicBrainz rows.
    #[tokio::test]
    async fn acoustid_rows_outlive_musicbrainz_rows_on_a_day_base_policy() {
        let root = mediapm_utils::temp::cache_dir().expect("cache directory");
        let cache = MediaTaggerHttpCache::new(
            Some(root.path().to_path_buf()),
            CacheExpiryPolicy::from_seconds(24 * 60 * 60),
        );
        let twenty_five_hours = 25 * 60 * 60;

        cache
            .write_json_payload(
                "musicbrainz-payloads",
                "recording=old|release=|all_tags=1|ars=0|endpoint=musicbrainz.org",
                &serde_json::json!({"recording": {"id": "old", "title": "old"}}),
            )
            .await
            .expect("seed musicbrainz row");
        cache
            .write_json_payload(
                "acoustid-lookups",
                "fingerprint=old|endpoint=https://api.acoustid.org/v2/lookup",
                &serde_json::json!({"status": "ok"}),
            )
            .await
            .expect("seed acoustid row");
        age_index_row(
            &cache,
            "musicbrainz-payloads",
            "recording=old|release=|all_tags=1|ars=0|endpoint=musicbrainz.org",
            twenty_five_hours,
        );
        age_index_row(
            &cache,
            "acoustid-lookups",
            "fingerprint=old|endpoint=https://api.acoustid.org/v2/lookup",
            twenty_five_hours,
        );

        let musicbrainz = cache
            .read_json_payload::<serde_json::Value>(
                "musicbrainz-payloads",
                "recording=old|release=|all_tags=1|ars=0|endpoint=musicbrainz.org",
            )
            .await
            .expect("musicbrainz row");
        let acoustid = cache
            .read_json_payload::<serde_json::Value>(
                "acoustid-lookups",
                "fingerprint=old|endpoint=https://api.acoustid.org/v2/lookup",
            )
            .await
            .expect("acoustid row");

        assert!(
            !musicbrainz.is_fresh,
            "a twenty-five-hour-old MusicBrainz row is stale on a day-long base"
        );
        assert!(
            acoustid.is_fresh,
            "a twenty-five-hour-old AcoustID answer stays fresh under the long-lived tier"
        );
    }

    /// A never-expire base keeps every namespace fresh, however old the rows.
    #[tokio::test]
    async fn a_never_expire_base_keeps_every_namespace_fresh() {
        let root = mediapm_utils::temp::cache_dir().expect("cache directory");
        let cache = MediaTaggerHttpCache::new(
            Some(root.path().to_path_buf()),
            CacheExpiryPolicy::from_seconds(-1),
        );
        let ten_years = 10 * 365 * 24 * 60 * 60;

        for (namespace, key) in [
            ("musicbrainz-payloads", "recording=ancient|release="),
            ("caa-entries", "https://example.test/ancient"),
            ("caa-images", "https://example.test/ancient.jpg"),
            ("acoustid-lookups", "fingerprint=ancient|endpoint=https://api.acoustid.org/v2/lookup"),
        ] {
            cache
                .write_json_payload(namespace, key, &serde_json::json!({"status": "ok"}))
                .await
                .expect("seed row");
            age_index_row(&cache, namespace, key, ten_years);

            let row =
                cache.read_json_payload::<serde_json::Value>(namespace, key).await.expect("row");
            assert!(row.is_fresh, "{namespace}: a never-expire base must hold for every namespace");
        }
    }

    /// A raised base policy carries the long-lived tier with it.
    #[tokio::test]
    async fn a_raised_base_policy_carries_the_long_lived_tier() {
        let root = mediapm_utils::temp::cache_dir().expect("cache directory");
        let cache = MediaTaggerHttpCache::new(
            Some(root.path().to_path_buf()),
            CacheExpiryPolicy::from_seconds(60 * 24 * 60 * 60),
        );
        let forty_days = 40 * 24 * 60 * 60;

        cache
            .write_bytes_payload("caa-images", "https://example.test/aged.jpg", b"jpeg")
            .await
            .expect("seed image row");
        age_index_row(&cache, "caa-images", "https://example.test/aged.jpg", forty_days);

        let images = cache
            .read_bytes_payload("caa-images", "https://example.test/aged.jpg")
            .await
            .expect("image row");
        assert!(
            images.is_fresh,
            "raising the base past the long-lived constant must raise image-row freshness with it"
        );
    }

    /// Reads one index row's blob hash for store-level assertions.
    fn index_row_hash(cache: &MediaTaggerHttpCache, namespace: &str, key: &str) -> Hash {
        let logical_key = MediaTaggerHttpCache::cache_key(namespace, key);
        cache
            .index
            .lock()
            .expect("index lock")
            .entries
            .get(&logical_key)
            .expect("index row")
            .hash
            .parse()
            .expect("blob hash")
    }

    /// A metadata row past its budget is retired while a long-lived image
    /// row of the same age survives, and only the expired row's blob leaves
    /// the store.
    #[tokio::test]
    async fn eviction_drops_expired_rows_and_reclaims_only_their_blobs() {
        let root = mediapm_utils::temp::cache_dir().expect("cache directory");
        let cache = MediaTaggerHttpCache::new(
            Some(root.path().to_path_buf()),
            CacheExpiryPolicy::from_seconds(1),
        );
        let one_hour = 60 * 60;

        cache
            .write_json_payload(
                "caa-entries",
                "https://example.test/expired",
                &serde_json::json!({"images": []}),
            )
            .await
            .expect("seed expired row");
        cache
            .write_bytes_payload("caa-images", "https://example.test/live.jpg", b"jpeg")
            .await
            .expect("seed live row");
        age_index_row(&cache, "caa-entries", "https://example.test/expired", one_hour);
        age_index_row(&cache, "caa-images", "https://example.test/live.jpg", one_hour);
        let expired_hash = index_row_hash(&cache, "caa-entries", "https://example.test/expired");
        let live_hash = index_row_hash(&cache, "caa-images", "https://example.test/live.jpg");

        let dropped = cache.evict_expired().await.expect("eviction");
        assert_eq!(dropped, 1, "only the expired metadata row is retired");

        let expired_key =
            MediaTaggerHttpCache::cache_key("caa-entries", "https://example.test/expired");
        assert!(
            !cache.index.lock().expect("index lock").entries.contains_key(&expired_key),
            "the expired row must be gone from the index"
        );
        let live = cache
            .read_bytes_payload("caa-images", "https://example.test/live.jpg")
            .await
            .expect("live row");
        assert!(live.is_fresh, "the long-lived image row stays fresh and readable");

        let cas = cache.open_cache_cas().await.expect("cas").expect("store enabled");
        assert!(
            matches!(cas.stat(expired_hash).await, Err(CasError::NotFound(_))),
            "the expired row's blob must be reclaimed"
        );
        assert!(cas.stat(live_hash).await.is_ok(), "the live blob must stay in the store");
    }

    /// A row whose blob vanished behind the index's back drops out, and the
    /// prune persists so a reopened cache does not resurrect it.
    #[tokio::test]
    async fn eviction_drops_a_dangling_row_and_persists_the_prune() {
        let root = mediapm_utils::temp::cache_dir().expect("cache directory");
        let cache = MediaTaggerHttpCache::new(
            Some(root.path().to_path_buf()),
            CacheExpiryPolicy::from_seconds(60),
        );
        let endpoint = "https://example.test/dangling";
        cache
            .write_json_payload("caa-entries", endpoint, &serde_json::json!({"images": []}))
            .await
            .expect("seed row");
        let blob = index_row_hash(&cache, "caa-entries", endpoint);
        let cas = cache.open_cache_cas().await.expect("cas").expect("store enabled");
        cas.delete(blob).await.expect("delete blob behind the index's back");
        drop(cas);

        let dropped = cache.evict_expired().await.expect("eviction");
        assert_eq!(dropped, 1, "a row without a blob can never be read again");

        let reopened = MediaTaggerHttpCache::new(
            Some(root.path().to_path_buf()),
            CacheExpiryPolicy::from_seconds(60),
        );
        let key = MediaTaggerHttpCache::cache_key("caa-entries", endpoint);
        assert!(
            !reopened.index.lock().expect("index lock").entries.contains_key(&key),
            "the prune must persist so a fresh cache does not resurrect the row"
        );
    }

    /// A never-expire base retires no rows, however old.
    #[tokio::test]
    async fn eviction_keeps_every_row_under_a_never_expire_base() {
        let root = mediapm_utils::temp::cache_dir().expect("cache directory");
        let cache = MediaTaggerHttpCache::new(
            Some(root.path().to_path_buf()),
            CacheExpiryPolicy::from_seconds(-1),
        );
        let ten_years = 10 * 365 * 24 * 60 * 60;
        let rows = [
            ("musicbrainz-payloads", "recording=ancient|release="),
            ("caa-entries", "https://example.test/ancient"),
            ("caa-images", "https://example.test/ancient.jpg"),
            ("acoustid-lookups", "fingerprint=ancient|endpoint=https://api.acoustid.org/v2/lookup"),
        ];

        for (namespace, key) in rows {
            cache
                .write_json_payload(namespace, key, &serde_json::json!({"status": "ok"}))
                .await
                .expect("seed row");
            age_index_row(&cache, namespace, key, ten_years);
        }

        let dropped = cache.evict_expired().await.expect("eviction");
        assert_eq!(dropped, 0, "a never-expire base retires no rows");
        let index = cache.index.lock().expect("index lock");
        for (namespace, key) in rows {
            let logical_key = MediaTaggerHttpCache::cache_key(namespace, key);
            assert!(
                index.entries.contains_key(&logical_key),
                "{namespace}: a never-expire row must survive eviction"
            );
        }
    }

    /// A disabled cache retires nothing and touches no disk.
    #[tokio::test]
    async fn eviction_is_a_no_op_on_a_disabled_cache() {
        let cache = MediaTaggerHttpCache::new(None, CacheExpiryPolicy::from_seconds(1));
        let dropped = cache.evict_expired().await.expect("eviction on a disabled cache");
        assert_eq!(dropped, 0, "a disabled cache holds no rows to retire");
    }

    /// A cache dir that cannot persist the prune fails loudly.
    ///
    /// `write_index_file` persists by temp-file-plus-rename, so a read-only
    /// *file* would still accept the rename — the write fails only when the
    /// directory itself refuses new files. The chmod therefore seals the
    /// cache root, and is lifted again before the temp dir cleans up.
    #[cfg(unix)]
    #[tokio::test]
    async fn eviction_fails_loudly_when_the_index_cannot_be_persisted() {
        use std::os::unix::fs::PermissionsExt;

        let root = mediapm_utils::temp::cache_dir().expect("cache directory");
        let cache = MediaTaggerHttpCache::new(
            Some(root.path().to_path_buf()),
            CacheExpiryPolicy::from_seconds(1),
        );
        cache
            .write_json_payload(
                "caa-entries",
                "https://example.test/sealed",
                &serde_json::json!({"images": []}),
            )
            .await
            .expect("seed row");
        age_index_row(&cache, "caa-entries", "https://example.test/sealed", 3600);

        let mut sealed = fs::metadata(root.path()).expect("root metadata").permissions();
        sealed.set_mode(0o555);
        fs::set_permissions(root.path(), sealed).expect("seal cache root");

        let result = cache.evict_expired().await;

        let mut open = fs::metadata(root.path()).expect("root metadata").permissions();
        open.set_mode(0o755);
        fs::set_permissions(root.path(), open).expect("unseal cache root");

        let error = result.expect_err("a sealed cache dir must fail eviction, not skip it");
        assert!(
            format!("{error:#}").contains("media-tagger cache index"),
            "the error must name the failing index persist, got: {error:#}"
        );
    }

    /// The return value counts exactly the rows the index lost.
    #[tokio::test]
    async fn eviction_returns_the_number_of_rows_dropped() {
        let root = mediapm_utils::temp::cache_dir().expect("cache directory");
        let cache = MediaTaggerHttpCache::new(
            Some(root.path().to_path_buf()),
            CacheExpiryPolicy::from_seconds(1),
        );
        let one_hour = 60 * 60;

        for key in ["https://example.test/gone-a", "https://example.test/gone-b"] {
            cache
                .write_json_payload("caa-entries", key, &serde_json::json!({"images": []}))
                .await
                .expect("seed row");
            age_index_row(&cache, "caa-entries", key, one_hour);
        }
        cache
            .write_bytes_payload("caa-images", "https://example.test/stays.jpg", b"jpeg")
            .await
            .expect("seed live row");
        age_index_row(&cache, "caa-images", "https://example.test/stays.jpg", one_hour);

        let dropped = cache.evict_expired().await.expect("eviction");
        assert_eq!(dropped, 2, "the count must equal the rows the index lost");
        assert_eq!(
            cache.index.lock().expect("index lock").entries.len(),
            1,
            "one live row remains after eviction"
        );
    }
}
