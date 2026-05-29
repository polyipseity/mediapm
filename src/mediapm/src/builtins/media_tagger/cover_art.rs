//! Cover art fetching, caching, and slot artifact helpers.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, bail};
use mediapm_cas::{CasApi, FileSystemCas, Hash};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::time::sleep;

use crate::http_client::shared_http_client;

use super::musicbrainz::{MusicBrainzPayload, musicbrainz_payload_cache_key};
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
#[derive(Debug, Deserialize)]
pub(super) struct CoverArtArchiveResponse {
    /// Artwork entries published for one release/release-group entity.
    #[serde(default)]
    images: Vec<CoverArtArchiveImage>,
}

/// One Cover Art Archive image entry.
#[derive(Debug, Deserialize)]
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
}

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
}

/// One cached row plus freshness metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CachedValue<T> {
    /// Cached payload value.
    pub(super) payload: T,
    /// Whether this row is currently within configured expiry budget.
    pub(super) is_fresh: bool,
}

/// JSONC cache index for media-tagger HTTP response rows.
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

/// One media-tagger cache metadata row persisted inside `media-tagger.jsonc`.
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
    /// Optional JSONC index path under the cache root.
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

    /// Reads cached cover-art endpoint rows for one CAA endpoint URL.
    #[must_use]
    pub(super) async fn read_cover_art_entries(
        &self,
        endpoint: &str,
    ) -> Option<CachedValue<Vec<SelectedCoverArt>>> {
        self.read_json_payload("caa-entries", endpoint).await
    }

    /// Persists cached cover-art endpoint rows for one CAA endpoint URL.
    pub(super) async fn write_cover_art_entries(
        &self,
        endpoint: &str,
        payload: &[SelectedCoverArt],
    ) -> anyhow::Result<()> {
        self.write_json_payload("caa-entries", endpoint, &payload.to_vec()).await
    }

    /// Reads cached cover-art bytes for one artwork URL.
    #[must_use]
    pub(super) async fn read_cover_art_bytes(&self, url: &str) -> Option<CachedValue<Vec<u8>>> {
        self.read_bytes_payload("caa-images", url).await
    }

    /// Persists cached cover-art bytes for one artwork URL.
    pub(super) async fn write_cover_art_bytes(
        &self,
        url: &str,
        payload: &[u8],
    ) -> anyhow::Result<()> {
        self.write_bytes_payload("caa-images", url, payload).await
    }

    /// Reads cached MusicBrainz metadata payloads for one recording/release selection.
    #[must_use]
    pub(super) async fn read_musicbrainz_payload(
        &self,
        recording_mbid: &str,
        release_mbid: Option<&str>,
    ) -> Option<CachedValue<MusicBrainzPayload>> {
        self.read_json_payload(
            "musicbrainz-payloads",
            &musicbrainz_payload_cache_key(recording_mbid, release_mbid),
        )
        .await
    }

    /// Persists cached MusicBrainz metadata payloads for one recording/release selection.
    pub(super) async fn write_musicbrainz_payload(
        &self,
        recording_mbid: &str,
        release_mbid: Option<&str>,
        payload: &MusicBrainzPayload,
    ) -> anyhow::Result<()> {
        self.write_json_payload(
            "musicbrainz-payloads",
            &musicbrainz_payload_cache_key(recording_mbid, release_mbid),
            payload,
        )
        .await
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
            is_fresh: self.expiry.is_fresh(entry.fetched_unix_seconds, now),
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
            is_fresh: self.expiry.is_fresh(entry.fetched_unix_seconds, now),
            payload: bytes.to_vec(),
        })
    }

    /// Writes one JSON payload row to CAS and updates the JSONC index.
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
        let hash =
            cas.put(encoded).await.context("writing media-tagger cached payload bytes to CAS")?;
        self.upsert_index_entry(Self::cache_key(namespace, key), hash)
    }

    /// Writes one raw-bytes payload row to CAS and updates the JSONC index.
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
            .put(payload.to_vec())
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

    /// Upserts one cache metadata row and persists updated index JSONC.
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

    /// Removes one corrupted cache metadata row and persists updated index JSONC.
    pub(super) fn remove_index_entry(&self, cache_key: &str) {
        let Ok(mut index) = self.index.lock() else {
            return;
        };

        if index.entries.remove(cache_key).is_some() {
            let _ = self.write_index_file(&index);
        }
    }

    /// Persists current media-tagger cache index JSONC with replace-on-rename semantics.
    pub(super) fn write_index_file(&self, index: &MediaTaggerCacheIndex) -> anyhow::Result<()> {
        let Some(path) = self.index_path.as_ref() else {
            return Ok(());
        };

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!("creating media-tagger cache directory '{}'", parent.display())
            })?;
        }

        let rendered =
            serde_json::to_string_pretty(index).context("encoding media-tagger cache index")?;
        let temp_path = path.with_extension("jsonc.tmp");
        fs::write(&temp_path, format!("{rendered}\n")).with_context(|| {
            format!("writing temporary media-tagger cache index '{}'", temp_path.display())
        })?;

        if path.exists() {
            let _ = fs::remove_file(path);
        }

        fs::rename(&temp_path, path)
            .with_context(|| format!("replacing media-tagger cache index '{}'", path.display()))
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
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

/// Executes one HTTP GET with retry/backoff for transient failures.
pub(super) async fn http_get_with_retry(
    http_client: &reqwest::Client,
    url: &str,
) -> Result<reqwest::Response, reqwest::Error> {
    let mut backoff_millis = MEDIA_TAGGER_HTTP_RETRY_BASE_DELAY_MILLIS;

    for attempt in 0..MEDIA_TAGGER_HTTP_RETRY_ATTEMPTS {
        match http_client.get(url).send().await {
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
pub(super) async fn collect_musicbrainz_cover_art(
    payload: &MusicBrainzPayload,
    cache: &MediaTaggerHttpCache,
) -> anyhow::Result<Vec<SelectedCoverArt>> {
    let mut unique = BTreeMap::new();

    if let Some(release) = payload.release.as_ref() {
        for path in [
            format!("https://coverartarchive.org/release/{}", release.id),
            release
                .release_group
                .as_ref()
                .map(|group| format!("https://coverartarchive.org/release-group/{}", group.id))
                .unwrap_or_default(),
        ] {
            if path.trim().is_empty() {
                continue;
            }
            for entry in fetch_cover_art_entries(&path, cache).await? {
                unique.entry(entry.url.clone()).or_insert(entry);
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

/// Queries one Cover Art Archive endpoint and returns one selected URL per entry.
pub(super) async fn fetch_cover_art_entries(
    endpoint: &str,
    cache: &MediaTaggerHttpCache,
) -> anyhow::Result<Vec<SelectedCoverArt>> {
    let http_client = shared_http_client()
        .map_err(|error| anyhow::anyhow!("initializing shared HTTP client failed: {error}"))?;

    let cached_entries = cache.read_cover_art_entries(endpoint).await;
    if let Some(cached) = cached_entries.as_ref()
        && cached.is_fresh
    {
        return Ok(cached.payload.clone());
    }

    let stale_or_fresh_cached_payload =
        cached_entries.as_ref().map_or_else(Vec::new, |cached| cached.payload.clone());

    let Ok(response) = http_get_with_retry(http_client, endpoint).await else {
        return Ok(stale_or_fresh_cached_payload.clone());
    };

    let status = response.status();
    if status == reqwest::StatusCode::NOT_FOUND {
        let empty = Vec::<SelectedCoverArt>::new();
        let _ = cache.write_cover_art_entries(endpoint, &empty).await;
        return Ok(empty);
    }

    if status.is_server_error() {
        return Ok(stale_or_fresh_cached_payload.clone());
    }

    if !status.is_success() {
        return Ok(stale_or_fresh_cached_payload.clone());
    }

    let body = response.text().await.context("reading cover-art response body")?;
    let payload = serde_json::from_str::<CoverArtArchiveResponse>(&body)
        .context("decoding cover-art JSON")?;

    let mut entries = Vec::new();
    for image in payload.images {
        let Some(url) = select_highest_quality_cover_url(&image) else {
            continue;
        };

        let types = normalized_cover_art_types(&image);
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

    let _ = cache.write_cover_art_entries(endpoint, &entries).await;
    Ok(entries)
}

/// Returns one normalized highest-quality URL for one CAA image entry.
pub(super) fn select_highest_quality_cover_url(image: &CoverArtArchiveImage) -> Option<String> {
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

    candidates
        .into_iter()
        .max_by(|(left_score, left_url), (right_score, right_url)| {
            left_score.cmp(right_score).then_with(|| left_url.cmp(right_url))
        })
        .map(|(_, url)| url)
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
pub(super) async fn persist_cover_art_slot_artifacts(
    output_path: &Path,
    selected_cover_art: &[SelectedCoverArt],
    slot_count: usize,
    cache: &MediaTaggerHttpCache,
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
        let Some(bytes) = download_cover_art_bytes(http_client, &image.url, cache)
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

/// Downloads one cover-art payload, returning `None` when URL is unreachable.
pub(super) async fn download_cover_art_bytes(
    http_client: &reqwest::Client,
    url: &str,
    cache: &MediaTaggerHttpCache,
) -> anyhow::Result<Option<Vec<u8>>> {
    let cached_bytes = cache.read_cover_art_bytes(url).await;
    if let Some(cached) = cached_bytes.as_ref()
        && cached.is_fresh
    {
        return Ok((!cached.payload.is_empty()).then(|| cached.payload.clone()));
    }

    let stale_cached_payload = cached_bytes
        .as_ref()
        .and_then(|cached| (!cached.payload.is_empty()).then(|| cached.payload.clone()));

    let Ok(response) = http_get_with_retry(http_client, url).await else {
        return Ok(stale_cached_payload);
    };

    let status = response.status();
    if status == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }

    if status.is_server_error() {
        return Ok(stale_cached_payload);
    }

    if !status.is_success() {
        return Ok(stale_cached_payload);
    }

    let bytes = response
        .bytes()
        .await
        .with_context(|| format!("reading cover-art payload bytes from '{url}'"))?;

    if bytes.is_empty() {
        return Ok(stale_cached_payload);
    }

    let payload = bytes.to_vec();
    let _ = cache.write_cover_art_bytes(url, &payload).await;
    Ok(Some(payload))
}
