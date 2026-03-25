//! MusicBrainz provider adapter.
//!
//! Responsibilities:
//! - enforce request throttling,
//! - maintain local response cache with TTL,
//! - normalize MusicBrainz payloads into provider-domain candidates.

use std::{
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::{fs, time::sleep};

use crate::{
    configuration::config::MusicBrainzPolicy,
    domain::provider::{MusicBrainzQuery, ProviderCandidate, ProviderSearchResult},
    infrastructure::{
        provider::MusicBrainzProvider,
        store::{WorkspacePaths, atomic_write_bytes},
    },
    support::util::now_rfc3339,
};

/// Cached-response envelope stored on disk.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CacheEntry {
    fetched_at: String,
    response: Value,
}

/// Production MusicBrainz HTTP adapter with cache + rate limiting.
pub struct MusicBrainzHttpProvider {
    client: Client,
    policy: MusicBrainzPolicy,
    cache_dir: PathBuf,
    last_request_at: Option<Instant>,
}

impl MusicBrainzHttpProvider {
    /// Create a provider adapter bound to one workspace and policy.
    pub fn new(paths: &WorkspacePaths, policy: &MusicBrainzPolicy) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_millis(policy.timeout_ms))
            .user_agent(policy.user_agent.clone())
            .build()
            .context("failed to build MusicBrainz HTTP client")?;

        let cache_dir = paths.providers_dir.join("musicbrainz").join("cache");
        std::fs::create_dir_all(&cache_dir)?;

        Ok(Self { client, policy: policy.clone(), cache_dir, last_request_at: None })
    }

    /// Compute deterministic cache key for query + effective limit.
    pub fn cache_key_for_query(&self, query: &MusicBrainzQuery) -> Result<String> {
        let expression = query
            .effective_query()
            .ok_or_else(|| anyhow!("musicbrainz query must provide query text or hints"))?;
        let limit = query.limit.unwrap_or(self.policy.max_candidates).max(1);
        Ok(cache_key_from_expression(&expression, limit))
    }

    /// Compute cache path for one key.
    pub fn cache_path_for_key(&self, cache_key: &str) -> PathBuf {
        self.cache_dir.join(format!("{cache_key}.json"))
    }

    fn effective_query_and_limit(&self, query: &MusicBrainzQuery) -> Result<(String, usize)> {
        let expression = query
            .effective_query()
            .ok_or_else(|| anyhow!("musicbrainz query must provide query text or hints"))?;
        let limit = query.limit.unwrap_or(self.policy.max_candidates).max(1);
        Ok((expression, limit))
    }

    async fn read_fresh_cache(&self, cache_path: &Path) -> Result<Option<Value>> {
        if !cache_path.exists() {
            return Ok(None);
        }

        let bytes = fs::read(cache_path).await?;
        let entry: CacheEntry = serde_json::from_slice(&bytes)?;

        let fetched_at = OffsetDateTime::parse(&entry.fetched_at, &Rfc3339)?;
        let age_seconds = (OffsetDateTime::now_utc() - fetched_at).whole_seconds();

        if age_seconds <= self.policy.cache_ttl_seconds as i64 {
            return Ok(Some(entry.response));
        }

        Ok(None)
    }

    async fn write_cache(&self, cache_path: &Path, response: &Value) -> Result<()> {
        let entry = CacheEntry { fetched_at: now_rfc3339()?, response: response.clone() };
        let mut bytes = serde_json::to_vec_pretty(&entry)?;
        bytes.push(b'\n');
        atomic_write_bytes(cache_path, &bytes).await
    }

    async fn enforce_rate_limit(&mut self) {
        let min_interval = Duration::from_millis(self.policy.min_interval_ms);
        if let Some(last_request) = self.last_request_at {
            let elapsed = last_request.elapsed();
            if elapsed < min_interval {
                sleep(min_interval - elapsed).await;
            }
        }

        self.last_request_at = Some(Instant::now());
    }

    async fn request_recordings(&mut self, query_expression: &str, limit: usize) -> Result<Value> {
        self.enforce_rate_limit().await;

        let endpoint = format!("{}/recording", self.policy.base_url.trim_end_matches('/'));
        let limit_value = limit.to_string();

        let response = self
            .client
            .get(endpoint)
            .query(&[("query", query_expression), ("fmt", "json"), ("limit", &limit_value)])
            .send()
            .await
            .context("musicbrainz request failed")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_else(|_| "<body unavailable>".to_owned());
            return Err(anyhow!("musicbrainz request failed with status {}: {}", status, body));
        }

        response.json::<Value>().await.context("musicbrainz response was not valid JSON")
    }
}

#[async_trait]
impl MusicBrainzProvider for MusicBrainzHttpProvider {
    async fn search_recordings(
        &mut self,
        query: &MusicBrainzQuery,
    ) -> Result<ProviderSearchResult> {
        let (query_expression, limit) = self.effective_query_and_limit(query)?;
        let cache_key = cache_key_from_expression(&query_expression, limit);
        let cache_path = self.cache_path_for_key(&cache_key);

        if let Some(cached_response) = self.read_fresh_cache(&cache_path).await? {
            return Ok(ProviderSearchResult {
                candidates: parse_recording_candidates(&cached_response),
                cache_hit: true,
            });
        }

        let response = self.request_recordings(&query_expression, limit).await?;
        self.write_cache(&cache_path, &response).await?;

        Ok(ProviderSearchResult {
            candidates: parse_recording_candidates(&response),
            cache_hit: false,
        })
    }
}

fn cache_key_from_expression(query_expression: &str, limit: usize) -> String {
    let digest = blake3::hash(format!("query={query_expression}\nlimit={limit}").as_bytes());
    digest.to_hex().to_string()
}

fn parse_recording_candidates(payload: &Value) -> Vec<ProviderCandidate> {
    payload
        .get("recordings")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(parse_one_candidate)
        .collect()
}

fn parse_one_candidate(value: &Value) -> ProviderCandidate {
    let score = value.get("score").and_then(parse_score);
    let artist = parse_artist_credit(value.get("artist-credit"));
    let release = value
        .get("releases")
        .and_then(Value::as_array)
        .and_then(|items| items.first())
        .and_then(|item| item.get("title"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);

    ProviderCandidate {
        provider: "musicbrainz".to_owned(),
        entity_id: value.get("id").and_then(Value::as_str).map(ToOwned::to_owned),
        title: value.get("title").and_then(Value::as_str).map(ToOwned::to_owned),
        artist,
        release,
        score,
        raw: value.clone(),
    }
}

fn parse_score(value: &Value) -> Option<f64> {
    if let Some(number) = value.as_f64() {
        return Some(number);
    }

    value.as_str().and_then(|score| score.parse::<f64>().ok())
}

fn parse_artist_credit(value: Option<&Value>) -> Option<String> {
    let credits = value?.as_array()?;
    let mut names = Vec::new();

    for credit in credits {
        if let Some(name) = credit.get("name").and_then(Value::as_str)
            && !name.trim().is_empty()
        {
            names.push(name.to_owned());
            continue;
        }

        if let Some(name) =
            credit.get("artist").and_then(|artist| artist.get("name")).and_then(Value::as_str)
            && !name.trim().is_empty()
        {
            names.push(name.to_owned());
        }
    }

    if names.is_empty() { None } else { Some(names.join(", ")) }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tempfile::tempdir;

    use super::{MusicBrainzHttpProvider, parse_recording_candidates};
    use crate::{configuration::config::MusicBrainzPolicy, infrastructure::store::WorkspacePaths};

    #[tokio::test]
    async fn parses_recording_candidates_from_musicbrainz_payload() {
        let payload = json!({
            "recordings": [
                {
                    "id": "rec-1",
                    "title": "Song A",
                    "score": "98",
                    "artist-credit": [{"name": "Artist A"}],
                    "releases": [{"title": "Album A"}]
                }
            ]
        });

        let candidates = parse_recording_candidates(&payload);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].entity_id.as_deref(), Some("rec-1"));
        assert_eq!(candidates[0].title.as_deref(), Some("Song A"));
        assert_eq!(candidates[0].artist.as_deref(), Some("Artist A"));
        assert_eq!(candidates[0].release.as_deref(), Some("Album A"));
        assert_eq!(candidates[0].score, Some(98.0));
    }

    #[tokio::test]
    async fn cache_key_is_deterministic() {
        let workspace = tempdir().expect("temp workspace should create");
        let paths = WorkspacePaths::new(workspace.path());
        let provider = MusicBrainzHttpProvider::new(&paths, &MusicBrainzPolicy::default())
            .expect("provider should create");

        let query = crate::domain::provider::MusicBrainzQuery {
            query: Some("recording:\"Song\"".to_owned()),
            artist: None,
            title: None,
            release: None,
            limit: Some(3),
        };

        let left = provider.cache_key_for_query(&query).expect("left key should generate");
        let right = provider.cache_key_for_query(&query).expect("right key should generate");
        assert_eq!(left, right);
    }
}
