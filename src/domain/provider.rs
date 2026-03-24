//! Provider-domain models and normalization helpers.
//!
//! This module defines provider-agnostic domain structures used by enrichment
//! orchestration. The goal is to keep provider payload semantics explicit and
//! deterministic before they are merged into sidecars.

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// Normalized query input used by the MusicBrainz adapter.
#[derive(Clone, Debug, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct MusicBrainzQuery {
    /// Optional raw query expression override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,
    /// Artist hint for query synthesis.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artist: Option<String>,
    /// Recording/title hint for query synthesis.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// Optional release hint for query synthesis.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub release: Option<String>,
    /// Optional result limit override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
}

impl MusicBrainzQuery {
    /// Build effective query expression for MusicBrainz recording search.
    pub fn effective_query(&self) -> Option<String> {
        if let Some(raw) = self.query.as_deref().map(str::trim)
            && !raw.is_empty()
        {
            return Some(raw.to_owned());
        }

        let mut terms = Vec::new();

        if let Some(artist) = self.artist.as_deref().map(str::trim)
            && !artist.is_empty()
        {
            terms.push(format!("artist:\"{}\"", escape_musicbrainz_term(artist)));
        }

        if let Some(title) = self.title.as_deref().map(str::trim)
            && !title.is_empty()
        {
            terms.push(format!("recording:\"{}\"", escape_musicbrainz_term(title)));
        }

        if let Some(release) = self.release.as_deref().map(str::trim)
            && !release.is_empty()
        {
            terms.push(format!("release:\"{}\"", escape_musicbrainz_term(release)));
        }

        if terms.is_empty() { None } else { Some(terms.join(" AND ")) }
    }
}

/// Normalized provider candidate used by enrichment merge policy.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ProviderCandidate {
    /// Provider identifier (`musicbrainz`).
    pub provider: String,
    /// Provider entity identifier (MusicBrainz recording id).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entity_id: Option<String>,
    /// Candidate title/recording name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// Candidate artist display name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artist: Option<String>,
    /// Candidate release/album title.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub release: Option<String>,
    /// Optional provider confidence score.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub score: Option<f64>,
    /// Raw provider payload for traceability.
    #[serde(default)]
    pub raw: Value,
}

impl ProviderCandidate {
    /// Build metadata patch and field-level provenance map for this candidate.
    pub fn metadata_patch_with_provenance(&self) -> (Value, Value) {
        let mut tags = serde_json::Map::new();
        let mut provenance = serde_json::Map::new();

        if let Some(title) = &self.title {
            tags.insert("title".to_owned(), Value::String(title.clone()));
            provenance.insert("title".to_owned(), Value::String("provider.musicbrainz".to_owned()));
        }

        if let Some(artist) = &self.artist {
            tags.insert("artist".to_owned(), Value::String(artist.clone()));
            provenance
                .insert("artist".to_owned(), Value::String("provider.musicbrainz".to_owned()));
        }

        if let Some(release) = &self.release {
            tags.insert("album".to_owned(), Value::String(release.clone()));
            provenance.insert("album".to_owned(), Value::String("provider.musicbrainz".to_owned()));
        }

        let patch = json!({ "tags": Value::Object(tags) });
        (patch, Value::Object(provenance))
    }
}

/// Result of one provider search request.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ProviderSearchResult {
    /// Normalized candidates returned by provider.
    pub candidates: Vec<ProviderCandidate>,
    /// Whether response came from local cache.
    pub cache_hit: bool,
}

fn escape_musicbrainz_term(input: &str) -> String {
    input.replace('"', "\\\"")
}

#[cfg(test)]
mod tests {
    use super::{MusicBrainzQuery, ProviderCandidate};

    #[test]
    fn effective_query_prefers_explicit_query_field() {
        let query = MusicBrainzQuery {
            query: Some("recording:custom".to_owned()),
            artist: Some("Artist".to_owned()),
            title: Some("Song".to_owned()),
            release: None,
            limit: None,
        };

        assert_eq!(query.effective_query().as_deref(), Some("recording:custom"));
    }

    #[test]
    fn effective_query_synthesizes_from_hints() {
        let query = MusicBrainzQuery {
            query: None,
            artist: Some("Artist".to_owned()),
            title: Some("Song".to_owned()),
            release: Some("Album".to_owned()),
            limit: None,
        };

        let expression = query.effective_query().expect("query should synthesize");
        assert!(expression.contains("artist:\"Artist\""));
        assert!(expression.contains("recording:\"Song\""));
        assert!(expression.contains("release:\"Album\""));
    }

    #[test]
    fn candidate_builds_patch_and_field_provenance() {
        let candidate = ProviderCandidate {
            provider: "musicbrainz".to_owned(),
            entity_id: Some("id-1".to_owned()),
            title: Some("Track".to_owned()),
            artist: Some("Artist".to_owned()),
            release: Some("Album".to_owned()),
            score: Some(100.0),
            raw: serde_json::json!({}),
        };

        let (patch, provenance) = candidate.metadata_patch_with_provenance();

        assert_eq!(patch["tags"]["title"], "Track");
        assert_eq!(patch["tags"]["artist"], "Artist");
        assert_eq!(patch["tags"]["album"], "Album");

        assert_eq!(provenance["title"], "provider.musicbrainz");
        assert_eq!(provenance["artist"], "provider.musicbrainz");
        assert_eq!(provenance["album"], "provider.musicbrainz");
    }
}
