//! Provider integration interfaces and adapters.
//!
//! This module isolates external metadata-provider behavior behind explicit
//! interfaces so planning/execution logic can remain deterministic and testable.

pub mod musicbrainz;

use anyhow::Result;
use async_trait::async_trait;

use crate::domain::provider::{MusicBrainzQuery, ProviderSearchResult};

/// Trait for MusicBrainz-compatible provider clients.
#[async_trait]
pub trait MusicBrainzProvider {
    /// Search recording candidates for one normalized query.
    async fn search_recordings(&mut self, query: &MusicBrainzQuery)
    -> Result<ProviderSearchResult>;
}
