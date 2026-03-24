//! Provider integration interfaces and adapters.
//!
//! This module isolates external metadata-provider behavior behind explicit
//! interfaces so planning/execution logic can remain deterministic and testable.

pub mod musicbrainz;

use anyhow::Result;

use crate::domain::provider::{MusicBrainzQuery, ProviderSearchResult};

/// Trait for MusicBrainz-compatible provider clients.
pub trait MusicBrainzProvider {
    /// Search recording candidates for one normalized query.
    fn search_recordings(&mut self, query: &MusicBrainzQuery) -> Result<ProviderSearchResult>;
}
