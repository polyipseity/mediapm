use anyhow::Result;

use mediapm::{
    domain::provider::{MusicBrainzQuery, ProviderSearchResult},
    infrastructure::provider::MusicBrainzProvider,
};

struct DummyProvider;

impl MusicBrainzProvider for DummyProvider {
    fn search_recordings(&mut self, _query: &MusicBrainzQuery) -> Result<ProviderSearchResult> {
        Ok(ProviderSearchResult { candidates: vec![], cache_hit: true })
    }
}

#[test]
fn provider_trait_is_usable_from_integration_tests() {
    let mut provider = DummyProvider;
    let result = provider
        .search_recordings(&MusicBrainzQuery::default())
        .expect("dummy provider should succeed");

    assert!(result.cache_hit);
    assert!(result.candidates.is_empty());
}
