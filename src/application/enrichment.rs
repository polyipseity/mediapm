//! Provider-enrichment orchestration.
//!
//! This module coordinates provider queries and deterministic metadata merging.
//! It applies explicit priority rules:
//!
//! 1. User config overrides (highest)
//! 2. Manual/local metadata edits
//! 3. Provider metadata (MusicBrainz)
//! 4. Embedded source tags (lowest)

use std::cmp::Ordering;

use anyhow::{Result, anyhow};
use serde::Serialize;
use serde_json::{Value, json};

use crate::{
    configuration::config::{AppConfig, MusicBrainzQueryDecl},
    domain::{
        canonical::canonicalize_uri,
        model::{EditEvent, EditKind},
        provider::{MusicBrainzQuery, ProviderCandidate},
    },
    infrastructure::{
        provider::{MusicBrainzProvider, musicbrainz::MusicBrainzHttpProvider},
        store::{WorkspacePaths, read_sidecar, write_sidecar},
    },
    support::util::{merge_json_object, now_rfc3339, sort_json_value},
};

/// Enrichment execution summary for one sync run.
#[derive(Debug, Clone, Default, Serialize)]
pub struct ProviderEnrichmentSummary {
    /// Declared provider query entries in config.
    pub queries_declared: usize,
    /// Query entries that reached provider lookup stage.
    pub queries_attempted: usize,
    /// Number of provider lookups served from cache.
    pub cache_hits: usize,
    /// Sidecars rewritten after enrichment merge.
    pub sidecars_updated: usize,
    /// Query executions that failed.
    pub failures: usize,
    /// Non-fatal warnings encountered.
    pub warnings: Vec<String>,
}

/// Apply MusicBrainz enrichment using production provider adapter.
pub fn apply_musicbrainz_enrichment(
    paths: &WorkspacePaths,
    config: &AppConfig,
) -> Result<ProviderEnrichmentSummary> {
    if !config.policies.musicbrainz_enabled || config.provider_queries.is_empty() {
        return Ok(ProviderEnrichmentSummary::default());
    }

    let mut provider = MusicBrainzHttpProvider::new(paths, &config.policies.musicbrainz)?;
    apply_musicbrainz_enrichment_with_provider(paths, config, &mut provider)
}

/// Apply MusicBrainz enrichment with injected provider implementation.
pub fn apply_musicbrainz_enrichment_with_provider<P: MusicBrainzProvider>(
    paths: &WorkspacePaths,
    config: &AppConfig,
    provider: &mut P,
) -> Result<ProviderEnrichmentSummary> {
    let mut summary = ProviderEnrichmentSummary {
        queries_declared: config.provider_queries.len(),
        ..ProviderEnrichmentSummary::default()
    };

    for (uri_key, query_decl) in &config.provider_queries {
        let canonical_uri = match canonicalize_uri(uri_key, &paths.root) {
            Ok(uri) => uri.into_string(),
            Err(error) => {
                summary.warnings.push(format!(
                    "provider query key '{}' could not be canonicalized: {}",
                    uri_key, error
                ));
                continue;
            }
        };

        let query = query_from_decl(query_decl);
        if query.effective_query().is_none() {
            summary.warnings.push(format!(
                "provider query for '{}' has no usable fields (query/artist/title/release)",
                canonical_uri
            ));
            continue;
        }

        let Some(mut sidecar) = read_sidecar(paths, &canonical_uri)? else {
            summary.warnings.push(format!(
                "provider query for '{}' skipped because sidecar does not exist",
                canonical_uri
            ));
            continue;
        };

        summary.queries_attempted += 1;

        let provider_result = match provider.search_recordings(&query) {
            Ok(result) => result,
            Err(error) => {
                summary.failures += 1;
                summary
                    .warnings
                    .push(format!("provider query failed for '{}': {}", canonical_uri, error));
                continue;
            }
        };

        if provider_result.cache_hit {
            summary.cache_hits += 1;
        }

        let mut changed = false;
        let candidate_values = provider_result
            .candidates
            .iter()
            .map(serde_json::to_value)
            .collect::<Result<Vec<_>, _>>()?;

        if sidecar.provider_enrichment.musicbrainz.matches != candidate_values {
            sidecar.provider_enrichment.musicbrainz.matches = candidate_values;
            changed = true;
        }

        let selected_candidate = select_best_candidate(&provider_result.candidates);
        let mut metadata_patch = json!({ "tags": {} });
        let mut field_provenance = json!({});
        let mut skipped_fields = Vec::<String>::new();

        let mut variant_changed = false;
        let mut from_to_hash = None;

        if let Some(candidate) = &selected_candidate {
            let latest_index = sidecar
                .variants
                .len()
                .checked_sub(1)
                .ok_or_else(|| anyhow!("sidecar '{}' has no variants", canonical_uri))?;

            let variant_hash = sidecar.variants[latest_index].variant_hash;
            let user_override = config.metadata_overrides.get(&canonical_uri);
            let previous_provider_patch =
                sidecar.provider_enrichment.musicbrainz.applied.get("metadata_patch").cloned();

            let (effective_patch, effective_provenance, skipped) = derive_effective_provider_patch(
                candidate,
                &sidecar.variants[latest_index].metadata,
                user_override,
                previous_provider_patch.as_ref(),
            );

            skipped_fields = skipped;
            metadata_patch = effective_patch;
            field_provenance = effective_provenance;

            let before = sidecar.variants[latest_index].metadata.clone();
            merge_json_object(&mut sidecar.variants[latest_index].metadata, &metadata_patch);
            variant_changed = sidecar.variants[latest_index].metadata != before;

            if variant_changed {
                changed = true;
            }

            from_to_hash = Some(variant_hash);
        }

        let applied_payload = json!({
            "provider": "musicbrainz",
            "query": query,
            "selected_candidate": selected_candidate,
            "metadata_patch": metadata_patch,
            "field_provenance": field_provenance,
            "skipped_fields": skipped_fields,
            "merge_priority": [
                "user_overrides",
                "manual_local_edits",
                "provider_musicbrainz",
                "embedded_tags"
            ]
        });

        let signature = applied_signature(&applied_payload)?;
        let previous_signature = sidecar
            .provider_enrichment
            .musicbrainz
            .applied
            .get("signature")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);

        if previous_signature.as_deref() != Some(signature.as_str()) {
            let mut applied_with_signature = applied_payload.clone();
            applied_with_signature["signature"] = Value::String(signature.clone());
            sidecar.provider_enrichment.musicbrainz.applied = applied_with_signature;
            changed = true;

            if let Some(variant_hash) = from_to_hash {
                let event_id = format!(
                    "evt_provider_musicbrainz_{}_{}",
                    &variant_hash.to_hex()[..12],
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_nanos()
                );

                sidecar.edits.push(EditEvent {
                    event_id: event_id.clone(),
                    timestamp: now_rfc3339()?,
                    kind: EditKind::Revertable,
                    operation: "provider_musicbrainz_apply".to_owned(),
                    details: json!({
                        "signature": signature,
                        "metadata_changed": variant_changed,
                    }),
                    from_variant_hash: variant_hash,
                    to_variant_hash: variant_hash,
                });

                if let Some(latest_variant) = sidecar.variants.last_mut()
                    && !latest_variant
                        .lineage
                        .edit_event_ids
                        .iter()
                        .any(|existing| existing == &event_id)
                {
                    latest_variant.lineage.edit_event_ids.push(event_id);
                }
            }
        }

        if changed {
            write_sidecar(paths, &sidecar)?;
            summary.sidecars_updated += 1;
        }
    }

    Ok(summary)
}

fn query_from_decl(decl: &MusicBrainzQueryDecl) -> MusicBrainzQuery {
    MusicBrainzQuery {
        query: decl.query.clone(),
        artist: decl.artist.clone(),
        title: decl.title.clone(),
        release: decl.release.clone(),
        limit: decl.limit,
    }
}

fn select_best_candidate(candidates: &[ProviderCandidate]) -> Option<ProviderCandidate> {
    candidates.iter().max_by(|left, right| compare_candidate(left, right)).cloned()
}

fn compare_candidate(left: &ProviderCandidate, right: &ProviderCandidate) -> Ordering {
    let left_score = left.score.unwrap_or(0.0);
    let right_score = right.score.unwrap_or(0.0);

    match left_score.partial_cmp(&right_score).unwrap_or(Ordering::Equal) {
        Ordering::Equal => {
            left.entity_id.as_deref().unwrap_or("").cmp(right.entity_id.as_deref().unwrap_or(""))
        }
        ordering => ordering,
    }
}

fn derive_effective_provider_patch(
    candidate: &ProviderCandidate,
    current_metadata: &Value,
    user_override: Option<&Value>,
    previous_provider_patch: Option<&Value>,
) -> (Value, Value, Vec<String>) {
    let (candidate_patch, candidate_provenance) = candidate.metadata_patch_with_provenance();

    let current_tags =
        current_metadata.get("tags").and_then(Value::as_object).cloned().unwrap_or_default();

    let user_override_tags = user_override
        .and_then(|override_value| override_value.get("tags"))
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();

    let previous_provider_tags = previous_provider_patch
        .and_then(|patch| patch.get("tags"))
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();

    let candidate_tags =
        candidate_patch.get("tags").and_then(Value::as_object).cloned().unwrap_or_default();

    let candidate_provenance_tags = candidate_provenance.as_object().cloned().unwrap_or_default();

    let mut effective_tags = serde_json::Map::new();
    let mut effective_provenance = serde_json::Map::new();
    let mut skipped_fields = Vec::new();

    for (field, provider_value) in candidate_tags {
        if user_override_tags.contains_key(&field) {
            skipped_fields.push(field);
            continue;
        }

        let should_apply = match current_tags.get(&field) {
            None => true,
            Some(current_value) => match previous_provider_tags.get(&field) {
                Some(previous_provider_value) => current_value == previous_provider_value,
                None => false,
            },
        };

        if should_apply {
            effective_tags.insert(field.clone(), provider_value);
            if let Some(source) = candidate_provenance_tags.get(&field) {
                effective_provenance.insert(field, source.clone());
            }
        } else {
            skipped_fields.push(field);
        }
    }

    (
        json!({ "tags": Value::Object(effective_tags) }),
        Value::Object(effective_provenance),
        skipped_fields,
    )
}

fn applied_signature(payload: &Value) -> Result<String> {
    let mut canonical = payload.clone();
    sort_json_value(&mut canonical);
    let bytes = serde_json::to_vec(&canonical)?;
    Ok(blake3::hash(&bytes).to_hex().to_string())
}
