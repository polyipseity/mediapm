//! MusicBrainz payload fetching and FFmetadata mapping helpers.

use std::collections::BTreeMap;

use anyhow::Context;
use musicbrainz_rs::entity::recording::Recording;
use musicbrainz_rs::entity::release::Release;
use musicbrainz_rs::prelude::*;
use serde::{Deserialize, Serialize};

use super::cover_art::MediaTaggerHttpCache;
use super::util::{
    artist_credit_text, flatten_entity_json, join_unique, resolve_track_position,
    sanitize_metadata_key, truncate_metadata_value,
};

/// Recording + optional release payload fetched from MusicBrainz.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct MusicBrainzPayload {
    /// Recording entity resolved from MusicBrainz API.
    pub(super) recording: Recording,
    /// Optional release entity resolved from MusicBrainz API.
    pub(super) release: Option<Release>,
}

/// Fetches recording/release payloads through `musicbrainz_rs`.
pub(super) async fn fetch_musicbrainz_payloads(
    recording_mbid: &str,
    release_mbid: Option<&str>,
    write_all_tags: bool,
    release_ars: bool,
    cache: &MediaTaggerHttpCache,
) -> anyhow::Result<MusicBrainzPayload> {
    let cached_payload = cache.read_musicbrainz_payload(recording_mbid, release_mbid).await;
    if let Some(cached) = cached_payload.as_ref()
        && cached.is_fresh
    {
        return Ok(cached.payload.clone());
    }

    let stale_cached_payload = cached_payload.as_ref().map(|cached| cached.payload.clone());

    let fetched_payload = async {
        let mut recording_fetch_base = Recording::fetch();
        let mut recording_fetch = recording_fetch_base
            .id(recording_mbid)
            .with_artists()
            .with_releases()
            .with_isrcs()
            .with_genres()
            .with_tags();

        if write_all_tags {
            recording_fetch = recording_fetch.with_annotations().with_work_level_relations();
        }

        let recording = recording_fetch
            .execute_async()
            .await
            .with_context(|| format!("fetching MusicBrainz recording '{recording_mbid}'"))?;

        let release_id = release_mbid.map(ToOwned::to_owned).or_else(|| {
            recording
                .releases
                .as_ref()
                .and_then(|releases| releases.first())
                .map(|release| release.id.clone())
        });

        let release = if let Some(release_id) = release_id {
            let mut release_fetch_base = Release::fetch();
            let mut release_fetch = release_fetch_base
                .id(&release_id)
                .with_artists()
                .with_recordings()
                .with_release_groups()
                .with_media()
                .with_labels()
                .with_isrcs()
                .with_genres()
                .with_tags();

            if write_all_tags {
                release_fetch = release_fetch.with_annotations();
            }
            if write_all_tags || release_ars {
                release_fetch = release_fetch
                    .with_recording_level_relations()
                    .with_release_group_level_relations();
            }

            Some(
                release_fetch
                    .execute_async()
                    .await
                    .with_context(|| format!("fetching MusicBrainz release '{release_id}'"))?,
            )
        } else {
            None
        };

        Ok::<_, anyhow::Error>(MusicBrainzPayload { recording, release })
    }
    .await;

    match fetched_payload {
        Ok(payload) => {
            let _ = cache.write_musicbrainz_payload(recording_mbid, release_mbid, &payload).await;
            Ok(payload)
        }
        Err(error) => stale_cached_payload.ok_or(error),
    }
}

/// Builds deterministic cache key for one recording/release metadata lookup pair.
#[must_use]
pub(super) fn musicbrainz_payload_cache_key(
    recording_mbid: &str,
    release_mbid: Option<&str>,
) -> String {
    let release_mbid = release_mbid.map(str::trim).filter(|value| !value.is_empty()).unwrap_or("");
    format!("recording={recording_mbid}|release={release_mbid}")
}

/// Converts MusicBrainz payload into FFmetadata key/value map.
pub(super) fn build_ffmetadata_map(
    payload: &MusicBrainzPayload,
    write_all_tags: bool,
) -> anyhow::Result<BTreeMap<String, String>> {
    let mut tags = BTreeMap::new();

    tags.insert("title".to_string(), payload.recording.title.clone());
    tags.insert("musicbrainz_recordingid".to_string(), payload.recording.id.clone());
    tags.insert("musicbrainz_trackid".to_string(), payload.recording.id.clone());

    if let Some(artist) = artist_credit_text(payload.recording.artist_credit.as_deref()) {
        tags.insert("artist".to_string(), artist);
    }

    if let Some(first_release_date) = payload.recording.first_release_date.as_ref() {
        tags.insert("originaldate".to_string(), first_release_date.0.clone());
    }

    if let Some(isrcs) = payload.recording.isrcs.as_ref()
        && !isrcs.is_empty()
    {
        tags.insert("isrc".to_string(), join_unique(isrcs.iter().map(String::as_str)));
    }

    if let Some(genres) = payload.recording.genres.as_ref() {
        let genre_values = genres.iter().map(|genre| genre.name.as_str()).collect::<Vec<_>>();
        if !genre_values.is_empty() {
            tags.insert("genre".to_string(), join_unique(genre_values));
        }
    }

    if let Some(release) = payload.release.as_ref() {
        tags.insert("album".to_string(), release.title.clone());
        tags.insert("musicbrainz_albumid".to_string(), release.id.clone());

        if let Some(date) = release.date.as_ref() {
            tags.insert("date".to_string(), date.0.clone());
        }
        if let Some(barcode) = release.barcode.as_ref()
            && !barcode.trim().is_empty()
        {
            tags.insert("barcode".to_string(), barcode.clone());
        }
        if let Some(country) = release.country.as_ref()
            && !country.trim().is_empty()
        {
            tags.insert("releasecountry".to_string(), country.clone());
        }
        if let Some(label_info) = release.label_info.as_ref() {
            let labels = label_info
                .iter()
                .filter_map(|info| info.label.as_ref().map(|label| label.name.as_str()))
                .collect::<Vec<_>>();
            if !labels.is_empty() {
                tags.insert("label".to_string(), join_unique(labels));
            }

            let catalog_numbers = label_info
                .iter()
                .filter_map(|info| info.catalog_number.as_deref())
                .collect::<Vec<_>>();
            if !catalog_numbers.is_empty() {
                tags.insert("catalognumber".to_string(), join_unique(catalog_numbers));
            }
        }

        if let Some(release_group) = release.release_group.as_ref() {
            tags.insert("musicbrainz_releasegroupid".to_string(), release_group.id.clone());
            tags.insert("releasegroup".to_string(), release_group.title.clone());
        }

        if let Some(release_artist) = artist_credit_text(release.artist_credit.as_deref()) {
            tags.insert("albumartist".to_string(), release_artist);
        }

        if let Some(track_info) = resolve_track_position(&payload.recording.id, release) {
            tags.insert("tracknumber".to_string(), track_info.track_number);
            tags.insert("discnumber".to_string(), track_info.disc_number.to_string());
            if let Some(total_tracks) = track_info.total_tracks {
                tags.insert("tracktotal".to_string(), total_tracks.to_string());
                tags.insert("totaltracks".to_string(), total_tracks.to_string());
            }
            if let Some(total_discs) = track_info.total_discs {
                tags.insert("disctotal".to_string(), total_discs.to_string());
                tags.insert("totaldiscs".to_string(), total_discs.to_string());
            }
        }
    }

    if write_all_tags {
        let mut recording_flattened = BTreeMap::new();
        flatten_entity_json(
            "musicbrainz_recording",
            &serde_json::to_value(&payload.recording)?,
            &mut recording_flattened,
        );
        let mut release_flattened = BTreeMap::new();
        if let Some(release) = payload.release.as_ref() {
            flatten_entity_json(
                "musicbrainz_release",
                &serde_json::to_value(release)?,
                &mut release_flattened,
            );
        }

        insert_extended_picard_tags(&mut tags, &recording_flattened, &release_flattened);
    }

    Ok(tags)
}

/// Inserts optional Picard-compatible tags from flattened MusicBrainz payloads.
///
/// This helper intentionally emits only known Picard tag keys and rejects the
/// previous broad flatten/alias expansion to avoid non-Picard metadata keys.
pub(super) fn insert_extended_picard_tags(
    tags: &mut BTreeMap<String, String>,
    recording_flattened: &BTreeMap<String, String>,
    release_flattened: &BTreeMap<String, String>,
) {
    for (alias, source_key) in [
        ("albumartistsort", "musicbrainz_release_artist_credit_0_artist_sort_name"),
        ("artistsort", "musicbrainz_recording_artist_credit_0_artist_sort_name"),
        ("artistcredit", "artist"),
        ("albumartistcredit", "albumartist"),
        ("releasestatus", "musicbrainz_release_status"),
        ("releasetype", "musicbrainz_release_release_group_primary_type"),
        ("secondaryreleasetype", "musicbrainz_release_release_group_secondary_types"),
        ("primaryreleasetype", "musicbrainz_release_release_group_primary_type"),
        ("releasegroup", "musicbrainz_release_release_group_title"),
        ("musicbrainz_releasegroupid", "musicbrainz_release_release_group_id"),
        ("musicbrainz_artistid", "musicbrainz_recording_artist_credit_0_artist_id"),
        ("musicbrainz_albumartistid", "musicbrainz_release_artist_credit_0_artist_id"),
        ("recordingtitle", "musicbrainz_recording_title"),
        ("recordingcomment", "musicbrainz_recording_disambiguation"),
        ("releasecomment", "musicbrainz_release_disambiguation"),
        ("releasegroupcomment", "musicbrainz_release_release_group_disambiguation"),
        ("releaselanguage", "musicbrainz_release_text_representation_language"),
        ("script", "musicbrainz_release_text_representation_script"),
        ("releaseannotation", "musicbrainz_release_annotation"),
        ("recordingannotation", "musicbrainz_recording_annotation"),
        ("releasedate", "date"),
        ("releasecountry", "musicbrainz_release_country"),
        ("media", "musicbrainz_release_media_0_format"),
        ("asin", "musicbrainz_release_asin"),
    ] {
        if let Some(value) = tags.get(source_key).cloned().or_else(|| {
            recording_flattened
                .get(source_key)
                .cloned()
                .or_else(|| release_flattened.get(source_key).cloned())
        }) {
            insert_tag_if_absent(tags, alias, &value);
        }
    }
}

/// Inserts one normalized tag key/value when key is absent and value is non-empty.
pub(super) fn insert_tag_if_absent(tags: &mut BTreeMap<String, String>, key: &str, value: &str) {
    if value.trim().is_empty() {
        return;
    }
    tags.entry(sanitize_metadata_key(key)).or_insert_with(|| truncate_metadata_value(value));
}
