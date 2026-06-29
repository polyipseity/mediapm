//! Native mediapm metadata tagging pipeline.
//!
//! This module replaces the legacy external Picard step with an internal
//! `mediapm` flow:
//! 1. decode input audio and compute Chromaprint fingerprint,
//! 2. resolve MBIDs through AcoustID,
//! 3. fetch rich recording/release payloads through `musicbrainz_rs`,
//! 4. map metadata into `FFmetadata` key/value pairs,
//! 5. persist one `FFmetadata` document for downstream apply stages.
//!
//! Failure policy is controlled by `strict_identification`: when enabled,
//! unresolved identity or metadata-fetch failures abort the step; when disabled,
//! an empty `FFmetadata` document is written so downstream apply stages may
//! continue deterministically.
//! AcoustID lookup itself is only attempted when no recording MBID override is
//! supplied. Missing/empty lookup credentials are a hard error on that
//! autodetection path; when credentials are provided, lookup/authentication
//! failures are surfaced as hard errors.
//! MBID override sentinel behavior:
//! - empty/omitted or `auto` => allow AcoustID autodetection path,
//! - `none` => disable AcoustID autodetection path entirely.

#![allow(clippy::doc_markdown)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, bail};
use tokio::process::Command;

mod acoustid;
mod cover_art;
mod ffmetadata;
mod musicbrainz;
mod util;

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use tempfile::tempdir;

    use crate::builtins::media_tagger::acoustid::require_acoustid_api_key_for_lookup;
    use crate::builtins::media_tagger::cover_art::{
        CacheExpiryPolicy, CoverArtArchiveImage, MediaTaggerHttpCache, SelectedCoverArt,
        insert_musicbrainz_image_tags, normalized_cover_art_types,
        persist_cover_art_slot_artifacts, select_cover_art_for_tag_embedding,
        select_highest_quality_cover_url,
    };
    use crate::builtins::media_tagger::ffmetadata::parse_ffmetadata_global_map;
    use crate::builtins::media_tagger::musicbrainz::{
        insert_extended_picard_tags, musicbrainz_payload_cache_key,
    };
    use crate::builtins::media_tagger::util::resolve_ffmpeg_executable_from_configured_path;
    use crate::builtins::media_tagger::*;

    /// Protects strict autodetection policy: when `recording_mbid` is absent,
    /// missing/blank `AcoustID` credentials must fail immediately.
    #[tokio::test]
    async fn run_internal_media_tagger_fails_when_acoustid_key_is_missing_for_autodetect() {
        let workspace = tempdir().expect("tempdir");
        let input_path = workspace.path().join("input.mp3");
        let output_path = workspace.path().join("output.ffmetadata");
        std::fs::write(&input_path, b"dummy-bytes").expect("write input media stub");

        let result = run_internal_media_tagger(InternalMediaTaggerOptions {
            input_path: Some(input_path),
            output_path: output_path.clone(),
            acoustid_api_key: Some("   ".to_string()),
            acoustid_endpoint: DEFAULT_ACOUSTID_ENDPOINT.to_string(),
            musicbrainz_endpoint: DEFAULT_MUSICBRAINZ_ENDPOINT.to_string(),
            cache_dir: None,
            cache_expiry_seconds: DEFAULT_CACHE_EXPIRY_SECONDS,
            strict_identification: false,
            write_all_tags: true,
            write_all_images: true,
            save_images_to_tags: DEFAULT_SAVE_IMAGES_TO_TAGS,
            embed_only_one_front_image: DEFAULT_EMBED_ONLY_ONE_FRONT_IMAGE,
            ca_providers: DEFAULT_CA_PROVIDERS.to_string(),
            caa_image_types: DEFAULT_CAA_IMAGE_TYPES.to_string(),
            caa_image_size: DEFAULT_CAA_IMAGE_SIZE.to_string(),
            caa_approved_only: DEFAULT_CAA_APPROVED_ONLY,
            preserve_images: DEFAULT_PRESERVE_IMAGES,
            clear_existing_tags: DEFAULT_CLEAR_EXISTING_TAGS,
            enable_tag_saving: DEFAULT_ENABLE_TAG_SAVING,
            release_ars: DEFAULT_RELEASE_ARS,
            cover_art_slot_count: 8,
            recording_mbid: None,
            release_mbid: None,
        })
        .await;

        let error = result.expect_err("missing key on autodetect path must fail");
        assert!(
            error.to_string().contains("AcoustID lookup requires a non-empty API key"),
            "expected strict missing-key failure but got: {error:#}"
        );
        assert!(
            !error.to_string().contains("decoding media for fingerprinting"),
            "missing-key validation should fail before decode path"
        );

        assert!(
            output_path.exists(),
            "wrapper should still write fallback ffmetadata output after error"
        );
    }

    /// Protects helper behavior that enforces explicit key presence for
    /// autodetection lookups.
    #[test]
    fn require_acoustid_api_key_for_lookup_enforces_non_empty_key() {
        let missing = require_acoustid_api_key_for_lookup(None).expect_err("missing key must fail");
        assert!(
            missing.to_string().contains("AcoustID lookup requires a non-empty API key"),
            "expected missing-key diagnostic"
        );

        let provided = require_acoustid_api_key_for_lookup(Some("demo-key".to_string()))
            .expect("non-empty key should pass");
        assert_eq!(provided, "demo-key");
    }

    /// Protects behavior that when credentials are supplied, the
    /// `AcoustID` lookup path is attempted and failures are surfaced.
    #[tokio::test]
    async fn run_internal_media_tagger_with_key_attempts_lookup_path() {
        let workspace = tempdir().expect("tempdir");
        let input_path = workspace.path().join("input.mp3");
        let output_path = workspace.path().join("output.ffmetadata");
        std::fs::write(&input_path, b"dummy-bytes").expect("write input media stub");

        let result = run_internal_media_tagger(InternalMediaTaggerOptions {
            input_path: Some(input_path),
            output_path: output_path.clone(),
            acoustid_api_key: Some("demo-key".to_string()),
            acoustid_endpoint: DEFAULT_ACOUSTID_ENDPOINT.to_string(),
            musicbrainz_endpoint: DEFAULT_MUSICBRAINZ_ENDPOINT.to_string(),
            cache_dir: None,
            cache_expiry_seconds: DEFAULT_CACHE_EXPIRY_SECONDS,
            strict_identification: false,
            write_all_tags: true,
            write_all_images: true,
            save_images_to_tags: DEFAULT_SAVE_IMAGES_TO_TAGS,
            embed_only_one_front_image: DEFAULT_EMBED_ONLY_ONE_FRONT_IMAGE,
            ca_providers: DEFAULT_CA_PROVIDERS.to_string(),
            caa_image_types: DEFAULT_CAA_IMAGE_TYPES.to_string(),
            caa_image_size: DEFAULT_CAA_IMAGE_SIZE.to_string(),
            caa_approved_only: DEFAULT_CAA_APPROVED_ONLY,
            preserve_images: DEFAULT_PRESERVE_IMAGES,
            clear_existing_tags: DEFAULT_CLEAR_EXISTING_TAGS,
            enable_tag_saving: DEFAULT_ENABLE_TAG_SAVING,
            release_ars: DEFAULT_RELEASE_ARS,
            cover_art_slot_count: 8,
            recording_mbid: None,
            release_mbid: None,
        })
        .await;

        let error = result.expect_err("provided key should execute lookup path");
        assert!(
            error.to_string().contains("decoding media for fingerprinting"),
            "expected decode path failure when lookup path is active"
        );
        assert!(
            output_path.exists(),
            "ffmetadata output should still exist for workflow output-capture consistency"
        );
    }

    /// Protects sentinel behavior where `recording_mbid=none` disables AcoustID
    /// autodetection entirely.
    #[tokio::test]
    async fn run_internal_media_tagger_recording_none_skips_autodetect_path() {
        let workspace = tempdir().expect("tempdir");
        let input_path = workspace.path().join("input.mp3");
        let output_path = workspace.path().join("output.ffmetadata");
        std::fs::write(&input_path, b"dummy-bytes").expect("write input media stub");

        let result = run_internal_media_tagger(InternalMediaTaggerOptions {
            input_path: Some(input_path),
            output_path: output_path.clone(),
            acoustid_api_key: None,
            acoustid_endpoint: DEFAULT_ACOUSTID_ENDPOINT.to_string(),
            musicbrainz_endpoint: DEFAULT_MUSICBRAINZ_ENDPOINT.to_string(),
            cache_dir: None,
            cache_expiry_seconds: DEFAULT_CACHE_EXPIRY_SECONDS,
            strict_identification: true,
            write_all_tags: true,
            write_all_images: true,
            save_images_to_tags: DEFAULT_SAVE_IMAGES_TO_TAGS,
            embed_only_one_front_image: DEFAULT_EMBED_ONLY_ONE_FRONT_IMAGE,
            ca_providers: DEFAULT_CA_PROVIDERS.to_string(),
            caa_image_types: DEFAULT_CAA_IMAGE_TYPES.to_string(),
            caa_image_size: DEFAULT_CAA_IMAGE_SIZE.to_string(),
            caa_approved_only: DEFAULT_CAA_APPROVED_ONLY,
            preserve_images: DEFAULT_PRESERVE_IMAGES,
            clear_existing_tags: DEFAULT_CLEAR_EXISTING_TAGS,
            enable_tag_saving: DEFAULT_ENABLE_TAG_SAVING,
            release_ars: DEFAULT_RELEASE_ARS,
            cover_art_slot_count: 8,
            recording_mbid: Some("none".to_string()),
            release_mbid: None,
        })
        .await;

        result.expect("none sentinel should bypass autodetect and succeed with fallback metadata");
        assert!(output_path.exists(), "none sentinel path should emit fallback ffmetadata output");
    }

    /// Protects sentinel behavior where `recording_mbid=auto` is equivalent to an
    /// omitted recording MBID and keeps autodetection active.
    #[tokio::test]
    async fn run_internal_media_tagger_recording_auto_keeps_autodetect_path() {
        let workspace = tempdir().expect("tempdir");
        let input_path = workspace.path().join("input.mp3");
        let output_path = workspace.path().join("output.ffmetadata");
        std::fs::write(&input_path, b"dummy-bytes").expect("write input media stub");

        let result = run_internal_media_tagger(InternalMediaTaggerOptions {
            input_path: Some(input_path),
            output_path: output_path.clone(),
            acoustid_api_key: Some("demo-key".to_string()),
            acoustid_endpoint: DEFAULT_ACOUSTID_ENDPOINT.to_string(),
            musicbrainz_endpoint: DEFAULT_MUSICBRAINZ_ENDPOINT.to_string(),
            cache_dir: None,
            cache_expiry_seconds: DEFAULT_CACHE_EXPIRY_SECONDS,
            strict_identification: false,
            write_all_tags: true,
            write_all_images: true,
            save_images_to_tags: DEFAULT_SAVE_IMAGES_TO_TAGS,
            embed_only_one_front_image: DEFAULT_EMBED_ONLY_ONE_FRONT_IMAGE,
            ca_providers: DEFAULT_CA_PROVIDERS.to_string(),
            caa_image_types: DEFAULT_CAA_IMAGE_TYPES.to_string(),
            caa_image_size: DEFAULT_CAA_IMAGE_SIZE.to_string(),
            caa_approved_only: DEFAULT_CAA_APPROVED_ONLY,
            preserve_images: DEFAULT_PRESERVE_IMAGES,
            clear_existing_tags: DEFAULT_CLEAR_EXISTING_TAGS,
            enable_tag_saving: DEFAULT_ENABLE_TAG_SAVING,
            release_ars: DEFAULT_RELEASE_ARS,
            cover_art_slot_count: 8,
            recording_mbid: Some("auto".to_string()),
            release_mbid: Some("auto".to_string()),
        })
        .await;

        let error = result.expect_err("auto sentinel should keep lookup/decode path active");
        assert!(
            error.to_string().contains("decoding media for fingerprinting"),
            "expected decode path failure when autodetection is active"
        );
        assert!(
            output_path.exists(),
            "ffmetadata output should still exist for workflow output-capture consistency"
        );
    }

    /// Protects ffmetadata parsing for metadata-preserving merge behavior.
    #[test]
    fn parse_ffmetadata_global_map_decodes_escaped_pairs() {
        let parsed = parse_ffmetadata_global_map(
            ";FFMETADATA1\nartist=Example Artist\ncomment=hello\\=world\n[CHAPTER]\nstart=0\n",
        );

        assert_eq!(parsed.get("artist"), Some(&"Example Artist".to_string()));
        assert_eq!(parsed.get("comment"), Some(&"hello=world".to_string()));
        assert!(!parsed.contains_key("start"));
    }

    /// Protects Picard-compatible cover-art tag projection from discovered URLs.
    #[test]
    fn insert_musicbrainz_image_tags_emits_picard_coverart_aliases() {
        let mut tags = BTreeMap::new();
        insert_musicbrainz_image_tags(
            &mut tags,
            &[SelectedCoverArt {
                url: "https://example.test/front.jpg".to_string(),
                maintype: "front".to_string(),
                types: vec!["front".to_string(), "booklet".to_string()],
                comment: "Scanned sleeve".to_string(),
            }],
        );

        assert_eq!(tags.get("coverart_url"), Some(&"https://example.test/front.jpg".to_string()));
        assert_eq!(tags.get("coverart_url_0"), Some(&"https://example.test/front.jpg".to_string()));
        assert_eq!(tags.get("coverart_maintype"), Some(&"front".to_string()));
        assert_eq!(tags.get("coverart_types"), Some(&"front; booklet".to_string()));
        assert_eq!(tags.get("coverart_comment"), Some(&"Scanned sleeve".to_string()));
    }

    /// Protects cover-art quality selection so each artwork entry prefers the
    /// highest available source image over thumbnail fallbacks.
    #[test]
    fn select_highest_quality_cover_url_prefers_original_image() {
        let image = CoverArtArchiveImage {
            image: Some("https://example.test/original.jpg".to_string()),
            thumbnails: BTreeMap::from([
                ("250".to_string(), "https://example.test/250.jpg".to_string()),
                ("1200".to_string(), "https://example.test/1200.jpg".to_string()),
            ]),
            types: vec!["Front".to_string()],
            comment: None,
            approved: Some(true),
        };

        assert_eq!(
            select_highest_quality_cover_url(&image),
            Some("https://example.test/original.jpg".to_string())
        );
    }

    /// Protects no-backcompat behavior by ignoring legacy `front`/`back` bools.
    #[test]
    fn normalized_cover_art_types_ignores_legacy_bool_aliases() {
        let image: CoverArtArchiveImage = serde_json::from_value(serde_json::json!({
            "image": "https://example.test/original.jpg",
            "thumbnails": {},
            "types": [],
            "front": true,
            "back": true
        }))
        .expect("legacy bool payload should deserialize with unknown fields ignored");

        assert_eq!(normalized_cover_art_types(&image), vec!["other".to_string()]);
    }

    /// Protects Picard-compatible default embedding policy by selecting only the
    /// first front image when multiple cover-art entries are available.
    #[test]
    fn select_cover_art_for_tag_embedding_prefers_first_front_image() {
        let entries = vec![
            SelectedCoverArt {
                url: "https://example.test/back.jpg".to_string(),
                maintype: "back".to_string(),
                types: vec!["back".to_string()],
                comment: String::new(),
            },
            SelectedCoverArt {
                url: "https://example.test/front-a.jpg".to_string(),
                maintype: "front".to_string(),
                types: vec!["front".to_string(), "booklet".to_string()],
                comment: String::new(),
            },
            SelectedCoverArt {
                url: "https://example.test/front-b.jpg".to_string(),
                maintype: "front".to_string(),
                types: vec!["front".to_string()],
                comment: String::new(),
            },
        ];

        let selected = select_cover_art_for_tag_embedding(&entries, true);
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].url, "https://example.test/front-a.jpg");
    }

    /// Protects Picard-compatible default embedding policy by ensuring non-front
    /// image-only payloads do not get embedded when front-only mode is enabled.
    #[test]
    fn select_cover_art_for_tag_embedding_returns_empty_without_front_images() {
        let entries = vec![SelectedCoverArt {
            url: "https://example.test/back.jpg".to_string(),
            maintype: "back".to_string(),
            types: vec!["back".to_string()],
            comment: String::new(),
        }];

        let selected = select_cover_art_for_tag_embedding(&entries, true);
        assert!(selected.is_empty());
    }

    /// Protects explicit override behavior by allowing all selected images when
    /// front-only embedding mode is disabled.
    #[test]
    fn select_cover_art_for_tag_embedding_keeps_all_images_when_disabled() {
        let entries = vec![
            SelectedCoverArt {
                url: "https://example.test/back.jpg".to_string(),
                maintype: "back".to_string(),
                types: vec!["back".to_string()],
                comment: String::new(),
            },
            SelectedCoverArt {
                url: "https://example.test/front.jpg".to_string(),
                maintype: "front".to_string(),
                types: vec!["front".to_string()],
                comment: String::new(),
            },
        ];

        let selected = select_cover_art_for_tag_embedding(&entries, false);
        assert_eq!(selected, entries);
    }

    /// Protects deterministic cover-art slot fanout by requiring empty
    /// placeholder members for unused slots.
    #[tokio::test]
    async fn persist_cover_art_slot_artifacts_writes_empty_members_for_unused_slots() {
        let root = tempdir().expect("tempdir");
        let output_path = root.path().join("metadata").join("output.ffmeta");
        let cache = MediaTaggerHttpCache::new(None, CacheExpiryPolicy::from_seconds(0));

        persist_cover_art_slot_artifacts(&output_path, &[], 3, &cache)
            .await
            .expect("empty slot artifact write should succeed");

        let artifact_dir = root.path().join("coverart");
        for slot_index in 1..=3 {
            let image_path = artifact_dir.join(cover_art_slot_image_member_name(slot_index));
            let flag_path = artifact_dir.join(cover_art_slot_flag_member_name(slot_index));

            let image_bytes = std::fs::read(&image_path).expect("read image slot");
            let flag_bytes = std::fs::read(&flag_path).expect("read flag slot");
            assert!(image_bytes.is_empty(), "unused image slot should be empty bytes");
            assert!(flag_bytes.is_empty(), "unused flag slot should be empty bytes");
        }
    }

    /// Protects strict Picard mapping by preventing flattened source keys from
    /// leaking into generated ffmetadata output.
    #[test]
    fn insert_extended_picard_tags_does_not_emit_flattened_source_keys() {
        let mut tags = BTreeMap::new();
        let recording_flattened = BTreeMap::from([(
            "musicbrainz_recording_artist_credit_0_artist_sort_name".to_string(),
            "Recording Artist Sort".to_string(),
        )]);
        let release_flattened = BTreeMap::from([(
            "musicbrainz_release_artist_credit_0_artist_sort_name".to_string(),
            "Album Artist Sort".to_string(),
        )]);

        insert_extended_picard_tags(&mut tags, &recording_flattened, &release_flattened);

        assert_eq!(tags.get("artistsort"), Some(&"Recording Artist Sort".to_string()));
        assert_eq!(tags.get("albumartistsort"), Some(&"Album Artist Sort".to_string()));
        assert!(!tags.contains_key("musicbrainz_recording_artist_credit_0_artist_sort_name"));
        assert!(!tags.contains_key("musicbrainz_release_artist_credit_0_artist_sort_name"));
    }

    /// Protects cache policy semantics where negative expiry means
    /// "never expire" and non-negative values enforce age checks.
    #[test]
    fn cache_expiry_policy_handles_negative_never_expire() {
        let never_expire = CacheExpiryPolicy::from_seconds(-1);
        assert!(never_expire.is_fresh(1, 10_000_000));

        let one_second = CacheExpiryPolicy::from_seconds(1);
        assert!(one_second.is_fresh(99, 100));
        assert!(!one_second.is_fresh(98, 100));
    }

    /// Protects media-tagger JSON cache persistence so cached cover-art rows
    /// can be reused after transient upstream failures.
    #[tokio::test]
    async fn media_tagger_http_cache_round_trips_cover_entries() {
        let root = tempdir().expect("tempdir");
        let cache = MediaTaggerHttpCache::new(
            Some(root.path().join("cache-store")),
            CacheExpiryPolicy::from_seconds(60),
        );

        let entries = vec![SelectedCoverArt {
            url: "https://example.test/front.jpg".to_string(),
            maintype: "front".to_string(),
            types: vec!["front".to_string()],
            comment: "cover".to_string(),
        }];
        cache
            .write_cover_art_entries("https://coverartarchive.org/release/demo", &entries)
            .await
            .expect("write cached entries");

        let loaded = cache
            .read_cover_art_entries("https://coverartarchive.org/release/demo")
            .await
            .expect("read cached entries");
        assert!(loaded.is_fresh);
        assert_eq!(loaded.payload, entries);

        assert!(root.path().join("cache-store").join("store").exists());
        assert!(root.path().join("cache-store").join("media-tagger.json").exists());
        assert!(!root.path().join("cache-store").join("store").join("media-tagger").exists());
    }

    /// Protects metadata-cache persistence so MusicBrainz payload rows share
    /// the same CAS-backed expiry plumbing as cover-art cache rows.
    #[tokio::test]
    async fn media_tagger_http_cache_round_trips_musicbrainz_metadata_rows() {
        let root = tempdir().expect("tempdir");
        let cache = MediaTaggerHttpCache::new(
            Some(root.path().join("cache-store")),
            CacheExpiryPolicy::from_seconds(60),
        );
        let cache_key = musicbrainz_payload_cache_key("recording-demo", Some("release-demo"));
        let payload = serde_json::json!({
            "recording": {
                "id": "recording-demo",
                "title": "Demo Recording"
            },
            "release": {
                "id": "release-demo",
                "title": "Demo Release"
            }
        });

        cache
            .write_json_payload("musicbrainz-payloads", &cache_key, &payload)
            .await
            .expect("write cached metadata payload");

        let loaded = cache
            .read_json_payload::<serde_json::Value>("musicbrainz-payloads", &cache_key)
            .await
            .expect("read cached metadata payload");
        assert!(loaded.is_fresh);
        assert_eq!(loaded.payload, payload);
    }

    /// Protects payload-only runtime path handling by preserving a configured
    /// managed ffmpeg payload path as-is.
    #[test]
    fn resolve_ffmpeg_executable_from_configured_path_preserves_payload_path() {
        let input = "/tmp/demo/.mediapm/tools/mediapm.tools.ffmpeg+demo@v1/macos/ffmpeg";

        assert_eq!(resolve_ffmpeg_executable_from_configured_path(Some(input)), input);
    }

    /// Protects executable resolution by preserving configured payload paths even
    /// when the target file has not been materialized yet.
    #[test]
    fn resolve_ffmpeg_executable_from_configured_path_preserves_missing_payload_path() {
        let payload_path =
            "/tmp/demo/.mediapm/tools/mediapm.tools.ffmpeg+demo@v1/payload/macos/ffmpeg";

        assert_eq!(
            resolve_ffmpeg_executable_from_configured_path(Some(payload_path)),
            payload_path
        );
    }
}

use self::acoustid::{
    decode_and_fingerprint_audio, lookup_acoustid_match, require_acoustid_api_key_for_lookup,
    resolve_acoustid_api_key,
};
use self::cover_art::{
    CacheExpiryPolicy, MediaTaggerHttpCache, collect_musicbrainz_cover_art,
    insert_musicbrainz_image_tags, persist_cover_art_slot_artifacts,
    select_cover_art_for_tag_embedding,
};
use self::ffmetadata::parse_ffmetadata_global_map;
use self::musicbrainz::{build_ffmetadata_map, fetch_musicbrainz_payloads};
use self::util::{normalize_optional_text, resolve_ffmpeg_executable, write_ffmetadata_document};

/// Default AcoustID lookup endpoint used by the internal tagger.
pub const DEFAULT_ACOUSTID_ENDPOINT: &str = "https://api.acoustid.org/v2/lookup";
/// Default MusicBrainz API endpoint label used for diagnostics.
pub const DEFAULT_MUSICBRAINZ_ENDPOINT: &str = "https://musicbrainz.org/ws/2";

/// Target sample rate used for deterministic fingerprint extraction.
const FINGERPRINT_SAMPLE_RATE: u32 = 44_100;
/// Target channel count used for deterministic fingerprint extraction.
const FINGERPRINT_CHANNELS: u16 = 2;
/// Environment variable fallback for AcoustID API credentials.
const ACOUSTID_API_KEY_ENV: &str = "ACOUSTID_API_KEY";
/// Executable name used for decode/export metadata operations.
const FFMPEG_EXECUTABLE: &str = "ffmpeg";
/// Optional env-var override for ffmpeg executable used by internal tagger.
pub const MEDIA_TAGGER_FFMPEG_BIN_ENV: &str = "MEDIAPM_MEDIA_TAGGER_FFMPEG_BIN";
/// Maximum metadata entries emitted to `FFmetadata` output.
const MAX_FLATTENED_METADATA_ENTRIES: usize = 1_024;
/// Maximum flattened metadata value length to avoid runaway tags.
const MAX_FLATTENED_VALUE_LEN: usize = 4_096;
/// Default number of cover-art attachment slots prepared per invocation.
pub const DEFAULT_COVER_ART_SLOT_COUNT: usize = 16;
/// Managed default for embedding image payloads into output tags.
///
/// Picard default: `save_images_to_tags = true`.
pub const DEFAULT_SAVE_IMAGES_TO_TAGS: bool = true;
/// Managed default for Picard-compatible embedding subset selection.
///
/// Picard default: `embed_only_one_front_image = true`.
/// mediapm intentionally defaults to `false` so all selected CAA image kinds
/// can be embedded when image embedding is enabled.
pub const DEFAULT_EMBED_ONLY_ONE_FRONT_IMAGE: bool = false;
/// Managed default cover-art provider selector list.
///
/// This mirrors Picard's default provider order:
/// `Cover Art Archive`, `Allowed Cover Art URLs`,
/// `Cover Art Archive: Release Group`.
pub const DEFAULT_CA_PROVIDERS: &str = "caa_release,url_relationships,caa_release_group";
/// Managed default CAA image-type selector expression.
///
/// Expression syntax supports:
/// - `all` / `*` include-all token,
/// - comma-separated explicit include tokens,
/// - exclusion tokens prefixed with `-` / `!`.
///
/// Default keeps all known CAA kinds except `matrix/runout`,
/// `raw/unedited`, and `watermark`.
pub const DEFAULT_CAA_IMAGE_TYPES: &str = "all,-matrix/runout,-raw/unedited,-watermark";
/// Managed default CAA image-size selector.
///
/// Picard default is typically thumbnail-sized requests; mediapm intentionally
/// defaults to `full` for maximum-quality source retention.
pub const DEFAULT_CAA_IMAGE_SIZE: &str = "full";
/// Managed default CAA approval filter.
///
/// Picard default: `caa_approved_only = false`.
pub const DEFAULT_CAA_APPROVED_ONLY: bool = false;
/// Managed default policy for preserving embedded images when clear-tags mode
/// is requested.
pub const DEFAULT_PRESERVE_IMAGES: bool = false;
/// Managed default policy for clearing existing textual tags before applying
/// new metadata.
pub const DEFAULT_CLEAR_EXISTING_TAGS: bool = false;
/// Managed default policy for writing tags to output media.
pub const DEFAULT_ENABLE_TAG_SAVING: bool = true;
/// Managed default release-relationship lookup toggle.
///
/// Picard default: `release_ars = true`.
pub const DEFAULT_RELEASE_ARS: bool = true;
/// Default media-tagger HTTP cache expiry budget in seconds (one day).
pub const DEFAULT_CACHE_EXPIRY_SECONDS: i64 = 24 * 60 * 60;
/// Cache-index format marker for media-tagger JSON metadata rows.
const MEDIA_TAGGER_CACHE_INDEX_VERSION: u32 = 1;
/// Default media-tagger metadata index file name under cache root.
const MEDIA_TAGGER_CACHE_INDEX_FILE_NAME: &str = "media-tagger.json";
/// Retry count for transient media-tagger HTTP request failures.
const MEDIA_TAGGER_HTTP_RETRY_ATTEMPTS: usize = 3;
/// Initial retry backoff for transient media-tagger HTTP request failures.
const MEDIA_TAGGER_HTTP_RETRY_BASE_DELAY_MILLIS: u64 = 250;

/// Returns deterministic sandbox-artifact file name for one cover-art slot payload.
#[must_use]
pub(crate) fn cover_art_slot_image_member_name(slot_index: usize) -> String {
    format!("coverart-slot-{slot_index}.bin")
}

/// Returns deterministic sandbox-artifact file name for one cover-art slot flag.
#[must_use]
pub(crate) fn cover_art_slot_flag_member_name(slot_index: usize) -> String {
    format!("coverart-slot-{slot_index}.flag")
}

/// Runtime options for one internal media-tagger invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "media-tagger CLI/runtime contracts intentionally expose explicit independent toggles"
)]
pub struct InternalMediaTaggerOptions {
    /// Optional input media path to inspect and tag.
    ///
    /// This is required only for fingerprint-based autodetection paths.
    pub input_path: Option<PathBuf>,
    /// Output `FFmetadata` path generated by this metadata-fetch stage.
    pub output_path: PathBuf,
    /// Optional AcoustID API key override.
    pub acoustid_api_key: Option<String>,
    /// AcoustID lookup endpoint.
    pub acoustid_endpoint: String,
    /// MusicBrainz API endpoint used in diagnostics.
    pub musicbrainz_endpoint: String,
    /// Optional media-tagger HTTP cache root directory.
    ///
    /// When provided, endpoint and cover-art payload responses are cached
    /// through one shared cache layout:
    /// - `<cache_dir>/store/` CAS payload objects,
    /// - `<cache_dir>/media-tagger.json` key-to-hash metadata index.
    ///
    /// This keeps media-tagger cache semantics aligned with the workspace/user
    /// managed cache model and avoids dedicated per-tool directories inside
    /// `store/`.
    pub cache_dir: Option<PathBuf>,
    /// Cache expiry budget in seconds.
    ///
    /// Negative values disable cache expiry and keep rows indefinitely.
    pub cache_expiry_seconds: i64,
    /// Whether unresolved identity should fail the invocation.
    pub strict_identification: bool,
    /// Whether to emit extended `Picard`-compatible tags from available payloads.
    pub write_all_tags: bool,
    /// Whether to enrich metadata with `Picard`-compatible `coverart_*` tags.
    pub write_all_images: bool,
    /// Whether cover-art images should be embedded into saved tags.
    pub save_images_to_tags: bool,
    /// Whether embedding should keep only one front cover image.
    ///
    /// Mirrors Picard's default `embed_only_one_front_image = true` behavior:
    /// embed exactly one `front` image when present, otherwise embed none.
    pub embed_only_one_front_image: bool,
    /// Number of deterministic cover-art slots emitted for downstream apply.
    ///
    /// Managed workflow synthesis binds ffmpeg cover-image inputs to these
    /// slot files, so the internal tagger always writes all slot members:
    /// populated slots carry image bytes and "true" flags, unused slots carry
    /// empty payloads and empty flags.
    pub cover_art_slot_count: usize,
    /// Ordered provider selector list for cover-art discovery.
    pub ca_providers: String,
    /// CAA type-selector expression controlling which cover-art image kinds
    /// are eligible for embedding/tag metadata.
    pub caa_image_types: String,
    /// Requested CAA image-size selector.
    pub caa_image_size: String,
    /// Whether only CAA entries approved by the CAA moderation flow should be
    /// considered.
    pub caa_approved_only: bool,
    /// Whether existing embedded images should be preserved when
    /// `clear_existing_tags` is enabled.
    pub preserve_images: bool,
    /// Whether existing textual tags should be cleared before applying newly
    /// resolved metadata.
    pub clear_existing_tags: bool,
    /// Whether metadata/tag writing is enabled for this invocation.
    pub enable_tag_saving: bool,
    /// Whether release relationships should be considered by provider logic
    /// that depends on relationship metadata.
    pub release_ars: bool,
    /// Optional direct recording MBID override.
    ///
    /// Sentinel values:
    /// - `auto`/empty => allow AcoustID autodetection,
    /// - `none` => disable AcoustID autodetection entirely.
    pub recording_mbid: Option<String>,
    /// Optional direct release MBID override.
    ///
    /// Sentinel values:
    /// - `auto`/empty => treat as unspecified release MBID,
    /// - `none` => treat as unspecified release MBID and disable AcoustID
    ///   autodetection entirely.
    pub release_mbid: Option<String>,
}

/// Normalized MBID override mode from runtime input text.
#[derive(Debug, Clone, PartialEq, Eq)]
enum MbidOverride {
    /// Caller requested autodetection by omitting MBID or setting `auto`.
    Auto,
    /// Caller explicitly disabled autodetection with `none`.
    NoneSentinel,
    /// Caller provided one concrete MBID value.
    Explicit(String),
}

impl MbidOverride {
    /// Parses one optional MBID override text value.
    #[must_use]
    fn parse(raw: Option<&str>) -> Self {
        let Some(value) = normalize_optional_text(raw) else {
            return Self::Auto;
        };

        if value.eq_ignore_ascii_case("auto") {
            return Self::Auto;
        }
        if value.eq_ignore_ascii_case("none") {
            return Self::NoneSentinel;
        }

        Self::Explicit(value)
    }

    /// Returns true when this override disables AcoustID autodetection.
    #[must_use]
    const fn disables_autodetect(&self) -> bool {
        matches!(self, Self::NoneSentinel)
    }

    /// Returns one explicit MBID value when present.
    #[must_use]
    fn explicit_value(&self) -> Option<String> {
        match self {
            Self::Explicit(value) => Some(value.clone()),
            Self::Auto | Self::NoneSentinel => None,
        }
    }
}

/// Executes one full internal tagging pipeline.
///
/// # Errors
///
/// Returns an error when metadata identification/fetching fails in strict mode,
/// when fallback metadata output cannot be written, or when upstream tool/API
/// calls fail in a way that prevents deterministic output generation.
pub async fn run_internal_media_tagger(options: InternalMediaTaggerOptions) -> anyhow::Result<()> {
    if let Some(input_path) = options.input_path.as_ref()
        && !input_path.exists()
    {
        bail!("input media path '{}' does not exist", input_path.display());
    }

    let output_path = options.output_path.clone();
    let fallback_input = options.input_path.clone();
    match run_internal_media_tagger_impl(options).await {
        Ok(()) => Ok(()),
        Err(error) => {
            let fallback_map = fallback_ffmetadata_map(fallback_input.as_deref()).await;
            if let Err(write_error) = write_ffmetadata_document(&output_path, &fallback_map) {
                return Err(error.context(format!(
                    "failed to persist fallback ffmetadata output '{}' after tagging error: {write_error}",
                    output_path.display()
                )));
            }

            Err(error)
        }
    }
}

/// Executes one full internal tagging pipeline without fallback-output handling.
#[expect(
    clippy::too_many_lines,
    reason = "this item intentionally keeps end-to-end tagging flow explicit for maintainability"
)]
async fn run_internal_media_tagger_impl(options: InternalMediaTaggerOptions) -> anyhow::Result<()> {
    let resolved_input = options
        .input_path
        .as_ref()
        .map(|input_path| input_path.canonicalize().unwrap_or_else(|_| input_path.clone()));
    let media_tagger_cache = MediaTaggerHttpCache::new(
        options.cache_dir.clone(),
        CacheExpiryPolicy::from_seconds(options.cache_expiry_seconds),
    );
    if !options.enable_tag_saving {
        let fallback_map = if options.clear_existing_tags {
            BTreeMap::new()
        } else {
            fallback_ffmetadata_map(resolved_input.as_deref()).await
        };

        persist_cover_art_slot_artifacts(
            &options.output_path,
            &[],
            options.cover_art_slot_count,
            &media_tagger_cache,
        )
        .await
        .context("writing media-tagger cover-art slot artifacts")?;
        write_ffmetadata_document(&options.output_path, &fallback_map)?;
        return Ok(());
    }

    let recording_override = MbidOverride::parse(options.recording_mbid.as_deref());
    let release_override = MbidOverride::parse(options.release_mbid.as_deref());
    let disable_autodetect =
        recording_override.disables_autodetect() || release_override.disables_autodetect();

    let mut detected_recording_mbid = recording_override.explicit_value();
    let mut detected_release_mbid = release_override.explicit_value();

    if !disable_autodetect && detected_recording_mbid.is_none() {
        let Some(resolved_input) = resolved_input.as_ref() else {
            bail!(
                "input media is required when --recording-mbid is not provided (fingerprint autodetection path)"
            );
        };

        let acoustid_api_key = require_acoustid_api_key_for_lookup(resolve_acoustid_api_key(
            options.acoustid_api_key.as_deref(),
        ))?;
        let fingerprint_payload = decode_and_fingerprint_audio(resolved_input)
            .await
            .context("decoding media for fingerprinting")?;
        let match_result = lookup_acoustid_match(
            &options.acoustid_endpoint,
            &acoustid_api_key,
            &fingerprint_payload.fingerprint,
            fingerprint_payload.duration_seconds,
        )
        .await
        .with_context(|| {
            format!(
                "resolving recording identity through AcoustID endpoint '{}'",
                options.acoustid_endpoint
            )
        })?;

        detected_recording_mbid = detected_recording_mbid.or(match_result.recording_mbid);
        detected_release_mbid = detected_release_mbid.or(match_result.release_mbid);
    }

    if disable_autodetect && detected_recording_mbid.is_none() {
        let fallback_map = fallback_ffmetadata_map(resolved_input.as_deref()).await;
        write_ffmetadata_document(&options.output_path, &fallback_map)?;
        return Ok(());
    }

    let Some(recording_mbid) = detected_recording_mbid else {
        if options.strict_identification {
            bail!(
                "could not resolve recording MBID (set --recording-mbid or provide AcoustID API key via --acoustid-api-key/{ACOUSTID_API_KEY_ENV})"
            );
        }

        let fallback_map = fallback_ffmetadata_map(resolved_input.as_deref()).await;
        write_ffmetadata_document(&options.output_path, &fallback_map)?;
        return Ok(());
    };

    let metadata_payload = fetch_musicbrainz_payloads(
        &recording_mbid,
        detected_release_mbid.as_deref(),
        options.write_all_tags,
        options.release_ars,
        &media_tagger_cache,
    )
    .await
    .with_context(|| {
        format!(
            "fetching MusicBrainz entities from '{}' for recording '{}'",
            options.musicbrainz_endpoint, recording_mbid
        )
    });

    let metadata_payload = match metadata_payload {
        Ok(payload) => payload,
        Err(error) => {
            if options.strict_identification {
                return Err(error);
            }
            let fallback_map = fallback_ffmetadata_map(resolved_input.as_deref()).await;
            write_ffmetadata_document(&options.output_path, &fallback_map)?;
            return Ok(());
        }
    };

    let mut ffmetadata_map = build_ffmetadata_map(&metadata_payload, options.write_all_tags)
        .context("building FFmetadata mapping from MusicBrainz payload")?;

    let mut selected_cover_art = Vec::new();
    if options.write_all_images && options.save_images_to_tags {
        let discovered_cover_art = collect_musicbrainz_cover_art(
            &metadata_payload,
            &media_tagger_cache,
            &options.ca_providers,
            &options.caa_image_types,
            &options.caa_image_size,
            options.caa_approved_only,
            options.release_ars,
        )
        .await
        .context("collecting MusicBrainz cover-art entries")?;
        selected_cover_art = select_cover_art_for_tag_embedding(
            &discovered_cover_art,
            options.embed_only_one_front_image,
        );
        insert_musicbrainz_image_tags(&mut ffmetadata_map, &selected_cover_art);
    }

    if !options.clear_existing_tags
        && let Some(input_path) = resolved_input.as_deref()
    {
        let existing_metadata = extract_existing_ffmetadata_map(input_path).await?;
        if !existing_metadata.is_empty() {
            let mut merged = existing_metadata;
            for (key, value) in ffmetadata_map {
                merged.insert(key, value);
            }
            ffmetadata_map = merged;
        }
    }

    if ffmetadata_map.is_empty() {
        if options.strict_identification {
            bail!("resolved metadata payload did not produce any FFmetadata entries");
        }
        let fallback_map = fallback_ffmetadata_map(resolved_input.as_deref()).await;
        write_ffmetadata_document(&options.output_path, &fallback_map)?;
        return Ok(());
    }

    persist_cover_art_slot_artifacts(
        &options.output_path,
        &selected_cover_art,
        options.cover_art_slot_count,
        &media_tagger_cache,
    )
    .await
    .context("writing media-tagger cover-art slot artifacts")?;

    write_ffmetadata_document(&options.output_path, &ffmetadata_map)
}

/// Builds fallback FFmetadata content from existing input media metadata.
///
/// When input media is available, fallback output should preserve existing tags
/// to avoid destructive metadata clears in downstream ffmpeg apply stages.
async fn fallback_ffmetadata_map(input_path: Option<&Path>) -> BTreeMap<String, String> {
    let Some(input_path) = input_path else {
        return BTreeMap::new();
    };

    extract_existing_ffmetadata_map(input_path).await.unwrap_or_default()
}

/// Extracts current global metadata from one media file through ffmpeg
/// FFmetadata export and decodes it into a key/value map.
async fn extract_existing_ffmetadata_map(
    input_path: &Path,
) -> anyhow::Result<BTreeMap<String, String>> {
    let ffmpeg_executable = resolve_ffmpeg_executable();
    let output = Command::new(&ffmpeg_executable)
        .arg("-v")
        .arg("error")
        // Probe just enough for container metadata so we avoid wasting time
        // on large stream tables when the FFmetadata muxer only needs tags.
        .arg("-probesize")
        .arg("32k")
        .arg("-analyzeduration")
        .arg("0")
        .arg("-i")
        .arg(input_path)
        // Skip codec initialization: the FFmetadata muxer reads only
        // container-level tags and does not need to decode any streams.
        .arg("-vn")
        .arg("-an")
        .arg("-sn")
        .arg("-dn")
        .arg("-f")
        .arg("ffmetadata")
        .arg("-")
        .output()
        .await
        .with_context(|| {
            format!(
                "running '{ffmpeg_executable}' to extract existing metadata from '{}'",
                input_path.display()
            )
        })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "ffmpeg metadata extraction failed for '{}' with status {}: {stderr}",
            input_path.display(),
            output.status
        );
    }

    let text = String::from_utf8(output.stdout).with_context(|| {
        format!(
            "ffmpeg metadata extraction output for '{}' was not valid UTF-8",
            input_path.display()
        )
    })?;

    Ok(parse_ffmetadata_global_map(&text))
}
