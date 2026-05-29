use std::collections::BTreeMap;

use tempfile::tempdir;

use super::acoustid::require_acoustid_api_key_for_lookup;
use super::cover_art::{
    CacheExpiryPolicy, CoverArtArchiveImage, MediaTaggerHttpCache, SelectedCoverArt,
    insert_musicbrainz_image_tags, normalized_cover_art_types, persist_cover_art_slot_artifacts,
    select_cover_art_for_tag_embedding, select_highest_quality_cover_url,
};
use super::ffmetadata::parse_ffmetadata_global_map;
use super::musicbrainz::{insert_extended_picard_tags, musicbrainz_payload_cache_key};
use super::util::resolve_ffmpeg_executable_from_configured_path;
use super::*;

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

/// Protects media-tagger JSONC cache persistence so cached cover-art rows
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
    assert!(root.path().join("cache-store").join("media-tagger.jsonc").exists());
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
    let payload_path = "/tmp/demo/.mediapm/tools/mediapm.tools.ffmpeg+demo@v1/payload/macos/ffmpeg";

    assert_eq!(resolve_ffmpeg_executable_from_configured_path(Some(payload_path)), payload_path);
}
