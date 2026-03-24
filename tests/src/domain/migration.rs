use serde_json::json;

use mediapm::domain::migration::migrate_to_latest;

#[test]
fn migrates_minimal_v0_payload_to_latest_shape() {
    let input = json!({
        "uri": "file:///tmp/song.flac",
        "variants": []
    });

    let (migrated, provenance) = migrate_to_latest(input).expect("migration should succeed");

    assert_eq!(migrated["schema_version"], 1);
    assert_eq!(migrated["canonical_uri"], "file:///tmp/song.flac");
    assert!(migrated.get("original").is_some());
    assert!(!provenance.is_empty());
}

#[test]
fn latest_schema_payload_has_no_new_provenance() {
    let input = json!({
        "schema_version": 1,
        "canonical_uri": "file:///tmp/song.flac",
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-01T00:00:00Z",
        "original": {
            "original_variant_hash": "0000000000000000000000000000000000000000000000000000000000000000",
            "original_metadata": {}
        },
        "variants": [],
        "edits": [],
        "provider_enrichment": {"musicbrainz": {"matches": [], "applied": {}}},
        "migration_provenance": []
    });

    let (migrated, provenance) = migrate_to_latest(input).expect("migration should succeed");

    assert_eq!(migrated["schema_version"], 1);
    assert!(provenance.is_empty());
}
