use serde_json::json;

use mediapm::domain::model::{Blake3Hash, MediaRecord, VariantLineage, VariantRecord};

fn hash_with_last_byte(byte: u8) -> Blake3Hash {
    let mut bytes = [0_u8; 32];
    bytes[31] = byte;
    Blake3Hash::from_bytes(bytes)
}

#[test]
fn media_record_initializes_with_original_variant() {
    let variant = VariantRecord {
        variant_hash: hash_with_last_byte(1),
        object_relpath: ".mediapm/objects/blake3/00/example".to_owned(),
        byte_size: 42,
        container: Some("flac".to_owned()),
        probe: json!({"ok": true}),
        metadata: json!({"tags": {}}),
        lineage: VariantLineage { parent_variant_hash: None, edit_event_ids: vec![] },
    };

    let record = MediaRecord::new_initial(
        "file:///tmp/song.flac".to_owned(),
        "2026-01-01T00:00:00Z".to_owned(),
        variant.clone(),
        json!({"seed": true}),
    );

    assert_eq!(record.variants.len(), 1);
    assert_eq!(record.original.original_variant_hash, variant.variant_hash);
    assert!(record.has_variant(&variant.variant_hash));
    assert_eq!(record.latest_variant(), Some(&variant));
}

#[test]
fn blake3_hash_serializes_and_deserializes_as_hex_string() {
    let hash = hash_with_last_byte(7);
    let value = serde_json::to_value(hash).expect("hash should serialize");
    let parsed: Blake3Hash = serde_json::from_value(value).expect("hash should deserialize");

    assert_eq!(parsed, hash_with_last_byte(7));
}
