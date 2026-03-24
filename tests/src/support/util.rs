use serde_json::json;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use mediapm::support::util::{merge_json_object, now_rfc3339, sort_json_value};

#[test]
fn now_rfc3339_is_parseable() {
    let timestamp = now_rfc3339().expect("timestamp should generate");
    let parsed = OffsetDateTime::parse(&timestamp, &Rfc3339).expect("timestamp should parse");

    assert!(parsed.year() >= 2020);
}

#[test]
fn sort_json_value_orders_nested_object_keys() {
    let mut value = json!({
        "z": 1,
        "a": { "y": 2, "x": 1 }
    });

    sort_json_value(&mut value);

    let serialized = serde_json::to_string(&value).expect("json should serialize");
    assert!(
        serialized.find("\"a\"").expect("a key should exist")
            < serialized.find("\"z\"").expect("z key should exist")
    );
}

#[test]
fn merge_json_object_overlays_nested_values() {
    let mut base = json!({
        "tags": {
            "artist": "Old Artist",
            "album": "Old Album"
        },
        "technical": {
            "bitrate": 320
        }
    });

    let overlay = json!({
        "tags": {
            "artist": "New Artist"
        },
        "technical": {
            "sample_rate": 48000
        }
    });

    merge_json_object(&mut base, &overlay);

    assert_eq!(base["tags"]["artist"], "New Artist");
    assert_eq!(base["tags"]["album"], "Old Album");
    assert_eq!(base["technical"]["bitrate"], 320);
    assert_eq!(base["technical"]["sample_rate"], 48000);
}
