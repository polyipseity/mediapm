//! Reusable utility helpers.
//!
//! These helpers primarily support two cross-cutting concerns:
//! - deterministic serialization for stable sidecar/config diffs,
//! - predictable timestamp formatting for provenance fields.

use anyhow::Result;
use serde_json::Value;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

/// Return the current UTC timestamp formatted as RFC3339.
///
/// RFC3339 is chosen because it is both machine-parseable and readable in logs,
/// diffs, and user reports.
pub fn now_rfc3339() -> Result<String> {
    Ok(OffsetDateTime::now_utc().format(&Rfc3339)?)
}

/// Recursively sort all JSON object keys in place.
///
/// This is used to make sidecar serialization deterministic for reproducible
/// diffs and easier review.
pub fn sort_json_value(value: &mut Value) {
    match value {
        Value::Array(array) => {
            for item in array {
                sort_json_value(item);
            }
        }
        Value::Object(map) => {
            let mut entries: Vec<(String, Value)> =
                map.iter().map(|(key, value)| (key.clone(), value.clone())).collect();

            for (_, entry_value) in &mut entries {
                sort_json_value(entry_value);
            }

            entries.sort_by(|(left_key, _), (right_key, _)| left_key.cmp(right_key));

            map.clear();
            for (key, sorted_value) in entries {
                map.insert(key, sorted_value);
            }
        }
        _ => {}
    }
}

/// Merge `overlay` object values into `base` recursively.
///
/// Non-object values replace the previous value entirely.
///
/// This merge policy intentionally favors explicit overlay values, which keeps
/// metadata override behavior straightforward for users and tests.
pub fn merge_json_object(base: &mut Value, overlay: &Value) {
    let (Value::Object(base_map), Value::Object(overlay_map)) = (base, overlay) else {
        return;
    };

    for (key, overlay_value) in overlay_map {
        match (base_map.get_mut(key), overlay_value) {
            (Some(base_existing), Value::Object(_)) => {
                merge_json_object(base_existing, overlay_value);
            }
            _ => {
                base_map.insert(key.clone(), overlay_value.clone());
            }
        }
    }
}
