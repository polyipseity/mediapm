//! Shared utility helpers for mediapm.

use serde_json::Value;

/// Returns the first non-empty string value found at any of the given keys
/// in the JSON object.
#[must_use]
pub(crate) fn first_non_empty_json_string(value: &Value, keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Some(s) = value.get(*key).and_then(|v| v.as_str()) {
            let trimmed = s.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn first_non_empty_json_string_finds_first_key() {
        let value = json!({
            "title": "Title",
            "fulltitle": "Full Title"
        });
        assert_eq!(
            first_non_empty_json_string(&value, &["title", "fulltitle"]),
            Some("Title".to_string())
        );
    }

    #[test]
    fn first_non_empty_json_string_skips_empty() {
        let value = json!({
            "title": "",
            "fulltitle": "Full Title"
        });
        assert_eq!(
            first_non_empty_json_string(&value, &["title", "fulltitle"]),
            Some("Full Title".to_string())
        );
    }

    #[test]
    fn first_non_empty_json_string_returns_none_for_missing() {
        let value = json!({});
        assert_eq!(first_non_empty_json_string(&value, &["title"]), None);
    }
}
