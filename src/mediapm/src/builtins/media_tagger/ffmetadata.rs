//! FFmetadata document parsing helpers.

use std::collections::BTreeMap;

/// Parses ffmpeg `-f ffmetadata -` output into a global metadata map.
pub(super) fn parse_ffmetadata_global_map(document: &str) -> BTreeMap<String, String> {
    let mut metadata = BTreeMap::new();
    for raw_line in document.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with(';') {
            continue;
        }
        if line.starts_with('[') {
            break;
        }
        let Some((raw_key, raw_value)) = line.split_once('=') else {
            continue;
        };

        let key = decode_ffmetadata_escape(raw_key);
        let value = decode_ffmetadata_escape(raw_value);
        if key.trim().is_empty() || value.trim().is_empty() {
            continue;
        }
        metadata.insert(key, value);
    }

    metadata
}

/// Decodes ffmetadata backslash escapes for one scalar key/value text fragment.
pub(super) fn decode_ffmetadata_escape(value: &str) -> String {
    let mut decoded = String::new();
    let mut chars = value.chars();
    while let Some(character) = chars.next() {
        if character == '\\' {
            if let Some(next) = chars.next() {
                decoded.push(next);
            }
            continue;
        }
        decoded.push(character);
    }
    decoded
}
