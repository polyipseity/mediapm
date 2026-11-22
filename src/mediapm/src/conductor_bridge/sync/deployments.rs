//! The deployment state the generated document resolves against.
//!
//! A tool's active generated-document key is decided by the newest deployment
//! record for that tool id, not by key order in the document: two keys for one
//! tool share a bare id, so only state knows which one the last sync wrote.

use std::collections::BTreeMap;

use crate::config::ToolRegistryEntry;

/// The active generated-document key for each deployed tool id.
///
/// Built from [`MediaPmState::managed_tools`](crate::config::MediaPmState::managed_tools)
/// at the point the generated document is read, so resolution answers against the
/// state this run observed rather than against key order.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ToolDeployments {
    active_keys: BTreeMap<String, String>,
}

impl ToolDeployments {
    /// Builds the index from persisted deployment records.
    ///
    /// For each tool id the newest record by `deployed_at` wins, and its
    /// content-map hash names the key: a non-empty hash gives `{tool_id}@{hash}`,
    /// an empty hash gives the bare id. That is how a payload-free builtin
    /// resolves without a special case.
    #[must_use]
    pub fn from_records(records: &[ToolRegistryEntry]) -> Self {
        let mut newest: BTreeMap<&str, &ToolRegistryEntry> = BTreeMap::new();
        for record in records {
            let replace = newest
                .get(record.tool_id.as_str())
                .is_none_or(|current| record.deployed_at >= current.deployed_at);
            if replace {
                newest.insert(record.tool_id.as_str(), record);
            }
        }
        let active_keys = newest
            .into_iter()
            .map(|(tool_id, record)| (tool_id.to_string(), record_key(record)))
            .collect();
        Self { active_keys }
    }

    /// An empty index, for a document with no deployment record to consult.
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// The active generated-document key for `tool_name`, if a deployment named one.
    #[must_use]
    pub fn active_key(&self, tool_name: &str) -> Option<&str> {
        self.active_keys.get(tool_name).map(String::as_str)
    }
}

/// The generated-document key a record names.
///
/// A record with a content-map hash names `{tool_id}@{content_map_hash}`; a
/// record with no content-map hash names the bare id.
fn record_key(record: &ToolRegistryEntry) -> String {
    if record.content_map_hash.is_empty() {
        record.tool_id.clone()
    } else {
        format!("{}@{}", record.tool_id, record.content_map_hash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mediapm_utils::Timestamp;

    fn record(tool_id: &str, content_map_hash: &str, deployed_at_nanos: u64) -> ToolRegistryEntry {
        ToolRegistryEntry {
            tool_id: tool_id.to_string(),
            version: String::new(),
            canonical_version: String::new(),
            content_map_hash: content_map_hash.to_string(),
            deployed_at: Timestamp::from_unix_nanos(deployed_at_nanos),
            resolved_tag: None,
            resolved_version: None,
            resolved_vcs_hash: None,
        }
    }

    /// The newest record names the key, but the key needs the content-map hash:
    /// two sibling keys for one tool share its bare id, so `deployed_at` alone
    /// cannot say which one is live.
    #[test]
    fn the_newest_record_reconstructs_its_key() {
        let deployments = ToolDeployments::from_records(&[
            record("ffmpeg", "aaa", 10),
            record("ffmpeg", "bbb", 20),
            record("deno", "", 5),
        ]);
        assert_eq!(deployments.active_key("ffmpeg"), Some("ffmpeg@bbb"));
        assert_eq!(deployments.active_key("deno"), Some("deno"));
        assert_eq!(deployments.active_key("yt-dlp"), None);
    }

    /// A payload-free tool's record has no content-map hash, so its key is the
    /// bare id. That is how a host builtin resolves without a special case.
    #[test]
    fn an_empty_content_map_names_the_bare_key() {
        let deployments = ToolDeployments::from_records(&[
            record("media-tagger", "", 1),
            record("ffmpeg", "cc", 9),
        ]);
        assert_eq!(deployments.active_key("media-tagger"), Some("media-tagger"));
        assert_eq!(deployments.active_key("ffmpeg"), Some("ffmpeg@cc"));
    }
}
