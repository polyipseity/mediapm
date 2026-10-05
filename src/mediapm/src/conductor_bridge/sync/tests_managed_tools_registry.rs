//! The managed-tool index in `state.managed_tools` and the provenance backfill applied to it.

use super::*;

#[test]
fn index_managed_tools_empty() {
    let map = index_managed_tools(&[]);
    assert!(map.is_empty());
}

#[test]
fn index_managed_tools_single_tool() {
    let entries = vec![ToolRegistryEntry {
        tool_id: "ffmpeg".to_string(),
        version: String::new(),
        canonical_version: "v7.1".to_string(),
        content_map_hash: String::new(),
        deployed_at: mediapm_utils::Timestamp::default(),
        resolved_tag: None,
        resolved_version: None,
        resolved_vcs_hash: None,
    }];
    let map = index_managed_tools(&entries);
    assert_eq!(map.len(), 1);
    assert_eq!(map["ffmpeg"].len(), 1);
}

#[test]
fn index_managed_tools_multi_instance() {
    let entries = vec![
        ToolRegistryEntry {
            tool_id: "ffmpeg".to_string(),
            version: String::new(),
            canonical_version: "ffmpeg-v7.1".to_string(),
            content_map_hash: String::new(),
            deployed_at: mediapm_utils::Timestamp::default(),
            resolved_tag: None,
            resolved_version: None,
            resolved_vcs_hash: None,
        },
        ToolRegistryEntry {
            tool_id: "ffmpeg".to_string(),
            version: String::new(),
            canonical_version: "ffmpeg-v6.0".to_string(),
            content_map_hash: String::new(),
            deployed_at: mediapm_utils::Timestamp::default(),
            resolved_tag: None,
            resolved_version: None,
            resolved_vcs_hash: None,
        },
    ];
    let map = index_managed_tools(&entries);
    assert_eq!(map.len(), 1);
    assert_eq!(map["ffmpeg"].len(), 2);
}

#[test]
fn regression_inactive_index_managed_tools() {
    // An entry with empty content_map_hash is still indexed (the inactive
    // filter is applied at skip-check time, not at index time).
    let entries = vec![ToolRegistryEntry {
        tool_id: "ffmpeg".to_string(),
        version: String::new(),
        canonical_version: "ffmpeg-v7.1".to_string(),
        content_map_hash: String::new(), // inactive
        deployed_at: mediapm_utils::Timestamp::default(),
        resolved_tag: None,
        resolved_version: None,
        resolved_vcs_hash: None,
    }];
    let map = index_managed_tools(&entries);
    assert_eq!(map.len(), 1, "inactive entry should still be indexed");
    assert_eq!(map["ffmpeg"].len(), 1);
}

#[test]
fn regression_active_only_skips() {
    // Two entries with the same canonical_version and non-empty
    // content_map_hash → both active, skip check matches.
    let entries = vec![
        ToolRegistryEntry {
            tool_id: "ffmpeg".to_string(),
            version: String::new(),
            canonical_version: "ffmpeg-v7.1".to_string(),
            content_map_hash: "blake3:abc".to_string(),
            deployed_at: mediapm_utils::Timestamp::default(),
            resolved_tag: None,
            resolved_version: None,
            resolved_vcs_hash: None,
        },
        ToolRegistryEntry {
            tool_id: "ffmpeg".to_string(),
            version: String::new(),
            canonical_version: "ffmpeg-v7.1".to_string(),
            content_map_hash: "blake3:def".to_string(),
            deployed_at: mediapm_utils::Timestamp::default(),
            resolved_tag: None,
            resolved_version: None,
            resolved_vcs_hash: None,
        },
    ];
    let map = index_managed_tools(&entries);
    assert_eq!(map.len(), 1);
    assert_eq!(map["ffmpeg"].len(), 2);
}

fn backfill_entry(
    tool_id: &str,
    canonical_version: &str,
    resolved_tag: Option<&str>,
    resolved_version: Option<&str>,
    resolved_vcs_hash: Option<&str>,
) -> ToolRegistryEntry {
    ToolRegistryEntry {
        tool_id: tool_id.to_string(),
        version: String::new(),
        canonical_version: canonical_version.to_string(),
        content_map_hash: String::new(),
        deployed_at: mediapm_utils::Timestamp::default(),
        resolved_tag: resolved_tag.map(str::to_string),
        resolved_version: resolved_version.map(str::to_string),
        resolved_vcs_hash: resolved_vcs_hash.map(str::to_string),
    }
}

#[test]
fn apply_resolved_field_backfills_fills_none_fields_in_place() {
    let mut managed = vec![backfill_entry("ffmpeg", "ffmpeg-v7.1", None, None, None)];
    let backfills =
        vec![backfill_entry("ffmpeg", "ffmpeg-v7.1", Some("autobuild-2025-07-15"), None, None)];
    apply_resolved_field_backfills(&mut managed, &backfills);
    assert_eq!(managed[0].resolved_tag.as_deref(), Some("autobuild-2025-07-15"));
    // Why-empty fields stay `None` — backfills never invent values.
    assert_eq!(managed[0].resolved_version, None);
    assert_eq!(managed[0].resolved_vcs_hash, None);
}

#[test]
fn apply_resolved_field_backfills_never_overwrites_some() {
    let mut managed =
        vec![backfill_entry("yt-dlp", "yt-dlp-v2", Some("v2"), Some("2.0"), Some("abc"))];
    let backfills =
        vec![backfill_entry("yt-dlp", "yt-dlp-v2", Some("DIFFERENT"), Some("9.9"), Some("def"))];
    apply_resolved_field_backfills(&mut managed, &backfills);
    assert_eq!(managed[0].resolved_tag.as_deref(), Some("v2"));
    assert_eq!(managed[0].resolved_version.as_deref(), Some("2.0"));
    assert_eq!(managed[0].resolved_vcs_hash.as_deref(), Some("abc"));
}

#[test]
fn apply_resolved_field_backfills_noop_when_unchanged() {
    let mut managed = vec![backfill_entry("yt-dlp", "yt-dlp-v2", Some("v2"), None, None)];
    let backfills = vec![backfill_entry("yt-dlp", "yt-dlp-v2", Some("v2"), None, None)];
    apply_resolved_field_backfills(&mut managed, &backfills);
    assert_eq!(managed[0].resolved_tag.as_deref(), Some("v2"));
    assert_eq!(managed[0].resolved_version, None);
}

#[test]
fn apply_resolved_field_backfills_preserves_identity_fields() {
    let mut managed = vec![ToolRegistryEntry {
        tool_id: "ffmpeg".to_string(),
        version: "7.1".to_string(),
        canonical_version: "ffmpeg-v7.1".to_string(),
        content_map_hash: "blake3:abc".to_string(),
        deployed_at: mediapm_utils::Timestamp::from_unix_secs(1234),
        resolved_tag: None,
        resolved_version: None,
        resolved_vcs_hash: None,
    }];
    let backfills =
        vec![backfill_entry("ffmpeg", "ffmpeg-v7.1", Some("tag"), Some("ver"), Some("hash"))];
    apply_resolved_field_backfills(&mut managed, &backfills);
    assert_eq!(managed[0].version, "7.1");
    assert_eq!(managed[0].canonical_version, "ffmpeg-v7.1");
    assert_eq!(managed[0].content_map_hash, "blake3:abc");
    assert_eq!(managed[0].deployed_at, mediapm_utils::Timestamp::from_unix_secs(1234));
}

#[test]
fn apply_resolved_field_backfills_no_matching_entry_ignored() {
    let mut managed = vec![backfill_entry("yt-dlp", "yt-dlp-v2", None, None, None)];
    let backfills =
        vec![backfill_entry("ffmpeg", "ffmpeg-v7.1", Some("tag"), Some("ver"), Some("hash"))];
    apply_resolved_field_backfills(&mut managed, &backfills);
    assert_eq!(managed[0].resolved_tag, None);
    assert_eq!(managed[0].resolved_version, None);
    assert_eq!(managed[0].resolved_vcs_hash, None);
}

#[test]
fn apply_resolved_field_backfills_entry_not_in_backfills_unchanged() {
    let mut managed =
        vec![backfill_entry("sd", "sd-v1.1.0", Some("v1.1.0"), Some("1.1.0"), Some("xyz"))];
    let backfills =
        vec![backfill_entry("yt-dlp", "yt-dlp-v2", Some("v2"), Some("2.0"), Some("abc"))];
    apply_resolved_field_backfills(&mut managed, &backfills);
    assert_eq!(managed[0].resolved_tag.as_deref(), Some("v1.1.0"));
    assert_eq!(managed[0].resolved_version.as_deref(), Some("1.1.0"));
    assert_eq!(managed[0].resolved_vcs_hash.as_deref(), Some("xyz"));
}
