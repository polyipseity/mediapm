//! What the summary means by `added`, `updated` and `skipped`.
//!
//! A tool is counted by its identity against the record the previous pass left
//! behind, so a re-registration of the artifact already deployed is not an
//! update. These tests pin the classifier the counters read.

use super::*;

/// A record carrying only the fields the classifier reads.
fn record(tool_id: &str, canonical_version: &str, content_map_hash: &str) -> ToolRegistryEntry {
    ToolRegistryEntry {
        tool_id: tool_id.to_string(),
        version: "informational".to_string(),
        canonical_version: canonical_version.to_string(),
        content_map_hash: content_map_hash.to_string(),
        deployed_at: mediapm_utils::Timestamp::now(),
        resolved_tag: None,
        resolved_version: None,
        resolved_vcs_hash: None,
    }
}

/// Classification reads the tool's identity, never whether its entry exists.
///
/// The table is the whole rule: an absent record is an addition, an identity
/// that matches the recorded one is not a change, and either half of the
/// identity differing is an update.
#[test]
fn strict_classify_tool_change_reads_identity_not_presence() {
    let builtin = record("import", "0.0.0+deadbeef", "");
    let payload = record("yt-dlp", "2024.01.01", "blake3:aaa");

    let cases: [(&str, Option<&ToolRegistryEntry>, &ToolRegistryEntry, CountedChange); 5] = [
        ("no record at all", None, &builtin, CountedChange::Added),
        ("same canonical version and hash", Some(&payload), &payload, CountedChange::Unchanged),
        (
            "a different canonical version",
            Some(&payload),
            &record("yt-dlp", "2024.02.02", "blake3:aaa"),
            CountedChange::Updated,
        ),
        (
            "the same version with different bytes",
            Some(&payload),
            &record("yt-dlp", "2024.01.01", "blake3:bbb"),
            CountedChange::Updated,
        ),
        (
            "a builtin, whose identity is its build",
            Some(&builtin),
            &builtin,
            CountedChange::Unchanged,
        ),
    ];

    for (case, prior, new, expected) in cases {
        assert_eq!(
            classify_tool_change(prior, new),
            expected,
            "{case} must classify as {expected:?}",
        );
    }
}

/// The newest record for a tool is the one the tool ran as.
///
/// The registry keeps every deployment, so a pass that classified against an
/// older record would report an update for a tool whose current deployment is
/// the one this pass reproduces.
#[test]
fn strict_prior_record_reads_the_newest_deployment() {
    let older = ToolRegistryEntry {
        deployed_at: mediapm_utils::Timestamp::from_unix_secs(1_000),
        ..record("yt-dlp", "2024.01.01", "blake3:aaa")
    };
    let newer = ToolRegistryEntry {
        deployed_at: mediapm_utils::Timestamp::from_unix_secs(2_000),
        ..record("yt-dlp", "2024.02.02", "blake3:bbb")
    };
    let mut live_state = HashMap::new();
    live_state.insert("yt-dlp".to_string(), vec![newer.clone(), older]);

    let prior = prior_tool_record(&live_state, "yt-dlp").expect("the newest record is the prior");

    assert_eq!(prior.deployed_at, newer.deployed_at, "the newest deployment is the prior record");
    assert!(
        prior_tool_record(&live_state, "ffmpeg").is_none(),
        "a tool the registry never recorded has no prior record",
    );
}
