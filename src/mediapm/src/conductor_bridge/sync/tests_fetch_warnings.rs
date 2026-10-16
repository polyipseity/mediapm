//! The fetch-warning channel: a chain member that failed is reported even when
//! a later member served the source.
//!
//! The conductor layer detects the failure and carries it on
//! `DownloadedSources.warnings` (its own test pins that). This module pins the
//! mediapm-side hop, without which the list would be dropped on the floor and
//! a tool whose declared source is not serving would report a clean sync.

use crate::output::ProgressScreen;
use mediapm_conductor::NickelDocument;
use std::collections::BTreeMap;

use super::*;

/// Applies one `Fetched` outcome for an ffmpeg payload carrying `warnings`,
/// and returns the sync report that pass produced.
///
/// Mirrors the production shape closely enough that the assertion is about the
/// merge and not about the fixture: a non-empty content map (so the tool gets
/// a content-addressed key), one os-keyed executable path, and provider
/// metadata of the kind ffmpeg actually resolves.
fn report_after_fetched(warnings: Vec<String>) -> ToolSyncReport {
    let screen = ProgressScreen::disabled();
    let bar: Arc<dyn ProgressBarApi> = Arc::new(screen.add_bar(0, "fetched"));
    let progress = OverallProgress::new(&bar);
    let entry = ProvisionEntry {
        tool_id: "ffmpeg".to_string(),
        tool_requirement: ToolRequirement::default(),
        kind: EntryKind::Explicit,
    };
    let mut generated_doc = NickelDocument::default();
    let mut tool_runtimes = BTreeMap::new();
    let mut report = ToolSyncReport::default();
    let mut live_state = std::collections::HashMap::new();
    let mut pruned_tools = 0usize;
    let inherited_env_vars = BTreeMap::new();

    let content_map = BTreeMap::from([(
        "macos/aarch64/ffmpeg".to_string(),
        format!("blake3:{}", blake3::hash(b"payload").to_hex()),
    )]);
    let os_exec_paths = BTreeMap::from([("macos".to_string(), "aarch64/ffmpeg".to_string())]);

    apply_entry_outcome(
        &entry,
        EntryOutcome::Fetched {
            tool_id: "ffmpeg".to_string(),
            is_builtin_code: false,
            already_exists: false,
            payload: FetchedToolPayload {
                content_map,
                os_exec_paths,
                human_readable_version: "autobuild-2025-07-15-12-00".to_string(),
                canonical_version: "autobuild-2025-07-15-12-00+8.1.3-1".to_string(),
                resolved_tag: Some("autobuild-2025-07-15-12-00".to_string()),
                resolved_version: None,
                resolved_vcs_hash: None,
                warnings,
            },
        },
        &mut generated_doc,
        &mut tool_runtimes,
        &mut report,
        &mut live_state,
        &mut pruned_tools,
        &inherited_env_vars,
        0,
        &progress,
    );
    report
}

/// A payload whose source was served by a later chain member carries the
/// candidates that failed first; every one of them must reach the sync report.
///
/// That list is what the CLI prints as a `Δ` line and what turns the run's
/// exit status into `Warning`, so losing it here makes a covered source outage
/// look exactly like a clean sync.
#[test]
fn fetched_payload_warnings_reach_the_sync_report() {
    let warning = "tool ffmpeg os macos (arch aarch64): candidate failed (HTTP 404 at \
                   https://ffmpeg.martin-riedl.de/redirect/latest/macos/arm64/snapshot/ffmpeg.zip); \
                   served by \
                   https://repo.jellyfin.org/files/ffmpeg/macos/latest-8.x/arm64/\
                   jellyfin-ffmpeg_8.1.3-1_portable_macarm64-gpl.tar.xz"
        .to_string();

    let report = report_after_fetched(vec![warning.clone()]);

    assert_eq!(
        report.warnings,
        vec![warning],
        "the fetch warning must survive the apply pass, or the operator is told the sync was clean"
    );
    assert_eq!(
        report.tools_added, 1,
        "the tool still registers: a covered chain failure is a warning, not a failure"
    );
}

/// A source served by its first candidate reports nothing, so the warning
/// channel cannot be tripped by a healthy fetch.
#[test]
fn a_clean_fetch_adds_no_warnings_to_the_report() {
    let report = report_after_fetched(Vec::new());

    assert!(
        report.warnings.is_empty(),
        "a first-candidate fetch is silent, got: {:?}",
        report.warnings
    );
}
