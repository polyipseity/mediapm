#[allow(unused_imports)]
use super::super::inner::*;
#[allow(unused_imports)]
use super::*;

// ---- render_prefix_components tests (Phase 2) -----------------------

#[test]
fn render_prefix_components_all_fields() {
    let result = super::super::inner::render_prefix_components(
        &super::super::inner::PrefixComponents {
            marker: String::new(),
            tool_name: "wget".into(),
            version: "1.2.3".into(),
            phase: "fch".into(),
            count: "2".into(),
            total: "5".into(),
        },
        super::super::TrackStatus::Active,
    );
    assert_eq!(result, "\x1b[0mwget 1.2.3 [fch] 2/5", "all fields rendered");
}

#[test]
fn render_prefix_components_empty_version() {
    let result = super::super::inner::render_prefix_components(
        &super::super::inner::PrefixComponents {
            marker: String::new(),
            tool_name: "wget".into(),
            version: String::new(),
            phase: "fch".into(),
            count: "2".into(),
            total: "5".into(),
        },
        super::super::TrackStatus::Active,
    );
    assert_eq!(result, "\x1b[0mwget [fch] 2/5", "version omitted when empty");
}

#[test]
fn render_prefix_components_only_tool_name() {
    let result = super::super::inner::render_prefix_components(
        &super::super::inner::PrefixComponents {
            marker: String::new(),
            tool_name: "wget".into(),
            version: String::new(),
            phase: String::new(),
            count: String::new(),
            total: String::new(),
        },
        super::super::TrackStatus::Active,
    );
    assert_eq!(result, "\x1b[0mwget", "only tool name when rest empty");
}

#[test]
fn render_prefix_components_empty_phase_and_count() {
    let result = super::super::inner::render_prefix_components(
        &super::super::inner::PrefixComponents {
            marker: String::new(),
            tool_name: "wget".into(),
            version: "1.2.3".into(),
            phase: String::new(),
            count: String::new(),
            total: String::new(),
        },
        super::super::TrackStatus::Active,
    );
    assert_eq!(result, "\x1b[0mwget 1.2.3", "version without phase/count");
}

#[test]
fn render_prefix_components_marker_failed() {
    let result = super::super::inner::render_prefix_components(
        &super::super::inner::PrefixComponents {
            marker: "F".into(),
            tool_name: "wget".into(),
            version: "1.2.3".into(),
            phase: "fch".into(),
            count: "2".into(),
            total: "5".into(),
        },
        super::super::TrackStatus::Failed,
    );
    assert_eq!(
        result, "\x1b[0m\x1b[31m[F]\x1b[0m wget 1.2.3 [fch] 2/5",
        "failed marker rendered red"
    );
}

#[test]
fn render_prefix_components_marker_warning() {
    let result = super::super::inner::render_prefix_components(
        &super::super::inner::PrefixComponents {
            marker: "W".into(),
            tool_name: "wget".into(),
            version: "1.2.3".into(),
            phase: "fch".into(),
            count: "2".into(),
            total: "5".into(),
        },
        super::super::TrackStatus::Warning,
    );
    assert_eq!(
        result, "\x1b[0m\x1b[33m[W]\x1b[0m wget 1.2.3 [fch] 2/5",
        "warning marker rendered yellow"
    );
}

#[test]
fn render_prefix_components_normal_states_no_marker() {
    for status in [super::super::TrackStatus::Active, super::super::TrackStatus::Success] {
        let result = super::super::inner::render_prefix_components(
            &super::super::inner::PrefixComponents {
                marker: String::new(),
                tool_name: "child".into(),
                version: String::new(),
                phase: String::new(),
                count: String::new(),
                total: String::new(),
            },
            status,
        );
        assert_eq!(result, "\x1b[0mchild", "{status:?}: no bracket for empty marker");
    }
}

#[test]
fn render_prefix_components_marker_uncolored_when_status_normal() {
    // A non-empty marker with a normal status renders uncolored (no SGR
    // color codes around the bracket).
    let result = super::super::inner::render_prefix_components(
        &super::super::inner::PrefixComponents {
            marker: "F".into(),
            tool_name: "wget".into(),
            version: String::new(),
            phase: String::new(),
            count: String::new(),
            total: String::new(),
        },
        super::super::TrackStatus::Active,
    );
    assert_eq!(result, "\x1b[0m[F] wget", "marker bracket uncolored for normal status");
}

#[test]
fn render_prefix_components_always_starts_with_reset() {
    for status in [
        super::super::TrackStatus::Active,
        super::super::TrackStatus::Failed,
        super::super::TrackStatus::Warning,
        super::super::TrackStatus::Success,
    ] {
        let result = super::super::inner::render_prefix_components(
            &super::super::inner::PrefixComponents {
                marker: "F".into(),
                tool_name: "foo".into(),
                version: String::new(),
                phase: String::new(),
                count: String::new(),
                total: String::new(),
            },
            status,
        );
        assert!(
            result.starts_with("\x1b[0m"),
            "{status:?}: expected \\x1b[0m prefix, got {result:?}"
        );
    }
}

/// Render truncated prefix components with Active status for assertions.
fn render_prefix(parts: &super::super::inner::PrefixComponents) -> String {
    super::super::inner::render_prefix_components(parts, super::super::TrackStatus::Active)
}

// ---- Regression: TrackStatus marker behavior (post-simplification) ----

#[test]
fn regression_finish_warning_stores_warning_and_decodes() {
    let h = TrackedHandle::new(10);
    assert!(!h.is_finished());
    h.finish_warning();
    assert!(h.is_finished(), "finish_warning marks the handle finished");
    assert_eq!(h.snapshot().status, TrackStatus::Warning, "finish_warning stores Warning");
}

#[test]
fn regression_finish_error_stores_failed_and_decodes() {
    let h = TrackedHandle::new(10);
    h.finish_error();
    assert!(h.is_finished(), "finish_error marks the handle finished");
    assert_eq!(h.snapshot().status, TrackStatus::Failed, "finish_error stores Failed");
}

#[test]
fn regression_finish_success_stores_success_and_decodes() {
    let h = TrackedHandle::new(10);
    h.finish_success();
    assert!(h.is_finished(), "finish_success marks the handle finished");
    assert_eq!(h.snapshot().status, TrackStatus::Success, "finish_success stores Success");
}

#[test]
fn regression_render_prefix_components_status_markers() {
    // Failed => red [F], Warning => yellow [W], others uncolored.
    let parts = super::super::inner::PrefixComponents {
        marker: "F".into(),
        tool_name: "wget".into(),
        version: String::new(),
        phase: String::new(),
        count: String::new(),
        total: String::new(),
    };
    let failed = super::super::inner::render_prefix_components(&parts, TrackStatus::Failed);
    assert_eq!(failed, "\x1b[0m\x1b[31m[F]\x1b[0m wget", "failed marker rendered red");

    // Warning status colors the marker yellow; the marker glyph itself is
    // carried by the `marker` field (set to "W" at construction).
    let warning_parts = super::super::inner::PrefixComponents {
        marker: "W".into(),
        tool_name: "wget".into(),
        version: String::new(),
        phase: String::new(),
        count: String::new(),
        total: String::new(),
    };
    let warning =
        super::super::inner::render_prefix_components(&warning_parts, TrackStatus::Warning);
    assert_eq!(warning, "\x1b[0m\x1b[33m[W]\x1b[0m wget", "warning marker rendered yellow [W]");

    for status in [TrackStatus::Active, TrackStatus::Success] {
        let normal = super::super::inner::render_prefix_components(&parts, status);
        assert!(
            !normal.contains("\x1b[31m") && !normal.contains("\x1b[33m"),
            "{status:?}: marker must be uncolored (no red/yellow SGR)"
        );
        assert!(normal.contains("[F]"), "{status:?}: uncolored [F] bracket still present");
    }
}

#[test]
fn regression_bar_color_code_status_codes() {
    assert_eq!(super::super::inner::bar_color_code(TrackStatus::Failed, false), "31");
    assert_eq!(super::super::inner::bar_color_code(TrackStatus::Warning, false), "33");
    assert_eq!(super::super::inner::bar_color_code(TrackStatus::Success, false), "32");
}

#[test]
fn regression_snapshot_status_code_round_trip() {
    // Each known status decodes back to the correct TrackStatus via the
    // public finish_* API. The `_ => Success` fallback for unknown codes
    // is covered by the inner-module unit test below.
    let active = TrackedHandle::new(10);
    assert_eq!(active.snapshot().status, TrackStatus::Active);

    let success = TrackedHandle::new(10);
    success.finish_success();
    assert_eq!(success.snapshot().status, TrackStatus::Success);

    let failed = TrackedHandle::new(10);
    failed.finish_error();
    assert_eq!(failed.snapshot().status, TrackStatus::Failed);

    let warning = TrackedHandle::new(10);
    warning.finish_warning();
    assert_eq!(warning.snapshot().status, TrackStatus::Warning);

    // finish_and_clear stores code 5, which the snapshot decoder maps to
    // Success (the `_ => Success` fallback arm).
    let cleared = TrackedHandle::new(10);
    cleared.finish_and_clear();
    assert_eq!(cleared.snapshot().status, TrackStatus::Success);
}

// ---- prefix_components_from_str tests (Phase 2) -----------------------

#[test]
fn prefix_components_from_str_tool_name_only() {
    let result = super::super::inner::prefix_components_from_str("wget");
    assert_eq!(result.tool_name, "wget");
    assert!(result.marker.is_empty());
    assert!(result.version.is_empty());
    assert!(result.phase.is_empty());
    assert!(result.count.is_empty());
    assert!(result.total.is_empty());
}

#[test]
fn prefix_components_from_str_all_fields() {
    let result = super::super::inner::prefix_components_from_str("yt-dlp 2024.12.20 [fch] 2/5");
    assert_eq!(result.tool_name, "yt-dlp");
    assert_eq!(result.version, "2024.12.20");
    assert_eq!(result.phase, "fch");
    assert_eq!(result.count, "2");
    assert_eq!(result.total, "5");
}

#[test]
fn prefix_components_from_str_multiword_no_bracket() {
    // No-bracket labels with multiple words keep the whole string as
    // tool_name (regression: previously only the first token was kept).
    let result = super::super::inner::prefix_components_from_str("syncing tools");
    assert_eq!(result.tool_name, "syncing tools");
    assert!(result.version.is_empty());
    assert!(result.phase.is_empty());
    assert!(result.count.is_empty());
    assert!(result.total.is_empty());
}

#[test]
fn prefix_components_from_str_multiword_with_count_total() {
    // A trailing count/total token after a multi-word tool name is split
    // off; the rest of the string stays the tool name.
    let result = super::super::inner::prefix_components_from_str("syncing tools 2/5");
    assert_eq!(result.tool_name, "syncing tools");
    assert!(result.version.is_empty());
    assert!(result.phase.is_empty());
    assert_eq!(result.count, "2");
    assert_eq!(result.total, "5");
}

#[test]
fn prefix_components_from_str_keeps_no_bracket_trailing_nonslash_word() {
    // A trailing word without `/` is NOT a count/total token — the whole
    // string remains the tool name.
    let result = super::super::inner::prefix_components_from_str("materializing files");
    assert_eq!(result.tool_name, "materializing files");
    assert!(result.version.is_empty());
    assert!(result.phase.is_empty());
    assert!(result.count.is_empty());
    assert!(result.total.is_empty());
}

#[test]
fn shared_state_parses_label_into_components() {
    // add_bar labels must be parsed into PrefixComponents at construction
    // (regression: previously the entire label was stored as tool_name,
    // so semantic truncation chopped `[res]` off resolve bars).
    let h = TrackedHandle::with_label(100, "ffmpeg autobuild-2026-07-31 [res]");
    let snap = h.snapshot();
    assert_eq!(snap.prefix_components.tool_name, "ffmpeg");
    assert_eq!(snap.prefix_components.version, "autobuild-2026-07-31");
    assert_eq!(snap.prefix_components.phase, "res");
    assert!(snap.prefix_components.count.is_empty());
    assert!(snap.prefix_components.total.is_empty());

    let h = TrackedHandle::with_label(100, "syncing tools");
    let snap = h.snapshot();
    assert_eq!(snap.prefix_components.tool_name, "syncing tools");

    let h = TrackedHandle::with_label(100, "");
    let snap = h.snapshot();
    assert!(snap.prefix_components.tool_name.is_empty());
    assert!(snap.prefix_components.version.is_empty());
    assert!(snap.prefix_components.phase.is_empty());
}

#[test]
fn truncate_parsed_resolve_label_preserves_phase() {
    // The user-reported symptom: a long resolve label truncated to the
    // prefix budget must shrink the version first and keep `[res]`.
    let parts =
        super::super::inner::prefix_components_from_str("ffmpeg autobuild-2026-07-31 [res]");
    let result = super::super::inner::semantic_truncate_prefix(&parts, 26);
    assert_eq!(result.tool_name, "ffmpeg");
    assert_eq!(result.version, "autobuild-202");
    assert_eq!(result.phase, "res");
    assert!(result.count.is_empty());
    assert!(result.total.is_empty());
}

// ---- semantic_truncate_prefix tests (Phase 2) -----------------------

fn prefix_parts_wget() -> super::super::inner::PrefixComponents {
    super::super::inner::PrefixComponents {
        marker: String::new(),
        tool_name: "wget".into(),
        version: "1.2.3".into(),
        phase: "fch".into(),
        count: "2".into(),
        total: "5".into(),
    }
}

#[test]
fn semantic_truncate_prefix_already_fits() {
    let parts = prefix_parts_wget();
    let result = super::super::inner::semantic_truncate_prefix(&parts, 30);
    assert_eq!(render_prefix(&result), "\x1b[0mwget 1.2.3 [fch] 2/5", "no truncation when fits");
}

#[test]
fn semantic_truncate_prefix_remove_version() {
    let parts = prefix_parts_wget();
    // max=17: version shrunk to "1."
    let result = super::super::inner::semantic_truncate_prefix(&parts, 17);
    assert_eq!(render_prefix(&result), "\x1b[0mwget 1. [fch] 2/5", "version shortened by 3 chars");
}

#[test]
fn semantic_truncate_prefix_remove_version_full() {
    let parts = prefix_parts_wget();
    // max=15: version fully removed
    let result = super::super::inner::semantic_truncate_prefix(&parts, 15);
    assert_eq!(
        render_prefix(&result),
        "\x1b[0mwget [fch] 2/5",
        "version fully removed when excess covers it"
    );
}

#[test]
fn semantic_truncate_prefix_remove_version_and_count_total() {
    let parts = prefix_parts_wget();
    // max=11: version and count/total removed
    let result = super::super::inner::semantic_truncate_prefix(&parts, 11);
    assert_eq!(render_prefix(&result), "\x1b[0mwget [fch]", "version and count/total removed");
}

#[test]
fn semantic_truncate_prefix_count_total_removed_atomically() {
    let parts = prefix_parts_wget();
    // max=12: version gone, count/total removed as one pair — a bare "2/"
    // or "/5" must never appear.
    let result = super::super::inner::semantic_truncate_prefix(&parts, 12);
    assert_eq!(render_prefix(&result), "\x1b[0mwget [fch]", "count/total removed as atomic pair");
}

#[test]
fn semantic_truncate_prefix_phase_removed_atomically() {
    let parts = prefix_parts_wget();
    // max=8: count/total gone, phase removed entirely (never a bare "[f]")
    let result = super::super::inner::semantic_truncate_prefix(&parts, 8);
    assert_eq!(render_prefix(&result), "\x1b[0mwget", "phase removed entirely, no partial bracket");
}

#[test]
fn semantic_truncate_prefix_remove_version_count_phase() {
    let parts = prefix_parts_wget();
    // max=4: only tool name remains
    let result = super::super::inner::semantic_truncate_prefix(&parts, 4);
    assert_eq!(
        render_prefix(&result),
        "\x1b[0mwget",
        "tool name survives after version, count, phase removed"
    );
}

#[test]
fn semantic_truncate_prefix_tool_name_progressive_shrink() {
    let parts = prefix_parts_wget();
    // max=2: progressive shrink of tool name (no fallback string mangling)
    let result = super::super::inner::semantic_truncate_prefix(&parts, 2);
    assert_eq!(render_prefix(&result), "\x1b[0mwg", "tool name shrunk to 2 chars");
}

#[test]
fn semantic_truncate_prefix_empty() {
    let parts = super::super::inner::PrefixComponents::default();
    let result = super::super::inner::semantic_truncate_prefix(&parts, 30);
    assert_eq!(render_prefix(&result), "\x1b[0m", "empty prefix stays empty");
}

#[test]
fn semantic_truncate_prefix_marker_removed_before_tool_name() {
    // Marker is truncatable data: dropped before the tool name shrinks.
    let parts = super::super::inner::PrefixComponents {
        marker: "F".into(),
        tool_name: "wget".into(),
        version: String::new(),
        phase: "fch".into(),
        count: "0".into(),
        total: "1".into(),
    };
    // max=7: marker (4) + tool (4) = 8 > 7 — marker removed, tool intact.
    let result = super::super::inner::semantic_truncate_prefix(&parts, 7);
    assert!(result.marker.is_empty(), "marker removed before tool name");
    assert_eq!(render_prefix(&result), "\x1b[0mwget", "tool name intact after marker removal");
    // max=3: marker removed first, then tool shrinks to 3 chars.
    let result = super::super::inner::semantic_truncate_prefix(&parts, 3);
    assert_eq!(render_prefix(&result), "\x1b[0mwge", "marker dropped before tool shrank");
}

#[test]
fn semantic_truncate_prefix_marker_preserved_when_fits() {
    // max=9: after count/total and phase removal, marker (4) + tool (4)
    // = 8 <= 9 — marker preserved.
    let parts = super::super::inner::PrefixComponents {
        marker: "F".into(),
        tool_name: "wget".into(),
        version: String::new(),
        phase: "fch".into(),
        count: "0".into(),
        total: "1".into(),
    };
    let result = super::super::inner::semantic_truncate_prefix(&parts, 9);
    assert_eq!(
        super::super::inner::render_prefix_components(&result, super::super::TrackStatus::Failed),
        "\x1b[0m\x1b[31m[F]\x1b[0m wget",
        "marker preserved when it fits"
    );
}

#[test]
fn semantic_truncate_prefix_marker_failed_effective_width() {
    // max=4: marker (4) + tool (4) = 8 > 4 — marker removed so the tool
    // name fits the effective width.
    let parts = super::super::inner::PrefixComponents {
        marker: "F".into(),
        tool_name: "wget".into(),
        version: String::new(),
        phase: "fch".into(),
        count: "0".into(),
        total: "1".into(),
    };
    let result = super::super::inner::semantic_truncate_prefix(&parts, 4);
    assert_eq!(
        super::super::inner::render_prefix_components(&result, super::super::TrackStatus::Failed),
        "\x1b[0mwget",
        "marker removed to fit effective width"
    );
}

#[test]
fn semantic_truncate_prefix_version_shrinks_first_long_version() {
    // Regression: version must shrink (never mangle into a mid-char
    // suffix) before any other component is touched.
    let parts = super::super::inner::PrefixComponents {
        marker: String::new(),
        tool_name: "ffmpeg".into(),
        version: "autobuild-2026-07-31".into(),
        phase: "res".into(),
        count: "2".into(),
        total: "2".into(),
    };
    let result = super::super::inner::semantic_truncate_prefix(&parts, 30);
    assert_eq!(
        render_prefix(&result),
        "\x1b[0mffmpeg autobuild-202 [res] 2/2",
        "long version shrinks first, no mangled suffix"
    );
    let result = super::super::inner::semantic_truncate_prefix(&parts, 8);
    assert_eq!(render_prefix(&result), "\x1b[0mffmpeg", "all optional components dropped");
}

// ---- semantic_truncate_suffix tests (Phase 1) -----------------------

/// Canonical test parts: auto components (33 visible) + custom (10).
fn suffix_parts_full() -> SuffixComponents {
    SuffixComponents {
        count: "2".into(),
        total: "5".into(),
        elapsed: "0:00:05".into(),
        rate: Some("12.3 MiB/s".into()),
        eta: Some("[0:00:02]".into()),
        custom: "cached (1)".into(),
    }
}

/// Render truncated parts with the child-bar color code like production.
pub(crate) fn render_suffix(parts: &SuffixComponents) -> String {
    super::super::inner::render_suffix_components(parts, "33")
}

#[test]
fn semantic_truncate_suffix_fits() {
    let parts = suffix_parts_full();
    let result = super::super::inner::semantic_truncate_suffix(&parts, 100);
    assert_eq!(
        render_suffix(&result),
        " \x1b[33m2/5\x1b[0m 0:00:05 12.3 MiB/s [0:00:02] cached (1)",
        "full suffix when fits"
    );
}

#[test]
fn semantic_truncate_suffix_remove_custom_partial() {
    let parts = suffix_parts_full();
    let result = super::super::inner::semantic_truncate_suffix(&parts, 39);
    assert_eq!(
        render_suffix(&result),
        " \x1b[33m2/5\x1b[0m 0:00:05 12.3 MiB/s [0:00:02] cache",
        "custom shrunk to keep=5 (39 visible)"
    );
}

#[test]
fn semantic_truncate_suffix_remove_custom_all() {
    let parts = suffix_parts_full();
    let result = super::super::inner::semantic_truncate_suffix(&parts, 34);
    assert_eq!(
        render_suffix(&result),
        " \x1b[33m2/5\x1b[0m 0:00:05 12.3 MiB/s [0:00:02]",
        "custom fully removed (auto 33 <= 34)"
    );
}

#[test]
fn semantic_truncate_suffix_removes_eta() {
    let parts = suffix_parts_full();
    let result = super::super::inner::semantic_truncate_suffix(&parts, 30);
    assert_eq!(
        render_suffix(&result),
        " \x1b[33m2/5\x1b[0m 0:00:05 12.3 MiB/s",
        "eta removed entirely (23 <= 30)"
    );
}

#[test]
fn semantic_truncate_suffix_removes_rate() {
    let parts = suffix_parts_full();
    let result = super::super::inner::semantic_truncate_suffix(&parts, 22);
    assert_eq!(
        render_suffix(&result),
        " \x1b[33m2/5\x1b[0m 0:00:05",
        "rate removed entirely (12 <= 22)"
    );
}

#[test]
fn semantic_truncate_suffix_removes_elapsed() {
    let parts = suffix_parts_full();
    let result = super::super::inner::semantic_truncate_suffix(&parts, 11);
    assert_eq!(render_suffix(&result), " \x1b[33m2/5\x1b[0m", "elapsed removed entirely (4 <= 11)");
}

#[test]
fn semantic_truncate_suffix_fits_count_total() {
    let parts = suffix_parts_full();
    let result = super::super::inner::semantic_truncate_suffix(&parts, 4);
    assert_eq!(
        render_suffix(&result),
        " \x1b[33m2/5\x1b[0m",
        "count/total is the last unit and fits at 4"
    );
}

#[test]
fn semantic_truncate_suffix_removes_count_total() {
    let parts = suffix_parts_full();
    let result = super::super::inner::semantic_truncate_suffix(&parts, 3);
    assert_eq!(render_suffix(&result), "", "count/total removed entirely (0 <= 3)");
}

#[test]
fn semantic_truncate_suffix_count_total_atomic_no_partial() {
    // Width 3: the full unit ` 2/5` (4 visible) does not fit, but a partial
    // like ` 2/` (3 visible) would. Atomicity requires dropping the whole
    // pair — a bare count or partial unit is never shown.
    let parts = SuffixComponents {
        count: "2".into(),
        total: "5".into(),
        elapsed: "0:00:05".into(),
        rate: None,
        eta: None,
        custom: String::new(),
    };
    let result = super::super::inner::semantic_truncate_suffix(&parts, 3);
    assert_eq!(render_suffix(&result), "", "no partial count/total at width 3");
}

#[test]
fn semantic_truncate_suffix_auto_components_removed_without_custom() {
    // Full = 12 visible; at 10 elapsed is removed, leaving ` 2/5` (4).
    let parts = SuffixComponents {
        count: "2".into(),
        total: "5".into(),
        elapsed: "0:00:05".into(),
        rate: None,
        eta: None,
        custom: String::new(),
    };
    let result = super::super::inner::semantic_truncate_suffix(&parts, 10);
    assert_eq!(render_suffix(&result), " \x1b[33m2/5\x1b[0m", "elapsed removed at 10");
}

#[test]
fn semantic_truncate_suffix_empty_both() {
    let result = super::super::inner::semantic_truncate_suffix(&SuffixComponents::default(), 30);
    assert_eq!(render_suffix(&result), "", "both empty");
}

#[test]
fn semantic_truncate_suffix_custom_only() {
    let parts = SuffixComponents { custom: "custom".into(), ..Default::default() };
    let result = super::super::inner::semantic_truncate_suffix(&parts, 10);
    assert_eq!(render_suffix(&result), " custom", "custom appended after empty auto");
}

#[test]
fn render_suffix_components_with_rate() {
    // rate present, no eta
    let parts = SuffixComponents {
        count: "0".into(),
        total: "5".into(),
        elapsed: "0s".into(),
        rate: Some("0/d".into()),
        eta: None,
        custom: String::new(),
    };
    let result = super::super::inner::render_suffix_components(&parts, "33");
    assert_eq!(result, " \x1b[33m0/5\x1b[0m 0s 0/d");
    assert!(result.ends_with("0s 0/d"), "expected elapsed then rate at end: {result:?}");
}

#[test]
fn render_suffix_components_with_rate_and_eta() {
    // rate + eta
    let parts = SuffixComponents {
        count: "0".into(),
        total: "5".into(),
        elapsed: "0s".into(),
        rate: Some("0/d".into()),
        eta: Some("5s".into()),
        custom: String::new(),
    };
    let result = super::super::inner::render_suffix_components(&parts, "33");
    assert_eq!(result, " \x1b[33m0/5\x1b[0m 0s 0/d 5s");
    assert!(result.ends_with("0s 0/d 5s"), "expected elapsed rate eta at end: {result:?}");
}

#[test]
fn render_suffix_components_different_color_codes() {
    for (code, status_name) in [("31", "failed"), ("33", "child"), ("35", "overall")] {
        let parts = SuffixComponents {
            count: "1".into(),
            total: "2".into(),
            elapsed: "3s".into(),
            rate: Some("0/d".into()),
            eta: None,
            custom: String::new(),
        };
        let result = super::super::inner::render_suffix_components(&parts, code);
        assert!(
            result.contains(&format!("\x1b[{code}m")),
            "{status_name} should use code {code}: {result:?}"
        );
        assert!(result.contains("1/2"), "{status_name} count/total absent: {result:?}");
    }
}

#[test]
fn render_suffix_components_eta_suppressed_without_rate() {
    // Historical build_right_msg guard: eta renders only when rate is present.
    let parts = SuffixComponents {
        count: "0".into(),
        total: "5".into(),
        elapsed: "0s".into(),
        rate: None,
        eta: Some("5s".into()),
        custom: String::new(),
    };
    let result = super::super::inner::render_suffix_components(&parts, "33");
    assert_eq!(result, " \x1b[33m0/5\x1b[0m 0s", "eta suppressed without rate");
}

#[test]
fn render_suffix_components_custom_appended() {
    let parts = SuffixComponents {
        count: "0".into(),
        total: "5".into(),
        elapsed: "0s".into(),
        rate: Some("0/d".into()),
        eta: None,
        custom: "cached (1)".into(),
    };
    let result = super::super::inner::render_suffix_components(&parts, "33");
    assert_eq!(result, " \x1b[33m0/5\x1b[0m 0s 0/d cached (1)", "custom appended");
}
