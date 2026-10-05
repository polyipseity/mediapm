//! Integration tests for the centralized output primitives and per-phase
//! sync observer wiring.

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use mediapm::output::observer::materialization_icon;
    use mediapm::output::{sync_summary_icon, sync_summary_is_incomplete};
    use mediapm::{
        MaterializationSyncSummary, SyncLibraryOptions, SyncPhaseObserver, SyncPhaseReport,
        SyncSummary, ToolsSyncSummary, WorkflowSyncSummary,
    };
    use mediapm_utils::report::{StatusIcon, format_duration, format_result_line};

    /// Observer that records every phase report it receives, for assertion.
    struct RecordingObserver {
        reports: Arc<Mutex<Vec<SyncPhaseReport>>>,
    }

    impl RecordingObserver {
        fn new(reports: Arc<Mutex<Vec<SyncPhaseReport>>>) -> Self {
            Self { reports }
        }
    }

    impl SyncPhaseObserver for RecordingObserver {
        fn on_phase(&self, report: SyncPhaseReport) {
            self.reports.lock().unwrap().push(report);
        }
    }

    /// `format_result_line` produces the expected `icon op k=v` shape.
    #[test]
    fn format_result_line_matches_expected() {
        let line = format_result_line(
            StatusIcon::Success,
            "sync complete",
            &[("executed", &3 as &dyn std::fmt::Display), ("cached", &2)],
            None,
        );
        assert!(line.contains("✓"));
        assert!(line.contains("sync complete"));
        assert!(line.contains("executed=3"));
        assert!(line.contains("cached=2"));
    }

    /// `format_duration` correctness for a known duration.
    #[test]
    fn format_duration_known_value() {
        let d = std::time::Duration::from_secs(42);
        assert_eq!(format_duration(d), "42s");
    }

    /// `CliSyncObserver` renders the Tools phase line to stdout (capture via
    /// `format_result_line`).
    #[test]
    fn tools_phase_line_shape() {
        let s = ToolsSyncSummary {
            added_tools: 2,
            updated_tools: 1,
            pruned_tools: 0,
            removed_tools: 0,
            skipped_tools: 3,
            warnings: vec![],
        };
        let icon = if s.added_tools > 0 || s.updated_tools > 0 {
            StatusIcon::Success
        } else {
            StatusIcon::NoChange
        };
        let line = format_result_line(
            icon,
            "tools synced",
            &[("added", &s.added_tools as &dyn std::fmt::Display), ("updated", &s.updated_tools)],
            None,
        );
        assert!(line.contains("added=2"));
        assert!(line.contains("updated=1"));
    }

    /// `CliSyncObserver` renders the Workflow phase line with failure warning.
    #[test]
    fn workflow_phase_line_with_failure() {
        let s = WorkflowSyncSummary { executed_instances: 3, cached_instances: 0, failed_steps: 1 };
        let icon = if s.failed_steps > 0 { StatusIcon::Warning } else { StatusIcon::Success };
        let line = format_result_line(
            icon,
            "workflow",
            &[
                ("executed", &s.executed_instances as &dyn std::fmt::Display),
                ("cached", &s.cached_instances),
                ("failed", &s.failed_steps),
            ],
            None,
        );
        assert!(line.contains("Δ"));
        assert!(line.contains("failed=1"));
    }

    /// Builds a whole-run summary, leaving every field the rules under test do
    /// not name at zero.
    fn sync_summary(
        executed: usize,
        materialized: usize,
        skipped: usize,
        failed_steps: usize,
    ) -> SyncSummary {
        SyncSummary {
            executed_instances: executed,
            cached_instances: 0,
            materialized_paths: materialized,
            skipped_paths: skipped,
            removed_paths: 0,
            removed_empty_dirs: 0,
            added_tools: 0,
            updated_tools: 0,
            pruned_tools: 0,
            removed_tools: 0,
            skipped_tools: 0,
            workflow_failed_steps: failed_steps,
            warnings: vec![],
        }
    }

    /// A skipped hierarchy path makes the materialization line an error.
    ///
    /// The icon comes from `materialization_icon`, the function the observer
    /// calls, so a change to that rule is what this assertion sees. Recomputing
    /// the rule here would pass whatever the observer did.
    #[test]
    fn materialization_phase_line_is_an_error_when_a_path_is_skipped() {
        let s = MaterializationSyncSummary {
            materialized_paths: 5,
            skipped_paths: 3,
            removed_paths: 1,
            removed_empty_dirs: 0,
        };
        let icon = materialization_icon(&s);
        assert_eq!(
            icon,
            StatusIcon::Error,
            "a skipped path wrote nothing, so the phase line must not claim success; \
             summary: {s:?}"
        );
        let line = format_result_line(
            icon,
            "materialized",
            &[
                ("paths", &s.materialized_paths as &dyn std::fmt::Display),
                ("skipped", &s.skipped_paths),
                ("removed", &s.removed_paths),
            ],
            None,
        );
        assert!(line.contains(StatusIcon::Error.glyph()), "line: {line}");
        assert!(line.contains("paths=5"));
        assert!(line.contains("skipped=3"));
    }

    /// The counterpart: a run that skipped nothing still reports success.
    ///
    /// Without this, an observer that returned `Error` for every materialization
    /// phase would satisfy the test above.
    #[test]
    fn materialization_phase_line_is_a_success_when_no_path_is_skipped() {
        let s = MaterializationSyncSummary {
            materialized_paths: 5,
            skipped_paths: 0,
            removed_paths: 1,
            removed_empty_dirs: 0,
        };
        assert_eq!(materialization_icon(&s), StatusIcon::Success, "summary: {s:?}");

        let idle = MaterializationSyncSummary::default();
        assert_eq!(materialization_icon(&idle), StatusIcon::NoChange, "summary: {idle:?}");
    }

    /// A skipped path makes the whole-sync summary an error and marks the run
    /// incomplete, which is what the CLI turns into a non-zero exit.
    #[test]
    fn sync_summary_is_an_error_when_a_path_is_skipped() {
        let summary = sync_summary(2, 5, 3, 0);
        assert_eq!(sync_summary_icon(&summary), StatusIcon::Error, "summary: {summary:?}");
        assert!(
            sync_summary_is_incomplete(&summary),
            "the exit status reads this predicate, so a skipped path must set it; \
             summary: {summary:?}"
        );
    }

    /// The rule stops there: a failed workflow step stays a warning, and a run
    /// that skipped nothing keeps the icon its counts give it.
    #[test]
    fn sync_summary_icon_without_a_skipped_path() {
        let clean = sync_summary(2, 5, 0, 0);
        assert_eq!(sync_summary_icon(&clean), StatusIcon::Success, "summary: {clean:?}");
        assert!(!sync_summary_is_incomplete(&clean), "summary: {clean:?}");

        let failed_step = sync_summary(2, 5, 0, 1);
        assert_eq!(
            sync_summary_icon(&failed_step),
            StatusIcon::Warning,
            "summary: {failed_step:?}"
        );

        let quiet = sync_summary(0, 0, 0, 0);
        assert_eq!(sync_summary_icon(&quiet), StatusIcon::NoChange, "summary: {quiet:?}");
    }

    /// A skipped path outranks a failed workflow step.
    ///
    /// Both are unfinished business, but only the skip means an entry is missing
    /// from the library, which is the state a caller has to act on.
    #[test]
    fn sync_summary_is_an_error_when_a_path_is_skipped_and_a_step_failed() {
        let summary = sync_summary(2, 5, 1, 1);
        assert_eq!(sync_summary_icon(&summary), StatusIcon::Error, "summary: {summary:?}");
    }

    /// `RecordingObserver` collects phases in order.
    #[test]
    fn recording_observer_collects_phases() {
        let reports = Arc::new(Mutex::new(Vec::new()));
        let obs = RecordingObserver::new(reports.clone());

        obs.on_phase(SyncPhaseReport::Tools(ToolsSyncSummary {
            added_tools: 1,
            updated_tools: 0,
            pruned_tools: 0,
            removed_tools: 0,
            skipped_tools: 0,
            warnings: vec![],
        }));
        obs.on_phase(SyncPhaseReport::Workflow(WorkflowSyncSummary {
            executed_instances: 2,
            cached_instances: 0,
            failed_steps: 0,
        }));
        obs.on_phase(SyncPhaseReport::Materialization(MaterializationSyncSummary {
            materialized_paths: 3,
            skipped_paths: 0,
            removed_paths: 0,
            removed_empty_dirs: 0,
        }));

        let collected = reports.lock().unwrap();
        assert_eq!(collected.len(), 3);
        assert!(matches!(collected[0], SyncPhaseReport::Tools(_)));
        assert!(matches!(collected[1], SyncPhaseReport::Workflow(_)));
        assert!(matches!(collected[2], SyncPhaseReport::Materialization(_)));
    }

    /// `SyncLibraryOptions::default()` preserves pre-change behavior.
    #[test]
    fn sync_library_options_default() {
        let opts = SyncLibraryOptions::default();
        assert!(!opts.verify_materialization);
        assert!(!opts.check_tag_updates);
        assert!(!opts.no_progress);
        assert!(opts.observer.is_none());
    }

    /// `StatusIcon` variants produce distinct glyphs.
    #[test]
    fn status_icon_glyphs_distinct() {
        let mut buf = String::new();
        for icon in
            [StatusIcon::Success, StatusIcon::NoChange, StatusIcon::Warning, StatusIcon::Error]
        {
            let line = format_result_line(icon, "test", &[], None);
            buf.push_str(&line);
            buf.push('\n');
        }
        assert!(buf.contains("✓"));
        assert!(buf.contains("–"));
        assert!(buf.contains("Δ"));
        assert!(buf.contains("✗"));
    }
}
