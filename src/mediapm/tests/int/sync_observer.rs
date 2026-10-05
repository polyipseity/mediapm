//! Integration tests for the centralized output primitives and per-phase
//! sync observer wiring.

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use mediapm::output::observer::{materialization_icon, workflow_icon};
    use mediapm::output::{
        SyncOutcome, sync_outcome, sync_summary_icon, sync_summary_is_incomplete,
    };
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

    /// A failed workflow step makes the workflow phase line an error.
    ///
    /// The icon comes from `workflow_icon`, the function the observer calls.
    #[test]
    fn workflow_phase_line_is_an_error_when_a_step_failed() {
        let s = WorkflowSyncSummary { executed_instances: 3, cached_instances: 0, failed_steps: 1 };
        let icon = workflow_icon(&s);
        assert_eq!(icon, StatusIcon::Error, "summary: {s:?}");

        let idle =
            WorkflowSyncSummary { executed_instances: 0, cached_instances: 0, failed_steps: 0 };
        assert_eq!(workflow_icon(&idle), StatusIcon::NoChange, "summary: {idle:?}");

        let ran =
            WorkflowSyncSummary { executed_instances: 2, cached_instances: 1, failed_steps: 0 };
        assert_eq!(workflow_icon(&ran), StatusIcon::Success, "summary: {ran:?}");
    }

    /// Builds a whole-run summary, leaving every field the rules under test do
    /// not name at zero.
    fn sync_summary(
        executed: usize,
        materialized: usize,
        skipped: usize,
        missing: usize,
        failed_steps: usize,
    ) -> SyncSummary {
        SyncSummary {
            executed_instances: executed,
            cached_instances: 0,
            materialized_paths: materialized,
            skipped_paths: skipped,
            missing_paths: missing,
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

    /// The same summary, carrying one warning and nothing else out of place.
    fn warned_sync_summary(executed: usize, materialized: usize) -> SyncSummary {
        SyncSummary {
            warnings: vec!["tools require sync before library sync: ffmpeg".to_string()],
            ..sync_summary(executed, materialized, 0, 0, 0)
        }
    }

    /// A path the run could not produce makes the materialization line an error.
    ///
    /// The icon comes from `materialization_icon`, the function the observer
    /// calls, so a change to that rule is what this assertion sees. Recomputing
    /// the rule here would pass whatever the observer did.
    #[test]
    fn materialization_phase_line_is_an_error_when_a_path_is_missing() {
        let s = MaterializationSyncSummary {
            materialized_paths: 5,
            skipped_paths: 0,
            missing_paths: 3,
            removed_paths: 1,
            removed_empty_dirs: 0,
        };
        let icon = materialization_icon(&s);
        assert_eq!(
            icon,
            StatusIcon::Error,
            "a missing path wrote nothing, so the phase line must not claim success; \
             summary: {s:?}"
        );
        let line = format_result_line(
            icon,
            "materialized",
            &[
                ("paths", &s.materialized_paths as &dyn std::fmt::Display),
                ("missing", &s.missing_paths),
                ("removed", &s.removed_paths),
            ],
            None,
        );
        assert!(line.contains(StatusIcon::Error.glyph()), "line: {line}");
        assert!(line.contains("paths=5"));
        assert!(line.contains("missing=3"));
    }

    /// The counterpart: a run that missed nothing still reports success.
    ///
    /// Without this, an observer that returned `Error` for every materialization
    /// phase would satisfy the test above.
    #[test]
    fn materialization_phase_line_is_a_success_when_no_path_is_missing() {
        let s = MaterializationSyncSummary {
            materialized_paths: 5,
            skipped_paths: 0,
            missing_paths: 0,
            removed_paths: 1,
            removed_empty_dirs: 0,
        };
        assert_eq!(materialization_icon(&s), StatusIcon::Success, "summary: {s:?}");

        let idle = MaterializationSyncSummary::default();
        assert_eq!(materialization_icon(&idle), StatusIcon::NoChange, "summary: {idle:?}");
    }

    /// Entries that were already correct do not make the phase line an error.
    ///
    /// The regression this guards against runs the other way: `skipped_paths`
    /// used to mean an entry the run failed to produce, so keying the icon on
    /// it turned a second run over a correct library red. A skipped entry says
    /// the file was there with the right bytes.
    #[test]
    fn materialization_phase_line_is_not_an_error_when_paths_were_already_correct() {
        let s = MaterializationSyncSummary {
            materialized_paths: 2,
            skipped_paths: 3,
            missing_paths: 0,
            removed_paths: 0,
            removed_empty_dirs: 0,
        };
        assert_eq!(
            materialization_icon(&s),
            StatusIcon::Success,
            "nothing was missing, so the phase must not end red; summary: {s:?}"
        );

        let all_skipped = MaterializationSyncSummary {
            materialized_paths: 0,
            skipped_paths: 3,
            missing_paths: 0,
            removed_paths: 0,
            removed_empty_dirs: 0,
        };
        assert_eq!(
            materialization_icon(&all_skipped),
            StatusIcon::NoChange,
            "a run that changed nothing is not a failure; summary: {all_skipped:?}"
        );
    }

    /// A missing path makes the whole-sync summary an error and marks the run
    /// incomplete, which is what the CLI turns into a non-zero exit.
    #[test]
    fn sync_summary_is_an_error_when_a_path_is_missing() {
        let summary = sync_summary(2, 5, 0, 3, 0);
        assert_eq!(sync_summary_icon(&summary), StatusIcon::Error, "summary: {summary:?}");
        assert!(
            sync_summary_is_incomplete(&summary),
            "the exit status reads this predicate, so a missing path must set it; \
             summary: {summary:?}"
        );
        assert_eq!(sync_outcome(&summary), SyncOutcome::Error, "summary: {summary:?}");
    }

    /// A run whose entries were all already correct is clean, not a warning.
    ///
    /// This is the second direction worth covering. The rule used to treat any
    /// skip as damage, which would make a correctly synced library the worst
    /// case a caller could run.
    #[test]
    fn sync_summary_is_clean_when_every_entry_was_already_correct() {
        let summary = sync_summary(2, 5, 3, 0, 0);
        assert_eq!(
            sync_summary_icon(&summary),
            StatusIcon::Success,
            "a skipped entry matched its recorded hash and length; summary: {summary:?}"
        );
        assert!(
            !sync_summary_is_incomplete(&summary),
            "a skip leaves the library complete; summary: {summary:?}"
        );
        assert_eq!(sync_outcome(&summary), SyncOutcome::Clean, "summary: {summary:?}");
    }

    /// A second run that changed nothing is clean, and says it changed nothing.
    #[test]
    fn sync_summary_reports_no_change_when_nothing_happened() {
        let summary = sync_summary(0, 0, 3, 0, 0);
        assert_eq!(sync_summary_icon(&summary), StatusIcon::NoChange, "summary: {summary:?}");
        assert_eq!(sync_outcome(&summary), SyncOutcome::Clean, "summary: {summary:?}");
    }

    /// A failed workflow step is an error, not a warning.
    ///
    /// The old rule kept it a warning because the paths the step owns may still
    /// have materialized. They materialize from what the step was to produce,
    /// so a step that failed and outputs that materialized cannot both describe
    /// the same media.
    #[test]
    fn sync_summary_is_an_error_when_a_workflow_step_failed() {
        let summary = sync_summary(2, 5, 0, 0, 1);
        assert_eq!(
            sync_summary_icon(&summary),
            StatusIcon::Error,
            "the media the failed step was to produce is not on disk; summary: {summary:?}"
        );
        assert!(
            sync_summary_is_incomplete(&summary),
            "the same rule feeds the exit status, so a failed step must set it; \
             summary: {summary:?}"
        );
    }

    /// A warning-only run is a warning, and stays out of the error band.
    #[test]
    fn sync_summary_is_a_warning_when_the_run_only_warned() {
        let summary = warned_sync_summary(2, 5);
        assert_eq!(sync_summary_icon(&summary), StatusIcon::Warning, "summary: {summary:?}");
        assert!(
            !sync_summary_is_incomplete(&summary),
            "a warning is not a missing entry; summary: {summary:?}"
        );
        assert_eq!(sync_outcome(&summary), SyncOutcome::Warning, "summary: {summary:?}");
    }

    /// Both errors outrank a warning.
    ///
    /// A run can carry all three at once, and the caller has to act on the
    /// missing entries first.
    #[test]
    fn sync_summary_error_outranks_warning_and_skips() {
        let summary = SyncSummary {
            skipped_paths: 2,
            missing_paths: 1,
            workflow_failed_steps: 1,
            ..warned_sync_summary(2, 5)
        };
        assert_eq!(sync_summary_icon(&summary), StatusIcon::Error, "summary: {summary:?}");
    }

    /// The rule stops there: a run with nothing wrong keeps the icon its counts
    /// give it, whether or not it did any work.
    #[test]
    fn sync_summary_icon_without_a_missing_path_or_failed_step() {
        let clean = sync_summary(2, 5, 0, 0, 0);
        assert_eq!(sync_summary_icon(&clean), StatusIcon::Success, "summary: {clean:?}");
        assert!(!sync_summary_is_incomplete(&clean), "summary: {clean:?}");

        let quiet = sync_summary(0, 0, 0, 0, 0);
        assert_eq!(sync_summary_icon(&quiet), StatusIcon::NoChange, "summary: {quiet:?}");
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
            missing_paths: 0,
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
