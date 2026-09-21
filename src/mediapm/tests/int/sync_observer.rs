//! Integration tests for the centralized output primitives and per-phase
//! sync observer wiring.

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use mediapm::{
        MaterializationSyncSummary, SyncLibraryOptions, SyncPhaseObserver, SyncPhaseReport,
        ToolsSyncSummary, WorkflowSyncSummary,
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

    /// format_result_line produces the expected `icon op k=v` shape.
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

    /// format_duration correctness for a known duration.
    #[test]
    fn format_duration_known_value() {
        let d = std::time::Duration::from_secs(42);
        assert_eq!(format_duration(d), "42s");
    }

    /// CliSyncObserver renders the Tools phase line to stdout (capture via
    /// format_result_line).
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

    /// CliSyncObserver renders the Workflow phase line with failure warning.
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

    /// CliSyncObserver renders the Materialization phase line.
    #[test]
    fn materialization_phase_line_shape() {
        let s = MaterializationSyncSummary {
            materialized_paths: 5,
            skipped_paths: 3,
            removed_paths: 1,
            removed_empty_dirs: 0,
        };
        let icon = if s.materialized_paths > 0 || s.removed_paths > 0 {
            StatusIcon::Success
        } else {
            StatusIcon::NoChange
        };
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
        assert!(line.contains("✓"));
        assert!(line.contains("paths=5"));
        assert!(line.contains("skipped=3"));
    }

    /// RecordingObserver collects phases in order.
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

    /// SyncLibraryOptions::default() preserves pre-change behavior.
    #[test]
    fn sync_library_options_default() {
        let opts = SyncLibraryOptions::default();
        assert!(!opts.verify_materialization);
        assert!(!opts.check_tag_updates);
        assert!(!opts.no_progress);
        assert!(opts.observer.is_none());
    }

    /// StatusIcon variants produce distinct glyphs.
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
