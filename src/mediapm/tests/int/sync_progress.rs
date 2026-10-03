//! Integration tests for the one-terminal-per-sync contract.
//!
//! A library sync owns exactly one [`ProgressTerminal`] and every phase — tool
//! sync, workflow execution, materialization — renders through a screen derived
//! from it (spec S1). The seam these tests use,
//! [`MediaPmService::sync_library_with_progress_overrides`], exists because
//! `indicatif` is a dev-dependency of this crate: production code cannot build a
//! `MultiProgress`, so handing the service a terminal is the only way to
//! observe the frames it draws.

mod tests {
    use std::sync::{Arc, Mutex};

    use indicatif::{InMemoryTerm, MultiProgress, ProgressDrawTarget};
    use mediapm::{
        MediaRuntimeStorage, SyncLibraryOptions, SyncPhaseObserver, SyncPhaseReport,
        SyncProgressOverrides,
    };
    use mediapm_utils::progress::{DimensionSource, ProgressTerminal, TestDimensionSource};

    use crate::common::service_with_cache;

    /// Terminal height of the injected terminal, in rows.
    const ROWS: u16 = 24;
    /// Terminal width of the injected terminal, in columns.
    const COLS: u16 = 80;

    /// Records the injected terminal's grid as it stands when a phase reports.
    ///
    /// The snapshot has to be taken from inside the phase callback: the service
    /// fires it after that phase's screen has been joined, and a later phase's
    /// frame would otherwise overwrite — and, with `InMemoryTerm`'s zero
    /// scrollback, scroll away — the frame this assertion needs to see.
    struct PhaseGridObserver {
        /// The injected terminal's grid, shared with the test body.
        term: InMemoryTerm,
        /// `(phase name, grid at that phase)`, in report order.
        grids: Arc<Mutex<Vec<(&'static str, String)>>>,
    }

    impl SyncPhaseObserver for PhaseGridObserver {
        fn on_phase(&self, report: SyncPhaseReport) {
            let name = match report {
                SyncPhaseReport::Tools(_) => "tools",
                SyncPhaseReport::Workflow(_) => "workflow",
                SyncPhaseReport::Materialization(_) => "materialization",
            };
            self.grids.lock().expect("grid lock").push((name, self.term.contents()));
        }
    }

    /// Every phase of a library sync renders into the one terminal the caller
    /// injected — including the tool phase, which is the phase that used to
    /// build a terminal of its own behind the caller's back.
    ///
    /// Non-vacuous: the tool phase's grid must be non-empty before its label is
    /// asserted, so "the phase drew nothing here" cannot satisfy "the phase drew
    /// here". A sync that builds a second, private terminal for the tool phase
    /// leaves this grid empty at the tools report while the workflow and
    /// materialization reports still fill it.
    ///
    /// The workflow and materialization grids also have to stay clear of
    /// `[wf]` and `[mat]`. An overall bar names what it is, so a phase tag
    /// beside that name only restates it.
    #[tokio::test]
    async fn sync_runs_every_phase_through_one_terminal() {
        let (mut service, _root, _cache) =
            service_with_cache(MediaRuntimeStorage::default()).await.expect("service");

        let term = InMemoryTerm::new(ROWS, COLS);
        let dims = Arc::new(TestDimensionSource::new((ROWS, COLS)));
        let terminal = ProgressTerminal::builder()
            .with_multi_progress(MultiProgress::with_draw_target(ProgressDrawTarget::term_like(
                Box::new(term.clone()),
            )))
            .with_dim_source(Arc::clone(&dims) as Arc<dyn DimensionSource>)
            .with_pre_roll_capture(Box::new(InMemoryTerm::new(ROWS, COLS)))
            .with_ticker_enabled(false)
            .capacity(ROWS as usize)
            .build();

        let grids = Arc::new(Mutex::new(Vec::new()));
        let options = SyncLibraryOptions {
            observer: Some(Arc::new(PhaseGridObserver {
                term: term.clone(),
                grids: Arc::clone(&grids),
            })),
            ..Default::default()
        };

        service
            .sync_library_with_progress_overrides(
                options,
                SyncProgressOverrides { terminal: Some(terminal), no_progress: false },
            )
            .await
            .expect("library sync");

        let grids = grids.lock().expect("grid lock").clone();
        let names: Vec<&str> = grids.iter().map(|(name, _)| *name).collect();
        assert_eq!(
            names,
            vec!["tools", "workflow", "materialization"],
            "every phase must report exactly once, in order"
        );

        let (_, tools_grid) = &grids[0];
        assert!(
            !tools_grid.trim().is_empty(),
            "the tool phase drew nothing into the injected terminal, so it rendered somewhere else"
        );
        assert!(
            tools_grid.lines().any(|line| line.contains('█') || line.contains('░')),
            "no progress bar reached the injected terminal during the tool phase:\n{tools_grid}"
        );

        let (_, workflow_grid) = &grids[1];
        assert!(
            workflow_grid.contains("workflow"),
            "the workflow phase drew no overall bar into the injected terminal:\n{workflow_grid}"
        );
        assert!(
            !workflow_grid.contains("[wf]"),
            "the workflow phase tag is back. It returns if the overall seed regains its \
             `[wf]`, or if a worker slot regains its `[wf]`, so neither half of the \
             screen may name the phase:\n{workflow_grid}"
        );

        let (_, materialization_grid) = &grids[2];
        assert!(
            materialization_grid.contains("materializing"),
            "the materialization phase drew no overall bar into the injected terminal:\n{materialization_grid}"
        );
        assert!(
            !materialization_grid.contains("[mat]"),
            "the materialization phase tag is back on the overall row. It returns if that \
             row's `MaterializationBarLabel` sets `phase`, or if a per-entry row does; \
             only the per-entry rows may name a phase:\n{materialization_grid}"
        );
    }
}
