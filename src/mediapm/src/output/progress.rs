//! Progress bar rendering for long-running operations.
//!
//! This module re-exports types from `mediapm_utils::progress` to maintain
//! the existing `crate::output::progress::*` import paths within the mediapm
//! crate.  See `mediapm_utils::progress` for full documentation.

#[doc(inline)]
pub use mediapm_utils::progress::{
    DebugSlotState, DebugTickSnapshot, DimensionSource, PrefixComponents, ProgressBarApi,
    ProgressBarHandle, ProgressDebugSink, ProgressScreen, ProgressScreenApi, ProgressTerminal,
    SuffixComponents, TestDimensionSource, TestTimeSource, TimeSource,
};

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use indicatif::InMemoryTerm;
    use mediapm_utils::progress::{ProgressTerminalBuilder, TimeSource};

    use super::*;

    /// An in-memory grid paired with the time source these elapsed tests drive.
    fn mk_elapsed() -> (InMemoryTerm, Arc<TestTimeSource>) {
        (InMemoryTerm::new(24, 80), Arc::new(TestTimeSource::new()))
    }

    /// A fresh in-memory grid of the requested size for the resize tests.
    fn mk_resize(h: u16, w: u16) -> InMemoryTerm {
        InMemoryTerm::new(h, w)
    }

    /// A terminal builder whose gate is paired with `term`, the grid the test
    /// reads back, with pre-roll captured.
    ///
    /// The capture keeps the terminal's one-shot pre-roll (one blank line per
    /// terminal row) off fd 2 and out of that grid. An overall bar belongs to
    /// the *screen*: only `terminal.screen().with_overall()` registers the
    /// bottom slot.
    fn mk_terminal(term: &InMemoryTerm) -> ProgressTerminalBuilder {
        ProgressTerminal::builder()
            .with_term_like(Box::new(term.clone()))
            .with_pre_roll_capture(Box::new(InMemoryTerm::new(24, 80)))
    }

    #[test]
    fn consumer_child_bar_elapsed_starts_at_zero() {
        let (term, ts) = mk_elapsed();
        let terminal = mk_terminal(&term)
            .capacity(4)
            .with_time_source(ts.clone() as Arc<dyn TimeSource>)
            .build();
        let group = terminal.screen().build();
        let _ = group.add_bar(5, "tool-a");
        // The single writer paints on a frame and `tick` commits one: `add_bar`
        // alone leaves the grid empty (the deleted bar path painted on
        // creation). The elapsed string itself is unchanged — it starts at 0.
        group.tick();
        let contents = term.contents();
        assert!(contents.contains("0s"), "elapsed must start at 0, got:\n{contents}");
    }

    #[test]
    fn consumer_child_bar_elapsed_frozen_after_finish() {
        let (term, ts) = mk_elapsed();
        let terminal = mk_terminal(&term)
            .capacity(4)
            .with_time_source(ts.clone() as Arc<dyn TimeSource>)
            .build();
        let group = terminal.screen().build();
        let child = group.add_bar(5, "tool-a");
        child.set_position(5);
        child.finish_success();
        group.tick();
        let contents = term.contents();
        assert!(contents.contains("0s"), "elapsed must stay at 0 after finish, got:\n{contents}");
        assert!(contents.contains("5/5"), "bar must show final position 5/5");
        // Freeze over the row's lifetime: a Done row stops the clock, so a
        // later tick may not grow the elapsed.
        ts.advance(Duration::from_mins(1));
        group.tick();
        let contents = term.contents();
        assert!(
            contents.contains("0s") && contents.contains("5/5"),
            "a finished row must stay at 0s for its whole life, got:\n{contents}"
        );
    }

    #[test]
    fn consumer_child_bar_elapsed_frozen_after_finish_success() {
        let (term, ts) = mk_elapsed();
        let terminal = mk_terminal(&term)
            .capacity(4)
            .with_time_source(ts.clone() as Arc<dyn TimeSource>)
            .build();
        let group = terminal.screen().build();
        let child = group.add_bar(5, "tool-a");
        child.set_position(5);
        child.finish_success();
        group.tick();
        let contents = term.contents();
        assert!(
            contents.contains("0s"),
            "elapsed must stay at 0 after finish_success, got:\n{contents}"
        );
        assert!(contents.contains("5/5"), "final position must appear");
        // Freeze over the row's lifetime: a Done row stops the clock, so a
        // later tick may not grow the elapsed.
        ts.advance(Duration::from_mins(1));
        group.tick();
        let contents = term.contents();
        assert!(
            contents.contains("0s") && contents.contains("5/5"),
            "a finished row must stay at 0s for its whole life, got:\n{contents}"
        );
    }

    #[test]
    fn consumer_child_bar_elapsed_frozen_after_finish_error() {
        let (term, ts) = mk_elapsed();
        let terminal = mk_terminal(&term)
            .capacity(4)
            .with_time_source(ts.clone() as Arc<dyn TimeSource>)
            .build();
        let group = terminal.screen().build();
        let child = group.add_bar(5, "tool-a");
        child.set_position(2);
        child.finish_error();
        // The single writer paints on a frame; `tick` commits one (the
        // deleted bar path repainted on finish_error directly).
        group.tick();
        let contents = term.contents();
        assert!(
            contents.contains("0s"),
            "elapsed must stay at 0 after finish_error, got:\n{contents}"
        );
        // Freeze over the row's lifetime: a Failed row stops the clock too.
        ts.advance(Duration::from_mins(1));
        group.tick();
        let contents = term.contents();
        assert!(
            contents.contains("0s"),
            "a failed row must stay at 0s for its whole life, got:\n{contents}"
        );
    }

    #[test]
    fn consumer_child_bar_elapsed_frozen_after_finish_error_alt() {
        let (term, ts) = mk_elapsed();
        let terminal = mk_terminal(&term)
            .capacity(4)
            .with_time_source(ts.clone() as Arc<dyn TimeSource>)
            .build();
        let group = terminal.screen().build();
        let child = group.add_bar(5, "tool-a");
        child.set_position(3);
        child.finish_error();
        // The single writer paints on a frame; `tick` commits one (the
        // deleted bar path repainted on finish_error directly).
        group.tick();
        let contents = term.contents();
        assert!(
            contents.contains("0s"),
            "elapsed must stay at 0 after finish_error, got:\n{contents}"
        );
        // Freeze over the row's lifetime: a Failed row stops the clock too.
        ts.advance(Duration::from_mins(1));
        group.tick();
        let contents = term.contents();
        assert!(
            contents.contains("0s"),
            "a failed row must stay at 0s for its whole life, got:\n{contents}"
        );
    }

    #[test]
    fn sync_hierarchy_height_change() {
        let dims = Arc::new(TestDimensionSource::new((4, 80)));
        let term = mk_resize(6, 80);
        let terminal = mk_terminal(&term)
            .capacity(4)
            .with_dim_source(Arc::clone(&dims) as Arc<dyn DimensionSource>)
            .dynamic_height(true)
            .build();
        let group = terminal.screen().build();
        let _child = group.add_bar(10, "materialize");
        group.tick();

        let initial = term.contents();
        assert!(initial.contains("materialize"), "child visible before resize");
        let initial_count = initial.lines().count();

        dims.set((6, 80));
        group.tick();
        let after = term.contents();
        let after_count = after.lines().count();
        assert!(after.contains("materialize"), "child still visible after height resize");
        assert!(
            after_count > initial_count,
            "more lines after height growth (was {initial_count}, now {after_count})"
        );
    }

    #[test]
    fn reconcile_desired_tools_width_change() {
        let dims = Arc::new(TestDimensionSource::new((4, 80)));
        let term = mk_resize(4, 80);
        let terminal = mk_terminal(&term)
            // 3 child slots; the overall bar adds a fourth.
            .capacity(3)
            .with_dim_source(Arc::clone(&dims) as Arc<dyn DimensionSource>)
            .dynamic_height(false)
            .build();
        let (group, _overall) = terminal.screen().with_overall("syncing tools", 5).build();
        // The single writer paints on a frame; `tick` commits the first one
        // (the deleted bar path painted the overall row on screen build).
        group.tick();
        let contents_wide = term.contents();
        assert!(contents_wide.contains("syncing tools"), "overall visible");

        dims.set((4, 40));
        group.tick();
        let contents_narrow = term.contents();
        assert!(
            contents_narrow.contains("syncing tools"),
            "overall still visible after width resize"
        );
        // The dimension source's COLUMNS are inert for the drawn width: the
        // frame is measured at the write target's own width, pinned by
        // `dimension_source_columns_do_not_change_the_frame` in the utils
        // suite (a pinned ruling, not a regression). So the row keeps its
        // 80-column measurement across the dims change — what this pins at
        // the integration level is that reconcile's overall row survives a
        // width-resize signal undisturbed.
        assert_eq!(
            contents_wide, contents_narrow,
            "a dim-source width change must not disturb the drawn overall row"
        );
    }

    #[test]
    fn sync_hierarchy_complex_resize_scenario() {
        let dims = Arc::new(TestDimensionSource::new((4, 80)));
        let term = mk_resize(6, 80);
        let terminal = mk_terminal(&term)
            .capacity(4)
            .with_dim_source(Arc::clone(&dims) as Arc<dyn DimensionSource>)
            .dynamic_height(true)
            .build();
        let group = terminal.screen().build();
        let _child = group.add_bar(10, "materialize");
        group.tick();
        let original = term.contents();
        assert!(original.contains("materialize"), "child visible at start");

        // Grow height.
        dims.set((6, 80));
        group.tick();
        let grown = term.contents();
        assert!(grown.contains("materialize"), "child visible after growth");
        assert!(grown.lines().count() > original.lines().count(), "more lines after growth");

        // Restore original dimensions.
        dims.set((4, 80));
        group.tick();
        let restored = term.contents();
        assert!(restored.contains("materialize"), "child visible after restore");
        // After grow→shrink, the content bar shifts within the virtual terminal
        // buffer, so the count of trailing-empty lines may differ from the original.
        // What matters is the count of non-empty content lines.
        let restored_non_empty = restored.lines().filter(|l| !l.is_empty()).count();
        let original_non_empty = original.lines().filter(|l| !l.is_empty()).count();
        assert_eq!(
            restored_non_empty, original_non_empty,
            "restored non-empty line count matches original"
        );
    }
}
