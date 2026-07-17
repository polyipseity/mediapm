//! Progress bar rendering for long-running operations.
//!
//! This module re-exports types from `mediapm_utils::progress` to maintain
//! the existing `crate::output::progress::*` import paths within the mediapm
//! crate.  See `mediapm_utils::progress` for full documentation.

#[doc(inline)]
pub use mediapm_utils::progress::{
    DimensionSource, ProgressBarApi, ProgressGroup, ProgressGroupApi, TestDimensionSource,
    TestTimeSource, TimeSource, TrackedHandle,
};

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use indicatif::{InMemoryTerm, MultiProgress, ProgressDrawTarget};
    use mediapm_utils::progress::TimeSource;

    use super::*;

    // --- Helpers ---

    fn mk_elapsed() -> (MultiProgress, InMemoryTerm, Arc<TestTimeSource>) {
        let term = InMemoryTerm::new(24, 80);
        let target = ProgressDrawTarget::term_like(Box::new(term.clone()));
        let ts = Arc::new(TestTimeSource::new());
        (MultiProgress::with_draw_target(target), term, ts)
    }

    fn mk_resize(h: u16, w: u16) -> (MultiProgress, InMemoryTerm) {
        let term = InMemoryTerm::new(h, w);
        let target = ProgressDrawTarget::term_like(Box::new(term.clone()));
        (MultiProgress::with_draw_target(target), term)
    }

    // ── Progress elapsed / duration tests ──

    #[test]
    fn consumer_child_bar_elapsed_starts_at_zero() {
        let (mp, term, ts) = mk_elapsed();
        let group = ProgressGroup::builder()
            .with_multi_progress(mp)
            .capacity(4)
            .with_time_source(ts.clone() as Arc<dyn TimeSource>)
            .build();
        let _ = group.add_bar(5, "tool-a");
        let contents = term.contents();
        assert!(contents.contains("0s"), "elapsed must start at 0, got:\n{contents}");
    }

    #[test]
    fn consumer_child_bar_elapsed_frozen_after_finish() {
        let (mp, term, ts) = mk_elapsed();
        let group = ProgressGroup::builder()
            .with_multi_progress(mp)
            .capacity(4)
            .with_time_source(ts.clone() as Arc<dyn TimeSource>)
            .build();
        let child = group.add_bar(5, "tool-a");
        child.set_position(5);
        child.finish();
        group.tick();
        let contents = term.contents();
        assert!(contents.contains("0s"), "elapsed must stay at 0 after finish, got:\n{contents}");
        assert!(contents.contains("5/5"), "bar must show final position 5/5");
    }

    #[test]
    fn consumer_child_bar_elapsed_frozen_after_finish_success() {
        let (mp, term, ts) = mk_elapsed();
        let group = ProgressGroup::builder()
            .with_multi_progress(mp)
            .capacity(4)
            .with_time_source(ts.clone() as Arc<dyn TimeSource>)
            .build();
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
    }

    #[test]
    fn consumer_child_bar_elapsed_frozen_after_finish_error() {
        let (mp, term, ts) = mk_elapsed();
        let group = ProgressGroup::builder()
            .with_multi_progress(mp)
            .capacity(4)
            .with_time_source(ts.clone() as Arc<dyn TimeSource>)
            .build();
        let child = group.add_bar(5, "tool-a");
        child.set_position(2);
        child.finish_error();
        let contents = term.contents();
        assert!(
            contents.contains("0s"),
            "elapsed must stay at 0 after finish_error, got:\n{contents}"
        );
    }

    #[test]
    fn consumer_child_bar_elapsed_frozen_after_abandon() {
        let (mp, term, ts) = mk_elapsed();
        let group = ProgressGroup::builder()
            .with_multi_progress(mp)
            .capacity(4)
            .with_time_source(ts.clone() as Arc<dyn TimeSource>)
            .build();
        let child = group.add_bar(5, "tool-a");
        child.set_position(3);
        child.abandon();
        let contents = term.contents();
        assert!(contents.contains("0s"), "elapsed must stay at 0 after abandon, got:\n{contents}");
    }

    // ── Terminal resize reactivity tests ──

    #[test]
    fn sync_hierarchy_height_change() {
        let dims = Arc::new(TestDimensionSource::new((4, 80)));
        let (mp, term) = mk_resize(6, 80);
        let group = ProgressGroup::builder()
            .with_multi_progress(mp)
            .capacity(4)
            .with_dim_source(Arc::clone(&dims) as Arc<dyn DimensionSource>)
            .dynamic_height(true)
            .build();
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
        let (mp, term) = mk_resize(4, 80);
        let (group, _overall) = ProgressGroup::builder()
            .with_multi_progress(mp)
            .capacity(4)
            .with_overall("syncing tools", 5)
            .with_dim_source(Arc::clone(&dims) as Arc<dyn DimensionSource>)
            .dynamic_height(false)
            .build_with_overall();
        let contents_wide = term.contents();
        assert!(contents_wide.contains("syncing tools"), "overall visible");

        dims.set((4, 40));
        group.tick();
        let contents_narrow = term.contents();
        assert!(
            contents_narrow.contains("syncing tools"),
            "overall still visible after width resize"
        );
        assert_ne!(
            contents_wide, contents_narrow,
            "output changes when width narrows (different bar templates)"
        );
    }

    #[test]
    fn sync_hierarchy_complex_resize_scenario() {
        let dims = Arc::new(TestDimensionSource::new((4, 80)));
        let (mp, term) = mk_resize(6, 80);
        let group = ProgressGroup::builder()
            .with_multi_progress(mp)
            .capacity(4)
            .with_dim_source(Arc::clone(&dims) as Arc<dyn DimensionSource>)
            .dynamic_height(true)
            .build();
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
        assert_eq!(
            restored.lines().count(),
            original.lines().count(),
            "restored line count matches original"
        );
    }
}
