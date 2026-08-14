use indicatif::{InMemoryTerm, MultiProgress, ProgressDrawTarget};
use mediapm_utils::progress::{
    DimensionSource, ProgressGroup, TestDimensionSource, TestTimeSource, TimeSource, TrackedHandle,
};
use std::sync::Arc;

use super::common::*;

/// Helper: build a group with a wide terminal (4x120) but narrow dims (4x40),
/// so truncation uses the compact-template budget without wrap artifacts
/// (resize.rs pattern) and the full compact-template line is assertable.
fn resolve_group() -> (ProgressGroup, InMemoryTerm) {
    let dims = Arc::new(TestDimensionSource::new((4, 40)));
    let term = InMemoryTerm::new(4, 120);
    let target = ProgressDrawTarget::term_like(Box::new(term.clone()));
    let mp = MultiProgress::with_draw_target(target);
    let ts = Arc::new(TestTimeSource::new());
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(4)
        .with_overall("overall", 1)
        .with_dim_source(Arc::clone(&dims) as Arc<dyn DimensionSource>)
        .with_time_source(ts.clone() as Arc<dyn TimeSource>)
        .with_ticker_enabled(false)
        .build_with_overall();
    (group, term)
}

/// Long resolve label: version is truncated but `[res]` phase is preserved.
///
/// Production calls `group.add_bar(total, &format!("{tool_id}{version_suffix} [res]"))`
/// (e.g. `"ffmpeg autobuild-2026-07-31 [res]"`). The label must be parsed into
/// components at construction so truncation removes the version first and keeps
/// the phase tag visible.
#[test]
fn resolve_label_long_keeps_phase_tag() {
    // Wide terminal + narrow dims (see `resolve_group`): dims select the
    // compact template and truncation budget.
    let (group, term) = resolve_group();
    let child = group.add_bar(100, "ffmpeg autobuild-2026-07-31 [res]");
    child.set_position(0);
    group.tick();

    assert_eq!(
        term.contents(),
        concat!(
            "\n",
            "\n",
            "⠹     ffmpeg autobuil [res]  0/100 0s 0/d\n",
            "⠹                   overall  0/1 0s 0/d",
        ),
        "W=40: version truncated, [res] phase preserved",
    );
}

/// Multi-word label without bracket: entire label is the tool name.
///
/// Production calls `pg.add_bar(total, "syncing tools")` (multi-word label with
/// no phase). The no-bracket parser branch must keep the whole string as the
/// tool name instead of only the first token.
#[test]
fn resolve_label_multiword_no_bracket_keeps_whole_label() {
    // Wide terminal + narrow dims (see `resolve_group`).
    let (group, term) = resolve_group();
    let child = group.add_bar(100, "syncing tools");
    child.set_position(0);
    group.tick();

    assert_eq!(
        term.contents(),
        concat!(
            "\n",
            "\n",
            "⠹             syncing tools  0/100 0s 0/d\n",
            "⠹                   overall  0/1 0s 0/d",
        ),
        "W=40: multi-word label kept whole",
    );
}
