use indicatif::{InMemoryTerm, MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use mediapm_utils::progress::{
    DimensionSource, ProgressGroup, TestDimensionSource, TestTimeSource, TimeSource, TrackedHandle,
};
use std::sync::Arc;

use super::common::*;

// ═════════════════════════════════════════════════════════════════════════════
// Phase 1: Exact-output regression tests
// ═════════════════════════════════════════════════════════════════════════════

/// Exact output match for parallel workers with `finish_error` + active.
///
/// Uses `TestTimeSource` for deterministic rate and elapsed values.
#[test]
fn exact_consumer_parallel_worker_output() {
    let (mp, term, ts) = mk_with_size_and_ts(5, 80);
    let (group, _overall) = group_with_overall_and_ts(mp, 5, "overall", 10, &ts);

    let a = group.add_bar(5, "worker-a");
    let b = group.add_bar(5, "worker-b");

    a.advance(3);
    b.advance(2);
    ts.advance(std::time::Duration::from_secs(1));
    group.tick();

    a.finish_error();
    b.advance(1);
    ts.advance(std::time::Duration::from_secs(1));
    group.tick();

    assert_eq!(
        term.contents(),
        concat!(
            "\n",
            "\n",
            "⠏                   [F] worker-a ████████████░░░░░░░░░  3/5 1s\n",
            "⠦                       worker-b ████████████░░░░░░░░░  3/5 2s 17/m 7s\n",
            "⠸                        overall ░░░░░░░░░░░░░░░░░░░░░  0/10 2s 0/d",
        )
    );
}

/// Exact output match for sequential tool sync with slot recycling.
#[test]
fn exact_consumer_sync_too_many_tools_recycles() {
    let (mp, term) = mk_with_size(4, 80);
    let group = group(mp, 4);

    for i in 0..8 {
        let tool = group.add_bar(1, &format!("tool{i}"));
        tool.advance(1);
        tool.finish_success();
        group.tick();
    }

    assert_eq!(
        term.contents(),
        concat!(
            "⠙                          tool4 █████████████████████  1/1 0s\n",
            "⠙                          tool5 █████████████████████  1/1 0s\n",
            "⠏                          tool6 █████████████████████  1/1 0s\n",
            "⠏                          tool7 █████████████████████  1/1 0s",
        )
    );
}

/// Exact output match for retention of finished bars (sequential add/completion).
#[test]
fn exact_consumer_retention_finished_bar() {
    let (mp, term) = mk_with_size(4, 80);
    let group = group(mp, 4);

    let a = group.add_bar(2, "alpha");
    a.advance(2);
    a.finish_success();
    group.tick();

    let b = group.add_bar(3, "beta");
    b.advance(3);
    b.finish_success();
    group.tick();

    assert_eq!(
        term.contents(),
        concat!(
            "\n",
            "\n",
            "⠏                          alpha █████████████████████  2/2 0s\n",
            "⠏                           beta █████████████████████  3/3 0s",
        )
    );
}

/// Exact output match for multiple finished bars filling all slots.
#[test]
fn exact_consumer_retention_multiple_finished() {
    let (mp, term) = mk_with_size(4, 80);
    let group = group(mp, 4);

    for (i, _msg) in ["first", "second", "third", "fourth"].iter().enumerate() {
        let h = group.add_bar(1, &format!("task{i}"));
        h.advance(1);
        h.finish_success();
        group.tick();
    }

    assert_eq!(
        term.contents(),
        concat!(
            "⠙                          task0 █████████████████████  1/1 0s\n",
            "⠙                          task1 █████████████████████  1/1 0s\n",
            "⠏                          task2 █████████████████████  1/1 0s\n",
            "⠏                          task3 █████████████████████  1/1 0s",
        )
    );
}
