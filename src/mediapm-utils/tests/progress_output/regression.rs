use std::time::Duration;

use super::common::*;

#[test]
fn renderer_with_overall_always_bottom() {
    let (mp, term) = mk_with_size(4, 80);
    let (group, overall) = group_with_overall(mp, 4, "overall", 10);

    // Fill children
    for i in 0..3 {
        let h = group.add_bar(2, &format!("child{i}"));
        h.advance(2);
        h.finish();
        overall.advance(1);
    }
    overall.advance(1);
    overall.finish();
    group.tick();

    assert_eq!(
        term.contents(),
        concat!(
            "⠏                         child0 █████████████████████  2/2 0s\n",
            "⠏                         child1 █████████████████████  2/2 0s\n",
            "⠏                         child2 █████████████████████  2/2 0s\n",
            "⠏                        overall █████████████████████  4/10 0s",
        ),
    );
}

// ── Regression: child ordering is chronological top-to-bottom ──────────

#[test]
fn regression_child_order_chronological_top_to_bottom() {
    let (mp, term) = mk_with_size(5, 80);
    let (group, _overall) = group_with_overall(mp, 5, "overall", 10);

    let _c1 = group.add_bar(5, "first");
    let _c2 = group.add_bar(5, "second");
    let _c3 = group.add_bar(5, "third");
    group.tick();

    group.tick();

    assert_eq!(
        term.contents(),
        concat!(
            "\n",
            "⠸                          first ░░░░░░░░░░░░░░░░░░░░░  0/5 0s 0/d\n",
            "⠸                         second ░░░░░░░░░░░░░░░░░░░░░  0/5 0s 0/d\n",
            "⠴                          third ░░░░░░░░░░░░░░░░░░░░░  0/5 0s 0/d\n",
            "⠸                        overall ░░░░░░░░░░░░░░░░░░░░░  0/10 0s 0/d",
        ),
    );
}

// ── Regression: swap slot does not corrupt display ────────────────────

#[test]
fn regression_swap_slot_does_not_corrupt_display() {
    // Add 2 children, advance both, add 3rd (triggers shift). Verify all
    // children have correct positions and values.
    let (mp, term, ts) = mk_with_size_and_ts(5, 80);
    let (group, _overall) = group_with_overall_and_ts(mp, 5, "overall", 10, &ts);

    let c1 = group.add_bar(10, "alpha");
    let c2 = group.add_bar(10, "beta");
    c1.advance(3);
    c2.advance(7);
    ts.advance(Duration::from_secs(1));
    group.tick();

    assert_eq!(
        term.contents(),
        concat!(
            "\n",
            "\n",
            "⠸                          alpha ██████░░░░░░░░░░░░░░░  3/10 1s 18/m 23s\n",
            "⠼                           beta ██████████████░░░░░░░  7/10 1s 42/m 4s\n",
            "⠹                        overall ░░░░░░░░░░░░░░░░░░░░░  0/10 1s 0/d",
        ),
        "SWAP_BEFORE",
    );

    // Add 3rd child — triggers slot shift.
    let c3 = group.add_bar(10, "gamma");
    c3.advance(5);
    ts.advance(Duration::from_secs(1));
    group.tick();

    assert_eq!(
        term.contents(),
        concat!(
            "\n",
            "⠹                          alpha ██████░░░░░░░░░░░░░░░  3/10 2s 18/m 23s\n",
            "⠴                           beta ██████████████░░░░░░░  7/10 2s 42/m 4s\n",
            "⠧                          gamma ██████████░░░░░░░░░░░  5/10 1s 30/m 10s\n",
            "⠸                        overall ░░░░░░░░░░░░░░░░░░░░░  0/10 2s 0/d",
        ),
        "SWAP_AFTER",
    );
}

// ── Regression: overall bar never shifts ──────────────────────────────

#[test]
fn regression_overall_never_shifts() {
    let (mp, term, ts) = mk_with_size_and_ts(4, 80);
    let (group, overall) = group_with_overall_and_ts(mp, 4, "overall", 10, &ts);

    // Fill all 3 child slots + overall.
    let _c1 = group.add_bar(1, "a");
    let _c2 = group.add_bar(1, "b");
    let _c3 = group.add_bar(1, "c");
    overall.advance(3);
    ts.advance(Duration::from_secs(1));
    group.tick();

    assert_eq!(
        term.contents(),
        concat!(
            "⠹                              a ░░░░░░░░░░░░░░░░░░░░░  0/1 1s 0/d\n",
            "⠹                              b ░░░░░░░░░░░░░░░░░░░░░  0/1 1s 0/d\n",
            "⠼                              c ░░░░░░░░░░░░░░░░░░░░░  0/1 1s 0/d\n",
            "⠹                        overall ██████░░░░░░░░░░░░░░░  3/10 1s 18/m 23s",
        ),
        "OVERALL_NEVER1",
    );

    // Add more children than capacity.  Overall must stay at bottom.
    let _ = group.add_bar(1, "d");
    let _ = group.add_bar(1, "e");
    overall.advance(2);
    ts.advance(Duration::from_secs(1));
    group.tick();

    assert_eq!(
        term.contents(),
        concat!(
            "⠸                              a ░░░░░░░░░░░░░░░░░░░░░  0/1 2s 0/d\n",
            "⠸                              b ░░░░░░░░░░░░░░░░░░░░░  0/1 2s 0/d\n",
            "⠴                              c ░░░░░░░░░░░░░░░░░░░░░  0/1 2s 0/d\n",
            "⠼                        overall ██████████░░░░░░░░░░░  5/10 2s 28/m 10s",
        ),
        "OVERALL_NEVER2",
    );
}

// ── Regression: finish_and_clear via tick_fn on group-managed handle ────

#[test]
fn regression_finish_and_clear_with_tick_fn() {
    let (mp, term) = mk_with_size(5, 80);
    let (group, _overall) = group_with_overall(mp, 5, "overall", 3);

    let _c1 = group.add_bar(5, "keep");
    let c2 = group.add_bar(5, "clear");
    group.tick();

    // c2 is ProgressGroup-managed so mutating methods go through tick_fn.
    c2.finish_and_clear();
    group.tick();

    assert_eq!(
        term.contents(),
        concat!(
            "\n",
            "\n",
            "⠸                           keep ░░░░░░░░░░░░░░░░░░░░░  0/5 0s 0/d\n",
            "\n",
            "⠸                        overall ░░░░░░░░░░░░░░░░░░░░░  0/3 0s 0/d",
        ),
        "FINISH_CLEAR1",
    );

    // Ensure cleared bar is counted as finished — its state should not shift on next add_bar.
    let _c3 = group.add_bar(5, "new");
    group.tick();

    assert_eq!(
        term.contents(),
        concat!(
            "\n",
            "⠹                           keep ░░░░░░░░░░░░░░░░░░░░░  0/5 0s 0/d\n",
            "⠸                          clear ░░░░░░░░░░░░░░░░░░░░░  0/5 0s\n",
            "⠴                            new ░░░░░░░░░░░░░░░░░░░░░  0/5 0s 0/d\n",
            "⠼                        overall ░░░░░░░░░░░░░░░░░░░░░  0/3 0s 0/d",
        ),
        "FINISH_CLEAR2",
    );
}

// ── Regression: concurrent set_position + renderer.tick() ──

#[test]
fn regression_concurrent_set_and_sync() {
    let (mp, term, ts) = mk_with_size_and_ts(5, 80);
    let (group, _overall) = group_with_overall_and_ts(mp, 5, "overall", 100, &ts);

    let c1 = group.add_bar(50, "worker");
    // Rapid set_position to exercise tick_fn callback path.
    for i in 0..20 {
        c1.set_position(i * 2);
    }
    ts.advance(Duration::from_millis(0));
    group.tick();

    let output = term.contents();
    assert_eq!(
        output,
        concat!(
            "\n",
            "\n",
            "\n",
            "⠸                         worker ███████████████░░░░░░  38/50 0s 0/d\n",
            "⠹                        overall ░░░░░░░░░░░░░░░░░░░░░  0/100 0s 0/d",
        ),
    );
}

// ── Regression: recycle finished slot after full ──────────────────────

#[test]
fn regression_recycle_finished_slot_after_full() {
    let (mp, term, ts) = mk_with_size_and_ts(5, 80);
    let (group, overall) = group_with_overall_and_ts(mp, 5, "overall", 5, &ts);

    // Fill all 4 child slots.
    let children: Vec<_> = (0..4).map(|i| group.add_bar(2, &format!("task{i}"))).collect();
    overall.advance(4);
    ts.advance(Duration::from_secs(1));
    group.tick();

    assert_eq!(
        term.contents(),
        concat!(
            "⠹                          task0 ░░░░░░░░░░░░░░░░░░░░░  0/2 1s 0/d\n",
            "⠹                          task1 ░░░░░░░░░░░░░░░░░░░░░  0/2 1s 0/d\n",
            "⠹                          task2 ░░░░░░░░░░░░░░░░░░░░░  0/2 1s 0/d\n",
            "⠴                          task3 ░░░░░░░░░░░░░░░░░░░░░  0/2 1s 0/d\n",
            "⠹                        overall ████████████████░░░░░  4/5 1s 24/m 2s",
        ),
        "RECYCLE_FULL_PRE_CLEAR",
    );

    // Finish and clear task0 — must not panic or corrupt display.
    children[0].finish_and_clear();
    ts.advance(Duration::from_secs(1));
    group.tick();

    assert_eq!(
        term.contents(),
        concat!(
            "\n",
            "⠸                          task1 ░░░░░░░░░░░░░░░░░░░░░  0/2 2s 0/d\n",
            "⠸                          task2 ░░░░░░░░░░░░░░░░░░░░░  0/2 2s 0/d\n",
            "⠦                          task3 ░░░░░░░░░░░░░░░░░░░░░  0/2 2s 0/d\n",
            "⠸                        overall ████████████████░░░░░  4/5 2s 24/m 2s",
        ),
        "RECYCLE_FULL_CLEARED",
    );

    // Add a 5th child — it should reuse the recycled slot.
    let _c4 = group.add_bar(2, "task4");
    ts.advance(Duration::from_secs(1));
    group.tick();

    assert_eq!(
        term.contents(),
        concat!(
            "⠸                          task1 ░░░░░░░░░░░░░░░░░░░░░  0/2 2s 0/d\n",
            "⠼                          task2 ░░░░░░░░░░░░░░░░░░░░░  0/2 2s 0/d\n",
            "⠼                          task3 ░░░░░░░░░░░░░░░░░░░░░  0/2 2s 0/d\n",
            "⠇                          task4 ░░░░░░░░░░░░░░░░░░░░░  0/2 1s 0/d\n",
            "⠼                        overall ████████████████░░░░░  4/5 2s 24/m 2s",
        ),
        "RECYCLE_FULL_DUMP",
    );
}

// ── Regression: newest finished bars survive Phase 2 compact ────────────

#[test]
fn regression_recycle_oldest_finished_slot() {
    // Use W=80 so the overall bar's rate+ETA message (which can be 18+
    // visible chars) fits without wrapping in the compact template.
    let (mp, term, ts) = mk_with_size_and_ts(10, 80);
    let (group, overall) = group_with_overall_and_ts(mp, 6, "overall", 5, &ts);
    // child_cap = 5, overall at slot 5

    // Fill all 5 child slots with bars from 2 tools (simulating 3-phase
    // provisioning where each tool produces resolve/fetch/process bars).
    let a1 = group.add_bar(1, "a [resolve]");
    let a2 = group.add_bar(1, "a [fetch]");
    let a3 = group.add_bar(1, "a [process]");
    let b1 = group.add_bar(1, "b [resolve]");
    let b2 = group.add_bar(1, "b [fetch]");
    a1.finish();
    a2.finish();
    a3.finish();
    b1.finish();
    b2.finish();
    overall.advance(2);
    ts.advance(Duration::from_secs(1));
    group.tick();

    // All bars visible pre-compact.
    assert_eq!(
        term.contents(),
        concat!(
            "⠏                    a [resolve] █████████████████████  0/1 0s\n",
            "⠏                      a [fetch] █████████████████████  0/1 0s\n",
            "⠏                    a [process] █████████████████████  0/1 0s\n",
            "⠏                    b [resolve] █████████████████████  0/1 0s\n",
            "⠏                      b [fetch] █████████████████████  0/1 0s\n",
            "⠹                        overall ████████░░░░░░░░░░░░░  2/5 1s 12/m 15s",
        ),
        "RECYCLE_OLDEST_PRE_COMPACT",
    );

    // Add b [process] — triggers Phase 2 compact (all slots occupied).
    let b3 = group.add_bar(1, "b [process]");
    b3.finish();
    overall.advance(1);
    overall.finish();
    group.tick();

    // After compact: oldest (a1 at old_i=0) is recycled, everything
    // shifts up, b3 at bottom.  All 3 b-specific bars must be visible.
    assert_eq!(
        term.contents(),
        concat!(
            "⠏                      a [fetch] █████████████████████  0/1 0s\n",
            "⠏                    a [process] █████████████████████  0/1 0s\n",
            "⠏                    b [resolve] █████████████████████  0/1 0s\n",
            "⠏                      b [fetch] █████████████████████  0/1 0s\n",
            "⠏                    b [process] █████████████████████  0/1 0s\n",
            "⠏                        overall █████████████████████  3/5 1s",
        ),
        "RECYCLE_OLDEST_POST_COMPACT",
    );
}
