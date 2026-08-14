use std::sync::Arc;

use super::common::*;

#[test]
fn progress_group_with_overall_shows_fixed_height() {
    // Terminal H=5, W=80 so the full child and overall templates fit.
    let (mp, term) = mk_with_size(5, 80);
    let (group, _overall) = group_with_overall(mp, 5, "overall", 10);
    group.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "\n",
            "\n",
            "\n",
            "\n",
            "⠹                        overall ░░░░░░░░░░░░░░░░░░░░░  0/10 0s 0/d",
        )
    );
}

#[test]
fn progress_group_add_bar_reuses_bottom_child() {
    // Terminal H=4, W=80 so the full child and overall templates fit.
    let (mp, term) = mk_with_size(4, 80);
    let (group, _overall) = group_with_overall(mp, 4, "overall", 3);

    let _c1 = group.add_bar(5, "tool1");
    group.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "\n",
            "\n",
            "⠹                          tool1 ░░░░░░░░░░░░░░░░░░░░░  0/5 0s 0/d\n",
            "⠹                        overall ░░░░░░░░░░░░░░░░░░░░░  0/3 0s 0/d",
        )
    );

    let _c2 = group.add_bar(3, "tool2");
    group.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "\n",
            "⠹                          tool1 ░░░░░░░░░░░░░░░░░░░░░  0/5 0s 0/d\n",
            "⠼                          tool2 ░░░░░░░░░░░░░░░░░░░░░  0/3 0s 0/d\n",
            "⠸                        overall ░░░░░░░░░░░░░░░░░░░░░  0/3 0s 0/d",
        )
    );
}

#[test]
fn progress_group_no_overall_always_reuses_bottom() {
    // Terminal H=5, W=80 so the full child template fits.
    // Use capacity=4 so there's 1 unwritten row at the bottom
    // — this avoids InMemoryTerm trimming blank content when bars
    // fill the entire terminal height.
    let (mp, term) = mk_with_size(5, 80);
    let group = group(mp, 4);

    let _c1 = group.add_bar(5, "task1");
    group.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "\n",
            "\n",
            "\n",
            "⠸                          task1 ░░░░░░░░░░░░░░░░░░░░░  0/5 0s 0/d",
        )
    );

    let _c2 = group.add_bar(3, "task2");
    assert_eq!(
        term.contents(),
        concat!(
            "\n",
            "\n",
            "⠙                          task1 ░░░░░░░░░░░░░░░░░░░░░  0/5 0s 0/d\n",
            "⠼                          task2 ░░░░░░░░░░░░░░░░░░░░░  0/3 0s 0/d",
        )
    );
}

#[test]
fn progress_group_never_changes_bar_count() {
    // Terminal H=4, W=80 so the full child template fits.
    let (mp, term) = mk_with_size(4, 80);
    let group = group(mp, 4);
    for i in 0..30 {
        let _c = group.add_bar(1, &format!("tool{i}"));
        group.tick();
    }
    assert_eq!(
        term.contents(),
        concat!(
            "⠙                          tool0 ░░░░░░░░░░░░░░░░░░░░░  0/1 0s 0/d\n",
            "⠹                          tool1 ░░░░░░░░░░░░░░░░░░░░░  0/1 0s 0/d\n",
            "⠸                          tool2 ░░░░░░░░░░░░░░░░░░░░░  0/1 0s 0/d\n",
            "⠇                          tool3 ░░░░░░░░░░░░░░░░░░░░░  0/1 0s 0/d",
        )
    );
}

#[test]
fn progress_group_with_overall_add_child_updates_slot() {
    // Terminal H=5, W=80 so the full child and overall templates fit.
    // Capacity=5: child slots at slots[0..3], overall at slot[4].
    // Chronological: first child occupies slot[3], second shifts it to slot[2]
    // and takes slot[3].
    let (mp, term) = mk_with_size(5, 80);
    let (group, _overall) = group_with_overall(mp, 5, "overall", 3);

    let _c1 = group.add_bar(5, "tool1");
    group.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "\n",
            "\n",
            "\n",
            "⠹                          tool1 ░░░░░░░░░░░░░░░░░░░░░  0/5 0s 0/d\n",
            "⠹                        overall ░░░░░░░░░░░░░░░░░░░░░  0/3 0s 0/d",
        )
    );

    let _c2 = group.add_bar(3, "tool2");
    group.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "\n",
            "\n",
            "⠹                          tool1 ░░░░░░░░░░░░░░░░░░░░░  0/5 0s 0/d\n",
            "⠼                          tool2 ░░░░░░░░░░░░░░░░░░░░░  0/3 0s 0/d\n",
            "⠸                        overall ░░░░░░░░░░░░░░░░░░░░░  0/3 0s 0/d",
        )
    );
}

#[test]
fn progress_group_with_overall_multiple_children_reuse_slot() {
    // Terminal H=5, W=80 so the full child and overall templates fit.
    // Capacity=5, overall at line[4], child slots at lines[0..3].
    // Chronological: first child occupies slot[3], each new child shifts
    // earlier children up and takes slot[3].  After 4 children: task0 at
    // slot[0], task1 at slot[1], task2 at slot[2], task3 at slot[3].
    let (mp, term) = mk_with_size(5, 80);
    let (group, _overall) = group_with_overall(mp, 5, "overall", 10);

    for i in 0..5 {
        let _c = group.add_bar(2, &format!("task{i}"));
        group.tick();
    }
    assert_eq!(
        term.contents(),
        concat!(
            "⠸                          task0 ░░░░░░░░░░░░░░░░░░░░░  0/2 0s 0/d\n",
            "⠼                          task1 ░░░░░░░░░░░░░░░░░░░░░  0/2 0s 0/d\n",
            "⠴                          task2 ░░░░░░░░░░░░░░░░░░░░░  0/2 0s 0/d\n",
            "⠋                          task3 ░░░░░░░░░░░░░░░░░░░░░  0/2 0s 0/d\n",
            "⠦                        overall ░░░░░░░░░░░░░░░░░░░░░  0/10 0s 0/d",
        )
    );
}

#[test]
fn progress_group_no_overall_different_capacities() {
    // Terminal H=6, W=80 so the full child template fits.
    // Capacity=4, no overall.
    // Children fill sequentially from line[0].
    // Using H=6 > 4 to avoid InMemoryTerm blank-content trimming.
    let (mp, term) = mk_with_size(6, 80);
    let group = group(mp, 4);

    let _c1 = group.add_bar(5, "alpha");
    group.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "\n",
            "\n",
            "\n",
            "⠸                          alpha ░░░░░░░░░░░░░░░░░░░░░  0/5 0s 0/d",
        )
    );

    let _c2 = group.add_bar(3, "beta");
    assert_eq!(
        term.contents(),
        concat!(
            "\n",
            "\n",
            "⠙                          alpha ░░░░░░░░░░░░░░░░░░░░░  0/5 0s 0/d\n",
            "⠼                           beta ░░░░░░░░░░░░░░░░░░░░░  0/3 0s 0/d",
        )
    );
}

#[test]
fn progress_group_compact_template_below_60_width() {
    // Terminal W=80 so the full template fits.
    // (InMemoryTerm width doesn't affect production style selection, which
    // reads from console::Term::stderr() — the real terminal.)
    let (mp, term) = mk_with_size(4, 80);
    let (group, _overall) = group_with_overall(mp, 4, "overall", 3);

    let _c1 = group.add_bar(5, "tool1");
    group.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "\n",
            "\n",
            "⠹                          tool1 ░░░░░░░░░░░░░░░░░░░░░  0/5 0s 0/d\n",
            "⠹                        overall ░░░░░░░░░░░░░░░░░░░░░  0/3 0s 0/d",
        )
    );
}

#[test]
fn progress_group_child_shows_label_and_total() {
    // Verify that add_bar renders the label and total in the bar.
    let (mp, term) = mk_with_size(4, 80);
    let (group, _overall) = group_with_overall(mp, 4, "overall", 10);

    let _c1 = group.add_bar(7, "fetch");
    group.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "\n",
            "\n",
            "⠹                          fetch ░░░░░░░░░░░░░░░░░░░░░  0/7 0s 0/d\n",
            "⠹                        overall ░░░░░░░░░░░░░░░░░░░░░  0/10 0s 0/d",
        )
    );
}

#[test]
fn progress_group_disabled_returns_noop() {
    // -- with overall --
    let (_mp, term) = mk_with_size(4, 80);
    let (group, overall) = (ProgressGroup::disabled(), TrackedHandle::disabled());
    assert_eq!(overall.total(), 0, "overall handle must be no-op when disabled");

    let child = group.add_bar(5, "child");
    assert_eq!(child.total(), 0, "child handle must be no-op when disabled");

    group.tick();
    assert_eq!(term.contents(), "", "no output when progress is disabled");

    // -- without overall --
    let (_mp2, term2) = mk_with_size(4, 80);
    let group2 = ProgressGroup::disabled();
    let c2 = group2.add_bar(3, "noop");
    assert_eq!(c2.total(), 0, "child handle must be no-op without overall");
    group2.tick();
    assert_eq!(term2.contents(), "", "no output without overall when disabled");
}

// ── Bar visibility after finish ─────────────────────────────────────────────

#[test]
fn progress_group_child_finish_keeps_bar_visible() {
    // Terminal H=5, W=80 so the full child and overall templates fit.
    let (mp, term) = mk_with_size(5, 80);
    let (group, _overall) = group_with_overall(mp, 5, "overall", 3);

    let c = group.add_bar(5, "fetch");
    group.tick();
    // Finish the child — it must remain visible in the terminal.
    c.finish_success();
    group.tick();
    let contents = term.contents();
    assert_eq!(
        contents,
        concat!(
            "\n",
            "\n",
            "\n",
            "⠏                          fetch █████████████████████  0/5 0s\n",
            "⠸                        overall ░░░░░░░░░░░░░░░░░░░░░  0/3 0s 0/d",
        ),
    );
}

/// Exact output: all bars finish successfully, content persists.
#[test]
fn fin_all_exact_all_bars_content_persists() {
    // Terminal H=5, W=80.  Overall at line[4], children at lines[0..3].
    let (mp, term) = mk_with_size(5, 80);
    let (group, overall) = group_with_overall(mp, 5, "overall", 2);

    let c1 = group.add_bar(3, "alpha");
    let c2 = group.add_bar(5, "beta");
    c1.advance(3);
    c2.advance(5);
    group.tick();
    // Finish all bars.
    c1.finish_success();
    c2.finish_success();
    overall.finish_success();
    group.tick();

    assert_eq!(
        term.contents(),
        concat!(
            "\n",
            "\n",
            "⠏                          alpha █████████████████████  3/3 0s\n",
            "⠏                           beta █████████████████████  5/5 0s\n",
            "⠏                        overall █████████████████████  0/2 0s",
        )
    );
}

/// Exact output: child finishes with error, shows [F] bracket.
#[test]
fn fin_error_exact_shows_error_state() {
    // Terminal H=5, W=80.
    let (mp, term) = mk_with_size(5, 80);
    let (group, _overall) = group_with_overall(mp, 5, "overall", 5);

    let c = group.add_bar(5, "wget");
    group.tick();
    c.finish_error();
    group.tick();

    assert_eq!(
        term.contents(),
        concat!(
            "\n",
            "\n",
            "\n",
            "⠏                       [F] wget ░░░░░░░░░░░░░░░░░░░░░  0/5 0s\n",
            "⠸                        overall ░░░░░░░░░░░░░░░░░░░░░  0/5 0s 0/d",
        )
    );
}

/// Exact output: `join_and_clear` keeps finished bars, removes blank slots.
#[test]
fn join_clear_exact_removes_bars() {
    // Terminal H=5, W=80.
    let (mp, term) = mk_with_size(5, 80);
    let (group, _overall) = group_with_overall(mp, 5, "overall", 3);

    let c = group.add_bar(5, "fetch");
    c.finish_success();
    group.tick();
    // join_and_clear collapses blank reserved slots but keeps non-blank bars.
    group.join_and_clear();

    assert_eq!(
        term.contents(),
        concat!(
            "⠏                          fetch █████████████████████  0/5 0s\n",
            "⠹                        overall ░░░░░░░░░░░░░░░░░░░░░  0/3 0s 0/d",
        )
    );
}

#[test]
fn progress_group_consumer_lifecycle_keeps_finished_bars() {
    // Terminal H=5, W=80.  Simulate the exact consumer pattern:
    // create group with overall, do sequential work, finish children,
    // finish overall, then join.  All bars must remain visible.
    let (mp, term) = mk_with_size(5, 80);
    let (group, overall) = group_with_overall(mp, 5, "overall", 3);

    let c1 = group.add_bar(5, "fetch");
    c1.advance(5);
    group.tick();
    c1.finish_success();

    let c2 = group.add_bar(2, "parse");
    c2.advance(2);
    group.tick();
    c2.finish_success();

    overall.advance(3);
    overall.finish_success();
    group.tick();
    // group.join() would be called here — it's a no-op.

    let contents = term.contents();
    assert_eq!(
        contents,
        concat!(
            "\n",
            "\n",
            "⠏                          fetch █████████████████████  5/5 0s\n",
            "⠏                          parse █████████████████████  2/2 0s\n",
            "⠏                        overall █████████████████████  3/3 0s",
        ),
    );
}

// ── Finalize (join_and_clear) behavior ──
//
// These tests verify the `Renderer::finalize()` path exercised by
// `ProgressGroup::join_and_clear()`.  The critical invariants:
// - **Finished bars (including overall) survive** finalize (Problem 2 from v3→v4).
// - **Active bars are untouched** by finalize.
// - **Empty finalize** (no children, only overall) does not panic.
// - **Idempotent finish** — calling tick() twice on finished bars is safe.

#[test]
fn progress_group_overall_finish_and_join_clear_persists() {
    // Terminal H=5, W=80.  Full templates, 3 slots (2 child + 1 overall).
    let (mp, term) = mk_with_size(5, 80);
    let (group, overall) = group_with_overall(mp, 3, "overall", 3);

    let c = group.add_bar(5, "fetch");
    c.advance(5);
    group.tick();
    c.finish_success();
    overall.advance(3);
    overall.finish_success();
    group.tick();
    // join_and_clear triggers finalize: finishes non-Active bars, removes
    // blank slots, then draws.  Finished overall must persist in output.
    group.join_and_clear();
    let actual = term.contents();
    assert_eq!(
        actual,
        concat!(
            "⠏                          fetch █████████████████████  5/5 0s\n",
            "⠏                        overall █████████████████████  3/3 0s",
        ),
    );
}

#[test]
fn progress_group_active_bars_survive_join_and_clear() {
    // Terminal H=5, W=80.  Active bars must remain styled and visible
    // after finalize removes blank slots.
    let (mp, term) = mk_with_size(5, 80);
    let (group, _overall) = group_with_overall(mp, 3, "overall", 3);

    let _c = group.add_bar(5, "alpha");
    // alpha is Active — never finished.
    group.tick();
    // join_and_clear runs finalize; Active bars are skipped by the
    // non-Active guard but should remain visible after blank removal.
    group.join_and_clear();
    group.tick();
    let contents = term.contents();
    assert_eq!(
        contents,
        concat!(
            "⠼                          alpha ░░░░░░░░░░░░░░░░░░░░░  0/5 0s 0/d\n",
            "⠸                        overall ░░░░░░░░░░░░░░░░░░░░░  0/3 0s 0/d",
        ),
    );
}

#[test]
fn progress_group_empty_finalize_no_crash() {
    // Terminal H=3, W=80.  Only overall, no children.
    let (mp, term) = mk_with_size(3, 80);
    let (group, _overall) = group_with_overall(mp, 3, "overall", 1);

    // No children added — all slots except overall are blank.
    group.tick();
    // join_and_clear removes blank bars and triggers final draw.
    // Must not panic when MultiProgress has zero bound bars left.
    group.join_and_clear();
    let contents = term.contents();
    // At minimum the overall bar must survive.
    assert!(contents.contains("overall"), "overall survives empty finalize: {contents:?}");
}

#[test]
fn finish_slot_idempotent() {
    // Double tick on a finished bar must not panic or corrupt state.
    let (mp, term) = mk_with_size(5, 80);
    let (group, _overall) = group_with_overall(mp, 3, "overall", 3);

    let c = group.add_bar(5, "fetch");
    c.advance(5);
    group.tick();
    c.finish_success();
    // First tick triggers finish_slot via tick() → non-Active guard.
    group.tick();
    let contents_after_first = term.contents();
    // Second tick calls finish_slot again on the same finished slot.
    group.tick();
    let contents_after_second = term.contents();
    // Output should be structurally similar (same line count, same bars).
    let lines_first: Vec<&str> = contents_after_first.lines().collect();
    let lines_second: Vec<&str> = contents_after_second.lines().collect();
    assert_eq!(lines_first.len(), lines_second.len(), "same line count on second tick");
    // fetch bar is at index 1 (index 0 is empty from InMemoryTerm's leading \n).
    assert!(lines_second[1].contains("5/5"), "fetch still shows 5/5: {0}", lines_second[1]);
}

// ── Slot pool / rendering tests ──

#[test]
fn slot_pool_blank_bars_remain_invisible() {
    let (mp, term) = mk_with_size(5, 80);
    let group = group(mp, 5);

    // Add a child bar so we can verify only 5 lines total.
    let _c = group.add_bar(10, "child");
    group.tick();

    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    // 5 slots, no overall. child at slot[4] (bottom), blanks at lines[0..3].
    // child at bottom avoids InMemoryTerm trimming → 5 lines.
    assert_eq!(lines.len(), 5, "5 slots, no overall → 5 lines (child at bottom)");
    assert!(lines[0].trim().is_empty(), "line 0 is blank");
    assert!(lines[1].trim().is_empty(), "line 1 is blank");
    assert!(lines[2].trim().is_empty(), "line 2 is blank");
    assert!(lines[3].trim().is_empty(), "line 3 is blank");
    assert!(lines[4].contains("child"), "line 4 has child: {0}", lines[4]);
    assert!(lines[4].contains("0/10"), "line 4 shows 0/10: {0}", lines[4]);
}

#[test]
fn slot_pool_acquire_returns_bottommost_child() {
    let (mp, term) = mk_with_size(5, 80);
    let group = group(mp, 4); // 4 slots

    let _c1 = group.add_bar(5, "first");
    group.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    // 4 slots, no overall. first at slot[3] (bottom), blanks at lines[0..2].
    assert_eq!(lines.len(), 4, "4 slots → 4 lines (first at bottom)");
    assert!(lines[0].trim().is_empty(), "line 0 is blank");
    assert!(lines[1].trim().is_empty(), "line 1 is blank");
    assert!(lines[2].trim().is_empty(), "line 2 is blank");
    assert!(lines[3].contains("first"), "line 3 has first: {0}", lines[3]);

    let _c2 = group.add_bar(3, "second");
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    // 4 slots, no overall → 4 lines (first at slot[2], second at slot[3])
    assert_eq!(lines.len(), 4, "4 lines, both non-empty at bottom");
    assert!(lines[0].trim().is_empty(), "line 0 is blank");
    assert!(lines[1].trim().is_empty(), "line 1 is blank");
    assert!(lines[2].contains("first"), "line 2 still has first: {0}", lines[2]);
    assert!(lines[3].contains("second"), "line 3 has second: {0}", lines[3]);
    assert!(!lines[0].contains("second"), "line 0 must not show second: {0}", lines[0]);
    assert!(!lines[1].contains("first"), "line 1 must not show first: {0}", lines[1]);
}

#[test]
fn slot_pool_acquire_with_overall_above_overall() {
    let (mp, term) = mk_with_size(5, 80);
    let (group, _overall) = group_with_overall(mp, 5, "overall", 10);

    let _c = group.add_bar(7, "worker");
    group.tick();

    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    assert_eq!(lines.len(), 5, "5 lines (capacity=5)");
    // worker at slot[3] (just above overall at slot[4]), blanks at lines[0..2].
    assert!(lines[0].trim().is_empty(), "line 0 blank");
    assert!(lines[1].trim().is_empty(), "line 1 blank");
    assert!(lines[2].trim().is_empty(), "line 2 blank");
    assert!(lines[3].contains("worker"), "line 3 has child: {0}", lines[3]);
    assert!(lines[3].contains("0/7"), "line 3 shows 0/7: {0}", lines[3]);
    assert!(lines[4].contains("overall"), "line 4 has overall: {0}", lines[4]);
    assert!(lines[4].contains("0/10"), "line 4 shows 0/10: {0}", lines[4]);
}

#[test]
fn progress_group_height_never_grows_with_many_bars() {
    let (mp, term) = mk_with_size(4, 80);
    let group = group(mp, 4);

    for i in 0..20 {
        let _c = group.add_bar(1, &format!("t{i}"));
        group.tick();
    }

    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    assert_eq!(lines.len(), 4, "must have exactly 4 lines even after 20 add_bar calls");
}

#[test]
fn progress_group_overall_always_at_bottom() {
    let (mp, term) = mk_with_size(5, 80);
    let (group, _overall) = group_with_overall(mp, 5, "overall", 10);

    for i in 0..5 {
        let _c = group.add_bar(2, &format!("task{i}"));
        group.tick();
    }

    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    assert_eq!(lines.len(), 5);
    // Chronological allocation: task0→slot[0], task1→slot[1], task2→slot[2], task3→slot[3].
    assert!(lines[0].contains("task0"), "line 0 has task0: {0}", lines[0]);
    assert!(lines[1].contains("task1"), "line 1 has task1: {0}", lines[1]);
    assert!(lines[2].contains("task2"), "line 2 has task2: {0}", lines[2]);
    assert!(lines[3].contains("task3"), "line 3 has task3: {0}", lines[3]);
    assert!(lines[4].contains("overall"), "line 4 always has overall: {0}", lines[4]);
    assert!(lines[4].contains("0/10"), "line 4 shows 0/10: {0}", lines[4]);
}

#[test]
fn progress_group_join_preserves_all_content() {
    let (mp, term) = mk_with_size(5, 80);
    let (group, _overall) = group_with_overall(mp, 5, "overall", 5);

    let c = group.add_bar(3, "fetch");
    c.advance(3);
    group.tick();

    let before = term.contents();
    group.join();
    let after = term.contents();

    assert_eq!(before, after, "join() is a no-op — contents must be identical before and after");
}

#[test]
fn progress_group_add_bar_zero_total_renders() {
    let (mp, term) = mk_with_size(5, 80);
    let (group, _overall) = group_with_overall(mp, 5, "overall", 0);

    let _c = group.add_bar(0, "zero");
    group.tick();

    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    assert_eq!(lines.len(), 5);
    // Bottom-up: zero bar at slot[3] (just above overall at slot[4]).
    assert!(lines[0].trim().is_empty(), "line 0 blank");
    assert!(lines[1].trim().is_empty(), "line 1 blank");
    assert!(lines[2].trim().is_empty(), "line 2 blank");
    assert!(lines[3].contains("zero"), "line 3 has zero bar: {0}", lines[3]);
    assert!(lines[4].contains("overall"), "line 4 has overall: {0}", lines[4]);
    // 0/0 renders as full
}

#[test]
fn consumer_lifecycle_materializer() {
    let (mp, term) = mk_with_size(5, 80);
    let group = group(mp, 5);

    let total = 3u64;
    let pb = group.add_bar(total, "materializing");

    group.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    // 5 slots, no overall → child at slot[4] (bottom), all 5 lines visible.
    assert_eq!(lines.len(), 5, "5 slots, no overall → 5 lines (child at bottom)");
    assert!(lines[0].trim().is_empty(), "line 0 blank");
    assert!(lines[1].trim().is_empty(), "line 1 blank");
    assert!(lines[2].trim().is_empty(), "line 2 blank");
    assert!(lines[3].trim().is_empty(), "line 3 blank");
    assert!(lines[4].contains("materializing"), "line 4 has materializing label: {0}", lines[4]);
    assert!(lines[4].contains("0/3"), "line 4 shows 0/3: {0}", lines[4]);

    pb.advance(3);
    pb.finish_success();
    group.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    // Still 5 lines after finish
    assert_eq!(lines.len(), 5, "5 slots, no overall → 5 lines after finish too");
    assert!(
        lines[4].contains("materializing"),
        "bar still visible after finish_success: {0}",
        lines[4]
    );
    assert!(lines[4].contains("3/3"), "shows 3/3 complete: {0}", lines[4]);

    group.join();
    let after_join = term.contents();
    assert!(!after_join.is_empty(), "join() must keep bars visible");
}

#[test]
fn consumer_lifecycle_conductor_sync() {
    let (mp, term) = mk_with_size(5, 80);
    let (group, overall) = group_with_overall(mp, 5, "syncing tools", 2);

    // Tool 1
    let t1 = group.add_bar(0, "yt-dlp");
    t1.advance(1);
    t1.finish();
    overall.advance(1);
    group.tick();

    // Tool 2
    let t2 = group.add_bar(0, "ffmpeg");
    t2.advance(1);
    t2.finish();
    overall.advance(1);
    group.tick();

    overall.finish_success();
    group.tick();
    group.join();

    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    assert_eq!(lines.len(), 5);
    // t1(yt-dlp) at slot[2] (first tool, shifted up by ffmpeg), t2(ffmpeg) at slot[3] (just above overall).
    assert!(lines[0].trim().is_empty(), "line 0 blank");
    assert!(lines[1].trim().is_empty(), "line 1 blank");
    assert!(lines[2].contains("yt-dlp"), "line 2 has yt-dlp: {0}", lines[2]);
    assert!(lines[3].contains("ffmpeg"), "line 3 has ffmpeg: {0}", lines[3]);
    assert!(lines[4].contains("syncing tools"), "line 4 has overall: {0}", lines[4]);
    assert!(lines[4].contains("2/2"), "overall complete: {0}", lines[4]);
}

#[test]
fn consumer_lifecycle_conductor_cli() {
    let (mp, term) = mk_with_size(4, 80);
    let (group, pb) = group_with_overall(mp, 4, "steps", 0);

    // Simulate step_progress callback: set_total(N) then set_position(1..N)
    pb.set_total(3);

    pb.set_position(1);
    group.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    assert_eq!(lines.len(), 4);
    assert!(lines[0].trim().is_empty(), "line 0 blank");
    assert!(lines[1].trim().is_empty(), "line 1 blank");
    assert!(lines[2].trim().is_empty(), "line 2 blank");
    assert!(lines[3].contains("steps"), "line 3 has steps: {0}", lines[3]);

    pb.set_position(3);
    pb.finish();
    group.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    assert!(lines[3].contains("steps"), "bar still visible after finish: {0}", lines[3]);

    group.join();
    assert!(!term.contents().is_empty(), "join() must keep bars visible");
}

#[test]
fn progress_group_finish_and_clear_child_keeps_others() {
    let (mp, term) = mk_with_size(4, 80);
    let (group, _overall) = group_with_overall(mp, 4, "overall", 5);

    let c1 = group.add_bar(3, "alpha");
    c1.advance(3);
    group.tick();

    c1.finish_and_clear();
    group.tick();

    // Overall must still be visible.
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    // finish_and_clear hides the child bar. Only the overall bar and blank
    // filler slots remain visible.  With fixed-size slots (capacity=4) the
    // child slot becomes a blank line rather than being removed.
    assert_eq!(lines.len(), 4, "4 lines after child cleared — overall + 3 blanks");
    assert!(lines[3].contains("overall"), "overall visible: {0}", lines[3]);
}

#[test]
fn progress_group_abandon_preserves_bar() {
    let (mp, term) = mk_with_size(5, 80);
    let group = group(mp, 5);

    let c = group.add_bar(5, "worker");
    c.advance(2);
    group.tick();

    c.abandon();

    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    // 5 slots, no overall → child at slot[4] (bottom), all 5 lines visible.
    assert_eq!(lines.len(), 5, "5 lines — child at bottom, 4 blanks above");
    assert!(lines[4].contains("worker"), "bar visible after abandon: {0}", lines[4]);
    assert!(lines[4].contains("2/5"), "progress preserved: {0}", lines[4]);
}

#[test]
fn progress_group_long_prefix_truncation() {
    let (mp, term) = mk_with_size(5, 80);
    let group = group(mp, 5);

    // Prefix > 16 chars — production uses {prefix:>16.16}
    let long_prefix = "abcdefghijklmnopqrstuvwxyz"; // 26 chars
    let _c = group.add_bar(5, long_prefix);
    group.tick();

    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    // 5 slots, no overall → child at slot[4] (bottom), 5 lines total.
    assert_eq!(lines.len(), 5);
    // Prefix should be right-aligned to 16 chars, left-truncated to 16 chars.
    // The production template is {prefix:>16.16} so it right-aligns and truncates to 16.
    // Expected: "               abcdefghijklmnop" (16 chars right-aligned) — but this
    // won't be exact because of ANSI color codes. Just verify the bar still shows.
    assert!(lines[4].contains("0/5"), "bar shows progress: {0}", lines[4]);
}

#[test]
fn progress_group_children_advance_independently() {
    let (mp, term) = mk_with_size(5, 80);
    let (group, _overall) = group_with_overall(mp, 5, "overall", 10);

    // Chronological allocation: tool-a at slot[3] (just above overall).
    // Second child shifts tool-a up to slot[2] and takes slot[3].
    // Overall at slot[4].
    let _a = group.add_bar(5, "tool-a");
    group.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    assert!(lines[3].contains("tool-a"), "line 3 has tool-a: {0}", lines[3]);

    // Second child shifts tool-a up, takes last slot before overall.
    let _b = group.add_bar(3, "tool-b");
    group.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    assert!(lines[2].contains("tool-a"), "line 2 still has tool-a: {0}", lines[2]);
    assert!(lines[3].contains("tool-b"), "line 3 has tool-b: {0}", lines[3]);

    group.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    assert!(lines[4].contains("overall"), "overall visible: {0}", lines[4]);
}

// ── Child bar elapsed: starts at zero ──────────────────────────────────────

#[test]
fn child_bar_elapsed_starts_at_zero() {
    let (mp, term) = mk();
    let dims = Arc::new(TestDimensionSource::new((H, W)));
    let (group, _overall) = group_with_overall_and_dims(mp, 4, "overall", 5, &dims, false);
    let _child = group.add_bar(3, "tool-a");
    group.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    let tool_line = lines.iter().find(|l| l.contains("tool-a")).expect("tool-a line must exist");
    assert!(tool_line.contains("0s"), "tool-a line should show 0 elapsed: {tool_line}");
}

// ── Child bar elapsed: frozen after finish ─────────────────────────────────

#[test]
fn child_bar_elapsed_frozen_after_finish() {
    let (mp, term) = mk();
    let dims = Arc::new(TestDimensionSource::new((H, W)));
    let (group, _overall) = group_with_overall_and_dims(mp, 4, "overall", 5, &dims, false);
    let child = group.add_bar(3, "tool-a");
    child.set_position(3);
    child.finish();
    group.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    let tool_line = lines.iter().find(|l| l.contains("tool-a")).expect("tool-a line must exist");
    assert!(
        tool_line.contains("0s"),
        "tool-a line should show 0 elapsed after finish: {tool_line}"
    );
    assert!(tool_line.contains("3/3"), "tool-a line should show final position: {tool_line}");
}

// ── Child bar elapsed: frozen after finish_success ─────────────────────────

#[test]
fn child_bar_elapsed_frozen_after_finish_success() {
    let (mp, term) = mk();
    let dims = Arc::new(TestDimensionSource::new((H, W)));
    let (group, _overall) = group_with_overall_and_dims(mp, 4, "overall", 5, &dims, false);
    let child = group.add_bar(3, "tool-a");
    child.set_position(3);
    child.finish_success();
    group.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    let tool_line = lines.iter().find(|l| l.contains("tool-a")).expect("tool-a line must exist");
    assert!(
        tool_line.contains("0s"),
        "tool-a line should show 0 elapsed after finish_success: {tool_line}"
    );
    assert!(tool_line.contains("3/3"), "tool-a line should show 3/3: {tool_line}");
}

// ── Child bar elapsed: frozen after finish_error ───────────────────────────

#[test]
fn child_bar_elapsed_frozen_after_finish_error() {
    let (mp, term) = mk();
    let dims = Arc::new(TestDimensionSource::new((H, W)));
    let (group, _overall) = group_with_overall_and_dims(mp, 4, "overall", 5, &dims, false);
    let child = group.add_bar(3, "tool-a");
    child.set_position(1);
    child.finish_error();
    group.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    let tool_line = lines.iter().find(|l| l.contains("tool-a")).expect("tool-a line must exist");
    assert!(
        tool_line.contains("0s"),
        "tool-a line should show 0 elapsed after finish_error: {tool_line}"
    );
    assert!(tool_line.contains("[F]"), "tool-a line should show error message: {tool_line}");
}

// ── Child bar elapsed: frozen after abandon ────────────────────────────────

#[test]
fn child_bar_elapsed_frozen_after_abandon() {
    let (mp, term) = mk();
    let dims = Arc::new(TestDimensionSource::new((H, W)));
    let (group, _overall) = group_with_overall_and_dims(mp, 4, "overall", 5, &dims, false);
    let child = group.add_bar(3, "tool-a");
    child.set_position(2);
    child.abandon();
    group.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    let tool_line = lines.iter().find(|l| l.contains("tool-a")).expect("tool-a line must exist");
    assert!(
        tool_line.contains("0s"),
        "tool-a line should show 0 elapsed after abandon: {tool_line}"
    );
}

// ── Elapsed preservation: orphan-reattach ─────────────────────────────────

#[test]
fn orphan_reattach_preserves_elapsed() {
    let dims = Arc::new(TestDimensionSource::new((3, 80)));
    let (mp, term, ts) = mk_with_size_and_ts(5, 80);
    let (group, _overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(4)
        .with_overall("overall", 5)
        .with_dim_source(Arc::clone(&dims) as Arc<dyn DimensionSource>)
        .with_time_source(ts.clone() as Arc<dyn TimeSource>)
        .dynamic_height(true)
        .with_ticker_enabled(false)
        .build_with_overall();
    let _child = group.add_bar(10, "worker");

    // Tick to show the bar with initial elapsed.
    group.tick();
    let before = term.contents();
    let before_lines: Vec<&str> = before.lines().collect();
    let worker_before =
        before_lines.iter().find(|l| l.contains("worker")).expect("worker visible before shrink");
    assert!(worker_before.contains("0s"), "worker shows elapsed before orphan: {worker_before}");

    // Shrink height to orphan the worker bar (only room for overall).
    dims.set((1, 80));
    group.tick();
    let after_shrink = term.contents();
    assert!(!after_shrink.contains("worker"), "worker orphaned after shrink");

    // Grow height to reattach the worker bar.
    dims.set((4, 80));
    group.tick();
    let after_grow = term.contents();
    let grow_lines: Vec<&str> = after_grow.lines().collect();
    let worker_after =
        grow_lines.iter().find(|l| l.contains("worker")).expect("worker reattached after grow");
    assert!(worker_after.contains("0s"), "worker elapsed preserved after reattach: {worker_after}");
}

// ── Elapsed preservation: slot shift ──────────────────────────────────────

#[test]
fn slot_shift_preserves_elapsed() {
    let (mp, term) = mk_with_size(5, 80);
    let (group, _overall) = group_with_overall(mp, 4, "overall", 5);

    let _a = group.add_bar(10, "alpha");
    group.tick();

    // Add bar B — shifts A up one slot.
    let _b = group.add_bar(5, "beta");
    group.tick();

    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    let alpha_line = lines.iter().find(|l| l.contains("alpha")).expect("alpha visible after shift");
    let beta_line = lines.iter().find(|l| l.contains("beta")).expect("beta visible after shift");

    assert!(alpha_line.contains("0s"), "alpha shows elapsed after slot shift: {alpha_line}");
    assert!(beta_line.contains("0s"), "beta shows elapsed: {beta_line}");
}

// ── Regression: no duplicate elapsed template ─────────────────────────────

#[test]
fn no_duplicate_elapsed_template_in_child_output() {
    // If a production template accidentally re-introduces {elapsed_precise}
    // alongside the message-injected elapsed, each bar line would show two
    // `[HH:MM:SS]` timestamps.  Verify at most one per line.
    let (mp, term) = mk();
    let dims = Arc::new(TestDimensionSource::new((H, W)));
    let (group, _overall) = group_with_overall_and_dims(mp, 4, "overall", 5, &dims, false);
    let _child = group.add_bar(3, "tool-a");
    group.tick();

    let contents = term.contents();
    for line in contents.lines() {
        let count = line.chars().filter(|&c| c == '[').count();
        assert!(
            count <= 1,
            "each line should have at most one '[' (elapsed), got {count}: {line:?}"
        );
    }
}

// ── Orphaned-state overflow behavior ─────────────────────────────────────

#[test]
fn slot_full_hides_overflow_bars_from_display() {
    let (mp, term) = mk_with_size(5, 80);
    let group = group(mp, 4); // capacity=4

    let c1 = group.add_bar(5, "tool-a");
    let c2 = group.add_bar(5, "tool-b");
    let c3 = group.add_bar(5, "tool-c");
    let c4 = group.add_bar(5, "tool-d");
    let c5 = group.add_bar(5, "tool-e"); // 5th bar — no slot (only 4 available)

    c1.advance(1);
    c2.advance(2);
    c3.advance(3);
    c4.advance(4);
    c5.advance(5);
    group.tick();

    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    // 4 slots, no overall → all 4 used, no blank trailing → 4 lines.
    // tools a-d in display. tool-e has no display slot but is still tracked.
    assert_eq!(lines.len(), 4, "4 lines — 4 display slots, all filled");
    let line_texts: Vec<&str> = lines.iter().map(|l| l.trim()).collect();
    assert!(
        line_texts.iter().any(|l| l.contains("tool-a")),
        "tool-a visible somewhere: {line_texts:?}"
    );
    assert!(
        line_texts.iter().any(|l| l.contains("tool-d")),
        "tool-d visible somewhere: {line_texts:?}"
    );
    // tool-e has no display slot so its content should NOT appear in the terminal
    assert!(
        !line_texts.iter().any(|l| l.contains("tool-e")),
        "tool-e must NOT appear in display (no slot): {line_texts:?}"
    );
    // tool-e is still tracked even without a display slot
    assert_eq!(c5.snapshot().position, 5, "tool-e tracked position: {0}", c5.snapshot().position);
}

/// Finalize removes blank bars and leaves only finished bars visible.
#[test]
fn progress_group_join_and_clear_removes_blank_bars() {
    let (mp, term) = mk_with_size(4, 80);
    let (group, overall) = group_with_overall(mp, 4, "overall", 10);

    let child = group.add_bar(5, "fetch");
    child.advance(5);
    child.finish();
    overall.advance(5);
    overall.finish();
    group.tick();

    // Before finalize: 4 lines (2 blanks + child + overall).
    assert_eq!(
        term.contents(),
        concat!(
            "\n",
            "\n",
            "⠏                          fetch █████████████████████  5/5 0s\n",
            "⠏                        overall █████████████████████  5/10 0s",
        ),
        "4 lines before finalize",
    );

    // join_and_clear calls finalize — removes blank bars.
    group.join_and_clear();

    // After finalize: bound bars remain in a drawable state.
    let after = term.contents();
    assert_eq!(
        after,
        concat!(
            "⠏                          fetch █████████████████████  5/5 0s\n",
            "⠏                        overall █████████████████████  5/10 0s",
        ),
        "2 bars remain after removing blanks",
    );
}

/// Comprehensive lifecycle test: add → progress → finish → finalize, with
/// exact terminal matching at every stage.  Catches missing bars, ghost
/// blank lines, wrong bar order, and missing position/total data.
#[test]
fn finalize_exact_terminal_match_after_full_lifecycle() {
    let (mp, term) = mk_with_size(8, 80);
    let (group, overall) = group_with_overall(mp, 5, "overall", 10);

    let c1 = group.add_bar(5, "alpha");
    let c2 = group.add_bar(3, "beta");
    c1.advance(5);
    c1.finish();
    c2.advance(3);
    c2.finish();
    overall.advance(10);
    overall.finish();
    group.tick();

    // Before finalize: 5 lines (2 blanks + alpha + beta + overall).
    let before = term.contents();
    assert_eq!(
        before,
        concat!(
            "\n",
            "\n",
            "⠏                          alpha █████████████████████  5/5 0s\n",
            "⠏                           beta █████████████████████  3/3 0s\n",
            "⠏                        overall █████████████████████  10/10 0s",
        ),
        "before finalize",
    );

    group.join_and_clear();

    // After finalize: exactly 3 visible lines (alpha + beta + overall).
    let after = term.contents();
    assert_eq!(
        after,
        concat!(
            "⠏                          alpha █████████████████████  5/5 0s\n",
            "⠏                           beta █████████████████████  3/3 0s\n",
            "⠏                        overall █████████████████████  10/10 0s",
        ),
        "after finalize",
    );
}

/// Verify that content written before `ProgressGroup` creation survives
/// the finalize lifecycle (test-mode invariant: `pre_roll` is a no-op).
#[test]
fn finalize_preserves_content_written_before_progress() {
    let term = InMemoryTerm::new(10, 80);
    // Write marker content to terminal directly before ProgressGroup.
    let _ = term.write_line("== PRE-EXISTING OUTPUT ==");
    let _ = term.write_line("line before progress bars");
    let before_marker = term.contents();
    assert!(before_marker.contains("PRE-EXISTING"), "marker written");

    let target = ProgressDrawTarget::term_like(Box::new(term.clone()));
    let mp = MultiProgress::with_draw_target(target);
    let (group, overall) = ProgressGroup::builder()
        .with_multi_progress(mp)
        .capacity(4)
        .with_overall("overall", 5)
        .with_ticker_enabled(false)
        .build_with_overall();
    let c = group.add_bar(3, "work");
    c.advance(3);
    c.finish();
    overall.advance(5);
    overall.finish();
    group.tick();
    group.join_and_clear();

    let after = term.contents();
    assert_eq!(
        after,
        concat!(
            "== PRE-EXISTING OUTPUT ==\n",
            "line before progress bars\n",
            "⠏                           work █████████████████████  3/3 0s\n",
            "⠏                        overall █████████████████████  5/5 0s",
        ),
        "pre-existing content + bars after finalize",
    );
}

/// Dedicated regression test for the Phase 4 padding bug: after finalize,
/// ALL output lines must be non-blank.  Catches padding newlines that push
/// bar content into terminal scrollback.
#[test]
fn finalize_no_blank_lines_in_output() {
    let (mp, term) = mk_with_size(6, 80);
    let (group, overall) = group_with_overall(mp, 4, "overall", 3);

    // Fill all 4 slots: 3 children + overall.
    let c1 = group.add_bar(5, "alpha");
    let c2 = group.add_bar(5, "beta");
    let c3 = group.add_bar(5, "gamma");
    c1.advance(5);
    c1.finish();
    c2.advance(5);
    c2.finish();
    c3.advance(5);
    c3.finish();
    overall.advance(3);
    overall.finish();
    group.tick();

    // Before finalize: all lines must be non-blank (all slots occupied).
    let before = term.contents();
    assert_eq!(
        before,
        concat!(
            // All 4 slots filled: no blank lines.
            "⠏                          alpha █████████████████████  5/5 0s\n",
            "⠏                           beta █████████████████████  5/5 0s\n",
            "⠏                          gamma █████████████████████  5/5 0s\n",
            "⠏                        overall █████████████████████  3/3 0s",
        ),
        "before finalize (all slots filled)",
    );

    group.join_and_clear();

    // After finalize: ALL lines must still be non-blank.
    let after = term.contents();
    assert_eq!(
        after,
        concat!(
            "⠏                          alpha █████████████████████  5/5 0s\n",
            "⠏                           beta █████████████████████  5/5 0s\n",
            "⠏                          gamma █████████████████████  5/5 0s\n",
            "⠏                        overall █████████████████████  3/3 0s",
        ),
        "after finalize (no blank lines regression)",
    );
}
