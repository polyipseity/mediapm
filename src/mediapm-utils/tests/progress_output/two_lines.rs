use super::common::*;

// ═════════════════════════════════════════════════════════════════════════════
// Phase 4: Lifecycle exact-output tests
// ═════════════════════════════════════════════════════════════════════════════

/// Exact output: abandoned child with overall at W=80.
#[test]
fn two_lines_exact_abandoned_child() {
    let (mp, term, ts) = mk_with_size_and_ts(2, 80);
    let (group, _overall) = group_with_overall_and_ts(mp, 2, "overall", 1, &ts);
    let child = group.add_bar(5, "child");
    child.set_position(3);
    ts.advance(std::time::Duration::from_secs(1));
    group.tick();
    child.abandon();
    ts.advance(std::time::Duration::from_secs(1));
    group.tick();

    assert_eq!(
        term.contents(),
        concat!(
            "⠏                      [A] child ████████████░░░░░░░░░  3/5 1s\n",
            "⠸                        overall ░░░░░░░░░░░░░░░░░░░░░  0/1 2s 0/d",
        )
    );
}

/// Exact output: error child with overall at W=80.
#[test]
fn two_lines_exact_error_child() {
    let (mp, term, ts) = mk_with_size_and_ts(2, 80);
    let (group, _overall) = group_with_overall_and_ts(mp, 2, "overall", 1, &ts);
    let child = group.add_bar(5, "child");
    child.set_position(3);
    ts.advance(std::time::Duration::from_secs(1));
    group.tick();
    child.finish_error();
    ts.advance(std::time::Duration::from_secs(1));
    group.tick();

    assert_eq!(
        term.contents(),
        concat!(
            "⠏                      [F] child ████████████░░░░░░░░░  3/5 1s\n",
            "⠸                        overall ░░░░░░░░░░░░░░░░░░░░░  0/1 2s 0/d",
        )
    );
}

/// Exact output: success child with overall at W=80.
#[test]
fn two_lines_exact_success_child() {
    let (mp, term, ts) = mk_with_size_and_ts(2, 80);
    let (group, _overall) = group_with_overall_and_ts(mp, 2, "overall", 1, &ts);
    let child = group.add_bar(5, "child");
    child.set_position(5);
    ts.advance(std::time::Duration::from_secs(1));
    group.tick();
    child.finish_success();
    ts.advance(std::time::Duration::from_secs(1));
    group.tick();

    assert_eq!(
        term.contents(),
        concat!(
            "⠏                          child █████████████████████  5/5 1s\n",
            "⠸                        overall ░░░░░░░░░░░░░░░░░░░░░  0/1 2s 0/d"
        )
    );
}

// ═════════════════════════════════════════════════════════════════════════════
// Phase 4: Two-line exact-output tests (missing)
// ═════════════════════════════════════════════════════════════════════════════

/// Exact output: both child and overall finish successfully.
#[test]
fn two_lines_exact_both_finished() {
    let (mp, term, ts) = mk_with_size_and_ts(2, 80);
    let (group, overall) = group_with_overall_and_ts(mp, 2, "overall", 3, &ts);
    let child = group.add_bar(5, "child");
    child.set_position(5);
    ts.advance(std::time::Duration::from_secs(1));
    group.tick();
    child.finish_success();
    overall.advance(3);
    overall.finish_success();
    ts.advance(std::time::Duration::from_secs(1));
    group.tick();

    assert_eq!(
        term.contents(),
        concat!(
            "⠏                          child █████████████████████  5/5 1s\n",
            "⠏                        overall █████████████████████  3/3 1s",
        )
    );
}

/// Exact output: child finishes successfully, overall abandons.
#[test]
fn two_lines_exact_overall_abandoned() {
    let (mp, term, ts) = mk_with_size_and_ts(2, 80);
    let (group, overall) = group_with_overall_and_ts(mp, 2, "overall", 3, &ts);
    let child = group.add_bar(5, "child");
    child.set_position(5);
    ts.advance(std::time::Duration::from_secs(1));
    group.tick();
    child.finish_success();
    overall.abandon();
    ts.advance(std::time::Duration::from_secs(1));
    group.tick();

    assert_eq!(
        term.contents(),
        concat!(
            "⠏                          child █████████████████████  5/5 1s\n",
            "⠏                    [A] overall ░░░░░░░░░░░░░░░░░░░░░  0/3 1s",
        )
    );
}

/// Exact output: child finishes successfully, overall errors.
#[test]
fn two_lines_exact_overall_error() {
    let (mp, term, ts) = mk_with_size_and_ts(2, 80);
    let (group, overall) = group_with_overall_and_ts(mp, 2, "overall", 3, &ts);
    let child = group.add_bar(5, "child");
    child.set_position(5);
    ts.advance(std::time::Duration::from_secs(1));
    group.tick();
    child.finish_success();
    overall.finish_error();
    ts.advance(std::time::Duration::from_secs(1));
    group.tick();

    assert_eq!(
        term.contents(),
        concat!(
            "⠏                          child █████████████████████  5/5 1s\n",
            "⠏                    [F] overall ░░░░░░░░░░░░░░░░░░░░░  0/3 1s",
        )
    );
}
