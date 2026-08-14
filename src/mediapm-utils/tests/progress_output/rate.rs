use std::sync::Arc;

use super::common::*;

#[test]
fn rate_stable_on_stale_ticks() {
    let (mp, term) = mk_with_size(2, 80);
    let time_source = Arc::new(TestTimeSource::new());
    let (group, _overall) = group_with_overall_and_ts(mp, 2, "overall", 1, &time_source);
    let child = group.add_bar(1000, "test");
    // Advance a significant amount so the initial rate is clearly non-zero.
    child.set_position(500);
    group.tick();
    time_source.advance(std::time::Duration::from_millis(60));
    let after_progress = term.contents();
    assert_eq!(
        after_progress,
        concat!(
            "⠸                           test ██████████░░░░░░░░░░░  500/1.0k 0s 0/d\n",
            "⠹                        overall ░░░░░░░░░░░░░░░░░░░░░  0/1 0s 0/d",
        ),
    );

    // Now tick 20× with realistic ticker-interval delays.
    for _ in 0..20 {
        time_source.advance(std::time::Duration::from_millis(50));
        group.tick();
    }

    let after_stale = term.contents();
    assert_eq!(
        after_stale,
        concat!(
            "⠴                           test ██████████░░░░░░░░░░░  500/1.0k 0s 455/s 1s\n",
            "⠼                        overall ░░░░░░░░░░░░░░░░░░░░░  0/1 0s 0/d",
        ),
    );
}

/// Active bar: rate updates when more progress is made.
///
/// Advance → tick → capture rate, then advance more → tick → verify rate
/// has changed (not stuck at old value).
#[test]
fn rate_updates_on_progress() {
    let (mp, term) = mk_with_size(2, 80);
    let time_source = Arc::new(TestTimeSource::new());
    let (group, _overall) = group_with_overall_and_ts(mp, 2, "overall", 1, &time_source);
    let child = group.add_bar(2000, "test");
    child.set_position(10);
    time_source.advance(std::time::Duration::from_millis(2));
    group.tick();
    time_source.advance(std::time::Duration::from_millis(60));
    let after_small = term.contents();

    // Advance much more.
    child.set_position(1500);
    group.tick();
    time_source.advance(std::time::Duration::from_millis(60));
    let after_large = term.contents();

    assert_eq!(
        after_small,
        concat!(
            "⠸                           test ░░░░░░░░░░░░░░░░░░░░░  10/2.0k 0s 500/s 3s\n",
            "⠹                        overall ░░░░░░░░░░░░░░░░░░░░░  0/1 0s 0/d",
        ),
    );
    assert_eq!(
        after_large,
        concat!(
            "⠴                           test ███████████████░░░░░░  1.5k/2.0k 0s 2.9k/s 0s\n",
            "⠸                        overall ░░░░░░░░░░░░░░░░░░░░░  0/1 0s 0/d",
        ),
    );
    assert_ne!(after_small, after_large, "rate/progress must differ between 10 and 1500");
}

/// Active bar: rate is always shown even with zero progress.
#[test]
fn rate_always_shown() {
    let (mp, term) = mk_with_size(2, 80);
    let time_source = Arc::new(TestTimeSource::new());
    let (group, _overall) = group_with_overall_and_ts(mp, 2, "overall", 1, &time_source);
    let _child = group.add_bar(100, "idle");
    // No progress made — bar is still active.
    group.tick();
    time_source.advance(std::time::Duration::from_millis(60));
    assert_eq!(
        term.contents(),
        concat!(
            "⠹                           idle ░░░░░░░░░░░░░░░░░░░░░  0/100 0s 0/d\n",
            "⠹                        overall ░░░░░░░░░░░░░░░░░░░░░  0/1 0s 0/d",
        ),
    );
}

// ═════════════════════════════════════════════════════════════════════════════
// Phase 8: Rate exact-output tests
// ═════════════════════════════════════════════════════════════════════════════

/// Exact output: known rate after 500/1000 in 1s → 50/s.
#[test]
fn rate_exact_output_with_known_rate() {
    let (mp, term) = mk_with_size(2, 80);
    let ts = Arc::new(TestTimeSource::new());
    let (group, _overall) = group_with_overall_and_ts(mp, 2, "overall", 1, &ts);
    let child = group.add_bar(1000, "test");
    child.set_position(500);
    ts.advance(std::time::Duration::from_secs(1));
    group.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "⠸                           test ██████████░░░░░░░░░░░  500/1.0k 1s 50/s 10s\n",
            "⠹                        overall ░░░░░░░░░░░░░░░░░░░░░  0/1 1s 0/d",
        ),
    );
}

/// Exact output: idle bar with zero progress shows 0/d rate.
#[test]
fn rate_exact_output_idle() {
    let (mp, term) = mk_with_size(2, 80);
    let ts = Arc::new(TestTimeSource::new());
    let (group, _overall) = group_with_overall_and_ts(mp, 2, "overall", 1, &ts);
    let _child = group.add_bar(100, "idle");
    ts.advance(std::time::Duration::from_secs(2));
    group.tick();
    assert_eq!(
        term.contents(),
        concat!(
            "⠹                           idle ░░░░░░░░░░░░░░░░░░░░░  0/100 2s 0/d\n",
            "⠹                        overall ░░░░░░░░░░░░░░░░░░░░░  0/1 2s 0/d",
        ),
    );
}
