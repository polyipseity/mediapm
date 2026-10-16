//! Elapsed-field tests.
//!
//! The elapsed value is the last field of a bar's suffix. The tests it replaces
//! only asserted `contains("0s")` while the injected clock stayed at zero, so
//! they could not tell a working elapsed field from one that was hardcoded to
//! `0s` or frozen after the first draw. Every test here drives the clock to a
//! nonzero value and asserts the *field* (the last whitespace-separated token of
//! the bar's line) rather than a substring of the whole frame.

use std::sync::Arc;
use std::time::Duration;

use mediapm_utils::progress::{TestDimensionSource, TestTimeSource};

use super::common::{H, W, line_with, mk_with_capacity_and_ts, mk_with_dims, without_spinners};

/// Return the elapsed field of the line containing `label`.
///
/// The suffix renders as `count/total elapsed [rate] [eta] [custom]`, so the
/// elapsed is the *first* duration-shaped token (leading digit, trailing `s`)
/// rather than the last: the eta shares that shape and follows the rate. Taking
/// the first keeps this helper naming the field it documents — the count/total
/// token ends in a digit, and the rate ends in `/d`, `/m`, `/h` or `/s`, all of
/// which come after the elapsed in the rendered order.
fn elapsed_field(contents: &str, label: &str) -> String {
    line_with(contents, label)
        .split_whitespace()
        .find(|token| {
            token.ends_with('s') && token.chars().next().is_some_and(|ch| ch.is_ascii_digit())
        })
        .unwrap_or_else(|| panic!("no elapsed field on the {label:?} line: {contents:?}"))
        .to_string()
}

/// The elapsed field starts at zero and moves once the clock does.
#[test]
fn elapsed_starts_at_zero_and_advances() {
    let ts = Arc::new(TestTimeSource::new());
    let (terminal, term) = mk_with_capacity_and_ts(5, 80, 4, &ts);
    let (screen, _overall) = terminal.screen().with_overall("overall", 5).build();

    let child = screen.add_bar(3, "tool-a");
    child.set_animating(true);
    _overall.set_animating(true);
    child.advance(1);
    screen.tick();
    let at_zero = term.contents();

    ts.advance(Duration::from_secs(5));
    screen.tick();
    let after_five = term.contents();

    let first = elapsed_field(&at_zero, "tool-a");
    let second = elapsed_field(&after_five, "tool-a");
    assert_ne!(first, second, "elapsed must move with the clock: {first:?} vs {second:?}");
    assert_eq!(first, "0s", "elapsed starts at zero");
    assert_eq!(second, "5s", "elapsed reports the injected clock");
    assert_eq!(
        &at_zero,
        concat!(
            "\n",
            "\n",
            "\n",
            "⠋      tool-a █████████████████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  1/3 0s 0/d 2s\n",
            "⠋     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/5 0s 0/d 5s",
        ),
        "elapsed_starts_at_zero_and_advances/zero"
    );
    assert_eq!(
        after_five,
        concat!(
            "\n",
            "\n",
            "\n",
            "⠙      tool-a ████████████████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  1/3 5s 1/m 1m40s\n",
            "⠙     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/5 5s 0/d 5s",
        ),
        "elapsed_starts_at_zero_and_advances/five"
    );
}

/// A tick that observes no clock advance redraws an identical frame, and the
/// elapsed field resumes moving on the next advanced clock.
#[test]
fn elapsed_is_stable_on_a_stale_tick() {
    let ts = Arc::new(TestTimeSource::new());
    let (terminal, term) = mk_with_capacity_and_ts(5, 80, 4, &ts);
    let (screen, _overall) = terminal.screen().with_overall("overall", 5).build();

    let _child = screen.add_bar(3, "tool-a");
    _child.set_animating(true);
    _overall.set_animating(true);
    ts.advance(Duration::from_secs(3));
    screen.tick();
    let first = term.contents();
    assert_eq!(elapsed_field(&first, "tool-a"), "3s", "precondition: a nonzero elapsed");
    assert_eq!(
        first,
        concat!(
            "\n",
            "\n",
            "\n",
            "⠋      tool-a ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/3 3s 0/d 3s\n",
            "⠋     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/5 3s 0/d 5s",
        ),
        "elapsed_is_stable_on_a_stale_tick/first"
    );

    screen.tick();
    let second = term.contents();
    assert_eq!(
        elapsed_field(&second, "tool-a"),
        elapsed_field(&first, "tool-a"),
        "a tick with no clock movement must not change the elapsed field"
    );
    assert_eq!(
        elapsed_field(&second, "overall"),
        elapsed_field(&first, "overall"),
        "nor the overall bar's elapsed field"
    );
    assert_eq!(
        without_spinners(&second),
        without_spinners(&first),
        "the spinner is the only thing a stale tick may change"
    );

    ts.advance(Duration::from_secs(1));
    screen.tick();
    let third = term.contents();
    assert_eq!(elapsed_field(&third, "tool-a"), "4s", "elapsed resumes after the clock advances");
    assert_eq!(
        third,
        concat!(
            "\n",
            "\n",
            "\n",
            "⠹      tool-a ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/3 4s 0/d 3s\n",
            "⠹     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/5 4s 0/d 5s",
        ),
        "elapsed_is_stable_on_a_stale_tick/third"
    );
}

/// A finished bar's elapsed is frozen at its finish time, and this is a real
/// freeze rather than an artifact: a sibling bar on the same screen keeps
/// reporting the advancing clock.
#[test]
fn elapsed_freezes_when_a_bar_finishes() {
    let ts = Arc::new(TestTimeSource::new());
    let (terminal, term) = mk_with_capacity_and_ts(5, 80, 4, &ts);
    let (screen, _overall) = terminal.screen().with_overall("overall", 5).build();

    let finishing = screen.add_bar(3, "finishing");
    let _running = screen.add_bar(3, "running");
    finishing.set_animating(true);
    _running.set_animating(true);
    _overall.set_animating(true);

    ts.advance(Duration::from_secs(4));
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "\n",
            "\n",
            "⠋     finishing ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/3 4s 0/d 3s\n",
            "⠋       running ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/3 4s 0/d 3s\n",
            "⠋       overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/5 4s 0/d 5s",
        ),
        "elapsed_freezes_when_a_bar_finishes/before"
    );

    finishing.finish_error();
    ts.advance(Duration::from_secs(10));
    screen.tick();
    let contents = term.contents();

    assert_eq!(
        elapsed_field(&contents, "finishing"),
        "4s",
        "the finished bar keeps the elapsed it had at finish time"
    );
    assert_eq!(
        elapsed_field(&contents, "running"),
        "14s",
        "the sibling proves the clock really advanced to 14s"
    );
    assert!(contents.contains("[F]"), "the finished bar is marked failed: {contents:?}");
    assert_eq!(
        contents,
        concat!(
            "\n",
            "\n",
            "·              [F] finishing ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/3 4s\n",
            "⠙                    running ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/3 14s 0/d 3s\n",
            "⠙                    overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/5 14s 0/d 5s",
        ),
        "elapsed_freezes_when_a_bar_finishes/after"
    );
}

/// An elapsed value survives being orphaned (the grid shrinks until the bar
/// cannot be bound) and being reattached when the grid grows back: a reset
/// timer would restart at zero on reattach.
#[test]
fn elapsed_survives_orphan_reattach() {
    let dims = Arc::new(TestDimensionSource::new((5, 80)));
    let ts = Arc::new(TestTimeSource::new());
    let (terminal, term) = mk_with_dims(5, 80, 4, &dims, Some(&ts), true);
    let (screen, _overall) = terminal.screen().with_overall("overall", 5).build();

    let _worker = screen.add_bar(10, "worker");
    _worker.set_animating(true);
    _overall.set_animating(true);
    ts.advance(Duration::from_secs(3));
    screen.tick();
    let before = term.contents();
    assert_eq!(elapsed_field(&before, "worker"), "3s", "precondition: elapsed is running");
    assert_eq!(
        before,
        concat!(
            "\n",
            "\n",
            "\n",
            "⠋      worker ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/10 3s 0/d 10s\n",
            "⠋     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/5 3s 0/d 5s",
        ),
        "elapsed_survives_orphan_reattach/before"
    );

    dims.set((1, 80));
    ts.advance(Duration::from_secs(2));
    screen.tick();
    let orphaned = term.contents();
    assert!(!orphaned.contains("worker"), "the shrunken grid orphans the worker: {orphaned:?}");
    assert_eq!(
        orphaned,
        // Re-baselined with the plan-then-write swap: when the worker
        // orphans, the frame re-settles its widths from the rows that are
        // actually drawn (spec AC6 — a retained frame never carries an older
        // frame's geometry). The legacy capture froze the previous settle's
        // 16-wide suffix, leaving a 49-cell fill beside a 14-wide row; the
        // current settle reserves exactly what the remaining overall row
        // needs (14) and gives the columns back to the fill (51), keeping
        // the line at the full 80 columns.
        concat!(
            "⠙     overall ",
            "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░",
            "  0/5 5s 0/d 5s"
        ),
        "elapsed_survives_orphan_reattach/orphaned"
    );

    dims.set((5, 80));
    ts.advance(Duration::from_secs(2));
    screen.tick();
    let after = term.contents();
    let reattached = elapsed_field(&after, "worker");
    assert_ne!(reattached, "0s", "reattaching must not restart the elapsed timer");
    assert_eq!(
        after,
        // Re-baselined with the plan-then-write swap: a reattached bar with
        // no EMA yet draws its eta at the nominal floor — the Task 1 contract
        // `a_bar_before_the_ema_starts_reserves_the_eta_at_the_nominal_floor`
        // (`10s` at the 1.0/s floor for 10 remaining). The legacy capture
        // showed no eta here because indicatif hides an unknown one; the plan
        // renders the reserved value the budget already reserves columns for.
        concat!(
            "\n",
            "\n",
            "\n",
            "⠹      worker ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/10 7s 0/d 10s\n",
            "⠹     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/5 7s 0/d 5s",
        ),
        "elapsed_survives_orphan_reattach/after"
    );
}

/// An elapsed value survives a slot shift: adding a bar pushes its neighbours up
/// a line without restarting their timers.
#[test]
fn elapsed_survives_a_slot_shift() {
    let ts = Arc::new(TestTimeSource::new());
    let (terminal, term) = mk_with_capacity_and_ts(5, 80, 4, &ts);
    let (screen, _overall) = terminal.screen().with_overall("overall", 5).build();

    let _alpha = screen.add_bar(10, "alpha");
    _alpha.set_animating(true);
    _overall.set_animating(true);
    ts.advance(Duration::from_secs(2));
    screen.tick();

    let _beta = screen.add_bar(5, "beta");
    _beta.set_animating(true);
    ts.advance(Duration::from_secs(2));
    screen.tick();
    let contents = term.contents();

    assert_eq!(elapsed_field(&contents, "alpha"), "4s", "alpha's timer is not reset by the shift");
    assert_eq!(elapsed_field(&contents, "beta"), "2s", "beta starts its own timer");
    assert_eq!(
        contents,
        concat!(
            "\n",
            "\n",
            "⠙       alpha ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/10 4s 0/d 10s\n",
            "⠙        beta ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/5 2s 0/d 5s\n",
            "⠙     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/5 4s 0/d 5s",
        ),
        "elapsed_survives_a_slot_shift"
    );
}

/// Elapsed is independent of progress: an idle bar's elapsed keeps advancing
/// while its position stays put.
#[test]
fn elapsed_is_independent_of_progress() {
    let ts = Arc::new(TestTimeSource::new());
    let (terminal, term) = mk_with_capacity_and_ts(H, W, 4, &ts);
    let (screen, _overall) = terminal.screen().with_overall("overall", 1).build();
    let _child = screen.add_bar(3, "idle");
    _child.set_animating(true);
    _overall.set_animating(true);

    ts.advance(Duration::from_secs(6));
    screen.tick();
    let contents = term.contents();

    assert_eq!(elapsed_field(&contents, "idle"), "6s", "elapsed tracks the clock");
    assert!(contents.contains("0/3"), "progress is untouched: {contents:?}");
    assert_eq!(
        contents,
        concat!(
            "\n",
            "\n",
            "\n",
            "⠋        idle ░░░░░░░░░░░  0/3 6s 0/d 3s\n",
            "⠋     overall ░░░░░░░░░░░  0/1 6s 0/d 1s",
        ),
        "elapsed_is_independent_of_progress"
    );
}
