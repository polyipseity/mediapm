//! Consumer-shaped lifecycle tests.
//!
//! These reproduce the call sequences the real consumers use — parallel worker
//! pools, sequential tool sync, per-entry materialization, and the CLI's step
//! progress callback — and assert the exact frame at the end. They are the
//! integration boundary of the suite: a change that keeps every unit invariant
//! intact but breaks a consumer's slot usage or finish sequence is caught here.

use std::sync::Arc;
use std::time::Duration;

use mediapm_utils::progress::TestTimeSource;

use super::common::{mk_with_capacity, mk_with_capacity_and_ts};

/// Two parallel workers, one of which fails: the failed bar keeps its own
/// progress under the `[F]` marker while the active one keeps advancing.
#[test]
fn parallel_workers_with_a_failed_worker() {
    let ts = Arc::new(TestTimeSource::new());
    let (terminal, term) = mk_with_capacity_and_ts(6, 80, 5, &ts);
    let (screen, _overall) = terminal.screen().with_overall("overall", 10).build();

    let a = screen.add_bar(5, "worker-a");
    let b = screen.add_bar(5, "worker-b");

    a.advance(3);
    b.advance(2);
    ts.advance(Duration::from_secs(1));
    screen.tick();

    a.finish_error();
    b.advance(1);
    ts.advance(Duration::from_secs(1));
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "\n",
            "\n",
            "\n",
            "⠏              [F] worker-a █████████████████████░░░░░░░░░░░░░░  3/5 1s\n",
            "⠧                  worker-b █████████████████████░░░░░░░░░░░░░░  3/5 2s 17/m 7s\n",
            "⠸                   overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/10 2s 0/d"
        ),
        "parallel_workers_with_a_failed_worker"
    );
}

/// Sequential tool sync: eight bars pass through four slots, so only the four
/// most recent survive in the frame.
#[test]
fn tool_sync_recycles_finished_slots() {
    let (terminal, term) = mk_with_capacity(5, 80, 4);
    let screen = terminal.screen().build();

    for i in 0..8 {
        let tool = screen.add_bar(1, &format!("tool{i}"));
        tool.advance(1);
        tool.finish_success();
        screen.tick();
    }
    assert_eq!(
        &term.contents(),
        concat!(
            "⠙     tool4 ████████████████████████████████████████████████████████████  1/1 0s\n",
            "⠙     tool5 ████████████████████████████████████████████████████████████  1/1 0s\n",
            "⠏     tool6 ████████████████████████████████████████████████████████████  1/1 0s\n",
            "⠏     tool7 ████████████████████████████████████████████████████████████  1/1 0s"
        ),
        "tool_sync_recycles_finished_slots"
    );
}

/// Finished bars are retained while later bars are added, up to the slot count.
#[test]
fn finished_bars_are_retained() {
    let (terminal, term) = mk_with_capacity(5, 80, 4);
    let screen = terminal.screen().build();

    for i in 0..4 {
        let task = screen.add_bar(1, &format!("task{i}"));
        task.advance(1);
        task.finish_success();
        screen.tick();
    }
    assert_eq!(
        &term.contents(),
        concat!(
            "⠙     task0 ████████████████████████████████████████████████████████████  1/1 0s\n",
            "⠙     task1 ████████████████████████████████████████████████████████████  1/1 0s\n",
            "⠏     task2 ████████████████████████████████████████████████████████████  1/1 0s\n",
            "⠏     task3 ████████████████████████████████████████████████████████████  1/1 0s"
        ),
        "finished_bars_are_retained"
    );
}

/// The materializer drives one bar through progress and finish with no overall
/// bar; the bar stays visible afterwards.
#[test]
fn materializer_consumer_lifecycle() {
    let (terminal, term) = mk_with_capacity(6, 80, 5);
    let screen = terminal.screen().build();

    let bar = screen.add_bar(3, "materializing");
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "\n",
            "\n",
            "\n",
            "\n",
            "⠸     materializing ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/3 0s 0/d"
        ),
        "materializer_consumer_lifecycle/running"
    );

    bar.advance(3);
    bar.finish_success();
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "\n",
            "\n",
            "\n",
            "\n",
            "⠏     materializing ████████████████████████████████████████████████████  3/3 0s"
        ),
        "materializer_consumer_lifecycle/finished"
    );

    screen.join();
    assert!(term.contents().contains("materializing"), "join must keep the bar visible");
}

/// Tool sync adds one bar per tool, finishing each in turn, with the overall bar
/// pinned below them; the final frame collapses to the bound bars only.
///
/// The overall bar is used as a layout anchor: its own progress cannot be driven
/// through the handle the builder returns (see
/// `lifecycle::overall_handle_progress_does_not_reach_the_renderer`).
#[test]
fn tool_sync_consumer_lifecycle() {
    let (terminal, term) = mk_with_capacity(6, 80, 5);
    let (screen, _overall) = terminal.screen().with_overall("syncing tools", 2).build();

    let t1 = screen.add_bar(0, "yt-dlp");
    t1.advance(1);
    t1.finish_success();
    screen.tick();

    let t2 = screen.add_bar(0, "ffmpeg");
    t2.advance(1);
    t2.finish_success();
    screen.tick();

    screen.join();

    let contents = term.contents();
    for expected in ["yt-dlp", "ffmpeg", "syncing tools", "0/2"] {
        assert!(
            contents.contains(expected),
            "joined frame must contain {expected:?}: {contents:?}"
        );
    }
    assert_eq!(
        contents,
        concat!(
            "⠏            yt-dlp █████████████████████████████████████████████  1/0 0s\n",
            "⠏            ffmpeg █████████████████████████████████████████████  1/0 0s\n",
            "⠏     syncing tools █████████████████████████████████████████████  0/2 0s 0/d"
        ),
        "tool_sync_consumer_lifecycle"
    );
}

/// A consumer that adds one child at a time and finishes it immediately keeps
/// every finished bar in the frame.
#[test]
fn sequential_consumer_keeps_finished_bars() {
    let (terminal, term) = mk_with_capacity(6, 80, 5);
    let (screen, overall) = terminal.screen().with_overall("overall", 3).build();

    let c1 = screen.add_bar(5, "fetch");
    c1.advance(5);
    screen.tick();
    c1.finish_success();

    let c2 = screen.add_bar(2, "parse");
    c2.advance(2);
    screen.tick();
    c2.finish_success();

    overall.advance(3);
    overall.finish_success();
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "\n",
            "\n",
            "\n",
            "⠏       fetch ███████████████████████████████████████████████████  5/5 0s\n",
            "⠏       parse ██████████████████████████████████████████████████████████  2/2 0s\n",
            "⠏     overall ██████████████████████████████████████████████████████████  3/3 0s"
        ),
        "sequential_consumer_keeps_finished_bars"
    );
}

/// Three workers ending in different states (success, two failures) plus a
/// finished overall bar render their distinct markers and values.
#[test]
fn workers_with_mixed_finish_states() {
    let (terminal, term) = mk_with_capacity(5, 80, 4);
    let (screen, overall) = terminal.screen().with_overall("overall", 3).build();

    let c1 = screen.add_bar(2, "worker-a");
    let c2 = screen.add_bar(2, "worker-b");
    let c3 = screen.add_bar(2, "worker-c");
    c1.advance(2);
    c1.finish_success();
    c2.set_position(1);
    c2.finish_error();
    c3.set_position(1);
    c3.finish_error();
    overall.advance(3);
    overall.finish_success();
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "\n",
            "⠏                  worker-a ████████████████████████████████████████████  2/2 0s\n",
            "⠏              [F] worker-b ██████████████████████░░░░░░░░░░░░░░░░░░░░░░  1/2 0s\n",
            "⠏              [F] worker-c ██████████████████████░░░░░░░░░░░░░░░░░░░░░░  1/2 0s\n",
            "⠏                   overall ████████████████████████████████████████████  3/3 0s"
        ),
        "workers_with_mixed_finish_states"
    );
}

/// Two workers with different rates: each keeps its own rate and only the
/// slower one grows an ETA.
#[test]
fn workers_with_interleaved_advances() {
    let ts = Arc::new(TestTimeSource::new());
    let (terminal, term) = mk_with_capacity_and_ts(5, 80, 4, &ts);
    let (screen, overall) = terminal.screen().with_overall("overall", 10).build();

    let c1 = screen.add_bar(10, "fast");
    let c2 = screen.add_bar(10, "slow");
    c1.set_position(8);
    c2.set_position(3);
    overall.advance(11);
    ts.advance(Duration::from_secs(1));
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "\n",
            "\n",
            "⠸        fast ███████████████████████████████████████░░░░░░░░░░  8/10 1s 48/m 2s\n",
            "⠴        slow ██████████████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  3/10 1s 18/m\n",
            "⠸     overall █████████████████████████████████████████████████  11/10 1s 1.1/s"
        ),
        "workers_with_interleaved_advances"
    );
}

/// Three workers fill the three child slots of a four-row grid above the pinned
/// overall bar, each keeping its own position: the slot capacity counts child
/// slots, so the overall's line is not one of them.
#[test]
fn worker_children_fill_the_small_grid() {
    let ts = Arc::new(TestTimeSource::new());
    let (terminal, term) = mk_with_capacity_and_ts(4, 80, 3, &ts);
    let (screen, _overall) = terminal.screen().with_overall("overall", 1).build();

    let c1 = screen.add_bar(5, "child-a");
    let c2 = screen.add_bar(5, "child-b");
    let c3 = screen.add_bar(5, "child-c");
    c1.set_position(2);
    c2.set_position(4);
    c3.set_position(5);
    ts.advance(Duration::from_millis(1));
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "⠸     child-a ████████████████████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  2/5 0s 0/d\n",
            "⠸     child-b ████████████████████████████████████████░░░░░░░░░░░  4/5 0s 0/d\n",
            "⠦     child-c ███████████████████████████████████████████████████  5/5 0s 0/d\n",
            "⠹     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/1 0s 0/d"
        ),
        "worker_children_fill_the_small_grid"
    );
}

/// Capacity counts child slots: at capacity two with an overall bar, two
/// children plus the overall fill the three reserved lines, and a third child is
/// left unbound rather than displacing one of them.
#[test]
fn child_capacity_excludes_the_overall_slot() {
    let (terminal, term) = mk_with_capacity(3, 40, 2);
    let (screen, _overall) = terminal.screen().with_overall("overall", 1).build();
    let _c1 = screen.add_bar(3, "alpha");
    let _c2 = screen.add_bar(3, "beta");
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "⠹       alpha ░░░░░░░░░░░  0/3 0s 0/d\n",
            "⠼        beta ░░░░░░░░░░░  0/3 0s 0/d\n",
            "⠹     overall ░░░░░░░░░░░  0/1 0s 0/d"
        ),
        "child_capacity_excludes_the_overall_slot/two_children"
    );

    let (terminal, term) = mk_with_capacity(3, 40, 2);
    let (screen, _overall) = terminal.screen().with_overall("overall", 1).build();
    let _c1 = screen.add_bar(3, "alpha");
    let _c2 = screen.add_bar(3, "beta");
    let _c3 = screen.add_bar(3, "gamma");
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "⠹       alpha ░░░░░░░░░░░  0/3 0s 0/d\n",
            "⠼        beta ░░░░░░░░░░░  0/3 0s 0/d\n",
            "⠹     overall ░░░░░░░░░░░  0/1 0s 0/d"
        ),
        "child_capacity_excludes_the_overall_slot/three_children"
    );
}

/// Rapid absolute position writes collapse to the last value at the next tick.
#[test]
fn repeated_position_setting_yields_last_value() {
    let ts = Arc::new(TestTimeSource::new());
    let (terminal, term) = mk_with_capacity_and_ts(6, 80, 5, &ts);
    let (screen, _overall) = terminal.screen().with_overall("overall", 100).build();

    let worker = screen.add_bar(50, "worker");
    for i in 0..20 {
        worker.set_position(i * 2);
    }
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "\n",
            "\n",
            "\n",
            "\n",
            "⠼      worker ██████████████████████████████████░░░░░░░░░░░░  38/50 0s 0/d\n",
            "⠹     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/100 0s 0/d"
        ),
        "repeated_position_setting_yields_last_value"
    );
}

/// The conductor CLI's step-progress callback pattern: the overall bar is
/// created with an unknown total, its total is set once the step count is
/// known, and each step then writes an absolute position.
///
/// Restored from the pre-rebuild suite (`progress_group.rs:723`, inventory
/// `consumer_lifecycle_conductor_cli`), which could not be carried over while
/// the overall handle was disconnected from the renderer. The original asserted
/// only that the overall's line was present and non-empty after `join`; the
/// frames below pin the driven values instead, so a disconnected handle fails
/// the test rather than passing it.
#[test]
fn conductor_cli_step_progress_lifecycle() {
    let (terminal, term) = mk_with_capacity(5, 80, 4);
    let (screen, overall) = terminal.screen().with_overall("steps", 0).build();

    overall.set_total(3);
    overall.set_position(1);
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "\n",
            "\n",
            "\n",
            "\n",
            "⠹     steps █████████████████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  1/3 0s 0/d"
        ),
        "conductor_cli_step_progress_lifecycle/first_step"
    );

    overall.set_position(3);
    overall.finish_success();
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "\n",
            "\n",
            "\n",
            "\n",
            "⠏     steps ████████████████████████████████████████████████████████████  3/3 0s"
        ),
        "conductor_cli_step_progress_lifecycle/last_step"
    );

    screen.join();
    assert_eq!(
        &term.contents(),
        concat!("⠏     steps ████████████████████████████████████████████████████████████  3/3 0s"),
        "conductor_cli_step_progress_lifecycle/after_join"
    );
}
