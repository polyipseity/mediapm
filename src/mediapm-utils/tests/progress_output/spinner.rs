//! Spinner and bar-style tests.
//!
//! Two bar styles are reachable through the public API: `BarStyle::StepCount`
//! (the default) and `BarStyle::WorkerSpinner`. The tests that this file
//! replaces asserted spinner behaviour against raw `indicatif` bars, which the
//! suite no longer constructs; the style contract is asserted here through
//! `add_bar_with_style` instead. Colour is not observable — `InMemoryTerm`
//! records text without ANSI escapes — so the assertions use the differences
//! that survive into the text: which glyph the spinner holds and whether the
//! bar field is drawn at all.

use mediapm_utils::progress::BarStyle;

use super::common::{line_with, mk_with_capacity};

/// The first non-blank character of a line: the spinner position.
fn first_glyph(line: &str) -> char {
    line.chars()
        .find(|ch| !ch.is_whitespace())
        .expect("a bound bar line always starts with a spinner")
}

/// The cell-drawing characters of the default `indicatif` tick sequence.
const BRAILLE_TICKS: [char; 10] = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];

/// A worker-spinner bar renders a filled bar while busy, an empty track while
/// idle, and a completed bar once finished.
#[test]
fn worker_spinner_style_renders_idle_and_active() {
    let (terminal, term) = mk_with_capacity(5, 80, 4);
    let (screen, _overall) = terminal.screen().with_overall("overall", 1).build();

    let _idle = screen.add_bar_with_style(0, "idle", BarStyle::WorkerSpinner);
    let busy = screen.add_bar_with_style(5, "busy", BarStyle::WorkerSpinner);
    busy.advance(5);
    screen.tick();

    let active = term.contents();
    assert!(line_with(&active, "busy").contains('█'), "a busy worker fills its bar: {active:?}");
    assert!(
        !line_with(&active, "idle").contains('█'),
        "a worker with no total leaves its bar empty: {active:?}"
    );
    assert!(line_with(&active, "idle").contains('░'), "but its track is still drawn: {active:?}");
    assert_eq!(
        &active,
        concat!(
            "\n",
            "\n",
            "⠹        idle ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/0 0s 0/d\n",
            "⠴        busy ███████████████████████████████████████████████████  5/5 0s 0/d\n",
            "⠹     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/1 0s 0/d"
        ),
        "worker_spinner_style_renders_idle_and_active/active"
    );

    busy.finish_success();
    screen.tick();
    let finished = term.contents();
    assert!(
        line_with(&finished, "busy").contains("5/5"),
        "the finished worker keeps its final count: {finished:?}"
    );
    assert_eq!(
        &finished,
        concat!(
            "\n",
            "\n",
            "⠸        idle ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/0 0s 0/d\n",
            "⠏        busy ███████████████████████████████████████████████████  5/5 0s\n",
            "⠸     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/1 0s 0/d"
        ),
        "worker_spinner_style_renders_idle_and_active/finished"
    );
}

/// The spinner only animates while its bar is active: consecutive draws advance
/// the glyph of an unfinished bar and leave both a successful and a failed bar
/// frozen — a finished bar must never look busy again.
#[test]
fn spinner_advances_only_while_a_bar_is_active() {
    let (terminal, term) = mk_with_capacity(5, 80, 4);
    let (screen, _overall) = terminal.screen().with_overall("overall", 3).build();

    let active = screen.add_bar(5, "active");
    let done = screen.add_bar(5, "done");
    let failed = screen.add_bar(5, "failed");
    active.advance(1);
    done.advance(5);
    done.finish_success();
    failed.advance(2);
    failed.finish_error();

    screen.tick();
    let first = term.contents();
    screen.tick();
    let second = term.contents();

    let active_first = first_glyph(line_with(&first, "active"));
    let active_second = first_glyph(line_with(&second, "active"));
    assert!(
        BRAILLE_TICKS.contains(&active_first) && BRAILLE_TICKS.contains(&active_second),
        "the spinner holds braille ticks: {active_first:?}, {active_second:?}"
    );
    assert_ne!(active_first, active_second, "an active bar's spinner must advance on every draw");
    for label in ["done", "failed"] {
        assert_eq!(
            first_glyph(line_with(&first, label)),
            first_glyph(line_with(&second, label)),
            "a {label} bar's spinner must not advance"
        );
    }
    assert_eq!(
        &first,
        concat!(
            "\n",
            "⠸                  active ███████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  1/5 0s 0/d\n",
            "⠏                    done ███████████████████████████████████████  5/5 0s\n",
            "⠏              [F] failed ███████████████░░░░░░░░░░░░░░░░░░░░░░░░  2/5 0s\n",
            "⠹                 overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/3 0s 0/d"
        ),
        "spinner_advances_only_while_a_bar_is_active/first"
    );
    assert_eq!(
        &second,
        concat!(
            "\n",
            "⠼                  active ███████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  1/5 0s 0/d\n",
            "⠏                    done ███████████████████████████████████████  5/5 0s\n",
            "⠏              [F] failed ███████████████░░░░░░░░░░░░░░░░░░░░░░░░  2/5 0s\n",
            "⠸                 overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/3 0s 0/d"
        ),
        "spinner_advances_only_while_a_bar_is_active/second"
    );
}

/// A zero-total bar is rendered full by the default style and empty by the
/// worker-spinner style — the zero/zero guard, and the only style difference
/// that survives into text without colour.
#[test]
fn zero_total_fill_depends_on_the_bar_style() {
    let (terminal, term) = mk_with_capacity(4, 40, 3);
    let (screen, _overall) = terminal.screen().with_overall("overall", 1).build();

    let _tracked = screen.add_bar(0, "step-zero");
    let _blank = screen.add_bar_with_style(0, "worker-zero", BarStyle::WorkerSpinner);
    screen.tick();

    let contents = term.contents();
    let step = line_with(&contents, "step-zero");
    let worker = line_with(&contents, "worker-zero");
    assert!(step.contains('█'), "the default style fills a zero-total bar: {step:?}");
    assert!(!step.contains('░'), "so no cell is left blank: {step:?}");
    assert!(worker.contains('░'), "the worker style leaves it blank: {worker:?}");
    assert!(!worker.contains('█'), "so no cell is filled: {worker:?}");
    assert_eq!(
        &contents,
        concat!(
            "\n",
            "⠹       step-zero ███████  0/0 0s 0/d\n",
            "⠼     worker-zero ░░░░░░░  0/0 0s 0/d\n",
            "⠹         overall ░░░░░░░  0/1 0s 0/d"
        ),
        "zero_total_fill_depends_on_the_bar_style"
    );
}
