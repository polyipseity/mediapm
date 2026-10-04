//! Renders the workflow progress screen (screen B) at a chosen width.
//!
//! The bars are the ones the conductor coordinator registers during
//! `run_workflow`: a fixed grid of worker-slot bars that are created once per
//! pool member and reused for every dispatch, plus the pinned `workflow`
//! overall bar. A worker slot reads `[idle]` when nothing is running on it,
//! the workflow, step and tool names plus `[active]` when a step is, and
//! the `[W]` or `[F]` marker when a step ended in a warning or a failure. The
//! per-step bar above the overall bar carries a version and a progress tally.
//!
//! A worker row carries the workflow, the step and the tool it is running:
//! `default s3 (ffmpeg) [active]` is 28 columns. A step row carries a version as
//! well, and parts of a label give their columns up from opposite ends: a worker
//! row shortens its tool name from the front and keeps the tail, while a step row
//! shortens its version from the end and keeps the head. The slot is what the
//! terminal has left after the spinner, the separators and the suffix, so at
//! narrower widths those are what give way, then the bar itself, and a row whose
//! label no longer fits keeps its timing rather than going blank. Where the labels
//! give way is a measured fact about these seed labels and it moves when they
//! move, so it is recorded in `support/workflow.rs` rather than pinned here.
//!
//! The per-screen spec is the "Screen B: Workflow" section of
//! `.agents/instructions/progress-output.instructions.md`, which tabulates
//! what each worker-slot state fills into the label.
//!
//! The data is synthetic, so the example runs offline and in about a
//! millisecond.
//!
//! `--scenario` picks which of three shapes to draw, and each one is captured at
//! every width a transcript names. `baseline` is the screen as it shipped and
//! exists to show that nothing else moved; `dense` runs a whole worker pool at
//! once so the band of slots is long; `states` draws tool names and a version
//! wider than any committed transcript. The terminal height comes from the
//! scenario's own bar count, so no `--height` is needed to capture one and adding
//! a row to a scenario cannot leave its transcripts rendered at a height its
//! content outgrew.
//!
//! ```text
//! cargo run --package mediapm --example mediapm_progress_workflow
//! cargo run --package mediapm --example mediapm_progress_workflow -- --scenario dense --width 50
//! ```
//!
//! One `--` and no more: cargo takes everything after the first one, so a second
//! one reaches the example as an argument it does not read.
//!
//! What reaches stdout is the rendered grid with its colour escapes removed, so
//! redirecting the command writes the transcript checked in under
//! `examples/fixtures/mediapm_progress_workflow/`. Each transcript is named for
//! the scenario and width it was captured at, and
//! `workflow_screen_matches_every_committed_transcript` reads both back out of
//! those names.

#[path = "support/mod.rs"]
mod support;

// The scenario axis is declared rather than folded into `support`, because that
// file compiles into all three example binaries and each of them builds under
// `deny(warnings)`: a harness item only this screen called would read as dead
// code in the other two.
#[path = "support/scenarios.rs"]
mod scenarios;

#[path = "support/workflow.rs"]
mod workflow;

use scenarios::{Scenario, ScenarioName};
use support::ScreenConfig;

/// Print the workflow screen in the scenario and at the size the arguments
/// select.
///
/// `--scenario` comes off first and the size flags are read after it, so each
/// half of the command line is reported by the parser that owns it: a mistyped
/// scenario names the scenarios that exist, a mistyped width names the range.
///
/// The height comes from the scenario's own bar count unless `--height` is
/// given, which is what makes a capture reproducible without a number written
/// beside each scenario.
///
/// Under a test runner the argument list belongs to the runner, not to this
/// example, so a rejected argument is reported and the baseline scenario is
/// drawn at [`DEFAULT_WIDTH`] by [`DEFAULT_HEIGHT`] anyway. That keeps
/// `super::main()` callable from a test, which
/// `example-execution-policy.instructions.md` requires.
fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let explicit_height = height_was_asked_for(&args);
    let (name, size_args) = match scenarios::split_scenario_flag(args) {
        Ok(parsed) => parsed,
        Err(error) => {
            eprintln!("{error}");
            (ScenarioName::Baseline, Vec::new())
        }
    };
    let scenario = workflow::scenario(name);
    let config = match support::parse_screen_config(size_args) {
        Ok(config) => with_scenario_height(config, explicit_height, &scenario),
        Err(error) => {
            eprintln!("{error}");
            ScreenConfig { width: support::DEFAULT_WIDTH, height: support::DEFAULT_HEIGHT }
        }
    };
    // Without an explicit height this is the same call a transcript is captured
    // through, so a capture and the test that compares it cannot disagree about
    // which frame was meant.
    let grid =
        if explicit_height { scenario.render(config) } else { scenario.render_at(config.width) };
    println!("{}", support::strip_ansi_escapes(&grid));
}

/// The size to draw at, with the scenario's own height filling in for an absent
/// `--height`.
///
/// The size parser reports an absent height as
/// [`DEFAULT_HEIGHT`](support::DEFAULT_HEIGHT), which would pin every scenario to
/// the same capacity and take the derivation away from them.
fn with_scenario_height(
    config: ScreenConfig,
    explicit_height: bool,
    scenario: &Scenario,
) -> ScreenConfig {
    if explicit_height {
        return config;
    }
    ScreenConfig { height: scenario.height(), ..config }
}

/// Whether the command line asked for a height of its own.
///
/// Kept apart from the size parser because that one cannot tell an absent
/// `--height` from an explicit one: it has a single default to report either way,
/// and on this screen the two answers differ.
fn height_was_asked_for(args: &[String]) -> bool {
    args.windows(2).any(|pair| pair[0] == "--height")
}

#[cfg(test)]
mod tests {
    use super::scenarios::ScenarioName;
    use super::scenarios::assert_every_scenario_transcript_matches;
    use super::scenarios::split_scenario_flag;
    use super::support::ScreenConfig;
    use super::support::{
        DEFAULT_HEIGHT, DEFAULT_WIDTH, assert_every_transcript_matches,
        assert_narrow_rows_carry_a_label_or_a_count, assert_no_row_wraps_over_sweep_widths,
        parse_screen_config, strip_ansi_escapes,
    };
    use super::workflow::{SCENARIOS, scenario};
    use super::{height_was_asked_for, main, with_scenario_height};

    /// The workflow screen at every scenario and width a transcript names.
    ///
    /// Both come from the files in
    /// `examples/fixtures/mediapm_progress_workflow/`, so this test has no list
    /// of its own to fall out of date. A file that does not parse as a
    /// transcript name fails the test rather than going unread, and a new file is
    /// covered without touching this file. The scenario is read out of the name as
    /// well as the width, so a transcript captured in one scenario cannot pass
    /// against another.
    ///
    /// Every column is compared exactly except the spinner column, which is compared
    /// against the set of configured glyphs: which glyph a row shows at the end of a run
    /// follows the wall-clock spacing between its draws, so pinning it would record the
    /// machine that captured the transcript rather than the layout it shows.
    #[test]
    fn workflow_screen_matches_every_committed_transcript() {
        assert_every_scenario_transcript_matches(
            "mediapm_progress_workflow",
            "workflow",
            |name, width| scenario(name).render_at(width),
        );
    }

    /// The baseline transcripts keep the names they were captured with.
    ///
    /// They were written before the axis existed, and the width-only matcher reads
    /// a name with no scenario segment as the baseline, so they stay where they
    /// are. Between the two matchers every file in the directory is claimed once:
    /// this one takes the names without a scenario segment, and the one above
    /// takes the rest.
    ///
    /// Every column is compared exactly except the spinner column, which is compared
    /// against the set of configured glyphs: which glyph a row shows at the end of a run
    /// follows the wall-clock spacing between its draws, so pinning it would record the
    /// machine that captured the transcript rather than the layout it shows.
    #[test]
    fn the_baseline_transcripts_still_match_the_width_only_matcher() {
        assert_every_transcript_matches(
            "mediapm_progress_workflow",
            "workflow",
            scenario(ScenarioName::Baseline).height(),
            |config| scenario(ScenarioName::Baseline).render(config),
        );
    }

    /// Each scenario declares the number of bars its renderer draws, and the
    /// terminal height follows from that count rather than from a number written
    /// beside the scenario.
    ///
    /// [`Scenario::render_at`](super::scenarios::Scenario::render_at) checks the
    /// claim from both sides when a transcript is compared, so a count that is
    /// too large shows up as a short grid and a count that is too small shows up
    /// as a frame that moves with the height. This asserts the counts directly,
    /// because a scenario with no transcript checked at all would otherwise never
    /// be asked.
    #[test]
    fn every_scenario_declares_the_bars_it_draws() {
        for scenario in SCENARIOS {
            let drawn = strip_ansi_escapes(
                &scenario.render(ScreenConfig { width: DEFAULT_WIDTH, height: scenario.height() }),
            );
            assert_eq!(
                drawn.lines().count(),
                usize::from(scenario.bars()),
                "the {} scenario declares {} bars and draws {} lines",
                scenario.name(),
                scenario.bars(),
                drawn.lines().count()
            );
            assert_eq!(scenario.height(), scenario.bars() + 1);
        }
    }

    /// No width wraps a row, in any of the three scenarios.
    ///
    /// The prefix is the field that gives way, and the two labels on this screen
    /// give it up in opposite directions: a worker row shortens its tool name from
    /// the front, and a step row shortens its version from the end. The `states`
    /// scenario is where that has the most work, because its names and versions
    /// are wider than any committed transcript.
    ///
    /// It asserts nothing about the bar. Below the fill crossing the row drops it
    /// on purpose so the label can have the columns, so "carries a fill cell" is
    /// false by design there. The sweep below asks what survives the crossing: that
    /// a row still says something.
    #[test]
    fn workflow_screen_never_wraps_a_row() {
        for scenario in SCENARIOS {
            let height = scenario.height();
            let name = format!("workflow {}", scenario.name());
            assert_no_row_wraps_over_sweep_widths(&name, |config| {
                strip_ansi_escapes(&scenario.render(ScreenConfig { width: config.width, height }))
            });
        }
    }

    /// Once a width is readable on a scenario, every wider width is too.
    ///
    /// The sweep beside this one only asks that a row fits. This asks that it has
    /// something on it, which is the property a fit can be satisfied by breaking.
    /// Below the fill crossing the bar is dropped on purpose, so the row is
    /// carried by its label and its tally, and by its timing when it has neither;
    /// a scenario whose rows go empty again after a readable width is losing
    /// information it had.
    #[test]
    fn workflow_screen_rows_never_go_empty() {
        for scenario in SCENARIOS {
            let height = scenario.height();
            let name = format!("workflow {}", scenario.name());
            assert_narrow_rows_carry_a_label_or_a_count(&name, |config| {
                strip_ansi_escapes(&scenario.render(ScreenConfig { width: config.width, height }))
            });
        }
    }

    /// An absent `--height` means the scenario's own height, and an explicit one is
    /// the caller's to choose.
    ///
    /// The default the size parser reports is the suite's, which would pin every
    /// scenario to the same capacity and take the derivation away from them.
    #[test]
    fn only_an_explicit_height_overrides_the_derived_one() {
        let dense = scenario(ScenarioName::Dense);
        let without: Vec<String> =
            ["--scenario", "dense", "--width", "50"].map(str::to_string).into();
        let parsed = parse_screen_config(
            split_scenario_flag(without.clone()).unwrap_or_else(|e| panic!("{e}")).1,
        )
        .unwrap_or_else(|error| panic!("{error}"));
        let sized = with_scenario_height(parsed, height_was_asked_for(&without), &dense);
        assert_eq!(sized.height, dense.height());
        assert_eq!(sized.width, 50);

        let with: Vec<String> = ["--height", "40"].map(str::to_string).into();
        let parsed = parse_screen_config(
            split_scenario_flag(with.clone()).unwrap_or_else(|e| panic!("{e}")).1,
        )
        .unwrap_or_else(|error| panic!("{error}"));
        let sized = with_scenario_height(parsed, height_was_asked_for(&with), &dense);
        assert_eq!(sized.height, 40);
    }

    /// The flags that pick the size have to reach the terminal, and an absent
    /// flag has to fall back to the suite default rather than to zero.
    #[test]
    fn parse_screen_config_reads_width_and_height_flags() {
        assert_eq!(
            parse_screen_config(Vec::new()),
            Ok(ScreenConfig { width: DEFAULT_WIDTH, height: DEFAULT_HEIGHT })
        );
        assert_eq!(
            parse_screen_config(["--width".to_string(), "30".to_string()]),
            Ok(ScreenConfig { width: 30, height: DEFAULT_HEIGHT })
        );
        assert_eq!(
            parse_screen_config([
                "--height".to_string(),
                "12".to_string(),
                "--width".to_string(),
                "80".to_string()
            ]),
            Ok(ScreenConfig { width: 80, height: 12 })
        );
    }

    /// A rejected argument must name what was wrong, because the example is read
    /// by people who typed the command by hand.
    #[test]
    fn a_rejected_argument_names_what_was_wrong() {
        assert_eq!(reject(&["--width"]), "--width needs a value");
        assert_eq!(reject(&["--width", "wide"]), "--width needs a whole number, got \"wide\"");
        assert_eq!(reject(&["--width", "0"]), "--width must be between 8 and 500, got 0");
        assert_eq!(reject(&["--height", "1"]), "--height must be between 2 and 200, got 1");
        assert_eq!(
            reject(&["--rows", "3"]),
            "unexpected argument \"--rows\"; expected --width or --height"
        );
    }

    /// A rejected scenario names the ones that exist, and does not fall back.
    ///
    /// The person who typed `--scenario wide` has to pick the right one, so the
    /// message lists all three rather than saying the value is unknown. Answering
    /// with a default screen instead would hand back a render nobody asked for and
    /// leave the mistake to be found in a transcript months later.
    #[test]
    fn a_rejected_scenario_names_the_scenarios_that_exist() {
        assert_eq!(
            reject(&["--scenario", "wide"]),
            "--scenario must be one of baseline, dense, states, got \"wide\""
        );
        assert_eq!(reject(&["--scenario"]), "--scenario needs a value");
    }

    /// The rendered message for an argument list the harness refuses.
    fn reject(args: &[&str]) -> String {
        let owned: Vec<String> = args.iter().map(|arg| (*arg).to_string()).collect();
        match split_scenario_flag(owned) {
            Ok(parsed) => match parse_screen_config(parsed.1) {
                Ok(config) => panic!("{args:?} should have been rejected, got {config:?}"),
                Err(error) => error.to_string(),
            },
            Err(error) => error.to_string(),
        }
    }

    /// `example-execution-policy.instructions.md` requires this test to call
    /// `super::main()` literally, so it does. `main()` reads best-effort flags and
    /// falls back to the defaults when the argument list belongs to the test
    /// runner.
    #[test]
    fn main_is_exercised() {
        main();
    }
}
