//! Renders the materialization progress screen (screen C) at a chosen width.
//!
//! The bars are the ones `sync_hierarchy` registers: the pinned
//! `materializing` overall bar, one bar per hierarchy entry seeded with
//! `{relative_path} [stg]`, and, inside a media folder entry, one sub-bar per
//! extracted variant seeded with `{variant_name} [wrt]`. A media entry walks
//! its bar through `[stg]`, `[vrf]`, and `[cmt]`; a folder or playlist entry
//! keeps the phase it was created with, because only the media arm installs a
//! later one. See the module doc in `support/materialization.rs`.
//!
//! What each tag stands for is tabulated under "Materialization phase tags" in
//! `.agents/instructions/progress-output.instructions.md`: `stg` copies CAS
//! content into the staging area, `vrf` checks the staged bytes, `cmt` writes
//! into the library, and `wrt` is one file inside a folder variant. The
//! overall bar carries no tag, since the word beside it already names the
//! phase.
//!
//! The shared prefix slot is the widest label the screen draws, held to what
//! the terminal has left after the spinner, the separators and the suffix, with
//! a fill floor under the bar while there is room for a bar at all. Every entry
//! label here carries a path, so the path shortens from the front as the
//! terminal narrows, and the phase tag trails, which makes it the first field
//! a narrow row gives up. Where that leaves the labels is a measured fact
//! about these seed paths and it moves when they move, so it is recorded in
//! `support/materialization.rs` rather than pinned here.
//!
//! `--scenario` picks which of three shapes to draw, and each one is captured
//! at every width a transcript names. `baseline` is the screen as it shipped;
//! `dense` lays out an entry of every kind the materializer dispatches, so the
//! band of bars is long; `states` draws a warned row, a failed row, sub-bars
//! under a folder still unpacking, and paths wide enough to clip. The terminal
//! height comes from the scenario's own bar count, so a capture needs no
//! `--height` and a scenario that gains a row cannot leave its transcripts
//! rendered at a height its content outgrew.
//!
//! The data is synthetic, so the example runs offline and in about a
//! millisecond.
//!
//! ```text
//! cargo run --package mediapm --example mediapm_progress_materialize
//! cargo run --package mediapm --example mediapm_progress_materialize -- --scenario dense
//!     --width 50
//! ```
//!
//! One `--` and no more: cargo takes everything after the first one, so a
//! second one reaches the example as an argument it does not read.
//!
//! What reaches stdout is the rendered grid with its colour escapes removed, so
//! redirecting the command writes the transcript checked in under
//! `examples/fixtures/mediapm_progress_materialize/`. Each transcript is named
//! for the scenario and width it was captured at, and
//! `materialization_screen_matches_every_scenario_transcript` reads both back
//! out of those names.

#[path = "support/mod.rs"]
mod support;

// The scenario axis is declared rather than folded into `support`, because that
// file compiles into all three example binaries and each of them builds under
// `deny(warnings)`: a harness item only this screen called would read as dead
// code in the other two.
#[path = "support/scenarios.rs"]
mod scenarios;

#[path = "support/materialization.rs"]
mod materialization;

use scenarios::{Scenario, ScenarioName};
use support::ScreenConfig;

/// Print the materialization screen in the scenario and at the size the
/// arguments select.
///
/// `--scenario` comes off first and the size flags are read after it, so each
/// half of the command line is reported by the parser that owns it: a mistyped
/// scenario names the scenarios that exist, a mistyped width names the range.
///
/// The height comes from the scenario's own bar count unless `--height` is
/// given, which is what makes a capture reproducible: the height a transcript
/// needs is a property of the scenario rather than a number somebody has to
/// remember per width.
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
    let scenario = materialization::scenario(name);
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
/// [`DEFAULT_HEIGHT`](support::DEFAULT_HEIGHT), which is the right answer for a
/// screen that draws a fixed band and the wrong one here: an absent height means
/// whatever the scenario needs, so a capture never has to name one.
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
    use super::materialization::{SCENARIOS, render_materialization_screen_for_folder, scenario};
    use super::scenarios::ScenarioName;
    use super::scenarios::{assert_every_scenario_transcript_matches, split_scenario_flag};
    use super::support::ScreenConfig;
    use super::support::{
        DEFAULT_HEIGHT, DEFAULT_WIDTH, assert_every_transcript_matches,
        assert_narrow_rows_carry_a_label_or_a_count, assert_no_row_wraps_over_sweep_widths,
        parse_screen_config, strip_ansi_escapes,
    };
    use super::{height_was_asked_for, with_scenario_height};

    /// Media folder entry as `demo_hierarchy_spec::online_demo_media_folder_relative()`
    /// spells it, quoted rather than computed so the frame below stays a
    /// transcript of a fixed name.
    const WIDE_FOLDER_ENTRY: &str =
        "music videos/Rick Astley - Never Gonna Give You Up [youtube.dQw4w9WgXcQ]";

    /// The materialization screen at every scenario and width a transcript
    /// names.
    ///
    /// Both come from the files in
    /// `examples/fixtures/mediapm_progress_materialize/`, so this test has no
    /// list of its own to fall out of date. A file that does not parse as a
    /// transcript name fails the test rather than going unread, and a new file
    /// is covered without touching this file. The scenario is read out of the
    /// name as well as the width, so a transcript captured in one scenario
    /// cannot pass against another.
    ///
    /// Every column is compared exactly except the spinner column, which is compared
    /// against the set of configured glyphs: which glyph a row shows at the end of a run
    /// follows the wall-clock spacing between its draws, so pinning it would record the
    /// machine that captured the transcript rather than the layout it shows.
    ///
    /// What each row of the baseline frame stands for is described on
    /// `render_materialization_screen`, and what each row of `states` stands for
    /// on `render_states`.
    #[test]
    fn materialization_screen_matches_every_scenario_transcript() {
        assert_every_scenario_transcript_matches(
            "mediapm_progress_materialize",
            "materialize",
            |name, width| scenario(name).render_at(width),
        );
    }

    /// The baseline transcripts keep the names they were captured with.
    ///
    /// Their content moved when the overall seed lost its tag, so they are
    /// regenerated from the new render rather than left matching an older one,
    /// but the filenames are the ones on disk before the scenario axis
    /// existed. The width-only matcher reads a name with no scenario segment as
    /// the baseline, so between the two matchers every file in the directory is
    /// claimed once: this one takes the names without a scenario segment, and
    /// the one above takes the rest.
    ///
    /// Every column is compared exactly except the spinner column, which is compared
    /// against the set of configured glyphs: which glyph a row shows at the end of a run
    /// follows the wall-clock spacing between its draws, so pinning it would record the
    /// machine that captured the transcript rather than the layout it shows.
    #[test]
    fn the_baseline_transcripts_still_match_the_width_only_matcher() {
        assert_every_transcript_matches(
            "mediapm_progress_materialize",
            "materialize",
            scenario(ScenarioName::Baseline).height(),
            |config| scenario(ScenarioName::Baseline).render(config),
        );
    }

    /// No width wraps a row, in any of the three scenarios.
    ///
    /// Under the old rule the prefix kept the width of the longest seed and the
    /// row outgrew the terminal, so from the middle of the range down the grid
    /// interleaved a blank line per row. What gives way now is the prefix, one
    /// ranked field at a time, and the `dense` and `states` scenarios carry the
    /// longest paths on this screen, so they are where that has the most work.
    ///
    /// It asserts nothing about the bar. Below the fill crossing the row drops
    /// it on purpose so the label can have the columns, so "carries a fill
    /// cell" is false by design there. The sweep below asks what survives the
    /// crossing: that a row still says something.
    #[test]
    fn materialization_screen_never_wraps_a_row() {
        for scenario in SCENARIOS {
            let height = scenario.height();
            let name = format!("materialization {}", scenario.name());
            assert_no_row_wraps_over_sweep_widths(&name, |config| {
                strip_ansi_escapes(&scenario.render(ScreenConfig { width: config.width, height }))
            });
        }
    }

    /// Once a width is readable on a scenario, every wider width is too.
    ///
    /// The sweep beside this one only asks that a row fits. This asks that it
    /// has something on it, which is the property a fit can be satisfied by
    /// breaking. Below the fill crossing the bar is dropped on purpose, so the
    /// row is carried by its label and its tally, and by its timing when it has
    /// neither; a scenario whose rows go empty again after a readable width is
    /// losing information it had.
    #[test]
    fn materialization_screen_rows_never_go_empty() {
        for scenario in SCENARIOS {
            let height = scenario.height();
            let name = format!("materialization {}", scenario.name());
            assert_narrow_rows_carry_a_label_or_a_count(&name, |config| {
                strip_ansi_escapes(&scenario.render(ScreenConfig { width: config.width, height }))
            });
        }
    }

    /// An absent `--height` means the scenario's own height, and an explicit
    /// one is the caller's to choose.
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

    /// A rejected scenario names the ones that exist, and does not fall back.
    ///
    /// The person who typed `--scenario wide` has to pick the right one, so the
    /// message lists all three rather than saying the value is unknown.
    /// Answering with a default screen instead would hand back a render nobody
    /// asked for and leave the mistake to be found in a transcript months
    /// later.
    #[test]
    fn a_rejected_scenario_names_the_scenarios_that_exist() {
        assert_eq!(
            reject(&["--scenario", "wide"]),
            "--scenario must be one of baseline, dense, states, got \"wide\""
        );
        assert_eq!(reject(&["--scenario"]), "--scenario needs a value");
    }

    /// The frame a folder entry wider than the client budget produces.
    ///
    /// The folder is the online demo's. Its `entry_name` is 59 columns against
    /// the slot the terminal leaves, and `entry_name` is elastic, so the name
    /// is cut back to a boundary and rows two and three keep
    /// `Up [youtube.dQw4w9WgXcQ]`, the part that names the video. A name kept
    /// whole is a name `fit_segments` drops once nothing can shrink, and those
    /// two rows would render as a bare phase tag with no identity at all.
    ///
    /// Row two shows what a cut costs when the directory behind the name can
    /// yield no tail at all: the path is dropped, so the name takes the
    /// overage in its place and keeps `You Up [youtube.dQw4w9WgXcQ]` rather
    /// than the whole 58 columns. Row three keeps `links`, the extracted
    /// member the sub-bar is writing.
    ///
    /// The other rows are unchanged from the wide frame the transcripts carry,
    /// because their own labels already fill the slot. The slot is what the
    /// widest label measures, capped at `MAX_PREFIX_WIDTH`, so a name wider
    /// than the slot shortens inside it and costs the rows that needed no
    /// shortening nothing. Which fields survive that is the label's ranking
    /// and not the budget's business.
    ///
    /// This one keeps a `concat!` literal instead of a transcript. It renders a
    /// different seed from the one every committed transcript was captured
    /// from, so a transcript of it would need its own directory, and the
    /// filenames in that directory would have to parse against a different
    /// stem. One screen, one directory is the shape the walk above relies on.
    #[test]
    fn materialization_screen_keeps_the_tail_of_a_folder_name_wider_than_the_budget() {
        let config = ScreenConfig { width: DEFAULT_WIDTH, height: DEFAULT_HEIGHT };
        let grid = strip_ansi_escapes(&render_materialization_screen_for_folder(
            config,
            WIDE_FOLDER_ENTRY,
        ));
        assert_eq!(
            grid,
            concat!(
                "⠏     01 - Telepathy.flac o Children [cmt] ██████████████████████████ 6s\n",
                "⠸     e You Up [youtube.dQw4w9WgXcQ] [wrt] ░░░░░░░░░░░░░░░░░░░░░░░░░░ 6s 0/d\n",
                "⠴     Up [youtube.dQw4w9WgXcQ] links [wrt] ████████░░░░░░░░░░░░░░░░░░ 4s 0/d\n",
                "⠴     01 - In the Flesh?.m4a he Wall [vrf] ██████████████████████████ 0s 0/d\n",
                "⠋                            materializing ████████░░░░░░░░░░░░░░░░░░ 13s 1/m"
            )
        );
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

    /// A rejected argument must name what was wrong, because the example is
    /// read by people who typed the command by hand.
    #[test]
    fn parse_screen_config_rejects_bad_arguments() {
        assert_eq!(reject(&["--width"]), "--width needs a value");
        assert_eq!(reject(&["--width", "wide"]), "--width needs a whole number, got \"wide\"");
        assert_eq!(reject(&["--width", "0"]), "--width must be between 8 and 500, got 0");
        assert_eq!(reject(&["--height", "1"]), "--height must be between 2 and 200, got 1");
        assert_eq!(
            reject(&["--rows", "3"]),
            "unexpected argument \"--rows\"; expected --width or --height"
        );
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
    /// `super::main()` literally, so it does. `main()` reads best-effort flags
    /// and falls back to the defaults when the argument list belongs to the
    /// test runner.
    #[test]
    fn main_is_exercised() {
        super::main();
    }
}
