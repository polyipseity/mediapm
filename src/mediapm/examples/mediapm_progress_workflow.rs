//! Renders the workflow progress screen (screen B) at a chosen width.
//!
//! The bars are the ones the conductor coordinator registers during
//! `run_workflow`: a fixed grid of worker-slot bars that are created once per
//! pool member and reused for every dispatch, plus the pinned `workflow [wf]`
//! overall bar. A worker slot shows `(idle) [idle]` when nothing is running on
//! it, the workflow, step and tool names plus `[active]` when a step is, and
//! the `[W]` or `[F]` marker when a step ended in a warning or a failure. The
//! per-step bar above the overall bar carries a version and a progress tally.
//!
//! A worker row carries the workflow, the step and the tool it is running:
//! `[active] default s3 (ffmpeg)` is 28 columns. The slot is what the terminal
//! has left after the spinner, the separators, the suffix and a fill floor, so
//! at narrower widths the tool name goes before the bar does. Where the label
//! gives way is a measured fact about these seed labels and it moves when they
//! move, so it is recorded in `support/workflow.rs` rather than pinned here.
//!
//! The data is synthetic, so the example runs offline and in about a
//! millisecond.
//!
//! ```text
//! cargo run --package mediapm --example mediapm_progress_workflow
//! cargo run --package mediapm --example mediapm_progress_workflow -- --width 40
//! ```
//!
//! What reaches stdout is the rendered grid with its colour escapes removed, so
//! redirecting the command writes the transcript checked in under
//! `examples/fixtures/mediapm_progress_workflow/`. Each transcript is named for
//! the width it was captured at, and
//! `workflow_screen_matches_every_committed_transcript` reads the widths out of
//! those names, so covering another width is adding a file and nothing else.

#[path = "support/mod.rs"]
mod support;

#[path = "support/workflow.rs"]
mod workflow;

use support::ScreenConfig;

/// Print the workflow screen at the size the arguments select.
///
/// The flags are read strictly, so a human who types `--width wide` is told
/// which flag is wrong. Under a test runner the argument list belongs to the
/// runner, not to this example, so a rejected argument is reported and the
/// screen still renders at [`DEFAULT_WIDTH`] by [`DEFAULT_HEIGHT`]. That keeps
/// `super::main()` callable from a test, which
/// `example-execution-policy.instructions.md` requires.
fn main() {
    let config = match support::parse_screen_config(std::env::args().skip(1)) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("{error}");
            ScreenConfig { width: support::DEFAULT_WIDTH, height: support::DEFAULT_HEIGHT }
        }
    };
    let grid = workflow::render_workflow_screen(config);
    println!("{}", support::strip_ansi_escapes(&grid));
}

#[cfg(test)]
mod tests {
    use super::support::ScreenConfig;
    use super::support::{
        DEFAULT_HEIGHT, DEFAULT_WIDTH, assert_every_transcript_matches,
        assert_no_row_wraps_over_sweep_widths, parse_screen_config, strip_ansi_escapes,
    };
    use super::workflow::render_workflow_screen;

    /// The workflow screen at every width a transcript is checked in at.
    ///
    /// The widths come from the filenames in
    /// `examples/fixtures/mediapm_progress_workflow/`, so this test has no
    /// list of its own to fall out of date: a fixture that does not parse as a
    /// transcript name fails the test rather than going unread, and a new
    /// fixture is covered without touching this file.
    ///
    /// Reading one frame at the default width told us what the screen draws.
    /// It could not tell us where the label starts giving way, because a
    /// single frame has one width in it and nothing to compare against.
    #[test]
    fn workflow_screen_matches_every_committed_transcript() {
        assert_every_transcript_matches(
            "mediapm_progress_workflow",
            "workflow",
            DEFAULT_HEIGHT,
            |config| strip_ansi_escapes(&render_workflow_screen(config)),
        );
    }

    /// No width wraps a row, and no row loses its bar.
    ///
    /// This is the failure the width budget exists to remove. The prefix slot
    /// used to be sized from the seed label and never shrank, so from 40
    /// columns down the captured grid grew an interleaved blank line per row:
    /// the row was wider than the terminal and the spill landed below it,
    /// where a suffix such as `0s 0/d` reads as a second bar. What gives way
    /// now is the label. A row could also stop wrapping by going empty, so
    /// each one has to carry a fill character as well.
    #[test]
    fn workflow_screen_never_wraps_a_row() {
        assert_no_row_wraps_over_sweep_widths("workflow", |config| {
            strip_ansi_escapes(&render_workflow_screen(config))
        });
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
        match parse_screen_config(owned) {
            Ok(config) => panic!("{args:?} should have been rejected, got {config:?}"),
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
