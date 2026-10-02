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
//! `[active] default s3 (ffmpeg)` is 28 columns of the 39 the screen gives its
//! labels at this width. The slot is what the terminal has left after the
//! spinner, the separators, the suffix and a fill floor, so at narrower widths
//! the tool name goes before the bar does.
//!
//! The data is synthetic, so the example runs offline and in about a
//! millisecond.
//!
//! ```text
//! cargo run --package mediapm --example mediapm_progress_workflow
//! cargo run --package mediapm --example mediapm_progress_workflow -- --width 41
//! ```
//!
//! 41 leaves room for the activity markers, the tally and a five-cell fill.
//! `workflow_screen_never_wraps_a_row` covers the widths below it, where the
//! label gives way one field at a time and the row still fits on one line.
//!
//! What reaches stdout is the rendered grid with its colour escapes removed, so
//! redirecting the command writes the transcript checked in under
//! `examples/fixtures/mediapm_progress_workflow/`.

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
    use super::support::{DEFAULT_HEIGHT, DEFAULT_WIDTH, parse_screen_config, strip_ansi_escapes};
    use super::workflow::render_workflow_screen;

    /// The exact grid at `DEFAULT_WIDTH`, the width the tool-sync screen is
    /// pinned at too.
    ///
    /// Read the rows from the top: two worker slots still running
    /// (`[active]`), one that ended in a retry (`[W]`), one that ended in a
    /// failure (`[F]`), one that succeeded and dropped its identifiers
    /// (`[idle]`), the per-step bar mid-run, and the overall bar at four of
    /// twelve steps. The overall bar carries `[W]` because a step was lost, so
    /// the coordinator finishes the run as a warning.
    ///
    /// The two running rows carry the whole identity: marker, workflow, step
    /// and tool. The per-step row carries a version and a tally as well, which
    /// is the widest label on the screen, so it sets the shared slot and the
    /// worker rows right-align against it.
    #[test]
    fn workflow_screen_at_default_width_matches_inline_grid() {
        let config = ScreenConfig { width: DEFAULT_WIDTH, height: DEFAULT_HEIGHT };
        let grid = strip_ansi_escapes(&render_workflow_screen(config));
        assert_eq!(
            grid,
            concat!(
                "⠙           [active] default s3 (ffmpeg) ░░░░░░░░░░░░░░░░░░░ 22s 0/d\n",
                "⠙           [active] default s4 (yt-dlp) ░░░░░░░░░░░░░░░░░░░ 19s 0/d\n",
                "⠏                      [W] [idle] (idle) ███████████████████ 4s\n",
                "⠏                      [F] [idle] (idle) ███████████████████ 5s\n",
                "⠏                          [idle] (idle) ███████████████████ 6s\n",
                "⠇     [wf] 1/3 [7.1] default s3 (ffmpeg) ██████░░░░░░░░░░░░░ 1/3 22s 0/d\n",
                "⠏                      [W] workflow [wf] ██████░░░░░░░░░░░░░  4/12 23s",
            )
        );
    }

    /// The exact grid at 41 columns, where the activity markers, the step
    /// tally and a five-cell fill still fit on one line.
    ///
    /// The identity fields give way in the order the label ranks them: the
    /// identifiers go first, then the tool name, and the marker stays last
    /// because it is the only field that says the slot is alive.
    #[test]
    fn workflow_screen_at_narrow_width_matches_inline_grid() {
        let config = ScreenConfig { width: 41, height: DEFAULT_HEIGHT };
        let grid = strip_ansi_escapes(&render_workflow_screen(config));
        assert_eq!(
            grid,
            concat!(
                "⠙       [active] ░░░░ 22s 0/d\n",
                "⠙       [active] ░░░░ 19s 0/d\n",
                "⠏     [W] [idle] ████ 4s\n",
                "⠏     [F] [idle] ████ 5s\n",
                "⠏     [idle] …e) ████ 6s\n",
                "⠇       [wf] 1/3 █░░░ 1/3 22s 0/d\n",
                "⠏              w █░░░  4/12 23s",
            )
        );
    }

    /// No width wraps a row, down to the narrowest the harness accepts.
    ///
    /// This is the failure the width budget exists to remove. The prefix slot
    /// used to be sized from the seed label and never shrank, so from 40
    /// columns down the captured grid grew an interleaved blank line per row:
    /// the row was wider than the terminal and the spill landed below it, where
    /// a suffix such as `0s 0/d` reads as a second bar. What gives way now is
    /// the label, and the fill keeps its four cells at every width measured,
    /// which is why this asserts the row count and the row width rather than
    /// the fill.
    #[test]
    fn workflow_screen_never_wraps_a_row() {
        for width in [38, 36, 34, 32, 30, 28, 26, 24, 22, 20, 18, 16, 14, 12, 10, 8] {
            let config = ScreenConfig { width, height: DEFAULT_HEIGHT };
            let grid = strip_ansi_escapes(&render_workflow_screen(config));
            let rows: Vec<&str> = grid.lines().collect();
            assert_eq!(rows.len(), 7, "width {width} must draw seven rows: {rows:?}");
            for row in rows {
                assert!(
                    row.chars().count() <= usize::from(width),
                    "width {width} must not wrap: {row:?}",
                );
            }
        }
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
