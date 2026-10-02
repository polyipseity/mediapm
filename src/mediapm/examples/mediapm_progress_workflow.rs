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
//! The rows are narrower than the identity they carry. Every bar is seeded with
//! `idle [wf]`, the string `coordinator.rs:343` installs, and the renderer sizes
//! the prefix slot from the seed rather than from the drawn label, so tool names
//! arrive truncated. See the module doc in `support/workflow.rs`.
//!
//! The data is synthetic, so the example runs offline and in about a
//! millisecond.
//!
//! ```text
//! cargo run --package mediapm --example mediapm_progress_workflow
//! cargo run --package mediapm --example mediapm_progress_workflow -- --width 41
//! ```
//!
//! 41 is the narrowest width that still gives the bar a column, so it pins the
//! boundary from the working side. One column narrower the fill is gone
//! entirely, and three narrower still the transcript comes back with an
//! interleaved blank line per row. `workflow_screen_below_the_fill_boundary`
//! describes that regime rather than pinning it as a transcript.
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
    /// Every row is short of the identity the label carries. The prefix slot is
    /// thirteen columns because the widest seed is `workflow [wf]`, so the
    /// workflow, step and tool names are cut. That is the shape a live run
    /// draws, and a layout change that widens the slot has to change it here.
    #[test]
    fn workflow_screen_at_default_width_matches_inline_grid() {
        let config = ScreenConfig { width: DEFAULT_WIDTH, height: DEFAULT_HEIGHT };
        let grid = strip_ansi_escapes(&render_workflow_screen(config));
        assert_eq!(
            grid,
            concat!(
                "⠙          [active] ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░ 22s 0/d\n",
                "⠙          [active] ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░ 19s 0/d\n",
                "⠏     [W] [idle] …) ████████████████████████████████████████ 4s\n",
                "⠏     [F] [idle] …) ████████████████████████████████████████ 5s\n",
                "⠏     [idle] (idle) ████████████████████████████████████████ 6s\n",
                "⠇          [wf] 1/3 █████████████░░░░░░░░░░░░░░░░░░░░░░░░░░░ 1/3 22s 0/d\n",
                "⠏              work █████████████░░░░░░░░░░░░░░░░░░░░░░░░░░░  4/12 23s",
            )
        );
    }

    /// The exact grid at 41 columns, the narrowest width that still leaves the
    /// bar a column.
    ///
    /// Together with the 80-column case this pins the working side of the
    /// boundary. The failing side is described by
    /// `workflow_screen_below_the_fill_boundary`.
    #[test]
    fn workflow_screen_at_narrow_width_matches_inline_grid() {
        let config = ScreenConfig { width: 41, height: DEFAULT_HEIGHT };
        let grid = strip_ansi_escapes(&render_workflow_screen(config));
        assert_eq!(
            grid,
            concat!(
                "⠙          [active] ░ 22s 0/d\n",
                "⠙          [active] ░ 19s 0/d\n",
                "⠏     [W] [idle] …) █ 4s\n",
                "⠏     [F] [idle] …) █ 5s\n",
                "⠏     [idle] (idle) █ 6s\n",
                "⠇          [wf] 1/3 ░ 1/3 22s 0/d\n",
                "⠏              work ░  4/12 23s",
            )
        );
    }

    /// What happens below the fill boundary, described rather than pinned.
    ///
    /// The prefix slot does not shrink with the terminal, so the fill is the
    /// first thing to go. At 40 columns every bar draws at zero width and the
    /// seven rows come back with no fill between them. From 37 down to 20 the
    /// captured grid grows an interleaved blank line per row, because a row
    /// whose content exceeds the terminal wraps and the spill lands on the line
    /// below. No test asserts those exact bytes: a fixture made of blank lines
    /// would read like a transcript when it is the renderer reporting that it
    /// ran out of room, so the regime lives in this comment instead. The
    /// tool-sync screen breaks the same way below 54 columns.
    #[test]
    fn workflow_screen_below_the_fill_boundary() {
        for width in [40, 39, 37] {
            let config = ScreenConfig { width, height: DEFAULT_HEIGHT };
            let grid = strip_ansi_escapes(&render_workflow_screen(config));
            let rows: Vec<&str> = grid.lines().collect();
            let fill = rows.iter().filter(|row| row.contains(['\u{2588}', '\u{2591}'])).count();
            assert_eq!(fill, 0, "width {width} should draw no fill, got {rows:?}");
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
