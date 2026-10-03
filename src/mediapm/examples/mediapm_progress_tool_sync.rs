//! Renders the tool-sync progress screen (screen A) at a chosen width.
//!
//! The bars are the ones `reconcile_desired_tools` registers during a tool
//! sync: a `[res]` resolve bar per tool, a `[fch]` fetch bar and a `[pro]`
//! process bar for each tool that has payloads, the `[prn]` prune bar, and the
//! pinned `syncing tools` overall bar. Tool-sync rows draw the built-in prefix,
//! so what gives way under a narrow terminal is fixed by
//! `semantic_truncate_prefix`: the version shortens from the right, then the
//! tally is dropped whole, then the phase tag. Where that leaves the labels is
//! a measured fact about these seeds and it moves when they move, so it is
//! recorded in `support/tool_sync.rs` rather than pinned here.
//!
//! The per-screen spec is the "Screen A: tool-sync" section of
//! `.agents/instructions/progress-output.instructions.md`.
//!
//! The data is synthetic, so the example runs offline and in about a
//! millisecond.
//!
//! ```text
//! cargo run --package mediapm --example mediapm_progress_tool_sync
//! cargo run --package mediapm --example mediapm_progress_tool_sync -- --width 50
//! ```
//!
//! What reaches stdout is the rendered grid with its colour escapes removed, so
//! redirecting the command writes the transcript checked in under
//! `examples/fixtures/mediapm_progress_tool_sync/`. Each transcript is named for
//! the width it was captured at, and `tool_sync_screen_matches_every_committed_transcript`
//! reads the widths out of those names.

#[path = "support/mod.rs"]
mod support;

#[path = "support/tool_sync.rs"]
mod tool_sync;

use support::ScreenConfig;

/// Print the tool-sync screen at the size the arguments select.
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
    let grid = tool_sync::render_tool_sync_screen(config);
    println!("{}", support::strip_ansi_escapes(&grid));
}

#[cfg(test)]
mod tests {
    use super::support::ScreenConfig;
    use super::support::{
        DEFAULT_HEIGHT, DEFAULT_WIDTH, assert_every_transcript_matches,
        assert_narrow_rows_carry_a_label_or_a_count, assert_no_row_wraps_over_sweep_widths,
        parse_screen_config, strip_ansi_escapes,
    };
    use super::tool_sync::render_tool_sync_screen;

    /// The tool-sync screen at every width a transcript is checked in at.
    ///
    /// The widths come from the filenames in
    /// `examples/fixtures/mediapm_progress_tool_sync/`, so this test has no
    /// list of its own to fall out of date. A file in that directory that does
    /// not parse as a transcript name fails the test rather than going unread.
    ///
    /// The rows, read from the top: a resolve bar per tool, a fetch bar and a
    /// process bar for the tool that has payloads, the prune bar, and the
    /// overall bar. The overall bar carries no phase at any width, so its name
    /// is the whole of its identity.
    #[test]
    fn tool_sync_screen_matches_every_committed_transcript() {
        assert_every_transcript_matches(
            "mediapm_progress_tool_sync",
            "tool-sync",
            DEFAULT_HEIGHT,
            |config| strip_ansi_escapes(&render_tool_sync_screen(config)),
        );
    }

    /// No width wraps a row.
    ///
    /// The prefix is the field that gives way: the version shortens, then the
    /// tally, then the phase tag, then the label.
    ///
    /// It asserts nothing about the bar. Below the fill crossing the row drops
    /// it on purpose so the label can have the columns, so "carries a fill
    /// cell" is false by design there. The sweep below asks what survives the
    /// crossing: that a row still says something.
    #[test]
    fn tool_sync_screen_never_wraps_a_row() {
        assert_no_row_wraps_over_sweep_widths("tool sync", |config| {
            strip_ansi_escapes(&render_tool_sync_screen(config))
        });
    }

    /// Once a width is readable on this screen, every wider width is too.
    ///
    /// The sweep beside this one only asks that a row fits. This asks that it
    /// has something on it, which is the property a fit can be satisfied by
    /// breaking. Below the fill crossing the bar is dropped on purpose, so the
    /// row is carried by its label and its tally, and by its timing when it has
    /// neither; a screen whose rows go empty again after a readable width is
    /// losing information it had.
    #[test]
    fn tool_sync_screen_rows_never_go_empty() {
        assert_narrow_rows_carry_a_label_or_a_count("tool sync", |config| {
            strip_ansi_escapes(&render_tool_sync_screen(config))
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
        assert_eq!(reject(&["--width", "100000"]), "--width must be between 8 and 500, got 100000");
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
