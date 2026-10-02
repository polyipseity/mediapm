//! Renders the tool-sync progress screen (screen A) at a chosen width.
//!
//! The bars are the ones `reconcile_desired_tools` registers during a tool
//! sync: a `[res]` resolve bar per tool, a `[fch]` fetch bar and a `[pro]`
//! process bar for each tool that has payloads, the `[prn]` prune bar, and the
//! pinned `syncing tools` overall bar. The data is synthetic, so the example
//! runs offline and in about a millisecond.
//!
//! ```text
//! cargo run --package mediapm --example mediapm_progress_tool_sync
//! cargo run --package mediapm --example mediapm_progress_tool_sync -- --width 50
//! ```
//!
//! 50 is the narrowest width at which every row still renders its phase tag,
//! measured by rendering this screen at successive widths rather than taken
//! from the other two screens. Tool-sync rows draw the built-in prefix, so what
//! goes under pressure is fixed by `semantic_truncate_prefix`: the version is
//! shaved from the right first, so `yt-dlp v2025.1 [res]` loses the `1` and
//! then the whole version, then the tally is dropped whole, and the phase tag
//! is the last thing to go. At 49 the prune row has lost `[prn]` and the
//! overall bar reads `syncing tool`.
//! `tool_sync_screen_never_wraps_a_row` covers the widths below 50.
//!
//! What reaches stdout is the rendered grid with its colour escapes removed, so
//! redirecting the command writes the transcript checked in under
//! `examples/fixtures/mediapm_progress_tool_sync/`.

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
    use super::support::{DEFAULT_HEIGHT, DEFAULT_WIDTH, parse_screen_config, strip_ansi_escapes};
    use super::tool_sync::render_tool_sync_screen;

    /// The exact grid at `DEFAULT_WIDTH`, the width every screen is pinned at
    /// as well as the width the three examples default to.
    ///
    /// The prefix slot is what the widest label on the screen measures, so all
    /// seven rows render their label in full and the fill takes the rest of the
    /// line. Narrowing the terminal does not move that until the labels and the
    /// fill no longer fit between them, which
    /// `tool_sync_screen_at_narrowest_identifiable_width_matches_inline_grid`
    /// measures.
    #[test]
    fn tool_sync_screen_at_default_width_matches_inline_grid() {
        let config = ScreenConfig { width: DEFAULT_WIDTH, height: DEFAULT_HEIGHT };
        let grid = strip_ansi_escapes(&render_tool_sync_screen(config));
        assert_eq!(
            grid,
            concat!(
                "⠏         ffmpeg v7.1 [res] ██████████████████████████  2/2 0s 2 cached\n",
                "⠏      yt-dlp v2025.1 [res] ██████████████████████████  1/1 0s skipped, 1 cached\n",
                "⠏         deno v2.1.4 [res] ██████████████████████████  1/1 0s\n",
                "⠏     deno v2.1.4 [fch] 1/2 ██████████████████████████  31M/31M 3s 1 cached\n",
                "⠏     deno v2.1.4 [pro] 2/3 ██████████████████████████  3/3 2s\n",
                "⠏             pruning [prn] ██████████████████████████  2/2 1s\n",
                "⠏             syncing tools ██████████████████████████  3/3 9s",
            )
        );
    }

    /// The exact grid at 50 columns, the narrowest width at which every row
    /// still renders its phase tag. The overall bar is the exception the
    /// fixture records rather than works around: it is seeded `syncing tools`
    /// and carries no phase at any width, so the name it renders is the whole
    /// of its identity.
    ///
    /// Every row has already given something up to get here: the tally is
    /// gone from five rows and the version is shortened on the resolve rows.
    /// What this pins is that the phase tag outranks both, which is the order
    /// the removal table in `components.rs` records. One column narrower and
    /// `[prn]` is dropped off the prune row.
    #[test]
    fn tool_sync_screen_at_narrowest_identifiable_width_matches_inline_grid() {
        let config = ScreenConfig { width: 50, height: DEFAULT_HEIGHT };
        let grid = strip_ansi_escapes(&render_tool_sync_screen(config));
        assert_eq!(
            grid,
            concat!(
                "⠏      ffmpeg [res] ████  2/2 0s 2 cached\n",
                "⠏      yt-dlp [res] ████  1/1 0s skipped, 1 cached\n",
                "⠏     deno v2 [res] ████  1/1 0s\n",
                "⠏        deno [fch] ████  31M/31M 3s 1 cached\n",
                "⠏        deno [pro] ████  3/3 2s\n",
                "⠏     pruning [prn] ████  2/2 1s\n",
                "⠏     syncing tools ████  3/3 9s",
            )
        );
    }

    /// No width wraps a row, down to the narrowest the harness accepts.
    ///
    /// The prefix is the field that gives way: the version shortens, then the
    /// tally, then the phase tag, then the label. The fill keeps four cells
    /// from 9 columns up and three at the 8-column minimum the harness
    /// accepts, so this asserts the row count and the row width rather than
    /// the fill.
    #[test]
    fn tool_sync_screen_never_wraps_a_row() {
        for width in
            [48, 46, 44, 42, 40, 38, 36, 34, 32, 30, 28, 26, 24, 22, 20, 18, 16, 14, 12, 10, 8]
        {
            let config = ScreenConfig { width, height: DEFAULT_HEIGHT };
            let grid = strip_ansi_escapes(&render_tool_sync_screen(config));
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
