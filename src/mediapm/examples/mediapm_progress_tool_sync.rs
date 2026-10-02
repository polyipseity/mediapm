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
//! cargo run --package mediapm --example mediapm_progress_tool_sync -- --width 30
//! ```
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

    /// The exact grid at `DEFAULT_WIDTH`, the narrowest width
    /// `mediapm-utils/tests/progress_output/common.rs` calls `W`.
    ///
    /// The bar keeps twenty-six of the eighty columns. Both label columns are
    /// what they measure, and the fill takes the rest, so the screen renders the
    /// same at any width down to the point where the labels and the floor stop
    /// fitting between them.
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

    /// The exact grid at 60 columns, where the labels take what they measure and
    /// the bar is squeezed to six columns. Together with the 80-column case this
    /// pins both ends of the width-sensitive part of the layout.
    #[test]
    fn tool_sync_screen_at_narrow_width_matches_inline_grid() {
        let config = ScreenConfig { width: 60, height: DEFAULT_HEIGHT };
        let grid = strip_ansi_escapes(&render_tool_sync_screen(config));
        assert_eq!(
            grid,
            concat!(
                "⠏         ffmpeg v7.1 [res] ██████  2/2 0s 2 cached\n",
                "⠏      yt-dlp v2025.1 [res] ██████  1/1 0s skipped, 1 cached\n",
                "⠏         deno v2.1.4 [res] ██████  1/1 0s\n",
                "⠏     deno v2.1.4 [fch] 1/2 ██████  31M/31M 3s 1 cached\n",
                "⠏     deno v2.1.4 [pro] 2/3 ██████  3/3 2s\n",
                "⠏             pruning [prn] ██████  2/2 1s\n",
                "⠏             syncing tools ██████  3/3 9s",
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
