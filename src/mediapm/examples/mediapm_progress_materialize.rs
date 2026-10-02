//! Renders the materialization progress screen (screen C) at a chosen width.
//!
//! The bars are the ones `sync_hierarchy` registers: the pinned
//! `materializing [mat]` overall bar, one bar per hierarchy entry seeded with
//! `{relative_path} [stg]`, and, inside a media folder entry, one sub-bar per
//! extracted variant seeded with `{variant_name} [wrt]`. A media entry walks
//! its bar through `[stg]`, `[vrf]`, and `[cmt]`; a folder or playlist entry
//! keeps the phase it was created with, because only the media arm installs a
//! later one. See the module doc in `support/materialization.rs`.
//!
//! The rows are the same width whatever the terminal is, because the shared
//! prefix slot is sized from the longest seed and clamped to the renderer's
//! 40 column ceiling, and every seed but the overall bar's carries a path. No
//! row shows an elapsed or rate column: this label's suffix truncation returns
//! an empty string, which replaces the renderer's auto-derived suffix rather
//! than supplementing it. That is why this screen looks nothing like the
//! other two, and the module doc in `support/materialization.rs` has the
//! call sites.
//!
//! The data is synthetic, so the example runs offline and in about a
//! millisecond.
//!
//! ```text
//! cargo run --package mediapm --example mediapm_progress_materialize
//! cargo run --package mediapm --example mediapm_progress_materialize -- --width 39
//! ```
//!
//! 39 is the narrowest width that still gives every row a fill column,
//! measured by rendering this screen at successive widths rather than
//! inherited from the other two screens. At 38 two rows keep a fill and three
//! are bare, and at 37 no row has one and the grid starts interleaving a blank
//! line per row. `materialization_screen_below_the_fill_boundary` describes
//! that regime rather than pinning it as a transcript.
//!
//! What reaches stdout is the rendered grid with its colour escapes removed, so
//! redirecting the command writes the transcript checked in under
//! `examples/fixtures/mediapm_progress_materialize/`.

#[path = "support/mod.rs"]
mod support;

#[path = "support/materialization.rs"]
mod materialization;

use support::ScreenConfig;

/// Print the materialization screen at the size the arguments select.
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
    let grid = materialization::render_materialization_screen(config);
    println!("{}", support::strip_ansi_escapes(&grid));
}

#[cfg(test)]
mod tests {
    use super::materialization::{
        render_materialization_screen, render_materialization_screen_for_folder,
    };
    use super::support::ScreenConfig;
    use super::support::{DEFAULT_HEIGHT, DEFAULT_WIDTH, parse_screen_config, strip_ansi_escapes};

    /// Media folder entry as `demo_hierarchy_spec::online_demo_media_folder_relative()`
    /// spells it, quoted rather than computed so the frame below stays a
    /// transcript of a fixed name.
    const WIDE_FOLDER_ENTRY: &str =
        "music videos/Rick Astley - Never Gonna Give You Up [youtube.dQw4w9WgXcQ]";

    /// The exact grid at `DEFAULT_WIDTH`.
    ///
    /// Read the rows from the top. A finished media entry sits on `[cmt]`
    /// because that is the last label the media arm installs, and the label is
    /// never cleared on finish. Below it the folder entry is still on `[stg]`,
    /// which is the faithful render: the folder arm of the dispatch never
    /// installs a later phase label, so a folder row carries its creation
    /// phase for its whole run. The `[wrt]` sub-bar under it is one of three
    /// extracted link members in. The last entry has resolved its hash and is
    /// on `[vrf]`. The overall bar is at one of three entries and still
    /// active, because the other two entries have not committed.
    ///
    /// No row carries an `[F]` or `[W]` marker even though the materializer
    /// finishes bars with `finish_warning` and `finish_error`. No call site
    /// sets `status_marker`, so that segment never renders here.
    ///
    /// Every prefix is clipped by the shared prefix slot, which is sized from
    /// the longest seed, so the album row's directory is front-ellipsised to
    /// `…en` while the `[vrf]` row loses its path whole. No row carries an
    /// elapsed, rate, or eta column either: the label's `truncate_suffix`
    /// returns an empty string, which replaces the renderer's auto-derived
    /// suffix instead of supplementing it. A layout change that stops clamping
    /// has to change this transcript.
    #[test]
    fn materialization_screen_at_default_width_matches_inline_grid() {
        let config = ScreenConfig { width: DEFAULT_WIDTH, height: DEFAULT_HEIGHT };
        let grid = strip_ansi_escapes(&render_materialization_screen(config));
        assert_eq!(
            grid,
            concat!(
                "⠏     [cmt] 01 - Telepathy.flac …en ██████████████████████████████████████████\n",
                "⠸     [stg] Never Gonna Give You Up ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░\n",
                "⠴     [wrt] Never Gonna Give You Up ██████████████░░░░░░░░░░░░░░░░░░░░░░░░░░░░\n",
                "⠇      [vrf] 01 - In the Flesh?.m4a ██████████████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░\n",
                "⠋               [mat] materializing ██████████████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░",
            )
        );
    }

    /// The exact grid at 39 columns, the narrowest width that still leaves
    /// every row a fill column.
    ///
    /// Together with the 80-column case this pins the working side of the
    /// boundary. The failing side is described by
    /// `materialization_screen_below_the_fill_boundary`.
    #[test]
    fn materialization_screen_at_narrow_width_matches_inline_grid() {
        let config = ScreenConfig { width: 39, height: DEFAULT_HEIGHT };
        let grid = strip_ansi_escapes(&render_materialization_screen(config));
        assert_eq!(
            grid,
            concat!(
                "⠏     [cmt] 01 - Telepathy.flac …en █\n",
                "⠸     [stg] Never Gonna Give You Up ░\n",
                "⠴     [wrt] Never Gonna Give You Up ░\n",
                "⠇      [vrf] 01 - In the Flesh?.m4a ░░\n",
                "⠋               [mat] materializing ░░",
            )
        );
    }

    /// The frame a folder entry wider than the client budget produces.
    ///
    /// The folder is the online demo's. Its `entry_name` is 59 columns against
    /// a 36 column budget, and `entry_name` is a keep segment, so
    /// `fit_segments` drops it whole rather than shortening it and rows two
    /// and three keep nothing but their phase tag. The row loses its identity,
    /// and for a real music library a name that long is ordinary, not exotic.
    ///
    /// The over-long seed costs the other rows as well. The shared prefix slot
    /// is sized from the longest seed, so it grows from 36 columns to 43 and
    /// the two media rows surrender most of their directory to `[cmt] …
    /// Children` and `[vrf] …e Wall`. One folder name this wide costs every row
    /// on the screen its path, which is the reason the frame is pinned here
    /// rather than left to the module doc's word "over-long".
    #[test]
    fn materialization_screen_drops_a_folder_name_wider_than_the_budget() {
        let config = ScreenConfig { width: DEFAULT_WIDTH, height: DEFAULT_HEIGHT };
        let grid = strip_ansi_escapes(&render_materialization_screen_for_folder(
            config,
            WIDE_FOLDER_ENTRY,
        ));
        assert_eq!(
            grid,
            concat!(
                "⠏     [cmt] 01 - Telepathy.flac … Children ███████████████████████████████████\n",
                "⠸                                    [stg] ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░\n",
                "⠴                                    [wrt] ███████████░░░░░░░░░░░░░░░░░░░░░░░░\n",
                "⠇     [vrf] 01 - In the Flesh?.m4a …e Wall ████████████░░░░░░░░░░░░░░░░░░░░░░░░\n",
                "⠋                      [mat] materializing ████████████░░░░░░░░░░░░░░░░░░░░░░░░"
            )
        );
    }

    /// What happens below the fill boundary, described rather than pinned.
    ///
    /// The prefix slot is sized from the longest seed and clamped to 40
    /// columns, and it does not shrink with the terminal, so the fill is the
    /// first thing to go. At 38 columns two rows still draw a fill and three
    /// are bare; from 37 down to 30 no row draws one, and the captured grid
    /// grows an interleaved blank line per row as content wraps. No test
    /// asserts those exact bytes: a fixture made of blank lines would read
    /// like a transcript when it is the renderer reporting that it ran out of
    /// room, so the regime lives in this comment instead.
    #[test]
    fn materialization_screen_below_the_fill_boundary() {
        for width in [37, 36, 35, 30] {
            let config = ScreenConfig { width, height: DEFAULT_HEIGHT };
            let grid = strip_ansi_escapes(&render_materialization_screen(config));
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
