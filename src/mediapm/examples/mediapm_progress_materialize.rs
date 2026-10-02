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
//! The shared prefix slot is the widest label the screen draws, held to what
//! the terminal has left after the spinner, the separators, the suffix and a
//! fill floor. Every entry label here carries a path, so the path shortens
//! from the front as the terminal narrows and the file name survives longest.
//!
//! The data is synthetic, so the example runs offline and in about a
//! millisecond.
//!
//! ```text
//! cargo run --package mediapm --example mediapm_progress_materialize
//! cargo run --package mediapm --example mediapm_progress_materialize -- --width 39
//! ```
//!
//! 27 is the narrowest width at which every row still draws its phase tag,
//! measured by rendering this screen at successive widths rather than inherited
//! from the other two screens. Below it the prefix slot is empty and the rows
//! are the spinner, the fill and the suffix.
//! `materialization_screen_never_wraps_a_row` covers the widths under it.
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
    /// No row carries an `[F]` or `[W]` marker, because nothing here finishes
    /// with a warning or an error. The materializer does install one on those
    /// finishes; this screen just never takes that path.
    ///
    /// Every prefix shares one slot, and the longest label on the screen sets
    /// it, so the two rows whose name and directory compete keep both ends:
    /// `01 - Telepathy.flac` on the committed row and `… Children` on the one
    /// still staging. The suffix carries the auto-derived timing, `6s` and
    /// `0/d` per row.
    #[test]
    fn materialization_screen_at_default_width_matches_inline_grid() {
        let config = ScreenConfig { width: DEFAULT_WIDTH, height: DEFAULT_HEIGHT };
        let grid = strip_ansi_escapes(&render_materialization_screen(config));
        assert_eq!(
            grid,
            concat!(
                "⠏     [cmt] 01 - Telepathy.flac … Children ██████████████████████████ 6s\n",
                "⠸     [stg] Never Gonna Give You Up …stley ░░░░░░░░░░░░░░░░░░░░░░░░░░ 6s 0/d\n",
                "⠴     [wrt] …er Gonna Give You Up …y links ████████░░░░░░░░░░░░░░░░░░ 4s 0/d\n",
                "⠇     [vrf] 01 - In the Flesh?.m4a …e Wall ████████░░░░░░░░░░░░░░░░░░ 0s 0/d\n",
                "⠋                      [mat] materializing ████████░░░░░░░░░░░░░░░░░░ 13s 1/m",
            )
        );
    }

    /// The exact grid at 27 columns, the narrowest width at which every row
    /// still draws its phase tag.
    ///
    /// The slot has one field left here. The suffix takes what the active rows
    /// reserve, the fill keeps its four cells, and the phase tag is the only
    /// prefix segment short enough to survive the rest, which is why it is the
    /// one that is still readable. The width moved from 48 when the prefix
    /// started being budgeted from the terminal, so it is worth re-measuring
    /// rather than treating as fixed.
    ///
    /// At 26 the slot is empty on every row and each one is the spinner, the
    /// fill and the suffix, which is what
    /// `materialization_screen_never_wraps_a_row` covers.
    #[test]
    fn materialization_screen_at_narrowest_identifiable_width_matches_inline_grid() {
        let config = ScreenConfig { width: 27, height: DEFAULT_HEIGHT };
        let grid = strip_ansi_escapes(&render_materialization_screen(config));
        assert_eq!(
            grid,
            concat!(
                "⠏     [cmt] ████ 6s\n",
                "⠸     [stg] ░░░░ 6s 0/d\n",
                "⠴     [wrt] █░░░ 4s 0/d\n",
                "⠇     [vrf] █░░░ 0s 0/d\n",
                "⠋     [mat] █░░░ 13s 1/m",
            )
        );
    }

    /// The frame a folder entry wider than the client budget produces.
    ///
    /// The folder is the online demo's. Its `entry_name` is 59 columns against
    /// the slot the terminal leaves, and `entry_name` is elastic, so the name
    /// shortens from the front and rows two and three keep
    /// `[youtube.dQw4w9WgXcQ]`, the part that names the video. The old
    /// ranking made `entry_name` a `Segment::keep`, which `fit_segments`
    /// drops whole once nothing can shrink, and those two rows rendered as a
    /// bare `[stg]` and `[wrt]` with no identity at all.
    ///
    /// The other rows are unchanged from the 80-column grid above, because
    /// their own labels already fill the 40-column slot. The slot is what the
    /// widest label measures, capped at `MAX_PREFIX_WIDTH`, so a name wider
    /// than the slot shortens inside it and costs the rows that needed no
    /// shortening nothing. Which fields survive that is the label's ranking
    /// and not the budget's business.
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
                "⠏     [cmt] 01 - Telepathy.flac … Children ██████████████████████████ 6s\n",
                "⠸     [stg] …u Up [youtube.dQw4w9WgXcQ] …s ░░░░░░░░░░░░░░░░░░░░░░░░░░ 6s 0/d\n",
                "⠴     [wrt] …youtube.dQw4w9WgXcQ] …s links ████████░░░░░░░░░░░░░░░░░░ 4s 0/d\n",
                "⠇     [vrf] 01 - In the Flesh?.m4a …e Wall ████████░░░░░░░░░░░░░░░░░░ 0s 0/d\n",
                "⠋                      [mat] materializing ████████░░░░░░░░░░░░░░░░░░ 13s 1/m",
            )
        );
    }

    /// No width wraps a row, down to the narrowest the harness accepts.
    ///
    /// Under the old rule the prefix kept the width of the longest seed and the
    /// row outgrew the terminal, so from 46 columns down the grid interleaved a
    /// blank line per row. What gives way now is the prefix, one ranked field
    /// at a time, and the fill keeps four cells from 9 columns up and three at
    /// the 8-column minimum the harness accepts. The row count and the row
    /// width are what this pins; the fill is not what is at stake.
    #[test]
    fn materialization_screen_never_wraps_a_row() {
        for width in [26, 24, 22, 20, 18, 16, 14, 12, 10, 8] {
            let config = ScreenConfig { width, height: DEFAULT_HEIGHT };
            let grid = strip_ansi_escapes(&render_materialization_screen(config));
            let rows: Vec<&str> = grid.lines().collect();
            assert_eq!(rows.len(), 5, "width {width} must draw five rows: {rows:?}");
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
