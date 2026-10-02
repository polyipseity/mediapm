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
//! 40 column ceiling, and every seed but the overall bar's carries a path. The
//! module doc in `support/materialization.rs` has the arithmetic and the seed
//! sizes.
//!
//! The data is synthetic, so the example runs offline and in about a
//! millisecond.
//!
//! ```text
//! cargo run --package mediapm --example mediapm_progress_materialize
//! cargo run --package mediapm --example mediapm_progress_materialize -- --width 39
//! ```
//!
//! 48 is the narrowest width at which every row still draws a fill column,
//! measured by rendering this screen at successive widths rather than
//! inherited from the other two screens. Below it no row draws one, and at 46
//! and down the grid starts interleaving a blank line per row.
//! `materialization_screen_below_the_fill_boundary` describes that regime
//! rather than pinning it as a transcript. The width used to be 39, before
//! the materialization label started rendering the auto-derived suffix; see
//! `materialization_screen_at_fill_boundary_matches_inline_grid` for the
//! arithmetic.
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
    /// Every prefix shares one slot sized from the longest seed, so the name
    /// gives up four columns to its directory on the rows that have both. The
    /// suffix carries the auto-derived timing, `6s` and `0/d` per row. A layout
    /// change that stops clamping has to change this transcript.
    #[test]
    fn materialization_screen_at_default_width_matches_inline_grid() {
        let config = ScreenConfig { width: DEFAULT_WIDTH, height: DEFAULT_HEIGHT };
        let grid = strip_ansi_escapes(&render_materialization_screen(config));
        assert_eq!(
            grid,
            concat!(
                "⠏     [cmt] 01 - Telepathy.flac …en █████████████████████████████████ 6s\n",
                "⠸     [stg] …r Gonna Give You Up …y ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░ 6s 0/d\n",
                "⠴     [wrt] …a Give You Up …y links ███████████░░░░░░░░░░░░░░░░░░░░░░ 4s 0/d\n",
                "⠇     [vrf] …- In the Flesh?.m4a …l ███████████░░░░░░░░░░░░░░░░░░░░░░ 0s 0/d\n",
                "⠋               [mat] materializing ███████████░░░░░░░░░░░░░░░░░░░░░░ 13s 1/m",
            )
        );
    }

    /// The exact grid at 48 columns, the narrowest width at which every row
    /// still draws a fill column.
    ///
    /// This width used to be 39, and it moved because the materialization
    /// label started rendering the auto-derived suffix. The shared prefix
    /// slot on this screen is content-driven rather than width-driven: the
    /// longest seed is 29 columns and the renderer adds 4 bytes of ANSI
    /// overhead, so the client gets 29 columns back whatever the terminal is.
    /// That leaves 6 columns at a 39-column terminal once the spinner and the
    /// separators are laid out, and the new 6-column suffix now takes all of
    /// them, leaving the bar nothing and pushing the line to 43. From 48 the
    /// budget fits again. The number is a property of the current budget
    /// arithmetic, not of this screen, so it is worth re-measuring rather than
    /// treating as fixed.
    ///
    /// Together with the 80-column case this pins the working side of the
    /// boundary. The failing side is described by
    /// `materialization_screen_below_the_fill_boundary`.
    #[test]
    fn materialization_screen_at_fill_boundary_matches_inline_grid() {
        let config = ScreenConfig { width: 48, height: DEFAULT_HEIGHT };
        let grid = strip_ansi_escapes(&render_materialization_screen(config));
        assert_eq!(
            grid,
            concat!(
                "⠏     [cmt] 01 - Telepathy.flac …en █ 6s\n",
                "⠸     [stg] …r Gonna Give You Up …y ░ 6s 0/d\n",
                "⠴     [wrt] …a Give You Up …y links ░ 4s 0/d\n",
                "⠇     [vrf] …- In the Flesh?.m4a …l ░ 0s 0/d\n",
                "⠋               [mat] materializing ░ 13s 1/m",
            )
        );
    }

    /// The frame a folder entry wider than the client budget produces.
    ///
    /// The folder is the online demo's. Its `entry_name` is 59 columns against
    /// a 29 column budget, and `entry_name` is elastic, so the name
    /// shortens from the front and rows two and three keep
    /// `[youtube.dQw4w9WgXcQ]`, the part that names the video. The old
    /// ranking made `entry_name` a `Segment::keep`, which `fit_segments`
    /// drops whole once nothing can shrink, and those two rows rendered as a
    /// bare `[stg]` and `[wrt]` with no identity at all.
    ///
    /// The over-long seed still costs the other rows, because the shared
    /// prefix slot is sized from the longest seed: it grows from 29 columns to
    /// 40 and the two media rows surrender part of their directory to
    /// `… Children` and `…e Wall`. One folder name this wide costs every row
    /// on the screen some of its path, which is the reason the frame is
    /// pinned here rather than left to the module doc's word "over-long".
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

    /// What happens below the fill boundary, described rather than pinned.
    ///
    /// The prefix slot is sized from the longest seed and clamped to 40
    /// columns, and it does not shrink with the terminal, so the bar is the
    /// first thing to go. At 47 columns the grid is five lines but no row draws
    /// a fill; from 46 down to 37 it grows an interleaved blank line per row as
    /// the content wraps. No test asserts those exact bytes: a fixture made of
    /// blank lines would read like a transcript when it is the renderer
    /// reporting that it ran out of room, so the regime lives in this comment
    /// and in the width list instead.
    #[test]
    fn materialization_screen_below_the_fill_boundary() {
        for width in [47, 46, 45, 44, 43, 42, 41, 40, 39, 38, 37, 36, 35, 30] {
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
