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
//! What each tag stands for is tabulated under "Materialization phase tags" in
//! `.agents/instructions/progress-output.instructions.md`: `stg` copies CAS
//! content into the staging area, `vrf` checks the staged bytes, `cmt` writes
//! into the library, `wrt` is one file inside a folder variant, and `mat`
//! labels this screen's overall bar.
//!
//! The shared prefix slot is the widest label the screen draws, held to what
//! the terminal has left after the spinner, the separators and the suffix, with
//! a fill floor under the bar while there is room for a bar at all. Every entry
//! label here carries a path, so the path shortens
//! from the front as the terminal narrows and the file name survives longest.
//! Where that leaves the labels is a measured fact about these seed paths and
//! it moves when they move, so it is recorded in `support/materialization.rs`
//! rather than pinned here.
//!
//! The data is synthetic, so the example runs offline and in about a
//! millisecond.
//!
//! ```text
//! cargo run --package mediapm --example mediapm_progress_materialize
//! cargo run --package mediapm --example mediapm_progress_materialize -- --width 40
//! ```
//!
//! What reaches stdout is the rendered grid with its colour escapes removed, so
//! redirecting the command writes the transcript checked in under
//! `examples/fixtures/mediapm_progress_materialize/`. Each transcript is named
//! for the width it was captured at, and
//! `materialization_screen_matches_every_committed_transcript` reads the widths
//! out of those names.

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
    use super::support::{
        DEFAULT_HEIGHT, DEFAULT_WIDTH, assert_every_transcript_matches,
        assert_narrow_rows_carry_a_label_or_a_count, assert_no_row_wraps_over_sweep_widths,
        parse_screen_config, strip_ansi_escapes,
    };

    /// Media folder entry as `demo_hierarchy_spec::online_demo_media_folder_relative()`
    /// spells it, quoted rather than computed so the frame below stays a
    /// transcript of a fixed name.
    const WIDE_FOLDER_ENTRY: &str =
        "music videos/Rick Astley - Never Gonna Give You Up [youtube.dQw4w9WgXcQ]";

    /// The materialization screen at every width a transcript is checked in
    /// at.
    ///
    /// The widths come from the filenames in
    /// `examples/fixtures/mediapm_progress_materialize/`, so this test has no
    /// list of its own to fall out of date. A file in that directory that does
    /// not parse as a transcript name fails the test rather than going unread.
    ///
    /// Read the rows from the top in any one frame. A finished media entry sits
    /// on `[cmt]`, which is the last label the media arm installs and is never
    /// cleared on finish. Below it the folder entry is still on `[stg]`, which
    /// is the faithful render: the folder arm of the dispatch never installs a
    /// later phase label, so a folder row carries its creation phase for its
    /// whole run. The `[wrt]` sub-bar under it is one of three extracted link
    /// members in. The last entry has resolved its hash and is on `[vrf]`. The
    /// overall bar is at one of three entries and still active, because the
    /// other two have not committed. No row carries a marker, because nothing
    /// here finishes with a warning or an error.
    #[test]
    fn materialization_screen_matches_every_committed_transcript() {
        assert_every_transcript_matches(
            "mediapm_progress_materialize",
            "materialize",
            DEFAULT_HEIGHT,
            |config| strip_ansi_escapes(&render_materialization_screen(config)),
        );
    }

    /// No width wraps a row.
    ///
    /// Under the old rule the prefix kept the width of the longest seed and the
    /// row outgrew the terminal, so from the middle of the range down the grid
    /// interleaved a blank line per row. What gives way now is the prefix, one
    /// ranked field at a time.
    ///
    /// It asserts nothing about the bar. Below the fill crossing the row drops
    /// it on purpose so the label can have the columns, so "carries a fill
    /// cell" is false by design there. The sweep below asks what survives the
    /// crossing: that a row still says something.
    #[test]
    fn materialization_screen_never_wraps_a_row() {
        assert_no_row_wraps_over_sweep_widths("materialization", |config| {
            strip_ansi_escapes(&render_materialization_screen(config))
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
    fn materialization_screen_rows_never_go_empty() {
        assert_narrow_rows_carry_a_label_or_a_count("materialization", |config| {
            strip_ansi_escapes(&render_materialization_screen(config))
        });
    }

    /// The frame a folder entry wider than the client budget produces.
    ///
    /// The folder is the online demo's. Its `entry_name` is 58 columns against
    /// the slot the terminal leaves, and `entry_name` is elastic, so the name
    /// is cut back to a boundary and rows two and three keep
    /// `Up [youtube.dQw4w9WgXcQ]`, the part that names the video. The old
    /// ranking made `entry_name` a `Segment::keep`, which `fit_segments`
    /// drops whole once nothing can shrink, and those two rows rendered as a
    /// bare `[stg]` and `[wrt]` with no identity at all.
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
                "⠏       [cmt] 01 - Telepathy.flac Children ██████████████████████████ 6s\n",
                "⠸       [stg] You Up [youtube.dQw4w9WgXcQ] ░░░░░░░░░░░░░░░░░░░░░░░░░░ 6s 0/d\n",
                "⠴     [wrt] Up [youtube.dQw4w9WgXcQ] links ████████░░░░░░░░░░░░░░░░░░ 4s 0/d\n",
                "⠇        [vrf] 01 - In the Flesh?.m4a Wall ████████░░░░░░░░░░░░░░░░░░ 0s 0/d\n",
                "⠋                      [mat] materializing ████████░░░░░░░░░░░░░░░░░░ 13s 1/m"
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
