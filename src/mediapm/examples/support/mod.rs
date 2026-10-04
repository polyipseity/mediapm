//! Shared harness for the `mediapm_progress_*` examples.
//!
//! Each screen example drives a real [`ProgressTerminal`] whose draw target is
//! an [`InMemoryTerm`] at a size the caller chose, then prints the grid that
//! came back out. The transcript on stdout is therefore what the renderer drew
//! at that width, not a transcription of it. Nothing here touches the network,
//! the filesystem, or a CAS store.
//!
//! The harness supplies the pieces every screen needs: a [`ScreenConfig`]
//! parsed from `--width` and `--height`, a terminal built around the captured
//! grid, and a synthetic clock the screen can advance so the elapsed and rate
//! columns hold numbers instead of depending on wall time.
//!
//! Each screen's renderer lives in its own sibling file, `tool_sync.rs`,
//! `workflow.rs`, or `materialization.rs`, and each example declares only the
//! one it needs. Compiling every renderer into every example binary would
//! leave the unused ones dead, which `warnings = "deny"` turns into a build
//! failure.
//!
//! The files under `examples/fixtures/` hold the transcripts these renderers
//! produce, one per width. The directory listing is the list of widths: no
//! source file names them, and adding one is adding one file.
//!
//! The scenario axis lives in `support/scenarios.rs` rather than here, and an
//! example opts into it by declaring it. That split is what lets one of the three
//! screens move onto the axis while the other two have not: this file compiles
//! into every example binary, so an item here that only one screen called would
//! read as dead code in the other two, which build under `deny(warnings)`.

use std::fmt;
use std::sync::Arc;

#[cfg(test)]
use std::ops::RangeInclusive;
#[cfg(test)]
use std::path::{Path, PathBuf};

use indicatif::InMemoryTerm;
#[cfg(test)]
use mediapm_utils::progress::assert_frames_match;
use mediapm_utils::progress::{
    DimensionSource, ProgressTerminal, TestDimensionSource, TestTimeSource, TimeSource,
};

/// Terminal width used when `--width` is absent.
///
/// 80, not the `W` in `mediapm-utils/tests/progress_output/common.rs`. That
/// suite constant is 40 and its labels are short (`worker-a`), while a real
/// tool-sync label is `ffmpeg v7.1 [res]`. Both render without wrapping now
/// that the label slot is paid for out of the terminal width, so the choice is
/// what a transcript of a real screen should be, not a boundary.
pub const DEFAULT_WIDTH: u16 = 80;

/// Terminal height used when `--height` is absent.
///
/// The value is `H` in `mediapm-utils/tests/progress_output/common.rs`, for the
/// same reason as [`DEFAULT_WIDTH`].
pub const DEFAULT_HEIGHT: u16 = 24;

/// Narrowest width the harness accepts.
///
/// A bar needs room for a spinner, a prefix, a fill, and a suffix, so a width
/// this small shows only the left edge of every line. Nothing crashes there;
/// the bound exists so a mistyped width does not read as a real transcript.
const MIN_WIDTH: u16 = 8;

/// Largest width the harness accepts.
///
/// The prefix is capped at `MAX_PREFIX_WIDTH` and the suffix at
/// `MAX_SUFFIX_WIDTH` whatever the terminal is, so past a few hundred columns
/// the extra width is empty space on every line.
const MAX_WIDTH: u16 = 500;

/// Terminal widths the no-wrap sweep walks, inclusive.
///
/// The upper end is the widest committed transcript. The lower end is
/// [`MIN_WIDTH`], the narrowest the harness accepts, which is well below any
/// transcript: a transcript says what one width drew and cannot say that the
/// widths beside it neither wrapped nor went empty.
#[cfg(test)]
const SWEEP_WIDTHS: RangeInclusive<u16> = MIN_WIDTH..=120;

/// Shortest height the harness accepts.
const MIN_HEIGHT: u16 = 2;

/// Largest height the harness accepts.
///
/// A screen's frame is one line per reserved slot, so the height doubles as
/// the slot capacity. Past a few hundred lines a demo has far more slots than
/// bars and the extra space is never drawn on.
const MAX_HEIGHT: u16 = 200;

/// Flag that sets the terminal width.
const WIDTH_FLAG: &str = "--width";

/// Flag that sets the terminal height.
const HEIGHT_FLAG: &str = "--height";

/// Terminal size a screen renders at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScreenConfig {
    /// Terminal width in columns, which is what fixes the bar geometry.
    pub width: u16,
    /// Terminal height in rows. The renderer derives its slot capacity from
    /// this, so it also decides how many bars stay on screen at once.
    pub height: u16,
}

/// Why a set of command-line arguments did not produce a [`ScreenConfig`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScreenConfigError {
    /// A flag was the last argument, so it had no value to parse.
    MissingValue {
        /// The flag that was left without a value.
        flag: &'static str,
    },
    /// A value was not a base-10 unsigned integer.
    NotANumber {
        /// The flag whose value failed to parse.
        flag: &'static str,
        /// The text that was supplied.
        value: String,
    },
    /// A parsed value fell outside the range the harness accepts.
    OutOfRange {
        /// The flag whose value was out of range.
        flag: &'static str,
        /// The parsed value.
        value: u32,
        /// The inclusive lower bound that was violated.
        min: u32,
        /// The inclusive upper bound that was violated.
        max: u32,
    },
    /// An argument was neither a known flag nor the value of one.
    UnexpectedArgument {
        /// The argument as supplied.
        argument: String,
    },
}

impl fmt::Display for ScreenConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingValue { flag } => write!(f, "{flag} needs a value"),
            Self::NotANumber { flag, value } => {
                write!(f, "{flag} needs a whole number, got {value:?}")
            }
            Self::OutOfRange { flag, value, min, max } => {
                write!(f, "{flag} must be between {min} and {max}, got {value}")
            }
            Self::UnexpectedArgument { argument } => write!(
                f,
                "unexpected argument {argument:?}; expected {WIDTH_FLAG} or {HEIGHT_FLAG}"
            ),
        }
    }
}

impl std::error::Error for ScreenConfigError {}

/// Read `--width` and `--height` from an argument list.
///
/// An empty list yields [`DEFAULT_WIDTH`] by [`DEFAULT_HEIGHT`]. The two flags
/// may appear in either order, and a flag may be given at most once.
///
/// # Errors
///
/// Returns [`ScreenConfigError`] when a flag has no value, when a value is not
/// an unsigned integer, when a value falls outside the accepted range, or when
/// an argument is not a flag this harness knows.
pub fn parse_screen_config<I>(args: I) -> Result<ScreenConfig, ScreenConfigError>
where
    I: IntoIterator<Item = String>,
{
    let mut config = ScreenConfig { width: DEFAULT_WIDTH, height: DEFAULT_HEIGHT };
    let mut args = args.into_iter();
    while let Some(argument) = args.next() {
        let flag = match argument.as_str() {
            WIDTH_FLAG => WIDTH_FLAG,
            HEIGHT_FLAG => HEIGHT_FLAG,
            _ => return Err(ScreenConfigError::UnexpectedArgument { argument }),
        };
        let value = args.next().ok_or(ScreenConfigError::MissingValue { flag })?;
        let parsed: u32 = value
            .parse()
            .map_err(|_| ScreenConfigError::NotANumber { flag, value: value.clone() })?;
        match flag {
            WIDTH_FLAG => config.width = ranged(flag, parsed, MIN_WIDTH, MAX_WIDTH)?,
            _ => config.height = ranged(flag, parsed, MIN_HEIGHT, MAX_HEIGHT)?,
        }
    }
    Ok(config)
}

/// Narrow a parsed value to its inclusive bounds.
fn ranged(flag: &'static str, value: u32, min: u16, max: u16) -> Result<u16, ScreenConfigError> {
    match u16::try_from(value) {
        Ok(narrowed) if narrowed >= min && narrowed <= max => Ok(narrowed),
        _ => Err(ScreenConfigError::OutOfRange {
            flag,
            value,
            min: u32::from(min),
            max: u32::from(max),
        }),
    }
}

/// Build a terminal that draws into a captured grid of the requested size.
///
/// The returned clock is the one the renderer reads, so a screen can move
/// elapsed time forward without waiting on the wall clock. The draw target is
/// installed through
/// [`with_term_like`](mediapm_utils::progress::ProgressTerminalBuilder::with_term_like),
/// the same builder method production uses, so the write gate is exercised
/// exactly as it is during a real sync.
///
/// Visible to the sibling renderer modules rather than crate-wide, because a
/// screen renderer is the only thing that should open a captured terminal.
pub(super) fn capture_terminal(
    config: ScreenConfig,
) -> (ProgressTerminal, InMemoryTerm, Arc<TestTimeSource>) {
    let grid = InMemoryTerm::new(config.height, config.width);
    let clock = Arc::new(TestTimeSource::new());
    let terminal = ProgressTerminal::builder()
        .with_term_like(Box::new(grid.clone()))
        .with_dim_source(Arc::new(TestDimensionSource::new((config.height, config.width)))
            as Arc<dyn DimensionSource>)
        .dynamic_height(true)
        .with_pre_roll_capture(Box::new(InMemoryTerm::new(config.height, config.width)))
        .with_time_source(Arc::clone(&clock) as Arc<dyn TimeSource>)
        .with_ticker_enabled(false)
        .build();
    (terminal, grid, clock)
}

/// Remove ANSI SGR escape sequences from a rendered grid.
///
/// [`indicatif::InMemoryTerm::contents`] reassembles the drawn cells, and each
/// cell keeps the colour it was written with, so a raw grid is full of
/// `\x1b[...m` sequences. Stripping them leaves the layout readable and keeps
/// an exact assertion free of escape bytes.
///
/// A sequence that does not end in `m` is not a colour code and is left where
/// it is, since the captured grid holds none of those and silently dropping one
/// would change a length somebody is asserting on.
#[must_use]
pub fn strip_ansi_escapes(grid: &str) -> String {
    let mut stripped = String::with_capacity(grid.len());
    let mut chars = grid.chars().peekable();
    while let Some(character) = chars.next() {
        if character != '\x1b' {
            stripped.push(character);
            continue;
        }
        if chars.peek() != Some(&'[') {
            stripped.push('\x1b');
            continue;
        }
        let mut sequence = String::from("[");
        chars.next();
        let mut is_sgr = false;
        for next in chars.by_ref() {
            sequence.push(next);
            if next == 'm' {
                is_sgr = true;
                break;
            }
            if !next.is_ascii_digit() && next != ';' {
                break;
            }
        }
        if !is_sgr {
            stripped.push('\x1b');
            stripped.push_str(&sequence);
        }
    }
    stripped
}

/// Directory holding one screen's committed transcripts.
///
/// Resolved through `CARGO_MANIFEST_DIR` rather than the working directory,
/// because a test runner picks the working directory and the example is run
/// from the workspace root. The manifest points at the `mediapm` package, so
/// the transcripts sit at `<package>/examples/fixtures/<example>/`. They are
/// committed files: the walk below needs them in the checkout, and a missing
/// one is a failed test rather than a skipped one.
#[cfg(test)]
pub(super) fn fixture_directory(example: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/fixtures").join(example)
}

/// Read the terminal width a transcript filename names.
///
/// The name is `<stem>-width-<N>.txt`, where `N` is the `--width` the example
/// was run at. Anything else is an error naming the file, because a stray
/// `README.txt` or a `.bak` left in the directory would otherwise sit there
/// unread and quietly reduce what the screen is covered at.
///
/// A width above [`u16::MAX`] is the same kind of error: the harness takes a
/// `u16` and the number in the name has to be one it could have been run at.
#[cfg(test)]
fn fixture_width(path: &Path, stem: &str) -> u16 {
    let name = path.file_name().unwrap_or_default().to_string_lossy().into_owned();
    let parse_failure = || {
        panic!(
            "{name} is not a transcript filename: expected {stem}-width-<N>.txt, \
             where N is a terminal width the example accepts"
        )
    };
    name.strip_prefix(&format!("{stem}-width-"))
        .and_then(|rest| rest.strip_suffix(".txt"))
        .and_then(|digits| digits.parse::<u16>().ok())
        .unwrap_or_else(parse_failure)
}

/// Whether a transcript filename is the shape this matcher claims.
///
/// `<stem>-width-<N>.txt` is this matcher's shape and
/// `<stem>-<scenario>-width-<N>.txt` is the scenario axis's. A directory holds
/// both once a screen has adopted the axis and its baseline transcripts keep the
/// names they were captured with, so each matcher claims the names it can read
/// and steps over the ones it cannot. Neither steps over a name that fits
/// neither shape, so a file in the directory still cannot go unread.
#[cfg(test)]
fn names_the_size_only_shape(path: &Path, stem: &str) -> bool {
    let name = path.file_name().unwrap_or_default().to_string_lossy();
    name.starts_with(&format!("{stem}-width-")) && name.ends_with(".txt")
}

/// Render a screen at every width its committed transcripts name and compare.
///
/// The directory listing decides which widths are checked, so a new transcript
/// is covered the moment it lands and no source file has to learn about it.
/// Each transcript carries the one trailing newline that `main`'s `println!`
/// adds, which is why the comparison strips it before matching the grid.
///
/// The comparison is [`assert_frames_match`], which holds every column to an
/// exact match except the spinner column. That column advances with wall-clock
/// spacing between draws rather than with anything the layout says, so pinning
/// it made the transcripts a record of the machine that captured them. The grid
/// arrives with its escape sequences still in it, because the comparison strips
/// them itself.
#[cfg(test)]
pub fn assert_every_transcript_matches(
    example: &str,
    stem: &str,
    height: u16,
    render: impl Fn(ScreenConfig) -> String,
) {
    let directory = fixture_directory(example);
    let transcripts: Vec<PathBuf> = read_dir_sorted(&directory)
        .into_iter()
        .filter(|path| names_the_size_only_shape(path, stem))
        .collect();
    assert!(
        !transcripts.is_empty(),
        "{} holds no size-only transcripts, so {example} would be covered at no width at all",
        directory.display()
    );
    for path in transcripts {
        let width = fixture_width(&path, stem);
        let recorded = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("cannot read {}: {error}", path.display()));
        let grid = render(ScreenConfig { width, height });
        let Some(without_trailing_newline) = recorded.strip_suffix('\n') else {
            panic!("{} does not end with a newline", path.display());
        };
        assert!(
            !without_trailing_newline.ends_with('\n'),
            "{} ends with a blank line, so it no longer matches stdout",
            path.display()
        );
        assert_frames_match(
            without_trailing_newline,
            &grid,
            &format!("{} does not match what {example} renders at --width {width}", path.display()),
        );
    }
}

/// Render a screen at every width in [`SWEEP_WIDTHS`] and assert that no row wraps.
///
/// The failure this exists for: a row wider than the terminal spills onto the
/// line below, where a suffix reads as a second bar. Before the width budget
/// landed, the captured grid grew an interleaved blank line per row from 40
/// columns down, because the prefix slot was sized from the seed label and
/// never shrank.
///
/// It no longer asserts that a row carries a fill. Below the fill threshold a
/// row draws no bar on purpose and the label is what a reader wants there
/// instead, so "has a bar cell" is false by design at every width below 59 on
/// tool sync and materialization and below 66 on the workflow screen, whose
/// labels are seven columns wider than the other two. Which of those applies to
/// a given width is a property of that screen's labels, not of the width, so
/// pinning one number here would be wrong for two of the three screens.
///
/// Whether a narrow row says anything is checked by
/// [`assert_narrow_rows_carry_a_label_or_a_count`] instead, because that is a
/// separate property with its own failure.
///
/// Width is counted in characters, not bytes. The spinner glyph is three bytes
/// of UTF-8 and one column, so counting bytes lets a row that visibly wraps
/// pass, which is the one thing this test is here to catch.
#[cfg(test)]
pub fn assert_no_row_wraps_over_sweep_widths(
    screen: &str,
    render: impl Fn(ScreenConfig) -> String,
) {
    for width in SWEEP_WIDTHS {
        let grid = render(ScreenConfig { width, height: DEFAULT_HEIGHT });
        for row in grid.lines() {
            assert!(
                row.chars().count() <= usize::from(width),
                "{screen} at width {width} draws a row of {} columns, which wraps: {row:?}",
                row.chars().count()
            );
        }
    }
}

/// Render a screen at every width in [`SWEEP_WIDTHS`] and assert that a row
/// never goes back to saying nothing.
///
/// A row can satisfy "does not wrap" by being empty, which is how the old
/// prefix bug hid: the label was pushed off the line, the fill shrank to its
/// four-cell floor, and both properties looked fine. A fill alone does not
/// settle it either, because below the fill threshold the bar is dropped on
/// purpose and the label is what carries the row.
///
/// A row does go empty at the very bottom of the range, and the test says so
/// rather than pretending otherwise. Below the fill crossing the bar is gone,
/// so what is left of the row is the label and the suffix, and a row that has
/// neither keeps its timing rather than rendering a lone spinner. Measured
/// over the three example screens on 2026-10-03, the widths at which at least
/// one row is empty are 8 to 11 on tool sync, where the byte row's `31M/31M`
/// tally is one column wider than the suffix slot it is given; 8 to 12 on
/// materialization, where every row's suffix carries nothing but timing, so
/// the suffix slot measures nothing and the five-column phase tag does not
/// fit what the prefix is left; and 8 alone on the workflow screen, where the
/// overall bar's tally renders as ` 4/12` and the slot at that width is four
/// columns.
///
/// So the assertion is on the shape of that band and not on its absence: once a
/// screen has a width at which every row says something, no wider width may
/// have one that does not. A row going empty again at 40 after being readable
/// at 20 would be the bug this catches.
#[cfg(test)]
pub fn assert_narrow_rows_carry_a_label_or_a_count(
    screen: &str,
    render: impl Fn(ScreenConfig) -> String,
) {
    let mut seen_readable_width = false;
    for width in SWEEP_WIDTHS {
        let grid = render(ScreenConfig { width, height: DEFAULT_HEIGHT });
        let empty: Vec<&str> = grid.lines().filter(|row| !says_anything(row)).collect();
        if empty.is_empty() {
            seen_readable_width = true;
            continue;
        }
        assert!(
            !seen_readable_width,
            "{screen} at width {width} draws {empty:?}, but every row of this screen already \
             read at a narrower width"
        );
    }
}

/// Whether a rendered row says anything beyond its spinner.
///
/// A fill cell counts, because a bar is a row's own report of how far along it
/// is. So does any text past the spinner glyph, which is the label, or the
/// tally or the timing the row spends the columns it would have spent on a bar
/// on. The spinner is skipped rather than trimmed so the glyph itself can never
/// be the answer.
#[cfg(test)]
fn says_anything(row: &str) -> bool {
    if BAR_CELLS.iter().any(|cell| row.contains(*cell)) {
        return true;
    }
    row.chars().skip(1).collect::<String>().trim().chars().count() > 1
}

/// The two fill characters a bar draws.
///
/// Every progress style fills with `█` for the done part and `░` for the rest,
/// so a row carrying neither has no bar on it however well it fits. The two
/// glyphs are three bytes of UTF-8 each and one column, which is why the sweep
/// counts columns and not bytes.
#[cfg(test)]
const BAR_CELLS: [char; 2] = ['█', '░'];

/// Every regular file in a directory, sorted by path.
///
/// Sorting is what makes a failure report the same width first on a second
/// run; the filesystem's own order is not stable across machines.
#[cfg(test)]
pub(super) fn read_dir_sorted(directory: &Path) -> Vec<PathBuf> {
    let entries = std::fs::read_dir(directory)
        .unwrap_or_else(|error| panic!("cannot list {}: {error}", directory.display()));
    let mut paths: Vec<PathBuf> = entries
        .map(|entry| {
            entry.unwrap_or_else(|error| panic!("{}: {error}", directory.display())).path()
        })
        .filter(|path| path.is_file())
        .collect();
    paths.sort();
    paths
}
