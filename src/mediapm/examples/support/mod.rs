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
//! Each screen's renderer lives in its own sibling file, `tool_sync.rs` or
//! `workflow.rs`, and each example declares only the one it needs. Compiling
//! every renderer into every example binary would leave the unused ones dead,
//! which `warnings = "deny"` turns into a build failure.
//!
//! The files under `examples/fixtures/` hold the transcripts these renderers
//! produce, one per width.

use std::fmt;
use std::sync::Arc;

use indicatif::InMemoryTerm;
use mediapm_utils::progress::{
    DimensionSource, ProgressTerminal, TestDimensionSource, TestTimeSource, TimeSource,
};

/// Terminal width used when `--width` is absent.
///
/// 80, not the `W` in `mediapm-utils/tests/progress_output/common.rs`. That
/// suite constant is 40, but its labels are short (`worker-a`), while a real
/// tool-sync label is `ffmpeg v7.1 [res]`. The renderer clamps the prefix to
/// [`MAX_PREFIX_WIDTH`](MAX_PREFIX_WIDTH) and the suffix to
/// [`MAX_SUFFIX_WIDTH`](MAX_SUFFIX_WIDTH) without subtracting the terminal
/// width, so prefix plus suffix can exceed 40 on its own and the bar gets no
/// fill. 80 is the narrowest width where the tool-sync screen renders without
/// wrapping.
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
/// The renderer reserves at most `MAX_PREFIX_WIDTH` (40) for the prefix and
/// `MAX_SUFFIX_WIDTH` (65) for the suffix, so past a few hundred columns the
/// extra width is empty space on every line.
const MAX_WIDTH: u16 = 500;

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
