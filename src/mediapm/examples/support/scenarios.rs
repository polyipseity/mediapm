//! The scenario axis: three shapes per screen, and transcripts named for both.
//!
//! A capture picks a shape with `--scenario` and writes a transcript named
//! `<stem>-<scenario>-width-<N>.txt`. The transcript test reads the scenario and
//! the width back out of that name and re-renders with both, so a file captured
//! in one scenario cannot pass against another. The three names are the same on
//! every screen, which is what keeps a fixture directory reading the same way
//! whichever example wrote it.
//!
//! # Why this is a separate module
//!
//! `support/mod.rs` compiles into all three example binaries, and each of those
//! builds under `deny(warnings)`, which makes an item only one of them calls read
//! as dead code in the other two. An example opts into the scenario axis by
//! declaring this module, so a screen that has not moved yet keeps compiling
//! against the width-only matcher in `support/mod.rs` and nothing sits there
//! unused.
//!
//! # Height
//!
//! A scenario declares the bars it draws and the height follows from that count,
//! so there is no height to remember per scenario and a scenario that gains a row
//! cannot leave its transcripts captured at a height its content outgrew.
//! [`Scenario::height`] says why that count is one row short of what the terminal
//! needs, and [`Scenario::render_at`] is what refuses to draw a frame that lost a
//! row to the shortfall.

use std::fmt;

#[cfg(test)]
use std::path::{Path, PathBuf};

#[cfg(test)]
use mediapm_utils::progress::assert_frames_match;

use crate::support::ScreenConfig;

/// Flag that picks which of a screen's scenarios to draw.
const SCENARIO_FLAG: &str = "--scenario";

/// Which of a screen's scenarios to draw.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScenarioName {
    /// The screen as it shipped, renamed. It exists to show that nothing else
    /// moved, so its transcripts are the ones committed before the axis existed.
    Baseline,
    /// Enough bars to fill the terminal, so a transcript covers many rows at once
    /// rather than the handful the baseline fits.
    Dense,
    /// The rows the other two never draw: a warning, a failure, a sub-bar under a
    /// row still running, and labels long enough to clip.
    States,
}

impl ScenarioName {
    /// Every scenario name, in the order they read.
    pub const ALL: [ScenarioName; 3] = [Self::Baseline, Self::Dense, Self::States];

    /// The name as a transcript filename and `--scenario` spell it.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Baseline => "baseline",
            Self::Dense => "dense",
            Self::States => "states",
        }
    }

    /// Read a scenario name, or `None` when the text names no scenario.
    #[must_use]
    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|scenario| scenario.as_str() == name)
    }
}

impl fmt::Display for ScenarioName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Every scenario name as one comma-separated string.
///
/// Built from [`ScenarioName::ALL`] so the list a human is shown on a bad
/// `--scenario` cannot drift from the names the parser accepts.
fn scenario_list() -> String {
    ScenarioName::ALL.map(ScenarioName::as_str).join(", ")
}

/// Why a set of command-line arguments did not name a scenario.
///
/// Separate from [`crate::support::ScreenConfigError`] so the size flags keep
/// the messages they have always had. A rejected scenario needs a list rather
/// than a number, and the size parser has no list to give.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScenarioError {
    /// `--scenario` was the last argument, so it had no value to parse.
    MissingValue,
    /// `--scenario` named a scenario that does not exist.
    Unknown {
        /// The value as supplied.
        value: String,
    },
}

impl fmt::Display for ScenarioError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingValue => write!(f, "{SCENARIO_FLAG} needs a value"),
            Self::Unknown { value } => {
                write!(f, "{SCENARIO_FLAG} must be one of {}, got {value:?}", scenario_list())
            }
        }
    }
}

impl std::error::Error for ScenarioError {}

/// One shape of a screen, with the bars that shape puts on screen.
///
/// The terminal height is derived from `bars` rather than chosen per scenario. A
/// height written beside each scenario would be a second number to keep in step
/// with the first and nothing checks it, and the first number is the one that
/// changes whenever a scenario gains a row.
#[derive(Debug, Clone, Copy)]
pub struct Scenario {
    /// Name the transcript filenames and `--scenario` carry.
    name: ScenarioName,
    /// Bars the scenario draws, the pinned overall bar included.
    bars: u16,
    /// Renderer this scenario drives.
    render: fn(ScreenConfig) -> String,
}

impl Scenario {
    /// Pair a scenario name with the bars it draws and the renderer that draws
    /// them.
    ///
    /// `bars` is a claim the scenario then has to keep. [`Self::render_at`]
    /// refuses to hand back a frame that lost a row, so a scenario that grows is
    /// told to move this number rather than silently dropping its first row.
    #[must_use]
    pub const fn new(name: ScenarioName, bars: u16, render: fn(ScreenConfig) -> String) -> Self {
        Self { name, bars, render }
    }

    /// The name this scenario is captured under.
    #[must_use]
    pub const fn name(&self) -> ScenarioName {
        self.name
    }

    /// Bars this scenario draws, the pinned overall bar included.
    #[must_use]
    pub const fn bars(&self) -> u16 {
        self.bars
    }

    /// Terminal rows this scenario needs, derived from its own bar count.
    ///
    /// The renderer draws one line per bar, and committing the frame advances the
    /// cursor past it, which scrolls the top row away when the frame already fills
    /// the terminal. A seven-bar screen at seven rows comes back with six rows and
    /// nothing saying why, so the frame is given a row it does not draw.
    #[must_use]
    pub const fn height(&self) -> u16 {
        self.bars + 1
    }

    /// Draw this scenario at an explicit size, with no bar-count claim checked.
    ///
    /// This is what a width sweep and an explicit `--height` want: a screen drawn
    /// somewhere other than the height its content asks for, on purpose. Anything
    /// a transcript is captured from goes through [`Self::render_at`].
    pub fn render(&self, config: ScreenConfig) -> String {
        (self.render)(config)
    }

    /// Draw this scenario at `width`, at the height its bar count derives.
    ///
    /// The bar count is checked from both sides, because the two failures read
    /// the same from the outside. A count larger than what the scenario draws
    /// leaves the grid a line short, which is visible. A count smaller than that
    /// loses the extra row to a terminal one row too short and the grid comes
    /// back with the right number of lines, because the row is simply not there.
    /// That is the case a second draw catches: a frame that moves with the height
    /// is one whose height was doing something.
    ///
    /// # Panics
    ///
    /// Panics when the drawn grid does not have one line per declared bar, or
    /// when the grid differs from the one a taller terminal draws.
    pub fn render_at(&self, width: u16) -> String {
        let grid = self.render(ScreenConfig { width, height: self.height() });
        // Twice the derived height is more than any scenario needs, and it is a
        // multiple of the scenario's own count rather than a number written
        // beside it.
        let roomier = self.render(ScreenConfig { width, height: self.height() * 2 });
        let drawn = grid.lines().count();
        assert_eq!(
            drawn,
            usize::from(self.bars),
            "the {} scenario declares {} bars and draws {drawn} lines at {} rows",
            self.name(),
            self.bars(),
            self.height()
        );
        assert_eq!(
            grid,
            roomier,
            "the {} scenario draws a different frame at {} rows than it does at {}, so its \
             {} bars are not what it draws",
            self.name(),
            self.height(),
            self.height() * 2,
            self.bars()
        );
        grid
    }
}

/// Split `--scenario <name>` off an argument list, leaving the size flags behind.
///
/// The scenario comes off first so the size parser can keep the shape and the
/// messages it has always had. Everything the scenario flag did not consume is
/// passed through untouched, including a flag the size parser does not know, so
/// the size parser is the one that reports it.
///
/// An argument list with no `--scenario` reads as [`ScenarioName::Baseline`], and
/// a second one wins.
///
/// # Errors
///
/// Returns [`ScenarioError::MissingValue`] when `--scenario` was the last
/// argument, and [`ScenarioError::Unknown`] when its value names no scenario. The
/// message lists the names that exist, because the person who typed it is the one
/// who has to pick the right one; falling back to a default would answer with a
/// screen nobody asked for and leave the mistake to be found in a transcript
/// months later.
pub fn split_scenario_flag<I>(args: I) -> Result<(ScenarioName, Vec<String>), ScenarioError>
where
    I: IntoIterator<Item = String>,
{
    let args: Vec<String> = args.into_iter().collect();
    let mut remaining = Vec::with_capacity(args.len());
    let mut scenario = ScenarioName::Baseline;
    let mut args = args.into_iter();
    while let Some(argument) = args.next() {
        if argument != SCENARIO_FLAG {
            remaining.push(argument);
            continue;
        }
        let value = args.next().ok_or(ScenarioError::MissingValue)?;
        scenario = ScenarioName::parse(&value).ok_or(ScenarioError::Unknown { value })?;
    }
    Ok((scenario, remaining))
}

/// Read the scenario and width a transcript filename names.
///
/// The name is `<stem>-<scenario>-width-<N>.txt`, where `<scenario>` is one of
/// [`ScenarioName::ALL`] and `N` is the `--width` the example was run at.
///
/// A name with no scenario segment, `<stem>-width-<N>.txt`, is the shape these
/// files carried before the axis existed and reads as [`ScenarioName::Baseline`].
/// That is not a fallback for a mistyped name: a segment that names no scenario
/// is an error, because a typo there would quietly drop a whole scenario's
/// coverage without anything going red for the right reason.
///
/// Anything else is an error naming the file, so a stray `README.txt` or a `.bak`
/// left in the directory fails the test instead of sitting there unread. A width
/// above [`u16::MAX`] is the same kind of error: the harness takes a `u16` and
/// the number in the name has to be one it could have been run at.
#[cfg(test)]
fn fixture_transcript(path: &Path, stem: &str) -> (ScenarioName, u16) {
    let name = path.file_name().unwrap_or_default().to_string_lossy().into_owned();
    let parse_failure = || -> ! {
        panic!(
            "{name} is not a transcript filename: expected \
             {stem}-<scenario>-width-<N>.txt or {stem}-width-<N>.txt, where <scenario> is one \
             of {} and N is a terminal width the example accepts",
            scenario_list()
        )
    };
    let rest = name
        .strip_prefix(&format!("{stem}-"))
        .and_then(|rest| rest.strip_suffix(".txt"))
        .unwrap_or_else(|| parse_failure());
    let (scenario_name, digits) = match rest.split_once("-width-") {
        Some((scenario, digits)) => (scenario, digits),
        None => (
            ScenarioName::Baseline.as_str(),
            rest.strip_prefix("width-").unwrap_or_else(|| parse_failure()),
        ),
    };
    let scenario = ScenarioName::parse(scenario_name).unwrap_or_else(|| {
        panic!("{name} names scenario {scenario_name:?}, which is not one of {}", scenario_list())
    });
    let width = digits.parse::<u16>().unwrap_or_else(|_| parse_failure());
    (scenario, width)
}

/// Whether a transcript filename names a scenario.
///
/// `<stem>-<scenario>-width-<N>.txt` is the axis's shape and `<stem>-width-<N>.txt`
/// is the width-only matcher's in `support/mod.rs`. A directory holds both once a
/// screen has adopted the axis and its baseline transcripts keep the names they
/// were captured with, so each matcher claims the names it can read and steps over
/// the ones it cannot.
#[cfg(test)]
fn names_the_scenario_shape(path: &Path, stem: &str) -> bool {
    let name = path.file_name().unwrap_or_default().to_string_lossy();
    name.starts_with(&format!("{stem}-"))
        && name.contains("-width-")
        && !name.starts_with(&format!("{stem}-width-"))
}

/// Compare one rendered grid against one committed transcript.
///
/// The transcript carries the single trailing newline `main`'s `println!` adds, so
/// it is stripped before the comparison, and a file carrying a blank line at the
/// end no longer matches what stdout writes.
///
/// The comparison is [`assert_frames_match`] rather than an equality check, so
/// which glyph the spinner column ended on is animation phase rather than
/// recorded output. Every other column is compared exactly. The grid arrives
/// with its escape sequences still in it, because the comparison strips them
/// itself.
#[cfg(test)]
fn assert_transcript_matches(path: &Path, grid: &str, drawn_at: &str) {
    let recorded = std::fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("cannot read {}: {error}", path.display()));
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
        grid,
        &format!("{} does not match what the screen renders at {drawn_at}", path.display()),
    );
}

/// Render every scenario and width a screen's transcripts name and compare.
///
/// The directory listing decides which transcripts are checked, so a new one is
/// covered the moment it lands and no source file has to learn about it. Names
/// that carry no scenario segment belong to the width-only matcher in
/// `support/mod.rs` and are stepped over here; a name that fits neither shape
/// fails in both, so nothing in the directory can go unread.
#[cfg(test)]
pub fn assert_every_scenario_transcript_matches(
    example: &str,
    stem: &str,
    render: impl Fn(ScenarioName, u16) -> String,
) {
    let directory = crate::support::fixture_directory(example);
    let transcripts: Vec<PathBuf> = crate::support::read_dir_sorted(&directory)
        .into_iter()
        .filter(|path| names_the_scenario_shape(path, stem))
        .collect();
    assert!(
        !transcripts.is_empty(),
        "{} holds no transcripts naming a scenario, so {example} would be covered at no \
         scenario and no width at all",
        directory.display()
    );
    for path in transcripts {
        let (scenario, width) = fixture_transcript(&path, stem);
        let grid = render(scenario, width);
        assert_transcript_matches(
            &path,
            &grid,
            &format!("{SCENARIO_FLAG} {scenario} --width {width}"),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Scenario, ScenarioError, ScenarioName, fixture_transcript, names_the_scenario_shape,
        scenario_list, split_scenario_flag,
    };
    use crate::support::{ScreenConfig, parse_screen_config};
    use std::path::Path;

    /// A scenario name round-trips through the text a filename carries.
    ///
    /// The transcript test cannot work if a name `--scenario` accepts is a name
    /// the filename parser rejects, so the two are checked against each other
    /// rather than each against its own table.
    #[test]
    fn scenario_names_round_trip_through_their_text() {
        for scenario in ScenarioName::ALL {
            assert_eq!(ScenarioName::parse(scenario.as_str()), Some(scenario));
        }
        assert_eq!(scenario_list(), "baseline, dense, states");
    }

    /// A transcript name carries the scenario and the width it was captured at.
    #[test]
    fn a_transcript_name_yields_the_scenario_and_width_it_names() {
        assert_eq!(
            fixture_transcript(Path::new("tool-sync-dense-width-72.txt"), "tool-sync"),
            (ScenarioName::Dense, 72)
        );
    }

    /// A name with no scenario segment is the shape that predates the axis, so it
    /// reads as the baseline rather than going unread.
    ///
    /// A missing segment is the baseline; a segment that names something else is
    /// an error, because a typo there would quietly drop a whole scenario's
    /// coverage.
    #[test]
    fn a_transcript_name_without_a_scenario_reads_as_the_baseline() {
        assert_eq!(
            fixture_transcript(Path::new("tool-sync-width-40.txt"), "tool-sync"),
            (ScenarioName::Baseline, 40)
        );
    }

    /// The two matchers between them claim every name in a directory.
    ///
    /// Each steps over the shape the other reads, so a file both of them step
    /// over is a file neither checked. This pins that the two shapes partition
    /// the names rather than overlap.
    #[test]
    fn each_matcher_claims_exactly_one_name_shape() {
        let named = Path::new("tool-sync-dense-width-20.txt");
        let width_only = Path::new("tool-sync-width-20.txt");
        assert!(names_the_scenario_shape(named, "tool-sync"));
        assert!(!names_the_scenario_shape(width_only, "tool-sync"));
    }

    /// Splitting the scenario off leaves the size flags for the size parser.
    #[test]
    fn splitting_the_scenario_off_leaves_the_size_flags_alone() {
        let args: Vec<String> =
            ["--scenario", "dense", "--width", "50", "--height", "21"].map(str::to_string).into();
        let (scenario, remaining) =
            split_scenario_flag(args).unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(scenario, ScenarioName::Dense);
        assert_eq!(
            parse_screen_config(remaining).map(|config| (config.width, config.height)),
            Ok((50, 21))
        );
    }

    /// An absent scenario is the baseline, and a rejected one says what the
    /// choices are.
    #[test]
    fn an_absent_scenario_is_the_baseline_and_a_rejected_one_lists_them() {
        assert_eq!(
            split_scenario_flag(Vec::new()).map(|(scenario, rest)| (scenario, rest.len())),
            Ok((ScenarioName::Baseline, 0))
        );
        assert_eq!(
            split_scenario_flag(["--scenario".to_string(), "wide".to_string()]),
            Err(ScenarioError::Unknown { value: "wide".to_string() })
        );
        assert_eq!(
            reject(&["--scenario", "wide"]),
            "--scenario must be one of baseline, dense, states, got \"wide\""
        );
        assert_eq!(reject(&["--scenario"]), "--scenario needs a value");
    }

    /// A scenario's height follows from the bars it draws, not from a number
    /// written beside it.
    ///
    /// The row the commit advances past is the whole of the `+1`, so a seven-bar
    /// scenario needs eight rows.
    #[test]
    fn a_scenario_draws_one_row_more_than_its_bar_count() {
        let scenario = Scenario::new(ScenarioName::Baseline, 7, |_| String::new());
        assert_eq!(scenario.bars(), 7);
        assert_eq!(scenario.height(), 8);
    }

    /// A renderer whose frame carries the terminal height into its text.
    ///
    /// Two rows either way, so the bar-count check passes and the height
    /// comparison is the one left to catch it.
    fn height_dependent_render(config: ScreenConfig) -> String {
        format!("row one at {}\nrow two at {}", config.height, config.height)
    }

    /// `render_at` refuses a frame that moves with the terminal height.
    ///
    /// Nothing else in the suite draws a scenario at two heights, so a renderer
    /// that grew a height-dependent row would pass every transcript and fail
    /// only on the terminal somebody happened to run it on. The check is the
    /// second draw, and this is what says so: drop it and the panic goes with
    /// it, which is the point of pinning it here.
    #[test]
    fn render_at_refuses_a_frame_that_moves_with_the_terminal_height() {
        let scenario = Scenario::new(ScenarioName::Baseline, 2, height_dependent_render);
        let refusal = panic_message(|| {
            scenario.render_at(72);
        });
        assert!(
            refusal.contains("draws a different frame at 3 rows than it does at 6"),
            "{refusal}"
        );
    }

    /// `render_at` refuses a frame with fewer rows than the scenario declares.
    ///
    /// The other direction of the same bar count, and the one that reads as a
    /// missing row rather than as an extra one.
    #[test]
    fn render_at_refuses_a_frame_shorter_than_its_declared_bars() {
        let scenario = Scenario::new(ScenarioName::Baseline, 3, height_dependent_render);
        let refusal = panic_message(|| {
            scenario.render_at(72);
        });
        assert!(refusal.contains("declares 3 bars and draws 2 lines at 4 rows"), "{refusal}");
    }

    /// A frame that holds still across heights is handed back as it was drawn.
    ///
    /// The counterpart to the two refusals, so the checks cannot pass by
    /// refusing everything.
    #[test]
    fn render_at_hands_back_a_frame_that_does_not_move_with_the_height() {
        let scenario = Scenario::new(ScenarioName::Baseline, 2, |_| "one\ntwo".to_string());
        assert_eq!(scenario.render_at(72), "one\ntwo");
    }

    /// The message a refusal carries, so a test can read what it was told.
    ///
    /// The comparison fails by panicking, which is what a caller sees. Reading
    /// the panic back rather than marking each test `#[should_panic]` lets a
    /// test check that the right check fired rather than any check at all.
    fn panic_message(body: impl FnOnce()) -> String {
        let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(body))
            .err()
            .unwrap_or_else(|| panic!("the check did not fire"));
        match payload.downcast::<String>() {
            Ok(message) => *message,
            Err(payload) => match payload.downcast::<&str>() {
                Ok(message) => (*message).to_string(),
                Err(_) => "the check panicked without a message".to_string(),
            },
        }
    }

    /// The rendered message for an argument list the harness refuses.
    fn reject(args: &[&str]) -> String {
        let owned: Vec<String> = args.iter().map(|arg| (*arg).to_string()).collect();
        match split_scenario_flag(owned) {
            Ok(parsed) => panic!("{args:?} should have been rejected, got {parsed:?}"),
            Err(error) => error.to_string(),
        }
    }
}
