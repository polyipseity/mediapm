//! Screen C: the materialization progress screen.
//!
//! Renders what `sync_hierarchy` puts on the terminal, using the captured
//! terminal and synthetic clock the shared harness in `support/mod.rs` builds.
//! The example that pulls this module in is `mediapm_progress_materialize`.
//!
//! The seeds are quoted from the materializer rather than chosen, so the rows
//! here are the rows a live sync draws. There are three distinct seeds:
//!
//! - `materializing [mat]` on the overall bar, created by
//!   `with_overall` at `src/mediapm/src/service.rs:1349` and quoted by the
//!   recorder tests at `src/mediapm/src/materializer/mod.rs:1291`.
//! - `{relative_path} [stg]` on a per-entry bar, from
//!   `src/mediapm/src/materializer/mod.rs:398`.
//! - `{variant_name} [wrt]` on a per-extracted-file sub-bar, from
//!   `src/mediapm/src/materializer/mod.rs:746`.
//!
//! `materializing [mat]` is nineteen columns and the overall bar is the only
//! bar whose seed stays that short. Every other seed carries a path or a
//! variant name. The shared prefix slot is what the terminal has left after the
//! spinner, the separators, the suffix and a fill floor, and under it the
//! elastic path halves shorten from the head, so a folder name wider than the
//! whole budget shortens too rather than being dropped whole. The online
//! demo's media folder is named that long, so it is the ordinary case for a
//! real music library rather than an exotic one, and
//! `materialization_screen_keeps_the_tail_of_a_folder_name_wider_than_the_budget`
//! in `mediapm_progress_materialize.rs` pins the frame it renders.
//!
//! Every row carries the auto-derived timing the renderer passes in. The
//! client-truncation path replaces a bar's suffix with what
//! [`MaterializationBarLabel::truncate_suffix`] returns rather than
//! supplementing it, so that method has to read `elapsed`, `rate`, and `eta`
//! out of the merged `SuffixComponents` or the timing never reaches the
//! terminal. The conductor's labels pass the same suffix through
//! `fit_segments`, and so does this one.
//!
//! No row here carries an `[F]` or `[W]` marker, because nothing in this
//! screen finishes with a warning or an error. The materializer does install
//! one on those finishes, at the three call sites that used to change only a
//! bar's colour; this screen just takes neither path.
//!
//! The label is the library's own [`MaterializationBarLabel`], re-exported
//! from the `mediapm` crate root, and the path split is the library's
//! [`split_entry_path`]. The example therefore drives the real truncation
//! order: a reorder in
//! `src/mediapm/src/materializer/progress_labels.rs` changes these grids
//! rather than passing unnoticed against a local copy of the field list.
//!
//! Where the labels give way, measured on 2026-10-03 by rendering this screen
//! at every width the harness accepts: 27 columns is the narrowest at which
//! every row still draws its phase tag, and below it the prefix slot is empty
//! and each row is the spinner, its fill and its suffix. The active rows
//! reserve `6s 0/d`, the fill keeps its floor, and `[cmt]` is the only segment
//! short enough to survive what is left. Those are facts about the seed paths
//! quoted above, not a contract, and they move when a path changes length.
//! Nothing in the suite is named after them, because a fixture called for one
//! of those widths would go on testing that width after the seed it was
//! measured against had moved, and its name would be the only thing left still
//! claiming the number mattered.

use std::sync::Arc;
use std::time::Duration;

use mediapm::{MaterializationBarLabel, split_entry_path};

use crate::support::{ScreenConfig, capture_terminal};

/// A per-entry bar's three phases, from `add_bar(3, ...)` at
/// `src/mediapm/src/materializer/mod.rs:398`.
const ENTRY_PHASES: u64 = 3;

/// Extracted members the folder variant in this demo unpacks into. The sub-bar
/// totals whatever `extract_zip_folder_variant_bytes` returned, and the
/// `links` variant of the online demo carries three link files
/// (`.url`, `.webloc`, `.desktop`).
const LINK_VARIANT_MEMBERS: u64 = 3;

/// Media folder entry the screen renders, as a path relative to the library
/// root.
///
/// The basename is twenty-three columns, which fits the client budget on its
/// own, so the folder row reads like a normal frame. The online demo names the
/// same kind of folder wider than that, which is what
/// `render_materialization_screen_for_folder` exists to render.
const READABLE_FOLDER_ENTRY: &str = "Music/Rick Astley/Never Gonna Give You Up";

/// Install the label the materializer installs for a phase.
///
/// `prepare_hierarchy_entry` splits the relative path once with
/// [`split_entry_path`] at `src/mediapm/src/materializer/mod.rs:396` and
/// reuses the two halves for every phase it transitions through, so this takes
/// the same pair rather than splitting again. `status_marker` stays at its
/// default because no bar in this screen finishes with a warning or an error;
/// the materializer sets the marker at the three finishes that do, through
/// `mark_entry_bar_finished`.
fn phase_label(entry_path: &str, entry_name: &str, phase: &str) -> Arc<MaterializationBarLabel> {
    Arc::new(MaterializationBarLabel {
        entry_path: entry_path.to_string(),
        entry_name: entry_name.to_string(),
        phase: phase.to_string(),
        ..Default::default()
    })
}

/// Render the materialization screen at `config`'s size and return the raw grid.
///
/// The folder entry is `READABLE_FOLDER_ENTRY`. Pass another path to
/// `render_materialization_screen_for_folder` to see what a wider name does to
/// the row.
///
/// Three entries stand in for a sync. The first is a single media file that
/// runs stage, verify, and commit to completion, so its row carries the `[cmt]`
/// tag the media arm installs last and is drawn finished. The second is a
/// media folder holding a ZIP variant, whose writing shows up on a `[wrt]`
/// sub-bar underneath. The third is a media file partway through verify.
///
/// The folder row staying on `[stg]` is the faithful render, not a slip here.
/// Only the media arm calls `set_truncation` again after the initial `[stg]`
/// (`src/mediapm/src/materializer/mod.rs:431` and `:442`), so a media folder and
/// a playlist entry keep the phase they were created with for their whole run.
///
/// The grid keeps its ANSI colour escapes, the same bytes the terminal received.
/// Pass the result through [`strip_ansi_escapes`](crate::support::strip_ansi_escapes)
/// for a readable transcript.
#[must_use]
pub fn render_materialization_screen(config: ScreenConfig) -> String {
    render_materialization_screen_for(config, READABLE_FOLDER_ENTRY)
}

/// Render the same screen with the media folder entry named by `folder_entry`.
///
/// Test-only seam over `render_materialization_screen_for`. The two build one
/// screen and differ in one string, so pinning the over-budget case here reuses
/// the screen the fixtures already record instead of a second, near-identical
/// renderer that could drift from it.
#[cfg(test)]
#[must_use]
pub fn render_materialization_screen_for_folder(
    config: ScreenConfig,
    folder_entry: &str,
) -> String {
    render_materialization_screen_for(config, folder_entry)
}

/// Build the screen, with the media folder entry named by `folder_entry`.
fn render_materialization_screen_for(config: ScreenConfig, folder_entry: &str) -> String {
    /// Entries in the synthetic library, which is what the overall bar totals.
    const ENTRY_COUNT: u64 = 3;
    /// Seed the overall bar carries, from `src/mediapm/src/service.rs:1349`.
    const OVERALL_SEED: &str = "materializing [mat]";
    /// Variant whose ZIP members the folder entry unpacks. The name doubles as
    /// the sub-bar's seed and its `file_name` segment, as at
    /// `src/mediapm/src/materializer/mod.rs:746`.
    const LINK_VARIANT: &str = "links";

    // A folder name wider than the client budget shortens from the front rather
    // than being dropped whole, so both the name's tail and the directory's
    // tail survive. The default seed stays inside the budget;
    // `materialization_screen_keeps_the_tail_of_a_folder_name_wider_than_the_budget`
    // renders the online demo's name, which does not.
    let (folder_path, folder_name) = split_entry_path(folder_entry);
    let (album_path, album_name) = split_entry_path(
        "Music/Boards of Canada/Music Has the Right to Children/01 - Telepathy.flac",
    );
    let (single_path, single_name) =
        split_entry_path("Music/Pink Floyd/The Wall/01 - In the Flesh?.m4a");

    let (terminal, grid, clock) = capture_terminal(config);
    let (screen, overall) = terminal.screen().with_overall(OVERALL_SEED, 1).build();
    // `sync_hierarchy` replaces the placeholder total with the entry count and
    // installs the `[mat]` label on the caller's own handle, at
    // `src/mediapm/src/materializer/mod.rs:194` and `:196`.
    overall.set_total(ENTRY_COUNT);
    overall.set_truncation(Arc::new(MaterializationBarLabel {
        entry_name: "materializing".to_string(),
        phase: "mat".to_string(),
        ..Default::default()
    }));

    // The first entry is a media file, so it is the one row that walks the full
    // `[stg]` to `[vrf]` to `[cmt]` sequence.
    let finished = screen.add_bar(ENTRY_PHASES, &format!("{album_name} [stg]"));
    finished.set_truncation(phase_label(album_path, album_name, "stg"));
    screen.tick();
    clock.advance(Duration::from_secs(2));

    finished.set_truncation(phase_label(album_path, album_name, "vrf"));
    finished.advance(1);
    screen.tick();
    clock.advance(Duration::from_secs(1));

    finished.set_truncation(phase_label(album_path, album_name, "cmt"));
    finished.advance(1);
    screen.tick();
    clock.advance(Duration::from_secs(3));

    finished.advance(1);
    finished.finish_success();
    overall.advance(1);
    screen.tick();
    clock.advance(Duration::from_secs(1));

    // The second entry is a media folder. Its own bar keeps the `[stg]` label
    // it was created with, and the per-extracted-file sub-bar opens underneath
    // it once the folder arm reaches the variant it unpacks.
    let folder = screen.add_bar(ENTRY_PHASES, &format!("{folder_name} [stg]"));
    folder.set_truncation(phase_label(folder_path, folder_name, "stg"));
    screen.tick();
    clock.advance(Duration::from_secs(2));

    let write = screen.add_bar(LINK_VARIANT_MEMBERS, &format!("{LINK_VARIANT} [wrt]"));
    write.set_truncation(Arc::new(MaterializationBarLabel {
        entry_path: folder_path.to_string(),
        entry_name: folder_name.to_string(),
        file_name: LINK_VARIANT.to_string(),
        phase: "wrt".to_string(),
        ..Default::default()
    }));
    write.advance(1);
    screen.tick();
    clock.advance(Duration::from_secs(4));

    // The third entry is a media file that has resolved its hash and is on the
    // verify tag, still running when the transcript is read.
    let verifying = screen.add_bar(ENTRY_PHASES, &format!("{single_name} [stg]"));
    verifying.set_truncation(phase_label(single_path, single_name, "stg"));
    verifying.advance(1);
    verifying.set_truncation(phase_label(single_path, single_name, "vrf"));
    screen.tick();
    clock.advance(Duration::from_secs(2));

    // The sync is still in flight: the folder and the third entry have not
    // reached their commit phase, so the overall bar stays active at one of
    // three entries rather than being closed out.
    overall.set_position(1);
    screen.join();
    drop(terminal);
    grid.contents()
}
