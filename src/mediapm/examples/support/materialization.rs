//! Screen C: the materialization progress screen.
//!
//! Renders what `sync_hierarchy` puts on the terminal, using the captured
//! terminal and synthetic clock the shared harness in `support/mod.rs` builds.
//! The example that pulls this module in is `mediapm_progress_materialize`.
//!
//! The seeds are quoted from the materializer rather than chosen, because the
//! renderer measures each bar's seed to size the prefix slot the whole screen
//! shares, so a seed longer than production would widen that slot and draw
//! prefixes a live sync never draws. There are three distinct seeds:
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
//! variant name, so the shared prefix slot is clamped to the renderer's 40
//! column ceiling (`MAX_PREFIX_WIDTH` in
//! `src/mediapm-utils/src/progress/inner/components.rs:556`) and the rows are
//! 40 columns of prefix whatever the terminal width is. The client gets 36 of
//! those after the renderer's four byte reset. Only the labels that fit
//! survive, and under pressure the elastic directory path is shortened from
//! its head. A folder name longer than the whole 36 column budget is not
//! elastic, so it is dropped whole and the row is left with the bare `[stg]` or
//! `[wrt]` tag and no identity at all. The online demo's media folder is named
//! that long, so it is the ordinary case for a real music library rather than
//! an exotic one, and
//! `materialization_screen_drops_a_folder_name_wider_than_the_budget` in
//! `mediapm_progress_materialize.rs` pins the frame it renders.
//!
//! No row on this screen carries a suffix. [`MaterializationBarLabel`] returns
//! an empty string from `truncate_suffix` unconditionally
//! (`src/mediapm/src/materializer/progress_labels.rs:68-71`), and a bar with
//! client truncation installed has its suffix replaced by that return value
//! rather than supplemented by it, so the auto-derived elapsed, rate, and eta
//! columns never reach the terminal here. The conductor's labels pass the same
//! suffix through `fit_segments`, which is why the workflow screen shows
//! elapsed time and this one does not.
//!
//! No row carries an `[F]` or `[W]` marker either. Nothing outside the label's
//! own unit test writes `status_marker`: `finish_warning` at
//! `src/mediapm/src/materializer/mod.rs:525` and `finish_error` at `:557` and
//! `:573` set a bar's terminal state and stop there, so the status segment the
//! label builds for a non-empty marker never reaches a screen.
//!
//! The label is the library's own [`MaterializationBarLabel`], re-exported
//! from the `mediapm` crate root, and the path split is the library's
//! [`split_entry_path`]. The example therefore drives the real truncation
//! order: a reorder in
//! `src/mediapm/src/materializer/progress_labels.rs` changes these grids
//! rather than passing unnoticed against a local copy of the field list.

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
/// Chosen short enough that `entry_name` fits the 36 column client budget, so
/// the folder row reads like a normal frame. The online demo names the same
/// kind of folder wider than that, which is what
/// `render_materialization_screen_for_folder` exists to render.
const READABLE_FOLDER_ENTRY: &str = "Music/Rick Astley/Never Gonna Give You Up";

/// Install the label the materializer installs for a phase.
///
/// `prepare_hierarchy_entry` splits the relative path once with
/// [`split_entry_path`] at `src/mediapm/src/materializer/mod.rs:396` and
/// reuses the two halves for every phase it transitions through, so this takes
/// the same pair rather than splitting again. `status_marker` is left at its
/// default because no materializer call site sets it: `finish_warning` at
/// `mod.rs:525` and `finish_error` at `mod.rs:557` and `:573` change the bar's
/// terminal state without ever installing a `[F]` or `[W]` label, so that
/// segment never reaches a materialization screen.
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

    // `entry_name` is a keep segment, so a folder name wider than the 36 column
    // client budget is dropped whole rather than shortened and the row renders
    // as a bare `[stg]`. The default seed stays inside the budget for that
    // reason; `materialization_screen_drops_a_folder_name_wider_than_the_budget`
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
