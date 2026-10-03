//! Screen C: the materialization progress screen.
//!
//! Renders what `sync_hierarchy` puts on the terminal, using the captured
//! terminal and synthetic clock the shared harness in `support/mod.rs` builds.
//! The example that pulls this module in is `mediapm_progress_materialize`.
//!
//! The seeds are quoted from the materializer rather than chosen, so the rows
//! here are the rows a live sync draws. There are three distinct seeds:
//!
//! - `materializing` on the overall bar, created by
//!   `with_overall` at `src/mediapm/src/service.rs` and quoted by the
//!   recorder tests in the materializer.
//! - `{relative_path} [stg]` on a per-entry bar, from `EntryPhaseBar::create`.
//! - `{variant_name} [wrt]` on a per-extracted-file sub-bar, from the folder
//!   arm.
//!
//! `materializing` is thirteen columns and the overall bar is the only
//! bar whose seed stays that short. Every other seed carries a path or a
//! variant name. The overall bar carries no phase tag, because the word
//! already names what the bar is doing. The shared prefix slot is what the
//! terminal has left after the spinner, the separators and the suffix, with a
//! fill floor under the bar
//! while there is room for a bar at all, and under it the elastic path halves
//! shorten from the head, so a folder name wider than the
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
//! # Scenarios
//!
//! [`SCENARIOS`] holds the three shapes this screen is captured in. Each entry
//! declares how many bars its renderer draws, and the harness derives the
//! terminal height from that count, so a scenario and its transcripts cannot
//! drift apart.
//!
//! The `states` scenario draws a warned row, a failed row and two sub-bars, so
//! what each of those rows documents is worth naming rather than leaving for a
//! reader to find in a fixture.
//!
//! The `[W]` row is what a live sync reaches. A media entry whose variant
//! resolved no content hash is skipped with a warning, and the bar is finished
//! on `[vrf]`, the phase the media arm installed before resolving the hash,
//! with the marker set. That is the only warning a per-entry row carries.
//!
//! The `[F]` row is reached too, on the two arms that check their result before
//! finishing: a media folder and a playlist. Both finish on `[stg]`, the
//! only phase either of them declares. Neither can be reached
//! without the overall bar failing as well, since the same error returns from
//! `prepare_hierarchy_entry`, breaks the collect loop in `sync_hierarchy`, and
//! finishes the overall bar with an error (`:280`). That is why the `states`
//! overall bar is drawn finished on error beside the failed row rather than
//! staying active. It carries no marker of its own, because nothing installs
//! one on that bar: the marker rides on the entry bars.
//!
//! What no sync reaches is a warned overall bar. The overall bar only ever
//! finishes on success or on error (`:280` and `:282`), so a `[W]` on that row
//! is left out of every scenario rather than shown as something that happens.
//! A media entry that fails outright is the other row production never draws:
//! the media arm returns its errors with `?` and never finishes the bar, so
//! that entry's row keeps drawing as active.
//!
//! The `[wrt]` sub-bars and the over-long names are ordinary. A folder entry
//! with a ZIP variant opens one sub-bar per variant while its own row is still
//! on `[stg]`, and a YouTube-style media folder name runs to fifty-nine
//! columns, which is wider than the forty the prefix slot is capped at, so the
//! elastic name has to shorten it at every width here.
//!
//! The label is the library's own [`MaterializationBarLabel`], re-exported
//! from the `mediapm` crate root, and the path split is the library's
//! [`split_entry_path`]. The example therefore drives the real truncation
//! order: a reorder in
//! `src/mediapm/src/materializer/progress_labels.rs` changes these grids
//! rather than passing unnoticed against a local copy of the field list.
//!
//! Where the labels give way, measured on 2026-10-03 by rendering the
//! `baseline` scenario at every width the harness accepts: 19 columns is the
//! narrowest at which every row still draws its phase tag. The `[wrt]` sub-bar is the row that
//! loses the tag first, from 13 columns up, because its member name outranks
//! the tag and spends the columns the tag would have had. Below 13 the prefix
//! slot is empty and each row is the spinner and nothing else. A row in that
//! band has no suffix of its own to show either, so the suffix slot measures
//! nothing and there are no columns for the timing to go in either; that band
//! is the one place on any of the three screens where a row renders as a lone
//! spinner. Above the empty band the active rows reserve `6s 0/d` and the fill
//! keeps its floor until the bar crosses back at 59. Those are facts about the
//! seed paths quoted above, not a contract, and they move when a path changes
//! length. Nothing in the suite is named after them, because a fixture called
//! for one of those widths would go on testing that width after the seed it
//! was measured against had moved, and its name would be the only thing left
//! still claiming the number mattered.

use std::sync::Arc;
use std::time::Duration;

use mediapm::{MaterializationBarLabel, MaterializationPhase, split_entry_path};
use mediapm_utils::progress::{ProgressBarHandle, ProgressScreen};

use crate::scenarios::{Scenario, ScenarioName};
use crate::support::{ScreenConfig, capture_terminal};

/// The materialization screen in each of the three shapes it is captured in.
///
/// `baseline` draws the screen as it shipped: two media entries and a media
/// folder with one sub-bar. `dense` draws a library band, with an entry of every
/// kind the materializer dispatches. `states` draws the rows the other two
/// leave out, a warned entry, a failed one, and names wide enough to clip.
pub const SCENARIOS: [Scenario; 3] = [
    Scenario::new(ScenarioName::Baseline, 5, render_materialization_screen),
    Scenario::new(ScenarioName::Dense, 10, render_dense),
    Scenario::new(ScenarioName::States, 8, render_states),
];

/// The scenario this screen draws under `name`.
///
/// The table covers every name [`ScenarioName`] has, so the match is total by
/// construction rather than by a fallback that would quietly draw the wrong
/// screen.
#[must_use]
pub fn scenario(name: ScenarioName) -> Scenario {
    match name {
        ScenarioName::Baseline => SCENARIOS[0],
        ScenarioName::Dense => SCENARIOS[1],
        ScenarioName::States => SCENARIOS[2],
    }
}

/// A per-entry bar's total, which is what `EntryPhaseBar::create` hands to
/// `add_bar` in the materializer.
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
/// The materializer splits the relative path once with [`split_entry_path`]
/// when it opens the bar and reuses the two halves for every phase it
/// transitions through, so this takes the same pair rather than splitting
/// again. `status_marker` stays at its
/// default on the phase transitions, because the marker belongs to the finish
/// and not to the phase; [`mark_finished`] is the call that sets it.
fn phase_label(
    entry_path: &str,
    entry_name: &str,
    phase: MaterializationPhase,
) -> Arc<MaterializationBarLabel> {
    Arc::new(MaterializationBarLabel {
        entry_path: entry_path.to_string(),
        entry_name: entry_name.to_string(),
        phase: Some(phase),
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
/// A media folder and a playlist entry declare `[stg]` and no later phase,
/// because their arms install none, so they keep the phase they were created
/// with for their whole run.
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
    const OVERALL_SEED: &str = "materializing";
    /// Variant whose ZIP members the folder entry unpacks. The name doubles as
    /// the sub-bar's seed and its `file_name` segment, as the folder arm's
    /// `[wrt]` sub-bar builds it.
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
    // installs the label on the caller's own handle, whether that is the handle
    // it was given or one it opened itself. The label leaves `phase` unset, so
    // the overall row names the screen and no phase.
    overall.set_total(ENTRY_COUNT);
    overall.set_truncation(Arc::new(MaterializationBarLabel {
        entry_name: "materializing".to_string(),
        ..Default::default()
    }));

    // The first entry is a media file, so it is the one row that walks the full
    // `[stg]` to `[vrf]` to `[cmt]` sequence.
    let finished = screen.add_bar(ENTRY_PHASES, &format!("{album_name} [stg]"));
    finished.set_truncation(phase_label(album_path, album_name, MaterializationPhase::Staging));
    screen.tick();
    clock.advance(Duration::from_secs(2));

    finished.set_truncation(phase_label(album_path, album_name, MaterializationPhase::Verify));
    finished.advance(1);
    screen.tick();
    clock.advance(Duration::from_secs(1));

    finished.set_truncation(phase_label(album_path, album_name, MaterializationPhase::Commit));
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
    folder.set_truncation(phase_label(folder_path, folder_name, MaterializationPhase::Staging));
    screen.tick();
    clock.advance(Duration::from_secs(2));

    let write = screen.add_bar(LINK_VARIANT_MEMBERS, &format!("{LINK_VARIANT} [wrt]"));
    write.set_truncation(Arc::new(MaterializationBarLabel {
        entry_path: folder_path.to_string(),
        entry_name: folder_name.to_string(),
        file_name: LINK_VARIANT.to_string(),
        phase: Some(MaterializationPhase::Write),
        ..Default::default()
    }));
    write.advance(1);
    screen.tick();
    clock.advance(Duration::from_secs(4));

    // The third entry is a media file that has resolved its hash and is on the
    // verify tag, still running when the transcript is read.
    let verifying = screen.add_bar(ENTRY_PHASES, &format!("{single_name} [stg]"));
    verifying.set_truncation(phase_label(single_path, single_name, MaterializationPhase::Staging));
    verifying.advance(1);
    verifying.set_truncation(phase_label(single_path, single_name, MaterializationPhase::Verify));
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

/// Media entry the board of Canada album track, quoted from the online demo's
/// hierarchy and kept whole rather than shortened, because a real sync writes
/// a path this long.
const BOARDS_TRACK: &str =
    "Music/Boards of Canada/Music Has the Right to Children/01 - Telepathy.flac";

/// Second media entry from the same artist, on the album that follows it.
const GEOGADDI_TRACK: &str = "Music/Boards of Canada/Geogaddi/02 - Olson.wav";

/// Media entry from Pink Floyd's `The Wall`, the track the baseline scenario
/// leaves part-way through verify.
const WALL_TRACK: &str = "Music/Pink Floyd/The Wall/01 - In the Flesh?.m4a";

/// Second track off `The Wall`.
const THIN_ICE_TRACK: &str = "Music/Pink Floyd/The Wall/02 - The Thin Ice.m4a";

/// Media entry whose path is the widest of the ones here, at eighty columns
/// before any clipping.
const XTAL_TRACK: &str = "Music/Aphex Twin/Selected Ambient Works 85-92/02 - Xtal.m4a";

/// Media folder entry the online demo materializes, as
/// `demo_hierarchy_spec::online_demo_media_folder_relative()` spells it.
///
/// The basename is fifty-nine columns, so the row is the one the elastic
/// `entry_name` exists for.
const ONLINE_MEDIA_FOLDER: &str =
    "music videos/Rick Astley - Never Gonna Give You Up [youtube.dQw4w9WgXcQ]";

/// Playlist entry the online demo materializes, under the `playlists/` leaf
/// `demo_hierarchy_spec::ONLINE_DEMO_PLAYLIST` names.
const ONLINE_PLAYLIST: &str = "playlists/rickroll.m3u8";

/// Members the online demo's `thumbnails` variant unpacks, a JPEG and a WebP.
const THUMBNAIL_MEMBERS: u64 = 2;

/// Open a per-entry bar the way `prepare_hierarchy_entry` does.
///
/// The seed is `{relative_path} [stg]` (`EntryPhaseBar::create`) and the
/// label carries the split of that path the materializer keeps, so the row
/// reads what a live row reads rather than what a shorter seed would fit.
fn add_entry_bar(screen: &ProgressScreen, relative_path: &str) -> ProgressBarHandle {
    let (entry_path, entry_name) = split_entry_path(relative_path);
    let bar = screen.add_bar(ENTRY_PHASES, &format!("{relative_path} [stg]"));
    bar.set_truncation(phase_label(entry_path, entry_name, MaterializationPhase::Staging));
    bar
}

/// Re-install an entry bar's label on a later phase, as the media arm does
/// through `EntryPhaseBar::enter` at each of its two transitions.
fn set_entry_phase(bar: &ProgressBarHandle, relative_path: &str, phase: MaterializationPhase) {
    let (entry_path, entry_name) = split_entry_path(relative_path);
    bar.set_truncation(phase_label(entry_path, entry_name, phase));
}

/// Re-install an entry bar's label with a terminal marker, as
/// `EntryPhaseBar::finish` does.
///
/// The marker is the caller's, because it is the finish that decides between
/// them, and so is the phase: the warned media arm passes `W` on the `[vrf]`
/// it last installed, and the failed playlist passes `F` on the `[stg]` it
/// never left.
fn mark_finished(
    bar: &ProgressBarHandle,
    relative_path: &str,
    phase: MaterializationPhase,
    marker: &str,
) {
    let (entry_path, entry_name) = split_entry_path(relative_path);
    bar.set_truncation(Arc::new(MaterializationBarLabel {
        entry_path: entry_path.to_string(),
        entry_name: entry_name.to_string(),
        file_name: String::new(),
        phase: Some(phase),
        status_marker: marker.to_string(),
    }));
}

/// Open a `[wrt]` sub-bar for one ZIP variant of a folder entry.
///
/// The folder arm walks its variants one at a time, so at most one of these is
/// running at a time and the ones before it sit finished on the screen. The
/// sub-bar keeps the parent entry's path, so a reader can tell which folder a
/// member belongs to from the row alone.
fn add_write_bar(
    screen: &ProgressScreen,
    entry_relative_path: &str,
    variant_name: &str,
    members: u64,
) -> ProgressBarHandle {
    let (entry_path, entry_name) = split_entry_path(entry_relative_path);
    let bar = screen.add_bar(members, &format!("{variant_name} [wrt]"));
    bar.set_truncation(Arc::new(MaterializationBarLabel {
        entry_path: entry_path.to_string(),
        entry_name: entry_name.to_string(),
        file_name: variant_name.to_string(),
        phase: Some(MaterializationPhase::Write),
        ..Default::default()
    }));
    bar
}

/// Walk a media entry from `[stg]` to a finished `[cmt]` row.
///
/// The three transitions are the ones the media arm makes, in that order, and
/// each advance is the call the arm makes after the phase it is leaving. The
/// caller supplies the handle because the baseline scenario seeds its bars
/// itself, while `dense` and `states` share this.
fn commit_media_entry(bar: &ProgressBarHandle, relative_path: &str) {
    bar.advance(1);
    set_entry_phase(bar, relative_path, MaterializationPhase::Verify);
    bar.advance(1);
    set_entry_phase(bar, relative_path, MaterializationPhase::Commit);
    bar.advance(1);
    bar.finish_success();
}

/// A library band: an entry of every kind the materializer dispatches.
///
/// Seven entries and the two sub-bars a folder with two ZIP variants opens,
/// under the pinned overall bar. The media entries walk their phases, one
/// stops on `[vrf]` with its hash resolved, one is still staging, the folder
/// is unpacking its last variant, and the playlist has been written. That is
/// the band a real sync of a mixed library fills, which the three-entry
/// baseline has no room to show.
fn render_dense(config: ScreenConfig) -> String {
    /// Entries in the synthetic library, which is what the overall bar totals.
    const ENTRY_COUNT: u64 = 7;
    /// Seed the overall bar carries, from `src/mediapm/src/service.rs:1349`.
    const OVERALL_SEED: &str = "materializing";
    /// Variant that unpacks the JPEG and the WebP.
    const THUMBNAILS: &str = "thumbnails";
    /// Variant that unpacks the three link files.
    const LINKS: &str = "links";
    /// Entries whose task has returned by the time the last frame is drawn.
    const ENTRIES_RETURNED: u64 = 4;

    let (terminal, grid, clock) = capture_terminal(config);
    let (screen, overall) = terminal.screen().with_overall(OVERALL_SEED, 1).build();
    overall.set_total(ENTRY_COUNT);
    overall.set_truncation(Arc::new(MaterializationBarLabel {
        entry_name: "materializing".to_string(),
        ..Default::default()
    }));

    for (index, track) in [BOARDS_TRACK, GEOGADDI_TRACK, WALL_TRACK].into_iter().enumerate() {
        let bar = add_entry_bar(&screen, track);
        screen.tick();
        // A finished row keeps the seconds it had when it finished, so the
        // seconds are spent before the commit rather than after it. They differ
        // per entry, which is what a mixed library looks like.
        clock.advance(Duration::from_secs(2 + index as u64));
        commit_media_entry(&bar, track);
        overall.advance(1);
        screen.tick();
        clock.advance(Duration::from_secs(3));
    }

    // A media entry that resolved its hash is on `[vrf]` with the commit still
    // to come, which is where the baseline leaves its third entry.
    let verifying = add_entry_bar(&screen, THIN_ICE_TRACK);
    verifying.advance(1);
    set_entry_phase(&verifying, THIN_ICE_TRACK, MaterializationPhase::Verify);
    screen.tick();
    clock.advance(Duration::from_secs(2));

    // One that has not reached its hash yet is still on the phase it was
    // created with. The handle is not bound, because nothing happens to that
    // row before the frame is drawn.
    add_entry_bar(&screen, XTAL_TRACK);
    screen.tick();
    clock.advance(Duration::from_secs(1));

    // A media folder runs its variants one after another. The first has
    // finished writing its members and sits below its parent's row; the second
    // is part-way through.
    add_entry_bar(&screen, ONLINE_MEDIA_FOLDER);
    screen.tick();
    clock.advance(Duration::from_secs(2));

    let thumbnails = add_write_bar(&screen, ONLINE_MEDIA_FOLDER, THUMBNAILS, THUMBNAIL_MEMBERS);
    thumbnails.advance(THUMBNAIL_MEMBERS);
    clock.advance(Duration::from_secs(1));
    thumbnails.finish_success();
    screen.tick();
    clock.advance(Duration::from_secs(3));

    let links = add_write_bar(&screen, ONLINE_MEDIA_FOLDER, LINKS, LINK_VARIANT_MEMBERS);
    links.advance(1);
    screen.tick();
    clock.advance(Duration::from_secs(4));

    // A playlist entry never leaves `[stg]`: the arm that writes it installs no
    // later phase, so a written playlist reads as one that finished staging.
    let playlist = add_entry_bar(&screen, ONLINE_PLAYLIST);
    playlist.advance(1);
    clock.advance(Duration::from_secs(2));
    playlist.finish_success();
    overall.advance(1);
    screen.tick();
    clock.advance(Duration::from_secs(1));

    // Three entries are still in flight, so the overall bar is active rather
    // than closed out.
    overall.set_position(ENTRIES_RETURNED);
    screen.join();
    drop(terminal);
    grid.contents()
}

/// The rows the other two scenarios never draw.
///
/// A warned media entry, a failed playlist, two sub-bars under a folder whose
/// own row is still running, and paths wide enough that the elastic name has
/// to shorten at every committed width. Both markers are drawn here because the
/// materializer installs both on a finish; what it never installs is a warned
/// overall bar, so this scenario leaves the overall bar on the error the failed
/// entry causes. The module doc above names the call sites.
fn render_states(config: ScreenConfig) -> String {
    /// Entries in the synthetic library, which is what the overall bar totals.
    const ENTRY_COUNT: u64 = 7;
    /// Seed the overall bar carries, from `src/mediapm/src/service.rs:1349`.
    const OVERALL_SEED: &str = "materializing";
    /// Entries whose task had returned when the collect loop broke.
    const ENTRIES_RETURNED: u64 = 3;
    /// Variant that unpacks the JPEG and the WebP.
    const THUMBNAILS: &str = "thumbnails";
    /// Variant that unpacks the three link files.
    const LINKS: &str = "links";

    let (terminal, grid, clock) = capture_terminal(config);
    let (screen, overall) = terminal.screen().with_overall(OVERALL_SEED, 1).build();
    overall.set_total(ENTRY_COUNT);
    overall.set_truncation(Arc::new(MaterializationBarLabel {
        entry_name: "materializing".to_string(),
        ..Default::default()
    }));

    // A media entry whose variant resolved no content hash is skipped with a
    // warning, and this is the only warning a per-entry row has.
    let warned = add_entry_bar(&screen, THIN_ICE_TRACK);
    warned.advance(1);
    set_entry_phase(&warned, THIN_ICE_TRACK, MaterializationPhase::Verify);
    clock.advance(Duration::from_secs(2));
    mark_finished(&warned, THIN_ICE_TRACK, MaterializationPhase::Verify, "W");
    warned.finish_warning();
    overall.advance(1);
    screen.tick();

    // A committed row, whose name is wide enough that the elastic name shortens
    // it at every width this screen is captured at.
    let committed = add_entry_bar(&screen, BOARDS_TRACK);
    clock.advance(Duration::from_secs(5));
    commit_media_entry(&committed, BOARDS_TRACK);
    overall.advance(1);
    screen.tick();
    clock.advance(Duration::from_secs(2));

    // Still staging, so the row carries no marker and the phase it was created
    // with. Nothing happens to the row before the frame is drawn.
    add_entry_bar(&screen, XTAL_TRACK);
    screen.tick();
    clock.advance(Duration::from_secs(1));

    // A folder mid-variant: its own row is still on `[stg]` while its sub-bars
    // run below it, and its name is the widest the materializer produces.
    add_entry_bar(&screen, ONLINE_MEDIA_FOLDER);
    screen.tick();
    clock.advance(Duration::from_secs(2));

    let thumbnails = add_write_bar(&screen, ONLINE_MEDIA_FOLDER, THUMBNAILS, THUMBNAIL_MEMBERS);
    thumbnails.advance(THUMBNAIL_MEMBERS);
    clock.advance(Duration::from_secs(1));
    thumbnails.finish_success();
    screen.tick();
    clock.advance(Duration::from_secs(3));

    let links = add_write_bar(&screen, ONLINE_MEDIA_FOLDER, LINKS, LINK_VARIANT_MEMBERS);
    links.advance(1);
    screen.tick();
    clock.advance(Duration::from_secs(4));

    // The playlist arm is one of two that check their result before finishing,
    // and a failure there is the only `[F]` a folder or playlist row carries.
    let failed = add_entry_bar(&screen, ONLINE_PLAYLIST);
    failed.advance(1);
    clock.advance(Duration::from_secs(1));
    mark_finished(&failed, ONLINE_PLAYLIST, MaterializationPhase::Staging, "F");
    failed.finish_error();
    overall.advance(1);
    screen.tick();

    // The same error returns from `prepare_hierarchy_entry`, breaks the collect
    // loop, and finishes the overall bar on error, so the failed row and a
    // failed overall bar are one event rather than two. That row carries no
    // marker: production installs the marker on entry bars, and the overall bar
    // reaches its failure through its colour alone.
    overall.set_position(ENTRIES_RETURNED);
    overall.finish_error();
    screen.join();
    drop(terminal);
    grid.contents()
}
