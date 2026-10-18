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
//! - `{relative_path}` on a per-entry bar, from `EntryPhaseBar::create`.
//! - `{variant_name}` on a per-extracted-file sub-bar, from the folder
//!   arm.
//!
//! `materializing` is thirteen columns and the overall bar is the only
//! bar whose seed stays that short. Every other seed carries a path or a
//! variant name. The overall bar carries no identity of its own beyond the
//! word, which already names what the bar is doing. The shared prefix slot is what the
//! terminal has left after the spinner, the separators and the suffix, with a
//! fill floor under the bar
//! while there is room for a bar at all, and under it the elastic name
//! shortens from the head, so a folder name wider than the
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
//! with the marker set on the row's identity. That is the only warning a
//! per-entry row carries.
//!
//! The `[F]` row is reached too, on the two arms that check their result before
//! finishing: a media folder and a playlist. The playlist fails while
//! resolving a reference, which is before its write, so nothing was
//! committed; a folder that fails before its first variant is
//! written has committed nothing for the same reason. Neither can be reached
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
//! The sub-bars and the over-long names are ordinary. A folder entry
//! with a ZIP variant opens one sub-bar per variant from the first of them,
//! so a parent and its children name the same
//! work at two granularities. A YouTube-style media folder name runs to
//! fifty-nine columns, which is wider than the forty the prefix slot is capped
//! at, so the elastic name has to shorten it at every width here.
//!
//! The label is the library's own [`MaterializationBarLabel`], re-exported
//! from the `mediapm` crate root, and the path split is the library's
//! [`split_entry_path`]. The example therefore drives the real truncation
//! order: a reorder in
//! `src/mediapm/src/materializer/progress_labels.rs` changes these grids
//! rather than passing unnoticed against a local copy of the field list.
//!
//! Where the labels give way is a measured fact about the seed paths quoted
//! above, not a contract, and it moves when a path changes length. Nothing in
//! the suite is named after those numbers: a fixture called for one of those
//! widths would go on testing that width after the seed it was measured
//! against had moved, and its name would be the only thing left still
//! claiming the number mattered.

use std::cell::Cell;
use std::sync::Arc;
use std::time::Duration;

use mediapm::{MaterializationBarLabel, split_entry_path};
use mediapm_utils::progress::{ProgressBarHandle, ProgressScreen, TestTimeSource};

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
    Scenario::new(ScenarioName::Dense, 12, render_dense),
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

/// A scenario's declared frame count beside the frames it has drawn.
///
/// Every transcript under `examples/fixtures/` pins the bytes each frame drew,
/// spinner glyph included, so how many frames a scenario draws is part of what
/// a transcript records. Left to itself the count was however many `screen.tick()`
/// calls the operations happened to make, which made an edit to the operations a
/// silent edit to all thirty transcripts. The count is stated here instead, and
/// the two ways an operations list can disagree with it both end in a stated
/// outcome rather than in different bytes.
struct Frames {
    /// Frames the scenario draws.
    declared: u16,
    /// Frames drawn so far, which [`Frames::draw`] and [`Frames::finish`] both add to.
    drawn: Cell<u16>,
}

impl Frames {
    /// Hold the frame count `declared` states for the scenario being drawn.
    const fn new(declared: u16) -> Self {
        Self { declared, drawn: Cell::new(0) }
    }

    /// Draw one frame and move the synthetic clock on by `after`, which is the
    /// time the operations spent between this frame and the next one.
    ///
    /// The count is taken here rather than at the call site so that it tracks
    /// the frames and not the operations that happened to surround them.
    fn draw(&self, screen: &ProgressScreen, clock: &TestTimeSource, after: Duration) {
        screen.tick();
        clock.advance(after);
        self.drawn.set(self.drawn.get() + 1);
    }

    /// Draw the frames the operations left undrawn, and refuse the ones they
    /// drew beyond the declared count.
    ///
    /// A scenario that finishes its work early draws the rest of its frames
    /// here, with nothing new happening on screen between them. Those frames
    /// repeat the last state, which is what an idle screen draws anyway, and a
    /// scenario cannot end up with fewer frames than it declared because
    /// something was left out.
    ///
    /// # Panics
    ///
    /// Panics when the operations drew more frames than the scenario declared.
    /// The transcripts would then record a frame count the scenario does not
    /// claim, and the count they recorded would be a side effect of the
    /// operations rather than something anyone stated.
    fn finish(&self, screen: &ProgressScreen) {
        let drawn = self.drawn.get();
        assert!(
            drawn <= self.declared,
            "this scenario declares {} frames and its operations drew {drawn}",
            self.declared
        );
        for _ in drawn..self.declared {
            screen.tick();
        }
    }
}

/// A per-entry bar's total, which is what `EntryPhaseBar::create` hands to
/// `add_bar` in the materializer.
///
/// One, because an entry arm advances once after its work returns, so a
/// finished entry reads a full bar whether it succeeded, warned or failed.
const ENTRY_TOTAL: u64 = 1;

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

/// The label the materializer installs on an entry row.
///
/// The label carries the entry's basename and nothing else here: the
/// materializer's rows add the media id and variant the document resolves,
/// and this example's synthetic library declares none. `status_marker`
/// stays at its default until the finish; [`mark_finished`] is the call
/// that sets it.
fn entry_label(entry_name: &str) -> Arc<MaterializationBarLabel> {
    Arc::new(MaterializationBarLabel { entry_name: entry_name.to_string(), ..Default::default() })
}

/// Render the materialization screen at `config`'s size and return the raw grid.
///
/// The folder entry is `READABLE_FOLDER_ENTRY`. Pass another path to
/// `render_materialization_screen_for_folder` to see what a wider name does to
/// the row.
///
/// Three entries stand in for a sync. The first is a single media file that
/// runs its arm to completion, so its row is drawn finished. The second is a
/// media folder holding a ZIP variant, whose writing shows up on a
/// sub-bar underneath. The third is a media file still in flight.
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
    /// Seed the overall bar carries, from
    /// `src/mediapm/src/service.rs::sync_library_with_progress_overrides`.
    const OVERALL_SEED: &str = "materializing";
    /// Variant whose ZIP members the folder entry unpacks. The name doubles as
    /// the sub-bar's seed and its `variant` segment, as the folder arm's
    /// sub-bar builds it.
    const LINK_VARIANT: &str = "links";

    // A folder name wider than the client budget shortens from the front rather
    // than being dropped whole, so both the name's tail and the directory's
    // tail survive. The default seed stays inside the budget;
    // `materialization_screen_keeps_the_tail_of_a_folder_name_wider_than_the_budget`
    // renders the online demo's name, which does not.
    let (_, folder_name) = split_entry_path(folder_entry);
    let (_, album_name) = split_entry_path(BOARDS_TRACK);
    let (_, single_name) = split_entry_path(WALL_TRACK);

    let (terminal, grid, clock) = capture_terminal(config);
    let (screen, overall) = terminal.screen().with_overall(OVERALL_SEED, 1).build();
    // The worked examples mirror production, where the owner of a run's
    // overall bar opts it in while the run is active.
    overall.set_animating(true);
    // `sync_hierarchy` replaces the placeholder total with the entry count and
    // installs the label on the caller's own handle, whether that is the handle
    // it was given or one it opened itself. The label carries only the entry
    // name, so the overall row names the screen.
    overall.set_total(ENTRY_COUNT);
    overall.set_truncation(Arc::new(MaterializationBarLabel {
        entry_name: "materializing".to_string(),
        ..Default::default()
    }));
    let frames = Frames::new(7);

    // The first entry is a media file, so it is the one row that runs its arm
    // to completion. The arm reinstalls the row's identity at each phase it
    // passes through — the label no longer carries the phase, so the text does
    // not move — and the bar does not advance until the work returns, because
    // the arm advances once and the row totals one. The ticks stay where they
    // were, so each transition still gets a frame of its own.
    let finished = screen.add_bar(ENTRY_TOTAL, BOARDS_TRACK);
    // Demo bars all carry in-flight work; their finish parks the row.
    finished.set_animating(true);
    finished.set_truncation(entry_label(album_name));
    frames.draw(&screen, &clock, Duration::from_secs(2));

    finished.set_truncation(entry_label(album_name));
    frames.draw(&screen, &clock, Duration::from_secs(1));

    finished.set_truncation(entry_label(album_name));
    frames.draw(&screen, &clock, Duration::from_secs(3));

    finished.advance(1);
    finished.finish_success();
    overall.advance(1);
    frames.draw(&screen, &clock, Duration::from_secs(1));

    // The second entry is a media folder. Its own bar keeps the identity it
    // was created with while the arm works, and the per-extracted-file
    // sub-bar opens underneath it at the variant it unpacks.
    let folder = screen.add_bar(ENTRY_TOTAL, folder_entry);
    // Demo bars all carry in-flight work; their finish parks the row.
    folder.set_animating(true);
    folder.set_truncation(entry_label(folder_name));
    frames.draw(&screen, &clock, Duration::from_secs(2));

    let write = add_write_bar(&screen, folder_entry, LINK_VARIANT, LINK_VARIANT_MEMBERS);
    write.advance(1);
    frames.draw(&screen, &clock, Duration::from_secs(4));

    // The third entry is a media file whose work has returned but whose row has
    // not finished, still running when the transcript is read.
    let verifying = screen.add_bar(ENTRY_TOTAL, WALL_TRACK);
    // Demo bars all carry in-flight work; their finish parks the row.
    verifying.set_animating(true);
    verifying.set_truncation(entry_label(single_name));
    verifying.advance(1);
    verifying.set_truncation(entry_label(single_name));
    frames.draw(&screen, &clock, Duration::from_secs(2));

    // The sync is still in flight: the folder and the third entry have not
    // reached their commit phase, so the overall bar stays active at one of
    // three entries rather than being closed out.
    overall.set_position(1);
    frames.finish(&screen);
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
/// The seed is `{relative_path}` (`EntryPhaseBar::create`) and the label
/// carries the basename the materializer splits off that path, so the row
/// reads what a live row reads rather than what a longer seed would fit. The
/// total is [`ENTRY_TOTAL`], so a row that has advanced is already full.
fn add_entry_bar(screen: &ProgressScreen, relative_path: &str) -> ProgressBarHandle {
    let (_, entry_name) = split_entry_path(relative_path);
    let bar = screen.add_bar(ENTRY_TOTAL, relative_path);
    // Demo bars all carry in-flight work; their finish parks the row.
    bar.set_animating(true);
    bar.set_truncation(entry_label(entry_name));
    bar
}

/// Re-install an entry bar's label on a later phase, as the media arm does
/// through `EntryPhaseBar::enter` at each of its two transitions. The folder
/// arm makes one of these calls as well, at the variant it starts writing.
/// The label no longer carries the phase, so the re-install renders the same
/// text the row already draws — which is what a live sync renders too.
fn set_entry_identity(bar: &ProgressBarHandle, relative_path: &str) {
    let (_, entry_name) = split_entry_path(relative_path);
    bar.set_truncation(entry_label(entry_name));
}

/// Re-install an entry bar's label with a terminal marker, as
/// `EntryPhaseBar::finish` does.
///
/// The marker is the caller's, because it is the finish that decides between
/// them: the warned media arm passes `W`, and the failed playlist passes `F`
/// on the identity it failed with.
fn mark_finished(bar: &ProgressBarHandle, relative_path: &str, marker: &str) {
    let (_, entry_name) = split_entry_path(relative_path);
    bar.set_truncation(Arc::new(MaterializationBarLabel {
        entry_name: entry_name.to_string(),
        status_marker: marker.to_string(),
        ..Default::default()
    }));
}

/// Open a sub-bar for one ZIP variant of a folder entry.
///
/// The folder arm walks its variants one at a time, so at most one of these is
/// running at a time and the ones before it sit finished on the screen. The
/// sub-bar carries the parent entry's basename, so a reader can tell which
/// folder a member belongs to from the row alone.
fn add_write_bar(
    screen: &ProgressScreen,
    entry_relative_path: &str,
    variant_name: &str,
    members: u64,
) -> ProgressBarHandle {
    let (_, entry_name) = split_entry_path(entry_relative_path);
    let bar = screen.add_bar(members, variant_name);
    // Demo bars all carry in-flight work; their finish parks the row.
    bar.set_animating(true);
    bar.set_truncation(Arc::new(MaterializationBarLabel {
        variant: variant_name.to_string(),
        entry_name: entry_name.to_string(),
        ..Default::default()
    }));
    bar
}

/// Walk a media entry to a finished success row.
///
/// The two re-installs are the ones the media arm makes, in that order; they
/// render the same identity text, so nothing on screen moves between them.
/// The single advance is the call the arm makes once its work has returned.
/// The caller supplies the handle because the baseline scenario seeds its
/// bars itself, while `dense` and `states` share this.
fn commit_media_entry(bar: &ProgressBarHandle, relative_path: &str) {
    set_entry_identity(bar, relative_path);
    set_entry_identity(bar, relative_path);
    bar.advance(1);
    bar.finish_success();
}

/// A library band: an entry of every kind the materializer dispatches.
///
/// Eight entries and the three sub-bars that two folders with ZIP variants open,
/// under the pinned overall bar. The media entries walk their arm, one has
/// advanced but not finished, one is still untouched, and the playlist has been
/// written. Both folders write the variant each names, and each
/// has a sub-bar under it: the online demo's folder has finished its `thumbnails`
/// and is part-way through `links`, and the second folder is still on its first
/// variant. A mixed library has one folder per artist, so two of them is the
/// ordinary case and the three-entry baseline has no room to show it.
fn render_dense(config: ScreenConfig) -> String {
    /// Entries in the synthetic library, which is what the overall bar totals.
    const ENTRY_COUNT: u64 = 8;
    /// Seed the overall bar carries, from
    /// `src/mediapm/src/service.rs::sync_library_with_progress_overrides`.
    const OVERALL_SEED: &str = "materializing";
    /// Variant that unpacks the JPEG and the WebP.
    const THUMBNAILS: &str = "thumbnails";
    /// Variant that unpacks the three link files.
    const LINKS: &str = "links";
    /// Entries whose task has returned by the time the last frame is drawn.
    const ENTRIES_RETURNED: u64 = 4;

    let (terminal, grid, clock) = capture_terminal(config);
    let (screen, overall) = terminal.screen().with_overall(OVERALL_SEED, 1).build();
    // The worked examples mirror production, where the owner of a run's
    // overall bar opts it in while the run is active.
    overall.set_animating(true);
    overall.set_total(ENTRY_COUNT);
    overall.set_truncation(Arc::new(MaterializationBarLabel {
        entry_name: "materializing".to_string(),
        ..Default::default()
    }));
    let frames = Frames::new(13);

    for (index, track) in [BOARDS_TRACK, GEOGADDI_TRACK, WALL_TRACK].into_iter().enumerate() {
        let bar = add_entry_bar(&screen, track);
        frames.draw(&screen, &clock, Duration::ZERO);
        // A finished row keeps the seconds it had when it finished, so the
        // seconds are spent before the commit rather than after it. They differ
        // per entry, which is what a mixed library looks like.
        clock.advance(Duration::from_secs(2 + index as u64));
        commit_media_entry(&bar, track);
        overall.advance(1);
        frames.draw(&screen, &clock, Duration::from_secs(3));
    }

    // A media entry whose work has returned has advanced but not finished,
    // which is where the baseline leaves its third entry.
    let verifying = add_entry_bar(&screen, THIN_ICE_TRACK);
    verifying.advance(1);
    set_entry_identity(&verifying, THIN_ICE_TRACK);
    frames.draw(&screen, &clock, Duration::from_secs(2));

    // One whose work has not returned sits on the label it was
    // created with. The handle is not bound, because nothing happens to that
    // row before the frame is drawn.
    add_entry_bar(&screen, XTAL_TRACK);
    frames.draw(&screen, &clock, Duration::from_secs(1));

    // A media folder runs its variants one after another. Its own row
    // re-installs its identity at the first of them and stays there. The first
    // variant has finished writing its members and sits below its parent's row;
    // the second is part-way through.
    let folder = add_entry_bar(&screen, ONLINE_MEDIA_FOLDER);
    frames.draw(&screen, &clock, Duration::from_secs(2));

    set_entry_identity(&folder, ONLINE_MEDIA_FOLDER);
    let thumbnails = add_write_bar(&screen, ONLINE_MEDIA_FOLDER, THUMBNAILS, THUMBNAIL_MEMBERS);
    thumbnails.advance(THUMBNAIL_MEMBERS);
    clock.advance(Duration::from_secs(1));
    thumbnails.finish_success();
    frames.draw(&screen, &clock, Duration::from_secs(3));

    let links = add_write_bar(&screen, ONLINE_MEDIA_FOLDER, LINKS, LINK_VARIANT_MEMBERS);
    links.advance(1);
    frames.draw(&screen, &clock, Duration::from_secs(4));

    // A playlist resolves its references and then writes one file, and its row
    // finishes when that write returns.
    let playlist = add_entry_bar(&screen, ONLINE_PLAYLIST);
    playlist.advance(1);
    set_entry_identity(&playlist, ONLINE_PLAYLIST);
    clock.advance(Duration::from_secs(2));
    playlist.finish_success();
    overall.advance(1);
    frames.draw(&screen, &clock, Duration::from_secs(1));

    // A second folder, still on its first variant. Its own row re-installs its
    // identity the moment that variant starts, the way the folder above did,
    // and its sub-bar sits below it while the first folder's sub-bars sit below
    // that one.
    let second_folder = add_entry_bar(&screen, READABLE_FOLDER_ENTRY);
    set_entry_identity(&second_folder, READABLE_FOLDER_ENTRY);
    let second_thumbnails =
        add_write_bar(&screen, READABLE_FOLDER_ENTRY, THUMBNAILS, THUMBNAIL_MEMBERS);
    second_thumbnails.advance(1);
    frames.draw(&screen, &clock, Duration::from_secs(3));

    // Four entries are still in flight, so the overall bar is active rather
    // than closed out.
    overall.set_position(ENTRIES_RETURNED);
    frames.finish(&screen);
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
    /// Seed the overall bar carries, from
    /// `src/mediapm/src/service.rs::sync_library_with_progress_overrides`.
    const OVERALL_SEED: &str = "materializing";
    /// Entries whose task had returned when the collect loop broke.
    const ENTRIES_RETURNED: u64 = 3;
    /// Variant that unpacks the JPEG and the WebP.
    const THUMBNAILS: &str = "thumbnails";
    /// Variant that unpacks the three link files.
    const LINKS: &str = "links";

    let (terminal, grid, clock) = capture_terminal(config);
    let (screen, overall) = terminal.screen().with_overall(OVERALL_SEED, 1).build();
    // The worked examples mirror production, where the owner of a run's
    // overall bar opts it in while the run is active.
    overall.set_animating(true);
    overall.set_total(ENTRY_COUNT);
    overall.set_truncation(Arc::new(MaterializationBarLabel {
        entry_name: "materializing".to_string(),
        ..Default::default()
    }));
    let frames = Frames::new(7);

    // A media entry whose variant resolved no content hash is skipped with a
    // warning, and this is the only warning a per-entry row has.
    let warned = add_entry_bar(&screen, THIN_ICE_TRACK);
    warned.advance(1);
    set_entry_identity(&warned, THIN_ICE_TRACK);
    clock.advance(Duration::from_secs(2));
    mark_finished(&warned, THIN_ICE_TRACK, "W");
    warned.finish_warning();
    overall.advance(1);
    frames.draw(&screen, &clock, Duration::ZERO);

    // A committed row, whose name is wide enough that the elastic name shortens
    // it at every width this screen is captured at.
    let committed = add_entry_bar(&screen, BOARDS_TRACK);
    clock.advance(Duration::from_secs(5));
    commit_media_entry(&committed, BOARDS_TRACK);
    overall.advance(1);
    frames.draw(&screen, &clock, Duration::from_secs(2));

    // Still untouched, so the row carries no marker and the label it was
    // created with. Nothing happens to the row before the frame is drawn.
    add_entry_bar(&screen, XTAL_TRACK);
    frames.draw(&screen, &clock, Duration::from_secs(1));

    // A folder mid-variant: its own row re-installs its identity while its
    // sub-bars run below it, and its name is the widest the materializer
    // produces.
    let folder = add_entry_bar(&screen, ONLINE_MEDIA_FOLDER);
    frames.draw(&screen, &clock, Duration::from_secs(2));

    set_entry_identity(&folder, ONLINE_MEDIA_FOLDER);
    let thumbnails = add_write_bar(&screen, ONLINE_MEDIA_FOLDER, THUMBNAILS, THUMBNAIL_MEMBERS);
    thumbnails.advance(THUMBNAIL_MEMBERS);
    clock.advance(Duration::from_secs(1));
    thumbnails.finish_success();
    frames.draw(&screen, &clock, Duration::from_secs(3));

    let links = add_write_bar(&screen, ONLINE_MEDIA_FOLDER, LINKS, LINK_VARIANT_MEMBERS);
    links.advance(1);
    frames.draw(&screen, &clock, Duration::from_secs(4));

    // The playlist arm is one of two that check their result before finishing,
    // and a failure there is the only `[F]` a folder or playlist row carries.
    // The row carries the marker on the identity it failed with: an
    // unresolvable reference is refused before the write, so nothing was
    // committed.
    let failed = add_entry_bar(&screen, ONLINE_PLAYLIST);
    failed.advance(1);
    clock.advance(Duration::from_secs(1));
    mark_finished(&failed, ONLINE_PLAYLIST, "F");
    failed.finish_error();
    overall.advance(1);
    frames.draw(&screen, &clock, Duration::ZERO);

    // The same error returns from `prepare_hierarchy_entry`, breaks the collect
    // loop, and finishes the overall bar on error, so the failed row and a
    // failed overall bar are one event rather than two. That row carries no
    // marker: production installs the marker on entry bars, and the overall bar
    // reaches its failure through its colour alone.
    overall.set_position(ENTRIES_RETURNED);
    overall.finish_error();
    frames.finish(&screen);
    screen.join();
    drop(terminal);
    grid.contents()
}
