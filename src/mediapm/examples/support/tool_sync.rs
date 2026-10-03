//! Screen A: the tool-sync progress screen.
//!
//! Renders what `reconcile_desired_tools` puts on the terminal, using the
//! captured terminal and synthetic clock the shared harness in `support/mod.rs`
//! builds. The example that pulls this module in is
//! `mediapm_progress_tool_sync`.
//!
//! Every seed here is quoted from the coordinator rather than chosen. The
//! renderer measures each bar's seed to size the prefix slot the whole screen
//! shares, so a seed longer than production would widen that slot and draw
//! prefixes a live sync never draws. The production seeds are
//! `{tool_id}{version_suffix} [{phase}]` from
//! `src/mediapm/src/conductor_bridge/sync/provision.rs:213` (`[res]`), `:293`
//! (`[fch]`) and `:339` (`[pro]`), `pruning [prn]` from
//! `src/mediapm/src/conductor_bridge/sync/mod.rs:1404`, and the overall bar
//! `syncing tools` from `sync/mod.rs:1255`. `version_suffix` is a space and the
//! human-readable version, or empty when the tool reports none, per the doc at
//! `provision.rs:144`.
//!
//! These rows draw the built-in prefix, so what gives way under a narrow
//! terminal is fixed by `semantic_truncate_prefix` rather than by a ranking
//! this file chooses: the version is shaved from the right first, so
//! `yt-dlp v2025.1 [res]` loses the `1` and then the whole version, then the
//! tally is dropped whole, and the phase tag is the last thing to go.
//!
//! Where that leaves the labels, measured on 2026-10-03 by rendering this
//! screen at every width the harness accepts: 50 columns is the narrowest at
//! which all six phase-tagged rows still show their tag. One column narrower,
//! the prune row has lost `[prn]` and the overall bar reads `syncing tool`.
//! The overall bar is the exception at every width, because it carries no
//! phase and its name is the whole of its identity. Those are facts about the
//! seeds quoted above, not a contract, and they move when a tool name or a
//! version changes length. Nothing in the suite is named after them, because a
//! fixture called for one of those widths would go on testing that width after
//! the seed it was measured against had moved, and its name would be the only
//! thing left still claiming the number mattered.
//!
//! # Scenarios
//!
//! [`SCENARIOS`] holds the three shapes this screen is captured in. Each entry
//! declares how many bars its renderer draws, and the harness derives the
//! terminal height from that count, so a scenario and its transcripts cannot
//! drift apart.
//!
//! One row in the `states` scenario is documented here rather than left for the
//! reader to find in a fixture. The renderer folds a bar's terminal status into
//! its prefix marker (`SharedState::snapshot`, `renderer.rs:279`), so a warned row
//! draws `[W]` and a failed one `[F]` with no marker set by the caller. The `sd`
//! warning and the warned overall bar are what a live sync reaches: a tool that
//! fails to provision warns its still-active bars (`provision.rs:191`) and the
//! overall bar warns whenever `report.warnings` is not empty (`sync/mod.rs:1364`).
//! The `[F]` row is the one that is not: production warns rather than errors on
//! a failed tool, and `sync/mod.rs:1895` asserts that no `FinishError` reaches a
//! tool-sync bar. That row documents what the renderer draws for a failed bar,
//! the same standing the workflow screen's per-step bar has there.

use std::time::Duration;

use mediapm_utils::progress::{PrefixComponents, ProgressBarApi, StatusCount, SuffixComponents};

use crate::scenarios::{Scenario, ScenarioName};
use crate::support::{ScreenConfig, capture_terminal};

/// The tool-sync screen in each of the three shapes it is captured in.
///
/// `baseline` draws the screen as it shipped, three tools and a prune. `dense`
/// draws every managed tool a sync provisions, so the band of bars is long
/// enough for the height to be deciding something. `states` draws the rows the
/// other two never reach.
pub const SCENARIOS: [Scenario; 3] = [
    Scenario::new(ScenarioName::Baseline, 7, render_tool_sync_screen),
    Scenario::new(ScenarioName::Dense, 20, render_dense),
    Scenario::new(ScenarioName::States, 10, render_states),
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

/// Render the baseline tool-sync screen at `config`'s size and return the raw grid.
///
/// The grid keeps its ANSI colour escapes, the same bytes the terminal received.
/// Pass the result through [`strip_ansi_escapes`](crate::support::strip_ansi_escapes)
/// for a readable transcript.
///
/// The sequence mirrors what `reconcile_desired_tools` does for three tools: a
/// resolve bar whose metadata came from the cache, a resolve bar for a tool
/// that was already provisioned, and a full resolve/fetch/process run for the
/// third, followed by the prune bar and the pinned overall bar.
///
/// This is the `baseline` scenario, unchanged, so its transcripts stay
/// byte-identical to the ones committed before the scenario axis existed.
#[must_use]
pub fn render_tool_sync_screen(config: ScreenConfig) -> String {
    /// The number of tools the run provisions or skips, which is what the pinned
    /// overall bar totals.
    const TOOL_COUNT: u64 = 3;
    /// ffmpeg resolves two metadata URLs, the `BtbN` autobuild tag and the
    /// evermeet version. yt-dlp and deno resolve one each.
    const FFMPEG_LOOKUPS: u64 = 2;
    /// Total bytes of the two `deno` payloads the fetch bar tracks.
    const DENO_FETCH_BYTES: u64 = 31_457_280;
    /// Bytes already downloaded when the fetch bar is drawn mid-run.
    const DENO_FETCHED_BYTES: u64 = 14_680_064;

    let (terminal, grid, clock) = capture_terminal(config);
    let (screen, overall) = terminal.screen().with_overall("syncing tools", 1).build();
    // The caller pins the overall bar with a placeholder total; the tool phase
    // is what knows the tool count.
    overall.set_total(TOOL_COUNT);

    // ffmpeg: both lookups were cache hits, so the bar reports `2 cached`.
    let ffmpeg_resolve = screen.add_bar(FFMPEG_LOOKUPS, "ffmpeg v7.1 [res]");
    ffmpeg_resolve.set_suffix_components(SuffixComponents::status_list(&[StatusCount {
        word: "cached",
        count: Some(2),
    }]));
    ffmpeg_resolve.set_position(FFMPEG_LOOKUPS);
    ffmpeg_resolve.finish_success();
    overall.advance(1);
    screen.tick();
    clock.advance(Duration::from_secs(2));

    // yt-dlp: already provisioned at this version, so it resolves and stops.
    let ytdlp_resolve = screen.add_bar(1, "yt-dlp v2025.1 [res]");
    ytdlp_resolve.set_suffix_components(SuffixComponents::status_list(&[
        StatusCount { word: "skipped", count: None },
        StatusCount { word: "cached", count: Some(1) },
    ]));
    ytdlp_resolve.set_position(1);
    ytdlp_resolve.finish_success();
    overall.advance(1);
    screen.tick();
    clock.advance(Duration::from_secs(1));

    // deno: resolves from the network, then fetches and processes its payloads.
    let deno_resolve = screen.add_bar(1, "deno v2.1.4 [res]");
    deno_resolve.set_position(1);
    deno_resolve.finish_success();

    let deno_fetch = screen.add_bar(2, "deno v2.1.4 [fch]");
    set_phase_components(&deno_fetch, "deno", "v2.1.4", "fch", 1, 2);
    deno_fetch.set_total(DENO_FETCH_BYTES);
    deno_fetch.set_position(DENO_FETCHED_BYTES);
    screen.tick();
    clock.advance(Duration::from_secs(3));

    deno_fetch.set_position(DENO_FETCH_BYTES);
    deno_fetch.set_suffix_components(SuffixComponents::status_list(&[StatusCount {
        word: "cached",
        count: Some(1),
    }]));
    deno_fetch.finish_success();

    // An archive source contributes a decompress item and a compress item, a
    // binary source one import item: three items for these two sources.
    let deno_process = screen.add_bar(3, "deno v2.1.4 [pro]");
    set_phase_components(&deno_process, "deno", "v2.1.4", "pro", 2, 3);
    deno_process.set_position(2);
    screen.tick();
    clock.advance(Duration::from_secs(2));

    deno_process.set_position(3);
    deno_process.finish_success();
    overall.advance(1);
    screen.tick();

    // The prune bar counts the candidates the document rewrite will drop; it is
    // registered after the provisioning loop, directly above the overall bar.
    let prune = screen.add_bar(2, "pruning [prn]");
    prune.advance(1);
    screen.tick();
    clock.advance(Duration::from_secs(1));
    prune.advance(1);
    prune.finish_success();

    overall.set_position(TOOL_COUNT);
    overall.finish_success();
    screen.join();
    drop(terminal);
    grid.contents()
}

/// A sync that provisions every managed tool mediapm ships a provider for.
///
/// The six names are the six managed tools, so this is the band of bars a real
/// sync with no skips reaches: a resolve bar per tool, a fetch and a process bar
/// for each of them, the prune bar, and the pinned overall bar. The baseline
/// scenario stops at three tools with one already provisioned, which is the
/// smaller band this one exists to outgrow.
#[must_use]
fn render_dense(config: ScreenConfig) -> String {
    /// Managed tools with the human-readable version each one reports, from
    /// `tools/provider/mod.rs`.
    const TOOLS: [(&str, &str); 6] = [
        ("ffmpeg", "autobuild-2025-10-03-12-00+evermeet-8.1.2"),
        ("yt-dlp", "2025.09.26"),
        ("deno", "v2.1.4"),
        ("rsgain", "v2.24.1"),
        ("media-tagger", "0.1.0"),
        ("sd", "v1.10.0"),
    ];
    /// Payload bytes per tool, which is what each fetch bar totals.
    const PAYLOAD_BYTES: [u64; 6] =
        [171_966_464, 3_194_880, 31_457_280, 12_582_912, 20_971_520, 1_073_741_824];
    /// Items a process bar reports for one archive source: a decompress and a
    /// compress, per the arithmetic in `provision.rs:288`.
    const ARCHIVE_ITEMS: u64 = 2;

    let (terminal, grid, clock) = capture_terminal(config);
    let (screen, overall) = terminal.screen().with_overall("syncing tools", 1).build();
    let tool_count = TOOLS.len() as u64;
    overall.set_total(tool_count);

    for (index, (tool, version)) in TOOLS.iter().enumerate() {
        let resolve = screen.add_bar(1, &format!("{tool} {version} [res]"));
        resolve.set_position(1);
        resolve.finish_success();
        overall.advance(1);
        screen.tick();
        clock.advance(Duration::from_secs(1));

        // Every source here comes from the network, so the fetch bar opens
        // part-way through and the process bar opens behind it.
        let fetch = screen.add_bar(2, &format!("{tool} {version} [fch]"));
        set_phase_components(&fetch, tool, version, "fch", 1, 2);
        fetch.set_total(PAYLOAD_BYTES[index]);
        fetch.set_position(PAYLOAD_BYTES[index] / 2);
        screen.tick();
        clock.advance(Duration::from_secs(2));

        fetch.set_position(PAYLOAD_BYTES[index]);
        fetch.finish_success();

        let process = screen.add_bar(ARCHIVE_ITEMS, &format!("{tool} {version} [pro]"));
        set_phase_components(&process, tool, version, "pro", 1, ARCHIVE_ITEMS);
        process.set_position(1);
        screen.tick();
        clock.advance(Duration::from_secs(1));

        process.set_position(ARCHIVE_ITEMS);
        process.finish_success();
        screen.tick();
    }

    let prune = screen.add_bar(2, "pruning [prn]");
    prune.advance(1);
    prune.finish_success();

    overall.set_position(tool_count);
    overall.finish_success();
    screen.join();
    drop(terminal);
    grid.contents()
}

/// The rows the other two scenarios never draw.
///
/// Five tools and ten bars. Four of them finish and one does not, which is where
/// the warned row and the failed row come from. A fetch and a process bar open
/// under a tool whose own resolve bar is still running, which is what a sub-bar
/// is here: the renderer's band puts a later bar below an earlier one, so the
/// only way to see one is to leave the parent unfinished.
///
/// Two of the seeds are the longest versions mediapm reports: ffmpeg's composite
/// of a `BtbN` autobuild tag and the evermeet version, and media-tagger's package
/// version followed by the build's git hash. Both are wider than the widest
/// committed transcript, so the head-keeping version shrink runs at every width
/// here and never has enough room to keep the whole version.
#[must_use]
fn render_states(config: ScreenConfig) -> String {
    /// ffmpeg resolves the `BtbN` autobuild tag and the evermeet version.
    const FFMPEG_LOOKUPS: u64 = 2;
    /// Total bytes of the two payloads the `deno` fetch bar tracks.
    const DENO_FETCH_BYTES: u64 = 31_457_280;
    /// Tools this run touched, which is what the overall bar totals.
    const TOOL_COUNT: u64 = 5;
    /// How many of them finished. `sd` is the one that did not, so the bar warns
    /// with the tally short of its total.
    const TOOLS_COMPLETED: u64 = 4;
    /// Items the `deno` process bar reports for its one archive source: a
    /// decompress and a compress.
    const DENO_PROCESS_ITEMS: u64 = 2;

    let (terminal, grid, clock) = capture_terminal(config);
    let (screen, overall) = terminal.screen().with_overall("syncing tools", 1).build();
    overall.set_total(TOOL_COUNT);

    let tagger_resolve = screen.add_bar(1, MEDIA_TAGGER_LABEL);
    tagger_resolve.set_position(1);
    tagger_resolve.finish_success();
    overall.advance(1);
    screen.tick();
    clock.advance(Duration::from_secs(1));

    let ffmpeg_resolve = screen.add_bar(FFMPEG_LOOKUPS, FFMPEG_LABEL);
    ffmpeg_resolve.set_suffix_components(SuffixComponents::status_list(&[StatusCount {
        word: "cached",
        count: Some(2),
    }]));
    ffmpeg_resolve.set_position(FFMPEG_LOOKUPS);
    ffmpeg_resolve.finish_success();
    overall.advance(1);
    screen.tick();
    clock.advance(Duration::from_secs(2));

    // rsgain was already provisioned, so its resolve bar is the only row it has.
    let rsgain_resolve = screen.add_bar(1, RSGAIN_LABEL);
    rsgain_resolve.set_suffix_components(SuffixComponents::status_list(&[
        StatusCount { word: "skipped", count: None },
        StatusCount { word: "cached", count: Some(1) },
    ]));
    rsgain_resolve.set_position(1);
    rsgain_resolve.finish_success();
    overall.advance(1);
    screen.tick();
    clock.advance(Duration::from_secs(1));

    // deno is the sub-bar case: its fetch and process bars open while its own
    // resolve bar is still running, so they sit under a row that has not
    // finished rather than beside one that has.
    let deno_resolve = screen.add_bar(1, DENO_LABEL);

    let deno_fetch = screen.add_bar(2, DENO_FETCH_LABEL);
    set_phase_components(&deno_fetch, "deno", DENO_VERSION, "fch", 1, 2);
    deno_fetch.set_total(DENO_FETCH_BYTES);
    deno_fetch.set_position(DENO_FETCH_BYTES / 3);
    screen.tick();
    clock.advance(Duration::from_secs(3));

    deno_fetch.set_position(DENO_FETCH_BYTES);
    deno_fetch.finish_success();

    let deno_process = screen.add_bar(DENO_PROCESS_ITEMS, DENO_PROCESS_LABEL);
    set_phase_components(&deno_process, "deno", DENO_VERSION, "pro", 1, DENO_PROCESS_ITEMS);
    deno_process.set_position(1);
    screen.tick();
    clock.advance(Duration::from_secs(2));

    deno_process.set_position(DENO_PROCESS_ITEMS);
    deno_process.finish_success();
    deno_resolve.finish_success();
    overall.advance(1);
    screen.tick();

    // sd warns and then fails, which is the pair of terminal states the other
    // two scenarios leave out. The status alone puts the `[W]` and the `[F]` on
    // the row: the renderer folds it into the prefix marker before drawing.
    let sd_resolve = screen.add_bar(1, SD_LABEL);
    set_phase_components(&sd_resolve, "sd", SD_VERSION, "res", 1, 1);
    sd_resolve.set_position(1);
    sd_resolve.finish_warning();

    let sd_fetch = screen.add_bar(1, SD_FETCH_LABEL);
    set_phase_components(&sd_fetch, "sd", SD_VERSION, "fch", 0, 1);
    sd_fetch.set_position(0);
    sd_fetch.finish_error();
    screen.tick();

    let prune = screen.add_bar(2, "pruning [prn]");
    prune.advance(1);
    prune.finish_success();

    // A tool failed, so the overall bar warns, which is what the coordinator
    // does whenever `report.warnings` is not empty (`sync/mod.rs:1364`).
    overall.set_position(TOOLS_COMPLETED);
    overall.finish_warning();
    screen.join();
    drop(terminal);
    grid.contents()
}

/// Seed for media-tagger's resolve bar in the `states` scenario.
///
/// Its `human_readable_version` is the package version followed by the build's
/// git hash (`tools/provider/mod.rs:462`), so the label runs to sixty-six
/// columns. The hash is spelled out rather than abbreviated because how much of a
/// version survives the shrink is what this row is there to show.
const MEDIA_TAGGER_LABEL: &str =
    "media-tagger 0.1.0+8f3a1c07d5b2e6940af17c3d82b5e6f0a49d7c1b3 [res]";

/// Seed for ffmpeg's resolve bar in the `states` scenario.
///
/// ffmpeg reports its `BtbN` autobuild tag and the evermeet version as one
/// human-readable version (`tools/provider/mod.rs:391`).
const FFMPEG_LABEL: &str = "ffmpeg autobuild-2025-10-03-12-00+evermeet-8.1.2 [res]";

/// Seed for rsgain's resolve bar in the `states` scenario.
const RSGAIN_LABEL: &str = "rsgain v2.24.1 [res]";

/// Seed for deno's resolve bar in the `states` scenario.
const DENO_LABEL: &str = "deno v2.1.4 [res]";

/// Seed for deno's fetch bar in the `states` scenario.
const DENO_FETCH_LABEL: &str = "deno v2.1.4 [fch]";

/// Seed for deno's process bar in the `states` scenario.
const DENO_PROCESS_LABEL: &str = "deno v2.1.4 [pro]";

/// Version the `states` deno rows carry, kept apart from the seeds so the three
/// of them cannot disagree about it.
const DENO_VERSION: &str = "v2.1.4";

/// Seed for sd's resolve bar in the `states` scenario.
const SD_LABEL: &str = "sd v1.10.0 [res]";

/// Seed for sd's fetch bar in the `states` scenario.
const SD_FETCH_LABEL: &str = "sd v1.10.0 [fch]";

/// Version the `states` sd rows carry.
const SD_VERSION: &str = "v1.10.0";

/// Push a tool, a version, a phase and an item tally into a bar's built-in prefix.
///
/// This is the call the provider callback in `provision.rs:297` makes on every
/// frame, with the tally the callback was handed. The marker stays empty because
/// the renderer derives it from the bar's terminal status.
fn set_phase_components(
    bar: &impl ProgressBarApi,
    tool: &str,
    version: &str,
    phase: &str,
    count: u64,
    total: u64,
) {
    bar.set_prefix_components(PrefixComponents {
        marker: String::new(),
        tool_name: tool.to_string(),
        version: version.to_string(),
        phase: phase.to_string(),
        count: count.to_string(),
        total: total.to_string(),
    });
}
