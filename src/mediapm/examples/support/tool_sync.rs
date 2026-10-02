//! Screen A: the tool-sync progress screen.
//!
//! Renders what `reconcile_desired_tools` puts on the terminal for three
//! tools, using the captured terminal and synthetic clock the shared harness
//! in `support/mod.rs` builds. The example that pulls this module in is
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

use std::time::Duration;

use mediapm_utils::progress::{PrefixComponents, StatusCount, SuffixComponents};

use crate::support::{ScreenConfig, capture_terminal};

/// Render the tool-sync screen at `config`'s size and return the raw grid.
///
/// The grid keeps its ANSI colour escapes, the same bytes the terminal received.
/// Pass the result through [`strip_ansi_escapes`](crate::support::strip_ansi_escapes)
/// for a readable transcript.
///
/// The sequence mirrors what `reconcile_desired_tools` does for three tools: a
/// resolve bar whose metadata came from the cache, a resolve bar for a tool
/// that was already provisioned, and a full resolve/fetch/process run for the
/// third, followed by the prune bar and the pinned overall bar.
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
    deno_fetch.set_prefix_components(PrefixComponents {
        marker: String::new(),
        tool_name: "deno".to_string(),
        version: "v2.1.4".to_string(),
        phase: "fch".to_string(),
        count: "1".to_string(),
        total: "2".to_string(),
    });
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
    deno_process.set_prefix_components(PrefixComponents {
        marker: String::new(),
        tool_name: "deno".to_string(),
        version: "v2.1.4".to_string(),
        phase: "pro".to_string(),
        count: "2".to_string(),
        total: "3".to_string(),
    });
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
