//! End-to-end coverage for the `--no-progress` CLI flag.
//!
//! The two unit tests in `src/mediapm/src/service.rs` cover terminal
//! selection: `no_progress_selects_an_inert_terminal` proves the flag picks an
//! inert terminal, and `injected_terminal_is_used_as_given` proves the
//! selection helper is not simply always-inert. Neither one runs the binary,
//! so neither can catch the flag failing to survive the trip from the command
//! line into the sync, a phase that ignores it, or a terminal that leaks a
//! frame through a path the flag does not reach. Reading what the real binary
//! wrote to stderr is what closes that gap.
//!
//! Non-vacuity is the difficulty here. A subprocess's stderr is a pipe rather
//! than a TTY, so one might expect indicatif to hide bars on its own and let
//! "no frame on stderr" pass for the wrong reason. It does not hide them:
//! this crate builds its default draw target with
//! `ProgressDrawTarget::term_like(..)`, and that target kind skips the
//! `is_term` check `ProgressDrawTarget::stderr()` applies, while `console::Term`
//! writes the bytes to fd 2 either way. A piped run without the flag really
//! does paint. `sync_draws_a_frame_on_stderr_when_progress_is_allowed` pins
//! that control run, so the empty stderr of the suppressed run reads as "the
//! flag worked" rather than "nothing drew".

use std::path::Path;
use std::process::{Command, Output};

/// A V2 document with no media, no hierarchy and no tools. A sync over it
/// still walks all three phases, so all three are phases that would draw,
/// and with no tools to provision it performs no payload fetch and no
/// metadata lookup.
const MINIMAL_CONFIG: &str = "{ version = 2 }\n";

/// Substring the post-sync summary prints on stdout once the sync finishes.
/// Its presence proves the run reached the end of the pipeline rather than
/// failing before the phases that would have drawn.
const SYNC_COMPLETE_MARKER: &str = "sync complete";

/// Creates an isolated workspace holding [`MINIMAL_CONFIG`].
///
/// The returned `TempDir` owns the tree; keep it bound for the whole test so
/// the workspace is removed on drop.
fn isolated_workspace() -> tempfile::TempDir {
    let workspace = mediapm_utils::temp::artifact_dir().expect("artifact dir");
    std::fs::write(workspace.path().join("mediapm.ncl"), MINIMAL_CONFIG)
        .expect("write minimal mediapm.ncl");
    workspace
}

/// Runs `mediapm --root <workspace> sync`, optionally with `--no-progress`,
/// and returns the captured process output.
///
/// Isolation has two halves. The child gets a fresh `HOME` and no inherited
/// `XDG_CACHE_HOME`, so `dirs::cache_dir()` resolves under `cache_home` on
/// macOS and on Linux and the tool download cache stays off the real OS
/// cache. Every ambient `MEDIAPM_*` variable is removed, so an inherited
/// `MEDIAPM_ROOT` or `MEDIAPM_PROGRESS_DEBUG` in the test process cannot
/// steer the child. `PATH` and the rest of the environment are left alone so
/// the binary resolves the same tools it normally would.
fn run_sync(workspace: &Path, cache_home: &Path, no_progress: bool) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_mediapm"));
    command.arg("--root").arg(workspace).arg("sync");
    if no_progress {
        command.arg("--no-progress");
    }
    command.env("HOME", cache_home).env_remove("XDG_CACHE_HOME");
    for (key, _) in std::env::vars_os() {
        if key.to_string_lossy().starts_with("MEDIAPM_") {
            command.env_remove(&key);
        }
    }
    command.output().expect("mediapm binary should run")
}

/// True when `stream` carries a drawn progress frame.
///
/// The renderer is the only writer of block-fill and braille-spinner
/// characters: bar fills use `█` (`U+2588`) and `░` (`U+2591`), and every row
/// leads with a spinner drawn from the braille block (`U+2800..=U+28FF`).
/// Result lines, warnings and hints use other glyphs (`✓`, `–`, `Δ`, `✗`,
/// `→`), so keying on this character class measures a drawn frame without a
/// styled warning being mistaken for one.
fn stream_carries_drawn_frame(stream: &str) -> bool {
    stream
        .chars()
        .any(|c| matches!(c, '\u{2588}' | '\u{2591}') || ('\u{2800}'..='\u{28ff}').contains(&c))
}

/// True when `stream` carries a Rust panic marker.
fn stream_carries_panic(stream: &str) -> bool {
    stream.contains("panicked") || stream.contains("RUST_BACKTRACE")
}

/// A real `mediapm sync --no-progress` run paints no frame to stderr.
///
/// This is the end-to-end half of the `--no-progress` contract. It runs the
/// built binary against an isolated workspace and reads the bytes the process
/// wrote to fd 2. The unit tests stop at terminal selection inside the
/// library; only running the binary shows that the flag survives argument
/// parsing and reaches every phase of the sync.
///
/// The assertion is that no frame was drawn, measured by the renderer's own
/// glyph class rather than by the absence of one label string. The run is
/// non-vacuous because the same binary over the same config with the flag
/// omitted does paint; see
/// `sync_draws_a_frame_on_stderr_when_progress_is_allowed`.
#[test]
fn sync_draws_no_frame_on_stderr_with_no_progress() {
    let workspace = isolated_workspace();
    let cache_home = mediapm_utils::temp::cache_dir().expect("cache dir");

    let output = run_sync(workspace.path(), cache_home.path(), true);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success(), "sync --no-progress must succeed, stderr:\n{stderr}");
    assert!(
        stdout.contains(SYNC_COMPLETE_MARKER),
        "the sync must reach its summary, so the phases that would have drawn all ran; stdout:\n{stdout}"
    );
    assert!(
        !stream_carries_drawn_frame(&stderr),
        "--no-progress must leave no drawn frame on stderr, got:\n{stderr}"
    );
    assert!(
        !stream_carries_panic(&stderr) && !stream_carries_panic(&stdout),
        "no panic may appear in the captured streams; stderr:\n{stderr}\nstdout:\n{stdout}"
    );
}

/// The control run: the same binary and config without `--no-progress` does
/// paint a frame to the same piped stderr.
///
/// This is what keeps the suppressed run meaningful. A test that only asserted
/// "no frame without the flag" would also pass if the sync had stopped drawing
/// altogether, or if the process never reached a progress screen. Here the
/// identical invocation minus the flag produces a frame, so the flag is the
/// only thing that can account for the empty stderr in
/// `sync_draws_no_frame_on_stderr_with_no_progress`.
#[test]
fn sync_draws_a_frame_on_stderr_when_progress_is_allowed() {
    let workspace = isolated_workspace();
    let cache_home = mediapm_utils::temp::cache_dir().expect("cache dir");

    let output = run_sync(workspace.path(), cache_home.path(), false);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(output.status.success(), "sync must succeed, stderr:\n{stderr}");
    assert!(
        stream_carries_drawn_frame(&stderr),
        "this control run must paint a frame on a piped stderr; if it does not, \
         the suppressed run proves nothing, because the draw path is already \
         inert without the flag. stderr:\n{stderr}"
    );
}
