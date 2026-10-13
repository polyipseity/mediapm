//! What `mediapm tool sync` returns to the shell.
//!
//! The unit test `tool_sync_exits_three_when_a_tool_failed_to_provision` reads
//! a summary the test wrote by hand, so it cannot notice the command leaving by
//! another route, and it cannot see a warning line that never got printed. A
//! status is a promise to a script, so the promise is checked against a real
//! process here.
//!
//! Both workspaces run offline. The error comes from a tool id no provider
//! answers to, which fails at resolution before any download is attempted, so
//! the case is reachable without a network. A provisioning failure that got as
//! far as fetching a payload is not, and is left to the unit test.
//!
//! | status | meaning |
//! | --- | --- |
//! | 0 | every desired tool registered |
//! | 1 | a tool failed to register (provisioning failures are fatal, fail fast) |
//! | 4 | not used by this command: nothing in the library is missing |

use std::path::Path;
use std::process::Output;

use super::subprocess::run_tool_sync;
use mediapm::{ConfigVersionSpec, MediaPmDocument, ToolRequirement, save_mediapm_document};

/// Tool id no provider is registered for.
///
/// `resolve_tool_fetch` matches the id against the six managed tools and
/// returns an error for anything else, before it opens a socket. Under fail
/// fast that error aborts the whole sync, so this workspace reaches the error
/// status with no payload fetched.
const UNRESOLVABLE_TOOL: &str = "nonexistent-tool";

/// Substring the summary line prints on stdout once the tool sync finishes.
const TOOLS_SYNCED_MARKER: &str = "tools synced";

/// Exit status for a fatal error. A provisioning failure (resolve/fetch/
/// process) aborts the whole sync, so an unresolvable tool leaves here.
const EXIT_ERROR: i32 = 1;

/// Exit status the whole-sync command uses for a library left short. This
/// command must never produce it.
const EXIT_INCOMPLETE_LIBRARY: i32 = 4;

/// Writes a document into a fresh workspace.
///
/// `tools` is the only field any test here sets, so a workspace differs from
/// the next in the tool list alone.
fn workspace_with_tools(tool_ids: &[&str]) -> tempfile::TempDir {
    let workspace = mediapm_utils::temp::artifact_dir().expect("artifact dir");
    let document = MediaPmDocument {
        tools: tool_ids
            .iter()
            .map(|id| {
                (
                    (*id).to_string(),
                    ToolRequirement {
                        version_spec: ConfigVersionSpec::Latest,
                        ..ToolRequirement::default()
                    },
                )
            })
            .collect(),
        ..MediaPmDocument::default()
    };
    save_mediapm_document(&workspace.path().join("mediapm.ncl"), &document)
        .expect("the document is written into the workspace");
    workspace
}

/// Runs a tool sync over `workspace` with progress suppressed, so the captured
/// streams carry the summary line and its warnings and nothing else.
fn tool_sync(workspace: &Path, cache_home: &Path) -> Output {
    run_tool_sync(workspace, cache_home, true)
}

/// Renders an [`std::process::Output`] as a diagnostic naming both streams and
/// the status.
fn describe(what: &str, output: &Output) -> String {
    format!(
        "{what}\nstatus: {:?}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    )
}

/// A tool sync that registered everything the config asked for exits zero.
///
/// The control the other test reads against. A rule that warned on every tool
/// sync satisfies the warning test without this one.
#[test]
fn tool_sync_exits_zero_when_every_tool_registered() {
    let workspace = workspace_with_tools(&[]);
    let cache_home = mediapm_utils::temp::cache_dir().expect("cache dir");

    let output = tool_sync(workspace.path(), cache_home.path());
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert_eq!(
        output.status.code(),
        Some(0),
        "{}",
        describe("a clean tool sync must exit 0", &output)
    );
    assert!(
        stdout.contains(TOOLS_SYNCED_MARKER),
        "the run has to reach its summary, or exit zero reads as a command that never ran; \
         stdout:\n{stdout}"
    );
}

/// A tool the providers do not know aborts the sync with an error status.
///
/// Provisioning failures are fatal (fail fast): the reconcile loop aborts on
/// the first unresolvable tool, so the command exits with an error status
/// rather than the warning status it used to leave behind. The error text is
/// asserted alongside the status because a status alone cannot say which tool
/// failed.
#[test]
fn tool_sync_exits_error_when_a_tool_fails_to_register() {
    let workspace = workspace_with_tools(&[UNRESOLVABLE_TOOL]);
    let cache_home = mediapm_utils::temp::cache_dir().expect("cache dir");

    let output = tool_sync(workspace.path(), cache_home.path());
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert_eq!(
        output.status.code(),
        Some(EXIT_ERROR),
        "{}",
        describe(
            "a tool that failed to register must abort the sync with an error status",
            &output
        )
    );
    assert!(
        stderr.contains(UNRESOLVABLE_TOOL) && stderr.contains("no provider registered"),
        "a user told only that something failed cannot know which tool or why, so the error \
         line has to carry both the tool id and the underlying cause; stderr:\n{stderr}"
    );
}

/// The same run does not take the status the whole-sync command uses for a
/// library left short.
///
/// This is the mistake the warning status is easy to make. A tool-sync
/// failure — now fatal — is a provisioning failure, not an incomplete library,
/// so status 4 would tell a script the library is missing entries it never
/// wanted.
#[test]
fn tool_sync_does_not_exit_four_when_a_tool_failed_to_register() {
    let workspace = workspace_with_tools(&[UNRESOLVABLE_TOOL]);
    let cache_home = mediapm_utils::temp::cache_dir().expect("cache dir");

    let output = tool_sync(workspace.path(), cache_home.path());

    assert_ne!(
        output.status.code(),
        Some(EXIT_INCOMPLETE_LIBRARY),
        "{}",
        describe("a tool-sync warning is not an incomplete library", &output)
    );
}
