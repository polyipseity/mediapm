//! `mediapm conductor <args>` reaches the conductor CLI through the in-process
//! passthrough.
//!
//! clap hands help and version back as errors rather than printing them, so a
//! passthrough that returns `Ok(())` on that arm reports success while writing
//! nothing. No assertion inside the process can see that, because the symptom is
//! the absence of output. These tests spawn the built binary and read what it
//! wrote, which is the property a user observes.

use std::process::Command;

/// Runs the built `mediapm` binary with `args` after `conductor`, under a fresh
/// root.
///
/// The root is fresh per run for the reason the CAS tests use one: the
/// passthrough resolves runtime paths beneath it, and two runs sharing a root
/// would contend rather than test.
///
/// Returns the exit status, standard output and standard error as separate
/// values, because "printed nothing" and "failed" are different diagnoses and a
/// test that folded them together could not say which it had found.
fn run_conductor(args: &[&str]) -> (std::process::ExitStatus, String, String) {
    let root = mediapm_utils::temp::artifact_dir().expect("artifact dir");
    let output = Command::new(env!("CARGO_BIN_EXE_mediapm"))
        .arg("--root")
        .arg(root.path())
        .arg("conductor")
        .args(args)
        .output()
        .expect("the mediapm binary should run");
    (
        output.status,
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

/// A help request through the passthrough prints the help it was asked for.
///
/// The exit status alone cannot tell the two behaviours apart, because both
/// report success: the defect this covers returned `Ok(())` having written zero
/// bytes. The assertion is therefore on the text, and on it being clap's
/// rendering rather than an empty string that a status check would accept.
#[test]
fn conductor_help_passthrough_prints_the_help_it_was_asked_for() {
    let (status, stdout, stderr) = run_conductor(&["tool", "run", "--help"]);

    assert!(status.success(), "a help request must not fail; stderr: {stderr}");
    assert!(
        !stdout.trim().is_empty(),
        "the passthrough must print the help it was asked for rather than report success \
         silently; stdout: {stdout:?} stderr: {stderr:?}"
    );
    assert!(
        stdout.contains("Usage:"),
        "the printed text must be clap's rendering of the command, which carries a usage \
         line; stdout: {stdout}"
    );
}
