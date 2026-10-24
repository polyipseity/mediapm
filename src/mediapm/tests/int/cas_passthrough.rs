//! `mediapm cas <sub>` reaches the CAS CLI through the in-process passthrough.
//!
//! The entry point supplies the program name clap reads from position zero, and
//! the passthrough supplies a default for a flag the CAS CLI declares. Neither
//! is visible to a test of the injection alone, and the second only matters once
//! the first is in place. The defect these tests cover
//! survived for exactly that reason: the only coverage was the help route, which
//! short-circuits before the injection and before the parse, so the one path
//! that could not fail was the one path that was tested.
//!
//! These tests spawn the built binary rather than calling the library entry
//! point, because the defect was in what the binary passed to that entry point.

use std::process::Command;

/// Returns the CAS command's registered name.
///
/// This is the name the completion generator registers its function under, and
/// it is derived from the definition rather than spelled out, so renaming the
/// command moves this test with it instead of failing for the wrong reason.
fn cas_command_name() -> String {
    mediapm_cas_cli::command().get_name().to_owned()
}

/// Returns the top-level subcommand names the CAS definition declares and
/// exposes.
///
/// Hidden subcommands are excluded because the completion generator omits them,
/// so requiring one in the script would fail for a reason that says nothing
/// about the passthrough.
fn visible_cas_subcommands() -> Vec<String> {
    mediapm_cas_cli::command()
        .get_subcommands()
        .filter(|subcommand| !subcommand.is_hide_set())
        .map(|subcommand| subcommand.get_name().to_owned())
        .collect()
}

/// Runs the built `mediapm` binary with `args` after `cas`, under a fresh root.
///
/// Each run gets a workspace root of its own: the passthrough injects a store
/// path beneath the root and opens it, and a CAS store takes an exclusive lock,
/// so a shared root would make two runs contend rather than test.
///
/// Returns the exit status, standard output and standard error as separate
/// values, because the three answer different questions and a test that folded
/// them together could not say which failed.
fn run_cas(args: &[&str]) -> (std::process::ExitStatus, String, String) {
    let root = mediapm_utils::temp::artifact_dir().expect("artifact dir");
    let output = Command::new(env!("CARGO_BIN_EXE_mediapm"))
        .arg("--root")
        .arg(root.path())
        .arg("cas")
        .args(args)
        .output()
        .expect("the mediapm binary should run");
    (
        output.status,
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

/// A CAS subcommand reaches its handler through `mediapm cas`.
///
/// The exit status carries most of the weight here. The passthrough injects
/// `--storage <path>` ahead of the caller's arguments and the entry point
/// supplies the program name, so a successful run means clap accepted both: a
/// vector missing the program name has its first argument read as the program
/// name, and a vector naming a flag the CAS CLI does not declare is rejected as
/// a usage error. Neither failure can produce a completion script.
///
/// The two content assertions then say which CLI answered. The registered
/// function name is the half a shell needs: a script that registers under
/// another name completes nothing while reporting no error. Each subcommand the
/// definition exposes is then required to appear as a word of its own, so the
/// check depends neither on the order the generator emits them in nor on a
/// script that mentions the names only inside longer words.
#[test]
fn cas_subcommand_reaches_its_handler_through_the_binary() {
    let (status, stdout, stderr) = run_cas(&["completions", "bash"]);
    assert!(
        status.success(),
        "`mediapm cas completions bash` must succeed, because the passthrough injects a flag the \
         CAS CLI declares and the entry point supplies the program name; exited with {status}\n\
         stderr: {stderr}"
    );

    let name = cas_command_name();
    assert!(
        stdout.contains(&format!("_{name}(")),
        "the script must register `_{name}(`, the CAS command's own name rather than another \
         CLI's; stdout: {stdout}"
    );

    let subcommands = visible_cas_subcommands();
    assert!(
        !subcommands.is_empty(),
        "the CAS definition must declare subcommands, or the loop below would pass over an empty \
         set"
    );
    let tokens: Vec<&str> =
        stdout.split(|c: char| !(c.is_alphanumeric() || c == '-' || c == '_')).collect();
    for subcommand in &subcommands {
        assert!(
            tokens.contains(&subcommand.as_str()),
            "the script must name the CAS subcommand `{subcommand}` as a word of its own, which is \
             how the generator renders the command tree it was given; stdout: {stdout}"
        );
    }
}
