//! The completion scripts a build script writes agree with the CLIs they describe.
//!
//! Completion generation has two paths. Each build script renders its CLI's
//! scripts into `OUT_DIR/completions`, and each binary renders the same scripts
//! to stdout for its `completions` subcommand. They agree only while both name
//! the command the same way and cover the same shells, and nothing in the
//! compiler connects the two.
//!
//! This needs a test rather than a convention because generation used to be
//! gated on the release profile, so a debug build — and CI, which built debug —
//! never ran it. A CLI whose entry point had been broken for three months
//! shipped, because the only thing that exercised generation was a release
//! build nobody ran. `ci.yml` now builds the CLI-carrying packages in release
//! as its own job, and this is the test that fails when the two paths drift.
//!
//! # Scope
//!
//! `CARGO_BIN_EXE_*` exposes the current package's binaries alone, so only
//! `mediapm`'s binary is reachable from this test binary. The other two CLIs'
//! on-the-fly paths are covered by their own crates' tests. What this file adds
//! for all three is the generation surface, which is the half no crate's unit
//! tests reach: it renders through the same `build_utils` entry point the build
//! scripts call.

use std::collections::BTreeSet;
use std::path::Path;
use std::process::Command;

use clap::ValueEnum;
use clap_complete::Shell;
use mediapm_utils::temp::artifact_dir;

/// One CLI the workspace ships, paired with the file-name stem its build script
/// passes and the command definition that build script renders.
///
/// The two are separate fields because they are separate things: the stem names
/// the files, and the command's own name is what the script registers its
/// completion function under. They differ wherever a binary's file name is not
/// its command's name, which is the conductor's case.
struct ShippedCli {
    /// Stem the build script names the generated files after, taken from the
    /// `[[bin]]` name rather than from the command.
    bin_name: &'static str,
    /// The command definition the build script renders, as the CLI crate
    /// exposes it. A function pointer rather than a value, so each test starts
    /// from a definition no earlier test has rendered.
    command: fn() -> clap::Command,
}

/// Every CLI whose completions a build script writes.
///
/// The order is the order the matrix is reported in; nothing depends on it.
const SHIPPED_CLIS: &[ShippedCli] = &[
    ShippedCli { bin_name: "mediapm", command: mediapm_cli::command },
    ShippedCli { bin_name: "mediapm-cas", command: mediapm_cas_cli::command },
    ShippedCli { bin_name: "mediapm-conductor", command: mediapm_conductor_cli::command },
];

/// The CLI whose binary this test binary can spawn, by index into
/// [`SHIPPED_CLIS`]. Named rather than repeated as a literal so a reordering of
/// the list cannot silently point the binary comparison at another CLI.
const MEDIAPM_INDEX: usize = 0;

/// CLIs whose `completions` argument takes a free-form string instead of a
/// `clap_complete::Shell`.
///
/// `mediapm-cas` is the one, and it is a divergence rather than a design: the
/// other two declare the shell as `clap_complete::Shell`, so their accepted
/// values are readable and the shells they advertise cannot drift from the
/// shells generation writes. The CAS CLI hand-matches five string arms instead,
/// which no test can read. Its own binary still errors loudly on an unsupported
/// shell, so nothing is silently accepted; what is missing is only the ability
/// to compare its list against the generated one.
///
/// The list is an exception that expires: [`declared_completion_shells_match_the_generated_ones`]
/// fails when a CLI named here starts declaring its values, which is the fix
/// that would remove it from this list.
const FREE_FORM_SHELL_ARGUMENT: &[&str] = &["mediapm-cas"];

/// Renders every shipped CLI's completions the way the build scripts do, into
/// `dir`.
///
/// All three render into one directory, so the result is the full matrix of
/// shells times binaries rather than one CLI's scripts. The caller owns the
/// directory, which keeps this helper from naming a temporary-directory type of
/// its own.
///
/// # Panics
///
/// Panics when a rendering fails, which is `generate_completions`'s own
/// contract: a build script has no way to report a recoverable failure. A panic
/// also leaves `OUT_DIR` set, because the removal below runs on the success
/// path; nothing else in this test binary reads it, and every caller sets it
/// again before rendering.
fn generate_all(dir: &Path) {
    // `generate_completions` reads `OUT_DIR`, which Cargo sets only for a build
    // script. The guard is the process-wide environment lock, held across the
    // mutation and the removal so no sibling test can observe a half-installed
    // value. `lock_process_env` is the acquiring entry point; `env_lock` only
    // lends the same mutex to a caller that has to name it, so binding its
    // return value would hold no lock at all.
    let _guard = mediapm::example_isolation::lock_process_env();
    // SAFETY: the environment lock above is held for the whole mutation, and
    // `OUT_DIR` is removed again before the guard drops. Edition 2024 makes
    // `set_var` unsafe because a concurrent read of the environment from
    // another thread is undefined behaviour; the lock is what rules that out.
    unsafe { std::env::set_var("OUT_DIR", dir) };
    for cli in SHIPPED_CLIS {
        let mut command = (cli.command)();
        build_utils::generate_completions(&mut command, cli.bin_name);
    }
    unsafe { std::env::remove_var("OUT_DIR") };
}

/// The shells the completion generator itself can emit.
///
/// This is the independent half of the comparison in
/// [`generated_scripts_cover_every_shell_the_generator_supports`]: it comes from
/// `clap_complete`'s own enum rather than from the list generation iterates, so
/// the two agree only while the workspace covers every shell the generator
/// offers.
fn supported_shells() -> BTreeSet<String> {
    Shell::value_variants().iter().map(ToString::to_string).collect()
}

/// The shells the CLI declares it can emit completions for, or `None` when its
/// `completions` argument does not declare them.
///
/// Read from the CLI's own argument rather than from the generation code, so
/// the comparison in
/// [`declared_completion_shells_match_the_generated_ones`] is between two
/// independent statements of the same fact. A list derived from the generator
/// would agree with the generator by construction.
///
/// # Panics
///
/// Panics when the CLI declares no `completions` subcommand or no `shell`
/// argument, which would make the comparison below meaningless and so is a
/// failure to surface rather than a case to skip.
fn declared_shells(cli: &ShippedCli) -> Option<BTreeSet<String>> {
    let command = (cli.command)();
    let completions = command
        .get_subcommands()
        .find(|subcommand| subcommand.get_name() == "completions")
        .unwrap_or_else(|| panic!("{} declares no `completions` subcommand", cli.bin_name));
    let shell_argument = completions
        .get_arguments()
        .find(|argument| *argument.get_id() == "shell")
        .unwrap_or_else(|| panic!("{}'s `completions` takes no `shell` argument", cli.bin_name));
    let possible = shell_argument.get_possible_values();
    if possible.is_empty() {
        return None;
    }
    Some(possible.into_iter().map(|value| value.get_name().to_string()).collect())
}

/// The shells a build script wrote scripts for, read back from the file names.
///
/// # Panics
///
/// Panics when the directory cannot be read, which means generation reported
/// success without writing, and when it holds no file for the CLI at all.
fn generated_shells(dir: &Path, bin_name: &str) -> BTreeSet<String> {
    let completions = dir.join("completions");
    let prefix = format!("{bin_name}.");
    let mut shells = BTreeSet::new();
    for entry in std::fs::read_dir(&completions)
        .unwrap_or_else(|error| panic!("read {}: {error}", completions.display()))
    {
        let file_name = entry.expect("dir entry").file_name().to_string_lossy().into_owned();
        if let Some(shell) = file_name.strip_prefix(&prefix) {
            shells.insert(shell.to_owned());
        }
    }
    assert!(
        !shells.is_empty(),
        "generation wrote no script named `{prefix}*` into {}",
        completions.display()
    );
    shells
}

/// Reads one generated script, reporting its path on failure.
fn read_script(dir: &Path, file_name: &str) -> String {
    let path = dir.join("completions").join(file_name);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

/// Generation writes a script for every shell the generator can emit, for every
/// CLI.
///
/// The two sides come from different places on purpose: the shells generated
/// are the names of the files a build script wrote, and the shells the generator
/// supports are `clap_complete`'s own variants. A shell the generator gains and
/// the workspace does not cover ships a CLI whose completion silently does
/// nothing for the shells a user would reach for — silent, because neither path
/// reports an error for a shell it was never asked about.
#[test]
fn generated_scripts_cover_every_shell_the_generator_supports() {
    let dir = artifact_dir().expect("artifact dir");
    generate_all(dir.path());
    for cli in SHIPPED_CLIS {
        assert_eq!(
            generated_shells(dir.path(), cli.bin_name),
            supported_shells(),
            "the shells generation writes for {} must cover every shell the generator supports",
            cli.bin_name
        );
    }
}

/// A CLI that declares the shells it accepts declares exactly the shells
/// generation writes.
///
/// A shell the CLI advertises and generation does not write is a completion a
/// user can ask for and never get; one generation writes and the CLI rejects is
/// a file no invocation can reproduce. The exception is named in
/// [`FREE_FORM_SHELL_ARGUMENT`], and a CLI that stops being an exception fails
/// here, so the list cannot outlive the divergence it records.
#[test]
fn declared_completion_shells_match_the_generated_ones() {
    let dir = artifact_dir().expect("artifact dir");
    generate_all(dir.path());
    for cli in SHIPPED_CLIS {
        let is_free_form = FREE_FORM_SHELL_ARGUMENT.contains(&cli.bin_name);
        match declared_shells(cli) {
            Some(declared) => {
                assert!(
                    !is_free_form,
                    "{} declares its shells, so it is no longer an exception and must be removed \
                     from FREE_FORM_SHELL_ARGUMENT",
                    cli.bin_name
                );
                assert_eq!(
                    declared,
                    generated_shells(dir.path(), cli.bin_name),
                    "{} declares shells that generation must write",
                    cli.bin_name
                );
            }
            None => assert!(
                is_free_form,
                "{} declares no shells and is not a recorded exception, so nothing compares its \
                 accepted shells against the generated ones",
                cli.bin_name
            ),
        }
    }
}

/// The script registers its completion function under the command's name, not
/// under the file name it was written to.
///
/// A shell sources a script and then calls the function it registered, so a
/// script that registers under the wrong name completes nothing while reporting
/// no error — the failure looks exactly like completion being unimplemented.
/// The conductor is where the two names differ: its file is
/// `mediapm-conductor.bash` and the function inside is `_conductor`.
#[test]
fn generated_scripts_register_under_the_command_name() {
    let dir = artifact_dir().expect("artifact dir");
    generate_all(dir.path());
    for cli in SHIPPED_CLIS {
        let command = (cli.command)();
        let name = command.get_name().to_owned();
        let script = read_script(dir.path(), &format!("{}.bash", cli.bin_name));
        assert!(
            script.contains(&format!("_{name}(")),
            "{}'s script must register `_{name}(`, got: {}",
            cli.bin_name,
            script.chars().take(120).collect::<String>()
        );
        if name != cli.bin_name {
            assert!(
                !script.contains(&format!("_{}(", cli.bin_name)),
                "{}'s script must not register under its file name, which is not the command's",
                cli.bin_name
            );
        }
    }
}

/// The binary's own `completions` path renders what the build script wrote.
///
/// The two paths share a generator but not a name: the build script takes it
/// from the command, while each binary's handler passes its own. A handler
/// passing a stale literal would keep working for every shell a user runs it
/// with, and only disagree with the installed scripts — which is the
/// disagreement this asserts against, byte for byte.
#[test]
fn the_binary_renders_what_the_build_wrote() {
    let dir = artifact_dir().expect("artifact dir");
    generate_all(dir.path());
    let cli = &SHIPPED_CLIS[MEDIAPM_INDEX];
    for shell in declared_shells(cli).expect("mediapm declares its completion shells") {
        let generated = read_script(dir.path(), &format!("{}.{shell}", cli.bin_name));
        let output = Command::new(env!("CARGO_BIN_EXE_mediapm"))
            .args(["completions", &shell])
            .output()
            .expect("run the mediapm binary");
        assert!(
            output.status.success(),
            "`completions {shell}` failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            String::from_utf8_lossy(&output.stdout),
            generated,
            "the binary's {shell} script must be the one the build script wrote"
        );
    }
}
