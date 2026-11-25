//! The CAS binary's own entry point reaches its subcommands.
//!
//! `mediapm-cas` renders completion scripts from two places: its build script,
//! which writes them into `OUT_DIR` for installation, and its own `completions`
//! subcommand, which renders them to stdout on demand. Both drive the same
//! `clap_complete` generator over the same command definition, and nothing in
//! the compiler connects the two, so they agree only while each names the
//! command the same way.
//!
//! The on-the-fly half had no coverage: this crate's other integration tests
//! drive the library API, and the definition crate's unit tests parse the
//! command tree without ever spawning the binary. A handler that named the
//! command wrongly would still exit successfully and still print a script, and
//! a shell sourcing that script would register a completion function no
//! invocation reaches — a failure that looks exactly like completion being
//! unimplemented.
//!
//! These tests spawn the built binary, because that is the only way to reach the
//! path a user takes. Cargo builds the binary only when the `cli` feature is on,
//! so the module is gated to match.

use std::collections::BTreeSet;
use std::process::Command;

use bytes::Bytes;
use mediapm_cas::api::{CasApi, CasMaintenanceApi, ConstraintApi};
use mediapm_cas::hash::Hash;

use crate::common;

/// The name this CLI's completion scripts register their completions under.
///
/// Spelled out rather than read from `mediapm_cas_cli::command()`, because a
/// name derived from the code under test agrees with that code by construction.
/// This is the name a user types, and the name the build script bakes into the
/// installed scripts, so it is the independent fact the binary has to match. The
/// definition crate pins it with `#[command(name = "mediapm-cas")]`; its unit
/// tests assert the pin, and these assert the binary honours it.
const COMMAND_NAME: &str = "mediapm-cas";

/// The defining crate's name, which is what the command would be called if the
/// `#[command(name = ...)]` pin were dropped.
const DEFINING_CRATE_NAME: &str = "mediapm-cas-cli";

/// A subcommand of this CLI, used to prove a script describes this command
/// rather than an empty or generic one.
const DISTINCTIVE_SUBCOMMAND: &str = "delete";

/// One shell this CLI emits completions for, paired with the text its script
/// uses to register the completion under the command's name.
struct ShellRegistration {
    /// Shell name this CLI accepts as the `completions` argument.
    shell: &'static str,
    /// Text the rendered script must contain, with `{name}` standing in for the
    /// command name. The placeholder keeps the name stated once, in
    /// [`COMMAND_NAME`], so every shell's expectation moves with it.
    registers: &'static str,
}

/// Every shell this CLI advertises, with its own registration form.
///
/// The five shells register in five different ways, so one assertion cannot
/// cover them all: only bash and zsh define a `_<name>` function, fish names the
/// command on every `complete` line, elvish keys an `arg-completer` map by it,
/// and PowerShell passes it to `Register-ArgumentCompleter`. Each entry states
/// the form its own generator emits, so a script that registered under the wrong
/// name fails here whatever shell it is.
const SHELL_REGISTRATIONS: &[ShellRegistration] = &[
    ShellRegistration { shell: "bash", registers: "_{name}()" },
    ShellRegistration { shell: "zsh", registers: "_{name}()" },
    ShellRegistration { shell: "fish", registers: "complete -c {name}" },
    ShellRegistration { shell: "elvish", registers: "arg-completer[{name}]" },
    ShellRegistration { shell: "powershell", registers: "-CommandName '{name}'" },
];

/// What one run of the built binary produced.
struct BinaryRun {
    /// Whether the process exited with a success status.
    success: bool,
    /// Everything the process wrote to standard output.
    stdout: String,
    /// Everything the process wrote to standard error.
    stderr: String,
}

/// Runs the built CAS binary with `args` and captures its result.
///
/// The binary is located through Cargo's `CARGO_BIN_EXE_mediapm-cas`, which
/// Cargo sets for integration tests of the package that declares the binary, so
/// no target-directory path is guessed. A missing binary is a build
/// configuration failure, and `.expect` reports it as one.
fn run_binary(args: &[&str]) -> BinaryRun {
    let output = Command::new(env!("CARGO_BIN_EXE_mediapm-cas"))
        .args(args)
        .output()
        .expect("spawn the mediapm-cas binary");
    BinaryRun {
        success: output.status.success(),
        stdout: String::from_utf8(output.stdout).expect("CAS stdout is utf-8"),
        stderr: String::from_utf8(output.stderr).expect("CAS stderr is utf-8"),
    }
}

/// `completions <shell>` reaches its handler and emits a script for this CLI.
///
/// The handler is reached rather than merely parsed: a completion script is only
/// produced by running it. The assertion names a subcommand of this CLI, so the
/// script cannot be an empty or generic one, and it reads the script rather than
/// only the exit status, because a handler that printed nothing would still exit
/// successfully.
#[test]
fn completions_reaches_its_handler() {
    let run = run_binary(&["completions", "bash"]);
    assert!(run.success, "`completions bash` exited with failure; stderr: {}", run.stderr);
    assert!(
        run.stdout.contains(DISTINCTIVE_SUBCOMMAND),
        "the completion script does not name this CLI's subcommands; stdout: {}",
        run.stdout,
    );
}

/// Every advertised shell renders a script that registers under the command's
/// name.
///
/// A shell sources a script and then calls the completion it registered, so a
/// script registering under another name completes nothing while reporting no
/// error. The command name reaches the script through `Command::get_name`, which
/// is why the definition crate pins it: without that pin the name would be the
/// defining crate's, and every script would register under a name no user's
/// invocation reaches.
#[test]
fn every_advertised_shell_registers_under_the_command_name() {
    for entry in SHELL_REGISTRATIONS {
        let run = run_binary(&["completions", entry.shell]);
        assert!(
            run.success,
            "`completions {}` exited with failure; stderr: {}",
            entry.shell, run.stderr,
        );
        assert!(
            run.stdout.contains(&entry.registers.replace("{name}", COMMAND_NAME)),
            "the {} script must register the completion as `{}`; stdout: {}",
            entry.shell,
            entry.registers.replace("{name}", COMMAND_NAME),
            run.stdout,
        );
        assert!(
            !run.stdout.contains(DEFINING_CRATE_NAME),
            "the {} script registered under the defining crate's name, which is what dropping the \
             `#[command(name = ...)]` pin would produce; stdout: {}",
            entry.shell,
            run.stdout,
        );
    }
}

/// A shell this CLI does not support is rejected rather than silently accepted.
///
/// The `shell` argument is a free-form string, so nothing in the type system
/// restricts it to the shells the generator knows. The handler is the only thing
/// between a typo and a user believing completion is installed, so an
/// unsupported name has to fail loudly instead of emitting an empty script.
#[test]
fn an_unsupported_shell_is_rejected() {
    let run = run_binary(&["completions", "not-a-shell"]);
    assert!(
        !run.success,
        "`completions not-a-shell` must not report success; stdout: {}",
        run.stdout,
    );
    assert!(
        run.stderr.contains("not-a-shell"),
        "the failure must name the shell it rejected; stderr: {}",
        run.stderr,
    );
    assert!(
        run.stdout.is_empty(),
        "a rejected shell must not leave a script on stdout; stdout: {}",
        run.stdout,
    );
}

/// `corrupted` reports every hash the store has already recorded as corrupt.
///
/// The command is a read over recorded state, not a probe: it must name the hash
/// and both candidate paths, because the diagnostic is which file reads wrong and
/// the store cannot say more than "one of these two, or both".
#[tokio::test]
async fn corrupted_lists_recorded_objects() {
    let dir = common::output_dir();
    let target = {
        let cas = common::open_file_cas_verifying_at(dir.path()).await;
        let base = cas.put(Bytes::from(common::similar(b'F', b"CHANGED"))).await.unwrap();
        let target = cas.put(Bytes::from(common::similar(b'F', b"REPLACED"))).await.unwrap();
        cas.set_constraint(base, BTreeSet::from([Hash::empty()])).await.unwrap();
        cas.set_constraint(target, BTreeSet::from([base])).await.unwrap();
        cas.run_maintenance_cycle().await.unwrap();

        let (full, delta) = common::form_paths(&cas, target);
        common::corrupt(&full);
        common::corrupt(&delta);
        assert_eq!(cas.run_maintenance_cycle().await.unwrap().corrupted_objects, 1);
        target
    };

    let storage = dir.path().to_str().expect("artifact dir is utf-8");
    let run = run_binary(&["--storage", storage, "corrupted"]);
    assert!(run.success, "`corrupted` exited with failure; stderr: {}", run.stderr);

    let (full, delta) = {
        let cas = common::open_file_cas_verifying_at(dir.path()).await;
        common::form_paths(&cas, target)
    };
    assert!(
        run.stdout.contains(&target.to_hex()),
        "output must name the corrupt hash; stdout: {}",
        run.stdout
    );
    assert!(
        run.stdout.contains(&format!("  full:  {}", full.display())),
        "output must name the full candidate path; stdout: {}",
        run.stdout
    );
    assert!(
        run.stdout.contains(&format!("  delta: {}", delta.display())),
        "output must name the delta candidate path; stdout: {}",
        run.stdout
    );
}
