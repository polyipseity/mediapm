//! Shared build-script utilities for the mediapm workspace.
//!
//! The single entry point is [`generate_completions`], which renders a CLI's
//! shell completion scripts into the build script's `OUT_DIR`. It takes the
//! command definition itself rather than the name of a binary to invoke, so a
//! build script depends on the CLI crate and calls it in process: no
//! subprocess, no second build of the workspace, and no profile check.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use clap::Command;
use clap_complete::Shell;

/// Shells for which completion scripts are generated, paired with the
/// `clap_complete` generator that renders each one.
///
/// Every shell is generated on every call; the order only fixes the order the
/// files are written in.
const SHELLS: &[(&str, Shell)] = &[
    ("bash", Shell::Bash),
    ("elvish", Shell::Elvish),
    ("fish", Shell::Fish),
    ("powershell", Shell::PowerShell),
    ("zsh", Shell::Zsh),
];

/// Renders `command`'s completion script for one shell.
///
/// The name baked into the script comes from [`Command::get_name`] rather than
/// from the file the script is written to, because the two differ wherever a
/// binary's file name is not its command's name: the `mediapm-conductor`
/// binary writes completions for the command `conductor`. That name is what a
/// shell registers the completion function under, so a script carrying the
/// wrong one completes nothing.
fn render(command: &mut Command, shell: Shell) -> String {
    let name = command.get_name().to_string();
    let mut buf = Vec::new();
    clap_complete::generate(shell, command, &name, &mut buf);
    String::from_utf8(buf).expect("clap_complete renders UTF-8")
}

/// Rejects an empty rendering, returning the script unchanged otherwise.
///
/// A generator that produced nothing would leave a zero-byte script behind,
/// and a shell sourcing one registers no completion and reports no error, so
/// the failure stays invisible until someone notices completion has stopped
/// working. An empty rendering is a defect to surface, not a result to write.
///
/// One reachable cause is a command with no subcommands: `clap_complete`'s
/// fish generator renders nothing for one. Reaching this from a real build
/// therefore means the CLI grew nothing to complete, which is a decision to
/// make deliberately rather than to ship as a zero-byte script.
///
/// # Errors
///
/// Returns a description naming the shell when the rendering is empty or
/// whitespace-only.
fn reject_empty(shell: &str, script: String) -> Result<String, String> {
    if script.trim().is_empty() {
        return Err(format!("the generator produced an empty {shell} completion script"));
    }
    Ok(script)
}

/// Writes one completion script per shell under `out_dir/completions`.
///
/// `bin_name` names the files, while the script's own name comes from the
/// command, so a caller passes the binary's name and the scripts still
/// register under the command's.
///
/// # Errors
///
/// Returns a description of the first failure: a directory that cannot be
/// created or written, or an empty rendering.
fn write_all(command: &mut Command, bin_name: &str, out_dir: &Path) -> Result<(), String> {
    let completion_dir = out_dir.join("completions");
    fs::create_dir_all(&completion_dir)
        .map_err(|error| format!("create {}: {error}", completion_dir.display()))?;
    for (shell_name, shell) in SHELLS {
        let script = reject_empty(shell_name, render(command, *shell))?;
        let path = completion_dir.join(format!("{bin_name}.{shell_name}"));
        fs::write(&path, script).map_err(|error| format!("write {}: {error}", path.display()))?;
    }
    Ok(())
}

/// Generates `command`'s shell completion scripts into the build script's
/// `OUT_DIR`.
///
/// Call this from a `build.rs` with the same command definition the binary
/// parses, so the generated scripts describe the command the binary actually
/// accepts.
///
/// # Rerun tracking
///
/// This function emits no `cargo:rerun-if-changed` lines, and it does not need
/// to. The command definition lives in a crate the build script links as a
/// build-dependency, so a change there changes this build script's own binary
/// and cargo reruns it, and the binary cannot add to the command because it
/// parses with that same definition. A build script that named a file here
/// instead would narrow cargo's rerun set to that file and let the definitions
/// go stale without an error.
///
/// # Panics
///
/// Panics if `OUT_DIR` is unset, which is only reachable outside a build
/// script, or if generation fails. A build script has no way to report a
/// recoverable failure, and a missing completion script nobody notices is
/// worse than a build that stops.
pub fn generate_completions(command: &mut Command, bin_name: &str) {
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR"));
    if let Err(message) = write_all(command, bin_name, &out_dir) {
        panic!("completion generation failed: {message}");
    }
}

#[cfg(test)]
mod tests {
    use super::{SHELLS, reject_empty, render, write_all};
    use clap::Command;
    use clap_complete::Shell;
    use mediapm_utils::temp::artifact_dir;

    /// Every shell the completion scripts are shipped for, written out here
    /// rather than read from [`SHELLS`]. Deriving the expectation from the
    /// constant under test would make the assertion below hold for any list,
    /// including a list a shell had been dropped from.
    const EXPECTED_SHELLS: &[&str] = &["bash", "elvish", "fish", "powershell", "zsh"];

    /// A command with two subcommands, so a rendered script has something to
    /// complete without the test depending on a real CLI's definition.
    fn sample_command() -> Command {
        Command::new("sample").subcommand(Command::new("alpha")).subcommand(Command::new("beta"))
    }

    /// Reads one generated script, reporting the path on failure.
    fn read_script(dir: &std::path::Path, file_name: &str) -> String {
        let path = dir.join("completions").join(file_name);
        std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
    }

    /// Every shipped shell gets a file with content in it. A shell missing
    /// from the output, or written empty, ships a binary whose completion works
    /// for some shells and silently does nothing for others.
    #[test]
    fn writes_one_non_empty_script_per_shell() {
        let dir = artifact_dir().expect("artifact dir");
        write_all(&mut sample_command(), "sample", dir.path()).expect("write completions");

        assert_eq!(SHELLS.len(), EXPECTED_SHELLS.len(), "the shell list changed size");
        for shell in EXPECTED_SHELLS {
            let script = read_script(dir.path(), &format!("sample.{shell}"));
            assert!(!script.trim().is_empty(), "the {shell} script is empty");
        }
    }

    /// The script is named for the command, not for the file it is written to.
    /// A script that registers itself under the file name completes nothing
    /// when the command's name differs, which it does for `mediapm-conductor`.
    #[test]
    fn script_is_named_for_the_command_not_the_file() {
        let dir = artifact_dir().expect("artifact dir");
        write_all(
            &mut Command::new("actual").subcommand(Command::new("alpha")),
            "file-name-differs",
            dir.path(),
        )
        .expect("write completions");

        let script = read_script(dir.path(), "file-name-differs.bash");
        assert!(
            script.contains("_actual("),
            "the script must register under the command's name, got: {}",
            script.chars().take(120).collect::<String>()
        );
        assert!(
            !script.contains("file-name-differs"),
            "the script must not carry the file name it was written to"
        );
    }

    /// The predicate rejects an empty or whitespace-only rendering. This
    /// covers the predicate alone; the generation path that calls it is
    /// covered by [`an_empty_rendering_fails_the_generation_path`].
    #[test]
    fn an_empty_rendering_is_an_error() {
        assert!(reject_empty("bash", String::new()).is_err());
        assert!(reject_empty("bash", "  \n\t ".to_string()).is_err());
        assert!(reject_empty("bash", "_sample() { :; }".to_string()).is_ok());
    }

    /// The generation path refuses an empty rendering rather than only the
    /// predicate it calls, and leaves no zero-byte script behind. A call site
    /// that discarded the rejection would report success and ship a script that
    /// registers no completion.
    ///
    /// A command with no subcommands is the reachable case, because
    /// `clap_complete`'s fish generator renders nothing for one, so this drives
    /// the real generator rather than a stub. If that upstream behaviour
    /// changes, this fails loudly instead of losing its coverage quietly.
    #[test]
    fn an_empty_rendering_fails_the_generation_path() {
        let dir = artifact_dir().expect("artifact dir");
        let error = write_all(&mut Command::new("bare"), "bare", dir.path())
            .expect_err("a command with nothing to complete must not report success");
        assert!(error.contains("empty"), "the failure must name the empty rendering, got: {error}");
        for entry in std::fs::read_dir(dir.path().join("completions")).expect("read completions") {
            let path = entry.expect("dir entry").path();
            let length = std::fs::metadata(&path).expect("metadata").len();
            assert!(length > 0, "{} was written empty", path.display());
        }
    }

    /// A real command renders a script whose registration matches its name, so
    /// the check above is not passing on a generator that renders nothing.
    #[test]
    fn a_real_command_renders_a_script_naming_it() {
        let script = render(&mut sample_command(), Shell::Bash);
        assert!(
            script.contains("_sample("),
            "rendered: {}",
            script.chars().take(120).collect::<String>()
        );
        assert!(script.contains("alpha"), "the subcommands must reach the script");
    }
}
