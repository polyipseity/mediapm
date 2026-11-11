//! Integration tests for repository scripts: the temp-janitor production
//! scripts (`scripts/clean-mediapm-temp.{sh,ps1}`) and their self-tests.
//!
//! The test pipeline itself is not tested from here. It is the
//! `test-runner` crate, and it carries its own gate tests in Rust.
//!
//! Platform-smart via runtime probes: each interpreter (`bash`, `sh`,
//! `pwsh`) is probed by spawning a harmless command, and when the binary is
//! missing the test prints `skipped: ...` and passes without running. The one
//! exception is the bash shell itself: [`posix_bash`] resolves it by behaviour
//! rather than by name, because on Windows that name is the WSL launcher and a
//! probe of the name would skip a host whose Git Bash the janitor can sweep
//! with.
//! Sandboxes use the managed `mediapm-` prefix under the real temp dir and
//! are removed on drop.
//!
//! The bash janitor tests additionally need an `rm` that clears its argument.
//! Some hosts put a safe-delete wrapper first on `PATH`, and when its trash step
//! is denied (macOS Finder automation is the sandbox case) it exits non-zero
//! and leaves the tree in place, so the sweep cannot run there at all. The
//! harness probes for a working `rm`, hands the children a `PATH` whose first
//! `rm` is that one, and fails the suite when the host has none: a host that
//! cannot run the janitor has not proven the janitor, so a green run must never
//! stand for a sweep that never happened. The failure names the environment it
//! could not use.
//! Running `tests/scripts/test-clean-mediapm-temp.sh` by hand on such a host
//! still works: the self-test supplies its own working `rm` for its sweep cases.
//! The probe requires a candidate to exit 0 with its probe directory's source
//! path gone, which a trash move also satisfies, so the harness needs no special
//! case for a trash-moving `rm`: the janitor's sweep depends on the path being
//! gone, not on how it got there.

use std::any::Any;
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

/// Sequence number for sandbox names (unique per process).
static SANDBOX_SEQ: AtomicU64 = AtomicU64::new(0);

/// Production janitor scripts exercised by this crate.
const SCRIPTS: [&str; 2] = ["clean-mediapm-temp.sh", "clean-mediapm-temp.ps1"];

/// Script self-tests present in this crate's directory, gated for presence
/// and the executable bit by [`script_files_exist_and_are_executable`].
const TEST_SCRIPTS: [&str; 2] = ["test-clean-mediapm-temp.sh", "test-clean-mediapm-temp.ps1"];

/// Fake managed-prefix dirs the janitor must remove.
const FAKE_MEDIAPM_DIRS: [&str; 3] =
    ["mediapm-artifact-fake", "mediapm-cache-fake", "mediapm-runtime-fake"];

/// Non-mediapm dirs the janitor must never touch.
const CONTROL_DIRS: [&str; 2] = ["cli-add-hierarchy-123-456", "unrelated-dir"];

/// A managed-prefix temp sandbox removed on drop.
struct Sandbox {
    path: PathBuf,
}

impl Sandbox {
    /// Creates a unique `mediapm-{pid}-{seq}` sandbox under the real temp dir.
    fn new() -> Self {
        loop {
            let seq = SANDBOX_SEQ.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!("mediapm-{}-{seq}", std::process::id()));
            if !path.exists() {
                fs::create_dir_all(&path).expect("create sandbox dir");
                return Self { path };
            }
            // Crash leftover from a previous run with the same pid+seq: try
            // the next sequence number.
        }
    }
}

impl Drop for Sandbox {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

/// Repo root: the parent of this crate's manifest directory.
fn repo_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("tests crate must sit directly under the repository root")
}

/// Absolute path to a repository script.
#[must_use]
fn script_path(name: &str) -> PathBuf {
    repo_root().join("scripts").join(name)
}

/// Absolute path to a script self-test under this crate.
#[must_use]
fn test_script_path(name: &str) -> PathBuf {
    repo_root().join("tests").join("scripts").join(name)
}

/// Normalizes CRLF line endings to LF for cross-platform output matching.
#[must_use]
fn normalize(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).replace("\r\n", "\n")
}

/// Outcome of a spawned script: exit status plus normalized stdout/stderr.
#[must_use]
struct RunOutcome {
    status: ExitStatus,
    stdout: String,
    stderr: String,
}

/// Spawns and waits for a command with `TMPDIR`/`TMP`/`TEMP` sandboxed at
/// `sandbox`, normalizing CRLF in the captured output.
fn capture_output(mut cmd: Command, sandbox: &Path) -> RunOutcome {
    cmd.env("TMPDIR", sandbox).env("TMP", sandbox).env("TEMP", sandbox);
    let output = cmd.output().expect("spawn and wait for script");
    RunOutcome {
        status: output.status,
        stdout: normalize(&output.stdout),
        stderr: normalize(&output.stderr),
    }
}

/// The `rm` the bash janitor tests hand their children: the first candidate that
/// exited 0 with the probe directory's source path gone.
///
/// See [`path_with_clearing_rm_with_candidates`] for what makes a candidate
/// usable and for why the host's own `rm` cannot be assumed.
fn path_with_clearing_rm() -> PathBuf {
    path_with_clearing_rm_with_candidates(&candidate_list())
}

/// The first candidate that exited 0 with the probe directory's source path gone
/// afterwards, or a panic when no candidate does both.
///
/// Probed and verified by clearing, not by name or path: the wrapper this probe
/// exists for is called `rm` too, and one exists on hosts that report a non-zero
/// status while leaving the tree in place. A host with no candidate that exits 0
/// with the path gone fails the suite rather than passing quietly, because the
/// janitor's sweep cannot be proven there and an unproven sweep must not look
/// green.
///
/// The candidate list is a parameter so a test can hand in a candidate that
/// cannot run and observe that failure.
fn path_with_clearing_rm_with_candidates(candidates: &[PathBuf]) -> PathBuf {
    if let Some(found) = candidates.iter().find(|candidate| rm_clears_probe(candidate)) {
        return found.clone();
    }
    panic!(
        "no working rm on this host: every candidate failed to exit 0 with the probe \
         directory's source path gone, or could not run at all ({candidates:?}). The \
         janitor sweeps with `rm`, so `clean-mediapm-temp` cannot be proven here. Fix the \
         host's rm (a safe-delete wrapper whose trash step is denied is the usual cause) or \
         run the suite where rm exits 0 and clears the path."
    )
}

/// Candidate real `rm`s, most specific first.
///
/// `command -v rm` is last because it is whatever the host resolves, which
/// is the wrapper on the hosts this probe exists for; the probe is what makes
/// the order safe rather than the paths themselves.
///
/// On Windows the list is the shell's answer alone: the POSIX paths above name
/// nothing a Windows process can run, and a bare `rm` is not on the Windows
/// `PATH` either, so the only `rm` the janitor's tests can sweep with there is
/// the one the shell resolves.
fn candidate_list() -> Vec<PathBuf> {
    let mut candidates: Vec<PathBuf> = if cfg!(windows) {
        Vec::new()
    } else {
        ["/bin/rm", "/usr/bin/rm", "/usr/local/rm", "/usr/local/bin/rm"]
            .into_iter()
            .map(PathBuf::from)
            .filter(|candidate| !is_local_bin_shim(candidate))
            .collect()
    };
    candidates.extend(command_v("rm").filter(|rm| !is_local_bin_shim(rm)));
    candidates
}

/// Whether `path` sits in `~/.local/bin`, where hosts install wrappers that
/// shadow the system `rm`.
///
/// The probe already rejects a wrapper that cannot clear the path; this keeps a
/// working one out of the set. It is not a guard against a trash move: a working
/// trash-moving `rm` installed outside `~/.local/bin` (the list names
/// `/usr/local/bin/rm` and `/usr/local/rm`) is still selected, and the probe and
/// the janitor runs would then move their trees to the Trash.
fn is_local_bin_shim(path: &Path) -> bool {
    let Some(home) = std::env::var_os("HOME") else {
        return false;
    };
    path.parent() == Some(Path::new(&home).join(".local").join("bin").as_path())
}

/// `sh -c 'command -v <name>'` output, or `None` when that shell cannot answer.
///
/// The answer is asked of a shell because a shell is what resolves the name for
/// the janitor: on Windows the reply is a Git Bash path (`/usr/bin/rm`), which
/// is the only form of it the janitor's tests can run there. That shell is the
/// resolved POSIX one, never the name `bash`: `%SystemRoot%\System32\bash.exe`
/// is the WSL launcher, and it answers this question with its own failure.
fn command_v(name: &str) -> Option<PathBuf> {
    let shell: &Path = if cfg!(windows) { posix_bash()? } else { Path::new("sh") };
    let output = Command::new(shell).args(["-c", &format!("command -v {name}")]).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let path = std::str::from_utf8(&output.stdout).ok()?.trim();
    (!path.is_empty()).then(|| PathBuf::from(path))
}

/// Whether `candidate` clears a non-empty probe directory: exited 0 with the
/// probe directory's source path gone.
///
/// Which way the path disappeared is not asserted: whether the tree was
/// unlinked or moved to the Trash is not something the janitor's sweep depends
/// on, so a trash move satisfies this predicate and the harness needs no special
/// case for one.
///
/// Both halves matter: a trash wrapper that is denied exits non-zero and leaves
/// the source path, and a candidate that leaves it cannot prove the janitor's
/// sweep. The probe dir carries the managed `mediapm-` prefix, so a crash leak
/// is reclaimed by the janitor like any other.
///
/// The probe tree holds a file, because the sweeps it vouches for clear trees
/// that hold files too: a wrapper that clears an empty directory but refuses a
/// non-empty one would pass an empty probe and then fail the janitor's real
/// run, which is the false defect this probe exists to prevent.
///
/// The candidate is run through the shell on Windows: a POSIX path and a bare
/// `rm` are both meaningless to a Windows process, and the shell is what the
/// janitor sweeps with there. The probe path is passed as an argument rather
/// than pasted into the command, so a temp path with a space in it survives.
/// That shell is [`posix_bash`] rather than the name `bash`, and a host with no
/// candidate answering there rejects every candidate: without a POSIX shell
/// there is no `rm` for the janitor's tests to sweep with either.
fn rm_clears_probe(candidate: &Path) -> bool {
    let seq = SANDBOX_SEQ.fetch_add(1, Ordering::Relaxed);
    let probe = std::env::temp_dir().join(format!("mediapm-rm-probe-{}-{seq}", std::process::id()));
    if fs::create_dir_all(&probe).is_err() {
        return false;
    }
    if fs::write(probe.join("probe-file"), b"probe").is_err() {
        let _ = fs::remove_dir_all(&probe);
        return false;
    }
    let status = if cfg!(windows) {
        let Some(shell) = posix_bash() else {
            let _ = fs::remove_dir_all(&probe);
            return false;
        };
        Command::new(shell)
            .args(["-c", &format!("{} -rf \"$1\"", shell_quote(candidate)), "_"])
            .arg(&probe)
            .status()
    } else {
        Command::new(candidate).arg("-rf").arg(&probe).status()
    };
    let gone = !probe.exists();
    let _ = fs::remove_dir_all(&probe);
    matches!(status, Ok(status) if status.success()) && gone
}

/// Single-quotes `path` for the shim's `exec` line.
fn shell_quote(path: &Path) -> String {
    format!("'{}'", path.to_string_lossy().replace('\'', r"'\''"))
}

/// `path` in the one spelling both sides of a comparison are reduced to.
///
/// A twin prints the path its shell built, and the shell's spelling differs
/// from `Path::display` in two ways: the separator (`/` against `\`) and, on
/// Windows under MSYS, the drive form (`/c/Users/...` against `C:\Users\...`).
/// Every comparison of a script's message against a test-held path goes through
/// this function and [`shell_printed`], which share [`one_path_spelling`]. The
/// comparison is spelling-only, and both the separator and the drive form are
/// normalized on both sides rather than either side being compared as it prints,
/// whether the text is this bare path or a message that carries one.
fn printed_as_shell_path(path: &Path) -> String {
    one_path_spelling(&path.display().to_string())
}

/// `text` in that same spelling, for comparison with
/// [`printed_as_shell_path`].
///
/// The comparison is spelling-only, and both the separator and the drive form
/// are normalized on both sides: a script that printed `/c/Users/...` and a test
/// holding `C:\Users\...` name one directory. The text is a whole script
/// message, so the drive form is normalized where the path sits inside it —
/// `removed: /c/...` included — not only when a message is nothing but a path.
fn shell_printed(text: &str) -> String {
    one_path_spelling(text)
}

/// `path` in the spelling the PowerShell twin prints for it, for comparison.
///
/// A Rust temp path can differ from the same directory's printed form in the
/// segment names, not only in separators: on Windows `std::env::temp_dir()`
/// returns the 8.3 short name (`C:\Users\RUNNER~1\...`) while the janitor
/// child prints the long one (`C:\Users\runneradmin\...`) for the same tree —
/// the PowerShell twin resolves its root through `GetTempPath` and reports the
/// long form, which is what the CI log shows. `fs::canonicalize` yields that form,
/// so the expected side is resolved through it while the path still exists — the
/// tests that compare a removed tree call this before the sweep.
///
/// Only the PowerShell twin goes through this. The sh twin is handed
/// `TMPDIR`/`TMP`/`TEMP` set to the sandbox path as Rust spells it and prints the
/// root it globbed from that value, so its expectation stays
/// [`printed_as_shell_path`]. On Unix the path is already what either shell
/// prints: `canonicalize` would rewrite `/tmp` to `/private/tmp` on macOS, which
/// the shell does not, so it is not used there.
fn path_as_printed(path: &Path) -> String {
    #[cfg(windows)]
    if let Ok(resolved) = fs::canonicalize(path) {
        return printed_as_shell_path(&resolved);
    }
    printed_as_shell_path(path)
}

/// The expected spelling of `path` for the twin named by `script_name`.
///
/// See [`path_as_printed`] for why the two twins differ on Windows.
fn expected_root_spelling(script_name: &str, path: &Path) -> String {
    let is_pwsh = Path::new(script_name)
        .extension()
        .is_some_and(|extension| extension.eq_ignore_ascii_case("ps1"));
    if is_pwsh { path_as_printed(path) } else { printed_as_shell_path(path) }
}

/// `text` with its path spelling normalized: every `\` becomes `/`, an MSYS
/// drive group (`/c/`) becomes a Win32 one (`C:/`) where it starts a path, and
/// a leading drive letter is uppercased so `c:/` and `C:/` compare equal.
///
/// A drive group starts a path at the start of the text, after whitespace, or
/// after `:`, which is where a janitor line such as `removed: <root>` or
/// `cannot remove directories under <root>` puts its root. A comparison
/// therefore sees that root normalized wherever in the message it appears, not
/// only when the message is a bare path. Every other `/x/` is left alone, so a
/// one-letter segment that does not start a path, a longer first segment
/// (`/xy/...`), and a relative path are all compared as they were printed.
///
/// The argument is a script's message or a `Path::display`, never a path that is
/// opened afterwards, so rewriting it cannot change which directory is named:
/// the spellings this joins are one directory written two ways.
///
/// A Win32 path can also arrive in the verbatim form Rust's own APIs produce,
/// `\\?\C:\Users\me\x` and `\\?\UNC\server\share\x` for a UNC path. That prefix
/// names the same directory as the plain spelling and no shell prints it, so it
/// is stripped before the separator pass: without this a sweep that did remove
/// the tree compares unequal to the test's path and reads as one that removed
/// nothing.
fn one_path_spelling(text: &str) -> String {
    let unverbatim = match text.strip_prefix(r"\\?\UNC\") {
        Some(tail) => format!(r"\\{tail}"),
        None => match text.strip_prefix(r"\\?\") {
            Some(tail) => tail.to_string(),
            None => text.to_string(),
        },
    };
    let separated = unverbatim.replace('\\', "/");
    let bytes = separated.as_bytes();
    let mut normalized = String::with_capacity(separated.len());
    let mut copied = 0;
    let mut index = 0;
    while index + 2 < bytes.len() {
        let starts_a_path = bytes[index] == b'/'
            && bytes[index + 1].is_ascii_alphabetic()
            && bytes[index + 2] == b'/';
        // Slicing is safe here: a `/` byte is a character boundary, and the
        // group's own bytes are ASCII, so a multi-byte character is never cut.
        let eligible = starts_a_path
            && separated[..index]
                .chars()
                .next_back()
                .is_none_or(|preceding| preceding.is_whitespace() || preceding == ':');
        if eligible {
            normalized.push_str(&separated[copied..index]);
            normalized.push(char::from(bytes[index + 1].to_ascii_uppercase()));
            normalized.push_str(":/");
            index += 3;
            copied = index;
        } else {
            index += 1;
        }
    }
    normalized.push_str(&separated[copied..]);
    match normalized.as_bytes() {
        [letter, b':', b'/', ..] if letter.is_ascii_alphabetic() => {
            let letter = char::from(letter.to_ascii_uppercase());
            let tail = &normalized[1..];
            format!("{letter}{tail}")
        }
        _ => normalized,
    }
}

/// The drive forms a shell may print compare equal.
///
/// The sh twin prints whatever form its shell produced, and under MSYS that is
/// `/c/Users/...` where the test holds `C:\Users\...`. A comparison normalizing
/// only the separator would read those as two directories, so a sweep that did
/// remove the tree would look like it removed nothing. The janitor lines the
/// harness compares are whole messages with the root inside them, so the interior
/// form is what those comparisons actually meet. A leading segment that is not a
/// single letter is not a drive and must stay untouched, or an ordinary Unix path
/// could be mistaken for one.
#[test]
fn printed_paths_compare_in_one_shell_spelling() {
    let canonical = "C:/Users/me/x";
    assert_eq!(shell_printed("/c/Users/me/x"), canonical);
    assert_eq!(shell_printed("C:/Users/me/x"), canonical);
    assert_eq!(printed_as_shell_path(Path::new(r"C:\Users\me\x")), canonical);
    assert_eq!(printed_as_shell_path(Path::new("c:/Users/me/x")), canonical);

    // The verbatim form Rust hands back from a canonicalized path names the same
    // directory, and no shell prints the prefix.
    assert_eq!(printed_as_shell_path(Path::new(r"\\?\C:\Users\me\x")), canonical);
    assert_eq!(shell_printed(r"\\?\C:\Users\me\x"), canonical);
    assert_eq!(printed_as_shell_path(Path::new(r"\\?\UNC\server\share\x")), "//server/share/x");

    assert_eq!(shell_printed("removed: /c/Users/me/x"), "removed: C:/Users/me/x");
    assert_eq!(
        shell_printed("cannot remove directories under /d/scope/x"),
        "cannot remove directories under D:/scope/x"
    );

    assert_ne!(shell_printed("/x/Users/me"), canonical);
    assert_ne!(shell_printed("/xy/Users/me"), canonical);
    assert_ne!(shell_printed("Users/me/x"), canonical);
    assert_eq!(shell_printed("Users/me/x"), "Users/me/x");
    assert_eq!(shell_printed("/xy/Users/me"), "/xy/Users/me");
    assert_eq!(shell_printed("/tmp/mediapm-artifact-x"), "/tmp/mediapm-artifact-x");
}

/// A `PATH` value whose first `rm` runs `body` as a `/bin/sh` script.
///
/// The shim lives inside the test's sandbox so it is removed with it: a
/// `mediapm-`-prefixed dir that outlived the test would trip the orphan gate.
fn path_with_rm_shim(sandbox: &Path, body: &str) -> OsString {
    let bin = sandbox.join("rm-bin");
    fs::create_dir_all(&bin).expect("create rm shim dir");
    let shim = bin.join("rm");
    fs::write(&shim, format!("#!/bin/sh\n{body}")).expect("write rm shim");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let permissions = fs::Permissions::from_mode(0o755);
        fs::set_permissions(&shim, permissions).expect("make rm shim executable");
    }
    // The shim has to be executable to the shell that runs it, and on Windows
    // there is no mode bit for the test to write: MSYS2 derives the execute bits
    // from the file's ACL, and its header check is only the fallback for a volume
    // that carries no ACLs at all (`fstat_helper` in
    // `winsup/cygwin/fhandler/disk_file.cc` reads the security descriptor first
    // and reaches the `#!` test only in the `else` of that call). So the bits are
    // set through the same shell that will stat the file: `chmod +x` there sets
    // exactly what its own `stat` reports, which is what the sweeps rely on.
    //
    // Two things about that shell come from the Windows CI job. It is the
    // resolved POSIX one, not the name `bash`: the name is
    // `%SystemRoot%\System32\bash.exe`, the WSL launcher, and `chmod +x` through
    // it fails before it reaches the file. And the path is handed over in `/`
    // form, the separator every POSIX shell reads, rather than the Win32
    // `Path::display` spelling whose backslashes are ordinary characters to the
    // shell. A caller reaching here has already proven a shell exists, so the
    // expect states an invariant rather than a hope.
    #[cfg(windows)]
    {
        let shell =
            posix_bash().expect("the caller resolved a POSIX bash before seeding an rm shim");
        let chmod = Command::new(shell)
            .args(["-c", "chmod +x \"$1\"", "_"])
            .arg(shim.display().to_string().replace('\\', "/"))
            .status()
            .expect("spawn chmod for the rm shim");
        assert!(chmod.success(), "chmod +x failed for the rm shim at {}", shim.display());
    }
    //
    // The child is a shell, so `PATH` is the shell's own path list. That list is
    // `:`-separated in POSIX form and `;`-separated in Win32 form, and the two
    // cannot be mixed: a Win32 list is converted element by element at process
    // start (`conv_path_list` splits on `;` whenever the target is POSIX), so a
    // `:` after a Win32 path would be swallowed into that element instead of
    // ending it, and the shim would stop shadowing the host `rm`.
    let separator = if cfg!(windows) { ";" } else { ":" };
    let mut path = OsString::from(&bin);
    path.push(separator);
    path.push(std::env::var_os("PATH").unwrap_or_default());
    path
}

/// A `PATH` value whose first `rm` is `real_rm`.
fn path_with_rm_exec(sandbox: &Path, real_rm: &Path) -> OsString {
    path_with_rm_shim(sandbox, &format!("exec {} \"$@\"\n", shell_quote(real_rm)))
}

/// Runs a script under `shell` (a resolved POSIX shell, or `sh`) inside a
/// sandbox, with `rm` resolving to `real_rm`.
fn run_script(
    shell: &Path,
    script: &Path,
    sandbox: &Path,
    flags: &[&str],
    real_rm: &Path,
) -> RunOutcome {
    let mut cmd = Command::new(shell);
    cmd.arg(script).args(flags);
    cmd.env("PATH", path_with_rm_exec(sandbox, real_rm));
    capture_output(cmd, sandbox)
}

/// Runs a PowerShell script via `pwsh -NoProfile -File` inside a sandbox.
fn run_pwsh(script: &Path, sandbox: &Path, flags: &[&str]) -> RunOutcome {
    let mut cmd = Command::new("pwsh");
    cmd.args(["-NoProfile", "-File"]).arg(script).args(flags);
    capture_output(cmd, sandbox)
}

/// Probes an interpreter by spawning a harmless command; on absence prints
/// `skipped: ...` and returns `false`.
#[must_use]
fn skip_unless(program: &str, args: &[&str]) -> bool {
    if Command::new(program).args(args).output().is_ok() {
        true
    } else {
        println!("skipped: {program} not found");
        false
    }
}

/// The POSIX shell the bash janitor tests spawn, resolved by behaviour rather
/// than by name.
///
/// On Windows the name `bash` is not a POSIX shell: `%SystemRoot%\System32\bash.exe`
/// is the WSL launcher, and with no distribution installed it exits non-zero
/// with `Windows Subsystem for Linux has no installed distributions.` — the text
/// the Windows CI job captured from this suite. A test that spawned the name
/// would fail there while a real Git Bash sat on the same host, so every
/// candidate is asked to answer one POSIX command and the first that does is the
/// shell. The probe is the resolver, which is what makes the order in
/// [`posix_bash_candidates`] safe rather than the paths themselves.
///
/// Memoized because every spawn in the bash tests goes through it: a host with no
/// usable shell would otherwise pay one failed probe per spawn.
fn posix_bash() -> Option<&'static Path> {
    static RESOLVED: OnceLock<Option<PathBuf>> = OnceLock::new();
    RESOLVED.get_or_init(resolve_posix_bash).as_deref()
}

/// [`posix_bash`] or a skip line, in the shape [`skip_unless`] returns.
///
/// The bash tests skip through this rather than through [`skip_unless`] on the
/// name: a skip line for a name that resolves to the WSL launcher would report
/// the shell as missing while the cause was which `bash` the host resolved.
fn skip_unless_posix_bash() -> Option<&'static Path> {
    let shell = posix_bash();
    if shell.is_none() {
        println!(
            "skipped: no POSIX bash on this host (bash resolves to the WSL launcher or is absent)"
        );
    }
    shell
}

/// The shell [`posix_bash`] resolves, or nothing when no candidate answers a
/// POSIX command.
fn resolve_posix_bash() -> Option<PathBuf> {
    posix_bash_candidates().into_iter().find(|candidate| posix_shell_answers(candidate))
}

/// Where a POSIX shell may be installed, in the order it is looked for.
///
/// On Windows the Git for Windows layouts named by `ProgramFiles`,
/// `ProgramFiles(x86)` and `LOCALAPPDATA` come first, because a bare `bash`
/// there is the WSL launcher on the system path. A variable that is unset or
/// empty is skipped rather than reported: a host without `ProgramFiles(x86)` is
/// an ordinary 64-bit install, not a broken one. The name `bash` on `PATH` is
/// the last resort, for a host whose Git Bash is somewhere else again.
///
/// Elsewhere the bare name comes first and `/bin/bash` is the fallback, which
/// keeps the Unix behaviour the suite had when it spawned `bash` unqualified.
fn posix_bash_candidates() -> Vec<PathBuf> {
    if !cfg!(windows) {
        return ["bash", "/bin/bash"].into_iter().map(PathBuf::from).collect();
    }
    let mut candidates = Vec::new();
    for (variable, tail) in [
        ("ProgramFiles", r"Git\bin\bash.exe"),
        ("ProgramFiles", r"Git\usr\bin\bash.exe"),
        ("ProgramFiles(x86)", r"Git\bin\bash.exe"),
        ("LOCALAPPDATA", r"Programs\Git\bin\bash.exe"),
    ] {
        if let Some(base) = std::env::var_os(variable).filter(|base| !base.is_empty()) {
            candidates.push(Path::new(&base).join(tail));
        }
    }
    candidates.push(PathBuf::from("bash"));
    candidates
}

/// Whether `candidate` answers `command -v rm`, which is a shell's own statement
/// that it is a POSIX shell with a POSIX `PATH`.
///
/// The WSL launcher fails this: it exits non-zero and prints its own message, so
/// it cannot be selected however the name resolves on the host.
fn posix_shell_answers(candidate: &Path) -> bool {
    Command::new(candidate)
        .args(["-c", "command -v rm"])
        .output()
        .is_ok_and(|output| output.status.success() && !output.stdout.is_empty())
}

/// A sandbox whose `scope` subdir holds the dirs named `names`, plus that scope
/// path.
///
/// The first named dir also holds a `probe-file`, so the sweep is handed a tree
/// that is not empty: the shape [`rm_clears_probe`] probes for and the janitor's
/// real run has to delete.
///
/// The janitor scans with `find "$tmp_root" -maxdepth 1 -name 'mediapm-*'`,
/// which also matches the starting point itself. The sandbox root carries
/// the managed `mediapm-` prefix (so a leaked sandbox is reclaimed by the
/// janitor / orphan gate), so the seeded dirs live in a nested `scope`
/// subdir whose basename does not match the glob; the janitor is pointed at
/// `scope` via the child-scoped `TMPDIR`.
fn seeded_scope(names: &[&str]) -> (Sandbox, PathBuf) {
    let sandbox = Sandbox::new();
    let scope = sandbox.path.join("scope");
    fs::create_dir_all(&scope).expect("create janitor scope dir");
    for name in names {
        fs::create_dir_all(scope.join(name)).expect("create seeded sandbox dir");
    }
    if let Some(first) = names.first() {
        fs::write(scope.join(first).join("probe-file"), b"probe").expect("seed probe file");
    }
    (sandbox, scope)
}

/// Whether the test process runs as root, where Unix permission bits are
/// advisory and a sealed directory does not block removal.
#[cfg(unix)]
fn running_as_root() -> bool {
    Command::new("sh")
        .args(["-c", "id -u"])
        .output()
        .ok()
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .and_then(|stdout| stdout.trim().parse::<u32>().ok())
        .is_some_and(|uid| uid == 0)
}

/// Seals `path` so a sweep cannot empty it: mode `000` with a child inside.
///
/// `rm -rf` fails on it even after the janitor's `chmod -R u+w`, because that
/// chmod cannot open a directory it may not traverse. Sealing the sweep root
/// instead would break the sweep's own `mktemp`, which writes its probe and its
/// listing there.
#[cfg(unix)]
fn seal_with_child(path: &Path) {
    use std::os::unix::fs::PermissionsExt;

    fs::create_dir_all(path.join("inner")).expect("create sealed tree");
    fs::set_permissions(path, fs::Permissions::from_mode(0o000)).expect("seal the tree");
}

/// Restores a tree sealed by [`seal_with_child`] so the sandbox can be removed.
#[cfg(unix)]
fn unseal(path: &Path) {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).expect("unseal the tree");
}

/// Exercises the full janitor sequence (dry-run, real run, empty dry-run,
/// help, bogus arg) against a sandbox seeded with fake dirs.
fn assert_janitor_sequence(script_name: &str, run: impl Fn(&Path, &Path, &[&str]) -> RunOutcome) {
    let seeded: Vec<&str> = FAKE_MEDIAPM_DIRS.iter().copied().chain(CONTROL_DIRS).collect();
    let (_sandbox, scope) = seeded_scope(&seeded);
    let janitor = script_path(script_name);

    // Dry run: reports exactly the three mediapm-* dirs, never the controls.
    let dry = run(&janitor, &scope, &["--dry-run"]);
    assert!(dry.status.success(), "dry-run failed: {}", dry.stderr);
    let reported: Vec<&str> =
        dry.stdout.lines().filter(|line| line.starts_with("would remove: ")).collect();
    assert_eq!(reported.len(), 3, "dry-run reported {} removals, expected 3", reported.len());
    for line in &reported {
        assert!(
            FAKE_MEDIAPM_DIRS.iter().copied().any(|name| line.ends_with(name)),
            "dry-run reported an unexpected dir: {line}"
        );
        assert!(
            CONTROL_DIRS.iter().copied().all(|name| !line.contains(name)),
            "dry-run reported a control dir: {line}"
        );
    }
    assert!(
        dry.stdout.contains("would remove 3 mediapm temp director(ies)"),
        "dry-run missing count line: {}",
        dry.stdout
    );
    for name in CONTROL_DIRS {
        assert!(scope.join(name).is_dir(), "dry-run removed control {name}");
    }

    // Real run: removes exactly the three mediapm-* dirs, leaves controls.
    let real = run(&janitor, &scope, &[]);
    assert!(real.status.success(), "real run failed: {}", real.stderr);
    let removed: Vec<&str> =
        real.stdout.lines().filter(|line| line.starts_with("removed: ")).collect();
    assert_eq!(removed.len(), 3, "real run reported {} removals, expected 3", removed.len());
    assert!(
        real.stdout.contains("removed 3 mediapm temp director(ies)"),
        "real run missing count line: {}",
        real.stdout
    );
    for name in FAKE_MEDIAPM_DIRS {
        assert!(!scope.join(name).exists(), "{name} still exists after real run");
    }
    for name in CONTROL_DIRS {
        assert!(scope.join(name).is_dir(), "real run removed control {name}");
    }

    // Second dry run on the now-empty scope.
    let dry_again = run(&janitor, &scope, &["--dry-run"]);
    assert!(dry_again.status.success(), "second dry-run failed: {}", dry_again.stderr);
    assert!(
        dry_again.stdout.contains("no mediapm temp directories found"),
        "second dry-run missing empty message: {}",
        dry_again.stdout
    );
    assert!(!dry_again.stdout.contains("would remove: "), "second dry-run still reports removals");

    // Help exits 0 and documents the managed glob.
    let help = run(&janitor, &scope, &["--help"]);
    assert!(help.status.success(), "help failed: {}", help.stderr);
    assert!(help.stdout.contains("removes: mediapm-*"), "help missing glob doc: {}", help.stdout);

    // Unknown arguments fail with a stderr diagnostic.
    let bogus = run(&janitor, &scope, &["--bogus"]);
    assert!(!bogus.status.success(), "--bogus should fail");
    assert!(!bogus.stderr.is_empty(), "--bogus should print a stderr diagnostic");
}

#[test]
fn bash_janitor_dry_run_and_real_run() {
    let Some(shell) = skip_unless_posix_bash() else {
        return;
    };
    let rm = path_with_clearing_rm();
    assert_janitor_sequence("clean-mediapm-temp.sh", |script, sandbox, flags| {
        run_script(shell, script, sandbox, flags, &rm)
    });
}

/// A real run whose preflight passes but whose sweep cannot empty one tree must
/// name that tree on stderr and still fail, while the rest of the sweep proceeds:
/// one stuck tree must not hide the verdict for the others.
#[cfg(unix)]
#[test]
fn bash_janitor_reports_a_per_path_failure() {
    let Some(shell) = skip_unless_posix_bash() else {
        return;
    };
    if running_as_root() {
        println!("skipped: the sealed-tree case needs a non-root user");
        return;
    }

    let rm = path_with_clearing_rm();
    let (sandbox, scope) = seeded_scope(&FAKE_MEDIAPM_DIRS[..2]);
    let janitor = script_path("clean-mediapm-temp.sh");
    let sealed = scope.join(FAKE_MEDIAPM_DIRS[0]);
    let removable = scope.join(FAKE_MEDIAPM_DIRS[1]);
    seal_with_child(&sealed);

    let mut cmd = Command::new(shell);
    cmd.arg(&janitor);
    cmd.env("PATH", path_with_rm_exec(&sandbox.path, &rm));
    let partial = capture_output(cmd, &scope);
    unseal(&sealed);

    assert!(
        !partial.status.success(),
        "a tree the sweep cannot remove did not fail the run: stdout {}, stderr {}",
        partial.stdout,
        partial.stderr
    );
    assert!(
        shell_printed(&partial.stderr)
            .contains(&format!("failed to remove: {}", printed_as_shell_path(&sealed))),
        "the stuck tree is not named on stderr: {}",
        partial.stderr
    );
    assert!(
        partial.stderr.contains("1 director(ies) could not be removed"),
        "the failure summary is missing: {}",
        partial.stderr
    );
    assert!(
        shell_printed(&partial.stdout)
            .contains(&format!("removed: {}", printed_as_shell_path(&removable))),
        "the sweep stopped before the removable tree: {}",
        partial.stdout
    );
    assert!(sealed.is_dir(), "the stuck tree vanished, so the case proves nothing");
    assert!(!removable.exists(), "the removable tree survived next to a stuck one");
}

/// Depth of the chain [`seed_deep_chain`] seeds.
///
/// Deep enough that a recursive tree walk spends real stack on it, shallow
/// enough that every seeded path stays inside `PATH_MAX`: a one-character name
/// costs two bytes per level, so this keeps the deepest path near 700 bytes,
/// under the tightest limit among the hosts that run the suite (1024 on macOS).
const DEEP_CHAIN_LEVELS: usize = 300;

/// Depth of the chain on Windows, where `MAX_PATH` bounds the tree that can be
/// named at all.
///
/// Windows cannot address a path past `MAX_PATH` (260 characters, terminator
/// included) without the `\\?\` prefix, and neither janitor twin uses one, so
/// this is the platform's real limit rather than a test case. The deepest path
/// [`seed_deep_chain`] builds at this depth is the temp base
/// (`C:\Users\<user>\AppData\Local\Temp`, 39 characters on the CI runner) plus
/// the sandbox name (at most 17, for `mediapm-<pid>-<seq>`), `\scope` (6),
/// `\mediapm-artifact-fake` (22), two bytes per level (80) and `\leaf` (5):
/// 169 characters, 90 inside the limit.
const WINDOWS_DEEP_CHAIN_LEVELS: usize = 40;

/// The chain depth this host can seed and sweep, as deep as the host can name.
fn deep_chain_levels() -> usize {
    if cfg!(windows) { WINDOWS_DEEP_CHAIN_LEVELS } else { DEEP_CHAIN_LEVELS }
}

/// Name of each seeded chain level.
const DEEP_CHAIN_LEVEL: &str = "d";

/// The file at the bottom of the chain, so the sweep has to unlink a file and
/// not only empty directories.
const DEEP_CHAIN_LEAF: &str = "leaf";

/// Seeds `<scope>/<name>/d/d/...` [`deep_chain_levels`] deep with a leaf file at
/// the bottom, and returns the chain root.
///
/// The seeding steps into each level instead of building one long absolute path,
/// so the test's own writes never depend on the platform's path limit. The chain
/// still stays inside `PATH_MAX` on purpose, and that is the documented limit of
/// the thing under test rather than a test case: both twins address a tree
/// through paths (`chmod`/`rm` and `Get-Item`/`Remove-Item` all take one), so a
/// tree past `PATH_MAX` cannot be named by any of them, and seeding one would
/// leak a sandbox no removal here can address.
fn seed_deep_chain(scope: &Path, name: &str) -> PathBuf {
    let root = scope.join(name);
    fs::create_dir_all(&root).expect("create deep chain root");
    let original = std::env::current_dir().expect("read the working directory");
    std::env::set_current_dir(&root).expect("step into the deep chain root");
    for _ in 1..deep_chain_levels() {
        fs::create_dir(DEEP_CHAIN_LEVEL).expect("create a deep chain level");
        std::env::set_current_dir(DEEP_CHAIN_LEVEL).expect("step into a deep chain level");
    }
    fs::write(DEEP_CHAIN_LEAF, b"leaf").expect("seed the deep chain leaf file");
    std::env::set_current_dir(&original).expect("restore the working directory");
    root
}

/// Asserts one twin removes a seeded deep chain whole and still reports the
/// count it removed.
///
/// The chain is the shape a recursive walk spends its stack on: a sweep that
/// recurses per level is bounded by the tree's depth, so the failure this pins
/// is a walk that cannot finish, not a path too long to name.
fn assert_deep_chain_removed(script_name: &str, run: impl Fn(&Path, &Path, &[&str]) -> RunOutcome) {
    let (_sandbox, scope) = seeded_scope(&[]);
    let root = seed_deep_chain(&scope, FAKE_MEDIAPM_DIRS[0]);
    // Resolved before the sweep: the tree is gone afterwards, and the expected
    // spelling has to name the directory this twin prints for it.
    let expected_root = expected_root_spelling(script_name, &root);
    let janitor = script_path(script_name);

    let sweep = run(&janitor, &scope, &[]);

    assert!(
        sweep.status.success(),
        "deep-chain sweep failed: stdout {}, stderr {}",
        sweep.stdout,
        sweep.stderr
    );
    assert!(
        shell_printed(&sweep.stdout).contains(&format!("removed: {expected_root}")),
        "the sweep did not report the chain it removed: {}",
        sweep.stdout
    );
    assert!(
        sweep.stdout.contains("removed 1 mediapm temp director(ies)"),
        "the sweep did not print its count: {}",
        sweep.stdout
    );
    assert!(
        !sweep.stderr.contains("failed to remove: "),
        "the sweep failed on the deep chain: {}",
        sweep.stderr
    );
    assert!(!root.exists(), "the deep chain survived the sweep");
}

/// Seeds a `mediapm-*` symlink to a directory outside the sweep root and
/// asserts the sweep neither follows nor removes it.
///
/// The link's target sits beside the sweep root, not inside it: a sweep that
/// walks through the link unlinks files the janitor does not own, so the
/// target's contents surviving the run is the safety property this pins. Both
/// twins enumerate real directories only — `find` compares the entry's own type,
/// and `Get-ChildItem -Directory` reports a link as a container, which the pwsh
/// sweep has to skip — so the link is not a swept path and both runs report the
/// empty sweep.
#[cfg(unix)]
fn assert_root_symlink_not_followed(
    script_name: &str,
    run: impl Fn(&Path, &Path, &[&str]) -> RunOutcome,
) {
    let sandbox = Sandbox::new();
    let scope = sandbox.path.join("scope");
    fs::create_dir_all(&scope).expect("create janitor scope dir");
    let target = sandbox.path.join("symlink-target");
    fs::create_dir_all(target.join("nested")).expect("create the link target");
    fs::write(target.join("keep-file"), b"keep").expect("seed the link target's file");
    fs::write(target.join("nested").join("keep-nested"), b"keep")
        .expect("seed the link target's nested file");
    let link = scope.join(FAKE_MEDIAPM_DIRS[0]);
    std::os::unix::fs::symlink(&target, &link).expect("seed the mediapm-* symlink");
    let janitor = script_path(script_name);

    let dry = run(&janitor, &scope, &["--dry-run"]);
    assert!(dry.status.success(), "dry-run failed: {}", dry.stderr);
    assert!(
        dry.stdout.contains("no mediapm temp directories found"),
        "the dry run counted a symlink as a swept directory: {}",
        dry.stdout
    );

    let sweep = run(&janitor, &scope, &[]);
    assert!(sweep.status.success(), "sweep failed: {}", sweep.stderr);
    assert!(
        sweep.stdout.contains("no mediapm temp directories found"),
        "the sweep counted a symlink as a swept directory: {}",
        sweep.stdout
    );
    assert!(
        target.join("keep-file").is_file(),
        "the sweep followed the symlink and unlinked its target's file: {}",
        sweep.stdout
    );
    assert!(
        target.join("nested").join("keep-nested").is_file(),
        "the sweep followed the symlink and unlinked its target's nested file: {}",
        sweep.stdout
    );
    assert!(
        fs::symlink_metadata(&link).is_ok(),
        "the sweep removed the symlink itself: {}",
        sweep.stdout
    );
}

/// The sh twin never treats a `mediapm-*` symlink as a swept path: its `find`
/// tests the entry's own type, so a link is not a directory there.
#[cfg(unix)]
#[test]
fn bash_janitor_leaves_a_root_symlink_alone() {
    let Some(shell) = skip_unless_posix_bash() else {
        return;
    };
    let rm = path_with_clearing_rm();
    assert_root_symlink_not_followed("clean-mediapm-temp.sh", |script, sandbox, flags| {
        run_script(shell, script, sandbox, flags, &rm)
    });
}

#[test]
fn bash_janitor_removes_a_deep_chain() {
    let Some(shell) = skip_unless_posix_bash() else {
        return;
    };
    let rm = path_with_clearing_rm();
    assert_deep_chain_removed("clean-mediapm-temp.sh", |script, sandbox, flags| {
        run_script(shell, script, sandbox, flags, &rm)
    });
}

/// A sweep that matched a path and removed none of them must print its count
/// line, not `no mediapm temp directories found`: the empty line is a claim
/// about what the glob found, and this run found a path it could not remove.
/// The run still fails through the failure summary.
#[cfg(unix)]
#[test]
fn bash_janitor_does_not_call_a_failed_sweep_empty() {
    let Some(shell) = skip_unless_posix_bash() else {
        return;
    };
    if running_as_root() {
        println!("skipped: the sealed-tree case needs a non-root user");
        return;
    }

    let rm = path_with_clearing_rm();
    let (sandbox, scope) = seeded_scope(&FAKE_MEDIAPM_DIRS[..1]);
    let janitor = script_path("clean-mediapm-temp.sh");
    let sealed = scope.join(FAKE_MEDIAPM_DIRS[0]);
    seal_with_child(&sealed);

    let mut cmd = Command::new(shell);
    cmd.arg(&janitor);
    cmd.env("PATH", path_with_rm_exec(&sandbox.path, &rm));
    let sweep = capture_output(cmd, &scope);
    unseal(&sealed);

    assert!(
        !sweep.status.success(),
        "a tree the sweep cannot remove did not fail the run: stdout {}, stderr {}",
        sweep.stdout,
        sweep.stderr
    );
    assert!(
        shell_printed(&sweep.stderr)
            .contains(&format!("failed to remove: {}", printed_as_shell_path(&sealed))),
        "the stuck tree is not named on stderr: {}",
        sweep.stderr
    );
    assert!(
        sweep.stdout.contains("removed 0 mediapm temp director(ies)"),
        "a matched path the sweep left in place printed no count line: {}",
        sweep.stdout
    );
    assert!(
        !sweep.stdout.contains("no mediapm temp directories found"),
        "a failed sweep claimed the glob matched nothing: {}",
        sweep.stdout
    );
}

#[test]
fn pwsh_janitor_removes_a_deep_chain() {
    if !skip_unless("pwsh", &["--version"]) {
        return;
    }
    assert_deep_chain_removed("clean-mediapm-temp.ps1", run_pwsh);
}

/// Same shape as [`bash_janitor_leaves_a_root_symlink_alone`]: `-Directory`
/// reports a link to a directory as a container, so the pwsh twin has to skip
/// links explicitly to stay on the same set of paths as the sh twin.
#[cfg(unix)]
#[test]
fn pwsh_janitor_leaves_a_root_symlink_alone() {
    if !skip_unless("pwsh", &["--version"]) {
        return;
    }
    assert_root_symlink_not_followed("clean-mediapm-temp.ps1", run_pwsh);
}

/// Runs a janitor self-test, skipping when its interpreter is absent.
fn assert_self_test(
    script: &Path,
    ok_marker: &str,
    program: &str,
    probe: &[&str],
    run: impl Fn(&Path, &Path, &[&str]) -> RunOutcome,
) {
    if !skip_unless(program, probe) {
        return;
    }
    let sandbox = Sandbox::new();
    let out = run(script, &sandbox.path, &[]);
    assert!(out.status.success(), "self-test failed: {}", out.stderr);
    assert!(out.stdout.contains(ok_marker), "self-test missing OK line");
}

#[test]
fn bash_janitor_self_test() {
    // The self-test spawns `sh`, but the working `rm` this test resolves for it
    // is found through a POSIX shell (`command -v rm`), so this test needs one
    // just as the direct ones do. Skipping on the bare name would let the WSL
    // launcher stand for the shell that resolution depends on.
    if skip_unless_posix_bash().is_none() {
        return;
    }
    let rm = path_with_clearing_rm();
    assert_self_test(
        &test_script_path("test-clean-mediapm-temp.sh"),
        "test-clean-mediapm-temp: OK",
        "sh",
        &["-c", "exit 0"],
        |script, sandbox, flags| run_script(Path::new("sh"), script, sandbox, flags, &rm),
    );
}

/// A host whose only `rm` candidate cannot run must fail the suite, naming the
/// environment and what could not be proven.
///
/// The list holds one path that cannot exist, so the rejection travels the real
/// route: [`rm_clears_probe`] spawns the candidate, `Command::status()` fails,
/// and the panic is composed the way production composes it. A green run must
/// never stand for a sweep that never happened.
#[test]
fn no_working_rm_fails_the_harness() {
    let outcome = std::panic::catch_unwind(|| {
        path_with_clearing_rm_with_candidates(&[PathBuf::from("/nonexistent/rm-that-cannot-exist")])
    });
    let panic = outcome.expect_err("a candidate that cannot run must not resolve an rm");
    let message = panic_message(&panic);
    assert!(
        message.contains("no working rm"),
        "the failure names the environment, not just an index: {message}"
    );
    assert!(
        message.contains("clean-mediapm-temp"),
        "the failure says what could not be proven: {message}"
    );
    assert!(message.contains("rm-that-cannot-exist"), "the message names the candidate: {message}");
    assert!(message.contains("exit 0"), "the message states the predicate: {message}");
}

/// The text of a caught panic, which is boxed as a `&str` or a `String`.
///
/// The receiver is the box, not `&(dyn Any + Send)`: coercing the box away first
/// makes the box itself the payload the downcast compares against, and every
/// panic then reports as a non-string payload.
fn panic_message(panic: &Box<dyn Any + Send>) -> &str {
    panic
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
        .expect("a panic payload is a string")
}

#[test]
fn pwsh_janitor_dry_run_and_real_run() {
    if !skip_unless("pwsh", &["--version"]) {
        return;
    }
    assert_janitor_sequence("clean-mediapm-temp.ps1", run_pwsh);
}

#[test]
fn pwsh_janitor_self_test() {
    assert_self_test(
        &test_script_path("test-clean-mediapm-temp.ps1"),
        "test-clean-mediapm-temp.ps1: OK",
        "pwsh",
        &["--version"],
        run_pwsh,
    );
}

#[test]
fn script_files_exist_and_are_executable() {
    for (name, path) in SCRIPTS
        .iter()
        .map(|name| (*name, script_path(name)))
        .chain(TEST_SCRIPTS.iter().map(|name| (*name, test_script_path(name))))
    {
        assert!(path.is_file(), "missing script: {name}");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(&path).expect("stat script").permissions().mode();
            assert_ne!(mode & 0o111, 0, "script not executable: {name}");
        }
    }
}
