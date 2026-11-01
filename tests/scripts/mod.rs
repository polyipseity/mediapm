//! Integration tests for repository scripts: the temp-janitor production
//! scripts (`scripts/clean-mediapm-temp.{sh,ps1}`) and their self-tests.
//!
//! The test pipeline itself is not tested from here. It is the
//! `test-runner` crate, and it carries its own gate tests in Rust.
//!
//! Platform-smart via runtime probes: each interpreter (`bash`, `sh`,
//! `pwsh`) is probed by spawning a harmless command, and when the binary is
//! missing the test prints `skipped: ...` and passes without running.
//! Sandboxes use the managed `mediapm-` prefix under the real temp dir and
//! are removed on drop.
//!
//! The bash janitor tests additionally need an `rm` that deletes. Some hosts
//! put a safe-delete wrapper first on `PATH`, and when its trash step is
//! denied (macOS Finder automation is the sandbox case) it exits non-zero
//! and leaves the tree in place, so the sweep cannot run there at all. The
//! harness probes for a deleting `rm`, hands the children a `PATH` whose
//! first `rm` is that one, and skips with a message when the host has none,
//! so an environment limitation is never reported as a janitor defect.
//! Running `tests/scripts/test-clean-mediapm-temp.sh` by hand on such a host
//! still fails at the janitor's non-zero exit for the same reason; the
//! harness is what supplies the working `rm`.

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

/// Skip line for a host whose `rm` cannot delete.
const NO_WORKING_RM: &str = "no working rm on this host; the janitor's sweep cannot be proven here";

/// The `rm` the bash janitor tests hand their children, or `None` after
/// printing the skip line. See [`working_rm`] for why the host's own `rm`
/// cannot be assumed.
fn skip_unless_real_rm() -> Option<&'static Path> {
    let rm = working_rm();
    if rm.is_none() {
        println!("skipped: {NO_WORKING_RM}");
    }
    rm
}

/// A real `rm` on this host: one that deletes and exits 0.
///
/// Probed once per process and verified by deleting, not by name or path:
/// the wrapper this probe exists for is called `rm` too, and one exists on
/// this host that reports a non-zero status while leaving the tree in place.
fn working_rm() -> Option<&'static Path> {
    static WORKING: OnceLock<Option<PathBuf>> = OnceLock::new();
    WORKING
        .get_or_init(|| rm_candidates().into_iter().find(|candidate| rm_deletes(candidate)))
        .as_deref()
}

/// Candidate real `rm`s, most specific first.
///
/// `command -v rm` is last because it is whatever the host resolves, which
/// is the wrapper on the hosts this probe exists for; the delete check is
/// what makes the order safe rather than the paths themselves.
fn rm_candidates() -> Vec<PathBuf> {
    let mut candidates: Vec<PathBuf> =
        ["/bin/rm", "/usr/bin/rm", "/usr/local/rm", "/usr/local/bin/rm"]
            .into_iter()
            .map(PathBuf::from)
            .filter(|candidate| !is_local_bin_shim(candidate))
            .collect();
    candidates.extend(command_v("rm").filter(|rm| !is_local_bin_shim(rm)));
    candidates
}

/// Whether `path` sits in `~/.local/bin`, where hosts install wrappers that
/// shadow the system `rm`.
///
/// The delete check already rejects a broken wrapper; this keeps a working
/// one out of the probe, so a test never fills the user's Trash.
fn is_local_bin_shim(path: &Path) -> bool {
    let Some(home) = std::env::var_os("HOME") else {
        return false;
    };
    path.parent() == Some(Path::new(&home).join(".local").join("bin").as_path())
}

/// `sh -c 'command -v <name>'` output, or `None` when the shell cannot answer.
fn command_v(name: &str) -> Option<PathBuf> {
    let output = Command::new("sh").args(["-c", &format!("command -v {name}")]).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let path = std::str::from_utf8(&output.stdout).ok()?.trim();
    (!path.is_empty()).then(|| PathBuf::from(path))
}

/// Whether `candidate` deletes a directory: exit 0 with the directory gone.
///
/// Both halves matter: a trash wrapper that is denied exits non-zero and
/// leaves the tree, and a candidate that cannot delete cannot prove the
/// janitor's sweep. The probe dir carries the managed `mediapm-` prefix, so
/// a crash leak is reclaimed by the janitor like any other.
fn rm_deletes(candidate: &Path) -> bool {
    let seq = SANDBOX_SEQ.fetch_add(1, Ordering::Relaxed);
    let probe = std::env::temp_dir().join(format!("mediapm-rm-probe-{}-{seq}", std::process::id()));
    if fs::create_dir_all(&probe).is_err() {
        return false;
    }
    let status = Command::new(candidate).arg("-rf").arg(&probe).status();
    let gone = !probe.exists();
    let _ = fs::remove_dir_all(&probe);
    matches!(status, Ok(status) if status.success()) && gone
}

/// Single-quotes `path` for the shim's `exec` line.
fn shell_quote(path: &Path) -> String {
    format!("'{}'", path.to_string_lossy().replace('\'', r"'\''"))
}

/// A `PATH` value whose first `rm` is `real_rm`.
///
/// The shim lives inside the test's sandbox so it is removed with it: a
/// `mediapm-`-prefixed dir that outlived the test would trip the orphan gate.
fn path_with_real_rm(sandbox: &Path, real_rm: &Path) -> OsString {
    let bin = sandbox.join("rm-bin");
    fs::create_dir_all(&bin).expect("create rm shim dir");
    let shim = bin.join("rm");
    fs::write(&shim, format!("#!/bin/sh\nexec {} \"$@\"\n", shell_quote(real_rm)))
        .expect("write rm shim");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let permissions = fs::Permissions::from_mode(0o755);
        fs::set_permissions(&shim, permissions).expect("make rm shim executable");
    }
    let mut path = OsString::from(&bin);
    path.push(":");
    path.push(std::env::var_os("PATH").unwrap_or_default());
    path
}

/// Runs a script under `program` (e.g. `bash` or `sh`) inside a sandbox,
/// with `rm` resolving to `real_rm`.
fn run_script(
    program: &str,
    script: &Path,
    sandbox: &Path,
    flags: &[&str],
    real_rm: &Path,
) -> RunOutcome {
    let mut cmd = Command::new(program);
    cmd.arg(script).args(flags);
    cmd.env("PATH", path_with_real_rm(sandbox, real_rm));
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

/// Exercises the full janitor sequence (dry-run, real run, empty dry-run,
/// help, bogus arg) against a sandbox seeded with fake dirs.
///
/// The janitor scans with `find "$tmp_root" -maxdepth 1 -name 'mediapm-*'`,
/// which also matches the starting point itself. The sandbox root carries
/// the managed `mediapm-` prefix (so a leaked sandbox is reclaimed by the
/// janitor / orphan gate), so the seeded dirs live in a nested `scope`
/// subdir whose basename does not match the glob; the janitor is pointed at
/// `scope` via the child-scoped `TMPDIR`.
fn assert_janitor_sequence(script_name: &str, run: impl Fn(&Path, &Path, &[&str]) -> RunOutcome) {
    let sandbox = Sandbox::new();
    let scope = sandbox.path.join("scope");
    fs::create_dir_all(&scope).expect("create janitor scope dir");
    for name in FAKE_MEDIAPM_DIRS.iter().copied().chain(CONTROL_DIRS.iter().copied()) {
        fs::create_dir_all(scope.join(name)).expect("create seeded sandbox dir");
    }
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
    if !skip_unless("bash", &["--version"]) {
        return;
    }
    let Some(rm) = skip_unless_real_rm() else {
        return;
    };
    assert_janitor_sequence("clean-mediapm-temp.sh", |script, sandbox, flags| {
        run_script("bash", script, sandbox, flags, rm)
    });
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
    let Some(rm) = skip_unless_real_rm() else {
        return;
    };
    assert_self_test(
        &test_script_path("test-clean-mediapm-temp.sh"),
        "test-clean-mediapm-temp: OK",
        "sh",
        &["-c", "exit 0"],
        |script, sandbox, flags| run_script("sh", script, sandbox, flags, rm),
    );
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
