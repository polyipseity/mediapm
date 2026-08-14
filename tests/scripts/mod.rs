//! Integration tests for repository scripts: the temp-janitor production
//! scripts (`scripts/clean-mediapm-temp.{sh,ps1}`), their self-tests, and
//! the run-all-tests runner self-tests (`tests/scripts/test-run-all-tests.*`).
//!
//! Platform-smart via runtime probes: each interpreter (`bash`, `sh`,
//! `pwsh`) is probed by spawning a harmless command, and when the binary is
//! missing the test prints `skipped: ...` and passes without running.
//! Sandboxes use the managed `mediapm-` prefix under the real temp dir and
//! are removed on drop.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus};
use std::sync::atomic::{AtomicU64, Ordering};

/// Sequence number for sandbox names (unique per process).
static SANDBOX_SEQ: AtomicU64 = AtomicU64::new(0);

/// Production janitor scripts exercised by this crate.
const SCRIPTS: [&str; 2] = ["clean-mediapm-temp.sh", "clean-mediapm-temp.ps1"];

/// Script self-tests exercised by this crate (janitors + runners).
const TEST_SCRIPTS: [&str; 4] = [
    "test-clean-mediapm-temp.sh",
    "test-clean-mediapm-temp.ps1",
    "test-run-all-tests.sh",
    "test-run-all-tests.ps1",
];

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

/// Runs a script under `program` (e.g. `bash` or `sh`) inside a sandbox.
fn run_script(program: &str, script: &Path, sandbox: &Path, flags: &[&str]) -> RunOutcome {
    let mut cmd = Command::new(program);
    cmd.arg(script).args(flags);
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
    assert_janitor_sequence("clean-mediapm-temp.sh", |script, sandbox, flags| {
        run_script("bash", script, sandbox, flags)
    });
}

/// Runs the sh-based janitor/runner self-test, skipping when `sh` is absent.
fn assert_sh_self_test(script: &Path, ok_marker: &str) {
    if !skip_unless("sh", &["-c", "exit 0"]) {
        return;
    }
    let sandbox = Sandbox::new();
    let out = run_script("sh", script, &sandbox.path, &[]);
    assert!(out.status.success(), "self-test failed: {}", out.stderr);
    assert!(out.stdout.contains(ok_marker), "self-test missing OK line");
}

/// Runs the pwsh-based janitor/runner self-test, skipping when `pwsh` is absent.
fn assert_pwsh_self_test(script: &Path, ok_marker: &str) {
    if !skip_unless("pwsh", &["--version"]) {
        return;
    }
    let sandbox = Sandbox::new();
    let out = run_pwsh(script, &sandbox.path, &[]);
    assert!(out.status.success(), "self-test failed: {}", out.stderr);
    assert!(out.stdout.contains(ok_marker), "self-test missing OK line");
}

#[test]
fn bash_janitor_self_test() {
    assert_sh_self_test(
        &test_script_path("test-clean-mediapm-temp.sh"),
        "test-clean-mediapm-temp: OK",
    );
}

#[test]
fn pwsh_janitor_dry_run_and_real_run() {
    if !skip_unless("pwsh", &["--version"]) {
        return;
    }
    assert_janitor_sequence("clean-mediapm-temp.ps1", |script, sandbox, flags| {
        run_pwsh(script, sandbox, flags)
    });
}

#[test]
fn pwsh_janitor_self_test() {
    assert_pwsh_self_test(
        &test_script_path("test-clean-mediapm-temp.ps1"),
        "test-clean-mediapm-temp.ps1: OK",
    );
}

#[test]
fn bash_runner_self_test() {
    assert_sh_self_test(&test_script_path("test-run-all-tests.sh"), "test-run-all-tests.sh: OK");
}

#[test]
fn pwsh_runner_self_test() {
    assert_pwsh_self_test(
        &test_script_path("test-run-all-tests.ps1"),
        "test-run-all-tests.ps1: OK",
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
