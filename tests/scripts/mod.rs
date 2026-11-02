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
//! still works: the self-test supplies its own working `rm` for its sweep cases
//! and pins the janitor's refusal with a shim that removes nothing. The janitor
//! itself refuses such a host before it touches a path, so a real run there
//! reports no removal and exits non-zero; [`bash_janitor_refuses_a_failing_rm`]
//! pins that.
//! The probe requires a candidate to exit 0 with its probe directory's source
//! path gone, which a trash move also satisfies, so the harness needs no special
//! case for a trash-moving `rm`: the janitor's sweep depends on the path being
//! gone, not on how it got there.

use std::any::Any;
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus};
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
fn candidate_list() -> Vec<PathBuf> {
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

/// `sh -c 'command -v <name>'` output, or `None` when the shell cannot answer.
fn command_v(name: &str) -> Option<PathBuf> {
    let output = Command::new("sh").args(["-c", &format!("command -v {name}")]).output().ok()?;
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
    let status = Command::new(candidate).arg("-rf").arg(&probe).status();
    let gone = !probe.exists();
    let _ = fs::remove_dir_all(&probe);
    matches!(status, Ok(status) if status.success()) && gone
}

/// Single-quotes `path` for the shim's `exec` line.
fn shell_quote(path: &Path) -> String {
    format!("'{}'", path.to_string_lossy().replace('\'', r"'\''"))
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
    let mut path = OsString::from(&bin);
    path.push(":");
    path.push(std::env::var_os("PATH").unwrap_or_default());
    path
}

/// A `PATH` value whose first `rm` is `real_rm`.
fn path_with_rm_exec(sandbox: &Path, real_rm: &Path) -> OsString {
    path_with_rm_shim(sandbox, &format!("exec {} \"$@\"\n", shell_quote(real_rm)))
}

/// A `PATH` value whose first `rm` prints nothing and fails without deleting.
fn path_with_failing_rm(sandbox: &Path) -> OsString {
    path_with_rm_shim(sandbox, "exit 1\n")
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
    if !skip_unless("bash", &["--version"]) {
        return;
    }
    let rm = path_with_clearing_rm();
    assert_janitor_sequence("clean-mediapm-temp.sh", |script, sandbox, flags| {
        run_script("bash", script, sandbox, flags, &rm)
    });
}

/// A `--dry-run` never reaches `rm` in the sweep loop: the preflight is guarded
/// on a real run and the loop only prints, so the one removal a dry run does is
/// the trap's unlink of its own listing. That is what this pins: the shimmed
/// `rm` fails the trap, and the dry run must still report its sweep and exit 0.
///
/// The host's own `rm` is irrelevant here, so this test does not ask for one:
/// what it needs is a `PATH` whose `rm` fails. It proves that `PATH` shadows the
/// host's `rm` before running the janitor, so a shim that stopped shadowing
/// fails the test rather than leaving it green and pinning nothing.
#[test]
fn bash_janitor_dry_run_survives_a_failing_rm() {
    if !skip_unless("bash", &["--version"]) {
        return;
    }
    let seeded = &FAKE_MEDIAPM_DIRS[..2];
    let (sandbox, scope) = seeded_scope(seeded);
    let janitor = script_path("clean-mediapm-temp.sh");

    let path = path_with_failing_rm(&sandbox.path);

    // Prove the shim still shadows `rm` before trusting the run below: if the
    // host's `rm` resolved instead, the trap would pass either way and this
    // test would pin nothing.
    let scratch = sandbox.path.join("rm-shadow-probe");
    fs::create_dir_all(&scratch).expect("create rm shadow probe dir");
    let shadow_shim = Command::new("sh")
        .args(["-c", r#"rm -rf "$1""#, "_"])
        .arg(&scratch)
        .env("PATH", &path)
        .status()
        .expect("spawn rm shadow probe");
    assert!(
        !shadow_shim.success(),
        "the failing rm shim did not shadow the host rm, so the dry run below proves nothing"
    );

    let mut cmd = Command::new("bash");
    cmd.arg(&janitor).arg("--dry-run");
    cmd.env("PATH", &path);
    let dry = capture_output(cmd, &scope);

    assert!(
        dry.status.success(),
        "a failing rm failed the dry run: stdout {}, stderr {}",
        dry.stdout,
        dry.stderr
    );
    let reported: Vec<&str> =
        dry.stdout.lines().filter(|line| line.starts_with("would remove: ")).collect();
    assert_eq!(
        reported.len(),
        seeded.len(),
        "dry-run reported {reported:?}, expected {}",
        seeded.len()
    );
    for name in seeded {
        assert!(
            reported.iter().any(|line| line.ends_with(name)),
            "dry-run did not report {name}: {reported:?}"
        );
    }
}

/// A real run whose `rm` cannot remove a directory the janitor itself created
/// must refuse before it reports anything. `removed:` is a claim about a tree,
/// and a host that shims `rm` (a safe-delete wrapper whose trash step is denied)
/// would otherwise print it for trees left in place.
#[test]
fn bash_janitor_refuses_a_failing_rm() {
    if !skip_unless("bash", &["--version"]) {
        return;
    }
    let seeded = &FAKE_MEDIAPM_DIRS[..2];
    let (sandbox, scope) = seeded_scope(seeded);
    let janitor = script_path("clean-mediapm-temp.sh");

    let path = path_with_failing_rm(&sandbox.path);

    // Prove the shim still shadows `rm` before trusting the run below: if the
    // host's `rm` resolved instead, the refusal would be untested and the sweep
    // would look like the thing that produced the exit status.
    let scratch = sandbox.path.join("rm-shadow-probe");
    fs::create_dir_all(&scratch).expect("create rm shadow probe dir");
    let shadow_shim = Command::new("sh")
        .args(["-c", r#"rm -rf "$1""#, "_"])
        .arg(&scratch)
        .env("PATH", &path)
        .status()
        .expect("spawn rm shadow probe");
    assert!(
        !shadow_shim.success(),
        "the failing rm shim did not shadow the host rm, so the refusal below proves nothing"
    );

    let mut cmd = Command::new("bash");
    cmd.arg(&janitor);
    cmd.env("PATH", &path);
    let refused = capture_output(cmd, &scope);

    assert!(
        !refused.status.success(),
        "a failing rm did not fail the run: stdout {}, stderr {}",
        refused.stdout,
        refused.stderr
    );
    assert!(
        !refused.stdout.lines().any(|line| line.starts_with("removed:")),
        "the janitor reported removals it did not perform: {}",
        refused.stdout
    );
    let diagnostic = format!("cannot remove directories under {}", scope.display());
    assert!(
        refused.stderr.contains(&diagnostic),
        "the refusal does not name the sweep root: {}",
        refused.stderr
    );
    assert!(
        refused.stderr.contains(" survived"),
        "the refusal does not say the probe survived: {}",
        refused.stderr
    );
    for name in seeded {
        assert!(scope.join(name).is_dir(), "{name} is gone after a refused sweep");
    }
}

/// A real run whose preflight passes but whose sweep cannot empty one tree must
/// name that tree on stderr and still fail, while the rest of the sweep proceeds:
/// one stuck tree must not hide the verdict for the others.
#[cfg(unix)]
#[test]
fn bash_janitor_reports_a_per_path_failure() {
    if !skip_unless("bash", &["--version"]) {
        return;
    }
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

    let mut cmd = Command::new("bash");
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
        partial.stderr.contains(&format!("failed to remove: {}", sealed.display())),
        "the stuck tree is not named on stderr: {}",
        partial.stderr
    );
    assert!(
        partial.stderr.contains("1 director(ies) could not be removed"),
        "the failure summary is missing: {}",
        partial.stderr
    );
    assert!(
        partial.stdout.contains(&format!("removed: {}", removable.display())),
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

/// Name of each seeded chain level.
const DEEP_CHAIN_LEVEL: &str = "d";

/// The file at the bottom of the chain, so the sweep has to unlink a file and
/// not only empty directories.
const DEEP_CHAIN_LEAF: &str = "leaf";

/// Seeds `<scope>/<name>/d/d/...` [`DEEP_CHAIN_LEVELS`] deep with a leaf file at
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
    for _ in 1..DEEP_CHAIN_LEVELS {
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
    let janitor = script_path(script_name);

    let sweep = run(&janitor, &scope, &[]);

    assert!(
        sweep.status.success(),
        "deep-chain sweep failed: stdout {}, stderr {}",
        sweep.stdout,
        sweep.stderr
    );
    assert!(
        sweep.stdout.contains(&format!("removed: {}", root.display())),
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
    if !skip_unless("bash", &["--version"]) {
        return;
    }
    let rm = path_with_clearing_rm();
    assert_root_symlink_not_followed("clean-mediapm-temp.sh", |script, sandbox, flags| {
        run_script("bash", script, sandbox, flags, &rm)
    });
}

#[test]
fn bash_janitor_removes_a_deep_chain() {
    if !skip_unless("bash", &["--version"]) {
        return;
    }
    let rm = path_with_clearing_rm();
    assert_deep_chain_removed("clean-mediapm-temp.sh", |script, sandbox, flags| {
        run_script("bash", script, sandbox, flags, &rm)
    });
}

/// A sweep that matched a path and removed none of them must print its count
/// line, not `no mediapm temp directories found`: the empty line is a claim
/// about what the glob found, and this run found a path it could not remove.
/// The run still fails through the failure summary.
#[cfg(unix)]
#[test]
fn bash_janitor_does_not_call_a_failed_sweep_empty() {
    if !skip_unless("bash", &["--version"]) {
        return;
    }
    if running_as_root() {
        println!("skipped: the sealed-tree case needs a non-root user");
        return;
    }

    let rm = path_with_clearing_rm();
    let (sandbox, scope) = seeded_scope(&FAKE_MEDIAPM_DIRS[..1]);
    let janitor = script_path("clean-mediapm-temp.sh");
    let sealed = scope.join(FAKE_MEDIAPM_DIRS[0]);
    seal_with_child(&sealed);

    let mut cmd = Command::new("bash");
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
        sweep.stderr.contains(&format!("failed to remove: {}", sealed.display())),
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
    let rm = path_with_clearing_rm();
    assert_self_test(
        &test_script_path("test-clean-mediapm-temp.sh"),
        "test-clean-mediapm-temp: OK",
        "sh",
        &["-c", "exit 0"],
        |script, sandbox, flags| run_script("sh", script, sandbox, flags, &rm),
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
