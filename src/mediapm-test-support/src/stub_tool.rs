//! Locates the `mediapm-stub-tool` binary this crate ships for hermetic runs.
//!
//! A test that needs a managed tool to execute — the media-tagger apply step's
//! `ffmpeg`, a conductor step that must outlast a budget — cannot use a real
//! tool: the real one is a download, and a shell would reintroduce the platform
//! dependency the stub exists to remove. This module resolves the stub's path
//! from the running test binary's own location, which is the only fact a test
//! can rely on. Cargo places every package's binary targets in one target
//! directory, and `CARGO_BIN_EXE_<name>` is defined for the current package
//! alone, so a dev-dependency's binary is reachable only by path.
//!
//! # Why a located candidate must prove itself
//!
//! A stub that cannot be found would turn a step-execution assertion into a
//! no-op that passes, which is the failure class these tests exist to remove.
//! So [`stub_tool_path`] returns neither a path it has not seen run nor a panic
//! for a run that can build the stub itself.
//!
//! The search cannot trust a file name. `cargo test --all-targets` compiles the
//! bin target a second time as a libtest harness under `deps/`, and Cargo names
//! that copy the way it names the executable, so `mediapm_stub_tool-<hash>` is
//! either. A search that accepted the first file with the right name handed
//! every stub-based test a harness, which answered `running 0 tests` and exit 0
//! to the bare invocation the test expected to be a usage failure. Each candidate
//! is therefore spawned through `behaves_like_the_stub`, and one that answers
//! like a harness is skipped as the search continues.
//!
//! Absence is answered by compiling the stub from the source embedded in this
//! module (`STUB_TOOL_SOURCE`) into the running test binary's directory: a
//! consumer's `cargo test-pkg` selects that consumer's package alone, so this
//! dev-dependency's binary target is not among the targets Cargo builds and the
//! search legitimately finds nothing. The panic remains for the case no
//! executable can be produced from — a directory that cannot be written.
//!
//! # How a compiled fallback knows it is current
//!
//! A compiled fallback carries no Cargo fingerprint: Cargo never builds it, so
//! nothing else on the machine notices when `src/bin/mediapm-stub-tool.rs`
//! changes. Trusting the file because it exists would keep testing whatever the
//! source said when it was first compiled — across an edit, a checkout, or a
//! `git bisect` — and every stub-based test in the workspace would then test the
//! previous build while reporting the current one.
//!
//! So the compile records a fingerprint of the source it used beside the
//! executable, and [`stub_tool_path`] accepts a stub in the fallback slot only
//! when `compiled_from` finds that record naming the embedded source. A changed
//! source leaves the record mismatched, the search skips the artifact, and the run
//! reaches `compile_stub_into` again. Nothing but the file's content is
//! inspected, so the verdict does not depend on clock skew, and it costs one small
//! read per search.
//!
//! Cargo's own copies need no record: Cargo's fingerprint already decides whether
//! its binary target is current, and the copies it leaves in `deps/` are named per
//! build.

use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// File name of the stub binary, without the platform executable suffix.
pub const STUB_TOOL_BIN_NAME: &str = "mediapm-stub-tool";

/// The stub's source, embedded so a test process can build it without Cargo.
///
/// Embedding rather than locating the file keeps the compile fallback free of a
/// workspace-layout assumption: the bytes travel with the compiled test binary,
/// so a test process in any directory still has the source it must compile. It is
/// also the reference a fallback's recorded fingerprint is compared against, so
/// the bytes that build the stub and the bytes that decide its freshness are the
/// same bytes rather than two files that could disagree.
const STUB_TOOL_SOURCE: &str = include_str!("bin/mediapm-stub-tool.rs");

/// Suffix of the record that says which source a fallback was compiled from.
///
/// The record's path is derived by appending this suffix to the executable's
/// whole path, so the record lives beside the file it describes and no
/// name-splitting is needed: a caller has the executable's path and nothing else.
///
/// What keeps the record out of [`hashed_candidates`] is that its name keeps the
/// binary's hyphens and therefore never begins with the `mediapm_stub_tool-`
/// prefix the search derives from Cargo's underscore spelling; the dot takes no
/// part in that, because the prefix check rejects the name before the shape of
/// the hash is examined.
const SOURCE_FINGERPRINT_SUFFIX: &str = ".source-fingerprint";

/// How long a candidate may take to answer before it is judged not the stub.
///
/// The stub prints a usage line and exits, so a candidate still running this long
/// after its launch is not answering at all. Without the bound such a candidate
/// holds the locator — and with it the whole test run — for as long as it lives: an
/// impostor that sleeps, or a program that waits on something no one will send, is
/// the shape that turns one search into a hang. The bound is generous for a process
/// that prints one line, because the cost of a false rejection is a stub that
/// cannot be located.
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// How often a probe re-asks whether its candidate has exited.
///
/// The probe polls because `std::process::Child` offers no timeout of its own, and
/// no thread enforces the bound of the wait: a thread that waited would have to be
/// abandoned when the bound expired, and an abandoned thread is a leak in a test
/// process that lives until the whole suite finishes. The one thread this module
/// does start is the reader that takes the candidate's stderr, and the probe
/// waits on that reader for at most [`PROBE_TIMEOUT`]; the read's own comment
/// states what a candidate that leaves a writer behind costs.
const PROBE_POLL_INTERVAL: Duration = Duration::from_millis(10);

/// Separates the compiles one process makes.
///
/// A process id does not name a compile. Cargo runs a test binary's tests as
/// threads of one process, so several tests reach `compile_stub_into` at once
/// under one pid — `cargo test -p mediapm-test-support` is exactly that — and a
/// work directory keyed on the pid alone would be shared by all of them, one
/// thread removing the codegen objects or the staged executable another thread is
/// still linking. A process-wide counter, drawn before the directory is made,
/// gives every call a directory no other call in this process uses.
static COMPILE_INVOCATION: AtomicU64 = AtomicU64::new(0);

/// Compiles the stub into `directory` and returns the executable's path.
///
/// Each compile works in a directory of its own, and the finished executable is
/// moved into place by rename, so two callers that both find no stub neither
/// observe each other's half-written binary nor collide while building it:
/// `rustc` names its codegen objects after the crate rather than after the `-o`
/// path, so two compiles sharing an output directory sign the same object names
/// and the loser fails to link. The work directory sits under `directory` instead
/// of in the system temporary directory because a rename cannot cross
/// filesystems, and the move into place must be atomic.
///
/// The name of that directory carries the process id **and** a per-call counter
/// (`COMPILE_INVOCATION`) because the id alone does not separate two compiles:
/// a test binary runs its tests as threads of one process, so two tests can enter
/// this function at the same instant under the same pid and would then share the
/// directory the cleanup removes — one thread deleting the objects, or the staged
/// executable, of another still running its link.
///
/// The compile writes the source's fingerprint to the record beside the
/// executable, because nothing else tracks this file's input: Cargo does not know
/// it exists, so its own fingerprint can never say whether the file still matches
/// `src/bin/mediapm-stub-tool.rs`. The record is written *after* the executable is
/// in place, which is the safe order — a record written first could describe a
/// binary a concurrent compile is about to replace, and the search would then
/// trust the older file, while an interrupted compile leaves a record that does
/// not match and is rebuilt.
///
/// The move into place tolerates one failure. Windows refuses to replace a file
/// another process is executing, and a failed move is then not a lost build: the
/// file already at the destination answers what the move wanted, because another
/// compile in this run wrote it from the same source. `compiled_from` is what
/// separates that case from a move that left no build of this source behind,
/// which still panics.
///
/// The compiler is the one already running these tests: `rustc` is resolved from
/// `PATH`, and that resolution relies on the environment Cargo sets for a test
/// run. Invoked outside a Cargo test run, a rustup proxy with no default toolchain
/// configured refuses to start rather than choosing one, so a caller that reaches
/// this outside Cargo has to provide the toolchain environment Cargo would have.
///
/// # Panics
///
/// Panics when the work directory, the source or the record cannot be written,
/// when `rustc` cannot be spawned or rejects the source, or when the executable
/// cannot be moved into place **and** the destination holds no build of this
/// source. Every one of those leaves no runnable executable, and a path that does
/// not run is exactly the vacuous pass the locator exists to prevent.
#[must_use]
fn compile_stub_into(directory: &Path) -> PathBuf {
    let file_name = format!("{STUB_TOOL_BIN_NAME}{}", std::env::consts::EXE_SUFFIX);
    let target = directory.join(&file_name);
    let record = record_path(&target);
    let invocation = COMPILE_INVOCATION.fetch_add(1, Ordering::Relaxed);
    let build =
        directory.join(format!("{file_name}.mediapm-build-{}-{invocation}", std::process::id()));
    let source = build.join(format!("{STUB_TOOL_BIN_NAME}.rs"));
    let staged = build.join(&file_name);

    std::fs::create_dir_all(&build).unwrap_or_else(|error| {
        panic!("the stub's build directory must be writable at {}: {error}", build.display())
    });
    std::fs::write(&source, STUB_TOOL_SOURCE).unwrap_or_else(|error| {
        panic!("the embedded stub source must be writable at {}: {error}", source.display())
    });
    let output = std::process::Command::new("rustc")
        .args(["--edition", "2024", "--crate-name", "mediapm_stub_tool", "-o"])
        .arg(&staged)
        .arg(&source)
        .output()
        .unwrap_or_else(|error| panic!("rustc must be on PATH for a Cargo test run: {error}"));
    if !output.status.success() {
        // The work directory holds nothing worth keeping once the compile has
        // failed, and leaving it behind would put the compiler's objects in
        // `target/` for a run that never produced an executable.
        let _ = std::fs::remove_dir_all(&build);
        panic!("compiling the stub failed: {}", String::from_utf8_lossy(&output.stderr));
    }
    if let Err(error) = std::fs::rename(&staged, &target) {
        // A sharing violation on Windows is the expected failure here: the file at
        // `target` is the one a concurrent compile staged and another process is
        // executing. The destination is right when the record beside it names this
        // source, which only that compile's own move-then-record sequence writes.
        if !compiled_from(&target, STUB_TOOL_SOURCE) {
            let _ = std::fs::remove_dir_all(&build);
            panic!("the staged stub must move into place at {}: {error}", target.display());
        }
    }
    std::fs::write(&record, source_fingerprint(STUB_TOOL_SOURCE)).unwrap_or_else(|error| {
        panic!("the fallback's source record must be writable at {}: {error}", record.display())
    });
    let _ = std::fs::remove_dir_all(&build);
    target
}

/// Path of the record beside `executable` that says which source built it.
///
/// The record lives beside the file it describes, so removing the artifact
/// removes the claim about it as well.
fn record_path(executable: &Path) -> PathBuf {
    let mut record = executable.as_os_str().to_os_string();
    record.push(SOURCE_FINGERPRINT_SUFFIX);
    PathBuf::from(record)
}

/// Fingerprint of `source`, the value recorded beside a compiled fallback.
///
/// A fingerprint rather than the source itself keeps the record small and its
/// comparisons exact. Sixteen hex digits is the whole output of the hasher the
/// workspace already keys content-derived names with (`mediapm_utils::temp`'s
/// runtime directory), so a fingerprint reads the same way wherever it appears.
///
/// The hasher's algorithm is unspecified and may change between Rust releases,
/// which costs at most one recompile per toolchain change: a record that no
/// longer matches is a fallback the search rebuilds, never one it trusts.
#[must_use]
fn source_fingerprint(source: &str) -> String {
    let mut hasher = DefaultHasher::new();
    hasher.write(source.as_bytes());
    format!("{:016x}", hasher.finish())
}

/// Reports whether `executable` was compiled from `source`.
///
/// This is the invalidation check the locator runs before it trusts a fallback:
/// the answer is `false` for a source that changed and for a file that carries no
/// record at all, so a stub nobody compiled here is never taken to be the
/// fallback. It reads the record and nothing else, so the verdict depends on the
/// source it is asked about rather than on the file's age or on the timing of the
/// run, and it answers without executing or rebuilding anything.
fn compiled_from(executable: &Path, source: &str) -> bool {
    std::fs::read_to_string(record_path(executable))
        .is_ok_and(|recorded| recorded.trim() == source_fingerprint(source))
}

/// Reports whether `path` answers like the stub rather than like a test harness.
///
/// `cargo test --all-targets` compiles the bin target a second time as a libtest
/// harness under `deps/`, and Cargo names that copy the same way it names the
/// executable, so no file name tells them apart. The stub answers a bare
/// invocation with its usage failure — exit 2 and a `--mode is required` line —
/// while the harness answers `running 0 tests` and exits 0. Spawning the
/// candidate is the only check that separates them, and it costs one process
/// launch per candidate tested.
///
/// A candidate that cannot be spawned is not the stub either: handing a caller a
/// path that exists and cannot be executed moves the failure away from the
/// locator, which is the one place that can still act on it.
///
/// The wait is bounded by [`PROBE_TIMEOUT`], and a candidate still running when
/// the bound expires is killed and reaped before the probe answers `false`: the
/// answer for a program that never exits is the same one this probe gives a
/// program that exits wrongly, and no candidate outlives its own probe. The same
/// bound covers reading the candidate's stderr, because a candidate can exit and
/// still leave a descendant holding that pipe open. The candidate's stdin is
/// closed, so one that reads it gets end of input instead of waiting for a caller
/// that is not there.
fn behaves_like_the_stub(path: &Path) -> bool {
    let Ok(mut candidate) = std::process::Command::new(path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
    else {
        return false;
    };
    let deadline = Instant::now() + PROBE_TIMEOUT;
    let status = loop {
        match candidate.try_wait() {
            Ok(Some(status)) => break Some(status),
            Ok(None) if Instant::now() < deadline => std::thread::sleep(PROBE_POLL_INTERVAL),
            // A candidate that outlived the bound and one whose wait failed are the
            // same answer here: nothing about it says it is the stub.
            _ => break None,
        }
    };
    let Some(status) = status else {
        // The kill is what keeps the bound a bound, and the wait reaps the child
        // either way, so a rejected candidate leaves no process behind and no
        // zombie for this test process to carry.
        let _ = candidate.kill();
        let _ = candidate.wait();
        return false;
    };
    // The candidate has exited, so the bytes it wrote itself are already in the
    // pipe; what this read can still wait on is anything the candidate left
    // holding the write end. A descendant that inherited stderr keeps the pipe
    // open after its parent exits, and an unbounded read then waits on that
    // descendant rather than on the candidate the bound was meant to cover. So the
    // read is bounded by [`PROBE_TIMEOUT`] too, and a read that reaches the bound
    // is a candidate that left a writer behind — the stub prints one line and
    // spawns nothing, so a lingering writer is not the stub and the probe answers
    // `false`. The cost of answering that way is the reader thread: it stays
    // parked on the pipe its descendant still holds. It is detached and owns
    // nothing else, and it lives no longer than this process, which ends with the
    // test run, so a misbehaving candidate costs one parked thread instead of the
    // locator's whole search.
    let mut stderr = Vec::new();
    if let Some(mut pipe) = candidate.stderr.take() {
        let (sender, receiver) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let mut bytes = Vec::new();
            let _ = pipe.read_to_end(&mut bytes);
            let _ = sender.send(bytes);
        });
        match receiver.recv_timeout(PROBE_TIMEOUT) {
            Ok(bytes) => stderr = bytes,
            Err(_) => return false,
        }
    }
    status.code() == Some(2) && String::from_utf8_lossy(&stderr).contains("--mode is required")
}

/// Resolves the path to the `mediapm-stub-tool` binary.
///
/// Two kinds of stub can be on disk, written by two builders, and they are
/// trusted for different reasons.
///
/// Cargo writes a binary target to two shapes: `cargo build --bins` writes
/// `target/<profile>/mediapm-stub-tool`, and a test run writes a hash-suffixed
/// copy into `target/<profile>/deps/` and leaves the unhashed path alone. Those
/// are accepted on **Cargo's** provenance — Cargo's own fingerprint decides
/// whether they are current — and they must be a file that passes
/// `behaves_like_the_stub`, because `--all-targets` also leaves a libtest
/// harness under a name of that shape.
///
/// The fallback `compile_stub_into` writes is accepted on **its record**
/// instead: it sits in the running executable's own directory, which is `deps/`
/// for a test binary and so holds no plain-named target of Cargo's, and
/// `compiled_from` says whether the embedded source is still the one that built
/// it. A fallback whose record does not match is skipped, and the search ends in
/// `compile_stub_into` replacing it. That directory is Cargo's profile directory
/// when the running binary is not a test binary; the fallback replaces the
/// unhashed stub there, which is the same executable built from the same source,
/// and the replacement is what puts the file under a recorded fingerprint.
///
/// # Panics
///
/// Panics when the fallback cannot be produced: the running executable's
/// directory is missing, or `compile_stub_into` fails to write, compile, move
/// the executable into place, or record the source it used. Both mean no stub can
/// be run, which is the one outcome that must not reach a caller as a silently
/// absent tool.
#[must_use]
pub fn stub_tool_path() -> PathBuf {
    let suffix = std::env::consts::EXE_SUFFIX;
    let file_name = format!("{STUB_TOOL_BIN_NAME}{suffix}");
    let current = std::env::current_exe().unwrap_or_else(|error| {
        panic!("the running executable's path must be readable to locate {file_name}: {error}")
    });
    let stub_directory = current.parent().filter(|parent| parent.is_dir());

    // The fallback slot, checked first because it is this module's own file: the
    // artifact is the stub only when the record beside it names the source this
    // module embeds, so an edited stub source cannot be answered by the previous
    // build.
    if let Some(directory) = stub_directory {
        let fallback = directory.join(&file_name);
        if fallback.is_file()
            && compiled_from(&fallback, STUB_TOOL_SOURCE)
            && behaves_like_the_stub(&fallback)
        {
            return fallback;
        }
    }

    // Cargo's shapes, accepted on the fingerprint Cargo keeps for them.
    let mut candidates: Vec<PathBuf> = Vec::new();
    let mut hashed_directories: Vec<PathBuf> = Vec::new();
    if let Some(directory) = current.parent() {
        hashed_directories.push(directory.to_path_buf());
        if let Some(target_directory) = directory.parent() {
            candidates.push(target_directory.join(&file_name));
            hashed_directories.push(target_directory.join("deps"));
        }
    }
    candidates.extend(hashed_candidates(&hashed_directories, &file_name));

    if let Some(found) =
        candidates.iter().find(|candidate| candidate.is_file() && behaves_like_the_stub(candidate))
    {
        return found.clone();
    }
    // A consumer's `cargo test-pkg` builds no binary target of this crate, so the
    // search legitimately finds nothing — or found only a fallback its record
    // called stale. Compiling beside the running test binary leaves the stub where
    // the next search looks, so the run pays this once.
    let directory = stub_directory.unwrap_or_else(|| {
        panic!("the running executable's directory must exist: {}", current.display())
    });
    compile_stub_into(directory)
}

/// Lists the hash-suffixed copies of the stub in `directories`.
///
/// Cargo's hashed file name replaces the binary name's hyphens with underscores
/// and appends a build hash, so the prefix is derived rather than spelled out.
/// Only the executable qualifies: [`is_hashed_executable_name`] rejects the
/// sidecars Cargo writes beside it. Several copies can coexist after a change to
/// this crate's manifest; the newest is returned first, because that is the one
/// the current build wrote.
fn hashed_candidates(directories: &[PathBuf], file_name: &str) -> Vec<PathBuf> {
    let stem = file_name.strip_suffix(std::env::consts::EXE_SUFFIX).unwrap_or(file_name);
    let prefix = format!("{}-", stem.replace('-', "_"));
    let mut found: Vec<(std::time::SystemTime, PathBuf)> = Vec::new();
    for directory in directories {
        let Ok(entries) = std::fs::read_dir(directory) else {
            continue;
        };
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if !is_hashed_executable_name(name, &prefix) {
                continue;
            }
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let modified = entry
                .metadata()
                .and_then(|metadata| metadata.modified())
                .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
            found.push((modified, path));
        }
    }
    found.sort_by_key(|entry| std::cmp::Reverse(entry.0));
    found.into_iter().map(|(_, path)| path).collect()
}

/// Reports whether `name` is Cargo's hashed executable for a target.
///
/// Cargo's hashed file name is the stem, a build hash, and the platform suffix,
/// and nothing further. So the remainder after `prefix` must be a hash with no
/// further extension. Testing the suffix alone cannot answer this: `EXE_SUFFIX`
/// is the empty string on Unix, every string ends with the empty string, and the
/// `.d` and `.rmeta` sidecars Cargo writes for every target would pass as the
/// executable. Returning one of those is worse than finding nothing, because the
/// caller receives a path that exists and cannot be executed in place of one the
/// locator has seen run, so the failure moves from the locator to wherever the
/// stub is spawned.
///
/// A rejected name is not an error: `deps/` holds sidecars beside every target,
/// and skipping them is the purpose here. The emptiness check covers a prefix with
/// nothing after it; the dot rule covers a sidecar such as
/// `mediapm_stub_tool-.d`.
fn is_hashed_executable_name(name: &str, prefix: &str) -> bool {
    let Some(remainder) = name.strip_prefix(prefix) else {
        return false;
    };
    let Some(hash) = remainder.strip_suffix(std::env::consts::EXE_SUFFIX) else {
        return false;
    };
    !hash.is_empty() && !hash.contains('.')
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::process::Command;
    use std::time::{Duration, Instant};

    use crate::media_fixture::{
        MEDIA_FIXTURE_HEIGHT, MEDIA_FIXTURE_PROBE_DURATION, MEDIA_FIXTURE_WIDTH,
    };

    /// Runs the stub with `args` and returns its exit code, stdout and stderr.
    fn run_stub(args: &[&str]) -> (i32, String, String) {
        let output = Command::new(stub_tool_path())
            .args(args)
            .output()
            .expect("the located stub must be executable");
        (
            output.status.code().expect("the stub must exit with a code, not a signal"),
            String::from_utf8_lossy(&output.stdout).into_owned(),
            String::from_utf8_lossy(&output.stderr).into_owned(),
        )
    }

    /// The locator returns a real binary whose name marks it as the stub.
    ///
    /// The user-level guarantee: a hermetic test that needs a tool process
    /// resolves it without knowing which of Cargo's two output shapes this
    /// build produced. A locator that silently returned a non-existent path
    /// would make every caller's execution assertion vacuous.
    #[test]
    fn the_stub_is_locatable_from_a_running_test_binary() {
        let path = stub_tool_path();
        let name = path.file_name().and_then(|name| name.to_str()).unwrap_or_default();
        assert!(path.is_file(), "the stub must exist at {}", path.display());
        let expected_prefix = STUB_TOOL_BIN_NAME.replace('-', "_");
        assert!(
            name.starts_with(&expected_prefix) || name.starts_with(STUB_TOOL_BIN_NAME),
            "the located path must be the stub, found {name}",
        );
    }

    /// An executable that is not the stub is rejected by the same probe that accepts it.
    ///
    /// The user-level guarantee: `--all-targets` compiles the bin target again as a
    /// libtest harness and Cargo names that copy the way it names the executable, so
    /// acceptance by file name picks a harness. A harness exits 0 to a bare
    /// invocation; the stub exits 2 with its usage message. Without this rejection
    /// every stub-based test in the workspace tests a harness.
    #[cfg(unix)]
    #[test]
    fn a_candidate_that_is_not_the_stub_is_rejected() {
        use std::os::unix::fs::PermissionsExt;

        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let impostor = dir.path().join(format!("{STUB_TOOL_BIN_NAME}-impostor"));
        std::fs::write(&impostor, "#!/bin/sh\nexit 0\n").expect("the impostor must be writable");
        std::fs::set_permissions(&impostor, std::fs::Permissions::from_mode(0o755))
            .expect("the impostor must be executable");

        assert!(
            !behaves_like_the_stub(&impostor),
            "a process that exits 0 to a bare invocation is a test harness, not the stub",
        );
        assert!(
            behaves_like_the_stub(&stub_tool_path()),
            "the located stub must pass the same probe that rejects the impostor",
        );
    }

    /// A candidate that never answers is rejected within the probe's bound.
    ///
    /// The user-level guarantee: the probe runs every candidate it is offered, so
    /// one that never exits — a program reading stdin that never arrives, or an
    /// impostor that sleeps — would hold the locator, and with it every stub-based
    /// test's search, for as long as it lives. The bound turns that into the verdict
    /// the probe already gives a program that answers wrongly, and a wait that is
    /// polled rather than threaded leaves nothing behind to leak.
    ///
    /// The fixture `exec`s the sleep, so the process the probe kills is the one that
    /// sleeps rather than a shell whose child keeps running. Both timing assertions
    /// matter: less than twice the bound fails a probe that waits for the process,
    /// and at least the bound fails a probe that answered without ever running the
    /// fixture — a broken fixture spawn and a rejection by file name look the same
    /// from the outside otherwise.
    #[cfg(unix)]
    #[test]
    fn a_candidate_that_outlives_the_probe_bound_is_rejected() {
        use std::os::unix::fs::PermissionsExt;

        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let sleeper = dir.path().join(format!("{STUB_TOOL_BIN_NAME}-sleeper"));
        let seconds = PROBE_TIMEOUT.as_secs().saturating_mul(4);
        std::fs::write(&sleeper, format!("#!/bin/sh\nexec sleep {seconds}\n"))
            .expect("the sleeper must be writable");
        std::fs::set_permissions(&sleeper, std::fs::Permissions::from_mode(0o755))
            .expect("the sleeper must be executable");

        let started = Instant::now();
        let accepted = behaves_like_the_stub(&sleeper);
        let elapsed = started.elapsed();

        assert!(
            !accepted,
            "a candidate that never answers is not the stub, and answering `false` is what keeps \
             the search going",
        );
        assert!(
            elapsed >= PROBE_TIMEOUT,
            "the probe answers only after its bound, so the fixture it killed was still running: \
             {elapsed:?}",
        );
        assert!(
            elapsed < PROBE_TIMEOUT * 2,
            "the probe must answer within about twice its bound, took {elapsed:?}",
        );
    }

    /// A candidate that leaves a child holding its stderr keeps the pipe open after
    /// it exits, so an unbounded read waits on the child rather than on the
    /// candidate. The bound has to cover the read, not only the wait.
    #[cfg(unix)]
    #[test]
    fn a_candidate_whose_child_holds_stderr_is_rejected_within_the_bound() {
        use std::os::unix::fs::PermissionsExt;

        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let path = dir.path().join("mediapm-stub-tool");
        std::fs::write(&path, "#!/bin/sh\n( sleep 20 ) &\necho '--mode is required' >&2\nexit 2\n")
            .expect("write impostor");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
            .expect("make executable");

        let started = Instant::now();
        assert!(!behaves_like_the_stub(&path), "a lingering writer must not pass");
        let elapsed = started.elapsed();
        assert!(elapsed >= PROBE_TIMEOUT, "the wait must have run: {elapsed:?}");
        assert!(elapsed < PROBE_TIMEOUT * 2, "the read must be bounded: {elapsed:?}");
    }

    /// A candidate the platform cannot spawn is rejected without a permission bit.
    ///
    /// The user-level guarantee: the harness-rejection rule has two halves, and
    /// only the "answers like a harness" half has a portable impostor —
    /// `a_candidate_that_is_not_the_stub_is_rejected` needs an executable bit, so
    /// it is `unix`-only. The "cannot be spawned" half needs no permission bit: a
    /// text file is not a program on either platform, so the launch fails — on the
    /// missing execute bit, or on the bytes the loader will not start. The name
    /// carries the platform's executable suffix either way, so what the probe
    /// tries to launch is the file that was written, and the test asserts the
    /// refusal itself before it asserts the rejection: a platform that did launch
    /// the file would fail on the fixture rather than pass silently.
    ///
    /// The mutation this pins: returning `true` from the launch-failure arm of
    /// `behaves_like_the_stub` accepts a candidate that never ran, and the locator
    /// then hands a caller a path that cannot execute.
    #[test]
    fn a_candidate_that_cannot_be_spawned_is_rejected() {
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let unexecutable = dir
            .path()
            .join(format!("{STUB_TOOL_BIN_NAME}-unexecutable{}", std::env::consts::EXE_SUFFIX));
        std::fs::write(&unexecutable, "not an executable image\n")
            .expect("the impostor must be writable");

        assert!(
            Command::new(&unexecutable).output().is_err(),
            "the fixture must be a file the platform will not launch: {}",
            unexecutable.display(),
        );
        assert!(
            !behaves_like_the_stub(&unexecutable),
            "a file that cannot be spawned must not be accepted as the stub",
        );
    }

    /// A directory with no Cargo-built stub receives one that runs.
    ///
    /// The user-level guarantee: `cargo test-pkg <consumer>` builds no binary target
    /// from this crate, so a test that needs the stub must still get an executable.
    /// Compiling from the embedded source is what makes that run pass instead of
    /// panicking.
    #[test]
    fn a_stub_compiles_into_a_directory_that_has_none() {
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let path = compile_stub_into(dir.path());

        assert!(path.is_file(), "the compile must produce the executable at {}", path.display());
        let output = Command::new(&path)
            .args(["--mode", "copy"])
            .output()
            .expect("the compiled stub must be executable");
        assert_eq!(
            output.status.code(),
            Some(2),
            "the compiled stub must behave like the built one"
        );
    }

    /// A record that disagrees with the embedded source drives a rebuild, not trust.
    ///
    /// The user-level guarantee: the record is the only thing that notices an edit
    /// to `src/bin/mediapm-stub-tool.rs` for a fallback Cargo never built, so the
    /// locator has to consult it instead of accepting an artifact because it exists
    /// and runs. A locator that stopped comparing it would answer with the previous
    /// build while every stub-based test reported the current source.
    ///
    /// The locator may also move on to Cargo's own copy of the binary target, which
    /// carries no record of ours and needs none, so the assertion is the property
    /// that holds either way: the answer is never the artifact whose record
    /// disagrees with the embedded source.
    ///
    /// Nothing is asserted between writing the record and running the search, and
    /// that gap is deliberate: the record belongs to the one fallback the whole
    /// process shares, so a test running concurrently in this process can rebuild
    /// and rewrite it inside the window. An assertion there would fail a correct
    /// locator for another test's work, which is the flake this test must not
    /// reintroduce. What has to hold is the property after the search, and that is
    /// what is asserted.
    #[test]
    fn a_stale_record_makes_the_locator_rebuild_the_stub() {
        let stale = stub_tool_path();
        let record = record_path(&stale);
        std::fs::write(&record, "0000000000000000").expect("the record must be writable");

        let answer = stub_tool_path();

        assert!(
            answer != stale || compiled_from(&stale, STUB_TOOL_SOURCE),
            "the locator answered with {} and left the record reading {}, so it trusted an \
             artifact its record calls stale instead of rebuilding it",
            answer.display(),
            std::fs::read_to_string(&record).unwrap_or_default().trim(),
        );
    }

    /// A change to the stub's source is noticed by the compiled fallback.
    ///
    /// The user-level guarantee: nothing else rebuilds the fallback — Cargo does not
    /// know the file exists, so its fingerprint never covers it — and a fallback
    /// trusted by its name alone would keep testing the stub as it read when it was
    /// first compiled, across an edit, a checkout that reverted the file, or a
    /// `git bisect` step. The record the compile writes is what answers instead, and
    /// it answers against a source string rather than against the clock, so this
    /// asserts the verdict either way without touching the executable.
    #[test]
    fn a_changed_source_makes_the_compiled_stub_stale() {
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let path = compile_stub_into(dir.path());

        assert!(
            behaves_like_the_stub(&path),
            "the compile must produce a stub that runs, found {}",
            path.display(),
        );
        assert!(
            compiled_from(&path, STUB_TOOL_SOURCE),
            "the compiled stub must record the source it was built from",
        );
        assert!(
            !compiled_from(&path, "// the stub's source, one edit later\n"),
            "a different source must leave the compiled stub stale, so the search rebuilds it",
        );
    }

    /// Returns a hashed-artifact file name for the stub, platform suffix included.
    ///
    /// The hash is a placeholder. The locator matches the prefix and the shape of
    /// the name, never a particular hash, so a spelled-out value places a file
    /// exactly where the search looks.
    ///
    /// The prefix is derived from [`STUB_TOOL_BIN_NAME`] the way the locator
    /// derives it, because a spelled-out `mediapm_stub_tool` would keep passing
    /// against a prefix nothing builds after the `[[bin]]` target is renamed.
    fn hashed_name(hash: &str) -> String {
        fixture_name(&format!("{hash}{}", std::env::consts::EXE_SUFFIX))
    }

    /// Returns a sidecar file name Cargo writes beside the hashed executable.
    ///
    /// A sidecar shares the executable's derived prefix and differs only in what
    /// follows the build hash, so it takes the same derivation rather than a
    /// second spelling of the target's name.
    fn fixture_name(remainder: &str) -> String {
        format!("{}-{remainder}", STUB_TOOL_BIN_NAME.replace('-', "_"))
    }

    /// Sets a fixture's modification time, which the locator ranks by.
    ///
    /// The assertion needs times it chose rather than times the filesystem
    /// happened to assign: two writes in one test can land on the same clock
    /// tick, and an ordering assertion would then hold for the wrong reason. The
    /// file is opened for writing because setting a time is a mutation.
    fn set_modified(path: &std::path::Path, modified: std::time::SystemTime) {
        let file = std::fs::File::options()
            .write(true)
            .open(path)
            .expect("open the fixture for a modification-time update");
        file.set_modified(modified).expect("set the fixture's modification time");
    }

    /// The locator returns the executable alone, skipping Cargo's sidecars.
    ///
    /// `deps/` holds several files per hashed target under the executable's
    /// prefix: a `.d` dep-info file, a `.rmeta` for a library or binary target,
    /// the per-codegen-unit `.rcgu.o` objects that dominate a real
    /// `target/debug/deps/`, and a `.pdb` on Windows. Testing `EXE_SUFFIX` alone
    /// kept every one of them, because that suffix is the empty string on Unix and
    /// every string ends with the empty string: a run whose stub had not been
    /// built received a sidecar instead of the panic naming `cargo build-pkg
    /// mediapm-test-support`. That is how a mutation in an earlier effort
    /// appeared to reach nothing.
    ///
    /// The ordering assertion belongs beside the filtering one because both
    /// concern the returned list: two copies coexist after a manifest change, and
    /// the newer is the one the current build wrote.
    #[test]
    fn hashed_candidates_return_the_executable_and_not_cargo_sidecars() {
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let older = dir.path().join(hashed_name("0f1e2d3c4b5a6978"));
        let newer = dir.path().join(hashed_name("8877665544332211"));
        std::fs::write(&older, b"#!/bin/sh\nexit 0\n").expect("write the older executable");
        std::fs::write(&newer, b"#!/bin/sh\nexit 0\n").expect("write the newer executable");
        let now = std::time::SystemTime::now();
        set_modified(&older, now - std::time::Duration::from_hours(1));
        set_modified(&newer, now);

        for sidecar in [
            fixture_name("1111222233334444.d"),
            fixture_name("5555666677778888.rmeta"),
            fixture_name("9999aaaabbbbcccc.pdb"),
            fixture_name("aaaabbbbccccdddd.7f3a1b2.rcgu.o"),
        ] {
            std::fs::write(dir.path().join(&sidecar), b"sidecar").expect("write the sidecar");
        }
        // A directory can carry the executable's whole name shape, so only
        // `is_file` keeps it out. Handing one to a caller as a binary fails at
        // the spawn, far from the build step that would have named the cause.
        std::fs::create_dir(dir.path().join(hashed_name("ddddeeeeffff0000")))
            .expect("create a same-name directory");

        let stub_file_name = format!("{STUB_TOOL_BIN_NAME}{}", std::env::consts::EXE_SUFFIX);
        let candidates = hashed_candidates(&[dir.path().to_path_buf()], &stub_file_name);

        assert_eq!(candidates, vec![newer, older], "only the executables qualify, newest first");
    }

    /// `copy` writes the first input's bytes to the last argument.
    ///
    /// This is the property that lets the stub stand in for `ffmpeg`: the
    /// preset emits each output path as the command's trailing arguments, so a
    /// step with a single output slot ends with the path its capture expects and
    /// a run of the stub has produced a file there. Two inputs are passed
    /// deliberately, because `ffmpeg` takes the source first and the metadata
    /// document second: a stub that read the last `-i` instead would copy the
    /// wrong file while still looking like a working step.
    #[test]
    fn copy_mode_writes_the_first_input_to_the_last_argument() {
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let input = dir.path().join("input.bin");
        let second = dir.path().join("second.bin");
        let output = dir.path().join("output-0.mkv");
        std::fs::write(&input, b"payload-bytes").expect("write input");
        std::fs::write(&second, b"other-bytes").expect("write the second input");

        let (code, stdout, stderr) = run_stub(&[
            "--mode",
            "copy",
            "-i",
            input.to_str().expect("utf-8 input path"),
            "-i",
            second.to_str().expect("utf-8 second path"),
            "-c",
            "copy",
            output.to_str().expect("utf-8 output path"),
        ]);

        assert_eq!(code, 0, "a well-formed copy must succeed: {stderr}");
        assert!(stdout.is_empty(), "the stub must not write to stdout: {stdout}");
        assert_eq!(
            std::fs::read(&output).expect("read output"),
            b"payload-bytes",
            "the first `-i` is the input, not the last one",
        );
    }

    /// A copy whose command line carries no `-i` fails loudly.
    ///
    /// A stub that defaulted to an empty input would report success for a step
    /// whose command line never handed it anything, hiding the broken binding
    /// the test exists to catch.
    #[test]
    fn copy_mode_without_an_input_flag_is_a_usage_failure() {
        let (code, _, stderr) = run_stub(&["--mode", "copy", "output-0.mkv"]);
        assert_eq!(code, 2, "a missing input is a usage failure: {stderr}");
        assert!(
            stderr.contains("no `-i <path>` input"),
            "the failure must name the cause: {stderr}"
        );
    }

    /// A copy whose output names its input fails instead of doing nothing.
    ///
    /// Writing a file onto itself is a silent no-op that would leave a step
    /// looking successful while it produced nothing.
    #[test]
    fn copy_mode_with_the_input_as_the_output_is_a_usage_failure() {
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let input = dir.path().join("input.bin");
        std::fs::write(&input, b"payload-bytes").expect("write input");

        let (code, _, stderr) =
            run_stub(&["--mode", "copy", "-i", input.to_str().expect("utf-8 input path")]);

        assert_eq!(code, 2, "an output naming the input is a usage failure: {stderr}");
        assert!(stderr.contains("names the input"), "the failure must name the cause: {stderr}");
        assert_eq!(
            std::fs::read(&input).expect("read input"),
            b"payload-bytes",
            "the refused invocation must not have touched the input",
        );
    }

    /// An unreadable input is reported as an I/O failure, not a usage one.
    ///
    /// The distinction matters to a caller reading the code: an unusable command
    /// line means the wiring is wrong, a missing file means the staging is.
    #[test]
    fn copy_mode_with_a_missing_input_file_fails_with_the_io_exit_code() {
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let missing = dir.path().join("absent.bin");
        let output = dir.path().join("output-0.mkv");

        let (code, _, stderr) = run_stub(&[
            "--mode",
            "copy",
            "-i",
            missing.to_str().expect("utf-8 input path"),
            output.to_str().expect("utf-8 output path"),
        ]);

        assert_eq!(code, 3, "an unreadable input is an I/O failure: {stderr}");
        assert!(stderr.contains("cannot read input"), "the failure must name the cause: {stderr}");
        assert!(!output.exists(), "a failed copy must not leave an output behind");
    }

    /// An unknown mode is rejected rather than ignored, and the usage line names every mode.
    ///
    /// A stub that fell through to one of its modes would let a typo in a tool
    /// spec pass as a working step, and a usage line that still named only the
    /// modes of the day would send a caller looking for a mode that is there.
    #[test]
    fn an_unknown_mode_is_a_usage_failure() {
        let (code, _, stderr) = run_stub(&["--mode", "transcode"]);
        assert_eq!(code, 2, "an unknown mode is a usage failure: {stderr}");
        assert!(stderr.contains("unknown mode 'transcode'"), "the failure must name it: {stderr}");
        assert!(
            stderr.contains("copy")
                && stderr.contains("emit")
                && stderr.contains("probe")
                && stderr.contains("sleep"),
            "the usage line must name every mode a caller can ask for: {stderr}",
        );
    }

    /// A missing `--mode` is rejected.
    #[test]
    fn a_missing_mode_is_a_usage_failure() {
        let (code, _, stderr) = run_stub(&[]);
        assert_eq!(code, 2, "a missing mode is a usage failure: {stderr}");
        assert!(stderr.contains("--mode is required"), "the failure must name it: {stderr}");
    }

    /// Sleep mode without `--seconds` is rejected.
    #[test]
    fn sleep_mode_without_seconds_is_a_usage_failure() {
        let (code, _, stderr) = run_stub(&["--mode", "sleep"]);
        assert_eq!(code, 2, "a missing duration is a usage failure: {stderr}");
        assert!(stderr.contains("--seconds is required"), "the failure must name it: {stderr}");
    }

    /// A non-numeric `--seconds` is rejected rather than read as zero.
    ///
    /// Reading it as zero would turn a step meant to outlast a budget into an
    /// instant success, which is the opposite of what the caller asked for.
    #[test]
    fn sleep_mode_with_a_non_numeric_seconds_is_a_usage_failure() {
        let (code, _, stderr) = run_stub(&["--mode", "sleep", "--seconds", "3s"]);
        assert_eq!(code, 2, "a malformed duration is a usage failure: {stderr}");
        assert!(stderr.contains("whole number of seconds"), "the failure must name it: {stderr}");
    }

    /// Sleep mode really waits, which is the whole point of the mode.
    ///
    /// The conductor's RPC-layering tests install budgets of about a second and
    /// need a step that outlasts them, so a stub that returned immediately would
    /// make those tests pass for the wrong reason.
    #[test]
    fn sleep_mode_waits_for_the_requested_seconds() {
        let started = Instant::now();
        let (code, _, stderr) = run_stub(&["--mode", "sleep", "--seconds", "1"]);
        let elapsed = started.elapsed();

        assert_eq!(code, 0, "a well-formed sleep must succeed: {stderr}");
        assert!(
            elapsed >= Duration::from_millis(900),
            "one second of sleep must take about a second, took {elapsed:?}",
        );
    }

    /// `emit` writes every entry the manifest names, creating a missing parent directory.
    ///
    /// This is the property that lets the stub stand in for `yt-dlp`: a download
    /// writes a set of sidecars rather than one file, and the caller names every
    /// destination in the manifest, so the stub needs no knowledge of which tool
    /// asked for them. The destinations sit under a directory that does not exist
    /// yet, because that is the shape a fresh hierarchy has.
    #[test]
    fn emit_mode_writes_every_entry_and_creates_missing_parent_directories() {
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let first_source = dir.path().join("first.bin");
        let second_source = dir.path().join("second.bin");
        std::fs::write(&first_source, b"first-bytes").expect("write the first source");
        std::fs::write(&second_source, b"second-bytes").expect("write the second source");
        let first_output = dir.path().join("nested").join("deeper").join("first.info.json");
        let second_output = dir.path().join("nested").join("second.vtt");
        let manifest = dir.path().join("emit.manifest");
        std::fs::write(
            &manifest,
            format!(
                "{}\t{}\n{}\t{}\n",
                first_output.display(),
                first_source.display(),
                second_output.display(),
                second_source.display(),
            ),
        )
        .expect("write the manifest");

        let (code, stdout, stderr) = run_stub(&[
            "--mode",
            "emit",
            "--manifest",
            manifest.to_str().expect("utf-8 manifest path"),
        ]);

        assert_eq!(code, 0, "a well-formed emit must succeed: {stderr}");
        assert!(stdout.is_empty(), "the stub must not write to stdout: {stdout}");
        assert_eq!(
            std::fs::read(&first_output).expect("read the first output"),
            b"first-bytes",
            "the first entry's destination must carry its source's bytes",
        );
        assert_eq!(
            std::fs::read(&second_output).expect("read the second output"),
            b"second-bytes",
            "every entry must be written, not only the first",
        );
    }

    /// A source the manifest names but does not exist fails before anything is written.
    ///
    /// The caller's hierarchy consumes the whole set, so a stub that wrote the
    /// entries it could would leave a partial set behind and report success — the
    /// step would look complete while the links pointed at files that were never
    /// produced. Reading every source before the first destination is what makes
    /// the failure total.
    #[test]
    fn emit_mode_with_a_missing_source_writes_no_destination() {
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let present_source = dir.path().join("present.bin");
        std::fs::write(&present_source, b"present-bytes").expect("write the present source");
        let missing_source = dir.path().join("absent.bin");
        let present_output = dir.path().join("present.info.json");
        let missing_output = dir.path().join("absent.info.json");
        let manifest = dir.path().join("partial.manifest");
        std::fs::write(
            &manifest,
            format!(
                "{}\t{}\n{}\t{}\n",
                present_output.display(),
                present_source.display(),
                missing_output.display(),
                missing_source.display(),
            ),
        )
        .expect("write the manifest");

        let (code, _, stderr) = run_stub(&[
            "--mode",
            "emit",
            "--manifest",
            manifest.to_str().expect("utf-8 manifest path"),
        ]);

        assert_eq!(code, 3, "an unreadable source is an I/O failure: {stderr}");
        assert!(
            stderr.contains("cannot read manifest source"),
            "the failure must name the cause: {stderr}",
        );
        assert!(
            stderr.contains("absent.bin"),
            "the failure must name the source it could not read: {stderr}",
        );
        assert!(
            !present_output.exists(),
            "a failed emit must not write the entries it could have written",
        );
        assert!(!missing_output.exists(), "a failed emit must write no destination");
    }

    /// A destination that cannot be written rolls back the entries already written.
    ///
    /// Reading every source up front covers the source half of the mode's no-partial-set
    /// contract; the write half is this: a write that fails halfway leaves the entries
    /// before it on disk. The caller's hierarchy consumes the whole set, so those entries
    /// would let a step look complete while the set it was promised is not. The second
    /// destination's parent is an existing file, which is the shape no write can pass.
    #[test]
    fn emit_mode_rolls_back_the_destinations_it_wrote_when_a_later_entry_fails() {
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let first_source = dir.path().join("first.bin");
        let second_source = dir.path().join("second.bin");
        std::fs::write(&first_source, b"first-bytes").expect("write the first source");
        std::fs::write(&second_source, b"second-bytes").expect("write the second source");
        let first_output = dir.path().join("first.info.json");
        let blocked_parent = dir.path().join("blocked");
        std::fs::write(&blocked_parent, b"a file, not a directory")
            .expect("write the blocked parent");
        let second_output = blocked_parent.join("second.info.json");
        let manifest = dir.path().join("rollback.manifest");
        std::fs::write(
            &manifest,
            format!(
                "{}\t{}\n{}\t{}\n",
                first_output.display(),
                first_source.display(),
                second_output.display(),
                second_source.display(),
            ),
        )
        .expect("write the manifest");

        let (code, _, stderr) = run_stub(&[
            "--mode",
            "emit",
            "--manifest",
            manifest.to_str().expect("utf-8 manifest path"),
        ]);

        assert_eq!(code, 3, "an unwritable destination is an I/O failure: {stderr}");
        assert!(
            stderr.contains("cannot create the directory"),
            "the failure must name the cause: {stderr}",
        );
        assert!(
            !first_output.exists(),
            "a failed emit must roll back the destination it already wrote",
        );
        assert!(!second_output.exists(), "a failed emit must write no destination");
    }

    /// A manifest line that names no destination and source pair is a usage failure.
    ///
    /// A stub that skipped a line it could not read would emit a set that is
    /// silently smaller than the one the caller asked for.
    #[test]
    fn emit_mode_with_a_malformed_manifest_is_a_usage_failure() {
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let manifest = dir.path().join("malformed.manifest");

        for line in ["just-a-destination\n", "destination\t\n"] {
            std::fs::write(&manifest, line).expect("write the malformed manifest");
            let (code, _, stderr) = run_stub(&[
                "--mode",
                "emit",
                "--manifest",
                manifest.to_str().expect("utf-8 manifest path"),
            ]);

            assert_eq!(code, 2, "a malformed line is a usage failure: {stderr}");
            assert!(
                stderr.contains("manifest line 1 is malformed"),
                "the failure must name the line it could not read: {stderr}",
            );
        }
    }

    /// A manifest that names no entry is a usage failure, not a silent success.
    ///
    /// An emit that wrote nothing would let a step whose manifest was never
    /// populated pass as a successful one, which is the failure class this stub
    /// exists to make loud.
    #[test]
    fn emit_mode_with_an_empty_manifest_is_a_usage_failure() {
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let manifest = dir.path().join("empty.manifest");
        std::fs::write(&manifest, "").expect("write the empty manifest");

        let (code, _, stderr) = run_stub(&[
            "--mode",
            "emit",
            "--manifest",
            manifest.to_str().expect("utf-8 manifest path"),
        ]);

        assert_eq!(code, 2, "an empty manifest is a usage failure: {stderr}");
        assert!(stderr.contains("names no entry"), "the failure must name the cause: {stderr}");
    }

    /// Emit mode without `--manifest` is rejected.
    ///
    /// The flag is the mode's whole input, so a run that named no file set would
    /// otherwise succeed having written nothing.
    #[test]
    fn emit_mode_without_a_manifest_is_a_usage_failure() {
        let (code, _, stderr) = run_stub(&["--mode", "emit"]);
        assert_eq!(code, 2, "a missing manifest is a usage failure: {stderr}");
        assert!(stderr.contains("--manifest is required"), "the failure must name it: {stderr}");
    }

    /// Probe mode answers the `ffprobe`-shaped document on stdout.
    ///
    /// The stub stands in for `ffprobe`, and the media the offline run hands it is
    /// the committed fixture (`src/mediapm/tests/fixtures/media/demo.mkv`), so the
    /// document states that file's facts: a Matroska container, an H.264 video
    /// track, and an AAC audio track. `ffprobe` writes its JSON to stdout and exits
    /// 0, so a caller's read has to find it there rather than on stderr or in an
    /// exit code.
    #[test]
    fn probe_mode_prints_the_ffprobe_shaped_document_on_stdout() {
        let (code, stdout, stderr) = run_stub(&["--mode", "probe"]);

        assert_eq!(code, 0, "a probe must succeed: {stderr}");
        assert!(stderr.is_empty(), "the probe must answer on stdout, not stderr: {stderr}");
        let payload: serde_json::Value =
            serde_json::from_str(&stdout).expect("the probe output must be JSON");
        let format_name = payload
            .get("format")
            .and_then(|format| format.get("format_name"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        assert!(
            format_name.contains("matroska"),
            "the document must name the fixture's container, got '{format_name}': {stdout}",
        );
        // The probed duration is asserted against the fixture's pinned const
        // rather than against a literal, so a fixture regenerated to another
        // duration fails here instead of leaving the stub answering with a value
        // the committed file no longer reports.
        let duration = payload
            .get("format")
            .and_then(|format| format.get("duration"))
            .and_then(serde_json::Value::as_str);
        assert_eq!(
            duration,
            Some(MEDIA_FIXTURE_PROBE_DURATION),
            "the document must state the committed fixture's probed duration: {stdout}",
        );
        let streams = payload
            .get("streams")
            .and_then(serde_json::Value::as_array)
            .expect("the document must carry a streams array");
        // The dimensions are asserted against the fixture's pinned consts rather
        // than against literals, so a fixture regenerated at another resolution
        // fails here instead of leaving the stub answering with facts no file
        // carries any more.
        let video = streams
            .iter()
            .find(|stream| {
                stream.get("codec_type").and_then(serde_json::Value::as_str) == Some("video")
            })
            .expect("the document must carry a video stream");
        assert_eq!(
            video.get("codec_name").and_then(serde_json::Value::as_str),
            Some("h264"),
            "the fixture's video track is H.264: {stdout}",
        );
        assert_eq!(
            video.get("width").and_then(serde_json::Value::as_u64),
            Some(MEDIA_FIXTURE_WIDTH),
            "the document must state the committed fixture's width: {stdout}",
        );
        assert_eq!(
            video.get("height").and_then(serde_json::Value::as_u64),
            Some(MEDIA_FIXTURE_HEIGHT),
            "the document must state the committed fixture's height: {stdout}",
        );
        let audio = streams
            .iter()
            .find(|stream| {
                stream.get("codec_type").and_then(serde_json::Value::as_str) == Some("audio")
            })
            .expect("the document must carry an audio stream");
        assert_eq!(
            audio.get("codec_name").and_then(serde_json::Value::as_str),
            Some("aac"),
            "the fixture's audio track is AAC: {stdout}",
        );
    }

    /// The stub runs at all, which every other test here depends on.
    ///
    /// The locator's search covers the hash-suffixed copy Cargo writes for a
    /// test run as well as the unhashed one a `--bins` build writes, so this
    /// pins that the resolved path is executable rather than merely present.
    #[test]
    fn the_located_stub_executes() {
        let (code, _, stderr) = run_stub(&["--mode", "copy"]);
        assert_eq!(code, 2, "the stub ran and rejected the empty command line: {stderr}");
    }
}
