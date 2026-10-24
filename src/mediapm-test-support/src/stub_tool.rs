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
//! # Why absence is a panic
//!
//! A stub that cannot be found would turn a step-execution assertion into a
//! no-op that passes, which is the failure class these tests exist to remove.
//! [`stub_tool_path`] therefore panics, naming the command that builds it.

use std::path::PathBuf;

/// File name of the stub binary, without the platform executable suffix.
pub const STUB_TOOL_BIN_NAME: &str = "mediapm-stub-tool";

/// Resolves the path to the `mediapm-stub-tool` binary.
///
/// Cargo writes a binary target to two different shapes, so both are searched.
/// `cargo build --bins` writes `target/<profile>/mediapm-stub-tool`; a test run
/// writes a hash-suffixed copy into `target/<profile>/deps/` and leaves the
/// unhashed path alone, which is the shape a consumer's test binary sees. The
/// search therefore covers the running executable's own directory, the target
/// directory above it, and the `deps/` directories beside both — the hashed
/// copies are matched by name prefix because the hash is not predictable.
///
/// # Panics
///
/// Panics when the stub is absent from every candidate. The message names the
/// searched paths and `cargo build-pkg mediapm-test-support`, the cheapest
/// command that builds it, because `cargo test-pkg <consumer>` selects that
/// consumer's package alone and a dev-dependency's binary target is not among
/// the targets Cargo builds for it. The workspace gate (`cargo test-all`) passes
/// `--workspace --all-targets` and needs no such step.
#[must_use]
pub fn stub_tool_path() -> PathBuf {
    let suffix = std::env::consts::EXE_SUFFIX;
    let file_name = format!("{STUB_TOOL_BIN_NAME}{suffix}");
    let current = std::env::current_exe().unwrap_or_else(|error| {
        panic!("the running executable's path must be readable to locate {file_name}: {error}")
    });

    let mut candidates: Vec<PathBuf> = Vec::new();
    let mut hashed_directories: Vec<PathBuf> = Vec::new();
    if let Some(directory) = current.parent() {
        candidates.push(directory.join(&file_name));
        hashed_directories.push(directory.to_path_buf());
        if let Some(target_directory) = directory.parent() {
            candidates.push(target_directory.join(&file_name));
            hashed_directories.push(target_directory.join("deps"));
        }
    }
    candidates.extend(hashed_candidates(&hashed_directories, &file_name));

    if let Some(found) = candidates.iter().find(|candidate| candidate.is_file()) {
        return found.clone();
    }
    let searched = candidates.iter().map(|path| path.display().to_string()).collect::<Vec<_>>();
    panic!(
        "{file_name} was not found at {}; build it with `cargo build-pkg mediapm-test-support` (or \
         `cargo test-pkg mediapm-test-support`, which builds it too) — `cargo test-pkg <consumer>` \
         selects that package alone and does not build a dev-dependency's binary target",
        searched.join(" or ")
    )
}

/// Lists the hash-suffixed copies of the stub in `directories`.
///
/// Cargo's hashed file name replaces the binary name's hyphens with underscores
/// and appends a build hash, so the prefix is derived rather than spelled out.
/// Several copies can coexist after a change to this crate's manifest; the
/// newest is returned first, because that is the one the current build wrote.
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
            if !name.starts_with(&prefix) || !name.ends_with(std::env::consts::EXE_SUFFIX) {
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

#[cfg(test)]
mod tests {
    use super::*;

    use std::process::Command;
    use std::time::{Duration, Instant};

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
        assert!(
            name.starts_with("mediapm_stub_tool") || name.starts_with(STUB_TOOL_BIN_NAME),
            "the located path must be the stub, found {name}",
        );
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

    /// An unknown mode is rejected rather than ignored.
    ///
    /// A stub that fell through to one of its modes would let a typo in a tool
    /// spec pass as a working step.
    #[test]
    fn an_unknown_mode_is_a_usage_failure() {
        let (code, _, stderr) = run_stub(&["--mode", "transcode"]);
        assert_eq!(code, 2, "an unknown mode is a usage failure: {stderr}");
        assert!(stderr.contains("unknown mode 'transcode'"), "the failure must name it: {stderr}");
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
