//! A stand-in managed-tool executable for hermetic tests.
//!
//! Two modes cover the two ways a test needs a real process: `copy` stands in
//! for a tool that reads an input file and writes an output file, and `sleep`
//! stands in for a tool whose only interesting property is that it takes time.
//! Both replace something a test cannot have — a downloaded `ffmpeg`, or a
//! shell — and neither needs a shell, which is the point: a `sh -c` stand-in is
//! unavailable on a host without `sh` and reintroduces the platform dependency
//! it was meant to remove.
//!
//! # Modes
//!
//! `--mode copy` reads the file named after the **first** `-i` and writes its
//! bytes to the **last** argument. Every other argument is ignored, which is
//! what lets it stand in for `ffmpeg` when the command has a single output slot:
//! the preset emits one `${inputs.output_path_N}` per output slot, so the last
//! argument is slot 0's output path only when there is one of them. A command
//! with several output slots would hand the stub the highest-index path.
//!
//! `--mode sleep --seconds <n>` sleeps for `n` whole seconds.
//!
//! # Exit codes
//!
//! `0` on success, `2` when the command line is unusable (unknown mode, a
//! missing flag value, an output that names the input), and `3` when a file
//! operation fails. A stub that defaulted instead of failing would let a broken
//! command line pass as a successful step, so every unusable shape is loud.

use std::time::Duration;

/// Exit code for a command line the stub cannot act on.
const USAGE_EXIT: i32 = 2;
/// Exit code for a file operation that failed.
const IO_EXIT: i32 = 3;

/// A failure the stub reports on stderr before exiting non-zero.
struct StubError {
    /// Message printed on stderr, without the program-name prefix.
    message: String,
    /// Process exit code this failure maps to.
    exit_code: i32,
}

impl StubError {
    /// Builds a command-line failure, reported with [`USAGE_EXIT`].
    fn usage(message: impl Into<String>) -> Self {
        Self { message: message.into(), exit_code: USAGE_EXIT }
    }

    /// Builds a file-operation failure, reported with [`IO_EXIT`].
    fn io(message: impl Into<String>) -> Self {
        Self { message: message.into(), exit_code: IO_EXIT }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if let Err(error) = run(&args) {
        eprintln!("mediapm-stub-tool: {}", error.message);
        std::process::exit(error.exit_code);
    }
}

/// Dispatches one invocation on its `--mode` value.
///
/// # Errors
///
/// Returns a usage failure when `--mode` is absent or names no known mode, and
/// whatever the selected mode itself reports.
fn run(args: &[String]) -> Result<(), StubError> {
    let mode = value_after(args, "--mode")
        .ok_or_else(|| StubError::usage("--mode is required (expected `copy` or `sleep`)"))?;
    match mode.as_str() {
        "copy" => copy(args),
        "sleep" => sleep_for(args),
        unknown => {
            Err(StubError::usage(format!("unknown mode '{unknown}': expected `copy` or `sleep`")))
        }
    }
}

/// Copies the first `-i` input to the last argument.
///
/// # Errors
///
/// Returns a usage failure when no `-i` path is present or when the last
/// argument names the input itself (which would be a silent no-op), and an I/O
/// failure when the input cannot be read or the output cannot be written.
fn copy(args: &[String]) -> Result<(), StubError> {
    let input = first_input_path(args)?;
    let output =
        args.last().ok_or_else(|| StubError::usage("the output path must be the last argument"))?;
    if output == &input {
        return Err(StubError::usage(format!(
            "the output path '{output}' names the input; a copy needs two distinct paths"
        )));
    }
    let bytes = std::fs::read(&input)
        .map_err(|error| StubError::io(format!("cannot read input '{input}': {error}")))?;
    std::fs::write(output, &bytes)
        .map_err(|error| StubError::io(format!("cannot write output '{output}': {error}")))?;
    Ok(())
}

/// Sleeps for the whole number of seconds `--seconds` names.
///
/// # Errors
///
/// Returns a usage failure when `--seconds` is absent or is not a whole number
/// of seconds.
fn sleep_for(args: &[String]) -> Result<(), StubError> {
    let raw = value_after(args, "--seconds")
        .ok_or_else(|| StubError::usage("--seconds is required in sleep mode"))?;
    let seconds: u64 = raw.parse().map_err(|_| {
        StubError::usage(format!("--seconds must be a whole number of seconds, got '{raw}'"))
    })?;
    std::thread::sleep(Duration::from_secs(seconds));
    Ok(())
}

/// Returns the value of the first `--flag value` pair for `flag`.
///
/// A flag whose value is missing or is itself a flag yields `None`, so a
/// truncated command line is reported as the usage failure it is rather than
/// consuming the next flag as a value.
fn value_after(args: &[String], flag: &str) -> Option<String> {
    let index = args.iter().position(|arg| arg == flag)?;
    args.get(index + 1).filter(|value| !value.starts_with("--")).cloned()
}

/// Returns the path that follows the first `-i` argument.
///
/// # Errors
///
/// Returns a usage failure when no `-i` argument carries a following path.
fn first_input_path(args: &[String]) -> Result<String, StubError> {
    let index = args
        .iter()
        .position(|arg| arg == "-i")
        .ok_or_else(|| StubError::usage("no `-i <path>` input was given"))?;
    args.get(index + 1)
        .cloned()
        .ok_or_else(|| StubError::usage("the `-i` flag must be followed by an input path"))
}
