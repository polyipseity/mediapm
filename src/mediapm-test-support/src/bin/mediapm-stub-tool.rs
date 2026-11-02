//! A stand-in managed-tool executable for hermetic tests.
//!
//! Four modes cover the ways a test needs a real process: `copy` stands in for a
//! tool that reads an input file and writes an output file, `emit` for one that
//! writes a set of files a manifest names, `probe` for one that answers a
//! metadata query on stdout, and `sleep` for one whose only interesting property
//! is that it takes time. All of them replace something a test cannot have — a
//! downloaded `ffmpeg`, or a shell — and none needs a shell, which is the point:
//! a `sh -c` stand-in is unavailable on a host without `sh` and reintroduces the
//! platform dependency it was meant to remove.
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
//! `--mode emit --manifest <path>` writes the file set the manifest names. Each
//! line of the manifest is a destination and a source separated by a tab, and
//! every source is read before the first destination is written, so a source
//! that is missing fails the step without leaving a partial set behind. A
//! destination's missing parent directories are created. A destination that
//! cannot be written removes the destinations this invocation already wrote
//! before it reports the failure, so an emit that stops halfway leaves no
//! partial set behind either. Paths are used as given, so the caller decides
//! whether they are absolute or relative to the stub's working directory. The
//! manifest is the mode's whole input — it names the files and where they go —
//! so the stub needs no knowledge of which tool asked for them.
//!
//! `--mode probe` writes a fixed `ffprobe`-shaped JSON document to stdout. The
//! document states the committed media fixture's facts, because that is the
//! media the offline tests materialize: a caller that reads codecs or the
//! container through `ffprobe` gets that file's answer without a download.
//!
//! `--mode sleep --seconds <n>` sleeps for `n` whole seconds.
//!
//! # Exit codes
//!
//! `0` on success, `2` when the command line is unusable (unknown mode, a
//! missing flag value, an output that names the input, a manifest that names no
//! usable entry), and `3` when a file operation fails. A stub that defaulted
//! instead of failing would let a broken command line pass as a successful step,
//! so every unusable shape is loud.

use std::io::Write;
use std::time::Duration;

/// Exit code for a command line the stub cannot act on.
const USAGE_EXIT: i32 = 2;
/// Exit code for a file operation that failed.
const IO_EXIT: i32 = 3;

/// The `ffprobe`-shaped document `--mode probe` writes to stdout.
///
/// Every field is a fact of the committed media fixture the offline tests
/// materialize (`src/mediapm/tests/fixtures/media/demo.mkv`, whose README records
/// the command that regenerated it): a Matroska container, an H.264 video track
/// at 320x180, and a mono 44.1 kHz AAC audio track, running 10.007 s. The stub is
/// handed a mode rather than a path, so it cannot read the file it describes; the
/// document is a constant, and keeping it to the fixture's own facts is what
/// makes the answer it gives a caller true. The probe test asserts the facts
/// `src/mediapm-test-support/src/media_fixture.rs` pins against this document, so
/// a fixture regenerated at other dimensions or another probed duration fails the
/// test rather than leaving the stub answering with what the file no longer says.
/// A real `ffprobe` prints nothing for a field it does not know, so the fields the
/// fixture does not carry are absent rather than empty.
const PROBE_DOCUMENT: &str = r#"{
  "streams": [
    {
      "index": 0,
      "codec_name": "h264",
      "codec_type": "video",
      "width": 320,
      "height": 180,
      "pix_fmt": "yuv420p",
      "field_order": "progressive",
      "r_frame_rate": "15/1",
      "display_aspect_ratio": "16:9",
      "sample_aspect_ratio": "1:1"
    },
    {
      "index": 1,
      "codec_name": "aac",
      "codec_type": "audio",
      "sample_rate": "44100",
      "channels": 1,
      "channel_layout": "mono"
    }
  ],
  "format": {
    "format_name": "matroska,webm",
    "nb_streams": 2,
    "duration": "10.007000",
    "tags": {
      "title": "mediapm fixture",
      "ARTIST": "mediapm"
    }
  }
}
"#;

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
    let mode = value_after(args, "--mode").ok_or_else(|| {
        StubError::usage("--mode is required (expected `copy`, `emit`, `probe` or `sleep`)")
    })?;
    match mode.as_str() {
        "copy" => copy(args),
        "emit" => emit(args),
        "probe" => probe(),
        "sleep" => sleep_for(args),
        unknown => Err(StubError::usage(format!(
            "unknown mode '{unknown}': expected `copy`, `emit`, `probe` or `sleep`"
        ))),
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

/// One entry of an `emit` manifest: a destination path and whose bytes it gets.
struct EmitEntry {
    /// Path the source's bytes are written to, parents created first.
    destination: String,
    /// File whose bytes are written to `destination`.
    source: String,
}

/// Writes the file set the manifest `--manifest` names.
///
/// Every source is read before the first destination is written, and a
/// destination that cannot be written rolls back the destinations this
/// invocation already wrote, so a failure at any point leaves no partial set
/// behind: the caller's hierarchy consumes the whole set, and a set that is
/// silently smaller than the one the manifest named is exactly what this mode
/// exists to make loud.
///
/// # Errors
///
/// Returns a usage failure when `--manifest` is absent, when the manifest names
/// no entry, or when one of its lines is not a destination and a source
/// separated by a tab, and an I/O failure when the manifest or a source cannot
/// be read or a destination cannot be written.
fn emit(args: &[String]) -> Result<(), StubError> {
    let manifest = value_after(args, "--manifest")
        .ok_or_else(|| StubError::usage("--manifest is required in emit mode"))?;
    let text = std::fs::read_to_string(&manifest)
        .map_err(|error| StubError::io(format!("cannot read manifest '{manifest}': {error}")))?;
    let entries = parse_manifest(&text)?;
    // Every source is read before the first destination is written. A caller's
    // hierarchy consumes the whole set, so a source that is missing has to fail
    // the step rather than leave the entries that happened to be readable.
    let mut contents = Vec::with_capacity(entries.len());
    for entry in &entries {
        let bytes = std::fs::read(&entry.source).map_err(|error| {
            StubError::io(format!("cannot read manifest source '{}': {error}", entry.source))
        })?;
        contents.push(bytes);
    }
    // A destination written before a later entry fails is removed again, so the
    // entries that succeeded do not outlive the entry that did not: the mode's
    // contract is that a failed invocation leaves no partial set behind.
    let mut written: Vec<&str> = Vec::with_capacity(entries.len());
    for (entry, bytes) in entries.iter().zip(&contents) {
        if let Err(error) = write_destination(entry, bytes) {
            for &destination in &written {
                let _ = std::fs::remove_file(destination);
            }
            return Err(error);
        }
        written.push(entry.destination.as_str());
    }
    Ok(())
}

/// Writes one manifest entry's bytes to its destination, creating its parents.
///
/// # Errors
///
/// Returns an I/O failure when the destination's parent directories cannot be
/// created or the destination cannot be written.
fn write_destination(entry: &EmitEntry, bytes: &[u8]) -> Result<(), StubError> {
    if let Some(parent) = std::path::Path::new(&entry.destination)
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent).map_err(|error| {
            StubError::io(format!(
                "cannot create the directory for '{}': {error}",
                entry.destination
            ))
        })?;
    }
    std::fs::write(&entry.destination, bytes).map_err(|error| {
        StubError::io(format!("cannot write output '{}': {error}", entry.destination))
    })
}

/// Parses an `emit` manifest into its entries.
///
/// One entry per line, the destination first and the source second, separated by
/// a tab. Both shapes a manifest cannot be read as are usage failures: a line
/// that is not such a pair, and a manifest that names no entry at all. A stub
/// that skipped what it could not read would emit a set that is silently smaller
/// than the one the caller asked for, and a stub that emitted nothing would let a
/// step whose manifest was never populated pass as a successful one.
///
/// # Errors
///
/// Returns a usage failure naming the line that is not a pair, or naming the
/// manifest itself when it carries no entry.
fn parse_manifest(text: &str) -> Result<Vec<EmitEntry>, StubError> {
    let mut entries = Vec::new();
    for (index, line) in text.lines().enumerate() {
        let number = index + 1;
        let fields: Vec<&str> = line.split('\t').collect();
        match fields[..] {
            [destination, source] if !destination.is_empty() && !source.is_empty() => {
                entries.push(EmitEntry {
                    destination: destination.to_string(),
                    source: source.to_string(),
                });
            }
            _ => {
                return Err(StubError::usage(format!(
                    "manifest line {number} is malformed: expected a destination and a source \
                     separated by a tab"
                )));
            }
        }
    }
    if entries.is_empty() {
        return Err(StubError::usage("the manifest names no entry to emit"));
    }
    Ok(entries)
}

/// Writes the fixed `ffprobe`-shaped document to stdout.
///
/// # Errors
///
/// Returns an I/O failure when stdout cannot be written: a document that never
/// reached the caller would leave the caller reading an empty answer as though
/// the probe had found nothing.
fn probe() -> Result<(), StubError> {
    std::io::stdout().lock().write_all(PROBE_DOCUMENT.as_bytes()).map_err(|error| {
        StubError::io(format!("cannot write the probe document to stdout: {error}"))
    })
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
