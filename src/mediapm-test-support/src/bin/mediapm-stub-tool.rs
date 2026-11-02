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
//! # Running as a named tool
//!
//! An offline run does not invoke this binary: it invokes copies of it named
//! after the tools they stand in for, with the command line the real tool would
//! have been given, so there is no `--mode` to read. A copy with no `--mode`
//! dispatches on the file stem of its own `argv[0]`: `yt-dlp` (and the asset
//! names the provider provisions, `yt-dlp_macos` and `yt-dlp_linux`) emits,
//! `ffprobe` probes, and `ffmpeg`, `rsgain`, `sd`, `deno` and `deno.real` — the
//! name the provider's permission shim re-execs, `deno.real.exe` on Windows,
//! whose file stem the suffix leaves as `deno.real` — fall back to the copy
//! shape: the output is the first argument that is not itself an option,
//! scanning right to left, that looks like a path — one carrying a path
//! separator or a non-empty file extension — and the input is the path after the
//! first `-i` when the command line names one and the output itself otherwise.
//! That fallback is what covers the shapes an offline chain has: a tool that
//! reads an input slot and writes an output file (`ffmpeg`), a tool that rewrites
//! the file it is handed in place, which the copy leaves exactly as the step
//! staged it (`sd`, `rsgain`), and a tool invoked with nothing but flags (`deno`,
//! whose provider shim appends `--allow-all`), which has no file to copy and
//! succeeds.
//!
//! Scanning right to left is not enough on its own, because a real command line
//! ends with a flag's *value* as often as with a path: `ffmpeg -f ffmetadata -`
//! asks for an ffmetadata document on stdout and names no output file at all.
//! Taking the last token that is not an option chose the format's name there and
//! wrote a copy of the input under it in the stub's own working directory, outside
//! the sandbox the step was given. So a token qualifies only when it looks like a
//! path, and a command line naming no such token writes nothing and succeeds — a
//! tool handed no file has nothing to do. Any other program name stays the
//! missing-mode usage failure, so a copy that stands in for nothing — including the
//! test harness Cargo writes beside the stub — still answers the way the locator's
//! probe expects.
//!
//! The emit manifest path cannot travel on a command line the preset owns, so it
//! arrives in the environment: `MEDIAPM_STUB_MANIFEST` names it, and an emit with
//! no manifest is a usage failure rather than a silent no-op. When
//! `MEDIAPM_STUB_INVOCATIONS` is set it names a file that every invocation
//! appends one NUL-separated line to — the program's stem, then its arguments —
//! which is how an offline run shows a tool was reached without that tool
//! producing anything itself. Both are optional: a `--mode` run reads neither.
//!
//! # Exit codes
//!
//! `0` on success, `2` when the command line is unusable (unknown mode, a
//! missing flag value, an output that names the input, a manifest that names no
//! usable entry, an emit with no manifest to read), and `3` when a file
//! operation fails, including the invocation record. A stub that defaulted
//! instead of failing would let a broken command line pass as a successful step,
//! so every unusable shape is loud.

use std::ffi::OsStr;
use std::io::Write;
use std::path::Path;
use std::time::Duration;

/// Exit code for a command line the stub cannot act on.
const USAGE_EXIT: i32 = 2;
/// Exit code for a file operation that failed.
const IO_EXIT: i32 = 3;

/// Program names whose copies emit the manifest the environment names.
///
/// The provider provisions yt-dlp under its platform asset's name and the file
/// keeps that name, so every name the tool can be reached under emits.
const EMIT_PROGRAMS: [&str; 3] = ["yt-dlp", "yt-dlp_macos", "yt-dlp_linux"];

/// Program name whose copies answer the probe document.
const PROBE_PROGRAM: &str = "ffprobe";

/// Program names whose copies use the copy-shaped fallback.
///
/// `deno.real` is here because the provider's permission wrapper re-execs the
/// renamed binary (`wrap_deno_binary` in
/// `src/mediapm-conductor/src/tools/provider/mod.rs`), so the process the shim
/// starts carries that name rather than `deno`; on Windows the name is
/// `deno.real.exe`, which `program_stem` reduces to `deno.real`.
const FALLBACK_PROGRAMS: [&str; 5] = ["ffmpeg", "rsgain", "sd", "deno", "deno.real"];

/// Environment variable naming the manifest an emit reads.
///
/// Spelled in `stub_media::MANIFEST_ENV` too, because the chain that sets it
/// cannot import this binary's constants; a test there pins the two spellings
/// together.
const MANIFEST_ENV: &str = "MEDIAPM_STUB_MANIFEST";

/// Environment variable naming the file invocations are appended to.
///
/// Spelled in `stub_media::INVOCATION_RECORD_ENV` too, for the same reason as
/// [`MANIFEST_ENV`].
const INVOCATION_RECORD_ENV: &str = "MEDIAPM_STUB_INVOCATIONS";

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
    let mut raw = std::env::args();
    let program = raw.next();
    let args: Vec<String> = raw.collect();
    let result =
        record_invocation(program.as_deref(), &args).and_then(|()| run(program.as_deref(), &args));
    if let Err(error) = result {
        eprintln!("mediapm-stub-tool: {}", error.message);
        std::process::exit(error.exit_code);
    }
}

/// Dispatches one invocation.
///
/// `--mode` names the mode outright. A copy of this binary invoked as a real
/// tool has no such flag and is dispatched on its own name instead, which is the
/// only thing distinguishing the chain's copies from each other.
///
/// # Errors
///
/// Returns a usage failure when `--mode` names no known mode and no program name
/// claims the invocation, and whatever the selected mode itself reports.
fn run(program: Option<&str>, args: &[String]) -> Result<(), StubError> {
    match value_after(args, "--mode") {
        Some(mode) => run_mode(&mode, args),
        None => run_as_program(program, args),
    }
}

/// Runs the mode `--mode` names.
///
/// # Errors
///
/// Returns a usage failure for an unknown mode, and whatever the selected mode
/// itself reports.
fn run_mode(mode: &str, args: &[String]) -> Result<(), StubError> {
    match mode {
        "copy" => copy(args),
        "emit" => emit(args),
        "probe" => probe(),
        "sleep" => sleep_for(args),
        unknown => Err(StubError::usage(format!(
            "unknown mode '{unknown}': expected `copy`, `emit`, `probe` or `sleep`"
        ))),
    }
}

/// Runs the mode the invocation's own program name stands for.
///
/// # Errors
///
/// Returns the missing-mode usage failure for a name no mode claims, and whatever
/// the dispatched mode itself reports.
fn run_as_program(program: Option<&str>, args: &[String]) -> Result<(), StubError> {
    let stem = program_stem(program);
    if EMIT_PROGRAMS.contains(&stem.as_str()) {
        return emit_from_environment(args);
    }
    if stem == PROBE_PROGRAM {
        return probe();
    }
    if FALLBACK_PROGRAMS.contains(&stem.as_str()) {
        return fallback(args);
    }
    Err(missing_mode())
}

/// The failure an invocation that names no mode reports.
///
/// One spelling, because the locator matches it: a candidate is only accepted
/// when a bare run answers this message (`behaves_like_the_stub` in
/// `src/mediapm-test-support/src/lib.rs`).
fn missing_mode() -> StubError {
    StubError::usage("--mode is required (expected `copy`, `emit`, `probe` or `sleep`)")
}

/// The file stem of `argv[0]`, the name a copy of the stub was invoked under.
///
/// The stem rather than the whole name, because the provider provisions a
/// Windows asset as `yt-dlp.exe`.
fn program_stem(program: Option<&str>) -> String {
    program
        .and_then(|program| Path::new(program).file_stem())
        .and_then(OsStr::to_str)
        .unwrap_or_default()
        .to_string()
}

/// Emits the manifest the environment names.
///
/// The command line a preset renders belongs to the real tool, so the manifest
/// path reaches the stub through [`MANIFEST_ENV`] instead.
///
/// # Errors
///
/// Returns a usage failure when the environment names no manifest, and whatever
/// [`emit`] reports.
fn emit_from_environment(args: &[String]) -> Result<(), StubError> {
    let manifest =
        std::env::var(MANIFEST_ENV).ok().filter(|path| !path.is_empty()).ok_or_else(|| {
            StubError::usage(format!(
                "a copy invoked as yt-dlp emits the manifest named by {MANIFEST_ENV}, \
                 which is unset or empty"
            ))
        })?;
    let mut args = args.to_vec();
    args.push("--manifest".to_string());
    args.push(manifest);
    emit(&args)
}

/// Copies the way `copy` does, for the tools whose command line carries no mode.
///
/// The output is the first argument that is not itself an option, scanning right
/// to left, that looks like a path: a real command line ends with an option's
/// *value* as often as with a path — the provider's deno shim appends
/// `--allow-all` after the arguments it was given, and `ffmpeg -f ffmetadata -`
/// ends with a format's name and a stdout marker — so neither a flag nor a flag's
/// value is a file a tool is handed. A command line with no path-shaped argument
/// names nothing to write and succeeds.
///
/// # Errors
///
/// Returns an I/O failure when the input cannot be read or the output cannot be
/// written.
fn fallback(args: &[String]) -> Result<(), StubError> {
    let Some(output) =
        args.iter().rev().find(|arg| !arg.starts_with('-') && looks_like_a_path(arg))
    else {
        return Ok(());
    };
    // The `-i` input when the command line names one, else the path the command
    // line hands the tool: `sd` and `rsgain` rewrite the file they are given
    // in place, so that file is both the input and the output, and the copy below
    // leaves it as the step staged it. `first_input_path`'s usage failure belongs
    // to `copy`; here a command line with no usable `-i` is the in-place shape
    // rather than a mistake.
    let input = first_input_path(args).unwrap_or_else(|_| output.clone());
    if &input == output {
        return Ok(());
    }
    let bytes = std::fs::read(&input)
        .map_err(|error| StubError::io(format!("cannot read input '{input}': {error}")))?;
    std::fs::write(output, &bytes)
        .map_err(|error| StubError::io(format!("cannot write output '{output}': {error}")))?;
    Ok(())
}

/// Reports whether `token` looks like a file path rather than an option's value.
///
/// Two shapes qualify: a token carrying a path separator (`inputs/input.flac`)
/// and a token whose last dot leaves a non-empty extension (`output-0.mkv`, which
/// the preset emits as a sandbox-relative output slot). The test is about shape
/// rather than existence, because a step's output does not exist yet when the tool
/// is handed its path. Everything else — a format's name, a codec id, a number, a
/// constant — is a value, and writing one escapes the sandbox the step was given.
fn looks_like_a_path(token: &str) -> bool {
    let path = Path::new(token);
    path.parent().is_some_and(|parent| !parent.as_os_str().is_empty())
        || path.extension().is_some_and(|extension| !extension.is_empty())
}

/// Appends this invocation to the record the environment names, when it names one.
///
/// # Errors
///
/// Returns an I/O failure when the record cannot be opened or appended to: a run
/// whose evidence cannot be written must not pass as a run that produced none.
fn record_invocation(program: Option<&str>, args: &[String]) -> Result<(), StubError> {
    let Ok(path) = std::env::var(INVOCATION_RECORD_ENV) else {
        return Ok(());
    };
    if path.is_empty() {
        return Ok(());
    }
    let mut line = program_stem(program);
    for arg in args {
        line.push('\0');
        line.push_str(arg);
    }
    line.push('\n');
    let mut record =
        std::fs::OpenOptions::new().create(true).append(true).open(&path).map_err(|error| {
            StubError::io(format!("cannot open the invocation record '{path}': {error}"))
        })?;
    record.write_all(line.as_bytes()).map_err(|error| {
        StubError::io(format!("cannot append to the invocation record '{path}': {error}"))
    })
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
