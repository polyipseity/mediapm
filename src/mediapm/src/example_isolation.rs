//! Isolation helpers for `mediapm` examples and examples-as-tests.
//!
//! Sets `MEDIAPM_EXAMPLE_*` env overrides to [`mediapm_utils::temp`] directories,
//! and owns the example CLI preflight that builds a missing mediapm CLI on
//! demand ([`example_isolation::ensure_example_cli`]).
//! See `.agents/instructions/example-temp-isolation.instructions.md`.

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::LazyLock;
use std::time::Instant;

use crate::{
    MISSING_MEDIAPM_CLI_MESSAGE, MediaPmCliResolution, MediaPmError, invalid_override_message,
};

pub use mediapm_utils::temp::remove_dir_all_with_retry;

/// Env var overriding the example artifact root (workspace directory the example mutates).
pub const ARTIFACT_ROOT_ENV: &str = "MEDIAPM_EXAMPLE_ARTIFACT_ROOT";

/// Env var overriding the user-level tool download cache root for example runs.
pub const CACHE_ROOT_ENV: &str = "MEDIAPM_EXAMPLE_CACHE_ROOT";

/// Env var gating the Level 3 full online-sync path shared by the online demo
/// (`mediapm_demo_online`) and the non-example `online_sync_post_sync_dump`
/// regression test. The offline demo (`mediapm_demo`) is deterministic and has
/// no Level 3 gate.
///
/// Semantics differ by consumer (the 3-level model): the online demo `main()`
/// runs full sync unless explicitly disabled (`false|0|no|off`); the
/// regression test runs only on an enabled token (`1|true|yes|on`).
pub const RUN_ONLINE_SYNC_ENV: &str = "MEDIAPM_RUN_ONLINE_SYNC";

/// Process-wide lock serializing `MEDIAPM_EXAMPLE_*` env mutation.
///
/// Examples-as-tests run embedded in one process and can execute in parallel
/// (cargo test threads; nextest runs one process per test). Every
/// [`IsolatedExampleRoots`] guard holds this lock for its whole lifetime, and
/// tests that mutate env directly must hold it via [`lock_process_env`], so an
/// env read inside a test can never observe another test's overrides or a
/// guard's half-restored state.
static ENV_LOCK: LazyLock<parking_lot::Mutex<()>> = LazyLock::new(|| parking_lot::Mutex::new(()));

/// Acquires the process-wide env lock for direct env mutation outside a guard.
pub fn lock_process_env() -> parking_lot::MutexGuard<'static, ()> {
    ENV_LOCK.lock()
}

/// Borrows the process-wide env lock.
///
/// [`lock_process_env`] acquires it; this accessor hands out the same static to
/// a caller that must name the mutex it locks rather than take an implicit one.
/// `mediapm-test-support`'s `SeamGuard::new` takes its lock as a required
/// argument and is that caller: the provider seam tests below hold this lock
/// while reading the seam variables, so a seam guard taken under an independent
/// mutex would race them.
#[must_use]
pub fn env_lock() -> &'static parking_lot::Mutex<()> {
    &ENV_LOCK
}

/// Example user-level tool download cache root.
///
/// Honors [`CACHE_ROOT_ENV`] (set by [`IsolatedExampleRoots::with_cache`] for
/// hermetic examples-as-test runs) and otherwise resolves the real persistent
/// OS user-level cache via
/// [`mediapm_conductor::cache_user_level::default_mediapm_user_download_cache_root`],
/// so explicit `cargo run --example` runs share the cache with regular mediapm
/// syncs and persist downloaded tools across runs.
///
/// # Panics
///
/// Panics when the env override is unset and the OS cache directory cannot be
/// determined (mirrors the production sync error).
#[must_use]
pub fn user_level_cache_root() -> PathBuf {
    std::env::var_os(CACHE_ROOT_ENV).map_or_else(
        || {
            mediapm_conductor::cache_user_level::default_mediapm_user_download_cache_root()
                .expect("could not determine default user-level tool cache root")
        },
        PathBuf::from,
    )
}

/// Whether the example runs against an isolated (hermetic) cache root.
///
/// True exactly when [`CACHE_ROOT_ENV`] is present, i.e. inside
/// examples-as-tests using [`IsolatedExampleRoots::with_cache`]. Explicit
/// `cargo run --example` runs (env unset) resolve `false`, meaning the real
/// user-level cache is in use.
#[must_use]
pub fn uses_isolated_cache_root() -> bool {
    std::env::var_os(CACHE_ROOT_ENV).is_some()
}

/// Detects a CI environment from standard CI variables.
///
/// Used by the 3-level run model: Level 1 skips nondeterministic network paths
/// automatically in CI. Mirrors the pattern previously duplicated in
/// `mediapm_demo_online.rs` and `online_sync_post_sync_dump.rs`.
#[must_use]
pub fn ci_mode_detected() -> bool {
    std::env::var("CI")
        .is_ok_and(|v| !v.to_ascii_lowercase().is_empty() && v != "0" && v != "false")
        || std::env::var("GITHUB_ACTIONS").is_ok()
        || std::env::var("GITLAB_CI").is_ok()
        || std::env::var("CIRCLECI").is_ok()
        || std::env::var("TRAVIS").is_ok()
        || std::env::var("BUILDKITE").is_ok()
        || std::env::var("DRONE").is_ok()
}

/// Level 3 opt-in for the non-example `online_sync_post_sync_dump` regression
/// test. Enabled tokens: `1|true|yes|on`. Anything else (unset/disabled/unknown)
/// returns `false` (Level 2 skip). Independent of the `large-tests` Cargo feature.
#[must_use]
pub fn run_online_sync_enabled() -> bool {
    std::env::var(RUN_ONLINE_SYNC_ENV)
        .is_ok_and(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
}

/// Strict parser for the online demo `main()` Level 3 override.
///
/// - `None`/empty → `Ok(true)` (full sync; direct `cargo run --example` = Level 3).
/// - enabled token (`1|true|yes|on`) → `Ok(true)`.
/// - disabled token (`false|0|no|off`) → `Ok(false)` (reduced mode).
/// - unknown token → `Err(...)`.
///
/// Fail-fast on unknown tokens preserves the prior `run_sync_override_rejects_invalid_tokens`
/// contract.
#[expect(clippy::missing_errors_doc, reason = "public helper documented above")]
pub fn parse_run_online_sync_override(value: Option<&str>) -> Result<bool, String> {
    let Some(raw) = value else {
        return Ok(true);
    };

    let normalized = raw.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return Ok(true);
    }

    if matches!(normalized.as_str(), "true" | "1" | "yes" | "on") {
        return Ok(true);
    }

    if matches!(normalized.as_str(), "false" | "0" | "no" | "off") {
        return Ok(false);
    }

    Err(format!(
        "{RUN_ONLINE_SYNC_ENV} only accepts enabled (true/1/yes/on) or disabled (false/0/no/off) values; got '{normalized}'"
    ))
}

/// Env var disabling an example's on-demand build of the mediapm CLI.
///
/// A switch that only subtracts a capability, so the demo works out of the box:
/// the disable tokens (`0|false|no|off`, case-insensitive, trimmed) are the only
/// values that turn the build off, and absent, empty and unrecognized values all
/// leave it on.
const AUTO_BUILD_CLI_ENV: &str = "MEDIAPM_EXAMPLE_AUTO_BUILD_CLI";

/// Cargo's own path to the cargo that launched this process.
///
/// Set by cargo for every binary it runs and never guessed from `PATH`: another
/// cargo can carry another toolchain, profile or target directory, and the CLI
/// build has to land where the running binary looks for its sibling.
const CARGO_ENV: &str = "CARGO";

/// Cargo's manifest directory of the crate this process was built from.
///
/// Joined with `Cargo.toml` to form the `--manifest-path` a CLI build passes, so
/// the build resolves the same workspace and target directory.
const CARGO_MANIFEST_DIR_ENV: &str = "CARGO_MANIFEST_DIR";

/// How many trailing stderr lines a failed CLI build quotes in its error.
///
/// Cargo states its cause in its last lines; a full build log can be thousands
/// of lines and would bury the error it accompanies.
const CLI_BUILD_STDERR_LINES: usize = 20;

/// What an example's CLI preflight decided to do.
///
/// A decision, not an action: the caller owns the build's side effects, so every
/// case is a value a test can assert without spawning anything. Crate-private
/// because [`ensure_example_cli`] is the only public entry point: the plan is
/// how that function decides, not something a caller acts on.
#[derive(Debug, PartialEq, Eq)]
#[must_use]
pub(crate) enum CliPreflightPlan {
    /// A usable CLI already resolves; nothing to build.
    Ready(PathBuf),
    /// No CLI resolves; run this command, then resolve again.
    Build(CliBuildCommand),
    /// No CLI resolves and building is not possible here; the message is the
    /// reason, reported verbatim as the preflight error.
    Refuse(String),
}

/// The one command an example runs to build the mediapm CLI it needs.
///
/// Crate-private with its fields: only [`run_cli_build`] executes it and only
/// the message helpers quote it, so nothing outside this module builds one.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct CliBuildCommand {
    /// The cargo that launched this process (`CARGO`), never a `PATH` guess.
    pub(crate) cargo: PathBuf,
    /// The manifest passed as `--manifest-path`, so the build lands in the
    /// target directory the running cargo used.
    pub(crate) manifest: PathBuf,
    /// The arguments that follow the cargo executable, in order:
    /// `build --package mediapm --manifest-path <manifest>`.
    pub(crate) argv: Vec<String>,
}

/// Resolves the mediapm CLI an example needs, building it when none resolves.
///
/// Managed-tool launcher scripts dispatch through `MEDIAPM_EXECUTABLE`, so an
/// example that runs one needs a CLI build beside its own. `cargo run --example`
/// builds only the example, so the CLI can legitimately be absent; this builds
/// it from the running crate's manifest with the cargo that launched the
/// example.
///
/// Building is the last remedy, and three guards keep it there: never under a
/// test harness, never in CI (which builds the CLI as its own step, so a demo
/// build would hide a missing one), and never when `MEDIAPM_EXAMPLE_AUTO_BUILD_CLI`
/// holds a disable token (`0|false|no|off`, case-insensitive). Everything else
/// that prevents a build — no `CARGO`, no `CARGO_MANIFEST_DIR`, an invalid
/// `MEDIAPM_EXECUTABLE` — is refused with the reason instead of built over.
///
/// Prints one line before the build starts, because a first build of this
/// workspace can take minutes and there is no timeout to report progress, and one
/// line with the elapsed whole seconds when it succeeds.
///
/// That build is synchronous and untimed, so it spends part of whatever budget
/// the caller has already armed: the online demo spawns its hard timeout guard
/// before calling this preflight, so a cold CLI build counts against
/// `MEDIAPM_DEMO_ONLINE_TIMEOUT_SECS`.
///
/// `is_test` is the test-harness guard, and only the caller can state it: pass
/// `cfg!(test)` from the example's own crate, because a library unit cannot
/// observe the example's test build, so the guard has to be stated by the caller.
///
/// Not exporting the resolved path here is safe because nothing between this call
/// and the workflows needs it: `sync_library` exports `MEDIAPM_EXECUTABLE` in
/// `service.rs` before the first `run_workflow`, the generated launcher scripts
/// re-read the variable from the host environment when they exec, and tool
/// provisioning spawns no process at all.
///
/// # Errors
///
/// Returns [`MediaPmError::Workflow`] when resolution fails, when the build is
/// refused, when cargo exits non-zero, or when a successful build still leaves no
/// CLI beside the running binary.
pub fn ensure_example_cli(is_test: bool) -> Result<PathBuf, MediaPmError> {
    match plan_cli_preflight(
        crate::resolve_mediapm_cli()?,
        allow_build(is_test),
        std::env::var_os(CARGO_ENV).map(PathBuf::from),
        manifest_dir(),
    ) {
        CliPreflightPlan::Ready(path) => Ok(path),
        CliPreflightPlan::Refuse(message) => Err(MediaPmError::Workflow(message)),
        CliPreflightPlan::Build(command) => run_cli_build(&command),
    }
}

/// Decides what an example's CLI preflight does with what resolution found.
///
/// A resolvable CLI wins over everything, because building is only ever the
/// remedy for having none. An invalid override is refused rather than built
/// over: the variable is the user's statement of where the CLI is, and a build
/// would silently ignore it. A missing CLI is built only when `allow_build` says
/// so and both cargo inputs are known.
///
/// `allow_build` arrives precomputed so this decision reads no process state of
/// its own; `cargo` is `CARGO` and `manifest` is the running crate's `Cargo.toml`,
/// both `None` outside a cargo-launched process.
fn plan_cli_preflight(
    resolution: MediaPmCliResolution,
    allow_build: bool,
    cargo: Option<PathBuf>,
    manifest: Option<PathBuf>,
) -> CliPreflightPlan {
    match resolution {
        MediaPmCliResolution::Found(path) => CliPreflightPlan::Ready(path),
        MediaPmCliResolution::InvalidOverride(path) => {
            CliPreflightPlan::Refuse(invalid_override_message(&path))
        }
        MediaPmCliResolution::Missing if !allow_build => {
            CliPreflightPlan::Refuse(MISSING_MEDIAPM_CLI_MESSAGE.to_string())
        }
        MediaPmCliResolution::Missing => match (cargo, manifest) {
            (Some(cargo), Some(manifest)) => {
                CliPreflightPlan::Build(cli_build_command(cargo, manifest))
            }
            (None, None) => CliPreflightPlan::Refuse(format!(
                "{MISSING_MEDIAPM_CLI_MESSAGE}; neither CARGO nor CARGO_MANIFEST_DIR is set, so the demo cannot build one"
            )),
            (None, Some(_)) => CliPreflightPlan::Refuse(format!(
                "{MISSING_MEDIAPM_CLI_MESSAGE}; CARGO is not set, so the demo cannot build one"
            )),
            (Some(_), None) => CliPreflightPlan::Refuse(format!(
                "{MISSING_MEDIAPM_CLI_MESSAGE}; CARGO_MANIFEST_DIR is not set, so the demo cannot build one"
            )),
        },
    }
}

/// The cargo command that builds the CLI from the manifest the running example
/// was built from.
///
/// `--manifest-path` is what keeps the build in the target directory the running
/// binary came from: the manifest is the one cargo reported for this process, so
/// `CARGO_TARGET_DIR` and `--target-dir` conventions stay in force. The argv is
/// assembled here rather than at the spawn so the command a test asserts is the
/// command that runs.
fn cli_build_command(cargo: PathBuf, manifest: PathBuf) -> CliBuildCommand {
    let argv = vec![
        "build".to_string(),
        "--package".to_string(),
        "mediapm".to_string(),
        "--manifest-path".to_string(),
        manifest.display().to_string(),
    ];
    CliBuildCommand { cargo, manifest, argv }
}

/// Whether an example may build the CLI it needs.
///
/// All three guards must hold: `is_test` is `false` (the examples-as-tests run
/// reduced mode and the harness owns argv and output), [`ci_mode_detected`] is
/// `false`, and [`AUTO_BUILD_CLI_ENV`] holds no disable token.
///
/// `is_test` is passed in rather than read from `cfg!(test)` here: test-ness is a
/// property of the compilation unit that asks, so only the caller can answer for
/// itself — [`ensure_example_cli`] therefore takes it as its own parameter.
fn allow_build(is_test: bool) -> bool {
    allow_build_from(is_test, ci_mode_detected(), auto_build_cli_enabled())
}

/// The three-guard conjunction [`allow_build`] reads process state for.
///
/// Split out so the truth table is a pure function a test can pin: under a test
/// harness `is_test` short-circuits the other two conjuncts, so a test that went
/// through [`allow_build`] alone could not tell a dropped guard from a working
/// one. No conjunct is redundant — the harness owns argv and output, CI builds
/// the CLI as its own step (a demo build would hide a missing one), and
/// [`AUTO_BUILD_CLI_ENV`] is the user's opt-out.
fn allow_build_from(is_test: bool, ci_detected: bool, env_enabled: bool) -> bool {
    !is_test && !ci_detected && env_enabled
}

/// Whether [`AUTO_BUILD_CLI_ENV`] leaves the on-demand CLI build enabled.
///
/// Mirrors [`run_online_sync_enabled`]'s token parsing with the default inverted:
/// this switch only subtracts a capability, so absent, empty and unrecognized
/// values mean enabled, and only `0|false|no|off` (case-insensitive, trimmed)
/// disable the build.
fn auto_build_cli_enabled() -> bool {
    let disabled = std::env::var(AUTO_BUILD_CLI_ENV).is_ok_and(|value| {
        matches!(value.trim().to_ascii_lowercase().as_str(), "0" | "false" | "no" | "off")
    });
    !disabled
}

/// The `Cargo.toml` of the crate this process was built from.
///
/// `cargo run` and `cargo test` set `CARGO_MANIFEST_DIR` for the process they
/// launch, so an explicit example run carries it. A binary started another way
/// does not, and a build would then have to guess which manifest to build, so the
/// preflight refuses instead.
fn manifest_dir() -> Option<PathBuf> {
    std::env::var_os(CARGO_MANIFEST_DIR_ENV).map(|dir| PathBuf::from(dir).join("Cargo.toml"))
}

/// Runs a [`CliBuildCommand`] and resolves the CLI it was built to produce.
///
/// Spawns with this process's environment, so `CARGO_TARGET_DIR`, `RUSTFLAGS`,
/// `RUSTC_WRAPPER` and cargo configuration carry over, and captures both streams:
/// stdout is a build log, and stderr is what the failure message quotes. There is
/// no timeout — a first build of this workspace can take minutes, and a truncated
/// build is worse than a slow one; the line printed before the spawn is the
/// progress signal.
fn run_cli_build(command: &CliBuildCommand) -> Result<PathBuf, MediaPmError> {
    eprintln!("[mediapm] no mediapm CLI found; building one with cargo build --package mediapm");
    let started = Instant::now();
    let output = Command::new(&command.cargo).args(&command.argv).output().map_err(|error| {
        MediaPmError::Workflow(format!(
            "failed to run '{}' to build the mediapm CLI: {error}",
            command.cargo.display()
        ))
    })?;

    if !output.status.success() {
        return Err(MediaPmError::Workflow(cli_build_failure_message(
            command,
            output.status,
            &output.stderr,
        )));
    }

    let path = match crate::resolve_mediapm_cli()? {
        MediaPmCliResolution::Found(path) => path,
        // The override cannot become invalid during a build (it is checked before
        // one starts), so both arms are the same failure: the build succeeded but
        // wrote no CLI this process can find beside its own binary.
        MediaPmCliResolution::Missing | MediaPmCliResolution::InvalidOverride(_) => {
            return Err(MediaPmError::Workflow(missing_after_success_message(command)));
        }
    };
    eprintln!("[mediapm] built the mediapm CLI in {}s", started.elapsed().as_secs());
    Ok(path)
}

/// What a failed CLI build reports: the command, its status, cargo's own last
/// [`CLI_BUILD_STDERR_LINES`] lines, and the way out for a host that cannot run
/// the build at all.
///
/// The tail is quoted, not the whole log: a full build log can be thousands of
/// lines and would bury the status it accompanies. The limit is passed to
/// [`last_stderr_lines`] from this one constant, so the count the message
/// promises is the count the reader gets. [`MISSING_MEDIAPM_CLI_MESSAGE`] is
/// appended inside the first line, ahead of the quoted tail, so a reader on an
/// offline host is still told where `MEDIAPM_EXECUTABLE` can point instead.
fn cli_build_failure_message(
    command: &CliBuildCommand,
    status: std::process::ExitStatus,
    stderr: &[u8],
) -> String {
    format!(
        "`{} {}` failed with {}; the mediapm CLI was not built from '{}'; {MISSING_MEDIAPM_CLI_MESSAGE}. Last {CLI_BUILD_STDERR_LINES} lines of stderr:\n{}",
        command.cargo.display(),
        command.argv.join(" "),
        status,
        command.manifest.display(),
        last_stderr_lines(stderr, CLI_BUILD_STDERR_LINES),
    )
}

/// The profile a bare `cargo build` writes, and so the profile every
/// [`cli_build_command`] build lands in.
const DEFAULT_BUILD_PROFILE: &str = "debug";

/// What a successful CLI build reports when no CLI still resolves.
///
/// A successful build that leaves nothing findable beside the running binary is
/// a profile mismatch, not a cargo failure: [`cli_build_command`] passes no
/// `--profile`, so the build always writes the default `debug` profile, while
/// resolution probes the profile directory the running binary lives in. Naming
/// the running profile, when the executable path carries one, is what turns the
/// message into the command that fixes it.
fn missing_after_success_message(command: &CliBuildCommand) -> String {
    missing_after_success_message_for(command, running_profile().as_deref())
}

/// [`missing_after_success_message`] with the running profile supplied.
///
/// Split out so the message a profile-selected run reads is pinned by a test
/// instead of depending on the profile the test binary itself was built in.
/// The same-profile arm names the target-directory variables because that is
/// the only thing left to explain when the profile already matches: the build
/// and the probe agree on the profile, so they disagree on the target directory.
fn missing_after_success_message_for(
    command: &CliBuildCommand,
    running_profile: Option<&str>,
) -> String {
    let remedy = match running_profile {
        Some(profile) if profile == DEFAULT_BUILD_PROFILE => {
            "this run lives in that same profile, so the build landed in a target directory this binary does not probe; check CARGO_TARGET_DIR and --target-dir".to_string()
        }
        Some(profile) => format!(
            "this run lives in the `{profile}` profile, so a build for this run needs an explicit `cargo build --release -p mediapm`"
        ),
        None => "a run from another profile needs an explicit `cargo build --release -p mediapm`".to_string(),
    };
    format!(
        "the mediapm CLI build from '{}' succeeded, but no CLI appeared beside the running binary; the build writes the default `{DEFAULT_BUILD_PROFILE}` profile, and {remedy}",
        command.manifest.display()
    )
}

/// The cargo profile an executable path names, when its layout names one.
///
/// A cargo-built binary sits at `<target-dir>[/<triple>]/<profile>/<kind>/<name>`,
/// with `examples` or `deps` for the two kinds an example runs as, so the profile
/// is the directory two levels above the executable. The `<kind>` segment is what
/// makes the guess safe: a path under another layout answers `None` instead of
/// reporting whatever directory happens to enclose it as a profile.
///
/// Takes the path as an argument so the layout half is pinned by literals; see
/// [`running_profile`] for the half that reads this process.
fn profile_from_exe_path(path: &Path) -> Option<String> {
    let kind_dir = path.parent()?;
    let kind = kind_dir.file_name().and_then(std::ffi::OsStr::to_str);
    if !matches!(kind, Some("examples" | "deps")) {
        return None;
    }
    let profile = kind_dir.parent()?.file_name()?;
    Some(profile.to_string_lossy().into_owned())
}

/// The cargo profile the running binary lives in, when its path names one.
fn running_profile() -> Option<String> {
    profile_from_exe_path(&std::env::current_exe().ok()?)
}

/// The last `limit` lines of a build's stderr, as a failure message quotes them.
///
/// Decoded lossily because a cargo log can carry bytes from the tools it invoked.
/// An empty stream answers `(no stderr)` so the quoting clause never dangles.
fn last_stderr_lines(stderr: &[u8], limit: usize) -> String {
    let text = String::from_utf8_lossy(stderr);
    let lines: Vec<&str> = text.lines().collect();
    let start = lines.len().saturating_sub(limit);
    let tail = lines[start..].join("\n");
    if tail.is_empty() { "(no stderr)".to_string() } else { tail }
}

/// RAII guard pointing example env vars at unique prefixed tempdirs for one test run.
pub struct IsolatedExampleRoots {
    artifact_dir: tempfile::TempDir,
    cache_dir: Option<tempfile::TempDir>,
    previous_artifact_root: Option<OsString>,
    previous_cache_root: Option<OsString>,
    // Declared last so it drops last: env restore and tempdir cleanup in
    // `Drop` run while the process-wide env lock is still held.
    _env_lock: parking_lot::MutexGuard<'static, ()>,
}

impl IsolatedExampleRoots {
    /// Isolates artifact and user-level tool-download cache roots.
    ///
    /// # Panics
    ///
    /// Panics when the prefixed artifact or cache tempdir cannot be created.
    #[must_use]
    pub fn with_cache() -> Self {
        let env_lock = lock_process_env();
        let artifact_dir = mediapm_utils::temp::artifact_dir().expect("create temp artifact root");
        let cache_dir = mediapm_utils::temp::cache_dir().expect("create temp cache root");
        let previous_artifact_root = std::env::var_os(ARTIFACT_ROOT_ENV);
        let previous_cache_root = std::env::var_os(CACHE_ROOT_ENV);
        // SAFETY: test/example guard scopes env overrides to this struct's lifetime.
        unsafe {
            std::env::set_var(ARTIFACT_ROOT_ENV, artifact_dir.path());
            std::env::set_var(CACHE_ROOT_ENV, cache_dir.path());
        }
        Self {
            artifact_dir,
            cache_dir: Some(cache_dir),
            previous_artifact_root,
            previous_cache_root,
            _env_lock: env_lock,
        }
    }

    /// Isolates only the artifact root (no cache override).
    ///
    /// # Panics
    ///
    /// Panics when the prefixed artifact tempdir cannot be created.
    #[must_use]
    pub fn artifact_only() -> Self {
        let env_lock = lock_process_env();
        let artifact_dir = mediapm_utils::temp::artifact_dir().expect("create temp artifact root");
        let previous_artifact_root = std::env::var_os(ARTIFACT_ROOT_ENV);
        // SAFETY: test/example guard scopes env overrides to this struct's lifetime.
        unsafe {
            std::env::set_var(ARTIFACT_ROOT_ENV, artifact_dir.path());
        }
        Self {
            artifact_dir,
            cache_dir: None,
            previous_artifact_root,
            previous_cache_root: std::env::var_os(CACHE_ROOT_ENV),
            _env_lock: env_lock,
        }
    }

    /// Isolated artifact root path (the effective workspace / artifact directory).
    #[must_use]
    pub fn artifact_root_path(&self) -> &Path {
        self.artifact_dir.path()
    }

    /// Isolated user-level download cache path when [`Self::with_cache`] was used.
    #[must_use]
    pub fn cache_root_path(&self) -> Option<&Path> {
        self.cache_dir.as_ref().map(tempfile::TempDir::path)
    }
}

impl Drop for IsolatedExampleRoots {
    fn drop(&mut self) {
        // SAFETY: restore prior env values before deleting tempdirs.
        unsafe {
            match &self.previous_artifact_root {
                Some(value) => std::env::set_var(ARTIFACT_ROOT_ENV, value),
                None => std::env::remove_var(ARTIFACT_ROOT_ENV),
            }
            match &self.previous_cache_root {
                Some(value) => std::env::set_var(CACHE_ROOT_ENV, value),
                None => std::env::remove_var(CACHE_ROOT_ENV),
            }
        }

        let _ = remove_dir_all_with_retry(self.artifact_dir.path());
        if let Some(cache_dir) = &self.cache_dir {
            let _ = remove_dir_all_with_retry(cache_dir.path());
        }
    }
}

/// Creates a prefixed artifact tempdir for share-violation fallbacks during manual runs.
///
/// # Errors
///
/// Returns [`std::io::Error`] when the directory cannot be created.
pub fn isolated_artifact_dir() -> std::io::Result<(tempfile::TempDir, PathBuf)> {
    let dir = mediapm_utils::temp::artifact_dir()?;
    let path = dir.path().to_path_buf();
    Ok((dir, path))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Restores `CACHE_ROOT_ENV` to its prior value; every env-mutating test
    /// below holds [`lock_process_env`] for its whole body.
    fn restore_cache_root_env(previous: Option<OsString>) {
        // SAFETY: env mutation is serialized by the process-wide env lock.
        unsafe {
            match previous {
                Some(value) => std::env::set_var(CACHE_ROOT_ENV, value),
                None => std::env::remove_var(CACHE_ROOT_ENV),
            }
        }
    }

    #[test]
    fn user_level_cache_root_unset_uses_real_user_cache() {
        let _lock = lock_process_env();
        let previous = std::env::var_os(CACHE_ROOT_ENV);
        // SAFETY: env mutation is serialized by the process-wide env lock.
        unsafe {
            std::env::remove_var(CACHE_ROOT_ENV);
        }
        let expected =
            mediapm_conductor::cache_user_level::default_mediapm_user_download_cache_root()
                .expect("could not determine default user-level tool cache root");
        assert_eq!(user_level_cache_root(), expected);
        restore_cache_root_env(previous);
    }

    #[test]
    fn user_level_cache_root_override_wins() {
        let _lock = lock_process_env();
        let previous = std::env::var_os(CACHE_ROOT_ENV);
        // SAFETY: env mutation is serialized by the process-wide env lock.
        unsafe {
            std::env::set_var(CACHE_ROOT_ENV, "/tmp/example-cache-override");
        }
        assert_eq!(user_level_cache_root(), PathBuf::from("/tmp/example-cache-override"));
        restore_cache_root_env(previous);
    }

    #[test]
    fn uses_isolated_cache_root_true_when_env_set() {
        let _lock = lock_process_env();
        let previous = std::env::var_os(CACHE_ROOT_ENV);
        // SAFETY: env mutation is serialized by the process-wide env lock.
        unsafe {
            std::env::set_var(CACHE_ROOT_ENV, "/tmp/example-cache-override");
        }
        assert!(uses_isolated_cache_root());
        restore_cache_root_env(previous);
    }

    #[test]
    fn uses_isolated_cache_root_false_when_unset() {
        let _lock = lock_process_env();
        let previous = std::env::var_os(CACHE_ROOT_ENV);
        // SAFETY: env mutation is serialized by the process-wide env lock.
        unsafe {
            std::env::remove_var(CACHE_ROOT_ENV);
        }
        assert!(!uses_isolated_cache_root());
        restore_cache_root_env(previous);
    }

    #[test]
    fn isolated_roots_restore_env_and_cleanup() {
        let previous = std::env::var_os(ARTIFACT_ROOT_ENV);
        let guard = IsolatedExampleRoots::with_cache();
        assert!(std::env::var_os(ARTIFACT_ROOT_ENV).is_some());
        assert!(std::env::var_os(CACHE_ROOT_ENV).is_some());
        drop(guard);
        match &previous {
            Some(value) => assert_eq!(std::env::var_os(ARTIFACT_ROOT_ENV), Some(value.clone())),
            None => assert!(std::env::var_os(ARTIFACT_ROOT_ENV).is_none()),
        }
    }

    /// Sets `AUTO_BUILD_CLI_ENV` and restores it when dropped, so an assertion
    /// that panics cannot leak the token into the rest of the process.
    ///
    /// The caller holds [`lock_process_env`] for the guard's whole lifetime:
    /// that serializes the mutation against the other env-reading tests, and it
    /// is why the restore may touch the environment at all.
    struct AutoBuildCliEnv {
        previous: Option<OsString>,
    }

    impl AutoBuildCliEnv {
        /// Sets the variable to `value`, or removes it for `None`.
        fn set(value: Option<&str>) -> Self {
            let previous = std::env::var_os(AUTO_BUILD_CLI_ENV);
            // SAFETY: the caller holds the process-wide env lock.
            unsafe {
                match value {
                    Some(value) => std::env::set_var(AUTO_BUILD_CLI_ENV, value),
                    None => std::env::remove_var(AUTO_BUILD_CLI_ENV),
                }
            }
            Self { previous }
        }
    }

    impl Drop for AutoBuildCliEnv {
        fn drop(&mut self) {
            // SAFETY: the caller still holds the process-wide env lock.
            unsafe {
                match &self.previous {
                    Some(value) => std::env::set_var(AUTO_BUILD_CLI_ENV, value),
                    None => std::env::remove_var(AUTO_BUILD_CLI_ENV),
                }
            }
        }
    }

    /// Unwraps the refusal a preflight plan must be, so each test asserts the
    /// message it expects instead of silently accepting the wrong branch.
    fn refuse_message(plan: CliPreflightPlan) -> String {
        match plan {
            CliPreflightPlan::Refuse(message) => message,
            other => panic!("expected a refusal, got {other:?}"),
        }
    }

    /// The preflight builds only as the last remedy: a resolvable CLI wins over a
    /// permitted build, and an invalid override is reported rather than built
    /// over, because building would silently ignore the user's statement of where
    /// the CLI lives. The `allow_build(true)` row is the examples-as-tests case —
    /// `allow_build` answers `false` under a test harness — so the decision table
    /// itself proves a test never reaches the build.
    #[test]
    fn cli_preflight_builds_only_when_nothing_resolves() {
        let cargo = PathBuf::from("/opt/rust/bin/cargo");
        let manifest = PathBuf::from("/work/src/mediapm/Cargo.toml");
        let cli = PathBuf::from("/work/target/debug/mediapm");
        let rejected = PathBuf::from("/work/target/debug/examples/mediapm_demo_online");

        let cases = [
            (
                "a resolvable CLI wins over a permitted build",
                MediaPmCliResolution::Found(cli.clone()),
                true,
                CliPreflightPlan::Ready(cli.clone()),
            ),
            (
                "a resolvable CLI wins over a forbidden build",
                MediaPmCliResolution::Found(cli.clone()),
                false,
                CliPreflightPlan::Ready(cli.clone()),
            ),
            (
                "an invalid override is refused even when a build is permitted",
                MediaPmCliResolution::InvalidOverride(rejected.clone()),
                true,
                CliPreflightPlan::Refuse(invalid_override_message(&rejected)),
            ),
            (
                "a missing CLI with building forbidden is refused",
                MediaPmCliResolution::Missing,
                false,
                CliPreflightPlan::Refuse(MISSING_MEDIAPM_CLI_MESSAGE.to_string()),
            ),
            (
                "a missing CLI in a test harness is refused, never built",
                MediaPmCliResolution::Missing,
                allow_build(true),
                CliPreflightPlan::Refuse(MISSING_MEDIAPM_CLI_MESSAGE.to_string()),
            ),
            (
                "a missing CLI with building permitted is built",
                MediaPmCliResolution::Missing,
                true,
                CliPreflightPlan::Build(cli_build_command(cargo.clone(), manifest.clone())),
            ),
        ];

        for (case, resolution, build_allowed, expected) in cases {
            let plan = plan_cli_preflight(
                resolution,
                build_allowed,
                Some(cargo.clone()),
                Some(manifest.clone()),
            );
            assert_eq!(plan, expected, "case: {case}");
        }
    }

    /// The decision is all three guards, not any two of them. Under a test
    /// harness `is_test` short-circuits the other conjuncts, so [`allow_build`]
    /// itself can only ever answer `false` in a test; this pins the composition
    /// the caller depends on, where dropping the CI conjunct would let a demo
    /// build inside a CI run that builds the CLI as its own step.
    #[test]
    fn allow_build_requires_every_guard() {
        let cases = [
            ("all guards clear", false, false, true, true),
            ("a test harness", true, false, true, false),
            ("CI", false, true, true, false),
            ("the env opt-out", false, false, false, false),
            ("a test in CI", true, true, true, false),
            ("a test with the opt-out", true, false, false, false),
            ("CI with the opt-out", false, true, false, false),
            ("all three guards", true, true, false, false),
        ];

        for (case, is_test, ci_detected, env_enabled, expected) in cases {
            assert_eq!(
                allow_build_from(is_test, ci_detected, env_enabled),
                expected,
                "case: {case}"
            );
        }
    }

    /// A permitted build still needs both cargo inputs, and the refusal names the
    /// variable that is absent: "cannot build" without a name leaves the reader
    /// with nothing to set. The missing-CLI remedy stays in front of the clause,
    /// so the message still says how to get a CLI by hand.
    #[test]
    fn cli_preflight_refuses_without_cargo_or_manifest() {
        let cargo = PathBuf::from("/opt/rust/bin/cargo");
        let manifest = PathBuf::from("/work/src/mediapm/Cargo.toml");

        let without_cargo =
            plan_cli_preflight(MediaPmCliResolution::Missing, true, None, Some(manifest));
        let without_manifest =
            plan_cli_preflight(MediaPmCliResolution::Missing, true, Some(cargo), None);
        let without_either = plan_cli_preflight(MediaPmCliResolution::Missing, true, None, None);

        for (case, plan, clause) in [
            ("no CARGO", without_cargo, "; CARGO is not set, so the demo cannot build one"),
            (
                "no CARGO_MANIFEST_DIR",
                without_manifest,
                "; CARGO_MANIFEST_DIR is not set, so the demo cannot build one",
            ),
        ] {
            let message = refuse_message(plan);
            assert!(
                message.starts_with(MISSING_MEDIAPM_CLI_MESSAGE),
                "case {case}: keeps the missing-CLI remedy: {message}"
            );
            assert!(
                message.ends_with(clause),
                "case {case}: names the missing variable: {message}"
            );
        }

        let message = refuse_message(without_either);
        assert!(
            message.starts_with(MISSING_MEDIAPM_CLI_MESSAGE),
            "keeps the missing-CLI remedy: {message}"
        );
        assert!(
            message.contains("neither CARGO nor CARGO_MANIFEST_DIR is set"),
            "names both variables: {message}"
        );
    }

    /// The build targets the manifest the running binary came from, so cargo
    /// resolves the same workspace and writes the CLI beside the binary that
    /// needs it. A build from another manifest could land in a target directory
    /// the resolver beside this example never probes.
    #[test]
    fn cli_build_argv_targets_the_running_manifest() {
        let cargo = PathBuf::from("/opt/rust/bin/cargo");
        let manifest = PathBuf::from("/work/src/mediapm/Cargo.toml");

        let command = cli_build_command(cargo.clone(), manifest.clone());

        assert_eq!(
            command.argv,
            vec![
                "build".to_string(),
                "--package".to_string(),
                "mediapm".to_string(),
                "--manifest-path".to_string(),
                "/work/src/mediapm/Cargo.toml".to_string(),
            ]
        );
        assert_eq!(command.cargo, cargo, "uses the cargo that launched the example");
        assert_eq!(command.manifest, manifest, "keeps the manifest it passes");
    }

    /// A failing [`std::process::ExitStatus`] built without spawning anything:
    /// the message helpers only read its `Display`.
    fn failed_exit_status() -> std::process::ExitStatus {
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt as _;
            std::process::ExitStatus::from_raw(101)
        }
        #[cfg(windows)]
        {
            use std::os::windows::process::ExitStatusExt as _;
            std::process::ExitStatus::from_raw(101)
        }
    }

    /// A failed build's error names the command, its status and cargo's tail —
    /// the last [`CLI_BUILD_STDERR_LINES`] lines only. Quoting `stderr` whole
    /// would bury the status, and quoting it raw would print a build log inline.
    /// The missing-CLI remedy rides in the first line, ahead of the tail, so a
    /// host that cannot build at all still learns where `MEDIAPM_EXECUTABLE` can
    /// point.
    #[test]
    fn cli_build_failure_message_quotes_the_last_stderr_lines() {
        let command = cli_build_command(
            PathBuf::from("/opt/rust/bin/cargo"),
            PathBuf::from("/work/src/mediapm/Cargo.toml"),
        );
        let status = failed_exit_status();
        let stderr: Vec<String> = (1..=25).map(|line| format!("error line {line}")).collect();

        let message = cli_build_failure_message(&command, status, stderr.join("\n").as_bytes());

        assert!(
            message.starts_with(
                "`/opt/rust/bin/cargo build --package mediapm --manifest-path /work/src/mediapm/Cargo.toml` failed with "
            ),
            "names the command that failed: {message}"
        );
        assert!(
            message.contains(&format!("{status}; the mediapm CLI was not built from '/work/src/mediapm/Cargo.toml'; {MISSING_MEDIAPM_CLI_MESSAGE}. Last 20 lines of stderr:\n")),
            "quotes cargo's status and the manifest, offers the escape hatch, and promises 20 lines: {message}"
        );
        assert_eq!(
            message.lines().skip(1).collect::<Vec<_>>(),
            stderr[5..].iter().map(String::as_str).collect::<Vec<_>>(),
            "quotes exactly the newest 20 lines, in order"
        );
    }

    /// A build that succeeds without producing a findable CLI is reported as a
    /// profile mismatch, not a cargo failure: the build always writes the default
    /// `debug` profile, so a run built for another profile has to name its own
    /// build, and a run whose profile cannot be read is told the same without a
    /// profile name. Its own message, not the failed-build one.
    #[test]
    fn missing_after_success_message_names_the_debug_profile_and_the_running_one() {
        let command = cli_build_command(
            PathBuf::from("/opt/rust/bin/cargo"),
            PathBuf::from("/work/src/mediapm/Cargo.toml"),
        );
        let built = "the mediapm CLI build from '/work/src/mediapm/Cargo.toml' succeeded, but no CLI appeared beside the running binary; the build writes the default `debug` profile, and ";

        assert_eq!(
            missing_after_success_message_for(&command, Some("release")),
            format!(
                "{built}this run lives in the `release` profile, so a build for this run needs an explicit `cargo build --release -p mediapm`"
            )
        );
        assert_eq!(
            missing_after_success_message_for(&command, Some("debug")),
            format!(
                "{built}this run lives in that same profile, so the build landed in a target directory this binary does not probe; check CARGO_TARGET_DIR and --target-dir"
            )
        );
        assert_eq!(
            missing_after_success_message_for(&command, None),
            format!(
                "{built}a run from another profile needs an explicit `cargo build --release -p mediapm`"
            )
        );
    }

    /// [`profile_from_exe_path`] reads the two layouts an example runs as and
    /// refuses everything else, so a plugin- or install-layout binary is not
    /// told its enclosing directory is a cargo profile.
    #[test]
    fn profile_from_exe_path_reads_cargo_layouts_only() {
        assert_eq!(
            profile_from_exe_path(Path::new("/work/target/release/examples/x")).as_deref(),
            Some("release"),
            "an example built with `--release` lives in the release profile"
        );
        assert_eq!(
            profile_from_exe_path(Path::new("/work/target/debug/deps/mod-1")).as_deref(),
            Some("debug"),
            "a test binary in `deps` lives in the debug profile"
        );
        assert_eq!(
            profile_from_exe_path(Path::new("/usr/local/bin/x")),
            None,
            "an installed binary names no cargo profile"
        );
    }

    /// The message a profile-selected run reads is the arm for the profile this
    /// process's own path names, so the executable-path read is wired into the
    /// message. Compared against that arm rather than the arms in isolation:
    /// dropping the wiring would leave the message on the `None` arm while the
    /// expected arm stayed put.
    #[test]
    fn missing_after_success_message_reads_the_running_profile() {
        let command = cli_build_command(
            PathBuf::from("/opt/rust/bin/cargo"),
            PathBuf::from("/work/src/mediapm/Cargo.toml"),
        );
        let profile = profile_from_exe_path(&std::env::current_exe().expect("current exe path"));

        assert!(profile.is_some(), "the harness runs a cargo-layout binary: {profile:?}");

        let message = missing_after_success_message(&command);

        assert_eq!(
            message,
            missing_after_success_message_for(&command, profile.as_deref()),
            "renders the arm for the profile this binary's path names"
        );
        assert!(
            message.contains("the build writes the default `debug` profile, and "),
            "keeps the profile explanation: {message}"
        );
    }

    /// The quoting clause never dangles: an empty stream says so, and the tail is
    /// the boundary case — one line more than the limit drops the oldest only.
    #[test]
    fn last_stderr_lines_quotes_the_tail_or_says_so() {
        assert_eq!(last_stderr_lines(b"", CLI_BUILD_STDERR_LINES), "(no stderr)");
        assert_eq!(last_stderr_lines(b"a single line", CLI_BUILD_STDERR_LINES), "a single line");

        let over_limit: Vec<String> = (1..=21).map(|line| format!("line {line}")).collect();
        let tail = last_stderr_lines(over_limit.join("\n").as_bytes(), CLI_BUILD_STDERR_LINES);

        assert_eq!(
            tail.lines().collect::<Vec<_>>(),
            over_limit[1..].iter().map(String::as_str).collect::<Vec<_>>(),
            "keeps the newest lines and drops the oldest"
        );
    }

    /// `MEDIAPM_EXAMPLE_AUTO_BUILD_CLI` only subtracts a capability: the four
    /// disable tokens (trimmed, case-insensitive) turn the build off, and absent,
    /// empty and unrecognized values leave a demo that must work out of the box
    /// able to build.
    #[test]
    fn auto_build_cli_parses_disable_tokens() {
        let _lock = lock_process_env();

        for token in ["0", "false", "no", "off", "FALSE", " Off ", "NO"] {
            let _env = AutoBuildCliEnv::set(Some(token));
            assert!(!auto_build_cli_enabled(), "'{token}' disables the build");
        }

        for token in ["", "1", "true", "yes", "on", "maybe"] {
            let _env = AutoBuildCliEnv::set(Some(token));
            assert!(auto_build_cli_enabled(), "'{token}' leaves the build enabled");
        }

        let _env = AutoBuildCliEnv::set(None);
        assert!(auto_build_cli_enabled(), "an unset variable leaves the build enabled");
    }
}
