//! External process execution and builtin tool dispatching.

use std::collections::BTreeMap;
use std::path::Path;
use std::process::Stdio;

use mediapm_cas::{CasApi, Hash};
use tokio::process::{Child, Command};
use tokio::time::timeout;

use super::{EXECUTABLE_TIMEOUT_ENV_VAR, executable_timeout};
use crate::error::ConductorError;

/// Test-only builtin that fails its first `failures` invocations (keyed by
/// `key`) then succeeds. Used to exercise retry-to-success paths.
#[cfg(any(test, feature = "progress"))]
const FLAKY_BUILTIN_ID: &str = "flaky@v1";

/// Test-only builtin that records `start`/`finish` events to a log file and
/// optionally holds until the log contains an awaited line. Used by the
/// ready-queue scheduler tests to observe dispatch order as schedule facts
/// rather than timing guesses.
#[cfg(any(test, feature = "progress"))]
const SLOW_BUILTIN_ID: &str = "slow@v1";

/// Poll interval while a `slow@v1` invocation holds for an awaited line.
#[cfg(any(test, feature = "progress"))]
const SLOW_HOLD_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(10);

/// Fallback cap (milliseconds) on a `slow@v1` hold when the step declares
/// no `hold_timeout_ms` input. Bounds the wait so a run whose schedule never
/// produces the awaited line fails instead of hanging.
#[cfg(any(test, feature = "progress"))]
const SLOW_HOLD_TIMEOUT_DEFAULT_MS: u64 = 5_000;

/// Per-key attempt counter for [`FLAKY_BUILTIN_ID`].
#[cfg(any(test, feature = "progress"))]
static FLAKY_ATTEMPTS: std::sync::Mutex<std::collections::BTreeMap<String, usize>> =
    std::sync::Mutex::new(std::collections::BTreeMap::new());

/// Result of executing a tool process.
#[derive(Debug, Clone, Default)]
pub(super) struct ExecutionResult {
    /// Captured stdout bytes.
    pub(crate) stdout: Vec<u8>,
    /// Captured stderr bytes.
    pub(crate) stderr: Vec<u8>,
    /// Process exit code.
    pub(crate) exit_code: i32,
}

/// Runs an executable process with the given inputs and env vars.
///
/// The child is spawned with piped stdout/stderr and `kill_on_drop`: on a
/// watchdog timeout the surrounding future is dropped, which terminates the
/// process, while the caller-owned capture buffers survive for diagnostics.
///
/// # Errors
///
/// Returns [`ConductorError::Io`] when the process cannot be spawned, a
/// captured stream cannot be read, or the child cannot be waited on, and
/// [`ConductorError::Workflow`] when the watchdog times the process out or
/// its exit code is not in `success_codes`. A timeout error carries the
/// captured output tails (last 2048 bytes of each stream) so a killed step
/// still reports what its child wrote before the kill.
pub(super) async fn run_executable_process(
    command_parts: &[String],
    success_codes: &[i32],
    sandbox_dir: &Path,
    execution_env_vars: &BTreeMap<String, String>,
) -> Result<ExecutionResult, ConductorError> {
    let timeout_duration = executable_timeout();

    let Some((command, args)) = command_parts.split_first() else {
        return Err(ConductorError::Workflow(
            "executable process called with empty command_parts".to_string(),
        ));
    };

    let mut cmd = Command::new(command);
    cmd.args(args);
    cmd.current_dir(sandbox_dir);
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());
    cmd.kill_on_drop(true);

    // Apply inherited env vars: non-empty values are set directly, empty
    // values inherit the host process value.
    for (key, value) in execution_env_vars {
        if !value.is_empty() {
            cmd.env(key, value);
        } else if let Ok(host_val) = std::env::var(key) {
            cmd.env(key, host_val);
        }
    }

    let child = cmd
        .spawn()
        .map_err(|source| ConductorError::io("spawn executable process", command, source))?;

    // Buffers live outside the timed future: a timeout drops the future (and
    // with it the child, killed via `kill_on_drop`), but bytes the child
    // wrote before the kill stay readable for the timeout error below.
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    let result = timeout(timeout_duration, collect_child_output(child, &mut stdout, &mut stderr))
        .await
        .map_err(|_| {
            ConductorError::Workflow(format!(
                "executable process '{command}' timed out after {timeout_duration:?} \
             ({EXECUTABLE_TIMEOUT_ENV_VAR}); stdout tail (last 2048 bytes): {}; \
             stderr tail (last 2048 bytes): {}",
                output_tail(&stdout),
                output_tail(&stderr),
            ))
        })??;

    let exit_code = result;
    if !success_codes.is_empty() && !success_codes.contains(&exit_code) {
        let stderr_str = String::from_utf8_lossy(&stderr);
        return Err(ConductorError::Workflow(format!(
            "executable process '{command}' exited with code {exit_code}: {stderr_str}",
        )));
    }

    Ok(ExecutionResult { stdout, stderr, exit_code })
}

/// Renders the last 2048 bytes of one captured output stream for timeout
/// diagnostics; an empty buffer renders as `(empty)`.
fn output_tail(bytes: &[u8]) -> String {
    const TAIL_BYTES: usize = 2048;
    if bytes.is_empty() {
        return "(empty)".to_string();
    }
    String::from_utf8_lossy(&bytes[bytes.len() - TAIL_BYTES.min(bytes.len())..]).into_owned()
}

/// Collects stdout, stderr, and exit code from a spawned child process into
/// caller-owned buffers, which outlive this future on a watchdog timeout.
///
/// Both streams are drained concurrently: a child blocked writing one stream
/// while the parent reads the other must not wedge the run, and a watchdog
/// timeout must leave both buffers populated for the timeout diagnostic.
async fn collect_child_output(
    mut child: Child,
    stdout: &mut Vec<u8>,
    stderr: &mut Vec<u8>,
) -> Result<i32, ConductorError> {
    use tokio::io::AsyncReadExt;

    let mut out = child.stdout.take();
    let mut err = child.stderr.take();

    let drain_stdout = async {
        match out.as_mut() {
            Some(reader) => reader
                .read_to_end(stdout)
                .await
                .map(|_| ())
                .map_err(|source| ConductorError::io("read child stdout", "stdout", source)),
            None => Ok(()),
        }
    };
    let drain_stderr = async {
        match err.as_mut() {
            Some(reader) => reader
                .read_to_end(stderr)
                .await
                .map(|_| ())
                .map_err(|source| ConductorError::io("read child stderr", "stderr", source)),
            None => Ok(()),
        }
    };
    tokio::try_join!(drain_stdout, drain_stderr)?;

    let status = child
        .wait()
        .await
        .map_err(|source| ConductorError::io("wait for child process", "child", source))?;

    let exit_code = status.code().unwrap_or(-1);

    if !status.success() {
        let stderr_str = String::from_utf8_lossy(stderr);
        tracing::warn!("process exited with code {exit_code}: {stderr_str}",);
    }

    Ok(exit_code)
}

/// Runs a builtin tool by dispatching to the appropriate handler through the
/// [`ALL_BUILTINS`](crate::tools::ALL_BUILTINS) registry.
///
/// `cas` backs hash-valued builtin inputs (e.g. import `kind=cas_hash`);
/// impure builtins that ingest pre-existing content resolve their hashes
/// against the shared store rather than materializing sandbox copies.
pub(super) async fn run_builtin<C: CasApi + Send + Sync>(
    cas: &C,
    tool_name: &str,
    args: &BTreeMap<String, String>,
    outermost_config_dir: &Path,
    sandbox_dir: &Path,
) -> Result<ExecutionResult, ConductorError> {
    match tool_name {
        n if n == mediapm_conductor_builtin_echo::TOOL_BUILTIN_ID => {
            let result = mediapm_conductor_builtin_echo::execute(args, &BTreeMap::new())
                .map_err(|e| ConductorError::Workflow(format!("echo builtin failed: {e}")))?;
            // Serialize the StringMap to JSON stdout.
            let stdout = serde_json::to_vec(&result)
                .map_err(|e| ConductorError::Workflow(format!("echo serialization failed: {e}")))?;
            Ok(ExecutionResult { stdout, stderr: Vec::new(), exit_code: 0 })
        }
        n if n == mediapm_conductor_builtin_fs::TOOL_BUILTIN_ID => {
            mediapm_conductor_builtin_fs::execute_string_map(sandbox_dir, args, &BTreeMap::new())
                .map_err(|e| ConductorError::Workflow(format!("fs builtin failed: {e}")))?;
            // fs impure — success payload is the filesystem side effect.
            Ok(ExecutionResult { stdout: Vec::new(), stderr: Vec::new(), exit_code: 0 })
        }
        n if n == mediapm_conductor_builtin_import::TOOL_BUILTIN_ID => {
            let bytes = if args.get("kind").map(String::as_str) == Some("cas_hash") {
                // Resolve the CAS payload up front (the builtin resolver is
                // synchronous), then hand the fetched bytes to the builtin so
                // its argument/format validation still runs.
                let hash_text = args.get("hash").ok_or_else(|| {
                    ConductorError::Workflow("import kind='cas_hash' requires 'hash'".to_string())
                })?;
                let hash = hash_text.parse::<Hash>().map_err(|e| {
                    ConductorError::Workflow(format!(
                        "import kind='cas_hash' invalid hash '{hash_text}': {e}"
                    ))
                })?;
                let content = cas
                    .get(hash)
                    .await
                    .map_err(|e| {
                        ConductorError::Workflow(format!(
                            "import kind='cas_hash' CAS read failed: {e}"
                        ))
                    })?
                    .to_vec();
                mediapm_conductor_builtin_import::execute_content_map_with_hash_resolver(
                    outermost_config_dir,
                    args,
                    &BTreeMap::new(),
                    |_hash_text| Ok(content.clone()),
                )
                .map_err(|e| ConductorError::Workflow(format!("import builtin failed: {e}")))?
            } else {
                mediapm_conductor_builtin_import::execute_content_map(
                    outermost_config_dir,
                    args,
                    &BTreeMap::new(),
                )
                .map_err(|e| ConductorError::Workflow(format!("import builtin failed: {e}")))?
            };
            Ok(ExecutionResult { stdout: bytes, stderr: Vec::new(), exit_code: 0 })
        }
        n if n == mediapm_conductor_builtin_archive::TOOL_BUILTIN_ID => {
            let bytes = mediapm_conductor_builtin_archive::execute_content_map(
                args,
                &BTreeMap::<String, Vec<u8>>::new(),
            )
            .map_err(|e| ConductorError::Workflow(format!("archive builtin failed: {e}")))?;
            Ok(ExecutionResult { stdout: bytes, stderr: Vec::new(), exit_code: 0 })
        }
        n if n == mediapm_conductor_builtin_export::TOOL_BUILTIN_ID => {
            let result = mediapm_conductor_builtin_export::execute_string_map(
                outermost_config_dir,
                args,
                &BTreeMap::<String, Vec<u8>>::new(),
            )
            .map_err(|e| ConductorError::Workflow(format!("export builtin failed: {e}")))?;
            let stdout = serde_json::to_vec(&result).map_err(|e| {
                ConductorError::Workflow(format!("export serialization failed: {e}"))
            })?;
            Ok(ExecutionResult { stdout, stderr: Vec::new(), exit_code: 0 })
        }
        #[cfg(any(test, feature = "progress"))]
        n if n == FLAKY_BUILTIN_ID => {
            let failures: usize = args.get("failures").and_then(|s| s.parse().ok()).unwrap_or(0);
            let key = args.get("key").cloned().unwrap_or_else(|| "default".to_string());
            let attempt = {
                let mut map = FLAKY_ATTEMPTS.lock().expect("flaky lock");
                let entry = map.entry(key).or_insert(0);
                *entry += 1;
                *entry
            };
            if attempt <= failures {
                return Err(ConductorError::Workflow(format!(
                    "flaky builtin failed on attempt {attempt} of {failures}"
                )));
            }
            let stdout = serde_json::to_vec(&mediapm_utils::StringMap::from([
                ("stdout".to_string(), "ok".to_string()),
                ("stderr".to_string(), String::new()),
            ]))
            .map_err(|e| ConductorError::Workflow(format!("flaky serialization failed: {e}")))?;
            Ok(ExecutionResult { stdout, stderr: Vec::new(), exit_code: 0 })
        }
        #[cfg(any(test, feature = "progress"))]
        n if n == SLOW_BUILTIN_ID => run_slow_builtin(args).await,
        name => Err(ConductorError::Workflow(format!(
            "builtin tool '{name}' has no execute implementation"
        ))),
    }
}

/// Runs the `slow@v1` test builtin: appends `start <label>` to the log file
/// named by the `log` argument, optionally waits until the log contains the
/// `hold_until` line, then appends `finish <label>`.
///
/// The hold is what lets scheduler tests assert cross-branch ordering
/// deterministically: a held step can only finish after the awaited line was
/// appended, so "the held step finished after X started" is a schedule fact,
/// not a wall-clock race. The wait is bounded by `hold_timeout_ms` (default
/// [`SLOW_HOLD_TIMEOUT_DEFAULT_MS`]) so a schedule that never produces the
/// line fails the step instead of deadlocking the run.
///
/// # Errors
///
/// Returns [`ConductorError::Workflow`] when `log` or `label` is missing or
/// empty, when the log file cannot be opened or written, or when the hold
/// times out waiting for the awaited line.
#[cfg(any(test, feature = "progress"))]
async fn run_slow_builtin(
    args: &BTreeMap<String, String>,
) -> Result<ExecutionResult, ConductorError> {
    use std::io::Write as _;

    let log_path = args.get("log").filter(|path| !path.is_empty()).ok_or_else(|| {
        ConductorError::Workflow("slow builtin requires a non-empty 'log' path".to_string())
    })?;
    let label = args.get("label").filter(|label| !label.is_empty()).ok_or_else(|| {
        ConductorError::Workflow("slow builtin requires a non-empty 'label'".to_string())
    })?;
    let mut log =
        std::fs::OpenOptions::new().create(true).append(true).open(log_path).map_err(|e| {
            ConductorError::Workflow(format!("slow builtin could not open log '{log_path}': {e}"))
        })?;

    writeln!(log, "start {label}").map_err(|e| {
        ConductorError::Workflow(format!("slow builtin could not write to log '{log_path}': {e}"))
    })?;

    if let Some(hold_until) = args.get("hold_until").filter(|line| !line.is_empty()) {
        let timeout_ms: u64 = args
            .get("hold_timeout_ms")
            .and_then(|value| value.parse().ok())
            .unwrap_or(SLOW_HOLD_TIMEOUT_DEFAULT_MS);
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(timeout_ms);
        loop {
            // A transient read error or a line still being appended just
            // defers to the next poll; the deadline below is the failure
            // authority, so nothing is masked here.
            if let Ok(contents) = std::fs::read_to_string(log_path)
                && contents.lines().any(|line| line == hold_until)
            {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(ConductorError::Workflow(format!(
                    "slow builtin '{label}' timed out after {timeout_ms}ms waiting for \
                     '{hold_until}' in '{log_path}'"
                )));
            }
            tokio::time::sleep(SLOW_HOLD_POLL_INTERVAL).await;
        }
    }

    writeln!(log, "finish {label}").map_err(|e| {
        ConductorError::Workflow(format!("slow builtin could not write to log '{log_path}': {e}"))
    })?;
    Ok(ExecutionResult::default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestration::step_worker::EXECUTABLE_TIMEOUT_ENV_VAR;

    /// RAII guard that overrides [`EXECUTABLE_TIMEOUT_ENV_VAR`] for one test
    /// and restores the prior value (or removes the variable) on drop, so a
    /// panicking assertion cannot leak the override into sibling tests.
    struct ExecutableTimeoutOverride(Option<String>);

    impl ExecutableTimeoutOverride {
        /// Sets the executable-timeout env override to `secs` for this guard's lifetime.
        fn set(secs: u64) -> Self {
            let prior = std::env::var(EXECUTABLE_TIMEOUT_ENV_VAR).ok();
            // Safety: env mutation is process-global; the guard restores the
            // prior value on drop, and tests relying on this variable are
            // serialized by the per-test process boundary under nextest.
            unsafe { std::env::set_var(EXECUTABLE_TIMEOUT_ENV_VAR, secs.to_string()) };
            Self(prior)
        }
    }

    impl Drop for ExecutableTimeoutOverride {
        fn drop(&mut self) {
            // Safety: mirrors `set`; restores the pre-test env state.
            unsafe {
                match &self.0 {
                    Some(value) => std::env::set_var(EXECUTABLE_TIMEOUT_ENV_VAR, value),
                    None => std::env::remove_var(EXECUTABLE_TIMEOUT_ENV_VAR),
                }
            }
        }
    }

    /// Protects the guarantee that a watchdog-killed executable keeps its
    /// captured stdout/stderr: the error message must carry both output
    /// tails, because that message is the only evidence surviving the kill
    /// (the demo's stalled media-tagger step lost everything written by the
    /// child when the timeout dropped the buffers).
    #[cfg(unix)]
    #[tokio::test]
    async fn executable_timeout_error_carries_captured_output_tails() {
        let _timeout_override = ExecutableTimeoutOverride::set(1);
        let sandbox_dir = std::env::current_dir().expect("current dir");
        let command: Vec<String> =
            ["sh", "-c", "echo marker-stdout; echo marker-stderr >&2; sleep 30"]
                .iter()
                .map(|part| (*part).to_string())
                .collect();

        let error =
            run_executable_process(&command, &[], &sandbox_dir, &BTreeMap::<String, String>::new())
                .await
                .expect_err("watchdog timeout must fail the step");

        let message = error.to_string();
        assert!(message.contains("timed out"), "timeout wording missing: {message}");
        assert!(message.contains("marker-stdout"), "stdout tail missing: {message}");
        assert!(message.contains("marker-stderr"), "stderr tail missing: {message}");
    }
}
