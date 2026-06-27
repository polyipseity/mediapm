//! External process execution and builtin tool dispatching.

use std::collections::BTreeMap;
use std::path::Path;
use std::process::Stdio;

use tokio::process::{Child, Command};
use tokio::time::timeout;

use super::executable_timeout;
use crate::error::ConductorError;

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
pub(super) async fn run_executable_process(
    command_parts: &[String],
    _success_codes: &[i32],
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

    timeout(timeout_duration, collect_child_output(child)).await.map_err(|_| {
        ConductorError::Workflow(format!(
            "executable process '{command}' timed out after {timeout_duration:?}",
        ))
    })?
}

/// Collects stdout, stderr, and exit code from a spawned child process.
async fn collect_child_output(mut child: Child) -> Result<ExecutionResult, ConductorError> {
    use tokio::io::AsyncReadExt;

    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    if let Some(mut out) = child.stdout.take() {
        out.read_to_end(&mut stdout)
            .await
            .map_err(|source| ConductorError::io("read child stdout", "stdout", source))?;
    }
    if let Some(mut err) = child.stderr.take() {
        err.read_to_end(&mut stderr)
            .await
            .map_err(|source| ConductorError::io("read child stderr", "stderr", source))?;
    }

    let status = child
        .wait()
        .await
        .map_err(|source| ConductorError::io("wait for child process", "child", source))?;

    let exit_code = status.code().unwrap_or(-1);

    if !status.success() {
        let stderr_str = String::from_utf8_lossy(&stderr);
        tracing::warn!("process exited with code {exit_code}: {stderr_str}",);
    }

    Ok(ExecutionResult { stdout, stderr, exit_code })
}

/// Runs a builtin tool by dispatching to the appropriate handler through the
/// [`ALL_BUILTINS`](crate::tools::ALL_BUILTINS) registry.
pub(super) async fn run_builtin(
    tool_name: &str,
    args: &BTreeMap<String, String>,
    outermost_config_dir: &Path,
    sandbox_dir: &Path,
) -> Result<ExecutionResult, ConductorError> {
    let _registration = crate::tools::find_builtin(tool_name).ok_or_else(|| {
        ConductorError::Workflow(format!(
            "unknown builtin tool '{tool_name}' (not in tool registry)"
        ))
    })?;

    let params = args; // treat all resolved step inputs as params

    match tool_name {
        "echo" => {
            let result = mediapm_conductor_builtin_echo::execute(params, &BTreeMap::new())
                .map_err(|e| ConductorError::Workflow(format!("echo builtin failed: {e}")))?;
            // Serialize the StringMap to JSON stdout.
            let stdout = serde_json::to_vec(&result)
                .map_err(|e| ConductorError::Workflow(format!("echo serialization failed: {e}")))?;
            Ok(ExecutionResult { stdout, stderr: Vec::new(), exit_code: 0 })
        }
        "fs" => {
            mediapm_conductor_builtin_fs::execute_string_map(sandbox_dir, params, &BTreeMap::new())
                .map_err(|e| ConductorError::Workflow(format!("fs builtin failed: {e}")))?;
            // fs impure — success payload is the filesystem side effect.
            Ok(ExecutionResult { stdout: Vec::new(), stderr: Vec::new(), exit_code: 0 })
        }
        "import" => {
            let bytes = mediapm_conductor_builtin_import::execute_content_map(
                outermost_config_dir,
                params,
                &BTreeMap::new(),
            )
            .map_err(|e| ConductorError::Workflow(format!("import builtin failed: {e}")))?;
            Ok(ExecutionResult { stdout: bytes, stderr: Vec::new(), exit_code: 0 })
        }
        "archive" => {
            let bytes = mediapm_conductor_builtin_archive::execute_content_map(
                params,
                &BTreeMap::<String, Vec<u8>>::new(),
            )
            .map_err(|e| ConductorError::Workflow(format!("archive builtin failed: {e}")))?;
            Ok(ExecutionResult { stdout: bytes, stderr: Vec::new(), exit_code: 0 })
        }
        "export" => {
            let result = mediapm_conductor_builtin_export::execute_string_map(
                outermost_config_dir,
                params,
                &BTreeMap::<String, Vec<u8>>::new(),
            )
            .map_err(|e| ConductorError::Workflow(format!("export builtin failed: {e}")))?;
            let stdout = serde_json::to_vec(&result).map_err(|e| {
                ConductorError::Workflow(format!("export serialization failed: {e}"))
            })?;
            Ok(ExecutionResult { stdout, stderr: Vec::new(), exit_code: 0 })
        }
        name => Err(ConductorError::Workflow(format!(
            "builtin tool '{name}' has no execute implementation"
        ))),
    }
}
