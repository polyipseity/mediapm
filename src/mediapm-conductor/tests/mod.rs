//! Integration test harness for `mediapm-conductor`.
//!
//! `int` covers contract-focused integration behavior, `e2e` contains
//! multi-step workflows.

use std::collections::BTreeMap;

use mediapm_cas::InMemoryCas;
use mediapm_conductor::{
    Conductor, NickelDocument, RuntimeStoragePaths, ToolInputKind, ToolInputSpec, ToolKindSpec,
    ToolRuntime, ToolSpec, WorkflowSpec, WorkflowStepSpec, config::versions::encode_document,
};

mod e2e;
mod int;

// ---------------------------------------------------------------------------
// Shared harness helpers
// ---------------------------------------------------------------------------

/// Creates an echo@v1 `ToolSpec`.
fn echo_tool(name: &str) -> ToolSpec {
    ToolSpec {
        version: None,
        kind: ToolKindSpec::Builtin { builtin_id: "echo@v1".to_string() },
        name: name.into(),
        inputs: BTreeMap::from([(
            "text".into(),
            ToolInputSpec { kind: ToolInputKind::String, required: false },
        )]),
        default_inputs: BTreeMap::new(),
        outputs: BTreeMap::new(),
        runtime: ToolRuntime::default(),
    }
}

/// Creates a single echo `WorkflowStepSpec` (`id: "s1"`).
fn echo_step(tool_id: &str, text: &str) -> WorkflowStepSpec {
    WorkflowStepSpec {
        id: "s1".into(),
        tool: tool_id.into(),
        inputs: BTreeMap::from([("text".into(), text.into())]),
        outputs: BTreeMap::new(),
        max_retries: 0,
        depends_on: Vec::new(),
    }
}

/// Creates a flaky@v1 `ToolSpec` (test-only builtin that fails its first N
/// invocations then succeeds).
#[cfg(any(test, feature = "progress"))]
fn flaky_tool(name: &str) -> ToolSpec {
    ToolSpec {
        version: None,
        kind: ToolKindSpec::Builtin { builtin_id: "flaky@v1".to_string() },
        name: name.into(),
        inputs: BTreeMap::from([
            ("failures".into(), ToolInputSpec { kind: ToolInputKind::String, required: false }),
            ("key".into(), ToolInputSpec { kind: ToolInputKind::String, required: false }),
        ]),
        default_inputs: BTreeMap::new(),
        outputs: BTreeMap::new(),
        runtime: ToolRuntime::default(),
    }
}

/// Creates an input-less `WorkflowStepSpec` with explicit dependencies.
fn bare_step(id: &str, tool: &str, depends_on: &[&str]) -> WorkflowStepSpec {
    WorkflowStepSpec {
        id: id.into(),
        tool: tool.into(),
        inputs: BTreeMap::new(),
        outputs: BTreeMap::new(),
        max_retries: 0,
        depends_on: depends_on.iter().map(ToString::to_string).collect(),
    }
}

/// Creates a `WorkflowSpec` from raw steps.
fn workflow_with_steps(name: &str, steps: Vec<WorkflowStepSpec>) -> WorkflowSpec {
    WorkflowSpec { name: name.into(), display_name: None, description: None, impure: false, steps }
}

/// Creates a single-step echo `WorkflowSpec`.
fn echo_workflow(name: &str, tool_id: &str, text: &str) -> WorkflowSpec {
    workflow_with_steps(name, vec![echo_step(tool_id, text)])
}

/// Creates a `NickelDocument` with the given tools and workflows.
fn doc_with_workflows(
    tools: BTreeMap<String, ToolSpec>,
    workflows: Vec<WorkflowSpec>,
) -> NickelDocument {
    NickelDocument { tools, workflows, ..NickelDocument::default() }
}

/// Creates a `NickelDocument` with one echo tool and one workflow.
fn single_echo_doc(tool_id: &str, workflow_name: &str) -> NickelDocument {
    doc_with_workflows(
        BTreeMap::from([(tool_id.into(), echo_tool(tool_id))]),
        vec![echo_workflow(workflow_name, tool_id, workflow_name)],
    )
}

/// Creates a `NickelDocument` with two echo tools and two workflows
/// (distinct `tool_id` keys for independent cache entries).
fn dual_echo_doc() -> NickelDocument {
    doc_with_workflows(
        BTreeMap::from([
            ("echo-v1@v1".into(), echo_tool("echo-v1@v1")),
            ("echo-v2@v1".into(), echo_tool("echo-v2@v1")),
        ]),
        vec![
            echo_workflow("default", "echo-v1@v1", "default"),
            echo_workflow("updated", "echo-v2@v1", "updated"),
        ],
    )
}

/// Writes `doc` as the conductor config at `dir/conductor.ncl`.
fn write_conductor_config(dir: &std::path::Path, doc: NickelDocument) {
    std::fs::write(dir.join("conductor.ncl"), encode_document(doc).expect("encode"))
        .expect("write config");
}

/// A test fixture that owns a tempdir + `InMemoryCas` + `Conductor`.
///
/// The tempdir is cleaned up on drop.
struct TestConductor {
    dir: tempfile::TempDir,
    conductor: Conductor<InMemoryCas>,
}

impl TestConductor {
    fn new() -> Self {
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let conductor = Conductor::new(RuntimeStoragePaths::new(dir.path()), InMemoryCas::new());
        Self { dir, conductor }
    }

    /// Builds a conductor whose runtime tmp root is the stable
    /// `mediapm-runtime-{16hex}` path for its own tempdir (mirrors the
    /// mediapm app layer wiring in `paths.rs`), returning the runtime tmp
    /// root path for lifecycle assertions.
    fn with_runtime_tmp() -> (Self, std::path::PathBuf) {
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let runtime_tmp = mediapm_utils::temp::runtime_dir_for_workspace(dir.path());
        let mut paths = RuntimeStoragePaths::new(dir.path());
        paths.conductor_tmp_dir.clone_from(&runtime_tmp);
        let conductor = Conductor::new(paths, InMemoryCas::new());
        (Self { dir, conductor }, runtime_tmp)
    }

    fn path(&self) -> &std::path::Path {
        self.dir.path()
    }

    /// Stops the conductor and waits for its background pass to finish.
    ///
    /// A fixture owns the directory its conductor writes into, so it must
    /// quiesce that conductor before the directory goes: dropping a
    /// [`mediapm_cas::BackgroundMaintenanceGuard`] only requests the abort, and
    /// the pass that is already running would otherwise reach the state file
    /// after the directory has been removed. Consuming `self` keeps the two
    /// facts in order — a fixture cannot be read after it has been stopped.
    async fn shutdown(self) {
        self.conductor.shutdown().await.expect("conductor shutdown");
    }

    /// Stops the conductor when the test has already broken its actor client.
    ///
    /// [`Conductor::shutdown`] stops the background pass first and
    /// unconditionally, then asks the actor client to stop. A test that shrinks
    /// the RPC budget to make a run time out has already broken that second
    /// half, so its error says nothing about the fixture — but only that error
    /// is tolerated here, because the pass is what writes into the directory
    /// the fixture owns and any other failure is still a fixture problem.
    async fn shutdown_broken_client(self) {
        if let Err(error) = self.conductor.shutdown().await {
            assert!(
                error.to_string().contains("RPC timeout"),
                "only the client's own timed-out shutdown is expected here: {error}",
            );
        }
    }

    fn conductor(&self) -> &Conductor<InMemoryCas> {
        &self.conductor
    }

    /// Write a `NickelDocument` as the config for this conductor.
    fn write_config(&self, doc: NickelDocument) {
        write_conductor_config(self.dir.path(), doc);
    }
}

/// Serializes tests that read or mutate `MEDIAPM_CONDUCTOR_RPC_TIMEOUT_SECONDS`.
///
/// The knob has exactly one production read: the lazily-initialised
/// `ConductorActorClient` captures it once on first client use (any
/// `run_workflow` / `get_runtime_diagnostics` call). The coordinator reads
/// it no more — the `ExecuteStep` dispatch carries no transport timeout.
/// Tests that mutate the knob between client construction and that first
/// RPC must serialize against each other so a concurrent run cannot
/// capture the mutated value at capture time. (A `tokio::sync::Mutex` has
/// no poisoning, so a failing test cannot cascade here.)
///
/// Sibling workflow tests read the knob at client init *without* this
/// lock, so the pre-push gate (nextest, one process per test) is the safe
/// runner; `cargo test` threads mode is best-effort for these tests. The
/// lock is a `tokio::sync::Mutex` because the mutation window spans
/// `.await` points (client init and the workflow calls), which a
/// `std::sync::MutexGuard` cannot cover (`clippy::await_holding_lock`,
/// and it is not `Send`).
///
/// Note: the lib-side constant is `orchestration::config::ENV_RPC_TIMEOUT_SECONDS`,
/// which is not visible from this integration-test crate; the literal here
/// must stay in sync with it.
//
// `int::step_rpc_layering` is this module's only consumer and is compiled on
// every platform, so these items must be too; a gate here would leave that
// module with an unresolved import wherever the gate applied.
pub(crate) async fn rpc_env_lock() -> tokio::sync::MutexGuard<'static, ()> {
    static LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    LOCK.lock().await
}

/// RAII guard for `MEDIAPM_CONDUCTOR_RPC_TIMEOUT_SECONDS`: applies one
/// mutation and restores the prior state (value or absence) on drop, so a
/// panicking test cannot leak the mutated budget into sibling tests.
///
/// Hold [`rpc_env_lock`] for the guard's whole lifetime — `set_var` is not
/// thread-safe and sibling tests construct nodes that capture this value.
// Compiled on every platform, like its only consumer; see [`rpc_env_lock`].
pub(crate) struct RpcEnvGuard {
    /// Value the variable held before the guard mutated it, if any.
    previous: Option<String>,
}

impl RpcEnvGuard {
    /// Removes the variable for the guard's lifetime (the lib falls back to
    /// its default budget while unset).
    ///
    /// # Safety
    ///
    /// Caller must hold [`rpc_env_lock`].
    pub(crate) fn remove() -> Self {
        let previous = std::env::var("MEDIAPM_CONDUCTOR_RPC_TIMEOUT_SECONDS").ok();
        // SAFETY: caller holds rpc_env_lock; see struct docs.
        unsafe { std::env::remove_var("MEDIAPM_CONDUCTOR_RPC_TIMEOUT_SECONDS") };
        Self { previous }
    }

    /// Sets the variable to `seconds` for the guard's lifetime.
    ///
    /// # Safety
    ///
    /// Caller must hold [`rpc_env_lock`].
    pub(crate) fn set_seconds(seconds: &str) -> Self {
        let previous = std::env::var("MEDIAPM_CONDUCTOR_RPC_TIMEOUT_SECONDS").ok();
        // SAFETY: caller holds rpc_env_lock; see struct docs.
        unsafe { std::env::set_var("MEDIAPM_CONDUCTOR_RPC_TIMEOUT_SECONDS", seconds) };
        Self { previous }
    }
}

impl Drop for RpcEnvGuard {
    fn drop(&mut self) {
        // SAFETY: caller holds rpc_env_lock for the guard's lifetime; see struct docs.
        unsafe {
            match &self.previous {
                Some(value) => std::env::set_var("MEDIAPM_CONDUCTOR_RPC_TIMEOUT_SECONDS", value),
                None => std::env::remove_var("MEDIAPM_CONDUCTOR_RPC_TIMEOUT_SECONDS"),
            }
        }
    }
}
