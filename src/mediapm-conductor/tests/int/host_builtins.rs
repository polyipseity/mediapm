//! # Host-supplied builtin runners, and where an unregistered id is refused
//!
//! A conductor build ships a fixed set of builtin tools, and until now that was
//! the whole set: a tool that lived in the host program had to be reached by
//! spawning an executable, because the dispatcher only knew the ids compiled
//! into the conductor. A host now registers its own runners in
//! [`HostBuiltins`] and passes them to the run, and a step whose tool declares
//! no command parts executes in-process through
//! [`HostBuiltin`](mediapm_conductor::tools::HostBuiltin) exactly as it would
//! through a shipped builtin.
//!
//! Two things are asserted here, both through the [`Conductor`] facade rather
//! than the internal dispatch, because the option has to reach the executor for
//! either to be true:
//!
//! - the runner's stdout is what the step captures, so the seam carries bytes
//!   and not merely a call, and the runner is handed the step's resolved inputs
//!   and the run's content store;
//! - an id no runner supplies fails the run **before its first step**, with a
//!   message naming the id and the registration that is missing. The workflow
//!   puts an observable step ahead of it — a test-only builtin that appends to
//!   an event log the moment it runs — so an absent log is positive evidence
//!   that nothing executed, rather than the absence of an assertion.
//!
//! The static builtins are untouched by any of this: a host id may not shadow
//! one (refused at registration, unit-tested beside the registry), so a run
//! that supplies a registry still dispatches `echo@v1` and friends to the arms
//! compiled into the build.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use mediapm_cas::{CasApi, Hash};
use mediapm_conductor::api::RunWorkflowOptions;
use mediapm_conductor::tools::{HostBuiltin, HostBuiltinOutput, HostBuiltinRequest, HostBuiltins};
use mediapm_conductor::{
    ConductorError, OutputCaptureSpec, ToolInputKind, ToolInputSpec, ToolKindSpec, ToolRuntime,
    ToolSpec, WorkflowStepSpec,
};

use crate::{TestConductor, doc_with_workflows, workflow_with_steps};

/// The builtin id the host runner in these tests answers for.
///
/// Spelled like a shipped builtin id and deliberately not one: it is what a
/// document's `builtin` spec names, and nothing in this build supplies it.
const HOST_BUILTIN_ID: &str = "retag@v1";

/// The id the refusal test registers instead of [`HOST_BUILTIN_ID`], so the
/// check is proven to compare ids rather than merely notice a registry exists.
const OTHER_HOST_BUILTIN_ID: &str = "other@v1";

/// An id the refusal test's document names on a tool that **no step of any
/// workflow uses**.
///
/// The check is document-wide, so a tool the selected workflow never dispatches
/// still has no implementation if its id is unregistered, and the run refuses it
/// before its first step. Naming it only in the catalog is what distinguishes
/// this from a check scoped to the tools the run is about to use.
const UNUSED_HOST_BUILTIN_ID: &str = "orphan@v1";

/// What the host runner returns as its stdout.
const HOST_STDOUT: &[u8] = b"tagged-by-the-host-runner";

/// A host runner that reads one seeded blob through the seam's content store.
///
/// Reading the store is the point: it proves the handle a host builtin gets is
/// the run's store and not an empty stand-in, and it gives the test bytes that
/// could not have come from anywhere except the runner.
#[derive(Debug)]
struct SeededBlobRunner {
    /// The builtin id this runner answers for.
    id: &'static str,
    /// Hash of the blob the test seeded before the run.
    seeded: Hash,
    /// The step inputs the runner was handed, recorded once.
    seen_args: Mutex<Option<BTreeMap<String, String>>>,
    /// How many times the runner reached its work.
    runs: AtomicUsize,
}

impl SeededBlobRunner {
    /// A runner for `id` returning the bytes stored under `seeded`.
    fn new(id: &'static str, seeded: Hash) -> Self {
        Self { id, seeded, seen_args: Mutex::new(None), runs: AtomicUsize::new(0) }
    }

    /// The inputs the runner was handed, or a panic when it never ran.
    fn seen_args(&self) -> BTreeMap<String, String> {
        self.seen_args
            .lock()
            .expect("the args slot is only written by the runner")
            .clone()
            .expect("the runner must have been dispatched")
    }

    /// How many times the runner ran.
    fn runs(&self) -> usize {
        self.runs.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl HostBuiltin for SeededBlobRunner {
    fn id(&self) -> &str {
        self.id
    }

    async fn run(
        &self,
        request: HostBuiltinRequest<'_>,
    ) -> Result<HostBuiltinOutput, ConductorError> {
        let bytes = request.cas.get(self.seeded).await.map_err(|error| {
            ConductorError::Workflow(format!("host runner could not read its seeded blob: {error}"))
        })?;
        *self.seen_args.lock().expect("the args slot is only written by the runner") =
            Some(request.args.clone());
        self.runs.fetch_add(1, Ordering::SeqCst);
        Ok(HostBuiltinOutput { stdout: bytes.to_vec(), stderr: Vec::new(), exit_code: 0 })
    }
}

/// A `builtin` tool spec naming `builtin_id`, declaring one string input.
///
/// Declaring no command parts is what makes a step with this tool dispatch
/// in-process; the id is what decides which runner receives it. The spec's
/// `name` is the map key the steps reference, which is how a step names a tool.
fn host_tool(builtin_id: &str) -> ToolSpec {
    ToolSpec {
        version: None,
        kind: ToolKindSpec::Builtin { builtin_id: builtin_id.to_string() },
        name: builtin_id.to_string(),
        inputs: BTreeMap::from([(
            "text".to_string(),
            ToolInputSpec { kind: ToolInputKind::String, required: false },
        )]),
        default_inputs: BTreeMap::new(),
        outputs: BTreeMap::new(),
        runtime: ToolRuntime::default(),
    }
}

/// A step running `tool` with one input, capturing its stdout as `result`.
fn capturing_step(id: &str, tool: &str) -> WorkflowStepSpec {
    WorkflowStepSpec {
        id: id.to_string(),
        tool: tool.to_string(),
        inputs: BTreeMap::from([("text".to_string(), "hi".to_string())]),
        outputs: BTreeMap::from([(
            "result".to_string(),
            OutputCaptureSpec {
                name: "result".to_string(),
                capture: "stdout".to_string(),
                ..OutputCaptureSpec::default()
            },
        )]),
        max_retries: 0,
        depends_on: Vec::new(),
    }
}

/// The test-only `slow@v1` tool: it appends to an event log when it runs.
fn slow_tool() -> ToolSpec {
    ToolSpec {
        version: None,
        kind: ToolKindSpec::Builtin { builtin_id: "slow@v1".to_string() },
        name: "slow".to_string(),
        inputs: BTreeMap::from([
            ("log".to_string(), ToolInputSpec { kind: ToolInputKind::String, required: true }),
            ("label".to_string(), ToolInputSpec { kind: ToolInputKind::String, required: true }),
        ]),
        default_inputs: BTreeMap::new(),
        outputs: BTreeMap::new(),
        runtime: ToolRuntime::default(),
    }
}

/// A `slow@v1` step that appends `start <label>` to `log` when it runs.
fn slow_step(id: &str, log: &Path) -> WorkflowStepSpec {
    WorkflowStepSpec {
        id: id.to_string(),
        tool: "slow".to_string(),
        inputs: BTreeMap::from([
            ("log".to_string(), log.display().to_string()),
            ("label".to_string(), id.to_string()),
        ]),
        outputs: BTreeMap::new(),
        max_retries: 0,
        depends_on: Vec::new(),
    }
}

/// Returns the bytes the run recorded for the first output named `name`.
async fn recorded_output_bytes(tc: &TestConductor, name: &str) -> Vec<u8> {
    let state = tc.conductor().get_state().expect("the conductor state must be readable");
    let hash = state
        .tool_call_instances
        .values()
        .find_map(|instance| instance.outputs.get(name).map(|record| record.hash))
        .unwrap_or_else(|| panic!("no instance recorded an output named '{name}'"));
    tc.conductor()
        .cas()
        .get(hash)
        .await
        .expect("a recorded output's bytes must be in the store")
        .to_vec()
}

/// A step whose tool declares no command parts runs the host's runner, and the
/// runner's stdout is what the step captures.
///
/// Both halves matter. The captured bytes came from the content store through
/// the handle the seam hands the runner, so a dispatch that never reached the
/// runner — or reached it with an empty store — has nothing to capture; and the
/// recorded inputs are the step's resolved ones, so the runner is given the
/// same arguments a shipped builtin would get.
#[tokio::test]
async fn strict_a_no_command_step_runs_the_host_runner_and_captures_its_stdout() {
    let tc = TestConductor::new();
    let seeded = tc
        .conductor()
        .cas()
        .put(Bytes::from_static(HOST_STDOUT))
        .await
        .expect("seed the blob the host runner returns");
    let runner = Arc::new(SeededBlobRunner::new(HOST_BUILTIN_ID, seeded));

    tc.write_config(doc_with_workflows(
        BTreeMap::from([(HOST_BUILTIN_ID.to_string(), host_tool(HOST_BUILTIN_ID))]),
        vec![workflow_with_steps("default", vec![capturing_step("tag", HOST_BUILTIN_ID)])],
    ));

    let options = RunWorkflowOptions {
        host_builtins: HostBuiltins::new(vec![runner.clone() as Arc<dyn HostBuiltin>])
            .expect("one runner, one id"),
        ..RunWorkflowOptions::default()
    };
    let summary = tc
        .conductor()
        .run_workflow("default", options)
        .await
        .expect("a registered host builtin must run in-process");

    assert_eq!(
        (summary.total_steps, summary.executed_steps, summary.failed_steps),
        (1, 1, 0),
        "the step must execute through the host runner and succeed: {summary:?}"
    );
    assert_eq!(runner.runs(), 1, "the step must reach the host runner exactly once");
    assert_eq!(
        runner.seen_args().get("text").map(String::as_str),
        Some("hi"),
        "the runner must be handed the step's resolved inputs",
    );
    assert_eq!(
        recorded_output_bytes(&tc, "result").await,
        HOST_STDOUT,
        "what the runner wrote as stdout must be what the step captured",
    );
    tc.shutdown().await;
}

/// A document naming a builtin id no runner supplies fails the run before any
/// step runs, and the message names the id and the missing registration.
///
/// The workflow puts an observable root step ahead of the offending one, so the
/// assertion is a fact about execution rather than about the error alone: that
/// step appends to an event log the moment it runs, and the log stays absent.
/// The registry supplies a *different* id, which is what distinguishes a check
/// that compares ids from one that merely notices a host registry exists.
///
/// One of the document's tools is named by no step of any workflow and its id
/// is one no runner supplies either: the refusal names it too, which is the
/// document-wide half of the property — a check scoped to the selected
/// workflow's tools would let the run start.
#[tokio::test]
async fn strict_an_unregistered_host_builtin_fails_the_run_before_any_step() {
    let tc = TestConductor::new();
    let events = tc.path().join("events.log");
    tc.write_config(doc_with_workflows(
        BTreeMap::from([
            ("slow".to_string(), slow_tool()),
            (HOST_BUILTIN_ID.to_string(), host_tool(HOST_BUILTIN_ID)),
            (UNUSED_HOST_BUILTIN_ID.to_string(), host_tool(UNUSED_HOST_BUILTIN_ID)),
        ]),
        vec![workflow_with_steps(
            "default",
            vec![slow_step("ahead", &events), capturing_step("unknown", HOST_BUILTIN_ID)],
        )],
    ));

    let other =
        Arc::new(SeededBlobRunner::new(OTHER_HOST_BUILTIN_ID, Hash::from_content(b"unused")));
    let options = RunWorkflowOptions {
        host_builtins: HostBuiltins::new(vec![other as Arc<dyn HostBuiltin>])
            .expect("one runner, one id"),
        ..RunWorkflowOptions::default()
    };
    let error = tc
        .conductor()
        .run_workflow("default", options)
        .await
        .expect_err("an id no runner supplies cannot be dispatched");

    let message = error.to_string();
    assert!(
        message.contains(HOST_BUILTIN_ID),
        "the error must name the id the document used: {message}"
    );
    assert!(
        message.contains("register"),
        "the error must say the host has to register a runner for it: {message}"
    );
    assert!(
        message.contains(UNUSED_HOST_BUILTIN_ID),
        "the check covers every workflow's tools, so the error must name the id only the catalog \
         uses: {message}"
    );
    assert!(
        !events.exists(),
        "no step may run before the run refuses the unregistered id; the event log holds {:?}",
        std::fs::read_to_string(&events),
    );
    tc.shutdown().await;
}

/// The `mediapm-conductor` binary registers no host builtins, so a document
/// naming one is refused at run start with the sentence its CLI docs state.
///
/// `media-tagger` is the live case: it is the id mediapm's own host supplies a
/// runner for, so a document may legitimately name it, and this binary is the
/// one that cannot dispatch it. The empty registry is what the binary passes,
/// and the whole sentence is asserted rather than a substring, because the
/// sentence is the user-facing contract. The recording progress group is the
/// second half: the refusal precedes the first step, so the group records no
/// step bar — only the overall row the caller created before the run.
#[cfg(feature = "progress")]
#[tokio::test]
async fn strict_a_document_naming_media_tagger_is_refused_at_run_start() {
    use mediapm_utils::progress::recording::{ProgressOp, RecordingProgressTracker};

    let tc = TestConductor::new();
    tc.write_config(doc_with_workflows(
        BTreeMap::from([("media-tagger".to_string(), host_tool("media-tagger"))]),
        vec![workflow_with_steps("default", vec![capturing_step("tag", "media-tagger")])],
    ));

    let (tracker, overall) = RecordingProgressTracker::with_overall("workflow", 1);
    let error = tc
        .conductor()
        .run_workflow(
            "default",
            RunWorkflowOptions {
                host_builtins: HostBuiltins::default(),
                progress_group: Some(Arc::new(tracker.clone())),
                overall_bar: Some(Arc::new(overall)),
                ..RunWorkflowOptions::default()
            },
        )
        .await
        .expect_err("the conductor binary supplies no runner for media-tagger");

    let expected = concat!(
        "cannot dispatch builtin ids this run has no runner for: tool 'media-tagger' dispatches ",
        "builtin id 'media-tagger', which this conductor build does not ship and no host runner ",
        "registers; the host must register a host builtin under each of those ids (registered ",
        "host builtins: none)",
    );
    assert_eq!(error.to_string(), expected, "the refusal is the sentence the CLI docs state");
    assert_eq!(
        tracker.ops(),
        vec![ProgressOp::AddBar { total: 1, label: "workflow".to_string() }],
        "the refusal precedes every step, so the progress group saw no step bar",
    );
    tc.shutdown().await;
}
