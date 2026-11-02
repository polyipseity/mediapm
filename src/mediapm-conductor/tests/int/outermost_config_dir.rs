//! The directory a run's outermost document lives in reaches a host runner.
//!
//! `StepExecutionRequest.outermost_config_dir` is a contract: a builtin
//! resolves a relative path value against it, and a host runner that reads its
//! document from the run needs the directory the document actually lives in.
//! The value is derived from the documents the run loads, so a run that carries
//! anything else — the process working directory, which is what every request
//! used to hold — fails its step here instead of resolving a relative path
//! against the wrong root.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use mediapm_conductor::api::RunWorkflowOptions;
use mediapm_conductor::tools::{HostBuiltin, HostBuiltinOutput, HostBuiltinRequest, HostBuiltins};
use mediapm_conductor::{ConductorError, ToolKindSpec, ToolRuntime, ToolSpec};

use crate::{TestConductor, bare_step, doc_with_workflows, workflow_with_steps};

/// The builtin id the runner in this test answers for.
///
/// Spelled like a shipped builtin id and deliberately not one: it is what the
/// document's `builtin` spec names, and nothing in this build supplies it.
const HOST_BUILTIN_ID: &str = "doc-dir@v1";

/// A host runner that fails its step unless the run handed it the directory the
/// document lives in.
///
/// Failing the step rather than only recording the value is the point: a run
/// that carries the wrong directory cannot succeed, so the assertion is a fact
/// about the run and not only about a value the test read afterwards.
#[derive(Debug)]
struct DocumentDirRunner {
    /// The directory holding the document the run is started from.
    expected: PathBuf,
    /// The directory the run handed the runner, recorded once.
    seen: Mutex<Option<PathBuf>>,
}

impl DocumentDirRunner {
    /// A runner expecting `expected` as its config directory.
    fn new(expected: PathBuf) -> Self {
        Self { expected, seen: Mutex::new(None) }
    }

    /// The directory the run handed the runner, or a panic when it never ran.
    fn seen(&self) -> PathBuf {
        self.seen
            .lock()
            .expect("the seen slot is only written by the runner")
            .clone()
            .expect("the runner must have been dispatched")
    }
}

#[async_trait]
impl HostBuiltin for DocumentDirRunner {
    fn id(&self) -> &str {
        HOST_BUILTIN_ID
    }

    async fn run(
        &self,
        request: HostBuiltinRequest<'_>,
    ) -> Result<HostBuiltinOutput, ConductorError> {
        let seen = request.outermost_config_dir.to_path_buf();
        let matches = seen == self.expected;
        *self.seen.lock().expect("the seen slot is only written by the runner") =
            Some(seen.clone());
        if !matches {
            return Err(ConductorError::Workflow(format!(
                "the run handed the runner '{}', not the directory holding its document '{}'",
                seen.display(),
                self.expected.display(),
            )));
        }
        Ok(HostBuiltinOutput::default())
    }
}

/// A `builtin` tool spec naming [`HOST_BUILTIN_ID`], declaring no inputs.
///
/// Declaring no command parts is what makes the step dispatch in-process; the
/// id is what decides which runner receives it.
fn host_tool() -> ToolSpec {
    ToolSpec {
        version: None,
        kind: ToolKindSpec::Builtin { builtin_id: HOST_BUILTIN_ID.to_string() },
        name: HOST_BUILTIN_ID.to_string(),
        inputs: BTreeMap::new(),
        default_inputs: BTreeMap::new(),
        outputs: BTreeMap::new(),
        runtime: ToolRuntime::default(),
    }
}

/// A host runner receives the directory that holds the run's outermost
/// document, not the process working directory.
///
/// The fixture writes its document at `conductor.ncl` directly under the
/// conductor directory, so that directory is the one holding the outermost
/// document, and the runner fails its step unless it is handed exactly it.
#[tokio::test]
async fn a_host_runner_receives_the_directory_holding_the_outermost_document() {
    let tc = TestConductor::new();
    let runner = Arc::new(DocumentDirRunner::new(tc.path().to_path_buf()));
    tc.write_config(doc_with_workflows(
        BTreeMap::from([(HOST_BUILTIN_ID.to_string(), host_tool())]),
        vec![workflow_with_steps("default", vec![bare_step("tag", HOST_BUILTIN_ID, &[])])],
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
        .expect("a run must hand its runner the directory holding its document");

    assert_eq!(
        (summary.total_steps, summary.executed_steps, summary.failed_steps),
        (1, 1, 0),
        "the step must execute through the host runner and succeed: {summary:?}"
    );
    assert_eq!(
        runner.seen(),
        tc.path().to_path_buf(),
        "the runner must be handed the directory holding the document"
    );
    tc.shutdown().await;
}
