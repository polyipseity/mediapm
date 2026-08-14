//! Integration test harness for `mediapm-conductor`.
//!
//! `int` covers contract-focused integration behavior, `e2e` contains
//! multi-step workflows, and `prop` provides property-test scaffolding.

use std::collections::BTreeMap;

use mediapm_cas::InMemoryCas;
use mediapm_conductor::{
    NickelDocument, RuntimeStoragePaths, SimpleConductor, ToolInputKind, ToolInputSpec,
    ToolKindSpec, ToolRuntime, ToolSpec, WorkflowSpec, WorkflowStepSpec,
    config::versions::encode_document,
};

mod e2e;
mod int;
#[cfg(feature = "proptest")]
mod prop;

// ---------------------------------------------------------------------------
// Shared harness helpers
// ---------------------------------------------------------------------------

/// Creates an echo@v1 `ToolSpec`.
fn echo_tool(name: &str) -> ToolSpec {
    ToolSpec {
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

/// A test fixture that owns a tempdir + `InMemoryCas` + `SimpleConductor`.
///
/// The tempdir is cleaned up on drop.
struct TestConductor {
    dir: tempfile::TempDir,
    conductor: SimpleConductor<InMemoryCas>,
}

impl TestConductor {
    fn new() -> Self {
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let conductor =
            SimpleConductor::new(RuntimeStoragePaths::new(dir.path()), InMemoryCas::new());
        Self { dir, conductor }
    }

    fn path(&self) -> &std::path::Path {
        self.dir.path()
    }

    fn conductor(&self) -> &SimpleConductor<InMemoryCas> {
        &self.conductor
    }

    /// Write a `NickelDocument` as the config for this conductor.
    fn write_config(&self, doc: NickelDocument) {
        write_conductor_config(self.dir.path(), doc);
    }
}
