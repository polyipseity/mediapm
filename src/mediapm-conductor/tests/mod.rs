//! Integration test harness for `mediapm-conductor`.
//!
//! The `int` module covers contract-focused integration behavior, `e2e`
//! contains multi-step workflow scenarios, and `prop` is reserved for
//! property-driven coverage as the conductor evolves.

use std::collections::BTreeMap;

use mediapm_cas::InMemoryCas;
use mediapm_conductor::{
    NickelDocument, RuntimeStoragePaths, SimpleConductor, ToolInputKind, ToolInputSpec,
    ToolKindSpec, ToolRuntime, ToolSpec, WorkflowSpec, WorkflowStepSpec,
    model::config::versions::encode_document,
};

mod e2e;
mod int;
mod prop;

// ---------------------------------------------------------------------------
// Shared harness helpers
// ---------------------------------------------------------------------------

/// Creates an echo@1.0.0 `ToolSpec`.
fn echo_tool(name: &str) -> ToolSpec {
    ToolSpec {
        kind: ToolKindSpec::Builtin { name: "echo".into(), version: "1.0.0".into() },
        name: name.into(),
        version: "1.0.0".into(),
        inputs: BTreeMap::from([(
            "text".into(),
            ToolInputSpec {
                kind: ToolInputKind::String,
                description: String::new(),
                required: false,
            },
        )]),
        default_inputs: BTreeMap::new(),
        outputs: BTreeMap::new(),
        runtime: ToolRuntime::default(),
    }
}

/// Creates a single-step echo `WorkflowSpec`.
fn echo_workflow(name: &str, tool_id: &str, text: &str) -> WorkflowSpec {
    WorkflowSpec {
        name: name.into(),
        display_name: String::new(),
        description: String::new(),
        impure: false,
        steps: vec![WorkflowStepSpec {
            id: "s1".into(),
            tool: tool_id.into(),
            inputs: BTreeMap::from([("text".into(), text.into())]),
            outputs: BTreeMap::new(),
            max_retries: 0,
            depends_on: Vec::new(),
        }],
    }
}

/// Creates a `NickelDocument` with one echo tool and one workflow.
fn single_echo_doc(tool_id: &str, workflow_name: &str) -> NickelDocument {
    NickelDocument {
        tools: BTreeMap::from([(tool_id.into(), echo_tool(tool_id))]),
        workflows: vec![echo_workflow(workflow_name, tool_id, workflow_name)],
        ..NickelDocument::default()
    }
}

/// Creates a `NickelDocument` with two echo tools and two workflows
/// (distinct tool_id keys for independent cache entries).
fn dual_echo_doc() -> NickelDocument {
    NickelDocument {
        tools: BTreeMap::from([
            ("echo-v1@1.0.0".into(), echo_tool("echo-v1@1.0.0")),
            ("echo-v2@1.0.0".into(), echo_tool("echo-v2@1.0.0")),
        ]),
        workflows: vec![
            echo_workflow("default", "echo-v1@1.0.0", "default"),
            echo_workflow("updated", "echo-v2@1.0.0", "updated"),
        ],
        ..NickelDocument::default()
    }
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
        let dir = tempfile::tempdir().expect("tempdir");
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
        let config_path = self.dir.path().join("conductor.ncl");
        std::fs::write(&config_path, encode_document(doc).expect("encode")).expect("write config");
    }
}
