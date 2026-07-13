//! Preset for the `import` builtin tool.

use std::collections::BTreeMap;

use crate::tools::helpers::build_os_conditional_selector;
use crate::{OutputCaptureSpec, SaveMode, ToolInputKind, ToolInputSpec, ToolRuntime, ToolSpec};

/// Builds the [`ToolSpec`] and [`ToolRuntime`] for `import`.
#[must_use]
pub fn apply(os_exec_paths: &BTreeMap<String, String>) -> (ToolSpec, ToolRuntime) {
    let command_path = build_os_conditional_selector(os_exec_paths);
    let command = if command_path.is_empty() {
        vec!["${executable}".into(), "${*inputs.args}".into()]
    } else {
        vec![command_path, "${*inputs.args}".into()]
    };
    let spec = ToolSpec {
        name: "import".into(),
        kind: crate::ToolKindSpec::Executable {
            command,
            env_vars: BTreeMap::new(),
            success_codes: vec![0],
        },
        inputs: BTreeMap::from([(
            "args".into(),
            ToolInputSpec { kind: ToolInputKind::String, required: false },
        )]),
        default_inputs: BTreeMap::new(),
        outputs: BTreeMap::from([
            (
                "stdout".into(),
                OutputCaptureSpec {
                    name: "stdout".into(),
                    capture: "stdout".into(),
                    save: SaveMode::True,
                    allow_empty: false,
                    include_topmost_folder: true,
                },
            ),
            (
                "stderr".into(),
                OutputCaptureSpec {
                    name: "stderr".into(),
                    capture: "stderr".into(),
                    save: SaveMode::True,
                    allow_empty: false,
                    include_topmost_folder: true,
                },
            ),
        ]),
        runtime: ToolRuntime { impure: true, ..ToolRuntime::default() },
    };
    (spec, ToolRuntime { impure: true, ..ToolRuntime::default() })
}
