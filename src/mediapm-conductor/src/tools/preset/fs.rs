//! Preset for the `fs` builtin tool.

use std::collections::BTreeMap;

use crate::{OutputCaptureSpec, SaveMode, ToolInputKind, ToolInputSpec, ToolRuntime, ToolSpec};

/// Builds the [`ToolSpec`] and [`ToolRuntime`] for `fs`.
#[must_use]
pub fn apply() -> (ToolSpec, ToolRuntime) {
    let spec = ToolSpec {
        name: "fs".into(),
        kind: crate::ToolKindSpec::Executable {
            command: vec!["${executable}".into(), "${*inputs.args}".into()],
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
