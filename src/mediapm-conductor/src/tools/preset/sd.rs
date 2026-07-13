//! Preset for the `sd` managed tool.

use std::collections::BTreeMap;

use crate::{
    InputBinding, OutputCaptureSpec, SaveMode, ToolInputKind, ToolInputSpec, ToolRuntime, ToolSpec,
};

/// Builds the [`ToolSpec`] and [`ToolRuntime`] for `sd`.
#[must_use]
pub fn apply(
    content_map: BTreeMap<String, String>,
    command_selector: &str,
) -> (ToolSpec, ToolRuntime) {
    let command = vec![
        command_selector.to_string(),
        "${*inputs.leading_args}".to_string(),
        "${*inputs.pattern}".to_string(),
        "${*inputs.replacement}".to_string(),
        "${inputs.content:file(inputs/input.ffmeta)}".to_string(),
        "${*inputs.trailing_args}".to_string(),
    ];

    let inputs = BTreeMap::from([
        ("leading_args".into(), ToolInputSpec { kind: ToolInputKind::String, required: false }),
        ("trailing_args".into(), ToolInputSpec { kind: ToolInputKind::String, required: false }),
        ("content".into(), ToolInputSpec { kind: ToolInputKind::String, required: false }),
        ("pattern".into(), ToolInputSpec { kind: ToolInputKind::String, required: false }),
        ("replacement".into(), ToolInputSpec { kind: ToolInputKind::String, required: false }),
    ]);

    let outputs = BTreeMap::from([
        (
            "content".into(),
            OutputCaptureSpec {
                name: "content".into(),
                capture: "file:inputs/input.ffmeta".into(),
                save: SaveMode::True,
                allow_empty: false,
                include_topmost_folder: true,
            },
        ),
        (
            "sandbox_artifacts".into(),
            OutputCaptureSpec {
                name: "sandbox_artifacts".into(),
                capture: "folder:inputs".into(),
                save: SaveMode::True,
                allow_empty: false,
                include_topmost_folder: true,
            },
        ),
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
    ]);

    let default_inputs = BTreeMap::from([
        ("leading_args".into(), InputBinding::Vec(vec![])),
        ("trailing_args".into(), InputBinding::Vec(vec![])),
        ("pattern".into(), InputBinding::String(String::new())),
        ("replacement".into(), InputBinding::String(String::new())),
    ]);

    let spec = ToolSpec {
        name: "sd".into(),
        kind: crate::ToolKindSpec::Executable {
            command,
            env_vars: BTreeMap::new(),
            success_codes: vec![0],
        },
        inputs,
        default_inputs,
        outputs,
        runtime: ToolRuntime {
            impure: false,
            content_map: content_map.clone(),
            max_retries: 0,
            max_concurrent_calls: 0,
            ..ToolRuntime::default()
        },
    };

    (spec, ToolRuntime { content_map, ..ToolRuntime::default() })
}
