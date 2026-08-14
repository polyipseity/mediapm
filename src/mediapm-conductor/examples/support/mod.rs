//! Shared helpers for conductor examples.

use std::collections::BTreeMap;
use std::error::Error;
use std::fs;
use std::path::Path;

use mediapm_conductor::{ToolInputKind, ToolInputSpec, ToolKindSpec, ToolRuntime, ToolSpec};

/// Convenient result type for examples.
pub(crate) type ExampleResult<T> = Result<T, Box<dyn Error>>;

pub(crate) fn write_text_file(path: &Path, content: &str) -> ExampleResult<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, content)?;
    Ok(())
}

/// Builds the shared `echo@v1` tool spec used by all conductor examples.
pub(crate) fn echo_tool() -> ToolSpec {
    ToolSpec {
        kind: ToolKindSpec::Builtin { builtin_id: "echo@v1".into() },
        name: "echo".into(),
        inputs: BTreeMap::from([(
            "text".into(),
            ToolInputSpec { kind: ToolInputKind::String, required: false },
        )]),
        default_inputs: BTreeMap::new(),
        outputs: BTreeMap::new(),
        runtime: ToolRuntime::default(),
    }
}
