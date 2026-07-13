//! Tool preset dispatcher.
//!
//! Routes tool IDs to the appropriate per-tool preset builder.

pub(crate) mod archive;
pub(crate) mod echo;
pub(crate) mod export;
pub(crate) mod fs;
pub(crate) mod import;
pub(crate) mod sd;

use std::collections::BTreeMap;

use crate::{ToolRuntime, ToolSpec};

/// Builds a [`ToolSpec`] and [`ToolRuntime`] for the named tool.
///
/// # Panics
///
/// Panics if `tool_name` is not a recognized managed tool.
#[must_use]
pub fn apply_preset(
    tool_name: &str,
    content_map: BTreeMap<String, String>,
    os_exec_paths: &BTreeMap<String, String>,
) -> (ToolSpec, ToolRuntime) {
    match tool_name {
        n if n.eq_ignore_ascii_case("sd") => sd::apply(content_map, os_exec_paths),
        n if n.eq_ignore_ascii_case("echo") => echo::apply(os_exec_paths),
        n if n.eq_ignore_ascii_case("archive") => archive::apply(os_exec_paths),
        n if n.eq_ignore_ascii_case("export") => export::apply(os_exec_paths),
        n if n.eq_ignore_ascii_case("fs") => fs::apply(os_exec_paths),
        n if n.eq_ignore_ascii_case("import") => import::apply(os_exec_paths),
        _ => panic!("unknown managed tool: {tool_name}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ToolKindSpec;

    #[test]
    fn apply_preset_routes_all_registered_tools() {
        let empty_map = BTreeMap::new();
        let empty_paths = BTreeMap::new();
        for name in &["sd", "echo", "archive", "export", "fs", "import"] {
            let (spec, _runtime) = apply_preset(name, empty_map.clone(), &empty_paths);
            assert!(
                !spec.inputs.is_empty() || !spec.outputs.is_empty(),
                "tool {name}: should have at least one input or output"
            );
            // All tools produce Executable kind (builtins route through
            // `${executable}` placeholder).
            assert!(
                matches!(spec.kind, ToolKindSpec::Executable { .. }),
                "tool {name}: expected Executable kind, got {kind:?}",
                kind = spec.kind
            );
        }
    }

    #[test]
    #[should_panic(expected = "unknown managed tool")]
    fn apply_preset_rejects_unknown_tool() {
        let _ = apply_preset("nonexistent-tool", BTreeMap::new(), &BTreeMap::new());
    }
}
