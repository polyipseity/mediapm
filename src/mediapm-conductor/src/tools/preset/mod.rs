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
    command_selector: &str,
) -> (ToolSpec, ToolRuntime) {
    match tool_name {
        n if n.eq_ignore_ascii_case("sd") => sd::apply(content_map, command_selector),
        n if n.eq_ignore_ascii_case("echo") => echo::apply(),
        n if n.eq_ignore_ascii_case("archive") => archive::apply(),
        n if n.eq_ignore_ascii_case("export") => export::apply(),
        n if n.eq_ignore_ascii_case("fs") => fs::apply(),
        n if n.eq_ignore_ascii_case("import") => import::apply(),
        _ => panic!("unknown managed tool: {tool_name}"),
    }
}
