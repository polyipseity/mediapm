//! Tool lifecycle transitions.
//!
//! This module classifies managed tool names by how their content is ingested.

/// Returns true when the tool name identifies a builtin source-ingest
/// tool that requires special content-ingestion handling.
#[must_use]
pub(super) fn is_builtin_source_ingest_requirement(tool_name: &str) -> bool {
    tool_name.eq_ignore_ascii_case("import")
}
