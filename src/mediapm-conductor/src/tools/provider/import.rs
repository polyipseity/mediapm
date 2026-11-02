//! Provider for the `import` builtin tool.

use super::ResolvedToolFetch;

/// Returns the source descriptors for `import`: none.
///
/// `import` is a conductor builtin the host dispatches in-process, so its step
/// carries no command parts and there is nothing to download, generate or place
/// in a sandbox. Resolution still answers, because a declared builtin tool must
/// resolve: what it has is no payload.
#[must_use]
pub(crate) fn sources() -> ResolvedToolFetch {
    ResolvedToolFetch { tool_id: "import".to_string(), sources: Vec::new() }
}
