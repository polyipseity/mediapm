//! Provider source definitions for the conductor `import` builtin.
//!
//! `import` is a conductor builtin the host dispatches in-process: its step
//! carries no command parts, so [`sources`] is empty. The sync classifies it as
//! builtin code (`is_builtin_source_ingest_requirement`) and provisions nothing
//! for it — the bytes an import step consumes are the media the step names, not
//! a payload belonging to the tool.
//!
//! Canonical version: mediapm build-time git hash (VCS hash kind, fixed at
//! code-writing time), because the builtin ships inside mediapm with no
//! upstream tagged release.

use mediapm_conductor::tools::provider::ResolvedToolFetch;

/// Returns the source descriptors for `import`: none.
///
/// The tool still resolves, so a document that declares it keeps a version
/// identity and a registry record; what it has no longer is a payload.
#[must_use]
pub(crate) fn sources() -> ResolvedToolFetch {
    ResolvedToolFetch { tool_id: "import".to_string(), sources: Vec::new() }
}
