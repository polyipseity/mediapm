//! Provider source definitions for `media-tagger`.
//!
//! `media-tagger` is a builtin mediapm's host runs in-process, so it has no
//! payload to fetch: [`sources`] is empty, and the sync records the tool's
//! provenance and writes its tool entry as a builtin definition (see
//! [`crate::builtins::host_builtin_tool_spec`]) instead of installing anything.
//!
//! Canonical version: mediapm build-time git hash (VCS hash kind, fixed at
//! code-writing time).
//!
//! Resolved provenance fields: `resolved_tag` is always `None` — WHY:
//! media-tagger is shipped inside mediapm with no upstream tagged release; its
//! identity is the mediapm build (`resolved_version` = crate version,
//! `resolved_vcs_hash` = mediapm git hash).

use mediapm_conductor::tools::provider::ResolvedToolFetch;

/// Returns the source descriptors for `media-tagger`: none.
///
/// A host builtin's implementation is the runner mediapm registers on every
/// run, so there are no bytes, no per-OS slots and no launcher script. The
/// provider still answers, because the tool keeps a version identity and a
/// registry record: what it no longer has is a payload.
#[must_use]
pub(crate) fn sources() -> ResolvedToolFetch {
    ResolvedToolFetch { tool_id: "media-tagger".to_string(), sources: Vec::new() }
}
