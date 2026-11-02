//! Builtin command implementations exposed by the `mediapm` executable.
//!
//! These handlers are invoked through `mediapm builtin ...`.
//!
//! A builtin mediapm also *supplies to the conductor* lives here too: the
//! tagging step runs through `media_tagger::host::MediaTaggerBuiltin`, which
//! mediapm registers as a
//! [host builtin](mediapm_conductor::tools::host_builtin) on every run. The
//! `mediapm builtin media-tagger` subcommand stays as the way to run the same
//! pipeline by hand; it is no longer what a workflow step reaches.

#[cfg(feature = "media-tagger")]
pub mod media_tagger;

use std::sync::Arc;

use mediapm_conductor::error::ConductorError;
use mediapm_conductor::tools::{HostBuiltin, HostBuiltins};

#[cfg(feature = "media-tagger")]
use crate::tools::workflows::media_tagger::MEDIA_TAGGER_BUILTIN_ID;
#[cfg(feature = "media-tagger")]
use media_tagger::host::MediaTaggerBuiltin;

/// The builtin ids mediapm's host supplies to a conductor run.
///
/// One list, read where the two things that have to agree about it are decided:
/// the registry every run carries, and the generated document's retention set —
/// an id the host dispatches must have its tool entry survive a sync, and an id
/// it does not dispatch must not be retained. The ids are the *host* half of the
/// builtin catalogue; the conductor's own production builtins are a separate
/// set ([`mediapm_conductor::tools::production_builtins`]).
#[cfg(feature = "media-tagger")]
pub(crate) const HOST_BUILTIN_IDS: &[&str] = &[MEDIA_TAGGER_BUILTIN_ID];

/// The builtin ids mediapm's host supplies to a conductor run.
///
/// Empty in a build without the `media-tagger` feature: the runner does not
/// exist there, so a document naming it has no implementation and the run start
/// check says so.
#[cfg(not(feature = "media-tagger"))]
pub(crate) const HOST_BUILTIN_IDS: &[&str] = &[];

/// Returns whether `tool_id` names a builtin: a tool that runs in-process and
/// therefore has no payload.
///
/// A builtin's registry record carries no content map — there are no bytes to
/// fetch, so the sync writes an empty content-map hash and the tool's spec names
/// a builtin id rather than a command. An empty content map is therefore the
/// normal shape for these ids, not the sign of a sync that never ran: the reader
/// that asks this question still has to treat an empty map on a *fetched* tool
/// as a missing payload.
///
/// The two halves are one list because both kinds reach mediapm without a
/// payload for the same reason: `import` is the conductor's source-ingest
/// builtin, and [`HOST_BUILTIN_IDS`] are the ids this host supplies runners for.
#[must_use]
pub(crate) fn is_payload_free_builtin_id(tool_id: &str) -> bool {
    tool_id.eq_ignore_ascii_case(IMPORT_BUILTIN_ID)
        || HOST_BUILTIN_IDS.iter().any(|id| id.eq_ignore_ascii_case(tool_id))
}

/// The tool id of the conductor's source-ingest builtin.
///
/// mediapm declares no dependency on the crate that ships it: this crate reaches
/// `import` only through its provider (`tools::provider::import`), whose own
/// module doc states the same id.
const IMPORT_BUILTIN_ID: &str = "import";

/// Builds the registry of builtin runners mediapm supplies to a conductor run.
///
/// # Errors
///
/// Returns [`ConductorError::Workflow`] when the registry refuses an entry: a
/// duplicate id, or an id a builtin this conductor build ships already owns. The
/// list is a compile-time fact, so either is a wiring bug in this crate rather
/// than something a run can be asked to tolerate.
pub(crate) fn host_builtins() -> Result<HostBuiltins, ConductorError> {
    #[cfg(feature = "media-tagger")]
    let entries: Vec<Arc<dyn HostBuiltin>> =
        vec![Arc::new(MediaTaggerBuiltin) as Arc<dyn HostBuiltin>];
    #[cfg(not(feature = "media-tagger"))]
    let entries: Vec<Arc<dyn HostBuiltin>> = Vec::new();

    HostBuiltins::new(entries)
}

/// Returns the tool definition of a builtin mediapm's host supplies, if
/// `tool_id` names one.
///
/// [`HOST_BUILTIN_IDS`] is the set this answers for, and the only place the
/// host builtin ids are written down: the lookup refuses an id outside it, so
/// the published set and the definition it writes cannot name different ids.
/// A host builtin has no payload, so the sync has nothing to fetch and no spec
/// preset to build from a content map: its definition is the one the
/// synthesizer writes, and the sync asks for it by the tool id the library
/// declared.
#[must_use]
pub(crate) fn host_builtin_tool_spec(tool_id: &str) -> Option<mediapm_conductor::ToolSpec> {
    if !HOST_BUILTIN_IDS.iter().any(|id| id.eq_ignore_ascii_case(tool_id)) {
        return None;
    }
    // One arm per id the published set carries. A definition cannot be derived
    // from the id alone, so the builder is per-id code; the guard above is what
    // keeps this from becoming a second list of ids.
    #[cfg(feature = "media-tagger")]
    if tool_id.eq_ignore_ascii_case(MEDIA_TAGGER_BUILTIN_ID) {
        return Some(crate::tools::workflows::media_tagger::build_media_tagger_spec().0);
    }
    None
}
