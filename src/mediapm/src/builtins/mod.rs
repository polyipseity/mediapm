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
/// A host builtin has no payload, so the sync has nothing to fetch and no spec
/// preset to build from a content map: its definition is the one the
/// synthesizer writes, and the sync asks for it by the tool id the library
/// declared.
#[must_use]
pub(crate) fn host_builtin_tool_spec(tool_id: &str) -> Option<mediapm_conductor::ToolSpec> {
    match tool_id {
        #[cfg(feature = "media-tagger")]
        n if n.eq_ignore_ascii_case(MEDIA_TAGGER_BUILTIN_ID) => {
            Some(crate::tools::workflows::media_tagger::build_media_tagger_spec().0)
        }
        _ => None,
    }
}
