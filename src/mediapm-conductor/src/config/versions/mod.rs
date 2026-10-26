//! Versioned persistence for Nickel configuration envelopes.
//!
//! ## Policy
//!
//! - Historical schema migration uses Nickel files (`v<N>.ncl`), not Rust.
//! - This module bridges between persisted wire formats and runtime config
//!   types. Do not import unversioned config types directly from version modules.
//! - The latest bridge is in `v_latest.rs`.
//!
//! ## DO NOT REMOVE: versions policy guard
//!
//! - Version dispatch is Nickel: `mod.ncl` owns `current_version`,
//!   `supported_versions`, and `migrate_to`, and each `vN.ncl` owns the
//!   migration into itself. Do not re-introduce a parallel Rust dispatcher
//!   over the same versions.
//! - `v_latest.rs` owns the active `*Latest` boundary family. This module is
//!   the ONLY bridge between that family and the resolved (option-free) types
//!   in `config/mod.rs`: `from_boundary` (read direction), `into_boundary`
//!   (encode direction), and `merge_document_sources` (multi-document load +
//!   merge) all live here. `config/mod.rs` names no version path at all.
//! - This module MAY import from `v_latest`, but MUST NOT re-export any
//!   versioned symbol outside `versions/`. A re-export would let an outside
//!   caller name a `*Latest` type, which is the leak these entry points exist
//!   to close.
//! - `resolve_version_contract` is the registry: a version this build cannot
//!   name a contract file for must be an error, never a fallback to the
//!   newest contract.

pub(crate) mod v_latest;

mod merge;

use std::path::{Path, PathBuf};

use crate::error::ConductorError;

use super::nickel_io::{evaluate_document_source, migrate_document_source_to_version};

/// Source of the v1 Nickel contract (needed for backward migration validation).
pub(crate) const V1_NCL_SOURCE: &str = include_str!("v1.ncl");
/// Source of the v2 Nickel contract (needed for migration down from v3).
pub(crate) const V2_NCL_SOURCE: &str = include_str!("v2.ncl");
/// Source of the v3 Nickel contract (needed for migration down from v4).
pub(crate) const V3_NCL_SOURCE: &str = include_str!("v3.ncl");
/// Source of the v4 Nickel contract.
pub(crate) const V4_NCL_SOURCE: &str = include_str!("v4.ncl");

/// Fixed embedded migration helper module.
pub(crate) const MOD_NCL_SOURCE: &str = include_str!("mod.ncl");

/// Every contract source this build evaluates, under the file name its
/// registry imports it by.
///
/// A reader of an exported contract directory sees exactly the sources the
/// runtime validates a document against, so no hand-maintained schema can
/// describe a rule the process does not enforce. The list is the source of
/// truth for that export: a version added below reaches it without a second
/// edit, and `SUPPORTED_VERSIONS` stays the single statement of which versions
/// exist.
#[must_use]
pub fn contract_sources() -> &'static [(&'static str, &'static str)] {
    &[
        ("mod.ncl", MOD_NCL_SOURCE),
        ("v1.ncl", V1_NCL_SOURCE),
        ("v2.ncl", V2_NCL_SOURCE),
        ("v3.ncl", V3_NCL_SOURCE),
        ("v4.ncl", V4_NCL_SOURCE),
    ]
}

/// Every schema version this build reads, in order.
///
/// The contract resolution below and the diagnostic it reports on an unknown
/// version both read this list, so a version added to the arms alone cannot
/// reach a document while staying out of the message that says which versions
/// exist.
pub(crate) const SUPPORTED_VERSIONS: &[u32] = &[1, 2, 3, 4];

/// Resolves one requested schema marker to the embedded Nickel contract file name.
pub(super) fn resolve_version_contract(
    requested_version: u32,
    document_kind: &str,
) -> Result<&'static str, ConductorError> {
    match requested_version {
        1 => Ok("v1.ncl"),
        2 => Ok("v2.ncl"),
        3 => Ok("v3.ncl"),
        4 => Ok("v4.ncl"),
        _ => {
            let supported = SUPPORTED_VERSIONS.iter().map(u32::to_string).collect::<Vec<_>>();
            Err(ConductorError::Workflow(format!(
                "unsupported {document_kind} schema version {requested_version}; expected one of {}",
                supported.join(", ")
            )))
        }
    }
}

// ---------------------------------------------------------------------------
// Boundary entry points
//
// These are the ONLY way to cross between the `*Latest` wire family and the
// resolved runtime types. None of them names a `*Latest` type in its
// signature, so a caller outside `versions/` cannot construct one to pass in.
// ---------------------------------------------------------------------------

/// Resolves a raw Nickel document source into the runtime config document.
///
/// This is the read-direction boundary entry point: it parses (evaluating the
/// source through the versioned migration pipeline) and then resolves boundary
/// defaults in one step, so the caller never handles a `*Latest` value. The
/// `content_map` subset `external_data` invariant is enforced before the
/// resolved document is returned.
///
/// # Errors
///
/// Returns [`ConductorError::Serialization`] when the source is not valid
/// UTF-8 or Nickel evaluation/migration fails, and
/// [`ConductorError::Workflow`] when the resolved document violates the
/// `content_map` subset `external_data` invariant, stages a payload by a
/// strategy that needs the root it does not name, or gives a `builtin` spec an
/// id this build does not register.
pub(crate) fn from_boundary(source: &str) -> Result<crate::config::NickelDocument, ConductorError> {
    let envelope: v_latest::NickelEnvelopeLatest =
        evaluate_document_source(source, "configuration document")?;
    let doc: crate::config::NickelDocument = envelope.into();
    doc.validate_external_data_invariant()?;
    doc.validate_payload_staging_invariant()?;
    doc.validate_builtin_tool_ids()?;
    Ok(doc)
}

/// Converts a runtime config document into the latest-schema wire envelope.
///
/// This is the encode-direction boundary entry point: the `From` bridges in
/// `v_latest` apply boundary defaults, so the conversion is driven from inside
/// `versions/` and the caller never names the `*Latest` type it receives.
///
/// The caller is responsible for validating the `content_map` subset
/// `external_data` invariant first (see [`encode_document`]).
pub(crate) fn into_boundary(
    document: crate::config::NickelDocument,
) -> v_latest::NickelEnvelopeLatest {
    document.into()
}

/// Returns the schema version marker of the active boundary family.
///
/// Replaces direct use of the `NICKEL_VERSION_LATEST` constant outside
/// `versions/`, so the marker is read through the version module rather than
/// imported from it.
pub(crate) fn latest_version() -> u32 {
    v_latest::NICKEL_VERSION_LATEST
}

/// Loads, merges, and resolves every configuration document at `paths`.
///
/// This is the multi-document entry point. Merging is presence-preserving
/// (explicit beats implicit) and therefore operates on the raw wire envelope,
/// so it lives in [`merge`]. Routing load and merge through a single call
/// keeps [`merge::SourceDocument`] — and the `*Latest` type it wraps —
/// unnameable from outside `versions/`.
///
/// # Errors
///
/// Returns [`ConductorError::Io`] when a document cannot be read,
/// [`ConductorError::Serialization`] when a document is not valid UTF-8 or
/// fails Nickel evaluation/migration, and [`ConductorError::Workflow`] when
/// the merged documents conflict or violate the `content_map` subset
/// `external_data` invariant.
pub(crate) fn merge_document_sources(
    paths: &[PathBuf],
) -> Result<crate::config::NickelDocument, ConductorError> {
    let sources = paths
        .iter()
        .map(|path| merge::load_source(path.as_path()))
        .collect::<Result<Vec<_>, _>>()?;
    merge::merge_documents(&sources)
}

/// Restores human-readable fields from the document previously written at
/// `previous_path` onto `outgoing`.
///
/// `external_data` descriptions and workflow `display_name`/`description` are
/// not derivable from the resolved document, so re-saving a rebuilt document
/// would otherwise drop them. Only fields that are currently `None` on
/// `outgoing` are filled: an explicit outgoing value always wins.
///
/// # Errors
///
/// Returns [`ConductorError::Io`] when the previous document cannot be read,
/// and [`ConductorError::Serialization`] when it is not valid UTF-8 or fails
/// Nickel evaluation/migration. Callers that treat an absent or unreadable
/// previous document as "nothing to preserve" should discard this error.
pub(crate) fn restore_readable_fields(
    outgoing: &mut crate::config::NickelDocument,
    previous_path: &Path,
) -> Result<(), ConductorError> {
    let previous = merge::load_source(previous_path)?;
    merge::preserve_readable_fields(outgoing, &previous.envelope);
    Ok(())
}

// ---------------------------------------------------------------------------
// Document encoding / decoding (inlined from the removed iso.rs)
// ---------------------------------------------------------------------------

/// Encodes one configuration document to Nickel source bytes.
///
/// The document is first validated (the `content_map` ⊆ `external_data`
/// invariant and the payload-staging invariant), converted to a latest-schema
/// envelope, then rendered as Nickel source.
///
/// The document-level staging default is written only when it differs from
/// `in_place`, the value an absent field already means, and a per-tool strategy
/// equal to the default is omitted. That is what keeps a user document
/// loadable: mediapm merges the user's document over the generated one, the
/// merge rejects a field set to two different values, and a generated default
/// the generator invented would collide with a user document that states
/// `copy`.
///
/// Those two invariants are enforced here as well as at load, because a build
/// that writes a document it cannot read is worse than one that refuses to
/// write it: the generated-document loader discards an unreadable file and
/// regenerates, so the only trace of the mismatch would be a warning and a full
/// re-provision. The builtin-id check is the deliberate exception, and the code
/// below records why.
///
/// # Errors
///
/// Returns [`ConductorError`] when the document cannot be converted to the
/// latest schema, validation fails, or Nickel rendering fails.
pub fn encode_document(document: crate::config::NickelDocument) -> Result<Vec<u8>, ConductorError> {
    // Validate the content_map ⊆ external_data invariant before encoding.
    // This catches missing external_data entries at encode time (first
    // save), preventing silent production of an invalid document that would
    // fail on the next decode.
    document.validate_external_data_invariant()?;
    document.validate_payload_staging_invariant()?;
    // The builtin-id check runs on load and not here. A generated document
    // written by a build that carried a test double may name a builtin this
    // build does not register, and the prune has to be able to save the
    // document that drops the entry before the next load reads the file.
    let envelope = into_boundary(document);
    let bytes =
        mediapm_utils::nickel::render_document_as_nickel(&envelope, "configuration document")
            .map_err(ConductorError::Serialization)?;
    // Stamp every machine-written document with the shared generated-file
    // banner. All save paths (CLI export, standalone machine-doc save,
    // mediapm reconcile/synthesis) route through this function, so the
    // banner text is identical everywhere. `#` comment lines are ignored by
    // the Nickel evaluator, so decode is unaffected and `encode → decode →
    // encode` is byte-stable (see `encode_document_banner_round_trip_byte_stable`).
    Ok(mediapm_utils::generated::prepend_banner(&bytes))
}

/// Decodes bytes through the embedded Nickel migration wrapper into a runtime
/// config document.
///
/// The input bytes are interpreted as UTF-8 Nickel source, evaluated through
/// the versioned migration pipeline, and deserialized into a `NickelDocument`.
///
/// # Errors
///
/// Returns [`ConductorError`] when the bytes are not valid UTF-8, Nickel
/// evaluation fails, or the document does not match the expected schema.
pub fn decode_document(bytes: &[u8]) -> Result<crate::config::NickelDocument, ConductorError> {
    let source = std::str::from_utf8(bytes).map_err(|err| {
        ConductorError::Serialization(format!("document source is not valid UTF-8: {err}"))
    })?;
    from_boundary(source)
}

/// Decodes bytes through the embedded Nickel migration wrapper into the raw
/// latest-schema wire envelope, WITHOUT applying boundary defaults.
///
/// This preserves field presence (which fields were explicitly written in the
/// source document) for callers that must distinguish explicit from implicit
/// values, notably multi-document merging.  Presence-preserving fields:
/// `external_data` descriptions, workflow `display_name`/`description`, and
/// `external_data.save` (absent = implicit `Saved`).
///
/// # Errors
///
/// Returns [`ConductorError`] when the bytes are not valid UTF-8 or Nickel
/// evaluation/migration fails.
pub(crate) fn decode_document_envelope(
    bytes: &[u8],
) -> Result<v_latest::NickelEnvelopeLatest, ConductorError> {
    let source = std::str::from_utf8(bytes).map_err(|err| {
        ConductorError::Serialization(format!("document source is not valid UTF-8: {err}"))
    })?;
    evaluate_document_source(source, "configuration document")
}

/// Evaluates one Nickel document source to schema v1 through the embedded
/// migration pipeline and applies `validate_document_v1` to the result.
///
/// This is the version-locked validation path for the legacy v1 surface. The
/// runtime decode pipeline never applies v1 contracts directly (v1 documents
/// are always migrated forward to the latest schema), so this helper exists
/// for tests that must prove the v1 contract itself: strictness rejections
/// (S-B1..S-B5), R2 known-good round-trips, and R3 v2→v1 migration output.
///
/// `migrate_to 1` is the identity on v1 inputs, so this helper doubles as a
/// direct v1 validator for already-v1 documents.
///
/// # Errors
///
/// Returns [`ConductorError`] when the source fails to evaluate or when
/// migration output violates the v1 envelope contract.
pub fn validate_v1_document(source: &str) -> Result<serde_json::Value, ConductorError> {
    migrate_document_source_to_version(source, 1, "configuration document")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every file the registry imports is one the export can publish.
    ///
    /// `mod.ncl` imports its version modules by name, so an export missing one
    /// of them cannot be evaluated — the only reason to write a contract out.
    /// The check reads the registry source, so a version added to the module
    /// without a matching entry here fails rather than exporting a registry that
    /// does not run.
    #[test]
    fn strict_the_registry_imports_only_files_the_export_carries() {
        let published: std::collections::BTreeSet<&str> =
            contract_sources().iter().map(|(name, _)| *name).collect();
        let mut imports = 0;
        for segment in MOD_NCL_SOURCE.split("import \"").skip(1) {
            let Some(name) = segment.split('"').next() else {
                continue;
            };
            imports += 1;
            assert!(
                published.contains(name),
                "mod.ncl imports '{name}', which contract_sources() does not publish"
            );
        }
        assert!(imports > 0, "mod.ncl must import at least one version module");
        assert_eq!(
            published.len(),
            imports + 1,
            "the export carries the registry and its imports, and nothing else: {published:?}"
        );
    }

    /// Downgrading to v3 records on every tool the strategy it staged by, and
    /// leaves the rest of the document runtime alone.
    ///
    /// v3 has nowhere to state a document-wide default, so a hop that merely
    /// dropped the field would leave every inheriting tool staging in place — a
    /// silent behaviour change for a document written against v4. The result
    /// also has to be a v3 document: this evaluates through the target
    /// contract, so a hop that produced a shape v3 rejects fails here.
    ///
    /// v4 differs from v3 by exactly one field, and it lives inside the document
    /// runtime, so the hop has to remove that field rather than the record: v3
    /// still declares `retry_impure` and the inherited environment names, and a
    /// downgrade that dropped them would lose configuration neither document
    /// asked to change.
    #[test]
    fn regression_downgrading_to_v3_materializes_the_document_default() {
        let source = r#"{
            version = 4,
            tools = {
                "bare" = { kind = "executable", name = "bare", command = ["true"] },
                "empty_runtime" = {
                    kind = "executable",
                    name = "empty_runtime",
                    command = ["true"],
                    runtime = {},
                },
                "states" = {
                    kind = "executable",
                    name = "states",
                    command = ["true"],
                    runtime = { payload_staging = "copy" },
                },
            },
            runtime = {
                retry_impure = true,
                environment = {
                    platform_inherited_env_vars = { linux = ["MEDIAPM_TEST_INHERITED"] },
                },
                payload_staging = "hard_link",
            },
        }"#;
        let downgraded: serde_json::Value =
            crate::config::nickel_io::migrate_document_source_to_version(
                source,
                3,
                "configuration document",
            )
            .expect("a v4 document must migrate down to v3");
        assert_eq!(downgraded["version"].as_f64(), Some(3.0), "the hop stamps v3");
        for tool in ["bare", "empty_runtime"] {
            assert_eq!(
                downgraded["tools"][tool]["runtime"]["payload_staging"], "hard_link",
                "a tool that inherited the default must carry it under v3: {downgraded}"
            );
        }
        assert_eq!(
            downgraded["tools"]["states"]["runtime"]["payload_staging"], "copy",
            "a tool that stated a strategy keeps it: {downgraded}"
        );
        assert!(
            downgraded["runtime"].get("payload_staging").is_none(),
            "v3 has no document-level default, so the hop drops it: {}",
            downgraded["runtime"]
        );
        assert_eq!(
            downgraded["runtime"]["retry_impure"].as_bool(),
            Some(true),
            "the hop drops one field of the document runtime, not the runtime: {downgraded}"
        );
        assert_eq!(
            downgraded["runtime"]["environment"]["platform_inherited_env_vars"]["linux"][0],
            "MEDIAPM_TEST_INHERITED",
            "a name the document inherited must survive the downgrade: {downgraded}"
        );
    }

    /// The same hop leaves `in_place` absent, because that is what v3 already
    /// means by an absent field.
    #[test]
    fn regression_downgrading_to_v3_leaves_in_place_absent() {
        let source = r#"{
            version = 4,
            tools = {
                "t" = { kind = "executable", name = "t", command = ["true"] },
            },
        }"#;
        let downgraded: serde_json::Value =
            crate::config::nickel_io::migrate_document_source_to_version(
                source,
                3,
                "configuration document",
            )
            .expect("a v4 document must migrate down to v3");
        assert!(
            downgraded["tools"]["t"].get("payload_staging").is_none()
                && downgraded["tools"]["t"]
                    .get("runtime")
                    .and_then(|runtime| runtime.get("payload_staging"))
                    .is_none(),
            "in_place is what v3 means by absent, so the hop writes nothing: {downgraded}"
        );
    }
}
