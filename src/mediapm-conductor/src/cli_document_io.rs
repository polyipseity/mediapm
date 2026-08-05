//! Document I/O helpers for loading and saving conductor documents.
//!
//! Each document is a `.ncl` file that wraps a versioned Nickel envelope.
//! Loading evaluates the file through `nickel-lang-core`, saving renders
//! the document back through the latest-schema envelope.

use std::path::Path;

use crate::config::documents::NickelDocument;
use crate::error::ConductorError;

/// Loads a `NickelDocument` from a `.ncl` file path.
///
/// Reads the file, evaluates it through the versioned Nickel migration
/// pipeline, and returns the decoded document.
///
/// # Errors
///
/// Returns [`ConductorError::Io`] when the file cannot be read, or wraps
/// any Nickel evaluation or version‑migration error.
pub(crate) fn load_document(path: &Path) -> Result<NickelDocument, ConductorError> {
    let bytes = std::fs::read(path).map_err(|source| ConductorError::Io {
        operation: "reading config document".to_string(),
        path: path.to_path_buf(),
        source,
    })?;
    crate::config::versions::decode_document(&bytes)
}

/// Loads the raw latest-schema wire envelope from a `.ncl` file path.
///
/// Unlike [`load_document`], this applies NO boundary defaults, preserving
/// which fields were explicitly written in the source document.  Used by
/// multi-document merging (explicit beats implicit).
///
/// # Errors
///
/// Returns [`ConductorError::Io`] when the file cannot be read, or wraps
/// any Nickel evaluation or version‑migration error.
pub(crate) fn load_document_envelope(
    path: &Path,
) -> Result<crate::config::versions::v_latest::NickelEnvelopeLatest, ConductorError> {
    let bytes = std::fs::read(path).map_err(|source| ConductorError::Io {
        operation: "reading config document".to_string(),
        path: path.to_path_buf(),
        source,
    })?;
    crate::config::versions::decode_document_envelope(&bytes)
}

/// Saves a `NickelDocument` to a `.ncl` file.
///
/// Encodes the document through the latest‑schema envelope and writes the
/// resulting Nickel source to the given path.  Before encoding, human-readable
/// fields that are not mergeable — `external_data` descriptions and workflow
/// `display_name`/`description` — are preserved from the file being
/// overwritten (per hash / per name), so re-saving a rebuilt document does not
/// lose them.  Fresh entries stay `None` and are omitted from the output.
///
/// # Errors
///
/// Returns [`ConductorError::Io`] when the file cannot be written, or wraps
/// any encoding error.
pub(crate) fn save_document(path: &Path, document: &NickelDocument) -> Result<(), ConductorError> {
    let mut outgoing = document.clone();
    if let Ok(old) = load_document_envelope(path) {
        preserve_readable_fields(&mut outgoing, &old);
    }
    let bytes = crate::config::versions::encode_document(outgoing)?;
    std::fs::write(path, &bytes).map_err(|source| ConductorError::Io {
        operation: "writing config document".to_string(),
        path: path.to_path_buf(),
        source,
    })
}

/// Copies human-readable fields from the old wire envelope onto the outgoing
/// document for entries that already exist there; fresh entries keep `None`.
///
/// Only fills fields that are currently `None` — an explicit value in the
/// outgoing document always wins (explicit beats implicit).
fn preserve_readable_fields(
    outgoing: &mut NickelDocument,
    old: &crate::config::versions::v_latest::NickelEnvelopeLatest,
) {
    for (hash, old_entry) in &old.external_data {
        if let Some(entry) =
            outgoing.external_data.get_mut(hash).filter(|entry| entry.description.is_none())
        {
            entry.description.clone_from(&old_entry.description);
        }
    }
    for old_workflow in &old.workflows {
        if let Some(new_workflow) =
            outgoing.workflows.iter_mut().find(|w| w.name == old_workflow.name)
        {
            if new_workflow.display_name.is_none() {
                new_workflow.display_name.clone_from(&old_workflow.display_name);
            }
            if new_workflow.description.is_none() {
                new_workflow.description.clone_from(&old_workflow.description);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use mediapm_cas::Hash;

    use super::*;
    use crate::config::versions::v_latest::{
        ConductorRuntimeConfigLatest, NICKEL_VERSION_LATEST, NickelEnvelopeLatest,
    };

    /// Builds an empty latest-schema wire envelope.
    fn envelope() -> NickelEnvelopeLatest {
        NickelEnvelopeLatest {
            version: NICKEL_VERSION_LATEST,
            tools: BTreeMap::new(),
            workflows: Vec::new(),
            external_data: BTreeMap::new(),
            runtime: ConductorRuntimeConfigLatest::default(),
        }
    }

    /// Verifies that saving a rebuilt document preserves the external-data
    /// description of the file being overwritten (Q-A fold-in: human-readable
    /// fields survive re-save; explicit outgoing values win).
    #[test]
    fn save_preserves_old_file_external_data_description() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("conductor.ncl");
        let hash = Hash::from_content(b"payload");

        let mut env = envelope();
        env.external_data.insert(
            hash,
            crate::config::versions::v_latest::ExternalDataEntryLatest {
                hash: Some(hash),
                description: Some("original description".to_string()),
                save_mode: None,
            },
        );
        let first: NickelDocument = env.into();
        save_document(&path, &first).unwrap();

        // Rebuilt document: same hash, description lost (None).
        let mut rebuilt_env = envelope();
        rebuilt_env.external_data.insert(
            hash,
            crate::config::versions::v_latest::ExternalDataEntryLatest {
                hash: Some(hash),
                description: None,
                save_mode: None,
            },
        );
        let rebuilt: NickelDocument = rebuilt_env.into();
        save_document(&path, &rebuilt).unwrap();

        let loaded = load_document(&path).unwrap();
        assert_eq!(
            loaded.external_data.get(&hash).unwrap().description.as_deref(),
            Some("original description")
        );
    }

    /// Verifies that saving preserves workflow `display_name`/`description`
    /// from the file being overwritten.
    #[test]
    fn save_preserves_old_file_workflow_human_fields() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("conductor.ncl");

        let mut env = envelope();
        env.workflows.push(crate::config::versions::v_latest::WorkflowSpecLatest {
            name: "w".to_string(),
            display_name: Some("Human name".to_string()),
            description: Some("Human description".to_string()),
            impure: false,
            steps: Vec::new(),
        });
        let first: NickelDocument = env.into();
        save_document(&path, &first).unwrap();

        // Rebuilt document: same workflow name, human fields lost (None).
        let mut rebuilt_env = envelope();
        rebuilt_env.workflows.push(crate::config::versions::v_latest::WorkflowSpecLatest {
            name: "w".to_string(),
            display_name: None,
            description: None,
            impure: false,
            steps: Vec::new(),
        });
        let rebuilt: NickelDocument = rebuilt_env.into();
        save_document(&path, &rebuilt).unwrap();

        let loaded = load_document(&path).unwrap();
        assert_eq!(loaded.workflows[0].display_name.as_deref(), Some("Human name"));
        assert_eq!(loaded.workflows[0].description.as_deref(), Some("Human description"));
    }

    /// Verifies that an explicit description in the outgoing document wins
    /// over the old file's description (explicit beats implicit).
    #[test]
    fn save_outgoing_explicit_description_wins() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("conductor.ncl");
        let hash = Hash::from_content(b"payload");

        let mut env = envelope();
        env.external_data.insert(
            hash,
            crate::config::versions::v_latest::ExternalDataEntryLatest {
                hash: Some(hash),
                description: Some("old description".to_string()),
                save_mode: None,
            },
        );
        let first: NickelDocument = env.into();
        save_document(&path, &first).unwrap();

        let mut new_env = envelope();
        new_env.external_data.insert(
            hash,
            crate::config::versions::v_latest::ExternalDataEntryLatest {
                hash: Some(hash),
                description: Some("new description".to_string()),
                save_mode: None,
            },
        );
        let new_doc: NickelDocument = new_env.into();
        save_document(&path, &new_doc).unwrap();

        let loaded = load_document(&path).unwrap();
        assert_eq!(
            loaded.external_data.get(&hash).unwrap().description.as_deref(),
            Some("new description")
        );
    }

    /// Verifies that a fresh file keeps `None` descriptions (no stale fill).
    #[test]
    fn save_fresh_file_keeps_none_descriptions() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fresh.ncl");

        let mut env = envelope();
        env.external_data.insert(
            Hash::from_content(b"payload"),
            crate::config::versions::v_latest::ExternalDataEntryLatest {
                hash: None,
                description: None,
                save_mode: None,
            },
        );
        let doc: NickelDocument = env.into();
        save_document(&path, &doc).unwrap();

        let loaded = load_document(&path).unwrap();
        assert_eq!(loaded.external_data.len(), 1);
        let entry = loaded.external_data.values().next().unwrap();
        assert_eq!(entry.description, None);
    }
}
