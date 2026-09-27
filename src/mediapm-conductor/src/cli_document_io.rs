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
    // A missing, unreadable, or unparseable previous file simply means there
    // are no human-readable fields to carry forward; the save itself proceeds.
    let _ = crate::config::versions::restore_readable_fields(&mut outgoing, path);
    let bytes = crate::config::versions::encode_document(outgoing)?;
    std::fs::write(path, &bytes).map_err(|source| ConductorError::Io {
        operation: "writing config document".to_string(),
        path: path.to_path_buf(),
        source,
    })
}

#[cfg(test)]
mod tests {
    use mediapm_cas::Hash;

    use super::*;
    use crate::config::{ExternalDataEntry, WorkflowSpec};
    use crate::state::OutputSaveMode;

    /// Builds a resolved document with no tools, workflows, or external data.
    fn empty_document() -> NickelDocument {
        NickelDocument::default()
    }

    /// Builds a resolved external-data entry.
    fn external_data(description: Option<&str>) -> ExternalDataEntry {
        ExternalDataEntry {
            description: description.map(ToOwned::to_owned),
            save_mode: OutputSaveMode::Saved,
        }
    }

    /// Builds a resolved workflow with the given human-readable fields.
    fn workflow(display_name: Option<&str>, description: Option<&str>) -> WorkflowSpec {
        WorkflowSpec {
            name: "w".to_string(),
            display_name: display_name.map(ToOwned::to_owned),
            description: description.map(ToOwned::to_owned),
            impure: false,
            steps: Vec::new(),
        }
    }

    /// Verifies that saving a rebuilt document preserves the external-data
    /// description of the file being overwritten (Q-A fold-in: human-readable
    /// fields survive re-save; explicit outgoing values win).
    #[test]
    fn save_preserves_old_file_external_data_description() {
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let path = dir.path().join("conductor.ncl");
        let hash = Hash::from_content(b"payload");

        let mut first = empty_document();
        first.external_data.insert(hash, external_data(Some("original description")));
        save_document(&path, &first).unwrap();

        // Rebuilt document: same hash, description lost (None).
        let mut rebuilt = empty_document();
        rebuilt.external_data.insert(hash, external_data(None));
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
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let path = dir.path().join("conductor.ncl");

        let mut first = empty_document();
        first.workflows.push(workflow(Some("Human name"), Some("Human description")));
        save_document(&path, &first).unwrap();

        // Rebuilt document: same workflow name, human fields lost (None).
        let mut rebuilt = empty_document();
        rebuilt.workflows.push(workflow(None, None));
        save_document(&path, &rebuilt).unwrap();

        let loaded = load_document(&path).unwrap();
        assert_eq!(loaded.workflows[0].display_name.as_deref(), Some("Human name"));
        assert_eq!(loaded.workflows[0].description.as_deref(), Some("Human description"));
    }

    /// Verifies that an explicit description in the outgoing document wins
    /// over the old file's description (explicit beats implicit).
    #[test]
    fn save_outgoing_explicit_description_wins() {
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let path = dir.path().join("conductor.ncl");
        let hash = Hash::from_content(b"payload");

        let mut first = empty_document();
        first.external_data.insert(hash, external_data(Some("old description")));
        save_document(&path, &first).unwrap();

        let mut new_doc = empty_document();
        new_doc.external_data.insert(hash, external_data(Some("new description")));
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
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let path = dir.path().join("fresh.ncl");

        let mut doc = empty_document();
        doc.external_data.insert(Hash::from_content(b"payload"), external_data(None));
        save_document(&path, &doc).unwrap();

        let loaded = load_document(&path).unwrap();
        assert_eq!(loaded.external_data.len(), 1);
        let entry = loaded.external_data.values().next().unwrap();
        assert_eq!(entry.description, None);
    }
}
