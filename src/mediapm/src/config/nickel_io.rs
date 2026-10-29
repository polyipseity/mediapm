//! Nickel config evaluation and rendering utilities.

use std::path::Path;

use nickel_lang_core::eval::cache::CacheImpl;
use nickel_lang_core::program::ProgramBuilder;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::error::MediaPmError;

/// Evaluates one Nickel source file at `path` to its exported JSON value.
///
/// # Errors
///
/// Returns [`MediaPmError::Workflow`] when the Nickel program cannot be built
/// or evaluated, and [`MediaPmError::ConfigValidation`] (MPM-E004) when the
/// evaluated value cannot be rendered as JSON.
pub fn evaluate_nickel_source_to_json(path: &Path) -> Result<Value, MediaPmError> {
    let mut program = ProgramBuilder::new()
        .add_path(path)
        .build::<CacheImpl>()
        .map_err(|err| MediaPmError::Workflow(format!("failed to create Nickel program: {err}")))?;

    let nickel_value = program.eval_full_for_export().map_err(|err| {
        MediaPmError::Workflow(format!("failed to evaluate '{}': {err:?}", path.display()))
    })?;

    serde_json::to_value(&nickel_value).map_err(|err| MediaPmError::ConfigValidation {
        code: "MPM-E004",
        context: format!("failed to render Nickel value from '{}'", path.display()),
        detail: err.to_string(),
        suggestion: String::new(),
    })
}

/// Parses one floating-point value into `u64` when it is a non-negative
/// integer within representable range.
#[must_use]
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::float_cmp
)]
pub fn parse_non_negative_integral_u64(value: f64) -> Option<u64> {
    if value.is_sign_negative() || value.is_nan() || value.is_infinite() {
        return None;
    }
    if value.fract() != 0.0 {
        return None;
    }
    let unsigned = value as u64;
    if unsigned as f64 != value {
        return None;
    }
    Some(unsigned)
}

/// Parses one floating-point value into `u32` when it is a non-negative
/// integer within representable range.
#[must_use]
pub fn parse_non_negative_integral_u32(value: f64) -> Option<u32> {
    let raw_u64 = parse_non_negative_integral_u64(value)?;
    u32::try_from(raw_u64).ok()
}

/// Normalizes a Nickel-version-field document-level `version` marker.
///
/// Accepts both integer and floating-point JSON encoded version markers.
#[must_use]
pub fn normalize_version_field_to_u64(value: &serde_json::Value) -> Option<u64> {
    match value {
        serde_json::Value::Number(n) => {
            n.as_u64().or_else(|| n.as_f64().and_then(parse_non_negative_integral_u64))
        }
        _ => None,
    }
}

/// Loads and deserializes a JSON document from a Nickel source file.
fn load_json_document<T: DeserializeOwned>(path: &Path, label: &str) -> Result<T, MediaPmError> {
    let value = evaluate_nickel_source_to_json(path)?;
    serde_json::from_value(value).map_err(|err| MediaPmError::ConfigValidation {
        code: "MPM-E004",
        context: format!("failed to deserialize {label}"),
        detail: err.to_string(),
        suggestion: String::new(),
    })
}

/// Loads and parses a `mediapm.ncl` file.
///
/// The document is normalized on the way in, so a padded title or description
/// reaches the hierarchy already trimmed: those values become metadata
/// substitutions and literal path components, and whitespace that survived load
/// time would be part of the materialized path. A tool entry that states nothing
/// is dropped here for the same reason it is dropped when a document is encoded:
/// it carries no requirement.
///
/// # Errors
///
/// Returns an error when the document cannot be loaded or parsed.
pub fn load_mediapm_document(path: &Path) -> Result<crate::config::MediaPmDocument, MediaPmError> {
    let mut document: crate::config::MediaPmDocument =
        load_json_document(path, "mediapm document")?;
    document.normalize();
    Ok(document)
}

/// Serializes and writes a `mediapm.ncl` document as proper Nickel source.
///
/// The file is written as Nickel source (not JSON) because it is loaded back
/// via Nickel evaluation — JSON record syntax (`"key": value`) would parse as
/// contract annotations and fail.
///
/// # Errors
///
/// Returns an error when the document cannot be serialized or written.
pub fn save_mediapm_document(
    path: &Path,
    doc: &crate::config::MediaPmDocument,
) -> Result<(), MediaPmError> {
    let bytes = mediapm_utils::nickel::render_document_as_nickel(doc, "mediapm document")
        .map_err(MediaPmError::Serialization)?;
    std::fs::write(path, bytes).map_err(|err| MediaPmError::Io {
        operation: "write mediapm document".to_string(),
        path: path.to_path_buf(),
        source: err,
    })?;
    Ok(())
}

/// Loads and parses a `state.json` file.
///
/// Tries the JSON path first. If missing, looks for a legacy `state.ncl`
/// (derived by swapping the extension), evaluates it, migrates to JSON,
/// writes the result, and deletes the `.ncl` file. Returns
/// [`crate::config::MediaPmState::default`] when neither exists (first run or reset).
///
/// A document that names a schema version is decoded through that version's
/// reader, and a failure there is reported rather than swallowed: its contents
/// were written to hold data, and reading it as empty would discard that data
/// and let the next scan treat every recorded file as stale. A document that
/// names no version is refused for the same reason: every writer here emits the
/// field first, so a file without one is foreign or damaged, and there is no
/// rung of the ladder whose reader could be trusted with it. The only state a
/// load rebuilds is the one that does not exist.
///
/// # Errors
///
/// Returns [`MediaPmError::Serialization`] when the file cannot be read as JSON
/// or does not match the wire shape of the version it names, and
/// [`MediaPmError::Workflow`] when it names no version, or one this build does
/// not know.
pub fn load_mediapm_state_document(
    path: &Path,
) -> Result<crate::config::MediaPmState, MediaPmError> {
    // 1. Try JSON path.
    if path.exists() {
        let file = std::fs::File::open(path).map_err(|e| MediaPmError::Io {
            operation: "open mediapm state".to_string(),
            path: path.to_path_buf(),
            source: e,
        })?;
        let value: serde_json::Value = serde_json::from_reader(file).map_err(|e| {
            MediaPmError::Serialization(format!(
                "failed to parse mediapm state JSON at '{}': {e}; the file is left in place \
                 because a document that cannot be read may name a version whose contents \
                 would be lost by rebuilding it",
                path.display(),
            ))
        })?;
        if value.get("version").is_none() {
            return Err(MediaPmError::Workflow(format!(
                "mediapm state document at '{}' names no schema version; every writer here emits \
                 one, so the file is either foreign or damaged, and reading it as empty would \
                 discard the files it records",
                path.display(),
            )));
        }
        return crate::state::ser::from_json_value(value);
    }

    // 2. Try legacy Nickel path (.ncl).
    let nickel_path = path.with_extension("ncl");
    if nickel_path.exists() {
        let value = evaluate_nickel_source_to_json(&nickel_path)?;
        let state = crate::state::ser::migrate_from_old_nickel(value)?;

        // Save migrated state as JSON.
        save_mediapm_state_document(path, &state)?;

        // Remove the old .ncl file.
        std::fs::remove_file(&nickel_path).map_err(|e| MediaPmError::Io {
            operation: "remove legacy state.ncl".to_string(),
            path: nickel_path,
            source: e,
        })?;

        return Ok(state);
    }

    // 3. Neither exists — return default.
    Ok(crate::config::MediaPmState::default())
}

/// Serializes and writes a `state.json` document as pretty-printed JSON.
///
/// The document is published by rename, so a save that dies partway leaves the
/// previous document at the path rather than a prefix of the new one. That
/// matters here more than it would for a scratch file: a `state.json` this build
/// cannot parse is an error, not a rebuild, so a torn write would leave a
/// workspace that does not open.
///
/// # Errors
///
/// Returns an error when the state cannot be serialized, and an I/O error when
/// the document cannot be published — including the case where its directory is
/// gone, which this path does not create.
pub fn save_mediapm_state_document(
    path: &Path,
    state: &crate::config::MediaPmState,
) -> Result<(), MediaPmError> {
    let value = crate::state::ser::to_json_value(state)?;
    let json = serde_json::to_string_pretty(&value)
        .map_err(|e| MediaPmError::Serialization(format!("failed to format state JSON: {e}")))?;
    mediapm_utils::fs::write_bytes_atomically(path, json.as_bytes()).map_err(|source| {
        MediaPmError::Io {
            operation: "write mediapm state".to_string(),
            path: path.to_path_buf(),
            source,
        }
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    /// Writes one state document to `state.json` under a fresh artifact root.
    ///
    /// Returns the temp root and the path to the file. The root must outlive the
    /// test body: dropping it removes the document the assertions read back.
    fn state_file_with(contents: &[u8]) -> (tempfile::TempDir, std::path::PathBuf) {
        let root = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let path = root.path().join("state.json");
        std::fs::write(&path, contents).expect("write state document");
        (root, path)
    }

    /// Builds a state document holding one recorded file.
    ///
    /// A save of this value writes bytes that differ from any hand-written
    /// fixture, which is what lets a test tell a replacement from an in-place
    /// rewrite.
    fn state_with_one_file() -> crate::config::MediaPmState {
        let mut state = crate::config::MediaPmState::default();
        state.managed_files.insert(
            "media/a.mkv".to_string(),
            crate::config::ManagedFileRecord {
                media_id: "a".to_string(),
                variant: "primary".to_string(),
                hash: "blake3:a".to_string(),
            },
        );
        state
    }

    /// A save replaces the document instead of rewriting it in place.
    ///
    /// A reader that opened `state.json` before a save keeps the document it
    /// opened. An in-place rewrite truncates that same file and writes over it,
    /// so the handle sees the new bytes and, on a crash midway, sees a prefix of
    /// them. A replacement by rename leaves the reader on the old document and
    /// publishes the new one to the next open, which is the guarantee a torn save
    /// needs: the only document that ever exists at the path is a whole one.
    #[test]
    fn a_save_replaces_the_document_instead_of_rewriting_it() {
        let original = b"{\"version\": 5, \"managed_files\": {}, \"managed_tools\": []}";
        let (root, path) = state_file_with(original);
        let mut open_handle =
            std::fs::File::open(&path).expect("open the published document before the save");

        save_mediapm_state_document(&path, &state_with_one_file()).expect("save over the document");

        let mut still_open = String::new();
        std::io::Read::read_to_string(&mut open_handle, &mut still_open)
            .expect("read the document the handle opened");
        assert_eq!(
            still_open.as_bytes(),
            original,
            "the handle kept open across the save must still hold the document it opened",
        );

        let published = std::fs::read(&path).expect("read the published document");
        assert!(
            published.windows(11).any(|window| window == b"media/a.mkv"),
            "the new document must be at the path: {}",
            String::from_utf8_lossy(&published),
        );
        let siblings: Vec<String> = std::fs::read_dir(root.path())
            .expect("read the state directory")
            .map(|entry| entry.expect("entry").file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            siblings,
            vec!["state.json".to_string()],
            "a save must leave the document and no temporary: {siblings:?}",
        );
    }

    /// A publish that dies after its temporary exists leaves a document the next load reads.
    ///
    /// The user-level guarantee: an interrupted sync leaves a library that still
    /// opens. `state.json` is parsed strictly now — a document this build cannot
    /// read is an error rather than a rebuild — so a save that truncated the
    /// target and stopped would cost the user the recorded files of every media in
    /// the library, not one run.
    ///
    /// The failure is injected at the seam the save uses, on the path the save
    /// uses, and the assertion goes through the real loader rather than through
    /// `std::fs::read`, so this composes the two halves a user depends on.
    #[test]
    fn a_failed_state_publish_leaves_a_document_the_next_load_reads() {
        use std::io::Write as _;

        let (root, path) = state_file_with(b"{}");
        let previous = state_with_one_file();
        save_mediapm_state_document(&path, &previous).expect("save the first document");

        let error = mediapm_utils::fs::write_atomically(&path, |file| {
            file.write_all(b"{\"version\": 5, \"managed_fil")?;
            Err(std::io::Error::other("the disk went away"))
        })
        .expect_err("a write that fails must be reported");

        assert_eq!(error.to_string(), "the disk went away", "the writer's error must survive");
        let reloaded =
            load_mediapm_state_document(&path).expect("the previous document must still load");
        assert_eq!(
            reloaded.managed_files, previous.managed_files,
            "the failed publish replaced the recorded files",
        );
        let siblings: Vec<String> = std::fs::read_dir(root.path())
            .expect("read the state directory")
            .map(|entry| entry.expect("entry").file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            siblings,
            vec!["state.json".to_string()],
            "the failed publish left a temporary behind: {siblings:?}",
        );
    }

    /// A V4 document loads and is written back as V5, without the field V5 removed.
    ///
    /// The user-level guarantee: a library written by a V4 build keeps every file
    /// and tool it recorded. The deleted per-media workflow map is the one thing
    /// that does not survive, because the conductor's instance records are now its
    /// only home and a second copy could disagree with them.
    #[test]
    fn a_v4_state_document_migrates_to_v5() {
        let document = json!({
            "version": 4,
            "managed_files": {
                "/media/a.mkv": { "media_id": "a", "variant": "primary", "hash": "blake3:a" }
            },
            "managed_tools": [{
                "tool_id": "ffmpeg",
                "version": "7.1",
                "canonical_version": "ffmpeg-v7.1",
                "content_map_hash": "blake3:abc",
                "deployed_at": 1000
            }],
            "workflow_states": {
                "a": { "variant_hashes": { "primary": "blake3:p" }, "steps_completed": 2 }
            }
        });
        let (_root, path) = state_file_with(document.to_string().as_bytes());

        let state = load_mediapm_state_document(&path).expect("a V4 document must load");
        assert_eq!(state.version, 5, "the loader must resolve into the current runtime version");
        assert_eq!(state.managed_files.len(), 1, "the recorded files must survive the migration");
        assert_eq!(state.managed_tools.len(), 1, "the recorded tools must survive the migration");

        save_mediapm_state_document(&path, &state).expect("save the migrated state");
        let written: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).expect("read back")).expect("parse back");

        assert_eq!(written["version"], 5, "the ladder is one-way forward");
        assert!(
            written.get("workflow_states").is_none(),
            "V5 has no per-media workflow map, so it must not be written: {written}"
        );
        assert_eq!(
            written["managed_files"]["/media/a.mkv"]["hash"], "blake3:a",
            "the migrated document must still record the file it materialized: {written}"
        );
    }

    /// A state document naming a version this build does not know fails loudly.
    ///
    /// A newer writer must never have its state silently re-read as an older
    /// shape, and the file is left untouched for the build that wrote it.
    #[test]
    fn a_state_document_naming_an_unknown_version_is_an_error() {
        let (_root, path) = state_file_with(json!({ "version": 99 }).to_string().as_bytes());

        let error = load_mediapm_state_document(&path).expect_err("version 99 must not load");
        assert!(
            error.to_string().contains("99"),
            "the diagnostic must name the rejected version: {error}"
        );
        let on_disk = std::fs::read(&path).expect("the file must still be there");
        assert_eq!(
            on_disk,
            json!({ "version": 99 }).to_string().as_bytes(),
            "a rejected document must not be rewritten"
        );
    }

    /// A corrupt body that names a known version reports instead of rebuilding.
    ///
    /// The distinction the guard draws: the document states which ladder rung it
    /// belongs to, so its contents were written to hold data. Reading it as empty
    /// would discard that data and let the next scan treat every recorded file as
    /// stale.
    #[test]
    fn a_corrupt_state_document_naming_a_known_version_is_an_error() {
        let corrupt = json!({ "version": 4, "managed_files": { "a": 1 } });
        let (_root, path) = state_file_with(corrupt.to_string().as_bytes());

        let error =
            load_mediapm_state_document(&path).expect_err("a corrupt V4 body must not rebuild");
        assert!(
            error.to_string().contains("V4"),
            "the diagnostic must name the version it decoded as: {error}"
        );
        let on_disk = std::fs::read(&path).expect("the file must still be there");
        assert_eq!(on_disk, corrupt.to_string().as_bytes(), "a rejected file is never rewritten");
    }

    /// A versionless state document is refused rather than rebuilt.
    ///
    /// Every writer here emits `version` first, so a file without one is foreign
    /// or damaged. Rebuilding it would read the document as empty, discard the
    /// files it records, and let the next scan treat each of them as stale.
    #[test]
    fn a_versionless_state_document_is_an_error() {
        let versionless = json!({ "managed_files": {} });
        let (_root, path) = state_file_with(versionless.to_string().as_bytes());

        let error = load_mediapm_state_document(&path)
            .expect_err("a versionless document must not be read as empty");
        assert!(
            error.to_string().contains("names no schema version"),
            "the diagnostic must say what is missing: {error}"
        );
        let on_disk = std::fs::read(&path).expect("the file must still be there");
        assert_eq!(
            on_disk,
            versionless.to_string().as_bytes(),
            "a refused document is never rewritten"
        );
    }
}
