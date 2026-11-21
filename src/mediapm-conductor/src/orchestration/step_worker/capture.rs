//! Captures declared step outputs from execution results and persists to CAS.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use mediapm_cas::{CasApi, ConstraintApi};

use crate::config::{OutputCaptureSpec, SaveMode};
use crate::error::ConductorError;
use crate::state::CapturedOutput;

use super::process::ExecutionResult;

/// Returns the stream captures every execution records without a declaration.
///
/// `stdout`, `stderr` and `process_code` are always recorded, under a fixed spec
/// each, so a name-addressed capture set is never the whole set of names an
/// instance holds. This function is the single definition of that implicit set:
/// the capture phase records exactly these, and the executor's probe treats them
/// as producible for a step that binds one by name — otherwise a dependent
/// referencing `${step_output.<step>.stdout}` would be filtered out of the
/// probe's CAS presence check even though every instance of that work records
/// the name.
pub(crate) fn implicit_stream_specs() -> BTreeMap<String, OutputCaptureSpec> {
    [
        ("stdout", crate::config::CaptureKind::Stdout),
        ("stderr", crate::config::CaptureKind::Stderr),
        ("process_code", crate::config::CaptureKind::ProcessCode),
    ]
    .into_iter()
    .map(|(name, capture)| {
        let spec = OutputCaptureSpec {
            name: name.to_string(),
            capture,
            save: SaveMode::True,
            allow_empty: false,
        };
        (spec.name.clone(), spec)
    })
    .collect()
}

/// Captured step outputs: content-addressed records, each with its spec.
#[derive(Debug, Clone)]
pub(super) struct CapturedOutputs {
    /// Captured records keyed by output name, each carrying the spec that
    /// produced it. Captured outputs are always deterministic content
    /// (results, never preconditions).
    pub records: BTreeMap<String, CapturedOutput>,
}

/// Recursively walks a directory and returns all file paths found.
async fn walk_and_collect_file_paths(root: &Path) -> Result<Vec<PathBuf>, ConductorError> {
    let mut file_paths = Vec::new();
    let mut dir_entries = vec![root.to_path_buf()];
    while let Some(dir) = dir_entries.pop() {
        if !dir.exists() {
            continue;
        }
        let mut read_dir = tokio::fs::read_dir(&dir).await.map_err(|e| {
            ConductorError::Workflow(format!("failed to read directory '{}': {e}", dir.display()))
        })?;
        while let Some(entry) = read_dir
            .next_entry()
            .await
            .map_err(|e| ConductorError::Workflow(format!("failed to read entry: {e}")))?
        {
            if entry.file_type().await.is_ok_and(|t| t.is_dir()) {
                dir_entries.push(entry.path());
            } else {
                file_paths.push(entry.path());
            }
        }
    }
    Ok(file_paths)
}

/// Normalizes one sandbox file path to a forward-slash relative path.
fn sandbox_relative_path(path: &Path, sandbox_dir: &Path) -> Option<String> {
    path.strip_prefix(sandbox_dir)
        .ok()
        .map(|relative| relative.to_string_lossy().replace('\\', "/"))
}

/// Resolution of one capture attempt before CAS storage.
///
/// `AllowedEmpty` represents an allowed capture that matched nothing: the caller
/// must record `hash_nk: None` and store nothing (no `cas.put`, no keep-full
/// mark). `Missing` means nothing matched and the spec is strict; the dispatch
/// arm turns it into a step failure naming the output, the capture token, the
/// sandbox path, and the fact that `allow_empty` is false.
enum CaptureResolution {
    /// Captured bytes to persist to CAS.
    Store(Vec<u8>),
    /// Nothing matched and the spec allowed it.
    AllowedEmpty,
    /// Nothing matched and the spec did not allow it.
    Missing,
}

async fn capture_file_regex_output(
    regex: &regex::Regex,
    sandbox_dir: &Path,
    allow_empty: bool,
) -> Result<CaptureResolution, ConductorError> {
    let file_paths = walk_and_collect_file_paths(sandbox_dir).await?;
    let matched_paths = file_paths
        .iter()
        .filter(|path| {
            sandbox_relative_path(path, sandbox_dir)
                .is_some_and(|relative| regex.is_match(&relative))
        })
        .collect::<Vec<_>>();
    match matched_paths.len() {
        0 if allow_empty => Ok(CaptureResolution::AllowedEmpty),
        0 => Ok(CaptureResolution::Missing),
        1 => {
            let data = tokio::fs::read(matched_paths[0]).await.map_err(|e| {
                ConductorError::Workflow(format!(
                    "failed to read matched file '{}': {e}",
                    matched_paths[0].display()
                ))
            })?;
            Ok(CaptureResolution::Store(data))
        }
        _ => Err(ConductorError::Workflow(format!(
            "file_regex capture matched multiple sandbox files: {:?}",
            matched_paths
                .iter()
                .filter_map(|path| sandbox_relative_path(path, sandbox_dir))
                .collect::<Vec<_>>()
        ))),
    }
}

/// Archives sandbox files referenced by forward-slash relative paths into one ZIP blob.
#[cfg(test)]
async fn archive_sandbox_relative_files_as_zip(
    sandbox_dir: &Path,
    relative_paths: &[String],
) -> Result<Vec<u8>, ConductorError> {
    archive_sandbox_relative_files_as_zip_with_names(sandbox_dir, relative_paths, relative_paths)
        .await
}

/// Archives sandbox files into one ZIP blob using explicit archive entry names.
async fn archive_sandbox_relative_files_as_zip_with_names(
    sandbox_dir: &Path,
    archive_names: &[String],
    source_relative_paths: &[String],
) -> Result<Vec<u8>, ConductorError> {
    use std::io::Write;

    let cursor = std::io::Cursor::new(Vec::new());
    let mut writer = zip::ZipWriter::new(cursor);
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);

    for (entry_name, relative) in archive_names.iter().zip(source_relative_paths.iter()) {
        let full_path = sandbox_dir.join(relative);
        let bytes = tokio::fs::read(&full_path).await.map_err(|source| {
            ConductorError::Workflow(format!(
                "failed to read sandbox file '{}' for folder capture: {source}",
                full_path.display()
            ))
        })?;
        let entry_name = entry_name.replace('\\', "/");
        writer.start_file(entry_name.clone(), options).map_err(|source| {
            ConductorError::Workflow(format!("failed to start ZIP entry '{entry_name}': {source}"))
        })?;
        writer.write_all(&bytes).map_err(|source| {
            ConductorError::Workflow(format!("failed to write ZIP entry '{entry_name}': {source}"))
        })?;
    }

    let cursor = writer.finish().map_err(|source| {
        ConductorError::Workflow(format!("failed to finish ZIP archive: {source}"))
    })?;
    Ok(cursor.into_inner())
}

async fn capture_folder_regex_output(
    regex: &regex::Regex,
    sandbox_dir: &Path,
    allow_empty: bool,
    include_topmost: bool,
) -> Result<CaptureResolution, ConductorError> {
    let file_paths = walk_and_collect_file_paths(sandbox_dir).await?;
    let mut entries: Vec<(String, String)> = file_paths
        .iter()
        .filter_map(|path| sandbox_relative_path(path, sandbox_dir))
        .filter(|relative| regex.is_match(relative))
        .filter_map(|relative| {
            let archive_name = if include_topmost {
                relative.clone()
            } else {
                relative.split_once('/').map(|(_, rest)| rest.to_string()).unwrap_or_default()
            };
            if archive_name.is_empty() { None } else { Some((archive_name, relative)) }
        })
        .collect();
    entries.sort();
    entries.dedup_by(|a, b| a.0 == b.0);
    let file_list: Vec<String> = entries.iter().map(|(_, source)| source.clone()).collect();
    let archive_names: Vec<String> = entries.into_iter().map(|(name, _)| name).collect();
    if file_list.is_empty() {
        if allow_empty {
            return Ok(CaptureResolution::AllowedEmpty);
        }
        return Ok(CaptureResolution::Missing);
    }
    archive_sandbox_relative_files_as_zip_with_names(sandbox_dir, &archive_names, &file_list)
        .await
        .map(CaptureResolution::Store)
}

async fn capture_folder_output(
    sandbox_dir: &Path,
    relative_path: &str,
    allow_empty: bool,
    include_topmost: bool,
) -> Result<CaptureResolution, ConductorError> {
    let full_path = sandbox_dir.join(relative_path);
    let file_paths = walk_and_collect_file_paths(&full_path).await?;
    let mut entries: Vec<(String, String)> = if include_topmost {
        file_paths
            .iter()
            .filter_map(|p| sandbox_relative_path(p, sandbox_dir))
            .map(|rel| (rel.clone(), rel))
            .collect()
    } else {
        let prefix = sandbox_dir.join(relative_path);
        file_paths
            .iter()
            .filter_map(|p| {
                let source = sandbox_relative_path(p, sandbox_dir)?;
                let archive_name =
                    p.strip_prefix(&prefix).ok().map(|r| r.to_string_lossy().replace('\\', "/"))?;
                Some((archive_name, source))
            })
            .collect()
    };
    entries.sort();
    let (archive_names, source_paths): (Vec<String>, Vec<String>) = entries.into_iter().unzip();
    if source_paths.is_empty() {
        if allow_empty {
            return Ok(CaptureResolution::AllowedEmpty);
        }
        return Ok(CaptureResolution::Missing);
    }
    archive_sandbox_relative_files_as_zip_with_names(sandbox_dir, &archive_names, &source_paths)
        .await
        .map(CaptureResolution::Store)
}

/// Resolves one capture spec against the execution result and sandbox.
///
/// `Stdout`, `stderr`, and `process_code` read from `execution`; file and folder
/// captures read from `sandbox_dir`. A missing allowed-empty match becomes
/// [`CaptureResolution::AllowedEmpty`]; a missing strict match becomes
/// [`CaptureResolution::Missing`].
async fn resolve_capture_spec(
    spec: &OutputCaptureSpec,
    execution: &ExecutionResult,
    sandbox_dir: &Path,
) -> Result<CaptureResolution, ConductorError> {
    match &spec.capture {
        crate::config::CaptureKind::Stdout => {
            Ok(CaptureResolution::Store(execution.stdout.clone()))
        }
        crate::config::CaptureKind::Stderr => {
            Ok(CaptureResolution::Store(execution.stderr.clone()))
        }
        crate::config::CaptureKind::ProcessCode => {
            Ok(CaptureResolution::Store(execution.exit_code.to_string().into_bytes()))
        }
        crate::config::CaptureKind::File(relative_path) => {
            let full_path = sandbox_dir.join(relative_path);
            match tokio::fs::read(&full_path).await {
                Ok(data) => Ok(CaptureResolution::Store(data)),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    if spec.allow_empty {
                        Ok(CaptureResolution::AllowedEmpty)
                    } else {
                        Ok(CaptureResolution::Missing)
                    }
                }
                Err(error) => Err(ConductorError::Workflow(format!(
                    "failed to read capture '{}' (token '{}') at '{}': {error}",
                    spec.name,
                    spec.capture.to_token(),
                    full_path.display(),
                ))),
            }
        }
        crate::config::CaptureKind::FileRegex(pattern) => {
            let regex = regex::Regex::new(pattern).map_err(|e| {
                ConductorError::Workflow(format!("invalid file_regex pattern '{pattern}': {e}"))
            })?;
            capture_file_regex_output(&regex, sandbox_dir, spec.allow_empty).await
        }
        crate::config::CaptureKind::FolderRegex(pattern) => {
            let regex = regex::Regex::new(pattern).map_err(|e| {
                ConductorError::Workflow(format!("invalid folder_regex pattern '{pattern}': {e}"))
            })?;
            capture_folder_regex_output(&regex, sandbox_dir, spec.allow_empty, true).await
        }
        crate::config::CaptureKind::FolderFlatRegex(pattern) => {
            let regex = regex::Regex::new(pattern).map_err(|e| {
                ConductorError::Workflow(format!("invalid folder_regex pattern '{pattern}': {e}"))
            })?;
            capture_folder_regex_output(&regex, sandbox_dir, spec.allow_empty, false).await
        }
        crate::config::CaptureKind::Folder(relative_path) => {
            capture_folder_output(sandbox_dir, relative_path, spec.allow_empty, true).await
        }
        crate::config::CaptureKind::FolderFlat(relative_path) => {
            capture_folder_output(sandbox_dir, relative_path, spec.allow_empty, false).await
        }
    }
}

/// Captures declared outputs from the execution result and persists to CAS.
///
/// Every record returned carries the spec that produced it, including the
/// implicitly captured `stdout`, `stderr` and `process_code`, which are
/// generated rather than declared. Different tools declare a `content` name
/// for different captures, so a name-addressed record is only reusable when
/// the spec beside it says the bytes were produced the way the reusing step
/// would produce them.
///
/// A capture whose spec says [`SaveMode::Full`] also marks its object keep-full
/// in the store, because the mark is the only record of what `full` promises:
/// that these bytes are never re-encoded as a delta against another blob.
///
/// # Errors
///
/// Returns [`ConductorError::Cas`] when the bytes cannot be stored, and when a
/// `full` capture's marker cannot be recorded. The mark fails the step rather
/// than being retried or ignored: a run that recorded a `full` capture without
/// its mark would leave the promise in the state document and nothing in the
/// store, and only the next maintenance pass would discover the difference.
pub(super) async fn capture_outputs<C: CasApi + ConstraintApi + Send + Sync>(
    cas: &C,
    output_specs: &BTreeMap<String, OutputCaptureSpec>,
    execution: &ExecutionResult,
    sandbox_dir: &Path,
) -> Result<CapturedOutputs, ConductorError> {
    let mut records = BTreeMap::new();

    // Implicitly capture stdout, stderr, and process_code unless explicitly declared.
    let declared_names: BTreeSet<&str> = output_specs.keys().map(String::as_str).collect();
    let implicit_specs = implicit_stream_specs()
        .into_iter()
        .filter(|(name, _)| !declared_names.contains(name.as_str()))
        .collect::<BTreeMap<String, OutputCaptureSpec>>();

    let combined_specs: BTreeMap<&str, &OutputCaptureSpec> = output_specs
        .iter()
        .map(|(name, spec)| (name.as_str(), spec))
        .chain(implicit_specs.iter().map(|(name, spec)| (name.as_str(), spec)))
        .collect();

    for spec in combined_specs.values() {
        let saved_nk = !matches!(spec.save, SaveMode::False);
        let resolution = resolve_capture_spec(spec, execution, sandbox_dir).await?;

        match resolution {
            CaptureResolution::Store(data) => {
                let hash = cas.put(bytes::Bytes::from(data)).await.map_err(ConductorError::Cas)?;

                // After the put, so the mark can only describe an object that exists.
                if spec.save == SaveMode::Full {
                    cas.mark_keep_full(hash).await.map_err(ConductorError::Cas)?;
                }

                records.insert(
                    spec.name.clone(),
                    CapturedOutput {
                        capture: spec.capture.to_token(),
                        hash_nk: Some(hash),
                        saved_nk,
                    },
                );
            }
            CaptureResolution::AllowedEmpty => {
                records.insert(
                    spec.name.clone(),
                    CapturedOutput { capture: spec.capture.to_token(), hash_nk: None, saved_nk },
                );
            }
            CaptureResolution::Missing => {
                return Err(ConductorError::Workflow(format!(
                    "required capture '{}' (token '{}') matched nothing in sandbox '{}' and allow_empty is false",
                    spec.name,
                    spec.capture.to_token(),
                    sandbox_dir.display(),
                )));
            }
        }
    }

    Ok(CapturedOutputs { records })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::OutputCaptureSpec;
    use crate::config::SaveMode;
    use std::collections::BTreeMap;

    #[tokio::test]
    async fn captures_stdout() {
        let cas = mediapm_cas::storage::in_memory::new_in_memory_cas();
        let tmp = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let execution =
            ExecutionResult { stdout: b"hello".to_vec(), stderr: Vec::new(), exit_code: 0 };
        let mut output_specs = BTreeMap::new();
        output_specs.insert(
            "stdout".to_string(),
            OutputCaptureSpec {
                name: "stdout".to_string(),
                capture: crate::config::CaptureKind::Stdout,
                save: SaveMode::True,
                allow_empty: false,
            },
        );
        let outputs = capture_outputs(&cas, &output_specs, &execution, tmp.path()).await.unwrap();
        let out = outputs.records.get("stdout").unwrap();
        let data = cas.get(out.hash_nk.expect("hash")).await.unwrap();
        assert_eq!(data.as_ref(), b"hello");
    }

    #[tokio::test]
    async fn captures_stderr() {
        let cas = mediapm_cas::storage::in_memory::new_in_memory_cas();
        let tmp = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let execution =
            ExecutionResult { stdout: Vec::new(), stderr: b"error output".to_vec(), exit_code: 1 };
        let mut output_specs = BTreeMap::new();
        output_specs.insert(
            "stderr".to_string(),
            OutputCaptureSpec {
                name: "stderr".to_string(),
                capture: crate::config::CaptureKind::Stderr,
                save: SaveMode::True,
                allow_empty: false,
            },
        );
        let outputs = capture_outputs(&cas, &output_specs, &execution, tmp.path()).await.unwrap();
        let out = outputs.records.get("stderr").unwrap();
        let data = cas.get(out.hash_nk.expect("hash")).await.unwrap();
        assert_eq!(data.as_ref(), b"error output");
    }

    #[tokio::test]
    async fn captures_process_code() {
        let cas = mediapm_cas::storage::in_memory::new_in_memory_cas();
        let tmp = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let execution = ExecutionResult { stdout: Vec::new(), stderr: Vec::new(), exit_code: 42 };
        let mut output_specs = BTreeMap::new();
        output_specs.insert(
            "process_code".to_string(),
            OutputCaptureSpec {
                name: "process_code".to_string(),
                capture: crate::config::CaptureKind::ProcessCode,
                save: SaveMode::True,
                allow_empty: false,
            },
        );
        let outputs = capture_outputs(&cas, &output_specs, &execution, tmp.path()).await.unwrap();
        let out = outputs.records.get("process_code").unwrap();
        let data = cas.get(out.hash_nk.expect("hash")).await.unwrap();
        assert_eq!(data.as_ref(), b"42");
    }

    #[tokio::test]
    async fn captures_file() {
        let cas = mediapm_cas::storage::in_memory::new_in_memory_cas();
        let tmp = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let file_path = tmp.path().join("test.txt");
        tokio::fs::write(&file_path, b"file content").await.unwrap();
        let execution = ExecutionResult { stdout: Vec::new(), stderr: Vec::new(), exit_code: 0 };
        let mut output_specs = BTreeMap::new();
        output_specs.insert(
            "test_file".to_string(),
            OutputCaptureSpec {
                name: "test_file".to_string(),
                capture: crate::config::CaptureKind::parse_token("file:test.txt")
                    .expect("valid capture"),
                save: SaveMode::True,
                allow_empty: false,
            },
        );
        let outputs = capture_outputs(&cas, &output_specs, &execution, tmp.path()).await.unwrap();
        let out = outputs.records.get("test_file").unwrap();
        let data = cas.get(out.hash_nk.expect("hash")).await.unwrap();
        assert_eq!(data.as_ref(), b"file content");
    }

    #[tokio::test]
    async fn captures_file_regex() {
        let cas = mediapm_cas::storage::in_memory::new_in_memory_cas();
        let tmp = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let file_path = tmp.path().join("result.log");
        tokio::fs::write(&file_path, b"regex match").await.unwrap();
        let execution = ExecutionResult { stdout: Vec::new(), stderr: Vec::new(), exit_code: 0 };
        let mut output_specs = BTreeMap::new();
        output_specs.insert(
            "log".to_string(),
            OutputCaptureSpec {
                name: "log".to_string(),
                capture: crate::config::CaptureKind::parse_token("file_regex:result\\.\\w+")
                    .expect("valid capture"),
                save: SaveMode::True,
                allow_empty: false,
            },
        );
        let outputs = capture_outputs(&cas, &output_specs, &execution, tmp.path()).await.unwrap();
        let out = outputs.records.get("log").unwrap();
        let data = cas.get(out.hash_nk.expect("hash")).await.unwrap();
        assert_eq!(data.as_ref(), b"regex match");
    }

    #[tokio::test]
    async fn captures_file_regex_against_sandbox_relative_paths() {
        let cas = mediapm_cas::storage::in_memory::new_in_memory_cas();
        let tmp = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let downloads = tmp.path().join("downloads");
        tokio::fs::create_dir(&downloads).await.unwrap();
        tokio::fs::write(
            downloads.join("Rick Astley [dQw4w9WgXcQ]__mediapm__.info.json"),
            b"infojson",
        )
        .await
        .unwrap();
        let execution = ExecutionResult { stdout: Vec::new(), stderr: Vec::new(), exit_code: 0 };
        let mut output_specs = BTreeMap::new();
        output_specs.insert(
            "yt_dlp_infojson_file".to_string(),
            OutputCaptureSpec {
                name: "yt_dlp_infojson_file".to_string(),
                capture: crate::config::CaptureKind::parse_token(
                    "file_regex:^downloads/.+(?:__mediapm__)?[.]info[.]json$",
                )
                .expect("valid capture"),
                save: SaveMode::True,
                allow_empty: false,
            },
        );
        let outputs = capture_outputs(&cas, &output_specs, &execution, tmp.path()).await.unwrap();
        let out = outputs.records.get("yt_dlp_infojson_file").unwrap();
        let data = cas.get(out.hash_nk.expect("hash")).await.unwrap();
        assert_eq!(data.as_ref(), b"infojson");
    }

    #[tokio::test]
    async fn captures_folder_regex() {
        let cas = mediapm_cas::storage::in_memory::new_in_memory_cas();
        let tmp = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let downloads = tmp.path().join("downloads");
        tokio::fs::create_dir(&downloads).await.unwrap();
        tokio::fs::write(downloads.join("video.en.vtt"), b"WEBVTT").await.unwrap();
        let execution = ExecutionResult { stdout: Vec::new(), stderr: Vec::new(), exit_code: 0 };
        let mut output_specs = BTreeMap::new();
        output_specs.insert(
            "yt_dlp_subtitle_artifacts".to_string(),
            OutputCaptureSpec {
                name: "yt_dlp_subtitle_artifacts".to_string(),
                capture: crate::config::CaptureKind::parse_token(
                    "folder_regex:^downloads/(.+?)(?:__mediapm__)?[.]vtt$",
                )
                .expect("valid capture"),
                save: SaveMode::True,
                allow_empty: false,
            },
        );
        let outputs = capture_outputs(&cas, &output_specs, &execution, tmp.path()).await.unwrap();
        let out = outputs.records.get("yt_dlp_subtitle_artifacts").unwrap();
        let data = cas.get(out.hash_nk.expect("hash")).await.unwrap();
        let mut archive = zip::ZipArchive::new(std::io::Cursor::new(data.as_ref())).unwrap();
        assert_eq!(archive.len(), 1);
        let mut entry = archive.by_index(0).unwrap();
        assert_eq!(entry.name(), "downloads/video.en.vtt");
        let mut bytes = Vec::new();
        std::io::Read::read_to_end(&mut entry, &mut bytes).unwrap();
        assert_eq!(bytes, b"WEBVTT");
    }

    #[tokio::test]
    async fn folder_regex_empty_capture_errors_when_not_allowed() {
        let cas = mediapm_cas::storage::in_memory::new_in_memory_cas();
        let tmp = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let execution = ExecutionResult { stdout: Vec::new(), stderr: Vec::new(), exit_code: 0 };
        let mut output_specs = BTreeMap::new();
        output_specs.insert(
            "yt_dlp_subtitle_artifacts".to_string(),
            OutputCaptureSpec {
                name: "yt_dlp_subtitle_artifacts".to_string(),
                capture: crate::config::CaptureKind::parse_token(
                    "folder_regex:^downloads/(.+?)(?:__mediapm__)?[.]vtt$",
                )
                .expect("valid capture"),
                save: SaveMode::True,
                allow_empty: false,
            },
        );
        let error = capture_outputs(&cas, &output_specs, &execution, tmp.path()).await.unwrap_err();
        let message = error.to_string();
        assert!(message.contains("yt_dlp_subtitle_artifacts"), "unexpected error: {message}");
        assert!(
            message.contains("folder_regex:^downloads/(.+?)(?:__mediapm__)?[.]vtt$"),
            "unexpected error: {message}"
        );
        assert!(message.contains(&tmp.path().display().to_string()), "unexpected error: {message}");
        assert!(message.contains("allow_empty is false"), "unexpected error: {message}");
    }

    #[tokio::test]
    async fn captures_folder() {
        let cas = mediapm_cas::storage::in_memory::new_in_memory_cas();
        let tmp = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let subdir = tmp.path().join("subdir");
        tokio::fs::create_dir(&subdir).await.unwrap();
        tokio::fs::write(subdir.join("a.txt"), b"content_a").await.unwrap();
        tokio::fs::write(subdir.join("b.txt"), b"content_b").await.unwrap();
        let execution = ExecutionResult { stdout: Vec::new(), stderr: Vec::new(), exit_code: 0 };
        let mut output_specs = BTreeMap::new();
        output_specs.insert(
            "folder_out".to_string(),
            OutputCaptureSpec {
                name: "folder_out".to_string(),
                capture: crate::config::CaptureKind::parse_token("folder:subdir")
                    .expect("valid capture"),
                save: SaveMode::True,
                allow_empty: false,
            },
        );
        let outputs = capture_outputs(&cas, &output_specs, &execution, tmp.path()).await.unwrap();
        let out = outputs.records.get("folder_out").unwrap();
        let data = cas.get(out.hash_nk.expect("hash")).await.unwrap();
        let mut archive = zip::ZipArchive::new(std::io::Cursor::new(data.as_ref())).unwrap();
        assert_eq!(archive.len(), 2);
        let mut names = Vec::new();
        for index in 0..archive.len() {
            names.push(archive.by_index(index).unwrap().name().to_string());
        }
        assert!(names.contains(&"subdir/a.txt".to_string()));
        assert!(names.contains(&"subdir/b.txt".to_string()));
    }

    #[tokio::test]
    async fn implicit_outputs() {
        let cas = mediapm_cas::storage::in_memory::new_in_memory_cas();
        let tmp = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let execution =
            ExecutionResult { stdout: b"hello".to_vec(), stderr: b"error".to_vec(), exit_code: 1 };
        let output_specs = BTreeMap::new();
        let outputs = capture_outputs(&cas, &output_specs, &execution, tmp.path()).await.unwrap();
        assert_eq!(outputs.records.len(), 3);
        let stdout_out = outputs.records.get("stdout").unwrap();
        let stderr_out = outputs.records.get("stderr").unwrap();
        let code_out = outputs.records.get("process_code").unwrap();
        assert_eq!(cas.get(stdout_out.hash_nk.expect("hash")).await.unwrap().as_ref(), b"hello");
        assert_eq!(cas.get(stderr_out.hash_nk.expect("hash")).await.unwrap().as_ref(), b"error");
        assert_eq!(cas.get(code_out.hash_nk.expect("hash")).await.unwrap().as_ref(), b"1");
    }

    /// A recorded output carries the exact spec that produced it, including
    /// the fields a selector alone cannot express.
    ///
    /// Capture addressing is by name, and a name says nothing about how the
    /// bytes were produced. A reusing step compares the spec it declares
    /// against this record, so a record carrying only the selector would let a
    /// tolerant capture satisfy a strict declaration and the reverse — the
    /// entry would look present while the bytes on disk were not what the
    /// declaration promised.
    #[tokio::test]
    async fn a_recorded_output_carries_the_spec_that_produced_it() {
        let cas = mediapm_cas::storage::in_memory::new_in_memory_cas();
        let tmp = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        tokio::fs::write(tmp.path().join("out.mkv"), b"video").await.unwrap();
        let downloads = tmp.path().join("downloads");
        tokio::fs::create_dir(&downloads).await.unwrap();
        tokio::fs::write(downloads.join("clip.vtt"), b"WEBVTT").await.unwrap();

        let file_spec = OutputCaptureSpec {
            name: "content".to_string(),
            capture: crate::config::CaptureKind::parse_token("file:out.mkv")
                .expect("valid capture"),
            save: SaveMode::False,
            allow_empty: true,
        };
        let folder_spec = OutputCaptureSpec {
            name: "artifacts".to_string(),
            capture: crate::config::CaptureKind::parse_token("folder_regex:^downloads/.+[.]vtt$")
                .expect("valid capture"),
            save: SaveMode::True,
            allow_empty: true,
        };
        let output_specs = BTreeMap::from([
            ("content".to_string(), file_spec.clone()),
            ("artifacts".to_string(), folder_spec.clone()),
        ]);
        let execution = ExecutionResult { stdout: Vec::new(), stderr: Vec::new(), exit_code: 0 };

        let outputs = capture_outputs(&cas, &output_specs, &execution, tmp.path())
            .await
            .expect("both captures match a sandbox file");

        assert_eq!(outputs.records["content"].capture, file_spec.capture.to_token());
        assert_eq!(outputs.records["artifacts"].capture, folder_spec.capture.to_token());
    }

    /// The always-captured process streams are generated, not declared, and
    /// each records the capture token that produced it.
    #[tokio::test]
    async fn an_implicitly_captured_stream_records_the_spec_that_produced_it() {
        let cas = mediapm_cas::storage::in_memory::new_in_memory_cas();
        let tmp = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let execution =
            ExecutionResult { stdout: b"hello".to_vec(), stderr: b"oops".to_vec(), exit_code: 3 };

        let outputs = capture_outputs(&cas, &BTreeMap::new(), &execution, tmp.path())
            .await
            .expect("implicit captures never fail");

        for name in ["stdout", "stderr", "process_code"] {
            assert_eq!(outputs.records[name].capture, name);
        }
    }

    /// A declared name overrides the implicit stream of the same name.
    ///
    /// `stdout` is captured implicitly only when nothing declares it, which is
    /// what lets a tool redirect a name onto a file: the declared spec decides
    /// both the bytes and the record, so a step binding `stdout` sees the
    /// declared capture rather than the process stream the name would default
    /// to. A merge that let the implicit spec win would record bytes under a
    /// spec neither side declared.
    #[tokio::test]
    async fn a_declared_stdout_overrides_the_implicit_stream() {
        let cas = mediapm_cas::storage::in_memory::new_in_memory_cas();
        let tmp = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        tokio::fs::write(tmp.path().join("out.txt"), b"from-file").await.unwrap();
        let execution =
            ExecutionResult { stdout: b"from-stream".to_vec(), stderr: Vec::new(), exit_code: 0 };
        let declared = OutputCaptureSpec {
            name: "stdout".to_string(),
            capture: crate::config::CaptureKind::parse_token("file:out.txt")
                .expect("valid capture"),
            save: SaveMode::True,
            allow_empty: false,
        };

        let outputs = capture_outputs(
            &cas,
            &BTreeMap::from([("stdout".to_string(), declared.clone())]),
            &execution,
            tmp.path(),
        )
        .await
        .expect("the declared capture reads the sandbox file");

        let record = &outputs.records["stdout"];
        assert_eq!(
            record.capture,
            declared.capture.to_token(),
            "the declared spec must be the spec recorded for the name",
        );
        assert_eq!(
            cas.get(record.hash_nk.expect("hash")).await.unwrap().as_ref(),
            b"from-file",
            "the declared capture, not the process stream, decides the bytes",
        );
    }

    /// A `folder:` capture includes the matched folder's topmost path in archive
    /// entries, while `folder_flat:` strips it. Both layouts must be honored.
    #[tokio::test]
    async fn folder_capture_honors_included_and_flat_layouts() {
        let cas = mediapm_cas::storage::in_memory::new_in_memory_cas();
        let tmp = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let subdir = tmp.path().join("subdir");
        tokio::fs::create_dir(&subdir).await.unwrap();
        tokio::fs::write(subdir.join("a.txt"), b"content_a").await.unwrap();

        let execution = ExecutionResult { stdout: Vec::new(), stderr: Vec::new(), exit_code: 0 };
        let output_specs = BTreeMap::from([
            (
                "with_top".to_string(),
                OutputCaptureSpec {
                    name: "with_top".to_string(),
                    capture: crate::config::CaptureKind::parse_token("folder:subdir")
                        .expect("valid capture"),
                    save: SaveMode::True,
                    allow_empty: false,
                },
            ),
            (
                "flat".to_string(),
                OutputCaptureSpec {
                    name: "flat".to_string(),
                    capture: crate::config::CaptureKind::parse_token("folder_flat:subdir")
                        .expect("valid capture"),
                    save: SaveMode::True,
                    allow_empty: false,
                },
            ),
        ]);

        let outputs = capture_outputs(&cas, &output_specs, &execution, tmp.path())
            .await
            .expect("folder captures should succeed");

        let with_top =
            cas.get(outputs.records["with_top"].hash_nk.expect("hash")).await.expect("stored");
        let mut archive = zip::ZipArchive::new(std::io::Cursor::new(with_top.as_ref())).unwrap();
        assert_eq!(archive.len(), 1);
        assert_eq!(archive.by_index(0).unwrap().name(), "subdir/a.txt");

        let flat = cas.get(outputs.records["flat"].hash_nk.expect("hash")).await.expect("stored");
        let mut archive = zip::ZipArchive::new(std::io::Cursor::new(flat.as_ref())).unwrap();
        assert_eq!(archive.len(), 1);
        assert_eq!(archive.by_index(0).unwrap().name(), "a.txt");
    }

    /// A `folder_regex:` capture keeps matched sandbox-relative paths as archive
    /// entries, while `folder_flat_regex:` strips the topmost path component.
    /// Both layouts must be honored.
    #[tokio::test]
    async fn folder_regex_capture_honors_included_and_flat_layouts() {
        let cas = mediapm_cas::storage::in_memory::new_in_memory_cas();
        let tmp = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let downloads = tmp.path().join("downloads");
        tokio::fs::create_dir(&downloads).await.unwrap();
        tokio::fs::write(downloads.join("video.en.vtt"), b"WEBVTT").await.unwrap();

        let execution = ExecutionResult { stdout: Vec::new(), stderr: Vec::new(), exit_code: 0 };
        let output_specs = BTreeMap::from([
            (
                "with_top".to_string(),
                OutputCaptureSpec {
                    name: "with_top".to_string(),
                    capture: crate::config::CaptureKind::parse_token(
                        "folder_regex:^downloads/.+[.]vtt$",
                    )
                    .expect("valid capture"),
                    save: SaveMode::True,
                    allow_empty: false,
                },
            ),
            (
                "flat".to_string(),
                OutputCaptureSpec {
                    name: "flat".to_string(),
                    capture: crate::config::CaptureKind::parse_token(
                        "folder_flat_regex:^downloads/.+[.]vtt$",
                    )
                    .expect("valid capture"),
                    save: SaveMode::True,
                    allow_empty: false,
                },
            ),
        ]);

        let outputs = capture_outputs(&cas, &output_specs, &execution, tmp.path())
            .await
            .expect("folder regex captures should succeed");

        let with_top =
            cas.get(outputs.records["with_top"].hash_nk.expect("hash")).await.expect("stored");
        let mut archive = zip::ZipArchive::new(std::io::Cursor::new(with_top.as_ref())).unwrap();
        assert_eq!(archive.len(), 1);
        assert_eq!(archive.by_index(0).unwrap().name(), "downloads/video.en.vtt");

        let flat = cas.get(outputs.records["flat"].hash_nk.expect("hash")).await.expect("stored");
        let mut archive = zip::ZipArchive::new(std::io::Cursor::new(flat.as_ref())).unwrap();
        assert_eq!(archive.len(), 1);
        assert_eq!(archive.by_index(0).unwrap().name(), "video.en.vtt");
    }

    /// A strict `file_regex` capture that matches nothing fails the step.
    #[tokio::test]
    async fn strict_file_regex_with_no_match_returns_error() {
        let cas = mediapm_cas::storage::in_memory::new_in_memory_cas();
        let tmp = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let execution = ExecutionResult { stdout: Vec::new(), stderr: Vec::new(), exit_code: 0 };
        let output_specs = BTreeMap::from([(
            "missing_log".to_string(),
            OutputCaptureSpec {
                name: "missing_log".to_string(),
                capture: crate::config::CaptureKind::parse_token("file_regex:^nope\\.txt$")
                    .expect("valid capture"),
                save: SaveMode::True,
                allow_empty: false,
            },
        )]);

        let error = capture_outputs(&cas, &output_specs, &execution, tmp.path()).await.unwrap_err();
        let message = error.to_string();
        assert!(message.contains("missing_log"), "unexpected error: {message}");
        assert!(message.contains("file_regex:^nope\\.txt$"), "unexpected error: {message}");
        assert!(message.contains(&tmp.path().display().to_string()), "unexpected error: {message}");
        assert!(message.contains("allow_empty is false"), "unexpected error: {message}");
    }

    /// A strict file capture for a missing file fails the step.
    #[tokio::test]
    async fn strict_file_capture_with_missing_file_returns_error() {
        let cas = mediapm_cas::storage::in_memory::new_in_memory_cas();
        let tmp = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let execution = ExecutionResult { stdout: Vec::new(), stderr: Vec::new(), exit_code: 0 };
        let output_specs = BTreeMap::from([(
            "missing_file".to_string(),
            OutputCaptureSpec {
                name: "missing_file".to_string(),
                capture: crate::config::CaptureKind::parse_token("file:absent.txt")
                    .expect("valid capture"),
                save: SaveMode::True,
                allow_empty: false,
            },
        )]);

        let error = capture_outputs(&cas, &output_specs, &execution, tmp.path()).await.unwrap_err();
        let message = error.to_string();
        assert!(message.contains("missing_file"), "unexpected error: {message}");
        assert!(message.contains("file:absent.txt"), "unexpected error: {message}");
        assert!(message.contains(&tmp.path().display().to_string()), "unexpected error: {message}");
        assert!(message.contains("allow_empty is false"), "unexpected error: {message}");
    }

    /// A strict folder capture that matches nothing fails the step.
    #[tokio::test]
    async fn strict_folder_capture_with_no_match_returns_error() {
        let cas = mediapm_cas::storage::in_memory::new_in_memory_cas();
        let tmp = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        tokio::fs::create_dir(tmp.path().join("empty")).await.unwrap();
        let execution = ExecutionResult { stdout: Vec::new(), stderr: Vec::new(), exit_code: 0 };
        let output_specs = BTreeMap::from([(
            "folder_out".to_string(),
            OutputCaptureSpec {
                name: "folder_out".to_string(),
                capture: crate::config::CaptureKind::parse_token("folder:empty")
                    .expect("valid capture"),
                save: SaveMode::True,
                allow_empty: false,
            },
        )]);

        let error = capture_outputs(&cas, &output_specs, &execution, tmp.path()).await.unwrap_err();
        let message = error.to_string();
        assert!(message.contains("folder_out"), "unexpected error: {message}");
        assert!(message.contains("folder:empty"), "unexpected error: {message}");
        assert!(message.contains(&tmp.path().display().to_string()), "unexpected error: {message}");
        assert!(message.contains("allow_empty is false"), "unexpected error: {message}");
    }

    /// A tolerant folder capture that matches nothing records null.
    #[tokio::test]
    async fn tolerant_folder_capture_with_no_match_records_null() {
        let cas = mediapm_cas::storage::in_memory::new_in_memory_cas();
        let tmp = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        tokio::fs::create_dir(tmp.path().join("empty")).await.unwrap();
        let execution = ExecutionResult { stdout: Vec::new(), stderr: Vec::new(), exit_code: 0 };
        let output_specs = BTreeMap::from([(
            "folder_out".to_string(),
            OutputCaptureSpec {
                name: "folder_out".to_string(),
                capture: crate::config::CaptureKind::parse_token("folder:empty")
                    .expect("valid capture"),
                save: SaveMode::True,
                allow_empty: true,
            },
        )]);

        let outputs = capture_outputs(&cas, &output_specs, &execution, tmp.path())
            .await
            .expect("tolerant folder capture should succeed");
        let record = outputs.records.get("folder_out").expect("recorded output");
        assert!(record.hash_nk.is_none(), "matched nothing => hash_nk must be null");
    }

    /// An allowed-empty file capture for a missing file records a null hash and
    /// stores nothing in CAS.
    #[tokio::test]
    async fn allowed_empty_missing_file_records_null_and_stores_nothing() {
        let tmp = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let cas_root = tmp.path().join("store");
        let cas = mediapm_cas::FileSystemCas::open(&cas_root).await.expect("open filesystem CAS");
        let execution =
            ExecutionResult { stdout: b"out".to_vec(), stderr: b"err".to_vec(), exit_code: 1 };
        let output_specs = BTreeMap::from([(
            "maybe".to_string(),
            OutputCaptureSpec {
                name: "maybe".to_string(),
                capture: crate::config::CaptureKind::parse_token("file:absent.txt")
                    .expect("valid capture"),
                save: SaveMode::Full,
                allow_empty: true,
            },
        )]);

        let outputs = capture_outputs(&cas, &output_specs, &execution, tmp.path())
            .await
            .expect("allowed-empty missing file capture should succeed");
        let record = outputs.records.get("maybe").expect("recorded output");
        assert!(record.hash_nk.is_none(), "missing file with allow_empty => hash_nk must be null");
        let would_store: &[u8] = b"";
        let blob_path = cas
            .object_path_for_hash(mediapm_cas::Hash::from_content(would_store))
            .expect("filesystem store exposes object paths");
        assert!(
            !blob_path.exists(),
            "no bytes may be stored for an allowed-empty capture that matched nothing"
        );
    }

    /// An allowed-empty file capture fails when the path exists but cannot be
    /// read as a file.
    #[tokio::test]
    async fn allowed_empty_unreadable_file_fails() {
        let cas = mediapm_cas::storage::in_memory::new_in_memory_cas();
        let tmp = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        tokio::fs::create_dir(tmp.path().join("not_a_file")).await.unwrap();
        let execution = ExecutionResult { stdout: Vec::new(), stderr: Vec::new(), exit_code: 0 };
        let output_specs = BTreeMap::from([(
            "maybe".to_string(),
            OutputCaptureSpec {
                name: "maybe".to_string(),
                capture: crate::config::CaptureKind::parse_token("file:not_a_file")
                    .expect("valid capture"),
                save: SaveMode::True,
                allow_empty: true,
            },
        )]);

        let error = capture_outputs(&cas, &output_specs, &execution, tmp.path()).await.unwrap_err();
        let message = error.to_string();
        assert!(message.contains("maybe"), "unexpected error: {message}");
        assert!(message.contains("file:not_a_file"), "unexpected error: {message}");
        assert!(
            message.contains(&tmp.path().join("not_a_file").display().to_string()),
            "unexpected error: {message}"
        );
        assert!(
            !message.contains("matched nothing"),
            "unreadable must not be reported as absence: {message}"
        );
    }

    /// A strict file capture fails when the path exists but cannot be read as a
    /// file, naming the path.
    #[tokio::test]
    async fn strict_unreadable_file_fails_naming_the_path() {
        let cas = mediapm_cas::storage::in_memory::new_in_memory_cas();
        let tmp = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        tokio::fs::create_dir(tmp.path().join("not_a_file")).await.unwrap();
        let execution = ExecutionResult { stdout: Vec::new(), stderr: Vec::new(), exit_code: 0 };
        let output_specs = BTreeMap::from([(
            "out".to_string(),
            OutputCaptureSpec {
                name: "out".to_string(),
                capture: crate::config::CaptureKind::parse_token("file:not_a_file")
                    .expect("valid capture"),
                save: SaveMode::True,
                allow_empty: false,
            },
        )]);

        let error = capture_outputs(&cas, &output_specs, &execution, tmp.path()).await.unwrap_err();
        let message = error.to_string();
        assert!(message.contains("out"), "unexpected error: {message}");
        assert!(message.contains("file:not_a_file"), "unexpected error: {message}");
        assert!(
            message.contains(&tmp.path().join("not_a_file").display().to_string()),
            "unexpected error: {message}"
        );
        assert!(
            !message.contains("matched nothing"),
            "unreadable must not be reported as absence: {message}"
        );
    }

    /// An allowed-empty `file_regex` capture that matches nothing records a null hash and stores nothing in CAS.
    #[tokio::test]
    async fn allowed_empty_file_regex_records_null_and_stores_nothing() {
        let tmp = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let cas_root = tmp.path().join("store");
        let cas = mediapm_cas::FileSystemCas::open(&cas_root).await.expect("open filesystem CAS");
        // Non-empty streams so their legitimate stored bytes do not collide with
        // the empty-bytes blob a buggy implementation would have stored for this
        // allowed-empty-no-match capture.
        let execution =
            ExecutionResult { stdout: b"out".to_vec(), stderr: b"err".to_vec(), exit_code: 0 };
        let output_specs = BTreeMap::from([(
            "log".to_string(),
            OutputCaptureSpec {
                name: "log".to_string(),
                capture: crate::config::CaptureKind::parse_token("file_regex:^nope\\.txt$")
                    .expect("valid capture"),
                save: SaveMode::Full,
                allow_empty: true,
            },
        )]);

        let outputs = capture_outputs(&cas, &output_specs, &execution, tmp.path())
            .await
            .expect("allowed-empty file_regex capture should succeed");
        let record = outputs.records.get("log").expect("recorded output");
        assert_eq!(record.capture, "file_regex:^nope\\.txt$");
        assert!(record.hash_nk.is_none(), "matched nothing => hash_nk must be null");
        assert!(record.saved_nk, "SaveMode::Full is a saved retention fact");
        let would_store: &[u8] = b"";
        let blob_path = cas
            .object_path_for_hash(mediapm_cas::Hash::from_content(would_store))
            .expect("filesystem store exposes object paths");
        assert!(
            !blob_path.exists(),
            "no bytes may be stored for an allowed-empty capture that matched nothing"
        );
    }

    /// An allowed-empty folder capture that matches nothing records a null hash
    /// and stores nothing in CAS.
    #[tokio::test]
    async fn allowed_empty_folder_records_null_and_stores_nothing() {
        let cas = mediapm_cas::storage::in_memory::new_in_memory_cas();
        let tmp = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        tokio::fs::create_dir(tmp.path().join("empty")).await.unwrap();
        let execution = ExecutionResult { stdout: Vec::new(), stderr: Vec::new(), exit_code: 0 };
        let output_specs = BTreeMap::from([(
            "out".to_string(),
            OutputCaptureSpec {
                name: "out".to_string(),
                capture: crate::config::CaptureKind::parse_token("folder:empty")
                    .expect("valid capture"),
                save: SaveMode::Full,
                allow_empty: true,
            },
        )]);

        let outputs = capture_outputs(&cas, &output_specs, &execution, tmp.path())
            .await
            .expect("allowed-empty folder capture should succeed");
        let record = outputs.records.get("out").expect("recorded output");
        assert!(record.hash_nk.is_none(), "matched nothing => hash_nk must be null");
        let would_store =
            archive_sandbox_relative_files_as_zip(tmp.path(), &[]).await.expect("empty archive");
        assert!(
            cas.stat(mediapm_cas::Hash::from_content(&would_store)).await.is_err(),
            "no bytes may be stored for an allowed-empty capture that matched nothing"
        );
    }

    /// An allowed-empty `folder_regex` capture that matches nothing records a null
    /// hash and stores nothing in CAS.
    #[tokio::test]
    async fn allowed_empty_folder_regex_records_null_and_stores_nothing() {
        let cas = mediapm_cas::storage::in_memory::new_in_memory_cas();
        let tmp = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let execution = ExecutionResult { stdout: Vec::new(), stderr: Vec::new(), exit_code: 0 };
        let output_specs = BTreeMap::from([(
            "subs".to_string(),
            OutputCaptureSpec {
                name: "subs".to_string(),
                capture: crate::config::CaptureKind::parse_token(
                    "folder_regex:^downloads/.+[.]vtt$",
                )
                .expect("valid capture"),
                save: SaveMode::Full,
                allow_empty: true,
            },
        )]);

        let outputs = capture_outputs(&cas, &output_specs, &execution, tmp.path())
            .await
            .expect("allowed-empty folder_regex capture should succeed");
        let record = outputs.records.get("subs").expect("recorded output");
        assert!(record.hash_nk.is_none(), "matched nothing => hash_nk must be null");
        let would_store =
            archive_sandbox_relative_files_as_zip(tmp.path(), &[]).await.expect("empty archive");
        assert!(
            cas.stat(mediapm_cas::Hash::from_content(&would_store)).await.is_err(),
            "no bytes may be stored for an allowed-empty capture that matched nothing"
        );
    }
}
