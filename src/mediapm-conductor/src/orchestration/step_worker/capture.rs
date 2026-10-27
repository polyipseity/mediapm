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
pub(super) fn implicit_stream_specs() -> BTreeMap<String, OutputCaptureSpec> {
    ["stdout", "stderr", "process_code"]
        .into_iter()
        .map(|name| {
            let spec = OutputCaptureSpec {
                name: name.to_string(),
                capture: name.to_string(),
                save: SaveMode::True,
                allow_empty: false,
                include_topmost_folder: true,
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

async fn capture_file_regex_output(
    regex: &regex::Regex,
    sandbox_dir: &Path,
    allow_empty: bool,
) -> Result<Option<Vec<u8>>, ConductorError> {
    let file_paths = walk_and_collect_file_paths(sandbox_dir).await?;
    let matched_paths = file_paths
        .iter()
        .filter(|path| {
            sandbox_relative_path(path, sandbox_dir)
                .is_some_and(|relative| regex.is_match(&relative))
        })
        .collect::<Vec<_>>();
    match matched_paths.len() {
        0 if allow_empty => Ok(Some(Vec::new())),
        0 => Ok(None),
        1 => {
            let data = tokio::fs::read(matched_paths[0]).await.map_err(|e| {
                ConductorError::Workflow(format!(
                    "failed to read matched file '{}': {e}",
                    matched_paths[0].display()
                ))
            })?;
            Ok(Some(data))
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
async fn archive_sandbox_relative_files_as_zip(
    sandbox_dir: &Path,
    relative_paths: &[String],
) -> Result<Vec<u8>, ConductorError> {
    use std::io::Write;

    let cursor = std::io::Cursor::new(Vec::new());
    let mut writer = zip::ZipWriter::new(cursor);
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);

    for relative in relative_paths {
        let full_path = sandbox_dir.join(relative);
        let bytes = tokio::fs::read(&full_path).await.map_err(|source| {
            ConductorError::Workflow(format!(
                "failed to read sandbox file '{}' for folder capture: {source}",
                full_path.display()
            ))
        })?;
        let entry_name = relative.replace('\\', "/");
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
) -> Result<Vec<u8>, ConductorError> {
    let file_paths = walk_and_collect_file_paths(sandbox_dir).await?;
    let file_list: Vec<String> = file_paths
        .iter()
        .filter_map(|path| sandbox_relative_path(path, sandbox_dir))
        .filter(|relative| regex.is_match(relative))
        .collect();
    if file_list.is_empty() && !allow_empty {
        return Err(ConductorError::Workflow(format!(
            "folder_regex capture matched zero sandbox files for pattern '{}'",
            regex.as_str()
        )));
    }
    archive_sandbox_relative_files_as_zip(sandbox_dir, &file_list).await
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
        let data = match spec.capture.as_str() {
            "stdout" => execution.stdout.clone(),
            "stderr" => execution.stderr.clone(),
            "process_code" => execution.exit_code.to_string().into_bytes(),
            capture if capture.starts_with("file:") => {
                let relative_path = &capture[5..];
                let full_path = sandbox_dir.join(relative_path);
                match tokio::fs::read(&full_path).await {
                    Ok(data) => data,
                    Err(_) if spec.allow_empty => Vec::new(),
                    Err(_) => continue,
                }
            }
            capture if capture.starts_with("file_regex:") => {
                let pattern = &capture[12..];
                let regex = regex::Regex::new(pattern).map_err(|e| {
                    ConductorError::Workflow(format!("invalid file_regex pattern '{pattern}': {e}"))
                })?;
                match capture_file_regex_output(&regex, sandbox_dir, spec.allow_empty).await? {
                    Some(data) => data,
                    None => continue,
                }
            }
            capture if capture.starts_with("folder_regex:") => {
                let pattern = &capture[13..];
                let regex = regex::Regex::new(pattern).map_err(|e| {
                    ConductorError::Workflow(format!(
                        "invalid folder_regex pattern '{pattern}': {e}"
                    ))
                })?;
                capture_folder_regex_output(&regex, sandbox_dir, spec.allow_empty).await?
            }
            capture if capture.starts_with("folder:") => {
                let relative_path = &capture[7..];
                let full_path = sandbox_dir.join(relative_path);
                let file_paths = walk_and_collect_file_paths(&full_path).await?;
                let file_list: Vec<String> = if spec.include_topmost_folder {
                    file_paths
                        .iter()
                        .filter_map(|p| p.strip_prefix(sandbox_dir).ok())
                        .map(|p| p.to_string_lossy().to_string())
                        .collect()
                } else {
                    let prefix = sandbox_dir.join(relative_path);
                    file_paths
                        .iter()
                        .filter_map(|p| p.strip_prefix(&prefix).ok())
                        .map(|p| p.to_string_lossy().to_string())
                        .collect()
                };
                if file_list.is_empty() && !spec.allow_empty {
                    return Err(ConductorError::Workflow(format!(
                        "folder capture matched zero sandbox files under '{relative_path}'"
                    )));
                }
                archive_sandbox_relative_files_as_zip(sandbox_dir, &file_list).await?
            }
            _ => continue,
        };

        let hash = cas.put(bytes::Bytes::from(data)).await.map_err(ConductorError::Cas)?;

        // After the put, so the mark can only describe an object that exists.
        if spec.save == SaveMode::Full {
            cas.mark_keep_full(hash).await.map_err(ConductorError::Cas)?;
        }

        records.insert(spec.name.clone(), CapturedOutput { hash, capture: Some((*spec).clone()) });
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
                capture: "stdout".to_string(),
                save: SaveMode::True,
                allow_empty: false,
                include_topmost_folder: true,
            },
        );
        let outputs = capture_outputs(&cas, &output_specs, &execution, tmp.path()).await.unwrap();
        let out = outputs.records.get("stdout").unwrap();
        let data = cas.get(out.hash).await.unwrap();
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
                capture: "stderr".to_string(),
                save: SaveMode::True,
                allow_empty: false,
                include_topmost_folder: true,
            },
        );
        let outputs = capture_outputs(&cas, &output_specs, &execution, tmp.path()).await.unwrap();
        let out = outputs.records.get("stderr").unwrap();
        let data = cas.get(out.hash).await.unwrap();
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
                capture: "process_code".to_string(),
                save: SaveMode::True,
                allow_empty: false,
                include_topmost_folder: true,
            },
        );
        let outputs = capture_outputs(&cas, &output_specs, &execution, tmp.path()).await.unwrap();
        let out = outputs.records.get("process_code").unwrap();
        let data = cas.get(out.hash).await.unwrap();
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
                capture: "file:test.txt".to_string(),
                save: SaveMode::True,
                allow_empty: false,
                include_topmost_folder: true,
            },
        );
        let outputs = capture_outputs(&cas, &output_specs, &execution, tmp.path()).await.unwrap();
        let out = outputs.records.get("test_file").unwrap();
        let data = cas.get(out.hash).await.unwrap();
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
                capture: "file_regex:result\\.\\w+".to_string(),
                save: SaveMode::True,
                allow_empty: false,
                include_topmost_folder: true,
            },
        );
        let outputs = capture_outputs(&cas, &output_specs, &execution, tmp.path()).await.unwrap();
        let out = outputs.records.get("log").unwrap();
        let data = cas.get(out.hash).await.unwrap();
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
                capture: "file_regex:^downloads/.+(?:__mediapm__)?[.]info[.]json$".to_string(),
                save: SaveMode::True,
                allow_empty: false,
                include_topmost_folder: true,
            },
        );
        let outputs = capture_outputs(&cas, &output_specs, &execution, tmp.path()).await.unwrap();
        let out = outputs.records.get("yt_dlp_infojson_file").unwrap();
        let data = cas.get(out.hash).await.unwrap();
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
                capture: "folder_regex:^downloads/(.+?)(?:__mediapm__)?[.]vtt$".to_string(),
                save: SaveMode::True,
                allow_empty: false,
                include_topmost_folder: true,
            },
        );
        let outputs = capture_outputs(&cas, &output_specs, &execution, tmp.path()).await.unwrap();
        let out = outputs.records.get("yt_dlp_subtitle_artifacts").unwrap();
        let data = cas.get(out.hash).await.unwrap();
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
                capture: "folder_regex:^downloads/(.+?)(?:__mediapm__)?[.]vtt$".to_string(),
                save: SaveMode::True,
                allow_empty: false,
                include_topmost_folder: true,
            },
        );
        let error = capture_outputs(&cas, &output_specs, &execution, tmp.path()).await.unwrap_err();
        assert!(
            error.to_string().contains("folder_regex capture matched zero sandbox files"),
            "unexpected error: {error}"
        );
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
                capture: "folder:subdir".to_string(),
                save: SaveMode::True,
                allow_empty: false,
                include_topmost_folder: true,
            },
        );
        let outputs = capture_outputs(&cas, &output_specs, &execution, tmp.path()).await.unwrap();
        let out = outputs.records.get("folder_out").unwrap();
        let data = cas.get(out.hash).await.unwrap();
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
        assert_eq!(cas.get(stdout_out.hash).await.unwrap().as_ref(), b"hello");
        assert_eq!(cas.get(stderr_out.hash).await.unwrap().as_ref(), b"error");
        assert_eq!(cas.get(code_out.hash).await.unwrap().as_ref(), b"1");
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
            capture: "file:out.mkv".to_string(),
            save: SaveMode::False,
            allow_empty: true,
            include_topmost_folder: false,
        };
        let folder_spec = OutputCaptureSpec {
            name: "artifacts".to_string(),
            capture: "folder_regex:^downloads/.+[.]vtt$".to_string(),
            save: SaveMode::True,
            allow_empty: true,
            include_topmost_folder: false,
        };
        let output_specs = BTreeMap::from([
            ("content".to_string(), file_spec.clone()),
            ("artifacts".to_string(), folder_spec.clone()),
        ]);
        let execution = ExecutionResult { stdout: Vec::new(), stderr: Vec::new(), exit_code: 0 };

        let outputs = capture_outputs(&cas, &output_specs, &execution, tmp.path())
            .await
            .expect("both captures match a sandbox file");

        assert_eq!(outputs.records["content"].capture.as_ref(), Some(&file_spec));
        assert_eq!(outputs.records["artifacts"].capture.as_ref(), Some(&folder_spec));
    }

    /// The always-captured process streams are generated, not declared, and
    /// each still records the spec that produced it: a step that later binds
    /// `stdout` is comparing against a spec it never wrote.
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
            let spec = outputs.records[name]
                .capture
                .as_ref()
                .unwrap_or_else(|| panic!("implicit output `{name}` must record its spec"));
            assert_eq!(spec.name, name);
            assert_eq!(spec.capture, name);
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
            capture: "file:out.txt".to_string(),
            save: SaveMode::True,
            allow_empty: false,
            include_topmost_folder: true,
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
            record.capture.as_ref(),
            Some(&declared),
            "the declared spec must be the spec recorded for the name",
        );
        assert_eq!(
            cas.get(record.hash).await.unwrap().as_ref(),
            b"from-file",
            "the declared capture, not the process stream, decides the bytes",
        );
    }
}
