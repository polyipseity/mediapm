//! Captures declared step outputs from execution results and persists to CAS.

use std::path::{Path, PathBuf};

use mediapm_cas::CasApi;

use crate::config::OutputCaptureSpec;
use crate::error::ConductorError;
use crate::state::{OutputRef, OutputSaveMode, PersistenceFlags};

use super::process::ExecutionResult;

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

/// Captures declared outputs from the execution result and persists to CAS.
pub(super) async fn capture_outputs<C: CasApi + Send + Sync>(
    cas: &C,
    output_specs: &std::collections::BTreeMap<String, OutputCaptureSpec>,
    execution: &ExecutionResult,
    sandbox_dir: &Path,
    persistence: PersistenceFlags,
) -> Result<Vec<OutputRef>, ConductorError> {
    let mut outputs = Vec::new();
    let save_mode = if persistence.save { OutputSaveMode::Saved } else { OutputSaveMode::Unsaved };

    // Implicitly capture stdout, stderr, and process_code unless explicitly declared.
    let declared_names: std::collections::BTreeSet<&str> =
        output_specs.keys().map(String::as_str).collect();
    let implicit_specs = ["stdout", "stderr", "process_code"]
        .into_iter()
        .filter(|name| !declared_names.contains(name))
        .map(|name| OutputCaptureSpec {
            name: name.to_string(),
            capture: name.to_string(),
            save: crate::config::SaveMode::True,
            allow_empty: false,
            include_topmost_folder: true,
        })
        .map(|spec| (spec.name.clone(), spec))
        .collect::<std::collections::BTreeMap<String, OutputCaptureSpec>>();

    let combined_specs: std::collections::BTreeMap<&str, &OutputCaptureSpec> = output_specs
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
                let file_paths = walk_and_collect_file_paths(sandbox_dir).await?;
                let matched = file_paths.iter().find(|p| {
                    p.file_name().is_some_and(|name| regex.is_match(&name.to_string_lossy()))
                });
                match matched {
                    Some(path) => tokio::fs::read(path).await.map_err(|e| {
                        ConductorError::Workflow(format!(
                            "failed to read matched file '{}': {e}",
                            path.display()
                        ))
                    })?,
                    None if spec.allow_empty => Vec::new(),
                    None => continue,
                }
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
                serde_json::to_vec(&file_list).map_err(|e| {
                    ConductorError::Workflow(format!("failed to serialize folder listing: {e}"))
                })?
            }
            _ => continue,
        };

        let hash = cas.put(bytes::Bytes::from(data)).await.map_err(ConductorError::Cas)?;

        outputs.push(OutputRef { name: spec.name.clone(), hash, save_mode });
    }

    Ok(outputs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::OutputCaptureSpec;
    use crate::config::SaveMode;
    use std::collections::BTreeMap;
    use tempfile::TempDir;

    #[tokio::test]
    async fn captures_stdout() {
        let cas = mediapm_cas::storage::in_memory::new_in_memory_cas();
        let tmp = TempDir::new().expect("temp dir");
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
        let persistence = PersistenceFlags { save: true, force_full: false };
        let outputs = capture_outputs(&cas, &output_specs, &execution, tmp.path(), persistence)
            .await
            .unwrap();
        let out = outputs.iter().find(|o| o.name == "stdout").unwrap();
        let data = cas.get(out.hash).await.unwrap();
        assert_eq!(data.as_ref(), b"hello");
    }

    #[tokio::test]
    async fn captures_stderr() {
        let cas = mediapm_cas::storage::in_memory::new_in_memory_cas();
        let tmp = TempDir::new().expect("temp dir");
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
        let persistence = PersistenceFlags { save: true, force_full: false };
        let outputs = capture_outputs(&cas, &output_specs, &execution, tmp.path(), persistence)
            .await
            .unwrap();
        let out = outputs.iter().find(|o| o.name == "stderr").unwrap();
        let data = cas.get(out.hash).await.unwrap();
        assert_eq!(data.as_ref(), b"error output");
    }

    #[tokio::test]
    async fn captures_process_code() {
        let cas = mediapm_cas::storage::in_memory::new_in_memory_cas();
        let tmp = TempDir::new().expect("temp dir");
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
        let persistence = PersistenceFlags { save: true, force_full: false };
        let outputs = capture_outputs(&cas, &output_specs, &execution, tmp.path(), persistence)
            .await
            .unwrap();
        let out = outputs.iter().find(|o| o.name == "process_code").unwrap();
        let data = cas.get(out.hash).await.unwrap();
        assert_eq!(data.as_ref(), b"42");
    }

    #[tokio::test]
    async fn captures_file() {
        let cas = mediapm_cas::storage::in_memory::new_in_memory_cas();
        let tmp = TempDir::new().expect("temp dir");
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
        let persistence = PersistenceFlags { save: true, force_full: false };
        let outputs = capture_outputs(&cas, &output_specs, &execution, tmp.path(), persistence)
            .await
            .unwrap();
        let out = outputs.iter().find(|o| o.name == "test_file").unwrap();
        let data = cas.get(out.hash).await.unwrap();
        assert_eq!(data.as_ref(), b"file content");
    }

    #[tokio::test]
    async fn captures_file_regex() {
        let cas = mediapm_cas::storage::in_memory::new_in_memory_cas();
        let tmp = TempDir::new().expect("temp dir");
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
        let persistence = PersistenceFlags { save: true, force_full: false };
        let outputs = capture_outputs(&cas, &output_specs, &execution, tmp.path(), persistence)
            .await
            .unwrap();
        let out = outputs.iter().find(|o| o.name == "log").unwrap();
        let data = cas.get(out.hash).await.unwrap();
        assert_eq!(data.as_ref(), b"regex match");
    }

    #[tokio::test]
    async fn captures_folder() {
        let cas = mediapm_cas::storage::in_memory::new_in_memory_cas();
        let tmp = TempDir::new().expect("temp dir");
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
        let persistence = PersistenceFlags { save: true, force_full: false };
        let outputs = capture_outputs(&cas, &output_specs, &execution, tmp.path(), persistence)
            .await
            .unwrap();
        let out = outputs.iter().find(|o| o.name == "folder_out").unwrap();
        let data = cas.get(out.hash).await.unwrap();
        let file_list: Vec<String> = serde_json::from_slice(&data).unwrap();
        assert!(file_list.contains(&"subdir/a.txt".to_string()));
        assert!(file_list.contains(&"subdir/b.txt".to_string()));
    }

    #[tokio::test]
    async fn implicit_outputs() {
        let cas = mediapm_cas::storage::in_memory::new_in_memory_cas();
        let tmp = TempDir::new().expect("temp dir");
        let execution =
            ExecutionResult { stdout: b"hello".to_vec(), stderr: b"error".to_vec(), exit_code: 1 };
        let output_specs = BTreeMap::new();
        let persistence = PersistenceFlags { save: true, force_full: false };
        let outputs = capture_outputs(&cas, &output_specs, &execution, tmp.path(), persistence)
            .await
            .unwrap();
        assert_eq!(outputs.len(), 3);
        let stdout_out = outputs.iter().find(|o| o.name == "stdout").unwrap();
        let stderr_out = outputs.iter().find(|o| o.name == "stderr").unwrap();
        let code_out = outputs.iter().find(|o| o.name == "process_code").unwrap();
        assert_eq!(cas.get(stdout_out.hash).await.unwrap().as_ref(), b"hello");
        assert_eq!(cas.get(stderr_out.hash).await.unwrap().as_ref(), b"error");
        assert_eq!(cas.get(code_out.hash).await.unwrap().as_ref(), b"1");
    }
}
