//! Temporary sandbox directory management and tool content materialization.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use mediapm_cas::{CasApi, Hash};
use mediapm_conductor_builtin_archive;

use crate::error::ConductorError;

/// Creates a temporary sandbox directory for step execution.
pub(super) async fn create_sandbox(
    base_tmp_dir: &Path,
    instance_key: &str,
) -> Result<PathBuf, ConductorError> {
    let sandbox_root = base_tmp_dir.join("sandbox").join(sanitize_for_path(instance_key));
    tokio::fs::create_dir_all(&sandbox_root)
        .await
        .map_err(|source| ConductorError::io("create sandbox directory", &sandbox_root, source))?;
    Ok(sandbox_root)
}

/// Sanitizes a string for use as a path component.
pub(super) fn sanitize_for_path(s: &str) -> String {
    s.replace(|c: char| !c.is_ascii_alphanumeric() && c != '_' && c != '-', "_")
}

/// Materializes tool content map entries into the sandbox directory.
///
/// Entries with a trailing-slash key (e.g. `./{os}/`) are treated as
/// uncompressed ZIP blobs that are unpacked into the corresponding sandbox
/// subdirectory.  File-level entries are written verbatim.
pub(super) async fn materialize_content_map<C: CasApi + Send + Sync>(
    cas: &C,
    content_map: &BTreeMap<String, String>,
    sandbox_dir: &Path,
) -> Result<(), ConductorError> {
    for (relative_path, value) in content_map {
        // Try to parse as hash; if it fails, treat as inline bytes.
        if let Ok(hash) = value.parse::<Hash>() {
            let data = cas.get(hash).await.map_err(ConductorError::Cas)?;
            let target_path = sandbox_dir.join(relative_path);

            if relative_path.ends_with('/') {
                // Trailing-slash key → hash is an uncompressed ZIP blob; unpack it.
                let data_for_zip = data.clone();
                let dest_dir = target_path.clone();
                tokio::task::spawn_blocking(move || {
                    mediapm_conductor_builtin_archive::unpack_zip_bytes_to_directory(
                        &data_for_zip,
                        &dest_dir,
                    )
                    .map_err(|e| {
                        ConductorError::io(
                            "unpack zip content",
                            &dest_dir,
                            std::io::Error::other(e),
                        )
                    })
                })
                .await
                .map_err(|e| {
                    ConductorError::io(
                        "join spawn_blocking",
                        &target_path,
                        std::io::Error::other(e),
                    )
                })??;
            } else {
                // File-level entry: write bytes verbatim.
                if let Some(parent) = target_path.parent() {
                    tokio::fs::create_dir_all(parent).await.map_err(|source| {
                        ConductorError::io("create content parent directory", parent, source)
                    })?;
                }
                tokio::fs::write(&target_path, &data).await.map_err(|source| {
                    ConductorError::io("write tool content", &target_path, source)
                })?;
            }
        }
        // Non-hash values are skipped (inline descriptions).
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use mediapm_cas::InMemoryCas;

    use super::*;

    /// Creates a small uncompressed ZIP with the given entries.
    fn make_zip(entries: &[(&str, &[u8])]) -> Vec<u8> {
        use std::io::Write;
        let cursor = std::io::Cursor::new(Vec::new());
        let mut writer = zip::ZipWriter::new(cursor);
        let opts = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Stored);
        for (name, content) in entries {
            writer.start_file(*name, opts.clone()).unwrap();
            writer.write_all(content).unwrap();
        }
        let cursor = writer.finish().unwrap();
        cursor.into_inner()
    }

    #[tokio::test]
    async fn materialize_dir_entry_unpacks_zip() {
        let cas = InMemoryCas::default();
        let zip_bytes = make_zip(&[("foo.txt", b"hello")]);
        let hash = cas.put(bytes::Bytes::from(zip_bytes.clone())).await.unwrap();

        let mut cmap = BTreeMap::new();
        cmap.insert("./linux/".to_string(), hash.to_hex());

        let sandbox_dir = tempfile::tempdir().unwrap();
        materialize_content_map(&cas, &cmap, sandbox_dir.path()).await.unwrap();

        let unpacked = sandbox_dir.path().join("./linux/foo.txt");
        let content = std::fs::read(&unpacked).unwrap();
        assert_eq!(content, b"hello");
    }

    #[tokio::test]
    async fn materialize_file_entry_writes_verbatim() {
        let cas = InMemoryCas::default();
        let content = b"#!/bin/sh\necho hello\n";
        let hash = cas.put(bytes::Bytes::from(content.to_vec())).await.unwrap();

        let mut cmap = BTreeMap::new();
        cmap.insert("./linux/sd".to_string(), hash.to_hex());

        let sandbox_dir = tempfile::tempdir().unwrap();
        materialize_content_map(&cas, &cmap, sandbox_dir.path()).await.unwrap();

        let file_path = sandbox_dir.path().join("./linux/sd");
        let written = std::fs::read(&file_path).unwrap();
        assert_eq!(written, content);
    }

    #[tokio::test]
    async fn materialize_mixed_entries() {
        let cas = InMemoryCas::default();

        // ZIP entry
        let zip_bytes = make_zip(&[("bin/tool", b"binary")]);
        let zip_hash = cas.put(bytes::Bytes::from(zip_bytes)).await.unwrap();

        // File entry
        let file_content = b"config=value\n";
        let file_hash = cas.put(bytes::Bytes::from(file_content.to_vec())).await.unwrap();

        let mut cmap = BTreeMap::new();
        cmap.insert("./linux/".to_string(), zip_hash.to_hex());
        cmap.insert("./cfg".to_string(), file_hash.to_hex());

        let sandbox_dir = tempfile::tempdir().unwrap();
        materialize_content_map(&cas, &cmap, sandbox_dir.path()).await.unwrap();

        // ZIP-unpacked file
        let tool_path = sandbox_dir.path().join("./linux/bin/tool");
        assert!(tool_path.exists(), "ZIP-unpacked file should exist");
        assert_eq!(std::fs::read(&tool_path).unwrap(), b"binary");

        // Verbatim file
        let cfg_path = sandbox_dir.path().join("./cfg");
        assert!(cfg_path.exists(), "verbatim file should exist");
        assert_eq!(std::fs::read(&cfg_path).unwrap(), b"config=value\n");
    }

    #[tokio::test]
    async fn materialize_invalid_hash_skips_silently() {
        let cas = InMemoryCas::default();
        let mut cmap = BTreeMap::new();
        cmap.insert("./linux/".to_string(), "not-a-hash".to_string());

        let sandbox_dir = tempfile::tempdir().unwrap();
        materialize_content_map(&cas, &cmap, sandbox_dir.path()).await.unwrap();

        // No file should be created (the invalid hash is skipped).
        let path = sandbox_dir.path().join("./linux/");
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn materialize_empty_zip_packs_nothing() {
        let cas = InMemoryCas::default();
        let zip_bytes = make_zip(&[]);
        let hash = cas.put(bytes::Bytes::from(zip_bytes)).await.unwrap();

        let mut cmap = BTreeMap::new();
        cmap.insert("./linux/".to_string(), hash.to_hex());

        let sandbox_dir = tempfile::tempdir().unwrap();
        materialize_content_map(&cas, &cmap, sandbox_dir.path()).await.unwrap();

        // The directory entry was created (empty).
        let dir = sandbox_dir.path().join("./linux/");
        assert!(dir.is_dir());
        // No files inside.
        let mut entries = std::fs::read_dir(&dir).unwrap();
        assert!(entries.next().is_none());
    }
}
