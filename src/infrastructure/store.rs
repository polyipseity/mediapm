//! Workspace-local content-addressed storage and sidecar persistence.
//!
//! This module owns all paths under `.mediapm/` and provides:
//! - object hashing and storage,
//! - sidecar load/save/migration integration,
//! - deterministic + atomic JSON writes.
//!
//! Design constraints:
//! - object files are immutable and addressed by content hash,
//! - sidecars are mutable but written atomically and canonically,
//! - migration is applied at read boundaries so old sidecars remain usable.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use tokio::{
    fs::{self, File, OpenOptions},
    io::{AsyncReadExt, AsyncWriteExt},
};
use walkdir::WalkDir;

use crate::{
    domain::{
        migration::migrate_to_latest,
        model::{Blake3Hash, LATEST_SCHEMA_VERSION, MediaRecord},
    },
    support::util::{now_rfc3339, sort_json_value},
};

/// Derived workspace paths for mediapm's local store.
///
/// Centralizing path derivation in one type prevents subtle path drift between
/// commands and keeps on-disk layout changes localized.
#[derive(Clone, Debug)]
pub struct WorkspacePaths {
    /// Workspace root.
    pub root: PathBuf,
    /// `.mediapm` root.
    pub mediapm_dir: PathBuf,
    /// Object store root (`.mediapm/objects/blake3`).
    pub objects_dir: PathBuf,
    /// Sidecar root (`.mediapm/media`).
    pub media_dir: PathBuf,
    /// Provider cache root.
    pub providers_dir: PathBuf,
    /// Link state root.
    pub links_dir: PathBuf,
    /// Lock files root.
    pub locks_dir: PathBuf,
    /// Temporary files root.
    pub tmp_dir: PathBuf,
}

impl WorkspacePaths {
    /// Build derived paths for `root`.
    pub fn new(root: impl AsRef<Path>) -> Self {
        let root = root.as_ref().to_path_buf();
        let mediapm_dir = root.join(".mediapm");

        Self {
            root,
            objects_dir: mediapm_dir.join("objects").join("blake3"),
            media_dir: mediapm_dir.join("media"),
            providers_dir: mediapm_dir.join("providers"),
            links_dir: mediapm_dir.join("links"),
            locks_dir: mediapm_dir.join("locks"),
            tmp_dir: mediapm_dir.join("tmp"),
            mediapm_dir,
        }
    }

    /// Ensure all workspace store directories exist.
    pub async fn ensure_store_dirs(&self) -> Result<()> {
        fs::create_dir_all(&self.objects_dir).await?;
        fs::create_dir_all(&self.media_dir).await?;
        fs::create_dir_all(&self.providers_dir).await?;
        fs::create_dir_all(&self.links_dir).await?;
        fs::create_dir_all(&self.locks_dir).await?;
        fs::create_dir_all(&self.tmp_dir).await?;
        Ok(())
    }
}

/// Compute BLAKE3 hash for a file.
///
/// Hashing is streamed to avoid loading full files into memory.
pub async fn hash_file(path: &Path) -> Result<Blake3Hash> {
    let mut file =
        File::open(path).await.with_context(|| format!("failed to open {}", path.display()))?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0_u8; 1024 * 64];

    loop {
        let read_bytes = file.read(&mut buffer).await?;
        if read_bytes == 0 {
            break;
        }
        hasher.update(&buffer[..read_bytes]);
    }

    let mut hash = [0_u8; 32];
    hash.copy_from_slice(hasher.finalize().as_bytes());
    Ok(Blake3Hash::from_bytes(hash))
}

/// Compute relative object path for a hash using a 2-char fan-out prefix.
pub fn object_relpath(hash: &Blake3Hash) -> PathBuf {
    let hex_hash = hash.to_hex();
    let (prefix, suffix) = hex_hash.split_at(2);

    PathBuf::from(".mediapm").join("objects").join("blake3").join(prefix).join(suffix)
}

/// Compute absolute object path in this workspace.
pub fn object_abspath(paths: &WorkspacePaths, hash: &Blake3Hash) -> PathBuf {
    paths.root.join(object_relpath(hash))
}

/// Ensure object content exists in object store and return relative object path.
///
/// If another process already created the same object between write attempts,
/// this function treats that as success (deduplicated convergence).
pub async fn ensure_object(
    paths: &WorkspacePaths,
    source_file: &Path,
    hash: &Blake3Hash,
) -> Result<String> {
    let object_path = object_abspath(paths, hash);
    if object_path.exists() {
        return Ok(relpath_string(&object_relpath(hash)));
    }

    if let Some(parent) = object_path.parent() {
        fs::create_dir_all(parent).await?;
    }

    let tmp_name = format!(
        ".tmp-object-{}-{}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos()
    );
    let tmp_path = object_path.parent().unwrap_or(&paths.tmp_dir).join(tmp_name);

    let mut source = File::open(source_file).await?;
    let mut tmp_file = OpenOptions::new().create_new(true).write(true).open(&tmp_path).await?;
    tokio::io::copy(&mut source, &mut tmp_file).await?;
    tmp_file.sync_all().await?;

    match fs::rename(&tmp_path, &object_path).await {
        Ok(()) => {}
        Err(_error) if object_path.exists() => {
            let _ = fs::remove_file(&tmp_path).await;
            return Ok(relpath_string(&object_relpath(hash)));
        }
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "failed to move temporary object {} into place {}",
                    tmp_path.display(),
                    object_path.display()
                )
            });
        }
    }

    Ok(relpath_string(&object_relpath(hash)))
}

/// Deterministic media-id derived from canonical URI.
pub fn media_id_for_uri(canonical_uri: &str) -> String {
    blake3::hash(canonical_uri.as_bytes()).to_hex().to_string()
}

/// Sidecar path for one canonical URI.
pub fn sidecar_path_for_uri(paths: &WorkspacePaths, canonical_uri: &str) -> PathBuf {
    paths.media_dir.join(media_id_for_uri(canonical_uri)).join("media.json")
}

/// Load sidecar for one URI if present.
pub async fn read_sidecar(
    paths: &WorkspacePaths,
    canonical_uri: &str,
) -> Result<Option<MediaRecord>> {
    let sidecar_path = sidecar_path_for_uri(paths, canonical_uri);
    if !sidecar_path.exists() {
        return Ok(None);
    }

    Ok(Some(read_sidecar_from_path(paths, &sidecar_path).await?))
}

/// Read and migrate one sidecar file.
///
/// Read-time migration keeps command logic simple: callers can rely on latest
/// in-memory schema and only reason about one representation.
pub async fn read_sidecar_from_path(
    paths: &WorkspacePaths,
    sidecar_path: &Path,
) -> Result<MediaRecord> {
    let bytes = fs::read(sidecar_path).await?;
    let raw_value: serde_json::Value = serde_json::from_slice(&bytes)?;
    let (migrated_value, provenance) = migrate_to_latest(raw_value)?;

    let mut record: MediaRecord = serde_json::from_value(migrated_value)?;

    if !provenance.is_empty() {
        record.migration_provenance.extend(provenance);
        write_sidecar(paths, &record).await?;
    }

    Ok(record)
}

/// Load all sidecar records in the workspace.
pub async fn load_all_sidecars(paths: &WorkspacePaths) -> Result<Vec<MediaRecord>> {
    if !paths.media_dir.exists() {
        return Ok(Vec::new());
    }

    let mut sidecars = Vec::new();

    for entry in
        WalkDir::new(&paths.media_dir).follow_links(false).into_iter().filter_map(Result::ok)
    {
        if !entry.file_type().is_file() {
            continue;
        }

        if entry.file_name() != "media.json" {
            continue;
        }

        let record = read_sidecar_from_path(paths, entry.path()).await?;
        sidecars.push(record);
    }

    Ok(sidecars)
}

/// Build URI->sidecar index from all sidecar files.
pub async fn load_sidecar_index(paths: &WorkspacePaths) -> Result<HashMap<String, MediaRecord>> {
    let mut index = HashMap::new();

    for sidecar in load_all_sidecars(paths).await? {
        index.insert(sidecar.canonical_uri.clone(), sidecar);
    }

    Ok(index)
}

/// Canonically serialize and atomically persist one sidecar.
///
/// Canonical key ordering avoids noisy diffs; atomic write strategy avoids
/// torn/partial sidecars if process crashes mid-write.
pub async fn write_sidecar(paths: &WorkspacePaths, record: &MediaRecord) -> Result<()> {
    let sidecar_path = sidecar_path_for_uri(paths, &record.canonical_uri);

    if let Some(parent) = sidecar_path.parent() {
        fs::create_dir_all(parent).await?;
    }

    let mut canonical_record = record.clone();
    canonical_record.schema_version = LATEST_SCHEMA_VERSION;
    canonical_record.updated_at = now_rfc3339()?;

    let mut value = serde_json::to_value(&canonical_record)?;
    sort_json_value(&mut value);

    let mut content = serde_json::to_vec_pretty(&value)?;
    content.push(b'\n');

    atomic_write_bytes(&sidecar_path, &content).await
}

/// Atomically write bytes to `target_path` via temp-file + rename.
pub async fn atomic_write_bytes(target_path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = target_path.parent() {
        fs::create_dir_all(parent).await?;
    }

    let tmp_name = format!(
        ".tmp-{}-{}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos()
    );

    let tmp_path = target_path.parent().unwrap_or_else(|| Path::new(".")).join(tmp_name);

    {
        let mut tmp_file = OpenOptions::new().create_new(true).write(true).open(&tmp_path).await?;
        tmp_file.write_all(bytes).await?;
        tmp_file.sync_all().await?;
    }

    fs::rename(&tmp_path, target_path).await?;

    Ok(())
}

fn relpath_string(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use tempfile::tempdir;

    use super::{WorkspacePaths, hash_file, object_relpath};

    #[tokio::test]
    async fn object_relpath_is_fanned_out() {
        let digest = hash_file(Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml").as_path())
            .await
            .expect("Cargo.toml hash should be computable");
        let relpath = object_relpath(&digest);
        let path_text = relpath.to_string_lossy().replace('\\', "/");

        assert!(path_text.contains(".mediapm/objects/blake3/"));
    }

    #[tokio::test]
    async fn create_store_dirs() {
        let temp = tempdir().expect("tempdir should create");
        let paths = WorkspacePaths::new(temp.path());

        paths.ensure_store_dirs().await.expect("store dirs should be created");
        assert!(paths.objects_dir.exists());
        assert!(paths.media_dir.exists());
    }
}
