//! Persistent CAS artifact and visualization demo.
//!
//! This example creates deterministic fixture files, stores them in a
//! filesystem CAS, runs optimization/pruning, and emits visualization artifacts
//! you can inspect manually or through the `mediapm cas visualize` command.
//! Before each run, the example recreates its output directory to prevent
//! stale artifacts from previous runs from leaking into inspection results.
//!
//! Generated artifacts live under:
//! `src/cas/examples/.artifacts/demo/`
//!
//! Key outputs:
//! - `manifest.json` (object metadata)
//! - `topology.mmd` (Mermaid graph)
//! - `topology.json` (snapshot used by CLI/json views)
//! - `how-to-inspect.txt` (copy/paste commands)

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use mediapm_cas::{
    CasApi, CasMaintenanceApi, Constraint, FileSystemCas, Hash, OptimizeOptions, PruneReport,
    empty_content_hash,
};
use serde::Serialize;

#[derive(Debug, Clone)]
/// One deterministic input fixture written to the demo artifact tree.
struct DemoFileSpec {
    /// Relative path under the generated artifact root.
    relative_path: &'static str,
    /// File bytes to write at `relative_path`.
    payload: Vec<u8>,
}

#[derive(Debug, Serialize)]
/// Manifest row describing one stored demo object.
struct DemoFileRecord {
    /// Relative source file path that produced this object.
    relative_path: String,
    /// Canonical object hash string.
    hash: String,
    /// Logical reconstructed content length.
    content_len: u64,
    /// Persisted payload size (full or delta payload bytes).
    payload_len: u64,
    /// `true` when object is currently stored as delta payload.
    is_delta: bool,
    /// Optional base hash used for delta reconstruction.
    base_hash: Option<String>,
}

#[derive(Debug, Serialize)]
/// Top-level JSON manifest emitted by this demo.
struct DemoManifest {
    /// UNIX epoch timestamp at manifest generation time.
    generated_unix_epoch_seconds: u64,
    /// Filesystem CAS root directory for this demo run.
    cas_root: String,
    /// Root directory where fixture inputs were written.
    input_files_root: String,
    /// Canonical empty-content hash string.
    empty_content_hash: String,
    /// On-disk path of the empty bootstrap object.
    empty_content_object_path: String,
    /// Human-readable explanation for empty-object bootstrap semantics.
    empty_content_explanation: String,
    /// Number of objects rewritten by optimize pass.
    optimize_rewritten_objects: usize,
    /// Number of removed candidates reported by prune pass.
    prune_removed_candidates: usize,
    /// Path to Mermaid topology output file.
    topology_mermaid_path: String,
    /// Path to JSON topology snapshot output file.
    topology_json_path: String,
    /// Logical CAS store footprint without delta compression (bytes).
    store_size_without_delta_bytes: u64,
    /// Effective CAS store footprint with delta compression (bytes).
    store_size_with_delta_bytes: u64,
    /// Per-file object metadata rows.
    files: Vec<DemoFileRecord>,
}

#[derive(Debug)]
/// Summary of generated artifact locations printed by `main`.
struct DemoRunSummary {
    /// Root artifact directory containing inputs, store, and outputs.
    artifact_root: PathBuf,
    /// CAS repository root used for this run.
    cas_root: PathBuf,
    /// Path to generated manifest JSON.
    manifest_path: PathBuf,
    /// Path to generated Mermaid topology file.
    topology_mermaid_path: PathBuf,
}

/// Returns the deterministic artifact root used by this demo.
fn artifact_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples").join(".artifacts").join("demo")
}

/// Recreates the demo output directory so stale artifacts never survive reruns.
fn reset_demo_output_directory() -> Result<PathBuf, Box<dyn std::error::Error>> {
    let root = artifact_root();
    if root.exists() {
        std::fs::remove_dir_all(&root)?;
    }
    std::fs::create_dir_all(&root)?;
    Ok(root)
}

/// Builds deterministic file fixtures for repeatable demo output.
fn input_files_for_demo() -> Vec<DemoFileSpec> {
    let waveform: Vec<u8> = (0u32..2048).map(|i| ((i * 37 + 11) % 256) as u8).collect();

    vec![
        DemoFileSpec {
            relative_path: "inputs/album/track_01.txt",
            payload: b"mediapm demo track 01\nverse: const waves\nchorus: keep the hash\n".to_vec(),
        },
        DemoFileSpec {
            relative_path: "inputs/album/track_02.txt",
            payload: b"mediapm demo track 02\nverse: const waves\nchorus: keep the hash\nbridge: optimize once\n".to_vec(),
        },
        DemoFileSpec {
            relative_path: "inputs/metadata/library.json",
            payload: br#"{
  "collection": "demo",
  "kind": "generated-fixture",
  "seed": "fixed-content-v2"
}
"#
            .to_vec(),
        },
        DemoFileSpec { relative_path: "inputs/media/waveform.bin", payload: waveform },
    ]
}

/// Writes fixture files for the demo run.
fn write_demo_files(base: &Path, specs: &[DemoFileSpec]) -> Result<(), Box<dyn std::error::Error>> {
    for spec in specs {
        let path = base.join(spec.relative_path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, &spec.payload)?;
    }
    Ok(())
}

/// Returns whether one path segment is a lower-level hexadecimal fragment.
#[must_use]
fn is_hex_segment(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// Collects all object hashes currently present under one filesystem CAS root.
fn collect_store_object_hashes(
    cas_root: &Path,
) -> Result<BTreeSet<Hash>, Box<dyn std::error::Error>> {
    let mut hashes = BTreeSet::new();
    let objects_root = cas_root.join("v1");
    if !objects_root.exists() {
        return Ok(hashes);
    }

    for shard_entry in std::fs::read_dir(&objects_root)? {
        let shard_entry = shard_entry?;
        if !shard_entry.file_type()?.is_dir() {
            continue;
        }

        let shard_name = shard_entry.file_name();
        let shard_name = shard_name.to_string_lossy();
        if shard_name.len() != 2 || !is_hex_segment(&shard_name) {
            continue;
        }

        for object_entry in std::fs::read_dir(shard_entry.path())? {
            let object_entry = object_entry?;
            if !object_entry.file_type()?.is_file() {
                continue;
            }

            let file_name = object_entry.file_name();
            let file_name = file_name.to_string_lossy();
            let stem = file_name.strip_suffix(".diff").unwrap_or(&file_name);
            if stem.len() != 62 || !is_hex_segment(stem) {
                continue;
            }

            let hash_text = format!("blake3:{shard_name}{stem}");
            if let Ok(hash) = Hash::from_str(&hash_text) {
                let _ = hashes.insert(hash);
            }
        }
    }

    Ok(hashes)
}

/// Computes logical and effective store-size totals from all persisted objects.
async fn summarize_store_sizes(
    cas: &FileSystemCas,
    cas_root: &Path,
) -> Result<(u64, u64), Box<dyn std::error::Error>> {
    let mut without_delta = 0u64;
    let mut with_delta = 0u64;

    for hash in collect_store_object_hashes(cas_root)? {
        let info = cas.info(hash).await?;
        without_delta = without_delta.saturating_add(info.content_len);
        with_delta = with_delta.saturating_add(info.payload_len);
    }

    Ok((without_delta, with_delta))
}

/// Clears stale output files, then generates inspectable demo artifacts.
async fn generate_inspectable_artifacts() -> Result<DemoRunSummary, Box<dyn std::error::Error>> {
    let root = reset_demo_output_directory()?;

    let specs = input_files_for_demo();
    write_demo_files(&root, &specs)?;

    let cas_root = root.join("cas-store");
    let cas = FileSystemCas::open(&cas_root).await?;

    let mut stored: Vec<(String, Hash)> = Vec::with_capacity(specs.len());
    for spec in &specs {
        let hash = cas.put(spec.payload.clone()).await?;
        stored.push((spec.relative_path.to_string(), hash));
    }

    let track_01 = stored
        .iter()
        .find_map(|(path, hash)| (path.ends_with("track_01.txt")).then_some(*hash))
        .ok_or("missing track_01 hash")?;
    let track_02 = stored
        .iter()
        .find_map(|(path, hash)| (path.ends_with("track_02.txt")).then_some(*hash))
        .ok_or("missing track_02 hash")?;

    cas.set_constraint(Constraint {
        target_hash: track_02,
        potential_bases: BTreeSet::from([track_01]),
    })
    .await?;

    let optimize_report = cas.optimize_once(OptimizeOptions::default()).await?;
    let PruneReport { removed_candidates } = cas.prune_constraints().await?;

    let mut records = Vec::with_capacity(stored.len());
    for (path, hash) in &stored {
        let info = cas.info(*hash).await?;
        records.push(DemoFileRecord {
            relative_path: path.clone(),
            hash: hash.to_string(),
            content_len: info.content_len,
            payload_len: info.payload_len,
            is_delta: info.is_delta,
            base_hash: info.base_hash.map(|value| value.to_string()),
        });
    }

    let topology_mermaid = cas.visualize_mermaid(true).await?;
    let topology_mermaid_path = root.join("topology.mmd");
    std::fs::write(&topology_mermaid_path, topology_mermaid)?;

    let topology_snapshot = cas.topology_snapshot(true).await?;
    let topology_json_path = root.join("topology.json");
    std::fs::write(&topology_json_path, serde_json::to_vec_pretty(&topology_snapshot)?)?;

    let generated_unix_epoch_seconds =
        SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
    let (store_size_without_delta_bytes, store_size_with_delta_bytes) =
        summarize_store_sizes(&cas, &cas_root).await?;

    let input_root = root.join("inputs");
    let empty_hash = empty_content_hash();
    let empty_path = cas.object_path_for_hash(empty_hash);
    let manifest = DemoManifest {
        generated_unix_epoch_seconds,
        cas_root: cas_root.display().to_string(),
        input_files_root: input_root.display().to_string(),
        empty_content_hash: empty_hash.to_string(),
        empty_content_object_path: empty_path.display().to_string(),
        empty_content_explanation:
            "expected zero-byte bootstrap object for canonical empty-content hash".to_string(),
        optimize_rewritten_objects: optimize_report.rewritten_objects,
        prune_removed_candidates: removed_candidates,
        topology_mermaid_path: topology_mermaid_path.display().to_string(),
        topology_json_path: topology_json_path.display().to_string(),
        store_size_without_delta_bytes,
        store_size_with_delta_bytes,
        files: records,
    };

    let manifest_path = root.join("manifest.json");
    std::fs::write(&manifest_path, serde_json::to_vec_pretty(&manifest)?)?;

    let helper_path = root.join("how-to-inspect.txt");
    std::fs::write(
        &helper_path,
        format!(
            "Artifact root:\n  {}\n\nCAS root:\n  {}\n\nVisualization files:\n  mermaid: {}\n  json: {}\n\nCLI visualize commands:\n  cargo run -p mediapm -- cas visualize --root {} --format mermaid --output {}\n  cargo run -p mediapm -- cas visualize --root {} --format json --output {}\n  cargo run -p mediapm -- cas visualize --root {} --format text\n\nOther useful commands:\n  cargo run -p mediapm -- cas optimize --root {}\n  cargo run -p mediapm -- cas prune --root {}\n  cargo run -p mediapm -- cas get --root {} <HASH_FROM_MANIFEST>\n\nExpected empty bootstrap object:\n  hash={}\n  path={}\n  note=zero bytes is correct\n\nManifest:\n  {}\n",
            root.display(),
            cas_root.display(),
            topology_mermaid_path.display(),
            topology_json_path.display(),
            cas_root.display(),
            topology_mermaid_path.display(),
            cas_root.display(),
            topology_json_path.display(),
            cas_root.display(),
            cas_root.display(),
            cas_root.display(),
            cas_root.display(),
            empty_hash,
            empty_path.display(),
            manifest_path.display(),
        ),
    )?;

    Ok(DemoRunSummary { artifact_root: root, cas_root, manifest_path, topology_mermaid_path })
}

#[tokio::main]
/// Runs the full filesystem demo and prints generated artifact paths.
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let summary = generate_inspectable_artifacts().await?;
    println!("generated artifacts root: {}", summary.artifact_root.display());
    println!("generated cas root: {}", summary.cas_root.display());
    println!("manifest: {}", summary.manifest_path.display());
    println!("topology mermaid: {}", summary.topology_mermaid_path.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    /// Keeps fixture count/content deterministic for stable demo artifacts.
    fn fixture_set_is_deterministic_and_non_empty() {
        let fixtures = super::input_files_for_demo();
        assert_eq!(fixtures.len(), 4, "fixture count should remain stable");
        for fixture in fixtures {
            assert!(!fixture.relative_path.is_empty());
            assert!(!fixture.payload.is_empty());
        }
    }

    #[tokio::test]
    #[ignore = "creates persistent demo artifacts under src/cas/examples/.artifacts"]
    /// Smoke-tests artifact generation end-to-end.
    async fn generates_inspectable_artifacts_and_visualization_files() {
        let summary =
            super::generate_inspectable_artifacts().await.expect("generate demo artifacts");
        assert!(summary.cas_root.join("v1").exists(), "cas version directory missing");
        assert!(summary.manifest_path.is_file(), "manifest should be generated");
        assert!(summary.topology_mermaid_path.is_file(), "mermaid file should be generated");
    }
}
