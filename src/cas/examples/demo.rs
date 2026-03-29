//! Persistent CAS artifact and visualization demo.
//!
//! This example creates deterministic fixture files, stores them in a
//! filesystem CAS, runs optimization/pruning, and emits visualization artifacts
//! you can inspect manually or through the `mediapm cas visualize` command.
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
use std::time::{SystemTime, UNIX_EPOCH};

use mediapm_cas::{
    CasApi, CasMaintenanceApi, Constraint, FileSystemCas, Hash, OptimizeOptions, PruneReport,
    empty_content_hash,
};
use serde::Serialize;

#[derive(Debug, Clone)]
struct DemoFileSpec {
    relative_path: &'static str,
    payload: Vec<u8>,
}

#[derive(Debug, Serialize)]
struct DemoFileRecord {
    relative_path: String,
    hash: String,
    content_len: u64,
    payload_len: u64,
    is_delta: bool,
    base_hash: Option<String>,
}

#[derive(Debug, Serialize)]
struct DemoManifest {
    generated_unix_epoch_seconds: u64,
    cas_root: String,
    input_files_root: String,
    empty_content_hash: String,
    empty_content_object_path: String,
    empty_content_explanation: String,
    optimize_rewritten_objects: usize,
    prune_removed_candidates: usize,
    topology_mermaid_path: String,
    topology_json_path: String,
    files: Vec<DemoFileRecord>,
}

#[derive(Debug)]
struct DemoRunSummary {
    artifact_root: PathBuf,
    cas_root: PathBuf,
    manifest_path: PathBuf,
    topology_mermaid_path: PathBuf,
}

fn artifact_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples").join(".artifacts").join("demo")
}

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

async fn generate_inspectable_artifacts() -> Result<DemoRunSummary, Box<dyn std::error::Error>> {
    let root = artifact_root();
    if root.exists() {
        std::fs::remove_dir_all(&root)?;
    }
    std::fs::create_dir_all(&root)?;

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
    use super::{generate_inspectable_artifacts, input_files_for_demo};

    #[test]
    fn fixture_set_is_deterministic_and_non_empty() {
        let fixtures = input_files_for_demo();
        assert_eq!(fixtures.len(), 4, "fixture count should remain stable");
        for fixture in fixtures {
            assert!(!fixture.relative_path.is_empty());
            assert!(!fixture.payload.is_empty());
        }
    }

    #[tokio::test]
    #[ignore = "creates persistent demo artifacts under src/cas/examples/.artifacts"]
    async fn generates_inspectable_artifacts_and_visualization_files() {
        let summary = generate_inspectable_artifacts().await.expect("generate demo artifacts");
        assert!(summary.cas_root.join("v1").exists(), "cas version directory missing");
        assert!(summary.manifest_path.is_file(), "manifest should be generated");
        assert!(summary.topology_mermaid_path.is_file(), "mermaid file should be generated");
    }
}
