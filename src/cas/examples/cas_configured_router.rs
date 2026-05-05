//! Example of routing behavior by `ConfiguredCas` backend variant.
//!
//! What this example does:
//! 1. Reads `MEDIAPM_CAS_LOCATOR` (default `cas://memory`).
//! 2. Opens backend using `CasConfig::from_locator`.
//! 3. Matches on `ConfiguredCas` to branch app behavior.
//! 4. Stores and retrieves one payload.
//! 5. When using filesystem backend, renders Mermaid topology visualization.

use bytes::Bytes;
use mediapm_cas::{CasApi, CasConfig, ConfiguredCas};

/// Executes one backend-agnostic app flow and returns `(hash, payload_len, mermaid)`.
async fn run_application_flow(
    cas: &ConfiguredCas,
) -> Result<(String, usize, Option<String>), Box<dyn std::error::Error>> {
    match cas {
        ConfiguredCas::InMemory(_) => println!("running with in-memory backend"),
        ConfiguredCas::FileSystem(fs) => {
            println!("running with filesystem backend at {}", fs.root_path().display());
        }
    }

    let hash = cas.put(Bytes::from_static(b"application payload")).await?;
    let restored = cas.get(hash).await?;

    let mermaid = match cas {
        ConfiguredCas::FileSystem(fs) => Some(fs.visualize_mermaid(false).await?),
        ConfiguredCas::InMemory(_) => None,
    };

    println!("hash={hash} bytes={}", restored.len());
    Ok((hash.to_string(), restored.len(), mermaid))
}

/// Opens backend from locator and executes `run_application_flow`.
async fn run_application_flow_from_locator(
    locator: &str,
) -> Result<(String, usize, Option<String>), Box<dyn std::error::Error>> {
    let cas = CasConfig::from_locator(locator)?.open().await?;
    run_application_flow(&cas).await
}

#[tokio::main]
/// Runs the locator-driven configured backend example.
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let locator = std::env::var("MEDIAPM_CAS_LOCATOR").unwrap_or_else(|_| "cas://memory".into());
    let (_hash, _len, mermaid) = run_application_flow_from_locator(&locator).await?;
    if let Some(mermaid) = mermaid {
        println!("filesystem visualization:\n{mermaid}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    /// Verifies `cas://memory` resolves and executes the in-memory branch.
    async fn router_flow_supports_memory_locator() {
        let (_hash, len, mermaid) = super::run_application_flow_from_locator("cas://memory")
            .await
            .expect("run from memory locator");
        assert_eq!(len, "application payload".len());
        assert!(mermaid.is_none());
    }

    #[tokio::test]
    /// Verifies explicit filesystem configuration executes filesystem-specific flow.
    async fn router_flow_supports_filesystem_backend_variant() {
        let temp = tempfile::tempdir().expect("tempdir");
        let cas = mediapm_cas::CasConfig::filesystem(temp.path())
            .open()
            .await
            .expect("open filesystem config");
        let (_hash, len, mermaid) = super::run_application_flow(&cas).await.expect("run app flow");
        assert_eq!(len, "application payload".len());
        let mermaid = mermaid.expect("filesystem flow should return visualization");
        assert!(mermaid.contains("flowchart TD"));
    }
}
