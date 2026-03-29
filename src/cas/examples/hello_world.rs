//! Minimal `CasConfig` in-memory quickstart.
//!
//! What this example does:
//! 1. Opens an in-memory CAS backend.
//! 2. Stores a fixed UTF-8 payload.
//! 3. Reads it back by hash.
//! 4. Prints hash + restored content.
//!
//! This example intentionally performs no filesystem writes.

use bytes::Bytes;
use mediapm_cas::{CasApi, CasConfig};

/// Executes the hello-world CAS flow and returns `(hash, restored_payload)`.
async fn run_hello_world_demo() -> Result<(String, Bytes), Box<dyn std::error::Error>> {
    let cas = CasConfig::in_memory().open().await?;

    let hash = cas.put(Bytes::from_static(b"hello world from mediapm-cas")).await?;
    let restored = cas.get(hash).await?;

    Ok((hash.to_string(), restored))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (hash, restored) = run_hello_world_demo().await?;

    println!("stored hash: {hash}");
    println!("restored payload: {}", String::from_utf8_lossy(&restored));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::run_hello_world_demo;

    #[tokio::test]
    async fn hello_world_round_trips_expected_payload() {
        let (hash, restored) = run_hello_world_demo().await.expect("run hello-world demo");
        assert!(!hash.is_empty(), "hash should be printable and non-empty");
        assert_eq!(restored.as_ref(), b"hello world from mediapm-cas");
    }
}
