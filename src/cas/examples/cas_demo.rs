//! CAS API demo — put/get/stat/delete cycle with constraint usage.
//!
//! Run: `cargo run --example cas_demo -p mediapm-cas`

use bytes::Bytes;
use mediapm_cas::api::{CasApi, ConstraintApi};
use mediapm_cas::error::CasError;
use mediapm_cas::hash::Hash;
use mediapm_cas::storage::in_memory::new_in_memory_cas;
use std::collections::BTreeSet;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cas = new_in_memory_cas();

    // --- put/get/stat/delete cycle ---
    let data = Bytes::from_static(b"hello content-addressable world");
    let hash = cas.put(data.clone()).await?;
    println!("Stored: {hash}");

    let retrieved = cas.get(hash).await?;
    assert_eq!(retrieved, data);
    println!("Retrieved: {} bytes", retrieved.len());

    let info = cas.stat(hash).await?;
    println!("  encoding: {:?}, len: {}", info.encoding, info.len);
    assert_eq!(info.len, data.len() as u64);

    cas.delete(hash).await?;
    let result = cas.get(hash).await;
    assert!(matches!(result, Err(CasError::NotFound(_))));
    println!("Deleted and confirmed gone.\n");

    // --- zero hash sentinel ---
    let zero = Hash::zero();
    assert!(cas.get(zero).await?.is_empty());
    println!("Zero hash: sentinel ok.");

    // --- constraints ---
    let a = Hash::from_content(b"base data");
    let b = Hash::from_content(b"derived data");

    // Store the bases first, then set a constraint.
    cas.put(Bytes::from_static(b"base data")).await?;
    cas.put(Bytes::from_static(b"derived data")).await?;

    let mut bases = BTreeSet::new();
    bases.insert(a);
    cas.set_constraint(b, bases).await?;

    let stored = cas.get_constraint(b).await?;
    println!("\nConstraint for {b}: {stored:?}");

    // Effective bases intersect with live hashes.
    let live = [a, b].into_iter().collect();
    let effective = cas.effective_bases(b, &live).await?;
    println!("Effective bases: {effective:?}");

    Ok(())
}
