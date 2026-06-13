//! CAS maintenance demo — optimize, prune, GC, and repair operations.
//!
//! Run: `cargo run --example cas_maintenance -p mediapm-cas`

use bytes::Bytes;
use mediapm_cas::api::{CasApi, CasMaintenanceApi, ConstraintApi};
use mediapm_cas::storage::in_memory::new_in_memory_cas;
use std::collections::BTreeSet;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cas = new_in_memory_cas();

    // --- populate with objects ---
    let data = ["foo", "bar", "baz", "qux"];
    let mut hashes = Vec::with_capacity(data.len());
    for s in &data {
        hashes.push(cas.put(Bytes::from_static(s.as_bytes())).await?);
    }
    println!("Stored {} objects.", hashes.len());

    // --- set a constraint ---
    let mut bases = BTreeSet::new();
    bases.insert(hashes[0]);
    cas.set_constraint(hashes[1], bases).await?;
    println!("Constraint: {:?} → {:?}", hashes[1], hashes[0]);

    // --- optimize_once ---
    let opt = cas.optimize_once().await?;
    println!(
        "Optimize: {} WAL entries consumed, maintenance: {}",
        opt.wal_entries_consumed, opt.maintenance_done
    );

    // --- prune_constraints ---
    let pruned = cas.prune_constraints().await?;
    println!("Pruned {} constraint entries.", pruned.removed);

    // --- list_all_hashes ---
    let all = cas.list_all_hashes().await?;
    println!("Current objects: {}", all.len());

    Ok(())
}
