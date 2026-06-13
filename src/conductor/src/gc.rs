//! CAS maintenance orchestration for conductor.
//!
//! [`run_cas_gc_sweep`] composes CAS maintenance operations — optimize and
//! prune constraints — used by the background GC loop in the node actor and
//! by the CLI.

use mediapm_cas::{CasApi, CasMaintenanceApi, PruneReport};
use tracing;

use crate::error::ConductorError;

/// Runs full CAS maintenance: optimize, prune constraints.
///
/// This is the one-stop function called by the background GC loop (node
/// actor), the `RunGc` handler, and the CLI. It does **not** touch instance
/// GC — callers must run instance GC separately.
///
/// # Errors
///
/// Returns [`ConductorError::Cas`] when any maintenance operation fails.
pub async fn run_cas_gc_sweep<C>(cas: &C) -> Result<PruneReport, ConductorError>
where
    C: CasApi + CasMaintenanceApi,
{
    tracing::info!("GC phase 1/3: optimize_once");
    cas.optimize_once().await.map_err(ConductorError::Cas)?;
    tracing::info!("GC phase 2/3: prune_constraints");
    let report = cas.prune_constraints().await.map_err(ConductorError::Cas)?;
    tracing::info!("GC phase 3/3: complete (removed={})", report.removed);
    Ok(report)
}
