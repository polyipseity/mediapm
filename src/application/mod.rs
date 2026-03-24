//! Application orchestration layer.
//!
//! This layer converts domain/config data into executable plans and applies
//! those plans via the infrastructure layer.
//!
//! Why a dedicated application layer:
//! - keeps domain definitions independent from command runtime concerns,
//! - keeps CLI thin by centralizing orchestration behavior,
//! - provides a clear seam for future alternative frontends.

pub mod enrichment;
pub mod executor;
pub mod history;
pub mod planner;
