//! Domain layer for mediapm.
//!
//! This layer models **what the system is** rather than how commands are
//! executed. It includes:
//! - identity and URI normalization,
//! - sidecar schema and domain records,
//! - metadata probing shape,
//! - schema migration behavior.
//!
//! The domain layer is deliberately dependency-light and long-lived. Most
//! evolution in mediapm should happen by extending domain semantics first,
//! then adapting planner/executor behavior around those semantics.

pub mod canonical;
pub mod metadata;
pub mod migration;
pub mod model;
