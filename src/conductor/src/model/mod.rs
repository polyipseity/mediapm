//! Conductor data model.
//!
//! This module groups the two persisted schemas used by the conductor:
//! - user/machine configuration documents (Nickel-backed wire format),
//! - immutable orchestration state.

pub mod config;
pub mod state;
