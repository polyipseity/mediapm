//! Configuration layer.
//!
//! This module defines how users declare desired media state and how that
//! declaration is loaded from disk. It is intentionally separated from planning
//! and execution so configuration semantics can evolve without coupling to
//! filesystem side effects.

pub mod config;
