//! mediapm library crate.
//!
//! # Why this crate exists
//!
//! `mediapm` keeps a local media workspace in sync with a declarative desired
//! state. Instead of mutating files ad hoc, it models media identity via
//! canonical URIs and content identity via BLAKE3 hashes, then reconciles from
//! a deterministic plan.
//!
//! # Architectural style
//!
//! The codebase follows a functional-core / imperative-shell direction:
//!
//! - `configuration`: declarative config schema and IO.
//! - `domain`: identity rules, sidecar schema, metadata shape, migrations.
//! - `application`: planning and effect orchestration.
//! - `infrastructure`: filesystem-backed persistence and integrity utilities.
//! - `support`: shared utility helpers.
//!
//! In practice, commands flow as:
//!
//! 1. Parse config and canonicalize identity inputs.
//! 2. Build a deterministic `Plan` from desired declarations.
//! 3. Execute effects against `.mediapm/` storage.
//! 4. Verify invariants and expose machine-readable reports.
//!
//! This separation is intentional: it keeps planning testable and side effects
//! explicit, which is critical for debugging and trust when operating on media
//! libraries.

pub mod application;
pub mod configuration;
pub mod domain;
pub mod infrastructure;
pub mod support;
