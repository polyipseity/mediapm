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
//!
//! # Detailed architecture guide
//!
//! ## Design goals
//!
//! mediapm is intentionally optimized for:
//!
//! - **Determinism**: identical inputs should produce identical plans and
//!   predictable state transitions.
//! - **Auditability**: sidecars preserve provenance and lineage instead of
//!   overwriting prior history.
//! - **Safety**: storage writes aim to be atomic; destructive operations are
//!   explicit.
//! - **Composability**: planning and execution are separated so each can evolve
//!   independently.
//!
//! ## Core identity model
//!
//! Two identities coexist by design:
//!
//! 1. **Canonical URI identity** (`canonical_uri`)
//!    - Represents logical media item intent.
//!    - Prevents duplicate entries caused by different path spellings.
//! 2. **Content hash identity** (`Blake3Hash`)
//!    - Represents exact bytes for a concrete variant.
//!    - Backed by content-addressed storage under `.mediapm/objects/blake3/...`.
//!
//! This split allows one logical media declaration to accumulate multiple
//! concrete byte variants over time while retaining provenance.
//!
//! ## Layered module map
//!
//! - [`crate::configuration`]
//!   - Parse and persist declarative desired state.
//! - [`crate::domain`]
//!   - URI canonicalization, sidecar schema, metadata shape, migration chain.
//! - [`crate::application`]
//!   - Pure planning and effect execution orchestration.
//! - [`crate::infrastructure`]
//!   - Filesystem-backed store, verification, GC, formatting, provider adapters.
//! - [`crate::support`]
//!   - Shared deterministic utilities.
//!
//! ## Runtime flow
//!
//! Typical `sync`:
//!
//! 1. CLI loads config and resolves workspace.
//! 2. Planner canonicalizes identifiers and emits deterministic effects.
//! 3. Executor applies effects:
//!    - import bytes into object store,
//!    - update sidecar lineage/history,
//!    - materialize configured links,
//!    - optionally enrich metadata via provider queries.
//! 4. Sidecars are written canonically and atomically.
//!
//! Typical `verify`:
//!
//! 1. Load sidecars (migrating old schema versions on read).
//! 2. Validate references and object-hash consistency.
//! 3. Return complete issue report.
//!
//! ## Sidecar invariants to preserve
//!
//! - `original.original_variant_hash` points to a known variant.
//! - `variant.object_relpath` matches the hash-derived fan-out path scheme.
//! - Edit events reference known `from`/`to` variants.
//! - Schema version and migration provenance remain coherent.
//!
//! ## Storage safety model
//!
//! - Object files are immutable once created.
//! - Sidecars are mutable but serialized deterministically.
//! - Writes use temp-file + rename patterns.
//! - GC is dry-run by default and destructive only with explicit apply mode.
//!
//! ## Safe extension workflow
//!
//! 1. Define or extend domain semantics first.
//! 2. Update planner effect model if desired-state behavior changes.
//! 3. Implement execution/infrastructure behavior.
//! 4. Add/update tests for behavior guarantees.
//! 5. Update rustdoc so architecture intent remains clear.

pub mod application;
pub mod configuration;
pub mod domain;
pub mod infrastructure;
pub mod support;
