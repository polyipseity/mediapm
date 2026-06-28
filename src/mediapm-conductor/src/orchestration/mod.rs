//! Orchestration layer — actors, protocol types, and coordinator.
//!
//! This module is the runtime heart of conductor.  It owns the
//! [`WorkflowCoordinator`] that drives workflow execution across a pool of
//! step-worker actors, plus the protocol types those actors depend on.

pub(crate) mod config;
pub(crate) mod coordinator;
pub(crate) mod node;
pub(crate) mod protocol;
pub(crate) mod step_worker;
