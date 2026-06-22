//! Orchestration layer — actors, protocol types, coordinator, and profiler.
//!
//! This module is the runtime heart of conductor.  It owns the
//! [`WorkflowCoordinator`] that drives workflow execution across a pool of
//! step-worker actors, plus the protocol types and profiling helpers those
//! actors depend on.

pub(crate) mod actors;
pub(crate) mod config;
pub(crate) mod coordinator;
pub(crate) mod node;
pub(crate) mod profiler;
pub(crate) mod protocol;
