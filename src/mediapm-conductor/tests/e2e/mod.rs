//! End-to-end integration scenarios for the conductor runtime.

/// Workflow execution scenarios spanning repeated runs.
mod workflow;

/// Comprehensive workflow-lifecycle scenarios: cache, GC, tool update.
mod lifecycle;

/// External-data invariant and decode validation scenarios.
mod external_data_and_validation;

/// DAG cycle detection scenarios.
mod cycle;
