//! One owned value for a scheduled tool: its specification, with its runtime
//! inside it.
//!
//! A tool used to arrive as a `(ToolSpec, ToolRuntime)` pair whose two halves
//! were independently mutable while only the spec reached the generated
//! conductor document. Patching the returned runtime alone was therefore a
//! silent policy loss, and it happened. This type carries one value so the two
//! cannot disagree: the runtime a caller reads is derived from the spec it
//! mutated.

use mediapm_conductor::config::{ToolRuntime, ToolSpec};

/// A tool's specification and the runtime derived from it.
pub(crate) struct ToolDefinition {
    /// The one value a builder owns. Its `runtime` field is the only place a
    /// policy is stated.
    spec: ToolSpec,
}

impl ToolDefinition {
    /// Wrap an assembled specification.
    #[must_use]
    pub(crate) const fn new(spec: ToolSpec) -> Self {
        Self { spec }
    }

    /// The specification, for reading.
    #[must_use]
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "read accessor exercised by lib tests and kept for the Task 7 sync refactor"
        )
    )]
    pub(crate) const fn spec(&self) -> &ToolSpec {
        &self.spec
    }

    /// The specification, for a builder to stamp its policy into.
    pub(crate) const fn spec_mut(&mut self) -> &mut ToolSpec {
        &mut self.spec
    }

    /// The runtime the spec carries.
    ///
    /// Derived rather than stored: a stored copy would be the second value this
    /// type exists to remove.
    #[must_use]
    pub(crate) fn runtime(&self) -> ToolRuntime {
        self.spec.runtime.clone()
    }

    /// Take the specification.
    #[must_use]
    pub(crate) fn into_spec(self) -> ToolSpec {
        self.spec
    }

    /// Take the spec and the runtime derived from it.
    ///
    /// The only way to obtain both, so the pair a caller receives cannot carry
    /// a runtime the spec does not.
    #[must_use]
    pub(crate) fn into_parts(self) -> (ToolSpec, ToolRuntime) {
        let runtime = self.spec.runtime.clone();
        (self.spec, runtime)
    }
}
