//! Infrastructure layer for filesystem persistence and integrity operations.
//!
//! This layer is where side effects meet domain semantics. Keeping these
//! concerns isolated makes it easier to reason about safety (atomic writes,
//! referential integrity) and platform-specific behavior.

pub mod formatter;
pub mod gc;
pub mod provider;
pub mod store;
pub mod verify;
