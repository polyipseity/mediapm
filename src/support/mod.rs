//! Shared support layer.
//!
//! Utilities in this module are intentionally small, dependency-light helpers
//! reused across multiple layers. Centralizing them avoids duplicated logic for
//! deterministic JSON handling and timestamp formatting.

pub mod util;
