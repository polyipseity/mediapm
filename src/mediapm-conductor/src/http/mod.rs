//! Shared async HTTP client for conductor tool-fetch operations.
//!
//! # Decoupling contract
//!
//! This module is **fully self-contained**: it depends on external crates only
//! (`reqwest`, `std`) and intentionally imports **nothing from `crate::`**.
//! It defines its own error type [`HttpClientError`] rather than using
//! [`crate::error::ConductorError`].
//!
//! This design makes the module independently extractable into a standalone
//! crate. If you ever need to share this client outside `mediapm-conductor`,
//! copy the directory and adjust the `Cargo.toml` dependencies — no code
//! changes to the module body are required.
//!
//! # Error boundary
//!
//! The unit of encapsulation is [`HttpClientError`]. Every function returns
//! this type. The caller in `crate::tools::provider` maps it to
//! [`ConductorError`] at the call site, never inside this module.

pub mod client;
pub use client::*;
