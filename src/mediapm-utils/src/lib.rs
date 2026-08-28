//! Shared utilities for mediapm crate families.
//!
//! - [`types`] — `StringMap`, `BinaryInputMap`
//! - [`path`] — `PathMode` and path resolvers
//! - [`builtin`] — builtin descriptor helpers and CLI argument parsing
//! - [`timestamp`] — `Timestamp` (Unix-epoch nanoseconds)
//! - [`generated`] — `GENERATED_FILE_BANNER`, `prepend_banner`
//! - [`temp`] — prefixed temp directories for tests, examples, and runtime
//!
//! The `cli` feature enables `BuiltinCliArgs` and `parse_string_pairs`.

#![warn(clippy::all, clippy::pedantic, missing_docs)]
#![allow(clippy::module_name_repetitions)]

pub mod builtin;
pub mod generated;
pub mod path;
pub mod progress;
pub mod temp;
pub mod timestamp;
pub mod types;

#[cfg(feature = "nickel")]
pub mod nickel;

pub use timestamp::Timestamp;
pub use types::{BinaryInputMap, StringMap};
