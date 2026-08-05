//! Shared utilities for mediapm crate families.
//!
//! This crate provides:
//! - [`types`] — Common type aliases (`StringMap`, `BinaryInputMap`)
//! - [`path`] — Path-resolution utilities (`PathMode`, path resolvers)
//! - [`builtin`] — Builtin descriptor helpers and CLI argument parsing
//! - [`timestamp`] — Unix-epoch nanosecond timestamp type (`Timestamp`)
//! - [`generated`] — Shared generated-file banner (`GENERATED_FILE_BANNER`, `prepend_banner`)
//!
//! # Crate layout
//!
//! ```text
//! mediapm-utils
//! ├── types      — StringMap, BinaryInputMap
//! ├── path       — PathMode, parse_path_mode, resolve_path_for_root, etc.
//! ├── builtin    — describe(), describe_json_compact(), BuiltinCliArgs, parse_string_pairs
//! ├── timestamp  — Timestamp (Unix-epoch nanoseconds)
//! └── generated  — GENERATED_FILE_BANNER, prepend_banner
//! ```
//!
//! # Feature flags
//!
//! - `cli` (optional): enables `BuiltinCliArgs` and `parse_string_pairs`.

#![warn(clippy::all, clippy::pedantic, missing_docs)]
#![allow(clippy::module_name_repetitions)]

pub mod builtin;
pub mod generated;
pub mod path;
pub mod progress;
pub mod timestamp;
pub mod types;

#[cfg(feature = "nickel")]
pub mod nickel;

pub use timestamp::Timestamp;
pub use types::{BinaryInputMap, StringMap};
