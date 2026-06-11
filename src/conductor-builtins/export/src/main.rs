//! Standalone runner for the export builtin crate.
//!
//! `export` is impure and writes file/folder payloads to host paths.

mediapm_utils::builtin_main_single_writer!(mediapm_conductor_builtin_export);
