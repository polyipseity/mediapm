//! Standalone runner for the fs builtin crate.
//!
//! `fs` is impure, so CLI success is primarily the filesystem side effect.

mediapm_utils::builtin_main_single_writer!(mediapm_conductor_builtin_fs);
