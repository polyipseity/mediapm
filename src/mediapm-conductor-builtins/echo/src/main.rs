//! Standalone runner for the `echo` builtin tool.
//!
//! This runner uses the shared builtin CLI pattern: JSON string-map output.

mediapm_utils::builtin_main_single_writer!(mediapm_conductor_builtin_echo);
