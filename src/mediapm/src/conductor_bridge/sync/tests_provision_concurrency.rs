//! The bound on how many provisioning entries run at once.

use super::*;

#[test]
fn tool_provision_concurrency_default_returns_positive() {
    let concurrency = default_tool_provision_concurrency();
    assert!(concurrency >= 1, "concurrency must be >= 1, got {concurrency}");
}

#[test]
fn tool_provision_concurrency_env_override() {
    unsafe {
        std::env::set_var(ENV_TOOL_PROVISION_CONCURRENCY, "2");
    }
    let concurrency = default_tool_provision_concurrency();
    assert_eq!(concurrency, 2, "env override should return 2");
    unsafe {
        std::env::remove_var(ENV_TOOL_PROVISION_CONCURRENCY);
    }
}

#[test]
fn tool_provision_concurrency_env_invalid_falls_back() {
    unsafe {
        std::env::set_var(ENV_TOOL_PROVISION_CONCURRENCY, "xyz");
    }
    let concurrency = default_tool_provision_concurrency();
    assert!(concurrency >= 1, "invalid env should fall back to >= 1, got {concurrency}");
    unsafe {
        std::env::remove_var(ENV_TOOL_PROVISION_CONCURRENCY);
    }
}
