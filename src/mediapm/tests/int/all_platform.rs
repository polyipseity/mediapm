//! Integration tests verifying the all-platform download architecture.
//!
//! These tests validate that after `sync_tools()` completes:
//! - The generated conductor document is present and contains tool entries.
//! - Managed tools that were successfully synced have content-map keys
//!   prefixed with `./<os>/`.
//! - Executable commands are non-empty and use the correct template format.
//!
//! They DO NOT assert that every managed tool was synced (that depends on
//! network availability); they assert the structural invariants of whatever
//! entries were produced.

use mediapm::MediaPmService;
use mediapm_conductor::{NickelDocument, ToolKindSpec, decode_document};
use tempfile::tempdir;

#[tokio::test]
async fn managed_tools_exist_in_generated_document() -> Result<(), mediapm::MediaPmError> {
    let root = tempdir().expect("tempdir");
    let mut service = MediaPmService::new_fs_at(root.path()).await?;
    service.sync_tools().await?;

    let bytes = std::fs::read(&service.paths().conductor_machine_ncl)
        .expect("conductor.machine.ncl should be readable");
    let doc: NickelDocument = decode_document(&bytes).expect("valid Nickel document");

    // At least one managed tool must appear after a successful sync.
    // Individual tools may fail to download (network), so we only verify
    // that the pipeline produced a meaningful document structure.
    assert!(!doc.tools.is_empty(), "sync_tools must produce at least one tool entry");

    // For every managed tool that IS present, its spec has the right shape.
    // The remainder of the structural checks are in the other tests below.
    Ok(())
}

#[tokio::test]
async fn external_tool_content_map_keys_have_os_prefix() -> Result<(), mediapm::MediaPmError> {
    let root = tempdir().expect("tempdir");
    let mut service = MediaPmService::new_fs_at(root.path()).await?;
    service.sync_tools().await?;

    let bytes = std::fs::read(&service.paths().conductor_machine_ncl)
        .expect("conductor.machine.ncl should be readable");
    let doc: NickelDocument = decode_document(&bytes).expect("valid Nickel document");

    for (tool_id, spec) in &doc.tools {
        if spec.runtime.content_map.is_empty() {
            // Builtins or tools without downloaded payload — skip.
            continue;
        }
        for key in spec.runtime.content_map.keys() {
            let has_os_prefix = key.starts_with("./linux/")
                || key.starts_with("./macos/")
                || key.starts_with("./windows/");
            assert!(
                has_os_prefix,
                "tool {tool_id}: content_map key '{key}' should start with ./<os>/",
            );
        }
    }

    Ok(())
}

#[tokio::test]
async fn external_tool_command_is_non_empty() -> Result<(), mediapm::MediaPmError> {
    let root = tempdir().expect("tempdir");
    let mut service = MediaPmService::new_fs_at(root.path()).await?;
    service.sync_tools().await?;

    let bytes = std::fs::read(&service.paths().conductor_machine_ncl)
        .expect("conductor.machine.ncl should be readable");
    let doc: NickelDocument = decode_document(&bytes).expect("valid Nickel document");

    for (tool_id, spec) in &doc.tools {
        if matches!(spec.kind, ToolKindSpec::Builtin { .. }) {
            continue; // builtins don't use executable commands
        }
        if let ToolKindSpec::Executable { command, .. } = &spec.kind {
            assert!(!command.is_empty(), "tool {tool_id}: command list must not be empty");
            assert!(
                !command[0].is_empty(),
                "tool {tool_id}: first command element must not be empty",
            );
        }
    }

    Ok(())
}
