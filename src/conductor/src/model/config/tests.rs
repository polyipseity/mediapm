// Tests for conductor model config.
use std::collections::BTreeMap;

use mediapm_cas::Hash;

use super::{
    AddExternalDataOptions, AddToolConfigMode, AddToolOptions, ExternalContentRef,
    MachineNickelDocument, OutputSaveMode, ToolConfigSpec, ToolKindSpec, ToolSpec,
    UserNickelDocument,
};

/// Verifies add-tool can insert both tool spec and tool config in one call.
#[test]
fn add_tool_inserts_spec_and_config() {
    let mut document = UserNickelDocument::default();
    let options = AddToolOptions::new(ToolSpec {
        kind: ToolKindSpec::Executable {
            command: vec!["demo-tool".to_string()],
            env_vars: BTreeMap::new(),
            success_codes: vec![0],
        },
        ..ToolSpec::default()
    })
    .with_tool_config(ToolConfigSpec {
        max_concurrent_calls: 2,
        max_retries: -1,
        description: Some("demo executable runtime config".to_string()),
        input_defaults: BTreeMap::new(),
        env_vars: BTreeMap::new(),
        content_map: Some(BTreeMap::from([(
            "payload.txt".to_string(),
            Hash::from_content(b"demo-hash-a"),
        )])),
    });

    document.add_tool("demo@1.0.0", options).expect("add tool with config should succeed");

    assert!(document.tools.contains_key("demo@1.0.0"));
    assert!(document.tool_configs.contains_key("demo@1.0.0"));
    assert!(document.external_data.contains_key(&Hash::from_content(b"demo-hash-a")));
}

/// Verifies duplicate insertion fails unless overwrite policy is enabled.
#[test]
fn add_tool_rejects_duplicate_without_overwrite() {
    let mut document = UserNickelDocument::default();
    let options = AddToolOptions::new(ToolSpec::default());

    document.add_tool("echo@1.0.0", options.clone()).expect("first insert should succeed");

    let error = document
        .add_tool("echo@1.0.0", options)
        .expect_err("second insert without overwrite should fail");
    assert!(error.to_string().contains("already exists"));
}

/// Verifies overwrite mode can replace an entry and drop stale config.
#[test]
fn add_tool_overwrite_can_remove_existing_config() {
    let mut document = UserNickelDocument::default();

    document
        .add_tool(
            "tool@1.0.0",
            AddToolOptions::new(ToolSpec {
                kind: ToolKindSpec::Executable {
                    command: vec!["first".to_string()],
                    env_vars: BTreeMap::new(),
                    success_codes: vec![0],
                },
                ..ToolSpec::default()
            })
            .with_tool_config(ToolConfigSpec {
                max_concurrent_calls: 1,
                max_retries: -1,
                description: Some("initial executable runtime config".to_string()),
                input_defaults: BTreeMap::new(),
                env_vars: BTreeMap::new(),
                content_map: Some(BTreeMap::from([(
                    "payload.txt".to_string(),
                    Hash::from_content(b"demo-hash-b"),
                )])),
            }),
        )
        .expect("initial tool insert should succeed");

    document
        .add_tool(
            "tool@1.0.0",
            AddToolOptions::new(ToolSpec::default()).overwrite_existing(true).remove_tool_config(),
        )
        .expect("overwrite with remove config should succeed");

    assert!(document.tools.contains_key("tool@1.0.0"));
    assert!(!document.tool_configs.contains_key("tool@1.0.0"));
    assert!(!document.external_data.contains_key(&Hash::from_content(b"demo-hash-b")));
}

/// Verifies builtin entries reject `content_map` at add-tool validation time.
#[test]
fn add_tool_rejects_builtin_with_content_map() {
    let mut document = UserNickelDocument::default();

    let error = document
        .add_tool(
            "echo@1.0.0",
            AddToolOptions {
                spec: ToolSpec::default(),
                overwrite_existing: false,
                config_mode: AddToolConfigMode::Replace(ToolConfigSpec {
                    max_concurrent_calls: 1,
                    max_retries: -1,
                    description: Some("invalid builtin runtime config".to_string()),
                    input_defaults: BTreeMap::new(),
                    env_vars: BTreeMap::new(),
                    content_map: Some(BTreeMap::from([(
                        "payload.txt".to_string(),
                        Hash::from_content(b"demo-hash-c"),
                    )])),
                }),
            },
        )
        .expect_err("builtin content_map should fail validation");

    assert!(error.to_string().contains("cannot have tool_configs.content_map"));
}

/// Verifies machine external-data insertion succeeds for new hash keys.
#[test]
fn add_machine_external_data_inserts_entry() {
    let mut machine = MachineNickelDocument::default();
    let fixture_hash = Hash::from_content(b"fixture");
    machine
        .add_external_data(
            fixture_hash,
            AddExternalDataOptions::new(ExternalContentRef {
                description: Some("fixture payload".to_string()),
                save: None,
            }),
        )
        .expect("machine external data insert should succeed");

    assert!(machine.external_data.contains_key(&fixture_hash));
}

/// Verifies duplicate machine external-data insertion fails unless overwrite mode is enabled.
#[test]
fn add_machine_external_data_rejects_duplicate_without_overwrite() {
    let mut machine = MachineNickelDocument::default();
    let fixture_hash = Hash::from_content(b"fixture-a");
    machine
        .add_external_data(
            fixture_hash,
            AddExternalDataOptions::new(ExternalContentRef { description: None, save: None }),
        )
        .expect("first insert should succeed");

    let error = machine
        .add_external_data(
            fixture_hash,
            AddExternalDataOptions::new(ExternalContentRef { description: None, save: None }),
        )
        .expect_err("duplicate insert without overwrite should fail");

    assert!(error.to_string().contains("already exists"));
}

/// Verifies machine external-data insertion rejects unsaved (`false`) save policy.
#[test]
fn add_machine_external_data_rejects_unsaved_save_policy() {
    let mut machine = MachineNickelDocument::default();
    let fixture_hash = Hash::from_content(b"fixture-unsaved");

    let error = machine
        .add_external_data(
            fixture_hash,
            AddExternalDataOptions::new(ExternalContentRef {
                description: Some("fixture unsaved".to_string()),
                save: Some(OutputSaveMode::Unsaved),
            }),
        )
        .expect_err("unsaved external-data save policy should fail validation");

    assert!(error.to_string().contains("cannot be false/unsaved"));
}

/// Verifies stale managed tool-content roots are removed while non-managed
/// external-data entries are preserved.
#[test]
fn sync_tool_content_external_data_roots_prunes_only_managed_entries() {
    let stale_hash = Hash::from_content(b"stale-tool-content");
    let kept_hash = Hash::from_content(b"kept-user-entry");
    let active_hash = Hash::from_content(b"active-tool-content");

    let mut machine = MachineNickelDocument {
        external_data: BTreeMap::from([
            (
                stale_hash,
                ExternalContentRef {
                    description: Some("managed tool content CAS root for stale".to_string()),
                    save: None,
                },
            ),
            (
                kept_hash,
                ExternalContentRef {
                    description: Some("user-managed fixture".to_string()),
                    save: None,
                },
            ),
        ]),
        tool_configs: BTreeMap::from([(
            "tool@1.0.0".to_string(),
            ToolConfigSpec {
                max_concurrent_calls: -1,
                max_retries: -1,
                description: None,
                input_defaults: BTreeMap::new(),
                env_vars: BTreeMap::new(),
                content_map: Some(BTreeMap::from([("bin/tool".to_string(), active_hash)])),
            },
        )]),
        ..MachineNickelDocument::default()
    };

    machine.sync_tool_content_external_data_roots();

    assert!(machine.external_data.contains_key(&kept_hash));
    assert!(!machine.external_data.contains_key(&stale_hash));
    assert!(machine.external_data.contains_key(&active_hash));
}
