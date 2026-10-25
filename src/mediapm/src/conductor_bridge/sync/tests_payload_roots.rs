//! What the sync records about where a tool's payload lives.
//!
//! The executor resolves a payload-relative command under `runtime.payload_root`,
//! and the whole staging model rests on that root naming the directory the
//! provisioner actually filled. Nothing else writes it, so the sync's stamp is
//! the only producer, and an entry left behind by an earlier build is replaced
//! rather than trusted.

use mediapm_conductor::{
    NickelDocument, PayloadStaging, ToolKindSpec, ToolRuntime, ToolSpec,
    runtime_env::provisioned_payload_dir,
};
use std::collections::BTreeMap;

use super::*;

/// Builds paths whose tools directory sits in a caller-owned temp root.
fn paths_under(root: &std::path::Path) -> MediaPmPaths {
    MediaPmPaths::from_root(root)
}

/// Builds a generated document holding one tool spec with a non-empty content
/// map, keyed by `key`.
fn document_with_tool(key: &str, staging: PayloadStaging, payload_root: &str) -> NickelDocument {
    let mut document = NickelDocument::default();
    document.tools.insert(
        key.to_string(),
        ToolSpec {
            version: None,
            name: "scripted".to_string(),
            kind: ToolKindSpec::default(),
            runtime: ToolRuntime {
                content_map: BTreeMap::from([(
                    "linux/scripted".to_string(),
                    format!("blake3:{}", blake3::Hash::from([7u8; 32]).to_hex()),
                )]),
                payload_root: payload_root.to_string(),
                payload_staging: staging,
                ..Default::default()
            },
            ..Default::default()
        },
    );
    document
}

/// The stamp names the provisioned directory on both carriers, and the strategy
/// that reads it.
///
/// The document is what a standalone `mediapm-conductor run` reads and the
/// runtime map is what the same process executes from, so a stamp that reached
/// only one of them would leave the two disagreeing about the same tool.
#[test]
fn regression_stamp_payload_roots_names_the_provisioned_directory() {
    let root = mediapm_utils::temp::artifact_dir().expect("workspace root");
    let paths = paths_under(root.path());
    let conductor_tool_id = "scripted@blake3:abc";
    let payload_dir = provisioned_payload_dir(&paths.tools_dir, conductor_tool_id);
    std::fs::create_dir_all(&payload_dir).expect("provisioned payload dir");

    let mut document = document_with_tool(conductor_tool_id, PayloadStaging::Copy, "");
    let mut runtimes = BTreeMap::from([(
        conductor_tool_id.to_string(),
        ToolRuntime {
            content_map: document.tools[conductor_tool_id].runtime.content_map.clone(),
            payload_staging: PayloadStaging::Copy,
            ..Default::default()
        },
    )]);

    stamp_payload_roots(&paths, &mut document, &mut runtimes);

    let expected = payload_dir
        .to_str()
        .expect("the payload directory is a UTF-8 path under the temp root")
        .to_string();
    assert_eq!(
        document.tools[conductor_tool_id].runtime.payload_root, expected,
        "the document is what a standalone conductor run reads, so it has to name the root"
    );
    assert_eq!(
        document.tools[conductor_tool_id].runtime.payload_staging,
        PayloadStaging::InPlace,
        "a provisioned directory is executed from, not copied out of it"
    );
    assert_eq!(
        runtimes[conductor_tool_id].payload_root, expected,
        "the runtime map is what this process executes from, so it has to name the same root"
    );
    assert_eq!(runtimes[conductor_tool_id].payload_staging, PayloadStaging::InPlace);
}

/// A tool whose payload directory is absent keeps what its spec builder
/// declared.
///
/// Stamping a root that does not exist would turn a refusal into a wrong
/// answer: the executor refuses a missing root by name, and it can only do that
/// when nothing pretended the directory was there.
#[test]
fn a_tool_without_a_provisioned_directory_keeps_its_declared_strategy() {
    let root = mediapm_utils::temp::artifact_dir().expect("workspace root");
    let paths = paths_under(root.path());
    let conductor_tool_id = "scripted@blake3:def";

    let mut document = document_with_tool(conductor_tool_id, PayloadStaging::Copy, "");
    let mut runtimes = BTreeMap::from([(
        conductor_tool_id.to_string(),
        ToolRuntime {
            content_map: document.tools[conductor_tool_id].runtime.content_map.clone(),
            payload_staging: PayloadStaging::Copy,
            ..Default::default()
        },
    )]);

    stamp_payload_roots(&paths, &mut document, &mut runtimes);

    assert_eq!(
        document.tools[conductor_tool_id].runtime.payload_root, "",
        "nothing stamps a root for a payload that was not provisioned"
    );
    assert_eq!(document.tools[conductor_tool_id].runtime.payload_staging, PayloadStaging::Copy);
    assert_eq!(runtimes[conductor_tool_id].payload_staging, PayloadStaging::Copy);
}

/// A tool with an empty content map is not stamped even when a directory exists
/// under its id.
///
/// A zero-payload tool has nothing to execute from a payload root, and giving it
/// one would make the executor read a directory for a tool that needs none.
#[test]
fn a_tool_without_a_content_map_is_not_stamped() {
    let root = mediapm_utils::temp::artifact_dir().expect("workspace root");
    let paths = paths_under(root.path());
    let conductor_tool_id = "import@v1";
    let payload_dir = provisioned_payload_dir(&paths.tools_dir, conductor_tool_id);
    std::fs::create_dir_all(&payload_dir).expect("provisioned payload dir");

    let mut document = document_with_tool(conductor_tool_id, PayloadStaging::Copy, "");
    document.tools.get_mut(conductor_tool_id).expect("the spec").runtime.content_map.clear();
    let mut runtimes = BTreeMap::from([(
        conductor_tool_id.to_string(),
        ToolRuntime { payload_staging: PayloadStaging::Copy, ..Default::default() },
    )]);

    stamp_payload_roots(&paths, &mut document, &mut runtimes);

    assert_eq!(
        document.tools[conductor_tool_id].runtime.payload_root, "",
        "a tool with no payload has no payload root"
    );
    assert_eq!(runtimes[conductor_tool_id].payload_root, "");
}
