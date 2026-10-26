//! What the sync records about where a tool's payload lives.
//!
//! The executor resolves a payload-relative command under `runtime.payload_root`,
//! and the whole staging model rests on that root naming the directory the
//! provisioner actually filled. Nothing else writes it, so the sync's stamp is
//! the only producer, and an entry left behind by an earlier build is replaced
//! rather than trusted.

use mediapm_cas::Hash;
use mediapm_conductor::state::OutputSaveMode;
use mediapm_conductor::{
    ConfigPayloadStaging, ExternalDataEntry, NickelDocument, PayloadStaging, ToolKindSpec,
    ToolRuntime, ToolSpec, decode_document, encode_document, runtime_env::provisioned_payload_dir,
};
use std::collections::BTreeMap;

use super::*;

/// Builds paths whose tools directory sits in a caller-owned temp root.
fn paths_under(root: &std::path::Path) -> MediaPmPaths {
    MediaPmPaths::from_root(root)
}

/// Builds a generated document holding one tool spec with a non-empty content
/// map, keyed by `key`.
fn document_with_tool(
    key: &str,
    staging: ConfigPayloadStaging,
    payload_root: &str,
) -> NickelDocument {
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
/// The document is what a run executes a tool from, whether the run is this
/// process or a standalone `mediapm-conductor run`; the runtime map is what the
/// sync itself reads to materialize a payload and write `.env.generated`. A
/// stamp that reached only one of them would leave the two disagreeing about the
/// same tool.
#[test]
fn regression_stamp_payload_roots_names_the_provisioned_directory() {
    let root = mediapm_utils::temp::artifact_dir().expect("workspace root");
    let paths = paths_under(root.path());
    let conductor_tool_id = "scripted@blake3:abc";
    let payload_dir = provisioned_payload_dir(&paths.tools_dir, conductor_tool_id);
    std::fs::create_dir_all(&payload_dir).expect("provisioned payload dir");

    let mut document = document_with_tool(conductor_tool_id, ConfigPayloadStaging::Copy, "");
    let mut runtimes = BTreeMap::from([(
        conductor_tool_id.to_string(),
        ToolRuntime {
            content_map: document.tools[conductor_tool_id].runtime.content_map.clone(),
            payload_staging: ConfigPayloadStaging::Copy,
            ..Default::default()
        },
    )]);
    let choices = BTreeMap::from([("scripted".to_string(), ConfigPayloadStaging::InPlace)]);

    stamp_payload_roots(&paths, &mut document, &mut runtimes, &choices, PayloadStaging::InPlace);

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
        ConfigPayloadStaging::InPlace,
        "the tool stated a strategy, so the stamp carries that strategy rather than overriding it"
    );
    assert_eq!(
        runtimes[conductor_tool_id].payload_root, expected,
        "the runtime map drives provisioning and the generated dotenv, so it has to name the same root"
    );
    assert_eq!(runtimes[conductor_tool_id].payload_staging, ConfigPayloadStaging::InPlace);
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

    let mut document = document_with_tool(conductor_tool_id, ConfigPayloadStaging::Copy, "");
    let mut runtimes = BTreeMap::from([(
        conductor_tool_id.to_string(),
        ToolRuntime {
            content_map: document.tools[conductor_tool_id].runtime.content_map.clone(),
            payload_staging: ConfigPayloadStaging::Copy,
            ..Default::default()
        },
    )]);

    stamp_payload_roots(
        &paths,
        &mut document,
        &mut runtimes,
        &BTreeMap::new(),
        PayloadStaging::InPlace,
    );

    assert_eq!(
        document.tools[conductor_tool_id].runtime.payload_root, "",
        "nothing stamps a root for a payload that was not provisioned"
    );
    assert_eq!(
        document.tools[conductor_tool_id].runtime.payload_staging,
        ConfigPayloadStaging::Copy
    );
    assert_eq!(runtimes[conductor_tool_id].payload_staging, ConfigPayloadStaging::Copy);
}

/// A payload a run holds stays on disk through a retain pass that keeps nothing.
///
/// The run executes managed tools after the tool phase returns, and the default
/// `in_place` strategy executes them out of the provisioned tree itself. A
/// retain pass that removed that tree mid-run would take the bytes a live step
/// is running, so the guards the phase materializes have to outlive the phase.
/// This test is the reason the phase answers them instead of dropping them: with
/// the guards held the sweep skips the entry, and once they go it removes it.
///
/// Its reach stops at that mechanism. Whether the library sync keeps the guards
/// bound for the whole run is a property of a call site this test does not
/// enter, and no test here observes a retain pass racing a live step.
#[tokio::test]
async fn regression_a_payload_survives_a_retain_pass_while_the_run_holds_it() {
    let root = mediapm_utils::temp::artifact_dir().expect("workspace root");
    let paths = paths_under(root.path());
    std::fs::create_dir_all(&paths.tools_dir).expect("tools directory");
    let cas = open_workspace_cas_store(&paths).await.expect("open the workspace CAS");
    let hash = cas.put(bytes::Bytes::from_static(b"payload")).await.expect("import the payload");
    let conductor_tool_id = format!("scripted@{hash}");

    let content_map = BTreeMap::from([("linux/scripted".to_string(), hash.to_string())]);
    let mut document = document_with_tool(&conductor_tool_id, ConfigPayloadStaging::InPlace, "");
    document.tools.get_mut(&conductor_tool_id).expect("the spec").runtime.content_map =
        content_map.clone();
    let runtimes = BTreeMap::from([(
        conductor_tool_id.clone(),
        ToolRuntime {
            content_map,
            payload_staging: ConfigPayloadStaging::InPlace,
            ..Default::default()
        },
    )]);

    let guards = materialize_active_tool_runtimes(&paths.tools_dir, cas, &document, &runtimes)
        .await
        .expect("materializing the only active tool succeeds");
    let payload_dir = provisioned_payload_dir(&paths.tools_dir, &conductor_tool_id);
    assert!(payload_dir.is_dir(), "the phase has to materialize the payload it reports on");

    let held = retain_only_tool_dirs(paths.tools_dir.clone(), std::collections::HashSet::new())
        .await
        .expect("the retain pass runs");
    assert!(
        payload_dir.is_dir(),
        "a retain pass must keep a payload the run is still holding, or a live step loses its tool"
    );
    assert!(held.is_empty(), "an entry kept for being in use is not reported as a failed removal");

    drop(guards);
    let released = retain_only_tool_dirs(paths.tools_dir.clone(), std::collections::HashSet::new())
        .await
        .expect("the retain pass runs");
    assert!(!payload_dir.exists(), "once the run lets go, the same sweep reclaims the entry");
    assert!(released.is_empty(), "a successful removal is not a warning");
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

    let mut document = document_with_tool(conductor_tool_id, ConfigPayloadStaging::Copy, "");
    document.tools.get_mut(conductor_tool_id).expect("the spec").runtime.content_map.clear();
    let mut runtimes = BTreeMap::from([(
        conductor_tool_id.to_string(),
        ToolRuntime { payload_staging: ConfigPayloadStaging::Copy, ..Default::default() },
    )]);

    stamp_payload_roots(
        &paths,
        &mut document,
        &mut runtimes,
        &BTreeMap::new(),
        PayloadStaging::InPlace,
    );

    assert_eq!(
        document.tools[conductor_tool_id].runtime.payload_root, "",
        "a tool with no payload has no payload root"
    );
    assert_eq!(runtimes[conductor_tool_id].payload_root, "");
}

/// Encodes a document and hands back its source.
///
/// The source is what a reader loads and what a reviewer diffs, so the
/// assertions inspect it rather than the in-memory value it came from.
fn encode_source(document: NickelDocument) -> String {
    let bytes = encode_document(document).expect("the document encodes");
    String::from_utf8(bytes).expect("a Nickel document is UTF-8")
}

/// Declares every hash a document's tool content maps name, so the document
/// passes the external-data invariant and can be encoded.
fn declare_content_map_hashes(document: &mut NickelDocument) {
    let hashes: Vec<Hash> = document
        .tools
        .values()
        .flat_map(|spec| spec.runtime.content_map.values())
        .filter_map(|value| value.parse::<Hash>().ok())
        .collect();
    for hash in hashes {
        document.external_data.insert(
            hash,
            ExternalDataEntry { description: None, save_mode: OutputSaveMode::Saved },
        );
    }
}

/// Builds a generated document whose runtime states `document_default` and
/// whose tools state the staging and payload root each entry names.
///
/// A strategy that executes a tool out of the provisioned tree is refused
/// without a root, so the entries that ask for one carry a root the way a
/// stamped document does.
fn staged_document(
    document_default: PayloadStaging,
    tools: &[(&str, ConfigPayloadStaging, u8, &str)],
) -> NickelDocument {
    let mut document = NickelDocument::default();
    document.runtime.payload_staging = document_default;
    for (key, staging, hash_byte, payload_root) in tools {
        let content_map = BTreeMap::from([(
            format!("{key}/bin"),
            format!("blake3:{}", blake3::Hash::from([*hash_byte; 32]).to_hex()),
        )]);
        document.tools.insert(
            (*key).to_string(),
            ToolSpec {
                version: None,
                name: key.split('@').next().unwrap_or(key).to_string(),
                kind: ToolKindSpec::default(),
                runtime: ToolRuntime {
                    content_map,
                    payload_staging: *staging,
                    payload_root: (*payload_root).to_string(),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
    }
    declare_content_map_hashes(&mut document);
    document
}

/// A library that asks for `hard_link` gets `hard_link`, once the sync's stamp
/// and the encoder have both had their say.
///
/// The sync used to overwrite the strategy with `in_place` on every tool it
/// stamped, which made this request unrepresentable: the document could say it,
/// and nothing could execute it. The document default is where the request
/// lands, so the tool line stays absent and the emitted document states one
/// strategy for the whole library.
#[test]
fn regression_a_hard_link_request_survives_a_sync() {
    let root = mediapm_utils::temp::artifact_dir().expect("workspace root");
    let paths = paths_under(root.path());
    let conductor_tool_id = "scripted@blake3:hard";
    let payload_dir = provisioned_payload_dir(&paths.tools_dir, conductor_tool_id);
    std::fs::create_dir_all(&payload_dir).expect("provisioned payload dir");

    let mut document = document_with_tool(conductor_tool_id, ConfigPayloadStaging::Inherit, "");
    declare_content_map_hashes(&mut document);
    let mut runtimes = BTreeMap::from([(
        conductor_tool_id.to_string(),
        ToolRuntime {
            content_map: document.tools[conductor_tool_id].runtime.content_map.clone(),
            payload_staging: ConfigPayloadStaging::Inherit,
            ..Default::default()
        },
    )]);
    let choices = BTreeMap::from([("scripted".to_string(), ConfigPayloadStaging::HardLink)]);

    stamp_payload_roots(&paths, &mut document, &mut runtimes, &choices, PayloadStaging::HardLink);

    assert_eq!(
        document.runtime.payload_staging,
        PayloadStaging::HardLink,
        "the library records its request once, at the document level"
    );
    assert_eq!(
        runtimes[conductor_tool_id].payload_staging,
        ConfigPayloadStaging::HardLink,
        "the runtime map the sync itself reads carries the same choice"
    );

    let encoded = encode_source(document);
    assert_eq!(
        encoded.matches("payload_staging").count(),
        1,
        "the default is written once and the tool that repeats it is omitted:\n{encoded}"
    );
    let decoded = decode_document(encoded.as_bytes()).expect("the emitted document loads");
    assert_eq!(decoded.runtime.payload_staging, PayloadStaging::HardLink);
    assert_eq!(
        decoded.tools[conductor_tool_id].runtime.payload_staging,
        ConfigPayloadStaging::Inherit,
        "an omitted tool line loads as the choice to inherit the document default"
    );
}

/// An override is written where it differs and nowhere else.
///
/// The generated document is merged with the user's, and two explicit
/// conflicting values fail that merge, so a value the user never asked for is
/// not merely noise: it decides whether a `copy` request can load at all.
#[test]
fn regression_an_override_is_written_once_in_the_emitted_document() {
    let document = staged_document(
        PayloadStaging::Copy,
        &[
            ("chosen@blake3:aa", ConfigPayloadStaging::InPlace, 1, "/provisioned/chosen"),
            ("inheriting@blake3:bb", ConfigPayloadStaging::Inherit, 2, ""),
        ],
    );

    let encoded = encode_source(document);
    assert_eq!(
        encoded.matches("payload_staging").count(),
        2,
        "the document default plus the one tool that differs:\n{encoded}"
    );

    let decoded = decode_document(encoded.as_bytes()).expect("the emitted document loads");
    assert_eq!(decoded.runtime.payload_staging, PayloadStaging::Copy);
    assert_eq!(
        decoded.tools["chosen@blake3:aa"].runtime.payload_staging,
        ConfigPayloadStaging::InPlace
    );
    assert_eq!(
        decoded.tools["inheriting@blake3:bb"].runtime.payload_staging,
        ConfigPayloadStaging::Inherit
    );
}

/// A library that never decided stages nothing into the emitted document.
///
/// `in_place` is both the absent document default and the meaning of an absent
/// tool line, so writing either would state a decision the library never made.
#[test]
fn regression_a_document_with_no_staging_decision_emits_no_staging_field() {
    let document = staged_document(
        PayloadStaging::InPlace,
        &[("plain@blake3:cc", ConfigPayloadStaging::Inherit, 3, "/provisioned/plain")],
    );

    let encoded = encode_source(document);
    assert_eq!(
        encoded.matches("payload_staging").count(),
        0,
        "no level states a strategy, so no level is written:\n{encoded}"
    );

    let decoded = decode_document(encoded.as_bytes()).expect("the emitted document loads");
    assert_eq!(decoded.runtime.payload_staging, PayloadStaging::InPlace);
    assert_eq!(
        decoded.tools["plain@blake3:cc"].runtime.payload_staging,
        ConfigPayloadStaging::Inherit
    );
}

/// A `copy`-staged tool needs no payload root, and one that would need it is
/// refused when the document loads.
///
/// This is the loadability half of the omission rule: copying writes the content
/// map into the step sandbox, so a directory the provisioner filled is not
/// required, and a document that asks to copy must not be rejected for lacking
/// one.
#[test]
fn strict_a_copy_staged_tool_needs_no_payload_root() {
    let copying = staged_document(
        PayloadStaging::Copy,
        &[("copied@blake3:dd", ConfigPayloadStaging::Inherit, 4, "")],
    );
    let encoded = encode_source(copying);
    let decoded = decode_document(encoded.as_bytes())
        .expect("a copy-staged tool names no payload root and still loads");
    assert_eq!(decoded.runtime.payload_staging, PayloadStaging::Copy);

    let mut places = staged_document(
        PayloadStaging::InPlace,
        &[("placed@blake3:ee", ConfigPayloadStaging::Inherit, 5, "")],
    );
    places.runtime.payload_staging = PayloadStaging::InPlace;
    let error = encode_document(places)
        .expect_err("in_place without a payload root is a document that never decided");
    assert!(
        format!("{error}").contains("payload_root"),
        "the refusal names the field a reader has to fill: {error}"
    );
}
