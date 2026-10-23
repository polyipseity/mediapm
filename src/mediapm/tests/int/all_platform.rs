//! Integration tests verifying the all-platform download architecture.
//!
//! A tool sync provisions payloads for **every** supported OS, not only for
//! the host it runs on, because the generated conductor document is shared
//! across platforms: a tool that resolved one slot produces a command selector
//! with no branch for the others. [`assert_every_managed_tool_covers_every_os`]
//! is that contract, and both tests in this module run it — the hermetic one
//! against a mock upstream that answers all four hosts, the other against the
//! real hosts behind an `#[ignore]` opt-in.
//!
//! The stub upstream this module mounts lives in [`crate::common::stub_upstream`],
//! shared with the `tool_sync` modules so two tests cannot disagree about the
//! upstream's shape.
//!
//! Both tests require `--all-features`, because the seams they drive exist only
//! in a `test-seams` build and `mediapm`'s defaults exclude that feature. The
//! refusal that enforces it sits in `tests/int/mod.rs`, covering this module and
//! the `tool_sync` modules together, and every gate in this repository passes
//! `--all-features`.

use std::collections::{BTreeMap, BTreeSet};

use crate::common::stub_upstream::mock_all_managed_tools;
use crate::common::{read_generated_doc, service_with_cache, test_sync_progress_overrides};
use mediapm::MediaRuntimeStorage;
use mediapm_conductor::{NickelDocument, ToolKindSpec, ToolSpec};

/// The OS labels every managed tool's payload set must cover.
///
/// Order is fixed so the failure message names the slots in a stable order.
const MANAGED_OS_SLOTS: [&str; 3] = ["linux", "macos", "windows"];

/// The managed tools a full sync has to provision.
///
/// These are the tools `mediapm.ncl` can declare; the builtins the sync
/// registers (echo, fs, import, export, archive) carry no payload and are
/// deliberately absent.
///
/// `tools::provider::resolve_tool_fetch`'s dispatch is the source of truth for
/// this set, and this list has to be updated whenever that dispatch changes: a
/// seventh tool added there would otherwise leave the assertion below covering
/// nothing about it while the test kept passing. Deriving the list from a shared
/// provider constant would remove that coupling entirely, and the coverage
/// matrix in `.agents/coverage-matrix.md` needs the same pointer.
const MANAGED_TOOLS: [&str; 6] = ["yt-dlp", "ffmpeg", "deno", "rsgain", "sd", "media-tagger"];

/// The tools whose payloads come from a launcher script rather than a
/// download.
///
/// A launcher's bytes are generated per OS from one template, so two OS slots
/// may legitimately carry identical bytes; every fetched tool's slots carry
/// distinct upstream builds.
const LAUNCHER_ONLY_TOOLS: [&str; 1] = ["media-tagger"];

/// Content-map key prefix marking an inlined same-step dependency payload.
///
/// Inlining nests a dependency's own payload map under `deps/<tool_id>/`, so
/// those keys belong to the dependency's OS slots and not to the requester's.
const INLINED_DEP_PREFIX: &str = "deps/";

/// Declares every managed tool at `latest`.
///
/// `MediaRuntimeStorage::default()` declares no tools at all, so a sync over it
/// provisions only the builtins and every managed-tool assertion below would
/// pass vacuously. The whole set is declared explicitly for that reason.
fn all_managed_tools_storage() -> MediaRuntimeStorage {
    let mut storage = MediaRuntimeStorage::default();
    for tool_id in MANAGED_TOOLS {
        storage.tools.insert(tool_id.to_string(), tool_requirement());
    }
    storage
}

/// Builds one `latest` tool requirement with the schema's own defaults.
///
/// Going through serde rather than listing the fields keeps the fixture from
/// drifting from the schema: a new field with a serde default arrives filled
/// in, and a new required field fails here instead of silently taking a
/// hand-written value.
fn tool_requirement() -> mediapm::ToolRequirement {
    serde_json::from_value(serde_json::json!({}))
        .expect("a bare tool requirement resolves from schema defaults")
}

/// Finds the active spec for one managed tool.
///
/// The generated document keys each tool by `{name}@{content_map_hash}`, and a
/// pruned earlier generation keeps the same bare `name` with an emptied
/// content map, so the first spec that both names the tool and carries a
/// payload is the one the sync just wrote.
fn active_tool_spec<'a>(doc: &'a NickelDocument, tool_id: &str) -> &'a ToolSpec {
    doc.tools
        .values()
        .find(|spec| spec.name == tool_id && !spec.runtime.content_map.is_empty())
        .unwrap_or_else(|| {
            let names: Vec<&str> = doc.tools.values().map(|spec| spec.name.as_str()).collect();
            panic!("tool {tool_id}: no generated spec carries a payload (document holds {names:?})")
        })
}

/// Asserts every managed tool provisioned a payload for every OS slot.
///
/// For each managed tool the generated document must hold
///
/// - a spec carrying a non-empty content map, and
/// - at least one content-map entry under each of [`MANAGED_OS_SLOTS`], and
/// - for a tool whose payloads are downloads, no two OS slots sharing a
///   payload, so a slot cannot be filled by another slot's bytes, and
/// - an executable command whose first element names every one of those slots.
///
/// The command check is what makes the content-map check load-bearing: the
/// selector is built from the per-OS executable paths, so a document can only
/// name all three platforms if all three slots resolved.
///
/// Two limits here are deliberate, and both are easy to mistake for stronger
/// guarantees than they are. The distinctness check holds for the hermetic path
/// only: the mock gives every slot its own bytes by construction, real upstream
/// builds are not known to differ across slots, and the `#[ignore]`d twin that
/// would meet real bytes has never been run. And the per-OS check is slot-level
/// rather than arch-level: for a dual-arch `macos/` slot a host-arch key alone
/// satisfies the prefix test, so a dropped non-host-arch payload is not caught
/// here. That case is covered by
/// `dual_arch_slot_keys_carry_arch_and_exec_path_resolves_host_arch` in
/// mediapm-conductor and by `resolve_tool_fetch_each_fetched_tool_covers_three_oses`.
///
/// # Panics
///
/// Panics with the tool id and the offending keys or command when any of the
/// four invariants fails.
fn assert_every_managed_tool_covers_every_os(doc: &NickelDocument) {
    for tool_id in MANAGED_TOOLS {
        let spec = active_tool_spec(doc, tool_id);
        let content_map = &spec.runtime.content_map;

        let mut hashes_by_os: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
        for os in MANAGED_OS_SLOTS {
            let prefix = format!("{os}/");
            let hashes: BTreeSet<&str> = content_map
                .iter()
                .filter(|(key, _)| !key.starts_with(INLINED_DEP_PREFIX) && key.starts_with(&prefix))
                .map(|(_, hash)| hash.as_str())
                .collect();
            assert!(
                !hashes.is_empty(),
                "tool {tool_id}: no payload for the {os} OS slot; content-map keys are {:?}",
                content_map.keys().collect::<Vec<_>>(),
            );
            hashes_by_os.insert(os, hashes);
        }

        if !LAUNCHER_ONLY_TOOLS.contains(&tool_id) {
            for (first, second) in [("linux", "macos"), ("linux", "windows"), ("macos", "windows")]
            {
                let shared: Vec<&str> =
                    hashes_by_os[first].intersection(&hashes_by_os[second]).copied().collect();
                assert!(
                    shared.is_empty(),
                    "tool {tool_id}: the {first} and {second} slots carry the same payload {shared:?}",
                );
            }
        }

        let ToolKindSpec::Executable { command, .. } = &spec.kind else {
            panic!("tool {tool_id}: a managed tool must carry an executable command");
        };
        assert!(
            !command.is_empty() && !command[0].is_empty(),
            "tool {tool_id}: the command must name an executable",
        );
        for os in MANAGED_OS_SLOTS {
            assert!(
                command[0].contains(&format!("{os}/")),
                "tool {tool_id}: the command selector names no {os} slot: {}",
                command[0],
            );
        }
    }
}

/// A full sync against mocked upstream hosts provisions every managed tool's
/// payload for every OS slot.
///
/// This is the test that makes the all-platform contract checkable: it declares
/// all six managed tools, answers their metadata and payload traffic locally,
/// and asserts the document the sync wrote.
#[tokio::test]
async fn every_managed_tool_provisions_a_payload_for_every_os_slot()
-> Result<(), mediapm::MediaPmError> {
    let (_mock, _seams) = mock_all_managed_tools().await;

    let (mut service, _root, _cache_root) = service_with_cache(all_managed_tools_storage()).await?;
    service
        .sync_tools_with_progress_overrides(false, false, test_sync_progress_overrides())
        .await?;

    let doc = read_generated_doc(&service);
    assert_every_managed_tool_covers_every_os(&doc);
    Ok(())
}

/// The same contract against the real upstream hosts.
///
/// Ignored by default: it downloads every managed tool's payload for every OS,
/// which is hundreds of megabytes and needs network access. Run it with
/// `cargo test -p mediapm --all-features -- --ignored` when upstream behavior
/// is what is in question; the hermetic test above is the one that runs in the
/// suite.
#[tokio::test]
#[ignore = "downloads every managed tool from the real upstream hosts"]
async fn every_managed_tool_provisions_a_payload_for_every_os_slot_online()
-> Result<(), mediapm::MediaPmError> {
    let (mut service, _root, _cache_root) = service_with_cache(all_managed_tools_storage()).await?;
    service
        .sync_tools_with_progress_overrides(false, false, test_sync_progress_overrides())
        .await?;

    let doc = read_generated_doc(&service);
    assert_every_managed_tool_covers_every_os(&doc);
    Ok(())
}
