//! Integration tests verifying the all-platform download architecture.
//!
//! A tool sync provisions payloads for **every** supported OS, not only for
//! the host it runs on, because the generated conductor document is shared
//! across platforms: a tool that resolved one slot produces a command selector
//! with no branch for the others. [`assert_every_managed_tool_covers_every_os`]
//! is that contract, and both tests in this module run it — the hermetic one
//! against a [`MockUpstream`] that answers all four upstream hosts, the other
//! against the real hosts behind an `#[ignore]` opt-in.
//!
//! Both tests require `--all-features`, because the seams they drive exist only
//! in a `test-seams` build and `mediapm`'s defaults exclude that feature. A plain
//! `cargo test -p mediapm` therefore cannot compile this target at all: the
//! refusal below is deliberate, and every gate in this repository passes
//! `--all-features`.

use std::collections::{BTreeMap, BTreeSet};

use crate::common::{read_generated_doc, service_with_cache, test_sync_progress_overrides};
use mediapm::MediaRuntimeStorage;
use mediapm_conductor::tools::provider::host_arch_label;
use mediapm_conductor::{NickelDocument, ToolKindSpec, ToolSpec};
use mediapm_test_support::{MockUpstream, PayloadFormat, StubAsset, StubEntry};

// The hermetic test drives the provider base-URL seams, and those seams exist
// only in a `test-seams` build. Without the feature the sync would ignore the
// mock and reach the real hosts, which is the silent pass this refusal exists
// to prevent: a test that quietly downloads from the network is worse than one
// that will not build.
#[cfg(not(feature = "test-seams"))]
compile_error!(
    "all_platform's hermetic test drives the provider base-URL seams; run it with \
     `cargo test -p mediapm --all-features`"
);

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

/// The tag the mocked yt-dlp release reports.
const STUB_YT_DLP_TAG: &str = "2026.01.01";

/// The tag the mocked deno release reports.
const STUB_DENO_TAG: &str = "v2.5.0";

/// The tag the mocked rsgain release reports.
///
/// rsgain tags carry a leading `v` that the provider strips before building
/// the versioned asset names, so this fixture exercises that strip.
const STUB_RSGAIN_TAG: &str = "v3.7.0";

/// The tag the mocked sd release reports.
const STUB_SD_TAG: &str = "v1.1.0";

/// The commit hash every mocked GitHub tag ref reports as its object.
///
/// The value is arbitrary: it becomes the tool's canonical version, which is
/// compared as a string and never parsed.
const STUB_COMMIT_SHA: &str = "0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f";

/// The `BtbN` autobuild tag the mocked releases list names.
const STUB_BTBN_TAG: &str = "autobuild-2026-01-01-12-00";

/// The martin-riedl build id the mocked permalink redirects to.
///
/// martin-riedl publishes one build id for both macOS architectures, so both
/// permalinks redirect to a path carrying this id.
const STUB_MARTIN_RIEDL_BUILD_ID: &str = "1767225600_N-127260-g7bc3576910";

/// The jellyfin build line the mocked repository index names.
const STUB_JELLYFIN_LINE: &str = "latest-10.x";

/// The jellyfin version the mocked archive names carry.
const STUB_JELLYFIN_VERSION: &str = "7.1.1";

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

/// Builds the entries one archive slot packs: an executable named for the tool
/// plus its `.exe` twin.
///
/// The name matters twice. `find_os_executable` looks for `{tool_id}` and
/// `{tool_id}.exe`, and deno's permission wrapper renames the file it finds,
/// so a deno slot without either name fails the sync outright. The body names
/// the tool and the slot so two slots can never pack identical bytes.
fn archive_entries(tool: &str, slot: &str) -> Vec<StubEntry> {
    let body = |suffix: &str| format!("{tool} stub for {slot}{suffix}\n").into_bytes();
    vec![
        StubEntry::executable(tool, body("")),
        StubEntry::executable(format!("{tool}.exe"), body(".exe")),
    ]
}

/// Builds one archive asset with its slot's entries packed inside it.
fn archive_asset(name: &str, tool: &str, slot: &str, format: PayloadFormat) -> StubAsset {
    StubAsset { name: name.to_string(), bytes: format.pack(&archive_entries(tool, slot)) }
}

/// Builds one bare-binary asset whose bytes name the tool and the slot.
fn binary_asset(name: &str, tool: &str, slot: &str) -> StubAsset {
    StubAsset { name: name.to_string(), bytes: format!("{tool} stub for {slot}\n").into_bytes() }
}

/// Mounts yt-dlp's upstream traffic: one bare binary per OS slot.
///
/// yt-dlp's asset names carry no version token, so the tag reaches the URL
/// path alone.
async fn mount_yt_dlp(mock: &MockUpstream) {
    let assets = [
        binary_asset("yt-dlp.exe", "yt-dlp", "windows"),
        binary_asset("yt-dlp_macos", "yt-dlp", "macos"),
        binary_asset("yt-dlp_linux", "yt-dlp", "linux"),
    ];
    mock.mount_github_release("yt-dlp", "yt-dlp", STUB_YT_DLP_TAG, STUB_COMMIT_SHA, &assets).await;
}

/// Mounts deno's upstream traffic: one ZIP per OS slot.
///
/// The macOS and Linux asset names follow the host architecture, exactly as
/// the provider's own slot table does.
async fn mount_deno(mock: &MockUpstream) {
    let arch = host_arch_label();
    let assets = [
        archive_asset("deno-x86_64-pc-windows-msvc.zip", "deno", "windows", PayloadFormat::Zip),
        archive_asset(
            &format!("deno-{arch}-apple-darwin.zip"),
            "deno",
            "macos",
            PayloadFormat::Zip,
        ),
        archive_asset(
            &format!("deno-{arch}-unknown-linux-gnu.zip"),
            "deno",
            "linux",
            PayloadFormat::Zip,
        ),
    ];
    mock.mount_github_release("denoland", "deno", STUB_DENO_TAG, STUB_COMMIT_SHA, &assets).await;
}

/// Mounts rsgain's upstream traffic: two ZIPs and one `tar.xz`.
///
/// rsgain is a single `x86_64` build per OS, so no asset name follows the host
/// architecture. Its asset names embed the version with the tag's leading `v`
/// stripped, which is the shape the provider's own rewrite builds.
async fn mount_rsgain(mock: &MockUpstream) {
    let version = STUB_RSGAIN_TAG.strip_prefix('v').unwrap_or(STUB_RSGAIN_TAG);
    let assets = [
        archive_asset(
            &format!("rsgain-{version}-win64.zip"),
            "rsgain",
            "windows",
            PayloadFormat::Zip,
        ),
        archive_asset(
            &format!("rsgain-{version}-macOS-x86_64.zip"),
            "rsgain",
            "macos",
            PayloadFormat::Zip,
        ),
        archive_asset(
            &format!("rsgain-{version}-Linux.tar.xz"),
            "rsgain",
            "linux",
            PayloadFormat::TarXz,
        ),
    ];
    mock.mount_github_release("complexlogic", "rsgain", STUB_RSGAIN_TAG, STUB_COMMIT_SHA, &assets)
        .await;
}

/// Mounts sd's upstream traffic: one ZIP and two `tar.gz` archives.
///
/// sd's asset names embed the tag itself, `v` included.
async fn mount_sd(mock: &MockUpstream) {
    let arch = host_arch_label();
    let tag = STUB_SD_TAG;
    let assets = [
        archive_asset(
            &format!("sd-{tag}-x86_64-pc-windows-msvc.zip"),
            "sd",
            "windows",
            PayloadFormat::Zip,
        ),
        archive_asset(
            &format!("sd-{tag}-{arch}-apple-darwin.tar.gz"),
            "sd",
            "macos",
            PayloadFormat::TarGz,
        ),
        archive_asset(
            &format!("sd-{tag}-x86_64-unknown-linux-gnu.tar.gz"),
            "sd",
            "linux",
            PayloadFormat::TarGz,
        ),
    ];
    mock.mount_github_release("chmln", "sd", STUB_SD_TAG, STUB_COMMIT_SHA, &assets).await;
}

/// Mounts ffmpeg's upstream traffic: the `BtbN` alias downloads, the
/// martin-riedl macOS chain, and the jellyfin macOS fallback.
///
/// Both `BtbN` candidates are mounted, so the fixture does not depend on which
/// of the shared/non-shared assets the chain prefers. Both martin-riedl
/// channels are mounted for the same reason: the chain tries the snapshot
/// permalink first, and mounting the release permalink too keeps the fixture
/// from encoding that order. The jellyfin index and both pointer files are
/// read during resolve even though the snapshot permalink serves the fetch, so
/// a fallback that is never downloaded is still resolved.
async fn mount_ffmpeg(mock: &MockUpstream) {
    mock.mount_github_release_list("BtbN", "FFmpeg-Builds", &[STUB_BTBN_TAG]).await;
    let assets = [
        archive_asset(
            "ffmpeg-master-latest-win64-gpl-shared.zip",
            "ffmpeg",
            "windows",
            PayloadFormat::Zip,
        ),
        archive_asset(
            "ffmpeg-master-latest-win64-gpl.zip",
            "ffmpeg",
            "windows",
            PayloadFormat::Zip,
        ),
        archive_asset(
            "ffmpeg-master-latest-linux64-gpl-shared.tar.xz",
            "ffmpeg",
            "linux",
            PayloadFormat::TarXz,
        ),
        archive_asset(
            "ffmpeg-master-latest-linux64-gpl.tar.xz",
            "ffmpeg",
            "linux",
            PayloadFormat::TarXz,
        ),
    ];
    mock.mount_github_release("BtbN", "FFmpeg-Builds", STUB_BTBN_TAG, STUB_COMMIT_SHA, &assets)
        .await;

    for (canonical, site_label) in [("aarch64", "arm64"), ("x86_64", "amd64")] {
        let payload = PayloadFormat::Zip.pack(&archive_entries("ffmpeg", canonical));
        for channel in ["snapshot", "release"] {
            mock.mount_martin_riedl(site_label, channel, STUB_MARTIN_RIEDL_BUILD_ID, &payload)
                .await;
        }
    }

    for (canonical, site_dir, site_token) in
        [("aarch64", "arm64", "macarm64"), ("x86_64", "x86_64", "mac64")]
    {
        let archive =
            format!("jellyfin-ffmpeg_{STUB_JELLYFIN_VERSION}_portable_{site_token}-gpl.tar.xz");
        let payload = PayloadFormat::TarXz.pack(&archive_entries("ffmpeg", canonical));
        mock.mount_jellyfin(STUB_JELLYFIN_LINE, site_dir, site_token, &archive, &payload).await;
    }
}

/// Mounts every managed tool's upstream traffic on one mock server.
///
/// media-tagger is absent on purpose: it is a launcher-only builtin whose
/// script is generated locally, so it has no upstream to mock.
async fn mount_managed_tool_upstreams(mock: &MockUpstream) {
    mount_yt_dlp(mock).await;
    mount_deno(mock).await;
    mount_rsgain(mock).await;
    mount_sd(mock).await;
    mount_ffmpeg(mock).await;
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
    let mock = MockUpstream::start().await;
    mount_managed_tool_upstreams(&mock).await;
    let _seams = mock.all_seams(mediapm::example_isolation::env_lock());

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
