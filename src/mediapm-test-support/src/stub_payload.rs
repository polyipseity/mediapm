//! Synthetic per-OS tool payloads for hermetic provisioning tests.
//!
//! A provisioning test needs an upstream asset for every OS slot a provider
//! resolves: windows, linux and both macOS architectures. What those assets
//! *are* differs per tool — yt-dlp ships a bare binary per platform, ffmpeg
//! ships a ZIP for windows and a `tar.xz` for linux — while the shape the
//! pipeline cares about is only the container, because the asset's extension
//! decides which extractor runs. [`StubPayloadBuilder`] therefore packs
//! caller-supplied bytes into the container each slot declares, and the
//! per-tool stub bodies stay with the test that needs them.
//!
//! The host OS slot is the one that has to behave: the pipeline extracts it and
//! later runs it, so its stub must be executable. The pipeline restores the
//! execute bit on extraction, so a script needs no special handling, and
//! [`StubEntry::executable`] additionally records the bit inside the container
//! for the extractors that carry one.

use std::collections::BTreeMap;
use std::io::Write;

/// The archive container a slot's payload is packed into.
///
/// The variant follows the asset's extension rather than the tool, because the
/// pipeline picks its extractor from what the URL and the bytes say.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadFormat {
    /// The entry's bytes are the asset itself, with no container around them.
    Raw,
    /// A ZIP archive.
    Zip,
    /// A gzip-compressed tar archive.
    TarGz,
    /// An xz-compressed tar archive.
    TarXz,
}

impl PayloadFormat {
    /// The filename extension this format's assets carry.
    ///
    /// [`PayloadFormat::Raw`] contributes nothing: a bare binary's name is the
    /// provider's to choose, not this crate's.
    #[must_use]
    pub fn extension(self) -> &'static str {
        match self {
            Self::Raw => "",
            Self::Zip => ".zip",
            Self::TarGz => ".tar.gz",
            Self::TarXz => ".tar.xz",
        }
    }

    /// Packs `entries` into this format's container.
    ///
    /// # Panics
    ///
    /// Panics when this format is [`PayloadFormat::Raw`] and `entries` does not
    /// hold exactly one entry: a raw asset is one file, so a second entry has
    /// nowhere to go and zero entries have nothing to serve.
    #[must_use]
    pub fn pack(self, entries: &[StubEntry]) -> Vec<u8> {
        match self {
            Self::Raw => {
                assert!(
                    entries.len() == 1,
                    "a raw payload carries exactly one entry, got {}",
                    entries.len()
                );
                entries[0].body.clone()
            }
            Self::Zip => pack_zip(entries),
            Self::TarGz => pack_tar_gz(entries),
            Self::TarXz => pack_tar_xz(entries),
        }
    }
}

/// One file inside a synthetic payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StubEntry {
    /// Path of the file inside the container, with forward slashes.
    pub name: String,
    /// The file's bytes.
    pub body: Vec<u8>,
    /// Whether the container records the file as executable.
    ///
    /// The pipeline restores the execute bit on extraction regardless, so this
    /// only matters to a test that reads the container's own mode.
    pub executable: bool,
}

impl StubEntry {
    /// A plain file.
    #[must_use]
    pub fn data(name: impl Into<String>, body: impl Into<Vec<u8>>) -> Self {
        Self { name: name.into(), body: body.into(), executable: false }
    }

    /// A file the container marks executable.
    #[must_use]
    pub fn executable(name: impl Into<String>, body: impl Into<Vec<u8>>) -> Self {
        Self { name: name.into(), body: body.into(), executable: true }
    }

    /// The mode the container records for this entry.
    #[must_use]
    fn unix_mode(&self) -> u32 {
        if self.executable { 0o755 } else { 0o644 }
    }
}

/// One named payload, in the shape an upstream asset serves it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StubAsset {
    /// Asset filename, exactly as the provider's URL template will request it.
    pub name: String,
    /// The asset's bytes.
    pub bytes: Vec<u8>,
}

/// The synthetic payloads for one tool, keyed by OS slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StubPayload {
    /// Logical tool id the payloads stand in for.
    tool_id: String,
    /// One asset per `(os, arch)` slot.
    slots: BTreeMap<(String, String), StubAsset>,
}

impl StubPayload {
    /// Logical tool id the payloads stand in for.
    #[must_use]
    pub fn tool_id(&self) -> &str {
        &self.tool_id
    }

    /// The asset for one OS slot, or `None` when the slot has none.
    #[must_use]
    pub fn asset_for(&self, os: &str, arch: &str) -> Option<&StubAsset> {
        self.slots.get(&(os.to_string(), arch.to_string()))
    }

    /// Every slot's asset, in OS-then-arch order.
    #[must_use]
    pub fn assets(&self) -> Vec<StubAsset> {
        self.slots.values().cloned().collect()
    }

    /// Every `(os, arch)` slot this payload covers, in order.
    #[must_use]
    pub fn slots(&self) -> Vec<(&str, &str)> {
        self.slots.keys().map(|(os, arch)| (os.as_str(), arch.as_str())).collect()
    }
}

/// Accumulates per-OS payloads for one tool.
#[derive(Debug, Clone)]
pub struct StubPayloadBuilder {
    /// Logical tool id the payloads stand in for.
    tool_id: String,
    /// One asset per `(os, arch)` slot, packed as each slot is added.
    slots: BTreeMap<(String, String), StubAsset>,
}

impl StubPayloadBuilder {
    /// Starts a payload for `tool_id`.
    #[must_use]
    pub fn new(tool_id: impl Into<String>) -> Self {
        Self { tool_id: tool_id.into(), slots: BTreeMap::new() }
    }

    /// Adds one OS slot, packing `entries` into `format`.
    ///
    /// A repeated slot replaces the earlier one rather than accumulating a
    /// second asset, so a caller can restate a slot after learning its real
    /// shape.
    #[must_use]
    pub fn slot(
        mut self,
        os: impl Into<String>,
        arch: impl Into<String>,
        asset_name: impl Into<String>,
        format: PayloadFormat,
        entries: &[StubEntry],
    ) -> Self {
        let asset = StubAsset { name: asset_name.into(), bytes: format.pack(entries) };
        self.slots.insert((os.into(), arch.into()), asset);
        self
    }

    /// Finishes the payload.
    #[must_use]
    pub fn build(self) -> StubPayload {
        StubPayload { tool_id: self.tool_id, slots: self.slots }
    }
}

/// The OS label the provider layer uses for this host.
#[must_use]
pub fn host_os() -> &'static str {
    std::env::consts::OS
}

/// The architecture label the provider layer uses for this host.
#[must_use]
pub fn host_arch() -> &'static str {
    std::env::consts::ARCH
}

/// Packs `entries` into a ZIP archive.
fn pack_zip(entries: &[StubEntry]) -> Vec<u8> {
    let mut buffer = std::io::Cursor::new(Vec::new());
    let mut writer = zip::ZipWriter::new(&mut buffer);
    for entry in entries {
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Stored)
            .unix_permissions(entry.unix_mode());
        writer.start_file(entry.name.clone(), options).expect("zip entry starts");
        writer.write_all(&entry.body).expect("zip entry bytes");
    }
    writer.finish().expect("zip archive finishes");
    buffer.into_inner()
}

/// Appends `entries` to a tar stream.
fn append_tar<W: Write>(writer: W, entries: &[StubEntry]) -> W {
    let mut builder = tar::Builder::new(writer);
    for entry in entries {
        let mut header = tar::Header::new_gnu();
        header.set_size(entry.body.len() as u64);
        header.set_mode(entry.unix_mode());
        header.set_cksum();
        builder.append_data(&mut header, &entry.name, entry.body.as_slice()).expect("tar entry");
    }
    builder.into_inner().expect("tar stream flushes")
}

/// Packs `entries` into a gzip-compressed tar archive.
fn pack_tar_gz(entries: &[StubEntry]) -> Vec<u8> {
    let encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    let encoder = append_tar(encoder, entries);
    encoder.finish().expect("gzip stream finishes")
}

/// Packs `entries` into an xz-compressed tar archive.
fn pack_tar_xz(entries: &[StubEntry]) -> Vec<u8> {
    let encoder = xz2::write::XzEncoder::new(Vec::new(), 6);
    let encoder = append_tar(encoder, entries);
    encoder.finish().expect("xz stream finishes")
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    /// The marker every host stub prints, so a run can be asserted on output
    /// rather than on the fact that a process started.
    const STUB_MARKER: &str = "mediapm-stub-ran";

    /// Filename of the host stub inside its container.
    #[cfg(unix)]
    const HOST_STUB_NAME: &str = "stub.sh";
    /// Filename of the host stub inside its container.
    #[cfg(windows)]
    const HOST_STUB_NAME: &str = "stub.cmd";

    /// Body of the host stub, which prints [`STUB_MARKER`].
    #[cfg(unix)]
    fn host_stub_body() -> String {
        format!("#!/bin/sh\necho {STUB_MARKER}\n")
    }

    /// Body of the host stub, which prints [`STUB_MARKER`].
    #[cfg(windows)]
    fn host_stub_body() -> String {
        format!("@echo off\r\necho {STUB_MARKER}\r\n")
    }

    /// Extracts a ZIP archive into `dir` using the real ZIP reader.
    fn extract_zip(bytes: &[u8], dir: &Path) {
        let mut archive =
            zip::ZipArchive::new(std::io::Cursor::new(bytes.to_vec())).expect("zip opens");
        archive.extract(dir).expect("zip extracts");
    }

    /// Extracts a gzip-compressed tar archive into `dir`.
    fn extract_tar_gz(bytes: &[u8], dir: &Path) {
        let decoder = flate2::read::GzDecoder::new(std::io::Cursor::new(bytes.to_vec()));
        tar::Archive::new(decoder).unpack(dir).expect("tar.gz extracts");
    }

    /// Extracts an xz-compressed tar archive into `dir`.
    fn extract_tar_xz(bytes: &[u8], dir: &Path) {
        let decoder = xz2::read::XzDecoder::new(std::io::Cursor::new(bytes.to_vec()));
        tar::Archive::new(decoder).unpack(dir).expect("tar.xz extracts");
    }

    /// Runs `path` as a program and returns its stdout.
    fn run_stub(path: &Path) -> String {
        #[cfg(windows)]
        let output = std::process::Command::new("cmd")
            .arg("/C")
            .arg(path)
            .output()
            .expect("the host stub runs");
        #[cfg(unix)]
        let output = std::process::Command::new(path).output().expect("the host stub runs");
        assert!(output.status.success(), "the host stub must exit zero: {output:?}");
        String::from_utf8_lossy(&output.stdout).into_owned()
    }

    /// Marks `path` executable for the current user.
    #[cfg(unix)]
    fn make_executable(path: &Path) {
        use std::os::unix::fs::PermissionsExt;

        let mut permissions = std::fs::metadata(path).expect("stub metadata").permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(path, permissions).expect("stub chmod");
    }

    /// A windows-only no-op: an extracted `.cmd` runs through `cmd /C`.
    #[cfg(windows)]
    fn make_executable(_path: &Path) {}

    /// A ZIP payload round-trips through a real extraction with its bytes
    /// intact, so the container this crate writes is one the pipeline's
    /// extractor can read.
    #[test]
    fn a_zip_payload_extracts_with_its_bytes_intact() {
        let entries = [StubEntry::data("payload.txt", b"stub bytes".as_slice())];
        let bytes = PayloadFormat::Zip.pack(&entries);
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");

        extract_zip(&bytes, dir.path());

        assert_eq!(
            std::fs::read(dir.path().join("payload.txt")).expect("extracted file"),
            b"stub bytes"
        );
    }

    /// A `tar.gz` payload round-trips through a real extraction.
    #[test]
    fn a_tar_gz_payload_extracts_with_its_bytes_intact() {
        let entries = [StubEntry::data("payload.txt", b"stub bytes".as_slice())];
        let bytes = PayloadFormat::TarGz.pack(&entries);
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");

        extract_tar_gz(&bytes, dir.path());

        assert_eq!(
            std::fs::read(dir.path().join("payload.txt")).expect("extracted file"),
            b"stub bytes"
        );
    }

    /// A `tar.xz` payload round-trips through a real extraction, which is the
    /// container the linux ffmpeg slot declares.
    #[test]
    fn a_tar_xz_payload_extracts_with_its_bytes_intact() {
        let entries = [StubEntry::data("payload.txt", b"stub bytes".as_slice())];
        let bytes = PayloadFormat::TarXz.pack(&entries);
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");

        extract_tar_xz(&bytes, dir.path());

        assert_eq!(
            std::fs::read(dir.path().join("payload.txt")).expect("extracted file"),
            b"stub bytes"
        );
    }

    /// The host slot's stub extracts and executes, so a later task can stand a
    /// real tool up behind the mock rather than only serving bytes.
    #[test]
    fn the_host_stub_extracts_and_executes() {
        let entries = [StubEntry::executable(HOST_STUB_NAME, host_stub_body().into_bytes())];
        let bytes = PayloadFormat::Zip.pack(&entries);
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");

        extract_zip(&bytes, dir.path());
        let stub = dir.path().join(HOST_STUB_NAME);
        make_executable(&stub);

        assert!(
            run_stub(&stub).contains(STUB_MARKER),
            "the extracted stub must run and print its marker"
        );
    }

    /// A container records the execute bit its entry declares, which is what
    /// lets an extracted stub be run without a second chmod on the platforms
    /// whose extractor honours the mode.
    #[test]
    fn an_executable_entry_carries_the_execute_bit() {
        let entries = [
            StubEntry::executable("tool", b"#!/bin/sh\n".as_slice()),
            StubEntry::data("readme", b"plain".as_slice()),
        ];
        let bytes = PayloadFormat::Zip.pack(&entries);

        let mut archive = zip::ZipArchive::new(std::io::Cursor::new(bytes)).expect("zip opens");
        let tool_mode =
            archive.by_name("tool").expect("tool entry").unix_mode().expect("mode recorded");
        assert_eq!(tool_mode & 0o111, 0o111, "an executable entry must record the execute bit");
        let readme_mode =
            archive.by_name("readme").expect("readme entry").unix_mode().expect("mode recorded");
        assert_eq!(readme_mode & 0o111, 0, "a plain entry must not record the execute bit");
    }

    /// Every OS slot a provider resolves can be covered at once, and each slot
    /// carries the container its own extension implies.
    #[test]
    fn a_payload_covers_every_os_slot() {
        let entries = [StubEntry::data("tool", b"bytes".as_slice())];
        let payload = StubPayloadBuilder::new("ffmpeg")
            .slot("windows", "x86_64", "ffmpeg.zip", PayloadFormat::Zip, &entries)
            .slot("linux", "x86_64", "ffmpeg.tar.xz", PayloadFormat::TarXz, &entries)
            .slot("macos", "aarch64", "ffmpeg.zip", PayloadFormat::Zip, &entries)
            .slot("macos", "x86_64", "ffmpeg.zip", PayloadFormat::Zip, &entries)
            .build();

        assert_eq!(payload.tool_id(), "ffmpeg");
        assert_eq!(
            payload.slots(),
            vec![
                ("linux", "x86_64"),
                ("macos", "aarch64"),
                ("macos", "x86_64"),
                ("windows", "x86_64"),
            ]
        );
        assert_eq!(payload.assets().len(), 4);
        assert!(payload.asset_for("macos", "aarch64").is_some());
        assert!(payload.asset_for("macos", "riscv64").is_none());
    }

    /// A raw slot serves the entry's bytes with no container, which is how
    /// yt-dlp's per-platform binaries arrive.
    #[test]
    fn a_raw_slot_serves_the_entry_bytes_directly() {
        let entries = [StubEntry::data("yt-dlp", b"#!/bin/sh\n".as_slice())];
        let payload = StubPayloadBuilder::new("yt-dlp")
            .slot(host_os(), host_arch(), "yt-dlp", PayloadFormat::Raw, &entries)
            .build();

        let asset = payload.asset_for(host_os(), host_arch()).expect("host slot");
        assert_eq!(asset.name, "yt-dlp");
        assert_eq!(asset.bytes, b"#!/bin/sh\n");
        assert_eq!(PayloadFormat::Raw.extension(), "");
    }

    /// A repeated slot replaces the earlier one rather than producing two
    /// assets for one OS.
    #[test]
    fn a_repeated_slot_replaces_the_earlier_one() {
        let first = [StubEntry::data("tool", b"first".as_slice())];
        let second = [StubEntry::data("tool", b"second".as_slice())];
        let payload = StubPayloadBuilder::new("deno")
            .slot("linux", "x86_64", "deno.zip", PayloadFormat::Zip, &first)
            .slot("linux", "x86_64", "deno.zip", PayloadFormat::Zip, &second)
            .build();

        assert_eq!(payload.assets().len(), 1);
        let asset = payload.asset_for("linux", "x86_64").expect("linux slot");
        assert_ne!(asset.bytes, PayloadFormat::Zip.pack(&first));
        assert_eq!(asset.bytes, PayloadFormat::Zip.pack(&second));
    }
}
