//! Catalog entry for `deno` managed tool provisioning.

use super::{ARCHIVE_ZIP, PlatformValue, ToolCatalogEntry};

/// Declarative catalog record for `deno`.
pub(super) fn entry() -> ToolCatalogEntry {
    ToolCatalogEntry {
        id: "deno",
        description: "deno JavaScript and TypeScript runtime",
        homepage: "https://deno.com",
        latest: "latest",
        platforms: vec![
            (
                super::ToolOs::Windows,
                vec![PlatformValue {
                    url: "https://github.com/denoland/deno/releases/latest/download/deno-x86_64-pc-windows-msvc.zip",
                    arch: "x86_64",
                    checksum_sha256: None,
                }],
            ),
            (
                super::ToolOs::Macos,
                vec![
                    PlatformValue {
                        url: "https://github.com/denoland/deno/releases/latest/download/deno-aarch64-apple-darwin.zip",
                        arch: "aarch64",
                        checksum_sha256: None,
                    },
                    PlatformValue {
                        url: "https://github.com/denoland/deno/releases/latest/download/deno-x86_64-apple-darwin.zip",
                        arch: "x86_64",
                        checksum_sha256: None,
                    },
                ],
            ),
            (
                super::ToolOs::Linux,
                vec![
                    PlatformValue {
                        url: "https://github.com/denoland/deno/releases/latest/download/deno-aarch64-unknown-linux-gnu.zip",
                        arch: "aarch64",
                        checksum_sha256: None,
                    },
                    PlatformValue {
                        url: "https://github.com/denoland/deno/releases/latest/download/deno-x86_64-unknown-linux-gnu.zip",
                        arch: "x86_64",
                        checksum_sha256: None,
                    },
                ],
            ),
        ],
        archive_format: ARCHIVE_ZIP,
    }
}
