//! Command-line interface for `mediapm-cas`.
//!
//! This module owns argument parsing and command dispatch for the CAS CLI.

use clap::{CommandFactory, Parser, Subcommand};
use clap_complete::{Shell, generate};
use std::io::{self, Read, Write};

use crate::api::CasApi;
use crate::config::{CasConfig, CasIntegrityConfig, CasLocatorParseOptions};
use crate::error::CasError;
use crate::hash::{HASH_SIZE, Hash};

/// Top-level `mediapm-cas` CLI arguments.
#[derive(Debug, Parser)]
#[command(author, version, about = "mediapm CAS CLI")]
struct Cli {
    /// Storage backend locator (e.g. "memory", "/path/to/store").
    #[arg(long, default_value = "memory")]
    storage: String,

    /// Subcommand to execute.
    #[command(subcommand)]
    command: CasCommand,
}

/// CAS CLI subcommands.
#[derive(Debug, Subcommand)]
enum CasCommand {
    /// Store data from stdin and print the hash.
    Put,
    /// Retrieve data by hash and write to stdout.
    Get {
        /// BLAKE3 hash of the object (hex).
        hash: String,
    },
    /// Print metadata for an object.
    Stat {
        /// BLAKE3 hash of the object (hex).
        hash: String,
    },
    /// Delete an object by hash.
    Delete {
        /// BLAKE3 hash of the object (hex).
        hash: String,
    },
    /// Generate shell completions.
    Completions {
        /// Shell name (bash, elvish, fish, powershell, zsh).
        shell: String,
    },
}

/// Run the CLI from environment arguments and exit.
pub async fn run_from_env() -> anyhow::Result<()> {
    let cli = Cli::parse();
    run(cli).await
}

/// Run the CLI from explicit arguments (for library API callers).
pub async fn run_from_passthrough_args(args: &[String]) -> anyhow::Result<()> {
    let cli = Cli::try_parse_from(args.iter().map(String::as_str))?;
    run(cli).await
}

async fn run(cli: Cli) -> anyhow::Result<()> {
    let config = CasConfig::from_locator_with_options(
        &cli.storage,
        CasLocatorParseOptions { allow_plain_filesystem_path: true },
        CasIntegrityConfig { verify_on_read: Vec::new() },
    )?;
    let cas = config.open().await?;

    match cli.command {
        CasCommand::Put => {
            let mut buf = Vec::new();
            io::stdin().read_to_end(&mut buf)?;
            let data = bytes::Bytes::from(buf);
            let hash = cas.put(data).await?;
            println!("{hash}");
        }
        CasCommand::Get { hash } => {
            let hash = parse_hex_hash(&hash)?;
            let data = cas.get(hash).await.map_err(|e| match e {
                CasError::NotFound(h) => anyhow::anyhow!("object {h} not found"),
                other => anyhow::anyhow!("{other}"),
            })?;
            io::stdout().write_all(&data)?;
        }
        CasCommand::Stat { hash } => {
            let hash = parse_hex_hash(&hash)?;
            let info = cas.stat(hash).await.map_err(|e| match e {
                CasError::NotFound(h) => anyhow::anyhow!("object {h} not found"),
                other => anyhow::anyhow!("{other}"),
            })?;
            println!("hash: {hash}");
            println!("len: {}", info.len);
            println!("encoding: {:?}", info.encoding);
        }
        CasCommand::Delete { hash } => {
            let hash = parse_hex_hash(&hash)?;
            cas.delete(hash).await?;
            println!("deleted {hash}");
        }
        CasCommand::Completions { shell } => {
            let mut cmd = Cli::command();
            let name = cmd.get_name().to_string();
            let shell = shell.as_str();
            let mut stdout = io::stdout();
            match shell {
                "bash" => generate(Shell::Bash, &mut cmd, &name, &mut stdout),
                "elvish" => generate(Shell::Elvish, &mut cmd, &name, &mut stdout),
                "fish" => generate(Shell::Fish, &mut cmd, &name, &mut stdout),
                "powershell" => generate(Shell::PowerShell, &mut cmd, &name, &mut stdout),
                "zsh" => generate(Shell::Zsh, &mut cmd, &name, &mut stdout),
                _ => return Err(anyhow::anyhow!("unsupported shell: {shell}")),
            }
        }
    }

    Ok(())
}

/// Parse a 64-char hex string into a [`Hash`].
fn parse_hex_hash(s: &str) -> anyhow::Result<Hash> {
    if s.len() != HASH_SIZE * 2 {
        return Err(anyhow::anyhow!("invalid hash length: {}", s.len()));
    }
    let mut arr = [0u8; HASH_SIZE];
    for (i, byte) in arr.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16)
            .map_err(|_| anyhow::anyhow!("invalid hash: {s}"))?;
    }
    Ok(Hash::from_bytes(arr))
}
