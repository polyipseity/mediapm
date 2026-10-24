//! Command-line interface for `mediapm-cas`.
//!
//! This module owns argument parsing and command dispatch for the CAS CLI. The
//! clap definition it parses with lives in `mediapm-cas-cli` and is re-exported
//! below: a build script cannot depend on the package it builds, so a
//! definition that lived here would be unreachable from this package's own
//! build script.

use clap::{CommandFactory, Parser};
use clap_complete::{Shell, generate};
use std::io::{self, Read, Write};

use crate::api::CasApi;
use crate::config::CasConfig;
use crate::defaults;
use crate::error::CasError;
use crate::hash::Hash;

pub use mediapm_cas_cli::{CasCommand, Cli};

/// Run the CLI from environment arguments and exit.
///
/// # Errors
///
/// Returns anyhow errors from argument parsing and CAS operations.
pub async fn run_from_env() -> anyhow::Result<()> {
    let cli = Cli::parse();
    run(cli).await
}

/// Run the CLI from explicit arguments (for library API callers).
///
/// The vector carries the caller's arguments only. The program name clap reads
/// from position zero is supplied here from the command definition, so a caller
/// cannot omit it: a vector without one has its first real argument read as the
/// program name and its second as the subcommand, which routes nothing while
/// still looking like a successful parse.
///
/// # Errors
///
/// Returns anyhow errors from argument parsing and CAS operations.
pub async fn run_from_passthrough_args(args: &[String]) -> anyhow::Result<()> {
    if passthrough_requests_help_or_version(args) {
        let mut cmd = Cli::command();
        cmd.print_help()?;
        return Ok(());
    }
    let name = Cli::command().get_name().to_owned();
    let with_program_name = std::iter::once(name.as_str()).chain(args.iter().map(String::as_str));
    let cli = Cli::try_parse_from(with_program_name)?;
    run(cli).await
}

/// Returns true when passthrough argv explicitly requests help/version text.
fn passthrough_requests_help_or_version(args: &[String]) -> bool {
    args.iter().any(|arg| {
        matches!(arg.as_str(), "-h" | "--help" | "-V" | "--version")
            || arg.split_once('=').is_some_and(|(flag, _)| matches!(flag, "--help" | "--version"))
    })
}

async fn run(cli: Cli) -> anyhow::Result<()> {
    let config = CasConfig::from_locator_with_options(
        &cli.storage,
        &defaults::LOCATOR_PARSE_OPTIONS,
        defaults::INTEGRITY_CONFIG,
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
            let hash: Hash = hash.parse()?;
            let data = cas.get(hash).await.map_err(|e| match e {
                CasError::NotFound(h) => anyhow::anyhow!("object {h} not found"),
                other => anyhow::anyhow!("{other}"),
            })?;
            io::stdout().write_all(&data)?;
        }
        CasCommand::Stat { hash } => {
            let hash: Hash = hash.parse()?;
            let info = cas.stat(hash).await.map_err(|e| match e {
                CasError::NotFound(h) => anyhow::anyhow!("object {h} not found"),
                other => anyhow::anyhow!("{other}"),
            })?;
            println!("hash: {hash}");
            println!("len: {}", info.len);
            println!("encoding: {:?}", info.encoding);
        }
        CasCommand::Delete { hash } => {
            let hash: Hash = hash.parse()?;
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
