//! CLI-friendly CAS topology visualization helpers.
//!
//! This module intentionally lives in the Phase 1 crate so visualization
//! rendering and topology snapshot formatting are owned by CAS, not by higher
//! orchestration layers.
//!
//! Design notes:
//! - argument parsing (for example `clap`) remains in caller crates,
//! - topology data collection + output rendering/writing live here,
//! - callers pass one fully resolved request object.

use std::io::Write;
use std::path::{Path, PathBuf};

use crate::{
    CasError, CasTopologyEncoding, CasTopologySnapshot, FileSystemCas, render_topology_mermaid,
};

/// Output representation for one topology visualization request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CasVisualizeFormat {
    /// Mermaid flowchart markup.
    Mermaid,
    /// Pretty-printed JSON topology snapshot.
    Json,
    /// Human-readable multi-line text report.
    Text,
}

/// Input contract for one visualization run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CasVisualizeRequest {
    /// CAS root directory to inspect.
    pub root: PathBuf,
    /// Desired visualization output format.
    pub format: CasVisualizeFormat,
    /// Whether to include the canonical empty-content object in the snapshot.
    pub include_empty: bool,
    /// Optional file target. When absent, output is written to stdout.
    pub output: Option<PathBuf>,
}

impl CasVisualizeRequest {
    /// Creates one request with Phase 3 CLI-compatible defaults.
    #[must_use]
    pub fn with_default_root(root: Option<PathBuf>, format: CasVisualizeFormat) -> Self {
        Self {
            root: root.unwrap_or_else(default_cas_root),
            format,
            include_empty: false,
            output: None,
        }
    }
}

/// Returns the default CAS root path used by current CLIs.
#[must_use]
pub fn default_cas_root() -> PathBuf {
    PathBuf::from(".mediapm/cas")
}

/// Executes one topology visualization request.
///
/// # Errors
/// Returns [`CasError`] when opening CAS state, rendering JSON, or writing
/// output bytes fails.
pub async fn run_visualize_command(request: CasVisualizeRequest) -> Result<(), CasError> {
    let cas = FileSystemCas::open(&request.root).await?;
    let snapshot = cas.topology_snapshot(request.include_empty).await?;

    let rendered = match request.format {
        CasVisualizeFormat::Mermaid => render_topology_mermaid(&snapshot),
        CasVisualizeFormat::Json => {
            serde_json::to_string_pretty(&snapshot).map_err(CasError::json)?
        }
        CasVisualizeFormat::Text => render_topology_text(&snapshot),
    };

    write_cli_output(rendered.as_bytes(), request.output.as_deref())
}

/// Writes rendered output either to one file path or to stdout.
fn write_cli_output(bytes: &[u8], output: Option<&Path>) -> Result<(), CasError> {
    if let Some(output) = output {
        if let Some(parent) = output.parent() {
            std::fs::create_dir_all(parent).map_err(|source| {
                CasError::io("creating topology output parent directory", parent, source)
            })?;
        }

        std::fs::write(output, bytes)
            .map_err(|source| CasError::io("writing topology output file", output, source))?;
        return Ok(());
    }

    let mut stdout = std::io::stdout();
    stdout
        .write_all(bytes)
        .map_err(|source| CasError::stream_io("writing topology output to stdout", source))?;
    if !bytes.ends_with(b"\n") {
        stdout
            .write_all(b"\n")
            .map_err(|source| CasError::stream_io("writing topology trailing newline", source))?;
    }

    Ok(())
}

/// Renders one compact human-readable topology report.
fn render_topology_text(snapshot: &CasTopologySnapshot) -> String {
    let mut out = String::new();
    out.push_str("CAS topology report\n");
    out.push_str("===================\n");
    out.push_str(&format!(
        "nodes={} constraints={} include_empty={}\n\n",
        snapshot.nodes.len(),
        snapshot.constraints.len(),
        snapshot.include_empty
    ));

    out.push_str("nodes\n-----\n");
    for node in &snapshot.nodes {
        match node.encoding {
            CasTopologyEncoding::Full => out.push_str(&format!(
                "- {} kind=full depth={} content={}B payload={}B\n",
                node.hash, node.depth, node.content_len, node.payload_len
            )),
            CasTopologyEncoding::Delta { base_hash } => out.push_str(&format!(
                "- {} kind=delta(base={}) depth={} content={}B payload={}B\n",
                node.hash, base_hash, node.depth, node.content_len, node.payload_len
            )),
        }
    }

    out.push_str("\nconstraints\n-----------\n");
    if snapshot.constraints.is_empty() {
        out.push_str("(none)\n");
    } else {
        for row in &snapshot.constraints {
            let bases = row.bases.iter().map(ToString::to_string).collect::<Vec<_>>().join(", ");
            out.push_str(&format!("- {} -> [{}]\n", row.target_hash, bases));
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use crate::{CasTopologyConstraint, CasTopologyNode, Hash};

    use super::{CasTopologyEncoding, CasTopologySnapshot, render_topology_text, write_cli_output};

    /// Verifies text rendering includes node and constraint summaries.
    #[test]
    fn text_renderer_includes_nodes_and_constraints_sections() {
        let base = Hash::from_content(b"base");
        let target = Hash::from_content(b"target");
        let snapshot = CasTopologySnapshot {
            include_empty: false,
            nodes: vec![
                CasTopologyNode {
                    hash: base,
                    content_len: 4,
                    payload_len: 4,
                    depth: 1,
                    encoding: CasTopologyEncoding::Full,
                },
                CasTopologyNode {
                    hash: target,
                    content_len: 6,
                    payload_len: 2,
                    depth: 2,
                    encoding: CasTopologyEncoding::Delta { base_hash: base },
                },
            ],
            constraints: vec![CasTopologyConstraint { target_hash: target, bases: vec![base] }],
        };

        let rendered = render_topology_text(&snapshot);
        assert!(rendered.contains("CAS topology report"));
        assert!(rendered.contains("nodes"));
        assert!(rendered.contains("constraints"));
        assert!(rendered.contains("kind=delta"));
        assert!(rendered.contains(&target.to_string()));
        assert!(rendered.contains(&base.to_string()));
    }

    /// Verifies file-output mode creates parents and writes exact bytes.
    #[test]
    fn write_cli_output_writes_to_output_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let output = dir.path().join("nested").join("out.txt");

        write_cli_output(b"hello", Some(output.as_path())).expect("write output file");

        let content = std::fs::read_to_string(output).expect("read output file");
        assert_eq!(content, "hello");
    }
}
