//! CAS topology visualization data model and renderers.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::fmt::Write as _;

use crate::{Hash, empty_content_hash};

/// One object node in filesystem CAS topology.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CasTopologyNode {
    /// Object hash identity.
    pub hash: Hash,
    /// Reconstructed logical length.
    pub content_len: u64,
    /// Stored payload length (full bytes or delta bytes).
    pub payload_len: u64,
    /// Resolved base-chain depth.
    pub depth: u32,
    /// Storage encoding of this node.
    pub encoding: CasTopologyEncoding,
}

/// Storage encoding for one topology node.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum CasTopologyEncoding {
    /// Full object payload.
    Full,
    /// Delta payload against `base_hash`.
    Delta {
        /// Base object hash referenced by this delta payload.
        base_hash: Hash,
    },
}

/// One explicit constraint row in topology.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CasTopologyConstraint {
    /// Constrained target hash.
    pub target_hash: Hash,
    /// Explicitly allowed bases, sorted deterministically.
    pub bases: Vec<Hash>,
}

/// Snapshot of filesystem CAS object graph + explicit constraints.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CasTopologySnapshot {
    /// Whether implicit empty-content hash node is included in this snapshot.
    pub include_empty: bool,
    /// Object nodes sorted by hash.
    pub nodes: Vec<CasTopologyNode>,
    /// Explicit constraint rows sorted by target hash.
    pub constraints: Vec<CasTopologyConstraint>,
}

/// Renders a CAS topology snapshot as Mermaid flowchart markup.
pub fn render_topology_mermaid(snapshot: &CasTopologySnapshot) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "flowchart TD");
    let _ = writeln!(out, "  %% CAS topology visualization");
    let _ = writeln!(out, "  classDef full fill:#d9f7be,stroke:#389e0d,stroke-width:1px;");
    let _ = writeln!(out, "  classDef delta fill:#ffd591,stroke:#d46b08,stroke-width:1px;");
    let _ = writeln!(out, "  classDef empty fill:#f5f5f5,stroke:#8c8c8c,stroke-width:1px;");

    for node in &snapshot.nodes {
        let node_id = mermaid_node_id(node.hash);
        let hash_short = short_hash(node.hash);
        let line = match node.encoding {
            CasTopologyEncoding::Full => format!(
                "  {node_id}[\"{}\\nfull\\ncontent={}B payload={}B depth={}\"]:::{}",
                hash_short,
                node.content_len,
                node.payload_len,
                node.depth,
                if node.hash == empty_content_hash() { "empty" } else { "full" }
            ),
            CasTopologyEncoding::Delta { base_hash: _ } => format!(
                "  {node_id}[\"{}\\ndelta\\ncontent={}B payload={}B depth={}\"]:::delta",
                hash_short, node.content_len, node.payload_len, node.depth
            ),
        };
        let _ = writeln!(out, "{line}");
    }

    for node in &snapshot.nodes {
        if let CasTopologyEncoding::Delta { base_hash } = node.encoding {
            let from = mermaid_node_id(node.hash);
            let to = mermaid_node_id(base_hash);
            let _ = writeln!(out, "  {from} -->|base| {to}");
        }
    }

    for row in &snapshot.constraints {
        let from = mermaid_node_id(row.target_hash);
        for base in &row.bases {
            let to = mermaid_node_id(*base);
            let _ = writeln!(out, "  {from} -.->|allowed| {to}");
        }
    }

    out
}

/// Returns a depth-limited neighborhood snapshot centered on `target_hash`.
///
/// Neighborhood steps are measured along both delta-base edges and explicit
/// constraint edges. This keeps graph output bounded for large repositories.
pub fn topology_neighborhood_snapshot(
    snapshot: &CasTopologySnapshot,
    target_hash: Hash,
    max_steps: u32,
) -> CasTopologySnapshot {
    let mut adjacency: HashMap<Hash, Vec<Hash>> = HashMap::new();

    for node in &snapshot.nodes {
        adjacency.entry(node.hash).or_default();
        if let CasTopologyEncoding::Delta { base_hash } = node.encoding {
            adjacency.entry(node.hash).or_default().push(base_hash);
            adjacency.entry(base_hash).or_default().push(node.hash);
        }
    }

    for row in &snapshot.constraints {
        for base in &row.bases {
            adjacency.entry(row.target_hash).or_default().push(*base);
            adjacency.entry(*base).or_default().push(row.target_hash);
        }
    }

    let mut keep: BTreeSet<Hash> = BTreeSet::new();
    let mut queue = VecDeque::new();
    queue.push_back((target_hash, 0u32));
    keep.insert(target_hash);

    while let Some((current, distance)) = queue.pop_front() {
        if distance >= max_steps {
            continue;
        }

        for neighbor in adjacency.get(&current).into_iter().flat_map(|neighbors| neighbors.iter()) {
            if keep.insert(*neighbor) {
                queue.push_back((*neighbor, distance.saturating_add(1)));
            }
        }
    }

    let nodes =
        snapshot.nodes.iter().filter(|node| keep.contains(&node.hash)).cloned().collect::<Vec<_>>();

    let constraints = snapshot
        .constraints
        .iter()
        .filter_map(|row| {
            if !keep.contains(&row.target_hash) {
                return None;
            }

            let filtered_bases =
                row.bases.iter().copied().filter(|base| keep.contains(base)).collect::<Vec<_>>();

            if filtered_bases.is_empty() {
                None
            } else {
                Some(CasTopologyConstraint { target_hash: row.target_hash, bases: filtered_bases })
            }
        })
        .collect::<Vec<_>>();

    CasTopologySnapshot { include_empty: snapshot.include_empty, nodes, constraints }
}

/// Renders a depth-limited neighborhood as Mermaid flowchart markup.
pub fn render_topology_mermaid_neighborhood(
    snapshot: &CasTopologySnapshot,
    target_hash: Hash,
    max_steps: u32,
) -> String {
    let neighborhood = topology_neighborhood_snapshot(snapshot, target_hash, max_steps);
    render_topology_mermaid(&neighborhood)
}

/// Builds deterministic Mermaid node identifier from object hash hex.
fn mermaid_node_id(hash: Hash) -> String {
    format!("n{}", hash.to_hex())
}

/// Returns a compact human-readable hash label for diagram node text.
fn short_hash(hash: Hash) -> String {
    let value = hash.to_string();
    if value.len() <= 22 {
        value
    } else {
        let tail = &value[value.len().saturating_sub(12)..];
        format!("{}…{tail}", &value[..10])
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{
        CasTopologyConstraint, CasTopologyEncoding, CasTopologyNode, CasTopologySnapshot,
        render_topology_mermaid, topology_neighborhood_snapshot,
    };
    use crate::Hash;

    #[test]
    /// Ensures renderer emits nodes plus base/constraint edges.
    fn mermaid_renderer_emits_nodes_base_and_constraint_edges() {
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

        let rendered = render_topology_mermaid(&snapshot);
        assert!(rendered.contains("flowchart TD"));
        assert!(rendered.contains(&format!("n{}", target.to_hex())));
        assert!(rendered.contains(&format!("n{}", base.to_hex())));
        assert!(rendered.contains("-->|base|"));
        assert!(rendered.contains("-.->|allowed|"));
    }

    #[test]
    /// Ensures node labels include readable hash and size metadata text.
    fn mermaid_renderer_uses_readable_hash_labels() {
        let hash = Hash::from_content(b"readable-hash-label");
        let snapshot = CasTopologySnapshot {
            include_empty: false,
            nodes: vec![CasTopologyNode {
                hash,
                content_len: 20,
                payload_len: 20,
                depth: 1,
                encoding: CasTopologyEncoding::Full,
            }],
            constraints: vec![],
        };

        let rendered = render_topology_mermaid(&snapshot);
        assert!(rendered.contains("full"));
        assert!(rendered.contains("content=20B"));
        assert!(rendered.contains("payload=20B"));
    }

    #[test]
    /// Ensures neighborhood extraction respects configured step distance.
    fn neighborhood_snapshot_limits_nodes_by_step_distance() {
        let a = Hash::from_content(b"a");
        let b = Hash::from_content(b"b");
        let c = Hash::from_content(b"c");

        let snapshot = CasTopologySnapshot {
            include_empty: false,
            nodes: vec![
                CasTopologyNode {
                    hash: a,
                    content_len: 1,
                    payload_len: 1,
                    depth: 1,
                    encoding: CasTopologyEncoding::Full,
                },
                CasTopologyNode {
                    hash: b,
                    content_len: 1,
                    payload_len: 1,
                    depth: 2,
                    encoding: CasTopologyEncoding::Delta { base_hash: a },
                },
                CasTopologyNode {
                    hash: c,
                    content_len: 1,
                    payload_len: 1,
                    depth: 3,
                    encoding: CasTopologyEncoding::Delta { base_hash: b },
                },
            ],
            constraints: vec![],
        };

        let neighborhood = topology_neighborhood_snapshot(&snapshot, a, 1);
        let kept = neighborhood.nodes.iter().map(|node| node.hash).collect::<BTreeSet<_>>();
        assert!(kept.contains(&a));
        assert!(kept.contains(&b));
        assert!(!kept.contains(&c));
    }
}
