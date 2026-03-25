# Phase 1: High-Performance Unified Delta CAS (`src/cas/`)

## 1. Architectural Principles

* **Performance:** Zero-copy I/O where possible, BLAKE3 for SIMD-accelerated hashing, and `io-uring` support for asynchronous disk operations.
* **The "Everything is a Diff" Unified Model:** * The CAS implicitly stores the **Empty Content** (Hash of `[]`).
  * Every stored item is stored as **VCDIFF** instruction set (except for the Empty Content and the Full data).
  * "Full data" is simply a diff where the source is the Empty Content. The only difference is that when storing the data, we never use the VCDIFF file format, we store th full data directly, but we still treat it as a diff of the empty content in the index and optimization logic. This means we also need another index that simply points to all data that is stored as full data to distinguish whether it is a diff or full data when we want to retrieve it.
* **Actor Pattern:** Managed by `ractor`. State is localized within actors; external interaction happens via typed messages.
* **Async Agnostic:** Defaulting to `tokio`, but using traits to abstract the executor and I/O driver.

---

## 2. Technical Specifications

### Hashing & Pathing

* **Algorithm:** **BLAKE3**. It provides the best performance-to-security ratio, utilizing AVX-512 and multi-threading natively.
* **Storage Path:** `{root}/{version}/{algo}/{h[0:2]}/{h[2:4]}/{h[4..]}`.
* **Invariants:** The `Hash` type is a stack-allocated byte array (32 bytes for BLAKE3) wrapped in a struct that prevents unvalidated construction.

### Storage Logic: VCDIFF

* All files on disk are stored in a unified format: `[BaseHash (32b)][VCDIFF Payload]`.
* **The Root:** The system initializes by ensuring the file for `blake3("")` exists as a zero-byte diff.
* **Retrieval:** To `Get(Hash)`, the `StorageActor` reads the `BaseHash`. If the `BaseHash` is not the "Empty Content," it recursively resolves the base until it reaches the root, applying VCDIFF patches upward.

### Graph & Index Management

* **Storage:** **Redb** (Persistent, ACID, Type-safe KV store).
* **Connected Components:** The index stores an undirected graph where an edge exists if $A$ is a diff of $B$. Note, the connected components CANNOT have cycles, because that would mean the files reference each other as bases, which is invalid. The graph is guaranteed to be a DAG. Need to always ensure this is the case in the code.
* **Component Tracking:** Uses a **Disjoint Set Union (DSU)** algorithm to partition hashes into connected components, allowing the optimizer to narrow its search space to related files.

---

## 3. The Constraint & Optimizer System

### Unified Constraint Model

A constraint is no longer a command, but a **set of possibilities**.

* **Constraint Structure:** `(TargetHash, Set<PotentialBaseHashes>)`.
* **Implicit Constraint:** Every hash has an implicit base of the "Empty Content" to ensure it can always be stored.
* **Force Full Data:** Expressed as `Constraint(Hash, {EmptyContentHash})`.
* **Force Specific Diff:** Expressed as `Constraint(Hash, {TargetBaseHash})`.
* **Optimization:** Expressed as `Constraint(Hash, {BaseA, BaseB, BaseC, EmptyContent})`.

### The Optimizer Actor

The optimizer runs as a background process using an **Incremental Greedy Strategy**:

1. **Candidate Evaluation:** For a given `TargetHash`, the optimizer calculates the resulting file size for each `PotentialBaseHash` in its constraint set.
2. **Cost Function:** The optimizer minimizes $Cost = Size(Delta) + \alpha \cdot Depth(Chain)$, where $\alpha$ is a penalty for long reconstruction chains to preserve CPU performance.
3. **Refactor:** Once the "best" base is chosen, the optimizer rewrites the local file and updates the `IndexActor`.
4. **Pruning:** The optimizer periodically scans the `Constraint Table`. If a `PotentialBaseHash` no longer exists in the CAS, it is removed from the constraint set. If a set becomes empty, it defaults to the `EmptyContentHash`.

---

## 4. Implementation Detail: Data Migration (Optics)

Since storage formats and index schemas evolve, we use **Bidirectional Optics**:

* A `Lens` maps a `v1::StorageNode` to a `v2::StorageNode`.
* If the `store_version` in the path doesn't match the current binary version, the `StorageActor` uses the optics to transform the data on-the-fly or triggers a background migration.

---

## 5. Failure Resilience

* **Atomic Commits:** All writes use `tempfile` on the same mount point followed by an atomic `rename`.
* **Index Integrity:** Redb's ACID properties ensure the graph index never points to a file that doesn't exist.
* **Disk Pressure:** Actors monitor `fs::Metadata`. If disk space falls below a threshold, the `StorageActor` rejects `Put` requests, while the `OptimizerActor` pivots to a "Max Compression" mode to free space.

---

## 6. Public API & CLI

### Programmatic Interface (Traits)

```rust
#[async_trait]
pub trait CasAPI {
    /// Stores data by diffing against the Empty Content by default
    async fn put(&self, data: Bytes) -> Result<Hash, CasError>;

    /// Retrieves and reconstructs the full content
    async fn get(&self, hash: Hash) -> Result<Bytes, CasError>;

    /// Adds/Updates a constraint for a specific hash
    async fn set_constraint(&self, hash: Hash, bases: Vec<Hash>) -> Result<(), CasError>;
}
```

### CLI Commands

* `cas store <file>`: Uploads a file.
* `cas get <hash>`: Reconstructs and outputs file.
* `cas constraint add <hash> --bases <h1,h2,h3>`: Injects optimization targets.
* `cas optimize --run-once`: Forces the optimizer to evaluate all constraints immediately.

---

## 7. Testing & Quality

* **Unit Tests:** Every actor and codec has 100% coverage.
* **Integration Tests:** Simulated "Chaos" tests where the optimizer is interrupted mid-recompression.
* **Performance Benchmarks:** Comparison of reconstruction latency for diff chains of length 1, 5, and 10.
