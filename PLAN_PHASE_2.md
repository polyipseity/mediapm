# Phase 2: "Conductor" — High-Performance Functional Orchestration (`src/conductor/`)

This plan defines **Phase 2: Conductor**, a high-performance orchestration engine. It treats every process as a functional transformation of content, using the Phase 1 CAS for absolute persistence, storage optimization, and reproducibility.

## 1. Dual-File Configuration Layer (CUE)

Conductor uses **CUE** to merge human intent with machine-generated state.

* **`user.cue` (Human-Owned):** Defines tool schemas, workflow DAGs, and named **External Content** references.
* **`machine.cue` (Program-Owned):**
  * Stores **Tool Content Maps** (Relative Path -> Algo+Hash).
  * Stores injected **timestamps** for `is_impure` tools.
  * Stores the **State Pointer**: The Algo+Hash of the current **Orchestration State** blob in the CAS.
* **Conflict Resolution:** During unification, if `machine.cue` and `user.cue` provide conflicting values for a field (like a manual hash override), the Conductor requires manual resolution.

---

## 2. Tool Specification & Execution

### Tool Anatomy

* **Metadata:** Args, `runtime_env` (can reference inputs), and `is_impure` (triggers timestamp injection).
* **Inputs:** Resolved to plain content.
  * `${inputs.key}`: Verbatim content string in args.
  * `${inputs.key:file(path)}`: Materializes content to a file at `path` relative to CWD.
* **Outputs & Persistence Flags:**
  * `Save`: Boolean (Default: `true`). If `false`, the content is hashed and recorded but the blob is deleted from the CAS.
  * `ForceFull`: Boolean (Default: `false`). If `true`, the Phase 1 Optimizer is instructed **never** to store this output as a diff; it must remain full data for maximum random-access performance.

### The Orchestration State

Stored as a versioned, immutable blob in the CAS. It contains a collection of **Tool Call Instances**:
> **Instance Key:** `Hash(ToolMetadata + ResolvedInputVector)` (Note `ToolMetadata` does not contain the effective persistence flags. Also note `ToolContentMap` is not part of the hash to allow removing tool content and/or content updates without invalidating the instance.)
> **Resolved Input Vector:** A map of `InputName -> {PlainContent, SourceHash (if applicable)}`.
> **Output Map:** A map of `OutputName -> Algo+Hash`.
> The Orchestration State for each tool call instance also stores the effective persistence flags for each output (but not as part of the instance key), calculated via the merging logic described in the next section.

---

## 3. Deduplication & Incremental Logic

When multiple workflows or steps call the same tool with the same effective input vector, they are deduplicated by the key (`ToolMetadata + ResolvedInputVector` without `ToolContentMap` and the effective persistence flags).

* **Persistence Merging:**
  * **`Save` Logic:** Calculated via **Intersection**. An output is only "not saved" if *every* tool call referencing that output sets `Save: false`.
  * **`ForceFull` Logic:** Calculated via **Union**. If *at least one* tool call sets `ForceFull: true`, the output is stored as full data.
  * The final effective persistence flags are stored in the Orchestration State (not as key, part of the state) and sent as hints to the Phase 1 Optimizer.
* **Re-execution:** If an incremental update discovers a required output was not saved (`Save: false`), the Conductor automatically re-materializes inputs and re-runs the tool.

---

## 4. State Migration via Functional Optics

As the schema of the Orchestration State or Tool Metadata evolves, Conductor uses **Optics** (Lenses and Prisms) to avoid "World Rebuilds."

* **Bidirectional Mapping:** We define a `Lens<StateV1, StateV2>`.
* **Lazy Migration:** When the Conductor reads the State Pointer from `machine.cue`, if the version doesn't match the current binary, it applies the Lens to transform the data in-memory.
* **Reference Preservation:** This ensures that `v1` tool call instances are still discoverable by the `v2` Orchestrator without re-calculating hashes for millions of files.

---

## 5. Built-in Mini-Tools (`src/builtins/`)

Independent, date-versioned crates (e.g., `unzip@2026.03.25`).

* **`fs-ops`**: `copy`, `delete`, `create`. Marked `is_impure` (timestamped) so side-effects can be re-triggered by resetting the timestamp in CUE.
* **`zip`**: Supports `7z`, `zip`, `tar`.
* **`import`**:
  * **Input:** A raw Algo+Hash string.
  * **Behavior:** Fetches the data for a one-time workflow use.
  * **Impurity:** Always `is_impure`.
  * **Use Case:** Import a 10GB dataset -> Run Tool A -> Store Output -> Delete 10GB dataset from CAS. Because the tool call is cached, the workflow remains valid. (Alternatively, the user can also choose to not delete the dataset and just let it take up space in the CAS.)
  * **Note:** Note the output cache will have the same hash as the input, but this is okay.

---

## 6. Storage Optimization: Reverse-Diff

Conductor exploits the relationship between inputs and outputs to save space.

1. **Instruction:** For each tool call, Conductor sends a hint to the Phase 1 Optimizer.
2. **Logic:** Store **Input $N-1$ as a diff relative to Output $N$**.
3. **Rationale:** The "Result" of a tool is typically the most frequently accessed and "final" version of the data. By making the source the delta, we ensure the final output is "Full Data" for O(1) retrieval speed.

---

## 7. CLI & API

### Conductor CLI

* **`conductor import tool <path> --name <name>`**: Registers tool files in CAS and generates CUE metadata.
* **`conductor import data <path> [--description <desc>]`**: Standard external content registration. If missing description, defaults to the file name. Adds to the external data.
* **`conductor remove data <name>`**: remove external data reference from the CUE. This does not delete the blob from the CAS since it may be referenced by other workflows, but it makes it eligible for garbage collection if no workflows reference it.
* **`conductor remove tool <name> [--metadata]`**:
  * Default: Clears content hashes from `machine.cue`.
  * `--metadata`: Deletes the tool definition from `user.cue`.
* **`conductor gc`**: Prunes any CAS entry not found in the unified CUE view (External Data, Tool Content, or Orchestration State).
* **`conductor cas <args...>`**: Direct passthrough to Phase 1.

### API (Rust)

```rust
pub trait Conductor {
    /// Executes DAG, handles caching and reverse-diff hints
    async fn run_workflow(&self, user_cue: PathBuf, machine_cue: PathBuf) -> Result<Summary, Error>;

    /// Low-level access to the migrated Orchestration State
    async fn get_state(&self) -> Result<OrchestrationState, Error>;
}
```
