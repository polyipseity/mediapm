# Phase 2: "Conductor" — High-Performance Functional Orchestration (`src/conductor/`)

This plan defines **Phase 2: Conductor**, a high-performance orchestration engine. It treats every process as a functional transformation of content, using the Phase 1 CAS for absolute persistence, storage optimization, and reproducibility.

## 1. Dual-File Configuration Layer (Nickel)

Conductor uses **Nickel (`.ncl`)** to merge human intent with machine-generated state.

* **`user.ncl` (Human-Owned):** Defines tool schemas, workflow DAGs, and named **External Content** references.
* **`machine.ncl` (Program-Owned):**
  * Stores **Tool Content Maps** (Relative Path -> Algo+Hash).
  * Stores injected **timestamps** for `is_impure` tools.
  * Stores the **State Pointer**: The Algo+Hash of the current **Orchestration State** blob in the CAS.
* **Conflict Resolution:** During recursive Nickel merging, if `machine.ncl` and `user.ncl` provide non-mergeable values for the same field (for example, a manual hash override that conflicts with machine state), the Conductor requires manual resolution.

The Rust side embeds **`nickel-lang-core`** directly. There is no external Go-based evaluation layer and no CUE interpreter anywhere in Phase 2.

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

## 4. State Migration via Nickel + Functional Optics

As the schema of the configuration or the Orchestration State evolves, Conductor uses **Nickel migrations** for configuration documents and **Optics** (Lenses and Prisms) for Rust-side state envelopes to avoid "World Rebuilds."

### Nickel Migration Ladder

All `.ncl` document migrations must be authored in Nickel itself.

* **Atomic Steps:** Define one function per schema hop (`v1_to_v2`, `v2_to_v3`, ...).
* **Recursive Resolver:** A top-level `migrate` function examines `version`, applies the next hop, and recurses until the current version is reached.
* **Contract Validation:** The result of migration is checked against the latest Nickel contract before Rust deserializes it.
* **No Logic Duplication:** Structural reshaping stays in `.ncl`; Rust only deserializes the final upgraded structure.
* **Bidirectional Mapping:** We define a `Lens<StateV1, StateV2>`.
* **Lazy Migration:** When the Conductor reads the State Pointer from `machine.ncl`, if the version doesn't match the current binary, it applies the Lens to transform the data in-memory.
* **Reference Preservation:** This ensures that `v1` tool call instances are still discoverable by the `v2` Orchestrator without re-calculating hashes for millions of files.

For configuration documents, Rust wraps the persisted document source with a Nickel migration wrapper, evaluates it with `nickel-lang-core`, and only then deserializes it into the latest Rust struct.

---

## 5. Built-in Mini-Tools (`src/conductor-builtins/`)

Independent, versioned crates (for example, `builtin@v1.0.0`) with shared
contracts: CLI args are string-valued flags/options, API args are
`BTreeMap<String, String>` plus optional raw payload bytes for
content-oriented operations.

* **`echo`** (pure): reference runtime contract used for smoke tests and
  deterministic stream behavior validation.
* **`fs`** (impure): rooted
  filesystem staging operations (`ensure_dir`, `write_text`, `copy`).
* **`import`** (impure): ingress builtin with `kind=file|folder|fetch`.
  * `file` imports one file as bytes.
  * `folder` imports one folder as an uncompressed ZIP payload.
  * `fetch` performs HTTP(S) ingress with required `expected_hash` pinning.
* **`export`** (impure): filesystem egress builtin with `kind=file|folder`.
  * `folder` expects uncompressed ZIP folder payloads.
* **`archive`** (pure): ZIP-only content transforms.
  * `pack`: folder payload (uncompressed ZIP) -> archive bytes.
  * `unpack`: archive bytes -> folder payload (uncompressed ZIP).
  * `repack`: archive bytes -> normalized archive bytes.

Design boundary: builtins remain minimal "connective tissue" for bootstrap and
cross-platform consistency. Structured data transformations and domain-heavy
media operations remain external tools or Phase 3 concerns.

Filesystem interaction boundary: builtin-side host filesystem reads/writes must
go through `import`, `export`, or `fs` only.

---

## 6. Storage Optimization: Reverse-Diff

Conductor exploits the relationship between inputs and outputs to save space.

1. **Instruction:** For each tool call, Conductor sends a hint to the Phase 1 Optimizer.
2. **Logic:** Store **Input $N-1$ as a diff relative to Output $N$**.
3. **Rationale:** The "Result" of a tool is typically the most frequently accessed and "final" version of the data. By making the source the delta, we ensure the final output is "Full Data" for O(1) retrieval speed.

---

## 7. CLI & API

### Conductor CLI

* **`conductor import tool <path> --name <name>`**: Registers tool files in CAS and updates machine-owned Nickel metadata.
* **`conductor import data <path> [--description <desc>]`**: External data references are user-owned in `user.ncl`; Phase 2 currently expects the user to maintain these declarations explicitly.
* **`conductor remove data <name>`**: External data references are user-owned in `user.ncl`; Phase 2 currently expects the user to remove these declarations explicitly.
* **`conductor remove tool <name> [--metadata]`**:
  * Default: Clears content hashes from `machine.ncl`.
  * `--metadata`: Reserved for future user-document workflows; tool metadata remains user-owned.
* **`conductor gc`**: Prunes any CAS entry not found in the unified Nickel view (External Data, Tool Content, or Orchestration State).
* **`conductor cas <args...>`**: Direct passthrough to Phase 1.

### API (Rust)

```rust
pub trait Conductor {
    /// Executes DAG, handles caching and reverse-diff hints
  async fn run_workflow(&self, user_ncl: PathBuf, machine_ncl: PathBuf) -> Result<Summary, Error>;

    /// Low-level access to the migrated Orchestration State
    async fn get_state(&self) -> Result<OrchestrationState, Error>;
}
```
