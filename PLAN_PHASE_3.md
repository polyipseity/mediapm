# Phase 3: `mediapm` — Specialized Media Orchestration (`src/mediapm/`)

This plan defines **Phase 3: `mediapm`**, a specialized media manager built on the Conductor (Phase 2) and CAS (Phase 1). It transforms functional orchestration into a high-level media library system with strict cross-platform invariants and deterministic tool management.

## 1. Architectural Principles

* **Atomic Sync via Staging:** All filesystem mutations are prepared in `.mediapm/tmp/` before an atomic `rename` to the final destination.
* **The "Package" Concept:** A "Media" item is a bundle of content (Video/Audio) and accompanying sidecar files (Subtitles, Descriptions, Thumbnails).
* **Decoupled Hierarchy:** Media definitions (content) are separated from the Folder Hierarchy (view). One media variant can exist at multiple paths.
* **Strict Portability (NFD Only):** All paths must be normalized using **Unicode NFD** only to ensure total compatibility across macOS, Windows, and Linux. Note we do not normalize in the tool, we require the input paths to be NFD normalized and reject any that are not.

---

## 2. Configuration & State Management

### `mediapm.ncl` (Nickel) — Policy & Hierarchy

* **Media Registry:** Defines URIs and desired variants. Online sources must
   use `http/s`; local sources use `local:<id>` URIs.
* **Per-Media Shape:** Optional `description`; remote sources use explicit
   `download` config; local sources store `variant_hashes` (variant -> CAS hash
   pointer).
* **Hierarchy Definition:** Maps filesystem paths to media variants.
* **Transform Pipeline:** Ordered `transforms` list where each step declares
   `operation`, `targets`, and operation-specific `options` that reject unknown
   keys.
* **Runtime Storage Resolution:** `runtime_storage.mediapm_dir` defaults to
   `.mediapm` and resolves relative to the outermost `mediapm.ncl` directory
   when provided as a relative path. Relative
   `runtime_storage.library_dir` resolves relative to the outermost
   `mediapm.ncl` directory, while relative `runtime_storage.tmp_dir` resolves
   relative to the effective `mediapm_dir`.

### `mediapm.conductor.ncl` / `mediapm.conductor.machine.ncl` (Nickel) — Tooling & DAGs

* **Tool Wrappers:** High-performance schemas for `ffmpeg`, `yt-dlp`, etc.
* **Workflow Templates:** Defines the functional pipelines for local and online sources.
* **Mediapm-managed Conductor Defaults:** Phase 3 writes grouped runtime
   storage defaults as `conductor_dir = .mediapm`,
   `state_ncl = .mediapm/state.ncl`, and `cas_store_dir = .mediapm/store`.

### Materialization Root Default

* By default, materialized output is written directly under the directory that
   contains the topmost `mediapm.ncl` (no implicit `library/` wrapper folder).

### `.mediapm/lock.jsonc` — The Ground Truth

* Tracks every file managed by `mediapm`.
* Tracks cached outputs of permanent transcodes to ensure they are never accidentally deleted.

---

## 3. The Media Workflow Pipelines

### Online Source Pipeline

1. **Download:** Fetches media, subtitles, descriptions, and thumbnails.
2. **Transcode (Optional):** * Disabled by default.
   * **Permanent transcode:** If enabled (**true** by default for online sources), the original download output is **not cached** to save CAS space; the transcode output becomes the cached product. If permanent transcode is enabled, for safety (in case the workflow changes), the cached transcode output is marked as **External Data** to prevent deletion. Track the safety external data in the lockfile and periodically prune if the safety external data is not referenced by any workflow for a certain period (e.g., 30 days). Safety external data that is orphaned is warned in the CLI output during sync and can be manually pruned or fixed by the user.
3. **Apply Metadata (Enabled by Default):** Injects tags and covers. Manual provider ID overrides are supported to allow using metadata if auto search for metadata fails.

### Local Source Pipeline

Note, for a local source, you need to first import the data via the conductor to get the hash, then use the hash as the input to the pipeline. Additionally, if permanent transcode is enabled, you need to remove the data hash from the external data in the conductor after the transcode to save space since the transcode output becomes the cached product.

1. **Import:** The file is already materialized into the conductor. This step just mirrors the download step for local files and allows applying the same transcode and metadata logic.
2. **Transcode (Optional):** * Disabled by default.
   * **Permanent transcode:** If enabled (**false** by default for local sources), the original imported output is **not cached** to save CAS space; the transcode output becomes the cached product. If permanent transcode is enabled, for safety (in case the workflow changes), the cached transcode output is marked as **External Data** to prevent deletion. Track the safety external data in the lockfile and periodically prune if the safety external data is not referenced by any workflow for a certain period (e.g., 30 days). Safety external data that is orphaned is warned in the CLI output during sync and can be manually pruned or fixed by the user.
3. **Apply Metadata (Enabled by Default):** Injects tags and covers. Manual provider ID overrides are supported to allow using metadata if auto search for metadata fails.

---

## 4. Automated Tool Lifecycle (The Toolsmith)

### Decoupled Tool Management Protocol

* **`mediapm` (Declarative Frontend):** `mediapm.ncl` contains desired tool requirements (`tools.<name>` with required `version` or `tag`, optional matching pair).
* **`conductor` (Operational Backend):** Conductor machine state stores executable metadata and installed binary content-map references.

### Reconciliation (`mediapm sync`)

1. **Read `mediapm.ncl`:** Parse desired tools and versions.
2. **List registered tools:** Query conductor machine registry state.
3. **Reconcile:**
   * Missing tool -> add new immutable tool id (`<name>@<version>`).
   * Version mismatch -> register new immutable tool id and promote it as active in lock state.
4. **Activate:** Persist active tool pointer in `.mediapm/lock.jsonc` (`active_tools`).

### Upgrade & Pruning Semantics

* **Upgrade (default through `sync`):** Adds new desired version and keeps historical metadata.
* **Pruning (optional):** `mediapm tools prune --id <tool_id>` removes binary content references and underlying CAS blobs when possible, but retains metadata and marks tool status as `pruned`.

### Specialized Tool Integration Defaults

* **FFmpeg (`8.1`, source=`Evermeet`):** Prefer Evermeet static distribution metadata for macOS flows.
* **yt-dlp (`2026.03.17+`):** Track latest GitHub release by desired-state selector.
* **rsgain (`3.7`):** Prefer portable archive distributions.
* **media-tagger (`latest`):** Invoke built-in launcher (`mediapm internal media-tagger`) to run Chromaprint + AcoustID + MusicBrainz + FFmetadata + FFmpeg.

### Validation Rule

Before finalizing a tool registration, Phase 3 verifies:

* registry fingerprint can be represented as a deterministic Rust CAS hash string,
* executable responds successfully to `--version`.

---

## 5. The Materializer: Cross-Platform Safety

### Atomic Sync Strategy

1. **Stage:** Files are materialized into `.mediapm/tmp/`.
2. **Verify:** Checks NFD normalization and character invariants.
3. **Commit:** Atomic `rename` to the library.

### Path Invariants

* **NFD Normalization:** All filenames use Unicode NFD only. Reject unnormalized paths to prevent cross-platform issues.
* **Strict Rejection:** Rejects characters: `<`, `>`, `:`, `"`, `/`, `\`, `|`, `?`, `*`.
* **Link Hierarchy:** Uses **Hard Links** as the primary method to allow the same media to appear in multiple folders without extra disk cost. Fallback to symlink, then reflink, then copy.

---

## 6. CLI & API

### CLI Commands

* **`mediapm sync`**: Reconciles desired tool/media state from `mediapm.ncl`, executes conductor workflows, and materializes hierarchy via staging.
* **`mediapm tools list`**: Lists all registered tools with lifecycle status (`active`/`pruned`) and binary presence.
* **`mediapm tools prune --id <tool_id>`**: Removes one tool binary reference from conductor/CAS while preserving metadata.
* **`mediapm media add <URI>`**: Adds a media source to the `mediapm.ncl`. Note this does not configure any variants or hierarchy, just registers the source.
* **`mediapm media add-local <path>`**: Imports a local source and assigns a local URI. This also imports the media into the conductor as an external data. This does not configure any variants or hierarchy, just registers the source.
* **`mediapm cas <args...>`**: Direct passthrough to Phase 1.
* **`mediapm conductor <args...>`**: Direct passthrough to Phase 2.

### API (Rust)

```rust
pub trait MediaPM {
    /// Handles the 3-stage pipeline for online or local sources
    async fn process_source(&self, uri: Url, permanent: bool) -> Result<MediaPackage, MediaError>;

    /// Synchronizes the NFD-normalized hierarchy with atomic staging
    async fn sync_library(&self) -> Result<SyncSummary, MediaError>;
}
```
