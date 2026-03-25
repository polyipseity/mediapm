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

* **Media Registry:** Defines URIs and desired variants.
  * **Online:** Must use `http/s`.
  * **Local:** Uses a special URI schema (e.g., `local:<randomly generated UUID>`).
* **Hierarchy Definition:** Maps filesystem paths to media variants.
* **Metadata Provider IDs:** Allows manual override of metadata sources.

### `mediapm.conductor.cue` (CUE) — Tooling & DAGs

* **Tool Wrappers:** High-performance schemas for `ffmpeg`, `yt-dlp`, etc.
* **Workflow Templates:** Defines the functional pipelines for local and online sources.

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

* **Commit-Pinning:** Tools are added using Git commit hashes for absolute determinism.
* **Persistent Metadata:** Tool metadata is never removed unless no workflows reference it.
* **Version Fallback:** If a workflow's specific tool version is missing, `mediapm` promotes the workflow to the latest version and retries.
* **Lifecycle:** Updating a tool deletes the old binary reference but keeps the metadata for cache-lookup.

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

* **`mediapm tool add <name> [--tag <tag>] [--commit <hash>]`**: Provisions a binary to the conductor. Both tag or commit is optional. If missing, default to latest release tag. If both are provided, the commit must match the tag. The tool name passed to conductor is `<name>@<commit>`. For tags, resolve the commit hash at the time of addition and use that for the tool name to ensure immutability.
* **`mediapm tool remove <name> [--commit <hash>]`**: Removes a tool binary reference from the conductor. The metadata is kept for cache lookup, but the binary is removed to save space, regardless if it is still referenced by any workflow.
* **`mediapm tool list`**: Lists all registered tools with their metadata.
* **`mediapm tool update <name> [--tag <tag>] [--commit <hash>]`**: Updates a tool to a new version. This is effectively an add of the new version followed by a remove of the old version. The old version's metadata is kept for cache lookup, but the binary is removed to save space, regardless if it is still referenced by any workflow.
* **`mediapm media add <URI>`**: Adds a media source to the `mediapm.ncl`. Note this does not configure any variants or hierarchy, just registers the source.
* **`mediapm media add-local <path>`**: Imports a local source and assigns a local URI. This also imports the media into the conductor as an external data. This does not configure any variants or hierarchy, just registers the source.
* **`mediapm sync`**: Computes the diff between `.ncl` and `.lock.jsonc` and materializes via `.tmp/`.
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
