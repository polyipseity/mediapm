# mediapm — Implementation Plan (Extremely Detailed)

## 0) Vision, Product Positioning, and Persona

### 0.1 Project Identity

- **Name:** mediapm
- **One-line definition:** A declarative, workspace-local, content-addressed media organizer that records media provenance and transformation history while minimizing mutable state.
- **Tagline:** _Treat media like immutable artifacts, link them like configuration._

### 0.2 What mediapm Is / Is Not

**mediapm is:**

- A **content store + metadata + declarative linker**.
- A tool that **delegates downloading** and heavy fetch orchestration to other projects.
- A system that uses **URI as identity**, plus **content hash variants**.
- A **functional-core-first** Rust system.
- A workspace tool where links are **explicitly declared by the user** (not auto-magically discovered and rearranged).

**mediapm is not:**

- A generic “scan everything and auto-organize my files” media manager.
- A monolithic download manager.
- A database-centric application.

### 0.3 Primary Personas

1. **Collector / Archivist**

   - Cares about provenance, reproducibility, and exact source tracking.

2. **Music Enthusiast**

   - Cares about rich metadata quality, album/track correctness, and provider-backed enrichment (MusicBrainz now, more later).
   - Cares about preserving originals and clearly recording edits/transcodes.

3. **Technical User / Power User**

   - Wants deterministic operations, scriptability, and composable declarative control.

4. **Developer Integrator**

   - Wants a stable core library and simple CLI commands to build custom workflows.

### 0.4 Hard Product Constraints (From Requirements)

- **Rust implementation.**
- **No metadata DB** (no SQLite/Postgres/etc. for canonical metadata state).
- **JSON sidecars** are the canonical metadata storage.
- **URI is the only identity key** for source identity.
- A source URI may have multiple variants; variants are content-addressed by hash.
- Content store is in the workspace.
- Links are manually specified declaratively by the user.
- Keep design simple and command set minimal.
- Architecture should be as functional as practical (functional programming style, low mutable state).

---

## 1) Core Principles

1. **Declarative over imperative**

   - Desired state is expressed in config.
   - CLI reconciles actual state to desired state.

2. **Functional core, imperative shell**

   - Pure planning functions produce effects.
   - Side effects are executed in a thin shell layer.

3. **Minimal state, explicit state**

   - Persistent state = content-addressed objects + JSON sidecars + config files.
   - No hidden database state.

4. **Determinism where feasible**

   - Canonical serialization and reproducible plans.
   - Idempotent commands.

5. **Safety and provenance first**

   - Preserve original hashes and original metadata snapshots.
   - Record edit history with reversible/non-reversible classification.

6. **Cross-platform correctness**

   - Linux/macOS/Windows behavior documented and tested.

---

## 2) High-Level Architecture

### 2.1 Bounded Contexts

1. **Spec Engine**

   - Parses and evaluates declarative user spec.
   - Produces normalized desired state.

2. **Identity & Storage Core**

   - URI canonicalization.
   - Content hashing and object pathing.
   - Object store management.

3. **Metadata Core**

   - Extracts container metadata.
   - Maintains normalized and provider-enriched metadata.
   - Maintains sidecar schema/version lifecycle.

4. **History Core**

   - Records original metadata, original hash, and transformation chain.
   - Differentiates reversible vs non-reversible edits.

5. **Link Materializer**

   - Resolves declared links to chosen variant.
   - Creates symlink/hardlink/reflink/copy according to policy.

6. **Provider Integration Layer**

   - MusicBrainz now.
   - Provider abstraction for future providers.

7. **Planner/Executor**

   - Computes desired effects.
   - Executes effects atomically where possible.

### 2.2 Functional Core Pattern

Define a pure state transition model:

```text
(State, CommandInput) -> (NewStateProjection, Vec<Effect>)
```

Where:

- `State` is loaded from JSON + object store indexes + config.
- `CommandInput` is CLI args + runtime options.
- `NewStateProjection` is computed desired state (pure).
- `Effect` is explicit IO action (read/write/link/hash/network/probe/transcode invocation wrapper).

Imperative shell:

- Interprets and executes `Effect`s.
- Captures outcomes and writes final JSON updates.

---

## 3) Filesystem Layout (Workspace-local Store)

```text
<workspace>/
  mediapm.ncl                      # Nickel entry config (or chosen entrypoint)
  .mediapm/
    objects/
      blake3/
        ab/
          cdef...                 # content-addressed media object file
    media/
      <media-id>/
        media.json                # canonical metadata sidecar for media identity (URI-keyed)
        variants/
          <variant-hash>.json     # per-variant sidecar (or folded into media.json)
    providers/
      musicbrainz/
        cache/
          <cache-key>.json
    links/
      manifest.json               # optional materialization state snapshot
    locks/
    tmp/
```

### 3.1 Notes

- The **object filename** is hash-based.
- The **identity key** remains canonical URI in metadata JSON.
- We keep object files immutable once imported.

---

## 4) Identity, URI Canonicalization, and Variant Model

### 4.1 URI as Only Identity

- Canonical identity key: `canonical_uri: String`
- Canonicalization pipeline:
  1. Parse using `url` crate.
  2. Normalize scheme + host rules where applicable.
  3. Normalize path segments.
  4. Strip disallowed ambiguity where policy permits.
  5. For file paths, convert to canonical absolute file URI.

### 4.2 Variant Model

A single URI may produce multiple variants over time:

- Original import variant.
- Transcoded variant(s).
- Metadata-edited container variant(s).

Variant identity:

- `variant_hash` (BLAKE3 of bytes)
- `container_format`, `codec profile` info
- Parent lineage edges

### 4.3 Data Structures (Performance-oriented)

In-memory indexes (rebuilt from JSON at startup):

- `HashMap<CanonicalUri, MediaRecordRef>`
- `HashMap<Blake3Hash, VariantRecordRef>`
- `HashMap<CanonicalUri, SmallVec<[VariantHash; N]>>`
- Optional derived indexes for fast query by artist/album tag for CLI views.

Use compact binary hash type in memory:

- `[u8; 32]` wrapper newtype for BLAKE3.
- Avoid repeated hex string allocations in core algorithms.

---

## 5) JSON Sidecar Schema (No DB)

### 5.1 Canonical media.json (per URI)

```json
{
  "schema_version": 1,
  "canonical_uri": "file:///...",
  "created_at": "...",
  "updated_at": "...",
  "original": {
    "original_variant_hash": "...",
    "original_metadata": {
      "container": {},
      "tags": {},
      "streams": []
    }
  },
  "variants": [
    {
      "variant_hash": "...",
      "object_relpath": ".mediapm/objects/blake3/ab/cdef...",
      "byte_size": 123,
      "container": "flac",
      "probe": {},
      "metadata": {},
      "lineage": {
        "parent_variant_hash": null,
        "edit_event_ids": ["evt_..."]
      }
    }
  ],
  "edits": [
    {
      "event_id": "evt_...",
      "timestamp": "...",
      "kind": "revertable",
      "operation": "metadata_update",
      "details": {},
      "from_variant_hash": "...",
      "to_variant_hash": "..."
    },
    {
      "event_id": "evt_...",
      "timestamp": "...",
      "kind": "non_revertable",
      "operation": "transcode",
      "details": {},
      "from_variant_hash": "...",
      "to_variant_hash": "..."
    }
  ],
  "provider_enrichment": {
    "musicbrainz": {
      "matches": [],
      "applied": {}
    }
  }
}
```

### 5.2 Required semantics

- `original.original_variant_hash` never changes once set.
- `original.original_metadata` is immutable snapshot.
- `edits.kind` must be one of:
  - `revertable`
  - `non_revertable`

### 5.3 Canonical JSON writing

- Write JSON deterministically (stable map ordering/canonicalization strategy).
- Atomic write pattern: temp file -> fsync -> rename.

### 5.4 Schema migration

- Every sidecar carries `schema_version`.
- On load, apply sequential **version-to-version optics migrations** to the latest model.

### 5.5 Schema migration architecture (Functional Optics)

For metadata sidecar upgrades, use a lens-based transformation pipeline over `serde_json::Value`.

- Migration model:
  - `vN -> vN+1` is expressed as a composed chain of optics steps.
  - Steps focus and transform only targeted paths; non-focused data remains intact.
  - Migrations are declarative manifests, not ad-hoc procedural rebuilds.

- Optics types by use case:
  - **Lens**: rename/update required fields.
  - **Prism**: safely transform optional/enum-like branches.
  - **Traversal**: map over arrays/collections.

- Library strategy (2026):
   1. **`panproto-lens`** (default/primary): schema-to-schema migration with protolenses and law-oriented behavior.
   2. **`karpal-optics`** (secondary): profunctor-heavy functional optics composition for advanced cases.
   3. **`focus-rs`** (ergonomic utility): macro-focused deep `serde_json::Value` path updates in lightweight migrations.

Operational rules:

- Keep a `MigrationChain` registry for every supported version hop.
- Disallow skipping intermediate version transforms unless explicitly validated.
- Preserve unknown fields by default (forward compatibility).
- Canonicalize JSON after each successful migration write.
- Record migration provenance (`from_version`, `to_version`, timestamp, migration id).

Illustrative shape:

```rust
use panproto_lens::{lens, Migration};
use serde_json::Value;

pub fn migrate_v1_v2() -> Migration<Value> {
      Migration::builder()
            .step(lens!("rating").rename("user_score"))
            .step(lens!("tags").each().map(|tag: String| tag.to_uppercase()))
            .step(lens!("location").maybe().map(transform_location))
            .build()
}
```

This turns migration logic into composable "focus + transform" operations and minimizes accidental data loss during upgrades.

### 5.6 Migration strategy comparison (adopted)

| Feature | Old Way (Procedural) | New Way (Functional Optics) |
| :--- | :--- | :--- |
| **Logic** | `match version { ... }` + manual struct mapping | `MigrationChain` of composed Lenses/Prisms/Traversals |
| **Safety** | Elevated risk of dropping fields during remaps | Focused transforms only touch addressed data paths |
| **Boilerplate** | High (`V1`, `V2`, `V3` structs + manual conversion glue) | Lower (`serde_json::Value` + structural path optics) |
| **Sidecars** | Manual parse/rebuild-heavy migration code | Direct, composable transforms on loaded JSON buffer |

### 5.7 Internal proposal note (for team communication)

Proposed migration direction:

- Refactor metadata migrations to a lens-based transformation pipeline.
- Define migration steps as declarative optics composition rather than version-specific imperative conversion code.
- Preserve non-targeted sidecar state by default and improve maintainability as schema versions grow.

Expected result:

- Better upgrade safety,
- lower migration boilerplate,
- clearer review surface for schema evolution,
- and stronger long-term correctness through migration invariants/tests.

---

## 6) Metadata Extraction and Edit Tracking

### 6.1 Metadata extraction goals

For any common container format we support initially (MP3, FLAC, M4A/MP4, OGG, WAV):

- Container-level info.
- Stream-level technical metadata.
- Embedded tags.
- Optional embedded artwork references (with size/hash if extracted).

### 6.2 Original metadata snapshot

At first import per URI:

- Compute hash of imported bytes -> original variant hash.
- Probe and capture raw metadata snapshot into `original.original_metadata`.

### 6.3 Edit classes

1. **Revertable edits**

   - Example: tag/title/album/artist changes.
   - Represent as patch operations in history so they can be reapplied/reverted.

2. **Non-revertable edits**

   - Example: transcoding, lossy transformation, destructive transforms.
   - Must record full operation metadata and resulting variant hash.
   - Reversion means selecting an earlier variant, not inverse-transforming bytes.

### 6.4 Transformation provenance

Each edit event stores:

- Tool + version used.
- Parameters.
- Input hash and output hash.
- Classification (revertable/non-revertable).
- Optional user message.

---

## 7) Provider Metadata Architecture (MusicBrainz-first)

### 7.1 Provider abstraction

Define trait-like interface:

- `search(query, context) -> candidates`
- `fetch(entity_id) -> provider_payload`
- `map_to_normalized(provider_payload) -> normalized_patch`

### 7.2 MusicBrainz specifics

- Dedicated module for MusicBrainz API interaction.
- Respect strict rate limiting and required user-agent policy.
- Cache raw responses locally in `.mediapm/providers/musicbrainz/cache`.

### 7.3 Authority layering

When merging metadata:

Priority policy (configurable default):

1. User explicit overrides in declarative spec.
2. Manual local metadata edits.
3. Provider metadata (MusicBrainz).
4. Embedded source tags.

Store provenance per field where practical.

---

## 8) Declarative Language Choice and Config Model

### 8.1 Language requirement

Need a language that:

- Expresses data declaratively.
- Allows lightweight code expression for reducing repetition.
- Is existing and embeddable in Rust.

### 8.2 Recommended choice: Nickel

Rationale:

- Config-focused language with compositional features.
- Better ergonomics for modular config and merges than plain JSON/TOML.
- Suitable for a Nix-like declarative user experience without building a DSL from scratch.

### 8.3 Config model (conceptual)

Top-level declarations:

- `sources`: URI declarations and optional hints.
- `metadata_overrides`: explicit field overrides.
- `links`: manual desired links from workspace paths -> selected media/variant selection rules.
- `policies`: link strategy preferences, provider toggles, conflict policy.

### 8.4 Example (illustrative pseudo-Nickel)

```nickel
{
  sources = [
    {
      uri = "file:///music/inbox/song.flac",
      tags = { mood = "focus" }
    }
  ],

  links = [
    {
      path = "library/Artist/Album/01 - Song.flac",
      from_uri = "file:///music/inbox/song.flac",
      select = { prefer = "latest_non_lossy" }
    }
  ],

  metadata_overrides = {
    "file:///music/inbox/song.flac" = {
      title = "Song",
      artist = "Artist"
    }
  }
}
```

---

## 9) Link Materialization Strategy

### 9.1 User-declared links only

mediapm does not invent library paths automatically by default.

- The user declares links in config.
- Reconciliation creates/updates/removes links to match declaration.

### 9.2 Link methods and fallback policy

Support:

1. Symlink
2. Hardlink
3. Reflink/clone (platform/filesystem permitting)
4. Copy (last resort)

Policy:

- User-configurable preferred order.
- Per-platform capability detection.
- Deterministic fallback with explicit log reasons.

### 9.3 Idempotency

If current link target already matches selected variant hash/object, no-op.

---

## 10) Minimal Command Set (Simplicity Requirement)

Keep commands intentionally small:

1. `mediapm sync`

   - Main command.
   - Reads config, imports/probes as needed, enriches metadata per policy, reconciles links.

2. `mediapm plan`

   - Dry-run output of effects without applying.

3. `mediapm verify`

   - Verifies object integrity and sidecar consistency.

4. `mediapm gc`

   - Garbage collect unreachable objects/variants not referenced by current declared graph (with safety modes).

5. `mediapm fmt` (optional but useful)

   - Formats/normalizes config and optionally canonicalizes JSON sidecars.

No additional commands unless they materially reduce complexity.

---

## 11) Rust Crate & Module Design

### 11.1 Workspace layout

```text
crates/
  mediapm-core/         # pure domain logic, models, planning
  mediapm-fs/           # filesystem effects and path/link operations
  mediapm-metadata/     # probing/tagging abstraction
  mediapm-provider/     # provider trait + implementations (musicbrainz)
  mediapm-config/       # Nickel integration and config normalization
  mediapm-cli/          # clap-based CLI wrapper
```

### 11.2 Purity boundaries

- `mediapm-core` should be IO-free.
- IO traits/interfaces injected into executor layer.

### 11.3 Suggested libraries (initial)

- CLI: `clap`
- Serialization: `serde`, `serde_json`
- Schema migration optics: `panproto-lens` (primary), `karpal-optics` (advanced composition), `focus-rs` (ergonomic deep-path transforms)
- Hashing: `blake3`
- URL canonicalization: `url`
- Error handling: `thiserror`, `miette`/`anyhow` (choose one style consistently)
- Time: `time` or `chrono`
- HTTP for providers: `reqwest` (with strict timeout/retry policy)
- Async runtime (if needed): `tokio` (only where needed)

Metadata/probing/tagging crates should be selected after implementation spike validation.

---

## 12) Performance Design (No DB, Still Fast)

### 12.1 Performance goals

- Fast startup for medium libraries.
- Efficient incremental sync.
- Minimal redundant hashing/probing.

### 12.2 Tactics

1. **Content hash cache hints in sidecars**

   - Store file size + mtime + hash so unchanged local file imports can skip rehash if policy allows safe shortcut.
   - Optionally strict mode always rehashes.

2. **Incremental reconciliation**

   - Only reprocess sources affected by config or source file changes.

3. **Memory indexes from JSON on startup**

   - Build compact hash maps once.

4. **Parallelism**

   - Parallel hash/probe for independent files with bounded worker pool.

5. **Avoid giant monolithic JSON**

   - Per-URI sidecars keep file IO localized.

---

## 13) Safety, Atomicity, and Recovery

### 13.1 Atomic write protocol

For every JSON update:

1. Write to temp in same directory.
2. Flush and fsync.
3. Atomic rename over target.

### 13.2 Crash safety

- Effects should be resumable.
- `sync` can be rerun safely after interruption.

### 13.3 Integrity verification

`verify` checks:

- Object file hash matches path hash.
- Sidecar references valid objects.
- Lineage references valid hashes/events.
- Link targets correspond to resolved declared state.

### 13.4 Corruption handling

- Mark corrupted entries explicitly in report.
- Never silently delete suspicious data.

---

## 14) Functional Data Flow (End-to-End)

1. Load and evaluate Nickel config -> normalized desired declarations.
2. Load JSON sidecars -> current state model.
3. Compute plan in pure core:

   - Required imports
   - Required probes
   - Required provider fetches
   - Required sidecar updates
   - Required link operations
   - Optional GC candidates

4. Present plan (`plan`) or execute (`sync`).
5. Persist updated sidecars atomically.
6. Materialize links.
7. Emit concise machine-readable and human-readable summary.

---

## 15) Detailed Roadmap (Phased)

### Phase 0 — Foundation & ADRs (1–2 weeks)

Deliverables:

- ADR: URI identity semantics.
- ADR: JSON sidecar schema v1.
- ADR: edit classification model.
- ADR: functional core/effect model.
- Skeleton Rust workspace and CI.

Exit criteria:

- Compiles and runs basic CLI scaffolding.

### Phase 1 — Core models + storage + hashing (1–2 weeks)

Deliverables:

- Canonical URI module.
- BLAKE3 hashing and object store pathing.
- Sidecar read/write with atomic persistence.
- `verify` partial checks.

Exit criteria:

- Import-by-URI and store object with sidecar created.

### Phase 2 — Metadata extraction + original snapshot (2–3 weeks)

Deliverables:

- Probe abstraction and first implementation.
- Capture original metadata snapshot on first import.
- Variant record creation pipeline.

Exit criteria:

- Supported formats produce stable metadata snapshot.

### Phase 3 — Declarative config (Nickel) + planning engine (2–4 weeks)

Deliverables:

- Nickel config evaluation and normalized model.
- Pure planner producing effects.
- `plan` command complete.

Exit criteria:

- Dry-run accurately predicts sync actions.

### Phase 4 — Link materializer + sync (2–3 weeks)

Deliverables:

- Cross-platform link strategy manager.
- Idempotent reconcile engine.
- `sync` MVP end-to-end.

Exit criteria:

- Declarative links reliably materialize and update.

### Phase 5 — Edit history model + transformations (2–4 weeks)

Deliverables:

- Edit event schema with reversible/non-reversible classes.
- Metadata edit event recording.
- Transcode event recording API (actual transcode delegated).

Exit criteria:

- Full lineage trail maintained across variants.

### Phase 6 — MusicBrainz provider integration (2–3 weeks)

Deliverables:

- Provider trait + MusicBrainz adapter.
- Cache and rate-limited client.
- Field-level provenance merge.

Exit criteria:

- Enrichment works with deterministic merge policy.

### Phase 7 — GC + hardening + migration support (2–3 weeks)

Deliverables:

- Reachability GC.
- Optics-based schema migration framework (`Lens`/`Prism`/`Traversal` chain manifests).
- Version-hop migration registry and provenance recording.
- Migration law/invariant test harness (focus-preservation and unknown-field retention checks).
- Robust verify and repair hints.

Exit criteria:

- Safe cleanup and upgrade path validated.

### Phase 8 — Polish, docs, and release prep (1–2 weeks)

Deliverables:

- User docs and examples.
- Performance benchmarks.
- Packaging/release artifacts.

Exit criteria:

- v0.1.0 release candidate.

---

## 16) Testing Strategy (Implementation-Grade)

### 16.1 Test pyramid

1. Unit tests (core logic)
2. Property tests (planner invariants/idempotency)
3. Integration tests (filesystem + sidecars + links)
4. Golden tests (config -> plan output snapshots)

### 16.2 Critical invariants

- Same input state + config => same plan.
- Running sync twice without changes => zero-op second run.
- Original snapshot immutability.
- Every variant hash points to existing object.
- Every edit event references valid from/to variants.
- Migration transforms preserve all non-focused fields unless a migration step explicitly changes them.
- Migration chain is deterministic for identical input sidecar bytes.
- Field-path optics used for stable fields satisfy expected Get/Put-style behavior in tests.

### 16.3 Cross-platform CI matrix

- Linux, macOS, Windows.
- Capability-aware tests for symlink/hardlink/reflink behavior.

---

## 17) Observability, Logs, and UX

### 17.1 Output modes

- Human-readable concise mode.
- JSON output mode for scripting.

### 17.2 Plan/apply summaries

Always summarize:

- imports
- metadata updates
- provider calls/cache hits
- link creates/updates/removals
- errors/warnings

### 17.3 Error model

- Rich actionable errors (what failed, why, suggested remediation).
- Distinguish hard errors vs partial failures.

---

## 18) Risk Register and Mitigations

1. **Metadata crate coverage gaps**

   - Mitigation: abstraction layer + format capability matrix + fallback behavior.

2. **Cross-platform link inconsistencies**

   - Mitigation: explicit capability detection + deterministic fallback + tests.

3. **JSON schema evolution complexity**

   - Mitigation: strict versioning and migration framework from day 1.

4. **Performance degradation on large libraries**

   - Mitigation: incremental planning, bounded parallelism, compact indexes.

5. **Provider API policy changes**

   - Mitigation: provider isolation, caching, configurable disablement.

---

## 19) Example Command Behavior Specs (Concise)

### 19.1 `mediapm plan`

- Reads config/state.
- Produces deterministic action list.
- No side effects.
- Exit code non-zero only on invalid config/state load errors.

### 19.2 `mediapm sync`

- Executes plan with side effects.
- Atomic sidecar updates.
- Safe rerun after interruption.

### 19.3 `mediapm verify`

- Full integrity scan.
- Reports corruption/inconsistency.
- Optional `--json` machine report.

### 19.4 `mediapm gc`

- Computes unreachable objects from declared roots.
- Default dry-run.
- `--apply` required for deletion.

---

## 20) Implementation Checklist (Granular)

### 20.1 Domain model checklist

- [ ] Canonical URI type + parser + normalizer
- [ ] Hash newtype `[u8; 32]`
- [ ] MediaRecord/VariantRecord/EditEvent types
- [ ] Revertable/non-revertable enum + validation
- [ ] Schema version constants

### 20.2 Storage checklist

- [ ] Object path derivation
- [ ] Object existence + integrity checks
- [ ] Sidecar atomic read/write helpers
- [ ] Temp/lock management

### 20.3 Planner checklist

- [ ] Desired-state model from config
- [ ] Diff engine current vs desired
- [ ] Effect enum and executor contract
- [ ] Stable sort ordering for deterministic plans

### 20.4 Metadata checklist

- [ ] Probe adapter trait
- [ ] Container/tag extraction mapping
- [ ] Original snapshot capture
- [ ] Metadata patch representation for revertable edits

### 20.5 Provider checklist

- [ ] Provider trait
- [ ] MusicBrainz client wrapper
- [ ] Caching
- [ ] Merge/provenance policies

### 20.6 Linker checklist

- [ ] Capability detection
- [ ] Symlink/hardlink/reflink/copy implementations
- [ ] Idempotent reconcile logic

### 20.7 Quality checklist

- [ ] Unit tests + property tests
- [ ] Integration tests across OS
- [ ] Benchmark harness
- [ ] User documentation

### 20.8 Migration checklist (Functional Optics)

- [ ] Define per-version migration chain registry (`v1->v2`, `v2->v3`, ...)
- [ ] Implement first migrations with `panproto-lens`
- [ ] Add Prism-based optional-field handling patterns
- [ ] Add Traversal-based array transforms for nested tag lists
- [ ] Add migration provenance writeback in sidecars
- [ ] Add unknown-field preservation regression tests
- [ ] Add migration idempotency + determinism tests

---

## 21) Additional Technical Refinements from Research (Integrated)

This section captures concrete, high-impact improvements to fold into implementation decisions.

### 21.1 Crates to prioritize after spike validation

- **Hashing**: `blake3` (primary content hash algorithm).
- **Metadata probing**: `symphonia` (safe probing path).
- **Tag read/write**: `lofty` (format tag support path).
- **Canonical JSON**: evaluate JCS-compatible strategy (`serde_jcs` or equivalent deterministic serializer policy).
- **Atomic file writes**: `atomic-write-file` (or equivalent robust atomic write utility).
- **Path safety**: `path-clean`.
- **Parallelism**: `rayon` for batch hashing/probing.

### 21.2 CAS and GC hardening

- Object path fan-out by leading hash bytes to avoid huge flat directories.
- Mark-and-sweep GC rooted from active declarative link graph + sidecar references.
- Optional quarantine folder for uncertain deletions prior to permanent removal.

### 21.3 Metadata layering in sidecars

Keep both:

- `raw` extracted fields (loss-minimizing capture), and
- `normalized` fields (stable app-facing schema),

plus provenance of how normalized values were derived.

### 21.4 MusicBrainz operational discipline

- Enforce request throttling centrally.
- Cache successful and not-found responses with TTL policy.
- Record provider match confidence and rationale in sidecar provenance fields.

### 21.5 Functional correctness tests

Property-based tests for:

- planner idempotency,
- effect ordering determinism,
- migration round-trips,
- merge associativity constraints where intended.

### 21.6 Migration implementation style

- Replace ad-hoc `match version { ... }` mapping blocks with a declarative migration chain.
- Prefer path-focused transforms over whole-struct rebuilds to reduce boilerplate and accidental field drops.
- Keep migrations composable and reviewable as a manifest of optics steps.
- Use `serde_json::Value`-first transforms for intermediate schema versions where full Rust types are unnecessary.

---

## 22) Non-Goals (To Protect Scope)

For initial releases, mediapm will **not**:

- Implement an internal downloader ecosystem.
- Auto-generate complex library hierarchies without explicit declarations.
- Provide heavy GUI management interface.
- Support every media/container edge case at v0.1.

---

## 23) Definition of Done (for “extremely good for another agent to implement”)

This plan is implementation-ready when the implementing agent can:

1. Build crate skeleton exactly as outlined.
2. Implement core models and planner without ambiguity.
3. Implement sidecar schema and migration path deterministically.
4. Implement link reconciliation behavior with platform fallbacks.
5. Implement provider integration behind trait boundaries.
6. Verify progress via explicit test and phase exit criteria.

---

## 24) Immediate Next Step Execution Plan (Actionable)

1. Create ADRs and lock decisions:

   - URI canonicalization contract
   - Sidecar schema v1
   - Effect model
   - Link fallback policy

2. Implement Phase 0 + Phase 1 skeleton in Rust workspace.
3. Add golden tests for `plan` output early before feature expansion.
4. Spike metadata adapters (probe/tag) on representative music files.
5. Finalize Nickel config schema contracts and error diagnostics.

---

## 25) Summary

mediapm will be a **Rust, declarative, workspace-local, URI-identity-first media artifact manager** with:

- content-addressed variant storage,
- JSON-only metadata state,
- explicit provenance and edit history,
- functional-core architecture,
- minimal command surface,
- and a practical path to high-quality metadata enrichment (starting with MusicBrainz).

It is intentionally engineered as a reproducible media state reconciler rather than a traditional mutable media manager.
