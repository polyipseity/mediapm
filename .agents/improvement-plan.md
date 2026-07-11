# Instruction file improvement plan

Status summary: 2 files already deleted (`bases-preservation-in-put.instructions.md`, `tool-content-cache-refactoring.instructions.md`); 1 already fixed (`tooling-and-validation-detection.instructions.md` frontmatter). Remaining work below.

---

## Pass 1 — Targeted fixes (small, atomic, independent)

### 1.1 Fix stale `.gitkeep` language in `scripts-and-permissions.instructions.md`

- **File**: `scripts-and-permissions.instructions.md`
- **Change**: Replace "the directory may currently contain only `.gitkeep`" with current reality — it now has `build.rs` and `cargo-bin/`.
- **Rationale**: Stale description misleads agents.

### 1.2 Fix frontmatter field order in `versioning-and-migration.instructions.md`

- **File**: `versioning-and-migration.instructions.md`
- **Change**: Swap `name` and `description` so `description` comes first, matching every other instruction file.
- **Rationale**: Inconsistent frontmatter may confuse agent loading; all other files use `description` then `name`.

### 1.3 Remove stale reference in `spec-development-index.instructions.md`

- **File**: `spec-development-index.instructions.md`
- **Change**: Remove the row referencing the deleted `tool-content-cache-refactoring.instructions.md` from the "Related Instruction Files" table.
- **Rationale**: Dead link to a deleted file.

---

## Pass 2 — Merge detection trifecta

### 2.1 Consolidate `language-and-stack-detection.instructions.md` and `tooling-and-validation-detection.instructions.md`

- **Files**: Both files.
- **Change**: Merge into one file named `stack-and-tooling-detection.instructions.md`. Keep the narrower `applyTo` from `tooling-and-validation-detection` (adds tooling-specific config files). Delete the two originals.
- **Content to merge**: Both have very similar "detect before prescribing" rules. The `tooling-and-validation-detection` file adds tooling-specific sections (config coordination, validation guidance). The `language-and-stack-detection` file has detection order and evidence standards. Combine cleanly.
- **Rationale**: Eliminates ~50% duplication between these files.

### 2.2 Remove "Detection-first customization updates" from `markdown-and-customizations.instructions.md`

- **File**: `markdown-and-customizations.instructions.md`
- **Change**: Delete the 8-line "Detection-first customization updates" section. Replace with a one-sentence link referring to the merged detection instruction file.
- **Rationale**: That section says the same thing as the two detection files. After the merge in 2.1, one reference is enough.

---

## Pass 3 — Prune `mediapm-architecture.instructions.md`

This is the largest instruction file (~600 lines) and the primary source of duplication with root `AGENTS.md` and other instruction files.

### 3.1 Remove module layout section (duplicates `rust-workflow.instructions.md`)

- **Change**: Delete the "Module layout (source of truth)" block and the "When splitting one Rust module into multiple files" paragraph. These duplicate `rust-workflow.instructions.md` (and partly `AGENTS.md`).
- **Reference to keep**: Replace with "See `.agents/instructions/rust-workflow.instructions.md` for module split conventions."

### 3.2 Remove managed-tool default values (belong in per-crate AGENTS.md)

- **Change**: Delete the detailed managed-tool default specifications (yt-dlp settings, ffmpeg settings, rsgain settings, media-tagger settings — roughly 100+ lines of bullet points). These are managed-tool configuration defaults, not architecture invariants.
- **Where it goes**: These belong in `src/mediapm/AGENTS.md` where tool provisioning and managed-tool reconciliation is documented.
- **Rationale**: Architecture invariants should outlast implementation details. Tool defaults change when upstream tools update; architecture rules should not.

### 3.3 Remove `versions/` boundary policy (duplicates `versioning-and-migration.instructions.md`)

- **Change**: Delete the "Strict `versions/` boundary policy" section. It duplicates content from `versioning-and-migration.instructions.md`.
- **Reference to keep**: Replace with "See `.agents/instructions/versioning-and-migration.instructions.md` for version boundary policy."

### 3.4 Remove "CLI/API parity contract" section (duplicates `AGENTS.md` and `rust-workflow.instructions.md`)

- **Change**: Delete the CLI/API parity section. It duplicates `AGENTS.md` "CLI and API Parity" section and `rust-workflow.instructions.md` "CLI/API parity contract".
- **Rationale**: Source of truth is `AGENTS.md`.

### 3.5 Remove "Validation checklist" section (duplicates `rust-workflow.instructions.md`)

- **Change**: Delete the "Validation checklist after Rust edits" section at the end. It just references `rust-workflow.instructions.md`.
- **Rationale**: Redundant reference when the content it wraps is already in the referenced file.

### 3.6 Remove "Documentation requirements" section (duplicates `mediapm-testing-and-docstrings.instructions.md`)

- **Change**: Delete the "Documentation requirements for Rust code" section. It duplicates `mediapm-testing-and-docstrings.instructions.md` and `rust-workflow.instructions.md`.
- **Rationale**: File already exists dedicated to this topic.

### Result after Pass 3

`mediapm-architecture.instructions.md` shrinks from ~600 lines to ~200-250 lines, focused on actual cross-crate architecture invariants without duplication. The managed-tool defaults move to `src/mediapm/AGENTS.md` where they belong.

---

## Pass 4 — Deduplicate validation commands

### 4.1 Make `rust-workflow.instructions.md` canonical for validation

- **Change**: In `AGENTS.md` "Build and Test" section, replace the detailed command listing with a short summary + link to `rust-workflow.instructions.md`.
- **Change**: In `mediapm-testing-and-docstrings.instructions.md` "Validation commands" section, replace the command listing with a link to `rust-workflow.instructions.md`.
- **Rationale**: Validation commands currently listed in 3 places with ~80% content overlap. One source of truth, two references.

---

## Pass 5 — General deduplication across all files

### 5.1 Deduplicate module split convention

- **Current locations**: `AGENTS.md` (Conventions section), `rust-workflow.instructions.md`, `mediapm-architecture.instructions.md` (to be removed in 3.1).
- **Change**: Keep canonical version in `rust-workflow.instructions.md`. In `AGENTS.md`, replace the ~6-line module split paragraph with "See `rust-workflow.instructions.md` for module split conventions."

### 5.2 Deduplicate docstring requirements

- **Current locations**: `mediapm-testing-and-docstrings.instructions.md` (Rustdoc depth section), `rust-workflow.instructions.md` (Docstring completion bar), and `AGENTS.md` (Conventions).
- **Change**: Keep canonical version in `mediapm-testing-and-docstrings.instructions.md`. In `rust-workflow.instructions.md`, replace the "Docstring completion bar" section with a link. In `AGENTS.md`, reduce the 2-line docstring reference to "See `mediapm-testing-and-docstrings.instructions.md` for Rustdoc depth requirements."

### 5.3 Deduplicate CLI/API parity

- **Current locations**: `AGENTS.md` (dedicated section), `rust-workflow.instructions.md` (dedicated section).
- **Change**: Keep canonical in `AGENTS.md`. Replace the version in `rust-workflow.instructions.md` with a link.

### 5.4 Deduplicate frontmatter conventions

- **Current locations**: Global `workspace-guidance.instructions.md` (detailed), repo `markdown-and-customizations.instructions.md` (partial overlap).
- **Change**: In `markdown-and-customizations.instructions.md` "Customization frontmatter" section, remove the field conventions table and replace with one line: "Follow the frontmatter conventions in global `workspace-guidance.instructions.md`."
- **Rationale**: Global file is the canonical source.

---

## Pass 6 — Remove planning artifacts from `spec-development-index.instructions.md`

### 6.1 Remove Glossary, FAQ, Risk Assessment sections

- **File**: `spec-development-index.instructions.md`
- **Change**: Delete the Glossary, FAQ, Risk Assessment, and Testing Coverage Gaps sections. These are documentation/planning artifacts, not actionable instructions.
- **Rationale**: Keeps the file focused on its purpose — a cross-reference index. The removed content exists nowhere else and is not actively used. If needed, it can be archived to a README or a doc note.

### 6.2 Review "Cross-Crate Ambiguity Registry" and "Source Temp File Distribution"

- **Decision**: Keep these — they are cross-reference tables that serve the index purpose. The temp file distribution table is a historical note about the spec deletion migration, which remains useful for understanding where content went.
- **Change**: None needed for these sections.

---

## Pass 7 — (Optional) Add missing instruction files

### 7.1 Add `nickel.instructions.md`

- **New file**: `.agents/instructions/nickel.instructions.md`
- **`applyTo`**: `**/*.ncl`
- **Content**: Nickel file conventions for this repo — schema version markers, config document patterns, common pitfalls. Keep brief (1-2 paragraphs + bullets).
- **Priority**: Low. The `.ncl` files are used but infrequently edited.

### 7.2 Add `toml.instructions.md`

- **New file**: `.agents/instructions/toml.instructions.md`
- **`applyTo`**: `**/*.toml`
- **Content**: TOML conventions for this repo — dotted key usage, value formatting, comments, and file-specific rules (Cargo.toml, prek.toml, clippy.toml, rustfmt.toml).
- **Priority**: Low. TOML files are well-understood and rarely need agent guidance.

---

## Pass 8 — (Optional) Global instruction improvements

### 8.1 Split `core-behavior.instructions.md`

- **Change**: Extract multi-edit fallback, tool-retry discipline, and investigation protocol into a focused `execution-details.instructions.md`. Keep `core-behavior.md` with communication, terminal hygiene, research scope, and premise integrity — the truly invariant rules.
- **Rationale**: Keeps the core file lean (agents load it every turn). The extracted sections change more frequently as tool APIs evolve.
- **Priority**: Optional. The file is long but not bloated.

### 8.2 Add logging/progress protocol note

- **Change**: In `core-behavior.instructions.md`, consider adding a brief note about preferring structured tool output over verbose terminal logging, based on observed pattern of large terminal outputs causing context overflow (the most common input-token waste pattern).
- **Priority**: Low. The existing "Terminal hygiene" section already addresses this.

---

## Execution order (recommended)

```text
Pass 1 (targeted fixes) → atomic commits
Pass 2 (merge detection) → atomic commits
Pass 3 (prune arch file) → atomic commits
Pass 4 (dedup validation) → atomic commits
Pass 5 (general dedup) → # can split into sub-5.1, 5.2, 5.3, 5.4
Pass 6 (remove planning artifacts) → atomic commit
Pass 7 (optional: new instruction files) → separate pass
Pass 8 (optional: global instructions) → separate pass
```

Passes 1-6 can be done in parallel via `maintain` subagents on independent lanes. Passes 7-8 are optional and independent.
