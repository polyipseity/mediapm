---
description: "Use when creating commits or editing commit workflow/policy files in this repository."
name: "Commit Message Policy"
applyTo: "**"
---

# Commit Message Policy

## Required format

- Commit headers must follow Conventional Commits shape: `type(scope): subject`.
- Scope is mandatory for normal project commits.

## Forbidden crate-prefix headers

- Do not start commit headers with crate-prefix forms such as:
  - `mediapm: ...`
  - `conductor: ...`
  - `cas: ...`
  - or similar `<crate-or-tool>:` headers.
- Crate/tool identity belongs in the Conventional Commit scope, not as the header prefix.

## Examples

- ✅ `feat(mediapm): align demo-online sidecar hierarchy`
- ✅ `fix(conductor): reject invalid content_map traversal keys`
- ❌ `mediapm: align demo-online sidecar hierarchy`
- ❌ `conductor: reject invalid content_map traversal keys`

## Specification references

- Deleted monolithic files (`crate-specifications.md`, `elaboration-pass-edge-cases.md`). Use `.agents/instructions/spec-development-index.instructions.md` to locate the relevant per-crate AGENTS.md for any specification or edge-case content.

## Atomic commit workflow (unstaged changes → multiple commits)

When you have a set of unstaged changes and need to split them into multiple atomic commits, use this stash-first workflow.

1. **Format first** — Run the repo formatter so the working tree matches what the pre-commit hook will produce during `git commit`. This prevents merge conflicts when the stash (pre-format snapshot) is popped against hook-formatted files.

   ```bash
   prek run --all-files
   ```

   If that is unavailable, run formatters manually:

   ```bash
   cargo fmt && prek run end-of-file-fixer trailing-whitespace rumdl-fmt --all-files
   ```

2. `git stash` — save everything, get a clean working tree.
3. `git stash list` — note the stash reference (e.g. `stash@{0}`).
4. For each atomic commit you want to create:
   a. `git stash pop` — restore all remaining stashed changes to the working tree.
   b. `git add -p` (or `git add <specific-files>`) — stage **only** the changes that belong to this commit. Use interactive hunk selection if a single file contains changes for multiple commits.
   c. `git stash -u --keep-index` — stash everything that remains unstaged while keeping staged changes intact, so the pre-commit hook sees a clean working tree.
   d. `git commit -m "type(scope): precise message"` — commit only staged changes in a clean tree.
   - **If the commit fails because hooks modified staged files** (e.g., `cargo fmt` auto-fixed formatting): the hook-produced changes are now unstaged modifications. Stage them (`git add <modified-files>`) and retry step 4d. Do NOT retry without staging first — the hook will produce the same modifications and fail identically.
     e. `git stash list` — verify stash state is as expected.
5. After all commits are created, `git stash pop` any remaining leftovers.

### Hard rules

- **NEVER** rely on staged vs unstaged to separate changes across multiple commits. Pre-commit hooks (fmt, commitlint) may stash and pop, destroying the staged/unstaged boundary mid-flight.
- **NEVER** use `git commit --amend` while combining changes from multiple sources — it re-opens the last commit and interacts catastrophically with hook-driven stash/pop cycles.
- **ALWAYS** keep explicit track of the stash stack (`git stash list`) after every stash operation.
- **ALWAYS** use `git add -p` for fine-grained separation when one file has changes belonging to multiple commits.
- **NEVER** ask subagents to run git commit. Commit MUST be done by the MAIN agent to prevent race conditions.
