---
description: "Use when creating commits or editing commit workflow/policy files in this repository."
name: "Commit Message Policy"
applyTo: "**"
---

# Commit Message Policy

- Headers follow Conventional Commits: `type(scope): subject`. Scope is mandatory.
- Do not start headers with crate-prefix forms (`mediapm:`, `conductor:`, `cas:`, or similar `<crate-or-tool>:`). Crate/tool identity belongs in the scope.

Examples:

- ✅ `feat(mediapm): align demo-online sidecar hierarchy`
- ✅ `fix(conductor): reject invalid content_map traversal keys`
- ❌ `mediapm: align demo-online sidecar hierarchy`
- ❌ `conductor: reject invalid content_map traversal keys`

## Body rule

A commit message is a subject line and nothing else, unless a reader would misread the change from the subject alone.

Write a body only for one of three reasons: a mechanism was deleted, the behaviour now differs from a rule stated elsewhere, or a trade-off was accepted that a reviewer would otherwise object to.

When one applies, the body is at most three lines and states the why.

Do not restate the diff, list the files, quote test counts, or narrate the steps taken. A body that a reader can reconstruct from `git show` is noise, and it makes the log harder to scan than an empty one.
